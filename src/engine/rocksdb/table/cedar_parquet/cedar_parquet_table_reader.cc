// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "table/cedar_parquet/cedar_parquet_table_reader.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <utility>

#include "db/dbformat.h"
#include "memory/arena.h"
#include "table/cedar_parquet/parquet_plain_page.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"

namespace ROCKSDB_NAMESPACE::cedar_parquet {
namespace {

constexpr char kParquetMagic[] = "PAR1";
constexpr uint32_t kCedarFactsColumnFamilyId = 1;

Status ValidateCedarParquetFileIdentity(const CedarParquetFooter& footer) {
  const std::array<std::pair<const char*, const char*>, 7> required = {{
      {"cedar.format", kCedarParquetFormatVersion},
      {"cedar.canonical_schema", kCedarParquetCanonicalSchemaDigest},
      {"cedar.materialized_schema", kCedarParquetCanonicalSchemaDigest},
      {"cedar.comparator", kCedarParquetBytewiseComparatorName},
      {"cedar.comparator_digest", kCedarParquetComparatorDigest},
      {"cedar.fact_key_format", "part32.fact.v2"},
      {"cedar.feature_bits", kCedarParquetFeatureBits},
  }};
  if (footer.created_by != kCedarParquetCreatedBy ||
      footer.key_value_metadata.size() != required.size()) {
    return Status::Corruption("invalid Cedar Parquet file identity");
  }
  for (const auto& expected : required) {
    const CedarParquetKeyValueMetadata* found = nullptr;
    for (const auto& metadata : footer.key_value_metadata) {
      if (metadata.key != expected.first) continue;
      if (found != nullptr || !metadata.value.has_value()) {
        return Status::Corruption("invalid Cedar Parquet file identity");
      }
      found = &metadata;
    }
    if (found == nullptr || *found->value != expected.second) {
      return Status::Corruption("invalid Cedar Parquet file identity");
    }
  }
  return Status::OK();
}

struct CedarParquetFileRange {
  uint64_t offset;
  uint64_t size;
};

Status ValidateCanonicalRowBytes(const std::vector<std::string>& sort_keys,
                                 const std::vector<std::string>& internal_keys,
                                 const std::vector<std::string>& values,
                                 uint64_t max_row_bytes) {
  if (sort_keys.size() != internal_keys.size() ||
      sort_keys.size() != values.size()) {
    return Status::Corruption("Cedar Parquet canonical row length mismatch");
  }
  for (size_t index = 0; index < sort_keys.size(); ++index) {
    uint64_t charge = 0;
    for (const std::string* column : {&sort_keys[index], &internal_keys[index],
                                      &values[index]}) {
      if (charge > max_row_bytes ||
          column->size() > max_row_bytes - charge ||
          4 > max_row_bytes - charge - column->size()) {
        return Status::Corruption("Cedar Parquet row exceeds configured bound");
      }
      charge += column->size() + 4;
    }
  }
  return Status::OK();
}

uint32_t DecodeFixed32(const char* source) {
  uint32_t value = 0;
  for (uint32_t shift = 0; shift < 32; shift += 8) {
    value |= static_cast<uint32_t>(static_cast<unsigned char>(source[shift / 8])) << shift;
  }
  return value;
}

Status ReadExact(const RandomAccessFileReader* file, uint64_t offset, size_t size,
                 std::string* bytes) {
  bytes->assign(size, '\0');
  Slice result;
  IOStatus io_status = file->Read(IOOptions(), offset, size, &result, bytes->data(), nullptr);
  if (!io_status.ok()) return Status(io_status);
  if (result.size() != size) return Status::Corruption("truncated Cedar Parquet file");
  if (result.data() != bytes->data()) bytes->assign(result.data(), result.size());
  return Status::OK();
}

Status MakeCedarParquetSeekKey(const Slice& key, std::string* sort_key,
                               bool* is_prefix) {
  if (key.size() > 8 && key.size() < kCedarParquetV2UserKeyBytes &&
      static_cast<uint8_t>(key[0]) == 2) {
    sort_key->assign(key.data(), key.size() - 8);
    *is_prefix = true;
    return Status::OK();
  }
  if (key.size() <= kCedarParquetV2UserKeyBytes && !key.empty() &&
      static_cast<uint8_t>(key[0]) == 2) {
    sort_key->assign(key.data(), key.size());
    *is_prefix = true;
    return Status::OK();
  }
  *is_prefix = false;
  return EncodeCedarParquetSortKey(key, sort_key);
}

std::string LittleEndian(uint64_t value, size_t bytes) {
  std::string encoded(bytes, '\0');
  for (size_t index = 0; index < bytes; ++index) {
    encoded[index] = static_cast<char>(value >> (index * 8));
  }
  return encoded;
}

std::string Float32Bytes(float value) {
  uint32_t bits = 0;
  std::memcpy(&bits, &value, sizeof(bits));
  return LittleEndian(bits, sizeof(bits));
}

std::string Float64Bytes(double value) {
  uint64_t bits = 0;
  std::memcpy(&bits, &value, sizeof(bits));
  return LittleEndian(bits, sizeof(bits));
}

std::optional<std::string> ExpectedMaterializedValue(
    const CedarParquetMaterializedFact& fact, size_t column_index) {
  switch (column_index) {
    case 3: return LittleEndian(fact.part_id, 4);
    case 4: return LittleEndian(fact.fact_family, 4);
    case 5: return LittleEndian(fact.property_id, 4);
    case 6: return LittleEndian(fact.entity_id, 8);
    case 7: return LittleEndian(fact.valid_from, 8);
    case 8: return LittleEndian(fact.cedar_commit_seq, 8);
    case 9: return LittleEndian(fact.rocksdb_sequence, 8);
    case 10: return fact.rocksdb_deletion ? std::nullopt
                                           : std::optional<std::string>(LittleEndian(fact.operation, 4));
    case 11: return fact.rocksdb_deletion ? std::nullopt
                                           : std::optional<std::string>(LittleEndian(fact.schema_epoch, 4));
    case 12: return fact.rocksdb_deletion ? std::nullopt
                                           : std::optional<std::string>(LittleEndian(fact.physical_type, 4));
    case 13: return fact.bool_value.has_value()
                         ? std::optional<std::string>(std::string(1, *fact.bool_value ? '\1' : '\0'))
                         : std::nullopt;
    case 14: return fact.int32_value.has_value()
                         ? std::optional<std::string>(LittleEndian(
                               static_cast<uint32_t>(*fact.int32_value), 4))
                         : std::nullopt;
    case 15: return fact.int64_value.has_value()
                         ? std::optional<std::string>(LittleEndian(
                               static_cast<uint64_t>(*fact.int64_value), 8))
                         : std::nullopt;
    case 16: return fact.float32_value.has_value()
                         ? std::optional<std::string>(Float32Bytes(*fact.float32_value))
                         : std::nullopt;
    case 17: return fact.float64_value.has_value()
                         ? std::optional<std::string>(Float64Bytes(*fact.float64_value))
                         : std::nullopt;
    case 18: return fact.timestamp64_value.has_value()
                         ? std::optional<std::string>(LittleEndian(*fact.timestamp64_value, 8))
                         : std::nullopt;
    case 19: return fact.bytes_value;
    case 20: return fact.source_part_id.has_value()
                         ? std::optional<std::string>(LittleEndian(*fact.source_part_id, 4))
                         : std::nullopt;
    case 21: return fact.source_vertex_id.has_value()
                         ? std::optional<std::string>(LittleEndian(*fact.source_vertex_id, 8))
                         : std::nullopt;
    case 22: return fact.target_part_id.has_value()
                         ? std::optional<std::string>(LittleEndian(*fact.target_part_id, 4))
                         : std::nullopt;
    case 23: return fact.target_vertex_id.has_value()
                         ? std::optional<std::string>(LittleEndian(*fact.target_vertex_id, 8))
                         : std::nullopt;
    case 24: return fact.edge_type.has_value()
                         ? std::optional<std::string>(LittleEndian(*fact.edge_type, 8))
                         : std::nullopt;
    default: return std::nullopt;
  }
}

bool IsMaterializedColumn(CedarParquetColumnId id) {
  return static_cast<uint8_t>(id) >= 3 && static_cast<uint8_t>(id) <= 24;
}

Status MakeColumnVector(CedarParquetColumnId id, CedarParquetColumnVector* column) {
  column->id = id;
  column->present.clear();
  switch (id) {
    case CedarParquetColumnId::kPartId:
    case CedarParquetColumnId::kFactFamily:
    case CedarParquetColumnId::kPropertyId:
    case CedarParquetColumnId::kOperation:
    case CedarParquetColumnId::kSchemaEpoch:
    case CedarParquetColumnId::kPhysicalType:
    case CedarParquetColumnId::kSourcePartId:
    case CedarParquetColumnId::kTargetPartId:
      column->values = std::vector<uint32_t>{};
      return Status::OK();
    case CedarParquetColumnId::kEntityId:
    case CedarParquetColumnId::kValidFrom:
    case CedarParquetColumnId::kCedarCommitSeq:
    case CedarParquetColumnId::kRocksdbSequence:
    case CedarParquetColumnId::kTimestamp64Value:
    case CedarParquetColumnId::kSourceVertexId:
    case CedarParquetColumnId::kTargetVertexId:
    case CedarParquetColumnId::kEdgeType:
      column->values = std::vector<uint64_t>{};
      return Status::OK();
    case CedarParquetColumnId::kBoolValue:
      column->values = std::vector<uint8_t>{};
      return Status::OK();
    case CedarParquetColumnId::kInt32Value:
      column->values = std::vector<int32_t>{};
      return Status::OK();
    case CedarParquetColumnId::kInt64Value:
      column->values = std::vector<int64_t>{};
      return Status::OK();
    case CedarParquetColumnId::kFloat32Value:
      column->values = std::vector<float>{};
      return Status::OK();
    case CedarParquetColumnId::kFloat64Value:
      column->values = std::vector<double>{};
      return Status::OK();
    case CedarParquetColumnId::kBytesValue:
      column->values = std::vector<std::string>{};
      return Status::OK();
    default:
      return Status::InvalidArgument("Cedar Parquet scan projection is not materialized");
  }
}

uint64_t DecodeLittleEndian(const std::string& bytes) {
  uint64_t value = 0;
  for (size_t index = 0; index < bytes.size(); ++index) {
    value |= static_cast<uint64_t>(static_cast<unsigned char>(bytes[index])) <<
             (index * 8);
  }
  return value;
}

bool NumericPageIntersects(
    const CedarParquetFooter::ColumnChunk::PageIndex& page,
    const std::optional<uint64_t>& lower,
    const std::optional<uint64_t>& upper) {
  if (page.all_null) return false;
  uint64_t minimum = 0;
  uint64_t maximum = 0;
  if (!DecodeCedarParquetNumericIndexValue(page.min_value, &minimum) ||
      !DecodeCedarParquetNumericIndexValue(page.max_value, &maximum)) {
    return false;
  }
  return (!lower.has_value() || maximum >= *lower) &&
         (!upper.has_value() || minimum <= *upper);
}

Status ValidateNumericPageIndex(
    const CedarParquetFooter::ColumnChunk::PageIndex& page) {
  if (page.all_null) return Status::OK();
  uint64_t minimum = 0;
  uint64_t maximum = 0;
  if (!DecodeCedarParquetNumericIndexValue(page.min_value, &minimum) ||
      !DecodeCedarParquetNumericIndexValue(page.max_value, &maximum) ||
      minimum > maximum) {
    return Status::Corruption("invalid Cedar numeric column-index bounds");
  }
  return Status::OK();
}

Status AppendProjectedValue(CedarParquetColumnVector* column,
                            const std::optional<std::string>& encoded) {
  column->present.push_back(encoded.has_value() ? 1 : 0);
  const auto require_size = [&encoded](size_t size) {
    return encoded.has_value() && encoded->size() != size;
  };
  if (auto* uint32_values = std::get_if<std::vector<uint32_t>>(&column->values)) {
    if (require_size(4)) return Status::Corruption("invalid Cedar Parquet uint32 vector value");
    uint32_values->push_back(encoded.has_value() ? static_cast<uint32_t>(DecodeLittleEndian(*encoded))
                                                 : 0U);
  } else if (auto* uint64_values = std::get_if<std::vector<uint64_t>>(&column->values)) {
    if (require_size(8)) return Status::Corruption("invalid Cedar Parquet uint64 vector value");
    uint64_values->push_back(encoded.has_value() ? DecodeLittleEndian(*encoded) : 0U);
  } else if (auto* int32_values = std::get_if<std::vector<int32_t>>(&column->values)) {
    if (require_size(4)) return Status::Corruption("invalid Cedar Parquet int32 vector value");
    int32_values->push_back(encoded.has_value()
                                ? static_cast<int32_t>(static_cast<uint32_t>(DecodeLittleEndian(*encoded)))
                                : 0);
  } else if (auto* int64_values = std::get_if<std::vector<int64_t>>(&column->values)) {
    if (require_size(8)) return Status::Corruption("invalid Cedar Parquet int64 vector value");
    int64_values->push_back(encoded.has_value() ? static_cast<int64_t>(DecodeLittleEndian(*encoded)) : 0);
  } else if (auto* float_values = std::get_if<std::vector<float>>(&column->values)) {
    if (require_size(4)) return Status::Corruption("invalid Cedar Parquet float32 vector value");
    uint32_t bits = encoded.has_value() ? static_cast<uint32_t>(DecodeLittleEndian(*encoded)) : 0;
    float value = 0;
    std::memcpy(&value, &bits, sizeof(value));
    float_values->push_back(value);
  } else if (auto* double_values = std::get_if<std::vector<double>>(&column->values)) {
    if (require_size(8)) return Status::Corruption("invalid Cedar Parquet float64 vector value");
    uint64_t bits = encoded.has_value() ? DecodeLittleEndian(*encoded) : 0;
    double value = 0;
    std::memcpy(&value, &bits, sizeof(value));
    double_values->push_back(value);
  } else if (auto* bool_values = std::get_if<std::vector<uint8_t>>(&column->values)) {
    if (require_size(1)) return Status::Corruption("invalid Cedar Parquet bool vector value");
    bool_values->push_back(encoded.has_value() ? static_cast<uint8_t>((*encoded)[0]) : 0);
  } else if (auto* bytes_values = std::get_if<std::vector<std::string>>(&column->values)) {
    bytes_values->push_back(encoded.value_or(std::string()));
  } else {
    return Status::Corruption("unknown Cedar Parquet typed vector");
  }
  return Status::OK();
}

}  // namespace

CedarParquetProjectedCursor::CedarParquetProjectedCursor(
    const CedarParquetTableReader* reader, CedarParquetScanSpec spec,
    bool request_encoded_values, std::vector<CedarParquetColumnVector> columns)
    : reader_(reader),
      spec_(std::move(spec)),
      request_encoded_values_(request_encoded_values),
      columns_(std::move(columns)),
      status_(Status::OK()) {
  batch_.columns = columns_;
  for (auto& column : batch_.columns) {
    std::visit([](auto& values) { values.clear(); }, column.values);
    column.present.clear();
  }
  status_ = LoadNextPage();
}

bool CedarParquetProjectedCursor::Valid() const {
  return status_.ok() && row_group_index_ < reader_->footer_.row_groups.size() &&
         row_index_ < batch_.internal_keys.size();
}

Slice CedarParquetProjectedCursor::internal_key() const {
  if (!Valid()) return Slice();
  return Slice(batch_.internal_keys[row_index_]);
}

void CedarParquetProjectedCursor::Next() {
  if (!Valid()) return;
  ++row_index_;
  if (row_index_ < batch_.internal_keys.size()) return;
  ++page_index_;
  status_ = LoadNextPage();
}

Status CedarParquetProjectedCursor::LoadNextPage() {
  batch_.internal_keys.clear();
  batch_.encoded_values.clear();
  batch_.columns = columns_;
  for (auto& column : batch_.columns) {
    std::visit([](auto& values) { values.clear(); }, column.values);
    column.present.clear();
  }
  row_index_ = 0;

  while (row_group_index_ < reader_->footer_.row_groups.size()) {
    const auto& row_group = reader_->footer_.row_groups[row_group_index_];
    const auto& sort_column = row_group.columns.front();
    const auto& valid_column = row_group.columns[7];
    const auto& commit_column = row_group.columns[8];
    if (page_index_ >= sort_column.page_locations.size()) {
      ++row_group_index_;
      page_index_ = 0;
      continue;
    }
    const auto& page_bounds = sort_column.page_indexes[page_index_];
    if ((spec_.valid_from_min.has_value() || spec_.valid_from_max.has_value()) &&
        !NumericPageIntersects(valid_column.page_indexes[page_index_],
                               spec_.valid_from_min, spec_.valid_from_max)) {
      if (spec_.stats != nullptr) ++spec_.stats->pages_skipped;
      ++page_index_;
      continue;
    }
    if ((spec_.cedar_commit_seq_min.has_value() ||
         spec_.cedar_commit_seq_max.has_value()) &&
        !NumericPageIntersects(commit_column.page_indexes[page_index_],
                               spec_.cedar_commit_seq_min,
                               spec_.cedar_commit_seq_max)) {
      if (spec_.stats != nullptr) ++spec_.stats->pages_skipped;
      ++page_index_;
      continue;
    }
    if (spec_.sort_key_lower.has_value() &&
        page_bounds.max_value < *spec_.sort_key_lower) {
      ++page_index_;
      continue;
    }
    if (spec_.sort_key_upper.has_value() &&
        page_bounds.min_value > *spec_.sort_key_upper) {
      row_group_index_ = reader_->footer_.row_groups.size();
      return Status::OK();
    }

    if (spec_.stats != nullptr) {
      ++spec_.stats->pages_read;
      for (const auto& column : row_group.columns) {
        spec_.stats->bytes_read += static_cast<uint64_t>(
            column.page_locations[page_index_].compressed_page_size);
      }
    }

    std::vector<std::string> sort_keys;
    std::vector<std::string> internal_keys;
    Status status = reader_->DecodeColumnPage(row_group_index_, 0, page_index_,
                                              &sort_keys);
    if (!status.ok()) return status;
    status = reader_->DecodeColumnPage(row_group_index_, 1, page_index_,
                                       &internal_keys);
    if (!status.ok()) return status;
    if (sort_keys.size() != internal_keys.size()) {
      return Status::Corruption("Cedar projected cursor canonical page length mismatch");
    }

    std::vector<std::vector<std::optional<std::string>>> projected_pages;
    projected_pages.reserve(batch_.columns.size());
    for (const auto& column : batch_.columns) {
      std::vector<std::optional<std::string>> values;
      status = reader_->DecodePrimitiveColumnPage(
          row_group_index_, static_cast<size_t>(column.id), page_index_, &values);
      if (!status.ok()) return status;
      if (values.size() != internal_keys.size()) {
        return Status::Corruption("Cedar projected cursor column length mismatch");
      }
      projected_pages.push_back(std::move(values));
    }
    std::vector<std::optional<std::string>> valid_times;
    std::vector<std::optional<std::string>> commit_sequences;
    if (spec_.valid_from_min.has_value() || spec_.valid_from_max.has_value()) {
      status = reader_->DecodePrimitiveColumnPage(row_group_index_, 7, page_index_,
                                                  &valid_times);
      if (!status.ok()) return status;
    }
    if (spec_.cedar_commit_seq_min.has_value() ||
        spec_.cedar_commit_seq_max.has_value()) {
      status = reader_->DecodePrimitiveColumnPage(row_group_index_, 8, page_index_,
                                                  &commit_sequences);
      if (!status.ok()) return status;
    }
    std::vector<std::string> encoded_values;
    if (request_encoded_values_) {
      status = reader_->DecodeColumnPage(row_group_index_, 2, page_index_,
                                         &encoded_values);
      if (!status.ok()) return status;
      if (encoded_values.size() != internal_keys.size()) {
        return Status::Corruption(
            "Cedar projected cursor encoded-value page length mismatch");
      }
    }
    for (size_t row = 0; row < internal_keys.size(); ++row) {
      if (spec_.sort_key_lower.has_value() &&
          sort_keys[row] < *spec_.sort_key_lower) {
        continue;
      }
      if (spec_.sort_key_upper.has_value() &&
          sort_keys[row] > *spec_.sort_key_upper) {
        // Keep this row group live until the buffered in-range rows are consumed.
        break;
      }
      if (!valid_times.empty() &&
          (!valid_times[row].has_value() ||
           (spec_.valid_from_min.has_value() &&
            DecodeLittleEndian(*valid_times[row]) < *spec_.valid_from_min) ||
           (spec_.valid_from_max.has_value() &&
            DecodeLittleEndian(*valid_times[row]) > *spec_.valid_from_max))) {
        continue;
      }
      if (!commit_sequences.empty() &&
          (!commit_sequences[row].has_value() ||
           (spec_.cedar_commit_seq_min.has_value() &&
            DecodeLittleEndian(*commit_sequences[row]) < *spec_.cedar_commit_seq_min) ||
           (spec_.cedar_commit_seq_max.has_value() &&
            DecodeLittleEndian(*commit_sequences[row]) > *spec_.cedar_commit_seq_max))) {
        continue;
      }
      batch_.internal_keys.push_back(internal_keys[row]);
      if (request_encoded_values_) batch_.encoded_values.push_back(encoded_values[row]);
      for (size_t column = 0; column < batch_.columns.size(); ++column) {
        status = AppendProjectedValue(&batch_.columns[column],
                                      projected_pages[column][row]);
        if (!status.ok()) return status;
      }
    }
    if (!batch_.internal_keys.empty()) {
      if (spec_.stats != nullptr) {
        spec_.stats->rows_emitted += batch_.internal_keys.size();
      }
      return Status::OK();
    }
    if (row_group_index_ >= reader_->footer_.row_groups.size()) return Status::OK();
    ++page_index_;
  }
  return Status::OK();
}

class CedarParquetTableIterator final : public InternalIterator {
 public:
  explicit CedarParquetTableIterator(const CedarParquetTableReader* reader)
      : reader_(reader) {}

  bool Valid() const override {
    return status_.ok() && row_group_index_ < reader_->footer_.row_groups.size() &&
           decoded_ != nullptr && row_index_ < decoded_->internal_keys.size();
  }

  void SeekToFirst() override { PositionAt(0, 0, 0); }

  void SeekToLast() override {
    if (reader_->footer_.row_groups.empty()) {
      Invalidate();
      return;
    }
    const size_t group = reader_->footer_.row_groups.size() - 1;
    const size_t page = reader_->footer_.row_groups[group].columns.front().page_locations.size() - 1;
    if (!Load(group, page)) return;
    row_index_ = decoded_->internal_keys.empty() ? 0 : decoded_->internal_keys.size() - 1;
  }

  void Seek(const Slice& target) override {
    size_t group = 0;
    std::string sort_key;
    bool is_prefix = false;
    status_ = MakeCedarParquetSeekKey(target, &sort_key, &is_prefix);
    if (status_.ok()) status_ = reader_->FindRowGroup(sort_key, &group);
    if (!status_.ok()) return;
    if (group >= reader_->footer_.row_groups.size()) {
      Invalidate();
      return;
    }
    const auto& sort_column = reader_->footer_.row_groups[group].columns.front();
    size_t page = static_cast<size_t>(std::lower_bound(
        sort_column.page_indexes.begin(), sort_column.page_indexes.end(), sort_key,
        [](const CedarParquetFooter::ColumnChunk::PageIndex& bounds,
           const std::string& target_key) { return bounds.max_value < target_key; }) -
                                      sort_column.page_indexes.begin());
    if (page >= sort_column.page_locations.size()) {
      PositionAt(group + 1, 0, 0);
      return;
    }
    if (!Load(group, page)) return;
    if (is_prefix) {
      const auto position = std::lower_bound(decoded_->sort_keys.begin(),
                                             decoded_->sort_keys.end(), sort_key);
      row_index_ = static_cast<size_t>(position - decoded_->sort_keys.begin());
    } else {
      const auto position = std::lower_bound(
          decoded_->internal_keys.begin(), decoded_->internal_keys.end(), target,
          [this](const std::string& key, const Slice& seek_key) {
            return reader_->internal_comparator_->Compare(key, seek_key) < 0;
          });
      row_index_ = static_cast<size_t>(position - decoded_->internal_keys.begin());
    }
    if (row_index_ == decoded_->internal_keys.size()) {
      Advance();
    }
  }

  void SeekForPrev(const Slice& target) override {
    Seek(target);
    if (!status_.ok()) return;
    if (!Valid()) {
      SeekToLast();
      return;
    }
    if (reader_->internal_comparator_->Compare(key(), target) > 0) Prev();
  }

  void Next() override {
    if (!Valid()) return;
    ++row_index_;
    if (row_index_ == decoded_->internal_keys.size()) Advance();
  }

  void Prev() override {
    if (!Valid()) return;
    if (row_index_ > 0) {
      --row_index_;
      return;
    }
    if (page_index_ > 0) {
      if (Load(row_group_index_, page_index_ - 1)) {
        row_index_ = decoded_->internal_keys.size() - 1;
      }
      return;
    }
    if (row_group_index_ == 0) {
      Invalidate();
      return;
    }
    const size_t previous_group = row_group_index_ - 1;
    const size_t previous_page =
        reader_->footer_.row_groups[previous_group].columns.front().page_locations.size() - 1;
    if (!Load(previous_group, previous_page)) return;
    row_index_ = decoded_->internal_keys.size() - 1;
  }

  Slice key() const override { return Slice(decoded_->internal_keys[row_index_]); }
  Slice value() const override { return Slice(decoded_->values[row_index_]); }
  Status status() const override { return status_; }
  bool IsKeyPinned() const override { return true; }
  bool IsValuePinned() const override { return true; }

 private:
  void Invalidate() {
    row_group_index_ = reader_->footer_.row_groups.size();
    page_index_ = 0;
    row_index_ = 0;
    decoded_.reset();
  }

  bool Load(size_t row_group_index, size_t page_index) {
    if (row_group_index >= reader_->footer_.row_groups.size() ||
        page_index >=
            reader_->footer_.row_groups[row_group_index].columns.front().page_locations.size()) {
      Invalidate();
      return false;
    }
    if (row_group_index_ != row_group_index || page_index_ != page_index) {
      status_ = reader_->LoadCanonicalPage(row_group_index, page_index, &decoded_);
      if (!status_.ok()) {
        Invalidate();
        return false;
      }
      row_group_index_ = row_group_index;
      page_index_ = page_index;
    }
    return true;
  }

  void PositionAt(size_t row_group_index, size_t page_index, size_t row_index) {
    status_ = Status::OK();
    if (!Load(row_group_index, page_index)) return;
    row_index_ = row_index;
    if (!Valid()) Advance();
  }

  void Advance() {
    if (row_group_index_ >= reader_->footer_.row_groups.size()) {
      Invalidate();
      return;
    }
    const size_t page_count =
        reader_->footer_.row_groups[row_group_index_].columns.front().page_locations.size();
    if (page_index_ + 1 < page_count) {
      PositionAt(row_group_index_, page_index_ + 1, 0);
    } else {
      PositionAt(row_group_index_ + 1, 0, 0);
    }
  }

  const CedarParquetTableReader* reader_;
  std::shared_ptr<const CedarParquetTableReader::DecodedCanonicalPage> decoded_;
  size_t row_group_index_ = std::numeric_limits<size_t>::max();
  size_t page_index_ = 0;
  size_t row_index_ = 0;
  Status status_;
};

CedarParquetTableReader::CedarParquetTableReader(
    const InternalKeyComparator& internal_comparator,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    CedarParquetTableOptions options, CedarParquetFooter footer,
    std::shared_ptr<TableProperties> table_properties, size_t footer_bytes,
    std::vector<std::optional<CedarSplitBlockBloomFilter>> blooms)
    : internal_comparator_(&internal_comparator),
      file_(std::move(file)),
      file_size_(file_size),
      options_(std::move(options)),
      footer_(std::move(footer)),
      table_properties_(std::move(table_properties)),
      footer_bytes_(footer_bytes),
      user_key_blooms_(std::move(blooms)) {
  for (const auto& bloom : user_key_blooms_) {
    if (bloom.has_value()) bloom_bytes_ += bloom->bitset_bytes();
  }
}

Status CedarParquetTableReader::Open(
    const InternalKeyComparator& internal_comparator,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    CedarParquetTableOptions options,
    std::unique_ptr<CedarParquetTableReader>* reader) {
  Status status = options.Validate();
  if (!status.ok()) return status;
  if (file == nullptr || file_size < 12) return Status::Corruption("truncated Cedar Parquet file");
  if (internal_comparator.user_comparator()->Name() != options.comparator_name) {
    return Status::InvalidArgument("Cedar Parquet comparator does not match table");
  }
  std::string header;
  status = ReadExact(file.get(), 0, 4, &header);
  if (!status.ok()) return status;
  if (header != kParquetMagic) return Status::Corruption("missing Cedar Parquet header");
  std::string trailer;
  status = ReadExact(file.get(), file_size - 8, 8, &trailer);
  if (!status.ok()) return status;
  if (trailer.substr(4, 4) != kParquetMagic) return Status::Corruption("missing Cedar Parquet footer");
  const uint32_t footer_size = DecodeFixed32(trailer.data());
  if (footer_size > options.max_footer_bytes) {
    return Status::Corruption("Cedar Parquet footer exceeds configured bound");
  }
  if (footer_size > file_size - 12) return Status::Corruption("invalid Cedar Parquet footer size");
  std::string footer_bytes;
  status = ReadExact(file.get(), file_size - 8 - footer_size, footer_size, &footer_bytes);
  if (!status.ok()) return status;
  CedarParquetFooter footer;
  status = DecodeCompactFooter(footer_bytes, &footer);
  if (!status.ok()) return status;
  status = ValidateCedarParquetFileIdentity(footer);
  if (!status.ok()) return status;
  // A facts table emitted by the table builder always has at least one row
  // group. Keep the empty-footer kernel fixture readable, but never let the
  // table reader dereference an absent row group while opening an SST.
  if (footer.row_groups.empty()) {
    return Status::Corruption("Cedar Parquet table has no row groups");
  }
  const uint64_t footer_offset = file_size - 8 - footer_size;
  std::vector<std::optional<CedarSplitBlockBloomFilter>> blooms(footer.row_groups.size());
  std::vector<CedarParquetFileRange> declared_ranges;
  std::string previous_row_group_max;
  for (auto& row_group : footer.row_groups) {
    const auto& sort_column = row_group.columns.front();
    if (row_group.file_offset != sort_column.data_page_offset) {
      return Status::Corruption(
          "Cedar Parquet row-group file offset disagrees with first data page");
    }
    if (sort_column.min_value.size() != kCedarParquetV2SortKeyBytes ||
        sort_column.max_value.size() != kCedarParquetV2SortKeyBytes ||
        sort_column.min_value > sort_column.max_value) {
      return Status::Corruption("invalid Cedar Parquet row-group bounds");
    }
    if (!previous_row_group_max.empty() &&
        previous_row_group_max >= sort_column.min_value) {
      return Status::Corruption("Cedar Parquet row groups are unordered");
    }
    previous_row_group_max = sort_column.max_value;
    std::vector<CedarParquetFooter::ColumnChunk::PageLocation> expected_pages;
    for (auto& column : row_group.columns) {
      if (column.offset_index_offset < 4 || column.offset_index_length <= 0 ||
          static_cast<uint64_t>(column.offset_index_length) > options.max_index_bytes ||
          static_cast<uint64_t>(column.offset_index_offset) >= footer_offset ||
          static_cast<uint64_t>(column.offset_index_length) >
              footer_offset - static_cast<uint64_t>(column.offset_index_offset)) {
        return Status::Corruption("invalid Cedar Parquet offset-index bounds");
      }
      std::string index_bytes;
      status = ReadExact(file.get(),
                         static_cast<uint64_t>(column.offset_index_offset),
                         static_cast<size_t>(column.offset_index_length),
                         &index_bytes);
      if (!status.ok()) return status;
      declared_ranges.push_back({static_cast<uint64_t>(column.offset_index_offset),
                                 static_cast<uint64_t>(column.offset_index_length)});
      status = DecodeOffsetIndex(index_bytes, &column.page_locations);
      if (!status.ok()) return status;
      if (column.page_locations.front().offset != column.data_page_offset ||
          column.page_locations.front().first_row_index != 0) {
        return Status::Corruption("Cedar Parquet offset index disagrees with column");
      }
      uint64_t total_page_bytes = 0;
      uint64_t previous_page_end = 0;
      for (size_t page_index = 0; page_index < column.page_locations.size();
           ++page_index) {
        const auto& page = column.page_locations[page_index];
        if (static_cast<uint64_t>(page.offset) >= footer_offset ||
            static_cast<uint64_t>(page.compressed_page_size) > options.page_max_bytes ||
            static_cast<uint64_t>(page.compressed_page_size) >
                footer_offset - static_cast<uint64_t>(page.offset)) {
          return Status::Corruption("Cedar Parquet page lies outside data region");
        }
        if (page_index != 0 &&
            static_cast<uint64_t>(page.offset) < previous_page_end) {
          return Status::Corruption("Cedar Parquet pages overlap");
        }
        if (page.first_row_index >= row_group.num_rows) {
          return Status::Corruption("Cedar Parquet page starts outside row group");
        }
        if (static_cast<uint64_t>(page.compressed_page_size) >
            std::numeric_limits<uint64_t>::max() - total_page_bytes) {
          return Status::Corruption("Cedar Parquet page-size overflow");
        }
        total_page_bytes += static_cast<uint64_t>(page.compressed_page_size);
        previous_page_end = static_cast<uint64_t>(page.offset) +
                            static_cast<uint64_t>(page.compressed_page_size);
        declared_ranges.push_back({static_cast<uint64_t>(page.offset),
                                   static_cast<uint64_t>(page.compressed_page_size)});
      }
      if (total_page_bytes != static_cast<uint64_t>(column.total_compressed_size)) {
        return Status::Corruption("Cedar Parquet offset index size mismatch");
      }
      if (column.column_index_offset < 4 || column.column_index_length <= 0 ||
          static_cast<uint64_t>(column.column_index_length) > options.max_index_bytes ||
          static_cast<uint64_t>(column.column_index_offset) >= footer_offset ||
          static_cast<uint64_t>(column.column_index_length) >
              footer_offset - static_cast<uint64_t>(column.column_index_offset)) {
        return Status::Corruption("invalid Cedar Parquet column-index bounds");
      }
      std::string column_index_bytes;
      status = ReadExact(file.get(),
                         static_cast<uint64_t>(column.column_index_offset),
                         static_cast<size_t>(column.column_index_length),
                         &column_index_bytes);
      if (!status.ok()) return status;
      declared_ranges.push_back({static_cast<uint64_t>(column.column_index_offset),
                                 static_cast<uint64_t>(column.column_index_length)});
      status = DecodeColumnIndex(column_index_bytes, &column.page_indexes);
      if (!status.ok()) return status;
      if (column.page_indexes.size() != column.page_locations.size()) {
        return Status::Corruption("Cedar Parquet column index page count mismatch");
      }
      if (column.path == "sort_key") {
        std::string previous_page_max;
        for (const auto& page : column.page_indexes) {
          if (page.all_null ||
              page.min_value.size() != kCedarParquetV2SortKeyBytes ||
              page.max_value.size() != kCedarParquetV2SortKeyBytes ||
              page.min_value > page.max_value ||
              (!previous_page_max.empty() && previous_page_max >= page.min_value)) {
            return Status::Corruption("Cedar sort-key index contains invalid page bounds");
          }
          previous_page_max = page.max_value;
        }
        if (column.page_indexes.front().min_value != column.min_value ||
            column.page_indexes.back().max_value != column.max_value) {
          return Status::Corruption("Cedar sort-key page index disagrees with column bounds");
        }
      }
      if (expected_pages.empty()) {
        expected_pages = column.page_locations;
      } else if (expected_pages.size() != column.page_locations.size()) {
        return Status::Corruption("Cedar Parquet columns use different page boundaries");
      } else {
        for (size_t index = 0; index < expected_pages.size(); ++index) {
          if (expected_pages[index].first_row_index !=
              column.page_locations[index].first_row_index) {
            return Status::Corruption("Cedar Parquet columns use different page rows");
          }
        }
      }
    }
    const auto& sort_column_for_bloom = row_group.columns.front();
    if (sort_column_for_bloom.bloom_filter_offset >= 0 ||
        sort_column_for_bloom.bloom_filter_length > 0) {
      if (sort_column_for_bloom.bloom_filter_offset < 4 ||
          sort_column_for_bloom.bloom_filter_length <= 0 ||
          static_cast<uint64_t>(sort_column_for_bloom.bloom_filter_length) >
              options.max_index_bytes ||
          static_cast<uint64_t>(sort_column_for_bloom.bloom_filter_offset) >= footer_offset ||
          static_cast<uint64_t>(sort_column_for_bloom.bloom_filter_length) >
              footer_offset - static_cast<uint64_t>(sort_column_for_bloom.bloom_filter_offset)) {
        return Status::Corruption("invalid Cedar Parquet Bloom bounds");
      }
      std::string bloom_bytes;
      status = ReadExact(file.get(),
                         static_cast<uint64_t>(sort_column_for_bloom.bloom_filter_offset),
                         static_cast<size_t>(sort_column_for_bloom.bloom_filter_length),
                         &bloom_bytes);
      if (!status.ok()) return status;
      declared_ranges.push_back(
          {static_cast<uint64_t>(sort_column_for_bloom.bloom_filter_offset),
           static_cast<uint64_t>(sort_column_for_bloom.bloom_filter_length)});
      CedarSplitBlockBloomFilter bloom;
      status = CedarSplitBlockBloomFilter::Decode(bloom_bytes, &bloom);
      if (!status.ok()) return status;
      blooms[&row_group - footer.row_groups.data()] = std::move(bloom);
    }
  }
  std::sort(declared_ranges.begin(), declared_ranges.end(),
            [](const CedarParquetFileRange& left, const CedarParquetFileRange& right) {
              return left.offset < right.offset;
            });
  for (size_t index = 1; index < declared_ranges.size(); ++index) {
    const CedarParquetFileRange& previous = declared_ranges[index - 1];
    const CedarParquetFileRange& current = declared_ranges[index];
    if (previous.offset + previous.size > current.offset) {
      return Status::Corruption("Cedar Parquet data and metadata ranges overlap");
    }
  }

  auto properties = std::make_shared<TableProperties>();
  properties->num_entries = static_cast<uint64_t>(footer.num_rows);
  properties->num_data_blocks = static_cast<uint64_t>(footer.row_groups.size());
  properties->column_family_id = kCedarFactsColumnFamilyId;
  properties->column_family_name = "facts";
  properties->comparator_name = options.comparator_name;
  properties->compression_name = "UNCOMPRESSED";
  properties->format_version = 1;
  properties->user_collected_properties["cedar.parquet.format"] =
      kCedarParquetFormatVersion;
  properties->user_collected_properties["cedar.parquet.schema"] =
      kCedarParquetCanonicalSchemaDigest;
  for (const auto& row_group : footer.row_groups) {
    for (const auto& column : row_group.columns) {
      properties->data_size += static_cast<uint64_t>(column.total_compressed_size);
      properties->uncompressed_data_size +=
          static_cast<uint64_t>(column.total_uncompressed_size);
    }
  }
  reader->reset(new CedarParquetTableReader(internal_comparator, std::move(file),
                                             file_size, std::move(options),
                                             std::move(footer), std::move(properties),
                                             footer_bytes.size(), std::move(blooms)));
  return Status::OK();
}

Status CedarParquetTableReader::DecodeRowGroup(size_t row_group_index,
                                                DecodedRowGroup* decoded) const {
  if (row_group_index >= footer_.row_groups.size()) {
    return Status::InvalidArgument("Cedar Parquet row group is out of range");
  }
  const auto& row_group = footer_.row_groups[row_group_index];
  if (static_cast<uint64_t>(row_group.total_byte_size) > options_.row_group_max_bytes) {
    return Status::Corruption("Cedar Parquet row group exceeds configured bound");
  }
  std::array<std::vector<std::string>*, 3> columns = {
      &decoded->sort_keys, &decoded->internal_keys, &decoded->values};
  for (size_t index = 0; index < columns.size(); ++index) {
    const auto& column = row_group.columns[index];
    columns[index]->clear();
    columns[index]->reserve(static_cast<size_t>(row_group.num_rows));
    for (size_t page_index = 0; page_index < column.page_locations.size();
         ++page_index) {
      std::vector<std::string> page_values;
      Status status = DecodeColumnPage(row_group_index, index, page_index,
                                       &page_values);
      if (!status.ok()) return status;
      const auto& location = column.page_locations[page_index];
      if (location.first_row_index !=
          static_cast<int64_t>(columns[index]->size())) {
        return Status::Corruption("Cedar Parquet page row index is not contiguous");
      }
      columns[index]->insert(columns[index]->end(), page_values.begin(),
                             page_values.end());
    }
    if (columns[index]->size() != static_cast<size_t>(row_group.num_rows)) {
      return Status::Corruption("invalid Cedar Parquet row-group column length");
    }
  }
  Status row_status = ValidateCanonicalRowBytes(decoded->sort_keys, decoded->internal_keys,
                                                decoded->values, options_.max_row_bytes);
  if (!row_status.ok()) return row_status;
  const auto& sort_column = row_group.columns.front();
  if (sort_column.page_indexes.size() != sort_column.page_locations.size()) {
    return Status::Corruption("missing Cedar Parquet sort-key column index");
  }
  for (size_t page_index = 0; page_index < sort_column.page_indexes.size(); ++page_index) {
    const auto& page_bounds = sort_column.page_indexes[page_index];
    const size_t first = static_cast<size_t>(sort_column.page_locations[page_index].first_row_index);
    const size_t last = page_index + 1 < sort_column.page_locations.size()
                            ? static_cast<size_t>(sort_column.page_locations[page_index + 1].first_row_index)
                            : decoded->sort_keys.size();
    if (first >= last || page_bounds.min_value != decoded->sort_keys[first] ||
        page_bounds.max_value != decoded->sort_keys[last - 1]) {
      return Status::Corruption("Cedar Parquet column-index bounds disagree with data");
    }
  }
  for (size_t index = 0; index < decoded->internal_keys.size(); ++index) {
    std::string decoded_key;
    row_status = DecodeCedarParquetSortKey(decoded->sort_keys[index], &decoded_key);
    if (!row_status.ok() || decoded_key != decoded->internal_keys[index]) {
      return Status::Corruption("Cedar Parquet canonical key mismatch");
    }
    if (index > 0 && internal_comparator_->Compare(decoded->internal_keys[index - 1],
                                                    decoded->internal_keys[index]) >= 0) {
      return Status::Corruption("Cedar Parquet row group is unordered");
    }
  }
  if (decoded->sort_keys.front() != row_group.columns[0].min_value ||
      decoded->sort_keys.back() != row_group.columns[0].max_value) {
    return Status::Corruption("Cedar Parquet row-group bounds disagree with data");
  }
  for (size_t column_index = 3; column_index < row_group.columns.size(); ++column_index) {
    std::vector<std::optional<std::string>> materialized;
    const auto& column = row_group.columns[column_index];
    if (column.page_indexes.size() != column.page_locations.size()) {
      return Status::Corruption("missing Cedar Parquet materialized column index");
    }
    materialized.reserve(static_cast<size_t>(row_group.num_rows));
    for (size_t page_index = 0; page_index < column.page_locations.size(); ++page_index) {
      std::vector<std::optional<std::string>> page;
      Status status = DecodePrimitiveColumnPage(row_group_index, column_index,
                                                page_index, &page);
      if (!status.ok()) return status;
      bool all_null = true;
      std::string minimum;
      std::string maximum;
      for (const auto& value : page) {
        if (!value.has_value()) continue;
        std::string index_value = *value;
        if (column_index == 7 || column_index == 8) {
          if (!EncodeCedarParquetNumericIndexValueFromLittleEndian(
                  *value, &index_value)) {
            return Status::Corruption(
                "Cedar Parquet numeric materialized value has invalid width");
          }
        }
        if (all_null || index_value < minimum) minimum = index_value;
        if (all_null || index_value > maximum) maximum = index_value;
        all_null = false;
      }
      const auto& page_bounds = column.page_indexes[page_index];
      if (page_bounds.all_null != all_null ||
          (!all_null && (page_bounds.min_value != minimum ||
                         page_bounds.max_value != maximum))) {
        return Status::Corruption(
            "materialized Cedar Parquet column-index bounds disagree with data");
      }
      if (column.page_locations[page_index].first_row_index !=
          static_cast<int64_t>(materialized.size())) {
        return Status::Corruption("materialized Parquet page row index is not contiguous");
      }
      materialized.insert(materialized.end(), std::make_move_iterator(page.begin()),
                          std::make_move_iterator(page.end()));
    }
    if (materialized.size() != static_cast<size_t>(row_group.num_rows)) {
      return Status::Corruption("materialized Cedar Parquet column length mismatch");
    }
    for (size_t row_index = 0; row_index < materialized.size(); ++row_index) {
      CedarParquetMaterializedFact expected_fact;
      Status status = DecodeCedarParquetMaterializedFact(
          decoded->internal_keys[row_index], decoded->values[row_index], &expected_fact);
      if (!status.ok() || materialized[row_index] !=
                              ExpectedMaterializedValue(expected_fact, column_index)) {
        return Status::Corruption("materialized Cedar Parquet column disagrees with canonical fact");
      }
    }
  }
  return Status::OK();
}

Status CedarParquetTableReader::DecodePrimitiveColumnPage(
    size_t row_group_index, size_t column_index, size_t page_index,
    std::vector<std::optional<std::string>>* decoded) const {
  if (row_group_index >= footer_.row_groups.size()) {
    return Status::InvalidArgument("Cedar Parquet row group is out of range");
  }
  const auto& row_group = footer_.row_groups[row_group_index];
  if (column_index >= row_group.columns.size() ||
      page_index >= row_group.columns[column_index].page_locations.size()) {
    return Status::InvalidArgument("Cedar Parquet page is out of range");
  }
  const auto& location = row_group.columns[column_index].page_locations[page_index];
  std::string page;
  Status status = ReadExact(file_.get(), static_cast<uint64_t>(location.offset),
                            static_cast<size_t>(location.compressed_page_size), &page);
  if (!status.ok()) return status;
  const CedarParquetSchemaElement& schema = footer_.schema[column_index + 1];
  size_t consumed = 0;
  status = DecodePlainPrimitiveDataPage(
      page, schema.physical_type, schema.repetition_type == kParquetOptional,
      decoded, &consumed, schema.type_length,
      row_group.columns[column_index].compression_codec,
      static_cast<size_t>(options_.page_max_bytes),
      static_cast<size_t>(options_.max_row_bytes),
      static_cast<size_t>(options_.page_max_rows));
  if (!status.ok()) return status;
  const auto& locations = row_group.columns[column_index].page_locations;
  const int64_t first_row = locations[page_index].first_row_index;
  const int64_t next_row = page_index + 1 < locations.size()
                               ? locations[page_index + 1].first_row_index
                               : row_group.num_rows;
  if (next_row <= first_row ||
      static_cast<uint64_t>(next_row - first_row) != decoded->size()) {
    return Status::Corruption("Cedar Parquet page value count disagrees with offset index");
  }
  return Status::OK();
}

Status CedarParquetTableReader::DecodeColumnPage(
    size_t row_group_index, size_t column_index, size_t page_index,
    std::vector<std::string>* decoded) const {
  std::vector<std::optional<std::string>> primitive_values;
  Status status = DecodePrimitiveColumnPage(row_group_index, column_index, page_index,
                                            &primitive_values);
  if (!status.ok()) return status;
  decoded->clear();
  decoded->reserve(primitive_values.size());
  for (auto& value : primitive_values) {
    if (!value.has_value()) {
      return Status::Corruption("canonical Cedar Parquet column contains null");
    }
    decoded->push_back(std::move(*value));
  }
  return Status::OK();
}

Status CedarParquetTableReader::DecodeCanonicalPage(
    size_t row_group_index, size_t page_index,
    DecodedCanonicalPage* decoded) const {
  Status status = DecodeColumnPage(row_group_index, 0, page_index,
                                   &decoded->sort_keys);
  if (!status.ok()) return status;
  status = DecodeColumnPage(row_group_index, 1, page_index,
                            &decoded->internal_keys);
  if (!status.ok()) return status;
  status = DecodeColumnPage(row_group_index, 2, page_index, &decoded->values);
  if (!status.ok()) return status;
  if (decoded->sort_keys.size() != decoded->internal_keys.size() ||
      decoded->internal_keys.size() != decoded->values.size()) {
    return Status::Corruption("Cedar Parquet canonical page column length mismatch");
  }
  status = ValidateCanonicalRowBytes(decoded->sort_keys, decoded->internal_keys,
                                     decoded->values, options_.max_row_bytes);
  if (!status.ok()) return status;
  for (size_t index = 0; index < decoded->internal_keys.size(); ++index) {
    std::string decoded_key;
    status = DecodeCedarParquetSortKey(decoded->sort_keys[index], &decoded_key);
    if (!status.ok()) return status;
    if (decoded_key != decoded->internal_keys[index]) {
      return Status::Corruption("Cedar Parquet canonical page key mismatch");
    }
  }
  ++page_decode_count_;
  return Status::OK();
}

Status CedarParquetTableReader::LoadCanonicalPage(
    size_t row_group_index, size_t page_index,
    std::shared_ptr<const DecodedCanonicalPage>* decoded) const {
  if (decoded == nullptr) {
    return Status::InvalidArgument("missing Cedar Parquet page destination");
  }
  {
    std::lock_guard<std::mutex> lock(page_cache_mutex_);
    if (page_cache_.has_value() &&
        page_cache_->row_group_index == row_group_index &&
        page_cache_->page_index == page_index) {
      *decoded = page_cache_->decoded;
      return Status::OK();
    }
  }

  auto loaded = std::make_shared<DecodedCanonicalPage>();
  Status status = DecodeCanonicalPage(row_group_index, page_index, loaded.get());
  if (!status.ok()) return status;
  size_t bytes = 0;
  const auto add_column_bytes = [&bytes](const std::vector<std::string>& column) {
    for (const std::string& value : column) bytes += value.size();
  };
  add_column_bytes(loaded->sort_keys);
  add_column_bytes(loaded->internal_keys);
  add_column_bytes(loaded->values);
  if (bytes <= options_.page_max_bytes) {
    std::lock_guard<std::mutex> lock(page_cache_mutex_);
    page_cache_ = CachedCanonicalPage{row_group_index, page_index, bytes, loaded};
  }
  *decoded = std::move(loaded);
  return Status::OK();
}

Status CedarParquetTableReader::ScanProjected(
    const CedarParquetScanSpec& spec,
    const CedarParquetColumnarBatchVisitor& visitor) const {
  if (!visitor) return Status::InvalidArgument("Cedar Parquet scan", "missing visitor");
  if (spec.batch_row_limit == 0) {
    return Status::InvalidArgument("Cedar Parquet scan", "zero batch row limit");
  }
  if (spec.sort_key_lower.has_value() && spec.sort_key_upper.has_value() &&
      *spec.sort_key_lower > *spec.sort_key_upper) {
    return Status::InvalidArgument("Cedar Parquet scan", "invalid sort-key range");
  }
  if (spec.valid_from_min.has_value() && spec.valid_from_max.has_value() &&
      *spec.valid_from_min > *spec.valid_from_max) {
    return Status::InvalidArgument("Cedar Parquet scan", "invalid valid-time range");
  }
  if (spec.cedar_commit_seq_min.has_value() && spec.cedar_commit_seq_max.has_value() &&
      *spec.cedar_commit_seq_min > *spec.cedar_commit_seq_max) {
    return Status::InvalidArgument("Cedar Parquet scan", "invalid commit-sequence range");
  }
  if (spec.valid_from_min.has_value() || spec.valid_from_max.has_value() ||
      spec.cedar_commit_seq_min.has_value() || spec.cedar_commit_seq_max.has_value()) {
    for (const auto& row_group : footer_.row_groups) {
      for (size_t column_index : {size_t{7}, size_t{8}}) {
        if (column_index >= row_group.columns.size()) {
          return Status::Corruption("Cedar Parquet numeric column is missing");
        }
        for (const auto& page : row_group.columns[column_index].page_indexes) {
          Status numeric_status = ValidateNumericPageIndex(page);
          if (!numeric_status.ok()) return numeric_status;
        }
      }
    }
  }
  if ((spec.sort_key_lower.has_value() &&
       spec.sort_key_lower->size() != kCedarParquetV2SortKeyBytes) ||
      (spec.sort_key_upper.has_value() &&
       spec.sort_key_upper->size() != kCedarParquetV2SortKeyBytes)) {
    return Status::InvalidArgument("Cedar Parquet scan",
                                   "sort-key bounds must be normalized Cedar keys");
  }
  bool request_encoded_values = false;
  std::vector<CedarParquetColumnVector> columns;
  columns.reserve(spec.projection.size());
  for (CedarParquetColumnId id : spec.projection) {
    if (id == CedarParquetColumnId::kEncodedValue) {
      request_encoded_values = true;
      continue;
    }
    if (!IsMaterializedColumn(id)) {
      return Status::InvalidArgument("Cedar Parquet scan", "unsupported projection column");
    }
    for (const auto& existing : columns) {
      if (existing.id == id) {
        return Status::InvalidArgument("Cedar Parquet scan", "duplicate projection column");
      }
    }
    CedarParquetColumnVector column;
    Status status = MakeColumnVector(id, &column);
    if (!status.ok()) return status;
    columns.push_back(std::move(column));
  }

  CedarParquetColumnarBatch batch;
  batch.columns = columns;
  const auto flush = [&]() -> Status {
    if (batch.internal_keys.empty()) return Status::OK();
    if (spec.stats != nullptr) {
      spec.stats->rows_emitted += batch.row_count();
    }
    Status status = visitor(batch);
    if (!status.ok()) return status;
    batch = CedarParquetColumnarBatch();
    batch.columns = columns;
    return Status::OK();
  };

  for (size_t row_group_index = 0; row_group_index < footer_.row_groups.size();
       ++row_group_index) {
    const auto& row_group = footer_.row_groups[row_group_index];
    const auto& sort_column = row_group.columns.front();
    const auto& valid_column = row_group.columns[7];
    const auto& commit_column = row_group.columns[8];
    if ((spec.valid_from_min.has_value() || spec.valid_from_max.has_value()) &&
        (valid_column.page_indexes.empty() ||
         std::none_of(valid_column.page_indexes.begin(), valid_column.page_indexes.end(),
                      [&](const auto& page) {
                        return NumericPageIntersects(page, spec.valid_from_min,
                                                     spec.valid_from_max);
                      }))) {
      if (spec.stats != nullptr) {
        ++spec.stats->row_groups_skipped;
        spec.stats->pages_skipped += sort_column.page_locations.size();
      }
      continue;
    }
    if ((spec.cedar_commit_seq_min.has_value() || spec.cedar_commit_seq_max.has_value()) &&
        (commit_column.page_indexes.empty() ||
         std::none_of(commit_column.page_indexes.begin(), commit_column.page_indexes.end(),
                      [&](const auto& page) {
                        return NumericPageIntersects(page, spec.cedar_commit_seq_min,
                                                     spec.cedar_commit_seq_max);
                      }))) {
      if (spec.stats != nullptr) {
        ++spec.stats->row_groups_skipped;
        spec.stats->pages_skipped += sort_column.page_locations.size();
      }
      continue;
    }
    if (spec.sort_key_lower.has_value() && sort_column.max_value < *spec.sort_key_lower) {
      if (spec.stats != nullptr) ++spec.stats->row_groups_skipped;
      continue;
    }
    if (spec.sort_key_upper.has_value() && sort_column.min_value > *spec.sort_key_upper) {
      if (spec.stats != nullptr) {
        spec.stats->row_groups_skipped += footer_.row_groups.size() - row_group_index;
      }
      break;
    }
    for (size_t page_index = 0; page_index < sort_column.page_locations.size();
         ++page_index) {
      const auto& page_bounds = sort_column.page_indexes[page_index];
      if ((spec.valid_from_min.has_value() || spec.valid_from_max.has_value()) &&
          !NumericPageIntersects(valid_column.page_indexes[page_index],
                                 spec.valid_from_min, spec.valid_from_max)) {
        if (spec.stats != nullptr) ++spec.stats->pages_skipped;
        continue;
      }
      if ((spec.cedar_commit_seq_min.has_value() || spec.cedar_commit_seq_max.has_value()) &&
          !NumericPageIntersects(commit_column.page_indexes[page_index],
                                 spec.cedar_commit_seq_min,
                                 spec.cedar_commit_seq_max)) {
        if (spec.stats != nullptr) ++spec.stats->pages_skipped;
        continue;
      }
      if (spec.sort_key_lower.has_value() && page_bounds.max_value < *spec.sort_key_lower) {
        if (spec.stats != nullptr) ++spec.stats->pages_skipped;
        continue;
      }
      if (spec.sort_key_upper.has_value() && page_bounds.min_value > *spec.sort_key_upper) {
        if (spec.stats != nullptr) {
          spec.stats->pages_skipped += sort_column.page_locations.size() - page_index;
        }
        return flush();
      }

      if (spec.stats != nullptr) {
        ++spec.stats->pages_read;
        for (const auto& column : row_group.columns) {
          if (page_index < column.page_locations.size()) {
            spec.stats->bytes_read += static_cast<uint64_t>(
                column.page_locations[page_index].compressed_page_size);
          }
        }
      }

      std::vector<std::string> sort_keys;
      std::vector<std::string> internal_keys;
      Status status = DecodeColumnPage(row_group_index, 0, page_index, &sort_keys);
      if (!status.ok()) return status;
      status = DecodeColumnPage(row_group_index, 1, page_index, &internal_keys);
      if (!status.ok()) return status;
      if (sort_keys.size() != internal_keys.size()) {
        return Status::Corruption("Cedar Parquet scan canonical page length mismatch");
      }

      std::vector<std::vector<std::optional<std::string>>> projected_pages;
      projected_pages.reserve(columns.size());
      for (const auto& column : columns) {
        std::vector<std::optional<std::string>> values;
        status = DecodePrimitiveColumnPage(
            row_group_index, static_cast<size_t>(column.id), page_index, &values);
        if (!status.ok()) return status;
        if (values.size() != internal_keys.size()) {
          return Status::Corruption("Cedar Parquet scan projected page length mismatch");
        }
        projected_pages.push_back(std::move(values));
      }
      std::vector<std::optional<std::string>> valid_times;
      std::vector<std::optional<std::string>> commit_sequences;
      if (spec.valid_from_min.has_value() || spec.valid_from_max.has_value()) {
        status = DecodePrimitiveColumnPage(row_group_index, 7, page_index, &valid_times);
        if (!status.ok()) return status;
      }
      if (spec.cedar_commit_seq_min.has_value() || spec.cedar_commit_seq_max.has_value()) {
        status = DecodePrimitiveColumnPage(row_group_index, 8, page_index,
                                           &commit_sequences);
        if (!status.ok()) return status;
      }
      std::vector<std::string> encoded_values;
      if (request_encoded_values) {
        status = DecodeColumnPage(row_group_index, 2, page_index, &encoded_values);
        if (!status.ok()) return status;
        if (encoded_values.size() != internal_keys.size()) {
          return Status::Corruption(
              "Cedar Parquet scan encoded-value page length mismatch");
        }
      }

      for (size_t row = 0; row < internal_keys.size(); ++row) {
        if (spec.sort_key_lower.has_value() && sort_keys[row] < *spec.sort_key_lower) {
          continue;
        }
        if (spec.sort_key_upper.has_value() && sort_keys[row] > *spec.sort_key_upper) {
          return flush();
        }
        if (!valid_times.empty() &&
            (!valid_times[row].has_value() ||
             (spec.valid_from_min.has_value() &&
              DecodeLittleEndian(*valid_times[row]) < *spec.valid_from_min) ||
             (spec.valid_from_max.has_value() &&
              DecodeLittleEndian(*valid_times[row]) > *spec.valid_from_max))) {
          continue;
        }
        if (!commit_sequences.empty() &&
            (!commit_sequences[row].has_value() ||
             (spec.cedar_commit_seq_min.has_value() &&
              DecodeLittleEndian(*commit_sequences[row]) < *spec.cedar_commit_seq_min) ||
             (spec.cedar_commit_seq_max.has_value() &&
              DecodeLittleEndian(*commit_sequences[row]) > *spec.cedar_commit_seq_max))) {
          continue;
        }
        batch.internal_keys.push_back(internal_keys[row]);
        if (request_encoded_values) batch.encoded_values.push_back(encoded_values[row]);
        for (size_t column_index = 0; column_index < columns.size(); ++column_index) {
          status = AppendProjectedValue(&batch.columns[column_index],
                                        projected_pages[column_index][row]);
          if (!status.ok()) return status;
        }
        if (batch.row_count() == spec.batch_row_limit) {
          status = flush();
          if (!status.ok()) return status;
        }
      }
    }
  }
  return flush();
}

Status CedarParquetTableReader::NewProjectedCursor(
    const CedarParquetScanSpec& spec,
    std::unique_ptr<CedarParquetProjectedCursor>* cursor) const {
  if (cursor == nullptr) {
    return Status::InvalidArgument("Cedar Parquet projected cursor", "missing destination");
  }
  cursor->reset();
  if (spec.batch_row_limit == 0) {
    return Status::InvalidArgument("Cedar Parquet projected cursor", "zero batch row limit");
  }
  if ((spec.sort_key_lower.has_value() &&
       spec.sort_key_lower->size() != kCedarParquetV2SortKeyBytes) ||
      (spec.sort_key_upper.has_value() &&
       spec.sort_key_upper->size() != kCedarParquetV2SortKeyBytes)) {
    return Status::InvalidArgument(
        "Cedar Parquet projected cursor",
        "sort-key bounds must be normalized Cedar keys");
  }
  if (spec.sort_key_lower.has_value() && spec.sort_key_upper.has_value() &&
      *spec.sort_key_lower > *spec.sort_key_upper) {
    return Status::InvalidArgument("Cedar Parquet projected cursor",
                                   "invalid sort-key range");
  }
  if (spec.valid_from_min.has_value() && spec.valid_from_max.has_value() &&
      *spec.valid_from_min > *spec.valid_from_max) {
    return Status::InvalidArgument("Cedar Parquet projected cursor",
                                   "invalid valid-time range");
  }
  if (spec.cedar_commit_seq_min.has_value() && spec.cedar_commit_seq_max.has_value() &&
      *spec.cedar_commit_seq_min > *spec.cedar_commit_seq_max) {
    return Status::InvalidArgument("Cedar Parquet projected cursor",
                                   "invalid commit-sequence range");
  }
  if (spec.valid_from_min.has_value() || spec.valid_from_max.has_value() ||
      spec.cedar_commit_seq_min.has_value() || spec.cedar_commit_seq_max.has_value()) {
    for (const auto& row_group : footer_.row_groups) {
      for (size_t column_index : {size_t{7}, size_t{8}}) {
        if (column_index >= row_group.columns.size()) {
          return Status::Corruption("Cedar Parquet numeric column is missing");
        }
        for (const auto& page : row_group.columns[column_index].page_indexes) {
          Status numeric_status = ValidateNumericPageIndex(page);
          if (!numeric_status.ok()) return numeric_status;
        }
      }
    }
  }
  bool request_encoded_values = false;
  std::vector<CedarParquetColumnVector> columns;
  columns.reserve(spec.projection.size());
  for (CedarParquetColumnId id : spec.projection) {
    if (id == CedarParquetColumnId::kEncodedValue) {
      request_encoded_values = true;
      continue;
    }
    if (!IsMaterializedColumn(id)) {
      return Status::InvalidArgument(
          "Cedar Parquet projected cursor", "unsupported projection column");
    }
    for (const auto& existing : columns) {
      if (existing.id == id) {
        return Status::InvalidArgument(
            "Cedar Parquet projected cursor", "duplicate projection column");
      }
    }
    CedarParquetColumnVector column;
    Status status = MakeColumnVector(id, &column);
    if (!status.ok()) return status;
    columns.push_back(std::move(column));
  }
  cursor->reset(new CedarParquetProjectedCursor(
      this, spec, request_encoded_values, std::move(columns)));
  if (!(*cursor)->status().ok()) {
    Status status = (*cursor)->status();
    cursor->reset();
    return status;
  }
  return Status::OK();
}

Status CedarParquetTableReader::FindRowGroup(const std::string& sort_key,
                                              size_t* row_group_index) const {
  auto position = std::lower_bound(
      footer_.row_groups.begin(), footer_.row_groups.end(), sort_key,
      [](const CedarParquetFooter::RowGroup& row_group, const std::string& target) {
        return row_group.columns.front().max_value < target;
      });
  *row_group_index = static_cast<size_t>(position - footer_.row_groups.begin());
  return Status::OK();
}

InternalIterator* CedarParquetTableReader::NewIterator(
    const ReadOptions&, const SliceTransform*, Arena* arena, bool,
    TableReaderCaller, size_t, bool) {
  if (arena == nullptr) return new CedarParquetTableIterator(this);
  void* memory = arena->AllocateAligned(sizeof(CedarParquetTableIterator));
  return new (memory) CedarParquetTableIterator(this);
}

Status CedarParquetTableReader::Get(const ReadOptions& read_options, const Slice& key,
                                    GetContext* get_context,
                                    const SliceTransform*, bool) {
  static_cast<void>(read_options);
  ParsedInternalKey target;
  Status status = ParseInternalKey(key, &target, false);
  if (!status.ok()) return status;
  std::string sort_key;
  status = EncodeCedarParquetSortKey(key, &sort_key);
  if (!status.ok()) return status;
  size_t row_group_index = 0;
  status = FindRowGroup(sort_key, &row_group_index);
  if (!status.ok()) return status;
  if (row_group_index < user_key_blooms_.size() &&
      user_key_blooms_[row_group_index].has_value() &&
      !user_key_blooms_[row_group_index]->MayContain(
          std::string_view(target.user_key.data(), target.user_key.size()))) {
    return Status::OK();
  }
  for (; row_group_index < footer_.row_groups.size(); ++row_group_index) {
    const auto& row_group = footer_.row_groups[row_group_index];
    const auto& sort_column = row_group.columns.front();
    auto page = std::lower_bound(
        sort_column.page_indexes.begin(), sort_column.page_indexes.end(), sort_key,
        [](const CedarParquetFooter::ColumnChunk::PageIndex& bounds,
           const std::string& target_key) { return bounds.max_value < target_key; });
    for (size_t page_index = static_cast<size_t>(page - sort_column.page_indexes.begin());
         page_index < sort_column.page_locations.size(); ++page_index) {
      std::shared_ptr<const DecodedCanonicalPage> decoded_page;
      status = LoadCanonicalPage(row_group_index, page_index, &decoded_page);
      if (!status.ok()) return status;
      const auto& sort_keys = decoded_page->sort_keys;
      if (!sort_keys.empty() && sort_keys.back() < sort_key) continue;
      const auto& internal_keys = decoded_page->internal_keys;
      const auto& values = decoded_page->values;
      if (sort_keys.size() != internal_keys.size() ||
          internal_keys.size() != values.size()) {
        return Status::Corruption("Cedar Parquet page column length mismatch");
      }
      for (size_t row_index = 0; row_index < internal_keys.size(); ++row_index) {
        std::string decoded_key;
        status = DecodeCedarParquetSortKey(sort_keys[row_index], &decoded_key);
        if (!status.ok() || decoded_key != internal_keys[row_index]) {
          return Status::Corruption("Cedar Parquet canonical key mismatch");
        }
        if (internal_comparator_->Compare(internal_keys[row_index], key) < 0) continue;
        ParsedInternalKey parsed;
        status = ParseInternalKey(internal_keys[row_index], &parsed, false);
        if (!status.ok()) return status;
        if (parsed.user_key != target.user_key) return Status::OK();
        bool matched = false;
        const bool keep_reading = get_context->SaveValue(
            parsed, Slice(values[row_index]), &matched, &status);
        if (!status.ok() || !keep_reading) return status;
      }
      if (page_index + 1 < sort_column.page_indexes.size() &&
          (sort_column.page_indexes[page_index + 1].min_value.size() <
               target.user_key.size() ||
           sort_column.page_indexes[page_index + 1].min_value.compare(
               0, target.user_key.size(), target.user_key.data(),
               target.user_key.size()) != 0)) {
        return Status::OK();
      }
    }
    if (row_group_index + 1 < footer_.row_groups.size() &&
        (footer_.row_groups[row_group_index + 1].columns.front().min_value.size() <
             target.user_key.size() ||
         footer_.row_groups[row_group_index + 1].columns.front().min_value.compare(
             0, target.user_key.size(), target.user_key.data(),
             target.user_key.size()) != 0)) {
      return Status::OK();
    }
  }
  return Status::OK();
}

void CedarParquetTableReader::MultiGet(
    const ReadOptions& read_options, const MultiGetContext::Range* mget_range,
    const SliceTransform* prefix_extractor, bool skip_filters) {
  static_cast<void>(read_options);
  static_cast<void>(prefix_extractor);
  static_cast<void>(skip_filters);
  if (mget_range == nullptr) return;

  struct CachedPage {
    size_t row_group_index = 0;
    size_t page_index = 0;
    std::shared_ptr<const DecodedCanonicalPage> decoded;
  };
  std::vector<CachedPage> cache;

  auto load_page = [&](size_t row_group_index, size_t page_index,
                       const DecodedCanonicalPage** decoded) -> Status {
    for (const auto& cached : cache) {
      if (cached.row_group_index == row_group_index &&
          cached.page_index == page_index) {
        *decoded = cached.decoded.get();
        return Status::OK();
      }
    }
    cache.push_back(CachedPage());
    CachedPage& cached = cache.back();
    cached.row_group_index = row_group_index;
    cached.page_index = page_index;
    Status status = LoadCanonicalPage(row_group_index, page_index,
                                      &cached.decoded);
    if (!status.ok()) {
      cache.pop_back();
      return status;
    }
    *decoded = cached.decoded.get();
    return Status::OK();
  };

  for (auto iter = mget_range->begin(); iter != mget_range->end(); ++iter) {
    *iter->s = Status::OK();
    ParsedInternalKey target;
    Status status = ParseInternalKey(iter->ikey, &target, false);
    if (!status.ok()) {
      *iter->s = status;
      continue;
    }
    std::string sort_key;
    status = EncodeCedarParquetSortKey(iter->ikey, &sort_key);
    if (!status.ok()) {
      *iter->s = status;
      continue;
    }
    size_t row_group_index = 0;
    status = FindRowGroup(sort_key, &row_group_index);
    if (!status.ok()) {
      *iter->s = status;
      continue;
    }

    const size_t initial_row_group_index = row_group_index;
    if (row_group_index < user_key_blooms_.size() &&
        user_key_blooms_[row_group_index].has_value() &&
        !user_key_blooms_[row_group_index]->MayContain(
            std::string_view(target.user_key.data(), target.user_key.size()))) {
      continue;
    }
    bool request_done = false;
    for (; row_group_index < footer_.row_groups.size() && !request_done;
         ++row_group_index) {
      const auto& row_group = footer_.row_groups[row_group_index];
      if (row_group.columns.empty()) continue;
      const auto& sort_column = row_group.columns.front();
      size_t first_page = 0;
      if (row_group_index == initial_row_group_index) {
        first_page = static_cast<size_t>(std::lower_bound(
            sort_column.page_indexes.begin(), sort_column.page_indexes.end(),
            sort_key,
            [](const CedarParquetFooter::ColumnChunk::PageIndex& bounds,
               const std::string& target_key) {
              return bounds.max_value < target_key;
            }) - sort_column.page_indexes.begin());
      }
      if (first_page >= sort_column.page_locations.size()) continue;

      for (size_t page_index = first_page;
           page_index < sort_column.page_locations.size() && !request_done;
           ++page_index) {
        const DecodedCanonicalPage* page = nullptr;
        status = load_page(row_group_index, page_index, &page);
        if (!status.ok()) {
          *iter->s = status;
          request_done = true;
          break;
        }
        for (size_t row_index = 0; row_index < page->internal_keys.size();
             ++row_index) {
          const std::string& internal_key = page->internal_keys[row_index];
          if (internal_comparator_->Compare(internal_key, iter->ikey) < 0) {
            continue;
          }
          ParsedInternalKey parsed;
          status = ParseInternalKey(internal_key, &parsed, false);
          if (!status.ok()) {
            *iter->s = status;
            request_done = true;
            break;
          }
          if (!internal_comparator_->user_comparator()->Equal(
                  parsed.user_key, target.user_key)) {
            request_done = true;
            break;
          }
          bool matched = false;
          const bool keep_reading = iter->get_context->SaveValue(
              parsed, Slice(page->values[row_index]), &matched, &status);
          if (!status.ok()) {
            *iter->s = status;
            request_done = true;
            break;
          }
          if (!keep_reading) {
            request_done = true;
            break;
          }
        }
      }
    }
    if (!iter->s->ok()) continue;
  }
}

uint64_t CedarParquetTableReader::ApproximateOffsetOf(const ReadOptions&, const Slice& key,
                                                       TableReaderCaller) {
  std::string sort_key;
  bool is_prefix = false;
  if (!MakeCedarParquetSeekKey(key, &sort_key, &is_prefix).ok()) return file_size_;
  size_t row_group = 0;
  if (!FindRowGroup(sort_key, &row_group).ok() || row_group >= footer_.row_groups.size()) {
    return file_size_;
  }
  return static_cast<uint64_t>(footer_.row_groups[row_group].file_offset);
}

uint64_t CedarParquetTableReader::ApproximateSize(const ReadOptions& read_options,
                                                   const Slice& start, const Slice& end,
                                                   TableReaderCaller caller) {
  const uint64_t start_offset = start.empty() ? 0 : ApproximateOffsetOf(read_options, start, caller);
  const uint64_t end_offset = end.empty() ? file_size_ : ApproximateOffsetOf(read_options, end, caller);
  return end_offset > start_offset ? end_offset - start_offset : 0;
}

Status CedarParquetTableReader::VerifyChecksum(const ReadOptions&, TableReaderCaller,
                                                bool meta_blocks_only) {
  if (meta_blocks_only) return Status::OK();
  for (size_t index = 0; index < footer_.row_groups.size(); ++index) {
    DecodedRowGroup decoded;
    Status status = DecodeRowGroup(index, &decoded);
    if (!status.ok()) return status;
  }
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE::cedar_parquet
