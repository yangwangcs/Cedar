// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "table/cedar_parquet/cedar_parquet_table_builder.h"

#include <array>
#include <cstring>
#include <limits>
#include <optional>
#include <vector>

#include "db/dbformat.h"
#include "file/writable_file_writer.h"
#include "rocksdb/table_properties.h"
#include "table/cedar_parquet/parquet_plain_page.h"
#include "table/cedar_parquet/parquet_bloom_filter.h"

namespace ROCKSDB_NAMESPACE::cedar_parquet {
namespace {

constexpr char kParquetMagic[] = "PAR1";

std::vector<CedarParquetKeyValueMetadata> RequiredFooterMetadata() {
  return {
      {"cedar.format", std::string(kCedarParquetFormatVersion)},
      {"cedar.canonical_schema", std::string(kCedarParquetCanonicalSchemaDigest)},
      {"cedar.materialized_schema", std::string(kCedarParquetCanonicalSchemaDigest)},
      {"cedar.comparator", std::string(kCedarParquetBytewiseComparatorName)},
      {"cedar.comparator_digest", std::string(kCedarParquetComparatorDigest)},
      {"cedar.fact_key_format", "part32.fact.v2"},
      {"cedar.feature_bits", std::string(kCedarParquetFeatureBits)},
  };
}

Status AddChecked(uint64_t left, uint64_t right, uint64_t* total) {
  if (right > std::numeric_limits<uint64_t>::max() - left) {
    return Status::MemoryLimit("Cedar Parquet table size overflow");
  }
  *total = left + right;
  return Status::OK();
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

std::vector<std::optional<std::string>> MaterializedColumnValues(
    const std::vector<CedarParquetRow>& rows, size_t column_index) {
  std::vector<std::optional<std::string>> values;
  values.reserve(rows.size());
  for (const CedarParquetRow& row : rows) {
    const CedarParquetMaterializedFact& fact = row.materialized;
    switch (column_index) {
      case 0: values.emplace_back(row.sort_key); break;
      case 1: values.emplace_back(row.internal_key); break;
      case 2: values.emplace_back(row.encoded_value); break;
      case 3: values.emplace_back(LittleEndian(fact.part_id, 4)); break;
      case 4: values.emplace_back(LittleEndian(fact.fact_family, 4)); break;
      case 5: values.emplace_back(LittleEndian(fact.property_id, 4)); break;
      case 6: values.emplace_back(LittleEndian(fact.entity_id, 8)); break;
      case 7: values.emplace_back(LittleEndian(fact.valid_from, 8)); break;
      case 8: values.emplace_back(LittleEndian(fact.cedar_commit_seq, 8)); break;
      case 9: values.emplace_back(LittleEndian(fact.rocksdb_sequence, 8)); break;
      case 10:
        if (fact.rocksdb_deletion) values.emplace_back(std::nullopt);
        else values.emplace_back(LittleEndian(fact.operation, 4));
        break;
      case 11:
        if (fact.rocksdb_deletion) values.emplace_back(std::nullopt);
        else values.emplace_back(LittleEndian(fact.schema_epoch, 4));
        break;
      case 12:
        if (fact.rocksdb_deletion) values.emplace_back(std::nullopt);
        else values.emplace_back(LittleEndian(fact.physical_type, 4));
        break;
      case 13:
        if (fact.bool_value.has_value()) {
          values.emplace_back(std::string(1, *fact.bool_value ? '\1' : '\0'));
        } else values.emplace_back(std::nullopt);
        break;
      case 14:
        if (fact.int32_value.has_value()) values.emplace_back(LittleEndian(
            static_cast<uint32_t>(*fact.int32_value), 4));
        else values.emplace_back(std::nullopt);
        break;
      case 15:
        if (fact.int64_value.has_value()) values.emplace_back(LittleEndian(
            static_cast<uint64_t>(*fact.int64_value), 8));
        else values.emplace_back(std::nullopt);
        break;
      case 16:
        if (fact.float32_value.has_value()) values.emplace_back(Float32Bytes(*fact.float32_value));
        else values.emplace_back(std::nullopt);
        break;
      case 17:
        if (fact.float64_value.has_value()) values.emplace_back(Float64Bytes(*fact.float64_value));
        else values.emplace_back(std::nullopt);
        break;
      case 18:
        if (fact.timestamp64_value.has_value()) values.emplace_back(
            LittleEndian(*fact.timestamp64_value, 8));
        else values.emplace_back(std::nullopt);
        break;
      case 19:
        if (fact.bytes_value.has_value()) values.emplace_back(*fact.bytes_value);
        else values.emplace_back(std::nullopt);
        break;
      case 20:
        if (fact.source_part_id.has_value()) values.emplace_back(LittleEndian(*fact.source_part_id, 4));
        else values.emplace_back(std::nullopt);
        break;
      case 21:
        if (fact.source_vertex_id.has_value()) values.emplace_back(LittleEndian(*fact.source_vertex_id, 8));
        else values.emplace_back(std::nullopt);
        break;
      case 22:
        if (fact.target_part_id.has_value()) values.emplace_back(LittleEndian(*fact.target_part_id, 4));
        else values.emplace_back(std::nullopt);
        break;
      case 23:
        if (fact.target_vertex_id.has_value()) values.emplace_back(LittleEndian(*fact.target_vertex_id, 8));
        else values.emplace_back(std::nullopt);
        break;
      case 24:
        if (fact.edge_type.has_value()) values.emplace_back(LittleEndian(*fact.edge_type, 8));
        else values.emplace_back(std::nullopt);
        break;
      default:
        values.emplace_back(std::nullopt);
        break;
    }
  }
  return values;
}

uint64_t RowCharge(const CedarParquetRow& row) {
  return 12ULL + row.sort_key.size() + row.internal_key.size() +
         row.encoded_value.size();
}

}  // namespace

CedarParquetTableBuilder::CedarParquetTableBuilder(
    const TableBuilderOptions& table_builder_options,
    CedarParquetTableOptions parquet_options, WritableFileWriter* file)
    : internal_comparator_(&table_builder_options.internal_comparator),
      parquet_options_(std::move(parquet_options)),
      file_(file),
      row_group_builder_(parquet_options_),
      footer_(MakeRequiredFactsFooter(0, RequiredFooterMetadata())) {
  footer_.created_by = kCedarParquetCreatedBy;
  SetStatus(parquet_options_.Validate());
  if (status_.ok() && file_ == nullptr) {
    SetStatus(Status::InvalidArgument("Cedar Parquet table builder needs a file"));
  }
  if (status_.ok() && internal_comparator_->user_comparator()->Name() !=
                          parquet_options_.comparator_name) {
    SetStatus(Status::InvalidArgument("Cedar Parquet comparator does not match table"));
  }
  if (status_.ok()) {
    Write(Slice(kParquetMagic, sizeof(kParquetMagic) - 1));
  }

  properties_.column_family_id = table_builder_options.column_family_id;
  properties_.column_family_name = table_builder_options.column_family_name;
  properties_.comparator_name = parquet_options_.comparator_name;
  properties_.compression_name = "UNCOMPRESSED";
  properties_.format_version = 1;
  properties_.orig_file_number = table_builder_options.cur_file_num;
  properties_.db_id = table_builder_options.db_id;
  properties_.db_session_id = table_builder_options.db_session_id;
  properties_.oldest_key_time = table_builder_options.oldest_key_time;
  properties_.newest_key_time = table_builder_options.newest_key_time;
  properties_.file_creation_time = table_builder_options.file_creation_time;
  properties_.user_collected_properties["cedar.parquet.format"] =
      kCedarParquetFormatVersion;
  properties_.user_collected_properties["cedar.parquet.schema"] =
      kCedarParquetCanonicalSchemaDigest;
}

CedarParquetTableBuilder::~CedarParquetTableBuilder() = default;

void CedarParquetTableBuilder::SetStatus(Status status) {
  if (status_.ok() && !status.ok()) {
    status_ = std::move(status);
  }
}

void CedarParquetTableBuilder::Write(const Slice& bytes) {
  if (!status_.ok()) return;
  io_status_ = file_->Append(IOOptions(), bytes);
  if (!io_status_.ok()) {
    SetStatus(Status(io_status_));
  }
}

void CedarParquetTableBuilder::Add(const Slice& key, const Slice& value) {
  if (!status_.ok() || closed_) return;
  ParsedInternalKey parsed;
  Status status = ParseInternalKey(key, &parsed, false);
  if (!status.ok()) {
    SetStatus(std::move(status));
    return;
  }
  if (parsed.type == kTypeRangeDeletion) {
    SetStatus(Status::NotSupported("Cedar Parquet facts do not support range deletions"));
    return;
  }
  if (!last_internal_key_.empty() &&
      internal_comparator_->Compare(last_internal_key_, key) >= 0) {
    SetStatus(Status::InvalidArgument("Cedar Parquet input keys must be strictly ordered"));
    return;
  }

  CedarParquetRow row;
  status = EncodeCedarParquetRow(key, value, &row);
  if (!status.ok()) {
    SetStatus(std::move(status));
    return;
  }
  status = row_group_builder_.Add(std::move(row));
  if (status.IsIncomplete()) {
    FlushRowGroup();
    if (!status_.ok()) return;
    CedarParquetRow retry_row;
    status = EncodeCedarParquetRow(key, value, &retry_row);
    if (status.ok()) status = row_group_builder_.Add(std::move(retry_row));
  }
  if (!status.ok()) {
    SetStatus(std::move(status));
    return;
  }

  CedarParquetMaterializedFact materialized;
  status = DecodeCedarParquetMaterializedFact(key, value, &materialized);
  if (!status.ok()) {
    SetStatus(std::move(status));
    return;
  }
  status = row_group_builder_.SetLastMaterialized(std::move(materialized));
  if (!status.ok()) {
    SetStatus(std::move(status));
    return;
  }

  uint64_t total = 0;
  status = AddChecked(pre_compression_size_, key.size(), &total);
  if (status.ok()) status = AddChecked(total, value.size(), &pre_compression_size_);
  if (!status.ok()) {
    SetStatus(std::move(status));
    return;
  }
  properties_.num_entries++;
  properties_.raw_key_size += key.size();
  properties_.raw_value_size += value.size();
  if (parsed.type == kTypeDeletion || parsed.type == kTypeSingleDeletion) {
    properties_.num_deletions++;
  }
  properties_.key_largest_seqno =
      std::max(properties_.key_largest_seqno == UINT64_MAX ? 0 : properties_.key_largest_seqno,
               parsed.sequence);
  properties_.key_smallest_seqno = std::min(properties_.key_smallest_seqno, parsed.sequence);
  last_internal_key_.assign(key.data(), key.size());
}

void CedarParquetTableBuilder::FlushRowGroup() {
  if (!status_.ok() || row_group_builder_.empty()) return;
  const std::vector<CedarParquetRow>& rows = row_group_builder_.rows();
  struct Span {
    size_t begin = 0;
    size_t end = 0;
  };
  std::vector<Span> spans;
  size_t page_begin = 0;
  uint64_t page_bytes = 0;
  for (size_t index = 0; index < rows.size(); ++index) {
    const uint64_t charge = RowCharge(rows[index]);
    if (index != page_begin &&
        (index - page_begin >= parquet_options_.page_max_rows ||
         charge > parquet_options_.page_max_bytes -
                      std::min(page_bytes, parquet_options_.page_max_bytes))) {
      spans.push_back(Span{page_begin, index});
      page_begin = index;
      page_bytes = 0;
    }
    if (charge > std::numeric_limits<uint64_t>::max() - page_bytes) {
      SetStatus(Status::MemoryLimit("Cedar Parquet page size overflow"));
      return;
    }
    page_bytes += charge;
  }
  if (page_begin < rows.size()) spans.push_back(Span{page_begin, rows.size()});

  CedarParquetFooter::RowGroup row_group;
  row_group.num_rows = static_cast<int64_t>(rows.size());
  row_group.file_offset = static_cast<int64_t>(file_->GetFileSize());
  uint64_t total_compressed_page_size = 0;
  uint64_t total_uncompressed_page_size = 0;
  for (size_t index = 0; index + 1 < footer_.schema.size(); ++index) {
    const CedarParquetSchemaElement& schema = footer_.schema[index + 1];
    CedarParquetFooter::ColumnChunk column;
    column.path = schema.name;
    column.physical_type = schema.physical_type;
    column.type_length = schema.type_length;
    column.compression_codec = parquet_options_.page_compression;
    column.data_page_offset = static_cast<int64_t>(file_->GetFileSize());
    column.num_values = row_group.num_rows;
    if (index == 0) {
      column.min_value = rows.front().sort_key;
      column.max_value = rows.back().sort_key;
    }
    const std::vector<std::optional<std::string>> values =
        MaterializedColumnValues(rows, index);
    uint64_t column_compressed_size = 0;
    uint64_t column_uncompressed_size = 0;
    for (const Span& span : spans) {
      std::string page;
      CedarParquetDataPageSize page_size;
      std::vector<std::optional<std::string>> page_values(
          values.begin() + static_cast<std::ptrdiff_t>(span.begin),
          values.begin() + static_cast<std::ptrdiff_t>(span.end));
      SetStatus(EncodePlainPrimitiveDataPage(
          page_values, schema.physical_type,
          schema.repetition_type == kParquetOptional, &page, schema.type_length,
          parquet_options_.page_compression, &page_size));
      if (!status_.ok()) return;
      if (page.size() != page_size.compressed_size ||
          page.size() > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        SetStatus(Status::MemoryLimit("Cedar Parquet page exceeds i32 size"));
        return;
      }
      column.page_locations.push_back(
          {static_cast<int64_t>(file_->GetFileSize()),
           static_cast<int32_t>(page.size()), static_cast<int64_t>(span.begin)});
      bool all_null = true;
      std::string min_value;
      std::string max_value;
      for (size_t row_index = span.begin; row_index < span.end; ++row_index) {
        if (!values[row_index].has_value()) continue;
        std::string index_value = *values[row_index];
        if (index == 7 || index == 8) {
          if (!EncodeCedarParquetNumericIndexValueFromLittleEndian(
                  *values[row_index], &index_value)) {
            SetStatus(Status::Corruption(
                "Cedar Parquet numeric materialized value has invalid width"));
            return;
          }
        }
        if (all_null || index_value < min_value) {
          min_value = index_value;
        }
        if (all_null || index_value > max_value) {
          max_value = index_value;
        }
        all_null = false;
      }
      column.page_indexes.push_back(
          {std::move(min_value), std::move(max_value), all_null});
      Status status = AddChecked(column_compressed_size, page_size.compressed_size,
                                 &column_compressed_size);
      if (status.ok()) {
        status = AddChecked(column_uncompressed_size, page_size.uncompressed_size,
                            &column_uncompressed_size);
      }
      if (!status.ok()) {
        SetStatus(std::move(status));
        return;
      }
      Write(page);
      if (!status_.ok()) return;
    }
    if (index == 0) {
      std::vector<const std::string*> user_keys;
      user_keys.reserve(rows.size());
      std::vector<std::string> owned_user_keys;
      owned_user_keys.reserve(rows.size());
      for (const CedarParquetRow& row : rows) {
        ParsedInternalKey parsed;
        Status status = ParseInternalKey(row.internal_key, &parsed, false);
        if (!status.ok()) {
          SetStatus(std::move(status));
          return;
        }
        owned_user_keys.emplace_back(parsed.user_key.data(), parsed.user_key.size());
      }
      for (const std::string& user_key : owned_user_keys) user_keys.push_back(&user_key);
      std::string bloom;
      Status status = CedarSplitBlockBloomFilter::Build(
          user_keys.data(), user_keys.size(), &bloom);
      if (!status.ok()) {
        SetStatus(std::move(status));
        return;
      }
      if (bloom.size() > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        SetStatus(Status::MemoryLimit("Cedar Parquet Bloom exceeds i32 size"));
        return;
      }
      column.bloom_filter_offset = static_cast<int64_t>(file_->GetFileSize());
      column.bloom_filter_length = static_cast<int32_t>(bloom.size());
      Write(bloom);
      if (!status_.ok()) return;
    }
    column.total_compressed_size = static_cast<int64_t>(column_compressed_size);
    column.total_uncompressed_size = static_cast<int64_t>(column_uncompressed_size);
    Status status = AddChecked(total_compressed_page_size, column_compressed_size,
                               &total_compressed_page_size);
    if (status.ok()) {
      status = AddChecked(total_uncompressed_page_size, column_uncompressed_size,
                          &total_uncompressed_page_size);
    }
    if (!status.ok()) {
      SetStatus(std::move(status));
      return;
    }
    row_group.columns.push_back(std::move(column));
  }
  row_group.total_byte_size = static_cast<int64_t>(total_uncompressed_page_size);
  SetStatus(AddCedarParquetRowGroup(&footer_, std::move(row_group)));
  if (!status_.ok()) return;
  properties_.num_data_blocks++;
  properties_.data_size += total_compressed_page_size;
  properties_.uncompressed_data_size += total_uncompressed_page_size;
  row_group_builder_.Reset();
}

void CedarParquetTableBuilder::WriteOffsetIndexes() {
  for (auto& row_group : footer_.row_groups) {
    for (auto& column : row_group.columns) {
      std::string encoded;
      SetStatus(EncodeOffsetIndex(column.page_locations, &encoded));
      if (!status_.ok()) return;
      if (encoded.size() > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        SetStatus(Status::MemoryLimit("Cedar Parquet offset index exceeds i32 size"));
        return;
      }
      column.offset_index_offset = static_cast<int64_t>(file_->GetFileSize());
      column.offset_index_length = static_cast<int32_t>(encoded.size());
      Write(encoded);
      if (!status_.ok()) return;
    }
  }
}

void CedarParquetTableBuilder::WriteColumnIndexes() {
  for (auto& row_group : footer_.row_groups) {
    for (auto& column : row_group.columns) {
      std::string encoded;
      SetStatus(EncodeColumnIndex(column.page_indexes, &encoded));
      if (!status_.ok()) return;
      if (encoded.size() > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        SetStatus(Status::MemoryLimit("Cedar Parquet column index exceeds i32 size"));
        return;
      }
      column.column_index_offset = static_cast<int64_t>(file_->GetFileSize());
      column.column_index_length = static_cast<int32_t>(encoded.size());
      Write(encoded);
      if (!status_.ok()) return;
    }
  }
}

Status CedarParquetTableBuilder::Finish() {
  if (closed_) return status_;
  FlushRowGroup();
  if (status_.ok()) {
    WriteOffsetIndexes();
    if (status_.ok()) WriteColumnIndexes();
    std::string trailer;
    SetStatus(AppendParquetFooter(&trailer, footer_));
    if (status_.ok()) Write(trailer);
    if (status_.ok()) {
      io_status_ = file_->Flush(IOOptions());
      if (!io_status_.ok()) SetStatus(Status(io_status_));
    }
  }
  closed_ = true;
  return status_;
}

void CedarParquetTableBuilder::Abandon() {
  row_group_builder_.Reset();
  closed_ = true;
}

uint64_t CedarParquetTableBuilder::FileSize() const {
  return file_ == nullptr ? 0 : file_->GetFileSize();
}

std::string CedarParquetTableBuilder::GetFileChecksum() const {
  return file_ == nullptr ? kUnknownFileChecksum : file_->GetFileChecksum();
}

const char* CedarParquetTableBuilder::GetFileChecksumFuncName() const {
  return file_ == nullptr ? kUnknownFileChecksumFuncName
                          : file_->GetFileChecksumFuncName();
}

}  // namespace ROCKSDB_NAMESPACE::cedar_parquet
