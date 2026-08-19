// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "table/cedar_parquet/cedar_parquet_format.h"

#include <cstring>
#include <limits>

#include "db/dbformat.h"
#include "util/crc32c.h"

namespace ROCKSDB_NAMESPACE::cedar_parquet {
namespace {

constexpr size_t kInternalKeyFooterBytes = 8;
constexpr uint64_t kEncodedByteArrayLengthBytes = 4;

Status AddChecked(uint64_t left, uint64_t right, uint64_t* total) {
  if (right > std::numeric_limits<uint64_t>::max() - left) {
    return Status::MemoryLimit("Cedar Parquet row accounting overflow");
  }
  *total = left + right;
  return Status::OK();
}

void AppendBigEndian64(std::string* destination, uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    destination->push_back(static_cast<char>((value >> shift) & 0xffU));
  }
}

uint64_t DecodeBigEndian64(const char* source) {
  uint64_t value = 0;
  for (uint32_t index = 0; index < 8; ++index) {
    value = (value << 8U) | static_cast<unsigned char>(source[index]);
  }
  return value;
}

uint32_t DecodeBigEndian32(const char* source) {
  uint32_t value = 0;
  for (uint32_t index = 0; index < 4; ++index) {
    value = (value << 8U) | static_cast<unsigned char>(source[index]);
  }
  return value;
}

uint16_t DecodeBigEndian16(const char* source) {
  return static_cast<uint16_t>(
      (static_cast<uint16_t>(static_cast<unsigned char>(source[0])) << 8U) |
      static_cast<uint16_t>(static_cast<unsigned char>(source[1])));
}

bool IsStateFactFamily(uint8_t family) {
  return family == 1 || family == 3 || family == 4;
}

bool IsKnownFactFamily(uint8_t family) {
  return IsStateFactFamily(family) || family == 2 || family == 5;
}

void AppendLittleEndian64(std::string* destination, uint64_t value) {
  for (uint32_t shift = 0; shift < 64; shift += 8) {
    destination->push_back(static_cast<char>((value >> shift) & 0xffU));
  }
}

}  // namespace

Status CedarParquetTableOptions::Validate() const {
  if (row_group_max_rows == 0 || row_group_max_bytes == 0 ||
      page_max_rows == 0 || page_max_bytes == 0 || max_row_bytes == 0 ||
      max_footer_bytes == 0 || max_index_bytes == 0) {
    return Status::InvalidArgument("Cedar Parquet resource limits must be non-zero");
  }
  if (row_group_max_bytes < kCedarParquetMinimumRowGroupBytes) {
    return Status::InvalidArgument("Cedar Parquet row-group budget cannot hold metadata");
  }
  if (max_row_bytes > row_group_max_bytes) {
    return Status::InvalidArgument("Cedar Parquet row limit exceeds group budget");
  }
  if (comparator_name != kCedarParquetBytewiseComparatorName) {
    return Status::InvalidArgument("Cedar Parquet v2 requires BytewiseComparator");
  }
  if (page_compression != CedarParquetCompressionCodec::kUncompressed &&
      page_compression != CedarParquetCompressionCodec::kLz4Raw &&
      page_compression != CedarParquetCompressionCodec::kZstd) {
    return Status::InvalidArgument("unsupported Cedar Parquet page compression");
  }
  return Status::OK();
}

Status EncodeCedarParquetSortKey(const Slice& internal_key,
                                 std::string* sort_key) {
  ParsedInternalKey parsed;
  Status status = ParseInternalKey(internal_key, &parsed, false);
  if (!status.ok()) return status;
  if (parsed.user_key.size() != kCedarParquetV2UserKeyBytes ||
      static_cast<uint8_t>(parsed.user_key[0]) != 2) {
    return Status::Corruption("Cedar Parquet v2 requires a fixed PartID-aware fact key");
  }
  sort_key->assign(parsed.user_key.data(), parsed.user_key.size());
  sort_key->reserve(kCedarParquetV2SortKeyBytes);
  const uint64_t packed = PackSequenceAndType(parsed.sequence, parsed.type);
  AppendBigEndian64(sort_key, ~packed);
  return Status::OK();
}

Status DecodeCedarParquetSortKey(const Slice& sort_key,
                                 std::string* internal_key) {
  if (sort_key.size() != kCedarParquetV2SortKeyBytes ||
      static_cast<uint8_t>(sort_key[0]) != 2) {
    return Status::Corruption("invalid Cedar Parquet v2 sort key");
  }
  const uint64_t packed = ~DecodeBigEndian64(
      sort_key.data() + kCedarParquetV2UserKeyBytes);
  const SequenceNumber sequence = packed >> 8U;
  const ValueType type = static_cast<ValueType>(packed & 0xffU);
  if (sequence > kMaxSequenceNumber || !IsExtendedValueType(type)) {
    return Status::Corruption("invalid Cedar Parquet internal-key footer");
  }
  internal_key->assign(sort_key.data(), kCedarParquetV2UserKeyBytes);
  AppendLittleEndian64(internal_key, packed);
  ParsedInternalKey parsed;
  return ParseInternalKey(*internal_key, &parsed, false);
}

Status EncodeCedarParquetRow(const Slice& internal_key, const Slice& encoded_value,
                             CedarParquetRow* row) {
  Status status = EncodeCedarParquetSortKey(internal_key, &row->sort_key);
  if (!status.ok()) return status;
  row->internal_key.assign(internal_key.data(), internal_key.size());
  row->encoded_value.assign(encoded_value.data(), encoded_value.size());
  return Status::OK();
}

Status DecodeCedarParquetRow(const CedarParquetRow& row, std::string* internal_key,
                             std::string* encoded_value) {
  std::string decoded_key;
  Status status = DecodeCedarParquetSortKey(row.sort_key, &decoded_key);
  if (!status.ok()) return status;
  if (decoded_key != row.internal_key) {
    return Status::Corruption("Cedar Parquet canonical key and sort key disagree");
  }
  *internal_key = std::move(decoded_key);
  *encoded_value = row.encoded_value;
  return Status::OK();
}

Status DecodeCedarParquetMaterializedFact(
    const Slice& internal_key, const Slice& encoded_value,
    CedarParquetMaterializedFact* fact) {
  if (fact == nullptr) {
    return Status::InvalidArgument("Cedar Parquet materializer needs an output");
  }
  ParsedInternalKey parsed;
  Status status = ParseInternalKey(internal_key, &parsed, false);
  if (!status.ok()) return status;
  if (parsed.user_key.size() != kCedarParquetV2UserKeyBytes ||
      static_cast<uint8_t>(parsed.user_key[0]) != 2) {
    return Status::Corruption("Cedar Parquet materializer requires a v2 fact key");
  }
  const char* user_key = parsed.user_key.data();
  const uint8_t family = static_cast<uint8_t>(user_key[5]);
  const uint16_t property_id = DecodeBigEndian16(user_key + 6);
  const uint64_t entity_id = DecodeBigEndian64(user_key + 8);
  const uint64_t valid_from = ~DecodeBigEndian64(user_key + 16);
  const uint64_t cedar_commit_seq = ~DecodeBigEndian64(user_key + 24);
  if (!IsKnownFactFamily(family) || entity_id == 0 || cedar_commit_seq == 0 ||
      (IsStateFactFamily(family) && property_id != 0) ||
      (!IsStateFactFamily(family) && property_id == 0)) {
    return Status::Corruption("invalid Cedar Parquet materialized fact identity");
  }

  *fact = {};
  fact->part_id = DecodeBigEndian32(user_key + 1);
  fact->fact_family = family;
  fact->property_id = property_id;
  fact->entity_id = entity_id;
  fact->valid_from = valid_from;
  fact->cedar_commit_seq = cedar_commit_seq;
  fact->rocksdb_sequence = parsed.sequence;

  if (parsed.type == kTypeDeletion) {
    if (!encoded_value.empty()) {
      return Status::Corruption("RocksDB deletion has a Cedar value payload");
    }
    fact->rocksdb_deletion = true;
    return Status::OK();
  }
  if (parsed.type != kTypeValue) {
    return Status::NotSupported("Cedar Parquet facts support only value or deletion entries");
  }
  constexpr size_t kFactValueHeaderBytes = 11;
  constexpr size_t kFactValueChecksumBytes = 4;
  if (encoded_value.size() < kFactValueHeaderBytes + kFactValueChecksumBytes) {
    return Status::Corruption("truncated Cedar fact value");
  }
  const char* value = encoded_value.data();
  if (static_cast<uint8_t>(value[0]) != 1) {
    return Status::Corruption("unsupported Cedar fact value version");
  }
  const uint8_t operation = static_cast<uint8_t>(value[1]);
  const uint32_t schema_epoch = DecodeBigEndian32(value + 2);
  const uint8_t physical_type = static_cast<uint8_t>(value[6]);
  const uint32_t payload_length = DecodeBigEndian32(value + 7);
  constexpr uint8_t kEdgeIdentityValueKind = 9;
  if ((operation != 1 && operation != 2) || physical_type > kEdgeIdentityValueKind ||
      payload_length > encoded_value.size() - kFactValueHeaderBytes -
                           kFactValueChecksumBytes ||
      encoded_value.size() != kFactValueHeaderBytes + payload_length +
                                  kFactValueChecksumBytes) {
    return Status::Corruption("invalid Cedar fact value header");
  }
  const uint32_t stored_checksum = DecodeBigEndian32(
      value + encoded_value.size() - kFactValueChecksumBytes);
  if (stored_checksum !=
      crc32c::Value(value, encoded_value.size() - kFactValueChecksumBytes)) {
    return Status::Corruption("Cedar fact value CRC32C mismatch");
  }
  const bool property = !IsStateFactFamily(family);
  const bool edge_identity = family == 3;
  if ((edge_identity &&
       (valid_from != 0 || operation != 1 || schema_epoch != 0 ||
        physical_type != kEdgeIdentityValueKind || payload_length != 44)) ||
      (!edge_identity && physical_type == kEdgeIdentityValueKind) ||
      (!property && !edge_identity && (schema_epoch != 0 || physical_type != 0)) ||
      (property && schema_epoch == 0) ||
      (operation == 1 && property && physical_type == 0) ||
      (operation == 2 && physical_type != 0)) {
    return Status::Corruption("Cedar fact value disagrees with fact family");
  }

  fact->operation = operation;
  fact->schema_epoch = schema_epoch;
  fact->physical_type = physical_type;
  const char* payload = value + kFactValueHeaderBytes;
  if (edge_identity) {
    const uint32_t home_part = DecodeBigEndian32(payload);
    const uint64_t edge_id = DecodeBigEndian64(payload + 4);
    fact->source_part_id = DecodeBigEndian32(payload + 12);
    fact->source_vertex_id = DecodeBigEndian64(payload + 16);
    fact->target_part_id = DecodeBigEndian32(payload + 24);
    fact->target_vertex_id = DecodeBigEndian64(payload + 28);
    fact->edge_type = DecodeBigEndian64(payload + 36);
    if (home_part != fact->part_id || edge_id != fact->entity_id ||
        !fact->source_vertex_id.has_value() || *fact->source_vertex_id == 0 ||
        !fact->target_vertex_id.has_value() || *fact->target_vertex_id == 0 ||
        !fact->edge_type.has_value() || *fact->edge_type == 0 ||
        !fact->source_part_id.has_value() || *fact->source_part_id != home_part) {
      return Status::Corruption("Cedar edge identity payload disagrees with fact key");
    }
    fact->physical_type = 0;
    return Status::OK();
  }
  switch (physical_type) {
    case 0:
      if (payload_length != 0) {
        return Status::Corruption("Cedar fact has payload without a physical type");
      }
      break;
    case 1:
      if (payload_length != 1 || (payload[0] != 0 && payload[0] != 1)) {
        return Status::Corruption("invalid Cedar bool payload");
      }
      fact->bool_value = payload[0] == 1;
      break;
    case 2:
      if (payload_length != 4) return Status::Corruption("invalid Cedar int32 payload");
      fact->int32_value = static_cast<int32_t>(DecodeBigEndian32(payload));
      break;
    case 3:
      if (payload_length != 8) return Status::Corruption("invalid Cedar int64 payload");
      fact->int64_value = static_cast<int64_t>(DecodeBigEndian64(payload));
      break;
    case 4: {
      if (payload_length != 4) return Status::Corruption("invalid Cedar float32 payload");
      const uint32_t bits = DecodeBigEndian32(payload);
      float number = 0;
      std::memcpy(&number, &bits, sizeof(number));
      fact->float32_value = number;
      break;
    }
    case 5: {
      if (payload_length != 8) return Status::Corruption("invalid Cedar float64 payload");
      const uint64_t bits = DecodeBigEndian64(payload);
      double number = 0;
      std::memcpy(&number, &bits, sizeof(number));
      fact->float64_value = number;
      break;
    }
    case 6:
      if (payload_length != 8) {
        return Status::Corruption("invalid Cedar timestamp64 payload");
      }
      fact->timestamp64_value = DecodeBigEndian64(payload);
      break;
    case 7:
    case 8:
      fact->bytes_value = std::string(payload, payload_length);
      break;
    default:
      return Status::Corruption("unknown Cedar physical type");
  }
  return Status::OK();
}

CedarParquetRowGroupBuilder::CedarParquetRowGroupBuilder(
    CedarParquetTableOptions options)
    : options_(std::move(options)) {}

Status CedarParquetRowGroupBuilder::ChargeRow(const CedarParquetRow& row,
                                               uint64_t* charge) const {
  uint64_t total = 0;
  for (const std::string* column : {&row.sort_key, &row.internal_key,
                                    &row.encoded_value}) {
    Status status = AddChecked(total, column->size(), &total);
    if (!status.ok()) return status;
    status = AddChecked(total, kEncodedByteArrayLengthBytes, &total);
    if (!status.ok()) return status;
  }
  *charge = total;
  return Status::OK();
}

Status CedarParquetRowGroupBuilder::Add(CedarParquetRow row) {
  Status status = options_.Validate();
  if (!status.ok()) return status;
  std::string decoded_key;
  std::string decoded_value;
  status = DecodeCedarParquetRow(row, &decoded_key, &decoded_value);
  if (!status.ok()) return status;
  uint64_t charge = 0;
  status = ChargeRow(row, &charge);
  if (!status.ok()) return status;
  if (charge > options_.max_row_bytes || charge > options_.row_group_max_bytes) {
    return Status::MemoryLimit("Cedar Parquet row exceeds builder budget");
  }
  if (!rows_.empty() && rows_.back().sort_key >= row.sort_key) {
    return Status::InvalidArgument("Cedar Parquet input keys must be strictly ordered");
  }
  uint64_t next_size = 0;
  status = AddChecked(resident_bytes_, charge, &next_size);
  if (!status.ok()) return status;
  if (rows_.size() >= options_.row_group_max_rows ||
      next_size > options_.row_group_max_bytes) {
    return Status::Incomplete("Cedar Parquet row group is full");
  }
  rows_.push_back(std::move(row));
  resident_bytes_ = next_size;
  return Status::OK();
}

Status CedarParquetRowGroupBuilder::SetLastMaterialized(
    CedarParquetMaterializedFact materialized) {
  if (rows_.empty()) {
    return Status::InvalidArgument("no Cedar Parquet row to materialize");
  }
  rows_.back().materialized = std::move(materialized);
  return Status::OK();
}

void CedarParquetRowGroupBuilder::Reset() {
  rows_.clear();
  resident_bytes_ = 0;
}

}  // namespace ROCKSDB_NAMESPACE::cedar_parquet
