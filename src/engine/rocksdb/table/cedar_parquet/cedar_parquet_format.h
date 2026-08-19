// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "rocksdb/status.h"
#include "table/cedar_parquet/parquet_metadata.h"

namespace ROCKSDB_NAMESPACE::cedar_parquet {

inline constexpr char kCedarParquetFormatVersion[] = "cedar.parquet.facts.v2";
inline constexpr char kCedarParquetBytewiseComparatorName[] =
    "leveldb.BytewiseComparator";
inline constexpr char kCedarParquetCanonicalSchemaDigest[] =
    "3a34a5d310945d11f1e933a5c141a6c52ba2e1e269455f5f7a7dbe484991cd84";
inline constexpr char kCedarParquetComparatorDigest[] =
    "cedar.v2.internal-key.bytewise.v1";
inline constexpr char kCedarParquetCreatedBy[] =
    "Cedar authoritative-columnar facts v2";
inline constexpr char kCedarParquetFeatureBits[] =
    "plain_v1,offset_index,column_index,bloom_v1,lz4_raw,zstd";
inline constexpr uint64_t kCedarParquetMinimumRowGroupBytes = 1024;
inline constexpr uint64_t kCedarParquetDefaultMaxFooterBytes =
    4ULL * 1024ULL * 1024ULL;
inline constexpr uint64_t kCedarParquetDefaultMaxIndexBytes =
    4ULL * 1024ULL * 1024ULL;
inline constexpr size_t kCedarParquetV2UserKeyBytes = 32;
inline constexpr size_t kCedarParquetV2SortKeyBytes = 40;

struct CedarParquetTableOptions {
  uint32_t row_group_max_rows = 16U * 1024U;
  uint64_t row_group_max_bytes = 64ULL * 1024ULL * 1024ULL;
  uint32_t page_max_rows = 1024;
  uint64_t page_max_bytes = 1ULL * 1024ULL * 1024ULL;
  uint64_t max_row_bytes = 4ULL * 1024ULL * 1024ULL;
  uint64_t max_footer_bytes = kCedarParquetDefaultMaxFooterBytes;
  uint64_t max_index_bytes = kCedarParquetDefaultMaxIndexBytes;
  CedarParquetCompressionCodec page_compression =
      CedarParquetCompressionCodec::kUncompressed;
  std::string comparator_name = kCedarParquetBytewiseComparatorName;

  Status Validate() const;
};

struct CedarParquetMaterializedFact {
  uint32_t part_id = 0;
  uint32_t fact_family = 0;
  uint32_t property_id = 0;
  uint64_t entity_id = 0;
  uint64_t valid_from = 0;
  uint64_t cedar_commit_seq = 0;
  uint64_t rocksdb_sequence = 0;
  uint32_t operation = 0;
  uint32_t schema_epoch = 0;
  uint32_t physical_type = 0;
  std::optional<bool> bool_value;
  std::optional<int32_t> int32_value;
  std::optional<int64_t> int64_value;
  std::optional<float> float32_value;
  std::optional<double> float64_value;
  std::optional<uint64_t> timestamp64_value;
  std::optional<std::string> bytes_value;
  std::optional<uint32_t> source_part_id;
  std::optional<uint64_t> source_vertex_id;
  std::optional<uint32_t> target_part_id;
  std::optional<uint64_t> target_vertex_id;
  std::optional<uint64_t> edge_type;
  bool rocksdb_deletion = false;
};

struct CedarParquetRow {
  std::string sort_key;
  std::string internal_key;
  std::string encoded_value;
  CedarParquetMaterializedFact materialized;
};

enum class CedarParquetColumnId : uint8_t {
  kSortKey = 0,
  kInternalKey = 1,
  kEncodedValue = 2,
  kPartId = 3,
  kFactFamily = 4,
  kPropertyId = 5,
  kEntityId = 6,
  kValidFrom = 7,
  kCedarCommitSeq = 8,
  kRocksdbSequence = 9,
  kOperation = 10,
  kSchemaEpoch = 11,
  kPhysicalType = 12,
  kBoolValue = 13,
  kInt32Value = 14,
  kInt64Value = 15,
  kFloat32Value = 16,
  kFloat64Value = 17,
  kTimestamp64Value = 18,
  kBytesValue = 19,
  kSourcePartId = 20,
  kSourceVertexId = 21,
  kTargetPartId = 22,
  kTargetVertexId = 23,
  kEdgeType = 24,
};

using CedarParquetTypedVector = std::variant<
    std::vector<uint32_t>, std::vector<uint64_t>, std::vector<int32_t>,
    std::vector<int64_t>, std::vector<float>, std::vector<double>,
    std::vector<uint8_t>, std::vector<std::string>>;

struct CedarParquetColumnVector {
  CedarParquetColumnId id;
  CedarParquetTypedVector values;
  // One byte per row: 1 means the corresponding typed value is present.
  std::vector<uint8_t> present;
};

struct CedarParquetColumnarBatch {
  std::vector<std::string> internal_keys;
  std::vector<std::string> encoded_values;
  std::vector<CedarParquetColumnVector> columns;

  size_t row_count() const { return internal_keys.size(); }
};

struct CedarParquetScanSpec {
  std::optional<std::string> sort_key_lower;
  std::optional<std::string> sort_key_upper;
  std::vector<CedarParquetColumnId> projection;
  uint32_t batch_row_limit = 1024;
};

using CedarParquetColumnarBatchVisitor =
    std::function<Status(const CedarParquetColumnarBatch&)>;

Status DecodeCedarParquetMaterializedFact(
    const Slice& internal_key, const Slice& encoded_value,
    CedarParquetMaterializedFact* fact);

Status EncodeCedarParquetSortKey(const Slice& internal_key,
                                 std::string* sort_key);
Status DecodeCedarParquetSortKey(const Slice& sort_key,
                                 std::string* internal_key);
Status EncodeCedarParquetRow(const Slice& internal_key, const Slice& encoded_value,
                             CedarParquetRow* row);
Status DecodeCedarParquetRow(const CedarParquetRow& row, std::string* internal_key,
                             std::string* encoded_value);

class CedarParquetRowGroupBuilder {
 public:
  explicit CedarParquetRowGroupBuilder(CedarParquetTableOptions options);

  Status Add(CedarParquetRow row);
  Status SetLastMaterialized(CedarParquetMaterializedFact materialized);
  void Reset();

  const std::vector<CedarParquetRow>& rows() const { return rows_; }
  uint64_t resident_bytes() const { return resident_bytes_; }
  bool empty() const { return rows_.empty(); }

 private:
  Status ChargeRow(const CedarParquetRow& row, uint64_t* charge) const;

  CedarParquetTableOptions options_;
  std::vector<CedarParquetRow> rows_;
  uint64_t resident_bytes_ = 0;
};

}  // namespace ROCKSDB_NAMESPACE::cedar_parquet
