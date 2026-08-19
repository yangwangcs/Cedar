// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE::cedar_parquet {

inline constexpr int32_t kParquetBoolean = 0;
inline constexpr int32_t kParquetInt32 = 1;
inline constexpr int32_t kParquetInt64 = 2;
inline constexpr int32_t kParquetFloat = 4;
inline constexpr int32_t kParquetDouble = 5;
inline constexpr int32_t kParquetByteArray = 6;
inline constexpr int32_t kParquetFixedLenByteArray = 7;
inline constexpr int32_t kParquetRequired = 0;
inline constexpr int32_t kParquetOptional = 1;

enum class CedarParquetCompressionCodec : int32_t {
  kUncompressed = 0,
  kZstd = 6,
  kLz4Raw = 7,
};

struct CedarParquetSchemaElement {
  std::string name;
  int32_t physical_type = -1;
  int32_t type_length = 0;
  int32_t repetition_type = -1;
  int32_t num_children = 0;
};

struct CedarParquetKeyValueMetadata {
  std::string key;
  std::optional<std::string> value;
};

struct CedarParquetFooter {
  int32_t version = 2;
  std::vector<CedarParquetSchemaElement> schema;
  int64_t num_rows = 0;
  struct ColumnChunk {
    struct PageLocation {
      int64_t offset = 0;
      int32_t compressed_page_size = 0;
      int64_t first_row_index = 0;
    };
    struct PageIndex {
      std::string min_value;
      std::string max_value;
      bool all_null = false;
    };
    std::string path;
    int32_t physical_type = -1;
    int32_t type_length = 0;
    CedarParquetCompressionCodec compression_codec =
        CedarParquetCompressionCodec::kUncompressed;
    int64_t data_page_offset = 0;
    int64_t total_compressed_size = 0;
    int64_t total_uncompressed_size = 0;
    int64_t num_values = 0;
    std::string min_value;
    std::string max_value;
    int64_t offset_index_offset = -1;
    int32_t offset_index_length = 0;
    std::vector<PageLocation> page_locations;
    int64_t column_index_offset = -1;
    int32_t column_index_length = 0;
    int64_t bloom_filter_offset = -1;
    int32_t bloom_filter_length = 0;
    std::vector<PageIndex> page_indexes;
  };
  struct RowGroup {
    std::vector<ColumnChunk> columns;
    int64_t total_byte_size = 0;
    int64_t num_rows = 0;
    int64_t file_offset = 0;
  };
  std::vector<RowGroup> row_groups;
  std::vector<CedarParquetKeyValueMetadata> key_value_metadata;
  std::string created_by;
};

CedarParquetFooter MakeRequiredFactsFooter(
    int64_t num_rows,
    const std::vector<CedarParquetKeyValueMetadata>& key_value_metadata);
Status AddCedarParquetRowGroup(CedarParquetFooter* footer,
                               CedarParquetFooter::RowGroup row_group);
Status EncodeCompactFooter(const CedarParquetFooter& footer, std::string* encoded);
Status DecodeCompactFooter(const std::string& encoded, CedarParquetFooter* footer);
Status AppendParquetFooter(std::string* file, const CedarParquetFooter& footer);
Status ParseParquetFooter(const std::string& file, CedarParquetFooter* footer,
                          size_t* footer_offset);
Status EncodeOffsetIndex(
    const std::vector<CedarParquetFooter::ColumnChunk::PageLocation>& locations,
    std::string* encoded);
Status DecodeOffsetIndex(
    const std::string& encoded,
    std::vector<CedarParquetFooter::ColumnChunk::PageLocation>* locations);
Status EncodeColumnIndex(
    const std::vector<CedarParquetFooter::ColumnChunk::PageIndex>& pages,
    std::string* encoded);
Status DecodeColumnIndex(
    const std::string& encoded,
    std::vector<CedarParquetFooter::ColumnChunk::PageIndex>* pages);

}  // namespace ROCKSDB_NAMESPACE::cedar_parquet
