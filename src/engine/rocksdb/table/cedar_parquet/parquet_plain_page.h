// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <vector>

#include "rocksdb/slice.h"
#include "rocksdb/status.h"
#include "table/cedar_parquet/parquet_metadata.h"

namespace ROCKSDB_NAMESPACE::cedar_parquet {

struct CedarParquetDataPageSize {
  size_t compressed_size = 0;
  size_t uncompressed_size = 0;
};

Status EncodePlainByteArrayDataPage(const std::vector<Slice>& values,
                                    std::string* page,
                                    CedarParquetCompressionCodec codec =
                                        CedarParquetCompressionCodec::kUncompressed,
                                    CedarParquetDataPageSize* page_size = nullptr);
Status DecodePlainByteArrayDataPage(const std::string& page,
                                    std::vector<std::string>* values,
                                    size_t* consumed,
                                    CedarParquetCompressionCodec codec =
                                        CedarParquetCompressionCodec::kUncompressed,
                                    size_t max_uncompressed_body_bytes =
                                        64U * 1024U * 1024U);
Status DecodePlainByteArrayDataPage(const std::string& page,
                                    std::vector<std::string>* values,
                                    size_t* consumed,
                                    CedarParquetCompressionCodec codec,
                                    size_t max_uncompressed_body_bytes,
                                    size_t max_value_bytes, size_t max_values);

Status EncodePlainPrimitiveDataPage(
    const std::vector<std::optional<std::string>>& values, int32_t physical_type,
    bool optional, std::string* page, int32_t fixed_length = 0,
    CedarParquetCompressionCodec codec =
        CedarParquetCompressionCodec::kUncompressed,
    CedarParquetDataPageSize* page_size = nullptr);
Status DecodePlainPrimitiveDataPage(
    const std::string& page, int32_t physical_type, bool optional,
    std::vector<std::optional<std::string>>* values, size_t* consumed,
    int32_t fixed_length = 0, CedarParquetCompressionCodec codec =
                                  CedarParquetCompressionCodec::kUncompressed,
    size_t max_uncompressed_body_bytes = 64U * 1024U * 1024U);
Status DecodePlainPrimitiveDataPage(
    const std::string& page, int32_t physical_type, bool optional,
    std::vector<std::optional<std::string>>* values, size_t* consumed,
    int32_t fixed_length, CedarParquetCompressionCodec codec,
    size_t max_uncompressed_body_bytes,
    size_t max_value_bytes, size_t max_values);

}  // namespace ROCKSDB_NAMESPACE::cedar_parquet
