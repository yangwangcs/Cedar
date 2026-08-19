// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <string>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "table/cedar_parquet/cedar_parquet_format.h"

namespace ROCKSDB_NAMESPACE {

// Encodes the standard RocksDB seek sentinel for a fixed-size Cedar fact user
// key into the normalized Parquet sort-key space.
Status MakeCedarParquetSortLowerBound(const Slice& user_key, std::string* sort_key);

// Internal Cedar extension. The call holds one referenced SuperVersion for its
// whole duration and enumerates only files belonging to that Version. The
// supplied ReadOptions (including snapshot) are applied to the resulting
// canonical internal keys before a batch is delivered.
Status ScanCedarParquetFacts(
    DB* db, ColumnFamilyHandle* column_family, const ReadOptions& read_options,
    const cedar_parquet::CedarParquetScanSpec& spec,
    const cedar_parquet::CedarParquetColumnarBatchVisitor& visitor);

}  // namespace ROCKSDB_NAMESPACE
