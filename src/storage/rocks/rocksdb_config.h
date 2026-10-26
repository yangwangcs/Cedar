// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_FACT_ROCKSDB_CONFIG_H_
#define CEDAR_FACT_ROCKSDB_CONFIG_H_

#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "engine/rocksdb/table/cedar_parquet/cedar_parquet_read_resources.h"

#include "storage/facts/fact_store.h"

namespace cedar::internal {

struct ResolvedStorageProfile {
  uint64_t memory_budget_bytes = 0;
  uint64_t block_cache_bytes = 0;
  uint64_t storage_read_working_bytes = 0;
  uint64_t storage_reader_metadata_bytes = 0;
  uint64_t facts_write_buffer_bytes = 0;
  uint64_t meta_write_buffer_bytes = 0;
  uint64_t default_write_buffer_bytes = 0;
  uint64_t facts_memtable_bytes = 0;
  uint64_t meta_memtable_bytes = 0;
  uint64_t default_memtable_bytes = 0;
  uint32_t facts_write_buffer_count = 0;
  uint32_t meta_write_buffer_count = 0;
  uint32_t default_write_buffer_count = 0;
  uint32_t flush_worker_capacity = 0;
  uint32_t compaction_worker_capacity = 0;
  uint32_t normal_flush_credits = 0;
  uint32_t emergency_flush_credits = 0;
  uint32_t compaction_credits = 0;
  uint32_t max_background_jobs = 0;
  uint32_t max_subcompactions = 0;
  uint64_t max_total_wal_size = 0;
  uint64_t compaction_rate_limit_bytes_per_sec = 0;
};

StatusOr<ResolvedStorageProfile> ResolveStorageProfile(
    const FactStoreOptions& options);

// Validates Cedar's physical WAL placement policy before RocksDB opens the
// database. RocksDB remains responsible for the directory's WAL contents.
Status ValidateProductionWalPlacement(const FactStoreOptions& options);

rocksdb::Options MakeRocksDbOptions(const FactStoreOptions& options,
                                    bool is_new_database,
                                    const ResolvedStorageProfile* resolved = nullptr,
                                    std::shared_ptr<rocksdb::Cache>* shared_cache_out = nullptr);

std::vector<rocksdb::ColumnFamilyDescriptor> MakeRocksDbColumnFamilyDescriptors(
    const FactStoreOptions& store_options,
    const rocksdb::Options& options,
    const ResolvedStorageProfile* resolved = nullptr,
    rocksdb::cedar_parquet::CedarParquetReadResources resources = {});

}  // namespace cedar::internal

#endif  // CEDAR_FACT_ROCKSDB_CONFIG_H_
