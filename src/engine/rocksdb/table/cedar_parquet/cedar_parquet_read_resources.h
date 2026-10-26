// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <atomic>
#include <array>
#include <memory>
#include "rocksdb/cache.h"
#include "rocksdb/options.h"
#include "cedar_parquet_read_budget.h"

namespace ROCKSDB_NAMESPACE::cedar_parquet {
struct CedarParquetReadPolicy {
  ReadOptions options;
  bool admit = false;
  std::shared_ptr<CedarParquetQueryMemory> query_memory;
};

// Runtime-only resources. These are not serialized into table metadata.
struct ColumnPageMetrics {
  std::atomic<uint64_t> hits{0}, misses{0}, admissions{0}, admission_rejections{0};
  std::atomic<uint64_t> decoded_pages{0}, decoded_bytes{0};
  // Includes failed primitive decode attempts, after file I/O, before typed
  // conversion/validation. Budget callbacks within the decoder are included.
  std::atomic<uint64_t> decode_attempts{0}, decode_nanoseconds{0};
};
struct CanonicalPageCacheMetrics {
  std::atomic<uint64_t> hits{0}, misses{0}, decodes{0}, decoded_bytes{0};
  std::atomic<uint64_t> resident_charge_bytes{0}, pinned_charge_bytes{0};
  std::atomic<uint64_t> uncached_live_bytes{0}, insert_failures{0};
  std::atomic<uint64_t> column_decodes{0};
  std::atomic<uint64_t> decode_nanoseconds{0};
  std::array<ColumnPageMetrics, 25> columns;
  std::atomic<uint64_t> file_read_calls{0}, file_read_bytes{0};
  std::atomic<uint64_t> reader_metadata_bytes{0};
  std::atomic<uint64_t> reader_metadata_peak_bytes{0};
};
struct CedarParquetReadResources {
  std::shared_ptr<Cache> canonical_page_cache;
  std::shared_ptr<CanonicalPageCacheMetrics> metrics;
  std::shared_ptr<CedarParquetReadBudget> working_budget;
  // Long-lived reader metadata has a separate pool from page decode scratch.
  std::shared_ptr<CedarParquetReadBudget> metadata_budget;
};
}  // namespace ROCKSDB_NAMESPACE::cedar_parquet
