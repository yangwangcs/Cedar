// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <atomic>
#include <memory>
#include "rocksdb/cache.h"

namespace ROCKSDB_NAMESPACE::cedar_parquet {
// Runtime-only resources. These are not serialized into table metadata.
struct CanonicalPageCacheMetrics {
  std::atomic<uint64_t> hits{0}, misses{0}, decodes{0}, decoded_bytes{0};
  std::atomic<uint64_t> resident_charge_bytes{0}, pinned_charge_bytes{0};
  std::atomic<uint64_t> uncached_live_bytes{0}, insert_failures{0};
};
struct CedarParquetReadResources {
  std::shared_ptr<Cache> canonical_page_cache;
  std::shared_ptr<CanonicalPageCacheMetrics> metrics;
};
}  // namespace ROCKSDB_NAMESPACE::cedar_parquet
