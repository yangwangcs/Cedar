// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_STORAGE_OPTIONS_H_
#define CEDAR_STORAGE_OPTIONS_H_

#include <cstdint>
#include <string>

namespace cedar {

inline constexpr uint32_t kMaximumCommitBatchCount = 512;
inline constexpr uint64_t kMaximumCommitBatchBytes = 2ULL * 1024ULL * 1024ULL;

enum class StorageProfile : uint8_t { kDeveloper, kProductionAppend };

struct ProductionStorageOptions {
  uint64_t memory_budget_bytes = 0;
  uint64_t block_cache_bytes = 0;
  uint32_t max_background_jobs = 0;
  uint32_t max_commit_batch_count = kMaximumCommitBatchCount;
  uint64_t max_commit_batch_bytes = kMaximumCommitBatchBytes;
  uint64_t compaction_rate_limit_bytes_per_sec = 0;
  bool kernel_mode = false;
  bool diagnostic_periodic_tasks = false;
  uint32_t recycle_log_file_num = 0;
  std::string wal_directory;
  bool require_separate_wal_device = false;
};

struct RuntimeSamplingTiming {
  uint64_t pressure_properties_us = 0;
  uint64_t recovery_wal_bytes_us = 0;
  uint64_t runtime_metrics_properties_us = 0;
  uint64_t snapshot_publish_us = 0;
  uint64_t refresh_total_us = 0;
};

}  // namespace cedar

#endif  // CEDAR_STORAGE_OPTIONS_H_
