// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_STORAGE_OPTIONS_H_
#define CEDAR_STORAGE_OPTIONS_H_

#include <cstdint>
#include <string>

namespace cedar {

inline constexpr uint32_t kMaximumCommitBatchCount = 512;
inline constexpr uint64_t kMaximumCommitBatchBytes = 2ULL * 1024ULL * 1024ULL;

// Selects the durability primitive used by synchronous WAL writes.
// kFullDeviceFlush keeps the platform's strongest flush (F_FULLFSYNC on macOS).
// kFilesystemFlush uses fdatasync/fsync: it still survives a process crash but
// a power loss may discard commits the database already acknowledged.
enum class WalSyncMode : uint8_t {
  kFullDeviceFlush = 0,
  kFilesystemFlush = 1,
};

enum class StorageProfile : uint8_t {
  kDeveloper,
  // Test-only Kernel profile with intentionally small maintenance thresholds.
  // It exercises the same single-WAL and recovery path as production without
  // exposing production capacity controls to tests.
  kKernelTest,
  // Debug-only profile with deliberately small capacities. It selects the
  // same production classes and recovery paths; only limits are reduced.
  kDebugSmallThresholds,
  kProductionAppend,
};

constexpr bool UsesCedarKernelProfile(StorageProfile profile) {
  return profile == StorageProfile::kKernelTest ||
         profile == StorageProfile::kDebugSmallThresholds ||
         profile == StorageProfile::kProductionAppend;
}

struct ProductionStorageOptions {
  uint64_t memory_budget_bytes = 0;
  uint64_t block_cache_bytes = 0;
  uint32_t max_background_jobs = 0;
  uint64_t facts_memtable_bytes = 0;
  uint64_t meta_memtable_bytes = 0;
  uint64_t default_memtable_bytes = 0;
  uint32_t facts_memtable_count = 0;
  uint32_t meta_memtable_count = 0;
  uint32_t default_memtable_count = 0;
  uint32_t flush_worker_capacity = 0;
  uint32_t compaction_worker_capacity = 0;
  uint32_t normal_flush_credits = 0;
  uint32_t compaction_credits = 0;
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
