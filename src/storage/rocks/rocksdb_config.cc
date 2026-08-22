// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "storage/rocks/rocksdb_config.h"

#include <algorithm>
#include <exception>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <string>
#include <thread>

#include <sys/resource.h>
#include <sys/stat.h>
#include <unistd.h>

#include "rocksdb/cache.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/cedar_parquet/cedar_parquet_table_factory.h"
#include "kernel/query_debug_thresholds.h"

namespace cedar::internal {
namespace {

constexpr uint64_t kMiB = 1024ULL * 1024ULL;
constexpr uint64_t kGiB = 1024ULL * kMiB;
// KernelTest deliberately makes every persistent-state transition cheap to
// reach. The WBM still covers eight facts MemTables so the two Cedar-owned
// flush workers have a real, bounded drain window rather than artificial
// write-stop from an impossible queue depth.
constexpr uint64_t kKernelTestFactsWriteBufferBytes = 32ULL * 1024ULL;
constexpr uint64_t kKernelTestMetaWriteBufferBytes = 8ULL * 1024ULL;
constexpr uint64_t kKernelTestDefaultWriteBufferBytes = 8ULL * 1024ULL;
constexpr uint64_t kKernelTestBlockCacheBytes = 256ULL * 1024ULL;
constexpr uint64_t kKernelTestMemoryBudgetBytes = 1ULL * kMiB;
constexpr uint64_t kKernelTestWriteBufferManagerBytes = 1ULL * kMiB;
constexpr uint64_t kDebugWriteBufferManagerBytes = 256ULL * 1024ULL;
constexpr int kCedarFlushWorkerCapacity = 2;
constexpr int kCedarCompactionWorkerCapacity = 2;

uint64_t HostMemoryBytes() {
#if defined(__linux__)
  std::ifstream cgroup_limit("/sys/fs/cgroup/memory.max");
  std::string cgroup_value;
  if (cgroup_limit >> cgroup_value && cgroup_value != "max") {
    try {
      const uint64_t value = std::stoull(cgroup_value);
      if (value != 0 && value != UINT64_MAX) return value;
    } catch (const std::exception&) {
      // Fall through to the host limit. A malformed cgroup file must not make
      // an unconstrained process appear to have an arbitrary small budget.
    }
  }
#endif
  const long pages = sysconf(_SC_PHYS_PAGES);
  const long page_size = sysconf(_SC_PAGESIZE);
  if (pages <= 0 || page_size <= 0) return 0;
  const uint64_t page_count = static_cast<uint64_t>(pages);
  const uint64_t page_bytes = static_cast<uint64_t>(page_size);
  if (page_count > UINT64_MAX / page_bytes) return 0;
  return page_count * page_bytes;
}

bool HasProductionFileDescriptorBudget() {
  struct rlimit limit {};
  return getrlimit(RLIMIT_NOFILE, &limit) == 0 && limit.rlim_cur >= 8192;
}

std::shared_ptr<rocksdb::Cache> MakeBlockCache(uint64_t bytes) {
  return rocksdb::NewLRUCache(static_cast<size_t>(bytes), -1, true, 0.2);
}

std::shared_ptr<rocksdb::TableFactory> MakeTableFactory(
    const std::shared_ptr<rocksdb::Cache>& cache) {
  rocksdb::BlockBasedTableOptions table_options;
  table_options.block_cache = cache;
  table_options.cache_index_and_filter_blocks = true;
  table_options.cache_index_and_filter_blocks_with_high_priority = true;
  table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
  return std::shared_ptr<rocksdb::TableFactory>(
      rocksdb::NewBlockBasedTableFactory(table_options));
}

}  // namespace

StatusOr<ResolvedStorageProfile> ResolveStorageProfile(
    const FactStoreOptions& options) {
  ResolvedStorageProfile result;
  if (options.storage_profile == StorageProfile::kDeveloper) {
    result.memory_budget_bytes = options.write_buffer_bytes + options.block_cache_bytes;
    result.block_cache_bytes = options.block_cache_bytes;
    result.facts_write_buffer_bytes = options.write_buffer_bytes;
    result.meta_write_buffer_bytes = options.write_buffer_bytes;
    result.default_write_buffer_bytes = options.write_buffer_bytes;
    return result;
  }
  if (options.production.max_commit_batch_count == 0 ||
      options.production.max_commit_batch_count > kMaximumGroupCommitBatchCount ||
      options.production.max_commit_batch_bytes == 0 ||
      options.production.max_commit_batch_bytes > kMaximumGroupCommitBatchBytes) {
    return Status::InvalidArgument("production storage profile",
                                   "commit limits exceed the single-WAL protocol bounds");
  }
  if (options.group_commit_max_batch_size >
          options.production.max_commit_batch_count ||
      options.group_commit_max_batch_bytes >
          options.production.max_commit_batch_bytes) {
    return Status::InvalidArgument("production storage profile",
                                   "pipeline limits exceed the production profile cap");
  }
  if (!HasProductionFileDescriptorBudget()) {
    return Status::InvalidArgument("production storage profile",
                                   "RLIMIT_NOFILE must be at least 8192");
  }
  if (options.production.compaction_rate_limit_bytes_per_sec >
      static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
    return Status::InvalidArgument("production storage profile",
                                   "compaction rate limit exceeds int64 range");
  }
  if (options.production.recycle_log_file_num == 1 ||
      options.production.recycle_log_file_num > 4) {
    return Status::InvalidArgument(
        "production storage profile",
        "WAL recycling must be disabled or use two through four files");
  }
  if (options.storage_profile == StorageProfile::kKernelTest ||
      options.storage_profile == StorageProfile::kDebugSmallThresholds) {
    const bool debug = options.storage_profile == StorageProfile::kDebugSmallThresholds;
    result.memory_budget_bytes = kKernelTestMemoryBudgetBytes;
    result.block_cache_bytes = kKernelTestBlockCacheBytes;
    result.facts_write_buffer_bytes = debug ? kQueryDebugThresholds.memtable_bytes
                                            : kKernelTestFactsWriteBufferBytes;
    result.meta_write_buffer_bytes = debug ? kQueryDebugThresholds.memtable_bytes / 4
                                           : kKernelTestMetaWriteBufferBytes;
    result.default_write_buffer_bytes = debug ? kQueryDebugThresholds.memtable_bytes / 4
                                              : kKernelTestDefaultWriteBufferBytes;
    result.max_background_jobs = 2;
    result.max_subcompactions = 1;
    result.max_total_wal_size = 256ULL * 1024ULL;
    return result;
  }
  uint64_t budget = options.production.memory_budget_bytes;
  const uint64_t host_memory = HostMemoryBytes();
  if (budget == 0) {
    if (host_memory == 0) {
      return Status::InvalidArgument("production storage profile",
                                     "an explicit memory budget is required");
    }
    budget = host_memory * 40 / 100;
  }
  if (budget < kGiB) {
    return Status::InvalidArgument("production storage profile",
                                   "memory budget must be at least 1 GiB");
  }
  if (host_memory != 0 && budget > host_memory * 40 / 100) {
    return Status::InvalidArgument("production storage profile",
                                   "memory budget exceeds the 40% process limit");
  }
  result.memory_budget_bytes = budget;
  result.block_cache_bytes = options.production.block_cache_bytes == 0
                                 ? budget * 55 / 100
                                 : options.production.block_cache_bytes;
  if (result.block_cache_bytes > budget * 55 / 100) {
    return Status::InvalidArgument("production storage profile",
                                   "block cache exceeds the production allocation");
  }
  result.facts_write_buffer_bytes = budget * 25 / 100;
  result.meta_write_buffer_bytes = budget * 5 / 100;
  result.default_write_buffer_bytes = std::min<uint64_t>(32 * kMiB,
                                                         result.meta_write_buffer_bytes);
  result.max_background_jobs = options.production.max_background_jobs == 0
                                   ? 2
                                   : options.production.max_background_jobs;
  result.max_subcompactions = std::min<uint32_t>(2, result.max_background_jobs);
  result.max_total_wal_size = std::max(kGiB,
                                       2 * (result.facts_write_buffer_bytes +
                                            result.meta_write_buffer_bytes));
  result.compaction_rate_limit_bytes_per_sec =
      options.production.compaction_rate_limit_bytes_per_sec;
  return result;
}

Status ValidateProductionWalPlacement(const FactStoreOptions& options) {
  if (options.storage_profile != StorageProfile::kProductionAppend) {
    return Status::OK();
  }
  const std::string& wal_directory = options.production.wal_directory;
  if (!wal_directory.empty() &&
      !std::filesystem::path(wal_directory).is_absolute()) {
    return Status::InvalidArgument("production storage profile",
                                   "WAL directory must be absolute");
  }
  if (!options.production.require_separate_wal_device) {
    return Status::OK();
  }
  if (wal_directory.empty()) {
    return Status::InvalidArgument(
        "production storage profile",
        "separate WAL device requires an explicit WAL directory");
  }
  struct stat data_metadata {};
  struct stat wal_metadata {};
  if (stat(options.path.c_str(), &data_metadata) != 0 ||
      !S_ISDIR(data_metadata.st_mode)) {
    return Status::InvalidArgument("production storage profile",
                                   "database directory must exist");
  }
  if (stat(wal_directory.c_str(), &wal_metadata) != 0 ||
      !S_ISDIR(wal_metadata.st_mode)) {
    return Status::InvalidArgument(
        "production storage profile",
        "separate WAL device requires an existing WAL directory");
  }
  if (data_metadata.st_dev == wal_metadata.st_dev) {
    return Status::InvalidArgument(
        "production storage profile",
        "separate WAL device must differ from the database device");
  }
  return Status::OK();
}

rocksdb::Options MakeRocksDbOptions(const FactStoreOptions& options,
                                    bool is_new_database,
                                    const ResolvedStorageProfile* resolved) {
  rocksdb::Options result;
  result.create_if_missing = true;
  result.create_missing_column_families = is_new_database;
  result.comparator = rocksdb::BytewiseComparator();
  if (options.storage_profile == StorageProfile::kDeveloper || resolved == nullptr) {
    result.statistics = rocksdb::CreateDBStatistics();
    result.write_buffer_size = options.write_buffer_bytes;
    result.atomic_flush = true;
    result.enable_blob_files = true;
    result.min_blob_size = options.blob_threshold_bytes;
    result.table_factory = MakeTableFactory(MakeBlockCache(options.block_cache_bytes));
    return result;
  }

  const auto cache = MakeBlockCache(resolved->block_cache_bytes);
  result.write_buffer_size = resolved->default_write_buffer_bytes;
  const uint64_t write_buffer_manager_bytes =
      options.storage_profile == StorageProfile::kKernelTest
          ? kKernelTestWriteBufferManagerBytes
          : options.storage_profile == StorageProfile::kDebugSmallThresholds
                ? kDebugWriteBufferManagerBytes
          : resolved->facts_write_buffer_bytes + resolved->meta_write_buffer_bytes;
  result.write_buffer_manager = std::make_shared<rocksdb::WriteBufferManager>(
      static_cast<size_t>(write_buffer_manager_bytes),
      cache, false);
  result.atomic_flush = false;
  result.paranoid_checks = true;
  result.track_and_verify_wals_in_manifest = true;
  result.allow_concurrent_memtable_write = true;
  result.enable_write_thread_adaptive_yield = true;
  result.enable_pipelined_write = false;
  result.unordered_write = false;
  result.two_write_queues = false;
  // Cedar owns Kernel epoch assembly; this only bounds non-Kernel write groups.
  result.max_write_batch_group_size_bytes = 2ULL * 1024ULL * 1024ULL;
  result.manual_wal_flush = false;
  result.use_fsync = false;
  result.avoid_unnecessary_blocking_io = true;
  result.max_background_jobs = static_cast<int>(resolved->max_background_jobs);
  // Cedar grants every maintenance job. Declare fixed engine capacity rather
  // than inheriting RocksDB's max_background_jobs / 4 flush heuristic.
  result.max_background_flushes = kCedarFlushWorkerCapacity;
  result.max_background_compactions = kCedarCompactionWorkerCapacity;
  result.max_subcompactions = resolved->max_subcompactions;
  result.bytes_per_sync = kMiB;
  result.wal_bytes_per_sync = 0;
  result.wal_dir = options.production.wal_directory;
  result.recycle_log_file_num = options.production.recycle_log_file_num;
  result.cedar_kernel_mode =
      options.storage_profile == StorageProfile::kKernelTest ||
      options.storage_profile == StorageProfile::kDebugSmallThresholds ||
      options.production.kernel_mode;
  result.cedar_disable_periodic_tasks =
      !options.production.diagnostic_periodic_tasks;
  if (options.production.diagnostic_periodic_tasks) {
    result.statistics = rocksdb::CreateDBStatistics();
  } else {
    result.stats_dump_period_sec = 0;
    result.stats_persist_period_sec = 0;
  }
  result.max_total_wal_size = resolved->max_total_wal_size;
  result.max_open_files = 4096;
  result.keep_log_file_num = 10;
  if (resolved->compaction_rate_limit_bytes_per_sec != 0) {
    result.rate_limiter = std::shared_ptr<rocksdb::RateLimiter>(
        rocksdb::NewGenericRateLimiter(
            static_cast<int64_t>(resolved->compaction_rate_limit_bytes_per_sec)));
  }
  result.enable_blob_files = false;
  result.table_factory = MakeTableFactory(cache);
  return result;
}

std::vector<rocksdb::ColumnFamilyDescriptor> MakeRocksDbColumnFamilyDescriptors(
    const FactStoreOptions& store_options, const rocksdb::Options& options,
    const ResolvedStorageProfile* resolved) {
  rocksdb::ColumnFamilyOptions default_options(options);
  rocksdb::ColumnFamilyOptions facts_options(options);
  rocksdb::ColumnFamilyOptions meta_options(options);
  rocksdb::cedar_parquet::CedarParquetTableOptions parquet_options;
  if (resolved != nullptr) {
    parquet_options.page_compression =
        rocksdb::cedar_parquet::CedarParquetCompressionCodec::kLz4Raw;
  }
  facts_options.table_factory = std::shared_ptr<rocksdb::TableFactory>(
      rocksdb::NewCedarParquetFactTableFactory(parquet_options));
  facts_options.enable_blob_files = false;
  facts_options.memtable_factory = std::make_shared<rocksdb::PartitionedVersionRadixFactory>();
  facts_options.compression = rocksdb::kNoCompression;
  facts_options.bottommost_compression = rocksdb::kNoCompression;
  if (resolved != nullptr) {
    default_options.write_buffer_size = resolved->default_write_buffer_bytes;
    default_options.enable_blob_files = false;

    const bool kernel_test =
        store_options.storage_profile == StorageProfile::kKernelTest ||
        store_options.storage_profile == StorageProfile::kDebugSmallThresholds;
    facts_options.write_buffer_size = kernel_test
                                         ? (store_options.storage_profile == StorageProfile::kDebugSmallThresholds
                                                ? kQueryDebugThresholds.memtable_bytes
                                                : kKernelTestFactsWriteBufferBytes)
                                         : 128 * kMiB;
    // Two buffers can be draining under Cedar flush credits, while bounded
    // queued debt and one active buffer remain available for foreground work.
    facts_options.max_write_buffer_number = kernel_test ? 6 : 4;
    facts_options.min_write_buffer_number_to_merge = kernel_test ? 1 : 2;
    facts_options.level_compaction_dynamic_level_bytes = true;
    facts_options.target_file_size_base = kernel_test
                                              ? (store_options.storage_profile == StorageProfile::kDebugSmallThresholds
                                                     ? kQueryDebugThresholds.memtable_bytes
                                                     : 64ULL * 1024ULL)
                                              : 128 * kMiB;
    facts_options.level0_file_num_compaction_trigger = kernel_test ? 2 : 8;
    facts_options.level0_slowdown_writes_trigger = kernel_test ? 12 : 24;
    facts_options.level0_stop_writes_trigger = kernel_test ? 24 : 36;
    facts_options.soft_pending_compaction_bytes_limit =
        kernel_test ? 512ULL * 1024ULL : 8 * kGiB;
    facts_options.hard_pending_compaction_bytes_limit =
        kernel_test ? 2ULL * kMiB : 32 * kGiB;
    meta_options.write_buffer_size = kernel_test
                                        ? (store_options.storage_profile == StorageProfile::kDebugSmallThresholds
                                                ? kQueryDebugThresholds.memtable_bytes / 4
                                                : kKernelTestMetaWriteBufferBytes)
                                        : 32 * kMiB;
    meta_options.max_write_buffer_number = kernel_test ? 6 : 3;
    meta_options.min_write_buffer_number_to_merge = 1;
    meta_options.level0_file_num_compaction_trigger = kernel_test ? 2 : 4;
    meta_options.level0_slowdown_writes_trigger = 12;
    meta_options.level0_stop_writes_trigger = kernel_test ? 24 : 20;
    meta_options.enable_blob_files = false;
  }
  return {{rocksdb::kDefaultColumnFamilyName, std::move(default_options)},
          {"facts", std::move(facts_options)},
          {"meta", std::move(meta_options)}};
}

}  // namespace cedar::internal
