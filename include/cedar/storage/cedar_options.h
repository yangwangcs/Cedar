// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef FERN_OPTIONS_H_
#define FERN_OPTIONS_H_

#include <cstddef>
#include <cstdint>
#include <string>

#include "cedar/core/env.h"

namespace cedar {

// Granule configuration
struct GranuleOptions {
  // Number of rows per granule (default: 8192)
  size_t rows_per_granule = 8192;
  
  // Whether to enable delta encoding for entity IDs
  bool enable_delta_encoding = true;
  
  // Whether to enable delta-of-delta encoding for tx times
  bool enable_dod_encoding = true;
};

// Size-Tiered Compaction configuration for Zone-Columnar format
// Blob storage configuration for automatic large value handling
struct BlobStorageConfig {
  // Whether to enable automatic blob storage (default: true)
  bool enable_auto_blob = true;
  
  // Maximum inline string length (default: 4 bytes)
  // Strings longer than this are stored in blob files
  size_t inline_string_max_len = 4;
  
  // Minimum blob size (default: 1 byte)
  // Data smaller than this is forced to inline storage
  size_t min_blob_size = 1;
  
  // Maximum blob file size (default: 256MB)
  size_t max_blob_file_size = 256 * 1024 * 1024;
  
  // Blob file directory (default: "blobs" subdirectory in DB path)
  // If empty, uses db_path + "/blobs"
  std::string blob_dir;
};

// Size-Tiered Compaction configuration for Zone-Columnar format
struct SizeTieredCompactionConfig {
  // L0 capacity threshold (default: 64MB)
  uint64_t l0_max_size = 64 * 1024 * 1024;
  
  // Single SST file size target (default: 16MB)
  uint64_t l0_file_size = 16 * 1024 * 1024;
  
  // Maximum files in L0 (default: 4)
  size_t l0_max_files = 4;
  
  // Level size ratio multiplier (default: 4)
  double size_ratio = 4.0;
  
  // Maximum levels (default: 7, i.e., L0-L6)
  int max_levels = 7;
  
  // Trigger ratio (current_size / threshold > ratio triggers compaction)
  double level_size_trigger_ratio = 1.2;
  
  // Maximum merge width per compaction (default: 32)
  size_t max_merge_width = 32;
  
  // Number of background compaction threads (default: 2)
  int compaction_threads = 2;
  
  // Whether to enable background compaction (default: true)
  bool enable_background_compaction = true;
  
  // Tombstone cleanup level (L3+ can cleanup tombstones, default: 3)
  int tombstone_cleanup_level = 3;
  
  // Blob rewrite threshold (blobs larger than this are rewritten, default: 1MB)
  uint64_t blob_rewrite_threshold = 1024 * 1024;
};

// Legacy Compaction configuration (kept for backward compatibility)
struct CompactionConfig {
  // Minimum number of Frond files to trigger compaction (default: 4)
  size_t min_files = 4;
  
  // Minimum total size to trigger compaction in bytes (default: 64MB)
  size_t min_size = 64 * 1024 * 1024;
  
  // Target output file size for compaction in bytes (default: 256MB)
  size_t target_file_size = 256 * 1024 * 1024;
  
  // Maximum number of levels in LSM tree (default: 7)
  int max_levels = 7;
  
  CompactionConfig() = default;
  
  CompactionConfig(size_t min_files_, size_t min_size_, size_t target_file_size_)
      : min_files(min_files_), min_size(min_size_), target_file_size(target_file_size_) {}
};

// Cedar DB Options
struct CedarOptions {
  // Create the database if it is missing
  bool create_if_missing = false;
  
  // Return an error if the database already exists
  bool error_if_exists = false;
  
  // MemTable threshold in bytes (default: 4MB)
  size_t memtable_threshold = 4 * 1024 * 1024;
  
  // Write buffer size (default: 4MB)
  size_t write_buffer_size = 4 * 1024 * 1024;
  
  // Granule options
  GranuleOptions granule_options;
  
  // Compaction configuration
  CompactionConfig compaction_config;
  
  // Size-Tiered Compaction configuration (for Zone-Columnar format)
  // If use_zone_columnar_format is true, this config is used
  SizeTieredCompactionConfig size_tiered_config;
  
  // Blob storage configuration for automatic large value handling
  BlobStorageConfig blob_storage;
  
  // Whether to use Zone-Columnar format (default: true)
  // If false, uses legacy Frond format
  bool use_zone_columnar_format = true;
  
  // Column ID for this database (default: 0)
  uint16_t column_id = 0;
  
  // Environment for file operations
  cedar::Env* env = nullptr;
  
  // Whether to enable WAL (Write Ahead Log) for transaction support (default: true)
  bool enable_wal = true;
  
  // Whether to enable bloom filter (default: true)
  bool enable_bloom_filter = true;
  
  // Bloom filter bits per key (default: 10)
  int bloom_bits_per_key = 10;
  
  // Whether to verify checksums on read
  bool verify_checksums = true;
  
  // Whether to paranoid checks
  bool paranoid_checks = false;
  
  // File cache size - number of open file handles to cache (default: 256)
  // Larger values improve read performance but use more memory
  // Each cached file uses approximately 8KB-64KB depending on file size
  // For large datasets (>1GB), use 512-1024
  size_t file_cache_size = 256;
  
  // Helper to calculate optimal file cache size based on available memory
  static size_t CalculateOptimalCacheSize(size_t available_memory_mb) {
    // Use approximately 2% of available memory for file cache
    // Assuming average file size of 64MB for large datasets
    size_t max_handles = (available_memory_mb * 1024 * 1024) / (64 * 1024 * 1024) / 50;
    // Clamp between 128 and 1024
    if (max_handles < 128) return 128;
    if (max_handles > 1024) return 1024;
    return max_handles;
  }
};

// Read options
struct ReadOptions {
  // Whether to verify checksums
  bool verify_checksums = false;
  
  // Whether to fill cache
  bool fill_cache = true;
};

// Write options
struct WriteOptions {
  // If true, sync data to disk before returning
  bool sync = false;
};

// Flush options
struct FlushOptions {
  // If true, wait for flush to complete
  bool wait = true;
  // If true, allow write stall during flush
  bool allow_write_stall = true;
};

// ============================================================================
// MVCC Optimization Configuration (Phase 2)
// ============================================================================

// MVCC 优化配置结构体
struct MVCCOptimizationConfig {
  // 是否启用分片时间戳分配器
  // 推荐: 16+ 线程时启用
  bool enable_sharded_timestamp_allocator = true;
  
  // 分片数量 (默认: CPU 核心数)
  uint32_t timestamp_shard_count = 0;  // 0 = auto (CPU cores)
  
  // 批量预分配大小
  uint32_t timestamp_batch_size = 1000;
  
  // 是否启用版本链跳表索引
  // 推荐: 实体版本数 > 100 时启用
  bool enable_version_chain_index = true;
  
  // 自动构建索引的阈值
  size_t version_chain_index_threshold = 100;
  
  // 最大索引层数
  int version_chain_max_level = 4;
  
  // 是否启用 Delta 编码
  // 推荐: 版本值变化较小时启用
  bool enable_delta_encoding = false;  // 默认关闭，需显式开启
  
  // 每组最大 delta 数
  size_t delta_max_per_group = 16;
  
  // 是否启用时间范围布隆过滤器
  // 推荐: 大量 Frond 文件时启用
  bool enable_temporal_bloom_filter = true;
  
  // 过滤器假阳性率
  double temporal_filter_false_positive_rate = 0.01;
  
  // 时间桶粒度 (小时)
  uint32_t temporal_filter_hours_per_bucket = 24;
  
  // 便捷方法：启用所有优化
  static MVCCOptimizationConfig AllEnabled() {
    MVCCOptimizationConfig config;
    config.enable_sharded_timestamp_allocator = true;
    config.enable_version_chain_index = true;
    config.enable_delta_encoding = true;
    config.enable_temporal_bloom_filter = true;
    return config;
  }
  
  // 便捷方法：禁用所有优化 (向后兼容)
  static MVCCOptimizationConfig AllDisabled() {
    MVCCOptimizationConfig config;
    config.enable_sharded_timestamp_allocator = false;
    config.enable_version_chain_index = false;
    config.enable_delta_encoding = false;
    config.enable_temporal_bloom_filter = false;
    return config;
  }
  
  // ========== Phase 3: Scalability Optimizations ==========
  
  // 是否启用分片 WAL (推荐: 32+ 线程时启用)
  bool enable_sharded_wal = true;
  
  // WAL 分片数量
  uint32_t wal_shard_count = 0;  // 0 = auto (CPU cores)
  
  // WAL 批量写入超时 (微秒)
  uint32_t wal_batch_timeout_us = 100;
  
  // WAL 批量写入最大条数
  size_t wal_batch_max_size = 100;
  
  // 是否启用后台 WAL 合并
  bool wal_enable_background_merger = true;
  
  // WAL 合并间隔 (毫秒)
  uint32_t wal_merge_interval_ms = 100;
  
  // 是否启用完全无锁 MemTable
  bool enable_lockfree_memtable = true;
  
  // MemTable 初始容量
  size_t memtable_initial_capacity = 1024 * 1024;  // 1M entries
  
  // MemTable GC 间隔 (毫秒)
  uint32_t memtable_gc_interval_ms = 1000;
  
  // 是否启用 MemTable 预分配
  bool memtable_enable_preallocation = true;
  
  // 是否启用异步索引构建器
  bool enable_async_index_builder = true;
  
  // 索引构建器工作线程数
  uint32_t index_builder_worker_threads = 4;
  
  // 索引构建器最大并发构建数
  uint32_t index_builder_max_concurrent = 16;
  
  // 是否启用批量索引构建
  bool index_builder_enable_batch = true;
  
  // 批量索引构建的最大批次大小
  size_t index_builder_batch_size = 10;
  
  // 便捷方法：启用 Phase 3 全部优化 (32+ 线程优化)
  static MVCCOptimizationConfig Phase3Scalability() {
    MVCCOptimizationConfig config = AllEnabled();
    config.enable_sharded_wal = true;
    config.enable_lockfree_memtable = true;
    config.enable_async_index_builder = true;
    return config;
  }
};

// 扩展 CedarOptions
struct CedarOptionsMVCCExtended : public CedarOptions {
  // MVCC 优化配置
  MVCCOptimizationConfig mvcc_optimization;
};

}  // namespace cedar

#endif  // FERN_OPTIONS_H_
