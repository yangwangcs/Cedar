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

#include "cedar/storage/cedar_config.h"

#include <fstream>
#include <iostream>
#include <thread>

namespace cedar {

// ============================================================================
// CedarConfig 静态方法
// ============================================================================

CedarConfig CedarConfig::Default() {
  return CedarConfig();
}

CedarConfig CedarConfig::ForWorkload(WorkloadType type) {
  switch (type) {
    case WorkloadType::kReadHeavy:
      return ForReadHeavy();
    case WorkloadType::kWriteHeavy:
      return ForWriteHeavy();
    case WorkloadType::kHighConcurrency:
      return ForHighConcurrency();
    case WorkloadType::kLowLatency:
      return ForLowLatency();
    case WorkloadType::kHighThroughput:
      return ForHighThroughput();
    case WorkloadType::kMemoryConstrained:
      return ForMemoryConstrained();
    default:
      return Default();
  }
}

CedarConfig CedarConfig::ForReadHeavy() {
  CedarConfig config;
  
  // 大缓存
  config.cache.block_cache_size = 1024 * 1024 * 1024;  // 1GB
  config.cache.enable_version_chain_cache = true;
  config.cache.version_chain_cache_size = 100000;
  
  // 启用所有索引优化
  config.mvcc.enable_version_chain_index = true;
  config.mvcc.version_chain_index_threshold = 50;  // 更早构建索引
  config.mvcc.enable_temporal_bloom_filter = true;
  config.mvcc.temporal_filter_false_positive_rate = 0.005;  // 更低假阳性
  
  // 更大的 MemTable 减少刷盘
  config.db.memtable_threshold = 64 * 1024 * 1024;  // 64MB
  config.db.write_buffer_size = 64 * 1024 * 1024;
  
  // 布隆过滤器
  config.db.enable_bloom_filter = true;
  config.db.bloom_bits_per_key = 16;  // 更多位数
  
  return config;
}

CedarConfig CedarConfig::ForBalanced() {
  CedarConfig config;
  
  // 使用默认配置，但做一些平衡调整
  // 中等大小的 MemTable
  config.db.memtable_threshold = 32 * 1024 * 1024;   // 32MB
  config.db.write_buffer_size = 32 * 1024 * 1024;
  
  // 启用主要优化，但不过度
  config.mvcc.enable_sharded_timestamp_allocator = true;
  config.mvcc.enable_version_chain_index = true;
  config.mvcc.version_chain_index_threshold = 100;
  
  // Phase 3 优化选择性启用
  unsigned int cores = std::thread::hardware_concurrency();
  if (cores >= 16) {
    config.mvcc.enable_sharded_wal = true;
    config.mvcc.enable_lockfree_memtable = true;
    config.mvcc.enable_async_index_builder = true;
    config.wal.num_shards = cores;
  } else {
    config.mvcc.enable_sharded_wal = false;
    config.mvcc.enable_lockfree_memtable = false;
    config.mvcc.enable_async_index_builder = false;
  }
  
  // 平衡的缓存
  config.cache.block_cache_size = 64 * 1024 * 1024;  // 64MB
  
  return config;
}

CedarConfig CedarConfig::ForWriteHeavy() {
  CedarConfig config;
  
  // 大 MemTable 和写缓冲
  config.db.memtable_threshold = 128 * 1024 * 1024;  // 128MB
  config.db.write_buffer_size = 128 * 1024 * 1024;
  
  // 基于文件数量的阈值触发压缩
  // 当 Level 0 有 16 个文件 (~72MB @ 4.5MB each) 就触发
  config.lsm.min_files_for_compaction = 16;
  config.lsm.min_size_for_compaction = 32 * 1024 * 1024;  // 32MB (较低，主要用文件数触发)
  config.lsm.level0_file_num_compaction_trigger = 16;
  
  // 目标文件 64MB
  config.lsm.target_file_size = 64 * 1024 * 1024;
  
  // WAL 优化
  config.wal.group_commit_max_batch = 5000;
  config.wal.group_commit_timeout_us = 5000;
  
  // 禁用读优化相关
  config.db.enable_bloom_filter = false;
  config.mvcc.enable_temporal_bloom_filter = false;
  
  return config;
}

CedarConfig CedarConfig::ForHighConcurrency() {
  CedarConfig config;
  
  // 启用所有 Phase 3 优化
  config.mvcc.enable_deep_integration = true;
  config.mvcc.enable_sharded_timestamp_allocator = true;
  config.wal.enable_sharded_wal = true;
  config.memtable.enable_lockfree_memtable = true;
  config.mvcc.enable_async_index_builder = true;
  
  // 分片配置
  unsigned int cores = std::thread::hardware_concurrency();
  if (cores == 0) cores = 16;
  
  config.wal.num_shards = cores * 2;  // 2x CPU 核心
  config.mvcc.timestamp_shard_count = cores;
  config.mvcc.index_builder_worker_threads = cores / 2;
  
  // 减少锁竞争
  config.memtable.initial_capacity = 4 * 1024 * 1024;  // 4M entries
  config.memtable.enable_preallocation = true;
  
  // 事务优化
  config.transaction.parallel_validation = true;
  config.transaction.validation_threads = cores;
  
  return config;
}

CedarConfig CedarConfig::ForLowLatency() {
  CedarConfig config;
  
  // 小缓冲区减少延迟
  config.db.memtable_threshold = 1 * 1024 * 1024;  // 1MB
  config.db.write_buffer_size = 1 * 1024 * 1024;
  
  // 小批次快速提交
  config.wal.group_commit_max_batch = 10;
  config.wal.group_commit_timeout_us = 100;  // 100us
  
  // 异步索引构建避免阻塞
  config.mvcc.enable_async_index_builder = true;
  config.mvcc.index_builder_batch_timeout_ms = 1;  // 1ms
  
  // 立即刷盘
  config.wal.use_fsync = true;
  
  // 禁用可能增加延迟的功能
  config.mvcc.enable_delta_encoding = false;
  
  return config;
}

CedarConfig CedarConfig::ForHighThroughput() {
  CedarConfig config;
  
  // 大缓冲区
  config.db.memtable_threshold = 256 * 1024 * 1024;  // 256MB
  config.db.write_buffer_size = 256 * 1024 * 1024;
  
  // 大批量提交
  config.wal.group_commit_max_batch = 10000;
  config.wal.group_commit_timeout_us = 10000;  // 10ms
  
  // 高并发优化
  config.mvcc.enable_sharded_timestamp_allocator = true;
  config.mvcc.timestamp_batch_size = 10000;  // 大批量分配
  
  // 大 SST 文件减少文件数量
  config.lsm.target_file_size = 1024 * 1024 * 1024;  // 1GB
  
  // 延迟压缩
  config.lsm.level0_slowdown_writes_trigger = 100;
  config.lsm.level0_stop_writes_trigger = 200;
  
  return config;
}

CedarConfig CedarConfig::ForMemoryConstrained() {
  CedarConfig config;
  
  // 小内存占用
  config.db.memtable_threshold = 512 * 1024;  // 512KB
  config.db.write_buffer_size = 512 * 1024;
  
  // 小缓存
  config.cache.block_cache_size = 1 * 1024 * 1024;  // 1MB
  config.cache.enable_version_chain_cache = false;
  
  // 禁用内存密集型功能
  config.mvcc.enable_version_chain_index = false;
  config.mvcc.enable_async_index_builder = false;
  config.mvcc.enable_temporal_bloom_filter = false;
  config.db.enable_bloom_filter = false;
  
  // 小 SST 文件
  config.lsm.target_file_size = 16 * 1024 * 1024;  // 16MB
  
  // 积极压缩
  config.lsm.min_files_for_compaction = 2;
  config.lsm.level0_file_num_compaction_trigger = 2;
  
  return config;
}

// ============================================================================
// 转换为旧版 Options
// ============================================================================

CedarOptions CedarConfig::ToCedarOptions() const {
  CedarOptions options;
  
  options.create_if_missing = db.create_if_missing;
  options.error_if_exists = db.error_if_exists;
  options.paranoid_checks = db.paranoid_checks;
  options.memtable_threshold = db.memtable_threshold;
  options.write_buffer_size = db.write_buffer_size;
  options.column_id = db.column_id;
  options.enable_bloom_filter = db.enable_bloom_filter;
  options.bloom_bits_per_key = db.bloom_bits_per_key;
  options.verify_checksums = db.verify_checksums;
  
  // LSM 压缩配置
  options.compaction_config.min_files = lsm.min_files_for_compaction;
  options.compaction_config.min_size = lsm.min_size_for_compaction;
  options.compaction_config.target_file_size = lsm.target_file_size;
  options.compaction_config.max_levels = lsm.max_levels;
  
  return options;
}

MVCCOptimizationConfig CedarConfig::ToMVCCConfig() const {
  MVCCOptimizationConfig config;
  
  // Phase 1/2
  config.enable_sharded_timestamp_allocator = mvcc.enable_sharded_timestamp_allocator;
  config.timestamp_shard_count = mvcc.timestamp_shard_count;
  config.timestamp_batch_size = mvcc.timestamp_batch_size;
  
  config.enable_version_chain_index = mvcc.enable_version_chain_index;
  config.version_chain_index_threshold = mvcc.version_chain_index_threshold;
  config.version_chain_max_level = mvcc.version_chain_max_level;
  
  config.enable_delta_encoding = mvcc.enable_delta_encoding;
  config.delta_max_per_group = mvcc.delta_max_per_group;
  
  config.enable_temporal_bloom_filter = mvcc.enable_temporal_bloom_filter;
  config.temporal_filter_false_positive_rate = mvcc.temporal_filter_false_positive_rate;
  config.temporal_filter_hours_per_bucket = mvcc.temporal_filter_hours_per_bucket;
  
  // Phase 3
  config.enable_sharded_wal = wal.enable_sharded_wal;
  config.wal_shard_count = wal.num_shards;
  config.wal_batch_timeout_us = wal.batch_timeout_us;
  config.wal_batch_max_size = wal.batch_max_size;
  config.wal_enable_background_merger = wal.enable_background_merger;
  config.wal_merge_interval_ms = wal.merge_interval_ms;
  
  config.enable_lockfree_memtable = memtable.enable_lockfree_memtable;
  config.memtable_initial_capacity = memtable.initial_capacity;
  config.memtable_gc_interval_ms = memtable.gc_interval_ms;
  config.memtable_enable_preallocation = memtable.enable_preallocation;
  
  config.enable_async_index_builder = mvcc.enable_async_index_builder;
  config.index_builder_worker_threads = mvcc.index_builder_worker_threads;
  config.index_builder_max_concurrent = mvcc.index_builder_max_concurrent;
  config.index_builder_enable_batch = true;  // 默认启用
  config.index_builder_batch_size = mvcc.index_builder_batch_size;
  
  return config;
}

ShardedWalOptions CedarConfig::ToShardedWalOptions() const {
  ShardedWalOptions options;
  
  options.num_shards = wal.num_shards;
  if (options.num_shards == 0) {
    options.num_shards = std::thread::hardware_concurrency();
    if (options.num_shards == 0) options.num_shards = 16;
  }
  
  options.bind_by_thread_id = wal.bind_by_thread_id;
  options.max_file_size_per_shard = wal.max_file_size_per_shard;
  options.batch_timeout_us = wal.batch_timeout_us;
  options.batch_max_size = wal.batch_max_size;
  options.enable_background_merger = wal.enable_background_merger;
  options.merge_interval_ms = wal.merge_interval_ms;
  
  return options;
}

LockFreeMemTableOptions CedarConfig::ToLockFreeMemTableOptions() const {
  LockFreeMemTableOptions options;
  
  options.initial_capacity = memtable.initial_capacity;
  options.rehash_threshold = memtable.rehash_threshold;
  options.enable_version_chain_index = mvcc.enable_version_chain_index;
  options.index_build_threshold = mvcc.version_chain_index_threshold;
  options.gc_interval_ms = memtable.gc_interval_ms;
  options.gc_batch_size = memtable.gc_batch_size;
  options.enable_preallocation = memtable.enable_preallocation;
  options.preallocation_pool_size = memtable.preallocation_pool_size;
  
  return options;
}

AsyncIndexBuilderOptions CedarConfig::ToAsyncIndexBuilderOptions() const {
  AsyncIndexBuilderOptions options;
  
  options.num_worker_threads = mvcc.index_builder_worker_threads;
  options.build_threshold = mvcc.version_chain_index_threshold;
  options.max_concurrent_builds = mvcc.index_builder_max_concurrent;
  options.enable_batch_building = true;
  options.batch_max_size = mvcc.index_builder_batch_size;
  options.batch_timeout_ms = mvcc.index_builder_batch_timeout_ms;
  options.enable_build_cache = mvcc.enable_build_cache;
  options.build_cache_size = mvcc.build_cache_size;
  
  return options;
}

// ============================================================================
// 配置验证
// ============================================================================

Status CedarConfig::Validate() const {
  // 检查基本约束
  if (db.memtable_threshold < 1024) {
    return Status::InvalidArgument("memtable_threshold too small (< 1KB)");
  }
  
  if (db.write_buffer_size < 1024) {
    return Status::InvalidArgument("write_buffer_size too small (< 1KB)");
  }
  
  if (lsm.max_levels < 2 || lsm.max_levels > 10) {
    return Status::InvalidArgument("max_levels should be in [2, 10]");
  }
  
  if (wal.num_shards > 256) {
    return Status::InvalidArgument("wal.num_shards too large (> 256)");
  }
  
  if (mvcc.timestamp_shard_count > 256) {
    return Status::InvalidArgument("timestamp_shard_count too large (> 256)");
  }
  
  if (cache.block_cache_size < 1024) {
    return Status::InvalidArgument("block_cache_size too small (< 1KB)");
  }
  
  return Status::OK();
}

// ============================================================================
// 配置打印
// ============================================================================

void CedarConfig::Dump() const {
  std::cout << "\n╔══════════════════════════════════════════════════════════════════════════════╗\n";
  std::cout << "║                         Cedar Configuration                                   ║\n";
  std::cout << "╚══════════════════════════════════════════════════════════════════════════════╝\n";
  
  // DB
  std::cout << "\n[Database]\n";
  if (db.memtable_threshold >= 1024 * 1024) {
    std::cout << "  memtable_threshold: " << db.memtable_threshold / 1024 / 1024 << " MB\n";
  } else {
    std::cout << "  memtable_threshold: " << db.memtable_threshold / 1024 << " KB\n";
  }
  if (db.write_buffer_size >= 1024 * 1024) {
    std::cout << "  write_buffer_size: " << db.write_buffer_size / 1024 / 1024 << " MB\n";
  } else {
    std::cout << "  write_buffer_size: " << db.write_buffer_size / 1024 << " KB\n";
  }
  std::cout << "  enable_bloom_filter: " << (db.enable_bloom_filter ? "true" : "false") << "\n";
  
  // LSM
  std::cout << "\n[LSM Tree]\n";
  std::cout << "  max_levels: " << lsm.max_levels << "\n";
  std::cout << "  target_file_size: " << lsm.target_file_size / 1024 / 1024 << " MB\n";
  
  // WAL
  std::cout << "\n[WAL]\n";
  std::cout << "  enable_sharded_wal: " << (wal.enable_sharded_wal ? "true" : "false") << "\n";
  std::cout << "  num_shards: " << (wal.num_shards == 0 ? "auto" : std::to_string(wal.num_shards)) << "\n";
  
  // MVCC
  std::cout << "\n[MVCC Optimization]\n";
  std::cout << "  enable_sharded_timestamp_allocator: " << (mvcc.enable_sharded_timestamp_allocator ? "true" : "false") << "\n";
  std::cout << "  enable_version_chain_index: " << (mvcc.enable_version_chain_index ? "true" : "false") << "\n";
  std::cout << "  enable_sharded_wal: " << (wal.enable_sharded_wal ? "true" : "false") << "\n";
  std::cout << "  enable_lockfree_memtable: " << (memtable.enable_lockfree_memtable ? "true" : "false") << "\n";
  std::cout << "  enable_async_index_builder: " << (mvcc.enable_async_index_builder ? "true" : "false") << "\n";
  
  // Cache
  std::cout << "\n[Cache]\n";
  std::cout << "  block_cache_size: " << cache.block_cache_size / 1024 / 1024 << " MB\n";
  
  std::cout << "\n";
}

// ============================================================================
// 配置合并
// ============================================================================

void CedarConfig::MergeFrom(const CedarConfig& other) {
  // 简单实现：逐个字段覆盖非默认值
  // 实际应用中可以更智能地合并
  (void)other;
  // TODO: 实现完整的合并逻辑
}

// ============================================================================
// 文件加载/保存（简化实现）
// ============================================================================

Status CedarConfig::LoadFromFile(const std::string& path) {
  // 这里可以实现 JSON/YAML 配置文件解析
  // 简化版本：直接返回不支持
  (void)path;
  return Status::NotSupported("Configuration file loading not yet implemented");
}

Status CedarConfig::SaveToFile(const std::string& path) const {
  // 这里可以实现 JSON/YAML 配置文件保存
  (void)path;
  return Status::NotSupported("Configuration file saving not yet implemented");
}

// ============================================================================
// CedarConfigManager 单例实现
// ============================================================================

CedarConfigManager* CedarConfigManager::Instance() {
  static CedarConfigManager instance;
  return &instance;
}

Status CedarConfigManager::LoadConfig(const std::string& path) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  Status s = config_.LoadFromFile(path);
  if (!s.ok()) return s;
  
  config_path_ = path;
  
  // 验证配置
  s = config_.Validate();
  if (!s.ok()) return s;
  
  // 通知回调
  for (const auto& callback : callbacks_) {
    callback(config_);
  }
  
  return Status::OK();
}

Status CedarConfigManager::LoadDefaultConfig() {
  std::lock_guard<std::mutex> lock(mutex_);
  config_ = CedarConfig::Default();
  return Status::OK();
}

Status CedarConfigManager::ReloadConfig() {
  if (config_path_.empty()) {
    return Status::InvalidArgument("No config file loaded");
  }
  return LoadConfig(config_path_);
}

void CedarConfigManager::RegisterCallback(ConfigChangeCallback callback) {
  std::lock_guard<std::mutex> lock(mutex_);
  callbacks_.push_back(callback);
}

Status CedarConfigManager::ApplyToEngine(class LsmEngine* engine) {
  // 应用配置到引擎
  (void)engine;
  // TODO: 实现配置应用逻辑
  return Status::OK();
}

}  // namespace cedar
