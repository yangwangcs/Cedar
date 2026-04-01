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

// Cedar 统一配置中心
//
// 本文件提供集中式的参数管理，所有 LSM/MVCC/WAL/Compaction 参数都可以在这里配置
// 支持从配置文件加载，支持配置验证，支持预设配置模板

#ifndef FERN_FERN_CONFIG_H_
#define FERN_FERN_CONFIG_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "cedar/storage/cedar_options.h"
#include "cedar/storage/sharded_wal.h"
#include "cedar/storage/lockfree_memtable_full.h"
#include "cedar/storage/async_index_builder.h"
#include "cedar/core/env.h"

namespace cedar {


// 临时占位类型定义
struct ShardedWalOptions {
  bool enable = true;
  uint32_t shard_count = 16;
};

struct LockFreeMemTableOptions {
  size_t initial_capacity = 1000000;
  bool enable_async_index = true;
};

// ============================================================================
// 场景预设配置类型
// ============================================================================
enum class WorkloadType {
  kUnknown = 0,
  kReadHeavy,       // 读密集型 (80%+ 读)
  kWriteHeavy,      // 写密集型 (80%+ 写)
  kBalanced,        // 均衡型 (50/50)
  kHighConcurrency, // 高并发 (32+ 线程)
  kLowLatency,      // 低延迟优先
  kHighThroughput,  // 高吞吐优先
  kMemoryConstrained, // 内存受限
};

// ============================================================================
// 统一配置结构 - 包含所有可调参数
// ============================================================================
struct CedarConfig {
  // 版本号
  static constexpr uint32_t kVersion = 1;
  
  // ========================================================================
  // 1. 基础数据库参数 (原 CedarOptions)
  // ========================================================================
  struct {
    bool create_if_missing = false;           // 数据库不存在时创建
    bool error_if_exists = false;             // 已存在时报错
    bool paranoid_checks = false;             // 严格检查模式
    
    size_t memtable_threshold = 4 * 1024 * 1024;     // MemTable 阈值 (4MB)
    size_t write_buffer_size = 4 * 1024 * 1024;      // 写缓冲区 (4MB)
    uint16_t column_id = 0;                          // 列族 ID
    
    bool enable_bloom_filter = true;          // 启用布隆过滤器
    int bloom_bits_per_key = 10;              // 每键位数
    bool verify_checksums = true;             // 验证校验和
  } db;
  
  // ========================================================================
  // 2. LSM 树参数
  // ========================================================================
  struct {
    size_t min_files_for_compaction = 4;              // 触发压缩的最小文件数
    size_t min_size_for_compaction = 64 * 1024 * 1024; // 触发压缩的最小大小 (64MB)
    size_t target_file_size = 256 * 1024 * 1024;       // 目标 Frond 文件大小 (256MB)
    int max_levels = 7;                                // 最大层数
    
    double level_size_multiplier = 10.0;      // 每层大小倍数
    size_t level0_file_num_compaction_trigger = 4;  // L0 触发压缩的文件数
    size_t level0_slowdown_writes_trigger = 20;     // L0 减速写入阈值
    size_t level0_stop_writes_trigger = 36;         // L0 停止写入阈值
  } lsm;
  
  // ========================================================================
  // 3. WAL 参数
  // ========================================================================
  struct {
    // 基础 WAL
    size_t max_file_size = 64 * 1024 * 1024;           // 最大文件大小 (64MB)
    uint32_t group_commit_timeout_us = 1000;           // 组提交超时 (1ms)
    size_t group_commit_max_batch = 1000;              // 组提交批次
    bool use_fsync = false;                            // 使用 fsync
    size_t preallocate_size = 64 * 1024 * 1024;        // 预分配大小 (64MB)
    
    // 分片 WAL (Phase 3 高并发优化)
    bool enable_sharded_wal = true;                    // 启用分片 WAL
    uint32_t num_shards = 0;                           // 分片数 (0=auto=CPU核心数)
    bool bind_by_thread_id = true;                     // 按线程ID绑定
    size_t max_file_size_per_shard = 16 * 1024 * 1024; // 单分片大小 (16MB)
    uint32_t batch_timeout_us = 100;                   // 批量写入超时
    size_t batch_max_size = 100;                       // 批量写入大小
    bool enable_background_merger = true;              // 启用后台合并
    uint32_t merge_interval_ms = 100;                  // 合并间隔
  } wal;
  
  // ========================================================================
  // 4. MemTable 参数
  // ========================================================================
  struct {
    // 基础 MemTable
    enum MemTableType { kLockFree, kMutexBased };
    MemTableType type = kLockFree;                     // MemTable 类型
    
    // LockFree MemTable (Phase 3)
    bool enable_lockfree_memtable = true;              // 启用无锁 MemTable
    size_t initial_capacity = 1024 * 1024;             // 初始容量 (1M entries)
    double rehash_threshold = 0.75;                    // 重新哈希阈值
    bool enable_preallocation = true;                  // 启用预分配
    size_t preallocation_pool_size = 10000;            // 预分配池大小
    
    // GC 配置
    uint32_t gc_interval_ms = 1000;                    // GC 间隔
    size_t gc_batch_size = 1000;                       // GC 批次大小
  } memtable;
  
  // ========================================================================
  // 5. MVCC 优化参数 (Phase 1/2/3)
  // ========================================================================
  struct {
    // Phase 1/2: 时间戳分配器
    bool enable_sharded_timestamp_allocator = true;    // 分片时间戳分配器
    uint32_t timestamp_shard_count = 0;                // 分片数 (0=auto)
    uint32_t timestamp_batch_size = 1000;              // 批量预分配大小
    
    // Phase 1/2: 版本链索引
    bool enable_version_chain_index = true;            // 版本链跳表索引
    size_t version_chain_index_threshold = 100;        // 自动构建索引阈值
    int version_chain_max_level = 4;                   // 最大索引层数
    
    // Phase 1/2: Delta 编码
    bool enable_delta_encoding = false;                // Delta 编码 (默认关闭)
    size_t delta_max_per_group = 16;                   // 每组最大 delta 数
    
    // Phase 1/2: 时态布隆过滤器
    bool enable_temporal_bloom_filter = true;          // 时态布隆过滤器
    double temporal_filter_false_positive_rate = 0.01; // 假阳性率 (1%)
    uint32_t temporal_filter_hours_per_bucket = 24;    // 时间桶粒度 (小时)
    
    // Phase 3: 高并发优化开关
    bool enable_sharded_wal = true;                    // 启用分片 WAL
    bool enable_lockfree_memtable = true;              // 启用无锁 MemTable  
    bool enable_async_index_builder = true;            // 异步索引构建器
    uint32_t index_builder_worker_threads = 4;         // 工作线程数
    uint32_t index_builder_max_concurrent = 16;        // 最大并发构建数
    size_t index_builder_batch_size = 10;              // 批量构建大小
    uint32_t index_builder_batch_timeout_ms = 10;      // 批量超时
    bool enable_build_cache = true;                    // 构建缓存
    size_t build_cache_size = 1000;                    // 缓存大小
    
    // 深度集成开关
    bool enable_deep_integration = true;               // 启用深度集成
  } mvcc;
  
  // ========================================================================
  // 6. 事务参数
  // ========================================================================
  struct {
    bool enable_transaction = true;                    // 启用事务
    int default_isolation_level = 2;                   // 默认隔离级别 (2=Snapshot)
    uint64_t timeout_ms = 30000;                       // 超时 (30秒)
    uint32_t max_retries = 3;                          // 最大重试次数
    bool parallel_validation = true;                   // 并行验证
    uint32_t validation_threads = 4;                   // 验证线程数
    
    // OCC 特定配置
    bool enable_occ = true;                            // 启用 OCC
    size_t max_write_set_size = 10000;                 // 最大写集大小
    size_t max_read_set_size = 100000;                 // 最大读集大小
  } transaction;
  
  // ========================================================================
  // 7. 缓存参数
  // ========================================================================
  struct {
    size_t block_cache_size = 8 * 1024 * 1024;         // 块缓存 (8MB)
    size_t table_cache_size = 1024;                    // 表缓存 (条目数)
    int block_restart_interval = 16;                   // 块重启间隔
    size_t block_size = 4 * 1024;                      // 块大小 (4KB)
    
    // 版本链缓存
    size_t version_chain_cache_size = 10000;           // 版本链缓存
    bool enable_version_chain_cache = true;            // 启用版本链缓存
  } cache;
  
  // ========================================================================
  // 8. 文件系统参数
  // ========================================================================
  struct {
    int max_open_files = 1000;                         // 最大打开文件数
    bool use_direct_io = false;                        // 使用 Direct I/O
    bool advise_random_access = true;                  // 建议随机访问
    size_t prefetch_buffer_size = 256 * 1024;          // 预读缓冲区 (256KB)
  } filesystem;
  
  // ========================================================================
  // 9. 调试与监控参数
  // ========================================================================
  struct {
    bool enable_stats = true;                          // 启用统计
    uint32_t stats_dump_interval_sec = 600;            // 统计输出间隔 (10分钟)
    bool enable_slow_log = true;                       // 启用慢查询日志
    uint32_t slow_log_threshold_ms = 100;              // 慢查询阈值 (100ms)
    bool enable_trace = false;                         // 启用追踪
  } debug;
  
  // ========================================================================
  // 便捷方法
  // ========================================================================
  
  // 创建默认配置
  static CedarConfig Default();
  
  // 创建场景预设配置
  static CedarConfig ForWorkload(WorkloadType type);
  static CedarConfig ForReadHeavy();       // 读密集型
  static CedarConfig ForWriteHeavy();      // 写密集型
  static CedarConfig ForBalanced();        // 均衡型
  static CedarConfig ForHighConcurrency(); // 高并发
  static CedarConfig ForLowLatency();      // 低延迟
  static CedarConfig ForHighThroughput();  // 高吞吐
  static CedarConfig ForMemoryConstrained(); // 内存受限
  
  // 转换为旧版 Options (向后兼容)
  CedarOptions ToCedarOptions() const;
  MVCCOptimizationConfig ToMVCCConfig() const;
  ShardedWalOptions ToShardedWalOptions() const;
  LockFreeMemTableOptions ToLockFreeMemTableOptions() const;
  AsyncIndexBuilderOptions ToAsyncIndexBuilderOptions() const;
  
  // 从文件加载配置
  Status LoadFromFile(const std::string& path);
  Status SaveToFile(const std::string& path) const;
  
  // 配置验证
  Status Validate() const;
  
  // 打印配置
  void Dump() const;
  
  // 合并配置（覆盖当前值）
  void MergeFrom(const CedarConfig& other);
};

// ============================================================================
// 配置管理器 - 支持配置文件热重载
// ============================================================================
class CedarConfigManager {
 public:
  // 获取全局配置管理器实例
  static CedarConfigManager* Instance();
  
  // 加载配置
  Status LoadConfig(const std::string& path);
  Status LoadDefaultConfig();
  
  // 获取当前配置
  const CedarConfig& GetConfig() const { return config_; }
  CedarConfig& GetMutableConfig() { return config_; }
  
  // 更新配置（热重载）
  Status ReloadConfig();
  
  // 注册配置变更回调
  using ConfigChangeCallback = std::function<void(const CedarConfig&)>;
  void RegisterCallback(ConfigChangeCallback callback);
  
  // 应用配置到引擎
  Status ApplyToEngine(class LsmEngine* engine);
  
 private:
  CedarConfigManager() = default;
  
  CedarConfig config_;
  std::string config_path_;
  std::vector<ConfigChangeCallback> callbacks_;
  mutable std::mutex mutex_;
};

// ============================================================================
// 配置宏（便于代码中使用）
// ============================================================================

// 获取配置参数（线程安全）
#define FERN_CONFIG(param) \
  (cedar::CedarConfigManager::Instance()->GetConfig().param)

// 快速访问常用参数
#define FERN_DB(param)      FERN_CONFIG(db.param)
#define FERN_LSM(param)     FERN_CONFIG(lsm.param)
#define FERN_WAL(param)     FERN_CONFIG(wal.param)
#define FERN_MEMTABLE(param) FERN_CONFIG(memtable.param)
#define FERN_MVCC(param)    FERN_CONFIG(mvcc.param)
#define FERN_TXN(param)     FERN_CONFIG(transaction.param)
#define FERN_CACHE(param)   FERN_CONFIG(cache.param)

}  // namespace cedar

#endif  // FERN_FERN_CONFIG_H_
