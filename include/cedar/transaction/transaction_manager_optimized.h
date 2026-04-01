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

#ifndef FERN_TRANSACTION_MANAGER_OPTIMIZED_H_
#define FERN_TRANSACTION_MANAGER_OPTIMIZED_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "cedar/types/cedar_key.h"
#include "cedar/transaction/occ_transaction.h"
#include "cedar/transaction/sharded_timestamp_allocator.h"

namespace cedar {

// ============================================================================
// OptimizedTransactionManager - 优化的全局事务管理器
// ============================================================================
//
// 设计目标:
//   - 使用 ShardedTimestampAllocator 消除高并发下的时间戳分配瓶颈
//   - 保持与原有 TransactionManager 完全兼容的 API
//   - 提供配置选项在原始分配器和分片分配器之间切换
//   - 添加详细的性能统计用于监控和调优
//
// 架构:
//   +-----------------------------+
//   | OptimizedTransactionManager |
//   +-----------------------------+
//   |  - Transaction ID 分配器     |  <-- 原子计数器 (事务 ID 分配不频繁)
//   |  - Timestamp 分配器          |  <-- ShardedTimestampAllocator
//   |  - 活跃事务表                |  <-- 用于冲突检测和 GC
//   |  - 性能统计                  |  <-- 快速/慢速路径计数
//   +-----------------------------+
//
// 时间戳格式 (ShardedTimestampAllocator):
//   [63-32] Epoch (32位)       - 全局纪元
//   [31-16] ShardID (16位)     - 分片标识
//   [15-0]  LocalCounter (16位) - 分片内计数器
//
// 向后兼容:
//   通过 use_sharded_allocator_ 配置项，可以选择使用原始的原子计数器
//   或新的分片分配器。默认启用分片分配器以获得更好的并发性能。
//
// 使用示例:
//   // 默认配置：使用分片分配器
//   OptimizedTransactionManager manager;
//
//   // 自定义配置：64 个分片，每批预分配 2000 个时间戳
//   OptimizedTransactionManager manager(64, 2000);
//
//   // 向后兼容模式：使用原始原子分配器
//   OptimizedTransactionManager manager(0, 0, false);
//
class OptimizedTransactionManager {
 public:
  // ============================================================================
  // 性能统计扩展 - 包含分片分配器特有的统计信息
  // ============================================================================
  struct OptimizedStats : public TransactionStats {
    // 快速路径计数 (无原子操作，直接从线程本地缓存分配)
    std::atomic<uint64_t> fast_path_count{0};
    
    // 慢速路径计数 (需要从全局批量申请)
    std::atomic<uint64_t> slow_path_count{0};
    
    // 使用原始原子分配器的次数 (向后兼容模式)
    std::atomic<uint64_t> legacy_alloc_count{0};
    
    // 当前活跃分片数量
    std::atomic<uint32_t> active_shard_count{0};
    
    // 当前 Epoch
    std::atomic<uint32_t> current_epoch{0};
    
    OptimizedStats() = default;
    
    OptimizedStats(const OptimizedStats& other)
        : TransactionStats(other),
          fast_path_count(other.fast_path_count.load(std::memory_order_relaxed)),
          slow_path_count(other.slow_path_count.load(std::memory_order_relaxed)),
          legacy_alloc_count(other.legacy_alloc_count.load(std::memory_order_relaxed)),
          active_shard_count(other.active_shard_count.load(std::memory_order_relaxed)),
          current_epoch(other.current_epoch.load(std::memory_order_relaxed)) {}
    
    OptimizedStats& operator=(const OptimizedStats& other) {
      if (this != &other) {
        TransactionStats::operator=(other);
        fast_path_count.store(other.fast_path_count.load(std::memory_order_relaxed), 
                              std::memory_order_relaxed);
        slow_path_count.store(other.slow_path_count.load(std::memory_order_relaxed), 
                              std::memory_order_relaxed);
        legacy_alloc_count.store(other.legacy_alloc_count.load(std::memory_order_relaxed), 
                                 std::memory_order_relaxed);
        active_shard_count.store(other.active_shard_count.load(std::memory_order_relaxed), 
                                 std::memory_order_relaxed);
        current_epoch.store(other.current_epoch.load(std::memory_order_relaxed), 
                           std::memory_order_relaxed);
      }
      return *this;
    }
    
    // 计算快速路径比例 (用于性能监控)
    double GetFastPathRatio() const {
      uint64_t fast = fast_path_count.load(std::memory_order_relaxed);
      uint64_t slow = slow_path_count.load(std::memory_order_relaxed);
      uint64_t total = fast + slow;
      return total > 0 ? static_cast<double>(fast) / total : 0.0;
    }
  };

  // ============================================================================
  // 构造函数 / 析构函数
  // ============================================================================
  
  // 默认构造函数 - 启用分片分配器，使用默认配置
  // 默认使用 64 个分片，每批预分配 1000 个时间戳
  OptimizedTransactionManager()
      : OptimizedTransactionManager(64, ShardedTimestampAllocator::kDefaultBatchSize, true) {}
  
  // 完整配置构造函数
  //
  // @param num_shards: 分片数量，必须是 2 的幂次 (如 16, 32, 64, 128)
  //                    如果 use_sharded 为 false，此参数被忽略
  // @param batch_size: 批量预分配大小，影响慢速路径触发频率
  //                    较大的值减少全局原子操作，但增加时间戳间隔
  // @param use_sharded: 是否启用分片分配器
  //                     true  - 使用 ShardedTimestampAllocator (高并发优化)
  //                     false - 使用原始原子计数器 (向后兼容)
  //
  // 性能调优建议:
  //   - num_shards: 通常设置为 CPU 核心数的 2-4 倍
  //   - batch_size: 高并发场景设置较大 (如 1000-5000)，低并发设置较小 (如 100-500)
  explicit OptimizedTransactionManager(uint32_t num_shards,
                                       uint32_t batch_size,
                                       bool use_sharded = true)
      : next_txn_id_(1),
        next_timestamp_(1),
        use_sharded_allocator_(use_sharded),
        sharded_allocator_(use_sharded 
                           ? std::make_unique<ShardedTimestampAllocator>(num_shards, batch_size)
                           : nullptr) {}
  
  ~OptimizedTransactionManager() = default;
  
  // 禁止拷贝和移动
  OptimizedTransactionManager(const OptimizedTransactionManager&) = delete;
  OptimizedTransactionManager& operator=(const OptimizedTransactionManager&) = delete;
  OptimizedTransactionManager(OptimizedTransactionManager&&) = delete;
  OptimizedTransactionManager& operator=(OptimizedTransactionManager&&) = delete;

  // ============================================================================
  // 核心 API - 与原有 TransactionManager 完全兼容
  // ============================================================================
  
  // 分配新的事务 ID
  //
  // 事务 ID 使用简单的原子计数器，因为:
  //   1. 事务 ID 分配频率远低于时间戳分配
  //   2. 事务 ID 需要全局单调递增
  //   3. 不需要复杂的分片逻辑
  //
  // 线程安全: 是 (原子操作)
  uint64_t AllocateTransactionId() {
    return next_txn_id_.fetch_add(1, std::memory_order_relaxed);
  }
  
  // 分配单个时间戳
  //
  // 根据配置选择分配策略:
  //   - 分片模式: 使用 ShardedTimestampAllocator 的快速/慢速路径
  //   - 兼容模式: 使用全局原子计数器
  //
  // 线程安全: 是
  Timestamp AllocateTimestamp() {
    if (use_sharded_allocator_ && sharded_allocator_) {
      // 使用分片分配器
      Timestamp ts = sharded_allocator_->Allocate();
      
      // 更新统计 (简单启发式: 如果 LocalCounter 较小，认为是快速路径)
      UpdateAllocationStats(ts);
      
      return ts;
    } else {
      // 向后兼容: 使用原始原子计数器
      stats_.legacy_alloc_count.fetch_add(1, std::memory_order_relaxed);
      return Timestamp(next_timestamp_.fetch_add(1, std::memory_order_relaxed));
    }
  }
  
  // 批量预分配时间戳
  //
  // @param count: 需要分配的时间戳数量
  // @return: 批次中的第一个时间戳 (调用者需要自行维护偏移量)
  //
  // 注意: 返回的是批次起始时间戳，实际批量分配通过 AllocateTimestampBatchVector
  //       如果只需要单个起始值，可以使用此接口保持 API 兼容
  //
  // 线程安全: 是
  Timestamp AllocateTimestampBatch(uint32_t count) {
    if (use_sharded_allocator_ && sharded_allocator_) {
      // 分片分配器直接支持批量分配，返回第一个时间戳
      auto batch = sharded_allocator_->AllocateBatch(count);
      if (!batch.empty()) {
        // 批量分配都走慢速路径
        stats_.slow_path_count.fetch_add(1, std::memory_order_relaxed);
        return batch[0];
      }
      return Timestamp(0);  // 不应该发生
    } else {
      // 向后兼容: 原子增加 count
      stats_.legacy_alloc_count.fetch_add(1, std::memory_order_relaxed);
      return Timestamp(next_timestamp_.fetch_add(count, std::memory_order_relaxed));
    }
  }
  
  // 批量分配时间戳 - 返回完整向量 (扩展 API)
  //
  // @param count: 需要分配的时间戳数量
  // @return: 包含所有分配时间戳的向量
  //
  // 此接口比 AllocateTimestampBatch 更高效，因为:
  //   1. 避免调用者自行计算偏移
  //   2. 分片分配器可以直接返回预分配的批次
  //
  // 线程安全: 是
  std::vector<Timestamp> AllocateTimestampBatchVector(size_t count) {
    if (use_sharded_allocator_ && sharded_allocator_) {
      auto batch = sharded_allocator_->AllocateBatch(count);
      if (!batch.empty()) {
        stats_.slow_path_count.fetch_add(1, std::memory_order_relaxed);
      }
      return batch;
    } else {
      // 向后兼容: 逐个分配
      std::vector<Timestamp> result;
      result.reserve(count);
      uint64_t base = next_timestamp_.fetch_add(count, std::memory_order_relaxed);
      for (size_t i = 0; i < count; ++i) {
        result.emplace_back(base + i);
      }
      stats_.legacy_alloc_count.fetch_add(1, std::memory_order_relaxed);
      return result;
    }
  }
  
  // 注册活跃事务
  //
  // 事务开始时调用，将事务 ID 和开始时间戳注册到活跃事务表
  // 活跃事务表用于冲突检测和垃圾回收
  //
  // @param txn_id: 事务 ID (由 AllocateTransactionId 分配)
  // @param start_ts: 事务开始时间戳 (由 AllocateTimestamp 分配)
  //
  // 线程安全: 是 (内部加锁)
  void RegisterActiveTransaction(uint64_t txn_id, Timestamp start_ts) {
    std::lock_guard<std::mutex> lock(active_txns_mutex_);
    active_txns_[txn_id] = start_ts;
    stats_.txn_started.fetch_add(1, std::memory_order_relaxed);
  }
  
  // 注销活跃事务
  //
  // 事务结束(提交或回滚)时调用，从活跃事务表移除
  //
  // @param txn_id: 事务 ID
  //
  // 线程安全: 是 (内部加锁)
  void UnregisterActiveTransaction(uint64_t txn_id) {
    std::lock_guard<std::mutex> lock(active_txns_mutex_);
    auto it = active_txns_.find(txn_id);
    if (it != active_txns_.end()) {
      active_txns_.erase(it);
    }
  }
  
  // 获取活跃事务列表
  //
  // @return: 包含 (txn_id, start_timestamp) 的向量
  //
  // 用途:
  //   - 冲突检测: 获取所有可能冲突的活跃事务
  //   - 监控: 统计当前活跃事务数量
  //   - 调试: 输出当前事务状态
  //
  // 线程安全: 是 (内部加锁，返回副本)
  std::vector<std::pair<uint64_t, Timestamp>> GetActiveTransactions() const {
    std::lock_guard<std::mutex> lock(active_txns_mutex_);
    std::vector<std::pair<uint64_t, Timestamp>> result;
    result.reserve(active_txns_.size());
    for (const auto& [txn_id, ts] : active_txns_) {
      result.emplace_back(txn_id, ts);
    }
    return result;
  }
  
  // 检查是否有活跃事务在指定时间戳之前开始
  //
  // @param ts: 参考时间戳
  // @return: 如果有活跃事务的 start_ts < ts，返回 true
  //
  // 用途:
  //   - 垃圾回收: 判断是否可以清理某个版本之前的数据
  //   - 冲突检测: 快速检查潜在冲突
  //
  // 线程安全: 是
  bool HasActiveTransactionBefore(Timestamp ts) const {
    std::lock_guard<std::mutex> lock(active_txns_mutex_);
    for (const auto& [_, start_ts] : active_txns_) {
      if (start_ts < ts) {
        return true;
      }
    }
    return false;
  }
  
  // 获取最小活跃时间戳
  //
  // @return: 所有活跃事务中最小的 start_timestamp
  //          如果没有活跃事务，返回 Timestamp::Max()
  //
  // 用途:
  //   - 垃圾回收: 确定可以清理的最旧版本
  //   - 快照管理: 确定全局最小可见版本
  //
  // 注意: 对于分片时间戳，最小值的计算基于完整的 64 位比较
  //       由于 Epoch 在高 32 位，旧 Epoch 的时间戳总是小于新 Epoch
  //
  // 线程安全: 是
  Timestamp GetMinActiveTimestamp() const {
    std::lock_guard<std::mutex> lock(active_txns_mutex_);
    if (active_txns_.empty()) {
      return Timestamp::Max();
    }
    
    Timestamp min_ts = Timestamp::Max();
    for (const auto& [_, start_ts] : active_txns_) {
      if (start_ts < min_ts) {
        min_ts = start_ts;
      }
    }
    return min_ts;
  }
  
  // 获取基本统计信息 (兼容原有接口)
  //
  // 返回 TransactionStats 基类部分，用于与原有代码兼容
  TransactionStats GetStats() const {
    // 返回基类副本
    return static_cast<const TransactionStats&>(stats_);
  }
  
  // 获取完整统计信息 (扩展接口)
  //
  // 返回 OptimizedStats，包含分片分配器特有的统计
  OptimizedStats GetOptimizedStats() const {
    // 更新动态统计
    OptimizedStats result = stats_;
    if (sharded_allocator_) {
      result.active_shard_count.store(
          sharded_allocator_->GetShardCount(),
          std::memory_order_relaxed);
      result.current_epoch.store(
          sharded_allocator_->GetCurrentEpoch(),
          std::memory_order_relaxed);
    }
    return result;
  }
  
  // 获取可变统计引用 (用于内部更新)
  TransactionStats& mutable_stats() { return stats_; }
  
  // ============================================================================
  // 配置查询 API
  // ============================================================================
  
  // 是否使用分片分配器
  bool IsShardedAllocatorEnabled() const {
    return use_sharded_allocator_;
  }
  
  // 获取分片数量 (仅在分片模式下有效)
  uint32_t GetShardCount() const {
    return sharded_allocator_ ? sharded_allocator_->GetShardCount() : 0;
  }
  
  // 获取当前 Epoch (仅在分片模式下有效)
  uint32_t GetCurrentEpoch() const {
    return sharded_allocator_ ? sharded_allocator_->GetCurrentEpoch() : 0;
  }
  
  // ============================================================================
  // 调试和监控 API
  // ============================================================================
  
  // 获取当前活跃事务数量
  size_t GetActiveTransactionCount() const {
    std::lock_guard<std::mutex> lock(active_txns_mutex_);
    return active_txns_.size();
  }
  
  // 检查指定事务是否活跃
  bool IsTransactionActive(uint64_t txn_id) const {
    std::lock_guard<std::mutex> lock(active_txns_mutex_);
    return active_txns_.find(txn_id) != active_txns_.end();
  }
  
  // 获取指定事务的开始时间戳
  // @return: 如果事务活跃返回时间戳，否则返回 Timestamp::Null()
  Timestamp GetTransactionStartTimestamp(uint64_t txn_id) const {
    std::lock_guard<std::mutex> lock(active_txns_mutex_);
    auto it = active_txns_.find(txn_id);
    if (it != active_txns_.end()) {
      return it->second;
    }
    return Timestamp::Null();
  }

 private:
  // ============================================================================
  // 内部辅助方法
  // ============================================================================
  
  // 更新分配统计
  //
  // 启发式判断当前分配走的是快速路径还是慢速路径:
  //   - 快速路径: LocalCounter 较小 (< 100)，说明是批次内分配
  //   - 慢速路径: LocalCounter 较大，说明刚申请了新批次
  //
  // 注意: 这是一个启发式判断，不是 100% 准确，但足以用于性能监控
  void UpdateAllocationStats(Timestamp ts) {
    if (!sharded_allocator_) return;
    
    uint32_t local_counter = ShardedTimestampAllocator::ExtractLocalCounter(ts);
    // 如果 local_counter 在批次的前 100 个中，认为是快速路径
    // 否则可能是刚申请的新批次，认为是慢速路径
    if (local_counter < 100 && local_counter > 0) {
      stats_.fast_path_count.fetch_add(1, std::memory_order_relaxed);
    } else {
      stats_.slow_path_count.fetch_add(1, std::memory_order_relaxed);
    }
  }

 private:
  // ============================================================================
  // 成员变量
  // ============================================================================
  
  // 事务 ID 生成器 - 使用简单的原子计数器
  // 事务 ID 分配不频繁，不需要分片优化
  alignas(64) std::atomic<uint64_t> next_txn_id_;
  
  // 向后兼容: 原始时间戳生成器 (仅在非分片模式下使用)
  alignas(64) std::atomic<uint64_t> next_timestamp_;
  
  // 活跃事务表 (txn_id -> start_timestamp)
  // 需要保护活跃事务表的并发访问
  mutable std::mutex active_txns_mutex_;
  std::unordered_map<uint64_t, Timestamp> active_txns_;
  
  // 扩展的统计信息
  mutable OptimizedStats stats_;
  
  // 配置: 是否使用分片分配器
  const bool use_sharded_allocator_;
  
  // 分片时间戳分配器 (可选，根据配置决定)
  // 使用 unique_ptr 允许在构造时决定是否创建
  std::unique_ptr<ShardedTimestampAllocator> sharded_allocator_;
};

// ============================================================================
// 类型别名 - 便于代码迁移
// ============================================================================

// 为了保持向后兼容，可以提供类型别名
// 新代码可以直接使用 OptimizedTransactionManager
// using TransactionManager = OptimizedTransactionManager;

}  // namespace cedar

#endif  // FERN_TRANSACTION_MANAGER_OPTIMIZED_H_
