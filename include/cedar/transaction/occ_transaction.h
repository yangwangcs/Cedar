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

#ifndef FERN_OCC_TRANSACTION_H_
#define FERN_OCC_TRANSACTION_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cedar/types/cedar_key.h"
#include "cedar/core/slice.h"
#include "cedar/core/status.h"
#include "cedar/transaction/wal.h"
#include "cedar/transaction/sharded_timestamp_allocator.h"

namespace cedar {

class VSLMemTable;
class LsmEngine;

// 事务状态
enum class TransactionState : uint8_t {
  kActive = 0,       // 活跃状态
  kValidating = 1,   // 验证中
  kCommitting = 2,   // 提交中
  kCommitted = 3,    // 已提交
  kAborted = 4,      // 已回滚
};

// 事务隔离级别
enum class IsolationLevel : uint8_t {
  kReadUncommitted = 0,  // 读未提交
  kReadCommitted = 1,    // 读已提交
  kSnapshot = 2,         // 快照隔离 (默认)
  kSerializable = 3,     // 串行化
};

// 事务选项
struct TransactionOptions {
  IsolationLevel isolation = IsolationLevel::kSnapshot;
  uint64_t timeout_ms = 30000;  // 默认 30 秒超时
  uint32_t max_retries = 3;     // 最大重试次数
  bool parallel_validation = false;  // 是否启用并行验证
  uint32_t validation_threads = 4;   // 并行验证线程数
};

// 事务统计
struct TransactionStats {
  std::atomic<uint64_t> txn_started{0};
  std::atomic<uint64_t> txn_committed{0};
  std::atomic<uint64_t> txn_aborted{0};
  std::atomic<uint64_t> conflicts_detected{0};
  std::atomic<uint64_t> validation_failures{0};
  
  TransactionStats() = default;
  TransactionStats(const TransactionStats& other) {
    txn_started.store(other.txn_started.load(), std::memory_order_relaxed);
    txn_committed.store(other.txn_committed.load(), std::memory_order_relaxed);
    txn_aborted.store(other.txn_aborted.load(), std::memory_order_relaxed);
    conflicts_detected.store(other.conflicts_detected.load(), std::memory_order_relaxed);
    validation_failures.store(other.validation_failures.load(), std::memory_order_relaxed);
  }
  TransactionStats& operator=(const TransactionStats& other) {
    if (this != &other) {
      txn_started.store(other.txn_started.load(), std::memory_order_relaxed);
      txn_committed.store(other.txn_committed.load(), std::memory_order_relaxed);
      txn_aborted.store(other.txn_aborted.load(), std::memory_order_relaxed);
      conflicts_detected.store(other.conflicts_detected.load(), std::memory_order_relaxed);
      validation_failures.store(other.validation_failures.load(), std::memory_order_relaxed);
    }
    return *this;
  }
};

// 事务读集条目
struct ReadSetEntry {
  uint64_t entity_id;
  EntityType entity_type;
  uint16_t column_id;
  Timestamp read_timestamp;      // 读取时的业务时间戳
  Timestamp read_txn_version;    // 读取时的事务版本号（用于 OCC 验证）
  
  ReadSetEntry() = default;
  ReadSetEntry(uint64_t eid, EntityType type, uint16_t col, 
               Timestamp ts, Timestamp txn_ver)
      : entity_id(eid), entity_type(type), column_id(col),
        read_timestamp(ts), read_txn_version(txn_ver) {}
};

// 事务写集条目
struct WriteSetEntry {
  uint64_t entity_id;
  EntityType entity_type;
  uint16_t column_id;
  Descriptor descriptor;
  CedarKey key;  // 完整键 (用于 WAL)
  Timestamp user_timestamp;  // 用户指定的业务时间戳（用于时序数据和 Key）
  Timestamp txn_version;     // 事务版本号（用于 MVCC 版本控制）
  uint64_t target_id;        // 用于 Edge 存储对端节点 ID (dst for EdgeOut, src for EdgeIn)
  
  WriteSetEntry() = default;
  WriteSetEntry(uint64_t eid, EntityType type, uint16_t col, 
                const Descriptor& desc, const CedarKey& k,
                Timestamp business_ts = Timestamp(0),
                Timestamp txn_ver = Timestamp(0),
                uint64_t tgt = 0)
      : entity_id(eid), entity_type(type), column_id(col), 
        descriptor(desc), key(k), user_timestamp(business_ts), 
        txn_version(txn_ver), target_id(tgt) {}
};

// 事务冲突类型
enum class ConflictType : uint8_t {
  kNone = 0,
  kWriteWrite = 1,     // 写写冲突
  kReadWrite = 2,      // 读写冲突
  kWriteRead = 3,      // 写读冲突 (可重复读违反)
  kPhantomRead = 4,    // 幻读
};

// 冲突信息
struct ConflictInfo {
  ConflictType type;
  uint64_t entity_id;
  EntityType entity_type;
  uint16_t column_id;
  uint64_t conflicting_txn_id;  // 冲突的事务 ID
};

// 全局事务管理器 (用于分配事务 ID 和跟踪活跃事务)
class TransactionManager {
 public:
  TransactionManager();
  virtual ~TransactionManager() = default;
  
  // 禁止拷贝
  TransactionManager(const TransactionManager&) = delete;
  TransactionManager& operator=(const TransactionManager&) = delete;
  
  // 分配新的事务 ID
  uint64_t AllocateTransactionId();
  
  // 获取当前时间戳 (递增)
  Timestamp AllocateTimestamp();
  
  // 批量预分配时间戳 (减少原子操作竞争)
  Timestamp AllocateTimestampBatch(uint32_t count);
  
  // 注册活跃事务
  void RegisterActiveTransaction(uint64_t txn_id, Timestamp start_ts);
  
  // 注销活跃事务
  void UnregisterActiveTransaction(uint64_t txn_id);
  
  // 获取活跃事务列表
  std::vector<std::pair<uint64_t, Timestamp>> GetActiveTransactions() const;
  
  // 检查是否有活跃事务在指定时间戳之前开始
  bool HasActiveTransactionBefore(Timestamp ts) const;
  
  // 获取最小活跃时间戳 (用于垃圾回收)
  Timestamp GetMinActiveTimestamp() const;
  
  // 获取统计
  TransactionStats GetStats() const { return stats_; }
  TransactionStats& mutable_stats() { return stats_; }
  
 protected:
  // 事务 ID 生成器
  std::atomic<uint64_t> next_txn_id_{1};
  
  // 分片时间戳分配器 - 高并发优化，消除原子操作竞争
  // 使用 ShardedTimestampAllocator 替代简单的 std::atomic
  // 实测性能提升：单线程 2.4x，4 线程 25x
  ShardedTimestampAllocator timestamp_allocator_;
  
  // 活跃事务表 (txn_id -> start_timestamp)
  mutable std::mutex active_txns_mutex_;
  std::unordered_map<uint64_t, Timestamp> active_txns_;
  
  TransactionStats stats_;
};

// OCC 事务
// 
// 使用乐观并发控制，支持快照隔离
// 
// 生命周期:
//   1. Begin() - 开始事务，分配 txn_id 和 read_timestamp
//   2. Get/Put/Delete - 读写操作，记录到读集/写集
//   3. Commit() - 验证并提交
//     a. Validate() - 验证读集是否仍然有效
//     b. WriteToMemTable() - 将写集写入 MemTable
//     c. WriteToWAL() - 写入 WAL (如果启用)
//     d. MarkCommitted() - 标记事务提交完成
//   4. Abort() - 回滚事务
//
class OCCTransaction {
 public:
  // 构造函数
  OCCTransaction(TransactionManager* txn_manager,
                 VSLMemTable* memtable,
                 LsmEngine* lsm_engine,
                 WalWriter* wal_writer,
                 const TransactionOptions& options);
  
  ~OCCTransaction();
  
  // 禁止拷贝和移动
  OCCTransaction(const OCCTransaction&) = delete;
  OCCTransaction& operator=(const OCCTransaction&) = delete;
  OCCTransaction(OCCTransaction&&) = delete;
  OCCTransaction& operator=(OCCTransaction&&) = delete;
  
  // ========== 事务生命周期 ==========
  
  // 开始事务
  Status Begin();
  
  // 提交事务
  // 可能返回 Status::Conflict() 表示验证失败需要重试
  Status Commit();
  
  // 回滚事务
  Status Abort();
  
  // ========== 读写操作 ==========
  
  // 获取实体属性值 (顶点或边)
  // 对于多值属性，返回指定列的最新版本
  Status Get(uint64_t entity_id,
             EntityType entity_type,
             uint16_t column_id,
             Descriptor* descriptor,
             Timestamp* version_ts);
  
  // 获取实体所有列的快照
  Status GetAllColumns(uint64_t entity_id,
                       EntityType entity_type,
                       std::vector<std::pair<uint16_t, Descriptor>>* columns);
  
  // 写入实体属性值
  Status Put(uint64_t entity_id,
             EntityType entity_type,
             uint16_t column_id,
             const Descriptor& descriptor,
             Timestamp user_timestamp = Timestamp(0),  // 可选的业务时间戳
             uint64_t target_id = 0);  // 用于 Edge 存储对端节点 ID
  
  // 删除实体属性 (写入 Tombstone)
  Status Delete(uint64_t entity_id,
                EntityType entity_type,
                uint16_t column_id);
  
  // ========== 批量操作 ==========
  
  // 批量写入 (优化版本)
  Status PutBatch(const std::vector<WriteSetEntry>& entries);
  
  // ========== 查询接口 ==========
  
  // 查询实体历史版本 (基于快照时间戳)
  Status GetVersionHistory(uint64_t entity_id,
                           EntityType entity_type,
                           uint16_t column_id,
                           std::vector<std::pair<Timestamp, Descriptor>>* history);
  
  // ========== 状态查询 ==========
  
  uint64_t GetTransactionId() const { return txn_id_; }
  Timestamp GetReadTimestamp() const { return read_timestamp_; }
  Timestamp GetCommitTimestamp() const { return commit_timestamp_; }
  TransactionState GetState() const { return state_.load(); }
  
  bool IsCommitted() const {
    return state_.load() == TransactionState::kCommitted;
  }
  
  bool IsAborted() const {
    return state_.load() == TransactionState::kAborted;
  }
  
  // 获取冲突信息 (如果 Commit 返回 Conflict)
  const std::vector<ConflictInfo>& GetConflicts() const { return conflicts_; }
  
 private:
  // ========== 内部方法 ==========
  
  // 验证阶段 - OCC 核心
  // 检查读集中的所有记录是否未被其他事务修改
  Status Validate();
  
  // 验证单个读条目
  bool ValidateReadEntry(const ReadSetEntry& entry);
  
  // 并行验证读集
  void ValidateReadSetParallel();
  
  // 写入阶段
  Status WriteToMemTable();
  Status WriteToWAL();
  
  // 清理
  void Cleanup();
  
  // 构建 CedarKey
  CedarKey MakeKey(uint64_t entity_id, EntityType type, 
                  uint16_t column_id, Timestamp ts,
                  uint64_t target_id = 0);
  
 private:
  TransactionManager* txn_manager_;
  VSLMemTable* memtable_;
  LsmEngine* lsm_engine_;
  WalWriter* wal_writer_;
  TransactionOptions options_;
  
  // 事务标识
  uint64_t txn_id_ = 0;
  Timestamp read_timestamp_;
  Timestamp commit_timestamp_;
  
  // 事务状态
  std::atomic<TransactionState> state_{TransactionState::kActive};
  
  // 读集和写集
  std::vector<ReadSetEntry> read_set_;
  std::vector<WriteSetEntry> write_set_;
  
  // 用于快速查找写集
  std::unordered_set<std::string> write_set_keys_;
  
  // 冲突信息
  std::vector<ConflictInfo> conflicts_;
  
  // 互斥锁 (保护读写集)
  mutable std::mutex mutex_;
  
  // WAL 批次 (用于延迟写入)
  WalBatch wal_batch_;
};

// 事务重试包装器
// 自动处理冲突重试
class TransactionRetryWrapper {
 public:
  TransactionRetryWrapper(LsmEngine* lsm_engine,
                          const TransactionOptions& options);
  
  // 执行事务函数，自动处理重试
  // txn_func 接收一个 OCCTransaction* 参数
  template<typename Func>
  Status Execute(Func&& txn_func);
  
 private:
  LsmEngine* lsm_engine_;
  TransactionOptions options_;
};

}  // namespace cedar

#endif  // FERN_OCC_TRANSACTION_H_
