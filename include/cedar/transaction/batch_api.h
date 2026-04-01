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

#ifndef FERN_BATCH_API_H_
#define FERN_BATCH_API_H_

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "cedar/types/cedar_key.h"
#include "cedar/core/slice.h"
#include "cedar/core/status.h"
#include "cedar/transaction/occ_transaction.h"
#include "cedar/transaction/wal.h"

namespace cedar {

class LsmEngine;
class CedarMemTable;

// 批量操作类型
enum class BatchOpType : uint8_t {
  kPutVertex = 0,    // 写入顶点
  kPutEdge = 1,      // 写入边
  kDeleteVertex = 2, // 删除顶点
  kDeleteEdge = 3,   // 删除边
  kUpdateVertex = 4, // 更新顶点属性
  kUpdateEdge = 5,   // 更新边属性
};

// 批量操作条目
struct BatchEntry {
  BatchOpType type;
  uint64_t entity_id;
  EntityType entity_type;
  uint16_t column_id;
  Descriptor descriptor;
  
  // 边的额外信息
  uint64_t dst_id = 0;
  std::string edge_label;
  
  BatchEntry() = default;
  
  static BatchEntry PutVertex(uint64_t vertex_id, 
                               uint16_t column_id,
                               const Descriptor& desc);
  static BatchEntry PutEdge(uint64_t src_id,
                             uint64_t dst_id,
                             const std::string& label,
                             uint16_t column_id,
                             const Descriptor& desc);
  static BatchEntry DeleteVertex(uint64_t vertex_id, uint16_t column_id);
  static BatchEntry DeleteEdge(uint64_t src_id,
                                uint64_t dst_id,
                                const std::string& label,
                                uint16_t column_id);
};

// 批量写请求
class WriteBatch {
 public:
  WriteBatch();
  ~WriteBatch() = default;
  
  // 禁止拷贝，允许移动
  WriteBatch(const WriteBatch&) = delete;
  WriteBatch& operator=(const WriteBatch&) = delete;
  WriteBatch(WriteBatch&&) = default;
  WriteBatch& operator=(WriteBatch&&) = default;
  
  // 添加操作
  void PutVertex(uint64_t vertex_id,
                 uint16_t column_id,
                 const Descriptor& descriptor);
  
  void PutEdge(uint64_t src_id,
               uint64_t dst_id,
               const std::string& label,
               uint16_t column_id,
               const Descriptor& descriptor);
  
  void DeleteVertex(uint64_t vertex_id, uint16_t column_id);
  
  void DeleteEdge(uint64_t src_id,
                  uint64_t dst_id,
                  const std::string& label,
                  uint16_t column_id);
  
  void Put(uint64_t entity_id,
           EntityType entity_type,
           uint16_t column_id,
           const Descriptor& descriptor);
  
  void Delete(uint64_t entity_id,
              EntityType entity_type,
              uint16_t column_id);
  
  // 批量添加
  void Append(const WriteBatch& other);
  
  // 清空
  void Clear();
  
  // 获取条目数量
  size_t Count() const { return entries_.size(); }
  
  // 是否为空
  bool Empty() const { return entries_.empty(); }
  
  // 估算内存使用
  size_t ApproximateSize() const;
  
  // 迭代条目
  void Iterate(std::function<void(const BatchEntry&)> callback) const;
  
  // 编码/解码 (用于网络传输或持久化)
  std::string Encode() const;
  static StatusOr<WriteBatch> Decode(const Slice& data);
  
  // 转换为 WAL 批次
  // 修复: 添加 txn_version 参数以支持崩溃恢复后的完整 MVCC 隔离
  WalBatch ToWalBatch(Timestamp timestamp, uint64_t sequence, Timestamp txn_version) const;
  
  // 转换为事务写集
  // 注意：business_ts 是业务时间戳（用于 Key），txn_version 是事务版本号（用于 MVCC）
  std::vector<WriteSetEntry> ToWriteSet(Timestamp business_ts, 
                                         uint64_t txn_id,
                                         Timestamp txn_version) const;
  
 private:
  std::vector<BatchEntry> entries_;
};

// 批量读请求
class ReadBatch {
 public:
  struct ReadRequest {
    uint64_t entity_id;
    EntityType entity_type;
    uint16_t column_id;
    Timestamp timestamp;  // 0 表示读取最新
  };
  
  struct ReadResult {
    Status status;
    Descriptor descriptor;
    Timestamp version_timestamp;
  };
  
  ReadBatch();
  ~ReadBatch() = default;
  
  // 添加读取请求
  void Get(uint64_t entity_id,
           EntityType entity_type,
           uint16_t column_id,
           Timestamp timestamp = Timestamp());
  
  void GetLatest(uint64_t entity_id,
                 EntityType entity_type,
                 uint16_t column_id);
  
  // 清空
  void Clear();
  
  // 获取请求数量
  size_t Count() const { return requests_.size(); }
  
  // 迭代请求
  void Iterate(std::function<void(const ReadRequest&)> callback) const;
  
  // 设置结果
  void SetResult(size_t index, const ReadResult& result);
  
  // 获取结果
  const std::vector<ReadResult>& Results() const { return results_; }
  
 private:
  std::vector<ReadRequest> requests_;
  std::vector<ReadResult> results_;
};

// 批量执行选项
struct BatchOptions {
  // 是否使用事务 (OCC)
  bool use_transaction = true;
  
  // 事务选项
  TransactionOptions txn_options;
  
  // 是否同步写入 WAL
  bool sync_wal = false;
  
  // 批量大小阈值 (达到后自动提交)
  size_t auto_commit_threshold = 1000;
  
  // 超时 (毫秒)
  uint64_t timeout_ms = 30000;
};

// 批量执行结果
struct BatchResult {
  Status status;
  uint64_t txn_id = 0;
  Timestamp commit_timestamp;
  size_t entries_processed = 0;
  std::vector<Status> entry_statuses;  // 每个条目的状态 (如果单独追踪)
};

// 批量执行器
// 提供高阶 API 用于批量读写
class BatchExecutor {
 public:
  explicit BatchExecutor(LsmEngine* lsm_engine);
  ~BatchExecutor();
  
  // 禁止拷贝
  BatchExecutor(const BatchExecutor&) = delete;
  BatchExecutor& operator=(const BatchExecutor&) = delete;
  
  // ========== 同步批量操作 ==========
  
  // 批量写入 (非事务)
  // 直接写入 MemTable，不使用 OCC 验证
  BatchResult Write(const WriteBatch& batch, const BatchOptions& options);
  
  // 批量写入 (事务)
  // 使用 OCC 事务，保证原子性和隔离性
  BatchResult WriteTransactional(const WriteBatch& batch, 
                                  const TransactionOptions& txn_options);
  
  // 批量读取 - 重命名以避免与 ReadBatch 类名冲突
  ReadBatch ExecuteReadBatch(const ReadBatch& batch);
  
  // ========== 异步批量操作 ==========
  
  // 异步批量写入
  // 返回一个句柄用于查询状态
  class AsyncWriteHandle;
  std::unique_ptr<AsyncWriteHandle> WriteAsync(const WriteBatch& batch,
                                                const BatchOptions& options);
  
  // 等待异步操作完成
  static BatchResult WaitForAsyncWrite(AsyncWriteHandle* handle);
  
  // ========== 流式批量操作 ==========
  
  // 开始流式写入会话
  // 允许累积多个小批次，然后一次性提交
  class StreamingSession;
  std::unique_ptr<StreamingSession> BeginStreamingSession(
      const BatchOptions& options);
  
  // 提交流式会话
  static BatchResult CommitStreamingSession(StreamingSession* session);
  
 private:
  LsmEngine* lsm_engine_;
  std::unique_ptr<TransactionManager> txn_manager_;
};

// 异步写入句柄
class BatchExecutor::AsyncWriteHandle {
 public:
  bool IsComplete() const;
  BatchResult GetResult() const;
  void Wait() const;
  
 private:
  friend class BatchExecutor;
  
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

// 流式会话
class BatchExecutor::StreamingSession {
 public:
  void Add(const BatchEntry& entry);
  void AddBatch(const WriteBatch& batch);
  size_t Size() const;
  void Clear();
  
 private:
  friend class BatchExecutor;
  
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

// Group Commit 管理器
// 管理多个事务的批量提交，优化 I/O 性能
class GroupCommitManager {
 public:
  struct Config {
    // 组提交超时 (微秒)
    uint32_t timeout_us = 1000;
    
    // 最大批次大小
    size_t max_batch_size = 1000;
    
    // 最小批次大小 (即使超时，也等待达到此大小)
    size_t min_batch_size = 10;
    
    // 工作线程数
    size_t num_workers = 2;
  };
  
  explicit GroupCommitManager(WalWriter* wal_writer, const Config& config);
  ~GroupCommitManager();
  
  // 禁止拷贝
  GroupCommitManager(const GroupCommitManager&) = delete;
  GroupCommitManager& operator=(const GroupCommitManager&) = delete;
  
  // 提交事务到组提交队列
  // 返回的 future 在提交完成时被满足
  std::future<Status> Submit(uint64_t txn_id, const WalBatch& batch);
  
  // 强制刷新所有挂起的提交
  Status Flush();
  
  // 获取统计
  struct Stats {
    uint64_t total_commits = 0;
    uint64_t total_batches = 0;
    uint64_t avg_batch_size = 0;
    uint64_t total_wait_time_us = 0;
  };
  Stats GetStats() const;
  
 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace cedar

#endif  // FERN_BATCH_API_H_
