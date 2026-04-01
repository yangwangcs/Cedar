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

#ifndef FERN_BATCH_TRANSACTION_H_
#define FERN_BATCH_TRANSACTION_H_

#include <vector>
#include <functional>

#include "cedar/transaction/occ_transaction.h"
#include "cedar/storage/cedar_graph_storage.h"

namespace cedar {

// 批量事务执行器 - 将多个操作批量在一个事务中执行，减少事务开销
class BatchTransactionExecutor {
 public:
  explicit BatchTransactionExecutor(CedarGraphStorage* storage);
  
  ~BatchTransactionExecutor();
  
  // 添加操作到批次
  void AddPut(uint64_t entity_id,
              EntityType entity_type,
              uint16_t column_id,
              const Descriptor& descriptor,
              Timestamp user_timestamp = Timestamp(0));
  
  void AddGet(uint64_t entity_id,
              EntityType entity_type,
              uint16_t column_id);
  
  void AddDelete(uint64_t entity_id,
                 EntityType entity_type,
                 uint16_t column_id);
  
  // 执行所有操作（单事务）
  Status Execute();
  
  // 获取读操作结果（在 Execute 后调用）
  std::optional<Descriptor> GetResult(size_t index);
  
  // 清空批次
  void Clear();
  
  // 批次大小
  size_t Size() const { return operations_.size(); }
  
  // 是否为空
  bool Empty() const { return operations_.empty(); }
  
  // 操作类型
  enum class OpType { PUT, GET, DELETE };
  
 private:
  
  struct Operation {
    OpType type;
    uint64_t entity_id;
    EntityType entity_type;
    uint16_t column_id;
    Descriptor descriptor;      // for PUT
    Timestamp user_timestamp;   // for PUT
    
    Operation(OpType t, uint64_t eid, EntityType etype, uint16_t col,
              const Descriptor& desc = Descriptor(),
              Timestamp ts = Timestamp(0))
        : type(t), entity_id(eid), entity_type(etype), column_id(col),
          descriptor(desc), user_timestamp(ts) {}
  };
  
  CedarGraphStorage* storage_;
  std::vector<Operation> operations_;
  std::vector<std::optional<Descriptor>> get_results_;
};

// 简化批处理 API - 自动批量提交
class AutoBatchExecutor {
 public:
  struct Options {
    size_t batch_size = 100;           // 批次大小
    uint32_t flush_interval_ms = 10;   // 自动提交间隔
  };
  
  // 操作类型
  enum class OpType { PUT, GET, DELETE };
  
  AutoBatchExecutor(CedarGraphStorage* storage, const Options& options);
  
  ~AutoBatchExecutor();
  
  // 提交操作（可能立即执行或缓冲）
  Status SubmitPut(uint64_t entity_id,
                   EntityType entity_type,
                   uint16_t column_id,
                   const Descriptor& descriptor,
                   Timestamp user_timestamp = Timestamp(0));
  
  // 立即刷新所有缓冲的操作
  Status Flush();
  
  // 获取已执行的操作数
  uint64_t ExecutedCount() const { return executed_count_.load(); }
  
 private:
  struct BufferedOp {
    OpType type;
    uint64_t entity_id;
    EntityType entity_type;
    uint16_t column_id;
    Descriptor descriptor;
    Timestamp user_timestamp;
    
    BufferedOp(OpType t, uint64_t eid, EntityType et, uint16_t col,
               const Descriptor& desc = Descriptor(),
               Timestamp ts = Timestamp(0))
        : type(t), entity_id(eid), entity_type(et), column_id(col),
          descriptor(desc), user_timestamp(ts) {}
  };
  
  CedarGraphStorage* storage_;
  Options options_;
  
  std::vector<BufferedOp> buffer_;
  mutable std::mutex mutex_;
  std::atomic<uint64_t> executed_count_{0};
};

}  // namespace cedar

#endif  // FERN_BATCH_TRANSACTION_H_
