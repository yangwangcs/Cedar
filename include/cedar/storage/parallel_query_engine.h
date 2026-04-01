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

// =============================================================================
// 并行查询引擎 - 跨列并行查询
// =============================================================================
// 特性：
// 1. 多列查询并行执行（如查询用户的 name, age, email 同时发起）
// 2. 线程池复用，避免频繁创建线程
// 3. 结果合并，保持时间戳一致性
// =============================================================================

#ifndef FERN_PARALLEL_QUERY_ENGINE_H_
#define FERN_PARALLEL_QUERY_ENGINE_H_

#include <cstdint>
#include <future>
#include <memory>
#include <thread>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>

#include "cedar/types/cedar_types.h"
#include "cedar/types/descriptor.h"

namespace cedar {

class LsmEngine;
struct CedarKey;

// 单列查询请求
struct ColumnQueryRequest {
  uint64_t entity_id;
  uint16_t column_id;
  uint8_t entity_type;
  Timestamp timestamp;
};

// 单列查询结果
struct ColumnQueryResult {
  uint16_t column_id;
  std::optional<Descriptor> value;
  Status status;
};

// 并行查询配置
struct ParallelQueryConfig {
  // 查询线程池大小 (0 = 硬件线程数)
  int num_threads = 0;
  
  // 单查询最大并发列数
  int max_concurrent_columns = 8;
  
  // 小查询阈值（列数少于此值不启用并行）
  int parallel_threshold = 2;
  
  // 查询超时（毫秒）
  int timeout_ms = 5000;
};

// 查询任务（内部使用）
struct QueryTask {
  ColumnQueryRequest request;
  std::promise<ColumnQueryResult> promise;
  uint64_t submit_time;
};

// 线程池查询执行器
class ThreadPoolQueryExecutor {
 public:
  explicit ThreadPoolQueryExecutor(LsmEngine* engine, const ParallelQueryConfig& config);
  ~ThreadPoolQueryExecutor();
  
  // 启动/停止
  void Start();
  void Stop();
  
  // 提交单列查询
  std::future<ColumnQueryResult> Submit(const ColumnQueryRequest& request);
  
  // 批量提交
  std::vector<std::future<ColumnQueryResult>> SubmitBatch(
      const std::vector<ColumnQueryRequest>& requests);
  
  // 获取统计
  struct Stats {
    uint64_t total_queries = 0;
    uint64_t cache_hits = 0;
    double avg_latency_ms = 0;
    int active_threads = 0;
  };
  Stats GetStats() const;

 private:
  void WorkerThread();
 public:
  // 公开给 ParallelQueryEngine 使用
  ColumnQueryResult ExecuteQuery(const ColumnQueryRequest& request);
  
 private:
  LsmEngine* engine_;
  ParallelQueryConfig config_;
  
  std::vector<std::thread> workers_;
  std::queue<QueryTask> task_queue_;
  mutable std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::atomic<bool> shutdown_{false};
  
  // 统计
  mutable std::mutex stats_mutex_;
  Stats stats_;
};

// 并行查询引擎（对外接口）
class ParallelQueryEngine {
 public:
  ParallelQueryEngine(LsmEngine* engine, const ParallelQueryConfig& config);
  ~ParallelQueryEngine();
  
  // 并行多列查询
  // 同时查询多个列，自动并行化
  std::vector<ColumnQueryResult> QueryColumns(
      uint64_t entity_id,
      const std::vector<uint16_t>& column_ids,
      uint8_t entity_type = 0,
      Timestamp timestamp = Timestamp::Static());
  
  // 单个查询（优化路径：如果列数少，直接串行）
  std::optional<Descriptor> QuerySingle(
      uint64_t entity_id,
      uint16_t column_id,
      uint8_t entity_type = 0,
      Timestamp timestamp = Timestamp::Static());
  
  // 范围查询并行化
  struct RangeQueryRequest {
    uint64_t start_entity_id;
    uint64_t end_entity_id;
    uint16_t column_id;
    uint8_t entity_type;
  };
  
  struct RangeQueryResult {
    uint64_t entity_id;
    uint16_t column_id;
    Descriptor value;
  };
  
  // 并行范围查询（将范围分片并行扫描）
  std::vector<RangeQueryResult> QueryRangeParallel(
      const RangeQueryRequest& request,
      int num_shards = 4);
  
  // 获取执行器统计
  ThreadPoolQueryExecutor::Stats GetStats() const {
    return executor_->GetStats();
  }

 private:
  std::unique_ptr<ThreadPoolQueryExecutor> executor_;
};

}  // namespace cedar

#endif  // FERN_PARALLEL_QUERY_ENGINE_H_
