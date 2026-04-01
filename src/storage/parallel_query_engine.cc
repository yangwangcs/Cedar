// Copyright (c) 2025 The Cedar Authors. All rights reserved.

#include "cedar/storage/parallel_query_engine.h"
#include "cedar/storage/lsm_engine.h"

namespace cedar {

// ThreadPoolQueryExecutor 实现
ThreadPoolQueryExecutor::ThreadPoolQueryExecutor(LsmEngine* engine,
                                                  const ParallelQueryConfig& config)
    : engine_(engine), config_(config) {}

ThreadPoolQueryExecutor::~ThreadPoolQueryExecutor() {
  Stop();
}

void ThreadPoolQueryExecutor::Start() {
  int num_threads = config_.num_threads;
  if (num_threads <= 0) {
    num_threads = std::thread::hardware_concurrency();
    if (num_threads <= 0) num_threads = 4;
  }
  
  for (int i = 0; i < num_threads; ++i) {
    workers_.emplace_back(&ThreadPoolQueryExecutor::WorkerThread, this);
  }
}

void ThreadPoolQueryExecutor::Stop() {
  shutdown_ = true;
  queue_cv_.notify_all();
  
  for (auto& t : workers_) {
    if (t.joinable()) t.join();
  }
}

std::future<ColumnQueryResult> ThreadPoolQueryExecutor::Submit(
    const ColumnQueryRequest& request) {
  QueryTask task;
  task.request = request;
  task.submit_time = std::chrono::steady_clock::now().time_since_epoch().count();
  
  std::future<ColumnQueryResult> future = task.promise.get_future();
  
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    task_queue_.push(std::move(task));
  }
  
  queue_cv_.notify_one();
  return future;
}

std::vector<std::future<ColumnQueryResult>> ThreadPoolQueryExecutor::SubmitBatch(
    const std::vector<ColumnQueryRequest>& requests) {
  std::vector<std::future<ColumnQueryResult>> futures;
  futures.reserve(requests.size());
  
  for (const auto& req : requests) {
    futures.push_back(Submit(req));
  }
  
  return futures;
}

ThreadPoolQueryExecutor::Stats ThreadPoolQueryExecutor::GetStats() const {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  return stats_;
}

void ThreadPoolQueryExecutor::WorkerThread() {
  while (!shutdown_) {
    QueryTask task;
    
    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      queue_cv_.wait(lock, [this] { return shutdown_ || !task_queue_.empty(); });
      
      if (shutdown_ && task_queue_.empty()) return;
      if (task_queue_.empty()) continue;
      
      task = std::move(task_queue_.front());
      task_queue_.pop();
    }
    
    // 执行查询
    auto result = ExecuteQuery(task.request);
    task.promise.set_value(result);
    
    // 更新统计
    {
      std::lock_guard<std::mutex> lock(stats_mutex_);
      stats_.total_queries++;
    }
  }
}

ColumnQueryResult ThreadPoolQueryExecutor::ExecuteQuery(
    const ColumnQueryRequest& request) {
  ColumnQueryResult result;
  result.column_id = request.column_id;
  
  CedarKey key;
  if (request.entity_type == 0) {
    key = CedarKey::Vertex(request.entity_id, request.column_id, request.timestamp);
  } else {
    // Edge 类型需要 target_id，这里简化处理
    key = CedarKey::EdgeOut(request.entity_id, request.column_id, 0, request.timestamp);
  }
  
  result.value = engine_->Get(key);
  result.status = Status::OK();
  
  return result;
}

// ParallelQueryEngine 实现
ParallelQueryEngine::ParallelQueryEngine(LsmEngine* engine,
                                         const ParallelQueryConfig& config) {
  executor_ = std::make_unique<ThreadPoolQueryExecutor>(engine, config);
  executor_->Start();
}

ParallelQueryEngine::~ParallelQueryEngine() = default;

std::vector<ColumnQueryResult> ParallelQueryEngine::QueryColumns(
    uint64_t entity_id,
    const std::vector<uint16_t>& column_ids,
    uint8_t entity_type,
    Timestamp timestamp) {
  // 如果列数少，直接串行查询
  if (column_ids.size() <= 1) {
    std::vector<ColumnQueryResult> results;
    for (auto col_id : column_ids) {
      ColumnQueryRequest req{entity_id, col_id, entity_type, timestamp};
      results.push_back(executor_->ExecuteQuery(req));
    }
    return results;
  }
  
  // 并行查询
  std::vector<ColumnQueryRequest> requests;
  requests.reserve(column_ids.size());
  for (auto col_id : column_ids) {
    requests.push_back({entity_id, col_id, entity_type, timestamp});
  }
  
  auto futures = executor_->SubmitBatch(requests);
  
  std::vector<ColumnQueryResult> results;
  results.reserve(futures.size());
  for (auto& f : futures) {
    results.push_back(f.get());
  }
  
  return results;
}

std::optional<Descriptor> ParallelQueryEngine::QuerySingle(
    uint64_t entity_id,
    uint16_t column_id,
    uint8_t entity_type,
    Timestamp timestamp) {
  ColumnQueryRequest req{entity_id, column_id, entity_type, timestamp};
  auto future = executor_->Submit(req);
  return future.get().value;
}

std::vector<ParallelQueryEngine::RangeQueryResult> 
ParallelQueryEngine::QueryRangeParallel(const RangeQueryRequest& request,
                                        int num_shards) {
  // 分片并行扫描
  uint64_t range_size = request.end_entity_id - request.start_entity_id;
  uint64_t shard_size = range_size / num_shards;
  
  std::vector<std::future<std::vector<RangeQueryResult>>> futures;
  
  for (int i = 0; i < num_shards; ++i) {
    uint64_t shard_start = request.start_entity_id + i * shard_size;
    uint64_t shard_end = (i == num_shards - 1) ? request.end_entity_id 
                                                : shard_start + shard_size;
    
    futures.push_back(std::async(std::launch::async, [this, shard_start, shard_end, &request]() {
      std::vector<RangeQueryResult> shard_results;
      
      for (uint64_t id = shard_start; id < shard_end; ++id) {
        auto value = this->QuerySingle(id, request.column_id, request.entity_type);
        if (value) {
          shard_results.push_back({id, request.column_id, *value});
        }
      }
      
      return shard_results;
    }));
  }
  
  // 合并结果
  std::vector<RangeQueryResult> all_results;
  for (auto& f : futures) {
    auto shard = f.get();
    all_results.insert(all_results.end(), shard.begin(), shard.end());
  }
  
  return all_results;
}

}  // namespace cedar
