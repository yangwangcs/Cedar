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

#include "cedar/storage/async_index_builder.h"

#include <chrono>
#include <thread>

namespace cedar {

// ============== IndexBuildTask ==============

uint64_t IndexBuildTask::CalculatePriority(size_t version_count,
                                            std::chrono::steady_clock::time_point submit_time) {
  auto now = std::chrono::steady_clock::now();
  auto wait_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now - submit_time).count();
  
  uint64_t priority = version_count * 1000;
  if (wait_ms > 0 && static_cast<uint64_t>(wait_ms) < priority) {
    priority -= static_cast<uint64_t>(wait_ms);
  }
  
  return priority;
}

// ============== AsyncIndexBuilder ==============

AsyncIndexBuilder::AsyncIndexBuilder(const AsyncIndexBuilderOptions& options)
    : options_(options),
      tasks_submitted_(0),
      tasks_completed_(0),
      tasks_failed_(0),
      tasks_timeout_(0),
      indices_built_(0),
      versions_indexed_(0),
      batch_builds_(0),
      total_build_time_ms_(0) {}

AsyncIndexBuilder::~AsyncIndexBuilder() {
  Shutdown();
}

Status AsyncIndexBuilder::Initialize() {
  for (uint32_t i = 0; i < options_.num_worker_threads; ++i) {
    workers_.push_back(std::make_unique<std::thread>(
        &AsyncIndexBuilder::WorkerThreadLoop, this, i));
  }
  return Status::OK();
}

Status AsyncIndexBuilder::Shutdown() {
  stop_flag_.store(true, std::memory_order_release);
  queue_cv_.notify_all();
  
  for (auto& worker : workers_) {
    if (worker && worker->joinable()) {
      worker->join();
    }
  }
  
  workers_.clear();
  return Status::OK();
}

Status AsyncIndexBuilder::SubmitTask(const IndexBuildTask& task) {
  if (stop_flag_.load(std::memory_order_acquire)) {
    return Status::IOError("Builder is shutting down");
  }
  
  uint64_t entity_hash = HashEntity(task.entity_id, task.entity_type, task.column_id);
  
  {
    std::lock_guard<std::mutex> lock(building_mutex_);
    if (building_set_.count(entity_hash)) {
      return Status::OK();
    }
    building_set_[entity_hash] = std::chrono::steady_clock::now();
  }
  
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    if (task_queue_.size() >= options_.task_queue_capacity) {
      return Status::IOError("Task queue is full");
    }
    
    IndexBuildTask mutable_task = task;
    mutable_task.submit_time = std::chrono::steady_clock::now();
    
    if (options_.enable_priority_scheduling) {
      mutable_task.priority = IndexBuildTask::CalculatePriority(
          mutable_task.version_count, mutable_task.submit_time);
    }
    
    task_queue_.push(mutable_task);
    tasks_submitted_.fetch_add(1, std::memory_order_relaxed);
  }
  
  queue_cv_.notify_one();
  return Status::OK();
}

Status AsyncIndexBuilder::SubmitAndWait(const IndexBuildTask& task,
                                         IndexBuildResult* result,
                                         uint32_t timeout_ms) {
  if (!result) {
    return Status::InvalidArgument("Result pointer is null");
  }
  
  uint64_t entity_hash = HashEntity(task.entity_id, task.entity_type, task.column_id);
  
  std::promise<IndexBuildResult> promise;
  auto future = promise.get_future();
  
  {
    std::lock_guard<std::mutex> lock(promise_mutex_);
    result_promises_[entity_hash] = std::move(promise);
  }
  
  IndexBuildTask mutable_task = task;
  mutable_task.on_complete = [this, entity_hash](VersionChainIndex* index) {
    IndexBuildResult res;
    res.entity_id = entity_hash;
    res.success = (index != nullptr);
    res.index = index;
    
    std::lock_guard<std::mutex> lock(promise_mutex_);
    auto it = result_promises_.find(entity_hash);
    if (it != result_promises_.end()) {
      it->second.set_value(res);
      result_promises_.erase(it);
    }
  };
  
  Status s = SubmitTask(mutable_task);
  if (!s.ok()) {
    std::lock_guard<std::mutex> lock(promise_mutex_);
    result_promises_.erase(entity_hash);
    return s;
  }
  
  auto status = future.wait_for(std::chrono::milliseconds(timeout_ms));
  if (status == std::future_status::timeout) {
    tasks_timeout_.fetch_add(1, std::memory_order_relaxed);
    return Status::IOError("Index build timed out");
  }
  
  try {
    *result = future.get();
  } catch (...) {
    return Status::IOError("Failed to get result");
  }
  
  return Status::OK();
}

Status AsyncIndexBuilder::SubmitBatch(const std::vector<IndexBuildTask>& tasks) {
  for (const auto& task : tasks) {
    Status s = SubmitTask(task);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

Status AsyncIndexBuilder::MaybeSubmitBuild(uint64_t entity_id,
                                            EntityType entity_type,
                                            uint16_t column_id,
                                            TemporalVersionNode* version_head,
                                            size_t version_count) {
  if (!ShouldBuildIndex(version_count)) {
    return Status::OK();
  }
  
  if (options_.enable_build_cache && CheckCache(entity_id, entity_type, column_id)) {
    return Status::OK();
  }
  
  IndexBuildTask task;
  task.entity_id = entity_id;
  task.entity_type = entity_type;
  task.column_id = column_id;
  task.version_head = version_head;
  task.version_count = version_count;
  task.priority = version_count;
  
  return SubmitTask(task);
}

AsyncIndexBuilderStats AsyncIndexBuilder::GetStats() const {
  AsyncIndexBuilderStats stats;
  stats.tasks_submitted = tasks_submitted_.load(std::memory_order_relaxed);
  stats.tasks_completed = tasks_completed_.load(std::memory_order_relaxed);
  stats.tasks_failed = tasks_failed_.load(std::memory_order_relaxed);
  stats.tasks_timeout = tasks_timeout_.load(std::memory_order_relaxed);
  stats.indices_built = indices_built_.load(std::memory_order_relaxed);
  stats.versions_indexed = versions_indexed_.load(std::memory_order_relaxed);
  stats.batch_builds = batch_builds_.load(std::memory_order_relaxed);
  
  uint64_t completed = stats.tasks_completed;
  if (completed > 0) {
    stats.avg_build_time_ms = total_build_time_ms_.load(std::memory_order_relaxed) / completed;
  }
  return stats;
}

size_t AsyncIndexBuilder::GetQueueDepth() const {
  std::lock_guard<std::mutex> lock(queue_mutex_);
  return task_queue_.size();
}

bool AsyncIndexBuilder::IsBuilding(uint64_t entity_id,
                                    EntityType entity_type,
                                    uint16_t column_id) const {
  uint64_t entity_hash = HashEntity(entity_id, entity_type, column_id);
  std::lock_guard<std::mutex> lock(building_mutex_);
  return building_set_.count(entity_hash) > 0;
}

Status AsyncIndexBuilder::WaitForAll(uint32_t timeout_ms) {
  auto start = std::chrono::steady_clock::now();
  
  while (GetQueueDepth() > 0) {
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count();
    
    if (elapsed > timeout_ms) {
      return Status::IOError("Timeout waiting for all tasks");
    }
    
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  
  return Status::OK();
}

void AsyncIndexBuilder::WorkerThreadLoop(uint32_t worker_id) {
  (void)worker_id;
  
  while (!stop_flag_.load(std::memory_order_acquire)) {
    std::vector<IndexBuildTask> batch;
    
    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      queue_cv_.wait(lock, [this] {
        return !task_queue_.empty() || stop_flag_.load(std::memory_order_acquire);
      });
      
      if (stop_flag_.load(std::memory_order_acquire)) {
        break;
      }
      
      if (options_.enable_batch_building) {
        batch = CollectBatch();
      } else if (!task_queue_.empty()) {
        batch.push_back(task_queue_.top());
        task_queue_.pop();
      }
    }
    
    if (batch.size() == 1) {
      auto result = ExecuteBuild(batch[0]);
      if (result.success) {
        tasks_completed_.fetch_add(1, std::memory_order_relaxed);
        indices_built_.fetch_add(1, std::memory_order_relaxed);
        versions_indexed_.fetch_add(result.versions_indexed, std::memory_order_relaxed);
        
        uint64_t completed = tasks_completed_.load(std::memory_order_relaxed);
        if (completed > 0) {
          uint64_t new_avg = (total_build_time_ms_.load(std::memory_order_relaxed) * (completed - 1) 
                             + result.build_duration.count()) / completed;
          total_build_time_ms_.store(new_avg, std::memory_order_relaxed);
        }
        
        if (batch[0].on_complete) {
          batch[0].on_complete(result.index);
        }
      } else {
        tasks_failed_.fetch_add(1, std::memory_order_relaxed);
      }
    } else if (batch.size() > 1) {
      auto results = ExecuteBatchBuild(batch);
      batch_builds_.fetch_add(1, std::memory_order_relaxed);
      
      for (size_t i = 0; i < results.size(); ++i) {
        if (results[i].success) {
          tasks_completed_.fetch_add(1, std::memory_order_relaxed);
          indices_built_.fetch_add(1, std::memory_order_relaxed);
          
          if (i < batch.size() && batch[i].on_complete) {
            batch[i].on_complete(results[i].index);
          }
        } else {
          tasks_failed_.fetch_add(1, std::memory_order_relaxed);
        }
      }
    }
  }
}

IndexBuildResult AsyncIndexBuilder::ExecuteBuild(const IndexBuildTask& task) {
  IndexBuildResult result;
  result.entity_id = task.entity_id;
  result.entity_type = task.entity_type;
  result.column_id = task.column_id;
  result.success = false;
  result.index = nullptr;
  
  auto start = std::chrono::steady_clock::now();
  
  auto index = std::make_unique<VersionChainIndex>();
  
  if (index->Build(task.version_head, task.version_count)) {
    result.success = true;
    result.index = index.release();
    result.versions_indexed = task.version_count;
    
    if (options_.enable_build_cache) {
      UpdateCache(task.entity_id, task.entity_type, task.column_id, task.version_count);
    }
  } else {
    result.error_message = "Failed to build index";
  }
  
  auto end = std::chrono::steady_clock::now();
  result.build_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  
  uint64_t entity_hash = HashEntity(task.entity_id, task.entity_type, task.column_id);
  {
    std::lock_guard<std::mutex> lock(building_mutex_);
    building_set_.erase(entity_hash);
  }
  
  return result;
}

std::vector<IndexBuildResult> AsyncIndexBuilder::ExecuteBatchBuild(
    const std::vector<IndexBuildTask>& tasks) {
  std::vector<IndexBuildResult> results;
  results.reserve(tasks.size());
  
  for (const auto& task : tasks) {
    results.push_back(ExecuteBuild(task));
  }
  
  return results;
}

std::vector<IndexBuildTask> AsyncIndexBuilder::CollectBatch() {
  std::vector<IndexBuildTask> batch;
  batch.reserve(options_.batch_max_size);
  
  auto start = std::chrono::steady_clock::now();
  
  while (batch.size() < options_.batch_max_size && !task_queue_.empty()) {
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - start).count();
    
    if (!batch.empty() && elapsed > static_cast<int64_t>(options_.batch_timeout_ms)) {
      break;
    }
    
    batch.push_back(task_queue_.top());
    task_queue_.pop();
  }
  
  return batch;
}

bool AsyncIndexBuilder::CheckCache(uint64_t entity_id,
                                    EntityType entity_type,
                                    uint16_t column_id) {
  uint64_t entity_hash = HashEntity(entity_id, entity_type, column_id);
  
  std::lock_guard<std::mutex> lock(cache_mutex_);
  auto it = build_cache_.find(entity_hash);
  if (it != build_cache_.end() && it->second.valid) {
    return true;
  }
  return false;
}

void AsyncIndexBuilder::UpdateCache(uint64_t entity_id,
                                     EntityType entity_type,
                                     uint16_t column_id,
                                     size_t version_count) {
  uint64_t entity_hash = HashEntity(entity_id, entity_type, column_id);
  
  std::lock_guard<std::mutex> lock(cache_mutex_);
  
  if (build_cache_.size() >= options_.build_cache_size && !cache_lru_queue_.empty()) {
    uint64_t oldest = cache_lru_queue_.front();
    cache_lru_queue_.pop();
    build_cache_.erase(oldest);
  }
  
  BuildCacheEntry entry;
  entry.entity_hash = entity_hash;
  entry.build_time = std::chrono::steady_clock::now();
  entry.version_count_at_build = version_count;
  entry.valid = true;
  
  build_cache_[entity_hash] = entry;
  cache_lru_queue_.push(entity_hash);
}

uint64_t AsyncIndexBuilder::HashEntity(uint64_t entity_id,
                                        EntityType entity_type,
                                        uint16_t column_id) {
  uint64_t hash = 14695981039346656037ULL;
  hash ^= entity_id;
  hash *= 1099511628211ULL;
  hash ^= static_cast<uint64_t>(entity_type);
  hash *= 1099511628211ULL;
  hash ^= column_id;
  hash *= 1099511628211ULL;
  return hash;
}

}  // namespace cedar
