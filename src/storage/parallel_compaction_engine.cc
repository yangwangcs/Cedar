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

#include "cedar/storage/parallel_compaction_engine.h"

#include <algorithm>
#include <chrono>

namespace cedar {

// =============================================================================
// ColumnGroupTaskQueue 实现
// =============================================================================

ColumnGroupTaskQueue::ColumnGroupTaskQueue(const ParallelCompactionConfig& config)
    : config_(config) {}

ColumnGroupTaskQueue::~ColumnGroupTaskQueue() {
  Shutdown();
}

void ColumnGroupTaskQueue::Push(PrioritizedCompactionTask task) {
  std::unique_lock<std::mutex> lock(mutex_);
  if (shutdown_) return;
  
  queue_.push(std::move(task));
  cv_.notify_one();
}

bool ColumnGroupTaskQueue::Pop(PrioritizedCompactionTask* out_task, 
                               std::chrono::milliseconds timeout) {
  std::unique_lock<std::mutex> lock(mutex_);
  
  auto pred = [this] { return shutdown_ || !queue_.empty(); };
  
  if (timeout.count() > 0) {
    if (!cv_.wait_for(lock, timeout, pred)) {
      return false;
    }
  } else {
    cv_.wait(lock, pred);
  }
  
  if (shutdown_ || queue_.empty()) {
    return false;
  }
  
  *out_task = std::move(const_cast<PrioritizedCompactionTask&>(queue_.top()));
  queue_.pop();
  return true;
}

void ColumnGroupTaskQueue::MarkColumnBusy(const std::string& column_key, bool busy) {
  std::lock_guard<std::mutex> lock(mutex_);
  busy_columns_[column_key] = busy;
}

bool ColumnGroupTaskQueue::IsColumnBusy(const std::string& column_key) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = busy_columns_.find(column_key);
  return it != busy_columns_.end() && it->second;
}

size_t ColumnGroupTaskQueue::Size() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return queue_.size();
}

bool ColumnGroupTaskQueue::Empty() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return queue_.empty();
}

void ColumnGroupTaskQueue::Shutdown() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    shutdown_ = true;
  }
  cv_.notify_all();
}

// =============================================================================
// ThreadPoolCompactionExecutor 实现
// =============================================================================

ThreadPoolCompactionExecutor::ThreadPoolCompactionExecutor(
    SizeTieredCompactionEngine* engine,
    const ParallelCompactionConfig& config)
    : engine_(engine), config_(config) {
  task_queue_ = std::make_unique<ColumnGroupTaskQueue>(config);
}

ThreadPoolCompactionExecutor::~ThreadPoolCompactionExecutor() {
  Stop();
}

void ThreadPoolCompactionExecutor::Start() {
  int num_threads = config_.num_threads;
  if (num_threads <= 0) {
    num_threads = std::thread::hardware_concurrency();
    if (num_threads <= 0) num_threads = 4;
  }
  
  workers_.reserve(num_threads);
  for (int i = 0; i < num_threads; ++i) {
    workers_.emplace_back(&ThreadPoolCompactionExecutor::WorkerThread, this);
  }
}

void ThreadPoolCompactionExecutor::Stop() {
  shutdown_.store(true);
  task_queue_->Shutdown();
  
  for (auto& t : workers_) {
    if (t.joinable()) {
      t.join();
    }
  }
  workers_.clear();
}

void ThreadPoolCompactionExecutor::WaitForAll() {
  while (active_tasks_.load() > 0 || !task_queue_->Empty()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

void ThreadPoolCompactionExecutor::WorkerThread() {
  while (!shutdown_.load()) {
    PrioritizedCompactionTask ptask;
    
    // 获取任务（带超时）
    if (!task_queue_->Pop(&ptask, std::chrono::milliseconds(100))) {
      continue;
    }
    
    // 检查列是否忙（避免同列并发）
    if (task_queue_->IsColumnBusy(ptask.column_key)) {
      // 重新入队，稍后重试
      task_queue_->Push(std::move(ptask));
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      continue;
    }
    
    // 标记列忙
    task_queue_->MarkColumnBusy(ptask.column_key, true);
    active_tasks_.fetch_add(1);
    
    // 执行 Compaction（使用原始引擎的方法）
    Status s = engine_->ExecuteCompaction(ptask.task);
    (void)s;  // TODO: 处理错误
    
    // 标记列闲
    active_tasks_.fetch_sub(1);
    task_queue_->MarkColumnBusy(ptask.column_key, false);
  }
}

// =============================================================================
// ParallelCompactionEngine 实现
// =============================================================================

ParallelCompactionEngine::ParallelCompactionEngine(
    SizeTieredCompactionEngine* engine,
    const ParallelCompactionConfig& config)
    : engine_(engine), config_(config) {
  executor_ = std::make_unique<ThreadPoolCompactionExecutor>(engine, config);
}

ParallelCompactionEngine::~ParallelCompactionEngine() {
  Stop();
}

void ParallelCompactionEngine::Start() {
  executor_->Start();
}

void ParallelCompactionEngine::Stop() {
  executor_->Stop();
}

bool ParallelCompactionEngine::ScheduleIfNeeded() {
  if (!engine_->NeedsCompaction()) {
    return false;
  }
  
  auto task = engine_->PickNextCompaction();
  if (!task.has_value()) {
    return false;
  }
  
  // 计算优先级
  PrioritizedCompactionTask ptask;
  ptask.task = task.value();
  ptask.column_key = std::to_string(ptask.task.input_files[0].entity_type) + ":" +
                     std::to_string(ptask.task.input_files[0].column_id);
  
  // 简单优先级：层级越低越优先
  ptask.priority.level = ptask.task.input_level;
  ptask.priority.urgency = 1.0;
  ptask.priority.timestamp = std::chrono::steady_clock::now().time_since_epoch().count();
  
  // 提交到队列
  // Note: 这里简化处理，直接执行
  return true;
}

void ParallelCompactionEngine::WaitForAll() {
  executor_->WaitForAll();
}

int ParallelCompactionEngine::ActiveTasks() const {
  return executor_->ActiveTasks();
}

size_t ParallelCompactionEngine::PendingTasks() const {
  return 0;  // TODO
}

}  // namespace cedar
