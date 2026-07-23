// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/runtime/work_execution_service.h"

#include <exception>
#include <new>
#include <utility>

namespace cedar {
namespace {

thread_local WorkExecutionService* current_execution_service = nullptr;

Status InvokeWorkCallback(const std::function<Status()>& callback) {
  try {
    return callback();
  } catch (const std::exception& error) {
    return Status::Corruption("work execution callback", error.what());
  } catch (...) {
    return Status::Corruption("work execution callback", "unknown exception");
  }
}

}  // namespace

size_t WorkExecutionService::WorkClassIndex(WorkClass work_class) {
  switch (work_class) {
    case WorkClass::kCommitCritical: return 0;
    case WorkClass::kRecovery: return 1;
    case WorkClass::kShutdown: return 2;
    case WorkClass::kForegroundWrite: return 3;
    case WorkClass::kPointRead: return 4;
    case WorkClass::kInteractiveQuery: return 5;
    case WorkClass::kFlush: return 6;
    case WorkClass::kCompactionUrgent: return 7;
    case WorkClass::kAnalyticalQuery: return 8;
    case WorkClass::kCompactionNormal: return 9;
    case WorkClass::kIndexBuild: return 10;
    case WorkClass::kStatsMerge: return 11;
    case WorkClass::kBlobGc: return 12;
  }
  return 12;
}

Status WorkTaskHandle::Wait() const {
  if (!completion_) {
    return Status::InvalidArgument("work execution", "missing task completion handle");
  }
  std::unique_lock<std::mutex> lock(completion_->mutex);
  completion_->ready.wait(lock, [this] { return completion_->completed; });
  return completion_->status;
}

WorkExecutionService::~WorkExecutionService() { Stop().IgnoreError(); }

Status WorkExecutionService::Start() {
  std::unique_lock<std::mutex> lock(mutex_);
  if (scheduler_ == nullptr) {
    return Status::InvalidArgument("work execution", "missing scheduler");
  }
  if (running_) return Status::OK();
  stopping_ = false;
  preemptible_cancellation_requested_.fill(false);
  running_ = true;
  try {
    workers_.reserve(worker_count_);
    for (size_t index = 0; index < worker_count_; ++index) {
      workers_.emplace_back(&WorkExecutionService::WorkerLoop, this);
    }
  } catch (...) {
    stopping_ = true;
    lock.unlock();
    work_available_.notify_all();
    for (std::thread& worker : workers_) {
      if (worker.joinable()) worker.join();
    }
    workers_.clear();
    lock.lock();
    running_ = false;
    stopping_ = false;
    return Status::ResourceExhausted("work execution", "failed to start worker thread");
  }
  return Status::OK();
}

Status WorkExecutionService::Stop() {
  std::function<void()> stopping_hook;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!running_) return Status::OK();
    stopping_ = true;
    stopping_hook = stopping_hook_;
  }
  if (stopping_hook) stopping_hook();
  work_available_.notify_all();
  for (std::thread& worker : workers_) {
    if (worker.joinable()) worker.join();
  }
  workers_.clear();
  std::lock_guard<std::mutex> lock(mutex_);
  running_ = false;
  stopping_ = false;
  return Status::OK();
}

StatusOr<WorkTaskHandle> WorkExecutionService::Submit(
    WorkClass work_class, std::function<Status()> callback,
    uint64_t deadline_sequence) {
  return Submit(WorkTaskRequest{work_class, {}, false, deadline_sequence},
                std::move(callback));
}

bool WorkExecutionService::IsCurrentWorkerThread() const {
  return current_execution_service == this;
}

Status WorkExecutionService::WaitForTask(const WorkTaskHandle& handle) {
  if (!IsCurrentWorkerThread()) return handle.Wait();
  if (!handle) {
    return Status::InvalidArgument(
        "work execution", "missing task completion handle");
  }
  for (;;) {
    {
      std::lock_guard<std::mutex> completion_lock(handle.completion_->mutex);
      if (handle.completion_->completed) return handle.completion_->status;
    }

    RegisteredTask task;
    bool selected_task = false;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      const auto selected = scheduler_->NextExecutableWork();
      if (selected.has_value()) {
        const auto registered = tasks_.find(selected->id.value);
        if (registered != tasks_.end()) {
          task = std::move(registered->second);
          tasks_.erase(registered);
          running_tasks_.emplace(
              task.id.value,
              RunningTask{task.work_class, task.preemptible,
                          task.cancellation});
          selected_task = true;
        }
      }
    }
    if (selected_task) {
      ExecuteTask(std::move(task));
      continue;
    }
    return handle.Wait();
  }
}

Status WorkExecutionService::ConfigureResourceGovernor(
    ResourceGovernor* resource_governor) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (outstanding_tasks_ != 0) {
    return Status::InvalidArgument(
        "work execution", "cannot reconfigure resource governor with accepted tasks");
  }
  resource_governor_ = resource_governor;
  resource_extension_ = resource_governor == nullptr
      ? nullptr : resource_governor->SharedExtension();
  return Status::OK();
}

StatusOr<WorkTaskHandle> WorkExecutionService::Submit(
    WorkTaskRequest request, std::function<Status()> callback) {
  if (!callback) {
    return Status::InvalidArgument("work execution", "missing task callback");
  }
  std::unique_lock<std::mutex> lock(mutex_);
  const size_t work_class_index = WorkClassIndex(request.work_class);
  ++stats_.submitted[work_class_index];
  if (!running_ || stopping_) {
    ++stats_.rejected[work_class_index];
    return Status::InvalidArgument("work execution", "service is not accepting work");
  }
  if (request.preemptible &&
      preemptible_cancellation_requested_[work_class_index]) {
    if (request.cancellation != nullptr) request.cancellation->Cancel();
    ++stats_.rejected[work_class_index];
    return Status::QueryCancelled(
        "work execution", "optional maintenance class is draining");
  }

  std::optional<ResourceLease> grant;
  if (resource_extension_ != nullptr) {
    auto acquired = resource_extension_->Acquire(
        request.resources, request.commit_critical);
    if (!acquired.ok()) {
      ++stats_.rejected[work_class_index];
      return acquired.status();
    }
    grant.emplace(std::move(acquired).ConsumeValueOrDie());
  }

  try {
    auto allocated = scheduler_->AllocateExecutableTaskId();
    if (!allocated.ok()) {
      ++stats_.rejected[work_class_index];
      return allocated.status();
    }
    const ExecutableTaskId id = allocated.ValueOrDie();
    auto completion = std::make_shared<work_execution_internal::TaskCompletion>();
    std::shared_ptr<WorkCancellation> cancellation = request.cancellation;
    if (cancellation == nullptr) {
      cancellation = std::make_shared<WorkCancellation>();
    }
    const auto inserted = tasks_.emplace(
        id.value, RegisteredTask{id, request.work_class, std::move(callback),
                                 completion, std::move(grant),
                                 request.preemptible,
                                 std::move(cancellation)});
    if (!inserted.second) {
      ++stats_.rejected[work_class_index];
      return Status::ResourceExhausted("work execution", "duplicate task identifier");
    }
    try {
      scheduler_->EnqueueExecutable(
          request.work_class, id, request.deadline_sequence);
    } catch (...) {
      tasks_.erase(inserted.first);
      ++stats_.rejected[work_class_index];
      return Status::ResourceExhausted("work execution", "failed to enqueue task");
    }
    ++stats_.admitted[work_class_index];
    ++outstanding_tasks_;
    lock.unlock();
    work_available_.notify_one();
    return WorkTaskHandle(id, std::move(completion));
  } catch (const std::bad_alloc&) {
    ++stats_.rejected[work_class_index];
    return Status::ResourceExhausted("work execution", "failed to register task");
  } catch (...) {
    ++stats_.rejected[work_class_index];
    return Status::ResourceExhausted("work execution", "failed to register task");
  }
}

void WorkExecutionService::Cancel(ExecutableTaskId id) {
  std::shared_ptr<work_execution_internal::TaskCompletion> completion;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto task = tasks_.find(id.value);
    if (task == tasks_.end()) return;
    ++stats_.cancelled[WorkClassIndex(task->second.work_class)];
    completion = task->second.completion;
    tasks_.erase(task);
    --outstanding_tasks_;
    scheduler_->CancelExecutable(id);
  }
  Complete(completion,
           Status::QueryCancelled("work execution", "task cancelled before execution"));
  work_available_.notify_all();
}

size_t WorkExecutionService::CancelQueued(WorkClass work_class) {
  std::vector<std::shared_ptr<work_execution_internal::TaskCompletion>>
      completions;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    size_t matching_tasks = 0;
    for (const auto& task : tasks_) {
      if (task.second.work_class == work_class) ++matching_tasks;
    }
    completions.reserve(matching_tasks);
    for (auto task = tasks_.begin(); task != tasks_.end();) {
      if (task->second.work_class != work_class) {
        ++task;
        continue;
      }
      scheduler_->CancelExecutable(ExecutableTaskId{task->first});
      ++stats_.cancelled[WorkClassIndex(work_class)];
      --outstanding_tasks_;
      completions.push_back(task->second.completion);
      task = tasks_.erase(task);
    }
  }
  for (const auto& completion : completions) {
    Complete(completion, Status::QueryCancelled(
        "work execution", "queued task cancelled by pressure policy"));
  }
  if (!completions.empty()) work_available_.notify_all();
  return completions.size();
}

size_t WorkExecutionService::CancelPreemptible(WorkClass work_class) {
  std::vector<std::shared_ptr<work_execution_internal::TaskCompletion>>
      completions;
  size_t cancelled = 0;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    preemptible_cancellation_requested_[WorkClassIndex(work_class)] = true;
    size_t matching_tasks = 0;
    for (const auto& task : tasks_) {
      if (task.second.work_class == work_class) {
        ++matching_tasks;
      }
    }
    completions.reserve(matching_tasks);
    for (auto task = tasks_.begin(); task != tasks_.end();) {
      if (task->second.work_class != work_class) {
        ++task;
        continue;
      }
      scheduler_->CancelExecutable(ExecutableTaskId{task->first});
      ++stats_.cancelled[WorkClassIndex(work_class)];
      ++cancelled;
      --outstanding_tasks_;
      completions.push_back(task->second.completion);
      task = tasks_.erase(task);
    }
    for (auto& running : running_tasks_) {
      if (running.second.work_class != work_class ||
          !running.second.preemptible ||
          running.second.cancellation == nullptr) {
        continue;
      }
      if (running.second.cancellation->Cancel()) {
        ++stats_.cancelled[WorkClassIndex(work_class)];
        ++cancelled;
      }
    }
  }
  for (const auto& completion : completions) {
    Complete(completion, Status::QueryCancelled(
        "work execution", "optional maintenance cancelled before execution"));
  }
  if (!completions.empty()) work_available_.notify_all();
  return cancelled;
}

void WorkExecutionService::Complete(
    const std::shared_ptr<work_execution_internal::TaskCompletion>& completion,
    Status status) {
  {
    std::lock_guard<std::mutex> lock(completion->mutex);
    if (completion->completed) return;
    completion->status = std::move(status);
    completion->completed = true;
  }
  completion->ready.notify_all();
}

void WorkExecutionService::ExecuteTask(RegisteredTask task) {
  Status status = InvokeWorkCallback(task.callback);
  task.grant.reset();
  std::lock_guard<std::mutex> lock(mutex_);
  Complete(task.completion, status);
  running_tasks_.erase(task.id.value);
  --outstanding_tasks_;
  ++stats_.completed[WorkClassIndex(task.work_class)];
}

WorkExecutionStats WorkExecutionService::stats() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return stats_;
}

Status WorkExecutionService::ExportMetrics(MetricRegistry* metrics) {
  if (metrics == nullptr) {
    return Status::InvalidArgument("work execution metrics", "missing metric registry");
  }
  static constexpr std::array<const char*, 13> kWorkClassLabels = {
      "commit_critical", "recovery", "shutdown", "foreground_write",
      "point_read", "interactive_query", "flush", "compaction_urgent",
      "analytical_query", "compaction_normal", "index_build", "stats_merge",
      "blob_gc"};
  static constexpr std::array<const char*, 5> kMetricNames = {
      "cedar_scheduler_tasks_submitted_total",
      "cedar_scheduler_tasks_admitted_total",
      "cedar_scheduler_tasks_rejected_total",
      "cedar_scheduler_tasks_cancelled_total",
      "cedar_scheduler_tasks_completed_total"};

  for (const char* metric_name : kMetricNames) {
    const Status registered = metrics->Register(
        MetricDefinition{metric_name, MetricType::kCounter, "count", 1});
    if (!registered.ok()) return registered;
    for (const char* label : kWorkClassLabels) {
      const Status seeded = metrics->AddCounter(metric_name, label, 0);
      if (!seeded.ok()) return seeded;
    }
  }

  std::lock_guard<std::mutex> lock(mutex_);
  const std::array<std::array<uint64_t, 13>*, 5> current = {
      &stats_.submitted, &stats_.admitted, &stats_.rejected,
      &stats_.cancelled, &stats_.completed};
  const std::array<std::array<uint64_t, 13>*, 5> exported = {
      &exported_stats_.submitted, &exported_stats_.admitted,
      &exported_stats_.rejected, &exported_stats_.cancelled,
      &exported_stats_.completed};
  for (size_t metric_index = 0; metric_index < kMetricNames.size();
       ++metric_index) {
    for (size_t class_index = 0; class_index < kWorkClassLabels.size();
         ++class_index) {
      const uint64_t value = (*current[metric_index])[class_index];
      const uint64_t previous = (*exported[metric_index])[class_index];
      const Status updated = metrics->AddCounter(
          kMetricNames[metric_index], kWorkClassLabels[class_index],
          value - previous);
      if (!updated.ok()) return updated;
      (*exported[metric_index])[class_index] = value;
    }
  }
  return Status::OK();
}

void WorkExecutionService::WorkerLoop() {
  current_execution_service = this;
  while (true) {
    RegisteredTask task;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      work_available_.wait(lock, [this] {
        return stopping_ || scheduler_->HasExecutableWork();
      });
      const auto selected = scheduler_->NextExecutableWork();
      if (!selected.has_value()) {
        if (stopping_) {
          current_execution_service = nullptr;
          return;
        }
        continue;
      }
      const auto registered = tasks_.find(selected->id.value);
      if (registered == tasks_.end()) continue;
      task = std::move(registered->second);
      tasks_.erase(registered);
      running_tasks_.emplace(
          task.id.value,
          RunningTask{task.work_class, task.preemptible,
                      task.cancellation});
    }
    ExecuteTask(std::move(task));
  }
}

}  // namespace cedar
