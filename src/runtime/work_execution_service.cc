// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/runtime/work_execution_service.h"

#include <array>
#include <exception>
#include <new>
#include <utility>

namespace cedar {
namespace {

thread_local WorkExecutionService* current_execution_service = nullptr;

std::vector<uint64_t> QueueDelayBounds() {
  static constexpr std::array<uint64_t, 12> kBounds = {
      1'000,       5'000,       10'000,      50'000,
      100'000,     500'000,     1'000'000,   5'000'000,
      10'000'000,  50'000'000,  100'000'000, 1'000'000'000};
  return std::vector<uint64_t>(kBounds.begin(), kBounds.end());
}

uint64_t SaturatingAdd(uint64_t left, uint64_t right) {
  return right > UINT64_MAX - left ? UINT64_MAX : left + right;
}

void AccumulateResources(ResourceProfile* total,
                         const ResourceProfile& addition) {
  total->memory_bytes =
      SaturatingAdd(total->memory_bytes, addition.memory_bytes);
  total->io_tokens = SaturatingAdd(total->io_tokens, addition.io_tokens);
  total->descriptors =
      SaturatingAdd(total->descriptors, addition.descriptors);
  total->temporary_bytes =
      SaturatingAdd(total->temporary_bytes, addition.temporary_bytes);
  total->cpu_slots = SaturatingAdd(total->cpu_slots, addition.cpu_slots);
  total->sequential_read_bytes = SaturatingAdd(
      total->sequential_read_bytes, addition.sequential_read_bytes);
  total->random_read_ops =
      SaturatingAdd(total->random_read_ops, addition.random_read_ops);
  total->write_bytes =
      SaturatingAdd(total->write_bytes, addition.write_bytes);
  total->metadata_ops =
      SaturatingAdd(total->metadata_ops, addition.metadata_ops);
}

uint64_t ResourceDimension(const ResourceProfile& resources, size_t index) {
  switch (index) {
    case 0: return resources.memory_bytes;
    case 1: return resources.io_tokens;
    case 2: return resources.descriptors;
    case 3: return resources.temporary_bytes;
    case 4: return resources.cpu_slots;
    case 5: return resources.sequential_read_bytes;
    case 6: return resources.random_read_ops;
    case 7: return resources.write_bytes;
    case 8: return resources.metadata_ops;
  }
  return 0;
}

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
          RecordDispatchLocked(*selected, registered->second);
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
                                 std::move(cancellation),
                                 request.resources,
                                 std::chrono::steady_clock::now()});
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
    AccumulateResources(&stats_.admitted_resources[work_class_index],
                        request.resources);
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
  const auto service_started = std::chrono::steady_clock::now();
  Status status = InvokeWorkCallback(task.callback);
  const auto service_elapsed =
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - service_started);
  task.grant.reset();
  std::lock_guard<std::mutex> lock(mutex_);
  Complete(task.completion, status);
  running_tasks_.erase(task.id.value);
  --outstanding_tasks_;
  const size_t class_index = WorkClassIndex(task.work_class);
  ++stats_.completed[class_index];
  service_histograms_[class_index].Observe(
      static_cast<uint64_t>(service_elapsed.count()));
}

WorkExecutionStats WorkExecutionService::stats() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return stats_;
}

void WorkExecutionService::InitializeQueueDelayHistograms() {
  const std::vector<uint64_t> bounds = QueueDelayBounds();
  for (size_t index = 0; index < queue_delay_histograms_.size(); ++index) {
    queue_delay_histograms_[index] = Histogram(bounds);
    exported_queue_delay_histograms_[index] = Histogram(bounds);
    service_histograms_[index] = Histogram(bounds);
    exported_service_histograms_[index] = Histogram(bounds);
  }
}

void WorkExecutionService::RecordDispatchLocked(
    const ScheduledExecutableWork& work, const RegisteredTask& task) {
  const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now() - task.enqueued_at);
  const size_t class_index = WorkClassIndex(task.work_class);
  queue_delay_histograms_[class_index].Observe(
      static_cast<uint64_t>(elapsed.count()));
  if (work.deadline_sequence != 0 &&
      work.dispatch_sequence > work.deadline_sequence) {
    ++stats_.deadline_misses[class_index];
  }
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
  static constexpr const char* kQueueDelayMetric =
      "cedar_scheduler_queue_delay_ns";
  static constexpr const char* kDeadlineMissMetric =
      "cedar_scheduler_deadline_misses_total";
  static constexpr const char* kServiceMetric = "cedar_scheduler_service_ns";
  static constexpr std::array<const char*, 9> kGrantMetricNames = {
      "cedar_scheduler_grant_memory_bytes_total",
      "cedar_scheduler_grant_io_tokens_total",
      "cedar_scheduler_grant_descriptors_total",
      "cedar_scheduler_grant_temporary_bytes_total",
      "cedar_scheduler_grant_cpu_slots_total",
      "cedar_scheduler_grant_sequential_read_bytes_total",
      "cedar_scheduler_grant_random_read_ops_total",
      "cedar_scheduler_grant_write_bytes_total",
      "cedar_scheduler_grant_metadata_ops_total"};
  static constexpr std::array<const char*, 9> kGrantMetricUnits = {
      "bytes", "tokens", "descriptors", "bytes", "slots",
      "bytes", "operations", "bytes", "operations"};

  for (const char* metric_name : kMetricNames) {
    const Status registered = metrics->Register(
        MetricDefinition{metric_name, MetricType::kCounter, "count", 1});
    if (!registered.ok()) return registered;
    for (const char* label : kWorkClassLabels) {
      const Status seeded = metrics->AddCounter(metric_name, label, 0);
      if (!seeded.ok()) return seeded;
    }
  }
  const std::vector<uint64_t> queue_delay_bounds = QueueDelayBounds();
  Status registered = metrics->Register(MetricDefinition{
      kQueueDelayMetric, MetricType::kHistogram, "ns", 1,
      queue_delay_bounds});
  if (!registered.ok()) return registered;
  registered = metrics->Register(MetricDefinition{
      kDeadlineMissMetric, MetricType::kCounter, "count", 1});
  if (!registered.ok()) return registered;
  registered = metrics->Register(MetricDefinition{
      kServiceMetric, MetricType::kHistogram, "ns", 1,
      queue_delay_bounds});
  if (!registered.ok()) return registered;
  for (size_t index = 0; index < kGrantMetricNames.size(); ++index) {
    registered = metrics->Register(MetricDefinition{
        kGrantMetricNames[index], MetricType::kCounter,
        kGrantMetricUnits[index], 1});
    if (!registered.ok()) return registered;
  }
  const Histogram empty_queue_delay(queue_delay_bounds);
  for (const char* label : kWorkClassLabels) {
    Status seeded =
        metrics->MergeHistogram(kQueueDelayMetric, label, empty_queue_delay);
    if (!seeded.ok()) return seeded;
    seeded = metrics->AddCounter(kDeadlineMissMetric, label, 0);
    if (!seeded.ok()) return seeded;
    seeded = metrics->MergeHistogram(kServiceMetric, label, empty_queue_delay);
    if (!seeded.ok()) return seeded;
    for (const char* metric_name : kGrantMetricNames) {
      seeded = metrics->AddCounter(metric_name, label, 0);
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
  for (size_t class_index = 0; class_index < kWorkClassLabels.size();
       ++class_index) {
    auto queue_delay_delta = queue_delay_histograms_[class_index].DifferenceFrom(
        exported_queue_delay_histograms_[class_index]);
    if (!queue_delay_delta.ok()) return queue_delay_delta.status();
    Status updated = metrics->MergeHistogram(
        kQueueDelayMetric, kWorkClassLabels[class_index],
        queue_delay_delta.ValueOrDie());
    if (!updated.ok()) return updated;
    const uint64_t deadline_misses = stats_.deadline_misses[class_index];
    const uint64_t previous_deadline_misses =
        exported_stats_.deadline_misses[class_index];
    updated = metrics->AddCounter(
        kDeadlineMissMetric, kWorkClassLabels[class_index],
        deadline_misses - previous_deadline_misses);
    if (!updated.ok()) return updated;
    exported_queue_delay_histograms_[class_index] =
        queue_delay_histograms_[class_index];
    exported_stats_.deadline_misses[class_index] = deadline_misses;

    auto service_delta = service_histograms_[class_index].DifferenceFrom(
        exported_service_histograms_[class_index]);
    if (!service_delta.ok()) return service_delta.status();
    updated = metrics->MergeHistogram(
        kServiceMetric, kWorkClassLabels[class_index],
        service_delta.ValueOrDie());
    if (!updated.ok()) return updated;
    exported_service_histograms_[class_index] = service_histograms_[class_index];

    for (size_t resource_index = 0;
         resource_index < kGrantMetricNames.size(); ++resource_index) {
      const uint64_t value = ResourceDimension(
          stats_.admitted_resources[class_index], resource_index);
      const uint64_t previous = ResourceDimension(
          exported_stats_.admitted_resources[class_index], resource_index);
      updated = metrics->AddCounter(
          kGrantMetricNames[resource_index], kWorkClassLabels[class_index],
          value - previous);
      if (!updated.ok()) return updated;
    }
    exported_stats_.admitted_resources[class_index] =
        stats_.admitted_resources[class_index];
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
      RecordDispatchLocked(*selected, registered->second);
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
