// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_RUNTIME_WORK_EXECUTION_SERVICE_H_
#define CEDAR_RUNTIME_WORK_EXECUTION_SERVICE_H_

#include <array>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/observability/metric_registry.h"
#include "cedar/runtime/resource_profile.h"
#include "cedar/runtime/work_cancellation.h"
#include "cedar/runtime/work_scheduler.h"

namespace cedar {

namespace work_execution_internal {
struct TaskCompletion {
  mutable std::mutex mutex;
  std::condition_variable ready;
  bool completed = false;
  Status status = Status::OK();
};
}  // namespace work_execution_internal

class WorkTaskHandle {
 public:
  WorkTaskHandle() = default;

  ExecutableTaskId id() const { return id_; }
  Status Wait() const;
  explicit operator bool() const { return completion_ != nullptr; }

 private:
  friend class WorkExecutionService;
  WorkTaskHandle(ExecutableTaskId id,
                 std::shared_ptr<work_execution_internal::TaskCompletion> completion)
      : id_(id), completion_(std::move(completion)) {}

  ExecutableTaskId id_;
  std::shared_ptr<work_execution_internal::TaskCompletion> completion_;
};

struct WorkTaskRequest {
  WorkClass work_class = WorkClass::kAnalyticalQuery;
  ResourceProfile resources;
  bool commit_critical = false;
  uint64_t deadline_sequence = 0;
  bool preemptible = false;
  std::shared_ptr<WorkCancellation> cancellation;
};

struct WorkExecutionStats {
  std::array<uint64_t, 13> submitted{};
  std::array<uint64_t, 13> admitted{};
  std::array<uint64_t, 13> rejected{};
  std::array<uint64_t, 13> cancelled{};
  std::array<uint64_t, 13> completed{};
  std::array<uint64_t, 13> deadline_misses{};
  std::array<ResourceProfile, 13> admitted_resources{};
};

class WorkExecutionService {
 public:
  explicit WorkExecutionService(WorkScheduler* scheduler, size_t worker_count = 1)
      : scheduler_(scheduler), worker_count_(worker_count == 0 ? 1 : worker_count) {
    InitializeQueueDelayHistograms();
  }
  explicit WorkExecutionService(std::shared_ptr<WorkScheduler> scheduler,
                                size_t worker_count = 1)
      : owned_scheduler_(std::move(scheduler)),
        scheduler_(owned_scheduler_.get()),
        worker_count_(worker_count == 0 ? 1 : worker_count) {
    InitializeQueueDelayHistograms();
  }
  ~WorkExecutionService();

  WorkExecutionService(const WorkExecutionService&) = delete;
  WorkExecutionService& operator=(const WorkExecutionService&) = delete;

  Status Start();
  Status Stop();
  void SetStoppingHookForTesting(std::function<void()> hook) {
    std::lock_guard<std::mutex> lock(mutex_);
    stopping_hook_ = std::move(hook);
  }
  Status ConfigureResourceGovernor(ResourceGovernor* resource_governor);
  StatusOr<WorkTaskHandle> Submit(WorkClass work_class,
                                  std::function<Status()> callback,
                                  uint64_t deadline_sequence = 0);
  StatusOr<WorkTaskHandle> Submit(WorkTaskRequest request,
                                  std::function<Status()> callback);
  bool IsCurrentWorkerThread() const;
  Status WaitForTask(const WorkTaskHandle& handle);
  void Cancel(ExecutableTaskId id);
  size_t CancelQueued(WorkClass work_class);
  size_t CancelPreemptible(WorkClass work_class);
  WorkExecutionStats stats() const;
  Status ExportMetrics(MetricRegistry* metrics);

 private:
  struct RegisteredTask {
    ExecutableTaskId id;
    WorkClass work_class = WorkClass::kAnalyticalQuery;
    std::function<Status()> callback;
    std::shared_ptr<work_execution_internal::TaskCompletion> completion;
    std::optional<ResourceLease> grant;
    bool preemptible = false;
    std::shared_ptr<WorkCancellation> cancellation;
    ResourceProfile resources;
    std::chrono::steady_clock::time_point enqueued_at;
  };

  struct RunningTask {
    WorkClass work_class = WorkClass::kAnalyticalQuery;
    bool preemptible = false;
    std::shared_ptr<WorkCancellation> cancellation;
  };

  static size_t WorkClassIndex(WorkClass work_class);
  static void Complete(
      const std::shared_ptr<work_execution_internal::TaskCompletion>& completion,
      Status status);
  void ExecuteTask(RegisteredTask task);
  void RecordDispatchLocked(const ScheduledExecutableWork& work,
                            const RegisteredTask& task);
  void InitializeQueueDelayHistograms();
  void WorkerLoop();

  std::shared_ptr<WorkScheduler> owned_scheduler_;
  WorkScheduler* scheduler_ = nullptr;
  size_t worker_count_ = 1;
  mutable std::mutex mutex_;
  std::condition_variable work_available_;
  std::unordered_map<uint64_t, RegisteredTask> tasks_;
  std::unordered_map<uint64_t, RunningTask> running_tasks_;
  std::vector<std::thread> workers_;
  ResourceGovernor* resource_governor_ = nullptr;
  // The extension owns the governor state independently of the database
  // object. This keeps accepted query streams safe when CedarDatabase is
  // destroyed before the stream is drained.
  std::shared_ptr<ResourceGovernorExtension> resource_extension_;
  WorkExecutionStats stats_;
  WorkExecutionStats exported_stats_;
  std::array<Histogram, 13> queue_delay_histograms_;
  std::array<Histogram, 13> exported_queue_delay_histograms_;
  std::array<Histogram, 13> service_histograms_;
  std::array<Histogram, 13> exported_service_histograms_;
  std::array<bool, 13> preemptible_cancellation_requested_{};
  uint64_t outstanding_tasks_ = 0;
  bool running_ = false;
  bool stopping_ = false;
  std::function<void()> stopping_hook_;
};

}  // namespace cedar

#endif  // CEDAR_RUNTIME_WORK_EXECUTION_SERVICE_H_
