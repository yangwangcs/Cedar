// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_RUNTIME_MAINTENANCE_EXECUTOR_H_
#define CEDAR_RUNTIME_MAINTENANCE_EXECUTOR_H_

#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <utility>

#include "cedar/core/status.h"
#include "cedar/runtime/io_governor.h"
#include "cedar/runtime/resource_profile.h"
#include "cedar/runtime/work_execution_service.h"

namespace cedar {

struct MaintenanceTaskSpec {
  WorkClass work_class = WorkClass::kCompactionNormal;
  ResourceProfile resources;
  IoTokenRequest io;
  bool commit_critical = false;
  bool preemptible = false;
  std::function<Status(const std::shared_ptr<WorkCancellation>&)> run;
};

// A single-node maintenance dispatcher.  It intentionally executes selected
// callbacks synchronously: correctness-sensitive callers retain ownership of
// their FrozenMemTable or VersionSet pins while scheduling, while all resource
// admission and queue selection still use the one shared scheduler/governors.
class MaintenanceExecutor {
 public:
  Status Configure(WorkExecutionService* execution_service,
                   ResourceGovernor* resources,
                   IoGovernor* io_governor);
  void SetCancellationObserverForTesting(
      WorkCancellation::CheckpointObserverForTesting observer) {
    std::lock_guard<std::mutex> lock(mutex_);
    cancellation_observer_for_testing_ = std::move(observer);
  }

  Status SubmitAndRun(MaintenanceTaskSpec spec);

 private:
  static Status Run(const MaintenanceTaskSpec& spec,
                    ResourceGovernor* resources,
                    IoGovernor* io_governor,
                    const std::shared_ptr<WorkCancellation>& cancellation);
  static Status RunIoAndCallback(const MaintenanceTaskSpec& spec,
                                 IoGovernor* io_governor,
                                 const std::shared_ptr<WorkCancellation>& cancellation);

  mutable std::mutex mutex_;
  WorkExecutionService* execution_service_ = nullptr;
  ResourceGovernor* resources_ = nullptr;
  IoGovernor* io_governor_ = nullptr;
  WorkCancellation::CheckpointObserverForTesting
      cancellation_observer_for_testing_;
};

}  // namespace cedar

#endif  // CEDAR_RUNTIME_MAINTENANCE_EXECUTOR_H_
