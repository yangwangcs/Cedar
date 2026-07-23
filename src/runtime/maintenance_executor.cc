// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/runtime/maintenance_executor.h"

#include <chrono>
#include <optional>

namespace cedar {
namespace {

uint64_t MonotonicNowNs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count());
}

}  // namespace

Status MaintenanceExecutor::Configure(WorkExecutionService* execution_service,
                                      ResourceGovernor* resources,
                                      IoGovernor* io_governor) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (execution_service != nullptr) {
    const Status configured =
        execution_service->ConfigureResourceGovernor(resources);
    if (!configured.ok()) return configured;
  }
  execution_service_ = execution_service;
  resources_ = resources;
  io_governor_ = io_governor;
  return Status::OK();
}

Status MaintenanceExecutor::Run(const MaintenanceTaskSpec& spec,
                                ResourceGovernor* resources,
                                IoGovernor* io_governor,
                                const std::shared_ptr<WorkCancellation>& cancellation) {
  if (!spec.run) return Status::InvalidArgument("maintenance", "missing task callback");
  std::optional<ResourceLease> lease;
  if (resources != nullptr) {
    auto acquired = resources->Acquire(spec.resources, spec.commit_critical);
    if (!acquired.ok()) return acquired.status();
    lease.emplace(std::move(acquired).ConsumeValueOrDie());
  }
  return RunIoAndCallback(spec, io_governor, cancellation);
}

Status MaintenanceExecutor::RunIoAndCallback(
    const MaintenanceTaskSpec& spec, IoGovernor* io_governor,
    const std::shared_ptr<WorkCancellation>& cancellation) {
  if (!spec.run) return Status::InvalidArgument("maintenance", "missing task callback");
  const Status before_io = cancellation->Checkpoint("maintenance");
  if (!before_io.ok()) return before_io;
  if (io_governor != nullptr) {
    IoTokenRequest request = spec.io;
    request.commit_critical = spec.commit_critical;
    const Status admitted = io_governor->TryAcquire(request, MonotonicNowNs());
    if (!admitted.ok()) return admitted;
  }
  const Status before_callback = cancellation->Checkpoint("maintenance");
  if (!before_callback.ok()) return before_callback;
  return spec.run(cancellation);
}

Status MaintenanceExecutor::SubmitAndRun(MaintenanceTaskSpec spec) {
  WorkExecutionService* execution_service = nullptr;
  ResourceGovernor* resources = nullptr;
  IoGovernor* io_governor = nullptr;
  WorkCancellation::CheckpointObserverForTesting
      cancellation_observer_for_testing;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    execution_service = execution_service_;
    resources = resources_;
    io_governor = io_governor_;
    cancellation_observer_for_testing = cancellation_observer_for_testing_;
  }
  auto cancellation = std::make_shared<WorkCancellation>();
  if (cancellation_observer_for_testing) {
    cancellation->SetCheckpointObserverForTesting(
        std::move(cancellation_observer_for_testing));
  }
  if (execution_service == nullptr) {
    return Run(spec, resources, io_governor, cancellation);
  }
  auto owned_spec = std::make_shared<MaintenanceTaskSpec>(std::move(spec));
  WorkTaskRequest request{owned_spec->work_class, owned_spec->resources,
                          owned_spec->commit_critical, 0,
                          owned_spec->preemptible, cancellation};
  const auto callback = [owned_spec, io_governor, cancellation] {
    return RunIoAndCallback(*owned_spec, io_governor, cancellation);
  };
  const auto submitted = execution_service->Submit(request, callback);
  if (!submitted.ok()) return submitted.status();
  return execution_service->WaitForTask(submitted.ValueOrDie());
}

}  // namespace cedar
