// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_KERNEL_MAINTENANCE_CONTROLLER_H_
#define CEDAR_KERNEL_MAINTENANCE_CONTROLLER_H_

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include "kernel/maintenance_policy.h"

namespace cedar {

class MaintenanceAdapter {
 public:
  virtual ~MaintenanceAdapter() = default;
  virtual StatusOr<CedarMaintenanceCompletion> RunFlush(
      const CedarMaintenanceDecision& decision,
      const std::atomic<bool>* wal_sync_critical) = 0;
  virtual StatusOr<CedarMaintenanceCompletion> RunCompaction(
      const CedarMaintenanceDecision& decision,
      const std::atomic<bool>* wal_sync_critical) = 0;
};

class MaintenanceController {
 public:
  explicit MaintenanceController(MaintenanceAdapter* adapter);
  ~MaintenanceController();

  MaintenanceController(const MaintenanceController&) = delete;
  MaintenanceController& operator=(const MaintenanceController&) = delete;

  Status Start();
  void PublishSnapshot(CedarRuntimeSnapshot snapshot);
  void SetWalSyncCritical(bool critical);
  void Stop();
  CedarMaintenanceMetrics metrics() const;

 private:
  enum class Lane : uint8_t { kFlush, kCompaction };

  void RunLane(Lane lane);
  std::optional<CedarMaintenanceDecision> NextDecisionLocked(Lane lane) const;
  bool LaneHasWorkLocked(Lane lane) const;
  void CompleteLocked(const CedarMaintenanceCompletion& completion);

  MaintenanceAdapter* adapter_ = nullptr;
  mutable std::mutex mutex_;
  std::condition_variable work_available_;
  std::vector<std::thread> flush_workers_;
  std::vector<std::thread> compaction_workers_;
  std::optional<CedarRuntimeSnapshot> snapshot_;
  CedarMaintenanceHistory history_;
  CedarMaintenanceMetrics metrics_;
  uint64_t published_sequence_ = 0;
  uint64_t flush_dispatched_sequence_ = 0;
  uint32_t flush_dispatched_credits_ = 0;
  uint64_t compaction_dispatched_sequence_ = 0;
  uint32_t compaction_dispatched_credits_ = 0;
  uint32_t flush_outstanding_ = 0;
  uint32_t compaction_outstanding_ = 0;
  bool started_ = false;
  bool stopping_ = false;
  std::atomic<bool> wal_sync_critical_{false};
};

}  // namespace cedar

#endif  // CEDAR_KERNEL_MAINTENANCE_CONTROLLER_H_
