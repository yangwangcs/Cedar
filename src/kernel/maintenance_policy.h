// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_KERNEL_MAINTENANCE_POLICY_H_
#define CEDAR_KERNEL_MAINTENANCE_POLICY_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>

#include "cedar/core/status.h"
#include "storage/facts/fact_store.h"
#include "cedar/runtime/pressure_controller.h"

namespace cedar {

enum class CedarMaintenanceKind : uint8_t { kFlush = 0, kCompaction };

enum class CedarMaintenancePriority : uint8_t { kNone = 0, kNormal, kEmergency };

enum class CedarMaintenanceYield : uint8_t {
  kNone = 0,
  kNoDebt,
  kStaleGeneration,
  kInputBudget,
  kOutputBudget,
  kDeadline,
  kWalSync,
  kManualConflict,
  kRecovery,
  kShutdown,
  kInvariantViolation,
};

struct CedarRuntimeSnapshot {
  uint64_t generation = 0;
  uint64_t sampled_at_us = 0;
  PressureSample pressure;
  RocksDbRuntimeMetrics rocksdb;
  PressureState pressure_state = PressureState::kNormal;
};

struct CedarMaintenanceCompletion {
  uint64_t grant_id = 0;
  CedarMaintenanceKind kind = CedarMaintenanceKind::kFlush;
  CedarMaintenanceYield yield = CedarMaintenanceYield::kNone;
  uint64_t input_bytes = 0;
  uint64_t output_bytes = 0;
  uint64_t elapsed_us = 0;
  uint64_t remaining_smallest_complete_unit_bytes = 0;
  Status status = Status::OK();
};

struct CedarMaintenanceDecision {
  CedarMaintenanceKind kind = CedarMaintenanceKind::kFlush;
  CedarMaintenancePriority priority = CedarMaintenancePriority::kNone;
  uint64_t max_input_bytes = 0;
  uint64_t max_output_bytes = 0;
  uint64_t deadline_us = 0;
  uint64_t snapshot_generation = 0;
  bool yield_for_wal_sync = true;
};

struct CedarMaintenancePlan {
  std::optional<CedarMaintenanceDecision> flush;
  std::optional<CedarMaintenanceDecision> compaction;
};

struct CedarMaintenanceHistory {
  std::optional<CedarMaintenanceCompletion> last_flush;
  std::optional<CedarMaintenanceCompletion> last_compaction;
};

inline constexpr size_t kCedarMaintenanceYieldCount = 11;

struct CedarMaintenanceMetrics {
  uint64_t snapshots_published = 0;
  uint64_t flush_grants_requested = 0;
  uint64_t flush_grants_accepted = 0;
  uint64_t compaction_grants_requested = 0;
  uint64_t compaction_grants_accepted = 0;
  uint64_t stale_grants = 0;
  uint64_t completed_grants = 0;
  uint64_t queue_delay_us = 0;
  uint64_t input_bytes = 0;
  uint64_t output_bytes = 0;
  uint64_t atomic_overrun_bytes = 0;
  uint64_t recovery_exception_jobs = 0;
  uint64_t unexplained_autonomous_jobs = 0;
  uint64_t maintenance_errors = 0;
  uint64_t max_snapshot_age_us = 0;
  std::optional<Status> first_error;
  std::array<uint64_t, kCedarMaintenanceYieldCount> yields{};
};

CedarMaintenancePlan SelectCedarMaintenance(
    const CedarRuntimeSnapshot& snapshot,
    const CedarMaintenanceHistory& history,
    bool wal_sync_critical);

}  // namespace cedar

#endif  // CEDAR_KERNEL_MAINTENANCE_POLICY_H_
