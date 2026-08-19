// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "kernel/maintenance_policy.h"

#include <algorithm>
#include <limits>

namespace cedar {
namespace {

constexpr uint64_t kBaselineBudgetBytes = 8ULL * 1024ULL * 1024ULL;
constexpr uint64_t kNormalFlushBytes = 1ULL * 1024ULL * 1024ULL;
constexpr uint64_t kEmergencyFlushBytes = 64ULL * 1024ULL * 1024ULL;
constexpr uint64_t kSoftWalBytes = 768ULL * 1024ULL * 1024ULL;
constexpr uint64_t kNormalL0Files = 4;
constexpr uint64_t kEmergencyL0Files = 24;
constexpr uint64_t kNormalWbmPercent = 70;
constexpr uint64_t kEmergencyWbmPercent = 85;
constexpr uint64_t kGrantDeadlineUs = 5'000'000;

uint64_t SaturatingAdd(uint64_t left, uint64_t right) {
  return left > std::numeric_limits<uint64_t>::max() - right
             ? std::numeric_limits<uint64_t>::max()
             : left + right;
}

uint64_t PercentCeiling(uint64_t value, uint64_t percent) {
  const uint64_t quotient = value / 100;
  const uint64_t remainder = value % 100;
  return SaturatingAdd(quotient * percent,
                       (remainder * percent + 99) / 100);
}

bool WbmAtLeast(const RocksDbRuntimeMetrics& metrics, uint64_t percent) {
  return metrics.write_buffer_manager_limit_bytes != 0 &&
         metrics.write_buffer_manager_bytes >=
             PercentCeiling(metrics.write_buffer_manager_limit_bytes, percent);
}

uint64_t NextBudget(
    uint64_t baseline,
    const std::optional<CedarMaintenanceCompletion>& prior) {
  if (!prior.has_value() ||
      (prior->yield != CedarMaintenanceYield::kInputBudget &&
       prior->yield != CedarMaintenanceYield::kOutputBudget)) {
    return baseline;
  }
  return std::max(baseline, prior->remaining_smallest_complete_unit_bytes);
}

uint64_t FlushUnitBytes(const RocksDbRuntimeMetrics& metrics) {
  uint64_t largest = 0;
  for (const auto& column_family : metrics.column_families) {
    largest = std::max(largest, SaturatingAdd(column_family.active_memtable_bytes,
                                               column_family.immutable_memtable_bytes));
  }
  return std::max(largest, SaturatingAdd(metrics.total_active_memtable_bytes,
                                          metrics.total_immutable_memtable_bytes));
}

CedarMaintenanceDecision FlushDecision(CedarMaintenancePriority priority,
                                        uint64_t budget,
                                        bool yield_for_wal_sync = true) {
  return {CedarMaintenanceKind::kFlush, priority, budget, budget,
          kGrantDeadlineUs, 0, yield_for_wal_sync};
}

CedarMaintenanceDecision CompactionDecision(CedarMaintenancePriority priority,
                                             uint64_t budget) {
  return {CedarMaintenanceKind::kCompaction, priority, budget, budget,
          kGrantDeadlineUs, 0, true};
}

}  // namespace

CedarMaintenancePlan SelectCedarMaintenance(
    const CedarRuntimeSnapshot& snapshot,
    const CedarMaintenanceHistory& history,
    bool wal_sync_critical) {
  CedarMaintenancePlan plan;
  const RocksDbRuntimeMetrics& metrics = snapshot.rocksdb;

  // These states are owned by RocksDB lifecycle/recovery. Cedar must not issue
  // grants that race recovery, shutdown, manual operations, or a failed DB.
  if (metrics.background_errors != 0 || metrics.background_errors_total != 0 ||
      snapshot.pressure.background_error != 0 || metrics.manual_conflict ||
      metrics.recovery_in_progress || metrics.shutting_down) {
    return plan;
  }

  const uint64_t flush_budget = std::max(
      NextBudget(kBaselineBudgetBytes, history.last_flush), FlushUnitBytes(metrics));
  const uint64_t compaction_budget =
      NextBudget(kBaselineBudgetBytes, history.last_compaction);
  const bool emergency_flush = metrics.write_stopped != 0 ||
      WbmAtLeast(metrics, kEmergencyWbmPercent) ||
      metrics.total_immutable_memtable_bytes >= kEmergencyFlushBytes;
  const bool normal_flush = WbmAtLeast(metrics, kNormalWbmPercent) ||
      metrics.total_immutable_memtable_bytes >= kNormalFlushBytes ||
      metrics.retained_wal_bytes >= kSoftWalBytes;

  if (emergency_flush) {
    plan.flush = FlushDecision(CedarMaintenancePriority::kEmergency,
                               flush_budget, metrics.write_stopped == 0);
  } else if (normal_flush) {
    plan.flush = FlushDecision(CedarMaintenancePriority::kNormal, flush_budget);
  }

  const bool emergency_compaction = metrics.total_l0_files >= kEmergencyL0Files ||
      (metrics.write_stopped != 0 && metrics.total_pending_compaction_bytes != 0);
  const bool normal_compaction = metrics.total_l0_files >= kNormalL0Files ||
      metrics.total_pending_compaction_bytes != 0;
  if (emergency_compaction) {
    plan.compaction = CompactionDecision(CedarMaintenancePriority::kEmergency,
                                         compaction_budget);
  } else if (!wal_sync_critical && normal_compaction) {
    plan.compaction = CompactionDecision(CedarMaintenancePriority::kNormal,
                                         compaction_budget);
  }
  if (plan.flush.has_value()) plan.flush->snapshot_generation = snapshot.generation;
  if (plan.compaction.has_value()) {
    plan.compaction->snapshot_generation = snapshot.generation;
  }
  return plan;
}

}  // namespace cedar
