// Copyright 2026 The Cedar Authors
#pragma once

#include <atomic>
#include <cstdint>
#include <vector>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class DB;

enum class CedarColumnFamilyRole : uint8_t {
  kDefault,
  kFacts,
  kMeta,
  kOther,
};

// Cedar grants are one-shot permissions for RocksDB's native maintenance
// queues. deadline_us is a relative timeout from submission.
enum class CedarMaintenanceKind : uint8_t {
  kFlush,
  kCompaction,
};

enum class CedarMaintenancePriority : uint8_t {
  kBackground,
  kNormal,
  kEmergency,
};

enum class CedarMaintenanceYield : uint8_t {
  kNone,
  kNoDebt,
  kInputBudget,
  kOutputBudget,
  kWalSync,
  kManualConflict,
  kDeadline,
  kRecovery,
  kShutdown,
  kError,
};

struct CedarMaintenanceGrant {
  uint64_t snapshot_generation = 0;
  CedarMaintenanceKind kind = CedarMaintenanceKind::kFlush;
  CedarMaintenancePriority priority = CedarMaintenancePriority::kNormal;
  uint64_t max_input_bytes = 0;
  uint64_t max_output_bytes = 0;
  uint64_t deadline_us = 0;
  const std::atomic<bool>* wal_sync_critical = nullptr;
};

struct CedarMaintenanceResult {
  uint64_t grant_id = 0;
  uint64_t input_bytes = 0;
  uint64_t output_bytes = 0;
  uint32_t selected_column_family_id = 0;
  CedarMaintenanceYield yield = CedarMaintenanceYield::kNone;
  uint64_t remaining_smallest_complete_unit_bytes = 0;
  uint64_t atomic_overrun_bytes = 0;
  uint64_t elapsed_us = 0;
  uint64_t flush_queue_depth = 0;
  uint64_t unscheduled_flushes = 0;
  uint64_t scheduled_flushes = 0;
  uint64_t running_flushes = 0;
  bool input_budget_exceeded = false;
  bool output_budget_exceeded = false;
  bool recovery_exception = false;
};

struct CedarColumnFamilyDebt {
  uint32_t id = 0;
  CedarColumnFamilyRole role = CedarColumnFamilyRole::kOther;
  uint64_t active_memtable_bytes = 0;
  uint64_t immutable_memtable_bytes = 0;
  uint64_t immutable_memtable_count = 0;
  uint64_t oldest_immutable_age_us = 0;
  uint64_t l0_files = 0;
  uint64_t pending_compaction_bytes = 0;
  bool flush_pending = false;
  bool compaction_pending = false;
};

struct CedarMaintenanceSnapshot {
  uint64_t generation = 0;
  uint64_t sampled_at_us = 0;
  uint64_t total_active_memtable_bytes = 0;
  uint64_t total_immutable_memtable_bytes = 0;
  uint64_t total_immutable_memtable_count = 0;
  uint64_t write_buffer_manager_bytes = 0;
  uint64_t write_buffer_manager_limit_bytes = 0;
  uint64_t retained_wal_bytes = 0;
  uint64_t total_l0_files = 0;
  uint64_t total_pending_compaction_bytes = 0;
  uint64_t running_flushes = 0;
  uint64_t running_compactions = 0;
  uint64_t flush_queue_depth = 0;
  uint64_t unscheduled_flushes = 0;
  uint64_t scheduled_flushes = 0;
  uint64_t background_errors = 0;
  bool write_delayed = false;
  bool write_stopped = false;
  bool manual_conflict = false;
  bool recovery_in_progress = false;
  bool shutting_down = false;
  uint64_t block_cache_usage_bytes = 0;
  uint64_t block_cache_pinned_bytes = 0;
  uint64_t live_sst_bytes = 0;
  uint64_t blob_file_bytes = 0;
  uint64_t delayed_write_rate_bytes_per_sec = 0;
  uint64_t maybe_schedule_flush_or_compaction_calls = 0;
  uint64_t background_flush_calls = 0;
  uint64_t manual_compaction_calls = 0;
  uint64_t periodic_task_registrations = 0;
  uint64_t recovery_flush_exceptions = 0;
  std::vector<CedarColumnFamilyDebt> column_families;
};

Status PollCedarMaintenance(DB* db, CedarMaintenanceSnapshot* snapshot);
Status RunCedarMaintenance(DB* db, const CedarMaintenanceGrant& grant,
                           CedarMaintenanceResult* result);

}  // namespace ROCKSDB_NAMESPACE
