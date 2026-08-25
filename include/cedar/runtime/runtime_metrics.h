// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_RUNTIME_RUNTIME_METRICS_H_
#define CEDAR_RUNTIME_RUNTIME_METRICS_H_

#include <cstdint>

namespace cedar {

// Cedar-facing storage and runtime measurements. The backing storage engine
// remains an implementation detail; these fields describe Cedar's workload
// and capacity state rather than a native engine API.
struct RuntimeMetrics {
  uint64_t retained_wal_bytes = 0;
  uint64_t maintenance_snapshot_age_us = 0;
  uint64_t maintenance_flush_grants_accepted = 0;
  uint64_t maintenance_compaction_grants_accepted = 0;
  uint64_t maintenance_flush_grants_requested = 0;
  uint64_t maintenance_completed_grants = 0;
  uint64_t maintenance_flush_wal_sync_yields = 0;
  uint64_t maintenance_flush_deadline_yields = 0;
  uint64_t maintenance_last_flush_queue_depth = 0;
  uint64_t maintenance_last_unscheduled_flushes = 0;
  uint64_t maintenance_last_scheduled_flushes = 0;
  uint64_t maintenance_last_running_flushes = 0;
  uint64_t background_flush_calls = 0;
  uint64_t manual_compaction_calls = 0;
  uint64_t periodic_task_registrations = 0;
  uint64_t recovery_flush_exceptions = 0;
  uint64_t maintenance_errors = 0;
  uint64_t active_fact_bytes = 0;
  uint64_t immutable_fact_bytes = 0;
  uint64_t immutable_fact_count = 0;
  uint64_t l0_file_count = 0;
  uint64_t pending_compaction_bytes = 0;
  uint64_t write_buffer_bytes = 0;
  uint64_t write_buffer_limit_bytes = 0;
  uint64_t background_error_count = 0;
  uint64_t cache_usage_bytes = 0;
  uint64_t cache_pinned_bytes = 0;
  uint64_t running_flushes = 0;
  uint64_t running_compactions = 0;
  uint64_t flush_queue_depth = 0;
  uint64_t unscheduled_flushes = 0;
  uint64_t scheduled_flushes = 0;
  bool facts_flush_pending = false;
  bool facts_compaction_pending = false;
  uint64_t live_fact_bytes = 0;
  uint64_t write_stopped = 0;
  uint64_t delayed_write_rate_bytes_per_sec = 0;
  uint64_t cache_hits = 0;
  uint64_t cache_misses = 0;
  uint64_t compressed_block_count = 0;
  uint64_t compression_input_bytes = 0;
  uint64_t compression_output_bytes = 0;
  uint64_t point_read_operations = 0;
  uint64_t multi_get_operations = 0;
  uint64_t multi_get_batches = 0;
  uint64_t projected_scan_rows = 0;
  uint64_t projected_scan_bytes_read = 0;
  uint64_t projected_scan_pages_skipped = 0;
  uint64_t projected_scan_pages_read = 0;
  uint64_t projected_scan_physical_bytes_read = 0;
  uint64_t canonical_scan_bytes_read = 0;
  uint64_t canonical_read_physical_bytes = 0;
  uint64_t logical_facts_bytes = 0;
  uint64_t obsolete_fact_bytes = 0;
  uint64_t temporary_output_bytes = 0;
  uint64_t free_disk_bytes = UINT64_MAX;
  uint64_t free_disk_percent = 100;
};

}  // namespace cedar

#endif  // CEDAR_RUNTIME_RUNTIME_METRICS_H_
