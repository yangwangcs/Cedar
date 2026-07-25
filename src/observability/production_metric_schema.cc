// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/observability/production_metric_schema.h"

#include <array>

namespace cedar {
namespace {

std::vector<uint64_t> LatencyBounds() {
  static constexpr std::array<uint64_t, 12> kBounds = {
      1'000, 5'000, 10'000, 50'000, 100'000, 500'000,
      1'000'000, 5'000'000, 10'000'000, 50'000'000,
      100'000'000, 1'000'000'000};
  return std::vector<uint64_t>(kBounds.begin(), kBounds.end());
}

MetricDefinition Counter(const char* name, const char* unit = "count") {
  return MetricDefinition{name, MetricType::kCounter, unit, 1};
}

MetricDefinition Gauge(const char* name, const char* unit) {
  return MetricDefinition{name, MetricType::kGauge, unit, 1};
}

MetricDefinition Latency(const char* name) {
  return MetricDefinition{name, MetricType::kHistogram, "ns", 1,
                          LatencyBounds()};
}

}  // namespace

const std::vector<MetricDefinition>& ProductionMetricDefinitions() {
  static const std::vector<MetricDefinition> definitions = {
      Counter("cedar_txn_started_total"),
      Counter("cedar_txn_committed_total"),
      Counter("cedar_txn_aborted_total"),
      Counter("cedar_txn_indeterminate_total"),
      Counter("cedar_txn_conflict_total"),
      Latency("cedar_txn_commit_latency_ns"),
      Latency("cedar_txn_prepare_latency_ns"),
      Latency("cedar_txn_decision_latency_ns"),
      Latency("cedar_wal_fsync_latency_ns"),
      Latency("cedar_decisionlog_fsync_latency_ns"),
      Latency("cedar_txn_visible_prefix_stall_ns"),
      Gauge("cedar_txn_visible_prefix_lag_seq", "sequence"),
      Counter("cedar_wal_append_bytes_total", "bytes"),
      Gauge("cedar_prepared_txn_age_ns", "ns"),
      Counter("cedar_commit_completion_stall_total"),

      Counter("cedar_query_started_total"),
      Counter("cedar_query_completed_total"),
      Counter("cedar_query_failed_total"),
      Counter("cedar_query_cancelled_total"),
      Latency("cedar_query_queue_delay_ns"),
      Latency("cedar_query_latency_ns"),
      Gauge("cedar_query_snapshot_age_ns", "ns"),
      Counter("cedar_query_result_rows_total", "rows"),
      Counter("cedar_query_result_intervals_total", "intervals"),
      Counter("cedar_query_spill_bytes_total", "bytes"),
      Gauge("cedar_query_memory_peak_bytes", "bytes"),
      Counter("cedar_query_plan_cache_hit_total"),
      Counter("cedar_query_admission_reject_total"),

      Counter("cedar_operator_input_rows_total", "rows"),
      Counter("cedar_operator_output_rows_total", "rows"),
      Counter("cedar_operator_input_intervals_total", "intervals"),
      Counter("cedar_operator_output_intervals_total", "intervals"),
      Counter("cedar_operator_batches_total", "batches"),
      Counter("cedar_operator_cpu_ns", "ns"),
      Counter("cedar_operator_blocked_ns", "ns"),
      Gauge("cedar_operator_memory_peak_bytes", "bytes"),
      Counter("cedar_operator_selection_rows_total", "rows"),
      Gauge("cedar_path_frontier_rows", "rows"),
      Counter("cedar_expand_candidates_total", "rows"),
      Counter("cedar_expand_emitted_total", "rows"),
      Counter("cedar_temporal_intersection_empty_total"),

      Counter("cedar_sst_files_considered_total", "files"),
      Counter("cedar_sst_files_pruned_total", "files"),
      Counter("cedar_blocks_considered_total", "blocks"),
      Counter("cedar_blocks_pruned_total", "blocks"),
      Counter("cedar_pages_read_total", "pages"),
      Counter("cedar_pages_decoded_total", "pages"),
      Counter("cedar_page_read_bytes_total", "bytes"),
      Counter("cedar_page_decode_bytes_total", "bytes"),
      Counter("cedar_page_corruption_total"),
      Counter("cedar_property_gather_requests_total"),
      Counter("cedar_blob_refs_seen_total"),
      Counter("cedar_blob_payload_reads_total"),
      Counter("cedar_blob_payload_bytes_total", "bytes"),
      Counter("cedar_blob_hash_lookup_total"),
      Counter("cedar_blob_cache_hit_total"),

      Counter("cedar_index_probe_total"),
      Counter("cedar_index_candidate_rows_total", "rows"),
      Counter("cedar_index_validated_rows_total", "rows"),
      Counter("cedar_index_false_candidate_rows_total", "rows"),
      Counter("cedar_index_fallback_scan_total"),
      Gauge("cedar_index_coverage_ratio", "per_mille"),
      Counter("cedar_index_sidecar_build_bytes_total", "bytes"),
      Counter("cedar_index_sidecar_repair_total"),
      Counter("cedar_optimizer_plan_time_ns", "ns"),
      Gauge("cedar_optimizer_memo_groups", "groups"),
      Gauge("cedar_optimizer_alternatives", "alternatives"),
      Counter("cedar_optimizer_budget_exhausted_total"),
      Gauge("cedar_optimizer_estimate_error_ratio", "per_mille"),
      Counter("cedar_optimizer_runtime_switch_total"),

      Latency("cedar_scheduler_queue_delay_ns"),
      Latency("cedar_scheduler_service_ns"),
      Counter("cedar_scheduler_deadline_misses_total"),
      Counter("cedar_scheduler_grant_memory_bytes_total", "bytes"),
      Counter("cedar_scheduler_grant_io_tokens_total", "tokens"),
      Counter("cedar_scheduler_grant_descriptors_total", "descriptors"),
      Counter("cedar_scheduler_grant_temporary_bytes_total", "bytes"),
      Counter("cedar_scheduler_grant_cpu_slots_total", "slots"),
      Counter("cedar_scheduler_grant_sequential_read_bytes_total", "bytes"),
      Counter("cedar_scheduler_grant_random_read_ops_total", "operations"),
      Counter("cedar_scheduler_grant_write_bytes_total", "bytes"),
      Counter("cedar_scheduler_grant_metadata_ops_total", "operations"),
      Counter("cedar_scheduler_admission_reject_total"),
      Counter("cedar_scheduler_pressure_transition_total"),
      Counter("cedar_scheduler_write_stall_ns", "ns"),
      Gauge("cedar_memory_pool_bytes", "bytes"),
      Counter("cedar_memory_reservation_fail_total"),
      Counter("cedar_io_tokens_consumed_total", "tokens"),
      Counter("cedar_cache_hit_total"),
      Counter("cedar_cache_bypass_total"),
      Counter("cedar_cache_eviction_total"),
      Gauge("cedar_cache_pinned_bytes", "bytes"),
      Gauge("cedar_cache_resident_peak_bytes", "bytes"),
      Counter("cedar_flush_bytes_total", "bytes"),
      Counter("cedar_compaction_input_bytes_total", "bytes"),
      Counter("cedar_compaction_output_bytes_total", "bytes"),
      Gauge("cedar_compaction_write_amplification", "per_mille"),
      Gauge("cedar_compaction_buffer_peak_bytes", "bytes"),
      Gauge("cedar_compaction_buffer_peak_events", "events"),
      Counter("cedar_blob_gc_relocated_bytes_total", "bytes"),
  };
  return definitions;
}

Status RegisterProductionMetricSchema(MetricRegistry* registry) {
  if (registry == nullptr) {
    return Status::InvalidArgument("production metric schema",
                                   "missing metric registry");
  }
  for (const MetricDefinition& definition : ProductionMetricDefinitions()) {
    const Status status = registry->Register(definition);
    if (!status.ok()) return status;
  }
  return Status::OK();
}

}  // namespace cedar
