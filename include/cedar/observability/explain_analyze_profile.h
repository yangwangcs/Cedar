// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_OBSERVABILITY_EXPLAIN_ANALYZE_PROFILE_H_
#define CEDAR_OBSERVABILITY_EXPLAIN_ANALYZE_PROFILE_H_

#include <cstdint>
#include <map>
#include <string>

#include "cedar/tcypher/physical_plan.h"
#include "cedar/observability/operator_runtime_stats.h"
#include "cedar/optimizer/runtime_feedback.h"

namespace cedar {

struct ExplainAnalyzeRuntimeProfile {
  uint64_t snapshot_seq = 0;
  uint64_t version_set_generation = 0;
  uint64_t catalog_generation = 0;
  uint64_t statistics_snapshot_id = 0;
  uint64_t executed_physical_plan_id = 0;
  uint64_t result_batches = 0;
  uint64_t output_rows = 0;
  std::string terminal_status = "OK";

  uint64_t base_events_visited = 0;
  uint64_t sst_blocks_read = 0;
  uint64_t sst_physical_bytes_read = 0;
  uint64_t page_bytes_decoded = 0;
  uint64_t page_bytes_skipped = 0;
  uint64_t page_decode_count = 0;
  uint64_t page_decode_latency_ns = 0;
  uint64_t root_sst_blocks_read = 0;
  uint64_t boundary_sst_blocks_read = 0;
  uint64_t projection_payload_bytes_copied = 0;

  uint64_t memtable_delta_probes = 0;
  uint64_t memtable_delta_candidates = 0;
  uint64_t index_candidate_items_processed = 0;
  uint64_t index_advisory_fallbacks = 0;
  uint64_t index_sidecar_bytes_read = 0;
  bool has_runtime_feedback = false;
  SelectivityBucket runtime_feedback_bucket =
      SelectivityBucket::kNonSelective;
  uint64_t runtime_feedback_observations = 0;
  bool runtime_feedback_applied = false;
  CandidateSource runtime_feedback_source = CandidateSource::kBase;
  uint64_t runtime_feedback_base_rows = 0;
  uint64_t runtime_feedback_candidate_rows = 0;
  uint64_t index_adaptive_reoptimizations = 0;
  uint64_t index_adaptive_intersection_predicates_dropped = 0;
  uint64_t index_adaptive_unopened_fragments_skipped = 0;
  uint64_t index_adaptive_unopened_delta_sources_skipped = 0;
  uint64_t index_adaptive_sampled_candidates = 0;
  uint64_t index_dynamic_filter_input_rows = 0;
  uint64_t index_dynamic_filter_rejected_rows = 0;
  uint64_t index_dynamic_filter_output_rows = 0;

  bool has_selected_access_path = false;
  CandidateSource selected_access_path = CandidateSource::kBase;
  bool has_executed_access_path = false;
  CandidateSource executed_access_path = CandidateSource::kBase;
  uint64_t selected_access_path_score = 0;
  CostVector selected_access_path_cost;
  std::string selected_access_path_rationale;
  bool access_path_fallback = false;
  bool has_selected_graph_order = false;
  GraphOrder selected_graph_order = GraphOrder::kAdjacencyFirst;
  bool has_executed_graph_order = false;
  GraphOrder executed_graph_order = GraphOrder::kAdjacencyFirst;

  uint64_t memory_used_bytes = 0;
  uint64_t memory_peak_bytes = 0;
  uint64_t memory_soft_limit_bytes = 0;
  uint64_t memory_hard_limit_bytes = 0;

  uint64_t pipelines_built = 0;
  uint64_t morsels_scheduled = 0;
  uint64_t morsels_completed = 0;
  uint64_t scheduler_dispatches = 0;
  uint64_t result_queue_high_water = 0;
  uint64_t physical_output_rows = 0;
  uint64_t pipeline_reoptimization_checks = 0;
  uint64_t pipeline_reoptimizations = 0;
  uint64_t pipeline_reoptimization_sampled_rows = 0;
  uint64_t pipeline_reoptimization_sampled_batches = 0;
  uint64_t pipeline_reoptimization_prefix_memory_bytes = 0;

  uint64_t path_frontier_hops = 0;
  uint64_t path_frontier_input_states = 0;
  uint64_t path_frontier_output_states = 0;
  uint64_t path_frontier_completed_paths = 0;
  uint64_t path_frontier_repartitions = 0;
  uint64_t path_frontier_partitions = 0;
  uint64_t path_frontier_max_partition_size = 0;
  uint64_t path_frontier_spill_starts = 0;
  uint64_t path_frontier_spill_bytes = 0;

  uint64_t hash_join_build_input_rows = 0;
  uint64_t hash_join_probe_input_rows = 0;
  uint64_t hash_join_spill_starts = 0;
  uint64_t hash_join_dynamic_filter_input_rows = 0;
  uint64_t hash_join_dynamic_filter_rejected_rows = 0;
  uint64_t hash_join_dynamic_filter_output_rows = 0;
  uint64_t hash_join_dynamic_filter_spill_rows_avoided = 0;
  uint64_t hash_join_dynamic_filter_memory_bytes = 0;
  uint64_t hash_join_dynamic_filter_memory_disables = 0;
  uint64_t hash_join_build_side_switches = 0;
  std::map<OperatorRuntimeKey, OperatorRuntimeCounters> operator_counters;
};

std::string SerializeExplainAnalyzeProfile(
    const PhysicalPlan& plan, const ExplainAnalyzeRuntimeProfile& runtime);
std::string SerializeExplainAnalyzeProfile(
    const PhysicalHashJoinPlan& plan,
    const ExplainAnalyzeRuntimeProfile& runtime);
std::string SerializeExplainAnalyzeProfile(
    const PhysicalMultiHashJoinPlan& plan,
    const ExplainAnalyzeRuntimeProfile& runtime);

}  // namespace cedar

#endif  // CEDAR_OBSERVABILITY_EXPLAIN_ANALYZE_PROFILE_H_
