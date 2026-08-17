// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TCYPHER_EXECUTOR_H_
#define CEDAR_TCYPHER_EXECUTOR_H_

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/index/index_catalog.h"
#include "cedar/index/memtable_delta_index.h"
#include "cedar/index/index_sidecar.h"
#include "cedar/observability/operator_runtime_stats.h"
#include "cedar/optimizer/runtime_feedback.h"
#include "cedar/schema/schema_registry.h"
#include "cedar/statistics/stats_snapshot.h"
#include "cedar/storage/temporal_event.h"
#include "cedar/storage/temporal_memtable.h"
#include "cedar/tcypher/runtime/query_result.h"
#include "cedar/tcypher/runtime/cancellation.h"
#include "cedar/tcypher/runtime/query_memory.h"
#include "cedar/tcypher/storage/temporal_scan.h"
#include "cedar/transaction/commit_timeline.h"

namespace cedar {

class TransactionCoordinator;
class TcypherSession;
class WorkExecutionService;
class ResourceGovernorExtension;
class IoGovernor;

// Immutable source rows and the already-validated sidecar selected from the
// same pinned VersionSnapshot. Callers that cannot provide this pairing leave
// index_sources empty and execute the correct base path.
struct TcypherIndexSource {
  uint64_t index_id = 0;
  uint64_t source_sst_id = 0;
  std::vector<TemporalEvent> events;
  IndexSidecar sidecar;
  std::optional<PinnedSstSource> pinned_sst_source;
  std::optional<IndexDefinition> definition;
  std::optional<IndexFragment> fragment;
  std::string sidecar_path;
  std::map<uint64_t, TemporalEvent> loaded_events_by_ordinal;
  std::vector<std::shared_ptr<void>> ordinal_read_retentions;
  std::optional<uint64_t> validated_source_row_count;
};

// Query-private snapshot of the rebuildable active/frozen MemTable index.
// Ordinals always address `events` from the same captured generation.
struct TcypherDeltaIndexSource {
  uint64_t index_id = 0;
  uint64_t source_generation = 0;
  std::vector<TemporalEvent> events;
  MemtableDeltaIndex index;
  std::shared_ptr<const TemporalMemTable> pinned_memtable;
  std::optional<IndexDefinition> definition;
};

enum class TcypherWorkloadClass : uint8_t {
  kInteractive,
  kAnalytical,
};

struct TcypherExecutionStats {
  TcypherExecutionStats()
      : operator_runtime(std::make_shared<OperatorRuntimeStatsRegistry>()) {}

  uint64_t memtable_delta_probes = 0;
  uint64_t memtable_delta_candidates = 0;
  uint64_t base_events_visited = 0;
  uint64_t sst_blocks_read = 0;
  uint64_t sst_physical_bytes_read = 0;
  uint64_t page_bytes_decoded = 0;
  uint64_t page_bytes_skipped = 0;
  uint64_t page_decode_count = 0;
  uint64_t page_decode_latency_ns = 0;
  uint64_t max_sst_cursor_buffered_events = 0;
  uint64_t base_history_materialized_events = 0;
  uint64_t pinned_root_scan_opens = 0;
  uint64_t pinned_property_point_scans = 0;
  uint64_t pinned_boundary_point_scans = 0;
  uint64_t root_sst_blocks_read = 0;
  uint64_t boundary_sst_blocks_read = 0;
  uint64_t metadata_derived_bytes_reserved = 0;
  uint64_t vector_materialized_root_events = 0;
  uint64_t physical_plan_builds = 0;
  uint64_t physical_multi_join_builds = 0;
  uint64_t physical_cross_join_builds = 0;
  uint64_t pipeline_builds = 0;
  uint64_t morsels_scheduled = 0;
  uint64_t morsels_completed = 0;
  uint64_t scheduler_dispatches = 0;
  uint64_t result_queue_high_water = 0;
  uint64_t physical_output_rows = 0;
  uint64_t logical_result_bytes = 0;
  uint64_t candidate_intervals = 0;
  uint64_t output_intervals = 0;
  uint64_t blob_refs_seen = 0;
  uint64_t blob_payload_reads = 0;
  uint64_t blob_predicate_probe_bytes_reserved = 0;
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
  uint64_t handwritten_root_invocations = 0;
  uint64_t last_physical_plan_id = 0;
  uint64_t executed_physical_plan_id = 0;
  uint64_t index_sst_sources_materialized = 0;
  uint64_t index_delta_sources_materialized = 0;
  uint64_t index_delta_events_indexed = 0;
  uint64_t index_preparation_yields = 0;
  uint64_t index_metadata_items_processed = 0;
  uint64_t index_metadata_yields = 0;
  uint64_t index_max_metadata_items_per_morsel = 0;
  uint64_t index_advisory_fallbacks = 0;
  uint64_t index_candidate_items_processed = 0;
  uint64_t index_candidate_entity_count = 0;
  uint64_t index_candidate_preparation_yields = 0;
  uint64_t index_advisory_sidecar_bytes_read = 0;
  uint64_t index_predicate_literals_canonicalized = 0;
  uint64_t index_predicate_canonicalization_yields = 0;
  uint64_t index_predicate_literal_charge_items = 0;
  uint64_t index_predicate_literal_charge_yields = 0;
  uint64_t index_candidate_literal_lookup_comparisons = 0;
  uint64_t index_advisory_sst_block_count_reads = 0;
  uint64_t index_advisory_ordinal_read_calls = 0;
  uint64_t index_adaptive_reoptimizations = 0;
  uint64_t index_adaptive_intersection_predicates_dropped = 0;
  uint64_t index_adaptive_unopened_fragments_skipped = 0;
  uint64_t index_adaptive_unopened_delta_sources_skipped = 0;
  uint64_t index_adaptive_sampled_candidates = 0;
  uint64_t index_dynamic_filter_input_rows = 0;
  uint64_t index_dynamic_filter_rejected_rows = 0;
  uint64_t index_dynamic_filter_output_rows = 0;
  uint64_t projection_gather_payload_bytes_copied = 0;
  uint64_t physical_project_payload_bytes_copied = 0;
  uint64_t hash_join_build_side_left = 0;
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
  uint64_t cross_join_replay_input_rows = 0;
  uint64_t cross_join_stream_input_rows = 0;
  uint64_t cross_join_output_rows = 0;
  uint64_t cross_join_spill_starts = 0;
  uint64_t cross_join_spill_bytes = 0;
  uint64_t cross_join_build_side_switches = 0;
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
  std::optional<RuntimeFeedbackKey> runtime_feedback_key;
  uint64_t runtime_feedback_observations = 0;
  uint64_t runtime_feedback_base_rows = 0;
  uint64_t runtime_feedback_candidate_rows = 0;
  bool runtime_feedback_applied = false;
  CandidateSource runtime_feedback_source = CandidateSource::kBase;
  std::shared_ptr<OperatorRuntimeStatsRegistry> operator_runtime;
};

struct TcypherQueryOptions {
  uint64_t statement_start_valid_time = 0;
  uint32_t batch_capacity = kTcypherStandardBatchCapacity;
  std::map<std::string, uint64_t> timestamp_parameters;
  std::shared_ptr<QueryCancellation> cancellation;
  std::shared_ptr<QueryMemoryAccount> memory_account;
  std::string spill_directory;
  TcypherWorkloadClass workload_class = TcypherWorkloadClass::kInteractive;
  std::shared_ptr<TcypherExecutionStats> execution_stats;
  // Database execution overwrites this with its shared extension capability.
  std::shared_ptr<ResourceGovernorExtension> spill_resource_extensions;
  // Optional bounded progress hook used by admission/cancellation owners. It
  // is invoked once before each physical Expand segment starts.
  std::function<void(uint32_t)> expand_segment_observer;
};

struct TcypherExecutionContext {
  const CommitTimeline& commit_timeline;
  uint64_t visible_seq_ceiling;
  TcypherQueryOptions options;
  std::shared_ptr<const SchemaSnapshot> schema_snapshot;
  std::vector<TemporalEvent> committed_events;
  TransactionCoordinator* transaction_coordinator = nullptr;
  TcypherSession* session = nullptr;
  // Pinned with the query snapshot. Definitions are advisory candidate
  // sources; temporal visibility continues to be resolved from base events.
  std::shared_ptr<const IndexCatalogSnapshot> index_catalog_snapshot;
  std::shared_ptr<const VersionSnapshot> version_snapshot;
  std::vector<TcypherIndexSource> index_sources;
  std::vector<TcypherDeltaIndexSource> delta_index_sources;
  // Base storage is pinned independently from the query-local session
  // overlay. SST rows are decoded block-at-a-time when execution needs them.
  std::vector<PinnedSstSource> sst_event_sources;
  std::vector<std::shared_ptr<const TemporalMemTable>> memtable_event_sources;
  std::vector<TemporalEvent> session_overlay_events;
  std::shared_ptr<WorkExecutionService> work_execution_service;
  IoGovernor* io_governor = nullptr;
  std::optional<uint64_t> pinned_visible_seq_ceiling;
  uint64_t blob_reader_epoch = 0;
  SystemHlc statement_start_hlc{0, 0};
  bool runtime_sources_from_snapshot = false;
  std::shared_ptr<const PinnedStatsSnapshot> statistics_snapshot;
  std::shared_ptr<RuntimeFeedbackStore> runtime_feedback;
  std::vector<std::shared_ptr<QueryMemoryLease>> runtime_feedback_index_leases;
  std::optional<CandidateSource> root_access_path;
  std::vector<size_t> root_access_predicate_indices;
};

StatusOr<std::unique_ptr<QueryResultStream>> ExecuteTcypher(
    const std::string& query, TcypherExecutionContext context);

}  // namespace cedar

#endif  // CEDAR_TCYPHER_EXECUTOR_H_
