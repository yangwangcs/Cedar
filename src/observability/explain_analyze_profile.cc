// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/observability/explain_analyze_profile.h"

#include <algorithm>
#include <sstream>
#include <utility>
#include <vector>

namespace cedar {
namespace {

struct ProfileOperator {
  uint64_t source_plan_id = 0;
  PhysicalOperatorSpec op;
  uint32_t pipeline_id = 0;
};

struct ProfilePipeline {
  uint64_t source_plan_id = 0;
  PipelineDescriptor pipeline;
};

std::string EscapeJson(const std::string& input) {
  std::string output;
  output.reserve(input.size());
  for (const unsigned char character : input) {
    switch (character) {
      case '"': output.append("\\\""); break;
      case '\\': output.append("\\\\"); break;
      case '\b': output.append("\\b"); break;
      case '\f': output.append("\\f"); break;
      case '\n': output.append("\\n"); break;
      case '\r': output.append("\\r"); break;
      case '\t': output.append("\\t"); break;
      default: output.push_back(static_cast<char>(character)); break;
    }
  }
  return output;
}

const char* SelectivityBucketName(SelectivityBucket bucket) {
  switch (bucket) {
    case SelectivityBucket::kVerySelective: return "very_selective";
    case SelectivityBucket::kModerate: return "moderate";
    case SelectivityBucket::kNonSelective: return "non_selective";
  }
  return "non_selective";
}

const char* CandidateSourceName(CandidateSource source) {
  switch (source) {
    case CandidateSource::kBase: return "base";
    case CandidateSource::kIndex: return "index";
    case CandidateSource::kHybrid: return "hybrid";
    case CandidateSource::kIntersection: return "intersection";
  }
  return "base";
}

const char* GraphOrderName(GraphOrder order) {
  switch (order) {
    case GraphOrder::kIndexFirst: return "index-first";
    case GraphOrder::kAdjacencyFirst: return "adjacency-first";
  }
  return "adjacency-first";
}

uint32_t PipelineFor(
    OperatorId operator_id, const std::vector<PipelineDescriptor>& pipelines) {
  for (const PipelineDescriptor& pipeline : pipelines) {
    if (std::find(pipeline.operators.begin(), pipeline.operators.end(),
                  operator_id) != pipeline.operators.end()) {
      return pipeline.id.value;
    }
  }
  return 0;
}

void AppendRootPlan(const PhysicalPlan& plan,
                    std::vector<ProfileOperator>* operators,
                    std::vector<ProfilePipeline>* pipelines) {
  for (const PhysicalOperatorSpec& op : plan.operators()) {
    operators->push_back(
        ProfileOperator{plan.plan_id(), op, PipelineFor(op.id, plan.pipelines())});
  }
  for (const PhysicalOperatorSpec& op : plan.post_result_operators()) {
    operators->push_back(
        ProfileOperator{plan.plan_id(), op, PipelineFor(op.id, plan.pipelines())});
  }
  for (const PipelineDescriptor& pipeline : plan.pipelines()) {
    pipelines->push_back(ProfilePipeline{plan.plan_id(), pipeline});
  }
}

void AppendOperator(
    uint64_t plan_id, const PhysicalOperatorSpec& op,
    const std::vector<PipelineDescriptor>& plan_pipelines,
    std::vector<ProfileOperator>* operators) {
  operators->push_back(
      ProfileOperator{plan_id, op, PipelineFor(op.id, plan_pipelines)});
}

void AppendPipelines(
    uint64_t plan_id, const std::vector<PipelineDescriptor>& source,
    std::vector<ProfilePipeline>* pipelines) {
  for (const PipelineDescriptor& pipeline : source) {
    pipelines->push_back(ProfilePipeline{plan_id, pipeline});
  }
}

void SerializeSlotIds(std::ostringstream* output,
                      const std::vector<SlotId>& slots) {
  *output << '[';
  for (size_t index = 0; index < slots.size(); ++index) {
    if (index != 0) *output << ',';
    *output << slots[index].value;
  }
  *output << ']';
}

void SerializeOperatorGraph(
    std::ostringstream* output, const std::vector<ProfileOperator>& operators,
    const std::vector<ProfilePipeline>& pipelines,
    const std::map<OperatorRuntimeKey, OperatorRuntimeCounters>& counters) {
  *output << ",\"operators\":[";
  for (size_t index = 0; index < operators.size(); ++index) {
    if (index != 0) *output << ',';
    const ProfileOperator& item = operators[index];
    *output << "{\"source_plan_id\":" << item.source_plan_id
            << ",\"operator_id\":" << item.op.id.value
            << ",\"kind\":\"" << PhysicalOperatorKindName(item.op.kind)
            << "\",\"pipeline_id\":" << item.pipeline_id
            << ",\"required_slots\":";
    SerializeSlotIds(output, item.op.required_slots);
    *output << ",\"produced_slots\":";
    SerializeSlotIds(output, item.op.produced_slots);
    const auto found = counters.find(
        OperatorRuntimeKey{item.source_plan_id, item.op.id.value});
    const OperatorRuntimeCounters empty;
    const OperatorRuntimeCounters& runtime =
        found == counters.end() ? empty : found->second;
    *output << ",\"counters\":{\"input_rows\":" << runtime.input_rows
            << ",\"output_rows\":" << runtime.output_rows
            << ",\"batches\":" << runtime.batches
            << ",\"input_intervals\":" << runtime.input_intervals
            << ",\"output_intervals\":" << runtime.output_intervals
            << ",\"pages_read\":" << runtime.pages_read
            << ",\"index_candidates\":" << runtime.index_candidates
            << ",\"blob_payload_reads\":" << runtime.blob_payload_reads
            << ",\"memory_peak_bytes\":" << runtime.memory_peak_bytes
            << ",\"spill_bytes\":" << runtime.spill_bytes
            << ",\"cpu_ns\":" << runtime.cpu_ns
            << ",\"blocked_ns\":" << runtime.blocked_ns
            << ",\"build_input_rows\":" << runtime.build_input_rows
            << ",\"probe_input_rows\":" << runtime.probe_input_rows
            << ",\"spill_starts\":" << runtime.spill_starts << '}';
    *output << '}';
  }
  *output << "],\"pipelines\":[";
  for (size_t index = 0; index < pipelines.size(); ++index) {
    if (index != 0) *output << ',';
    const ProfilePipeline& item = pipelines[index];
    *output << "{\"source_plan_id\":" << item.source_plan_id
            << ",\"pipeline_id\":" << item.pipeline.id.value
            << ",\"operators\":[";
    for (size_t op = 0; op < item.pipeline.operators.size(); ++op) {
      if (op != 0) *output << ',';
      *output << item.pipeline.operators[op].value;
    }
    *output << "],\"dependencies\":[";
    for (size_t dependency = 0;
         dependency < item.pipeline.dependencies.size(); ++dependency) {
      if (dependency != 0) *output << ',';
      *output << item.pipeline.dependencies[dependency].value;
    }
    *output << "]}";
  }
  *output << ']';
}

std::string Serialize(
    const char* plan_kind, uint64_t physical_plan_id,
    const std::string& plan_text, const ExplainAnalyzeRuntimeProfile& runtime,
    const std::vector<ProfileOperator>& operators,
    const std::vector<ProfilePipeline>& pipelines) {
  std::ostringstream output;
  output << "{\"profile_schema_version\":1"
         << ",\"query_id\":" << physical_plan_id
         << ",\"plan_kind\":\"" << plan_kind << "\""
         << ",\"physical_plan_id\":" << physical_plan_id
         << ",\"executed_physical_plan_id\":"
         << runtime.executed_physical_plan_id
         << ",\"plan_text\":\"" << EscapeJson(plan_text) << "\""
         << ",\"rejected_alternatives\":[]"
         << ",\"snapshot\":{\"snapshot_seq\":" << runtime.snapshot_seq
         << ",\"version_set_generation\":"
         << runtime.version_set_generation
         << ",\"catalog_generation\":" << runtime.catalog_generation
         << ",\"statistics_snapshot_id\":"
         << runtime.statistics_snapshot_id << "}"
         << ",\"execution\":{\"result_batches\":" << runtime.result_batches
         << ",\"output_rows\":" << runtime.output_rows
         << ",\"physical_output_rows\":" << runtime.physical_output_rows
         << ",\"terminal_status\":\""
         << EscapeJson(runtime.terminal_status) << "\"}"
         << ",\"storage\":{\"base_events_visited\":"
         << runtime.base_events_visited
         << ",\"sst_blocks_read\":" << runtime.sst_blocks_read
         << ",\"sst_physical_bytes_read\":"
         << runtime.sst_physical_bytes_read
         << ",\"page_bytes_decoded\":" << runtime.page_bytes_decoded
         << ",\"page_bytes_skipped\":" << runtime.page_bytes_skipped
         << ",\"page_decode_count\":" << runtime.page_decode_count
         << ",\"page_decode_latency_ns\":"
         << runtime.page_decode_latency_ns
         << ",\"root_sst_blocks_read\":" << runtime.root_sst_blocks_read
         << ",\"boundary_sst_blocks_read\":"
         << runtime.boundary_sst_blocks_read
         << ",\"projection_payload_bytes_copied\":"
         << runtime.projection_payload_bytes_copied << "}"
         << ",\"index\":{\"memtable_delta_probes\":"
         << runtime.memtable_delta_probes
         << ",\"memtable_delta_candidates\":"
         << runtime.memtable_delta_candidates
         << ",\"candidate_items_processed\":"
         << runtime.index_candidate_items_processed
         << ",\"advisory_fallbacks\":" << runtime.index_advisory_fallbacks
         << ",\"sidecar_bytes_read\":" << runtime.index_sidecar_bytes_read
         << ",\"runtime_feedback_bucket\":\""
         << (runtime.has_runtime_feedback
                 ? SelectivityBucketName(runtime.runtime_feedback_bucket)
                 : "none")
         << "\",\"runtime_feedback_observations\":"
         << runtime.runtime_feedback_observations
         << ",\"runtime_feedback_applied\":"
         << (runtime.runtime_feedback_applied ? "true" : "false")
         << ",\"runtime_feedback_source\":\""
         << CandidateSourceName(runtime.runtime_feedback_source)
         << "\",\"runtime_feedback_base_rows\":"
         << runtime.runtime_feedback_base_rows
         << ",\"runtime_feedback_candidate_rows\":"
         << runtime.runtime_feedback_candidate_rows
         << ",\"adaptive_reoptimizations\":"
         << runtime.index_adaptive_reoptimizations
         << ",\"adaptive_intersection_predicates_dropped\":"
         << runtime.index_adaptive_intersection_predicates_dropped
         << ",\"adaptive_unopened_fragments_skipped\":"
         << runtime.index_adaptive_unopened_fragments_skipped
         << ",\"adaptive_unopened_delta_sources_skipped\":"
         << runtime.index_adaptive_unopened_delta_sources_skipped
         << ",\"adaptive_sampled_candidates\":"
         << runtime.index_adaptive_sampled_candidates
         << ",\"dynamic_filter_input_rows\":"
         << runtime.index_dynamic_filter_input_rows
         << ",\"dynamic_filter_rejected_rows\":"
         << runtime.index_dynamic_filter_rejected_rows
         << ",\"dynamic_filter_output_rows\":"
         << runtime.index_dynamic_filter_output_rows
         << "}"
         << ",\"optimizer\":{\"selected_access_path\":\""
         << (runtime.has_selected_access_path
                 ? CandidateSourceName(runtime.selected_access_path)
                 : "none")
         << "\",\"executed_access_path\":\""
         << (runtime.has_executed_access_path
                 ? CandidateSourceName(runtime.executed_access_path)
                 : "none")
         << "\",\"access_path_score\":"
         << runtime.selected_access_path_score
         << ",\"access_path_cost\":{\"cpu_ns\":"
         << runtime.selected_access_path_cost.cpu_ns
         << ",\"sequential_bytes\":"
         << runtime.selected_access_path_cost.sequential_bytes
         << ",\"random_reads\":"
         << runtime.selected_access_path_cost.random_reads
         << ",\"decoded_bytes\":"
         << runtime.selected_access_path_cost.decoded_bytes
         << ",\"output_rows\":"
         << runtime.selected_access_path_cost.output_rows
         << ",\"output_intervals\":"
         << runtime.selected_access_path_cost.output_intervals
         << ",\"memory_peak\":"
         << runtime.selected_access_path_cost.memory_peak
         << ",\"spill_bytes\":"
         << runtime.selected_access_path_cost.spill_bytes
         << ",\"blob_bytes\":"
         << runtime.selected_access_path_cost.blob_bytes
         << ",\"confidence\":"
         << runtime.selected_access_path_cost.confidence_per_mille
         << "},\"access_path_rationale\":\""
         << EscapeJson(runtime.selected_access_path_rationale)
         << "\",\"access_path_fallback\":"
         << (runtime.access_path_fallback ? "true" : "false")
         << ",\"selected_graph_order\":\""
         << (runtime.has_selected_graph_order
                 ? GraphOrderName(runtime.selected_graph_order)
                 : "none")
         << "\",\"executed_graph_order\":\""
         << (runtime.has_executed_graph_order
                 ? GraphOrderName(runtime.executed_graph_order)
                 : "none")
         << "\"}"
         << ",\"memory\":{\"peak_bytes\":" << runtime.memory_peak_bytes
         << ",\"current_bytes\":" << runtime.memory_used_bytes
         << ",\"soft_limit_bytes\":" << runtime.memory_soft_limit_bytes
         << ",\"hard_limit_bytes\":" << runtime.memory_hard_limit_bytes
         << "}"
         << ",\"scheduler\":{\"pipelines_built\":"
         << runtime.pipelines_built
         << ",\"morsels_scheduled\":" << runtime.morsels_scheduled
         << ",\"morsels_completed\":" << runtime.morsels_completed
         << ",\"dispatches\":" << runtime.scheduler_dispatches
         << ",\"result_queue_high_water\":"
         << runtime.result_queue_high_water
         << ",\"reoptimization_checks\":"
         << runtime.pipeline_reoptimization_checks
         << ",\"reoptimizations\":"
         << runtime.pipeline_reoptimizations
         << ",\"reoptimization_sampled_rows\":"
         << runtime.pipeline_reoptimization_sampled_rows
         << ",\"reoptimization_sampled_batches\":"
         << runtime.pipeline_reoptimization_sampled_batches
         << ",\"reoptimization_prefix_memory_bytes\":"
         << runtime.pipeline_reoptimization_prefix_memory_bytes << "}"
         << ",\"frontier\":{\"hops\":" << runtime.path_frontier_hops
         << ",\"input_states\":" << runtime.path_frontier_input_states
         << ",\"output_states\":" << runtime.path_frontier_output_states
         << ",\"completed_paths\":"
         << runtime.path_frontier_completed_paths
         << ",\"repartitions\":" << runtime.path_frontier_repartitions
         << ",\"partitions\":" << runtime.path_frontier_partitions
         << ",\"max_partition_size\":"
         << runtime.path_frontier_max_partition_size
         << ",\"spill_starts\":" << runtime.path_frontier_spill_starts
         << ",\"spill_bytes\":" << runtime.path_frontier_spill_bytes << "}"
         << ",\"join\":{\"build_input_rows\":"
         << runtime.hash_join_build_input_rows
         << ",\"probe_input_rows\":" << runtime.hash_join_probe_input_rows
         << ",\"spill_starts\":" << runtime.hash_join_spill_starts
         << ",\"dynamic_filter_input_rows\":"
         << runtime.hash_join_dynamic_filter_input_rows
         << ",\"dynamic_filter_rejected_rows\":"
         << runtime.hash_join_dynamic_filter_rejected_rows
         << ",\"dynamic_filter_output_rows\":"
         << runtime.hash_join_dynamic_filter_output_rows
         << ",\"dynamic_filter_spill_rows_avoided\":"
         << runtime.hash_join_dynamic_filter_spill_rows_avoided
         << ",\"dynamic_filter_memory_bytes\":"
         << runtime.hash_join_dynamic_filter_memory_bytes
         << ",\"dynamic_filter_memory_disables\":"
         << runtime.hash_join_dynamic_filter_memory_disables
         << ",\"build_side_switches\":"
         << runtime.hash_join_build_side_switches << "}";
  SerializeOperatorGraph(
      &output, operators, pipelines, runtime.operator_counters);
  output << '}';
  return output.str();
}

}  // namespace

std::string SerializeExplainAnalyzeProfile(
    const PhysicalPlan& plan, const ExplainAnalyzeRuntimeProfile& runtime) {
  std::vector<ProfileOperator> operators;
  std::vector<ProfilePipeline> pipelines;
  AppendRootPlan(plan, &operators, &pipelines);
  return Serialize("PhysicalPlan", plan.plan_id(), FormatPhysicalPlan(plan),
                   runtime, operators, pipelines);
}

std::string SerializeExplainAnalyzeProfile(
    const PhysicalHashJoinPlan& plan,
    const ExplainAnalyzeRuntimeProfile& runtime) {
  std::vector<ProfileOperator> operators;
  std::vector<ProfilePipeline> pipelines;
  AppendRootPlan(*plan.left, &operators, &pipelines);
  AppendRootPlan(*plan.right, &operators, &pipelines);
  AppendOperator(plan.plan_id, plan.join, plan.pipelines, &operators);
  for (const PhysicalOperatorSpec& op : plan.post_join_operators) {
    AppendOperator(plan.plan_id, op, plan.pipelines, &operators);
  }
  AppendPipelines(plan.plan_id, plan.pipelines, &pipelines);
  return Serialize("PhysicalHashJoinPlan", plan.plan_id,
                   FormatPhysicalHashJoinPlan(plan), runtime, operators,
                   pipelines);
}

std::string SerializeExplainAnalyzeProfile(
    const PhysicalMultiHashJoinPlan& plan,
    const ExplainAnalyzeRuntimeProfile& runtime) {
  std::vector<ProfileOperator> operators;
  std::vector<ProfilePipeline> pipelines;
  for (const std::shared_ptr<const PhysicalPlan>& input : plan.inputs) {
    if (input) AppendRootPlan(*input, &operators, &pipelines);
  }
  for (const PhysicalMultiHashJoinStep& step : plan.steps) {
    AppendOperator(plan.plan_id, step.join, plan.pipelines, &operators);
  }
  for (const PhysicalOperatorSpec& op : plan.post_join_operators) {
    AppendOperator(plan.plan_id, op, plan.pipelines, &operators);
  }
  AppendPipelines(plan.plan_id, plan.pipelines, &pipelines);
  return Serialize("PhysicalMultiHashJoinPlan", plan.plan_id,
                   FormatPhysicalMultiHashJoinPlan(plan), runtime, operators,
                   pipelines);
}

}  // namespace cedar
