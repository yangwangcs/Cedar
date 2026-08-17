// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_OPTIMIZER_COST_MODEL_H_
#define CEDAR_OPTIMIZER_COST_MODEL_H_

#include <cstdint>
#include <string>

namespace cedar {

enum class CandidateSource : uint8_t { kBase, kIndex, kHybrid, kIntersection };

// Graph-order alternatives describe the first operator in a graph pattern.
// Index-first starts from selective property candidates and then expands
// adjacency; adjacency-first expands from a bound/root source and applies
// property filtering afterwards.  The distinction is intentionally separate
// from CandidateSource, which describes the physical scan used by a root.
enum class GraphOrder : uint8_t { kIndexFirst, kAdjacencyFirst };

struct CostVector {
  uint64_t cpu_ns = 0;
  uint64_t sequential_bytes = 0;
  uint64_t random_reads = 0;
  uint64_t decoded_bytes = 0;
  uint64_t output_rows = 0;
  uint64_t output_intervals = 0;
  uint64_t memory_peak = 0;
  uint64_t spill_bytes = 0;
  uint64_t blob_bytes = 0;
  uint32_t confidence_per_mille = 0;
};

struct WorkloadCostProfile {
  uint64_t cpu_ns_weight = 1;
  uint64_t sequential_byte_weight = 1;
  // A validated posting normally lands in a page/block already selected by
  // the sidecar, so it is materially cheaper than an uncoalesced 4 KiB I/O.
  uint64_t random_read_weight = 1024;
  uint64_t decoded_byte_weight = 1;
  uint64_t memory_byte_weight = 1;
  uint64_t spill_byte_weight = 4;
  uint64_t blob_byte_weight = 2;
  uint64_t uncertainty_penalty = 1;
};

struct ScanCostEstimate {
  uint64_t base_rows = 0;
  uint64_t index_candidate_rows = 0;
  uint64_t uncovered_base_rows = 0;
  uint64_t validation_versions_per_candidate = 1;
  // A two-index alternative is available only when both input candidate
  // streams and their entity/edge identity intersection are known under the
  // same pinned catalog snapshot.
  bool intersection_available = false;
  uint64_t left_index_candidate_rows = 0;
  uint64_t right_index_candidate_rows = 0;
  uint64_t intersection_candidate_rows = 0;
  uint32_t residual_predicate_count = 0;
  bool feedback_applied = false;
};

struct OptimizerBudget {
  uint32_t remaining_alternatives = 0;
  bool exhausted = false;
};

struct GraphOrderEstimate {
  uint64_t index_start_rows = 0;
  uint64_t index_validation_rows = 0;
  uint64_t adjacency_start_rows = 0;
  uint64_t adjacency_degree = 1;
  bool index_available = false;
  bool adjacency_available = true;
};

struct GraphOrderDecision {
  GraphOrder order = GraphOrder::kAdjacencyFirst;
  uint64_t score = 0;
  uint64_t index_score = 0;
  uint64_t adjacency_score = 0;
  bool budget_exhausted = false;
  std::string rationale;
};

struct AccessPathDecision {
  CandidateSource source = CandidateSource::kBase;
  CostVector cost;
  uint64_t score = 0;
  bool budget_exhausted = false;
  std::string rationale;
};

uint64_t ScoreCost(const CostVector& cost, const WorkloadCostProfile& profile);
CostVector EstimateBaseScanCost(const ScanCostEstimate& estimate);
CostVector EstimateIndexScanCost(const ScanCostEstimate& estimate,
                                 CandidateSource source);
CostVector EstimateIndexIntersectionCost(const ScanCostEstimate& estimate);
GraphOrderDecision ChooseGraphOrderDecision(
    const GraphOrderEstimate& estimate, const OptimizerBudget& budget,
    const WorkloadCostProfile& profile = {});
AccessPathDecision ChooseAccessPathDecision(const ScanCostEstimate& estimate,
                                            const OptimizerBudget& budget,
                                            const WorkloadCostProfile& profile = {});
inline CandidateSource ChooseAccessPath(const ScanCostEstimate& estimate,
                                        const OptimizerBudget& budget) {
  return ChooseAccessPathDecision(estimate, budget).source;
}

}  // namespace cedar

#endif  // CEDAR_OPTIMIZER_COST_MODEL_H_
