// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/optimizer/cost_model.h"

#include <limits>

namespace cedar {
namespace {

uint64_t SaturatingAdd(uint64_t left, uint64_t right) {
  return left > std::numeric_limits<uint64_t>::max() - right
      ? std::numeric_limits<uint64_t>::max() : left + right;
}
uint64_t SaturatingMultiply(uint64_t left, uint64_t right) {
  return left == 0 || right == 0 ? 0
      : left > std::numeric_limits<uint64_t>::max() / right
          ? std::numeric_limits<uint64_t>::max() : left * right;
}

}  // namespace

uint64_t ScoreCost(const CostVector& cost, const WorkloadCostProfile& profile) {
  uint64_t score = 0;
  score = SaturatingAdd(score, SaturatingMultiply(cost.cpu_ns, profile.cpu_ns_weight));
  score = SaturatingAdd(score,
                        SaturatingMultiply(cost.sequential_bytes, profile.sequential_byte_weight));
  score = SaturatingAdd(score, SaturatingMultiply(cost.random_reads, profile.random_read_weight));
  score = SaturatingAdd(score,
                        SaturatingMultiply(cost.decoded_bytes, profile.decoded_byte_weight));
  score = SaturatingAdd(score, SaturatingMultiply(cost.memory_peak, profile.memory_byte_weight));
  score = SaturatingAdd(score, SaturatingMultiply(cost.spill_bytes, profile.spill_byte_weight));
  score = SaturatingAdd(score, SaturatingMultiply(cost.blob_bytes, profile.blob_byte_weight));
  const uint32_t confidence = cost.confidence_per_mille > 1000 ? 1000 : cost.confidence_per_mille;
  const uint64_t uncertainty = SaturatingMultiply(1000 - confidence, profile.uncertainty_penalty);
  return SaturatingAdd(score, uncertainty);
}

CostVector EstimateBaseScanCost(const ScanCostEstimate& estimate) {
  const uint64_t rows = estimate.base_rows;
  return CostVector{rows, SaturatingMultiply(rows, 32), 0, SaturatingMultiply(rows, 32),
                    rows, rows, SaturatingMultiply(rows, 8), 0, 0, 900U};
}

CostVector EstimateIndexScanCost(const ScanCostEstimate& estimate, CandidateSource source) {
  const uint64_t versions = estimate.validation_versions_per_candidate == 0
      ? 1 : estimate.validation_versions_per_candidate;
  const uint64_t validation_rows = SaturatingMultiply(estimate.index_candidate_rows, versions);
  const uint64_t uncovered = source == CandidateSource::kHybrid
      ? estimate.uncovered_base_rows : 0;
  const uint64_t rows = SaturatingAdd(validation_rows, uncovered);
  const uint64_t residual_rows = SaturatingMultiply(
      validation_rows, estimate.residual_predicate_count);
  return CostVector{
                    SaturatingAdd(SaturatingMultiply(rows, 2),
                                  SaturatingMultiply(residual_rows, 3)),
                    SaturatingMultiply(uncovered, 32),
                    SaturatingAdd(validation_rows, residual_rows),
                    SaturatingAdd(SaturatingMultiply(rows, 24),
                                  SaturatingMultiply(residual_rows, 64)),
                    rows, rows,
                    SaturatingAdd(SaturatingMultiply(rows, 8),
                                  SaturatingMultiply(residual_rows, 32)),
                    0, 0,
                    source == CandidateSource::kIndex ? 650U : 750U};
}

CostVector EstimateIndexIntersectionCost(const ScanCostEstimate& estimate) {
  const uint64_t versions = estimate.validation_versions_per_candidate == 0
      ? 1 : estimate.validation_versions_per_candidate;
  const uint64_t probe_rows = SaturatingAdd(estimate.left_index_candidate_rows,
                                            estimate.right_index_candidate_rows);
  const uint64_t validation_rows = SaturatingMultiply(
      estimate.intersection_candidate_rows, versions);
  const uint64_t rows = SaturatingAdd(probe_rows, validation_rows);
  return CostVector{SaturatingMultiply(rows, 3), 0, probe_rows,
                    SaturatingMultiply(rows, 24), validation_rows,
                    validation_rows, SaturatingMultiply(probe_rows, 16), 0, 0, 600U};
}

GraphOrderDecision ChooseGraphOrderDecision(
    const GraphOrderEstimate& estimate, const OptimizerBudget& budget,
    const WorkloadCostProfile& profile) {
  GraphOrderDecision decision;
  const bool budget_exhausted = budget.exhausted || budget.remaining_alternatives == 0;
  decision.budget_exhausted = budget_exhausted;

  const uint64_t degree = estimate.adjacency_degree == 0
      ? 1 : estimate.adjacency_degree;
  const uint64_t adjacency_rows = SaturatingMultiply(
      estimate.adjacency_start_rows, degree);
  const CostVector adjacency_cost{
      SaturatingMultiply(adjacency_rows, 3),
      SaturatingMultiply(estimate.adjacency_start_rows, 32),
      estimate.adjacency_start_rows,
      SaturatingMultiply(adjacency_rows, 32),
      adjacency_rows, adjacency_rows,
      SaturatingMultiply(estimate.adjacency_start_rows, 16), 0, 0, 700U};
  decision.adjacency_score = ScoreCost(adjacency_cost, profile);

  if (estimate.index_available) {
    const uint64_t validation = estimate.index_validation_rows == 0
        ? estimate.index_start_rows : estimate.index_validation_rows;
    const CostVector index_cost{
        SaturatingMultiply(validation, 2),
        SaturatingMultiply(estimate.index_start_rows, 16),
        validation,
        SaturatingMultiply(validation, 24),
        validation, validation,
        SaturatingMultiply(validation, 8), 0, 0, 650U};
    decision.index_score = ScoreCost(index_cost, profile);
  } else {
    decision.index_score = std::numeric_limits<uint64_t>::max();
  }

  if (budget_exhausted) {
    decision.order = estimate.adjacency_available
        ? GraphOrder::kAdjacencyFirst : GraphOrder::kIndexFirst;
    decision.score = decision.order == GraphOrder::kAdjacencyFirst
        ? decision.adjacency_score : decision.index_score;
    decision.rationale = decision.order == GraphOrder::kAdjacencyFirst
        ? "adjacency-first graph order after optimizer budget exhaustion"
        : "index-first graph order after optimizer budget exhaustion";
    return decision;
  }

  if (!estimate.adjacency_available && !estimate.index_available) {
    decision.order = GraphOrder::kAdjacencyFirst;
    decision.score = 0;
    decision.rationale = "adjacency-first graph order with no legal alternative";
    return decision;
  }
  if (!estimate.adjacency_available ||
      (estimate.index_available && decision.index_score < decision.adjacency_score)) {
    decision.order = GraphOrder::kIndexFirst;
    decision.score = decision.index_score;
    decision.rationale = "index-first graph order selected by cost";
  } else {
    decision.order = GraphOrder::kAdjacencyFirst;
    decision.score = decision.adjacency_score;
    decision.rationale = "adjacency-first graph order selected by cost";
  }
  return decision;
}

AccessPathDecision ChooseAccessPathDecision(const ScanCostEstimate& estimate,
                                            const OptimizerBudget& budget,
                                            const WorkloadCostProfile& profile) {
  AccessPathDecision base;
  base.source = CandidateSource::kBase;
  base.cost = EstimateBaseScanCost(estimate);
  base.score = ScoreCost(base.cost, profile);
  base.rationale = "base temporal scan";
  if (budget.exhausted || budget.remaining_alternatives == 0 || estimate.base_rows == 0) {
    base.budget_exhausted = budget.exhausted || budget.remaining_alternatives == 0;
    if (base.budget_exhausted) base.rationale = "base scan after optimizer budget exhaustion";
    return base;
  }
  const CandidateSource candidate_source = estimate.uncovered_base_rows == 0
      ? CandidateSource::kIndex : CandidateSource::kHybrid;
  AccessPathDecision candidate;
  candidate.source = candidate_source;
  candidate.cost = EstimateIndexScanCost(estimate, candidate_source);
  candidate.score = ScoreCost(candidate.cost, profile);
  candidate.rationale = candidate_source == CandidateSource::kIndex
      ? "covered index candidates with temporal base validation"
      : "index candidates plus uncovered base scan";
  AccessPathDecision selected = candidate.score < base.score ? candidate : base;
  if (estimate.intersection_available && budget.remaining_alternatives >= 2) {
    AccessPathDecision intersection;
    intersection.source = CandidateSource::kIntersection;
    intersection.cost = EstimateIndexIntersectionCost(estimate);
    intersection.score = ScoreCost(intersection.cost, profile);
    intersection.rationale = "two index candidate streams intersected before temporal validation";
    if (intersection.score < selected.score) selected = std::move(intersection);
  } else if (estimate.intersection_available && budget.remaining_alternatives < 2) {
    selected.budget_exhausted = true;
  }
  if (estimate.feedback_applied) {
    selected.rationale += " using confident runtime feedback";
  }
  return selected;
}

}  // namespace cedar
