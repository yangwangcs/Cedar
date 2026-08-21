// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_PLANNER_QUERY_PLANNER_H_
#define CEDAR_QUERY_PLANNER_QUERY_PLANNER_H_

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/query/query.h"
#include "cedar/query/types.h"
#include "query/logical/logical_plan.h"
#include "query/projection/projection_manifest.h"
#include "query/projection/query_delta.h"

namespace cedar::internal {

enum class PhysicalOpKind : uint8_t {
  kCanonicalScan,
  kProjectionScan,
  kDeltaMerge,
  kCanonicalFallback,
  kAdjacencySeek,
  kLaneExchange,
  kFilter,
  kProject,
  kAggregate,
  kSort,
};

enum class CoverageSource : uint8_t { kProjection, kCanonical, kDeltaMerge };

struct CoverageSlice {
  CoverageSource source = CoverageSource::kCanonical;
  ValidTimeInterval interval;
  std::optional<uint64_t> projection_generation;
  std::optional<CommitSeq> projection_base;
  bool operator==(const CoverageSlice&) const = default;
};

struct QueryCostEstimate {
  uint64_t rows = 0;
  uint64_t pages = 0;
  uint64_t physical_bytes = 0;
  uint64_t decoded_bytes = 0;
  uint64_t random_reads = 0;
  uint64_t dirty_chains = 0;
  uint64_t interval_fragments = 0;
  uint64_t fanout = 0;
  uint64_t memory_bytes = 0;
  uint64_t spill_bytes = 0;
  uint64_t first_result_us = 0;
  bool uncertain = false;
};

struct CoverageSliceDescription {
  CoverageSource source = CoverageSource::kCanonical;
  uint64_t from = 0;
  std::optional<uint64_t> to;
  std::optional<uint64_t> projection_generation;
  std::optional<CommitSeq> projection_base;
};

struct QueryPlanNodeDescription {
  LogicalOpKind logical = LogicalOpKind::kVertexScan;
  PhysicalOpKind physical = PhysicalOpKind::kCanonicalScan;
  QueryExecutionMode lane = QueryExecutionMode::kAuto;
  QueryCostEstimate estimate;
  std::optional<uint64_t> projection_generation;
  std::optional<CommitSeq> projection_base;
  std::vector<CoverageSliceDescription> coverage;
  std::vector<QueryPlanNodeDescription> children;
};

struct PhysicalPlan {
  QueryExecutionMode lane = QueryExecutionMode::kInteractive;
  QueryCostEstimate estimate;
  std::vector<CoverageSlice> coverage_slices;
  std::vector<PhysicalOpKind> operations;
  std::vector<std::string> pushdowns;
  bool spill_allowed = false;
  bool has_lane_exchange = false;
  bool conservative = false;
  QueryPlanNodeDescription explain;

  const std::vector<CoverageSlice>& slices() const { return coverage_slices; }
  const std::vector<PhysicalOpKind>& ops() const { return operations; }
};

// Immutable catalog metadata copied out of a ProjectionGeneration. Keeping
// this view free of file handles makes planning safe while a generation is
// retired by the store.
struct ProjectionCatalogView {
  ProjectionCatalogView() = default;
  explicit ProjectionCatalogView(const ProjectionManifest& manifest)
      : database_identity(manifest.database_identity),
        generation_id(manifest.generation_id),
        base_seq(manifest.base_seq),
        regions(manifest.regions) {}
  std::string database_identity;
  uint64_t generation_id = 0;
  CommitSeq base_seq;
  std::vector<CoverageRegion> regions;
};

struct QueryStatisticsView {
  uint64_t candidate_rows = 0;
  uint64_t pages = 0;
  uint64_t physical_bytes = 0;
  uint64_t decoded_bytes = 0;
  uint64_t dirty_chains = 0;
  uint64_t interval_fragments = 0;
  uint64_t fanout = 0;
  bool known = false;
};

struct PlanningContext {
  CommitSeq snapshot_seq;
  const ProjectionCatalogView& projections;
  const QueryDeltaView& delta;
  const QueryStatisticsView& statistics;
  QueryOptions options;
  std::string database_identity;
  uint32_t schema_epoch = 0;
};

class QueryPlanner {
 public:
  static StatusOr<PhysicalPlan> Bind(const LogicalPlanNode& logical,
                                     const PlanningContext& context);
  static std::string ExplainLogical(const LogicalPlanNode& logical);
  static std::string ExplainPhysical(const PhysicalPlan& plan);

 private:
  QueryPlanner() = delete;
};

inline CoverageSlice Projection(ValidTimeInterval interval) {
  return {CoverageSource::kProjection, std::move(interval), std::nullopt,
          std::nullopt};
}
inline CoverageSlice Canonical(ValidTimeInterval interval) {
  return {CoverageSource::kCanonical, std::move(interval), std::nullopt,
          std::nullopt};
}

}  // namespace cedar::internal

#endif  // CEDAR_QUERY_PLANNER_QUERY_PLANNER_H_
