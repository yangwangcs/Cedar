// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_LOGICAL_LOGICAL_PLAN_H_
#define CEDAR_QUERY_LOGICAL_LOGICAL_PLAN_H_

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "cedar/query/query.h"

namespace cedar::internal {

enum class LogicalOpKind : uint8_t {
  kVertexScan, kEdgeScan,
  kStateAt, kEventsBetween, kChangesBetween, kHistory,
  kStateOverlaps, kStateThroughout,
  kExpandOut, kExpandIn, kExpandBoth,
  kBindProperty, kFilter, kProject,
  kInnerJoin, kSemiJoin, kAntiJoin, kUnionAll, kDistinct, kSort, kLimit,
  kAggregateRows, kTemporalAggregate, kKHopExpand, kCoexistingShortestPath,
  kEarliestArrival, kLatestDeparture, kFastestDuration,
};

class LogicalPlanNode {
 public:
  LogicalPlanNode(LogicalOpKind kind, RowSchema schema,
                  std::vector<std::shared_ptr<const LogicalPlanNode>> inputs = {},
                  std::optional<TemporalScope> scope = std::nullopt)
      : kind_(kind), schema_(std::move(schema)), inputs_(std::move(inputs)),
        scope_(std::move(scope)) {}
  LogicalOpKind kind() const { return kind_; }
  const RowSchema& schema() const { return schema_; }
  const std::vector<std::shared_ptr<const LogicalPlanNode>>& inputs() const { return inputs_; }
 private:
  const LogicalOpKind kind_;
  const RowSchema schema_;
  const std::vector<std::shared_ptr<const LogicalPlanNode>> inputs_;
  const std::optional<TemporalScope> scope_;
};

}  // namespace cedar::internal

#endif  // CEDAR_QUERY_LOGICAL_LOGICAL_PLAN_H_
