// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_LOGICAL_LOGICAL_PLAN_H_
#define CEDAR_QUERY_LOGICAL_LOGICAL_PLAN_H_

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "cedar/query/query.h"
#include "query/logical/expression.h"

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

struct PropertyBinding {
  SlotId source;
  PropertyId property;
  RowColumn output;
};

struct LogicalPlanPayload {
  std::optional<TemporalScope> scope;
  std::optional<ExpandSpec> expand_spec;
  std::optional<PropertyBinding> property_binding;
  std::shared_ptr<const ExpressionNode> predicate;
};

class LogicalPlanNode {
 public:
  LogicalPlanNode(LogicalOpKind kind, RowSchema schema,
                  std::vector<std::shared_ptr<const LogicalPlanNode>> inputs = {},
                  LogicalPlanPayload payload = {})
      : kind_(kind), schema_(std::move(schema)), inputs_(std::move(inputs)),
        payload_(std::move(payload)) {}
  LogicalOpKind kind() const { return kind_; }
  const RowSchema& schema() const { return schema_; }
  const std::vector<std::shared_ptr<const LogicalPlanNode>>& inputs() const { return inputs_; }
  const std::optional<TemporalScope>& scope() const { return payload_.scope; }
  const std::optional<ExpandSpec>& expand_spec() const {
    return payload_.expand_spec;
  }
  const std::optional<PropertyBinding>& property_binding() const {
    return payload_.property_binding;
  }
  const std::shared_ptr<const ExpressionNode>& predicate() const {
    return payload_.predicate;
  }
 private:
  const LogicalOpKind kind_;
  const RowSchema schema_;
  const std::vector<std::shared_ptr<const LogicalPlanNode>> inputs_;
  const LogicalPlanPayload payload_;
};

class LogicalPlanInspector {
 public:
  static const LogicalPlanNode* Inspect(const Query& query) {
    return query.root_.get();
  }
  static std::shared_ptr<const LogicalPlanNode> InspectShared(const Query& query) {
    return query.root_;
  }
};

}  // namespace cedar::internal

#endif  // CEDAR_QUERY_LOGICAL_LOGICAL_PLAN_H_
