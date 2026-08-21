// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/logical/logical_plan.h"

#include <algorithm>
#include <type_traits>
#include <unordered_set>

namespace cedar {
namespace {

Status ValidateScope(const TemporalScope& scope) {
  return std::visit([](const auto& value) -> Status {
    using T = std::decay_t<decltype(value)>;
    if constexpr (std::is_same_v<T, At>) {
      return Status::OK();
    } else if constexpr (std::is_same_v<T, History>) {
      return value.interval ? value.interval->Validate() : Status::OK();
    } else {
      return value.interval.Validate();
    }
  }, scope);
}

bool Contains(const RowSchema& schema, const RowColumn& expected) {
  return std::find(schema.columns().begin(), schema.columns().end(), expected) != schema.columns().end();
}

bool HasSlotId(const RowSchema& schema, SlotId slot) {
  return std::any_of(schema.columns().begin(), schema.columns().end(), [slot](const RowColumn& column) { return column.slot == slot; });
}

Status ValidateColumns(const std::vector<RowColumn>& columns) {
  std::unordered_set<uint32_t> slots;
  for (const RowColumn& column : columns) {
    if (column.slot.value == 0 || !slots.insert(column.slot.value).second) {
      return Status::InvalidArgument("logical plan contains duplicate SlotId");
    }
  }
  return Status::OK();
}

std::shared_ptr<const internal::LogicalPlanNode> Append(internal::LogicalOpKind kind,
    const std::shared_ptr<const internal::LogicalPlanNode>& input, RowSchema schema) {
  return std::make_shared<const internal::LogicalPlanNode>(kind, std::move(schema),
      std::vector<std::shared_ptr<const internal::LogicalPlanNode>>{input});
}

}  // namespace

StatusOr<Query> Query::Vertices(Slot<VertexRef> vertex, TemporalScope scope) {
  if (vertex.id().value == 0) return Status::InvalidArgument("vertex SlotId is invalid");
  if (Status status = ValidateScope(scope); !status.ok()) return status;
  return Query(std::make_shared<const internal::LogicalPlanNode>(internal::LogicalOpKind::kVertexScan,
      RowSchema({{vertex.id(), vertex.type(), false}}),
      std::vector<std::shared_ptr<const internal::LogicalPlanNode>>{}, std::move(scope)));
}

StatusOr<Query> Query::Edges(Slot<EdgeRef> edge, TemporalScope scope) {
  if (edge.id().value == 0) return Status::InvalidArgument("edge SlotId is invalid");
  if (Status status = ValidateScope(scope); !status.ok()) return status;
  return Query(std::make_shared<const internal::LogicalPlanNode>(internal::LogicalOpKind::kEdgeScan,
      RowSchema({{edge.id(), edge.type(), false}}),
      std::vector<std::shared_ptr<const internal::LogicalPlanNode>>{}, std::move(scope)));
}

StatusOr<Query> Query::Expand(const ExpandSpec& spec) const {
  if (!root_ || !Contains(schema(), {spec.source.id(), QueryType::kVertexRef, false}) ||
      spec.edge.id().value == 0 || spec.destination.id().value == 0 ||
      spec.edge.id() == spec.destination.id() || spec.source.id() == spec.edge.id() || spec.source.id() == spec.destination.id()) {
    return Status::InvalidArgument("expand slots are invalid or duplicate");
  }
  std::vector<RowColumn> columns = schema().columns();
  columns.push_back({spec.edge.id(), spec.edge.type(), false});
  columns.push_back({spec.destination.id(), spec.destination.type(), false});
  const auto kind = spec.direction == ExpandDirection::kOut ? internal::LogicalOpKind::kExpandOut :
      spec.direction == ExpandDirection::kIn ? internal::LogicalOpKind::kExpandIn : internal::LogicalOpKind::kExpandBoth;
  return Query(Append(kind, root_, RowSchema(std::move(columns))));
}

StatusOr<Query> Query::BindVertexPropertyImpl(SlotId vertex, PropertyId property, RowColumn output) const {
  if (!root_ || !property.valid() || output.slot.value == 0 ||
      !Contains(schema(), {vertex, QueryType::kVertexRef, false}) || HasSlotId(schema(), output.slot)) {
    return Status::InvalidArgument("property binding is invalid or duplicates a SlotId");
  }
  std::vector<RowColumn> columns = schema().columns();
  columns.push_back(output);
  return Query(Append(internal::LogicalOpKind::kBindProperty, root_, RowSchema(std::move(columns))));
}

StatusOr<Query> Query::Where(Expr<bool> predicate) const {
  if (!root_ || !predicate.valid()) return Status::InvalidArgument("filter predicate is invalid");
  return Query(Append(internal::LogicalOpKind::kFilter, root_, schema()));
}

StatusOr<Query> Query::Select(std::vector<Projection> projections) const {
  if (!root_) return Status::InvalidArgument("query has no logical plan");
  std::vector<RowColumn> columns;
  columns.reserve(projections.size());
  for (const Projection& projection : projections) {
    if (!Contains(schema(), projection.column)) return Status::InvalidArgument("projection is not in the input schema");
    columns.push_back(projection.column);
  }
  if (Status status = ValidateColumns(columns); !status.ok()) return status;
  return Query(Append(internal::LogicalOpKind::kProject, root_, RowSchema(std::move(columns))));
}

const RowSchema& Query::schema() const {
  static const RowSchema empty;
  return root_ ? root_->schema() : empty;
}

}  // namespace cedar
