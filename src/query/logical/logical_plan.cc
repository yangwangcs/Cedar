// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/logical/logical_plan.h"

#include <algorithm>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <limits>

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

const RowColumn* FindColumn(const RowSchema& schema, SlotId slot,
                            QueryType type, bool optional) {
  const auto found = std::find_if(
      schema.columns().begin(), schema.columns().end(),
      [slot, type, optional](const RowColumn& column) {
        return column.slot == slot && column.type == type &&
               column.optional == optional;
      });
  return found == schema.columns().end() ? nullptr : &*found;
}

bool Contains(const RowSchema& schema, SlotId slot, QueryType type,
              bool optional) {
  return FindColumn(schema, slot, type, optional) != nullptr;
}

bool HasSlotId(const RowSchema& schema, SlotId slot) {
  return std::any_of(schema.columns().begin(), schema.columns().end(),
                     [slot](const RowColumn& column) {
                       return column.slot == slot;
                     });
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
    const std::shared_ptr<const internal::LogicalPlanNode>& input,
    RowSchema schema, internal::LogicalPlanPayload payload = {}) {
  return std::make_shared<const internal::LogicalPlanNode>(
      kind, std::move(schema),
      std::vector<std::shared_ptr<const internal::LogicalPlanNode>>{input},
      std::move(payload));
}

internal::LogicalOpKind TemporalKind(const TemporalScope& scope) {
  return std::visit([](const auto& value) {
    using T = std::decay_t<decltype(value)>;
    if constexpr (std::is_same_v<T, At>) {
      return internal::LogicalOpKind::kStateAt;
    } else if constexpr (std::is_same_v<T, Events>) {
      return internal::LogicalOpKind::kEventsBetween;
    } else if constexpr (std::is_same_v<T, Changes>) {
      return internal::LogicalOpKind::kChangesBetween;
    } else if constexpr (std::is_same_v<T, Overlaps>) {
      return internal::LogicalOpKind::kStateOverlaps;
    } else if constexpr (std::is_same_v<T, Throughout>) {
      return internal::LogicalOpKind::kStateThroughout;
    } else {
      return internal::LogicalOpKind::kHistory;
    }
  }, scope);
}

std::shared_ptr<const internal::LogicalPlanNode> MakeScopedScan(
    internal::LogicalOpKind scan_kind, RowColumn column, TemporalScope scope) {
  RowSchema schema({column});
  const auto scan = std::make_shared<const internal::LogicalPlanNode>(
      scan_kind, schema,
      std::vector<std::shared_ptr<const internal::LogicalPlanNode>>{});
  internal::LogicalPlanPayload payload;
  payload.scope = std::move(scope);
  return Append(TemporalKind(*payload.scope), scan, std::move(schema),
                std::move(payload));
}

std::shared_ptr<const internal::LogicalPlanNode> MakeScopedPoint(
    VertexRef ref, RowColumn column, TemporalScope scope) {
  RowSchema schema({column});
  internal::LogicalPlanPayload point_payload;
  point_payload.point_ref = ref;
  const auto point = std::make_shared<const internal::LogicalPlanNode>(
      internal::LogicalOpKind::kVertexPointLookup, schema,
      std::vector<std::shared_ptr<const internal::LogicalPlanNode>>{},
      std::move(point_payload));
  internal::LogicalPlanPayload temporal_payload;
  temporal_payload.scope = std::move(scope);
  return Append(TemporalKind(*temporal_payload.scope), point,
                std::move(schema), std::move(temporal_payload));
}

Status ValidateExpressionSlots(const internal::ExpressionNode& expression,
                               const RowSchema& schema,
                               std::unordered_map<uint32_t, QueryType>&
                                   parameter_types) {
  if (expression.kind() == internal::ExpressionKind::kSlot &&
      !Contains(schema, expression.slot(), expression.type(),
                expression.optional())) {
    return Status::InvalidArgument("filter references a slot outside the input schema");
  }
  if (expression.kind() == internal::ExpressionKind::kParameter) {
    if (expression.parameter().value == 0) {
      return Status::InvalidArgument("filter contains an invalid ParameterId");
    }
    const auto [it, inserted] =
        parameter_types.emplace(expression.parameter().value, expression.type());
    if (!inserted && it->second != expression.type()) {
      return Status::InvalidArgument(
          "filter uses a ParameterId with conflicting QueryTypes");
    }
  }
  for (const auto& child : expression.children()) {
    if (!child) {
      return Status::InvalidArgument("filter predicate contains an invalid expression");
    }
    if (Status status =
            ValidateExpressionSlots(*child, schema, parameter_types);
        !status.ok()) {
      return status;
    }
  }
  return Status::OK();
}

}  // namespace

StatusOr<Query> Query::Vertices(Slot<VertexRef> vertex, TemporalScope scope) {
  if (vertex.id().value == 0) {
    return Status::InvalidArgument("vertex SlotId is invalid");
  }
  if (Status status = ValidateScope(scope); !status.ok()) return status;
  return Query(MakeScopedScan(internal::LogicalOpKind::kVertexScan,
                              {vertex.id(), vertex.name(), vertex.type(), false},
                              std::move(scope)));
}

StatusOr<Query> Query::VertexPoint(VertexRef ref, Slot<VertexRef> vertex,
                                   TemporalScope scope) {
  if (!ref.valid() || vertex.id().value == 0) {
    return Status::InvalidArgument("vertex point lookup",
                                   "vertex reference or SlotId is invalid");
  }
  if (Status status = ValidateScope(scope); !status.ok()) return status;
  return Query(MakeScopedPoint(
      ref, {vertex.id(), vertex.name(), vertex.type(), false},
      std::move(scope)));
}

StatusOr<Query> Query::Edges(Slot<EdgeRef> edge, TemporalScope scope) {
  if (edge.id().value == 0) {
    return Status::InvalidArgument("edge SlotId is invalid");
  }
  if (Status status = ValidateScope(scope); !status.ok()) return status;
  return Query(MakeScopedScan(internal::LogicalOpKind::kEdgeScan,
                              {edge.id(), edge.name(), edge.type(), false},
                              std::move(scope)));
}

StatusOr<Query> Query::Expand(const ExpandSpec& spec) const {
  if (!root_ || !Contains(schema(), spec.source.id(), QueryType::kVertexRef,
                          false) ||
      spec.edge.id().value == 0 || spec.destination.id().value == 0 ||
      spec.edge.id() == spec.destination.id() ||
      spec.source.id() == spec.edge.id() ||
      spec.source.id() == spec.destination.id() ||
      HasSlotId(schema(), spec.edge.id()) ||
      HasSlotId(schema(), spec.destination.id())) {
    return Status::InvalidArgument("expand slots are invalid or duplicate");
  }
  std::vector<RowColumn> columns = schema().columns();
  columns.push_back({spec.edge.id(), spec.edge.name(), spec.edge.type(), false});
  columns.push_back(
      {spec.destination.id(), spec.destination.name(), spec.destination.type(), false});
  internal::LogicalOpKind kind;
  switch (spec.direction) {
    case ExpandDirection::kOut:
      kind = internal::LogicalOpKind::kExpandOut;
      break;
    case ExpandDirection::kIn:
      kind = internal::LogicalOpKind::kExpandIn;
      break;
    case ExpandDirection::kBoth:
      kind = internal::LogicalOpKind::kExpandBoth;
      break;
    default:
      return Status::InvalidArgument("expand direction is invalid");
  }
  internal::LogicalPlanPayload payload;
  payload.expand_spec = spec;
  return Query(Append(kind, root_, RowSchema(std::move(columns)),
                      std::move(payload)));
}

StatusOr<Query> Query::KHopExpand(const ExpandSpec& spec, uint32_t max_hops) const {
  if (max_hops == 0) {
    return Status::InvalidArgument("k-hop expand", "max_hops must be non-zero");
  }
  auto expanded = Expand(spec);
  if (!expanded.ok()) return expanded.status();
  const auto root = expanded.ValueOrDie().root_;
  // Preserve the expansion shape while marking the logical node as bounded
  // k-hop work.  The runtime consumes this marker when it materializes the
  // frontier.
  const auto* node = root.get();
  internal::LogicalPlanPayload payload;
  payload.expand_spec = spec;
  payload.max_hops = max_hops;
  return Query(Append(internal::LogicalOpKind::kKHopExpand,
                      node->inputs().front(), node->schema(), std::move(payload)));
}

StatusOr<Query> Query::CoexistingShortestPath(const ExpandSpec& spec,
                                             uint32_t max_hops,
                                             Slot<PathValue> path) const {
  if (max_hops == 0) {
    return Status::InvalidArgument("coexisting shortest path",
                                   "max_hops must be non-zero");
  }
  if (!root_ || !Contains(schema(), spec.source.id(), QueryType::kVertexRef,
                          false) || spec.edge.id().value == 0 ||
      spec.destination.id().value == 0 || path.id().value == 0 ||
      HasSlotId(schema(), spec.edge.id()) ||
      HasSlotId(schema(), spec.destination.id()) || HasSlotId(schema(), path.id()) ||
      spec.source.id() == spec.edge.id() || spec.source.id() == spec.destination.id() ||
      spec.edge.id() == spec.destination.id()) {
    return Status::InvalidArgument("coexisting shortest path",
                                   "slots are invalid or duplicate");
  }
  std::vector<RowColumn> columns = schema().columns();
  columns.push_back({spec.edge.id(), spec.edge.name(), spec.edge.type(), false});
  columns.push_back({spec.destination.id(), spec.destination.name(),
                     spec.destination.type(), false});
  columns.push_back({path.id(), path.name(), path.type(), false});
  internal::LogicalPlanPayload payload;
  payload.expand_spec = spec;
  payload.max_hops = max_hops;
  return Query(Append(internal::LogicalOpKind::kCoexistingShortestPath, root_,
                      RowSchema(std::move(columns)), std::move(payload)));
}

namespace {
StatusOr<std::shared_ptr<const internal::LogicalPlanNode>> AppendJourney(const Query& query, const ExpandSpec& spec,
                              uint32_t max_hops, PropertyId duration_property,
                              Slot<JourneyValue> journey,
                              internal::LogicalOpKind kind, uint8_t objective) {
  if (max_hops == 0 || !duration_property.valid() || journey.id().value == 0 ||
      !internal::LogicalPlanInspector::Inspect(query) ||
      !Contains(query.schema(), spec.source.id(), QueryType::kVertexRef, false) ||
      spec.edge.id().value == 0 || spec.destination.id().value == 0 ||
      HasSlotId(query.schema(), spec.edge.id()) ||
      HasSlotId(query.schema(), spec.destination.id()) ||
      HasSlotId(query.schema(), journey.id())) {
    return Status::InvalidArgument("temporal journey", "slots or duration are invalid");
  }
  std::vector<RowColumn> columns = query.schema().columns();
  columns.push_back({spec.edge.id(), spec.edge.name(), spec.edge.type(), false});
  columns.push_back({spec.destination.id(), spec.destination.name(), spec.destination.type(), false});
  columns.push_back({journey.id(), journey.name(), journey.type(), false});
  internal::LogicalPlanPayload payload;
  payload.expand_spec = spec;
  payload.max_hops = max_hops;
  payload.journey_duration_property = duration_property;
  payload.journey_slot = journey.id();
  payload.journey_objective = objective;
  return Append(kind, internal::LogicalPlanInspector::InspectShared(query),
                RowSchema(std::move(columns)), std::move(payload));
}
}  // namespace

StatusOr<Query> Query::EarliestArrival(const ExpandSpec& spec, uint32_t max_hops,
                                       PropertyId duration_property,
                                       Slot<JourneyValue> journey) const {
  auto root = AppendJourney(*this, spec, max_hops, duration_property, journey,
                            internal::LogicalOpKind::kEarliestArrival, 0);
  if (!root.ok()) return root.status();
  return Query(root.ValueOrDie());
}

StatusOr<Query> Query::LatestDeparture(const ExpandSpec& spec, uint32_t max_hops,
                                       PropertyId duration_property,
                                       Slot<JourneyValue> journey) const {
  auto root = AppendJourney(*this, spec, max_hops, duration_property, journey,
                            internal::LogicalOpKind::kLatestDeparture, 1);
  if (!root.ok()) return root.status();
  return Query(root.ValueOrDie());
}

StatusOr<Query> Query::FastestDuration(const ExpandSpec& spec, uint32_t max_hops,
                                       PropertyId duration_property,
                                       Slot<JourneyValue> journey) const {
  auto root = AppendJourney(*this, spec, max_hops, duration_property, journey,
                            internal::LogicalOpKind::kFastestDuration, 2);
  if (!root.ok()) return root.status();
  return Query(root.ValueOrDie());
}

StatusOr<Query> Query::BindVertexPropertyImpl(SlotId vertex,
                                               PropertyId property,
                                               RowColumn output) const {
  if (!root_ || !property.valid() || output.slot.value == 0 ||
      !Contains(schema(), vertex, QueryType::kVertexRef, false) ||
      HasSlotId(schema(), output.slot)) {
    return Status::InvalidArgument("property binding is invalid or duplicates a SlotId");
  }
  std::vector<RowColumn> columns = schema().columns();
  columns.push_back(output);
  internal::LogicalPlanPayload payload;
  payload.property_binding = internal::PropertyBinding{vertex, property, output};
  return Query(Append(internal::LogicalOpKind::kBindProperty, root_,
                      RowSchema(std::move(columns)), std::move(payload)));
}

StatusOr<Query> Query::BindEdgePropertyImpl(SlotId edge, PropertyId property,
                                             RowColumn output) const {
  if (!root_ || !property.valid() || output.slot.value == 0 ||
      !Contains(schema(), edge, QueryType::kEdgeRef, false) ||
      HasSlotId(schema(), output.slot)) {
    return Status::InvalidArgument(
        "property binding is invalid or duplicates a SlotId");
  }
  std::vector<RowColumn> columns = schema().columns();
  columns.push_back(output);
  internal::LogicalPlanPayload payload;
  payload.property_binding = internal::PropertyBinding{edge, property, output};
  return Query(Append(internal::LogicalOpKind::kBindProperty, root_,
                      RowSchema(std::move(columns)), std::move(payload)));
}

StatusOr<Query> Query::Where(Expr<bool> predicate) const {
  if (!root_ || !predicate.valid()) {
    return Status::InvalidArgument("filter predicate is invalid");
  }
  const auto expression = internal::ExpressionInspector::Share(predicate);
  std::unordered_map<uint32_t, QueryType> parameter_types;
  if (Status status =
          ValidateExpressionSlots(*expression, schema(), parameter_types);
      !status.ok()) {
    return status;
  }
  internal::LogicalPlanPayload payload;
  payload.predicate = std::move(expression);
  return Query(Append(internal::LogicalOpKind::kFilter, root_, schema(),
                      std::move(payload)));
}

StatusOr<Query> Query::Select(std::vector<Projection> projections) const {
  if (!root_) return Status::InvalidArgument("query has no logical plan");
  std::vector<RowColumn> columns;
  columns.reserve(projections.size());
  for (const Projection& projection : projections) {
    const RowColumn* input_column = FindColumn(
        schema(), projection.column.slot, projection.column.type,
        projection.column.optional);
    if (input_column == nullptr) {
      return Status::InvalidArgument("projection is not in the input schema");
    }
    columns.push_back(*input_column);
  }
  if (Status status = ValidateColumns(columns); !status.ok()) return status;
  return Query(Append(internal::LogicalOpKind::kProject, root_,
                      RowSchema(std::move(columns))));
}

StatusOr<Query> Query::Limit(size_t offset, size_t count) const {
  if (!root_) return Status::InvalidArgument("query has no logical plan");
  internal::LogicalPlanPayload payload;
  payload.limit_offset = offset;
  payload.limit_count = count;
  return Query(Append(internal::LogicalOpKind::kLimit, root_, schema(),
                      std::move(payload)));
}

StatusOr<Query> Query::ProjectMetadata(SlotId source, MetadataKind kind,
                                       Projection output) const {
  if (!root_ || source.value == 0 || output.column.slot.value == 0 ||
      HasSlotId(schema(), output.column.slot)) {
    return Status::InvalidArgument("metadata projection", "invalid or duplicate slot");
  }
  const auto source_column = std::find_if(
      schema().columns().begin(), schema().columns().end(),
      [source](const RowColumn& column) { return column.slot == source; });
  if (source_column == schema().columns().end() ||
      (source_column->type != QueryType::kVertexRef &&
       source_column->type != QueryType::kEdgeRef) ||
      (kind == MetadataKind::kValidFrom && output.column.type != QueryType::kValidTime) ||
      (kind == MetadataKind::kCommitSeq && output.column.type != QueryType::kCommitSeq)) {
    return Status::InvalidArgument("metadata projection", "source or output type is invalid");
  }
  std::vector<RowColumn> columns = schema().columns();
  columns.push_back(output.column);
  internal::LogicalPlanPayload payload;
  payload.metadata_binding = internal::MetadataBinding{source, kind, output.column};
  return Query(Append(internal::LogicalOpKind::kMetadataProject, root_,
                      RowSchema(std::move(columns)), std::move(payload)));
}

const RowSchema& Query::schema() const {
  static const RowSchema empty;
  return root_ ? root_->schema() : empty;
}

Query Query::WithExecutionScope(ExecutionScope scope) const {
  Query result(root_);
  result.execution_scope_ = std::move(scope);
  return result;
}

}  // namespace cedar
