// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_QUERY_H_
#define CEDAR_QUERY_QUERY_H_

#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "cedar/query/expression.h"

namespace cedar {
struct PathValue;
struct JourneyValue;
namespace internal {
class LogicalPlanInspector;
class LogicalPlanNode;
}
struct RowColumn {
  SlotId slot;
  std::string name;
  QueryType type;
  bool optional = false;

  bool operator==(const RowColumn&) const = default;
};

class RowSchema {
 public:
  RowSchema() = default;
  explicit RowSchema(std::vector<RowColumn> columns)
      : columns_(std::move(columns)) {}

  const std::vector<RowColumn>& columns() const { return columns_; }

 private:
  std::vector<RowColumn> columns_;
};

struct Projection {
  RowColumn column;
};

template <typename T, bool Optional>
Projection Project(const Slot<T, Optional>& slot) {
  return Projection{{slot.id(), slot.name(), slot.type(), Optional}};
}

enum class ExpandDirection : uint8_t { kOut, kIn, kBoth };
struct ExpandSpec {
  Slot<VertexRef> source;
  Slot<EdgeRef> edge;
  Slot<VertexRef> destination;
  ExpandDirection direction = ExpandDirection::kOut;
  std::optional<uint64_t> edge_type;
};

class Query {
 public:
  static StatusOr<Query> Vertices(Slot<VertexRef> vertex, TemporalScope scope);
  static StatusOr<Query> Edges(Slot<EdgeRef> edge, TemporalScope scope);
  StatusOr<Query> Expand(const ExpandSpec& spec) const;
  StatusOr<Query> KHopExpand(const ExpandSpec& spec, uint32_t max_hops) const;
  StatusOr<Query> CoexistingShortestPath(const ExpandSpec& spec,
                                         uint32_t max_hops,
                                         Slot<PathValue> path) const;
  StatusOr<Query> EarliestArrival(const ExpandSpec& spec, uint32_t max_hops,
                                  PropertyId duration_property,
                                  Slot<JourneyValue> journey) const;
  StatusOr<Query> LatestDeparture(const ExpandSpec& spec, uint32_t max_hops,
                                  PropertyId duration_property,
                                  Slot<JourneyValue> journey) const;
  StatusOr<Query> FastestDuration(const ExpandSpec& spec, uint32_t max_hops,
                                  PropertyId duration_property,
                                  Slot<JourneyValue> journey) const;
  template <typename T>
  StatusOr<Query> BindVertexProperty(Slot<VertexRef> vertex,
                                     PropertyId property,
                                     OptionalSlot<T> output) const {
    return BindVertexPropertyImpl(
        vertex.id(), property, {output.id(), output.name(), output.type(), true});
  }
  template <typename T>
  StatusOr<Query> BindEdgeProperty(Slot<EdgeRef> edge, PropertyId property,
                                   OptionalSlot<T> output) const {
    return BindEdgePropertyImpl(
        edge.id(), property, {output.id(), output.name(), output.type(), true});
  }
  StatusOr<Query> Where(Expr<bool> predicate) const;
  StatusOr<Query> Select(std::vector<Projection> projections) const;
  const RowSchema& schema() const;
 private:
  explicit Query(std::shared_ptr<const internal::LogicalPlanNode> root)
      : root_(std::move(root)) {}

  StatusOr<Query> BindVertexPropertyImpl(SlotId vertex, PropertyId property,
                                         RowColumn output) const;
  StatusOr<Query> BindEdgePropertyImpl(SlotId edge, PropertyId property,
                                       RowColumn output) const;
  std::shared_ptr<const internal::LogicalPlanNode> root_;
  friend class internal::LogicalPlanInspector;
};
}  // namespace cedar

#endif  // CEDAR_QUERY_QUERY_H_
