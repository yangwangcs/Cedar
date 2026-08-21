// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_QUERY_H_
#define CEDAR_QUERY_QUERY_H_

#include <memory>
#include <utility>
#include <vector>

#include "cedar/query/expression.h"

namespace cedar {
namespace internal { class LogicalPlanNode; }
struct RowColumn { SlotId slot; QueryType type; bool optional = false; bool operator==(const RowColumn&) const = default; };
class RowSchema {
 public:
  RowSchema() = default;
  explicit RowSchema(std::vector<RowColumn> columns) : columns_(std::move(columns)) {}
  const std::vector<RowColumn>& columns() const { return columns_; }
 private:
  std::vector<RowColumn> columns_;
};
struct Projection { RowColumn column; };
template <typename T, bool Optional> Projection Project(const Slot<T, Optional>& slot) {
  return Projection{{slot.id(), slot.type(), Optional}};
}
enum class ExpandDirection : uint8_t { kOut, kIn, kBoth };
struct ExpandSpec { Slot<VertexRef> source; Slot<EdgeRef> edge; Slot<VertexRef> destination; ExpandDirection direction = ExpandDirection::kOut; };

class Query {
 public:
  static StatusOr<Query> Vertices(Slot<VertexRef> vertex, TemporalScope scope);
  static StatusOr<Query> Edges(Slot<EdgeRef> edge, TemporalScope scope);
  StatusOr<Query> Expand(const ExpandSpec& spec) const;
  template <typename T>
  StatusOr<Query> BindVertexProperty(Slot<VertexRef> vertex, PropertyId property, OptionalSlot<T> output) const {
    return BindVertexPropertyImpl(vertex.id(), property, {output.id(), output.type(), true});
  }
  StatusOr<Query> Where(Expr<bool> predicate) const;
  StatusOr<Query> Select(std::vector<Projection> projections) const;
  const RowSchema& schema() const;
 private:
  explicit Query(std::shared_ptr<const internal::LogicalPlanNode> root) : root_(std::move(root)) {}
  StatusOr<Query> BindVertexPropertyImpl(SlotId vertex, PropertyId property, RowColumn output) const;
  std::shared_ptr<const internal::LogicalPlanNode> root_;
};
}  // namespace cedar

#endif  // CEDAR_QUERY_QUERY_H_
