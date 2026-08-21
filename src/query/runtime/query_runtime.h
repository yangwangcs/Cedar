// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_RUNTIME_QUERY_RUNTIME_H_
#define CEDAR_QUERY_RUNTIME_QUERY_RUNTIME_H_

#include <memory>
#include <optional>
#include <vector>

#include "cedar/query/query.h"
#include "cedar/query/result.h"
#include "cedar/schema.h"
#include "cedar/snapshot.h"
#include "query/logical/expression.h"

namespace cedar::internal {

struct PreparedPropertyBinding {
  SlotId source;
  PropertyId property;
  RowColumn output;
  PropertyEntityKind entity_kind;
  std::optional<PropertyDefinition> definition;
};

struct PreparedQueryPlan {
  bool canonical_temporal = false;
  FactFamily entity_family = FactFamily::kVertexState;
  SlotId entity_slot;
  TemporalScope scope = At{ValidTime{0}};
  std::vector<PreparedPropertyBinding> property_bindings;
  std::shared_ptr<const ExpressionNode> predicate;
  std::vector<RowColumn> output_columns;
  std::vector<PropertyId> referenced_properties;
};

StatusOr<PreparedQueryPlan> AnalyzeQuery(const Query& query);

class QueryRuntime {
 public:
  static StatusOr<QueryCursor> Execute(const PreparedQueryPlan& plan,
                                       Snapshot snapshot,
                                       const Bindings& bindings,
                                       const QueryOptions& options);
};

}  // namespace cedar::internal

#endif  // CEDAR_QUERY_RUNTIME_QUERY_RUNTIME_H_
