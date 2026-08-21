// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_RUNTIME_QUERY_RUNTIME_H_
#define CEDAR_QUERY_RUNTIME_QUERY_RUNTIME_H_

#include <limits>
#include <memory>
#include <optional>
#include <vector>

#include "cedar/query/query.h"
#include "cedar/query/result.h"
#include "cedar/schema.h"
#include "cedar/snapshot.h"
#include "query/logical/expression.h"
#include "query/logical/logical_plan.h"
#include "query/runtime/relational.h"

namespace cedar::internal {

struct PreparedPropertyBinding {
  SlotId source;
  PropertyId property;
  RowColumn output;
  PropertyEntityKind entity_kind;
  std::optional<PropertyDefinition> definition;
};

// Private pull-runtime boundary between logical relational nodes and physical
// vector operators. It is intentionally unavailable from the public Query API.
struct RuntimeRelationalInput {
  BatchStream left;
  BatchStream right;
  size_t left_key = 0;
  size_t right_key = 0;
  size_t estimated_rows = 0;
  bool sorted_keys = false;
  bool temporal = false;
  std::vector<SortKey> sort_keys;
  size_t offset = 0;
  size_t count = 0;
  std::vector<size_t> group_by;
  std::vector<AggregateSpec> aggregates;
};

struct RuntimeRelationalResult {
  BatchStream stream;
  std::optional<JoinAlgorithm> join_algorithm;
};

struct PreparedQueryPlan {
  bool canonical_temporal = false;
  FactFamily entity_family = FactFamily::kVertexState;
  SlotId entity_slot;
  TemporalScope scope = At{ValidTime{0}};
  std::vector<PreparedPropertyBinding> property_bindings;
  std::shared_ptr<const ExpressionNode> predicate;
  std::vector<RowColumn> output_columns;
  std::optional<SlotId> effective_output_slot;
  std::vector<PropertyId> referenced_properties;
  std::optional<LogicalOpKind> relational_kind;
  std::optional<RuntimeRelationalInput> relational_input;
};

StatusOr<PreparedQueryPlan> AnalyzeQuery(const Query& query);

StatusOr<RuntimeRelationalResult> ExecuteRelationalPlanNode(
    LogicalOpKind kind, RuntimeRelationalInput input,
    QueryReservation* reservation,
    FragmentBudget* fragment_budget = nullptr,
    size_t max_output_rows = std::numeric_limits<size_t>::max());

class QueryRuntime {
 public:
  static StatusOr<QueryCursor> Execute(const PreparedQueryPlan& plan,
                                       Snapshot snapshot,
                                       const Bindings& bindings,
                                       const QueryOptions& options);
};

}  // namespace cedar::internal

#endif  // CEDAR_QUERY_RUNTIME_QUERY_RUNTIME_H_
