// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_RUNTIME_QUERY_RUNTIME_H_
#define CEDAR_QUERY_RUNTIME_QUERY_RUNTIME_H_

#include <vector>

#include "cedar/query/query.h"
#include "cedar/query/result.h"
#include "cedar/snapshot.h"

namespace cedar::internal {

struct PreparedQueryPlan {
  bool canonical_vertex_state_at = false;
  SlotId vertex_slot;
  ValidTime valid_time;
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
