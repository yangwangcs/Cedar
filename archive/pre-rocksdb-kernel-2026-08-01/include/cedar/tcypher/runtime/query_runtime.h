// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TCYPHER_RUNTIME_QUERY_RUNTIME_H_
#define CEDAR_TCYPHER_RUNTIME_QUERY_RUNTIME_H_

#include <memory>

#include "cedar/tcypher/executor.h"
#include "cedar/tcypher/physical_plan.h"
#include "cedar/tcypher/query_snapshot.h"

namespace cedar {

StatusOr<QuerySnapshot> BuildPhysicalQuerySnapshot(
    const BoundTcypherStatement& statement,
    const TcypherExecutionContext& context);

StatusOr<std::unique_ptr<QueryResultStream>> OpenPhysicalRootPointRuntime(
    std::shared_ptr<const PhysicalPlan> plan, QuerySnapshot snapshot,
    TcypherExecutionContext context);

// The first physical join is intentionally narrow: child plans project
// [binding, scalar-key], and the output is the paired bindings.  Both inputs
// remain pinned physical scans; only build-side key/value pairs are retained.
StatusOr<std::unique_ptr<QueryResultStream>> OpenPhysicalHashJoinRuntime(
    std::shared_ptr<const PhysicalHashJoinPlan> plan, QuerySnapshot snapshot,
    TcypherExecutionContext context);

StatusOr<std::unique_ptr<QueryResultStream>> OpenPhysicalMultiHashJoinRuntime(
    std::shared_ptr<const PhysicalMultiHashJoinPlan> plan,
    QuerySnapshot snapshot, TcypherExecutionContext context);

}  // namespace cedar

#endif  // CEDAR_TCYPHER_RUNTIME_QUERY_RUNTIME_H_
