// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_KERNEL_POINT_READ_H_
#define CEDAR_KERNEL_POINT_READ_H_

#include <optional>

#include "cedar/core/status.h"
#include "cedar/fact/fact.h"
#include "storage/facts/fact_store.h"

namespace cedar {

// Resolves a single property point against an already-pinned store snapshot.
// This is the one read path shared by Snapshot::Get and Database::GetPointAt:
// it records the point-read metric, reads the fact at valid_time, and applies
// the edge-endpoint visibility filter to kEdgeProperty facts. Keeping both
// public entry points on this helper is what prevents their semantics from
// diverging.
StatusOr<std::optional<Value>> ReadPropertyPointAt(
    const FactStore& store, const StoreSnapshot& snapshot, const FactRef& ref,
    ValidTime valid_time);

}  // namespace cedar

#endif  // CEDAR_KERNEL_POINT_READ_H_
