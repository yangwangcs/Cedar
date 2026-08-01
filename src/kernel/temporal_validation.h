// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_KERNEL_TEMPORAL_VALIDATION_H_
#define CEDAR_KERNEL_TEMPORAL_VALIDATION_H_

#include <vector>

#include "cedar/fact/fact_store.h"

namespace cedar {

StatusOr<std::vector<SnapshotWriteDependency>> DeriveSnapshotWriteDependencies(
    const FactStore& store, const StoreSnapshot& snapshot,
    const std::vector<PendingFactMutation>& mutations);

StatusOr<StrictReadDependency> CaptureStrictReadDependency(
    const FactStore& store, const StoreSnapshot& snapshot, const FactRef& ref,
    ValidTime valid_time);

}  // namespace cedar

#endif  // CEDAR_KERNEL_TEMPORAL_VALIDATION_H_
