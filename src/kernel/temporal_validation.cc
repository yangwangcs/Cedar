// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "kernel/temporal_validation.h"

namespace cedar {

StatusOr<SnapshotWriteDependency> CaptureSnapshotWriteDependency(
    const FactStore& store, const StoreSnapshot& snapshot, const FactRef& ref,
    ValidTime valid_time) {
  const auto neighborhood = store.ReadTemporalNeighborhood(snapshot, ref, valid_time);
  if (!neighborhood.ok()) return neighborhood.status();
  return SnapshotWriteDependency{ref, valid_time,
      neighborhood.ValueOrDie().predecessor, neighborhood.ValueOrDie().successor,
      snapshot.commit_seq()};
}

StatusOr<std::vector<SnapshotWriteDependency>> DeriveSnapshotWriteDependencies(
    const FactStore& store, const StoreSnapshot& snapshot,
    const std::vector<PendingFactMutation>& mutations) {
  std::vector<SnapshotWriteDependency> dependencies;
  dependencies.reserve(mutations.size());
  for (const PendingFactMutation& mutation : mutations) {
    const Status valid = mutation.Validate();
    if (!valid.ok()) return valid;

    auto dependency = CaptureSnapshotWriteDependency(
        store, snapshot, mutation.ref, mutation.valid_from);
    if (!dependency.ok()) return dependency.status();
    dependencies.push_back(dependency.ConsumeValueOrDie());
  }
  return dependencies;
}

StatusOr<StrictReadDependency> CaptureStrictReadDependency(
    const FactStore& store, const StoreSnapshot& snapshot, const FactRef& ref,
    ValidTime valid_time) {
  const Status valid = ref.Validate();
  if (!valid.ok()) return valid;

  const auto neighborhood = store.ReadTemporalNeighborhood(snapshot, ref, valid_time);
  if (!neighborhood.ok()) return neighborhood.status();
  StrictReadDependency dependency{ref, valid_time, snapshot.commit_seq()};
  dependency.observed_event = neighborhood.ValueOrDie().observed;
  dependency.predecessor = neighborhood.ValueOrDie().predecessor;
  dependency.successor = neighborhood.ValueOrDie().successor;
  return dependency;
}

}  // namespace cedar
