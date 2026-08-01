// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "kernel/temporal_validation.h"

#include <set>

namespace cedar {

StatusOr<std::vector<SnapshotWriteDependency>> DeriveSnapshotWriteDependencies(
    const FactStore& store, const StoreSnapshot& snapshot,
    const std::vector<PendingFactMutation>& mutations) {
  std::vector<SnapshotWriteDependency> dependencies;
  dependencies.reserve(mutations.size());
  for (const PendingFactMutation& mutation : mutations) {
    const Status valid = mutation.Validate();
    if (!valid.ok()) return valid;

    std::set<uint64_t> boundaries;
    const Status scanned = store.Scan(
        snapshot, FactPrefix::Exact(mutation.ref),
        [&boundaries](const FactEvent& event) {
          boundaries.insert(event.valid_from.value);
          return Status::OK();
        });
    if (!scanned.ok()) return scanned;

    const auto predecessor_boundary =
        boundaries.lower_bound(mutation.valid_from.value);
    std::optional<ValidTime> predecessor;
    if (predecessor_boundary != boundaries.begin()) {
      predecessor = ValidTime{*std::prev(predecessor_boundary)};
    }
    const auto successor = boundaries.upper_bound(mutation.valid_from.value);
    std::optional<ValidTime> successor_time;
    if (successor != boundaries.end()) {
      successor_time = ValidTime{*successor};
    }
    dependencies.push_back(SnapshotWriteDependency{
        mutation.ref, mutation.valid_from, predecessor, successor_time,
        snapshot.commit_seq()});
  }
  return dependencies;
}

}  // namespace cedar
