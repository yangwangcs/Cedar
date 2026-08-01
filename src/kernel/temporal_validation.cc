// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "kernel/temporal_validation.h"

#include <map>
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

StatusOr<StrictReadDependency> CaptureStrictReadDependency(
    const FactStore& store, const StoreSnapshot& snapshot, const FactRef& ref,
    ValidTime valid_time) {
  const Status valid = ref.Validate();
  if (!valid.ok()) return valid;

  std::map<uint64_t, FactEvent> boundaries;
  const Status scanned = store.Scan(
      snapshot, FactPrefix::Exact(ref), [&boundaries](const FactEvent& event) {
        const auto found = boundaries.find(event.valid_from.value);
        if (found == boundaries.end() ||
            found->second.commit_seq.value < event.commit_seq.value) {
          boundaries.insert_or_assign(event.valid_from.value, event);
        }
        return Status::OK();
      });
  if (!scanned.ok()) return scanned;

  StrictReadDependency dependency{ref, valid_time, snapshot.commit_seq()};
  const auto successor = boundaries.upper_bound(valid_time.value);
  if (successor != boundaries.end()) {
    dependency.successor = ValidTime{successor->first};
  }
  if (successor != boundaries.begin()) {
    const auto observed = std::prev(successor);
    dependency.observed_event = observed->second;
    dependency.predecessor = ValidTime{observed->first};
  }
  return dependency;
}

}  // namespace cedar
