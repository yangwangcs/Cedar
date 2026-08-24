// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_RUNTIME_TEMPORAL_SOURCE_H_
#define CEDAR_QUERY_RUNTIME_TEMPORAL_SOURCE_H_

#include <optional>
#include <vector>

#include "cedar/query/types.h"
#include "cedar/snapshot.h"

namespace cedar::internal {

struct EventRow {
  FactRef ref;
  ValidTime valid_from;
  CommitSeq commit_seq;
  FactOperation operation;
  std::optional<Value> value;
  uint32_t schema_epoch = 0;

  bool operator==(const EventRow&) const = default;
};

struct ChangeRow {
  FactRef ref;
  ValidTime valid_from;
  std::optional<Value> before;
  std::optional<Value> after;

  bool operator==(const ChangeRow&) const = default;
};

struct StateRow {
  FactRef ref;
  ValidTimeInterval effective;
  std::optional<Value> value;

  bool operator==(const StateRow&) const = default;
};

class TemporalSource {
 public:
  static StatusOr<std::vector<EventRow>> ReadEvents(
      Snapshot& snapshot, FactFamily family, PropertyId property,
      const ValidTimeInterval& interval);
  static StatusOr<std::vector<ChangeRow>> ReadChanges(
      Snapshot& snapshot, FactFamily family, PropertyId property,
      const ValidTimeInterval& interval);
  static StatusOr<std::vector<StateRow>> ReadAt(
      Snapshot& snapshot, FactFamily family, PropertyId property,
      ValidTime valid_time);
  static StatusOr<std::vector<StateRow>> ReadHistory(
      Snapshot& snapshot, FactFamily family, PropertyId property,
      std::optional<ValidTimeInterval> interval);
  static StatusOr<std::vector<StateRow>> ReadOverlaps(
      Snapshot& snapshot, FactFamily family, PropertyId property,
      const ValidTimeInterval& interval);
  static StatusOr<std::vector<StateRow>> ReadThroughout(
      Snapshot& snapshot, FactFamily family, PropertyId property,
      const ValidTimeInterval& interval);
};

}  // namespace cedar::internal

#endif  // CEDAR_QUERY_RUNTIME_TEMPORAL_SOURCE_H_
