// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/tcypher/runtime/interval_derive.h"

#include <map>

namespace cedar {

StatusOr<std::vector<TemporalInterval>> DeriveVisibleIntervals(
    const std::vector<TemporalEvent>& events, const LogicalKey& key,
    uint64_t snapshot_seq, uint64_t range_start, uint64_t range_end) {
  if (range_start >= range_end) {
    return Status::InvalidArgument("interval derive", "empty or reversed valid-time range");
  }
  std::map<uint64_t, TemporalEvent> selected;
  for (const TemporalEvent& event : events) {
    if (event.logical_key() != key || event.commit_seq() > snapshot_seq) continue;
    const auto existing = selected.find(event.valid_from());
    if (existing == selected.end() || event.commit_seq() > existing->second.commit_seq()) {
      selected.insert_or_assign(event.valid_from(), event);
    }
  }
  std::vector<TemporalInterval> intervals;
  for (auto iterator = selected.begin(); iterator != selected.end(); ++iterator) {
    const auto successor = std::next(iterator);
    const uint64_t valid_to = successor == selected.end() ? kTemporalInfinity : successor->first;
    if (iterator->first < range_end && valid_to > range_start) {
      intervals.push_back(TemporalInterval{iterator->second, iterator->first, valid_to});
    }
  }
  return intervals;
}

StatusOr<std::vector<RawTemporalInterval>> DeriveRawTemporalIntervals(
    const std::vector<RawTemporalFact>& facts, uint64_t range_start,
    uint64_t range_end) {
  if (range_start >= range_end) {
    return Status::InvalidArgument("interval derive", "empty or reversed valid-time range");
  }
  std::map<uint64_t, RawTemporalFact> selected;
  for (const RawTemporalFact& fact : facts) {
    const auto existing = selected.find(fact.valid_from);
    if (existing == selected.end() || fact.commit_seq > existing->second.commit_seq) {
      selected.insert_or_assign(fact.valid_from, fact);
    }
  }
  std::vector<RawTemporalInterval> intervals;
  for (auto iterator = selected.begin(); iterator != selected.end(); ++iterator) {
    const auto successor = std::next(iterator);
    const uint64_t valid_to = successor == selected.end()
        ? kTemporalInfinity : successor->first;
    if (iterator->first < range_end && valid_to > range_start) {
      intervals.push_back(RawTemporalInterval{
          iterator->second, iterator->first, valid_to});
    }
  }
  return intervals;
}

}  // namespace cedar
