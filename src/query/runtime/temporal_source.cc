// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/runtime/temporal_source.h"

#include <map>
#include <utility>

#include "query/temporal/corrected_chain.h"
#include "query/temporal/interval.h"

namespace cedar::internal {
namespace {

struct FactEntityKey {
  PartId part_id;
  uint64_t entity_id = 0;

  bool operator<(const FactEntityKey& other) const {
    if (part_id.value != other.part_id.value) {
      return part_id.value < other.part_id.value;
    }
    return entity_id < other.entity_id;
  }
};

using FactChains = std::map<FactEntityKey, std::vector<FactEvent>>;

StatusOr<FactChains> ReadChains(Snapshot& snapshot, FactFamily family,
                                PropertyId property) {
  FactChains chains;
  // PartId{0} is a real partition. ScanFamily is the storage boundary for a
  // family-wide read and visits every home partition.
  const Status scanned = snapshot.ScanFamily(
      family, [&chains, property](const FactEvent& event) {
        if (event.ref.property_id() != property) return Status::OK();
        chains[{event.ref.part_id(), event.ref.entity_id()}].push_back(event);
        return Status::OK();
      });
  if (!scanned.ok()) return scanned;
  return chains;
}

bool Contains(const ValidTimeInterval& interval, ValidTime time) {
  return interval.from.value <= time.value &&
         (!interval.to.has_value() || time.value < interval.to->value);
}

StatusOr<std::vector<StateRow>> Materialize(
    Snapshot& snapshot, FactFamily family, PropertyId property,
    std::optional<ValidTimeInterval> bounds) {
  if (bounds.has_value()) {
    const Status valid = bounds->Validate();
    if (!valid.ok()) return valid;
  }
  auto chains = ReadChains(snapshot, family, property);
  if (!chains.ok()) return chains.status();

  std::vector<StateRow> rows;
  for (const auto& [key, events] : chains.ValueOrDie()) {
    auto corrected = ResolveCorrectedBoundaries(events, snapshot.commit_seq());
    if (!corrected.ok()) return corrected.status();
    for (const StateInterval& state :
         MaterializePresentState(corrected.ValueOrDie())) {
      std::optional<ValidTimeInterval> effective = state.interval;
      if (bounds.has_value()) effective = Clip(state.interval, *bounds);
      if (!effective.has_value()) continue;
      rows.push_back({FactRef{key.part_id, family, property, key.entity_id}, *effective,
                      state.value});
    }
  }
  return rows;
}

}  // namespace

StatusOr<std::vector<EventRow>> TemporalSource::ReadEvents(
    Snapshot& snapshot, FactFamily family, PropertyId property,
    const ValidTimeInterval& interval) {
  const Status valid = interval.Validate();
  if (!valid.ok()) return valid;
  auto chains = ReadChains(snapshot, family, property);
  if (!chains.ok()) return chains.status();

  std::vector<EventRow> rows;
  for (const auto& [key, events] : chains.ValueOrDie()) {
    auto corrected = ResolveCorrectedBoundaries(events, snapshot.commit_seq());
    if (!corrected.ok()) return corrected.status();
    for (const CorrectedBoundary& boundary : corrected.ValueOrDie()) {
      if (!Contains(interval, boundary.valid_from)) continue;
      rows.push_back({FactRef{key.part_id, family, property, key.entity_id},
                      boundary.valid_from, boundary.commit_seq,
                      boundary.operation, boundary.value,
                      boundary.schema_epoch});
    }
  }
  return rows;
}

StatusOr<std::vector<ChangeRow>> TemporalSource::ReadChanges(
    Snapshot& snapshot, FactFamily family, PropertyId property,
    const ValidTimeInterval& interval) {
  const Status valid = interval.Validate();
  if (!valid.ok()) return valid;
  auto chains = ReadChains(snapshot, family, property);
  if (!chains.ok()) return chains.status();

  std::vector<ChangeRow> rows;
  for (const auto& [key, events] : chains.ValueOrDie()) {
    auto corrected = ResolveCorrectedBoundaries(events, snapshot.commit_seq());
    if (!corrected.ok()) return corrected.status();
    bool present = false;
    std::optional<Value> value;
    for (const CorrectedBoundary& boundary : corrected.ValueOrDie()) {
      const bool after_present = boundary.operation == FactOperation::kPut;
      const std::optional<Value> after = after_present ? boundary.value
                                                       : std::nullopt;
      const bool changed = present != after_present ||
                           (present && value != after);
      if (changed && Contains(interval, boundary.valid_from)) {
        rows.push_back({FactRef{key.part_id, family, property, key.entity_id},
                        boundary.valid_from, present ? value : std::nullopt,
                        after_present ? after : std::nullopt});
      }
      present = after_present;
      value = after;
    }
  }
  return rows;
}

StatusOr<std::vector<StateRow>> TemporalSource::ReadAt(
    Snapshot& snapshot, FactFamily family, PropertyId property,
    ValidTime valid_time) {
  auto rows = Materialize(snapshot, family, property, std::nullopt);
  if (!rows.ok()) return rows.status();
  std::vector<StateRow> result;
  for (StateRow& row : rows.ValueOrDie()) {
    if (Contains(row.effective, valid_time)) result.push_back(std::move(row));
  }
  return result;
}

StatusOr<std::vector<StateRow>> TemporalSource::ReadHistory(
    Snapshot& snapshot, FactFamily family, PropertyId property,
    std::optional<ValidTimeInterval> interval) {
  return Materialize(snapshot, family, property, std::move(interval));
}

StatusOr<std::vector<StateRow>> TemporalSource::ReadOverlaps(
    Snapshot& snapshot, FactFamily family, PropertyId property,
    const ValidTimeInterval& interval) {
  return Materialize(snapshot, family, property, interval);
}

StatusOr<std::vector<StateRow>> TemporalSource::ReadThroughout(
    Snapshot& snapshot, FactFamily family, PropertyId property,
    const ValidTimeInterval& interval) {
  const Status valid = interval.Validate();
  if (!valid.ok()) return valid;
  auto rows = Materialize(snapshot, family, property, std::nullopt);
  if (!rows.ok()) return rows.status();
  std::vector<StateRow> result;
  for (const StateRow& row : rows.ValueOrDie()) {
    const bool starts_before = row.effective.from.value <= interval.from.value;
    const bool ends_after = !row.effective.to.has_value() ||
                            (interval.to.has_value() &&
                             interval.to->value <= row.effective.to->value);
    if (starts_before && ends_after) {
      result.push_back({row.ref, interval, row.value});
    }
  }
  return result;
}

}  // namespace cedar::internal
