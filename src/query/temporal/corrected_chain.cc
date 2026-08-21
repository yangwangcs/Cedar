// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/temporal/corrected_chain.h"

#include <algorithm>

namespace cedar::internal {
namespace {

bool IsBefore(const ValidTime& left, const ValidTime& right) {
  return left.value < right.value;
}

bool IsBeforeOrEqual(const ValidTime& left, const ValidTime& right) {
  return left.value <= right.value;
}

CorrectedBoundary ToBoundary(const FactEvent& event) {
  return CorrectedBoundary{event.valid_from, event.commit_seq, event.operation,
                           event.schema_epoch, event.value,
                           event.edge_identity};
}

void Apply(const CorrectedBoundary& boundary, std::optional<Value>* value) {
  if (boundary.operation == FactOperation::kPut) {
    *value = boundary.value;
  } else {
    value->reset();
  }
}

}  // namespace

StatusOr<std::vector<CorrectedBoundary>> ResolveCorrectedBoundaries(
    const std::vector<FactEvent>& events, CommitSeq snapshot_seq) {
  std::vector<FactEvent> visible;
  visible.reserve(events.size());
  std::optional<FactRef> ref;
  for (const FactEvent& event : events) {
    const Status status = event.Validate();
    if (!status.ok()) return status;
    if (!ref.has_value()) {
      ref = event.ref;
    } else if (*ref != event.ref) {
      return Status::InvalidArgument("corrected fact chain", "mixed fact references");
    }
    if (event.commit_seq.value <= snapshot_seq.value) visible.push_back(event);
  }

  std::sort(visible.begin(), visible.end(), [](const FactEvent& left,
                                                const FactEvent& right) {
    if (left.valid_from != right.valid_from) {
      return left.valid_from.value < right.valid_from.value;
    }
    return left.commit_seq.value < right.commit_seq.value;
  });

  std::vector<CorrectedBoundary> result;
  for (size_t index = 0; index < visible.size();) {
    size_t next = index + 1;
    while (next < visible.size() &&
           visible[next].valid_from == visible[index].valid_from) {
      ++next;
    }
    result.push_back(ToBoundary(visible[next - 1]));
    index = next;
  }
  return result;
}

std::vector<StateInterval> MaterializePresentState(
    const std::vector<CorrectedBoundary>& boundaries) {
  std::vector<StateInterval> present;
  bool is_present = false;
  std::optional<Value> value;
  for (size_t index = 0; index < boundaries.size(); ++index) {
    is_present = boundaries[index].operation == FactOperation::kPut;
    Apply(boundaries[index], &value);
    const std::optional<ValidTime> to =
        index + 1 == boundaries.size()
            ? std::nullopt
            : std::optional<ValidTime>(boundaries[index + 1].valid_from);
    if (is_present) {
      present.push_back(
          StateInterval{{boundaries[index].valid_from, to}, value});
    }
  }
  return Coalesce(std::move(present));
}

std::vector<StateInterval> MaterializeMissingState(
    const std::vector<CorrectedBoundary>& boundaries,
    const ValidTimeInterval& enclosing_entity_interval) {
  if (!enclosing_entity_interval.Validate().ok()) return {};

  std::optional<Value> value;
  size_t index = 0;
  while (index < boundaries.size() &&
         IsBeforeOrEqual(boundaries[index].valid_from,
                         enclosing_entity_interval.from)) {
    Apply(boundaries[index], &value);
    ++index;
  }

  ValidTime cursor = enclosing_entity_interval.from;
  std::vector<StateInterval> missing;
  while (index < boundaries.size()) {
    const ValidTime next = boundaries[index].valid_from;
    if (enclosing_entity_interval.to.has_value() &&
        !IsBefore(next, *enclosing_entity_interval.to)) {
      break;
    }
    if (!value.has_value() && IsBefore(cursor, next)) {
      missing.push_back(StateInterval{{cursor, next}, std::nullopt});
    }
    Apply(boundaries[index], &value);
    cursor = next;
    ++index;
  }
  if (!value.has_value() &&
      (!enclosing_entity_interval.to.has_value() ||
       IsBefore(cursor, *enclosing_entity_interval.to))) {
    missing.push_back(StateInterval{{cursor, enclosing_entity_interval.to},
                                    std::nullopt});
  }
  return Coalesce(std::move(missing));
}

}  // namespace cedar::internal
