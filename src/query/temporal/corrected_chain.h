// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_TEMPORAL_CORRECTED_CHAIN_H_
#define CEDAR_QUERY_TEMPORAL_CORRECTED_CHAIN_H_

#include <optional>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/fact/fact.h"
#include "query/temporal/interval.h"

namespace cedar::internal {

struct CorrectedBoundary {
  ValidTime valid_from;
  CommitSeq commit_seq;
  FactOperation operation;
  uint32_t schema_epoch;
  std::optional<Value> value;
  std::optional<EdgeIdentity> edge_identity;

  bool operator==(const CorrectedBoundary&) const = default;
};

StatusOr<std::vector<CorrectedBoundary>> ResolveCorrectedBoundaries(
    const std::vector<FactEvent>& events, CommitSeq snapshot_seq);
std::vector<StateInterval> MaterializePresentState(
    const std::vector<CorrectedBoundary>& boundaries);
std::vector<StateInterval> MaterializeMissingState(
    const std::vector<CorrectedBoundary>& boundaries,
    const ValidTimeInterval& enclosing_entity_interval);

}  // namespace cedar::internal

#endif  // CEDAR_QUERY_TEMPORAL_CORRECTED_CHAIN_H_
