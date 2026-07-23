// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TCYPHER_RUNTIME_INTERVAL_ALIGN_H_
#define CEDAR_TCYPHER_RUNTIME_INTERVAL_ALIGN_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/tcypher/runtime/interval_derive.h"

namespace cedar {

struct AlignedTemporalInterval {
  uint64_t valid_from;
  uint64_t valid_to;
  std::vector<std::shared_ptr<const TemporalEvent>> facts;
};

struct AlignedRawTemporalInterval {
  uint64_t valid_from = 0;
  uint64_t valid_to = 0;
  std::vector<RawTemporalFact> facts;
};

struct RawRangeExpandRow {
  uint64_t valid_from = 0;
  uint64_t valid_to = 0;
  RawTemporalFact source;
  RawTemporalFact edge;
  RawTemporalFact target;
};

// Aligns one interval stream per demanded fact. Outputs use source event
// boundaries, never query-boundary clipping, and skip spans missing any fact.
StatusOr<std::vector<AlignedTemporalInterval>> AlignTemporalIntervals(
    const std::vector<std::vector<TemporalInterval>>& streams,
    uint64_t range_start, uint64_t range_end);
StatusOr<std::vector<AlignedRawTemporalInterval>> AlignRawTemporalIntervals(
    const std::vector<std::vector<RawTemporalInterval>>& streams,
    uint64_t range_start, uint64_t range_end);
StatusOr<std::vector<RawRangeExpandRow>> ExpandRawIntervalHop(
    const std::vector<RawTemporalInterval>& source_intervals,
    const std::vector<RawTemporalInterval>& edge_intervals,
    const std::vector<RawTemporalInterval>& target_intervals,
    uint64_t range_start, uint64_t range_end);

}  // namespace cedar

#endif  // CEDAR_TCYPHER_RUNTIME_INTERVAL_ALIGN_H_
