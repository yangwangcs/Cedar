// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TCYPHER_RUNTIME_INTERVAL_DERIVE_H_
#define CEDAR_TCYPHER_RUNTIME_INTERVAL_DERIVE_H_

#include <cstdint>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/storage/temporal_event.h"
#include "cedar/tcypher/storage/temporal_scan.h"

namespace cedar {

constexpr uint64_t kTemporalInfinity = UINT64_MAX;

struct TemporalInterval {
  TemporalEvent event;
  uint64_t valid_from;
  uint64_t valid_to;
};

struct RawTemporalInterval {
  RawTemporalFact fact;
  uint64_t valid_from = 0;
  uint64_t valid_to = 0;
};

// Produces implicit, true event intervals intersecting [range_start, range_end).
// Bounds are not clipped to the request because later interval alignment needs
// the actual successor boundary.
StatusOr<std::vector<TemporalInterval>> DeriveVisibleIntervals(
    const std::vector<TemporalEvent>& events, const LogicalKey& key,
    uint64_t snapshot_seq, uint64_t range_start, uint64_t range_end);
StatusOr<std::vector<RawTemporalInterval>> DeriveRawTemporalIntervals(
    const std::vector<RawTemporalFact>& facts, uint64_t range_start,
    uint64_t range_end);

}  // namespace cedar

#endif  // CEDAR_TCYPHER_RUNTIME_INTERVAL_DERIVE_H_
