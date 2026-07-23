// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TCYPHER_RUNTIME_TEMPORAL_COALESCE_H_
#define CEDAR_TCYPHER_RUNTIME_TEMPORAL_COALESCE_H_

#include <vector>

#include "cedar/core/status.h"
#include "cedar/tcypher/runtime/interval_align.h"

namespace cedar {

StatusOr<std::vector<AlignedTemporalInterval>> CoalesceTemporalIntervals(
    const std::vector<AlignedTemporalInterval>& intervals,
    bool provenance_demanded = true);

}  // namespace cedar

#endif  // CEDAR_TCYPHER_RUNTIME_TEMPORAL_COALESCE_H_
