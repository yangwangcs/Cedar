// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TCYPHER_RUNTIME_VECTOR_EXPAND_H_
#define CEDAR_TCYPHER_RUNTIME_VECTOR_EXPAND_H_

#include <cstdint>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/storage/temporal_event.h"
#include "cedar/tcypher/runtime/column_batch.h"

namespace cedar {

enum ExpandedFactColumn : uint32_t {
  kExpandedSourceId = 0,
  kExpandedTargetId = 1,
  kExpandedEdgeId = 2,
  kExpandedEdgeType = 3,
  kExpandedValidFrom = 4,
  kExpandedCommitSeq = 5,
};

struct VectorExpandSpec {
  EntityType direction;
  uint64_t valid_time;
  uint64_t snapshot_seq;
  uint32_t max_output_rows = kTcypherStandardBatchCapacity;
};

Status ExpandAsOfBatch(const ColumnBatch& sources,
                       const std::vector<TemporalEvent>& candidates,
                       const VectorExpandSpec& spec, ColumnBatch* expanded);

}  // namespace cedar

#endif  // CEDAR_TCYPHER_RUNTIME_VECTOR_EXPAND_H_
