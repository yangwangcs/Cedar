// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TCYPHER_QUERY_SNAPSHOT_H_
#define CEDAR_TCYPHER_QUERY_SNAPSHOT_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/schema/schema_registry.h"
#include "cedar/storage/version_set.h"
#include "cedar/tcypher/syntax/parser.h"
#include "cedar/transaction/system_hlc.h"

namespace cedar {

struct ResolvedTemporalContext {
  TemporalAxis axis;
  uint64_t snapshot_seq;
  uint64_t valid_time = 0;
};

struct QuerySnapshot {
  uint64_t visible_seq_ceiling;
  std::shared_ptr<const VersionSnapshot> pinned_version_set;
  std::shared_ptr<const SchemaSnapshot> pinned_schema_registry;
  uint64_t blob_reader_epoch;
  SystemHlc statement_start_hlc;
  std::vector<ResolvedTemporalContext> resolved_temporal_contexts;
};

StatusOr<QuerySnapshot> CreateQuerySnapshot(
    uint64_t visible_seq_ceiling,
    std::shared_ptr<const VersionSnapshot> pinned_version_set,
    std::shared_ptr<const SchemaSnapshot> pinned_schema_registry,
    uint64_t blob_reader_epoch, SystemHlc statement_start_hlc,
    std::vector<ResolvedTemporalContext> resolved_temporal_contexts);

}  // namespace cedar

#endif  // CEDAR_TCYPHER_QUERY_SNAPSHOT_H_
