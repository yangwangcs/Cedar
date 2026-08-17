// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/tcypher/query_snapshot.h"

namespace cedar {

StatusOr<QuerySnapshot> CreateQuerySnapshot(
    uint64_t visible_seq_ceiling,
    std::shared_ptr<const VersionSnapshot> pinned_version_set,
    std::shared_ptr<const SchemaSnapshot> pinned_schema_registry,
    uint64_t blob_reader_epoch, SystemHlc statement_start_hlc,
    std::vector<ResolvedTemporalContext> resolved_temporal_contexts) {
  if (!pinned_version_set || !pinned_schema_registry) {
    return Status::InvalidArgument("query snapshot", "missing pinned metadata view");
  }
  for (const ResolvedTemporalContext& context : resolved_temporal_contexts) {
    if (context.snapshot_seq > visible_seq_ceiling) {
      return Status::InvalidArgument("query snapshot", "temporal cutoff exceeds visible prefix");
    }
  }
  return QuerySnapshot{visible_seq_ceiling, std::move(pinned_version_set),
                       std::move(pinned_schema_registry), blob_reader_epoch,
                       statement_start_hlc, std::move(resolved_temporal_contexts)};
}

}  // namespace cedar
