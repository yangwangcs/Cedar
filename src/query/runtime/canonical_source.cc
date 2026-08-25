// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/runtime/canonical_source.h"

#include "query/temporal/corrected_chain.h"
#include "query/runtime/fact_chain_cursor.h"

namespace cedar::internal {

StatusOr<std::vector<VertexRef>> CanonicalSource::ReadVerticesAt(
    Snapshot& snapshot, ValidTime valid_time, const PartScope& part_scope) {
  FactChainCursor cursor(FactBatchOrder::kIdentityValidDescCommitDesc,
                         snapshot.commit_seq());
  auto consume = [&cursor, &part_scope](const FactEvent& event) {
        if (!part_scope.Contains(event.ref.part_id())) return Status::OK();
        return cursor.Consume(event);
      };
  Status scanned;
  if (part_scope.kind == PartScopeKind::kAll) {
    scanned = snapshot.ScanFamily(FactFamily::kVertexState, consume);
  } else {
    for (PartId part : part_scope.parts) {
      FactScanSpec spec;
      spec.part_id = part;
      spec.family = FactFamily::kVertexState;
      spec.property_id = PropertyId{};
      scanned = snapshot.EventScan(spec, [&consume](const FactEventBatch& batch) {
        for (const FactEvent& event : batch.events) {
          Status status = consume(event);
          if (!status.ok()) return status;
        }
        return Status::OK();
      });
      if (!scanned.ok()) break;
    }
  }
  if (!scanned.ok()) return scanned;

  const Status finished = cursor.Finish(snapshot.commit_seq());
  if (!finished.ok()) return finished;
  std::vector<VertexRef> vertices;
  for (const auto& chain : cursor.chains()) {
    const auto& boundaries = chain.boundaries;
    for (size_t index = 0; index < boundaries.size(); ++index) {
      if (boundaries[index].valid_from.value > valid_time.value) break;
      const bool contains =
          index + 1 == boundaries.size() ||
          valid_time.value < boundaries[index + 1].valid_from.value;
      if (contains && boundaries[index].operation == FactOperation::kPut) {
        vertices.push_back(
            VertexRef{chain.ref.part_id(), VertexId{chain.ref.entity_id()}});
      }
      if (contains) break;
    }
  }
  return vertices;
}

}  // namespace cedar::internal
