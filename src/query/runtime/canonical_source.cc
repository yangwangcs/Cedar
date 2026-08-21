// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/runtime/canonical_source.h"

#include <map>

#include "query/temporal/corrected_chain.h"

namespace cedar::internal {

StatusOr<std::vector<VertexRef>> CanonicalSource::ReadVerticesAt(
    Snapshot& snapshot, ValidTime valid_time) {
  std::map<uint64_t, std::vector<FactEvent>> chains;
  FactScanSpec scan;
  scan.part_id = PartId{0};
  scan.family = FactFamily::kVertexState;
  scan.property_id = PropertyId{};
  scan.valid_time = valid_time;
  const Status scanned = snapshot.EventScan(
      scan, [&chains](const FactEventBatch& batch) {
        for (const FactEvent& event : batch.events) {
          chains[event.ref.entity_id()].push_back(event);
        }
        return Status::OK();
      });
  if (!scanned.ok()) return scanned;

  std::vector<VertexRef> vertices;
  for (const auto& [entity_id, events] : chains) {
    const auto corrected =
        ResolveCorrectedBoundaries(events, snapshot.commit_seq());
    if (!corrected.ok()) return corrected.status();
    const auto& boundaries = corrected.ValueOrDie();
    for (size_t index = 0; index < boundaries.size(); ++index) {
      if (boundaries[index].valid_from.value > valid_time.value) break;
      const bool contains =
          index + 1 == boundaries.size() ||
          valid_time.value < boundaries[index + 1].valid_from.value;
      if (contains && boundaries[index].operation == FactOperation::kPut) {
        vertices.push_back(
            VertexRef{PartId{0}, VertexId{entity_id}});
      }
      if (contains) break;
    }
  }
  return vertices;
}

}  // namespace cedar::internal
