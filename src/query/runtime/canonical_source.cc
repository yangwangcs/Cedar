// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/runtime/canonical_source.h"

#include <map>

#include "query/temporal/corrected_chain.h"

namespace cedar::internal {

StatusOr<std::vector<VertexRef>> CanonicalSource::ReadVerticesAt(
    Snapshot& snapshot, ValidTime valid_time) {
  struct VertexKey {
    PartId part_id;
    uint64_t entity_id = 0;

    bool operator<(const VertexKey& other) const {
      if (part_id.value != other.part_id.value) {
        return part_id.value < other.part_id.value;
      }
      return entity_id < other.entity_id;
    }
  };
  std::map<VertexKey, std::vector<FactEvent>> chains;
  const Status scanned = snapshot.ScanFamily(
      FactFamily::kVertexState, [&chains](const FactEvent& event) {
        chains[{event.ref.part_id(), event.ref.entity_id()}].push_back(event);
        return Status::OK();
      });
  if (!scanned.ok()) return scanned;

  std::vector<VertexRef> vertices;
  for (const auto& [key, events] : chains) {
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
            VertexRef{key.part_id, VertexId{key.entity_id}});
      }
      if (contains) break;
    }
  }
  return vertices;
}

}  // namespace cedar::internal
