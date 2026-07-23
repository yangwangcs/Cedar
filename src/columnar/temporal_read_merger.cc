// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/columnar/temporal_read_merger.h"

#include "cedar/columnar/sst.h"

namespace cedar {
namespace {

bool SameLogicalEntityType(EntityType left, EntityType right) {
  if (left == right) return true;
  return left != EntityType::Vertex && right != EntityType::Vertex;
}

}  // namespace

StatusOr<std::optional<TemporalEvent>> MergeTemporalReadEvent(
    const std::string& db_path, const VersionSnapshot& version,
    const std::vector<TemporalEvent>& memtable_events, const LogicalKey& key,
    uint64_t valid_time, uint64_t snapshot_seq, CacheManager* cache_manager,
    SstReadStats* stats, IoGovernor* io_governor) {
  std::vector<TemporalEvent> candidates;
  for (const TemporalEvent& event : memtable_events) {
    if (event.logical_key() == key) candidates.push_back(event);
  }
  for (const SstFileMeta& file : version.files) {
    if (!SameLogicalEntityType(file.partition.entity_type, key.entity_type()) ||
        file.partition.key_kind != key.kind() ||
        file.partition.column_id != key.schema_column_id()) continue;
    const auto events = ReadSstCandidatesForKey(
        db_path + "/" + file.relative_path, key, cache_manager, stats,
        io_governor);
    if (!events.ok()) return events.status();
    for (const TemporalEvent& event : events.ValueOrDie()) {
      if (event.logical_key() == key) candidates.push_back(event);
    }
  }
  return ResolveVisibleEvent(candidates, key, valid_time, snapshot_seq);
}

StatusOr<std::optional<Value>> MergeTemporalRead(
    const std::string& db_path, const VersionSnapshot& version,
    const std::vector<TemporalEvent>& memtable_events, const LogicalKey& key,
    uint64_t valid_time, uint64_t snapshot_seq, CacheManager* cache_manager) {
  const auto event = MergeTemporalReadEvent(db_path, version, memtable_events, key,
                                            valid_time, snapshot_seq, cache_manager);
  if (!event.ok()) return event.status();
  if (!event.ValueOrDie().has_value() || event.ValueOrDie()->is_delete()) {
    return std::optional<Value>{};
  }
  if (event.ValueOrDie()->is_blob_reference()) {
    return Status::BlobCorruption("temporal read", "BlobRef requires BlobStore resolution");
  }
  return std::optional<Value>{event.ValueOrDie()->value()};
}
}  // namespace cedar
