// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/statistics/stats_fragment.h"

#include <limits>
#include <set>

#include "cedar/index/canonical_value.h"

namespace cedar {

StatusOr<ResourceProfile> EstimateStatsFragmentResources(
    const SstFileStatistics& source_statistics,
    const ColumnSchema& schema) {
  if (!ValidateColumnSchema(schema, true).ok()) {
    return Status::InvalidArgument("stats fragment estimate",
                                   "invalid schema");
  }
  uint64_t canonical_bytes = 0;
  switch (schema.physical_type) {
    case PhysicalType::kBool:
      canonical_bytes = 1;
      break;
    case PhysicalType::kInt32:
    case PhysicalType::kFloat32:
      canonical_bytes = 4;
      break;
    case PhysicalType::kInt64:
    case PhysicalType::kFloat64:
    case PhysicalType::kTimestamp64:
      canonical_bytes = 8;
      break;
    case PhysicalType::kString:
    case PhysicalType::kBinary:
      canonical_bytes = std::max<uint64_t>(schema.blob_threshold, 40);
      break;
  }
  constexpr uint64_t kDistinctEntryOverhead = 64;
  if (canonical_bytes > std::numeric_limits<uint64_t>::max() -
                            kDistinctEntryOverhead) {
    return Status::InvalidArgument("stats fragment estimate",
                                   "distinct entry overflow");
  }
  const uint64_t entry_bytes = canonical_bytes + kDistinctEntryOverhead;
  if (source_statistics.put_count != 0 &&
      entry_bytes > std::numeric_limits<uint64_t>::max() /
                        source_statistics.put_count) {
    return Status::InvalidArgument("stats fragment estimate",
                                   "distinct set overflow");
  }
  return ResourceProfile{
      source_statistics.put_count * entry_bytes, 0, 0, 0, 1};
}

StatusOr<StatsFragment> BuildStatsFragment(uint64_t source_identity,
                                           EntityType entity_type, uint16_t column_id,
                                           const std::vector<TemporalEvent>& events,
                                           std::shared_ptr<WorkCancellation> cancellation) {
  if (source_identity == 0) {
    return Status::InvalidArgument("stats fragment", "source identity is required");
  }
  StatsFragment stats{source_identity, entity_type, column_id, 0, 0, 0, 0,
                      std::numeric_limits<uint64_t>::max(), 0,
                      std::numeric_limits<uint64_t>::max(), 0};
  std::set<std::pair<uint8_t, std::string>> distinct;
  uint64_t ordinal = 0;
  for (const TemporalEvent& event : events) {
    if (cancellation != nullptr && ordinal % 64 == 0) {
      const Status checkpoint =
          cancellation->Checkpoint("stats fragment build");
      if (!checkpoint.ok()) return checkpoint;
    }
    ++ordinal;
    const LogicalKey& key = event.logical_key();
    if (key.entity_type() != entity_type || key.kind() != LogicalKeyKind::kProperty ||
        key.column_id() != column_id) {
      continue;
    }
    ++stats.row_count;
    stats.min_valid_from = std::min(stats.min_valid_from, event.valid_from());
    stats.max_valid_from = std::max(stats.max_valid_from, event.valid_from());
    stats.min_commit_seq = std::min(stats.min_commit_seq, event.commit_seq());
    stats.max_commit_seq = std::max(stats.max_commit_seq, event.commit_seq());
    if (event.operation() == TemporalOperation::kDelete) {
      ++stats.delete_count;
      continue;
    }
    ++stats.put_count;
    IndexCanonicalValue canonical;
    if (event.is_blob_reference()) {
      canonical = EncodeIndexBlobHash(*event.blob_ref());
    } else {
      const auto value = EncodeIndexCanonicalValue(event.value());
      if (!value.ok()) return value.status();
      canonical = value.ValueOrDie();
    }
    distinct.emplace(static_cast<uint8_t>(canonical.type), canonical.bytes);
  }
  if (stats.row_count == 0) {
    stats.min_valid_from = 0;
    stats.min_commit_seq = 0;
  }
  stats.distinct_value_count = distinct.size();
  if (cancellation != nullptr) {
    const Status checkpoint =
        cancellation->Checkpoint("stats fragment build");
    if (!checkpoint.ok()) return checkpoint;
  }
  return stats;
}

}  // namespace cedar
