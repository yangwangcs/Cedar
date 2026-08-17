// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_COLUMNAR_TEMPORAL_READ_MERGER_H_
#define CEDAR_COLUMNAR_TEMPORAL_READ_MERGER_H_

#include "cedar/storage/version_set.h"
#include "cedar/storage/temporal_event.h"

namespace cedar {

class CacheManager;
class IoGovernor;
struct SstReadStats;

StatusOr<std::optional<TemporalEvent>> MergeTemporalReadEvent(
    const std::string& db_path, const VersionSnapshot& version,
    const std::vector<TemporalEvent>& memtable_events, const LogicalKey& key,
    uint64_t valid_time, uint64_t snapshot_seq,
    CacheManager* cache_manager = nullptr, SstReadStats* stats = nullptr,
    IoGovernor* io_governor = nullptr);

StatusOr<std::optional<Value>> MergeTemporalRead(
    const std::string& db_path, const VersionSnapshot& version,
    const std::vector<TemporalEvent>& memtable_events, const LogicalKey& key,
    uint64_t valid_time, uint64_t snapshot_seq, CacheManager* cache_manager = nullptr);
}  // namespace cedar

#endif  // CEDAR_COLUMNAR_TEMPORAL_READ_MERGER_H_
