// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_STATISTICS_STATS_SNAPSHOT_H_
#define CEDAR_STATISTICS_STATS_SNAPSHOT_H_

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/runtime/resource_profile.h"
#include "cedar/statistics/stats_fragment.h"
#include "cedar/storage/version_set.h"

namespace cedar {

enum class StatsSnapshotFaultPoint : uint8_t {
  kBeforeCheckpointRename,
  kAfterCheckpointRename,
};

// A statistics result is advisory only. Incomplete or corrupt state must cause
// planning to use conservative estimates rather than inventing missing data.
struct StatsSnapshot {
  uint64_t version_set_generation = 0;
  uint64_t statistics_snapshot_id = 0;
  std::vector<uint64_t> source_identities;
  StatsFragment aggregate{};
  bool complete = false;
  bool conservative = true;
};

class PinnedStatsSnapshot {
 public:
  uint64_t statistics_snapshot_id() const { return statistics_snapshot_id_; }
  StatusOr<StatsSnapshot> SnapshotFor(const VersionSnapshot& version_set,
                                      EntityType entity_type,
                                      uint16_t column_id) const;

 private:
  friend class StatsSnapshotStore;

  uint64_t statistics_snapshot_id_ = 0;
  std::map<uint64_t, StatsFragment> fragments_;
  bool checkpoint_corrupt_ = false;
};

class StatsSnapshotStore {
 public:
  explicit StatsSnapshotStore(std::string checkpoint_path)
      : checkpoint_path_(std::move(checkpoint_path)) {}

  // A corrupt optional checkpoint is discarded. Callers can still plan using
  // the conservative snapshot returned by SnapshotFor().
  Status Open();
  Status Upsert(const StatsFragment& fragment);
  Status UpsertExpected(const StatsFragment& fragment,
                        uint64_t expected_generation);
  StatusOr<ResourceProfile> EstimateUpsertResources(
      const StatsFragment& fragment,
      uint64_t expected_generation) const;
  uint64_t generation() const;
  std::shared_ptr<const PinnedStatsSnapshot> Pin() const;
  StatusOr<StatsSnapshot> SnapshotFor(const VersionSnapshot& version_set,
                                      EntityType entity_type, uint16_t column_id) const;
  void SetFaultInjectorForTesting(
      std::function<Status(StatsSnapshotFaultPoint)> injector) {
    fault_injector_ = std::move(injector);
  }

 private:
  Status UpsertLocked(const StatsFragment& fragment,
                      uint64_t expected_generation);
  Status PersistProjected(
      uint64_t generation,
      const std::map<uint64_t, StatsFragment>& fragments) const;

  std::string checkpoint_path_;
  mutable std::mutex mutex_;
  std::map<uint64_t, StatsFragment> fragments_;
  uint64_t generation_ = 0;
  bool checkpoint_corrupt_ = false;
  std::function<Status(StatsSnapshotFaultPoint)> fault_injector_;
};

}  // namespace cedar

#endif  // CEDAR_STATISTICS_STATS_SNAPSHOT_H_
