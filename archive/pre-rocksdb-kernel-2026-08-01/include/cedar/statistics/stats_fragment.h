// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_STATISTICS_STATS_FRAGMENT_H_
#define CEDAR_STATISTICS_STATS_FRAGMENT_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/columnar/sst.h"
#include "cedar/runtime/resource_profile.h"
#include "cedar/runtime/work_cancellation.h"
#include "cedar/storage/temporal_event.h"

namespace cedar {

constexpr uint32_t kStatsFragmentFormatV1 = 1;

struct StatsFragment {
  uint64_t source_identity;
  EntityType entity_type;
  uint16_t column_id;
  uint64_t row_count;
  uint64_t put_count;
  uint64_t delete_count;
  uint64_t distinct_value_count;
  uint64_t min_valid_from;
  uint64_t max_valid_from;
  uint64_t min_commit_seq;
  uint64_t max_commit_seq;
  uint32_t format_version = kStatsFragmentFormatV1;
};

StatusOr<StatsFragment> BuildStatsFragment(uint64_t source_identity,
                                           EntityType entity_type, uint16_t column_id,
                                           const std::vector<TemporalEvent>& events,
                                           std::shared_ptr<WorkCancellation> cancellation = nullptr);
StatusOr<ResourceProfile> EstimateStatsFragmentResources(
    const SstFileStatistics& source_statistics,
    const ColumnSchema& schema);

}  // namespace cedar

#endif  // CEDAR_STATISTICS_STATS_FRAGMENT_H_
