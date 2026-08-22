// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_KERNEL_QUERY_DEBUG_THRESHOLDS_H_
#define CEDAR_KERNEL_QUERY_DEBUG_THRESHOLDS_H_

#include <cstdint>

namespace cedar::internal {

struct QueryDebugThresholds {
  uint64_t memtable_bytes = 64ULL << 10;
  uint64_t projection_segment_bytes = 64ULL << 10;
  uint64_t projection_page_bytes = 4ULL << 10;
  uint64_t query_delta_soft_bytes = 64ULL << 10;
  uint64_t query_delta_hard_bytes = 128ULL << 10;
  uint64_t query_memory_bytes = 32ULL << 10;
  uint64_t scratch_run_bytes = 16ULL << 10;
  uint32_t delta_lag_soft_commits = 8;
  uint32_t delta_lag_hard_commits = 32;
  uint32_t manifest_commits_per_generation = 16;
};

inline constexpr QueryDebugThresholds kQueryDebugThresholds{};

}  // namespace cedar::internal

#endif  // CEDAR_KERNEL_QUERY_DEBUG_THRESHOLDS_H_
