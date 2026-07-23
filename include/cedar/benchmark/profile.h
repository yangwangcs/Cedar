// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_BENCHMARK_PROFILE_H_
#define CEDAR_BENCHMARK_PROFILE_H_

#include <cstdint>
#include <string>

#include "cedar/benchmark/cedar_tg.h"
#include "cedar/core/status.h"

namespace cedar {

enum class BenchmarkScaleProfile : uint8_t {
  kCi = 1,
  kWorkstation = 2,
  kPaper = 3,
  kStress = 4,
};

enum class BenchmarkCacheMode : uint8_t {
  kColdProcessAndDatabase = 1,
  kColdDatabaseWarmProcess = 2,
  kWarmMetadataOnly = 3,
  kWarmFullWorkingSet = 4,
  kSteadyStateWithBackgroundMaintenance = 5,
};

struct BenchmarkProfile {
  BenchmarkScaleProfile scale = BenchmarkScaleProfile::kCi;
  std::string name;
  CedarTgConfig dataset;
  uint32_t worker_count = 1;
  uint32_t queue_capacity = 1;
  uint64_t arrival_interval_ns = 1000;
  std::string resource_profile_id;
};

struct BenchmarkCachePlan {
  bool reopen_before_measurement = false;
  bool read_full_working_set = false;
  bool run_background_maintenance = false;
};

StatusOr<BenchmarkScaleProfile> ParseBenchmarkScaleProfile(
    const std::string& name);
BenchmarkProfile ResolveBenchmarkProfile(BenchmarkScaleProfile profile,
                                         uint64_t seed);

StatusOr<BenchmarkCacheMode> ParseBenchmarkCacheMode(const std::string& name);
std::string BenchmarkCacheModeName(BenchmarkCacheMode mode);
std::string BenchmarkCachePreparationDescription(BenchmarkCacheMode mode);
BenchmarkCachePlan ResolveBenchmarkCachePlan(BenchmarkCacheMode mode);

// Cedar's public benchmark binary exercises the production durability path.
// Unsafe modes require a separate, explicitly labeled ablation binary and are
// rejected here rather than silently changing database semantics.
Status ValidateBenchmarkDurabilityMode(const std::string& mode);

}  // namespace cedar

#endif  // CEDAR_BENCHMARK_PROFILE_H_
