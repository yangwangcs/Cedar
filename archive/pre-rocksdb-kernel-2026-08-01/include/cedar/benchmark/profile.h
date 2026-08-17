// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_BENCHMARK_PROFILE_H_
#define CEDAR_BENCHMARK_PROFILE_H_

#include <cstdint>
#include <string>
#include <string_view>

#include "cedar/benchmark/cedar_tg.h"
#include "cedar/core/status.h"

namespace cedar {

struct BenchmarkEnvironment;

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
  uint32_t minimum_logical_cpu_count = 1;
  uint64_t minimum_memory_bytes = 0;
  uint64_t minimum_free_storage_bytes = 0;
};

struct BenchmarkCachePlan {
  bool reopen_before_measurement = false;
  bool read_full_working_set = false;
  bool run_background_maintenance = false;
};

struct BenchmarkBaselineApproval {
  bool approved_production_baseline = false;
  std::string approval_id;
  std::string binary_sha256;
  std::string source_commit;
  std::string approved_by;
};

struct BenchmarkBinaryProvenance {
  std::string source_commit;
  bool source_dirty = true;
  std::string instrumentation_profile_id;
  uint32_t database_format_version = 0;
};

StatusOr<BenchmarkScaleProfile> ParseBenchmarkScaleProfile(
    const std::string& name);
BenchmarkProfile ResolveBenchmarkProfile(BenchmarkScaleProfile profile,
                                         uint64_t seed);

// Production release profiles are exact contracts. This rejects CI/paper
// substitutions, dataset or worker shrinkage, and hosts below the declared
// CPU, memory, or durable-storage floor before dataset generation begins.
Status ValidateProductionBenchmarkPreflight(
    const BenchmarkProfile& profile, const CedarTgConfig& actual_dataset,
    uint64_t actual_worker_count, const BenchmarkEnvironment& environment);
Status ValidateApprovedProductionBaseline(
    const BenchmarkBaselineApproval& approval,
    const std::string& actual_baseline_sha256,
    const std::string& candidate_sha256);

// Strict decimal parser shared by benchmark CLIs. Signs, whitespace,
// partial parses, and uint64 overflow are rejected.
StatusOr<uint64_t> ParseBenchmarkUnsigned(std::string_view text);

// Production artifacts must identify one full Git commit and be built from a
// clean source tree. Baseline approval adds an exact commit match separately.
Status ValidateProductionArtifactSourceProvenance(
    const std::string& source_commit, bool source_dirty);
Status ValidateProductionBenchmarkBinaryProvenance(
    const BenchmarkBinaryProvenance& provenance,
    const std::string& expected_source_commit = {});

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
