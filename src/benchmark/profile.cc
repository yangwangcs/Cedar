// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/benchmark/profile.h"

namespace cedar {

StatusOr<BenchmarkScaleProfile> ParseBenchmarkScaleProfile(
    const std::string& name) {
  if (name == "ci") return BenchmarkScaleProfile::kCi;
  if (name == "workstation") return BenchmarkScaleProfile::kWorkstation;
  if (name == "paper") return BenchmarkScaleProfile::kPaper;
  if (name == "stress") return BenchmarkScaleProfile::kStress;
  return Status::InvalidArgument("benchmark profile",
                                 "unknown scale profile");
}

BenchmarkProfile ResolveBenchmarkProfile(BenchmarkScaleProfile profile,
                                         uint64_t seed) {
  BenchmarkProfile resolved;
  resolved.scale = profile;
  resolved.dataset.seed = seed;
  switch (profile) {
    case BenchmarkScaleProfile::kCi:
      resolved.name = "ci";
      resolved.dataset.vertex_count = 100;
      resolved.dataset.edge_count = 200;
      resolved.dataset.property_events_per_vertex = 2;
      resolved.dataset.valid_time_span = 1000;
      resolved.worker_count = 1;
      resolved.queue_capacity = 512;
      resolved.arrival_interval_ns = 1000;
      break;
    case BenchmarkScaleProfile::kWorkstation:
      resolved.name = "workstation";
      resolved.dataset.vertex_count = 1000000;
      resolved.dataset.edge_count = 4000000;
      resolved.dataset.property_events_per_vertex = 4;
      resolved.dataset.valid_time_span = 1000000000ULL;
      resolved.worker_count = 8;
      resolved.queue_capacity = 65536;
      resolved.arrival_interval_ns = 1000;
      break;
    case BenchmarkScaleProfile::kPaper:
      resolved.name = "paper";
      resolved.dataset.vertex_count = 10000000;
      resolved.dataset.edge_count = 40000000;
      resolved.dataset.property_events_per_vertex = 8;
      resolved.dataset.valid_time_span = 1000000000000ULL;
      resolved.worker_count = 16;
      resolved.queue_capacity = 262144;
      resolved.arrival_interval_ns = 1000;
      break;
    case BenchmarkScaleProfile::kStress:
      resolved.name = "stress";
      resolved.dataset.vertex_count = 25000000;
      resolved.dataset.edge_count = 100000000;
      resolved.dataset.property_events_per_vertex = 16;
      resolved.dataset.valid_time_span = 10000000000000ULL;
      resolved.worker_count = 32;
      resolved.queue_capacity = 1048576;
      resolved.arrival_interval_ns = 0;
      break;
  }
  resolved.resource_profile_id = resolved.name + "-workers-" +
      std::to_string(resolved.worker_count);
  return resolved;
}

StatusOr<BenchmarkCacheMode> ParseBenchmarkCacheMode(
    const std::string& name) {
  if (name == "cold_process_and_database") {
    return BenchmarkCacheMode::kColdProcessAndDatabase;
  }
  if (name == "cold_database_warm_process") {
    return BenchmarkCacheMode::kColdDatabaseWarmProcess;
  }
  if (name == "warm_metadata_only") {
    return BenchmarkCacheMode::kWarmMetadataOnly;
  }
  if (name == "warm_full_working_set") {
    return BenchmarkCacheMode::kWarmFullWorkingSet;
  }
  if (name == "steady_state_with_background_maintenance") {
    return BenchmarkCacheMode::kSteadyStateWithBackgroundMaintenance;
  }
  return Status::InvalidArgument("benchmark profile", "unknown cache mode");
}

std::string BenchmarkCacheModeName(BenchmarkCacheMode mode) {
  switch (mode) {
    case BenchmarkCacheMode::kColdProcessAndDatabase:
      return "cold_process_and_database";
    case BenchmarkCacheMode::kColdDatabaseWarmProcess:
      return "cold_database_warm_process";
    case BenchmarkCacheMode::kWarmMetadataOnly:
      return "warm_metadata_only";
    case BenchmarkCacheMode::kWarmFullWorkingSet:
      return "warm_full_working_set";
    case BenchmarkCacheMode::kSteadyStateWithBackgroundMaintenance:
      return "steady_state_with_background_maintenance";
  }
  return "unknown";
}

std::string BenchmarkCachePreparationDescription(BenchmarkCacheMode mode) {
  switch (mode) {
    case BenchmarkCacheMode::kColdProcessAndDatabase:
      return "fresh benchmark process and fresh database directory";
    case BenchmarkCacheMode::kColdDatabaseWarmProcess:
      return "warm benchmark process with a fresh database directory";
    case BenchmarkCacheMode::kWarmMetadataOnly:
      return "reopened database with metadata loaded and data pages untouched";
    case BenchmarkCacheMode::kWarmFullWorkingSet:
      return "reopened database after deterministic full working-set read";
    case BenchmarkCacheMode::kSteadyStateWithBackgroundMaintenance:
      return "warmed working set with admitted maintenance active during measurement";
  }
  return "unknown cache preparation";
}

BenchmarkCachePlan ResolveBenchmarkCachePlan(BenchmarkCacheMode mode) {
  switch (mode) {
    case BenchmarkCacheMode::kColdProcessAndDatabase:
    case BenchmarkCacheMode::kColdDatabaseWarmProcess:
      return BenchmarkCachePlan{};
    case BenchmarkCacheMode::kWarmMetadataOnly:
      return BenchmarkCachePlan{true, false, false};
    case BenchmarkCacheMode::kWarmFullWorkingSet:
      return BenchmarkCachePlan{true, true, false};
    case BenchmarkCacheMode::kSteadyStateWithBackgroundMaintenance:
      return BenchmarkCachePlan{true, true, true};
  }
  return BenchmarkCachePlan{};
}

Status ValidateBenchmarkDurabilityMode(const std::string& mode) {
  if (mode == "durable") return Status::OK();
  if (mode == "NON_DURABLE") {
    return Status::NotSupported(
        "benchmark profile",
        "NON_DURABLE requires a separate explicitly labeled ablation binary");
  }
  return Status::InvalidArgument("benchmark profile",
                                 "unknown durability mode");
}

}  // namespace cedar
