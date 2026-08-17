// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/benchmark/profile.h"

#include <algorithm>
#include <charconv>
#include <cctype>

#include "cedar/benchmark/environment_probe.h"
#include "cedar/observability/instrumentation_profile.h"
#include "cedar/transaction/database_format.h"

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
      resolved.minimum_logical_cpu_count = 1;
      resolved.minimum_memory_bytes = 512ULL * 1024ULL * 1024ULL;
      resolved.minimum_free_storage_bytes = 1ULL * 1024ULL * 1024ULL * 1024ULL;
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
      resolved.minimum_logical_cpu_count = 8;
      resolved.minimum_memory_bytes = 32ULL * 1024ULL * 1024ULL * 1024ULL;
      resolved.minimum_free_storage_bytes =
          256ULL * 1024ULL * 1024ULL * 1024ULL;
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
      resolved.minimum_logical_cpu_count = 16;
      resolved.minimum_memory_bytes = 128ULL * 1024ULL * 1024ULL * 1024ULL;
      resolved.minimum_free_storage_bytes =
          1ULL * 1024ULL * 1024ULL * 1024ULL * 1024ULL;
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
      resolved.minimum_logical_cpu_count = 32;
      resolved.minimum_memory_bytes = 256ULL * 1024ULL * 1024ULL * 1024ULL;
      resolved.minimum_free_storage_bytes =
          2ULL * 1024ULL * 1024ULL * 1024ULL * 1024ULL;
      break;
  }
  resolved.resource_profile_id = resolved.name + "-workers-" +
      std::to_string(resolved.worker_count);
  return resolved;
}

Status ValidateProductionBenchmarkPreflight(
    const BenchmarkProfile& profile, const CedarTgConfig& actual_dataset,
    uint64_t actual_worker_count, const BenchmarkEnvironment& environment) {
  if (profile.scale != BenchmarkScaleProfile::kWorkstation &&
      profile.scale != BenchmarkScaleProfile::kStress) {
    return Status::NotSupported(
        "benchmark production preflight",
        "only workstation and stress profiles are production-release inputs");
  }
  const CedarTgConfig& expected = profile.dataset;
  if (actual_dataset.seed != expected.seed ||
      actual_dataset.vertex_count != expected.vertex_count ||
      actual_dataset.edge_count != expected.edge_count ||
      actual_dataset.property_events_per_vertex !=
          expected.property_events_per_vertex ||
      actual_dataset.valid_time_span != expected.valid_time_span) {
    return Status::InvalidArgument(
        "benchmark production preflight",
        "named production profile dataset was changed or silently shrunk");
  }
  if (actual_worker_count != profile.worker_count) {
    return Status::InvalidArgument(
        "benchmark production preflight",
        "named production profile worker count was changed or silently shrunk");
  }
  if (!environment.resource_limit_provenance_complete) {
    return Status::InvalidArgument(
        "benchmark production preflight",
        "CPU or memory resource-limit provenance is incomplete");
  }
  if (!environment.storage_provenance_complete) {
    return Status::InvalidArgument(
        "benchmark production preflight",
        "storage device and filesystem provenance is incomplete");
  }
  if (environment.logical_cpu_count < profile.minimum_logical_cpu_count) {
    return Status::ResourceExhausted(
        "benchmark production preflight",
        "logical CPU count is below the named profile minimum");
  }
  if (environment.memory_limit_bytes < profile.minimum_memory_bytes) {
    return Status::ResourceExhausted(
        "benchmark production preflight",
        "memory is below the named profile minimum");
  }
  if (environment.storage_free_bytes < profile.minimum_free_storage_bytes) {
    return Status::ResourceExhausted(
        "benchmark production preflight",
        "free durable storage is below the named profile minimum");
  }
  if (environment.storage_device_and_filesystem.empty()) {
    return Status::InvalidArgument(
        "benchmark production preflight",
        "storage device and filesystem provenance is unavailable");
  }
  return Status::OK();
}

Status ValidateApprovedProductionBaseline(
    const BenchmarkBaselineApproval& approval,
    const std::string& actual_baseline_sha256,
    const std::string& candidate_sha256) {
  const auto is_hex = [](const std::string& value, size_t length) {
    if (value.size() != length) return false;
    for (const unsigned char character : value) {
      if (!std::isxdigit(character)) return false;
    }
    return true;
  };
  if (!approval.approved_production_baseline) {
    return Status::InvalidArgument(
        "benchmark production baseline",
        "approved_production_baseline must be true");
  }
  if (approval.approval_id.empty() || approval.approved_by.empty()) {
    return Status::InvalidArgument(
        "benchmark production baseline",
        "approval ID and approving authority are required");
  }
  if (!is_hex(approval.binary_sha256, 64) ||
      !is_hex(actual_baseline_sha256, 64) ||
      approval.binary_sha256 != actual_baseline_sha256) {
    return Status::InvalidArgument(
        "benchmark production baseline",
        "approved baseline SHA-256 does not match the baseline binary");
  }
  if (!is_hex(candidate_sha256, 64) ||
      actual_baseline_sha256 == candidate_sha256) {
    return Status::InvalidArgument(
        "benchmark production baseline",
        "baseline and candidate must have distinct SHA-256 values");
  }
  if (!is_hex(approval.source_commit, 40)) {
    return Status::InvalidArgument(
        "benchmark production baseline",
        "approved baseline source commit must be a full 40-digit hash");
  }
  return Status::OK();
}

StatusOr<uint64_t> ParseBenchmarkUnsigned(std::string_view text) {
  if (text.empty() || text.front() < '0' || text.front() > '9') {
    return Status::InvalidArgument("benchmark argument",
                                   "expected an unsigned decimal integer");
  }
  uint64_t value = 0;
  const auto parsed = std::from_chars(text.data(), text.data() + text.size(),
                                      value, 10);
  if (parsed.ec != std::errc() || parsed.ptr != text.data() + text.size()) {
    return Status::InvalidArgument("benchmark argument",
                                   "expected an unsigned decimal integer");
  }
  return value;
}

Status ValidateProductionArtifactSourceProvenance(
    const std::string& source_commit, bool source_dirty) {
  if (source_dirty) {
    return Status::InvalidArgument("benchmark production source",
                                   "source tree must be clean");
  }
  if (source_commit.size() != 40 ||
      !std::all_of(source_commit.begin(), source_commit.end(),
                   [](unsigned char character) {
                     return std::isxdigit(character) != 0;
                   })) {
    return Status::InvalidArgument(
        "benchmark production source",
        "source commit must be a full 40-digit hash");
  }
  return Status::OK();
}

Status ValidateProductionBenchmarkBinaryProvenance(
    const BenchmarkBinaryProvenance& provenance,
    const std::string& expected_source_commit) {
  Status status = ValidateProductionArtifactSourceProvenance(
      provenance.source_commit, provenance.source_dirty);
  if (!status.ok()) return status;
  if (!expected_source_commit.empty() &&
      provenance.source_commit != expected_source_commit) {
    return Status::InvalidArgument(
        "benchmark production binary",
        "source commit does not match the approved commit");
  }
  if (provenance.instrumentation_profile_id !=
      kInstrumentationProfileTier0Tier1) {
    return Status::InvalidArgument(
        "benchmark production binary",
        "instrumentation profile must be tier0-tier1");
  }
  if (provenance.database_format_version != kCedarDatabaseFormatVersion) {
    return Status::InvalidArgument(
        "benchmark production binary",
        "database format must be the current clean-break format");
  }
  return Status::OK();
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
