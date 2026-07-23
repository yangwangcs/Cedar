// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_BENCHMARK_RUN_MANIFEST_H_
#define CEDAR_BENCHMARK_RUN_MANIFEST_H_

#include <cstdint>
#include <string>

#include "cedar/core/status.h"
#include "cedar/observability/instrumentation_profile.h"
#include "cedar/transaction/database_format.h"

namespace cedar {

// All fields are deliberate benchmark provenance, never database state.
struct BenchmarkRunManifest {
  uint32_t protocol_version = 1;
  std::string source_commit;
  bool source_dirty = false;
  std::string binary_hash;
  std::string compiler_and_flags;
  std::string os_kernel;
  std::string cpu_model_and_count;
  uint64_t memory_limit_bytes = 0;
  std::string storage_device_and_filesystem;
  std::string resource_profile_id;
  std::string instrumentation_profile_id =
      kInstrumentationProfileTier0Tier1;
  uint32_t database_format_version = kCedarDatabaseFormatVersion;
  std::string language_version = "CEDAR_TCypher_V1";
  std::string schema_hash;
  std::string dataset_id;
  std::string dataset_hash;
  std::string dataset_profile_id = "explicit";
  uint64_t dataset_vertex_count = 0;
  uint64_t dataset_edge_count = 0;
  uint32_t dataset_property_events_per_vertex = 0;
  uint64_t dataset_valid_time_span = 0;
  // External-derived datasets (for example LDBC) carry their provenance in
  // the manifest rather than relying on a free-form report note.
  std::string source_dataset_kind = "cedar-tg";
  std::string source_dataset_license;
  std::string source_transform_policy;
  uint64_t generator_seed = 0;
  std::string workload_id;
  std::string workload_hash;
  std::string durability_mode = "durable";
  std::string cache_mode = "cold_process_and_database";
  uint32_t worker_limit = 0;
  std::string execution_nonce;
};

std::string SerializeBenchmarkRunManifest(const BenchmarkRunManifest& manifest);
std::string BenchmarkRunId(const BenchmarkRunManifest& manifest);
std::string BenchmarkRunIdWithImplicitTier0Tier1(
    const BenchmarkRunManifest& manifest);
Status WriteBenchmarkRunManifest(const std::string& path,
                                 const BenchmarkRunManifest& manifest);

}  // namespace cedar

#endif  // CEDAR_BENCHMARK_RUN_MANIFEST_H_
