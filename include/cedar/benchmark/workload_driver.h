// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_BENCHMARK_WORKLOAD_DRIVER_H_
#define CEDAR_BENCHMARK_WORKLOAD_DRIVER_H_

#include <cstdint>
#include <string>
#include <vector>

#include "cedar/benchmark/artifact_writer.h"
#include "cedar/benchmark/cedar_tg.h"
#include "cedar/core/status.h"

namespace cedar {

class CedarDatabase;

enum class BenchmarkWorkloadFamily : uint8_t {
  kPointRead,
  kBitemporalPointRead,
  kAnalyticalVertexCount,
  kValidTimeRange,
  kGraphOneHop,
  kBlobProjection,
  kDurableIngestion,
  kIndexEquality,
  kMaintenanceCycle,
  kHtapBalanced,
  kRecovery,
};

const char* BenchmarkWorkloadFamilyName(BenchmarkWorkloadFamily family);
StatusOr<BenchmarkWorkloadFamily> ParseBenchmarkWorkloadFamily(
    const std::string& name);
std::string BenchmarkDurableIngestionValue(
    const CedarTgDataset& dataset, uint64_t vertex_id);
std::string BenchmarkHtapIngestionValue(
    const CedarTgDataset& dataset, uint64_t vertex_id);
std::string BenchmarkRecoveryValue(
    const CedarTgDataset& dataset, uint64_t vertex_id);

struct BenchmarkWorkloadConfig {
  BenchmarkWorkloadFamily family = BenchmarkWorkloadFamily::kPointRead;
  uint64_t arrival_interval_ns = 1000;
  uint32_t worker_count = 1;
  uint32_t queue_capacity = 4096;
  uint32_t vertex_property_schema_epoch = 0;
};

struct BenchmarkWorkloadResult {
  std::string measurement_mode;
  std::vector<BenchmarkOperationSample> samples;
  uint64_t elapsed_ns = 0;
  uint64_t logical_work_units = 0;
  uint64_t logical_result_bytes = 0;
  uint64_t candidate_intervals = 0;
  uint64_t output_intervals = 0;
  uint64_t blob_refs_seen = 0;
  uint64_t blob_payload_reads = 0;
  uint64_t physical_read_bytes = 0;
  uint64_t physical_write_bytes = 0;
  bool physical_read_bytes_available = false;
  bool physical_write_bytes_available = false;
  BenchmarkDurableWriteBytes durable_write_bytes;
  BenchmarkDerivedMetrics derived_metrics;
  TransactionMeasurementWindow transaction_measurements;
  std::string result_checksum;
  bool verified = false;
  Status terminal_status = Status::OK();
};

StatusOr<BenchmarkWorkloadResult> RunBenchmarkWorkload(
    CedarDatabase* database, const CedarTgDataset& dataset,
    const BenchmarkWorkloadConfig& config);

Status PrepareBenchmarkWorkload(
    CedarDatabase* database, const CedarTgDataset& dataset,
    const BenchmarkWorkloadConfig& config);

// Deterministically touches every logical key in the generated dataset using
// the public read API. This is used only by explicitly warm benchmark modes.
Status WarmBenchmarkWorkingSet(CedarDatabase* database,
                               const CedarTgDataset& dataset);

}  // namespace cedar

#endif  // CEDAR_BENCHMARK_WORKLOAD_DRIVER_H_
