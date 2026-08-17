// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_BENCHMARK_ARTIFACT_WRITER_H_
#define CEDAR_BENCHMARK_ARTIFACT_WRITER_H_

#include <cstdint>
#include <string>
#include <vector>

#include "cedar/benchmark/environment_probe.h"
#include "cedar/benchmark/phase_runner.h"
#include "cedar/benchmark/run_manifest.h"
#include "cedar/core/status.h"
#include "cedar/transaction/transaction_measurements.h"

namespace cedar {

struct BenchmarkOperationSample {
  uint64_t requested_arrival_ns = 0;
  uint64_t admitted_ns = 0;
  uint64_t started_ns = 0;
  uint64_t completed_ns = 0;
  std::string terminal_status = "PASS";
};

struct BenchmarkVerification {
  bool load_passed = false;
  bool result_passed = false;
  bool reopen_passed = false;
  std::string result_checksum;
  std::string detail;
};

struct BenchmarkRatio {
  uint64_t numerator = 0;
  uint64_t denominator = 0;
};

struct BenchmarkLag {
  bool available = false;
  uint64_t committed_seq = 0;
  uint64_t visible_seq = 0;
};

struct BenchmarkDurableWriteBytes {
  uint64_t wal = 0;
  uint64_t decision_log = 0;
  uint64_t sst_flush = 0;
  uint64_t compaction = 0;
  uint64_t blob = 0;
  uint64_t manifest = 0;
};

struct BenchmarkDerivedMetrics {
  BenchmarkRatio write_amplification;
  BenchmarkRatio read_amplification;
  BenchmarkRatio space_amplification;
  BenchmarkRatio index_survival;
  BenchmarkRatio interval_survival;
  BenchmarkRatio blob_materialization;
  BenchmarkRatio cache_admission;
  BenchmarkRatio maintenance_share;
  BenchmarkLag visible_prefix_lag;
};

struct BenchmarkArtifactSummary {
  uint32_t schema_version = 3;
  std::string measurement_mode = "closed_loop";
  std::string cache_preparation;
  std::string maintenance_state = "idle";
  uint64_t warmup_sample_count = 0;
  uint64_t measurement_elapsed_ns = 0;
  uint64_t measured_work_units = 0;
  double measurement_throughput = 0.0;
  uint64_t latency_p50_ns = 0;
  uint64_t latency_p95_ns = 0;
  uint64_t latency_p99_ns = 0;
  uint64_t latency_p999_ns = 0;
  std::vector<BenchmarkOperationSample> measured_samples;
  uint64_t logical_work_units = 0;
  uint64_t physical_read_bytes = 0;
  uint64_t physical_write_bytes = 0;
  bool physical_read_bytes_available = false;
  bool physical_write_bytes_available = false;
  BenchmarkDurableWriteBytes durable_write_bytes;
  std::string metrics_json;
  std::string histograms_json;
  std::string traces_json;
  std::string explain_json;
  bool metrics_artifact_present = false;
  bool histograms_artifact_present = false;
  bool traces_artifact_present = false;
  bool explain_artifact_present = false;
  BenchmarkDerivedMetrics derived_metrics;
  TransactionMeasurementWindow transaction_measurements;
  std::vector<BenchmarkPhaseRecord> phases;
};

struct BenchmarkArtifactPaths {
  std::string run_directory;
  std::string manifest_path;
  std::string summary_path;
  std::string verification_path;
  std::string environment_path;
  std::string metrics_path;
  std::string histograms_path;
  std::string traces_path;
  std::string explain_path;
  std::string report_path;
};

// Writes only the supplied artifact directory. It does not create, open, or
// mutate a database. Incomplete protocol records remain explicit in summary.
StatusOr<BenchmarkArtifactPaths> WriteBenchmarkArtifacts(
    const std::string& results_root, const BenchmarkRunManifest& manifest,
    const BenchmarkEnvironment& environment, const BenchmarkArtifactSummary& summary,
    const BenchmarkVerification& verification);

std::string SerializeBenchmarkArtifactSummary(const BenchmarkArtifactSummary& summary);
std::string SerializeBenchmarkVerification(const BenchmarkVerification& verification,
                                           bool protocol_complete);

}  // namespace cedar

#endif  // CEDAR_BENCHMARK_ARTIFACT_WRITER_H_
