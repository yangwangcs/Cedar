// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_BENCHMARK_REGRESSION_COMPARE_H_
#define CEDAR_BENCHMARK_REGRESSION_COMPARE_H_

#include <cstdint>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/transaction/database_format.h"

namespace cedar {

struct BenchmarkBaselineKey {
  uint32_t protocol_version = 1;
  std::string hardware_profile;
  std::string dataset_hash;
  std::string workload_hash;
  std::string durability_mode;
  std::string cache_mode;
  std::string resource_profile_id;
  uint32_t database_format_version = kCedarDatabaseFormatVersion;
};

std::string BenchmarkBaselineKeyId(const BenchmarkBaselineKey& key);

struct BenchmarkRegressionSample {
  bool verification_passed = false;
  double throughput = 0.0;
  double p99_latency_ns = 0.0;
  double memory_bytes = 0.0;
  double amplification = 0.0;
};

enum class BenchmarkRunArm : uint8_t { kBaseline, kCandidate };
std::vector<BenchmarkRunArm> BuildAlternatingPairedOrder(uint32_t pair_count, uint64_t seed);

enum class BenchmarkRegressionStatus : uint8_t {
  kPass,
  kFail,
  kNoisy,
  kIncompatible,
  kInvalid,
};

struct BenchmarkRegressionPolicy {
  uint32_t minimum_pair_count = 5;
  double throughput_regression_fraction = 0.05;
  double p99_regression_fraction = 0.10;
  double resource_regression_fraction = 0.10;
  double maximum_coefficient_of_variation = 0.15;
  uint32_t bootstrap_resamples = 2000;
  uint64_t bootstrap_seed = 1;
  bool gate_memory = false;
  bool gate_amplification = false;
};

struct BenchmarkRegressionResult {
  BenchmarkRegressionStatus status = BenchmarkRegressionStatus::kInvalid;
  double throughput_relative_median = 0.0;
  double throughput_ci_low = 0.0;
  double throughput_ci_high = 0.0;
  double p99_relative_median = 0.0;
  double p99_ci_low = 0.0;
  double p99_ci_high = 0.0;
  double baseline_cv = 0.0;
  double candidate_cv = 0.0;
  std::string detail;
};

struct InstrumentationOverheadPolicy {
  uint32_t minimum_pair_count = 5;
  double maximum_throughput_overhead_fraction = 0.02;
  double maximum_p99_overhead_fraction = 0.05;
  double maximum_coefficient_of_variation = 0.15;
  uint32_t bootstrap_resamples = 2000;
  uint64_t bootstrap_seed = 1;
};

BenchmarkRegressionResult ComparePairedBenchmarkRuns(
    const BenchmarkBaselineKey& expected_key, const BenchmarkBaselineKey& candidate_key,
    const std::vector<BenchmarkRegressionSample>& baseline,
    const std::vector<BenchmarkRegressionSample>& candidate,
    const BenchmarkRegressionPolicy& policy = BenchmarkRegressionPolicy());

BenchmarkRegressionResult CompareInstrumentationOverheadRuns(
    const BenchmarkBaselineKey& minimal_key,
    const BenchmarkBaselineKey& instrumented_key,
    const std::string& minimal_profile_id,
    const std::string& instrumented_profile_id,
    const std::vector<BenchmarkRegressionSample>& minimal,
    const std::vector<BenchmarkRegressionSample>& instrumented,
    const InstrumentationOverheadPolicy& policy =
        InstrumentationOverheadPolicy());

}  // namespace cedar

#endif  // CEDAR_BENCHMARK_REGRESSION_COMPARE_H_
