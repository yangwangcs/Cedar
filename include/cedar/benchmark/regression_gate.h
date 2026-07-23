// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_BENCHMARK_REGRESSION_GATE_H_
#define CEDAR_BENCHMARK_REGRESSION_GATE_H_

#include <cstdint>
#include <string>
#include <vector>

#include "cedar/benchmark/regression_compare.h"
#include "cedar/core/status.h"

namespace cedar {

const char* BenchmarkRegressionStatusName(BenchmarkRegressionStatus status);

struct BenchmarkRegressionGateArtifact {
  BenchmarkRegressionResult result;
  std::string baseline_key_id;
  std::string candidate_key_id;
  uint64_t pair_count = 0;
  bool release_gate_passed = false;
  std::string output_path;
};

std::string SerializeBenchmarkRegressionGate(
    const BenchmarkRegressionGateArtifact& artifact,
    const BenchmarkRegressionPolicy& policy);

StatusOr<BenchmarkRegressionGateArtifact> WriteBenchmarkRegressionGate(
    const std::string& output_path,
    const BenchmarkBaselineKey& expected_key,
    const BenchmarkBaselineKey& candidate_key,
    const std::vector<BenchmarkRegressionSample>& baseline,
    const std::vector<BenchmarkRegressionSample>& candidate,
    const BenchmarkRegressionPolicy& policy = BenchmarkRegressionPolicy());

StatusOr<BenchmarkRegressionGateArtifact> WriteInstrumentationOverheadGate(
    const std::string& output_path,
    const BenchmarkBaselineKey& minimal_key,
    const BenchmarkBaselineKey& instrumented_key,
    const std::string& minimal_profile_id,
    const std::string& instrumented_profile_id,
    const std::vector<BenchmarkRegressionSample>& minimal,
    const std::vector<BenchmarkRegressionSample>& instrumented,
    const InstrumentationOverheadPolicy& policy =
        InstrumentationOverheadPolicy());

}  // namespace cedar

#endif  // CEDAR_BENCHMARK_REGRESSION_GATE_H_
