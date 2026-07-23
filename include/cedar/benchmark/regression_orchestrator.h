// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_BENCHMARK_REGRESSION_ORCHESTRATOR_H_
#define CEDAR_BENCHMARK_REGRESSION_ORCHESTRATOR_H_

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "cedar/benchmark/regression_gate.h"
#include "cedar/core/status.h"

namespace cedar {

using BenchmarkRegressionRunner = std::function<
    StatusOr<BenchmarkRegressionSample>(BenchmarkRunArm, uint32_t)>;

struct BenchmarkPairedRunRecord {
  uint32_t pair_index = 0;
  BenchmarkRunArm arm = BenchmarkRunArm::kBaseline;
  Status status = Status::OK();
};

struct BenchmarkPairedGateRun {
  std::vector<BenchmarkPairedRunRecord> records;
  std::vector<BenchmarkRegressionSample> baseline;
  std::vector<BenchmarkRegressionSample> candidate;
  BenchmarkRegressionGateArtifact gate;
};

StatusOr<BenchmarkPairedGateRun> RunPairedBenchmarkGate(
    const std::string& output_path,
    const BenchmarkBaselineKey& expected_key,
    const BenchmarkBaselineKey& candidate_key,
    uint32_t pair_count, uint64_t order_seed,
    const BenchmarkRegressionRunner& runner,
    const BenchmarkRegressionPolicy& policy = BenchmarkRegressionPolicy());

}  // namespace cedar

#endif  // CEDAR_BENCHMARK_REGRESSION_ORCHESTRATOR_H_
