// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/benchmark/regression_orchestrator.h"

#include <utility>

namespace cedar {

StatusOr<BenchmarkPairedGateRun> RunPairedBenchmarkGate(
    const std::string& output_path,
    const BenchmarkBaselineKey& expected_key,
    const BenchmarkBaselineKey& candidate_key,
    uint32_t pair_count, uint64_t order_seed,
    const BenchmarkRegressionRunner& runner,
    const BenchmarkRegressionPolicy& policy) {
  if (output_path.empty() || pair_count == 0 || !runner) {
    return Status::InvalidArgument(
        "benchmark regression orchestrator",
        "output path, pair count, and runner are required");
  }

  BenchmarkPairedGateRun run;
  run.records.reserve(static_cast<size_t>(pair_count) * 2);
  run.baseline.resize(pair_count);
  run.candidate.resize(pair_count);
  const std::vector<BenchmarkRunArm> order =
      BuildAlternatingPairedOrder(pair_count, order_seed);
  for (size_t sequence = 0; sequence < order.size(); ++sequence) {
    const uint32_t pair_index = static_cast<uint32_t>(sequence / 2);
    const BenchmarkRunArm arm = order[sequence];
    auto sample = runner(arm, pair_index);
    BenchmarkPairedRunRecord record;
    record.pair_index = pair_index;
    record.arm = arm;
    if (!sample.ok()) {
      record.status = sample.status();
    } else if (arm == BenchmarkRunArm::kBaseline) {
      run.baseline[pair_index] = sample.ValueOrDie();
    } else {
      run.candidate[pair_index] = sample.ValueOrDie();
    }
    run.records.push_back(std::move(record));
  }

  auto gate = WriteBenchmarkRegressionGate(
      output_path, expected_key, candidate_key, run.baseline, run.candidate,
      policy);
  if (!gate.ok()) return gate.status();
  run.gate = std::move(gate).ConsumeValueOrDie();
  return run;
}

}  // namespace cedar
