// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_BENCHMARK_PHASE_RUNNER_H_
#define CEDAR_BENCHMARK_PHASE_RUNNER_H_

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "cedar/core/status.h"

namespace cedar {

// The runner owns protocol ordering only. Individual phase callbacks use public
// database APIs and may not bypass persistence or snapshot validation.
enum class BenchmarkPhase : uint8_t {
  kEnvironmentCheck,
  kDatabaseCreateOrOpen,
  kDatasetLoad,
  kLoadVerification,
  kCachePrepare,
  kWarmup,
  kMeasurement,
  kDrainAndMaintenance,
  kResultVerification,
  kReopenVerification,
  kArtifactFinalize,
};

const char* BenchmarkPhaseName(BenchmarkPhase phase);

struct BenchmarkPhaseRecord {
  BenchmarkPhase phase;
  uint64_t elapsed_ns = 0;
  std::string terminal_status = "PASS";
};

class BenchmarkPhaseRunner {
 public:
  using PhaseCallback = std::function<Status(BenchmarkPhase)>;

  // Stops on the first failure. The failed phase is retained in records(), so
  // callers can emit an incomplete artifact without inventing a success state.
  Status Run(const PhaseCallback& callback);

  const std::vector<BenchmarkPhaseRecord>& records() const { return records_; }
  bool complete() const { return complete_; }

 private:
  std::vector<BenchmarkPhaseRecord> records_;
  bool complete_ = false;
};

}  // namespace cedar

#endif  // CEDAR_BENCHMARK_PHASE_RUNNER_H_
