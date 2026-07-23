// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/benchmark/phase_runner.h"

#include <array>
#include <chrono>

namespace cedar {
namespace {

constexpr std::array<BenchmarkPhase, 11> kProtocolPhases = {
    BenchmarkPhase::kEnvironmentCheck,
    BenchmarkPhase::kDatabaseCreateOrOpen,
    BenchmarkPhase::kDatasetLoad,
    BenchmarkPhase::kLoadVerification,
    BenchmarkPhase::kCachePrepare,
    BenchmarkPhase::kWarmup,
    BenchmarkPhase::kMeasurement,
    BenchmarkPhase::kDrainAndMaintenance,
    BenchmarkPhase::kResultVerification,
    BenchmarkPhase::kReopenVerification,
    BenchmarkPhase::kArtifactFinalize,
};

}  // namespace

const char* BenchmarkPhaseName(BenchmarkPhase phase) {
  switch (phase) {
    case BenchmarkPhase::kEnvironmentCheck: return "environment_check";
    case BenchmarkPhase::kDatabaseCreateOrOpen: return "database_create_or_open";
    case BenchmarkPhase::kDatasetLoad: return "dataset_load";
    case BenchmarkPhase::kLoadVerification: return "load_verification";
    case BenchmarkPhase::kCachePrepare: return "cache_prepare";
    case BenchmarkPhase::kWarmup: return "warmup";
    case BenchmarkPhase::kMeasurement: return "measurement";
    case BenchmarkPhase::kDrainAndMaintenance: return "drain_and_maintenance";
    case BenchmarkPhase::kResultVerification: return "result_verification";
    case BenchmarkPhase::kReopenVerification: return "reopen_verification";
    case BenchmarkPhase::kArtifactFinalize: return "artifact_finalize";
  }
  return "unknown";
}

Status BenchmarkPhaseRunner::Run(const PhaseCallback& callback) {
  if (!callback) return Status::InvalidArgument("benchmark runner", "phase callback is required");
  records_.clear();
  complete_ = false;
  for (const BenchmarkPhase phase : kProtocolPhases) {
    const auto started = std::chrono::steady_clock::now();
    const Status status = callback(phase);
    const auto finished = std::chrono::steady_clock::now();
    BenchmarkPhaseRecord record;
    record.phase = phase;
    record.elapsed_ns = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
        finished - started).count());
    record.terminal_status = status.ok() ? "PASS" : status.ToString();
    records_.push_back(std::move(record));
    if (!status.ok()) return status;
  }
  complete_ = true;
  return Status::OK();
}

}  // namespace cedar
