// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_BENCHMARK_FAULT_CAMPAIGN_H_
#define CEDAR_BENCHMARK_FAULT_CAMPAIGN_H_

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "cedar/benchmark/workload_driver.h"
#include "cedar/core/status.h"

namespace cedar {

class CedarDatabase;

enum class BenchmarkFaultScenario : uint8_t {
  kCommitAfterPrepareDurable = 1,
  kCommitAfterDecisionDurable = 2,
  kManifestAfterRename = 3,
  kBlobIndexPartialWrite = 4,
  kSstAfterRename = 5,
  kSidecarAfterRename = 6,
  kBlobGcAfterManifestRename = 7,
  kAcceptedWorkShutdown = 8,
};

const char* BenchmarkFaultScenarioName(BenchmarkFaultScenario scenario);
std::string BenchmarkFaultVerificationDetail(
    BenchmarkFaultScenario scenario);
StatusOr<BenchmarkFaultScenario> ParseBenchmarkFaultScenario(
    const std::string& name);

std::string BenchmarkFaultValue(
    const CedarTgDataset& dataset, uint64_t vertex_id);

struct BenchmarkFaultCampaignConfig {
  BenchmarkFaultScenario scenario =
      BenchmarkFaultScenario::kCommitAfterPrepareDurable;
  uint32_t vertex_property_schema_epoch = 0;
  uint64_t valid_time = 0;
  std::function<std::unique_ptr<CedarDatabase>()> reopen_database;
};

// Executes one expected commit fault, destroys the uncertain process-local
// database state, reopens from durable files, and verifies the outcome at the
// selected durability boundary.
StatusOr<BenchmarkWorkloadResult> RunBenchmarkFaultCampaign(
    std::unique_ptr<CedarDatabase>* database,
    const CedarTgDataset& dataset,
    const BenchmarkFaultCampaignConfig& config);

}  // namespace cedar

#endif  // CEDAR_BENCHMARK_FAULT_CAMPAIGN_H_
