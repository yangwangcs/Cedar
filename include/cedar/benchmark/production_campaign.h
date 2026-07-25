// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_BENCHMARK_PRODUCTION_CAMPAIGN_H_
#define CEDAR_BENCHMARK_PRODUCTION_CAMPAIGN_H_

#include <cstdint>
#include <string>
#include <vector>

#include "cedar/benchmark/profile.h"
#include "cedar/core/status.h"

namespace cedar {

enum class ProductionCampaignCommandKind : uint8_t {
  kPairedBenchmark = 1,
  kFaultReopen = 2,
};

struct ProductionCampaignConfig {
  std::string pair_runner_path = "cedar_bench_pair";
  std::string baseline_path;
  std::string candidate_path;
  std::string baseline_blake3;
  std::string candidate_blake3;
  std::string baseline_sha256;
  std::string candidate_sha256;
  BenchmarkScaleProfile profile = BenchmarkScaleProfile::kWorkstation;
  uint64_t seed = 0;
  std::string results_root;
  uint32_t pair_count = 5;
  uint64_t order_seed = 1;
};

struct ProductionCampaignCommand {
  std::string id;
  ProductionCampaignCommandKind kind =
      ProductionCampaignCommandKind::kPairedBenchmark;
  std::string profile;
  std::string workload;
  std::string cache_mode;
  std::string fault_scenario;
  std::string baseline_blake3;
  std::string candidate_blake3;
  std::string baseline_sha256;
  std::string candidate_sha256;
  uint64_t seed = 0;
  uint64_t order_seed = 0;
  uint32_t pair_count = 0;
  std::string output_root;
  std::vector<std::string> argv;
};

struct ProductionCampaignEvidenceBinding {
  std::string path;
  std::string kind;
  std::string sha256;
};

StatusOr<std::vector<ProductionCampaignCommand>> BuildProductionCampaignPlan(
    const ProductionCampaignConfig& config);

Status VerifyProductionCampaignCommandOutput(
    const ProductionCampaignCommand& command);

Status ValidateProductionCampaignCompletionSet(
    const std::vector<ProductionCampaignCommand>& plan,
    const std::vector<std::string>& completed_command_ids);

Status ValidateProductionCampaignFinalization(
    const std::vector<ProductionCampaignCommand>& plan,
    const std::vector<std::string>& completed_command_ids);

StatusOr<std::string> SerializeProductionCampaignCommandIndexJson(
    const ProductionCampaignCommand& command,
    const std::vector<ProductionCampaignEvidenceBinding>& evidence);

bool ProductionCampaignEvidencePathMayDescend(
    const std::string& relative_path);

Status ValidateProductionCampaignEvidenceLedgerBindings(
    const std::vector<ProductionCampaignEvidenceBinding>& index_evidence,
    const std::vector<ProductionCampaignEvidenceBinding>& ledger_entries);

Status ValidateProductionCampaignLedgerSnapshot(
    const std::vector<ProductionCampaignEvidenceBinding>& expected_entries,
    const std::vector<ProductionCampaignEvidenceBinding>& actual_entries);

}  // namespace cedar

#endif  // CEDAR_BENCHMARK_PRODUCTION_CAMPAIGN_H_
