// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/benchmark/production_campaign.h"

#include <cctype>
#include <filesystem>
#include <map>
#include <set>
#include <sstream>

#include "cedar/benchmark/fault_campaign.h"
#include "cedar/benchmark/artifact_reader.h"
#include "cedar/benchmark/workload_driver.h"

namespace cedar {
namespace {

std::string ProfileName(BenchmarkScaleProfile profile) {
  switch (profile) {
    case BenchmarkScaleProfile::kWorkstation: return "workstation";
    case BenchmarkScaleProfile::kStress: return "stress";
    case BenchmarkScaleProfile::kCi: return "ci";
    case BenchmarkScaleProfile::kPaper: return "paper";
  }
  return {};
}

std::string JsonEscape(const std::string& value) {
  std::string escaped;
  for (const unsigned char character : value) {
    switch (character) {
      case '\"': escaped += "\\\""; break;
      case '\\': escaped += "\\\\"; break;
      case '\b': escaped += "\\b"; break;
      case '\f': escaped += "\\f"; break;
      case '\n': escaped += "\\n"; break;
      case '\r': escaped += "\\r"; break;
      case '\t': escaped += "\\t"; break;
      default:
        if (character < 0x20) {
          static const char* hex = "0123456789abcdef";
          escaped += "\\u00";
          escaped.push_back(hex[(character >> 4) & 0xf]);
          escaped.push_back(hex[character & 0xf]);
        } else {
          escaped.push_back(static_cast<char>(character));
        }
    }
  }
  return escaped;
}

bool IsSha256(const std::string& value) {
  if (value.size() != 64) return false;
  for (const unsigned char character : value) {
    if (!std::isxdigit(character)) return false;
  }
  return true;
}

}  // namespace

StatusOr<std::vector<ProductionCampaignCommand>> BuildProductionCampaignPlan(
    const ProductionCampaignConfig& config) {
  if (config.profile != BenchmarkScaleProfile::kWorkstation &&
      config.profile != BenchmarkScaleProfile::kStress) {
    return Status::NotSupported(
        "production campaign", "only workstation and stress are supported");
  }
  if (config.pair_runner_path.empty() || config.baseline_path.empty() ||
      config.candidate_path.empty() || config.results_root.empty()) {
    return Status::InvalidArgument(
        "production campaign", "runner, binaries, and results root are required");
  }
  if (std::filesystem::path(config.baseline_path).lexically_normal() ==
      std::filesystem::path(config.candidate_path).lexically_normal()) {
    return Status::InvalidArgument(
        "production campaign", "baseline and candidate paths must differ");
  }
  if (config.pair_count < 5 || config.pair_count > 1000) {
    return Status::InvalidArgument(
        "production campaign", "pair count must be in [5,1000]");
  }

  const std::string profile = ProfileName(config.profile);
  const std::vector<BenchmarkWorkloadFamily> workloads = {
      BenchmarkWorkloadFamily::kPointRead,
      BenchmarkWorkloadFamily::kBitemporalPointRead,
      BenchmarkWorkloadFamily::kAnalyticalVertexCount,
      BenchmarkWorkloadFamily::kValidTimeRange,
      BenchmarkWorkloadFamily::kGraphOneHop,
      BenchmarkWorkloadFamily::kBlobProjection,
      BenchmarkWorkloadFamily::kDurableIngestion,
      BenchmarkWorkloadFamily::kIndexEquality,
      BenchmarkWorkloadFamily::kIndexPathMatrix,
      BenchmarkWorkloadFamily::kSchedulerSaturation,
      BenchmarkWorkloadFamily::kMaintenanceCycle,
      BenchmarkWorkloadFamily::kHtapBalanced,
      BenchmarkWorkloadFamily::kRecovery,
  };
  const std::vector<BenchmarkCacheMode> cache_modes = {
      BenchmarkCacheMode::kColdProcessAndDatabase,
      BenchmarkCacheMode::kColdDatabaseWarmProcess,
      BenchmarkCacheMode::kWarmMetadataOnly,
      BenchmarkCacheMode::kWarmFullWorkingSet,
      BenchmarkCacheMode::kSteadyStateWithBackgroundMaintenance,
  };
  const std::vector<BenchmarkFaultScenario> faults = {
      BenchmarkFaultScenario::kCommitAfterPrepareDurable,
      BenchmarkFaultScenario::kCommitAfterDecisionDurable,
      BenchmarkFaultScenario::kManifestAfterRename,
      BenchmarkFaultScenario::kBlobIndexPartialWrite,
      BenchmarkFaultScenario::kSstAfterRename,
      BenchmarkFaultScenario::kSidecarAfterRename,
      BenchmarkFaultScenario::kBlobGcAfterManifestRename,
      BenchmarkFaultScenario::kAcceptedWorkShutdown,
      BenchmarkFaultScenario::kTcypherSpillDiskFull,
      BenchmarkFaultScenario::kTcypherSpillCorruption,
  };

  std::vector<ProductionCampaignCommand> commands;
  commands.reserve(workloads.size() * cache_modes.size() + faults.size());
  for (const BenchmarkWorkloadFamily workload_family : workloads) {
    const std::string workload = BenchmarkWorkloadFamilyName(workload_family);
    for (const BenchmarkCacheMode mode : cache_modes) {
      const std::string cache_mode = BenchmarkCacheModeName(mode);
      ProductionCampaignCommand command;
      command.id = "paired-" + workload + "-" + cache_mode;
      command.kind = ProductionCampaignCommandKind::kPairedBenchmark;
      command.profile = profile;
      command.workload = workload;
      command.cache_mode = cache_mode;
      command.pair_count = config.pair_count;
      command.baseline_blake3 = config.baseline_blake3;
      command.candidate_blake3 = config.candidate_blake3;
      command.baseline_sha256 = config.baseline_sha256;
      command.candidate_sha256 = config.candidate_sha256;
      command.seed = config.seed;
      command.order_seed = config.order_seed;
      command.output_root =
          (std::filesystem::path(config.results_root) / "commands" /
           command.id).string();
      command.argv = {
          config.pair_runner_path, "--production-release",
          config.baseline_path, config.candidate_path, profile,
          std::to_string(config.seed), command.output_root, workload,
          cache_mode, std::to_string(config.pair_count),
          std::to_string(config.order_seed)};
      commands.push_back(std::move(command));
    }
  }
  for (const BenchmarkFaultScenario scenario : faults) {
    const std::string fault = BenchmarkFaultScenarioName(scenario);
    ProductionCampaignCommand command;
    command.id = "fault-" + fault;
    command.kind = ProductionCampaignCommandKind::kFaultReopen;
    command.profile = profile;
    command.fault_scenario = fault;
    command.candidate_blake3 = config.candidate_blake3;
    command.candidate_sha256 = config.candidate_sha256;
    command.seed = config.seed;
    command.order_seed = config.order_seed;
    command.output_root =
        (std::filesystem::path(config.results_root) / "commands" /
         command.id).string();
    command.argv = {
        config.candidate_path, "--fault", fault, profile,
        std::to_string(config.seed), command.output_root};
    commands.push_back(std::move(command));
  }
  return commands;
}

Status VerifyProductionCampaignCommandOutput(
    const ProductionCampaignCommand& command) {
  if (command.id.empty() || command.output_root.empty()) {
    return Status::InvalidArgument("production campaign",
                                   "command identity and output root are required");
  }
  if (command.kind == ProductionCampaignCommandKind::kPairedBenchmark) {
    return VerifyPairedBenchmarkOutput(command.output_root,
                                       command.pair_count, command.profile,
                                       command.workload, command.cache_mode,
                                       command.baseline_blake3,
                                       command.candidate_blake3,
                                       command.baseline_sha256,
                                       command.candidate_sha256,
                                       command.seed, command.order_seed);
  }
  if (command.kind != ProductionCampaignCommandKind::kFaultReopen ||
      command.profile.empty() || command.fault_scenario.empty()) {
    return Status::InvalidArgument("production campaign",
                                   "fault command metadata is incomplete");
  }
  std::error_code error;
  size_t artifact_count = 0;
  std::filesystem::path artifact_path;
  for (std::filesystem::directory_iterator iterator(command.output_root, error),
       end; !error && iterator != end; iterator.increment(error)) {
    if (!iterator->is_directory(error)) continue;
    const std::filesystem::path candidate = iterator->path();
    if (std::filesystem::exists(candidate / "manifest.json") &&
        std::filesystem::exists(candidate / "summary.json") &&
        std::filesystem::exists(candidate / "verification.json")) {
      artifact_path = candidate;
      ++artifact_count;
    }
  }
  if (error || artifact_count != 1) {
    return Status::Corruption("production campaign",
                              "fault command must contain exactly one artifact");
  }
  const auto artifact = ReadBenchmarkArtifact(artifact_path.string());
  if (!artifact.ok()) return artifact.status();
  const BenchmarkArtifactRecord& record = artifact.ValueOrDie();
  if (!record.protocol_complete || !record.verification.load_passed ||
      !record.verification.result_passed ||
      !record.verification.reopen_passed || record.manifest.source_dirty ||
      record.manifest.dataset_profile_id != command.profile ||
      record.manifest.generator_seed != command.seed ||
      record.manifest.workload_id !=
          "cedar-fault-" + command.fault_scenario ||
      (!command.candidate_blake3.empty() &&
       record.manifest.binary_hash != command.candidate_blake3)) {
    return Status::Corruption("production campaign",
                              "fault artifact identity or verification mismatches");
  }
  Status status = ValidateProductionArtifactSourceProvenance(
      record.manifest.source_commit, record.manifest.source_dirty);
  if (!status.ok()) return status;
  return RegenerateBenchmarkReport(artifact_path.string());
}

Status ValidateProductionCampaignCompletionSet(
    const std::vector<ProductionCampaignCommand>& plan,
    const std::vector<std::string>& completed_command_ids) {
  std::set<std::string> expected;
  for (const ProductionCampaignCommand& command : plan) {
    if (command.id.empty() || !expected.insert(command.id).second) {
      return Status::InvalidArgument("production campaign",
                                     "plan command IDs must be unique");
    }
  }
  std::set<std::string> completed;
  for (const std::string& id : completed_command_ids) {
    if (id.empty() || !completed.insert(id).second) {
      return Status::Corruption("production campaign",
                                "completion command IDs must be unique");
    }
  }
  if (completed != expected) {
    return Status::Corruption("production campaign",
                              "completion set does not match the plan");
  }
  return Status::OK();
}

Status ValidateProductionCampaignFinalization(
    const std::vector<ProductionCampaignCommand>& plan,
    const std::vector<std::string>& completed_command_ids) {
  Status status =
      ValidateProductionCampaignCompletionSet(plan, completed_command_ids);
  if (!status.ok()) return status;
  for (const ProductionCampaignCommand& command : plan) {
    status = VerifyProductionCampaignCommandOutput(command);
    if (!status.ok()) return status;
  }
  return Status::OK();
}

StatusOr<std::string> SerializeProductionCampaignCommandIndexJson(
    const ProductionCampaignCommand& command,
    const std::vector<ProductionCampaignEvidenceBinding>& evidence) {
  if (command.id.empty() || command.output_root.empty() || command.argv.empty()) {
    return Status::InvalidArgument(
        "production campaign", "index command identity is incomplete");
  }
  for (const ProductionCampaignEvidenceBinding& binding : evidence) {
    if (binding.path.empty() || binding.kind.empty() ||
        !IsSha256(binding.sha256)) {
      return Status::InvalidArgument(
          "production campaign", "index evidence binding is incomplete");
    }
  }
  std::ostringstream output;
  output << "{\"id\":\"" << JsonEscape(command.id)
         << "\",\"kind\":\""
         << (command.kind == ProductionCampaignCommandKind::kPairedBenchmark
                 ? "paired"
                 : "fault")
         << "\",\"output_root\":\"" << JsonEscape(command.output_root)
         << "\",\"seed\":" << command.seed
         << ",\"order_seed\":" << command.order_seed << ",\"argv\":[";
  for (size_t index = 0; index < command.argv.size(); ++index) {
    if (index != 0) output << ',';
    output << '\"' << JsonEscape(command.argv[index]) << '\"';
  }
  output << "],\"evidence\":[";
  for (size_t index = 0; index < evidence.size(); ++index) {
    if (index != 0) output << ',';
    output << "{\"kind\":\"" << JsonEscape(evidence[index].kind)
           << "\",\"path\":\"" << JsonEscape(evidence[index].path)
           << "\",\"sha256\":\"" << evidence[index].sha256 << "\"}";
  }
  output << "]}";
  return output.str();
}

bool ProductionCampaignEvidencePathMayDescend(
    const std::string& relative_path) {
  for (const std::filesystem::path& component :
       std::filesystem::path(relative_path)) {
    if (component == "database") return false;
  }
  return true;
}

Status ValidateProductionCampaignEvidenceLedgerBindings(
    const std::vector<ProductionCampaignEvidenceBinding>& index_evidence,
    const std::vector<ProductionCampaignEvidenceBinding>& ledger_entries) {
  std::map<std::string, std::string> expected;
  for (const ProductionCampaignEvidenceBinding& binding : index_evidence) {
    if (binding.path.empty() || !IsSha256(binding.sha256) ||
        !expected.emplace(binding.path, binding.sha256).second) {
      return Status::InvalidArgument(
          "production campaign", "index evidence bindings are invalid");
    }
  }
  std::map<std::string, std::string> actual;
  for (const ProductionCampaignEvidenceBinding& binding : ledger_entries) {
    if (binding.path.empty() || !IsSha256(binding.sha256) ||
        !actual.emplace(binding.path, binding.sha256).second) {
      return Status::InvalidArgument(
          "production campaign", "ledger evidence bindings are invalid");
    }
  }
  for (const auto& [path, digest] : expected) {
    const auto found = actual.find(path);
    if (found == actual.end() || found->second != digest) {
      return Status::Corruption(
          "production campaign", "index evidence does not match final ledger");
    }
  }
  return Status::OK();
}

Status ValidateProductionCampaignLedgerSnapshot(
    const std::vector<ProductionCampaignEvidenceBinding>& expected_entries,
    const std::vector<ProductionCampaignEvidenceBinding>& actual_entries) {
  std::map<std::string, std::string> expected;
  std::map<std::string, std::string> actual;
  for (const ProductionCampaignEvidenceBinding& binding : expected_entries) {
    if (binding.path.empty() || !IsSha256(binding.sha256) ||
        !expected.emplace(binding.path, binding.sha256).second) {
      return Status::InvalidArgument(
          "production campaign", "expected ledger snapshot is invalid");
    }
  }
  for (const ProductionCampaignEvidenceBinding& binding : actual_entries) {
    if (binding.path.empty() || !IsSha256(binding.sha256) ||
        !actual.emplace(binding.path, binding.sha256).second) {
      return Status::InvalidArgument(
          "production campaign", "actual ledger snapshot is invalid");
    }
  }
  if (expected != actual) {
    return Status::Corruption(
        "production campaign", "campaign ledger snapshot changed during seal");
  }
  return Status::OK();
}

}  // namespace cedar
