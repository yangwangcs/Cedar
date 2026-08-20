// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "benchmarks/cedar_kernel_bench_options.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <filesystem>
#include <optional>
#include <utility>
#include <string_view>

namespace cedar::benchmark {
namespace {

std::optional<uint64_t> ParseUnsigned(std::string_view value) {
  uint64_t parsed = 0;
  const auto result = std::from_chars(value.data(), value.data() + value.size(), parsed);
  if (value.empty() || result.ec != std::errc() ||
      result.ptr != value.data() + value.size()) return std::nullopt;
  return parsed;
}

Status Invalid(const std::string& message) {
  return Status::InvalidArgument("cedar_kernel_bench", message);
}

}  // namespace

const char* KernelWorkloadName(KernelWorkload workload) {
  switch (workload) {
    case KernelWorkload::kPropertyPut: return "property-put";
    case KernelWorkload::kPointRead: return "point-read";
    case KernelWorkload::kMultiGet: return "multi-get";
    case KernelWorkload::kProjectedEventScan: return "projected-event-scan";
    case KernelWorkload::kFullEventScan: return "full-event-scan";
    case KernelWorkload::kMixed90Write10PointRead: return "mixed-90-write-10-point-read";
    case KernelWorkload::kMixedAppendProjectedScan: return "mixed-append-projected-scan";
  }
  return "unknown";
}

StatusOr<KernelBenchmarkOptions> ParseKernelBenchmarkOptions(
    const std::vector<std::string>& arguments) {
  KernelBenchmarkOptions options;
  for (size_t i = 0; i < arguments.size();) {
    if (i + 1 >= arguments.size()) return Invalid("missing option value");
    const std::string& name = arguments[i++];
    const std::string& value = arguments[i++];
    if (name == "--path") {
      if (!std::filesystem::path(value).is_absolute()) return Invalid("--path must be absolute");
      options.path = value;
      options.database_path = value;
    } else if (name == "--database-path" || name == "--seed-db") {
      if (!std::filesystem::path(value).is_absolute()) {
        return Invalid(name + " must be absolute");
      }
      if (name == "--database-path") {
        options.database_path = value;
        options.path = value;
      } else {
        options.seed_database = value;
      }
    } else if (name == "--seed") {
      const auto parsed = ParseUnsigned(value);
      if (!parsed.has_value()) return Invalid("seed must be unsigned");
      options.seed = *parsed;
    } else if (name == "--prepare-seed") {
      if (value == "true") options.prepare_seed_database = true;
      else if (value == "false") options.prepare_seed_database = false;
      else return Invalid("prepare-seed must be true or false");
    } else if (name == "--duration-seconds") {
      const auto parsed = ParseUnsigned(value);
      if (!parsed.has_value() || *parsed == 0) return Invalid("duration must be positive");
      options.duration_seconds = *parsed;
    } else if (name == "--operations") {
      const auto parsed = ParseUnsigned(value);
      if (!parsed.has_value() || *parsed == 0) return Invalid("operations must be positive");
      options.operations = *parsed;
    } else if (name == "--read-operations") {
      const auto parsed = ParseUnsigned(value);
      if (!parsed.has_value() || *parsed == 0) return Invalid("read operations must be positive");
      options.read_operations = *parsed;
    } else if (name == "--writer-clients") {
      const auto parsed = ParseUnsigned(value);
      if (!parsed.has_value() || *parsed == 0 || *parsed > 32) {
        return Invalid("writer clients must be in [1, 32]");
      }
      options.writer_clients = static_cast<uint32_t>(*parsed);
    } else if (name == "--campaign") {
      if (value == "none") options.campaign = CampaignKind::kNone;
      else if (value == "smoke") options.campaign = CampaignKind::kSmoke;
      else if (value == "warm") options.campaign = CampaignKind::kWarm;
      else if (value == "preflight") options.campaign = CampaignKind::kPreflight;
      else if (value == "sustained") options.campaign = CampaignKind::kSustained;
      else return Invalid("campaign must be none, smoke, warm, preflight, or sustained");
    } else if (name == "--workload") {
      const std::array<std::pair<std::string_view, KernelWorkload>, 7> workloads = {{
          {"property-put", KernelWorkload::kPropertyPut},
          {"point-read", KernelWorkload::kPointRead},
          {"multi-get", KernelWorkload::kMultiGet},
          {"projected-event-scan", KernelWorkload::kProjectedEventScan},
          {"full-event-scan", KernelWorkload::kFullEventScan},
          {"mixed-90-write-10-point-read", KernelWorkload::kMixed90Write10PointRead},
          {"mixed-append-projected-scan", KernelWorkload::kMixedAppendProjectedScan}}};
      const auto match = std::find_if(workloads.begin(), workloads.end(),
                                      [&value](const auto& candidate) {
                                        return candidate.first == value;
                                      });
      if (match == workloads.end()) return Invalid("unsupported workload");
      options.workload = match->second;
    } else if (name == "--verify-reopen") {
      if (value == "true") options.verify_reopen = true;
      else if (value == "false") options.verify_reopen = false;
      else return Invalid("verify-reopen must be true or false");
    } else {
      return Invalid("unsupported option " + name);
    }
  }
  if (options.path.empty() && !options.database_path.empty()) {
    options.path = options.database_path;
  }
  if (options.prepare_seed_database && options.seed_database.empty()) {
    return Invalid("prepare-seed requires --seed-db");
  }
  if (!options.seed_database.empty() && !options.database_path.empty()) {
    std::error_code error;
    const auto seed = std::filesystem::weakly_canonical(options.seed_database, error);
    if (error) return Invalid("cannot resolve --seed-db");
    const auto destination =
        std::filesystem::weakly_canonical(options.database_path, error);
    if (error) return Invalid("cannot resolve --database-path");
    if (seed == destination) {
      return Invalid("seed and destination paths must differ");
    }
  }
  if (options.campaign == CampaignKind::kWarm && options.duration_seconds != 30) {
    return Invalid("warm campaign requires duration-seconds=30");
  }
  if (options.campaign == CampaignKind::kPreflight &&
      options.duration_seconds != 60 && options.duration_seconds != 300) {
    return Invalid("preflight campaign requires duration-seconds=60 or 300");
  }
  if (options.campaign == CampaignKind::kSustained && options.duration_seconds < 1'800) {
    return Invalid("sustained campaign requires duration-seconds>=1800");
  }
  return options;
}

std::string BenchmarkQualificationStatus(const KernelBenchmarkOptions& options,
                                         const KernelBenchmarkSample& sample) {
  if (options.duration_seconds < 1'800 || sample.elapsed_seconds < 1'800) {
    return "warm_not_sustained";
  }
  if (options.verify_reopen && !sample.reopen_verified) return "sustained_reopen_failed";
  if (sample.writer_failures != 0) return "sustained_writer_failure";
  if (sample.write_stopped != 0) return "sustained_write_stopped";
  if (sample.background_errors != 0) return "sustained_background_error";
  if (sample.maintenance_errors != 0) return "sustained_maintenance_error";
  if (sample.unexplained_autonomous_jobs != 0) {
    return "sustained_unexplained_autonomous_maintenance";
  }
  if (sample.maintenance_max_snapshot_age_us > 250'000) {
    return "sustained_stale_maintenance_snapshot";
  }
  if (sample.pending_compaction_bytes >= 32ULL * 1024ULL * 1024ULL * 1024ULL) {
    return "sustained_pending_compaction_debt";
  }
  if (sample.n_plus_one_eligible_epochs == 0) return "sustained_n_plus_one_not_exercised";
  if (sample.n_plus_one_promoted_epochs * 100 <
      sample.n_plus_one_eligible_epochs * 95) {
    return "sustained_n_plus_one_below_95_percent";
  }
  return "sustained_local_gates_passed";
}

int CampaignExitCode(const KernelBenchmarkOptions& options,
                     const KernelBenchmarkSample& sample) {
  if (options.campaign == CampaignKind::kNone) return 0;
  const bool duration_complete =
      options.campaign == CampaignKind::kSmoke ||
      sample.elapsed_seconds >= static_cast<double>(options.duration_seconds);
  if (!duration_complete ||
      (options.verify_reopen && !sample.reopen_verified) || sample.writer_failures != 0 ||
      sample.write_stopped != 0 || sample.background_errors != 0 ||
      sample.maintenance_errors != 0 ||
      sample.unexplained_autonomous_jobs != 0 ||
      sample.retained_wal_bytes >= 1024ULL * 1024ULL * 1024ULL ||
      sample.maintenance_max_snapshot_age_us > 250'000 ||
      sample.pending_compaction_bytes >= 32ULL * 1024ULL * 1024ULL * 1024ULL) {
    return 1;
  }
  if (options.campaign == CampaignKind::kSustained &&
      BenchmarkQualificationStatus(options, sample) !=
          "sustained_local_gates_passed") {
    return 1;
  }
  return 0;
}

}  // namespace cedar::benchmark
