// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <fcntl.h>
#include <iostream>
#include <sstream>
#include <set>
#include <string>
#include <sys/wait.h>
#include <tuple>
#include <unistd.h>
#include <utility>
#include <vector>

#include "cedar/benchmark/artifact_reader.h"
#include "cedar/benchmark/environment_probe.h"
#include "cedar/benchmark/production_campaign.h"
#include "cedar/benchmark/profile.h"

extern char** environ;

namespace {

cedar::Status WriteAtomically(const std::string& path,
                              const std::string& content) {
  const std::filesystem::path target(path);
  std::error_code error;
  std::filesystem::create_directories(target.parent_path(), error);
  if (error) return cedar::Status::IOError(path, error.message());
  const std::string temporary = path + ".tmp";
  const int descriptor =
      ::open(temporary.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (descriptor < 0) {
    return cedar::Status::IOError(temporary, std::strerror(errno));
  }
  const char* cursor = content.data();
  size_t remaining = content.size();
  while (remaining != 0) {
    const ssize_t written = ::write(descriptor, cursor, remaining);
    if (written < 0 && errno == EINTR) continue;
    if (written <= 0) {
      const cedar::Status status = cedar::Status::IOError(
          temporary, written < 0 ? std::strerror(errno) : "short write");
      ::close(descriptor);
      ::unlink(temporary.c_str());
      return status;
    }
    cursor += written;
    remaining -= static_cast<size_t>(written);
  }
  if (::fsync(descriptor) != 0 || ::close(descriptor) != 0) {
    const cedar::Status status =
        cedar::Status::IOError(temporary, std::strerror(errno));
    ::unlink(temporary.c_str());
    return status;
  }
  if (::rename(temporary.c_str(), path.c_str()) != 0) {
    const cedar::Status status = cedar::Status::IOError(path,
                                                        std::strerror(errno));
    ::unlink(temporary.c_str());
    return status;
  }
  const int directory = ::open(target.parent_path().c_str(), O_RDONLY);
  if (directory < 0) {
    return cedar::Status::IOError(target.parent_path().string(),
                                  std::strerror(errno));
  }
  const int sync_result = ::fsync(directory);
  const int close_result = ::close(directory);
  if (sync_result != 0 || close_result != 0) {
    return cedar::Status::IOError(target.parent_path().string(),
                                  std::strerror(errno));
  }
  return cedar::Status::OK();
}

cedar::BenchmarkBaselineApproval ReadApproval() {
  const auto value = [](const char* name) {
    const char* text = std::getenv(name);
    return text == nullptr ? std::string() : std::string(text);
  };
  cedar::BenchmarkBaselineApproval approval;
  approval.approved_production_baseline =
      value("CEDAR_APPROVED_PRODUCTION_BASELINE") == "true";
  approval.approval_id = value("CEDAR_APPROVED_BASELINE_ID");
  approval.binary_sha256 = value("CEDAR_APPROVED_BASELINE_SHA256");
  approval.source_commit = value("CEDAR_APPROVED_BASELINE_SOURCE_COMMIT");
  approval.approved_by = value("CEDAR_APPROVED_BASELINE_AUTHORITY");
  return approval;
}

cedar::Status RunChild(
    const cedar::ProductionCampaignCommand& command,
    const cedar::BenchmarkExecutableSnapshot& executable_snapshot) {
  std::vector<char*> arguments;
  arguments.reserve(command.argv.size() + 1);
  for (const std::string& argument : command.argv) {
    arguments.push_back(const_cast<char*>(argument.c_str()));
  }
  arguments.push_back(nullptr);
  const pid_t child = ::fork();
  if (child < 0) {
    return cedar::Status::IOError("production campaign", std::strerror(errno));
  }
  if (child == 0) {
#if defined(__linux__)
    const int executable_fd = ::dup(executable_snapshot.fd());
    if (executable_fd < 0) _exit(127);
    ::fexecve(executable_fd, arguments.data(), environ);
#else
    ::execv(arguments.front(), arguments.data());
#endif
    _exit(127);
  }
  int status = 0;
  while (::waitpid(child, &status, 0) < 0) {
    if (errno != EINTR) {
      return cedar::Status::IOError("production campaign",
                                    std::strerror(errno));
    }
  }
  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
    return cedar::Status::IOError(
        command.id, WIFSIGNALED(status)
                        ? "child terminated by signal " +
                              std::to_string(WTERMSIG(status))
                        : "child exited with status " +
                              std::to_string(WEXITSTATUS(status)));
  }
  return cedar::Status::OK();
}

std::string ConfigurationText(
    const cedar::ProductionCampaignConfig& config,
    const cedar::BenchmarkExecutableSnapshot& baseline,
    const cedar::BenchmarkExecutableSnapshot& candidate,
    const cedar::BenchmarkBinaryProvenance& baseline_provenance,
    const cedar::BenchmarkBinaryProvenance& candidate_provenance,
    const cedar::BenchmarkBaselineApproval& approval) {
  const cedar::BenchmarkProfile profile =
      cedar::ResolveBenchmarkProfile(config.profile, config.seed);
  std::ostringstream output;
  output << "production_campaign_schema_version=1\n"
         << "profile=" << profile.name << "\n"
         << "seed=" << config.seed << "\n"
         << "pair_count=" << config.pair_count << "\n"
         << "order_seed=" << config.order_seed << "\n"
         << "baseline_sha256=" << baseline.sha256 << "\n"
         << "candidate_sha256=" << candidate.sha256 << "\n"
         << "baseline_source_commit=" << baseline_provenance.source_commit
         << "\n"
         << "candidate_source_commit=" << candidate_provenance.source_commit
         << "\n"
         << "approval_id=" << approval.approval_id << "\n"
         << "approved_by=" << approval.approved_by << "\n";
  return output.str();
}

cedar::Status RequireMatchingConfiguration(const std::string& path,
                                           const std::string& expected) {
  std::ifstream input(path, std::ios::binary);
  if (!input) {
    return cedar::Status::Corruption("production campaign",
                                     "existing root has no configuration");
  }
  const std::string actual((std::istreambuf_iterator<char>(input)),
                           std::istreambuf_iterator<char>());
  return actual == expected
             ? cedar::Status::OK()
             : cedar::Status::Corruption(
                   "production campaign",
                   "existing root configuration does not match this run");
}

cedar::Status ArchiveExecutable(
    const cedar::BenchmarkExecutableSnapshot& snapshot,
    const std::filesystem::path& destination) {
  std::error_code error;
  std::filesystem::create_directories(destination.parent_path(), error);
  if (error) {
    return cedar::Status::IOError(destination.string(), error.message());
  }
  if (!std::filesystem::exists(destination, error)) {
    const std::filesystem::path temporary = destination.string() + ".tmp";
    std::filesystem::copy_file(snapshot.path, temporary,
                               std::filesystem::copy_options::none, error);
    if (error) {
      std::filesystem::remove(temporary);
      return cedar::Status::IOError(temporary.string(), error.message());
    }
    std::filesystem::permissions(
        temporary, std::filesystem::perms::owner_read |
                       std::filesystem::perms::owner_exec |
                       std::filesystem::perms::group_read |
                       std::filesystem::perms::group_exec,
        std::filesystem::perm_options::replace, error);
    if (error) {
      std::filesystem::remove(temporary);
      return cedar::Status::IOError(temporary.string(), error.message());
    }
    const int descriptor = ::open(temporary.c_str(), O_RDONLY);
    if (descriptor < 0) {
      std::filesystem::remove(temporary);
      return cedar::Status::IOError(temporary.string(), std::strerror(errno));
    }
    const int sync_result = ::fsync(descriptor);
    const int sync_error = errno;
    const int close_result = ::close(descriptor);
    const int close_error = errno;
    if (sync_result != 0 || close_result != 0) {
      std::filesystem::remove(temporary);
      return cedar::Status::IOError(
          temporary.string(),
          std::strerror(sync_result != 0 ? sync_error : close_error));
    }
    std::filesystem::rename(temporary, destination, error);
    if (error) {
      std::filesystem::remove(temporary);
      return cedar::Status::IOError(destination.string(), error.message());
    }
  }
  const auto archived =
      cedar::CreateBenchmarkExecutableSnapshot(destination.string());
  if (!archived.ok()) return archived.status();
  if (archived.ValueOrDie().sha256 != snapshot.sha256 ||
      archived.ValueOrDie().blake3 != snapshot.blake3) {
    return cedar::Status::Corruption("production campaign",
                                     "archived executable bytes mismatch");
  }
  return cedar::Status::OK();
}

bool IsCampaignLedgerFile(const std::filesystem::path& relative) {
  if (relative == "SHA256SUMS" || relative.extension() == ".tmp") return false;
  if (relative.string().rfind("binaries/", 0) == 0 ||
      relative.string().rfind("state/", 0) == 0 ||
      relative == "campaign-config.txt" || relative == "campaign-index.json") {
    return true;
  }
  for (const std::filesystem::path& component : relative) {
    if (component == "database") return false;
  }
  const std::string filename = relative.filename().string();
  static const std::set<std::string> included = {
      "production-preflight.json", "paired-runs.json",
      "regression-gate.json", "manifest.json", "summary.json",
      "verification.json", "environment.txt", "report.md",
      "metrics.json", "histograms.json", "traces.json", "explain.json"};
  return included.find(filename) != included.end();
}

std::string JsonEscape(const std::string& value) {
  std::string escaped;
  for (const unsigned char character : value) {
    switch (character) {
      case '\"': escaped += "\\\""; break;
      case '\\': escaped += "\\\\"; break;
      case '\b': escaped += "\\b"; break;
      case '\f': escaped += "\\f"; break;
      case '\t': escaped += "\\t"; break;
      case '\n': escaped += "\\n"; break;
      case '\r': escaped += "\\r"; break;
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

cedar::StatusOr<std::vector<cedar::ProductionCampaignEvidenceBinding>>
CommandEvidenceBindings(
    const cedar::ProductionCampaignCommand& command) {
  std::vector<cedar::ProductionCampaignEvidenceBinding> bindings;
  const std::filesystem::path root(command.output_root);
  const auto append = [&](const std::filesystem::path& path,
                          const std::string& kind) -> cedar::Status {
    const auto digest = cedar::BenchmarkFileSha256(path.string());
    if (!digest.ok()) return digest.status();
    std::error_code relative_error;
    const std::filesystem::path relative =
        std::filesystem::relative(path, root, relative_error);
    if (relative_error || relative.empty() ||
        relative.generic_string().rfind("../", 0) == 0) {
      return cedar::Status::Corruption(
          "production campaign", "command evidence path escapes its root");
    }
    bindings.push_back(
        {relative.generic_string(), kind, digest.ValueOrDie()});
    return cedar::Status::OK();
  };
  if (command.kind == cedar::ProductionCampaignCommandKind::kPairedBenchmark) {
    cedar::Status status =
        append(root / "regression-gate.json", "paired-gate");
    if (status.ok()) {
      status = append(root / "paired-runs.json", "paired-ledger");
    }
    if (status.ok()) {
      status = append(root / "production-preflight.json", "paired-preflight");
    }
    if (!status.ok()) return status;
  }
  std::error_code error;
  size_t artifact_count = 0;
  for (std::filesystem::recursive_directory_iterator iterator(root, error),
       end; !error && iterator != end; iterator.increment(error)) {
    if (iterator->is_directory(error)) {
      const std::filesystem::path relative =
          std::filesystem::relative(iterator->path(), root, error);
      if (error) break;
      if (!cedar::ProductionCampaignEvidencePathMayDescend(
              relative.generic_string())) {
        iterator.disable_recursion_pending();
        continue;
      }
    }
    if (iterator->is_directory(error) &&
        std::filesystem::exists(iterator->path() / "manifest.json") &&
        std::filesystem::exists(iterator->path() / "summary.json") &&
        std::filesystem::exists(iterator->path() / "verification.json")) {
      ++artifact_count;
      cedar::Status status = append(iterator->path() / "manifest.json",
                                    "child-manifest");
      if (status.ok()) {
        status = append(iterator->path() / "summary.json", "child-summary");
      }
      if (status.ok()) {
        status = append(iterator->path() / "verification.json",
                        "child-verification");
      }
      if (!status.ok()) return status;
    }
  }
  if (error) return cedar::Status::IOError(root.string(), error.message());
  const size_t expected_artifacts =
      command.kind == cedar::ProductionCampaignCommandKind::kPairedBenchmark
          ? static_cast<size_t>(command.pair_count) * 2
          : 1;
  if (artifact_count != expected_artifacts) {
    return cedar::Status::Corruption(
        "production campaign", "command child evidence count mismatches");
  }
  std::sort(bindings.begin(), bindings.end(),
            [](const auto& left, const auto& right) {
              return std::tie(left.path, left.kind) <
                     std::tie(right.path, right.kind);
            });
  return bindings;
}

struct CampaignLedgerSeal {
  std::vector<std::string> paths;
  std::vector<cedar::ProductionCampaignEvidenceBinding> entries;
};

cedar::StatusOr<CampaignLedgerSeal> WriteCampaignLedger(
    const std::filesystem::path& root,
    const std::vector<cedar::ProductionCampaignEvidenceBinding>&
        index_evidence,
    const std::vector<cedar::ProductionCampaignEvidenceBinding>*
        expected_snapshot = nullptr) {
  std::vector<std::filesystem::path> files;
  std::error_code error;
  for (std::filesystem::recursive_directory_iterator iterator(root, error), end;
       !error && iterator != end; iterator.increment(error)) {
    if (iterator->is_directory(error)) {
      const std::filesystem::path relative =
          std::filesystem::relative(iterator->path(), root, error);
      if (error) break;
      if (!cedar::ProductionCampaignEvidencePathMayDescend(
              relative.generic_string())) {
        iterator.disable_recursion_pending();
        continue;
      }
    }
    if (!iterator->is_regular_file(error)) continue;
    const std::filesystem::path relative =
        std::filesystem::relative(iterator->path(), root, error);
    if (error) break;
    if (IsCampaignLedgerFile(relative)) files.push_back(relative);
  }
  if (error) return cedar::Status::IOError(root.string(), error.message());
  std::sort(files.begin(), files.end());
  std::ostringstream ledger;
  std::vector<cedar::ProductionCampaignEvidenceBinding> ledger_entries;
  ledger_entries.reserve(files.size());
  for (const std::filesystem::path& relative : files) {
    const auto digest =
        cedar::BenchmarkFileSha256((root / relative).string());
    if (!digest.ok()) return digest.status();
    ledger << digest.ValueOrDie() << "  " << relative.generic_string() << "\n";
    ledger_entries.push_back(
        {relative.generic_string(), "ledger", digest.ValueOrDie()});
  }
  cedar::Status status =
      cedar::ValidateProductionCampaignEvidenceLedgerBindings(
          index_evidence, ledger_entries);
  if (!status.ok()) return status;
  if (expected_snapshot != nullptr) {
    status = cedar::ValidateProductionCampaignLedgerSnapshot(
        *expected_snapshot, ledger_entries);
    if (!status.ok()) return status;
  }
  status = WriteAtomically((root / "SHA256SUMS").string(), ledger.str());
  if (!status.ok()) return status;
  std::vector<std::string> expected;
  expected.reserve(files.size());
  for (const std::filesystem::path& relative : files) {
    expected.push_back(relative.generic_string());
  }
  status = cedar::VerifySha256LedgerDirectory(root.string(), expected);
  if (!status.ok()) return status;
  return CampaignLedgerSeal{std::move(expected), std::move(ledger_entries)};
}

cedar::StatusOr<std::vector<std::string>> ReadCompletedCommandIds(
    const std::filesystem::path& state_root) {
  std::vector<std::string> ids;
  std::error_code error;
  for (std::filesystem::directory_iterator iterator(state_root, error), end;
       !error && iterator != end; iterator.increment(error)) {
    if (!iterator->is_regular_file(error) ||
        iterator->path().extension() != ".complete") {
      return cedar::Status::Corruption("production campaign",
                                       "state directory has an invalid entry");
    }
    std::ifstream input(iterator->path(), std::ios::binary);
    const std::string content((std::istreambuf_iterator<char>(input)),
                              std::istreambuf_iterator<char>());
    if (content !=
        "production_campaign_state_schema_version=1\nstatus=PASS\n") {
      return cedar::Status::Corruption("production campaign",
                                       "command state is malformed");
    }
    ids.push_back(iterator->path().stem().string());
  }
  if (error) return cedar::Status::IOError(state_root.string(), error.message());
  return ids;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc == 4 && std::string(argv[1]) == "--verify-paired") {
    const auto pair_count = cedar::ParseBenchmarkUnsigned(argv[3]);
    if (!pair_count.ok() || pair_count.ValueOrDie() < 5 ||
        pair_count.ValueOrDie() > std::numeric_limits<uint32_t>::max()) {
      std::cerr << "pair-count must be in [5,4294967295]\n";
      return 2;
    }
    const cedar::Status status = cedar::VerifyPairedBenchmarkOutput(
        argv[2], static_cast<uint32_t>(pair_count.ValueOrDie()));
    if (!status.ok()) {
      std::cerr << status.ToString() << "\n";
      return 1;
    }
    return 0;
  }
  if (argc < 6 || argc > 8) {
    std::cerr << "usage: cedar_production_campaign <baseline> <candidate> "
                 "<workstation|stress> <seed> <results-root> "
                 "[pair-count] [order-seed]\n"
                 "       cedar_production_campaign --verify-paired "
                 "<paired-root> <pair-count>\n";
    return 2;
  }
  const auto parsed_profile = cedar::ParseBenchmarkScaleProfile(argv[3]);
  const auto parsed_seed = cedar::ParseBenchmarkUnsigned(argv[4]);
  if (!parsed_profile.ok() || !parsed_seed.ok()) {
    std::cerr << (!parsed_profile.ok() ? parsed_profile.status().ToString()
                                      : parsed_seed.status().ToString())
              << "\n";
    return 2;
  }
  uint64_t pair_count = 5;
  uint64_t order_seed = 1;
  if (argc >= 7) {
    const auto parsed = cedar::ParseBenchmarkUnsigned(argv[6]);
    if (!parsed.ok() || parsed.ValueOrDie() < 5 ||
        parsed.ValueOrDie() > 1000) {
      std::cerr << "pair-count must be in [5,1000]\n";
      return 2;
    }
    pair_count = parsed.ValueOrDie();
  }
  if (argc == 8) {
    const auto parsed = cedar::ParseBenchmarkUnsigned(argv[7]);
    if (!parsed.ok()) {
      std::cerr << parsed.status().ToString() << "\n";
      return 2;
    }
    order_seed = parsed.ValueOrDie();
  }
  if (pair_count > std::numeric_limits<uint32_t>::max()) return 2;

  const std::filesystem::path baseline_path =
      std::filesystem::absolute(argv[1]);
  const std::filesystem::path candidate_path =
      std::filesystem::absolute(argv[2]);
  const std::filesystem::path root = std::filesystem::absolute(argv[5]);
  const std::filesystem::path pair_runner =
      std::filesystem::absolute(argv[0]).parent_path() / "cedar_bench_pair";
  const auto baseline =
      cedar::CreateBenchmarkExecutableSnapshot(baseline_path.string());
  const auto candidate =
      cedar::CreateBenchmarkExecutableSnapshot(candidate_path.string());
  const auto pair_runner_snapshot =
      cedar::CreateBenchmarkExecutableSnapshot(pair_runner.string());
  if (!baseline.ok() || !candidate.ok() || !pair_runner_snapshot.ok()) {
    std::cerr << (!baseline.ok() ? baseline.status().ToString()
                 : !candidate.ok() ? candidate.status().ToString()
                                   : pair_runner_snapshot.status().ToString())
              << "\n";
    return 2;
  }
  if (baseline.ValueOrDie().sha256 == candidate.ValueOrDie().sha256) {
    std::cerr << "baseline and candidate SHA-256 values must differ\n";
    return 2;
  }
  const cedar::BenchmarkBaselineApproval approval = ReadApproval();
  cedar::Status status = cedar::ValidateApprovedProductionBaseline(
      approval, baseline.ValueOrDie().sha256, candidate.ValueOrDie().sha256);
  if (!status.ok()) {
    std::cerr << status.ToString() << "\n";
    return 2;
  }
  const auto baseline_provenance =
      cedar::ReadBenchmarkBinaryProvenance(baseline.ValueOrDie());
  const auto candidate_provenance =
      cedar::ReadBenchmarkBinaryProvenance(candidate.ValueOrDie());
  if (!baseline_provenance.ok() || !candidate_provenance.ok()) {
    std::cerr << (!baseline_provenance.ok()
                      ? baseline_provenance.status().ToString()
                      : candidate_provenance.status().ToString())
              << "\n";
    return 2;
  }
  status = cedar::ValidateProductionBenchmarkBinaryProvenance(
      baseline_provenance.ValueOrDie(), approval.source_commit);
  if (status.ok()) {
    status = cedar::ValidateProductionBenchmarkBinaryProvenance(
        candidate_provenance.ValueOrDie());
  }
  if (!status.ok()) {
    std::cerr << status.ToString() << "\n";
    return 2;
  }
  const cedar::BenchmarkProfile profile = cedar::ResolveBenchmarkProfile(
      parsed_profile.ValueOrDie(), parsed_seed.ValueOrDie());
  const cedar::BenchmarkEnvironment environment =
      cedar::ProbeBenchmarkEnvironment(root.string());
  status = cedar::ValidateProductionBenchmarkPreflight(
      profile, profile.dataset, profile.worker_count, environment);
  if (!status.ok()) {
    std::cerr << status.ToString() << "\n";
    return 1;
  }

  cedar::ProductionCampaignConfig config;
  config.profile = parsed_profile.ValueOrDie();
  config.seed = parsed_seed.ValueOrDie();
  config.results_root = root.string();
  config.pair_count = static_cast<uint32_t>(pair_count);
  config.order_seed = order_seed;
  config.baseline_blake3 = baseline.ValueOrDie().blake3;
  config.candidate_blake3 = candidate.ValueOrDie().blake3;
  config.baseline_sha256 = baseline.ValueOrDie().sha256;
  config.candidate_sha256 = candidate.ValueOrDie().sha256;
  const std::string configuration = ConfigurationText(
      config, baseline.ValueOrDie(), candidate.ValueOrDie(),
      baseline_provenance.ValueOrDie(), candidate_provenance.ValueOrDie(),
      approval);
  const std::filesystem::path configuration_path = root / "campaign-config.txt";
  std::error_code error;
  if (std::filesystem::exists(root, error)) {
    if (error || !std::filesystem::is_directory(root, error)) {
      std::cerr << "results root is not a directory\n";
      return 1;
    }
    status = RequireMatchingConfiguration(configuration_path.string(),
                                          configuration);
  } else {
    std::filesystem::create_directories(root, error);
    status = error ? cedar::Status::IOError(root.string(), error.message())
                   : WriteAtomically(configuration_path.string(), configuration);
  }
  if (!status.ok()) {
    std::cerr << status.ToString() << "\n";
    return 1;
  }
  const std::filesystem::path archived_baseline =
      root / "binaries" / "baseline";
  const std::filesystem::path archived_candidate =
      root / "binaries" / "candidate";
  const std::filesystem::path archived_pair_runner =
      root / "binaries" / "cedar_bench_pair";
  status = ArchiveExecutable(baseline.ValueOrDie(), archived_baseline);
  if (status.ok()) {
    status = ArchiveExecutable(candidate.ValueOrDie(), archived_candidate);
  }
  if (status.ok()) {
    status = ArchiveExecutable(pair_runner_snapshot.ValueOrDie(),
                               archived_pair_runner);
  }
  if (!status.ok()) {
    std::cerr << status.ToString() << "\n";
    return 1;
  }
  const auto archived_baseline_snapshot =
      cedar::CreateBenchmarkExecutableSnapshot(archived_baseline.string());
  const auto archived_candidate_snapshot =
      cedar::CreateBenchmarkExecutableSnapshot(archived_candidate.string());
  const auto archived_pair_runner_snapshot =
      cedar::CreateBenchmarkExecutableSnapshot(archived_pair_runner.string());
  if (!archived_baseline_snapshot.ok() || !archived_candidate_snapshot.ok() ||
      !archived_pair_runner_snapshot.ok()) {
    std::cerr << (!archived_baseline_snapshot.ok()
                      ? archived_baseline_snapshot.status().ToString()
                  : !archived_candidate_snapshot.ok()
                      ? archived_candidate_snapshot.status().ToString()
                      : archived_pair_runner_snapshot.status().ToString())
              << "\n";
    return 1;
  }
  config.baseline_path = archived_baseline_snapshot.ValueOrDie().path;
  config.candidate_path = archived_candidate_snapshot.ValueOrDie().path;
  config.pair_runner_path = archived_pair_runner_snapshot.ValueOrDie().path;
  const auto plan = cedar::BuildProductionCampaignPlan(config);
  if (!plan.ok()) {
    std::cerr << plan.status().ToString() << "\n";
    return 2;
  }

  for (const cedar::ProductionCampaignCommand& command : plan.ValueOrDie()) {
    if (std::filesystem::exists(command.output_root, error)) {
      status = cedar::VerifyProductionCampaignCommandOutput(command);
      if (!status.ok()) {
        std::cerr << command.id << " has incomplete existing output: "
                  << status.ToString() << "\n";
        return 1;
      }
      status = WriteAtomically(
          (root / "state" / (command.id + ".complete")).string(),
          "production_campaign_state_schema_version=1\nstatus=PASS\n");
      if (!status.ok()) {
        std::cerr << status.ToString() << "\n";
        return 1;
      }
      std::cout << "resume=verified command=" << command.id << "\n";
      continue;
    }
    status = cedar::VerifyBenchmarkExecutableSnapshot(
        archived_baseline_snapshot.ValueOrDie());
    if (status.ok()) {
      status = cedar::VerifyBenchmarkExecutableSnapshot(
          archived_candidate_snapshot.ValueOrDie());
    }
    if (status.ok()) {
      status = cedar::VerifyBenchmarkExecutableSnapshot(
          archived_pair_runner_snapshot.ValueOrDie());
    }
    if (!status.ok()) {
      std::cerr << status.ToString() << "\n";
      return 1;
    }
    std::cout << "run command=" << command.id << "\n";
    const cedar::BenchmarkExecutableSnapshot& command_executable =
        command.kind == cedar::ProductionCampaignCommandKind::kPairedBenchmark
            ? archived_pair_runner_snapshot.ValueOrDie()
            : archived_candidate_snapshot.ValueOrDie();
    status = RunChild(command, command_executable);
    if (status.ok()) {
      status = cedar::VerifyBenchmarkExecutableSnapshot(
          archived_baseline_snapshot.ValueOrDie());
    }
    if (status.ok()) {
      status = cedar::VerifyBenchmarkExecutableSnapshot(
          archived_candidate_snapshot.ValueOrDie());
    }
    if (status.ok()) {
      status = cedar::VerifyBenchmarkExecutableSnapshot(
          archived_pair_runner_snapshot.ValueOrDie());
    }
    if (status.ok()) {
      status = cedar::VerifyProductionCampaignCommandOutput(command);
    }
    if (!status.ok()) {
      std::cerr << status.ToString() << "\n";
      return 1;
    }
    status = WriteAtomically(
        (root / "state" / (command.id + ".complete")).string(),
        "production_campaign_state_schema_version=1\nstatus=PASS\n");
    if (!status.ok()) {
      std::cerr << status.ToString() << "\n";
      return 1;
    }
  }

  const auto completed = ReadCompletedCommandIds(root / "state");
  if (!completed.ok()) {
    std::cerr << completed.status().ToString() << "\n";
    return 1;
  }
  status = cedar::ValidateProductionCampaignFinalization(
      plan.ValueOrDie(), completed.ValueOrDie());
  if (!status.ok()) {
    std::cerr << status.ToString() << "\n";
    return 1;
  }
  std::vector<std::string> command_json;
  command_json.reserve(plan.ValueOrDie().size());
  std::vector<cedar::ProductionCampaignEvidenceBinding> index_evidence;
  for (const cedar::ProductionCampaignCommand& command : plan.ValueOrDie()) {
    const auto evidence = CommandEvidenceBindings(command);
    if (!evidence.ok()) {
      std::cerr << evidence.status().ToString() << "\n";
      return 1;
    }
    const auto serialized = cedar::SerializeProductionCampaignCommandIndexJson(
        command, evidence.ValueOrDie());
    if (!serialized.ok()) {
      std::cerr << serialized.status().ToString() << "\n";
      return 1;
    }
    command_json.push_back(serialized.ValueOrDie());
    std::error_code relative_error;
    const std::filesystem::path command_relative =
        std::filesystem::relative(command.output_root, root, relative_error);
    if (relative_error || command_relative.empty() ||
        command_relative.generic_string().rfind("../", 0) == 0) {
      std::cerr << "command output escapes campaign root\n";
      return 1;
    }
    for (const cedar::ProductionCampaignEvidenceBinding& binding :
         evidence.ValueOrDie()) {
      index_evidence.push_back(
          {(command_relative / binding.path).generic_string(), binding.kind,
           binding.sha256});
    }
  }
  std::ostringstream index;
  index << "{\"production_campaign_index_schema_version\":2"
        << ",\"status\":\"PASS\",\"command_count\":"
        << plan.ValueOrDie().size()
        << ",\"baseline_sha256\":\"" << baseline.ValueOrDie().sha256
        << "\",\"candidate_sha256\":\"" << candidate.ValueOrDie().sha256
        << "\",\"baseline_source_commit\":\""
        << baseline_provenance.ValueOrDie().source_commit
        << "\",\"candidate_source_commit\":\""
        << candidate_provenance.ValueOrDie().source_commit
        << "\",\"approval_id\":\"" << JsonEscape(approval.approval_id)
        << "\",\"approved_by\":\"" << JsonEscape(approval.approved_by)
        << "\",\"profile\":\"" << JsonEscape(profile.name)
        << "\",\"host\":{\"os_kernel\":\""
        << JsonEscape(environment.os_kernel) << "\",\"cpu\":\""
        << JsonEscape(environment.cpu_model_and_count)
        << "\",\"logical_cpu_count\":" << environment.logical_cpu_count
        << ",\"memory_limit_bytes\":" << environment.memory_limit_bytes
        << ",\"storage_free_bytes\":" << environment.storage_free_bytes
        << ",\"storage_device_and_filesystem\":\""
        << JsonEscape(environment.storage_device_and_filesystem)
        << "\"},\"ledger_scope\":\"executables,configuration,state,"
           "paired-gates,artifact-protocol-and-report-files;"
           "database-directories-excluded\",\"commands\":[";
  for (size_t command_index = 0; command_index < command_json.size();
       ++command_index) {
    if (command_index != 0) index << ',';
    index << command_json[command_index];
  }
  index << "]}";
  status = WriteAtomically((root / "campaign-index.json").string(),
                           index.str());
  cedar::StatusOr<CampaignLedgerSeal> ledger_seal =
      status.ok() ? WriteCampaignLedger(root, index_evidence)
                  : cedar::StatusOr<CampaignLedgerSeal>(status);
  if (status.ok() && !ledger_seal.ok()) status = ledger_seal.status();
  if (status.ok()) {
    status = cedar::ValidateProductionCampaignFinalization(
        plan.ValueOrDie(), completed.ValueOrDie());
  }
  if (status.ok()) {
    const auto resealed = WriteCampaignLedger(
        root, index_evidence, &ledger_seal.ValueOrDie().entries);
    if (!resealed.ok()) {
      status = resealed.status();
    } else {
      status = cedar::VerifySha256LedgerDirectory(
          root.string(), resealed.ValueOrDie().paths);
    }
  }
  if (!status.ok()) {
    std::cerr << status.ToString() << "\n";
    return 1;
  }
  std::cout << "status=PASS command_count=" << plan.ValueOrDie().size()
            << " root=" << root << "\n";
  return 0;
}
