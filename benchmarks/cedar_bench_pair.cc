// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <fcntl.h>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

#include "cedar/benchmark/artifact_reader.h"
#include "cedar/benchmark/regression_compare.h"
#include "cedar/benchmark/regression_gate.h"
#include "cedar/blob/blob_store.h"
#include "cedar/core/status.h"
#include "cedar/observability/instrumentation_profile.h"

namespace {

using cedar::BenchmarkArtifactRecord;
using cedar::BenchmarkBaselineKey;
using cedar::BenchmarkRegressionSample;
using cedar::BenchmarkRunArm;
using cedar::Status;
using cedar::StatusOr;

bool ParseUnsigned(const char* text, uint64_t* value) {
  if (text == nullptr || value == nullptr || *text == '\0') return false;
  char* end = nullptr;
  const unsigned long long parsed = std::strtoull(text, &end, 10);
  if (end == nullptr || *end != '\0') return false;
  *value = static_cast<uint64_t>(parsed);
  return true;
}

std::string ReadBinaryHash(const std::string& path) {
  std::ifstream input(path, std::ios::binary);
  if (!input) return {};
  const std::string bytes((std::istreambuf_iterator<char>(input)),
                          std::istreambuf_iterator<char>());
  return cedar::BlobHashHex(cedar::Blake3Hash(bytes));
}

std::string JsonEscape(const std::string& value) {
  std::string escaped;
  for (const unsigned char character : value) {
    switch (character) {
      case '"': escaped += "\\\""; break;
      case '\\': escaped += "\\\\"; break;
      case '\n': escaped += "\\n"; break;
      case '\r': escaped += "\\r"; break;
      case '\t': escaped += "\\t"; break;
      default: escaped.push_back(static_cast<char>(character)); break;
    }
  }
  return escaped;
}

Status WriteAtomically(const std::string& path, const std::string& content) {
  const std::filesystem::path target(path);
  std::error_code error;
  std::filesystem::create_directories(target.parent_path(), error);
  if (error) return Status::IOError(path, error.message());
  const std::string temporary = path + ".tmp";
  const int fd = ::open(temporary.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) return Status::IOError(temporary, std::strerror(errno));
  const char* cursor = content.data();
  size_t remaining = content.size();
  Status status = Status::OK();
  while (remaining != 0) {
    const ssize_t written = ::write(fd, cursor, remaining);
    if (written < 0) {
      if (errno == EINTR) continue;
      status = Status::IOError(temporary, std::strerror(errno));
      break;
    }
    cursor += written;
    remaining -= static_cast<size_t>(written);
  }
  if (status.ok() && ::fsync(fd) != 0) status = Status::IOError(temporary, std::strerror(errno));
  if (::close(fd) != 0 && status.ok()) status = Status::IOError(temporary, std::strerror(errno));
  if (!status.ok()) {
    ::unlink(temporary.c_str());
    return status;
  }
  if (::rename(temporary.c_str(), path.c_str()) != 0) {
    ::unlink(temporary.c_str());
    return Status::IOError(path, std::strerror(errno));
  }
  const int directory_fd = ::open(target.parent_path().c_str(), O_RDONLY);
  if (directory_fd < 0) return Status::IOError(target.parent_path().string(), std::strerror(errno));
  if (::fsync(directory_fd) != 0) {
    const Status fsync_status = Status::IOError(target.parent_path().string(), std::strerror(errno));
    ::close(directory_fd);
    return fsync_status;
  }
  if (::close(directory_fd) != 0) {
    return Status::IOError(target.parent_path().string(), std::strerror(errno));
  }
  return Status::OK();
}

Status RunChild(const std::string& binary, uint64_t seed, uint64_t vertices,
                uint64_t edges, const std::string& results_root,
                const std::string& workload) {
  const pid_t child = ::fork();
  if (child < 0) return Status::IOError("cedar_bench_pair", std::strerror(errno));
  if (child == 0) {
    const std::string seed_text = std::to_string(seed);
    const std::string vertices_text = std::to_string(vertices);
    const std::string edges_text = std::to_string(edges);
    ::execl(binary.c_str(), binary.c_str(), seed_text.c_str(), vertices_text.c_str(),
            edges_text.c_str(), results_root.c_str(), workload.c_str(), nullptr);
    _exit(127);
  }
  int status = 0;
  while (::waitpid(child, &status, 0) < 0) {
    if (errno != EINTR) return Status::IOError("cedar_bench_pair", std::strerror(errno));
  }
  if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
    return Status::IOError(binary, "benchmark child exited unsuccessfully");
  }
  return Status::OK();
}

StatusOr<std::string> FindOnlyArtifact(const std::string& root) {
  std::error_code error;
  size_t count = 0;
  std::string artifact;
  for (std::filesystem::directory_iterator it(root, error), end; !error && it != end;
       it.increment(error)) {
    if (!it->is_directory(error)) continue;
    const std::filesystem::path candidate = it->path();
    if (std::filesystem::exists(candidate / "manifest.json") &&
        std::filesystem::exists(candidate / "summary.json") &&
        std::filesystem::exists(candidate / "verification.json")) {
      artifact = candidate.string();
      ++count;
    }
  }
  if (error) return Status::IOError(root, error.message());
  if (count != 1) {
    return Status::Corruption("cedar_bench_pair",
                              "expected exactly one artifact in " + root);
  }
  return artifact;
}

BenchmarkBaselineKey KeyFor(const BenchmarkArtifactRecord& record) {
  BenchmarkBaselineKey key;
  key.hardware_profile = record.manifest.os_kernel + "\n" +
      record.manifest.cpu_model_and_count + "\n" +
      record.manifest.storage_device_and_filesystem;
  key.dataset_hash = record.manifest.dataset_hash;
  key.workload_hash = record.manifest.workload_hash;
  key.durability_mode = record.manifest.durability_mode;
  key.cache_mode = record.manifest.cache_mode;
  key.resource_profile_id = record.manifest.resource_profile_id;
  key.database_format_version = record.manifest.database_format_version;
  return key;
}

BenchmarkRegressionSample SampleFor(const BenchmarkArtifactRecord& record) {
  const auto& summary = record.summary;
  const bool verified = record.protocol_complete && record.verification.load_passed &&
      record.verification.result_passed && record.verification.reopen_passed;
  const double amplification = summary.derived_metrics.space_amplification.denominator == 0
      ? 0.0
      : static_cast<double>(summary.derived_metrics.space_amplification.numerator) /
            static_cast<double>(summary.derived_metrics.space_amplification.denominator);
  return BenchmarkRegressionSample{verified, summary.measurement_throughput,
                                   static_cast<double>(summary.latency_p99_ns), 0.0,
                                   amplification};
}

struct PairRecord {
  uint32_t pair = 0;
  BenchmarkRunArm arm = BenchmarkRunArm::kBaseline;
  std::string status;
  std::string binary_hash;
  std::string instrumentation_profile_id;
  std::string run_id;
  std::string artifact;
};

std::string ArmName(BenchmarkRunArm arm) {
  return arm == BenchmarkRunArm::kBaseline ? "baseline" : "candidate";
}

std::string SerializeRecords(const std::vector<PairRecord>& records) {
  std::ostringstream output;
  output << "{\"paired_schema_version\":1,\"records\":[";
  for (size_t index = 0; index < records.size(); ++index) {
    if (index != 0) output << ',';
    const PairRecord& record = records[index];
    output << "{\"pair\":" << record.pair
           << ",\"arm\":\"" << ArmName(record.arm)
           << "\",\"status\":\"" << JsonEscape(record.status)
           << "\",\"binary_hash\":\"" << JsonEscape(record.binary_hash)
           << "\",\"instrumentation_profile_id\":\""
           << JsonEscape(record.instrumentation_profile_id)
           << "\",\"run_id\":\"" << JsonEscape(record.run_id)
           << "\",\"artifact\":\"" << JsonEscape(record.artifact) << "\"}";
  }
  output << "]}";
  return output.str();
}

}  // namespace

int main(int argc, char** argv) {
  const bool instrumentation_overhead =
      argc >= 2 && std::string(argv[1]) == "--instrumentation-overhead";
  const int first_argument = instrumentation_overhead ? 2 : 1;
  if (argc < first_argument + 7 || argc > first_argument + 9) {
    std::cerr << "usage: cedar_bench_pair <baseline> <candidate> <seed> <vertices> "
                 "<edges> <results-root> <workload> [pair-count] [order-seed]\n";
    std::cerr << "   or: cedar_bench_pair --instrumentation-overhead <minimal-baseline> "
                 "<tier01-candidate> <seed> <vertices> <edges> <results-root> "
                 "<workload> [pair-count] [order-seed]\n";
    return 2;
  }
  const std::filesystem::path baseline_path =
      std::filesystem::absolute(argv[first_argument]);
  const std::filesystem::path candidate_path =
      std::filesystem::absolute(argv[first_argument + 1]);
  if (baseline_path == candidate_path) {
    std::cerr << "baseline and candidate binaries must differ\n";
    return 2;
  }
  const std::string baseline_hash = ReadBinaryHash(baseline_path.string());
  const std::string candidate_hash = ReadBinaryHash(candidate_path.string());
  if (baseline_hash.empty() || candidate_hash.empty() || baseline_hash == candidate_hash) {
    std::cerr << "baseline and candidate must be readable and have distinct hashes\n";
    return 2;
  }
  uint64_t seed = 0;
  uint64_t vertices = 0;
  uint64_t edges = 0;
  if (!ParseUnsigned(argv[first_argument + 2], &seed) ||
      !ParseUnsigned(argv[first_argument + 3], &vertices) ||
      !ParseUnsigned(argv[first_argument + 4], &edges)) {
    std::cerr << "seed, vertices, and edges must be unsigned integers\n";
    return 2;
  }
  uint64_t pair_count = 5;
  uint64_t order_seed = 1;
  if (argc >= first_argument + 8 &&
      (!ParseUnsigned(argv[first_argument + 7], &pair_count) ||
       pair_count < 5 || pair_count > 1000)) {
    std::cerr << "pair-count must be in [5,1000]\n";
    return 2;
  }
  if (argc == first_argument + 9 &&
      !ParseUnsigned(argv[first_argument + 8], &order_seed)) {
    std::cerr << "order-seed must be unsigned\n";
    return 2;
  }
  const std::filesystem::path root =
      std::filesystem::absolute(argv[first_argument + 5]);
  std::error_code error;
  std::filesystem::create_directories(root, error);
  if (error) {
    std::cerr << "unable to create results root: " << error.message() << "\n";
    return 1;
  }

  std::vector<BenchmarkRegressionSample> baseline_samples(pair_count);
  std::vector<BenchmarkRegressionSample> candidate_samples(pair_count);
  std::vector<PairRecord> records;
  records.reserve(pair_count * 2);
  BenchmarkBaselineKey baseline_key;
  BenchmarkBaselineKey candidate_key;
  bool have_baseline_key = false;
  bool have_candidate_key = false;
  std::string baseline_profile_id;
  std::string candidate_profile_id;
  const auto order = cedar::BuildAlternatingPairedOrder(
      static_cast<uint32_t>(pair_count), order_seed);
  for (size_t sequence = 0; sequence < order.size(); ++sequence) {
    const uint32_t pair = static_cast<uint32_t>(sequence / 2);
    const BenchmarkRunArm arm = order[sequence];
    const std::string arm_root = (root / ("pair-" + std::to_string(pair) + "-" +
                                          ArmName(arm))).string();
    std::filesystem::create_directories(arm_root, error);
    if (error) {
      std::cerr << "unable to create pair directory: " << error.message() << "\n";
      return 1;
    }
    const std::string& binary = arm == BenchmarkRunArm::kBaseline
        ? baseline_path.string() : candidate_path.string();
    PairRecord record{pair, arm, "child-failed", arm == BenchmarkRunArm::kBaseline
                                      ? baseline_hash : candidate_hash, {}, {}, {}};
    const Status child = RunChild(binary, seed, vertices, edges, arm_root,
                                  argv[first_argument + 6]);
    const auto artifact = FindOnlyArtifact(arm_root);
    if (artifact.ok()) {
      record.artifact = artifact.ValueOrDie();
      const auto parsed = cedar::ReadBenchmarkArtifact(record.artifact);
      if (parsed.ok()) {
        record.run_id = parsed.ValueOrDie().run_id;
        record.instrumentation_profile_id =
            parsed.ValueOrDie().manifest.instrumentation_profile_id;
        record.status = child.ok() ? "PASS" : "CHILD_FAILED";
        const BenchmarkBaselineKey key = KeyFor(parsed.ValueOrDie());
        const std::string expected_hash = arm == BenchmarkRunArm::kBaseline
            ? baseline_hash : candidate_hash;
        if (parsed.ValueOrDie().manifest.binary_hash != expected_hash) {
          record.status = "BINARY_PROVENANCE_MISMATCH";
        } else if (arm == BenchmarkRunArm::kBaseline) {
          if (!have_baseline_key) { baseline_key = key; have_baseline_key = true; }
          if (baseline_profile_id.empty()) {
            baseline_profile_id = record.instrumentation_profile_id;
          }
          if (cedar::BenchmarkBaselineKeyId(key) != cedar::BenchmarkBaselineKeyId(baseline_key)) {
            record.status = "BASELINE_KEY_MISMATCH";
          } else if (instrumentation_overhead &&
                     record.instrumentation_profile_id != baseline_profile_id) {
            record.status = "BASELINE_PROFILE_MISMATCH";
          }
          baseline_samples[pair] = SampleFor(parsed.ValueOrDie());
          if (!baseline_samples[pair].verification_passed && child.ok()) {
            record.status = "VERIFICATION_FAILED";
          }
          if (!child.ok()) baseline_samples[pair].verification_passed = false;
        } else {
          if (!have_candidate_key) { candidate_key = key; have_candidate_key = true; }
          if (candidate_profile_id.empty()) {
            candidate_profile_id = record.instrumentation_profile_id;
          }
          if (cedar::BenchmarkBaselineKeyId(key) != cedar::BenchmarkBaselineKeyId(candidate_key)) {
            record.status = "CANDIDATE_KEY_MISMATCH";
          } else if (instrumentation_overhead &&
                     record.instrumentation_profile_id != candidate_profile_id) {
            record.status = "CANDIDATE_PROFILE_MISMATCH";
          }
          candidate_samples[pair] = SampleFor(parsed.ValueOrDie());
          if (!candidate_samples[pair].verification_passed && child.ok()) {
            record.status = "VERIFICATION_FAILED";
          }
          if (!child.ok()) candidate_samples[pair].verification_passed = false;
        }
      } else {
        record.status = "ARTIFACT_INVALID";
      }
    } else {
      record.status = child.ok() ? "ARTIFACT_MISSING" : "CHILD_FAILED_NO_ARTIFACT";
    }
    records.push_back(std::move(record));
  }

  if (!have_baseline_key) baseline_key = BenchmarkBaselineKey{};
  if (!have_candidate_key) candidate_key = BenchmarkBaselineKey{};
  const std::string gate_path = (root / "regression-gate.json").string();
  const auto gate = instrumentation_overhead
      ? cedar::WriteInstrumentationOverheadGate(
            gate_path, baseline_key, candidate_key, baseline_profile_id,
            candidate_profile_id, baseline_samples, candidate_samples)
      : cedar::WriteBenchmarkRegressionGate(
            gate_path, baseline_key, candidate_key, baseline_samples,
            candidate_samples);
  if (!gate.ok()) {
    std::cerr << gate.status().ToString() << "\n";
    return 1;
  }
  const Status provenance = WriteAtomically(
      (root / "paired-runs.json").string(), SerializeRecords(records));
  if (!provenance.ok()) {
    std::cerr << provenance.ToString() << "\n";
    return 1;
  }
  std::cout << "status=" << cedar::BenchmarkRegressionStatusName(gate.ValueOrDie().result.status)
            << " pair_count=" << gate.ValueOrDie().pair_count
            << " gate=" << gate_path << " provenance=" << (root / "paired-runs.json")
            << "\n";
  return gate.ValueOrDie().release_gate_passed ? 0 : 1;
}
