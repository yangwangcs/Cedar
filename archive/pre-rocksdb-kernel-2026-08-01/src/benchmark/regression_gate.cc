// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/benchmark/regression_gate.h"

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <iomanip>
#include <sstream>
#include <unistd.h>

namespace cedar {
namespace {

std::string EscapeJson(const std::string& input) {
  std::string output;
  output.reserve(input.size());
  for (const unsigned char character : input) {
    switch (character) {
      case '"': output.append("\\\""); break;
      case '\\': output.append("\\\\"); break;
      case '\n': output.append("\\n"); break;
      case '\r': output.append("\\r"); break;
      case '\t': output.append("\\t"); break;
      default: output.push_back(static_cast<char>(character)); break;
    }
  }
  return output;
}

Status WriteAll(int fd, const std::string& content,
                const std::string& path) {
  const char* cursor = content.data();
  size_t remaining = content.size();
  while (remaining != 0) {
    const ssize_t written = ::write(fd, cursor, remaining);
    if (written < 0) {
      if (errno == EINTR) continue;
      return Status::IOError(path, std::strerror(errno));
    }
    cursor += written;
    remaining -= static_cast<size_t>(written);
  }
  return Status::OK();
}

Status WriteAtomically(const std::string& path, const std::string& content) {
  const std::filesystem::path target(path);
  if (target.parent_path().empty()) {
    return Status::InvalidArgument("benchmark regression gate",
                                   "output path requires a parent directory");
  }
  std::error_code error;
  std::filesystem::create_directories(target.parent_path(), error);
  if (error) return Status::IOError(path, error.message());
  const std::string temporary = path + ".tmp";
  const int fd = ::open(temporary.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) return Status::IOError(temporary, std::strerror(errno));
  Status status = WriteAll(fd, content, temporary);
  if (status.ok() && ::fsync(fd) != 0) {
    status = Status::IOError(temporary, std::strerror(errno));
  }
  if (::close(fd) != 0 && status.ok()) {
    status = Status::IOError(temporary, std::strerror(errno));
  }
  if (!status.ok()) {
    ::unlink(temporary.c_str());
    return status;
  }
  if (::rename(temporary.c_str(), path.c_str()) != 0) {
    const Status rename_status = Status::IOError(path, std::strerror(errno));
    ::unlink(temporary.c_str());
    return rename_status;
  }
  const int directory_fd = ::open(target.parent_path().c_str(), O_RDONLY);
  if (directory_fd < 0) {
    return Status::IOError(target.parent_path().string(), std::strerror(errno));
  }
  if (::fsync(directory_fd) != 0) {
    const Status fsync_status =
        Status::IOError(target.parent_path().string(), std::strerror(errno));
    ::close(directory_fd);
    return fsync_status;
  }
  if (::close(directory_fd) != 0) {
    return Status::IOError(target.parent_path().string(), std::strerror(errno));
  }
  return Status::OK();
}

}  // namespace

const char* BenchmarkRegressionStatusName(BenchmarkRegressionStatus status) {
  switch (status) {
    case BenchmarkRegressionStatus::kPass: return "PASS";
    case BenchmarkRegressionStatus::kFail: return "FAIL";
    case BenchmarkRegressionStatus::kNoisy: return "NOISY";
    case BenchmarkRegressionStatus::kIncompatible: return "INCOMPATIBLE";
    case BenchmarkRegressionStatus::kInvalid: return "INVALID";
  }
  return "INVALID";
}

std::string SerializeBenchmarkRegressionGate(
    const BenchmarkRegressionGateArtifact& artifact,
    const BenchmarkRegressionPolicy& policy) {
  const BenchmarkRegressionResult& result = artifact.result;
  std::ostringstream output;
  output << std::setprecision(17)
         << "{\"regression_schema_version\":1"
         << ",\"status\":\""
         << BenchmarkRegressionStatusName(result.status) << "\""
         << ",\"release_gate_passed\":"
         << (artifact.release_gate_passed ? "true" : "false")
         << ",\"correctness_zero_tolerance\":true"
         << ",\"baseline_key_id\":\""
         << EscapeJson(artifact.baseline_key_id) << "\""
         << ",\"candidate_key_id\":\""
         << EscapeJson(artifact.candidate_key_id) << "\""
         << ",\"pair_count\":" << artifact.pair_count
         << ",\"policy\":{\"minimum_pair_count\":"
         << policy.minimum_pair_count
         << ",\"throughput_regression_fraction\":"
         << policy.throughput_regression_fraction
         << ",\"p99_regression_fraction\":"
         << policy.p99_regression_fraction
         << ",\"resource_regression_fraction\":"
         << policy.resource_regression_fraction
         << ",\"maximum_coefficient_of_variation\":"
         << policy.maximum_coefficient_of_variation
         << ",\"bootstrap_resamples\":" << policy.bootstrap_resamples
         << ",\"bootstrap_seed\":" << policy.bootstrap_seed
         << ",\"gate_memory\":" << (policy.gate_memory ? "true" : "false")
         << ",\"gate_amplification\":"
         << (policy.gate_amplification ? "true" : "false") << "}"
         << ",\"throughput\":{\"relative_median\":"
         << result.throughput_relative_median
         << ",\"ci_low\":" << result.throughput_ci_low
         << ",\"ci_high\":" << result.throughput_ci_high << "}"
         << ",\"p99_latency\":{\"relative_median\":"
         << result.p99_relative_median
         << ",\"ci_low\":" << result.p99_ci_low
         << ",\"ci_high\":" << result.p99_ci_high << "}"
         << ",\"variation\":{\"baseline_cv\":" << result.baseline_cv
         << ",\"candidate_cv\":" << result.candidate_cv << "}"
         << ",\"detail\":\"" << EscapeJson(result.detail) << "\"}";
  return output.str();
}

StatusOr<BenchmarkRegressionGateArtifact> WriteBenchmarkRegressionGate(
    const std::string& output_path,
    const BenchmarkBaselineKey& expected_key,
    const BenchmarkBaselineKey& candidate_key,
    const std::vector<BenchmarkRegressionSample>& baseline,
    const std::vector<BenchmarkRegressionSample>& candidate,
    const BenchmarkRegressionPolicy& policy) {
  if (output_path.empty()) {
    return Status::InvalidArgument("benchmark regression gate",
                                   "output path is required");
  }
  BenchmarkRegressionGateArtifact artifact;
  artifact.result = ComparePairedBenchmarkRuns(
      expected_key, candidate_key, baseline, candidate, policy);
  artifact.baseline_key_id = BenchmarkBaselineKeyId(expected_key);
  artifact.candidate_key_id = BenchmarkBaselineKeyId(candidate_key);
  artifact.pair_count = baseline.size() == candidate.size()
      ? static_cast<uint64_t>(baseline.size()) : 0;
  artifact.release_gate_passed =
      artifact.result.status == BenchmarkRegressionStatus::kPass;
  artifact.output_path = output_path;
  const Status written = WriteAtomically(
      output_path, SerializeBenchmarkRegressionGate(artifact, policy));
  if (!written.ok()) return written;
  return artifact;
}

StatusOr<BenchmarkRegressionGateArtifact> WriteInstrumentationOverheadGate(
    const std::string& output_path,
    const BenchmarkBaselineKey& minimal_key,
    const BenchmarkBaselineKey& instrumented_key,
    const std::string& minimal_profile_id,
    const std::string& instrumented_profile_id,
    const std::vector<BenchmarkRegressionSample>& minimal,
    const std::vector<BenchmarkRegressionSample>& instrumented,
    const InstrumentationOverheadPolicy& policy) {
  if (output_path.empty()) {
    return Status::InvalidArgument("instrumentation overhead gate",
                                   "output path is required");
  }
  BenchmarkRegressionGateArtifact artifact;
  artifact.result = CompareInstrumentationOverheadRuns(
      minimal_key, instrumented_key, minimal_profile_id,
      instrumented_profile_id, minimal, instrumented, policy);
  artifact.baseline_key_id = BenchmarkBaselineKeyId(minimal_key);
  artifact.candidate_key_id = BenchmarkBaselineKeyId(instrumented_key);
  artifact.pair_count = minimal.size() == instrumented.size()
      ? static_cast<uint64_t>(minimal.size()) : 0;
  artifact.release_gate_passed =
      artifact.result.status == BenchmarkRegressionStatus::kPass;
  artifact.output_path = output_path;
  const BenchmarkRegressionResult& result = artifact.result;
  std::ostringstream output;
  output << std::setprecision(17)
         << "{\"regression_schema_version\":1"
         << ",\"gate_kind\":\"instrumentation_overhead\""
         << ",\"status\":\""
         << BenchmarkRegressionStatusName(result.status) << "\""
         << ",\"release_gate_passed\":"
         << (artifact.release_gate_passed ? "true" : "false")
         << ",\"minimal_profile_id\":\""
         << EscapeJson(minimal_profile_id) << "\""
         << ",\"instrumented_profile_id\":\""
         << EscapeJson(instrumented_profile_id) << "\""
         << ",\"baseline_key_id\":\""
         << EscapeJson(artifact.baseline_key_id) << "\""
         << ",\"candidate_key_id\":\""
         << EscapeJson(artifact.candidate_key_id) << "\""
         << ",\"pair_count\":" << artifact.pair_count
         << ",\"policy\":{\"minimum_pair_count\":"
         << policy.minimum_pair_count
         << ",\"maximum_throughput_overhead_fraction\":"
         << policy.maximum_throughput_overhead_fraction
         << ",\"maximum_p99_overhead_fraction\":"
         << policy.maximum_p99_overhead_fraction
         << ",\"maximum_coefficient_of_variation\":"
         << policy.maximum_coefficient_of_variation
         << ",\"bootstrap_resamples\":" << policy.bootstrap_resamples
         << ",\"bootstrap_seed\":" << policy.bootstrap_seed << "}"
         << ",\"throughput_relative_median\":"
         << result.throughput_relative_median
         << ",\"p99_relative_median\":" << result.p99_relative_median
         << ",\"detail\":\"" << EscapeJson(result.detail) << "\"}";
  const Status written = WriteAtomically(output_path, output.str());
  if (!written.ok()) return written;
  return artifact;
}

}  // namespace cedar
