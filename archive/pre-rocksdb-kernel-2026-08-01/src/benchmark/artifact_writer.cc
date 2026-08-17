// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/benchmark/artifact_writer.h"

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <iomanip>
#include <sstream>
#include <unistd.h>

#include "cedar/benchmark/report_builder.h"

namespace cedar {
namespace {

std::string EscapeJson(const std::string& input) {
  std::string output;
  output.reserve(input.size());
  for (unsigned char character : input) {
    switch (character) {
      case '"': output.append("\\\""); break;
      case '\\': output.append("\\\\"); break;
      case '\b': output.append("\\b"); break;
      case '\f': output.append("\\f"); break;
      case '\n': output.append("\\n"); break;
      case '\r': output.append("\\r"); break;
      case '\t': output.append("\\t"); break;
      default:
        if (character < 0x20) {
          constexpr char kHex[] = "0123456789abcdef";
          output.append("\\u00");
          output.push_back(kHex[character >> 4]);
          output.push_back(kHex[character & 0x0f]);
        } else {
          output.push_back(static_cast<char>(character));
        }
    }
  }
  return output;
}

Status WriteAll(int fd, const std::string& content, const std::string& path) {
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
  std::error_code error;
  std::filesystem::create_directories(target.parent_path(), error);
  if (error) return Status::IOError(path, error.message());
  const std::string temporary = path + ".tmp";
  const int fd = ::open(temporary.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) return Status::IOError(temporary, std::strerror(errno));
  Status status = WriteAll(fd, content, temporary);
  if (status.ok() && ::fsync(fd) != 0) status = Status::IOError(temporary, std::strerror(errno));
  if (::close(fd) != 0 && status.ok()) status = Status::IOError(temporary, std::strerror(errno));
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
  if (directory_fd < 0) return Status::IOError(target.parent_path().string(), std::strerror(errno));
  if (::fsync(directory_fd) != 0) {
    const Status fsync_status = Status::IOError(target.parent_path().string(), std::strerror(errno));
    ::close(directory_fd);
    return fsync_status;
  }
  if (::close(directory_fd) != 0) return Status::IOError(target.parent_path().string(), std::strerror(errno));
  return Status::OK();
}

bool ProtocolComplete(const std::vector<BenchmarkPhaseRecord>& phases) {
  constexpr size_t kExpectedPhaseCount = 11;
  if (phases.size() != kExpectedPhaseCount) return false;
  for (const BenchmarkPhaseRecord& phase : phases) {
    if (phase.terminal_status != "PASS") return false;
  }
  return true;
}

void SerializeRatio(std::ostringstream* output, const char* name,
                    const BenchmarkRatio& ratio, bool* first) {
  if (!*first) *output << ',';
  *first = false;
  const bool defined = ratio.denominator != 0;
  *output << '"' << name << "\":{\"defined\":"
          << (defined ? "true" : "false")
          << ",\"numerator\":" << ratio.numerator
          << ",\"denominator\":" << ratio.denominator
          << ",\"value\":";
  if (defined) {
    *output << std::setprecision(17)
            << static_cast<double>(ratio.numerator) /
                   static_cast<double>(ratio.denominator);
  } else {
    *output << "null";
  }
  *output << '}';
}

void SerializeLag(std::ostringstream* output, const BenchmarkLag& lag,
                  bool* first) {
  if (!*first) *output << ',';
  *first = false;
  const bool defined = lag.available && lag.committed_seq >= lag.visible_seq;
  *output << "\"visible_prefix_lag\":{\"defined\":"
          << (defined ? "true" : "false")
          << ",\"committed_seq\":" << lag.committed_seq
          << ",\"visible_seq\":" << lag.visible_seq
          << ",\"value\":";
  if (defined) {
    *output << lag.committed_seq - lag.visible_seq;
  } else {
    *output << "null";
  }
  *output << '}';
}

void SerializeOptionalU64(std::ostringstream* output, bool defined,
                          uint64_t value) {
  if (defined) *output << value;
  else *output << "null";
}

void SerializeTransactionDistribution(
    std::ostringstream* output, const char* name,
    const TransactionMeasurementDistribution& distribution, bool* first) {
  if (!*first) *output << ',';
  *first = false;
  const bool defined = distribution.defined && distribution.sample_count != 0;
  *output << '"' << name << "\":{\"defined\":"
          << (defined ? "true" : "false")
          << ",\"sample_count\":" << distribution.sample_count
          << ",\"min_ns\":";
  SerializeOptionalU64(output, defined, distribution.min_ns);
  *output << ",\"p50_ns\":";
  SerializeOptionalU64(output, defined, distribution.p50_ns);
  *output << ",\"p95_ns\":";
  SerializeOptionalU64(output, defined, distribution.p95_ns);
  *output << ",\"p99_ns\":";
  SerializeOptionalU64(output, defined, distribution.p99_ns);
  *output << ",\"p999_ns\":";
  SerializeOptionalU64(output, defined, distribution.p999_ns);
  *output << ",\"max_ns\":";
  SerializeOptionalU64(output, defined, distribution.max_ns);
  *output << ",\"sum_ns\":";
  SerializeOptionalU64(output, defined, distribution.sum_ns);
  *output << '}';
}

void SerializeTransactionMeasurements(
    std::ostringstream* output, const TransactionMeasurementWindow& measurements) {
  const bool available = measurements.available;
  *output << "\"transaction_measurements\":{\"available\":"
          << (available ? "true" : "false")
          << ",\"availability_reason\":\""
          << EscapeJson(measurements.availability_reason)
          << "\",\"started\":" << measurements.started
          << ",\"committed\":" << measurements.committed
          << ",\"aborted\":" << measurements.aborted
          << ",\"indeterminate\":" << measurements.indeterminate
          << ",\"conflicts\":" << measurements.conflicts
          << ",\"visible_prefix_nonzero_stalls\":"
          << measurements.visible_prefix_nonzero_stalls
          << ",\"visible_prefix_max_lag_seq\":{\"defined\":"
          << (measurements.visible_prefix_max_lag_defined ? "true" : "false")
          << ",\"value\":";
  SerializeOptionalU64(output, measurements.visible_prefix_max_lag_defined,
                       measurements.visible_prefix_max_lag_seq);
  *output << "},\"conflict_abort_rate\":{\"defined\":"
          << (measurements.conflict_abort_rate.defined ? "true" : "false")
          << ",\"numerator\":" << measurements.conflict_abort_rate.numerator
          << ",\"denominator\":" << measurements.conflict_abort_rate.denominator
          << ",\"value\":";
  if (measurements.conflict_abort_rate.defined) {
    *output << std::setprecision(17)
            << static_cast<double>(measurements.conflict_abort_rate.numerator) /
                   static_cast<double>(measurements.conflict_abort_rate.denominator);
  } else {
    *output << "null";
  }
  *output << "},";
  bool first = true;
  SerializeTransactionDistribution(output, "commit_latency",
                                   measurements.commit_latency, &first);
  SerializeTransactionDistribution(output, "prepare_latency",
                                   measurements.prepare_latency, &first);
  SerializeTransactionDistribution(output, "decision_latency",
                                   measurements.decision_latency, &first);
  SerializeTransactionDistribution(output, "decision_fsync_latency",
                                   measurements.decision_fsync_latency, &first);
  SerializeTransactionDistribution(output, "visible_prefix_wait_success",
                                   measurements.visible_prefix_wait_success, &first);
  SerializeTransactionDistribution(output, "visible_prefix_wait_failure",
                                   measurements.visible_prefix_wait_failure, &first);
  *output << '}';
}

}  // namespace

std::string SerializeBenchmarkArtifactSummary(const BenchmarkArtifactSummary& summary) {
  std::ostringstream output;
  output << "{\"artifact_schema_version\":" << summary.schema_version
         << ",\"measurement_mode\":\"" << EscapeJson(summary.measurement_mode)
         << "\",\"cache_preparation\":\"" << EscapeJson(summary.cache_preparation)
         << "\",\"maintenance_state\":\"" << EscapeJson(summary.maintenance_state)
         << "\",\"warmup_sample_count\":" << summary.warmup_sample_count
         << ",\"measurement_elapsed_ns\":" << summary.measurement_elapsed_ns
         << ",\"measured_work_units\":" << summary.measured_work_units
         << ",\"measurement_throughput\":" << std::setprecision(17)
         << summary.measurement_throughput
         << ",\"latency_p50_ns\":" << summary.latency_p50_ns
         << ",\"latency_p95_ns\":" << summary.latency_p95_ns
         << ",\"latency_p99_ns\":" << summary.latency_p99_ns
         << ",\"latency_p999_ns\":" << summary.latency_p999_ns
         << ",\"logical_work_units\":" << summary.logical_work_units
         << ",\"physical_read_bytes\":" << summary.physical_read_bytes
         << ",\"physical_write_bytes\":" << summary.physical_write_bytes
         << ",\"physical_read_bytes_available\":"
         << (summary.physical_read_bytes_available ? "true" : "false")
         << ",\"physical_write_bytes_available\":"
         << (summary.physical_write_bytes_available ? "true" : "false")
         << ",\"durable_write_bytes\":{\"wal\":"
         << summary.durable_write_bytes.wal
         << ",\"decision_log\":"
         << summary.durable_write_bytes.decision_log
         << ",\"sst_flush\":"
         << summary.durable_write_bytes.sst_flush
         << ",\"compaction\":"
         << summary.durable_write_bytes.compaction
         << ",\"blob\":" << summary.durable_write_bytes.blob
         << ",\"manifest\":" << summary.durable_write_bytes.manifest
         << "}"
         << ",\"metrics_artifact_present\":"
         << (summary.metrics_artifact_present || !summary.metrics_json.empty() ? "true" : "false")
         << ",\"histograms_artifact_present\":"
         << (summary.histograms_artifact_present || !summary.histograms_json.empty() ? "true" : "false")
         << ",\"traces_artifact_present\":"
         << (summary.traces_artifact_present || !summary.traces_json.empty() ? "true" : "false")
         << ",\"explain_artifact_present\":"
         << (summary.explain_artifact_present || !summary.explain_json.empty() ? "true" : "false")
         << ",\"derived_metrics\":{";
  bool first_metric = true;
  SerializeRatio(&output, "write_amplification",
                 summary.derived_metrics.write_amplification, &first_metric);
  SerializeRatio(&output, "read_amplification",
                 summary.derived_metrics.read_amplification, &first_metric);
  SerializeRatio(&output, "space_amplification",
                 summary.derived_metrics.space_amplification, &first_metric);
  SerializeRatio(&output, "index_survival",
                 summary.derived_metrics.index_survival, &first_metric);
  SerializeRatio(&output, "interval_survival",
                 summary.derived_metrics.interval_survival, &first_metric);
  SerializeRatio(&output, "blob_materialization",
                 summary.derived_metrics.blob_materialization, &first_metric);
  SerializeRatio(&output, "cache_admission",
                 summary.derived_metrics.cache_admission, &first_metric);
  SerializeRatio(&output, "maintenance_share",
                 summary.derived_metrics.maintenance_share, &first_metric);
  SerializeLag(&output, summary.derived_metrics.visible_prefix_lag,
               &first_metric);
  output << "},";
  SerializeTransactionMeasurements(&output, summary.transaction_measurements);
  output << ",\"phases\":[";
  for (size_t index = 0; index < summary.phases.size(); ++index) {
    if (index != 0) output << ',';
    const BenchmarkPhaseRecord& phase = summary.phases[index];
    output << "{\"name\":\"" << BenchmarkPhaseName(phase.phase)
           << "\",\"elapsed_ns\":" << phase.elapsed_ns
           << ",\"terminal_status\":\"" << EscapeJson(phase.terminal_status) << "\"}";
  }
  output << "],\"measured_samples\":[";
  for (size_t index = 0; index < summary.measured_samples.size(); ++index) {
    if (index != 0) output << ',';
    const BenchmarkOperationSample& sample = summary.measured_samples[index];
    output << "{\"requested_arrival_ns\":" << sample.requested_arrival_ns
           << ",\"admitted_ns\":" << sample.admitted_ns
           << ",\"started_ns\":" << sample.started_ns
           << ",\"completed_ns\":" << sample.completed_ns
           << ",\"terminal_status\":\"" << EscapeJson(sample.terminal_status) << "\"}";
  }
  return output.str() + "]}";
}

std::string SerializeBenchmarkVerification(const BenchmarkVerification& verification,
                                           bool protocol_complete) {
  const bool passed = protocol_complete && verification.load_passed &&
      verification.result_passed && verification.reopen_passed;
  std::ostringstream output;
  output << "{\"verification_schema_version\":1,\"status\":\""
         << (passed ? "PASS" : "INVALID") << "\",\"protocol_complete\":"
         << (protocol_complete ? "true" : "false")
         << ",\"load_passed\":" << (verification.load_passed ? "true" : "false")
         << ",\"result_passed\":" << (verification.result_passed ? "true" : "false")
         << ",\"reopen_passed\":" << (verification.reopen_passed ? "true" : "false")
         << ",\"result_checksum\":\"" << EscapeJson(verification.result_checksum)
         << "\",\"detail\":\"" << EscapeJson(verification.detail) << "\"}";
  return output.str();
}

StatusOr<BenchmarkArtifactPaths> WriteBenchmarkArtifacts(
    const std::string& results_root, const BenchmarkRunManifest& manifest,
    const BenchmarkEnvironment& environment, const BenchmarkArtifactSummary& summary,
    const BenchmarkVerification& verification) {
  if (results_root.empty()) return Status::InvalidArgument("benchmark artifact", "results root is required");
  const std::string run_id = BenchmarkRunId(manifest);
  BenchmarkArtifactPaths paths;
  paths.run_directory = (std::filesystem::path(results_root) / run_id).string();
  paths.manifest_path = (std::filesystem::path(paths.run_directory) / "manifest.json").string();
  paths.summary_path = (std::filesystem::path(paths.run_directory) / "summary.json").string();
  paths.verification_path = (std::filesystem::path(paths.run_directory) / "verification.json").string();
  paths.environment_path = (std::filesystem::path(paths.run_directory) / "environment.txt").string();
  paths.metrics_path = (std::filesystem::path(paths.run_directory) / "metrics.json").string();
  paths.histograms_path = (std::filesystem::path(paths.run_directory) / "histograms.json").string();
  paths.traces_path = (std::filesystem::path(paths.run_directory) / "traces.json").string();
  paths.explain_path = (std::filesystem::path(paths.run_directory) / "explain" / "profile.json").string();
  paths.report_path = (std::filesystem::path(paths.run_directory) / "report.md").string();
  Status status = WriteBenchmarkRunManifest(paths.manifest_path, manifest);
  if (!status.ok()) return status;
  status = WriteAtomically(paths.summary_path, SerializeBenchmarkArtifactSummary(summary));
  if (!status.ok()) return status;
  const bool complete = ProtocolComplete(summary.phases);
  status = WriteAtomically(paths.verification_path, SerializeBenchmarkVerification(verification, complete));
  if (!status.ok()) return status;
  std::ostringstream environment_text;
  environment_text << "os_kernel=" << environment.os_kernel << '\n'
                   << "cpu_model_and_count=" << environment.cpu_model_and_count << '\n'
                   << "compiler_and_flags=" << environment.compiler_and_flags << '\n'
                   << "logical_cpu_count=" << environment.logical_cpu_count << '\n'
                   << "memory_limit_bytes=" << environment.memory_limit_bytes << '\n'
                   << "storage_free_bytes=" << environment.storage_free_bytes << '\n'
                   << "resource_limit_provenance_complete="
                   << (environment.resource_limit_provenance_complete
                           ? "true" : "false") << '\n'
                   << "storage_provenance_complete="
                   << (environment.storage_provenance_complete
                           ? "true" : "false") << '\n'
                   << "storage_device_and_filesystem="
                   << environment.storage_device_and_filesystem << '\n';
  status = WriteAtomically(paths.environment_path, environment_text.str());
  if (!status.ok()) return status;
  if (!summary.metrics_json.empty()) {
    status = WriteAtomically(paths.metrics_path, summary.metrics_json);
    if (!status.ok()) return status;
  }
  if (!summary.histograms_json.empty()) {
    status = WriteAtomically(paths.histograms_path, summary.histograms_json);
    if (!status.ok()) return status;
  }
  if (!summary.traces_json.empty()) {
    status = WriteAtomically(paths.traces_path, summary.traces_json);
    if (!status.ok()) return status;
  }
  if (!summary.explain_json.empty()) {
    status = WriteAtomically(paths.explain_path, summary.explain_json);
    if (!status.ok()) return status;
  }
  status = WriteAtomically(paths.report_path,
                           BuildBenchmarkReport(manifest, summary, verification, complete));
  if (!status.ok()) return status;
  return paths;
}

}  // namespace cedar
