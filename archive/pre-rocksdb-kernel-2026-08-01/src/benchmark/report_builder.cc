// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/benchmark/report_builder.h"

#include <algorithm>
#include <iomanip>
#include <sstream>
#include <vector>

namespace cedar {
namespace {

uint64_t Percentile(std::vector<uint64_t> values, double quantile) {
  if (values.empty()) return 0;
  std::sort(values.begin(), values.end());
  const size_t index = static_cast<size_t>(quantile * static_cast<double>(values.size() - 1));
  return values[index];
}

std::vector<uint64_t> CompletedLatencies(const BenchmarkArtifactSummary& summary) {
  std::vector<uint64_t> values;
  values.reserve(summary.measured_samples.size());
  for (const BenchmarkOperationSample& sample : summary.measured_samples) {
    if (sample.terminal_status == "PASS" && sample.completed_ns >= sample.requested_arrival_ns) {
      values.push_back(sample.completed_ns - sample.requested_arrival_ns);
    }
  }
  return values;
}

std::string RatioText(const BenchmarkRatio& ratio) {
  std::ostringstream output;
  if (ratio.denominator == 0) {
    output << "undefined (" << ratio.numerator << " / 0)";
  } else {
    output << ratio.numerator << " / " << ratio.denominator << " = "
           << std::fixed << std::setprecision(4)
           << static_cast<double>(ratio.numerator) /
                  static_cast<double>(ratio.denominator);
  }
  return output.str();
}

std::string LagText(const BenchmarkLag& lag) {
  std::ostringstream output;
  if (!lag.available || lag.committed_seq < lag.visible_seq) {
    output << "undefined (" << lag.committed_seq << " - "
           << lag.visible_seq << ')';
  } else {
    output << lag.committed_seq << " - " << lag.visible_seq << " = "
           << lag.committed_seq - lag.visible_seq;
  }
  return output.str();
}

std::string TransactionDistributionText(
    const TransactionMeasurementDistribution& distribution) {
  if (!distribution.defined || distribution.sample_count == 0) {
    return "unknown";
  }
  std::ostringstream output;
  output << "n=" << distribution.sample_count << ", p50="
         << distribution.p50_ns << ", p95=" << distribution.p95_ns
         << ", p99=" << distribution.p99_ns << ", max="
         << distribution.max_ns;
  return output.str();
}

std::string TransactionRatioText(const TransactionMeasurementRatio& ratio) {
  if (!ratio.defined) return "unknown";
  std::ostringstream output;
  output << ratio.numerator << " / " << ratio.denominator << " = "
         << std::fixed << std::setprecision(4)
         << static_cast<double>(ratio.numerator) /
                static_cast<double>(ratio.denominator);
  return output.str();
}

}  // namespace

std::string BuildBenchmarkReport(const BenchmarkRunManifest& manifest,
                                 const BenchmarkArtifactSummary& summary,
                                 const BenchmarkVerification& verification,
                                 bool protocol_complete) {
  const std::vector<uint64_t> latencies = CompletedLatencies(summary);
  const bool verified = protocol_complete && verification.load_passed &&
      verification.result_passed && verification.reopen_passed;
  std::ostringstream output;
  output << "# Cedar Benchmark Report\n\n";
  output << "Run ID: `" << BenchmarkRunId(manifest) << "`\n\n";
  output << "Verification: **" << (verified ? "PASS" : "INVALID") << "**\n\n";
  output << "| Field | Value |\n|---|---|\n";
  output << "| Dataset | " << manifest.dataset_id << " (`" << manifest.dataset_hash << "`) |\n";
  output << "| Dataset profile | " << manifest.dataset_profile_id << " |\n";
  output << "| Dataset vertices | " << manifest.dataset_vertex_count << " |\n";
  output << "| Dataset edges | " << manifest.dataset_edge_count << " |\n";
  output << "| Property events per vertex | "
         << manifest.dataset_property_events_per_vertex << " |\n";
  output << "| Valid-time span | " << manifest.dataset_valid_time_span
         << " |\n";
  output << "| Workload | " << manifest.workload_id << " (`" << manifest.workload_hash << "`) |\n";
  output << "| Durability | " << manifest.durability_mode << " |\n";
  output << "| Cache mode | " << manifest.cache_mode << " |\n";
  output << "| Resource profile | " << manifest.resource_profile_id << " |\n";
  output << "| Arrival model | " << summary.measurement_mode << " |\n";
  output << "| Warmup samples excluded | " << summary.warmup_sample_count << " |\n";
  output << "| Measurement elapsed ns | " << summary.measurement_elapsed_ns << " |\n";
  output << "| Measured work units | " << summary.measured_work_units << " |\n";
  output << "| Measured samples | " << summary.measured_samples.size() << " |\n";
  output << "| Logical work units | " << summary.logical_work_units << " |\n";
  output << "| Physical read bytes | ";
  if (summary.physical_read_bytes_available) output << summary.physical_read_bytes;
  else output << "undefined";
  output << " |\n";
  output << "| Physical write bytes | ";
  if (summary.physical_write_bytes_available) output << summary.physical_write_bytes;
  else output << "undefined";
  output << " |\n";
  output << "| WAL durable bytes | " << summary.durable_write_bytes.wal
         << " |\n";
  output << "| DecisionLog durable bytes | "
         << summary.durable_write_bytes.decision_log << " |\n";
  output << "| SST flush durable bytes | "
         << summary.durable_write_bytes.sst_flush << " |\n";
  output << "| Compaction durable bytes | "
         << summary.durable_write_bytes.compaction << " |\n";
  output << "| Blob durable bytes | " << summary.durable_write_bytes.blob
         << " |\n";
  output << "| Manifest durable bytes | "
         << summary.durable_write_bytes.manifest << " |\n";
  output << "| Metrics artifact | "
         << (summary.metrics_artifact_present || !summary.metrics_json.empty()
                 ? "present" : "absent") << " |\n";
  output << "| Histograms artifact | "
         << (summary.histograms_artifact_present || !summary.histograms_json.empty()
                 ? "present" : "absent") << " |\n";
  output << "| Traces artifact | "
         << (summary.traces_artifact_present || !summary.traces_json.empty()
                 ? "present" : "absent") << " |\n";
  output << "| Explain profile artifact | "
         << (summary.explain_artifact_present || !summary.explain_json.empty()
                 ? "present" : "absent") << " |\n\n";
  output << "## Derived Metrics\n\n";
  output << "Ratios retain their original numerator and denominator; a zero "
            "denominator is reported as undefined.\n\n";
  output << "| Metric | Numerator / denominator |\n|---|---:|\n";
  output << "| Write amplification | "
         << RatioText(summary.derived_metrics.write_amplification) << " |\n";
  output << "| Read amplification | "
         << RatioText(summary.derived_metrics.read_amplification) << " |\n";
  output << "| Space amplification | "
         << RatioText(summary.derived_metrics.space_amplification) << " |\n";
  output << "| Index survival | "
         << RatioText(summary.derived_metrics.index_survival) << " |\n";
  output << "| Interval survival | "
         << RatioText(summary.derived_metrics.interval_survival) << " |\n";
  output << "| Blob materialization | "
         << RatioText(summary.derived_metrics.blob_materialization) << " |\n";
  output << "| Cache admission | "
         << RatioText(summary.derived_metrics.cache_admission) << " |\n";
  output << "| Maintenance share | "
         << RatioText(summary.derived_metrics.maintenance_share) << " |\n";
  output << "| Visible prefix lag | "
         << LagText(summary.derived_metrics.visible_prefix_lag) << " |\n\n";
  output << "## Transaction Measurements\n\n";
  if (!summary.transaction_measurements.available) {
    output << "Transaction measurements: unknown ("
           << summary.transaction_measurements.availability_reason << ")\n\n";
  } else {
    const TransactionMeasurementWindow& measurements =
        summary.transaction_measurements;
    output << "| Field | Value |\n|---|---:|\n";
    output << "| Started | " << measurements.started << " |\n";
    output << "| Committed | " << measurements.committed << " |\n";
    output << "| Aborted | " << measurements.aborted << " |\n";
    output << "| Indeterminate | " << measurements.indeterminate << " |\n";
    output << "| Conflict abort rate | "
           << TransactionRatioText(measurements.conflict_abort_rate) << " |\n";
    output << "| PREPARE latency | "
           << TransactionDistributionText(measurements.prepare_latency) << " |\n";
    output << "| Decision latency | "
           << TransactionDistributionText(measurements.decision_latency) << " |\n";
    output << "| DecisionLog fsync latency | "
           << TransactionDistributionText(measurements.decision_fsync_latency) << " |\n";
    output << "| Visible-prefix wait | "
           << TransactionDistributionText(
                  measurements.visible_prefix_wait_success) << " |\n";
    output << "| Visible-prefix maximum lag | ";
    if (measurements.visible_prefix_max_lag_defined) {
      output << measurements.visible_prefix_max_lag_seq;
    } else {
      output << "unknown";
    }
    output << " |\n\n";
  }
  if (verified && !latencies.empty()) {
    output << "## Throughput\n\n";
    output << "Measured throughput: " << std::fixed << std::setprecision(4)
           << summary.measurement_throughput << " logical units/sec\n\n";
    output << "## Client-visible Latency\n\n";
    output << "All values include requested-arrival-to-completion time.\n\n";
    output << "| p50 ns | p95 ns | p99 ns |\n|---:|---:|---:|\n| "
           << summary.latency_p50_ns << " | " << summary.latency_p95_ns
           << " | " << summary.latency_p99_ns << " |\n\n";
    output << "p99.9 ns: " << summary.latency_p999_ns << "\n\n";
  } else {
    output << "No headline performance result is emitted because verification did not pass "
              "or no measured samples were retained.\n\n";
  }
  output << "## Verification\n\n";
  output << "| Load | Result | Reopen | Protocol |\n|---|---|---|---|\n| "
         << (verification.load_passed ? "PASS" : "FAIL") << " | "
         << (verification.result_passed ? "PASS" : "FAIL") << " | "
         << (verification.reopen_passed ? "PASS" : "FAIL") << " | "
         << (protocol_complete ? "COMPLETE" : "INCOMPLETE") << " |\n\n";
  output << "Verification detail: " << verification.detail << "\n";
  return output.str();
}

}  // namespace cedar
