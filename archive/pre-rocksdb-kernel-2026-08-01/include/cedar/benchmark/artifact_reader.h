// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_BENCHMARK_ARTIFACT_READER_H_
#define CEDAR_BENCHMARK_ARTIFACT_READER_H_

#include <memory>
#include <optional>
#include <vector>
#include <string>
#include <string_view>

#include "cedar/benchmark/artifact_writer.h"
#include "cedar/benchmark/profile.h"
#include "cedar/core/status.h"
#include "cedar/observability/metric_registry.h"

namespace cedar {

struct BenchmarkArtifactRecord {
  BenchmarkRunManifest manifest;
  BenchmarkArtifactSummary summary;
  BenchmarkVerification verification;
  bool protocol_complete = false;
  std::string run_id;
};

struct BenchmarkExecutableSnapshotState;

struct BenchmarkExecutableSnapshot {
  std::string path;
  std::string blake3;
  std::string sha256;
  std::shared_ptr<BenchmarkExecutableSnapshotState> state;

  int fd() const;
};

// Reads and validates the archived manifest, summary, and verification files
// without opening Cedar. The record is suitable for release regression tools;
// incomplete or provenance-inconsistent artifacts are rejected.
StatusOr<BenchmarkArtifactRecord> ReadBenchmarkArtifact(
    const std::string& run_directory);

// Rebuilds report.md solely from the archived manifest.json, summary.json,
// and verification.json in run_directory. The archived schemas and manifest
// run ID are validated before report.md is atomically replaced.
Status RegenerateBenchmarkReport(const std::string& run_directory);

// Strictly validates one paired benchmark root, its passing release gate,
// complete paired-run ledger, and every referenced child artifact.
Status VerifyPairedBenchmarkOutput(const std::string& output_root,
                                   uint32_t expected_pair_count,
                                   const std::string& expected_profile = {},
                                   const std::string& expected_workload = {},
                                   const std::string& expected_cache_mode = {},
                                   const std::string& expected_baseline_blake3 = {},
                                   const std::string& expected_candidate_blake3 = {},
                                   const std::string& expected_baseline_sha256 = {},
                                   const std::string& expected_candidate_sha256 = {},
                                   std::optional<uint64_t> expected_seed = std::nullopt,
                                   std::optional<uint64_t> expected_order_seed = std::nullopt);

Status ValidatePairedCampaignSeedIdentity(
    uint32_t paired_schema_version, uint64_t ledger_generator_seed,
    uint64_t ledger_order_seed, uint64_t artifact_generator_seed,
    uint64_t expected_generator_seed, uint64_t expected_order_seed);

// Validates the current clean-break release-evidence manifest structure.
// File bytes remain bound by the accompanying SHA256SUMS verifier.
Status ValidateReleaseEvidenceManifest(std::string_view manifest_json);

// Validates the release-evidence manifest, every SHA256SUMS entry, and the
// manifest's binary/log hash bindings within an evidence directory.
Status VerifyReleaseEvidenceDirectory(const std::string& evidence_directory);

Status VerifySha256LedgerDirectory(
    const std::string& directory,
    const std::vector<std::string>& expected_relative_paths);

// Computes the same lowercase SHA-256 digest used by the release-evidence
// directory verifier. This is used to bind approved production binaries
// without conflating the benchmark manifest's existing BLAKE3 identity.
StatusOr<std::string> BenchmarkFileSha256(const std::string& path);

StatusOr<BenchmarkExecutableSnapshot> CreateBenchmarkExecutableSnapshot(
    const std::string& path);

Status VerifyBenchmarkExecutableSnapshot(
    const BenchmarkExecutableSnapshot& snapshot);

// Executes the immutable snapshot's read-only provenance probe and parses its
// exact source, instrumentation, and database-format identity.
StatusOr<BenchmarkBinaryProvenance> ReadBenchmarkBinaryProvenance(
    const BenchmarkExecutableSnapshot& snapshot);

// Validates an exported production metric snapshot against the versioned
// schema and workload-specific minimum activity requirements.
Status ValidateProductionMetricArtifact(
    std::string_view metrics_json,
    const std::vector<MetricActivityRequirement>& activity_requirements);

// Validates the archived metrics.json belonging to a benchmark artifact.
Status ValidateBenchmarkArtifactProductionMetrics(
    const std::string& run_directory,
    const std::vector<MetricActivityRequirement>& activity_requirements);

}  // namespace cedar

#endif  // CEDAR_BENCHMARK_ARTIFACT_READER_H_
