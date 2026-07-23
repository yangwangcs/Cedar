// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_BENCHMARK_ARTIFACT_READER_H_
#define CEDAR_BENCHMARK_ARTIFACT_READER_H_

#include <string>
#include <string_view>

#include "cedar/benchmark/artifact_writer.h"
#include "cedar/core/status.h"

namespace cedar {

struct BenchmarkArtifactRecord {
  BenchmarkRunManifest manifest;
  BenchmarkArtifactSummary summary;
  BenchmarkVerification verification;
  bool protocol_complete = false;
  std::string run_id;
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

// Validates the current clean-break release-evidence manifest structure.
// File bytes remain bound by the accompanying SHA256SUMS verifier.
Status ValidateReleaseEvidenceManifest(std::string_view manifest_json);

}  // namespace cedar

#endif  // CEDAR_BENCHMARK_ARTIFACT_READER_H_
