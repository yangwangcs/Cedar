// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_BENCHMARK_REPORT_BUILDER_H_
#define CEDAR_BENCHMARK_REPORT_BUILDER_H_

#include <string>

#include "cedar/benchmark/artifact_writer.h"
#include "cedar/benchmark/run_manifest.h"

namespace cedar {

// Rendering has no dependency on the engine and can be repeated from the
// structured manifest, summary, and verification records retained per run.
std::string BuildBenchmarkReport(const BenchmarkRunManifest& manifest,
                                 const BenchmarkArtifactSummary& summary,
                                 const BenchmarkVerification& verification,
                                 bool protocol_complete);

}  // namespace cedar

#endif  // CEDAR_BENCHMARK_REPORT_BUILDER_H_
