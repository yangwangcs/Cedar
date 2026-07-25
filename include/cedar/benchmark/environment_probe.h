// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_BENCHMARK_ENVIRONMENT_PROBE_H_
#define CEDAR_BENCHMARK_ENVIRONMENT_PROBE_H_

#include <cstdint>
#include <string>

namespace cedar {

struct BenchmarkEnvironment {
  std::string os_kernel;
  std::string cpu_model_and_count;
  std::string compiler_and_flags;
  uint32_t logical_cpu_count = 0;
  uint64_t memory_limit_bytes = 0;
  uint64_t storage_free_bytes = 0;
  std::string storage_device_and_filesystem;
  bool resource_limit_provenance_complete = false;
  bool storage_provenance_complete = false;
};

// Reads process environment only. It never opens or mutates the benchmark DB.
BenchmarkEnvironment ProbeBenchmarkEnvironment(
    const std::string& storage_path = ".");

bool LinuxCgroupAncestryProvenanceCompleteForTesting(
    const std::string& hierarchy_root, uint64_t namespace_inode);

}  // namespace cedar

#endif  // CEDAR_BENCHMARK_ENVIRONMENT_PROBE_H_
