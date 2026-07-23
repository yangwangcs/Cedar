// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_BENCHMARK_ENVIRONMENT_PROBE_H_
#define CEDAR_BENCHMARK_ENVIRONMENT_PROBE_H_

#include <string>

namespace cedar {

struct BenchmarkEnvironment {
  std::string os_kernel;
  std::string cpu_model_and_count;
  std::string compiler_and_flags;
};

// Reads process environment only. It never opens or mutates the benchmark DB.
BenchmarkEnvironment ProbeBenchmarkEnvironment();

}  // namespace cedar

#endif  // CEDAR_BENCHMARK_ENVIRONMENT_PROBE_H_
