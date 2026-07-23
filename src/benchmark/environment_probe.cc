// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/benchmark/environment_probe.h"

#include <thread>
#include <sys/utsname.h>

namespace cedar {

BenchmarkEnvironment ProbeBenchmarkEnvironment() {
  BenchmarkEnvironment environment;
  struct utsname information {};
  if (::uname(&information) == 0) {
    environment.os_kernel = std::string(information.sysname) + " " + information.release +
        " " + information.machine;
  } else {
    environment.os_kernel = "unknown";
  }
  const unsigned int threads = std::thread::hardware_concurrency();
  environment.cpu_model_and_count = "logical_cpus=" +
      std::to_string(threads == 0 ? 1 : threads);
#if defined(__clang__)
  environment.compiler_and_flags = std::string("clang ") + __clang_version__;
#elif defined(__GNUC__)
  environment.compiler_and_flags = std::string("gcc ") + __VERSION__;
#else
  environment.compiler_and_flags = "unknown compiler";
#endif
  return environment;
}

}  // namespace cedar
