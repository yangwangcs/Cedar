// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_BENCHMARK_ARRIVAL_SCHEDULER_H_
#define CEDAR_BENCHMARK_ARRIVAL_SCHEDULER_H_

#include <cstdint>
#include <functional>
#include <vector>

#include "cedar/benchmark/artifact_writer.h"
#include "cedar/core/status.h"

namespace cedar {

struct OpenLoopScheduleConfig {
  uint64_t operation_count = 0;
  uint64_t arrival_interval_ns = 0;
  uint64_t start_delay_ns = 0;
  uint32_t worker_count = 1;
  uint32_t queue_capacity = 1;
};

struct OpenLoopScheduleResult {
  std::vector<BenchmarkOperationSample> samples;
  uint64_t elapsed_ns = 0;
  Status terminal_status = Status::OK();
};

using BenchmarkOperation = std::function<Status(uint64_t operation_id)>;

StatusOr<OpenLoopScheduleResult> RunOpenLoopSchedule(
    const OpenLoopScheduleConfig& config, const BenchmarkOperation& operation);

}  // namespace cedar

#endif  // CEDAR_BENCHMARK_ARRIVAL_SCHEDULER_H_
