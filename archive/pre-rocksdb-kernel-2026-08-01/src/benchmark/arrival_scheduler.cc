// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/benchmark/arrival_scheduler.h"

#include <chrono>
#include <condition_variable>
#include <deque>
#include <limits>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

namespace cedar {
namespace {

using Clock = std::chrono::steady_clock;

uint64_t ElapsedNs(Clock::time_point origin) {
  const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
      Clock::now() - origin).count();
  return elapsed <= 0 ? 0 : static_cast<uint64_t>(elapsed);
}

Status ValidateConfig(const OpenLoopScheduleConfig& config,
                      const BenchmarkOperation& operation) {
  if (!operation) {
    return Status::InvalidArgument("benchmark arrival scheduler",
                                   "operation callback is required");
  }
  if (config.worker_count == 0 || config.queue_capacity == 0) {
    return Status::InvalidArgument("benchmark arrival scheduler",
                                   "worker and queue capacity must be positive");
  }
  if (config.operation_count > 1 && config.arrival_interval_ns != 0 &&
      config.operation_count - 1 >
          (std::numeric_limits<uint64_t>::max() - config.start_delay_ns) /
              config.arrival_interval_ns) {
    return Status::InvalidArgument("benchmark arrival scheduler",
                                   "arrival schedule overflows nanoseconds");
  }
  return Status::OK();
}

}  // namespace

StatusOr<OpenLoopScheduleResult> RunOpenLoopSchedule(
    const OpenLoopScheduleConfig& config, const BenchmarkOperation& operation) {
  const Status validated = ValidateConfig(config, operation);
  if (!validated.ok()) return validated;

  OpenLoopScheduleResult result;
  result.samples.resize(config.operation_count);
  if (config.operation_count == 0) return result;

  std::mutex mutex;
  std::condition_variable not_empty;
  std::condition_variable not_full;
  std::deque<uint64_t> queue;
  bool scheduling_complete = false;
  const Clock::time_point origin = Clock::now();

  std::vector<std::thread> workers;
  workers.reserve(config.worker_count);
  for (uint32_t worker = 0; worker < config.worker_count; ++worker) {
    workers.emplace_back([&] {
      for (;;) {
        uint64_t operation_id = 0;
        {
          std::unique_lock<std::mutex> lock(mutex);
          not_empty.wait(lock, [&] {
            return scheduling_complete || !queue.empty();
          });
          if (queue.empty()) return;
          operation_id = queue.front();
          queue.pop_front();
          not_full.notify_one();
        }
        BenchmarkOperationSample& sample = result.samples[operation_id];
        sample.started_ns = ElapsedNs(origin);
        Status operation_status = Status::OK();
        try {
          operation_status = operation(operation_id);
        } catch (const std::exception& error) {
          operation_status = Status::Corruption(
              "benchmark arrival scheduler", error.what());
        } catch (...) {
          operation_status = Status::Corruption(
              "benchmark arrival scheduler", "operation threw an unknown exception");
        }
        sample.completed_ns = ElapsedNs(origin);
        sample.terminal_status =
            operation_status.ok() ? "PASS" : operation_status.ToString();
        if (!operation_status.ok()) {
          std::lock_guard<std::mutex> lock(mutex);
          if (result.terminal_status.ok()) {
            result.terminal_status = std::move(operation_status);
          }
        }
      }
    });
  }

  for (uint64_t operation_id = 0;
       operation_id < config.operation_count; ++operation_id) {
    const uint64_t requested = config.start_delay_ns +
        operation_id * config.arrival_interval_ns;
    BenchmarkOperationSample& sample = result.samples[operation_id];
    sample.requested_arrival_ns = requested;
    std::this_thread::sleep_until(
        origin + std::chrono::nanoseconds(requested));
    {
      std::unique_lock<std::mutex> lock(mutex);
      not_full.wait(lock, [&] {
        return queue.size() < config.queue_capacity;
      });
      sample.admitted_ns = ElapsedNs(origin);
      queue.push_back(operation_id);
    }
    not_empty.notify_one();
  }
  {
    std::lock_guard<std::mutex> lock(mutex);
    scheduling_complete = true;
  }
  not_empty.notify_all();
  for (std::thread& worker : workers) worker.join();
  result.elapsed_ns = ElapsedNs(origin);
  return result;
}

}  // namespace cedar
