#include "benchmarks/cedar_kernel_bench_workload.h"

#include <atomic>
#include <barrier>
#include <chrono>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "cedar/database.h"

namespace cedar::benchmark {
namespace {

StatusOr<CommitHandle> WriteVertexAsync(Database* database, uint64_t id) {
  auto transaction = database->BeginTransaction();
  if (!transaction.ok()) return transaction.status();
  const Status asserted = transaction.ValueOrDie()->Assert(
      EntityFact::Vertex({PartId{1}, VertexId{id}}), ValidTime{1});
  if (!asserted.ok()) return asserted;
  return transaction.ValueOrDie()->CommitAsync();
}

bool IsStalePressureAdmission(const Status& status) {
  return status.IsResourceExhausted() &&
         status.ToString().find("runtime pressure snapshot is stale") !=
             std::string::npos;
}

}  // namespace

DatabaseOptions MakeBenchmarkDatabaseOptions(const KernelBenchmarkOptions& options) {
  DatabaseOptions database_options;
  database_options.path = options.path;
  database_options.storage_profile = StorageProfile::kProductionAppend;
  database_options.production.memory_budget_bytes = 1ULL * 1024ULL * 1024ULL * 1024ULL;
  database_options.production.kernel_mode = true;
  database_options.query_runtime.query_memory_bytes = 32ULL * 1024ULL * 1024ULL;
  database_options.query_runtime.projection_cache_bytes = 32ULL * 1024ULL * 1024ULL;
  database_options.query_runtime.query_delta_bytes = 32ULL * 1024ULL * 1024ULL;
  database_options.group_commit_max_batch_size = options.group_max_batch;
  database_options.group_commit_max_batch_bytes = options.group_max_bytes;
  database_options.group_commit_window_us = options.group_window_us;
  database_options.group_commit_max_queue_requests = options.group_queue_requests;
  return database_options;
}

BoundedWriterResult RunBoundedWriters(Database* database, uint32_t clients,
                                      uint64_t duration_seconds) {
  BoundedWriterResult result;
  if (database == nullptr) {
    result.status = Status::InvalidArgument("bounded writers", "database is null");
    return result;
  }
  if (clients == 0 || clients > 128 || duration_seconds == 0) {
    result.status = Status::InvalidArgument("bounded writers", "bounds are invalid");
    return result;
  }

  std::atomic<uint64_t> next_id{1};
  std::atomic<uint64_t> attempted{0};
  std::atomic<uint64_t> committed{0};
  std::atomic<uint64_t> failures{0};
  std::mutex status_mutex;
  Status first_failure = Status::OK();
  std::barrier start_line(static_cast<std::ptrdiff_t>(clients));
  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(duration_seconds);
  std::vector<std::jthread> workers;
  workers.reserve(clients);
  for (uint32_t index = 0; index < clients; ++index) {
    workers.emplace_back([&] {
      start_line.arrive_and_wait();
      while (std::chrono::steady_clock::now() < deadline) {
        attempted.fetch_add(1, std::memory_order_relaxed);
        auto handle = WriteVertexAsync(database, next_id.fetch_add(1));
        if (!handle.ok()) {
          failures.fetch_add(1, std::memory_order_relaxed);
          std::lock_guard<std::mutex> lock(status_mutex);
          if (first_failure.ok()) first_failure = handle.status();
          continue;
        }
        StatusOr<CommitResult> completed_result =
            std::move(handle).ConsumeValueOrDie().Wait();
        Status status = Status::OK();
        if (!completed_result.ok()) {
          status = completed_result.status();
        } else if (completed_result.ValueOrDie().outcome != CommitOutcome::kCommitted) {
          status = completed_result.ValueOrDie().status;
        }
        if (!status.ok()) {
          failures.fetch_add(1, std::memory_order_relaxed);
          std::lock_guard<std::mutex> lock(status_mutex);
          if (first_failure.ok()) first_failure = status;
        } else {
          committed.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }
  workers.clear();
  result.attempted = attempted.load(std::memory_order_relaxed);
  result.committed = committed.load(std::memory_order_relaxed);
  result.failures = failures.load(std::memory_order_relaxed);
  result.status = first_failure.ok()
                      ? Status::OK()
                      : Status::IOError("bounded writers", first_failure.ToString());
  return result;
}

BoundedWriterResult RunFixedWriters(Database* database, uint32_t clients,
                                    uint32_t commits_per_client) {
  BoundedWriterResult result;
  if (database == nullptr) {
    result.status = Status::InvalidArgument("fixed writers", "database is null");
    return result;
  }
  if (clients == 0 || clients > 128 || commits_per_client == 0) {
    result.status = Status::InvalidArgument("fixed writers", "bounds are invalid");
    return result;
  }

  std::atomic<uint64_t> next_id{1};
  std::atomic<uint64_t> attempted{0};
  std::atomic<uint64_t> committed{0};
  std::atomic<uint64_t> failures{0};
  std::mutex status_mutex;
  Status first_failure = Status::OK();
  std::barrier start_line(static_cast<std::ptrdiff_t>(clients));
  std::vector<std::jthread> workers;
  workers.reserve(clients);
  for (uint32_t index = 0; index < clients; ++index) {
    workers.emplace_back([&] {
    start_line.arrive_and_wait();
    for (uint32_t commit = 0; commit < commits_per_client; ++commit) {
        Status status;
        for (;;) {
          attempted.fetch_add(1, std::memory_order_relaxed);
          auto handle = WriteVertexAsync(database, next_id.fetch_add(1));
          status = handle.ok() ? Status::OK() : handle.status();
          if (status.ok()) {
            StatusOr<CommitResult> completed_result =
                std::move(handle).ConsumeValueOrDie().Wait();
            if (!completed_result.ok()) {
              status = completed_result.status();
            } else if (completed_result.ValueOrDie().outcome !=
                       CommitOutcome::kCommitted) {
              status = completed_result.ValueOrDie().status;
            }
          }
          // A sampler tick can race this intentionally high-concurrency
          // benchmark. The request has not entered the append path in this
          // case, so retry only this freshness admission result. Pressure,
          // queue, and durability failures remain hard benchmark failures.
          if (!IsStalePressureAdmission(status)) break;
          std::this_thread::yield();
        }
        if (!status.ok()) {
          failures.fetch_add(1, std::memory_order_relaxed);
          std::lock_guard<std::mutex> lock(status_mutex);
          if (first_failure.ok()) first_failure = status;
        } else {
          committed.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }
  workers.clear();
  result.attempted = attempted.load(std::memory_order_relaxed);
  result.committed = committed.load(std::memory_order_relaxed);
  result.failures = failures.load(std::memory_order_relaxed);
  result.status = first_failure.ok()
                      ? Status::OK()
                      : Status::IOError("fixed writers", first_failure.ToString());
  return result;
}

}  // namespace cedar::benchmark
