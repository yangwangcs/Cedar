#include "benchmarks/cedar_kernel_bench_workload.h"

#include <atomic>
#include <barrier>
#include <chrono>
#include <mutex>
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

}  // namespace

DatabaseOptions MakeBenchmarkDatabaseOptions(const KernelBenchmarkOptions& options) {
  DatabaseOptions database_options;
  database_options.path = options.path;
  database_options.storage_profile = StorageProfile::kProductionAppend;
  database_options.production.memory_budget_bytes = 1ULL * 1024ULL * 1024ULL * 1024ULL;
  database_options.production.kernel_mode = true;
  return database_options;
}

BoundedWriterResult RunBoundedWriters(Database* database, uint32_t clients,
                                      uint64_t duration_seconds) {
  BoundedWriterResult result;
  if (database == nullptr) {
    result.status = Status::InvalidArgument("bounded writers", "database is null");
    return result;
  }
  if (clients == 0 || clients > 32 || duration_seconds == 0) {
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
      std::vector<CommitHandle> pending;
      pending.reserve(2);
      const auto wait_one = [&] {
        StatusOr<CommitResult> completed = pending.front().Wait();
        pending.erase(pending.begin());
        if (!completed.ok()) return completed.status();
        if (completed.ValueOrDie().outcome != CommitOutcome::kCommitted) {
          return completed.ValueOrDie().status;
        }
        committed.fetch_add(1, std::memory_order_relaxed);
        return Status::OK();
      };
      while (std::chrono::steady_clock::now() < deadline) {
        attempted.fetch_add(1, std::memory_order_relaxed);
        auto handle = WriteVertexAsync(database, next_id.fetch_add(1));
        if (!handle.ok()) {
          failures.fetch_add(1, std::memory_order_relaxed);
          std::lock_guard<std::mutex> lock(status_mutex);
          if (first_failure.ok()) first_failure = handle.status();
          continue;
        }
        pending.push_back(std::move(handle).ConsumeValueOrDie());
        if (pending.size() == 2) {
          const Status status = wait_one();
          if (!status.ok()) {
            failures.fetch_add(1, std::memory_order_relaxed);
            std::lock_guard<std::mutex> lock(status_mutex);
            if (first_failure.ok()) first_failure = status;
          }
        }
      }
      while (!pending.empty()) {
        const Status status = wait_one();
        if (!status.ok()) {
          failures.fetch_add(1, std::memory_order_relaxed);
          std::lock_guard<std::mutex> lock(status_mutex);
          if (first_failure.ok()) first_failure = status;
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

}  // namespace cedar::benchmark
