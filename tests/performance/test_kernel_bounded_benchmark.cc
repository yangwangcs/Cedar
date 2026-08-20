#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <future>
#include <mutex>
#include <string>
#include <thread>

#include <unistd.h>

#include "benchmarks/cedar_kernel_bench_workload.h"
#include "benchmarks/cedar_kernel_bench_options.h"
#include "cedar/database.h"

namespace cedar::benchmark {
namespace {

TEST(KernelBoundedBenchmarkTest, BenchmarkAlwaysUsesCedarKernelMode) {
  KernelBenchmarkOptions options;
  options.path = "/tmp/cedar-kernel-profile";
  const DatabaseOptions database_options = MakeBenchmarkDatabaseOptions(options);
  EXPECT_EQ(database_options.storage_profile, StorageProfile::kProductionAppend);
  EXPECT_TRUE(database_options.production.kernel_mode);
}

TEST(KernelBoundedBenchmarkTest, BenchmarkSampleCarriesAppendStageMetrics) {
  KernelBenchmarkSample sample;
  sample.commit_pipeline.latency.queue.buckets[0] = 11;
  sample.commit_pipeline.latency.wal_sync.total_us = 29;
  sample.commit_pipeline.latency.publication.max_us = 47;
  EXPECT_EQ(sample.commit_pipeline.latency.queue.buckets[0], 11U);
  EXPECT_EQ(sample.commit_pipeline.latency.wal_sync.total_us, 29U);
  EXPECT_EQ(sample.commit_pipeline.latency.publication.max_us, 47U);
}

TEST(KernelBoundedBenchmarkTest, ConcurrentWritersExerciseNPlusOne) {
  char pattern[] = "/tmp/cedar_bounded_benchmark_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  DatabaseOptions options;
  options.path = path;
  options.storage_profile = StorageProfile::kProductionAppend;
  options.production.memory_budget_bytes = 1ULL * 1024ULL * 1024ULL * 1024ULL;
  options.production.kernel_mode = true;
  // This test exercises the commit pipeline, not disk-pressure admission.
  options.runtime_pressure_override_for_testing = [](PressureSample* sample) {
    sample->free_disk_bytes = UINT64_MAX;
    sample->free_disk_percent = 100;
  };
  // A single-request first epoch leaves the queued independent writes for the
  // N+1 preflight; the regular production batch policy would group them first.
  options.group_commit_max_batch_size = 1;
  options.group_commit_window_us = 0;
  std::mutex gate_mutex;
  std::condition_variable gate_cv;
  bool first_prewrite_entered = false;
  bool release_first_prewrite = false;
  uint32_t enqueued = 0;
  options.commit_prewrite_fault_injector_for_testing = [&] {
    std::unique_lock<std::mutex> lock(gate_mutex);
    if (!first_prewrite_entered) {
      first_prewrite_entered = true;
      gate_cv.notify_all();
      gate_cv.wait(lock, [&] { return release_first_prewrite; });
    }
    return Status::OK();
  };
  options.append_commit_enqueued_observer_for_testing = [&] {
    std::lock_guard<std::mutex> lock(gate_mutex);
    ++enqueued;
    gate_cv.notify_all();
  };
  auto opened = Database::Open(options);
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();

  auto writers = std::async(std::launch::async, [&] {
    return RunBoundedWriters(database.get(), 32, 1);
  });
  bool saw_first_prewrite = false;
  bool saw_successor_enqueue = false;
  {
    std::unique_lock<std::mutex> lock(gate_mutex);
    saw_first_prewrite = gate_cv.wait_for(
        lock, std::chrono::seconds(2), [&] { return first_prewrite_entered; });
    saw_successor_enqueue = gate_cv.wait_for(
        lock, std::chrono::seconds(2), [&] { return enqueued >= 2; });
    release_first_prewrite = true;
  }
  gate_cv.notify_all();
  const auto result = writers.get();
  EXPECT_TRUE(saw_first_prewrite);
  EXPECT_TRUE(saw_successor_enqueue);
  EXPECT_TRUE(result.status.ok()) << result.status.ToString();
  EXPECT_GT(result.committed, 0U);
  EXPECT_EQ(result.failures, 0U);
  const auto metrics = database->GetCommitPipelineMetrics();
  EXPECT_GT(metrics.n_plus_one_eligible_epochs, 0U);
  EXPECT_GT(metrics.n_plus_one_promoted_epochs, 0U);
  EXPECT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

}  // namespace
}  // namespace cedar::benchmark
