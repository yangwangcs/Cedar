#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <string>
#include <thread>

#include <unistd.h>

#include "benchmarks/cedar_kernel_bench_workload.h"
#include "benchmarks/cedar_kernel_bench_options.h"
#include "cedar/database.h"

namespace cedar::benchmark {
namespace {

TEST(KernelBoundedBenchmarkTest, LeanUsesProductionStorageWithoutKernelMode) {
  KernelBenchmarkOptions options;
  options.path = "/tmp/cedar-lean-profile";
  options.execution_profile = BenchmarkExecutionProfile::kLean;
  const DatabaseOptions database_options = MakeBenchmarkDatabaseOptions(options);
  EXPECT_EQ(database_options.storage_profile, StorageProfile::kProductionAppend);
  EXPECT_FALSE(database_options.production.kernel_mode);
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
  std::atomic<bool> delay_first_prewrite{true};
  options.commit_prewrite_fault_injector_for_testing = [&] {
    if (delay_first_prewrite.exchange(false)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return Status::OK();
  };
  auto opened = Database::Open(options);
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();

  const auto result = RunBoundedWriters(database.get(), 32, 1);
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
