#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <atomic>
#include <string>

#include "query/resource/query_resource_pool.h"
#include "query/resource/query_scratch.h"

namespace cedar::internal {
namespace {

QueryResourcePoolOptions OptionsWithMemory(uint64_t bytes) {
  QueryResourcePoolOptions options;
  options.memory_bytes = bytes;
  options.scratch_bytes = bytes;
  options.read_bytes = bytes;
  options.prefetch_bytes = bytes;
  options.decoded_rows = bytes;
  options.output_rows = bytes;
  options.output_bytes = bytes;
  options.interval_fragments = bytes;
  options.graph_labels = bytes;
  options.visited_vertices = bytes;
  options.cpu_us = bytes;
  options.max_parallelism = 1;
  return options;
}

QueryBudget InteractiveBudget(uint64_t memory) {
  QueryBudget budget;
  budget.memory_bytes = memory;
  budget.scratch_bytes = memory;
  budget.read_bytes = memory;
  budget.prefetch_bytes = memory;
  budget.decoded_rows = memory;
  budget.output_rows = memory;
  budget.output_bytes = memory;
  budget.interval_fragments = memory;
  budget.graph_labels = memory;
  budget.visited_vertices = memory;
  budget.cpu_us = memory;
  budget.max_parallelism = 1;
  return budget;
}

TEST(QueryResourcePoolTest, NeverAllocatesBeyondReservation) {
  QueryResourcePool pool(OptionsWithMemory(1024));
  auto query = pool.Admit(InteractiveBudget(768));
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  EXPECT_TRUE(query.ValueOrDie().ReserveMemory(512).ok());
  EXPECT_TRUE(query.ValueOrDie().ReserveMemory(513).IsResourceExhausted());
}

TEST(QueryResourcePoolTest, OverflowIsResourceExhausted) {
  QueryResourcePool pool(OptionsWithMemory(UINT64_MAX));
  auto query = pool.Admit(InteractiveBudget(UINT64_MAX));
  ASSERT_TRUE(query.ok());
  EXPECT_TRUE(query.ValueOrDie().ReserveMemory(UINT64_MAX).ok());
  EXPECT_TRUE(query.ValueOrDie().ReserveMemory(1).IsResourceExhausted());
}

TEST(QueryResourcePoolTest, PoolAdmissionReleasesOnReservationDestruction) {
  QueryResourcePool pool(OptionsWithMemory(1024));
  auto first = pool.Admit(InteractiveBudget(1024));
  ASSERT_TRUE(first.ok());
  auto second = pool.Admit(InteractiveBudget(1));
  EXPECT_TRUE(second.status().IsResourceExhausted());
  first = StatusOr<QueryReservation>(QueryReservation(1));
  auto third = pool.Admit(InteractiveBudget(1));
  EXPECT_TRUE(third.ok());
}

TEST(QueryResourcePoolTest, AdmissionAggregatesEveryBudgetDimension) {
  auto options = OptionsWithMemory(100);
  options.max_parallelism = 4;
  options.reserved_interactive_workers = 1;
  QueryResourcePool pool(options);
  auto first = pool.Admit(InteractiveBudget(60));
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  auto second = pool.Admit(InteractiveBudget(41));
  ASSERT_FALSE(second.ok());
  EXPECT_NE(second.status().ToString().find("memory_bytes"), std::string::npos);
  first = StatusOr<QueryReservation>(QueryReservation(1));
  EXPECT_TRUE(pool.Admit(InteractiveBudget(100)).ok());
}

TEST(QueryResourcePoolTest, AnalyticalAdmissionLeavesReservedInteractiveWorkers) {
  auto options = OptionsWithMemory(1024);
  options.max_parallelism = 4;
  options.reserved_interactive_workers = 1;
  QueryResourcePool pool(options);
  auto analytical = InteractiveBudget(128);
  analytical.max_parallelism = 4;
  EXPECT_TRUE(pool.Admit(analytical, QueryExecutionMode::kAnalytical)
                  .status()
                  .IsResourceExhausted());
  analytical.max_parallelism = 3;
  EXPECT_TRUE(pool.Admit(analytical, QueryExecutionMode::kAnalytical).ok());
}

TEST(QueryResourcePoolTest, WalCriticalBlocksAnalyticalIo) {
  std::atomic<bool> critical{true};
  auto options = OptionsWithMemory(1024);
  options.read_bytes = 1024;
  options.wal_sync_critical = &critical;
  QueryResourcePool pool(options);
  auto permit = pool.AcquireIo(QueryWorkClass::kAnalytical, 1);
  EXPECT_TRUE(permit.status().IsResourceExhausted());
  critical.store(false);
  permit = pool.AcquireIo(QueryWorkClass::kAnalytical, 1);
  EXPECT_TRUE(permit.ok());
}

TEST(QueryScratchTest, WritesAndReadsVerifiedRun) {
  const auto root = std::filesystem::temp_directory_path() / "cedar-task11-scratch";
  std::filesystem::remove_all(root);
  QueryScratch scratch(root, "instance", "query", 1024);
  auto run = scratch.WriteRun("run-0", "payload");
  ASSERT_TRUE(run.ok()) << run.status().ToString();
  auto read = scratch.ReadRun(run.ValueOrDie());
  ASSERT_TRUE(read.ok()) << read.status().ToString();
  EXPECT_EQ(read.ValueOrDie(), "payload");
  EXPECT_TRUE(scratch.Cleanup().ok());
  EXPECT_FALSE(std::filesystem::exists(scratch.query_directory()));
}

TEST(QueryScratchTest, RejectsPathEscape) {
  const auto root = std::filesystem::temp_directory_path() / "cedar-task11-scratch-escape";
  std::filesystem::remove_all(root);
  QueryScratch scratch(root, "instance", "query", 1024);
  EXPECT_TRUE(scratch.WriteRun("../escape", "payload").status().IsInvalidArgument());
  EXPECT_TRUE(scratch.Cleanup().ok());
}

TEST(QueryScratchTest, RejectsOverwriteAndDetectsCorruption) {
  const auto root = std::filesystem::temp_directory_path() / "cedar-task11-scratch-corrupt";
  std::filesystem::remove_all(root);
  QueryScratch scratch(root, "instance", "query", 1024);
  auto run = scratch.WriteRun("run-0", "payload");
  ASSERT_TRUE(run.ok());
  EXPECT_TRUE(scratch.WriteRun("run-0", "other").status().IsInvalidArgument());
  std::fstream file(run.ValueOrDie(), std::ios::in | std::ios::out | std::ios::binary);
  file.seekp(-1, std::ios::end);
  char byte = 0;
  file.write(&byte, 1);
  file.close();
  EXPECT_TRUE(scratch.ReadRun(run.ValueOrDie()).status().IsCorruption());
  EXPECT_TRUE(scratch.Cleanup().ok());
}

}  // namespace
}  // namespace cedar::internal
