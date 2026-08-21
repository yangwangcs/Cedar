#include <gtest/gtest.h>

#include <array>
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

TEST(QueryScratchTest, EnforcesReadAndWriteRateWindows) {
  const auto root = std::filesystem::temp_directory_path() /
                    "cedar-task11-scratch-rate";
  std::filesystem::remove_all(root);
  QueryScratch scratch(root, "instance", "query", 1024);
  scratch.SetRateLimits(4, 4);
  EXPECT_TRUE(scratch.WriteRun("too-large", "12345").status().IsResourceExhausted());
  scratch.SetRateLimits(4, 0);
  auto run = scratch.WriteRun("run-0", "12345");
  ASSERT_TRUE(run.ok()) << run.status().ToString();
  EXPECT_TRUE(scratch.ReadRun(run.ValueOrDie()).status().IsResourceExhausted());
  EXPECT_TRUE(scratch.Cleanup().ok());
}

TEST(QueryScratchTest, ChecksCancellationAtRunBoundaries) {
  const auto root = std::filesystem::temp_directory_path() /
                    "cedar-task11-scratch-abort";
  std::filesystem::remove_all(root);
  QueryScratch scratch(root, "instance", "query", 1024);
  std::atomic<bool> cancelled{true};
  scratch.SetAbortCheck([&cancelled] {
    return cancelled.load() ? Status::QueryCancelled("query", "cancelled")
                             : Status::OK();
  });
  EXPECT_TRUE(scratch.WriteRun("run-0", "payload").status().IsQueryCancelled());
  cancelled.store(false);
  auto run = scratch.WriteRun("run-0", "payload");
  ASSERT_TRUE(run.ok()) << run.status().ToString();
  cancelled.store(true);
  EXPECT_TRUE(scratch.ReadRun(run.ValueOrDie()).status().IsQueryCancelled());
  EXPECT_TRUE(scratch.Cleanup().ok());
}

TEST(QueryScratchTest, AcquiresAnalyticalIoAtRunBoundaries) {
  const auto root = std::filesystem::temp_directory_path() /
                    "cedar-task11-scratch-io-permit";
  std::filesystem::remove_all(root);
  std::atomic<bool> critical{true};
  auto options = OptionsWithMemory(4096);
  options.wal_sync_critical = &critical;
  QueryResourcePool pool(options);
  QueryScratch scratch(root, "instance", "query", 4096);
  scratch.SetIoAdmission([&pool](uint64_t bytes)
                             -> StatusOr<std::shared_ptr<IoPermit>> {
    auto permit = pool.AcquireIo(QueryWorkClass::kAnalytical, bytes);
    if (!permit.ok()) return permit.status();
    return std::make_shared<IoPermit>(std::move(permit).ConsumeValueOrDie());
  });
  EXPECT_TRUE(scratch.WriteRun("run-0", "payload").status().IsResourceExhausted());
  critical.store(false);
  auto run = scratch.WriteRun("run-0", "payload");
  ASSERT_TRUE(run.ok()) << run.status().ToString();
  critical.store(true);
  EXPECT_TRUE(scratch.ReadRun(run.ValueOrDie()).status().IsResourceExhausted());
  EXPECT_TRUE(scratch.Cleanup().ok());
}

TEST(QueryScratchTest, ReservesReadBudgetBeforePayloadAllocation) {
  const auto root = std::filesystem::temp_directory_path() /
                    "cedar-task11-scratch-read-reservation";
  std::filesystem::remove_all(root);
  QueryScratch writer(root, "instance", "writer", 4096);
  auto run = writer.WriteRun("run-0", "payload");
  ASSERT_TRUE(run.ok()) << run.status().ToString();

  std::array<uint64_t, static_cast<size_t>(ResourceDimension::kCount)> limits;
  limits.fill(UINT64_MAX);
  limits[static_cast<size_t>(ResourceDimension::kReadBytes)] = 1;
  QueryReservation reservation(limits);
  QueryScratch reader(root, "instance", "writer", 4096, &reservation);
  EXPECT_TRUE(reader.ReadRun(run.ValueOrDie()).status().IsResourceExhausted());
  EXPECT_EQ(reservation.used(ResourceDimension::kReadBytes), 0U);
  EXPECT_TRUE(writer.Cleanup().ok());
}

TEST(QueryScratchTest, RejectsPathEscape) {
  const auto root = std::filesystem::temp_directory_path() / "cedar-task11-scratch-escape";
  std::filesystem::remove_all(root);
  QueryScratch scratch(root, "instance", "query", 1024);
  EXPECT_TRUE(scratch.WriteRun("../escape", "payload").status().IsInvalidArgument());
  EXPECT_TRUE(scratch.Cleanup().ok());
}

TEST(QueryScratchTest, FailedDirectoryCreationReleasesFreeSpaceAdmission) {
  const auto root = std::filesystem::temp_directory_path() /
                    "cedar-task11-scratch-admission-file";
  std::filesystem::remove_all(root);
  {
    std::ofstream blocker(root);
    blocker << "not a directory";
  }
  std::error_code ec;
  const uint64_t available = std::filesystem::space(root, ec).available;
  ASSERT_FALSE(ec);
  ASSERT_GT(available, 1U);
  const uint64_t reserve = available - 1024;

  QueryScratch first(root, "instance", "first", 1024, reserve);
  EXPECT_TRUE(first.WriteRun("run-0", "payload").status().IsIOError());
  EXPECT_TRUE(first.Cleanup().ok());

  // This succeeds only if the failed first write returned its process-wide
  // free-space admission even though no query directory was created.
  QueryScratch second(root, "instance", "second", 1024, reserve);
  EXPECT_TRUE(second.WriteRun("run-0", "payload").status().IsIOError());
  EXPECT_TRUE(second.Cleanup().ok());
  std::filesystem::remove(root);
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
  std::array<uint64_t, static_cast<size_t>(ResourceDimension::kCount)> limits;
  limits.fill(UINT64_MAX);
  QueryReservation reservation(limits);
  QueryScratch reader(root, "instance", "query", 1024, &reservation);
  EXPECT_TRUE(reader.ReadRun(run.ValueOrDie()).status().IsCorruption());
  EXPECT_EQ(reservation.used(ResourceDimension::kReadBytes), 0U);
  EXPECT_TRUE(scratch.Cleanup().ok());
}

}  // namespace
}  // namespace cedar::internal
