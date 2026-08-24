#include <gtest/gtest.h>

#include <algorithm>
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

TEST(KernelBoundedBenchmarkTest, BenchmarkPassesGroupAdmissionControlsToDatabase) {
  KernelBenchmarkOptions options;
  options.path = "/tmp/cedar-kernel-profile";
  options.group_max_batch = 256;
  options.group_max_bytes = 1ULL * 1024ULL * 1024ULL;
  options.group_window_us = 250;
  options.group_queue_requests = 2'048;

  const DatabaseOptions database_options = MakeBenchmarkDatabaseOptions(options);

  EXPECT_EQ(database_options.group_commit_max_batch_size, 256U);
  EXPECT_EQ(database_options.group_commit_max_batch_bytes, 1ULL * 1024ULL * 1024ULL);
  EXPECT_EQ(database_options.group_commit_window_us, 250U);
  EXPECT_EQ(database_options.group_commit_max_queue_requests, 2'048U);
}

TEST(KernelBoundedBenchmarkTest, BenchmarkSampleCarriesAppendStageMetrics) {
  KernelBenchmarkSample sample;
  sample.commit_pipeline.latency.queue.buckets[0] = 11;
  sample.commit_pipeline.latency.wal_sync.total_us = 29;
  sample.commit_pipeline.latency.publication.max_us = 47;
  sample.commit_pipeline.group_fill.groups = 5;
  sample.commit_pipeline.group_fill.total_transactions = 11;
  sample.commit_pipeline.group_fill.max_transactions = 4;
  sample.commit_pipeline.group_fill.buckets[GroupFillBucket(4)] = 3;
  EXPECT_EQ(sample.commit_pipeline.latency.queue.buckets[0], 11U);
  EXPECT_EQ(sample.commit_pipeline.latency.wal_sync.total_us, 29U);
  EXPECT_EQ(sample.commit_pipeline.latency.publication.max_us, 47U);
  EXPECT_EQ(sample.commit_pipeline.group_fill.groups, 5U);
  EXPECT_EQ(sample.commit_pipeline.group_fill.total_transactions, 11U);
  EXPECT_EQ(sample.commit_pipeline.group_fill.max_transactions, 4U);
  EXPECT_EQ(sample.commit_pipeline.group_fill.buckets[GroupFillBucket(4)], 3U);
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
  options.query_runtime.query_memory_bytes = 32ULL * 1024ULL * 1024ULL;
  options.query_runtime.projection_cache_bytes = 32ULL * 1024ULL * 1024ULL;
  options.query_runtime.query_delta_bytes = 32ULL * 1024ULL * 1024ULL;
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

TEST(KernelBoundedBenchmarkTest,
     KernelTestProfileCompletesNativeFlushesAndReopens) {
  char pattern[] = "/tmp/cedar_kernel_test_maintenance_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  DatabaseOptions options;
  options.path = path;
  options.storage_profile = StorageProfile::kKernelTest;
  options.query_runtime.query_memory_bytes = 128ULL * 1024ULL;
  options.query_runtime.projection_cache_bytes = 128ULL * 1024ULL;
  options.query_runtime.query_delta_bytes = 128ULL * 1024ULL;
  options.group_commit_max_batch_size = 64;
  options.group_commit_window_us = 200;
  options.runtime_pressure_override_for_testing = [](PressureSample* sample) {
    sample->free_disk_bytes = UINT64_MAX;
    sample->free_disk_percent = 100;
  };
  auto opened = Database::Open(options);
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();

  auto writers = std::async(std::launch::async, [&] {
    return RunBoundedWriters(database.get(), 32, 2);
  });
  bool saw_orphaned_high_pressure_debt = false;
  RuntimeMetrics orphaned_metrics;
  while (writers.wait_for(std::chrono::milliseconds(0)) !=
         std::future_status::ready) {
    const auto sampled = database->SampleRuntimeMetrics();
    ASSERT_TRUE(sampled.ok()) << sampled.status().ToString();
    const RuntimeMetrics& current = sampled.ValueOrDie();
    if (current.write_buffer_limit_bytes != 0 &&
        current.write_buffer_bytes * 100 >=
            current.write_buffer_limit_bytes * 85 &&
        current.immutable_fact_count != 0 && !current.facts_flush_pending &&
        current.flush_queue_depth == 0 && current.unscheduled_flushes == 0 &&
        current.scheduled_flushes == 0 && current.running_flushes == 0) {
      saw_orphaned_high_pressure_debt = true;
      orphaned_metrics = current;
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  const BoundedWriterResult writer_result = writers.get();
  EXPECT_GT(writer_result.committed, 0U);
  EXPECT_FALSE(saw_orphaned_high_pressure_debt)
      << "immutable_fact_count=" << orphaned_metrics.immutable_fact_count
      << " immutable_fact_bytes=" << orphaned_metrics.immutable_fact_bytes
      << " active_fact_bytes=" << orphaned_metrics.active_fact_bytes
      << " write_buffer_bytes=" << orphaned_metrics.write_buffer_bytes
      << " write_buffer_limit=" << orphaned_metrics.write_buffer_limit_bytes
      << " facts_flush_pending=" << orphaned_metrics.facts_flush_pending
      << " flush_queue_depth=" << orphaned_metrics.flush_queue_depth
      << " unscheduled_flushes=" << orphaned_metrics.unscheduled_flushes
      << " scheduled_flushes=" << orphaned_metrics.scheduled_flushes
      << " running_flushes=" << orphaned_metrics.running_flushes;

  // A partial active MemTable is not a native flush candidate. Cedar must
  // remain able to admit the next durable transaction, which lets RocksDB own
  // the next MemTable switch and request creation.
  StatusOr<RuntimeMetrics> metrics = database->SampleRuntimeMetrics();
  ASSERT_TRUE(metrics.ok()) << metrics.status().ToString();
  EXPECT_GT(metrics.ValueOrDie().maintenance_flush_grants_accepted, 0U);
  EXPECT_GT(metrics.ValueOrDie().maintenance_completed_grants, 0U);
  EXPECT_EQ(metrics.ValueOrDie().maintenance_flush_deadline_yields, 0U);

  auto transaction = database->BeginTransaction(
      TransactionOptions{.commit_deadline_us = 5'000'000});
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex({PartId{1}, VertexId{999999}}),
                           ValidTime{1})
                  .ok());
  const auto committed = transaction.ValueOrDie()->Commit();
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  EXPECT_EQ(committed.ValueOrDie().outcome, CommitOutcome::kCommitted)
      << committed.ValueOrDie().status.ToString()
      << " write_buffer_bytes=" << metrics.ValueOrDie().write_buffer_bytes
      << " write_buffer_limit="
      << metrics.ValueOrDie().write_buffer_limit_bytes
      << " facts_flush_pending=" << metrics.ValueOrDie().facts_flush_pending
      << " flush_queue_depth=" << metrics.ValueOrDie().flush_queue_depth;
  ASSERT_TRUE(database->Close().ok());

  auto reopened = Database::Open(options);
  ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
  EXPECT_TRUE(reopened.ValueOrDie()->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelBoundedBenchmarkTest,
     KernelTestProfileDoesNotRejectAsyncWritersAtFlushDebtLimit) {
  char pattern[] = "/tmp/cedar_kernel_test_pressure_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  DatabaseOptions options;
  options.path = path;
  options.storage_profile = StorageProfile::kKernelTest;
  options.query_runtime.query_memory_bytes = 128ULL * 1024ULL;
  options.query_runtime.projection_cache_bytes = 128ULL * 1024ULL;
  options.query_runtime.query_delta_bytes = 128ULL * 1024ULL;
  options.group_commit_max_batch_size = 64;
  options.group_commit_window_us = 200;
  options.runtime_pressure_override_for_testing = [](PressureSample* sample) {
    sample->free_disk_bytes = UINT64_MAX;
    sample->free_disk_percent = 100;
  };
  auto opened = Database::Open(options);
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();

  auto writers = std::async(std::launch::async, [&] {
    return RunFixedWriters(database.get(), 64, 32);
  });
  RuntimeMetrics peak;
  while (writers.wait_for(std::chrono::milliseconds(0)) !=
         std::future_status::ready) {
    const auto current = database->SampleRuntimeMetrics();
    ASSERT_TRUE(current.ok()) << current.status().ToString();
    const RuntimeMetrics& metrics = current.ValueOrDie();
    peak.write_stopped = std::max(peak.write_stopped, metrics.write_stopped);
    peak.immutable_fact_count =
        std::max(peak.immutable_fact_count, metrics.immutable_fact_count);
    peak.immutable_fact_bytes =
        std::max(peak.immutable_fact_bytes, metrics.immutable_fact_bytes);
    peak.l0_file_count = std::max(peak.l0_file_count, metrics.l0_file_count);
    peak.pending_compaction_bytes = std::max(peak.pending_compaction_bytes,
                                             metrics.pending_compaction_bytes);
    peak.write_buffer_bytes =
        std::max(peak.write_buffer_bytes, metrics.write_buffer_bytes);
    peak.flush_queue_depth =
        std::max(peak.flush_queue_depth, metrics.flush_queue_depth);
    peak.unscheduled_flushes =
        std::max(peak.unscheduled_flushes, metrics.unscheduled_flushes);
    peak.scheduled_flushes =
        std::max(peak.scheduled_flushes, metrics.scheduled_flushes);
    peak.running_flushes =
        std::max(peak.running_flushes, metrics.running_flushes);
    peak.maintenance_flush_deadline_yields = std::max(
        peak.maintenance_flush_deadline_yields,
        metrics.maintenance_flush_deadline_yields);
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  const BoundedWriterResult writer_result = writers.get();
  const auto sampled = database->SampleRuntimeMetrics();
  ASSERT_TRUE(sampled.ok()) << sampled.status().ToString();
  const RuntimeMetrics& metrics = sampled.ValueOrDie();

  EXPECT_TRUE(writer_result.status.ok())
      << writer_result.status.ToString()
      << " write_stopped=" << peak.write_stopped
      << " immutable_fact_count=" << peak.immutable_fact_count
      << " immutable_fact_bytes=" << peak.immutable_fact_bytes
      << " l0_file_count=" << peak.l0_file_count
      << " pending_compaction_bytes=" << peak.pending_compaction_bytes
      << " write_buffer_bytes=" << peak.write_buffer_bytes
      << " write_buffer_limit=" << metrics.write_buffer_limit_bytes
      << " flush_queue_depth=" << peak.flush_queue_depth
      << " unscheduled_flushes=" << peak.unscheduled_flushes
      << " scheduled_flushes=" << peak.scheduled_flushes
      << " running_flushes=" << peak.running_flushes
      << " maintenance_deadline_yields="
      << peak.maintenance_flush_deadline_yields;
  EXPECT_EQ(writer_result.failures, 0U);
  EXPECT_EQ(writer_result.committed, 64U * 32U);
  EXPECT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

}  // namespace
}  // namespace cedar::benchmark
