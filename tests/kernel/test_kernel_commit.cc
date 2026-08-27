// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#if defined(__APPLE__)
#include <pthread/qos.h>
#endif

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "cedar/database.h"
#include "storage/facts/fact_store.h"
#include "kernel/database_impl.h"

namespace cedar {
namespace {

class KernelCommitTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_kernel_commit_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    path_ = pattern;
    auto opened = Database::Open(DatabaseOptions{.path = path_});
    ASSERT_TRUE(opened.ok()) << opened.status().ToString();
    database_ = std::move(opened).ConsumeValueOrDie();
  }

  void TearDown() override {
    database_.reset();
    std::filesystem::remove_all(path_);
  }

  std::unique_ptr<Transaction> Begin(TransactionOptions options = {}) {
    auto transaction = database_->BeginTransaction(options);
    EXPECT_TRUE(transaction.ok()) << transaction.status().ToString();
    return std::move(transaction).ConsumeValueOrDie();
  }

  std::string path_;
  std::unique_ptr<Database> database_;
};

TEST_F(KernelCommitTest, CommitsMultipleFactsAndResolvesThePublicTransaction) {
  std::unique_ptr<Transaction> transaction = Begin();
  ASSERT_TRUE(transaction->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10}).ok());
  ASSERT_TRUE(transaction->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}), ValidTime{20}).ok());

  const auto committed = transaction->Commit();
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  EXPECT_EQ(committed.ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_TRUE(committed.ValueOrDie().txn_id.valid());
  EXPECT_EQ(committed.ValueOrDie().commit_seq, CommitSeq{1});

  const auto resolved = database_->ResolveTransaction(committed.ValueOrDie().txn_id);
  ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
  ASSERT_TRUE(resolved.ValueOrDie().has_value());
  EXPECT_EQ(resolved.ValueOrDie()->outcome, CommitOutcome::kCommitted);
  EXPECT_EQ(resolved.ValueOrDie()->commit_seq, CommitSeq{1});

  const auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  EXPECT_TRUE(snapshot.ValueOrDie().Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10})
                  .ValueOrDie());
  EXPECT_TRUE(snapshot.ValueOrDie().Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}), ValidTime{20})
                  .ValueOrDie());
}

TEST_F(KernelCommitTest, AcceptsAsyncCommitDurablyThenWaitsForItsTerminalResult) {
  std::unique_ptr<Transaction> transaction = Begin();
  ASSERT_TRUE(transaction->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{71}}), ValidTime{10}).ok());

  const auto accepted = transaction->CommitAsync();
  ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();
  EXPECT_EQ(accepted.ValueOrDie().acceptance(), CommitAcceptance::kAccepted);
  EXPECT_TRUE(accepted.ValueOrDie().txn_id().valid());

  const auto completed = accepted.ValueOrDie().Wait();
  ASSERT_TRUE(completed.ok()) << completed.status().ToString();
  EXPECT_EQ(completed.ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_EQ(completed.ValueOrDie().txn_id, accepted.ValueOrDie().txn_id());
  EXPECT_TRUE(database_->ResolveTransaction(accepted.ValueOrDie().txn_id())
                  .ValueOrDie()
                  .has_value());
}

TEST_F(KernelCommitTest, KeepsAsyncCommitHiddenUntilPublicationCompletes) {
  std::unique_ptr<Transaction> transaction = Begin();
  ASSERT_TRUE(transaction->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{72}}), ValidTime{10}).ok());

  const auto accepted = transaction->CommitAsync();
  ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();
  EXPECT_EQ(accepted.ValueOrDie().acceptance(), CommitAcceptance::kAccepted);

  const auto completed = accepted.ValueOrDie().Wait();
  ASSERT_TRUE(completed.ok()) << completed.status().ToString();
  EXPECT_EQ(completed.ValueOrDie().outcome, CommitOutcome::kCommitted);
  const auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  EXPECT_TRUE(snapshot.ValueOrDie()
                  .Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{72}}), ValidTime{10})
                  .ValueOrDie());
}

TEST(KernelPreparedCommitTest,
     ExternalDecisionRecoveryPreservesPreparedFactsUntilCertifiedFinalize) {
  char pattern[] = "/tmp/cedar_kernel_external_prepare_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  const auto options = [&] {
    DatabaseOptions value;
    value.path = path;
    value.prepared_commit_recovery =
        PreparedCommitRecoveryPolicy::kAwaitExternalDecision;
    return value;
  };
  const PreparedCommitBatch batch{
      TxnId{901}, 7001,
      {FactEvent{FactRef{PartId{0}, FactFamily::kVertexState, PropertyId{}, 91},
                 ValidTime{11}, CommitSeq{1}, FactOperation::kPut, 0,
                 std::nullopt, std::nullopt}}};

  auto opened = Database::Open(options());
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  ASSERT_TRUE(database->PersistPreparedCommit(batch).ok());
  ASSERT_TRUE(database->Close().ok());
  database.reset();

  opened = Database::Open(options());
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  database = std::move(opened).ConsumeValueOrDie();
  const auto prepared = database->ListPreparedCommits();
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  ASSERT_EQ(prepared.ValueOrDie().size(), 1U);
  EXPECT_EQ(prepared.ValueOrDie().front().txn_id, batch.txn_id);
  auto hidden = database->BeginSnapshot();
  ASSERT_TRUE(hidden.ok()) << hidden.status().ToString();
  EXPECT_FALSE(hidden.ValueOrDie()
                   .Exists(EntityFact::Vertex(
                               VertexRef{PartId{0}, VertexId{91}}),
                           ValidTime{11})
                   .ValueOrDie());

  const std::string certificate = "txnd=81;term=7;index=23";
  const auto finalized =
      database->FinalizePreparedCommit(batch.txn_id, certificate);
  ASSERT_TRUE(finalized.ok()) << finalized.status().ToString();
  EXPECT_EQ(finalized.ValueOrDie().outcome, CommitOutcome::kCommitted);
  const auto replay =
      database->FinalizePreparedCommit(batch.txn_id, certificate);
  ASSERT_TRUE(replay.ok()) << replay.status().ToString();
  EXPECT_EQ(replay.ValueOrDie().commit_seq,
            finalized.ValueOrDie().commit_seq);
  EXPECT_TRUE(database
                  ->FinalizePreparedCommit(batch.txn_id,
                                           "txnd=81;term=8;index=23")
                  .status()
                  .IsConflict());
  auto visible = database->BeginSnapshot();
  ASSERT_TRUE(visible.ok()) << visible.status().ToString();
  EXPECT_TRUE(visible.ValueOrDie()
                  .Exists(EntityFact::Vertex(
                              VertexRef{PartId{0}, VertexId{91}}),
                          ValidTime{11})
                  .ValueOrDie());
  hidden = Status::InvalidArgument("test", "released");
  visible = Status::InvalidArgument("test", "released");
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelPreparedCommitTest,
     CertifiedAbortSurvivesReopenAndRejectsCommitCertificateSubstitution) {
  char pattern[] = "/tmp/cedar_kernel_external_abort_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  DatabaseOptions options;
  options.path = path;
  options.prepared_commit_recovery =
      PreparedCommitRecoveryPolicy::kAwaitExternalDecision;
  const PreparedCommitBatch batch{
      TxnId{902}, 7002,
      {FactEvent{FactRef{PartId{0}, FactFamily::kVertexState, PropertyId{}, 92},
                 ValidTime{12}, CommitSeq{1}, FactOperation::kPut, 0,
                 std::nullopt, std::nullopt}}};

  auto opened = Database::Open(options);
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  ASSERT_TRUE(database->PersistPreparedCommit(batch).ok());
  ASSERT_TRUE(database->AbortPreparedCommit(batch.txn_id, "txnd-abort-31").ok());
  auto decision = database->ResolvePreparedCommitDecision(batch.txn_id);
  ASSERT_TRUE(decision.ok()) << decision.status().ToString();
  ASSERT_TRUE(decision.ValueOrDie().has_value());
  EXPECT_EQ(decision.ValueOrDie()->outcome,
            PreparedCommitDecisionOutcome::kAbort);
  EXPECT_EQ(decision.ValueOrDie()->certificate, "txnd-abort-31");
  EXPECT_TRUE(database->AbortPreparedCommit(batch.txn_id, "txnd-abort-31").ok());
  EXPECT_TRUE(database->AbortPreparedCommit(batch.txn_id, "txnd-abort-32")
                  .IsConflict());
  EXPECT_TRUE(database->FinalizePreparedCommit(batch.txn_id, "txnd-abort-31")
                  .status()
                  .IsConflict());
  ASSERT_TRUE(database->Close().ok());
  database.reset();

  opened = Database::Open(options);
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  database = std::move(opened).ConsumeValueOrDie();
  decision = database->ResolvePreparedCommitDecision(batch.txn_id);
  ASSERT_TRUE(decision.ok()) << decision.status().ToString();
  ASSERT_TRUE(decision.ValueOrDie().has_value());
  EXPECT_EQ(decision.ValueOrDie()->outcome,
            PreparedCommitDecisionOutcome::kAbort);
  EXPECT_EQ(decision.ValueOrDie()->certificate, "txnd-abort-31");
  ASSERT_TRUE(database->ListPreparedCommits().ok());
  EXPECT_TRUE(database->ListPreparedCommits().ValueOrDie().empty());
  auto snapshot = database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  EXPECT_FALSE(snapshot.ValueOrDie()
                   .Exists(EntityFact::Vertex(
                               VertexRef{PartId{0}, VertexId{92}}),
                           ValidTime{12})
                   .ValueOrDie());
  snapshot = Status::InvalidArgument("test", "released");
  EXPECT_TRUE(database->AbortPreparedCommit(batch.txn_id, "txnd-abort-31").ok());
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelPreparedCommitTest,
     CertifiedAbortReplayFinishesInterruptedPreparedCleanup) {
  char pattern[] = "/tmp/cedar_kernel_external_abort_replay_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  const StoreCommitBatch prepared{
      TxnId{903}, 7003,
      {{EntityFact::Vertex(VertexRef{PartId{0}, VertexId{93}}).ref(),
        ValidTime{13}, FactOperation::kPut, 0, std::nullopt}}};
  const StorePreparedDecision decision{
      prepared.txn_id, StorePreparedDecisionOutcome::kAbort,
      "txnd-abort-interrupted"};
  {
    FactStore store(FactStoreOptions{path});
    ASSERT_TRUE(store.Open().ok());
    ASSERT_TRUE(store.PersistPreparedCommit(prepared).ok());
    ASSERT_TRUE(store.PersistPreparedDecision(decision).ok());
    ASSERT_TRUE(store.Close().ok());
  }

  DatabaseOptions options;
  options.path = path;
  options.prepared_commit_recovery =
      PreparedCommitRecoveryPolicy::kAwaitExternalDecision;
  auto opened = Database::Open(options);
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  auto still_prepared = database->ListPreparedCommits();
  ASSERT_TRUE(still_prepared.ok()) << still_prepared.status().ToString();
  ASSERT_EQ(still_prepared.ValueOrDie().size(), 1U);

  ASSERT_TRUE(database
                  ->AbortPreparedCommit(prepared.txn_id, decision.certificate)
                  .ok());
  auto cleaned = database->ListPreparedCommits();
  ASSERT_TRUE(cleaned.ok()) << cleaned.status().ToString();
  EXPECT_TRUE(cleaned.ValueOrDie().empty());
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelAsyncCommitTest, UsesOneDurableWriteForIndependentAsyncCommits) {
  char pattern[] = "/tmp/cedar_kernel_async_group_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::atomic<uint32_t> final_writes = 0;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 2,
      .group_commit_window_us = 20'000,
      .commit_prewrite_fault_injector_for_testing = [&final_writes] {
        ++final_writes;
        return Status::OK();
      }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
  auto first = database->BeginTransaction();
  auto second = database->BeginTransaction();
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  ASSERT_TRUE(second.ok()) << second.status().ToString();
  ASSERT_TRUE(first.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{91}}), ValidTime{10}).ok());
  ASSERT_TRUE(second.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{92}}), ValidTime{10}).ok());

  std::optional<StatusOr<CommitHandle>> first_handle;
  std::optional<StatusOr<CommitHandle>> second_handle;
  std::thread first_thread([&] { first_handle.emplace(first.ValueOrDie()->CommitAsync()); });
  std::thread second_thread([&] { second_handle.emplace(second.ValueOrDie()->CommitAsync()); });
  first_thread.join();
  second_thread.join();
  ASSERT_TRUE(first_handle->ok()) << first_handle->status().ToString();
  ASSERT_TRUE(second_handle->ok()) << second_handle->status().ToString();
  EXPECT_EQ(first_handle->ValueOrDie().Wait().ValueOrDie().outcome,
            CommitOutcome::kCommitted);
  EXPECT_EQ(second_handle->ValueOrDie().Wait().ValueOrDie().outcome,
            CommitOutcome::kCommitted);
  EXPECT_EQ(final_writes.load(), 1U);
  const CommitPipelineMetrics metrics = database->GetCommitPipelineMetrics();
  EXPECT_EQ(metrics.append_fast_path + metrics.general_path, 2U);
  EXPECT_EQ(metrics.append_fast_path, 2U);
  EXPECT_EQ(metrics.latency.queue.count, 2U);
  EXPECT_EQ(metrics.latency.validation.count, 1U);
  EXPECT_EQ(metrics.latency.assembly.count, 1U);
  EXPECT_EQ(metrics.latency.wal_append.count, 1U);
  EXPECT_EQ(metrics.latency.wal_sync.count, 1U);
  EXPECT_EQ(metrics.latency.manifest.count, 1U);
  EXPECT_EQ(metrics.latency.memtable_insert.count, 1U);
  EXPECT_EQ(metrics.latency.wal_callback.count, 1U);
  EXPECT_EQ(metrics.latency.publication.count, 1U);
  EXPECT_EQ(metrics.latency.end_to_end.count, 2U);
  EXPECT_GT(metrics.latency.db_write.count, 0U);
  EXPECT_GE(metrics.runtime.cache_hits, 0U);
  EXPECT_GE(metrics.runtime.compressed_block_count, 0U);
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelAsyncCommitTest, AsyncCommitUsesSynchronousDurableWrite) {
  char pattern[] = "/tmp/cedar_kernel_async_final_unsynced_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::atomic<bool> final_write_was_synchronous = false;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 1,
      .group_commit_window_us = 0,
      .commit_write_options_observer_for_testing =
          [&final_write_was_synchronous](bool sync) {
            final_write_was_synchronous.store(sync);
          }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
  auto transaction = database->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{93}}), ValidTime{10})
                  .ok());

  const auto accepted = transaction.ValueOrDie()->CommitAsync();
  ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();
  ASSERT_TRUE(accepted.ValueOrDie().Wait().ok());
  EXPECT_TRUE(final_write_was_synchronous.load());

  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelRuntimeSamplerTest, SamplesOnCadenceInsteadOfEveryCommitEpoch) {
  char pattern[] = "/tmp/cedar_runtime_sampler_cadence_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  std::atomic<uint32_t> sample_count = 0;
  std::mutex prewrite_mutex;
  std::condition_variable prewrite_cv;
  bool writer_entered = false;
  bool release_writer = false;
  auto opened = Database::Open(DatabaseOptions{
      .path = pattern,
      .group_commit_max_batch_size = 1,
      .group_commit_window_us = 0,
      .commit_prewrite_fault_injector_for_testing = [&] {
        std::unique_lock<std::mutex> lock(prewrite_mutex);
        writer_entered = true;
        prewrite_cv.notify_all();
        prewrite_cv.wait(lock, [&] { return release_writer; });
        return Status::OK();
      },
      .runtime_sample_observer_for_testing = [&] { ++sample_count; }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  auto transaction = database->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{701}}),
                           ValidTime{10})
                  .ok());
  std::optional<StatusOr<CommitResult>> committed;
  std::thread writer([&] { committed.emplace(transaction.ValueOrDie()->Commit()); });
  bool writer_started = false;
  {
    std::unique_lock<std::mutex> lock(prewrite_mutex);
    writer_started = prewrite_cv.wait_for(lock, std::chrono::seconds(2),
                                          [&] { return writer_entered; });
  }
  if (!writer_started) {
    {
      std::lock_guard<std::mutex> lock(prewrite_mutex);
      release_writer = true;
    }
    prewrite_cv.notify_all();
    writer.join();
    ASSERT_TRUE(committed.has_value());
    ASSERT_TRUE(database->Close().ok());
    std::filesystem::remove_all(pattern);
    FAIL() << "commit did not reach the prewrite barrier";
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(125));
  {
    std::lock_guard<std::mutex> lock(prewrite_mutex);
    release_writer = true;
  }
  prewrite_cv.notify_all();
  writer.join();
  ASSERT_TRUE(committed.has_value());
  ASSERT_TRUE(committed->ok()) << committed->status().ToString();
  EXPECT_EQ(committed->ValueOrDie().outcome, CommitOutcome::kCommitted);
  for (uint64_t id = 703; id != 711; ++id) {
    auto next = database->BeginTransaction();
    ASSERT_TRUE(next.ok()) << next.status().ToString();
    ASSERT_TRUE(next.ValueOrDie()
                    ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{id}}),
                             ValidTime{10})
                    .ok());
    const auto next_committed = next.ValueOrDie()->Commit();
    ASSERT_TRUE(next_committed.ok()) << next_committed.status().ToString();
    EXPECT_EQ(next_committed.ValueOrDie().outcome, CommitOutcome::kCommitted);
  }
  EXPECT_LT(sample_count.load(), 8U);
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(pattern);
}

TEST(KernelRuntimeSamplerTest, DoesNotRunASecondRefreshWorker) {
  char pattern[] = "/tmp/cedar_runtime_sampler_single_worker_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  std::mutex sampler_mutex;
  std::condition_variable sampler_cv;
  uint32_t publications = 0;
  bool release_second = false;
  auto opened = Database::Open(DatabaseOptions{
      .path = pattern,
      .runtime_snapshot_before_publish_observer_for_testing = [&] {
        std::unique_lock<std::mutex> lock(sampler_mutex);
        ++publications;
        sampler_cv.notify_all();
        if (publications == 2) {
          sampler_cv.wait(lock, [&] { return release_second; });
        }
      }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  {
    std::unique_lock<std::mutex> lock(sampler_mutex);
    ASSERT_TRUE(sampler_cv.wait_for(lock, std::chrono::seconds(2), [&] {
      return publications >= 2;
    }));
  }
  {
    std::lock_guard<std::mutex> lock(sampler_mutex);
    release_second = true;
  }
  sampler_cv.notify_all();
  {
    std::unique_lock<std::mutex> lock(sampler_mutex);
    const bool saw_third_refresh = sampler_cv.wait_for(
        lock, std::chrono::milliseconds(30), [&] { return publications >= 3; });
    EXPECT_FALSE(saw_third_refresh);
  }
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(pattern);
}

#if defined(__APPLE__)
TEST(KernelRuntimeSamplerTest, UsesCedarForegroundQos) {
  char pattern[] = "/tmp/cedar_runtime_sampler_qos_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  std::mutex mutex;
  std::condition_variable condition;
  bool observed = false;
  qos_class_t observed_qos = QOS_CLASS_UNSPECIFIED;
  DatabaseOptions options;
  options.path = pattern;
  options.runtime_sampler_thread_started_observer_for_testing = [&] {
    int relative_priority = 0;
    EXPECT_EQ(pthread_get_qos_class_np(pthread_self(), &observed_qos,
                                       &relative_priority),
              0);
    std::lock_guard<std::mutex> lock(mutex);
    observed = true;
    condition.notify_all();
  };
  auto database = Database::Open(std::move(options));
  ASSERT_TRUE(database.ok()) << database.status().ToString();
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(condition.wait_for(lock, std::chrono::seconds(2), [&] {
      return observed;
    }));
  }
  EXPECT_EQ(observed_qos, QOS_CLASS_USER_INTERACTIVE);
  ASSERT_TRUE(database.ValueOrDie()->Close().ok());
  std::filesystem::remove_all(pattern);
}
#endif

TEST(KernelRuntimeSamplerTest, ReadsRecoveryWalBytesOncePerRuntimeRefresh) {
  char pattern[] = "/tmp/cedar_runtime_sampler_wal_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  std::atomic<uint32_t> sample_count = 0;
  std::atomic<uint32_t> recovery_wal_read_count = 0;
  std::atomic<uint64_t> max_recovery_wal_bytes_us = 0;
  auto opened = Database::Open(DatabaseOptions{
      .path = pattern,
      .runtime_sample_observer_for_testing = [&] { ++sample_count; },
      .runtime_sampling_timing_observer_for_testing =
          [&](const RuntimeSamplingTiming& timing) {
            ++recovery_wal_read_count;
            uint64_t observed = max_recovery_wal_bytes_us.load();
            while (observed < timing.recovery_wal_bytes_us &&
                   !max_recovery_wal_bytes_us.compare_exchange_weak(
                       observed, timing.recovery_wal_bytes_us)) {
            }
          }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();

  std::this_thread::sleep_for(std::chrono::milliseconds(125));
  ASSERT_TRUE(database->Close().ok());

  EXPECT_GE(sample_count.load(), 2U);
  EXPECT_EQ(recovery_wal_read_count.load(), sample_count.load());
  EXPECT_LT(max_recovery_wal_bytes_us.load(), 250'000U);
  std::filesystem::remove_all(pattern);
}

TEST(KernelRuntimeSamplerTest, UsesNormalSoftAndHardCadences) {
  char pattern[] = "/tmp/cedar_runtime_sampler_pressure_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  enum class TestPressure : uint8_t { kNormal, kSoft, kHard };
  std::atomic<TestPressure> pressure = TestPressure::kNormal;
  std::mutex interval_mutex;
  std::condition_variable interval_cv;
  std::vector<uint64_t> intervals;
  auto opened = Database::Open(DatabaseOptions{
      .path = pattern,
      .runtime_sampler_interval_observer_for_testing = [&](uint64_t interval_ms) {
        std::lock_guard<std::mutex> lock(interval_mutex);
        intervals.push_back(interval_ms);
        interval_cv.notify_all();
      },
      .runtime_pressure_override_for_testing = [&](PressureSample* sample) {
        switch (pressure.load()) {
          case TestPressure::kNormal:
            break;
          case TestPressure::kSoft:
            sample->l0_files = 16;
            break;
          case TestPressure::kHard:
            sample->l0_files = 24;
            break;
        }
      }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  const auto observed = [&](uint64_t target) {
    std::unique_lock<std::mutex> lock(interval_mutex);
    return interval_cv.wait_for(lock, std::chrono::seconds(2), [&] {
      return std::find(intervals.begin(), intervals.end(), target) != intervals.end();
    });
  };
  ASSERT_TRUE(observed(50));
  pressure.store(TestPressure::kSoft);
  ASSERT_TRUE(observed(10));
  pressure.store(TestPressure::kHard);
  ASSERT_TRUE(observed(5));
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(pattern);
}

TEST(KernelRuntimeSamplerTest, StopsAdmissionWhenRuntimeSnapshotIsStale) {
  char pattern[] = "/tmp/cedar_runtime_sampler_stale_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  std::atomic<uint32_t> sample_count = 0;
  std::mutex sample_mutex;
  std::condition_variable sample_cv;
  bool block_samples = false;
  bool release_samples = false;
  auto opened = Database::Open(DatabaseOptions{
      .path = pattern,
      .runtime_sample_observer_for_testing = [&] {
        std::unique_lock<std::mutex> lock(sample_mutex);
        ++sample_count;
        sample_cv.notify_all();
        if (block_samples) {
          sample_cv.wait(lock, [&] { return release_samples; });
        }
      }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  {
    std::lock_guard<std::mutex> lock(sample_mutex);
    block_samples = true;
  }
  bool sampler_blocked = false;
  {
    std::unique_lock<std::mutex> lock(sample_mutex);
    sampler_blocked = sample_cv.wait_for(lock, std::chrono::seconds(2), [&] {
      return sample_count.load() >= 2;
    });
  }
  if (!sampler_blocked) {
    {
      std::lock_guard<std::mutex> lock(sample_mutex);
      release_samples = true;
    }
    sample_cv.notify_all();
    ASSERT_TRUE(database->Close().ok());
    std::filesystem::remove_all(pattern);
    FAIL() << "runtime sampler did not start a follow-up sample";
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  auto transaction = database->BeginTransaction();
  std::optional<StatusOr<CommitResult>> committed;
  Status asserted = Status::OK();
  if (transaction.ok()) {
    asserted = transaction.ValueOrDie()
                   ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{702}}),
                            ValidTime{10});
    if (asserted.ok()) committed.emplace(transaction.ValueOrDie()->Commit());
  }
  {
    std::lock_guard<std::mutex> lock(sample_mutex);
    release_samples = true;
  }
  sample_cv.notify_all();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(asserted.ok()) << asserted.ToString();
  ASSERT_TRUE(committed.has_value());
  ASSERT_TRUE(committed->ok()) << committed->status().ToString();
  EXPECT_EQ(committed->ValueOrDie().outcome, CommitOutcome::kAborted);
  EXPECT_TRUE(committed->ValueOrDie().status.IsResourceExhausted());
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(pattern);
}

TEST(KernelRuntimeSamplerTest, PublishesFreshTimestampAfterDelayedSnapshotPublication) {
  char pattern[] = "/tmp/cedar_runtime_sampler_publish_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  std::mutex sampler_mutex;
  std::condition_variable sampler_cv;
  uint32_t publication_started = 0;
  uint32_t publication_finished = 0;
  bool block_publication = false;
  bool release_publication = false;
  auto opened = Database::Open(DatabaseOptions{
      .path = pattern,
      .runtime_snapshot_before_publish_observer_for_testing = [&] {
        std::unique_lock<std::mutex> lock(sampler_mutex);
        ++publication_started;
        sampler_cv.notify_all();
        if (block_publication) {
          sampler_cv.wait(lock, [&] { return release_publication; });
        }
      },
      .runtime_snapshot_published_observer_for_testing = [&] {
        std::lock_guard<std::mutex> lock(sampler_mutex);
        ++publication_finished;
        sampler_cv.notify_all();
      }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  {
    std::unique_lock<std::mutex> lock(sampler_mutex);
    block_publication = true;
    ASSERT_TRUE(sampler_cv.wait_for(lock, std::chrono::seconds(2), [&] {
      return publication_started >= 2;
    }));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  {
    std::lock_guard<std::mutex> lock(sampler_mutex);
    release_publication = true;
  }
  sampler_cv.notify_all();
  {
    std::unique_lock<std::mutex> lock(sampler_mutex);
    ASSERT_TRUE(sampler_cv.wait_for(lock, std::chrono::seconds(2), [&] {
      return publication_finished >= 2;
    }));
  }

  auto transaction = database->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{703}}),
                           ValidTime{10})
                  .ok());
  const auto committed = transaction.ValueOrDie()->Commit();
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  EXPECT_EQ(committed.ValueOrDie().outcome, CommitOutcome::kCommitted);

  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(pattern);
}

TEST(KernelRuntimeSamplerTest, PublishesWhileCommitCompletionLockIsHeld) {
  char pattern[] = "/tmp/cedar_runtime_sampler_completion_lock_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  std::mutex completion_mutex;
  std::condition_variable completion_cv;
  bool completion_entered = false;
  bool release_completion = false;
  std::mutex publication_mutex;
  std::condition_variable publication_cv;
  uint32_t publications = 0;
  auto opened = Database::Open(DatabaseOptions{
      .path = pattern,
      .group_commit_max_batch_size = 1,
      .group_commit_window_us = 0,
      .commit_result_processing_observer_for_testing = [&] {
        std::unique_lock<std::mutex> lock(completion_mutex);
        completion_entered = true;
        completion_cv.notify_all();
        completion_cv.wait(lock, [&] { return release_completion; });
      },
      .runtime_snapshot_published_observer_for_testing = [&] {
        std::lock_guard<std::mutex> lock(publication_mutex);
        ++publications;
        publication_cv.notify_all();
      }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  {
    std::lock_guard<std::mutex> lock(publication_mutex);
    publications = 0;
  }
  auto transaction = database->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{704}}),
                           ValidTime{10})
                  .ok());

  std::optional<StatusOr<CommitResult>> committed;
  std::thread writer([&] { committed.emplace(transaction.ValueOrDie()->Commit()); });
  bool entered = false;
  {
    std::unique_lock<std::mutex> lock(completion_mutex);
    entered = completion_cv.wait_for(lock, std::chrono::seconds(2), [&] {
      return completion_entered;
    });
  }
  bool published_while_blocked = false;
  if (entered) {
    std::unique_lock<std::mutex> lock(publication_mutex);
    published_while_blocked = publication_cv.wait_for(
        lock, std::chrono::milliseconds(300), [&] { return publications >= 2; });
  }
  {
    std::lock_guard<std::mutex> lock(completion_mutex);
    release_completion = true;
  }
  completion_cv.notify_all();
  writer.join();

  EXPECT_TRUE(entered);
  EXPECT_TRUE(published_while_blocked);
  ASSERT_TRUE(committed.has_value());
  ASSERT_TRUE(committed->ok()) << committed->status().ToString();
  EXPECT_EQ(committed->ValueOrDie().outcome, CommitOutcome::kCommitted);
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(pattern);
}

TEST(KernelRuntimeSamplerTest, RetainedWalHardLimitRejectsSyncAndAsyncAdmission) {
  char pattern[] = "/tmp/cedar_runtime_sampler_wal_limit_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  auto opened = Database::Open(DatabaseOptions{
      .path = pattern,
      .runtime_pressure_override_for_testing = [](PressureSample* sample) {
        sample->retained_wal_bytes = 1ULL << 30;
      }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();

  auto synchronous = database->BeginTransaction();
  ASSERT_TRUE(synchronous.ok()) << synchronous.status().ToString();
  ASSERT_TRUE(synchronous.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{703}}),
                           ValidTime{10})
                  .ok());
  const auto sync_result = synchronous.ValueOrDie()->Commit();
  ASSERT_TRUE(sync_result.ok()) << sync_result.status().ToString();
  EXPECT_EQ(sync_result.ValueOrDie().outcome, CommitOutcome::kAborted);
  EXPECT_TRUE(sync_result.ValueOrDie().status.IsResourceExhausted());

  auto asynchronous = database->BeginTransaction();
  ASSERT_TRUE(asynchronous.ok()) << asynchronous.status().ToString();
  ASSERT_TRUE(asynchronous.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{704}}),
                           ValidTime{10})
                  .ok());
  const auto async_result = asynchronous.ValueOrDie()->CommitAsync();
  ASSERT_FALSE(async_result.ok());
  EXPECT_TRUE(async_result.status().IsResourceExhausted());

  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(pattern);
}

TEST(KernelAdmissionControlTest, BoundsMailboxAdmissionWithoutBlockingCallers) {
  char pattern[] = "/tmp/cedar_kernel_admission_control_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  std::mutex prewrite_mutex;
  std::condition_variable prewrite_cv;
  bool entered = false;
  bool release = false;
  auto opened = Database::Open(DatabaseOptions{
      .path = pattern,
      .group_commit_max_batch_size = 1,
      .group_commit_window_us = 0,
      .commit_prewrite_fault_injector_for_testing = [&] {
        std::unique_lock<std::mutex> lock(prewrite_mutex);
        entered = true;
        prewrite_cv.notify_all();
        prewrite_cv.wait(lock, [&] { return release; });
        return Status::OK();
      },
      .storage_profile = StorageProfile::kProductionAppend,
      .production = ProductionStorageOptions{
          .memory_budget_bytes = 1ULL * 1024ULL * 1024ULL * 1024ULL,
          .kernel_mode = true},
      .runtime_pressure_override_for_testing = [](PressureSample* sample) {
        sample->free_disk_bytes = UINT64_MAX;
        sample->free_disk_percent = 100;
      },
      .async_executor = AsyncExecutorOptions{1, 1, 4ULL * 1024ULL * 1024ULL},
      .query_runtime = QueryRuntimeOptions{
          .query_workers = 4,
          .reserved_interactive_workers = 1,
          .query_memory_bytes = 32ULL * 1024ULL * 1024ULL,
          .projection_cache_bytes = 32ULL * 1024ULL * 1024ULL,
          .query_delta_bytes = 32ULL * 1024ULL * 1024ULL}});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  auto first = database->BeginTransaction();
  auto second = database->BeginTransaction();
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  ASSERT_TRUE(first.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{800}}), ValidTime{10})
                  .ok());
  ASSERT_TRUE(second.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{801}}), ValidTime{10})
                  .ok());
  std::optional<StatusOr<CommitHandle>> first_result;
  std::thread first_thread([&] {
    first_result.emplace(first.ValueOrDie()->CommitAsync());
  });
  {
    std::unique_lock<std::mutex> lock(prewrite_mutex);
    const bool entered_prewrite = prewrite_cv.wait_for(
        lock, std::chrono::seconds(2), [&] { return entered; });
    if (!entered_prewrite) {
      release = true;
      lock.unlock();
      prewrite_cv.notify_all();
      first_thread.join();
      ASSERT_TRUE(first_result.has_value());
      ADD_FAILURE() << "first async submission never entered prewrite: "
                    << (first_result->ok() ? "accepted" :
                        first_result->status().ToString());
      return;
    }
  }
  const auto started_at = std::chrono::steady_clock::now();
  const auto rejected = second.ValueOrDie()->CommitAsync();
  EXPECT_TRUE(rejected.status().IsResourceExhausted());
  EXPECT_LT(std::chrono::steady_clock::now() - started_at, std::chrono::milliseconds(20));
  {
    std::lock_guard<std::mutex> lock(prewrite_mutex);
    release = true;
  }
  prewrite_cv.notify_all();
  first_thread.join();
  ASSERT_TRUE(first_result->ok()) << first_result->status().ToString();
  ASSERT_TRUE(first_result->ValueOrDie().Wait().ok());
  const CommitPipelineMetrics metrics = database->GetCommitPipelineMetrics();
  EXPECT_EQ(metrics.async_mailbox_requests_reserved, 0U);
  EXPECT_EQ(metrics.async_mailbox_bytes_reserved, 0U);
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(pattern);
}

TEST(KernelAsyncCommitTest, FreshTransactionDoesNotReadPersistedOutcomeBeforeWrite) {
  char pattern[] = "/tmp/cedar_kernel_fresh_transaction_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::atomic<uint32_t> transaction_outcome_reads = 0;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 1,
      .group_commit_window_us = 0,
      .commit_transaction_lookup_observer_for_testing =
          [&transaction_outcome_reads] { ++transaction_outcome_reads; }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
  auto transaction = database->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{95}}), ValidTime{10})
                  .ok());

  const auto accepted = transaction.ValueOrDie()->CommitAsync();
  ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();
  ASSERT_TRUE(accepted.ValueOrDie().Wait().ok());
  EXPECT_EQ(transaction_outcome_reads.load(), 0U);

  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelSnapshotValidationTest,
     UsesRecentWriteIndexForCoveredSnapshotAndFallsBackForStaleConflict) {
  char pattern[] = "/tmp/cedar_kernel_snapshot_validation_index_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::atomic<uint32_t> canonical_scans = 0;
  DatabaseOptions options;
  options.path = path;
  options.group_commit_max_batch_size = 1;
  options.group_commit_window_us = 0;
  options.validation_scan_observer_for_testing = [&canonical_scans] {
    canonical_scans.fetch_add(1, std::memory_order_relaxed);
  };
  auto opened = Database::Open(std::move(options));
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();

  auto first = database->BeginTransaction();
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  ASSERT_TRUE(first.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{971}}), ValidTime{10})
                  .ok());
  ASSERT_TRUE(first.ValueOrDie()->Commit().ok());
  EXPECT_EQ(canonical_scans.load(std::memory_order_relaxed), 0U);

  auto stale = database->BeginTransaction();
  auto winner = database->BeginTransaction();
  ASSERT_TRUE(stale.ok()) << stale.status().ToString();
  ASSERT_TRUE(winner.ok()) << winner.status().ToString();
  ASSERT_TRUE(stale.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{972}}), ValidTime{10})
                  .ok());
  ASSERT_TRUE(winner.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{972}}), ValidTime{10})
                  .ok());
  ASSERT_TRUE(winner.ValueOrDie()->Commit().ok());
  const auto stale_result = stale.ValueOrDie()->Commit();
  ASSERT_TRUE(stale_result.ok()) << stale_result.status().ToString();
  EXPECT_EQ(stale_result.ValueOrDie().outcome, CommitOutcome::kAborted);
  EXPECT_TRUE(stale_result.ValueOrDie().status.IsConflict());
  EXPECT_GT(canonical_scans.load(std::memory_order_relaxed), 0U);

  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelTransactionAllocationTest, AllocatesLeasedTransactionIdWhileWriterIsBlocked) {
  char pattern[] = "/tmp/cedar_kernel_transaction_lease_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::mutex prewrite_mutex;
  std::condition_variable prewrite_cv;
  bool writer_entered = false;
  bool release_writer = false;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 1,
      .group_commit_window_us = 0,
      .commit_prewrite_fault_injector_for_testing = [&] {
        std::unique_lock<std::mutex> lock(prewrite_mutex);
        writer_entered = true;
        prewrite_cv.notify_all();
        prewrite_cv.wait(lock, [&] { return release_writer; });
        return Status::OK();
      }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
  auto first = database->BeginTransaction();
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  ASSERT_TRUE(first.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{96}}), ValidTime{10})
                  .ok());

  std::future<StatusOr<CommitHandle>> committed = std::async(
      std::launch::async, [&] { return first.ValueOrDie()->CommitAsync(); });
  {
    std::unique_lock<std::mutex> lock(prewrite_mutex);
    ASSERT_TRUE(prewrite_cv.wait_for(lock, std::chrono::seconds(2),
                                     [&] { return writer_entered; }));
  }

  std::future<StatusOr<std::unique_ptr<Transaction>>> allocated = std::async(
      std::launch::async, [&] { return database->BeginTransaction(); });
  const bool allocated_before_publication =
      allocated.wait_for(std::chrono::milliseconds(100)) == std::future_status::ready;
  {
    std::lock_guard<std::mutex> lock(prewrite_mutex);
    release_writer = true;
  }
  prewrite_cv.notify_all();

  ASSERT_TRUE(committed.get().ok());
  ASSERT_TRUE(allocated_before_publication);
  ASSERT_TRUE(allocated.get().ok());
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelCommitDurabilityTest, SynchronousCommitRetainsItsDurableWrite) {
  char pattern[] = "/tmp/cedar_kernel_sync_commit_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::atomic<bool> commit_write_was_synchronous = false;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 1,
      .group_commit_window_us = 0,
      .commit_write_options_observer_for_testing =
          [&commit_write_was_synchronous](bool sync) {
            commit_write_was_synchronous.store(sync);
          }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
  auto transaction = database->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{94}}), ValidTime{10})
                  .ok());

  ASSERT_TRUE(transaction.ValueOrDie()->Commit().ok());
  EXPECT_TRUE(commit_write_was_synchronous.load());

  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST_F(KernelCommitTest, PersistsAsyncValidationAbortAsATerminalResult) {
  std::unique_ptr<Transaction> first = Begin();
  std::unique_ptr<Transaction> second = Begin();
  ASSERT_TRUE(first->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{101}}), ValidTime{10}).ok());
  ASSERT_TRUE(second->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{101}}), ValidTime{10}).ok());

  const auto committed = first->Commit();
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  const auto accepted = second->CommitAsync();
  ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();
  const auto completed = accepted.ValueOrDie().Wait();
  ASSERT_TRUE(completed.ok()) << completed.status().ToString();
  EXPECT_EQ(completed.ValueOrDie().outcome, CommitOutcome::kAborted);
  EXPECT_TRUE(completed.ValueOrDie().status.IsConflict());

  const auto resolved = database_->ResolveTransaction(accepted.ValueOrDie().txn_id());
  ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
  ASSERT_TRUE(resolved.ValueOrDie().has_value());
  EXPECT_EQ(resolved.ValueOrDie()->outcome, CommitOutcome::kAborted);
}

TEST(KernelAsyncCommitTest, PersistsValidationAbortAlongsideIndependentWinner) {
  char pattern[] = "/tmp/cedar_kernel_async_abort_group_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::atomic<uint32_t> physical_writes = 0;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 2,
      .group_commit_window_us = 20'000,
      .commit_prewrite_fault_injector_for_testing = [&physical_writes] {
        ++physical_writes;
        return Status::OK();
      }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();

  auto seed = database->BeginTransaction();
  ASSERT_TRUE(seed.ok()) << seed.status().ToString();
  ASSERT_TRUE(seed.ValueOrDie()
                  ->Assert(EdgeIdentity{EdgeId{101}, VertexId{1}, VertexId{2}, 7},
                           ValidTime{10})
                  .ok());
  ASSERT_EQ(seed.ValueOrDie()->Commit().ValueOrDie().outcome,
            CommitOutcome::kCommitted);
  physical_writes.store(0);

  auto invalid = database->BeginTransaction();
  auto winner = database->BeginTransaction();
  ASSERT_TRUE(invalid.ok()) << invalid.status().ToString();
  ASSERT_TRUE(winner.ok()) << winner.status().ToString();
  ASSERT_TRUE(invalid.ValueOrDie()
                  ->Assert(EdgeIdentity{EdgeId{101}, VertexId{1}, VertexId{3}, 7},
                           ValidTime{20})
                  .ok());
  ASSERT_TRUE(winner.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{102}}), ValidTime{10})
                  .ok());

  std::optional<StatusOr<CommitHandle>> invalid_handle;
  std::optional<StatusOr<CommitHandle>> winner_handle;
  std::thread invalid_thread(
      [&] { invalid_handle.emplace(invalid.ValueOrDie()->CommitAsync()); });
  std::thread winner_thread(
      [&] { winner_handle.emplace(winner.ValueOrDie()->CommitAsync()); });
  invalid_thread.join();
  winner_thread.join();

  ASSERT_TRUE(invalid_handle->ok()) << invalid_handle->status().ToString();
  ASSERT_TRUE(winner_handle->ok()) << winner_handle->status().ToString();
  const TxnId invalid_id = invalid_handle->ValueOrDie().txn_id();
  const TxnId winner_id = winner_handle->ValueOrDie().txn_id();
  const auto invalid_result = invalid_handle->ValueOrDie().Wait();
  const auto winner_result = winner_handle->ValueOrDie().Wait();
  ASSERT_TRUE(invalid_result.ok()) << invalid_result.status().ToString();
  ASSERT_TRUE(winner_result.ok()) << winner_result.status().ToString();
  EXPECT_EQ(invalid_result.ValueOrDie().outcome, CommitOutcome::kAborted);
  EXPECT_TRUE(invalid_result.ValueOrDie().status.IsIdentityConflict())
      << invalid_result.ValueOrDie().status.ToString();
  EXPECT_EQ(winner_result.ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_EQ(physical_writes.load(), 1U);

  ASSERT_TRUE(database->Close().ok());
  database.reset();
  auto reopened = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
  database = std::move(reopened).ConsumeValueOrDie();
  const auto recovered_abort = database->ResolveTransaction(invalid_id);
  const auto recovered_winner = database->ResolveTransaction(winner_id);
  ASSERT_TRUE(recovered_abort.ok()) << recovered_abort.status().ToString();
  ASSERT_TRUE(recovered_winner.ok()) << recovered_winner.status().ToString();
  ASSERT_TRUE(recovered_abort.ValueOrDie().has_value());
  ASSERT_TRUE(recovered_winner.ValueOrDie().has_value());
  EXPECT_EQ(recovered_abort.ValueOrDie()->outcome, CommitOutcome::kAborted);
  EXPECT_EQ(recovered_winner.ValueOrDie()->outcome, CommitOutcome::kCommitted);
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelAsyncCommitRecoveryTest, ResumesDurablePrepareWhenDatabaseReopens) {
  char pattern[] = "/tmp/cedar_kernel_async_recovery_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  const StoreCommitBatch prepared{
      TxnId{1}, 1,
      {{EntityFact::Vertex(VertexRef{PartId{0}, VertexId{81}}).ref(), ValidTime{10},
        FactOperation::kPut, 0, std::nullopt}}};
  {
    FactStore store(FactStoreOptions{path});
    ASSERT_TRUE(store.Open().ok());
    ASSERT_TRUE(store.PersistPreparedCommit(prepared).ok());
    ASSERT_TRUE(store.Close().ok());
  }

  auto opened = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
  ASSERT_TRUE(database->Close().ok());
  database.reset();

  auto reopened = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
  database = std::move(reopened).ConsumeValueOrDie();
  const auto resolved = database->ResolveTransaction(TxnId{1});
  ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
  ASSERT_TRUE(resolved.ValueOrDie().has_value());
  EXPECT_EQ(resolved.ValueOrDie()->outcome, CommitOutcome::kCommitted);
  EXPECT_EQ(resolved.ValueOrDie()->commit_seq, CommitSeq{1});
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest, GroupsIndependentVertexAssertionsIntoOneDurableWrite) {
  char pattern[] = "/tmp/cedar_kernel_group_commit_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::atomic<uint32_t> physical_writes = 0;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 2,
      .group_commit_window_us = 20'000,
      .commit_prewrite_fault_injector_for_testing = [&physical_writes] {
        ++physical_writes;
        return Status::OK();
      }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();

  auto first = database->BeginTransaction();
  auto second = database->BeginTransaction();
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  ASSERT_TRUE(second.ok()) << second.status().ToString();
  ASSERT_TRUE(first.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10})
                  .ok());
  ASSERT_TRUE(second.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}), ValidTime{20})
                  .ok());

  std::optional<StatusOr<CommitResult>> first_result;
  std::optional<StatusOr<CommitResult>> second_result;
  std::thread first_thread([&first, &first_result] {
    first_result.emplace(first.ValueOrDie()->Commit());
  });
  std::thread second_thread([&second, &second_result] {
    second_result.emplace(second.ValueOrDie()->Commit());
  });
  first_thread.join();
  second_thread.join();

  ASSERT_TRUE(first_result->ok()) << first_result->status().ToString();
  ASSERT_TRUE(second_result->ok()) << second_result->status().ToString();
  EXPECT_EQ(first_result->ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_EQ(second_result->ValueOrDie().outcome, CommitOutcome::kCommitted);
  const TxnId first_id = first_result->ValueOrDie().txn_id;
  const TxnId second_id = second_result->ValueOrDie().txn_id;
  EXPECT_EQ(physical_writes.load(), 1U);
  const CommitPipelineMetrics metrics = database->GetCommitPipelineMetrics();
  EXPECT_EQ(metrics.submitted, 2U);
  EXPECT_EQ(metrics.durably_accepted, 2U);
  EXPECT_EQ(metrics.published, 2U);
  {
    const auto snapshot = database->BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    EXPECT_TRUE(snapshot.ValueOrDie()
                    .Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10})
                    .ValueOrDie());
    EXPECT_TRUE(snapshot.ValueOrDie()
                    .Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}), ValidTime{20})
                    .ValueOrDie());
  }
  ASSERT_TRUE(database->Close().ok());
  database.reset();

  auto reopened = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
  database = std::move(reopened).ConsumeValueOrDie();
  EXPECT_TRUE(database->ResolveTransaction(first_id).ValueOrDie().has_value());
  EXPECT_TRUE(database->ResolveTransaction(second_id).ValueOrDie().has_value());
  {
    const auto recovered_snapshot = database->BeginSnapshot();
    ASSERT_TRUE(recovered_snapshot.ok()) << recovered_snapshot.status().ToString();
    EXPECT_TRUE(recovered_snapshot.ValueOrDie()
                    .Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10})
                    .ValueOrDie());
    EXPECT_TRUE(recovered_snapshot.ValueOrDie()
                    .Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}), ValidTime{20})
                    .ValueOrDie());
  }
  ASSERT_TRUE(database->Close().ok());
  database.reset();
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest, KernelCommitReportsGroupFill) {
  char pattern[] = "/tmp/cedar_kernel_group_fill_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 2,
      .group_commit_window_us = 20'000});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();

  auto first = database->BeginTransaction();
  auto second = database->BeginTransaction();
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  ASSERT_TRUE(second.ok()) << second.status().ToString();
  ASSERT_TRUE(first.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{3}}),
                           ValidTime{10})
                  .ok());
  ASSERT_TRUE(second.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{4}}),
                           ValidTime{20})
                  .ok());

  std::optional<StatusOr<CommitResult>> first_result;
  std::optional<StatusOr<CommitResult>> second_result;
  std::thread first_thread(
      [&] { first_result.emplace(first.ValueOrDie()->Commit()); });
  std::thread second_thread(
      [&] { second_result.emplace(second.ValueOrDie()->Commit()); });
  first_thread.join();
  second_thread.join();

  ASSERT_TRUE(first_result->ok()) << first_result->status().ToString();
  ASSERT_TRUE(second_result->ok()) << second_result->status().ToString();
  EXPECT_EQ(first_result->ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_EQ(second_result->ValueOrDie().outcome, CommitOutcome::kCommitted);
  const CommitPipelineMetrics metrics = database->GetCommitPipelineMetrics();
  EXPECT_EQ(metrics.group_fill.groups, 1U);
  EXPECT_EQ(metrics.group_fill.total_transactions, 2U);
  EXPECT_EQ(metrics.group_fill.buckets[GroupFillBucket(2)], 1U);
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest, EnablesBoundedGroupCommitByDefault) {
  const DatabaseOptions options;
  EXPECT_EQ(options.group_commit_max_batch_size, 128U);
  EXPECT_EQ(options.group_commit_max_batch_bytes, 2ULL * 1024ULL * 1024ULL);
  EXPECT_EQ(options.group_commit_max_queue_requests, 1024U);
  EXPECT_EQ(options.group_commit_max_queue_bytes, 16ULL * 1024ULL * 1024ULL);
  EXPECT_EQ(options.group_commit_window_us, 200U);
}

TEST(KernelGroupCommitTest, RejectsGroupCountAboveProtocolMaximum) {
  char pattern[] = "/tmp/cedar_kernel_group_count_limit_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  auto opened = Database::Open(DatabaseOptions{
      .path = pattern, .group_commit_max_batch_size = 513});
  ASSERT_FALSE(opened.ok());
  EXPECT_TRUE(opened.status().IsInvalidArgument())
      << opened.status().ToString();
  std::filesystem::remove_all(pattern);
}

TEST(KernelGroupCommitTest, RejectsEncodedBatchBeforeDurableWrite) {
  char pattern[] = "/tmp/cedar_kernel_group_bytes_limit_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  std::atomic<uint32_t> physical_writes = 0;
  auto opened = Database::Open(DatabaseOptions{
      .path = pattern,
      .group_commit_max_batch_size = 1,
      .commit_prewrite_fault_injector_for_testing = [&physical_writes] {
        ++physical_writes;
        return Status::OK();
      },
      .group_commit_max_batch_bytes = 512});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
  ASSERT_TRUE(database->RegisterProperty(PropertyDefinition{
                                      PropertyId{7}, 0, "payload",
                                      PropertyEntityKind::kVertex,
                                      PhysicalType::kString, 4096})
                   .ok());
  auto transaction = database->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Set(PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{7}),
                        ValidTime{10}, Value::String(std::string(4096, 'x')))
                  .ok());

  const auto result = transaction.ValueOrDie()->Commit();
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ(result.ValueOrDie().outcome, CommitOutcome::kAborted);
  EXPECT_TRUE(result.ValueOrDie().status.IsResourceExhausted())
      << result.ValueOrDie().status.ToString();
  EXPECT_EQ(physical_writes.load(), 0U);
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(pattern);
}

TEST(KernelGroupCommitTest, RejectsAdmissionWhenPendingQueueIsFull) {
  char pattern[] = "/tmp/cedar_kernel_group_queue_limit_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  std::mutex prewrite_mutex;
  std::condition_variable prewrite_cv;
  std::mutex enqueue_mutex;
  std::condition_variable enqueue_cv;
  std::atomic<uint32_t> enqueued = 0;
  bool first_prewrite_entered = false;
  bool release_first_prewrite = false;
  auto opened = Database::Open(DatabaseOptions{
      .path = pattern,
      .group_commit_max_batch_size = 1,
      .group_commit_window_us = 0,
      .commit_prewrite_fault_injector_for_testing = [&] {
        std::unique_lock<std::mutex> lock(prewrite_mutex);
        if (!first_prewrite_entered) {
          first_prewrite_entered = true;
          prewrite_cv.notify_all();
          prewrite_cv.wait(lock, [&] { return release_first_prewrite; });
        }
        return Status::OK();
      },
      .group_commit_max_queue_requests = 1,
      .group_commit_max_queue_bytes = 1ULL * 1024ULL * 1024ULL,
      .append_commit_enqueued_observer_for_testing = [&] {
        ++enqueued;
        enqueue_cv.notify_all();
      }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();

  auto first = database->BeginTransaction();
  auto second = database->BeginTransaction();
  auto third = database->BeginTransaction();
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  ASSERT_TRUE(third.ok());
  ASSERT_TRUE(first.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{201}}),
                                         ValidTime{10})
                  .ok());
  ASSERT_TRUE(second.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{202}}),
                                          ValidTime{10})
                  .ok());
  ASSERT_TRUE(third.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{203}}),
                                         ValidTime{10})
                  .ok());

  std::optional<StatusOr<CommitHandle>> first_result;
  std::thread first_thread(
      [&] { first_result.emplace(first.ValueOrDie()->CommitAsync()); });
  {
    std::unique_lock<std::mutex> lock(prewrite_mutex);
    ASSERT_TRUE(prewrite_cv.wait_for(
        lock, std::chrono::seconds(2), [&] { return first_prewrite_entered; }));
  }

  std::optional<StatusOr<CommitHandle>> second_result;
  std::thread second_thread([&] {
    second_result.emplace(second.ValueOrDie()->CommitAsync());
  });
  {
    std::unique_lock<std::mutex> lock(enqueue_mutex);
    ASSERT_TRUE(enqueue_cv.wait_for(lock, std::chrono::seconds(2),
                                    [&] { return enqueued.load() >= 2; }));
  }

  std::optional<StatusOr<CommitHandle>> third_result;
  std::thread third_thread([&] {
    third_result.emplace(third.ValueOrDie()->CommitAsync());
  });
  {
    std::lock_guard<std::mutex> lock(prewrite_mutex);
    release_first_prewrite = true;
  }
  prewrite_cv.notify_all();
  first_thread.join();
  second_thread.join();
  third_thread.join();

  ASSERT_TRUE(first_result->ok()) << first_result->status().ToString();
  ASSERT_TRUE(first_result->ValueOrDie().Wait().ok());
  ASSERT_TRUE(second_result->ok()) << second_result->status().ToString();
  ASSERT_TRUE(second_result->ValueOrDie().Wait().ok());
  ASSERT_TRUE(second_result.has_value());
  ASSERT_TRUE(third_result.has_value());
  EXPECT_FALSE(third_result->ok());
  EXPECT_TRUE(third_result->status().IsResourceExhausted());
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(pattern);
}

TEST(KernelAsyncCommitTest, RejectsWhenCedarMailboxIsFullWithoutWaiting) {
  char pattern[] = "/tmp/cedar_kernel_async_mailbox_limit_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  std::mutex prewrite_mutex;
  std::condition_variable prewrite_cv;
  bool first_prewrite_entered = false;
  bool release_first_prewrite = false;
  auto opened = Database::Open(DatabaseOptions{
      .path = pattern,
      .group_commit_max_batch_size = 1,
      .group_commit_window_us = 0,
      .commit_prewrite_fault_injector_for_testing = [&] {
        std::unique_lock<std::mutex> lock(prewrite_mutex);
        if (!first_prewrite_entered) {
          first_prewrite_entered = true;
          prewrite_cv.notify_all();
          prewrite_cv.wait(lock, [&] { return release_first_prewrite; });
        }
        return Status::OK();
      },
      .async_executor = AsyncExecutorOptions{1, 1, 4ULL * 1024ULL * 1024ULL}});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
  auto first = database->BeginTransaction();
  auto second = database->BeginTransaction();
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  ASSERT_TRUE(first.ValueOrDie()->Assert(
                              EntityFact::Vertex(VertexRef{PartId{0}, VertexId{401}}),
                              ValidTime{10})
                  .ok());
  ASSERT_TRUE(second.ValueOrDie()->Assert(
                               EntityFact::Vertex(VertexRef{PartId{0}, VertexId{402}}),
                               ValidTime{10})
                  .ok());

  std::optional<StatusOr<CommitHandle>> first_result;
  std::thread first_thread(
      [&] { first_result.emplace(first.ValueOrDie()->CommitAsync()); });
  {
    std::unique_lock<std::mutex> lock(prewrite_mutex);
    ASSERT_TRUE(prewrite_cv.wait_for(
        lock, std::chrono::seconds(2), [&] { return first_prewrite_entered; }));
  }

  const auto started_at = std::chrono::steady_clock::now();
  const auto second_result = second.ValueOrDie()->CommitAsync();
  EXPECT_FALSE(second_result.ok());
  EXPECT_TRUE(second_result.status().IsResourceExhausted())
      << second_result.status().ToString();
  EXPECT_LT(std::chrono::steady_clock::now() - started_at,
            std::chrono::milliseconds(20));

  {
    std::lock_guard<std::mutex> lock(prewrite_mutex);
    release_first_prewrite = true;
  }
  prewrite_cv.notify_all();
  first_thread.join();
  ASSERT_TRUE(first_result->ok()) << first_result->status().ToString();
  ASSERT_TRUE(first_result->ValueOrDie().Wait().ok());
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(pattern);
}

TEST(KernelGroupCommitTest, CountsSelectedEpochAgainstQueueByteReservation) {
  char pattern[] = "/tmp/cedar_kernel_group_reserved_bytes_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  std::mutex prewrite_mutex;
  std::condition_variable prewrite_cv;
  bool first_prewrite_entered = false;
  bool release_first_prewrite = false;
  auto opened = Database::Open(DatabaseOptions{
      .path = pattern,
      .group_commit_max_batch_size = 1,
      .group_commit_window_us = 0,
      .commit_prewrite_fault_injector_for_testing = [&] {
        std::unique_lock<std::mutex> lock(prewrite_mutex);
        if (!first_prewrite_entered) {
          first_prewrite_entered = true;
          prewrite_cv.notify_all();
          prewrite_cv.wait(lock, [&] { return release_first_prewrite; });
        }
        return Status::OK();
      },
      .group_commit_max_queue_requests = 2,
      .group_commit_max_queue_bytes = 2'200});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();

  auto first = database->BeginTransaction();
  auto second = database->BeginTransaction();
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  ASSERT_TRUE(first.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{301}}),
                                         ValidTime{10})
                  .ok());
  ASSERT_TRUE(second.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{302}}),
                                          ValidTime{10})
                  .ok());

  std::optional<StatusOr<CommitHandle>> first_result;
  std::thread first_thread(
      [&] { first_result.emplace(first.ValueOrDie()->CommitAsync()); });
  bool entered = false;
  {
    std::unique_lock<std::mutex> lock(prewrite_mutex);
    entered = prewrite_cv.wait_for(
        lock, std::chrono::seconds(2), [&] { return first_prewrite_entered; });
  }
  if (!entered) {
    {
      std::lock_guard<std::mutex> lock(prewrite_mutex);
      release_first_prewrite = true;
    }
    prewrite_cv.notify_all();
    first_thread.join();
    ASSERT_TRUE(first_result.has_value());
    FAIL() << (first_result->ok() ? "first commit did not reach prewrite"
                                 : first_result->status().ToString());
  }

  std::optional<StatusOr<CommitHandle>> second_result;
  std::thread second_thread([&] {
    second_result.emplace(second.ValueOrDie()->CommitAsync());
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  {
    std::lock_guard<std::mutex> lock(prewrite_mutex);
    release_first_prewrite = true;
  }
  prewrite_cv.notify_all();
  first_thread.join();
  second_thread.join();
  ASSERT_TRUE(second_result.has_value());
  ASSERT_FALSE(second_result->ok());
  EXPECT_TRUE(second_result->status().IsResourceExhausted())
      << second_result->status().ToString();
  ASSERT_TRUE(first_result->ok()) << first_result->status().ToString();
  ASSERT_TRUE(first_result->ValueOrDie().Wait().ok());
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(pattern);
}

TEST(KernelGroupCommitTest, SyncAdmissionWaitsForQueueSpaceWithinDeadline) {
  char pattern[] = "/tmp/cedar_kernel_soft_wait_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  std::mutex prewrite_mutex;
  std::condition_variable prewrite_cv;
  bool first_prewrite_entered = false;
  bool release_first_prewrite = false;
  auto opened = Database::Open(DatabaseOptions{
      .path = pattern,
      .group_commit_max_batch_size = 1,
      .group_commit_window_us = 0,
      .commit_prewrite_fault_injector_for_testing = [&] {
        std::unique_lock<std::mutex> lock(prewrite_mutex);
        if (!first_prewrite_entered) {
          first_prewrite_entered = true;
          prewrite_cv.notify_all();
          prewrite_cv.wait(lock, [&] { return release_first_prewrite; });
        }
        return Status::OK();
      },
      .group_commit_max_queue_requests = 1,
      .group_commit_max_queue_bytes = 4096});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  auto first = database->BeginTransaction();
  auto second = database->BeginTransaction(
      TransactionOptions{.isolation = IsolationLevel::kSnapshot,
                          .commit_deadline_us = 500'000});
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  ASSERT_TRUE(first.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{501}}),
                                         ValidTime{10})
                  .ok());
  ASSERT_TRUE(second.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{502}}),
                                          ValidTime{10})
                  .ok());
  std::optional<StatusOr<CommitHandle>> first_result;
  std::thread first_thread(
      [&] { first_result.emplace(first.ValueOrDie()->CommitAsync()); });
  {
    std::unique_lock<std::mutex> lock(prewrite_mutex);
    ASSERT_TRUE(prewrite_cv.wait_for(
        lock, std::chrono::seconds(2), [&] { return first_prewrite_entered; }));
  }
  std::optional<StatusOr<CommitResult>> second_result;
  std::thread second_thread(
      [&] { second_result.emplace(second.ValueOrDie()->Commit()); });
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(second_result.has_value());
  {
    std::lock_guard<std::mutex> lock(prewrite_mutex);
    release_first_prewrite = true;
  }
  prewrite_cv.notify_all();
  first_thread.join();
  second_thread.join();
  ASSERT_TRUE(second_result.has_value());
  ASSERT_TRUE(second_result->ok()) << second_result->status().ToString();
  EXPECT_EQ(second_result->ValueOrDie().outcome, CommitOutcome::kCommitted);
  ASSERT_TRUE(first_result.has_value());
  ASSERT_TRUE(first_result->ok()) << first_result->status().ToString();
  ASSERT_TRUE(first_result->ValueOrDie().Wait().ok());
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(pattern);
}

TEST(KernelGroupCommitTest, PreflightsNextEpochAgainstImmutableOverlay) {
  char pattern[] = "/tmp/cedar_kernel_n_plus_one_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  std::mutex prewrite_mutex;
  std::condition_variable prewrite_cv;
  bool first_prewrite_entered = false;
  bool release_first_prewrite = false;
  std::mutex enqueue_mutex;
  std::condition_variable enqueue_cv;
  uint32_t enqueued = 0;
  auto opened = Database::Open(DatabaseOptions{
      .path = pattern,
      .group_commit_max_batch_size = 1,
      .group_commit_window_us = 0,
      .commit_prewrite_fault_injector_for_testing = [&] {
        std::unique_lock<std::mutex> lock(prewrite_mutex);
        if (!first_prewrite_entered) {
          first_prewrite_entered = true;
          prewrite_cv.notify_all();
          prewrite_cv.wait(lock, [&] { return release_first_prewrite; });
        }
        return Status::OK();
      },
      .group_commit_max_queue_requests = 2,
      .group_commit_max_queue_bytes = 4096,
      .append_commit_enqueued_observer_for_testing = [&] {
        std::lock_guard<std::mutex> lock(enqueue_mutex);
        ++enqueued;
        enqueue_cv.notify_all();
      }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  auto first = database->BeginTransaction();
  auto second = database->BeginTransaction();
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  ASSERT_TRUE(first.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{601}}),
                                         ValidTime{10})
                  .ok());
  ASSERT_TRUE(second.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{602}}),
                                          ValidTime{10})
                  .ok());
  std::optional<StatusOr<CommitHandle>> first_result;
  std::optional<StatusOr<CommitHandle>> second_result;
  std::thread first_thread(
      [&] { first_result.emplace(first.ValueOrDie()->CommitAsync()); });
  {
    std::unique_lock<std::mutex> lock(prewrite_mutex);
    ASSERT_TRUE(prewrite_cv.wait_for(
        lock, std::chrono::seconds(2), [&] { return first_prewrite_entered; }));
  }
  std::thread second_thread(
      [&] { second_result.emplace(second.ValueOrDie()->CommitAsync()); });
  {
    std::unique_lock<std::mutex> lock(enqueue_mutex);
    ASSERT_TRUE(enqueue_cv.wait_for(lock, std::chrono::seconds(2),
                                    [&] { return enqueued == 2; }));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  {
    std::lock_guard<std::mutex> lock(prewrite_mutex);
    release_first_prewrite = true;
  }
  prewrite_cv.notify_all();
  first_thread.join();
  second_thread.join();
  ASSERT_TRUE(first_result->ok()) << first_result->status().ToString();
  ASSERT_TRUE(second_result->ok()) << second_result->status().ToString();
  EXPECT_TRUE(first_result->ValueOrDie().Wait().ok());
  EXPECT_TRUE(second_result->ValueOrDie().Wait().ok());
  const CommitPipelineMetrics metrics = database->GetCommitPipelineMetrics();
  EXPECT_GE(metrics.pending_overlay_peak, 1U);
  EXPECT_GE(metrics.n_plus_one_preflight_transactions, 1U);
  EXPECT_GE(metrics.n_plus_one_decided_transactions, 1U);
  EXPECT_EQ(metrics.n_plus_one_preflight_epochs, 1U);
  EXPECT_EQ(metrics.n_plus_one_decided_epochs, 1U);
  EXPECT_EQ(metrics.n_plus_one_eligible_epochs, 1U);
  EXPECT_EQ(metrics.n_plus_one_promoted_epochs, 1U);
  EXPECT_EQ(metrics.n_plus_one_discarded_epochs, 0U);
  EXPECT_GT(metrics.n_plus_one_hidden_cpu_us, 0U);
  EXPECT_GE(metrics.n_plus_one_eligible, 1U);
  EXPECT_GE(metrics.n_plus_one_promoted, 1U);
  uint64_t discard_count = 0;
  for (const uint64_t count : metrics.n_plus_one_discarded_by_reason) {
    discard_count += count;
  }
  EXPECT_EQ(discard_count, 0U);
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(pattern);
}

TEST(KernelGroupCommitTest, CancelsEligibleNPlusOneWhenQueuedRequestTimesOut) {
  char pattern[] = "/tmp/cedar_kernel_n_plus_one_timeout_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  std::mutex prewrite_mutex;
  std::condition_variable prewrite_cv;
  bool first_prewrite_entered = false;
  bool release_first_prewrite = false;
  auto opened = Database::Open(DatabaseOptions{
      .path = pattern,
      .group_commit_max_batch_size = 1,
      .group_commit_window_us = 0,
      .commit_prewrite_fault_injector_for_testing = [&] {
        std::unique_lock<std::mutex> lock(prewrite_mutex);
        if (!first_prewrite_entered) {
          first_prewrite_entered = true;
          prewrite_cv.notify_all();
          prewrite_cv.wait(lock, [&] { return release_first_prewrite; });
        }
        return Status::OK();
      },
      .group_commit_max_queue_requests = 2,
      .group_commit_max_queue_bytes = 4096});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  auto first = database->BeginTransaction();
  auto second = database->BeginTransaction(
      TransactionOptions{.isolation = IsolationLevel::kSnapshot,
                          .commit_deadline_us = 500'000});
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  ASSERT_TRUE(first.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{651}}),
                           ValidTime{10})
                  .ok());
  ASSERT_TRUE(second.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{652}}),
                           ValidTime{10})
                  .ok());

  std::optional<StatusOr<CommitHandle>> first_result;
  std::thread first_thread(
      [&] { first_result.emplace(first.ValueOrDie()->CommitAsync()); });
  {
    std::unique_lock<std::mutex> lock(prewrite_mutex);
    ASSERT_TRUE(prewrite_cv.wait_for(
        lock, std::chrono::seconds(2), [&] { return first_prewrite_entered; }));
  }

  std::optional<StatusOr<CommitHandle>> second_result;
  std::thread second_thread(
      [&] { second_result.emplace(second.ValueOrDie()->CommitAsync()); });
  const auto eligible_deadline = std::chrono::steady_clock::now() +
                                 std::chrono::seconds(2);
  while (std::chrono::steady_clock::now() < eligible_deadline &&
         database->GetCommitPipelineMetrics().n_plus_one_eligible == 0) {
    std::this_thread::yield();
  }
  ASSERT_GE(database->GetCommitPipelineMetrics().n_plus_one_eligible, 1U);
  second_thread.join();
  ASSERT_TRUE(second_result.has_value());
  ASSERT_FALSE(second_result->ok());
  EXPECT_TRUE(second_result->status().IsResourceExhausted())
      << second_result->status().ToString();

  const CommitPipelineMetrics cancelled_metrics =
      database->GetCommitPipelineMetrics();
  const size_t cancelled_index =
      static_cast<size_t>(NPlusOneDiscardReason::kCancelled);
  ASSERT_LT(cancelled_index,
            cancelled_metrics.n_plus_one_discarded_by_reason.size());
  EXPECT_EQ(cancelled_metrics.n_plus_one_discarded_by_reason[cancelled_index],
            1U);
  EXPECT_EQ(cancelled_metrics.n_plus_one_discards, 1U);
  EXPECT_EQ(cancelled_metrics.rejected, 1U);

  {
    std::lock_guard<std::mutex> lock(prewrite_mutex);
    release_first_prewrite = true;
  }
  prewrite_cv.notify_all();
  first_thread.join();
  ASSERT_TRUE(first_result.has_value());
  ASSERT_TRUE(first_result->ok()) << first_result->status().ToString();
  ASSERT_TRUE(first_result->ValueOrDie().Wait().ok());
  const CommitPipelineMetrics drained_metrics = database->GetCommitPipelineMetrics();
  EXPECT_EQ(drained_metrics.async_mailbox_requests_reserved, 0U);
  EXPECT_EQ(drained_metrics.async_mailbox_bytes_reserved, 0U);
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(pattern);
}

TEST(KernelGroupCommitTest, DeadlineDoesNotRejectAfterWriterSelection) {
  char pattern[] = "/tmp/cedar_kernel_selected_deadline_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  std::mutex prewrite_mutex;
  std::condition_variable prewrite_cv;
  bool prewrite_entered = false;
  bool release_prewrite = false;
  auto opened = Database::Open(DatabaseOptions{
      .path = pattern,
      .group_commit_max_batch_size = 1,
      .group_commit_window_us = 0,
      .commit_prewrite_fault_injector_for_testing = [&] {
        std::unique_lock<std::mutex> lock(prewrite_mutex);
        prewrite_entered = true;
        prewrite_cv.notify_all();
        prewrite_cv.wait(lock, [&] { return release_prewrite; });
        return Status::OK();
      }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  auto transaction = database->BeginTransaction(
      TransactionOptions{.isolation = IsolationLevel::kSnapshot,
                          .commit_deadline_us = 100'000});
  ASSERT_TRUE(transaction.ok());
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(
                               VertexRef{PartId{0}, VertexId{653}}),
                           ValidTime{10})
                  .ok());

  std::mutex result_mutex;
  std::condition_variable result_cv;
  bool commit_returned = false;
  std::optional<StatusOr<CommitHandle>> result;
  std::thread committing([&] {
    auto committed = transaction.ValueOrDie()->CommitAsync();
    {
      std::lock_guard<std::mutex> lock(result_mutex);
      result.emplace(std::move(committed));
      commit_returned = true;
    }
    result_cv.notify_all();
  });
  {
    std::unique_lock<std::mutex> lock(prewrite_mutex);
    ASSERT_TRUE(prewrite_cv.wait_for(
        lock, std::chrono::seconds(2), [&] { return prewrite_entered; }));
  }
  {
    std::unique_lock<std::mutex> lock(result_mutex);
    EXPECT_FALSE(result_cv.wait_for(lock, std::chrono::milliseconds(250),
                                    [&] { return commit_returned; }));
  }
  {
    std::lock_guard<std::mutex> lock(prewrite_mutex);
    release_prewrite = true;
  }
  prewrite_cv.notify_all();
  committing.join();

  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->ok()) << result->status().ToString();
  const auto committed = result->ValueOrDie().Wait();
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  EXPECT_EQ(committed.ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_EQ(database->GetCommitPipelineMetrics().rejected, 0U);
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(pattern);
}

void ExerciseNPlusOnePredecessorDiscard(
    const Status& injected, NPlusOneDiscardReason expected_reason) {
  char pattern[] = "/tmp/cedar_kernel_n_plus_one_discard_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::mutex prewrite_mutex;
  std::condition_variable prewrite_cv;
  bool first_prewrite_entered = false;
  bool release_first_prewrite = false;
  std::atomic<bool> inject_first{true};
  std::mutex enqueue_mutex;
  std::condition_variable enqueue_cv;
  uint32_t enqueued = 0;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 1,
      .group_commit_window_us = 0,
      .commit_prewrite_fault_injector_for_testing = [&] {
        if (!inject_first.exchange(false, std::memory_order_acq_rel)) {
          return Status::OK();
        }
        std::unique_lock<std::mutex> lock(prewrite_mutex);
        first_prewrite_entered = true;
        prewrite_cv.notify_all();
        prewrite_cv.wait(lock, [&] { return release_first_prewrite; });
        return injected;
      },
      .group_commit_max_queue_requests = 2,
      .group_commit_max_queue_bytes = 4096,
      .append_commit_enqueued_observer_for_testing = [&] {
        std::lock_guard<std::mutex> lock(enqueue_mutex);
        ++enqueued;
        enqueue_cv.notify_all();
      }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  auto first = database->BeginTransaction();
  auto second = database->BeginTransaction();
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  ASSERT_TRUE(first.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{701}}),
                           ValidTime{10})
                  .ok());
  ASSERT_TRUE(second.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{702}}),
                           ValidTime{10})
                  .ok());
  std::optional<StatusOr<CommitResult>> first_result;
  std::optional<StatusOr<CommitResult>> second_result;
  std::thread first_thread(
      [&] { first_result.emplace(first.ValueOrDie()->Commit()); });
  {
    std::unique_lock<std::mutex> lock(prewrite_mutex);
    ASSERT_TRUE(prewrite_cv.wait_for(
        lock, std::chrono::seconds(2), [&] { return first_prewrite_entered; }));
  }
  std::thread second_thread(
      [&] { second_result.emplace(second.ValueOrDie()->Commit()); });
  {
    std::unique_lock<std::mutex> lock(enqueue_mutex);
    ASSERT_TRUE(enqueue_cv.wait_for(lock, std::chrono::seconds(2),
                                    [&] { return enqueued == 2; }));
  }
  const auto eligible_deadline = std::chrono::steady_clock::now() +
                                 std::chrono::seconds(2);
  bool eligible = false;
  while (std::chrono::steady_clock::now() < eligible_deadline) {
    if (database->GetCommitPipelineMetrics().n_plus_one_eligible >= 1) {
      eligible = true;
      break;
    }
    std::this_thread::yield();
  }
  ASSERT_TRUE(eligible);
  {
    std::lock_guard<std::mutex> lock(prewrite_mutex);
    release_first_prewrite = true;
  }
  prewrite_cv.notify_all();
  first_thread.join();
  second_thread.join();
  ASSERT_TRUE(first_result.has_value());
  ASSERT_TRUE(second_result.has_value());
  ASSERT_TRUE(first_result->ok()) << first_result->status().ToString();
  ASSERT_TRUE(second_result->ok()) << second_result->status().ToString();
  EXPECT_NE(first_result->ValueOrDie().outcome, CommitOutcome::kCommitted);
  if (injected.IsIndeterminate()) {
    EXPECT_NE(second_result->ValueOrDie().outcome, CommitOutcome::kCommitted);
  } else {
    EXPECT_EQ(second_result->ValueOrDie().outcome, CommitOutcome::kCommitted);
  }
  const CommitPipelineMetrics metrics = database->GetCommitPipelineMetrics();
  const size_t reason_index = static_cast<size_t>(expected_reason);
  ASSERT_LT(reason_index, metrics.n_plus_one_discarded_by_reason.size());
  EXPECT_GE(metrics.n_plus_one_discarded_by_reason[reason_index], 1U);
  EXPECT_GE(metrics.n_plus_one_discards, 1U);
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest, DiscardsNPlusOneOnPredecessorFailure) {
  ExerciseNPlusOnePredecessorDiscard(
      Status::InvalidArgument("test", "injected predecessor failure"),
      NPlusOneDiscardReason::kPredecessorFailure);
}

TEST(KernelGroupCommitTest, DiscardsNPlusOneOnPredecessorIndeterminate) {
  ExerciseNPlusOnePredecessorDiscard(
      Status::Indeterminate("test", "injected predecessor uncertainty"),
      NPlusOneDiscardReason::kIndeterminate);
}

TEST(KernelGroupCommitTest, DiscardsNPlusOneOnPredecessorCancellation) {
  ExerciseNPlusOnePredecessorDiscard(
      Status::QueryCancelled("test", "injected predecessor cancellation"),
      NPlusOneDiscardReason::kCancelled);
}

TEST(KernelGroupCommitTest, DiscardsEligibleNPlusOneOnShutdown) {
  char pattern[] = "/tmp/cedar_kernel_n_plus_one_shutdown_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::mutex prewrite_mutex;
  std::condition_variable prewrite_cv;
  bool first_prewrite_entered = false;
  bool release_first_prewrite = false;
  std::mutex shutdown_mutex;
  std::condition_variable shutdown_cv;
  bool stop_reached = false;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 1,
      .group_commit_window_us = 0,
      .commit_prewrite_fault_injector_for_testing = [&] {
        std::unique_lock<std::mutex> lock(prewrite_mutex);
        if (!first_prewrite_entered) {
          first_prewrite_entered = true;
          prewrite_cv.notify_all();
        }
        prewrite_cv.wait(lock, [&] { return release_first_prewrite; });
        return Status::OK();
      },
      .group_commit_max_queue_requests = 2,
      .group_commit_max_queue_bytes = 4096,
      .shutdown_stage_observer_for_testing = [&](const char* stage) {
        if (std::string(stage) != "queue_worker_stop") return;
        {
          std::lock_guard<std::mutex> lock(prewrite_mutex);
          release_first_prewrite = true;
        }
        prewrite_cv.notify_all();
        {
          std::lock_guard<std::mutex> lock(shutdown_mutex);
          stop_reached = true;
        }
        shutdown_cv.notify_all();
      },
      .stop_pipeline_before_drain_for_testing = true});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  auto first = database->BeginTransaction();
  auto second = database->BeginTransaction();
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  ASSERT_TRUE(first.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{801}}),
                           ValidTime{10})
                  .ok());
  ASSERT_TRUE(second.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{802}}),
                           ValidTime{10})
                  .ok());
  std::optional<StatusOr<CommitHandle>> first_handle;
  std::thread first_thread([&] {
    first_handle.emplace(first.ValueOrDie()->CommitAsync());
  });
  {
    std::unique_lock<std::mutex> lock(prewrite_mutex);
    ASSERT_TRUE(prewrite_cv.wait_for(
        lock, std::chrono::seconds(2), [&] { return first_prewrite_entered; }));
  }
  std::optional<StatusOr<CommitHandle>> second_handle;
  std::thread second_thread([&] {
    second_handle.emplace(second.ValueOrDie()->CommitAsync());
  });

  const auto eligible_deadline = std::chrono::steady_clock::now() +
                                 std::chrono::seconds(2);
  while (std::chrono::steady_clock::now() < eligible_deadline &&
         database->GetCommitPipelineMetrics().n_plus_one_eligible == 0) {
    std::this_thread::yield();
  }
  ASSERT_GE(database->GetCommitPipelineMetrics().n_plus_one_eligible, 1U);

  std::thread closing([&] { ASSERT_TRUE(database->Close().ok()); });
  {
    std::unique_lock<std::mutex> lock(shutdown_mutex);
    ASSERT_TRUE(shutdown_cv.wait_for(
        lock, std::chrono::seconds(2), [&] { return stop_reached; }));
  }
  closing.join();
  first_thread.join();
  second_thread.join();
  ASSERT_TRUE(first_handle.has_value());
  ASSERT_TRUE(second_handle.has_value());
  EXPECT_TRUE((!first_handle->ok() && first_handle->status().IsShutdownInProgress()) ||
              first_handle->ok())
      << first_handle->status().ToString();
  EXPECT_TRUE((!second_handle->ok() && second_handle->status().IsShutdownInProgress()) ||
              second_handle->ok())
      << second_handle->status().ToString();
  if (first_handle->ok()) EXPECT_TRUE(first_handle->ValueOrDie().Wait().ok());
  if (second_handle->ok()) EXPECT_TRUE(second_handle->ValueOrDie().Wait().ok());
  const CommitPipelineMetrics metrics = database->GetCommitPipelineMetrics();
  const size_t reason_index = static_cast<size_t>(NPlusOneDiscardReason::kShutdown);
  ASSERT_LT(reason_index, metrics.n_plus_one_discarded_by_reason.size());
  EXPECT_GE(metrics.n_plus_one_discarded_by_reason[reason_index], 1U);
  EXPECT_GE(metrics.n_plus_one_discards, 1U);
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest,
     ShutdownDiscardsNPlusOneWhileManyCallersAreQueuedAndLeavesDatabaseReopenable) {
  constexpr uint32_t kTransactions = 32;
  char pattern[] = "/tmp/cedar_kernel_large_n_plus_one_shutdown_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::mutex prewrite_mutex;
  std::condition_variable prewrite_cv;
  bool first_prewrite_entered = false;
  bool release_first_prewrite = false;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = kTransactions,
      .group_commit_window_us = 0,
      .commit_prewrite_fault_injector_for_testing = [&] {
        std::unique_lock<std::mutex> lock(prewrite_mutex);
        if (!first_prewrite_entered) {
          first_prewrite_entered = true;
          prewrite_cv.notify_all();
          prewrite_cv.wait(lock, [&] { return release_first_prewrite; });
        }
        return Status::OK();
      },
      .group_commit_max_queue_requests = 64,
      .shutdown_stage_observer_for_testing = [&](const char* stage) {
        if (std::string(stage) != "queue_worker_stop") return;
        {
          std::lock_guard<std::mutex> lock(prewrite_mutex);
          release_first_prewrite = true;
        }
        prewrite_cv.notify_all();
      },
      .stop_pipeline_before_drain_for_testing = true});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();

  std::vector<std::optional<StatusOr<CommitHandle>>> submissions(kTransactions);
  const auto submit = [&](uint32_t index) {
    auto transaction = database->BeginTransaction();
    if (!transaction.ok()) {
      submissions[index].emplace(transaction.status());
      return;
    }
    const Status asserted = transaction.ValueOrDie()->Assert(
        EntityFact::Vertex(VertexRef{PartId{0}, VertexId{60'000 + index}}),
        ValidTime{index + 1});
    if (!asserted.ok()) {
      submissions[index].emplace(asserted);
      return;
    }
    submissions[index].emplace(transaction.ValueOrDie()->CommitAsync());
  };

  std::thread first([&] { submit(0); });
  bool first_entered = false;
  {
    std::unique_lock<std::mutex> lock(prewrite_mutex);
    first_entered = prewrite_cv.wait_for(
        lock, std::chrono::seconds(2), [&] { return first_prewrite_entered; });
  }
  if (!first_entered) {
    {
      std::lock_guard<std::mutex> lock(prewrite_mutex);
      release_first_prewrite = true;
    }
    prewrite_cv.notify_all();
    first.join();
    EXPECT_TRUE(database->Close().ok());
    std::filesystem::remove_all(path);
    FAIL() << "first prewrite did not start";
  }
  std::vector<std::thread> followers;
  followers.reserve(kTransactions - 1);
  for (uint32_t index = 1; index < kTransactions; ++index) {
    followers.emplace_back([&, index] { submit(index); });
  }

  const auto eligible_deadline = std::chrono::steady_clock::now() +
                                 std::chrono::seconds(2);
  while (std::chrono::steady_clock::now() < eligible_deadline &&
         database->GetCommitPipelineMetrics().n_plus_one_eligible == 0) {
    std::this_thread::yield();
  }
  EXPECT_GE(database->GetCommitPipelineMetrics().n_plus_one_eligible, 1U);

  const Status closed = database->Close();
  EXPECT_TRUE(closed.ok()) << closed.ToString();
  first.join();
  for (auto& follower : followers) follower.join();
  for (const auto& submission : submissions) {
    ASSERT_TRUE(submission.has_value());
    if (!submission->ok()) {
      EXPECT_TRUE(submission->status().IsShutdownInProgress())
          << submission->status().ToString();
      continue;
    }
    const auto completed = submission->ValueOrDie().Wait();
    ASSERT_TRUE(completed.ok()) << completed.status().ToString();
  }
  const CommitPipelineMetrics metrics = database->GetCommitPipelineMetrics();
  const size_t reason_index = static_cast<size_t>(NPlusOneDiscardReason::kShutdown);
  ASSERT_LT(reason_index, metrics.n_plus_one_discarded_by_reason.size());
  EXPECT_GE(metrics.n_plus_one_discarded_by_reason[reason_index], 1U);
  EXPECT_GE(metrics.n_plus_one_discards, 1U);
  database.reset();

  auto reopened = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
  EXPECT_TRUE(reopened.ValueOrDie()->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest, RetriesStalePredecidedEpochThroughTheNormalWriter) {
  char pattern[] = "/tmp/cedar_kernel_stale_predecision_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 128,
      .group_commit_window_us = 200,
      .group_commit_max_queue_requests = 512,
      .group_commit_max_queue_bytes = 1ULL << 20});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();

  constexpr uint32_t kTransactions = 64;
  std::barrier start(kTransactions + 1);
  std::atomic<bool> failed = false;
  std::vector<std::thread> threads;
  threads.reserve(kTransactions);
  for (uint32_t index = 0; index < kTransactions; ++index) {
    threads.emplace_back([&, index] {
      auto transaction = database->BeginTransaction();
      if (!transaction.ok() ||
          !transaction.ValueOrDie()
               ->Assert(EntityFact::Vertex(
                            VertexRef{PartId{0}, VertexId{index + 10'000}}),
                        ValidTime{index + 1})
               .ok()) {
        failed = true;
        start.arrive_and_wait();
        return;
      }
      start.arrive_and_wait();
      const auto committed = transaction.ValueOrDie()->CommitAsync();
      if (!committed.ok()) {
        failed = true;
        return;
      }
      const auto result = committed.ValueOrDie().Wait();
      if (!result.ok() || result.ValueOrDie().outcome != CommitOutcome::kCommitted) {
        failed = true;
      }
    });
  }
  start.arrive_and_wait();
  for (std::thread& thread : threads) thread.join();

  EXPECT_FALSE(failed.load());
  const CommitPipelineMetrics metrics = database->GetCommitPipelineMetrics();
  EXPECT_EQ(metrics.published, kTransactions);
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest, ConcurrentIndependentGroupsUseOneWalSyncEach) {
  constexpr uint32_t kTransactions = 32;
  char pattern[] = "/tmp/cedar_kernel_large_group_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  std::atomic<uint32_t> physical_writes = 0;
  auto opened = Database::Open(DatabaseOptions{
      .path = pattern,
      .group_commit_max_batch_size = kTransactions,
      .group_commit_window_us = 500'000,
      .commit_prewrite_fault_injector_for_testing = [&] {
        physical_writes.fetch_add(1, std::memory_order_relaxed);
        return Status::OK();
      },
      .group_commit_max_queue_requests = 128});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();

  std::barrier start(kTransactions + 1);
  std::atomic<bool> failed = false;
  std::vector<std::thread> workers;
  workers.reserve(kTransactions);
  for (uint32_t index = 0; index < kTransactions; ++index) {
    workers.emplace_back([&, index] {
      auto transaction = database->BeginTransaction();
      if (!transaction.ok() ||
          !transaction.ValueOrDie()
               ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{20'000 + index}}),
                        ValidTime{index + 1})
               .ok()) {
        failed.store(true, std::memory_order_relaxed);
        start.arrive_and_wait();
        return;
      }
      start.arrive_and_wait();
      const auto accepted = transaction.ValueOrDie()->CommitAsync();
      if (!accepted.ok()) {
        failed.store(true, std::memory_order_relaxed);
        return;
      }
      const auto completed = accepted.ValueOrDie().Wait();
      if (!completed.ok() ||
          completed.ValueOrDie().outcome != CommitOutcome::kCommitted) {
        failed.store(true, std::memory_order_relaxed);
      }
    });
  }
  start.arrive_and_wait();
  for (auto& worker : workers) worker.join();

  EXPECT_FALSE(failed.load(std::memory_order_relaxed));
  const CommitPipelineMetrics metrics = database->GetCommitPipelineMetrics();
  EXPECT_GE(metrics.group_fill.groups, 1U);
  EXPECT_EQ(metrics.group_fill.total_transactions, kTransactions);
  EXPECT_GE(metrics.group_fill.max_transactions, 2U);
  EXPECT_EQ(metrics.latency.wal_sync.count, metrics.group_fill.groups);
  EXPECT_EQ(physical_writes.load(std::memory_order_relaxed),
            metrics.group_fill.groups);
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(pattern);
}

TEST(KernelGroupCommitTest, ConflictingRequestsPreserveConflictUnderHighFanIn) {
  constexpr uint32_t kTransactions = 32;
  char pattern[] = "/tmp/cedar_kernel_group_conflict_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = kTransactions,
      .group_commit_window_us = 500'000,
      .group_commit_max_queue_requests = 128});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();

  std::barrier start(kTransactions + 1);
  std::atomic<bool> failed = false;
  std::vector<std::optional<CommitResult>> conflicting_results(2);
  std::vector<std::thread> workers;
  workers.reserve(kTransactions);
  for (uint32_t index = 0; index < kTransactions; ++index) {
    workers.emplace_back([&, index] {
      auto transaction = database->BeginTransaction();
      const uint64_t vertex_id = index < 2 ? 50'000 : 50'000 + index;
      if (!transaction.ok() ||
          !transaction.ValueOrDie()
               ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{vertex_id}}),
                        ValidTime{1})
               .ok()) {
        failed.store(true, std::memory_order_relaxed);
        start.arrive_and_wait();
        return;
      }
      start.arrive_and_wait();
      const auto committed = transaction.ValueOrDie()->Commit();
      if (!committed.ok() || committed.ValueOrDie().outcome == CommitOutcome::kIndeterminate) {
        failed.store(true, std::memory_order_relaxed);
        return;
      }
      if (index < 2) {
        conflicting_results[index].emplace(committed.ValueOrDie());
      } else if (committed.ValueOrDie().outcome != CommitOutcome::kCommitted) {
        failed.store(true, std::memory_order_relaxed);
      }
    });
  }
  start.arrive_and_wait();
  for (auto& worker : workers) worker.join();

  EXPECT_FALSE(failed.load(std::memory_order_relaxed));
  ASSERT_TRUE(conflicting_results[0].has_value());
  ASSERT_TRUE(conflicting_results[1].has_value());
  const uint32_t conflicting_commits =
      (conflicting_results[0]->outcome == CommitOutcome::kCommitted ? 1U : 0U) +
      (conflicting_results[1]->outcome == CommitOutcome::kCommitted ? 1U : 0U);
  EXPECT_EQ(conflicting_commits, 1U);
  const CommitResult& aborted = conflicting_results[0]->outcome == CommitOutcome::kAborted
                                    ? *conflicting_results[0]
                                    : *conflicting_results[1];
  EXPECT_EQ(aborted.outcome, CommitOutcome::kAborted);
  EXPECT_TRUE(aborted.status.IsConflict()) << aborted.status.ToString();
  const CommitPipelineMetrics metrics = database->GetCommitPipelineMetrics();
  EXPECT_EQ(metrics.group_fill.total_transactions, kTransactions);
  EXPECT_GE(metrics.group_fill.max_transactions, 2U);
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest, UsesOnlyCedarWriteSeam) {
  char pattern[] = "/tmp/cedar_kernel_profile_write_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  std::atomic<uint32_t> kernel_writes{0};
  auto opened = Database::Open(DatabaseOptions{
      .path = pattern,
      .storage_profile = StorageProfile::kProductionAppend,
      .production = ProductionStorageOptions{
          .memory_budget_bytes = 1ULL * 1024ULL * 1024ULL * 1024ULL,
          .kernel_mode = true},
      .runtime_pressure_override_for_testing = [](PressureSample* sample) {
        sample->free_disk_bytes = UINT64_MAX;
        sample->free_disk_percent = 100;
      },
      .kernel_write_observer_for_testing = [&](bool kernel) {
        EXPECT_TRUE(kernel);
        kernel_writes.fetch_add(1, std::memory_order_relaxed);
      },
      .query_runtime = QueryRuntimeOptions{
          .query_workers = 4,
          .reserved_interactive_workers = 1,
          .query_memory_bytes = 32ULL * 1024ULL * 1024ULL,
          .projection_cache_bytes = 32ULL * 1024ULL * 1024ULL,
          .query_delta_bytes = 32ULL * 1024ULL * 1024ULL}});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  auto transaction = database->BeginTransaction();
  ASSERT_TRUE(transaction.ok());
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{71}}),
                           ValidTime{7})
                  .ok());
  const auto committed = transaction.ValueOrDie()->Commit();
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  EXPECT_EQ(committed.ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_EQ(kernel_writes.load(std::memory_order_relaxed), 1U);
  const CommitPipelineMetrics metrics = database->GetCommitPipelineMetrics();
  EXPECT_EQ(metrics.latency.wal_append.count, 1U);
  EXPECT_EQ(metrics.latency.wal_sync.count, 1U);
  EXPECT_EQ(metrics.latency.manifest.count, 1U);
  EXPECT_EQ(metrics.latency.memtable_insert.count, 1U);
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(pattern);
}

TEST(KernelGroupCommitTest, NeverExceedsConfiguredPhysicalBatchSize) {
  constexpr uint32_t kTransactions = 32;
  constexpr uint32_t kMaximumBatchSize = 2;
  char pattern[] = "/tmp/cedar_kernel_bounded_group_commit_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::atomic<uint32_t> physical_writes = 0;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = kMaximumBatchSize,
      .group_commit_window_us = 200'000,
      .commit_prewrite_fault_injector_for_testing = [&physical_writes] {
        ++physical_writes;
        return Status::OK();
      }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();

  std::barrier start(kTransactions + 1);
  std::atomic<bool> failed = false;
  std::vector<std::thread> threads;
  threads.reserve(kTransactions);
  for (uint32_t index = 0; index < kTransactions; ++index) {
    threads.emplace_back([&, index] {
      auto transaction = database->BeginTransaction();
      if (!transaction.ok() ||
          !transaction.ValueOrDie()
               ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{index + 1}}),
                        ValidTime{index + 1})
               .ok()) {
        failed = true;
        start.arrive_and_wait();
        return;
      }
      start.arrive_and_wait();
      const auto committed = transaction.ValueOrDie()->Commit();
      if (!committed.ok() ||
          committed.ValueOrDie().outcome != CommitOutcome::kCommitted) {
        failed = true;
      }
    });
  }
  start.arrive_and_wait();
  for (std::thread& thread : threads) thread.join();

  EXPECT_FALSE(failed.load());
  EXPECT_GE(physical_writes.load(), kTransactions / kMaximumBatchSize);
  ASSERT_TRUE(database->Close().ok());
  database.reset();
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest, CommitsFullBatchWithoutWaitingForCollectionWindow) {
  char pattern[] = "/tmp/cedar_kernel_group_commit_window_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::atomic<uint32_t> physical_writes = 0;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 2,
      .group_commit_window_us = 200'000,
      .commit_prewrite_fault_injector_for_testing = [&physical_writes] {
        ++physical_writes;
        return Status::OK();
      }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();

  auto first = database->BeginTransaction();
  auto second = database->BeginTransaction();
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  ASSERT_TRUE(second.ok()) << second.status().ToString();
  ASSERT_TRUE(first.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10})
                  .ok());
  ASSERT_TRUE(second.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}), ValidTime{20})
                  .ok());

  std::optional<StatusOr<CommitResult>> first_result;
  std::optional<StatusOr<CommitResult>> second_result;
  std::barrier start(3);
  const auto started = std::chrono::steady_clock::now();
  std::thread first_thread([&] {
    start.arrive_and_wait();
    first_result.emplace(first.ValueOrDie()->Commit());
  });
  std::thread second_thread([&] {
    start.arrive_and_wait();
    second_result.emplace(second.ValueOrDie()->Commit());
  });
  start.arrive_and_wait();
  first_thread.join();
  second_thread.join();
  const auto elapsed = std::chrono::steady_clock::now() - started;

  ASSERT_TRUE(first_result->ok()) << first_result->status().ToString();
  ASSERT_TRUE(second_result->ok()) << second_result->status().ToString();
  EXPECT_EQ(first_result->ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_EQ(second_result->ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_EQ(physical_writes.load(), 1U);
  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count(),
            100);
  ASSERT_TRUE(database->Close().ok());
  database.reset();
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest, DoesNotGroupOverlappingIntervalsForOneFact) {
  char pattern[] = "/tmp/cedar_kernel_group_conflict_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 2,
      .group_commit_window_us = 20'000});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();

  auto first = database->BeginTransaction();
  auto second = database->BeginTransaction();
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  ASSERT_TRUE(second.ok()) << second.status().ToString();
  ASSERT_TRUE(first.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10})
                  .ok());
  ASSERT_TRUE(second.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{20})
                  .ok());

  std::optional<StatusOr<CommitResult>> first_result;
  std::optional<StatusOr<CommitResult>> second_result;
  std::thread first_thread([&first, &first_result] {
    first_result.emplace(first.ValueOrDie()->Commit());
  });
  std::thread second_thread([&second, &second_result] {
    second_result.emplace(second.ValueOrDie()->Commit());
  });
  first_thread.join();
  second_thread.join();

  ASSERT_TRUE(first_result->ok()) << first_result->status().ToString();
  ASSERT_TRUE(second_result->ok()) << second_result->status().ToString();
  const auto& first_commit = first_result->ValueOrDie();
  const auto& second_commit = second_result->ValueOrDie();
  EXPECT_NE(first_commit.outcome, second_commit.outcome);
  EXPECT_TRUE(first_commit.outcome == CommitOutcome::kAborted ||
              second_commit.outcome == CommitOutcome::kAborted);
  database.reset();
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest, GroupsIndependentPropertyTransactions) {
  char pattern[] = "/tmp/cedar_kernel_group_property_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::atomic<uint32_t> physical_writes = 0;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 2,
      .group_commit_window_us = 20'000,
      .commit_prewrite_fault_injector_for_testing = [&physical_writes] {
        ++physical_writes;
        return Status::OK();
      }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  ASSERT_TRUE(database->RegisterProperty(PropertyDefinition{
      PropertyId{9}, 0, "score", PropertyEntityKind::kVertex,
      PhysicalType::kInt64, 4096})
                  .ok());

  auto first = database->BeginTransaction();
  auto second = database->BeginTransaction();
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  ASSERT_TRUE(first.ValueOrDie()
                  ->Set(PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{9}),
                        ValidTime{10}, Value::Int64(1))
                  .ok());
  ASSERT_TRUE(second.ValueOrDie()
                  ->Set(PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{2}}, PropertyId{9}),
                        ValidTime{10}, Value::Int64(2))
                  .ok());

  std::optional<StatusOr<CommitResult>> first_result;
  std::optional<StatusOr<CommitResult>> second_result;
  std::thread first_thread([&] { first_result.emplace(first.ValueOrDie()->Commit()); });
  std::thread second_thread([&] { second_result.emplace(second.ValueOrDie()->Commit()); });
  first_thread.join();
  second_thread.join();
  ASSERT_TRUE(first_result->ok()) << first_result->status().ToString();
  ASSERT_TRUE(second_result->ok()) << second_result->status().ToString();
  EXPECT_EQ(first_result->ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_EQ(second_result->ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_EQ(physical_writes.load(), 1U);
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest, GroupsIndependentMultiFactTransactions) {
  char pattern[] = "/tmp/cedar_kernel_group_multifact_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::atomic<uint32_t> physical_writes = 0;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 2,
      .group_commit_window_us = 20'000,
      .commit_prewrite_fault_injector_for_testing = [&physical_writes] {
        ++physical_writes;
        return Status::OK();
      }});
  ASSERT_TRUE(opened.ok());
  auto database = std::move(opened).ConsumeValueOrDie();
  auto first = database->BeginTransaction();
  auto second = database->BeginTransaction();
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  ASSERT_TRUE(first.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{11}}), ValidTime{10}).ok());
  ASSERT_TRUE(first.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{12}}), ValidTime{10}).ok());
  ASSERT_TRUE(second.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{21}}), ValidTime{10}).ok());
  ASSERT_TRUE(second.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{22}}), ValidTime{10}).ok());

  std::optional<StatusOr<CommitResult>> first_result;
  std::optional<StatusOr<CommitResult>> second_result;
  std::thread first_thread([&] { first_result.emplace(first.ValueOrDie()->Commit()); });
  std::thread second_thread([&] { second_result.emplace(second.ValueOrDie()->Commit()); });
  first_thread.join();
  second_thread.join();
  ASSERT_TRUE(first_result->ok()) << first_result->status().ToString();
  ASSERT_TRUE(second_result->ok()) << second_result->status().ToString();
  EXPECT_EQ(physical_writes.load(), 1U);
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest, GroupsDeletesAndPutsForDifferentFacts) {
  char pattern[] = "/tmp/cedar_kernel_group_delete_put_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::atomic<uint32_t> physical_writes = 0;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 2,
      .group_commit_window_us = 20'000,
      .commit_prewrite_fault_injector_for_testing = [&physical_writes] {
        ++physical_writes;
        return Status::OK();
      }});
  ASSERT_TRUE(opened.ok());
  auto database = std::move(opened).ConsumeValueOrDie();
  auto seed = database->BeginTransaction();
  ASSERT_TRUE(seed.ok());
  ASSERT_TRUE(seed.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{31}}), ValidTime{10}).ok());
  ASSERT_TRUE(seed.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{32}}), ValidTime{10}).ok());
  ASSERT_EQ(seed.ValueOrDie()->Commit().ValueOrDie().outcome, CommitOutcome::kCommitted);
  physical_writes.store(0);

  auto first = database->BeginTransaction();
  auto second = database->BeginTransaction();
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  ASSERT_TRUE(first.ValueOrDie()->Retract(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{31}}), ValidTime{10}).ok());
  ASSERT_TRUE(second.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{32}}), ValidTime{20}).ok());
  std::optional<StatusOr<CommitResult>> first_result;
  std::optional<StatusOr<CommitResult>> second_result;
  std::thread first_thread([&] { first_result.emplace(first.ValueOrDie()->Commit()); });
  std::thread second_thread([&] { second_result.emplace(second.ValueOrDie()->Commit()); });
  first_thread.join();
  second_thread.join();
  ASSERT_TRUE(first_result->ok()) << first_result->status().ToString();
  ASSERT_TRUE(second_result->ok()) << second_result->status().ToString();
  EXPECT_EQ(physical_writes.load(), 1U);
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest, StopsAtFirstConflictingRequestWithoutStarvation) {
  char pattern[] = "/tmp/cedar_kernel_group_fairness_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto opened = Database::Open(DatabaseOptions{
      .path = path, .group_commit_max_batch_size = 3,
      .group_commit_window_us = 20'000});
  ASSERT_TRUE(opened.ok());
  auto database = std::move(opened).ConsumeValueOrDie();
  std::vector<std::unique_ptr<Transaction>> transactions;
  for (int i = 0; i < 3; ++i) {
    auto transaction = database->BeginTransaction();
    ASSERT_TRUE(transaction.ok());
    transactions.push_back(std::move(transaction).ConsumeValueOrDie());
  }
  ASSERT_TRUE(transactions[0]->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{41}}), ValidTime{10}).ok());
  ASSERT_TRUE(transactions[1]->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{41}}), ValidTime{10}).ok());
  ASSERT_TRUE(transactions[2]->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{42}}), ValidTime{10}).ok());
  std::vector<StatusOr<CommitResult>> results(3);
  std::vector<std::thread> threads;
  for (int i = 0; i < 3; ++i) {
    threads.emplace_back([&, i] { results[i] = transactions[i]->Commit(); });
  }
  for (auto& thread : threads) thread.join();
  for (const auto& result : results) ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_NE(results[0].ValueOrDie().outcome, results[1].ValueOrDie().outcome);
  EXPECT_TRUE(results[0].ValueOrDie().outcome == CommitOutcome::kCommitted ||
              results[1].ValueOrDie().outcome == CommitOutcome::kCommitted);
  EXPECT_EQ(results[2].ValueOrDie().outcome, CommitOutcome::kCommitted);
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest, DoesNotGroupStrictReaderWithIntersectingWriter) {
  char pattern[] = "/tmp/cedar_kernel_group_strict_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::atomic<uint32_t> physical_writes = 0;
  auto opened = Database::Open(DatabaseOptions{
      .path = path, .group_commit_max_batch_size = 2,
      .group_commit_window_us = 20'000,
      .commit_prewrite_fault_injector_for_testing = [&physical_writes] {
        ++physical_writes;
        return Status::OK();
      }});
  ASSERT_TRUE(opened.ok());
  auto database = std::move(opened).ConsumeValueOrDie();
  auto strict = database->BeginTransaction(TransactionOptions{.isolation = IsolationLevel::kStrict});
  auto writer = database->BeginTransaction();
  ASSERT_TRUE(strict.ok());
  ASSERT_TRUE(writer.ok());
  ASSERT_TRUE(strict.ValueOrDie()->Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{51}}), ValidTime{10}).ok());
  ASSERT_TRUE(strict.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{52}}), ValidTime{10}).ok());
  ASSERT_TRUE(writer.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{51}}), ValidTime{10}).ok());
  std::optional<StatusOr<CommitResult>> strict_result;
  std::optional<StatusOr<CommitResult>> writer_result;
  std::thread strict_thread([&] { strict_result.emplace(strict.ValueOrDie()->Commit()); });
  std::this_thread::sleep_for(std::chrono::milliseconds(1));
  std::thread writer_thread([&] { writer_result.emplace(writer.ValueOrDie()->Commit()); });
  strict_thread.join();
  writer_thread.join();
  ASSERT_TRUE(strict_result->ok()) << strict_result->status().ToString();
  ASSERT_TRUE(writer_result->ok()) << writer_result->status().ToString();
  EXPECT_EQ(physical_writes.load(), 2U);
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest, PreservesEdgeIdentityValidationForGroupedRequests) {
  char pattern[] = "/tmp/cedar_kernel_group_edge_identity_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto opened = Database::Open(DatabaseOptions{
      .path = path, .group_commit_max_batch_size = 2,
      .group_commit_window_us = 20'000});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();

  auto seed = database->BeginTransaction();
  ASSERT_TRUE(seed.ok());
  ASSERT_TRUE(seed.ValueOrDie()
                  ->Assert(EdgeIdentity{EdgeId{61}, VertexId{1}, VertexId{2}, 7},
                           ValidTime{10})
                  .ok());
  ASSERT_EQ(seed.ValueOrDie()->Commit().ValueOrDie().outcome,
            CommitOutcome::kCommitted);

  auto invalid_rebind = database->BeginTransaction();
  auto independent = database->BeginTransaction();
  ASSERT_TRUE(invalid_rebind.ok());
  ASSERT_TRUE(independent.ok());
  ASSERT_TRUE(invalid_rebind.ValueOrDie()
                  ->Assert(EdgeIdentity{EdgeId{61}, VertexId{1}, VertexId{3}, 7},
                           ValidTime{20})
                  .ok());
  ASSERT_TRUE(independent.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{62}}), ValidTime{10})
                  .ok());
  std::optional<StatusOr<CommitResult>> invalid_result;
  std::optional<StatusOr<CommitResult>> independent_result;
  std::thread invalid_thread([&] {
    invalid_result.emplace(invalid_rebind.ValueOrDie()->Commit());
  });
  std::thread independent_thread([&] {
    independent_result.emplace(independent.ValueOrDie()->Commit());
  });
  invalid_thread.join();
  independent_thread.join();

  ASSERT_TRUE(invalid_result->ok()) << invalid_result->status().ToString();
  ASSERT_TRUE(independent_result->ok()) << independent_result->status().ToString();
  EXPECT_EQ(invalid_result->ValueOrDie().outcome, CommitOutcome::kAborted);
  EXPECT_TRUE(invalid_result->ValueOrDie().status.IsIdentityConflict())
      << invalid_result->ValueOrDie().status.ToString();
  EXPECT_EQ(independent_result->ValueOrDie().outcome, CommitOutcome::kCommitted);
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest, AssignsConsecutiveSequencesAndRecoversEveryMember) {
  char pattern[] = "/tmp/cedar_kernel_group_reopen_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto opened = Database::Open(DatabaseOptions{
      .path = path, .group_commit_max_batch_size = 2,
      .group_commit_window_us = 20'000});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  auto first = database->BeginTransaction();
  auto second = database->BeginTransaction();
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  ASSERT_TRUE(first.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{71}}), ValidTime{10}).ok());
  ASSERT_TRUE(first.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{72}}), ValidTime{10}).ok());
  ASSERT_TRUE(second.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{73}}), ValidTime{10}).ok());
  ASSERT_TRUE(second.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{74}}), ValidTime{10}).ok());
  std::optional<StatusOr<CommitResult>> first_result;
  std::optional<StatusOr<CommitResult>> second_result;
  std::thread first_thread([&] { first_result.emplace(first.ValueOrDie()->Commit()); });
  std::thread second_thread([&] { second_result.emplace(second.ValueOrDie()->Commit()); });
  first_thread.join();
  second_thread.join();
  ASSERT_TRUE(first_result->ok()) << first_result->status().ToString();
  ASSERT_TRUE(second_result->ok()) << second_result->status().ToString();
  EXPECT_EQ(first_result->ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_EQ(second_result->ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_EQ(std::max(first_result->ValueOrDie().commit_seq.value,
                     second_result->ValueOrDie().commit_seq.value) -
                std::min(first_result->ValueOrDie().commit_seq.value,
                         second_result->ValueOrDie().commit_seq.value),
            1U);
  const TxnId first_id = first_result->ValueOrDie().txn_id;
  const TxnId second_id = second_result->ValueOrDie().txn_id;
  ASSERT_TRUE(database->Close().ok());
  database.reset();

  auto reopened = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
  database = std::move(reopened).ConsumeValueOrDie();
  const auto first_recovered = database->ResolveTransaction(first_id);
  const auto second_recovered = database->ResolveTransaction(second_id);
  ASSERT_TRUE(first_recovered.ok()) << first_recovered.status().ToString();
  ASSERT_TRUE(second_recovered.ok()) << second_recovered.status().ToString();
  EXPECT_TRUE(first_recovered.ValueOrDie().has_value());
  EXPECT_TRUE(second_recovered.ValueOrDie().has_value());
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitFailureTest, PostwriteIndeterminateMarksEveryMemberThenReopenResolves) {
  char pattern[] = "/tmp/cedar_kernel_group_indeterminate_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto opened = Database::Open(DatabaseOptions{
      .path = path, .group_commit_max_batch_size = 2,
      .group_commit_window_us = 20'000,
      .commit_fault_injector_for_testing = [] {
        return Status::Indeterminate("test", "injected after durable write");
      }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  auto first = database->BeginTransaction();
  auto second = database->BeginTransaction();
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  ASSERT_TRUE(first.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{81}}), ValidTime{10}).ok());
  ASSERT_TRUE(second.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{82}}), ValidTime{10}).ok());
  std::optional<StatusOr<CommitResult>> first_result;
  std::optional<StatusOr<CommitResult>> second_result;
  std::thread first_thread([&] { first_result.emplace(first.ValueOrDie()->Commit()); });
  std::thread second_thread([&] { second_result.emplace(second.ValueOrDie()->Commit()); });
  first_thread.join();
  second_thread.join();
  ASSERT_TRUE(first_result->ok()) << first_result->status().ToString();
  ASSERT_TRUE(second_result->ok()) << second_result->status().ToString();
  EXPECT_EQ(first_result->ValueOrDie().outcome, CommitOutcome::kIndeterminate);
  EXPECT_EQ(second_result->ValueOrDie().outcome, CommitOutcome::kIndeterminate);
  const TxnId first_id = first_result->ValueOrDie().txn_id;
  const TxnId second_id = second_result->ValueOrDie().txn_id;
  ASSERT_TRUE(database->Close().ok());
  database.reset();

  auto reopened = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
  database = std::move(reopened).ConsumeValueOrDie();
  const auto first_recovered = database->ResolveTransaction(first_id);
  const auto second_recovered = database->ResolveTransaction(second_id);
  ASSERT_TRUE(first_recovered.ok()) << first_recovered.status().ToString();
  ASSERT_TRUE(second_recovered.ok()) << second_recovered.status().ToString();
  ASSERT_TRUE(first_recovered.ValueOrDie().has_value());
  ASSERT_TRUE(second_recovered.ValueOrDie().has_value());
  EXPECT_EQ(first_recovered.ValueOrDie()->outcome, CommitOutcome::kCommitted);
  EXPECT_EQ(second_recovered.ValueOrDie()->outcome, CommitOutcome::kCommitted);
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitFailureTest,
     ConcurrentIndeterminateRecoveryMatchesDurableAcceptance) {
  constexpr uint32_t kTransactions = 32;
  char pattern[] = "/tmp/cedar_kernel_large_group_indeterminate_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::mutex enqueue_mutex;
  std::condition_variable enqueue_cv;
  std::atomic<uint32_t> enqueued = 0;
  std::atomic<uint32_t> physical_writes = 0;
  std::atomic<bool> physical_write_was_unsynced = false;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = kTransactions,
      .group_commit_window_us = 500'000,
      .commit_fault_injector_for_testing = [] {
        return Status::Indeterminate("test", "injected after durable group write");
      },
      .commit_write_options_observer_for_testing = [&](bool sync) {
        physical_writes.fetch_add(1, std::memory_order_relaxed);
        if (!sync) physical_write_was_unsynced.store(true, std::memory_order_relaxed);
      },
      .group_commit_max_queue_requests = 128,
      .append_commit_enqueued_observer_for_testing = [&] {
        enqueued.fetch_add(1, std::memory_order_relaxed);
        enqueue_cv.notify_all();
      },
      .append_commit_collection_observer_for_testing = [&] {
        std::unique_lock<std::mutex> lock(enqueue_mutex);
        enqueue_cv.wait_for(lock, std::chrono::seconds(2), [&] {
          return enqueued.load(std::memory_order_relaxed) == kTransactions;
        });
      }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();

  std::barrier start(kTransactions + 1);
  std::atomic<bool> failed = false;
  std::atomic<uint32_t> durable_handle_count = 0;
  std::vector<std::optional<TxnId>> durably_accepted_txn_ids(kTransactions);
  std::vector<std::thread> workers;
  workers.reserve(kTransactions);
  for (uint32_t index = 0; index < kTransactions; ++index) {
    workers.emplace_back([&, index] {
      auto transaction = database->BeginTransaction();
      if (!transaction.ok() ||
          !transaction.ValueOrDie()
               ->Assert(EntityFact::Vertex(
                            VertexRef{PartId{0}, VertexId{40'000 + index}}),
                        ValidTime{index + 1})
               .ok()) {
        failed.store(true, std::memory_order_relaxed);
        start.arrive_and_wait();
        return;
      }
      start.arrive_and_wait();
      const auto accepted = transaction.ValueOrDie()->CommitAsync();
      if (!accepted.ok()) {
        if (!accepted.status().IsIndeterminate() &&
            !accepted.status().IsRecoveryRequired()) {
          failed.store(true, std::memory_order_relaxed);
        }
        return;
      }
      durably_accepted_txn_ids[index] = accepted.ValueOrDie().txn_id();
      durable_handle_count.fetch_add(1, std::memory_order_relaxed);
      const auto completed = accepted.ValueOrDie().Wait();
      if (!completed.ok() ||
          completed.ValueOrDie().outcome != CommitOutcome::kIndeterminate) {
        failed.store(true, std::memory_order_relaxed);
        return;
      }
    });
  }
  start.arrive_and_wait();
  for (auto& worker : workers) worker.join();

  EXPECT_FALSE(failed.load(std::memory_order_relaxed));
  EXPECT_EQ(enqueued.load(std::memory_order_relaxed), kTransactions);
  const CommitPipelineMetrics metrics = database->GetCommitPipelineMetrics();
  EXPECT_EQ(metrics.group_fill.total_transactions, kTransactions);
  EXPECT_EQ(metrics.group_fill.max_transactions, kTransactions);
  ASSERT_EQ(durable_handle_count.load(std::memory_order_relaxed), kTransactions);
  EXPECT_EQ(metrics.durably_accepted, kTransactions);
  EXPECT_EQ(physical_writes.load(std::memory_order_relaxed), 1U);
  EXPECT_FALSE(physical_write_was_unsynced.load(std::memory_order_relaxed));
  std::unordered_set<uint64_t> durable_txn_values;
  for (const std::optional<TxnId>& txn_id : durably_accepted_txn_ids) {
    ASSERT_TRUE(txn_id.has_value());
    EXPECT_TRUE(txn_id->valid());
    EXPECT_TRUE(durable_txn_values.insert(txn_id->value).second)
        << "duplicate durable transaction id=" << txn_id->value;
  }
  ASSERT_EQ(durable_txn_values.size(), kTransactions);
  ASSERT_TRUE(database->Close().ok());
  database.reset();

  auto reopened = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
  database = std::move(reopened).ConsumeValueOrDie();
  for (const std::optional<TxnId>& accepted : durably_accepted_txn_ids) {
    ASSERT_TRUE(accepted.has_value());
    const auto resolved = database->ResolveTransaction(*accepted);
    ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
    ASSERT_TRUE(resolved.ValueOrDie().has_value())
        << "txn=" << accepted->value;
    EXPECT_EQ(resolved.ValueOrDie()->outcome, CommitOutcome::kCommitted);
  }
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitFailureTest,
     CollectionObserverDoesNotRunAfterAppendPipelineStops) {
  char pattern[] = "/tmp/cedar_kernel_collection_stop_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::atomic<uint32_t> collection_observations = 0;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .append_commit_collection_observer_for_testing = [&] {
        collection_observations.fetch_add(1, std::memory_order_relaxed);
      }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();

  ASSERT_TRUE(database->Close().ok());
  EXPECT_EQ(collection_observations.load(std::memory_order_relaxed), 0U);
  std::filesystem::remove_all(path);
}

TEST_F(KernelCommitTest, BindsEdgeIdentityAtomicallyAndRejectsConflictingBinding) {
  const EdgeIdentity identity{EdgeId{9}, VertexId{1}, VertexId{2}, 7};
  std::unique_ptr<Transaction> first = Begin();
  ASSERT_TRUE(first->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10}).ok());
  ASSERT_TRUE(first->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}), ValidTime{10}).ok());
  ASSERT_TRUE(first->Assert(identity, ValidTime{10}).ok());
  const auto first_commit = first->Commit();
  ASSERT_TRUE(first_commit.ok()) << first_commit.status().ToString();
  EXPECT_EQ(first_commit.ValueOrDie().outcome, CommitOutcome::kCommitted);

  const auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  EXPECT_TRUE(snapshot.ValueOrDie().Exists(EntityFact::Edge(EdgeRef{PartId{0}, EdgeId{9}}), ValidTime{10})
                  .ValueOrDie());

  std::unique_ptr<Transaction> reasserted = Begin();
  ASSERT_TRUE(reasserted->Assert(identity, ValidTime{20}).ok());
  const auto reasserted_commit = reasserted->Commit();
  ASSERT_TRUE(reasserted_commit.ok()) << reasserted_commit.status().ToString();
  EXPECT_EQ(reasserted_commit.ValueOrDie().outcome, CommitOutcome::kCommitted);

  std::unique_ptr<Transaction> conflicting = Begin();
  ASSERT_TRUE(conflicting->Assert(
                  EdgeIdentity{EdgeId{9}, VertexId{1}, VertexId{3}, 7}, ValidTime{20})
                  .ok());
  const auto rejected = conflicting->Commit();
  ASSERT_TRUE(rejected.ok()) << rejected.status().ToString();
  EXPECT_EQ(rejected.ValueOrDie().outcome, CommitOutcome::kAborted);
  EXPECT_TRUE(rejected.ValueOrDie().status.IsIdentityConflict())
      << rejected.ValueOrDie().status.ToString();
}

TEST_F(KernelCommitTest, RejectsStrictCommitWhenAnExactReadChanges) {
  std::unique_ptr<Transaction> seed = Begin();
  ASSERT_TRUE(seed->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10}).ok());
  ASSERT_EQ(seed->Commit().ValueOrDie().outcome, CommitOutcome::kCommitted);

  std::unique_ptr<Transaction> strict = Begin(
      TransactionOptions{.isolation = IsolationLevel::kStrict});
  ASSERT_TRUE(strict->Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10})
                  .ValueOrDie());
  ASSERT_TRUE(strict->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}), ValidTime{10}).ok());

  std::unique_ptr<Transaction> concurrent = Begin();
  ASSERT_TRUE(concurrent->Retract(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10}).ok());
  ASSERT_EQ(concurrent->Commit().ValueOrDie().outcome, CommitOutcome::kCommitted);

  const auto rejected = strict->Commit();
  ASSERT_TRUE(rejected.ok()) << rejected.status().ToString();
  EXPECT_EQ(rejected.ValueOrDie().outcome, CommitOutcome::kAborted);
  EXPECT_TRUE(rejected.ValueOrDie().status.IsConflict())
      << rejected.ValueOrDie().status.ToString();
}

TEST_F(KernelCommitTest, SnapshotTransactionScanExcludesLaterCommits) {
  std::unique_ptr<Transaction> seed = Begin();
  ASSERT_TRUE(seed->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10}).ok());
  ASSERT_EQ(seed->Commit().ValueOrDie().outcome, CommitOutcome::kCommitted);

  std::unique_ptr<Transaction> snapshot = Begin();

  std::unique_ptr<Transaction> concurrent = Begin();
  ASSERT_TRUE(concurrent->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}), ValidTime{20}).ok());
  ASSERT_EQ(concurrent->Commit().ValueOrDie().outcome, CommitOutcome::kCommitted);

  std::vector<FactEvent> events;
  ASSERT_TRUE(snapshot
                  ->Scan(FactFamily::kVertexState, PropertyId{},
                         [&events](const FactEvent& event) {
                           events.push_back(event);
                           return Status::OK();
                         })
                  .ok());
  ASSERT_EQ(events.size(), 1U);
  EXPECT_EQ(events[0].ref, EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}).ref());
  EXPECT_EQ(events[0].commit_seq, CommitSeq{1});
  EXPECT_TRUE(snapshot->Rollback().ok());
}

TEST_F(KernelCommitTest, PersistsPublicCommitAndTransactionIdAcrossReopen) {
  std::unique_ptr<Transaction> transaction = Begin();
  ASSERT_TRUE(transaction->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10}).ok());
  const auto committed = transaction->Commit();
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  const TxnId committed_id = committed.ValueOrDie().txn_id;
  ASSERT_TRUE(database_->Close().ok());
  database_.reset();

  auto reopened = Database::Open(DatabaseOptions{.path = path_});
  ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
  database_ = std::move(reopened).ConsumeValueOrDie();
  const auto resolved = database_->ResolveTransaction(committed_id);
  ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
  ASSERT_TRUE(resolved.ValueOrDie().has_value());
  EXPECT_EQ(resolved.ValueOrDie()->commit_seq, CommitSeq{1});
  const auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  EXPECT_TRUE(snapshot.ValueOrDie().Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10})
                  .ValueOrDie());

  std::unique_ptr<Transaction> next = Begin();
  const Status staged = next->Assert(
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}), ValidTime{10});
  ASSERT_TRUE(staged.ok()) << staged.ToString();
  const auto next_commit = next->Commit();
  ASSERT_TRUE(next_commit.ok()) << next_commit.status().ToString();
  EXPECT_NE(next_commit.ValueOrDie().txn_id, committed_id);
}

TEST(KernelCommitFailureTest, RecoversAnIndeterminateCommittedBatchAfterReopen) {
  char pattern[] = "/tmp/cedar_kernel_commit_fault_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto database = Database::Open(DatabaseOptions{
      .path = path,
      .commit_fault_injector_for_testing = [] {
        return Status::Indeterminate("test", "injected uncertain write");
      },
  });
  ASSERT_TRUE(database.ok()) << database.status().ToString();

  auto first = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  ASSERT_TRUE(first.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10})
                  .ok());
  ASSERT_TRUE(first.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{3}}), ValidTime{10})
                  .ok());
  const auto indeterminate = first.ValueOrDie()->Commit();
  ASSERT_TRUE(indeterminate.ok()) << indeterminate.status().ToString();
  EXPECT_EQ(indeterminate.ValueOrDie().outcome, CommitOutcome::kIndeterminate);
  EXPECT_TRUE(indeterminate.ValueOrDie().status.IsIndeterminate());
  const TxnId indeterminate_id = indeterminate.ValueOrDie().txn_id;

  auto second = database.ValueOrDie()->BeginTransaction();
  EXPECT_TRUE(second.status().IsRecoveryRequired()) << second.status().ToString();

  database.ValueOrDie().reset();
  auto reopened = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
  const auto resolved = reopened.ValueOrDie()->ResolveTransaction(indeterminate_id);
  ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
  ASSERT_TRUE(resolved.ValueOrDie().has_value());
  EXPECT_EQ(resolved.ValueOrDie()->outcome, CommitOutcome::kCommitted);
  {
    const auto snapshot = reopened.ValueOrDie()->BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    EXPECT_TRUE(snapshot.ValueOrDie().Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10})
                    .ValueOrDie());
    EXPECT_TRUE(snapshot.ValueOrDie().Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{3}}), ValidTime{10})
                    .ValueOrDie());
  }
  auto recovered = reopened.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(recovered.ok()) << recovered.status().ToString();
  ASSERT_TRUE(recovered.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}), ValidTime{10})
                  .ok());
  const auto recovered_commit = recovered.ValueOrDie()->Commit();
  ASSERT_TRUE(recovered_commit.ok()) << recovered_commit.status().ToString();
  EXPECT_EQ(recovered_commit.ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_NE(recovered_commit.ValueOrDie().txn_id, indeterminate_id);
  reopened.ValueOrDie().reset();
  std::filesystem::remove_all(path);
}

TEST(KernelCommitFailureTest, NeverReusesTransactionIdAfterPrewriteIndeterminate) {
  char pattern[] = "/tmp/cedar_kernel_commit_prewrite_fault_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto database = Database::Open(DatabaseOptions{
      .path = path,
      .commit_prewrite_fault_injector_for_testing = [] {
        return Status::Indeterminate("test", "injected prewrite uncertainty");
      },
  });
  ASSERT_TRUE(database.ok()) << database.status().ToString();

  auto first = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  ASSERT_TRUE(first.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10})
                  .ok());
  const auto indeterminate = first.ValueOrDie()->Commit();
  ASSERT_TRUE(indeterminate.ok()) << indeterminate.status().ToString();
  ASSERT_EQ(indeterminate.ValueOrDie().outcome, CommitOutcome::kIndeterminate);
  const TxnId indeterminate_id = indeterminate.ValueOrDie().txn_id;

  database.ValueOrDie().reset();
  auto reopened = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
  const auto unresolved = reopened.ValueOrDie()->ResolveTransaction(indeterminate_id);
  ASSERT_TRUE(unresolved.ok()) << unresolved.status().ToString();
  EXPECT_FALSE(unresolved.ValueOrDie().has_value());

  auto recovered = reopened.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(recovered.ok()) << recovered.status().ToString();
  ASSERT_TRUE(recovered.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}), ValidTime{10})
                  .ok());
  const auto recovered_commit = recovered.ValueOrDie()->Commit();
  ASSERT_TRUE(recovered_commit.ok()) << recovered_commit.status().ToString();
  EXPECT_EQ(recovered_commit.ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_NE(recovered_commit.ValueOrDie().txn_id, indeterminate_id);

  const auto still_unresolved = reopened.ValueOrDie()->ResolveTransaction(indeterminate_id);
  ASSERT_TRUE(still_unresolved.ok()) << still_unresolved.status().ToString();
  EXPECT_FALSE(still_unresolved.ValueOrDie().has_value());
  {
    const auto snapshot = reopened.ValueOrDie()->BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    EXPECT_FALSE(snapshot.ValueOrDie().Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10})
                     .ValueOrDie());
    EXPECT_TRUE(snapshot.ValueOrDie().Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}), ValidTime{10})
                    .ValueOrDie());
  }
  reopened.ValueOrDie().reset();
  std::filesystem::remove_all(path);
}

}  // namespace
}  // namespace cedar
