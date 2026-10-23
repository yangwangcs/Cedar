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
#include <cstring>
#include <filesystem>
#include <future>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "cedar/database.h"
#include "storage/facts/fact_store.h"
#include "storage/facts/group_commit_planner.h"
#include "kernel/database_impl.h"
#include "kernel/epoch_completion.h"

#if defined(CEDAR_ROCKSDB_SYNC_POINTS)
#include "test_util/sync_point.h"
#endif

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

TEST_F(KernelCommitTest,
       AllowsAdjacentLaterValidTimeWriteToTheSameFactFromAnOlderSnapshot) {
  ASSERT_TRUE(database_->RegisterProperty(PropertyDefinition{
                        PropertyId{7}, 0, "load", PropertyEntityKind::kVertex,
                        PhysicalType::kInt64, 4096})
                  .ok());
  const PropertyFact fact =
      PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{81}}, PropertyId{7});
  auto seed = Begin();
  ASSERT_TRUE(seed->Set(fact, ValidTime{10}, Value::Int64(10)).ok());
  const auto seeded = seed->Commit();
  ASSERT_TRUE(seeded.ok()) << seeded.status().ToString();
  ASSERT_EQ(seeded.ValueOrDie().outcome, CommitOutcome::kCommitted);

  // Both transactions pin their snapshot before the earlier valid-time write
  // commits, exactly as adjacent-hour writes do in a pipelined writer.
  std::unique_ptr<Transaction> earlier = Begin();
  std::unique_ptr<Transaction> later = Begin();
  ASSERT_TRUE(earlier->Set(fact, ValidTime{11}, Value::Int64(11)).ok());
  ASSERT_TRUE(later->Set(fact, ValidTime{12}, Value::Int64(12)).ok());

  const auto earlier_committed = earlier->Commit();
  ASSERT_TRUE(earlier_committed.ok()) << earlier_committed.status().ToString();
  EXPECT_EQ(earlier_committed.ValueOrDie().outcome, CommitOutcome::kCommitted);
  const auto later_committed = later->Commit();
  ASSERT_TRUE(later_committed.ok()) << later_committed.status().ToString();
  EXPECT_EQ(later_committed.ValueOrDie().outcome, CommitOutcome::kCommitted)
      << later_committed.ValueOrDie().status.ToString();

  // Both versions are observable at their own valid times afterwards.
  const auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  const auto at_eleven = snapshot.ValueOrDie().Get(fact, ValidTime{11});
  ASSERT_TRUE(at_eleven.ok()) << at_eleven.status().ToString();
  EXPECT_EQ(at_eleven.ValueOrDie(), std::optional<Value>{Value::Int64(11)});
  const auto at_twelve = snapshot.ValueOrDie().Get(fact, ValidTime{12});
  ASSERT_TRUE(at_twelve.ok()) << at_twelve.status().ToString();
  EXPECT_EQ(at_twelve.ValueOrDie(), std::optional<Value>{Value::Int64(12)});
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

TEST(KernelPreparedCommitTest,
     DurableFinalizeSynchronizesFactPublicationBeforeReturning) {
  char pattern[] = "/tmp/cedar_kernel_durable_finalize_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::vector<bool> commit_sync_modes;
  DatabaseOptions options;
  options.path = path;
  options.prepared_commit_recovery =
      PreparedCommitRecoveryPolicy::kAwaitExternalDecision;
  options.commit_write_options_observer_for_testing =
      [&commit_sync_modes](bool sync) { commit_sync_modes.push_back(sync); };
  auto opened = Database::Open(options);
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  const PreparedCommitBatch batch{
      TxnId{904}, 7004,
      {FactEvent{FactRef{PartId{0}, FactFamily::kVertexState, PropertyId{}, 94},
                 ValidTime{14}, CommitSeq{1}, FactOperation::kPut, 0,
                 std::nullopt, std::nullopt}}};
  ASSERT_TRUE(database->PersistPreparedCommit(batch).ok());

  const auto finalized = database->FinalizePreparedCommit(
      batch.txn_id, "txnd-commit-durable",
      PreparedFinalizeDurability::kWalSync);
  ASSERT_TRUE(finalized.ok()) << finalized.status().ToString();
  ASSERT_FALSE(commit_sync_modes.empty());
  EXPECT_TRUE(commit_sync_modes.back());
  ASSERT_TRUE(database->Close().ok());
  database.reset();

  opened = Database::Open(options);
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  database = std::move(opened).ConsumeValueOrDie();
  auto snapshot = database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  EXPECT_TRUE(snapshot.ValueOrDie()
                  .Exists(EntityFact::Vertex(
                              VertexRef{PartId{0}, VertexId{94}}),
                          ValidTime{14})
                  .ValueOrDie());
  ASSERT_TRUE(database->ListPreparedCommits().ok());
  EXPECT_TRUE(database->ListPreparedCommits().ValueOrDie().empty());
  snapshot = Status::InvalidArgument("test", "released");
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

#if defined(CEDAR_ROCKSDB_SYNC_POINTS)
TEST(KernelPreparedCommitTest,
     DurableReplaySynchronizesPriorBufferedFinalize) {
  char pattern[] = "/tmp/cedar_kernel_durable_replay_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  DatabaseOptions options;
  options.path = path;
  options.prepared_commit_recovery =
      PreparedCommitRecoveryPolicy::kAwaitExternalDecision;
  auto opened = Database::Open(options);
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  const PreparedCommitBatch batch{
      TxnId{905}, 7005,
      {FactEvent{FactRef{PartId{0}, FactFamily::kVertexState, PropertyId{}, 95},
                 ValidTime{15}, CommitSeq{1}, FactOperation::kPut, 0,
                 std::nullopt, std::nullopt}}};
  ASSERT_TRUE(database->PersistPreparedCommit(batch).ok());
  const std::string certificate = "txnd-commit-buffered-replay";
  const auto buffered = database->FinalizePreparedCommit(
      batch.txn_id, certificate, PreparedFinalizeDurability::kBuffered);
  ASSERT_TRUE(buffered.ok()) << buffered.status().ToString();

  std::atomic<uint64_t> wal_sync_calls{0};
  rocksdb::SyncPoint* sync_point = rocksdb::SyncPoint::GetInstance();
  sync_point->SetCallBack("DBImpl::SyncWAL:Begin", [&](void*) {
    wal_sync_calls.fetch_add(1, std::memory_order_relaxed);
  });
  sync_point->EnableProcessing();
  const auto replay = database->FinalizePreparedCommit(
      batch.txn_id, certificate, PreparedFinalizeDurability::kWalSync);
  sync_point->DisableProcessing();
  sync_point->ClearAllCallBacks();

  ASSERT_TRUE(replay.ok()) << replay.status().ToString();
  EXPECT_EQ(replay.ValueOrDie().commit_seq,
            buffered.ValueOrDie().commit_seq);
  EXPECT_EQ(wal_sync_calls.load(std::memory_order_relaxed), 1U);
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelPreparedCommitTest,
     DurableReplaySynchronizesBufferedFinalizeAfterReopen) {
  char pattern[] = "/tmp/cedar_kernel_durable_reopen_replay_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  DatabaseOptions options;
  options.path = path;
  options.prepared_commit_recovery =
      PreparedCommitRecoveryPolicy::kAwaitExternalDecision;
  const PreparedCommitBatch batch{
      TxnId{906}, 7006,
      {FactEvent{FactRef{PartId{0}, FactFamily::kVertexState, PropertyId{}, 96},
                 ValidTime{16}, CommitSeq{1}, FactOperation::kPut, 0,
                 std::nullopt, std::nullopt}}};
  const std::string certificate = "txnd-commit-buffered-reopen-replay";

  auto opened = Database::Open(options);
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  ASSERT_TRUE(database->PersistPreparedCommit(batch).ok());
  const auto buffered = database->FinalizePreparedCommit(
      batch.txn_id, certificate, PreparedFinalizeDurability::kBuffered);
  ASSERT_TRUE(buffered.ok()) << buffered.status().ToString();
  ASSERT_TRUE(database->Close().ok());
  database.reset();

  opened = Database::Open(options);
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  database = std::move(opened).ConsumeValueOrDie();
  std::atomic<uint64_t> wal_sync_calls{0};
  rocksdb::SyncPoint* sync_point = rocksdb::SyncPoint::GetInstance();
  sync_point->SetCallBack("DBImpl::SyncWAL:Begin", [&](void*) {
    wal_sync_calls.fetch_add(1, std::memory_order_relaxed);
  });
  sync_point->EnableProcessing();
  const auto replay = database->FinalizePreparedCommit(
      batch.txn_id, certificate, PreparedFinalizeDurability::kWalSync);
  sync_point->DisableProcessing();
  sync_point->ClearAllCallBacks();

  ASSERT_TRUE(replay.ok()) << replay.status().ToString();
  EXPECT_EQ(replay.ValueOrDie().commit_seq,
            buffered.ValueOrDie().commit_seq);
  EXPECT_EQ(wal_sync_calls.load(std::memory_order_relaxed), 1U);
  auto snapshot = database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  EXPECT_TRUE(snapshot.ValueOrDie()
                  .Exists(EntityFact::Vertex(
                              VertexRef{PartId{0}, VertexId{96}}),
                          ValidTime{16})
                  .ValueOrDie());
  ASSERT_TRUE(database->ListPreparedCommits().ok());
  EXPECT_TRUE(database->ListPreparedCommits().ValueOrDie().empty());
  snapshot = Status::InvalidArgument("test", "released");
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}
#endif

TEST(KernelAsyncCommitTest, UsesOneDurableWriteForIndependentAsyncCommits) {
  char pattern[] = "/tmp/cedar_kernel_async_group_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::atomic<uint32_t> final_writes = 0;
  std::mutex enqueue_mutex;
  std::condition_variable enqueue_cv;
  uint32_t enqueued = 0;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 2,
      .group_commit_window_us = 20'000,
      .commit_prewrite_fault_injector_for_testing = [&final_writes] {
        ++final_writes;
        return Status::OK();
      },
      .append_commit_enqueued_observer_for_testing = [&] {
        std::lock_guard<std::mutex> lock(enqueue_mutex);
        ++enqueued;
        enqueue_cv.notify_all();
      },
      .append_commit_collection_observer_for_testing = [&] {
        std::unique_lock<std::mutex> lock(enqueue_mutex);
        EXPECT_TRUE(enqueue_cv.wait_for(lock, std::chrono::seconds(2),
                                       [&] { return enqueued == 2; }));
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
      .predicted_preflight_min_bytes = 0,
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
  std::mutex sample_mutex;
  std::thread::id sampler_thread;
  bool sampled_outside_sampler = false;
  std::mutex prewrite_mutex;
  std::condition_variable prewrite_cv;
  bool writer_entered = false;
  bool release_writer = false;
  auto opened = Database::Open(DatabaseOptions{
      .path = pattern,
      .group_commit_max_batch_size = 1,
      .group_commit_window_us = 0,
      .predicted_preflight_min_bytes = 0,
      .commit_prewrite_fault_injector_for_testing = [&] {
        std::unique_lock<std::mutex> lock(prewrite_mutex);
        writer_entered = true;
        prewrite_cv.notify_all();
        prewrite_cv.wait(lock, [&] { return release_writer; });
        return Status::OK();
      },
      .runtime_sample_observer_for_testing = [&] {
        std::lock_guard<std::mutex> lock(sample_mutex);
        // Open performs the initial sample before starting the worker.
        if (sample_count.fetch_add(1) != 0 &&
            std::this_thread::get_id() != sampler_thread) {
          sampled_outside_sampler = true;
        }
      },
      .runtime_sampler_thread_started_observer_for_testing = [&] {
        std::lock_guard<std::mutex> lock(sample_mutex);
        sampler_thread = std::this_thread::get_id();
      }});
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
  EXPECT_GT(sample_count.load(), 1U);
  {
    std::lock_guard<std::mutex> lock(sample_mutex);
    EXPECT_FALSE(sampled_outside_sampler);
  }
  // Close legitimately performs one final refresh on its calling thread.
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
      .predicted_preflight_min_bytes = 0,
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
    entered = completion_cv.wait_for(lock, std::chrono::seconds(30), [&] {
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
      .predicted_preflight_min_bytes = 0,
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
      .predicted_preflight_min_bytes = 0,
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
  const auto metrics = database->GetCommitPipelineMetrics();
  EXPECT_EQ(metrics.canonical_validation_scans, canonical_scans.load());
  EXPECT_GT(metrics.canonical_validation_boundaries, 0U);

  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelSnapshotValidationTest, LongHistoryUnchangedIdentityAvoidsCanonicalScan) {
  char pattern[] = "/tmp/cedar_long_history_exact_proof_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  std::atomic<uint64_t> scans{0};
  DatabaseOptions options;
  options.path = pattern;
  options.group_commit_max_batch_size = 1;
  options.group_commit_window_us = 0;
  options.validation_scan_observer_for_testing = [&] { ++scans; };
  auto opened = Database::Open(options);
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  const EntityFact target = EntityFact::Vertex(VertexRef{PartId{0}, VertexId{971}});
  for (uint64_t t = 1; t <= 32; ++t) {
    auto tx = database->BeginTransaction(); ASSERT_TRUE(tx.ok());
    ASSERT_TRUE(tx.ValueOrDie()->Assert(target, ValidTime{t}).ok());
    auto committed = tx.ValueOrDie()->Commit(); ASSERT_TRUE(committed.ok());
    ASSERT_TRUE(committed.ValueOrDie().status.ok());
  }
  auto transaction = database->BeginTransaction(); ASSERT_TRUE(transaction.ok());
  ASSERT_TRUE(transaction.ValueOrDie()->Assert(target, ValidTime{33}).ok());
  auto unrelated = database->BeginTransaction(); ASSERT_TRUE(unrelated.ok());
  ASSERT_TRUE(unrelated.ValueOrDie()->Assert(
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{972}}), ValidTime{1}).ok());
  auto other = unrelated.ValueOrDie()->Commit(); ASSERT_TRUE(other.ok());
  ASSERT_TRUE(other.ValueOrDie().status.ok());
  const uint64_t before = scans.load();
  auto committed = transaction.ValueOrDie()->Commit(); ASSERT_TRUE(committed.ok());
  ASSERT_TRUE(committed.ValueOrDie().status.ok());
  EXPECT_EQ(scans.load(), before);
  EXPECT_GT(database->GetCommitPipelineMetrics().recent_write_exact_hits, 0U);
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(pattern);
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
      .predicted_preflight_min_bytes = 0,
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
      .predicted_preflight_min_bytes = 0,
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
  std::mutex collection_mutex;
  std::condition_variable collection_changed;
  bool grouping = false;
  bool release_collection = false;
  uint32_t enqueued = 0;
  DatabaseOptions options;
  options.path = path;
  options.group_commit_max_batch_size = 2;
  options.group_commit_window_us = 20'000;
  options.commit_prewrite_fault_injector_for_testing = [&physical_writes] {
    ++physical_writes;
    return Status::OK();
  };
  options.append_commit_enqueued_observer_for_testing = [&] {
    std::lock_guard lock(collection_mutex);
    if (grouping) ++enqueued;
    collection_changed.notify_all();
  };
  options.append_commit_collection_observer_for_testing = [&] {
    std::unique_lock lock(collection_mutex);
    if (grouping) {
      collection_changed.wait(lock, [&] { return release_collection; });
    }
  };
  auto opened = Database::Open(options);
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
  {
    std::lock_guard lock(collection_mutex);
    grouping = true;
  }
  std::thread invalid_thread(
      [&] { invalid_handle.emplace(invalid.ValueOrDie()->CommitAsync()); });
  std::thread winner_thread(
      [&] { winner_handle.emplace(winner.ValueOrDie()->CommitAsync()); });
  bool both_enqueued = false;
  {
    std::unique_lock lock(collection_mutex);
    both_enqueued = collection_changed.wait_for(
        lock, std::chrono::seconds(5), [&] { return enqueued == 2; });
    release_collection = true;
  }
  collection_changed.notify_all();
  invalid_thread.join();
  winner_thread.join();
  ASSERT_TRUE(both_enqueued);

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
  std::mutex gate_mutex;
  std::condition_variable gate_cv;
  uint32_t enqueued = 0;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 2,
      .group_commit_window_us = 20'000,
      .commit_prewrite_fault_injector_for_testing = [&physical_writes] {
        ++physical_writes;
        return Status::OK();
      },
      // Both callers must be queued before collection so a slow sanitizer run
      // cannot dispatch the first commit as a single-transaction group.
      .append_commit_enqueued_observer_for_testing = [&] {
        std::lock_guard<std::mutex> lock(gate_mutex);
        ++enqueued;
        gate_cv.notify_all();
      },
      .append_commit_collection_observer_for_testing = [&] {
        std::unique_lock<std::mutex> lock(gate_mutex);
        gate_cv.wait_for(lock, std::chrono::seconds(30),
                         [&] { return enqueued >= 2; });
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
  std::mutex gate_mutex;
  std::condition_variable gate_cv;
  uint32_t enqueued = 0;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 2,
      .group_commit_window_us = 20'000,
      // Both callers must be queued before collection so a slow sanitizer run
      // cannot dispatch the first commit as a single-transaction group.
      .append_commit_enqueued_observer_for_testing = [&] {
        std::lock_guard<std::mutex> lock(gate_mutex);
        ++enqueued;
        gate_cv.notify_all();
      },
      .append_commit_collection_observer_for_testing = [&] {
        std::unique_lock<std::mutex> lock(gate_mutex);
        gate_cv.wait_for(lock, std::chrono::seconds(30),
                         [&] { return enqueued >= 2; });
      }});
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

TEST(KernelGroupCommitTest, SingleTransactionCapDoesNotWaitToCollectImpossibleWork) {
  char pattern[] = "/tmp/cedar_single_group_dispatch_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto opened = Database::Open(DatabaseOptions{
      .path = path, .group_commit_max_batch_size = 1,
      .group_commit_window_us = 200'000});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  auto tx = database->BeginTransaction();
  ASSERT_TRUE(tx.ok());
  ASSERT_TRUE(tx.ValueOrDie()->Assert(
      EntityFact::Vertex({PartId{0}, VertexId{9001}}), ValidTime{1}).ok());
  auto result = tx.ValueOrDie()->Commit();
  ASSERT_TRUE(result.ok());
  EXPECT_EQ(result.ValueOrDie().outcome, CommitOutcome::kCommitted);
  // Observe only queue residence, not disk-sync latency. With a group cap of
  // one there is no possible batch benefit from a 200ms collection delay.
  EXPECT_LT(database->GetCommitPipelineMetrics().latency.queue.max_us, 100'000U);
  ASSERT_TRUE(database->Close().ok());
  database.reset();
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest, ConcurrentSyncWaitersReceiveEveryPublishedResult) {
  char pattern[] = "/tmp/cedar_sync_result_notifications_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto opened = Database::Open(DatabaseOptions{
      .path = path, .group_commit_max_batch_size = 16,
      .group_commit_window_us = 200});
  ASSERT_TRUE(opened.ok());
  auto database = std::move(opened).ConsumeValueOrDie();
  std::barrier start(16);
  std::atomic<uint64_t> failures{0};
  std::vector<std::thread> workers;
  for (uint64_t client = 0; client < 16; ++client) {
    workers.emplace_back([&, client] {
      start.arrive_and_wait();
      for (uint64_t iteration = 0; iteration < 8; ++iteration) {
        auto tx = database->BeginTransaction();
        if (!tx.ok()) { ++failures; continue; }
        auto status = tx.ValueOrDie()->Assert(
            EntityFact::Vertex({PartId{0}, VertexId{10000 + client * 8 + iteration}}),
            ValidTime{1});
        if (!status.ok()) { ++failures; continue; }
        auto committed = tx.ValueOrDie()->Commit();
        if (!committed.ok() || committed.ValueOrDie().outcome != CommitOutcome::kCommitted) ++failures;
      }
    });
  }
  for (auto& worker : workers) worker.join();
  EXPECT_EQ(failures.load(), 0U);
  EXPECT_EQ(database->GetCommitPipelineMetrics().published, 128U);
  {
    auto snapshot = database->BeginSnapshot();
    ASSERT_TRUE(snapshot.ok());
    for (uint64_t id = 10000; id < 10128; ++id) {
      const auto exists = snapshot.ValueOrDie().Exists(
          EntityFact::Vertex({PartId{0}, VertexId{id}}), ValidTime{1});
      ASSERT_TRUE(exists.ok());
      EXPECT_TRUE(exists.ValueOrDie());
    }
  }
  ASSERT_TRUE(database->Close().ok());
  database.reset();
  std::filesystem::remove_all(path);
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

// F13: kernel queue admission must reserve the v2 sequence record the store
// actually writes. A transaction whose v2 estimate exactly fits the configured
// byte cap is admitted; one byte less is rejected before any write.
TEST(KernelGroupCommitTest, V2AdmissionUsesTheExactEncodedEstimate) {
  constexpr size_t kFactCount = 64;
  StoreCommitBatch equivalent{TxnId{1}, 10, {}, {}, {}, {}, true};
  equivalent.mutations.reserve(kFactCount);
  for (size_t index = 0; index < kFactCount; ++index) {
    equivalent.mutations.push_back(PendingFactMutation{
        EntityFact::Vertex(VertexRef{PartId{0}, VertexId{index + 1}}).ref(),
        ValidTime{10}, FactOperation::kPut, 0, std::nullopt});
  }
  const size_t v2_estimate = internal::EstimateCommitBatchBytes(equivalent);
  ASSERT_GT(v2_estimate, 1U);

  const auto commit_facts = [](uint64_t byte_cap,
                               const std::string& path) {
    auto opened = Database::Open(DatabaseOptions{
        .path = path,
        .group_commit_max_batch_bytes = byte_cap});
    if (!opened.ok()) return std::pair<bool, Status>{false, opened.status()};
    std::unique_ptr<Database> database =
        std::move(opened).ConsumeValueOrDie();
    auto transaction = database->BeginTransaction();
    if (!transaction.ok()) {
      database->Close().IgnoreError();
      return std::pair<bool, Status>{false, transaction.status()};
    }
    for (size_t index = 0; index < kFactCount; ++index) {
      const Status asserted = transaction.ValueOrDie()->Assert(
          EntityFact::Vertex(VertexRef{PartId{0}, VertexId{index + 1}}),
          ValidTime{10});
      if (!asserted.ok()) {
        database->Close().IgnoreError();
        return std::pair<bool, Status>{false, asserted};
      }
    }
    const auto committed = transaction.ValueOrDie()->Commit();
    database->Close().IgnoreError();
    if (!committed.ok()) {
      return std::pair<bool, Status>{false, committed.status()};
    }
    if (committed.ValueOrDie().outcome != CommitOutcome::kCommitted) {
      return std::pair<bool, Status>{false, committed.ValueOrDie().status};
    }
    return std::pair<bool, Status>{true, Status::OK()};
  };

  {
    char pattern[] = "/tmp/cedar_kernel_v2_admission_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    const std::string path = pattern;
    const auto [committed, status] = commit_facts(v2_estimate, path);
    EXPECT_TRUE(committed) << status.ToString();
    EXPECT_TRUE(status.ok()) << status.ToString();
    std::filesystem::remove_all(path);
  }
  {
    char pattern[] = "/tmp/cedar_kernel_v2_rejection_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    const std::string path = pattern;
    const auto [committed, status] =
        commit_facts(v2_estimate - 1, path);
    EXPECT_FALSE(committed);
    EXPECT_TRUE(status.IsResourceExhausted()) << status.ToString();
    std::filesystem::remove_all(path);
  }
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
      .predicted_preflight_min_bytes = 0,
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
      .predicted_preflight_min_bytes = 0,
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
      .predicted_preflight_min_bytes = 0,
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
      // One single-fact transaction estimates below this cap (encoder-aligned
      // estimate + safety margin), two do not: the selected epoch still owns
      // its reservation while the writer is parked.
      .group_commit_max_queue_bytes = 700});
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

TEST(KernelGroupCommitTest, SelectionWakesQueueAdmissionBeforeSlowStorageCompletes) {
  char pattern[] = "/tmp/cedar_selection_admission_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  std::mutex gate_mutex;
  std::condition_variable gate;
  uint32_t collections = 0, writes = 0, admissions = 0, enqueued = 0;
  bool release_first_write = false, release_selection = false;
  bool release_second_write = false;
  DatabaseOptions options;
  options.path = pattern;
  options.group_commit_max_batch_size = 1;
  options.group_commit_window_us = 0;
  options.group_commit_max_queue_requests = 1;
  options.group_commit_max_queue_bytes = 1ULL << 20;
  options.append_commit_collection_observer_for_testing = [&] {
    std::unique_lock lock(gate_mutex);
    ++collections;
    gate.notify_all();
    if (collections == 2) gate.wait(lock, [&] { return release_selection; });
  };
  options.foreground_admission_observer_for_testing = [&] {
    std::lock_guard lock(gate_mutex);
    ++admissions;
    gate.notify_all();
  };
  options.append_commit_enqueued_observer_for_testing = [&] {
    std::lock_guard lock(gate_mutex);
    ++enqueued;
    gate.notify_all();
  };
  options.commit_prewrite_fault_injector_for_testing = [&] {
    std::unique_lock lock(gate_mutex);
    ++writes;
    gate.notify_all();
    if (writes == 1) gate.wait(lock, [&] { return release_first_write; });
    if (writes == 2) gate.wait(lock, [&] { return release_second_write; });
    return Status::OK();
  };
  auto opened = Database::Open(options);
  ASSERT_TRUE(opened.ok());
  auto database = std::move(opened).ConsumeValueOrDie();
  auto first = database->BeginTransaction();
  auto second = database->BeginTransaction();
  auto third = database->BeginTransaction(
      TransactionOptions{.commit_deadline_us = 2'000'000});
  ASSERT_TRUE(first.ok()); ASSERT_TRUE(second.ok()); ASSERT_TRUE(third.ok());
  ASSERT_TRUE(first.ValueOrDie()->Assert(
      EntityFact::Vertex({PartId{0}, VertexId{8001}}), ValidTime{1}).ok());
  ASSERT_TRUE(second.ValueOrDie()->Assert(
      EntityFact::Vertex({PartId{0}, VertexId{8002}}), ValidTime{1}).ok());
  ASSERT_TRUE(third.ValueOrDie()->Assert(
      EntityFact::Vertex({PartId{0}, VertexId{8003}}), ValidTime{1}).ok());
  std::optional<StatusOr<CommitResult>> results[3];
  std::thread a([&] { results[0].emplace(first.ValueOrDie()->Commit()); });
  {
    std::unique_lock lock(gate_mutex);
    gate.wait(lock, [&] { return writes == 1; });
  }
  std::thread b([&] { results[1].emplace(second.ValueOrDie()->Commit()); });
  {
    std::unique_lock lock(gate_mutex);
    gate.wait(lock, [&] { return enqueued == 2; });
    release_first_write = true;
    gate.notify_all();
    // A's reserved-byte notification has finished before B's selection gate.
    gate.wait(lock, [&] { return collections == 2; });
  }
  std::thread c([&] { results[2].emplace(third.ValueOrDie()->Commit()); });
  {
    std::unique_lock lock(gate_mutex);
    gate.wait(lock, [&] { return admissions == 3; });
  }
  // Let C sleep on the full count-limited queue while B is still queued.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  bool admitted_while_storage_blocked = false;
  {
    std::unique_lock lock(gate_mutex);
    release_selection = true;
    gate.notify_all();
    gate.wait(lock, [&] { return writes == 2; });
    admitted_while_storage_blocked = gate.wait_for(
        lock, std::chrono::milliseconds(500), [&] { return enqueued == 3; });
    release_second_write = true;
    gate.notify_all();
  }
  a.join(); b.join(); c.join();
  EXPECT_TRUE(admitted_while_storage_blocked);
  for (const auto& result : results) {
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->ok());
    EXPECT_EQ(result->ValueOrDie().outcome, CommitOutcome::kCommitted);
  }
  ASSERT_TRUE(database->Close().ok());
  database.reset();
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
      .predicted_preflight_min_bytes = 0,
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
      .predicted_preflight_min_bytes = 0,
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
      .predicted_preflight_min_bytes = 0,
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
      .predicted_preflight_min_bytes = 0,
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
      .predicted_preflight_min_bytes = 0,
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
      .predicted_preflight_min_bytes = 0,
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
      .predicted_preflight_min_bytes = 0,
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
  std::mutex gate_mutex;
  std::condition_variable gate_cv;
  uint32_t enqueued = 0;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 2,
      // A full batch must not wait for the collection window. A 1 s window
      // keeps that behaviour distinguishable from scheduler noise under
      // parallel ctest runs.
      .group_commit_window_us = 1'000'000,
      .commit_prewrite_fault_injector_for_testing = [&physical_writes] {
        ++physical_writes;
        return Status::OK();
      },
      // Wait for both callers to enqueue, then verify the full batch commits
      // without consuming the collection window.
      .append_commit_enqueued_observer_for_testing = [&] {
        std::lock_guard<std::mutex> lock(gate_mutex);
        ++enqueued;
        gate_cv.notify_all();
      },
      .append_commit_collection_observer_for_testing = [&] {
        std::unique_lock<std::mutex> lock(gate_mutex);
        gate_cv.wait_for(lock, std::chrono::seconds(30),
                         [&] { return enqueued >= 2; });
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
            500);
  ASSERT_TRUE(database->Close().ok());
  database.reset();
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest, DoesNotGroupCompetingWritesForOneFact) {
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
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10})
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
  std::mutex gate_mutex;
  std::condition_variable gate_cv;
  uint32_t enqueued = 0;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 2,
      .group_commit_window_us = 20'000,
      .commit_prewrite_fault_injector_for_testing = [&physical_writes] {
        ++physical_writes;
        return Status::OK();
      },
      // Both callers must be queued before collection so a slow sanitizer run
      // cannot dispatch the first commit as a single-transaction group.
      .append_commit_enqueued_observer_for_testing = [&] {
        std::lock_guard<std::mutex> lock(gate_mutex);
        ++enqueued;
        gate_cv.notify_all();
      },
      .append_commit_collection_observer_for_testing = [&] {
        std::unique_lock<std::mutex> lock(gate_mutex);
        gate_cv.wait_for(lock, std::chrono::seconds(30),
                         [&] { return enqueued >= 2; });
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
  std::mutex gate_mutex;
  std::condition_variable gate_cv;
  uint32_t enqueued = 0;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 2,
      .group_commit_window_us = 20'000,
      .commit_prewrite_fault_injector_for_testing = [&physical_writes] {
        ++physical_writes;
        return Status::OK();
      },
      // Both callers must be queued before collection so a slow sanitizer run
      // cannot dispatch the first commit as a single-transaction group.
      .append_commit_enqueued_observer_for_testing = [&] {
        std::lock_guard<std::mutex> lock(gate_mutex);
        ++enqueued;
        gate_cv.notify_all();
      },
      .append_commit_collection_observer_for_testing = [&] {
        std::unique_lock<std::mutex> lock(gate_mutex);
        gate_cv.wait_for(lock, std::chrono::seconds(30),
                         [&] { return enqueued >= 2; });
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
  std::mutex grouping_mutex;
  std::condition_variable grouping_changed;
  bool grouping = false;
  uint32_t enqueued = 0;
  DatabaseOptions options;
  options.path = path;
  options.group_commit_max_batch_size = 2;
  options.group_commit_window_us = 20'000;
  options.commit_prewrite_fault_injector_for_testing = [&physical_writes] {
    ++physical_writes;
    return Status::OK();
  };
  options.append_commit_enqueued_observer_for_testing = [&] {
    std::lock_guard lock(grouping_mutex);
    if (grouping) ++enqueued;
    grouping_changed.notify_all();
  };
  options.append_commit_collection_observer_for_testing = [&] {
    std::unique_lock lock(grouping_mutex);
    if (grouping) {
      EXPECT_TRUE(grouping_changed.wait_for(lock, std::chrono::seconds(30),
                                           [&] { return enqueued == 2; }));
    }
  };
  auto opened = Database::Open(options);
  ASSERT_TRUE(opened.ok());
  auto database = std::move(opened).ConsumeValueOrDie();
  auto seed = database->BeginTransaction();
  ASSERT_TRUE(seed.ok());
  ASSERT_TRUE(seed.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{31}}), ValidTime{10}).ok());
  ASSERT_TRUE(seed.ValueOrDie()->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{32}}), ValidTime{10}).ok());
  ASSERT_EQ(seed.ValueOrDie()->Commit().ValueOrDie().outcome, CommitOutcome::kCommitted);
  physical_writes.store(0);
  {
    std::lock_guard lock(grouping_mutex);
    grouping = true;
  }

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
  std::mutex gate_mutex;
  std::condition_variable gate_cv;
  uint32_t enqueued = 0;
  auto opened = Database::Open(DatabaseOptions{
      .path = path, .group_commit_max_batch_size = 2,
      .group_commit_window_us = 20'000,
      .commit_fault_injector_for_testing = [] {
        return Status::Indeterminate("test", "injected after durable write");
      },
      // Both callers must be in the same group before the post-write fault is
      // injected; otherwise a slow sanitizer run can split them.
      .append_commit_enqueued_observer_for_testing = [&] {
        std::lock_guard<std::mutex> lock(gate_mutex);
        ++enqueued;
        gate_cv.notify_all();
      },
      .append_commit_collection_observer_for_testing = [&] {
        std::unique_lock<std::mutex> lock(gate_mutex);
        gate_cv.wait_for(lock, std::chrono::seconds(30),
                         [&] { return enqueued >= 2; });
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
      // This test synchronizes collection through the collection observer and
      // then expects one full group. Predicted epochs can be decided before
      // all callers have enqueued, so disable preflight for this test.
      .predicted_preflight_min_bytes = std::numeric_limits<uint64_t>::max(),
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
        // Parallel ctest jobs can delay enqueue well past a couple of seconds;
        // the barrier guarantees all callers arrive, so wait generously
        // instead of letting the worker collect a partial group.
        enqueue_cv.wait_for(lock, std::chrono::seconds(30), [&] {
          return enqueued.load(std::memory_order_relaxed) == kTransactions;
        });
      },
      // Parallel ctest jobs can delay callers past the production 5 ms latency
      // SLO, which makes the adaptive controller split the queued batch. Pin
      // the SLO so the observer-synchronized batch stays one group.
      .collection_latency_slo_us_for_testing = std::numeric_limits<uint64_t>::max(),
      });
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

TEST(KernelSnapshotValidationTest, ManyDependenciesDetectOverlappingConflict) {
  char pattern[] = "/tmp/cedar_many_dependencies_conflict_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto opened = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();

  auto stale = database->BeginTransaction();
  ASSERT_TRUE(stale.ok()) << stale.status().ToString();
  for (uint64_t index = 0; index < 64; ++index) {
    ASSERT_TRUE(stale.ValueOrDie()
                    ->Assert(EntityFact::Vertex(VertexRef{PartId{0},
                                                          VertexId{1000 + index}}),
                             ValidTime{10})
                    .ok());
  }
  auto winner = database->BeginTransaction();
  ASSERT_TRUE(winner.ok()) << winner.status().ToString();
  ASSERT_TRUE(winner.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1037}}),
                           ValidTime{10})
                  .ok());
  const auto winner_commit = winner.ValueOrDie()->Commit();
  ASSERT_TRUE(winner_commit.ok()) << winner_commit.status().ToString();
  ASSERT_EQ(winner_commit.ValueOrDie().outcome, CommitOutcome::kCommitted);

  const auto stale_commit = stale.ValueOrDie()->Commit();
  ASSERT_TRUE(stale_commit.ok()) << stale_commit.status().ToString();
  EXPECT_EQ(stale_commit.ValueOrDie().outcome, CommitOutcome::kAborted);
  EXPECT_TRUE(stale_commit.ValueOrDie().status.IsConflict());
  EXPECT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelSnapshotValidationTest, DisjointDependenciesCommitAfterBatchScan) {
  char pattern[] = "/tmp/cedar_many_dependencies_disjoint_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto opened = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();

  auto transaction = database->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  for (uint64_t index = 0; index < 64; ++index) {
    ASSERT_TRUE(transaction.ValueOrDie()
                    ->Assert(EntityFact::Vertex(VertexRef{PartId{0},
                                                          VertexId{2000 + index}}),
                             ValidTime{10})
                    .ok());
  }
  const auto commit = transaction.ValueOrDie()->Commit();
  ASSERT_TRUE(commit.ok()) << commit.status().ToString();
  EXPECT_EQ(commit.ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelConcurrentValidationTest,
     StaleSnapshotBlindAppendsKeepOneWinner) {
  char pattern[] = "/tmp/cedar_predicted_stale_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto opened = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();

  constexpr uint32_t kThreads = 8;
  constexpr uint32_t kFactsPerTransaction = 256;
  std::barrier start(kThreads + 1);
  std::atomic<uint32_t> committed{0};
  std::atomic<uint32_t> conflicted{0};
  std::atomic<bool> failed{false};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (uint32_t thread = 0; thread < kThreads; ++thread) {
    threads.emplace_back([&, thread] {
      auto transaction = database->BeginTransaction();
      if (!transaction.ok()) {
        failed.store(true);
        start.arrive_and_wait();
        return;
      }
      auto& staged = transaction.ValueOrDie();
      for (uint32_t index = 0; index < kFactsPerTransaction; ++index) {
        const uint64_t vertex_id = 100000 + thread * 1000 + index;
        if (!staged
                 ->Assert(EntityFact::Vertex(
                              VertexRef{PartId{0}, VertexId{vertex_id}}),
                          ValidTime{10})
                 .ok()) {
          failed.store(true);
        }
      }
      // Every transaction also touches one shared fact. All snapshots are
      // taken before any commit, so exactly one blind append may win.
      if (!staged
               ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{7}}),
                        ValidTime{10})
               .ok()) {
        failed.store(true);
      }
      start.arrive_and_wait();
      const auto result = staged->Commit();
      if (!result.ok()) {
        failed.store(true);
        return;
      }
      if (result.ValueOrDie().outcome == CommitOutcome::kCommitted) {
        committed.fetch_add(1);
      } else if (result.ValueOrDie().status.IsConflict()) {
        conflicted.fetch_add(1);
      }
    });
  }
  start.arrive_and_wait();
  for (auto& thread : threads) thread.join();

  EXPECT_FALSE(failed.load());
  EXPECT_EQ(committed.load(), 1U);
  EXPECT_EQ(conflicted.load(), kThreads - 1);
  EXPECT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelConcurrentValidationTest, PredictedBlindAppendSurvivesReopen) {
  char pattern[] = "/tmp/cedar_predicted_reopen_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto opened = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();

  constexpr uint32_t kThreads = 4;
  constexpr uint32_t kFactsPerTransaction = 256;
  std::atomic<bool> failed{false};
  std::vector<std::thread> threads;
  for (uint32_t thread = 0; thread < kThreads; ++thread) {
    threads.emplace_back([&, thread] {
      auto transaction = database->BeginTransaction();
      if (!transaction.ok()) {
        failed.store(true);
        return;
      }
      for (uint32_t index = 0; index < kFactsPerTransaction; ++index) {
        const uint64_t vertex_id = 200000 + thread * 1000 + index;
        if (!transaction.ValueOrDie()
                 ->Assert(EntityFact::Vertex(
                              VertexRef{PartId{0}, VertexId{vertex_id}}),
                          ValidTime{10})
                 .ok()) {
          failed.store(true);
        }
      }
      const auto result = transaction.ValueOrDie()->Commit();
      if (!result.ok() ||
          result.ValueOrDie().outcome != CommitOutcome::kCommitted) {
        failed.store(true);
      }
    });
  }
  for (auto& thread : threads) thread.join();
  EXPECT_FALSE(failed.load());
  const Status closed = database->Close();
  ASSERT_TRUE(closed.ok()) << closed.ToString();
  database.reset();

  auto reopened = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
  database = std::move(reopened).ConsumeValueOrDie();
  {
    const auto snapshot = database->BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    for (uint32_t thread = 0; thread < kThreads; ++thread) {
      for (uint32_t index = 0; index < kFactsPerTransaction; index += 37) {
        const uint64_t vertex_id = 200000 + thread * 1000 + index;
        const auto exists = snapshot.ValueOrDie().Exists(
            EntityFact::Vertex(VertexRef{PartId{0}, VertexId{vertex_id}}),
            ValidTime{10});
        ASSERT_TRUE(exists.ok()) << exists.status().ToString();
        EXPECT_TRUE(exists.ValueOrDie());
      }
    }
  }
  EXPECT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

// Opt-in pipelined commit acceptance (Task 13). CommitAsyncPipelined returns
// once the request is admitted into the append pipeline (mailbox handoff plus
// append queue admission) instead of waiting for epoch selection, WAL
// durability or publication; CommitHandle::Wait stays the barrier for the
// durable terminal result. The tests below park the append worker at the
// durability boundary with the existing prewrite fault injector.
// Group-formation tests pin a healthy pressure sample (background maintenance
// must not change batch limits) and a latency SLO far above the test's queueing
// delay. Without the latter, requests that wait while the caller submits the
// rest of the burst can age past the 5 ms SLO, which makes the adaptive
// controller halve the batch and split an otherwise full group (reproduced
// under ASan). The tests below exercise grouping, not adaptive adaptation.
void PinHealthyPressure(PressureSample* sample) { *sample = PressureSample{}; }

class WalDurabilityGate {
 public:
  Status Hook() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!armed_) return Status::OK();
    entered_ = true;
    changed_.notify_all();
    changed_.wait(lock, [this] { return released_; });
    return Status::OK();
  }

  void Arm() {
    std::lock_guard<std::mutex> lock(mutex_);
    armed_ = true;
    entered_ = false;
    released_ = false;
  }

  bool WaitUntilEntered(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mutex_);
    return changed_.wait_for(lock, timeout, [this] { return entered_; });
  }

  void Release() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      released_ = true;
    }
    changed_.notify_all();
  }

 private:
  std::mutex mutex_;
  std::condition_variable changed_;
  bool armed_ = false;
  bool entered_ = false;
  bool released_ = false;
};

TEST(KernelGroupCommitTest, PipelinedAcceptsBeforeWalDurabilityAndWaitIsTheBarrier) {
  char pattern[] = "/tmp/cedar_kernel_pipelined_gate_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  WalDurabilityGate gate;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 8,
      .group_commit_window_us = 0,
      .predicted_preflight_min_bytes = 0,
      .commit_prewrite_fault_injector_for_testing =
          [&gate] { return gate.Hook(); }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();

  auto first = database->BeginTransaction();
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  ASSERT_TRUE(first.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{9'101}}),
                           ValidTime{10})
                  .ok());
  gate.Arm();
  auto submission = std::async(
      std::launch::async, [&] { return first.ValueOrDie()->CommitAsyncPipelined(); });
  const bool accepted_early =
      submission.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
  if (!accepted_early) gate.Release();
  auto accepted = submission.get();
  EXPECT_TRUE(accepted_early)
      << "pipelined acceptance blocked on WAL durability";
  gate.Release();
  ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();
  EXPECT_EQ(accepted.ValueOrDie().acceptance(), CommitAcceptance::kEnqueued);
  {
    const auto completed = accepted.ValueOrDie().Wait();
    ASSERT_TRUE(completed.ok()) << completed.status().ToString();
    EXPECT_EQ(completed.ValueOrDie().outcome, CommitOutcome::kCommitted);
    EXPECT_EQ(completed.ValueOrDie().commit_seq, CommitSeq{1});
    EXPECT_EQ(completed.ValueOrDie().txn_id, accepted.ValueOrDie().txn_id());
  }

  // While a second accepted request is parked at the durability boundary its
  // Wait() must not report any result; the barrier only lifts once the group
  // has synced, published and produced the terminal outcome.
  auto second = database->BeginTransaction();
  ASSERT_TRUE(second.ok()) << second.status().ToString();
  ASSERT_TRUE(second.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{9'102}}),
                           ValidTime{10})
                  .ok());
  gate.Arm();
  auto second_submission = std::async(std::launch::async, [&] {
    return second.ValueOrDie()->CommitAsyncPipelined();
  });
  const bool second_early = second_submission.wait_for(std::chrono::seconds(5)) ==
                            std::future_status::ready;
  if (!second_early) gate.Release();
  auto second_accepted = second_submission.get();
  EXPECT_TRUE(second_early)
      << "pipelined acceptance blocked on WAL durability";
  ASSERT_TRUE(second_accepted.ok()) << second_accepted.status().ToString();
  const bool parked =
      gate.WaitUntilEntered(std::chrono::milliseconds(5000));
  if (!parked) gate.Release();
  ASSERT_TRUE(parked) << "request never reached the durability boundary";
  auto completion = std::async(
      std::launch::async, [&] { return second_accepted.ValueOrDie().Wait(); });
  EXPECT_EQ(completion.wait_for(std::chrono::milliseconds(300)),
            std::future_status::timeout)
      << "Wait returned before the WAL group was durable and published";
  gate.Release();
  ASSERT_EQ(completion.wait_for(std::chrono::seconds(10)),
            std::future_status::ready);
  const auto completed = completion.get();
  ASSERT_TRUE(completed.ok()) << completed.status().ToString();
  EXPECT_EQ(completed.ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_EQ(completed.ValueOrDie().commit_seq, CommitSeq{2});
  EXPECT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest, LegacyCommitStillBlocksOnWalDurability) {
  char pattern[] = "/tmp/cedar_kernel_legacy_durability_gate_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  WalDurabilityGate gate;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 8,
      .group_commit_window_us = 0,
      .predicted_preflight_min_bytes = 0,
      .commit_prewrite_fault_injector_for_testing =
          [&gate] { return gate.Hook(); }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
  auto transaction = database->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{9'201}}),
                           ValidTime{10})
                  .ok());
  gate.Arm();
  auto submission = std::async(
      std::launch::async, [&] { return transaction.ValueOrDie()->CommitAsync(); });
  EXPECT_TRUE(gate.WaitUntilEntered(std::chrono::milliseconds(5000)))
      << "legacy request never reached the durability boundary";
  const bool returned_early =
      submission.wait_for(std::chrono::milliseconds(300)) ==
      std::future_status::ready;
  gate.Release();
  auto accepted = submission.get();
  EXPECT_FALSE(returned_early)
      << "legacy CommitAsync must not return before WAL durability";
  ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();
  const auto completed = accepted.ValueOrDie().Wait();
  ASSERT_TRUE(completed.ok()) << completed.status().ToString();
  EXPECT_EQ(completed.ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_EQ(completed.ValueOrDie().commit_seq, CommitSeq{1});
  EXPECT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest,
     PipelinedGroupsMultipleRequestsFromOneCallerIntoOneWalWrite) {
  constexpr uint32_t kRequests = 8;
  char pattern[] = "/tmp/cedar_kernel_pipelined_group_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::mutex gate_mutex;
  std::condition_variable gate_cv;
  uint32_t enqueued = 0;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = kRequests,
      .group_commit_window_us = 20'000,
      .predicted_preflight_min_bytes = 0,
      // Every request must be admitted before collection so one caller's
      // pipelined submissions can form a single full group.
      .append_commit_enqueued_observer_for_testing = [&] {
        std::lock_guard<std::mutex> lock(gate_mutex);
        ++enqueued;
        gate_cv.notify_all();
      },
      .append_commit_collection_observer_for_testing = [&] {
        std::unique_lock<std::mutex> lock(gate_mutex);
        gate_cv.wait_for(lock, std::chrono::seconds(30),
                         [&] { return enqueued >= kRequests; });
      },
      .collection_latency_slo_us_for_testing = 60'000'000,
      .runtime_pressure_override_for_testing = PinHealthyPressure});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();

  std::vector<CommitHandle> handles;
  handles.reserve(kRequests);
  for (uint32_t index = 0; index < kRequests; ++index) {
    auto transaction = database->BeginTransaction();
    ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
    ASSERT_TRUE(transaction.ValueOrDie()
                    ->Assert(EntityFact::Vertex(VertexRef{
                                  PartId{0}, VertexId{9'300 + index}}),
                             ValidTime{index + 1})
                    .ok());
    auto accepted = transaction.ValueOrDie()->CommitAsyncPipelined();
    ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();
    handles.push_back(std::move(accepted).ValueOrDie());
  }

  for (uint32_t index = 0; index < handles.size(); ++index) {
    const auto completed = handles[index].Wait();
    ASSERT_TRUE(completed.ok()) << completed.status().ToString();
    EXPECT_EQ(completed.ValueOrDie().outcome, CommitOutcome::kCommitted);
    EXPECT_EQ(completed.ValueOrDie().commit_seq, CommitSeq{index + 1});
  }
  const CommitPipelineMetrics metrics = database->GetCommitPipelineMetrics();
  EXPECT_EQ(metrics.submitted, kRequests);
  EXPECT_EQ(metrics.group_fill.groups, 1U);
  EXPECT_EQ(metrics.group_fill.total_transactions, kRequests);
  EXPECT_EQ(metrics.latency.wal_sync.count, 1U);
  EXPECT_EQ(metrics.durably_accepted, kRequests);
  EXPECT_EQ(metrics.published, kRequests);
  EXPECT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest, PipelinedHandleSurvivesCloseDrain) {
  char pattern[] = "/tmp/cedar_kernel_pipelined_close_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  WalDurabilityGate gate;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 4,
      .group_commit_window_us = 0,
      .predicted_preflight_min_bytes = 0,
      .commit_prewrite_fault_injector_for_testing =
          [&gate] { return gate.Hook(); }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
  auto transaction = database->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{9'401}}),
                           ValidTime{10})
                  .ok());
  gate.Arm();
  auto submission = std::async(
      std::launch::async, [&] { return transaction.ValueOrDie()->CommitAsyncPipelined(); });
  const bool accepted_early =
      submission.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
  if (!accepted_early) gate.Release();
  auto accepted = submission.get();
  EXPECT_TRUE(accepted_early);
  ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();
  const bool parked = gate.WaitUntilEntered(std::chrono::milliseconds(5000));
  if (!parked) gate.Release();
  ASSERT_TRUE(parked) << "request never reached the durability boundary";

  // Close must drain the accepted request (its submission call already
  // returned) instead of dropping it; it cannot finish while the WAL write is
  // parked, and the retained handle resolves to the committed result once the
  // drain lets the group complete.
  std::atomic<bool> close_done{false};
  Status close_status = Status::OK();
  std::thread closing([&] {
    close_status = database->Close();
    close_done.store(true);
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  const bool closed_while_parked = close_done.load();
  gate.Release();
  closing.join();
  EXPECT_FALSE(closed_while_parked)
      << "Close returned while an accepted write was still undurable";
  EXPECT_TRUE(close_status.ok()) << close_status.ToString();
  const auto completed = accepted.ValueOrDie().Wait();
  ASSERT_TRUE(completed.ok()) << completed.status().ToString();
  EXPECT_EQ(completed.ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_EQ(completed.ValueOrDie().commit_seq, CommitSeq{1});
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest, PipelinedReportsIndeterminateThroughWait) {
  char pattern[] = "/tmp/cedar_kernel_pipelined_indeterminate_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 8,
      .group_commit_window_us = 0,
      .predicted_preflight_min_bytes = 0,
      .commit_fault_injector_for_testing = [] {
        return Status::Indeterminate("test", "injected uncertain write");
      }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  auto transaction = database->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{9'501}}),
                           ValidTime{10})
                  .ok());
  auto accepted = transaction.ValueOrDie()->CommitAsyncPipelined();
  ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();
  const auto completed = accepted.ValueOrDie().Wait();
  ASSERT_TRUE(completed.ok()) << completed.status().ToString();
  EXPECT_EQ(completed.ValueOrDie().outcome, CommitOutcome::kIndeterminate);
  EXPECT_TRUE(completed.ValueOrDie().status.IsIndeterminate());
  EXPECT_EQ(completed.ValueOrDie().commit_seq, CommitSeq{});
  database.reset();
  std::filesystem::remove_all(path);
}

// B5 regression: a WAL-less terminal outcome (pre-write error,
// RecoveryRequired, pre-durability indeterminate) publishes its real epoch
// result but never releases the async submission ticket (the WAL-durable
// callback does not run). Close's executor sweep used to fail that ticket and
// overwrite the handle with a synthetic ShutdownInProgress, masking the real
// terminal state - including kIndeterminate, where reporting a definite abort
// invites a retry of a write that may already be durable. The sweep must not
// preempt an epoch-bound terminal result.
bool IsInjectedIndeterminate(const Status& status) {
  return status.IsIndeterminate();
}

bool IsInjectedRecoveryRequired(const Status& status) {
  return status.IsRecoveryRequired();
}

void ExpectWalLessTerminalSurvivesClose(Status injected, bool pipelined,
                                        CommitOutcome expected_outcome,
                                        bool (*status_matches)(const Status&)) {
  char pattern[] = "/tmp/cedar_kernel_walless_close_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 4,
      .group_commit_window_us = 0,
      .predicted_preflight_min_bytes = 0,
      .commit_prewrite_fault_injector_for_testing =
          [injected] { return injected; }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
  auto transaction = database->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{9'580}}),
                           ValidTime{10})
                  .ok());
  auto submitted = pipelined ? transaction.ValueOrDie()->CommitAsyncPipelined()
                             : transaction.ValueOrDie()->CommitAsync();
  ASSERT_TRUE(submitted.ok()) << submitted.status().ToString();
  CommitHandle handle = std::move(submitted).ValueOrDie();

  // Close drains the (already terminal but still ticketed) request and then the
  // executor sweep fails every remaining ticket. The real terminal outcome must
  // survive that sweep.
  const Status closed = database->Close();
  ASSERT_TRUE(closed.ok()) << closed.ToString();
  const auto completed = handle.Wait();
  ASSERT_TRUE(completed.ok()) << completed.status().ToString();
  EXPECT_EQ(completed.ValueOrDie().outcome, expected_outcome);
  EXPECT_TRUE(status_matches(completed.ValueOrDie().status))
      << completed.ValueOrDie().status.ToString();
  EXPECT_FALSE(completed.ValueOrDie().status.IsShutdownInProgress());
  std::filesystem::remove_all(path);
}

// Legacy (non-pipelined) variant of the same WAL-less terminal contract. The
// public CommitAsync() reports a failed terminal as an error status instead of
// returning a handle, so the retained kernel handle can only be observed at the
// Database::Impl submission layer. This exercises the exact legacy submission
// (pipelined=false), its retained ticket, and the shutdown sweep that used to
// overwrite the epoch result with ShutdownInProgress.
void ExpectWalLessLegacyHandleSurvivesStop(Status injected,
                                           bool (*status_matches)(const Status&)) {
  char pattern[] = "/tmp/cedar_kernel_walless_legacy_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::shared_ptr<CommitHandle::State> observed_handle;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 4,
      .group_commit_window_us = 0,
      .predicted_preflight_min_bytes = 0,
      .commit_prewrite_fault_injector_for_testing =
          [injected] { return injected; },
      .async_commit_handle_observer_for_testing =
          [&observed_handle](
              const std::shared_ptr<CommitHandle::State>& state) {
            observed_handle = state;
          }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
  auto transaction = database->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{9'590}}),
                           ValidTime{10})
                  .ok());
  // The legacy caller sees the real terminal status directly.
  const auto submitted = transaction.ValueOrDie()->CommitAsync();
  ASSERT_FALSE(submitted.ok());
  EXPECT_TRUE(status_matches(submitted.status())) << submitted.status().ToString();
  ASSERT_NE(observed_handle, nullptr)
      << "legacy CommitAsync did not expose its terminal handle";

  // Close drains the terminal (but still ticketed) request and its executor
  // sweep fails the retained ticket. The published epoch terminal must survive.
  const Status closed = database->Close();
  ASSERT_TRUE(closed.ok()) << closed.ToString();
  std::shared_ptr<internal::EpochCompletion> completion;
  size_t ordinal = 0;
  {
    std::lock_guard<std::mutex> lock(observed_handle->mutex);
    ASSERT_FALSE(observed_handle->result.has_value())
        << "the close sweep overwrote the published epoch terminal with "
        << observed_handle->result->status.ToString();
    completion = observed_handle->epoch_completion;
    ordinal = observed_handle->epoch_result_ordinal;
  }
  ASSERT_NE(completion, nullptr);
  // This is exactly what CommitHandle::Wait() now returns for a bound epoch.
  const auto terminal = completion->WaitForResult(ordinal);
  ASSERT_TRUE(terminal.ok()) << terminal.status().ToString();
  EXPECT_TRUE(status_matches(terminal.ValueOrDie().status))
      << terminal.ValueOrDie().status.ToString();
  EXPECT_FALSE(terminal.ValueOrDie().status.IsShutdownInProgress());
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest, WalLessIndeterminateLegacySurvivesCloseSweep) {
  ExpectWalLessLegacyHandleSurvivesStop(
      Status::Indeterminate("test", "injected pre-durability uncertainty"),
      IsInjectedIndeterminate);
}

TEST(KernelGroupCommitTest, WalLessIndeterminatePipelinedSurvivesCloseSweep) {
  ExpectWalLessTerminalSurvivesClose(
      Status::Indeterminate("test", "injected pre-durability uncertainty"),
      /*pipelined=*/true, CommitOutcome::kIndeterminate,
      IsInjectedIndeterminate);
}

TEST(KernelGroupCommitTest, WalLessRecoveryRequiredLegacySurvivesCloseSweep) {
  ExpectWalLessLegacyHandleSurvivesStop(
      Status::RecoveryRequired("test", "injected recovery requirement"),
      IsInjectedRecoveryRequired);
}

TEST(KernelGroupCommitTest, WalLessRecoveryRequiredPipelinedSurvivesCloseSweep) {
  ExpectWalLessTerminalSurvivesClose(
      Status::RecoveryRequired("test", "injected recovery requirement"),
      /*pipelined=*/true, CommitOutcome::kIndeterminate,
      IsInjectedRecoveryRequired);
}

// B5 ordering regression: the append worker used to signal the drained barrier
// before it published the epoch's terminal results, so the Close sweep could
// observe (and overwrite) a handle whose real outcome was not published yet.
// This observer fires at the exact drained notification and records whether the
// epoch was already published at that point; it must always have been.
TEST(KernelGroupCommitTest, ClosePublishesEpochTerminalBeforeDrainedBarrier) {
  char pattern[] = "/tmp/cedar_kernel_drain_publication_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::mutex observations_mutex;
  std::vector<bool> published_at_drain;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 4,
      .group_commit_window_us = 0,
      .predicted_preflight_min_bytes = 0,
      .commit_prewrite_fault_injector_for_testing = [] {
        return Status::Indeterminate("test", "injected pre-durability uncertainty");
      },
      .append_drained_observer_for_testing = [&](bool epoch_published) {
        std::lock_guard<std::mutex> lock(observations_mutex);
        published_at_drain.push_back(epoch_published);
      }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
  auto transaction = database->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{9'581}}),
                           ValidTime{10})
                  .ok());
  auto accepted = transaction.ValueOrDie()->CommitAsyncPipelined();
  ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();
  CommitHandle handle = std::move(accepted).ValueOrDie();

  ASSERT_TRUE(database->Close().ok());
  {
    std::lock_guard<std::mutex> lock(observations_mutex);
    ASSERT_FALSE(published_at_drain.empty())
        << "the drained barrier was never observed";
    for (bool epoch_published : published_at_drain) {
      EXPECT_TRUE(epoch_published)
          << "the drained barrier was signalled before the epoch terminal "
             "result was published";
    }
  }
  const auto completed = handle.Wait();
  ASSERT_TRUE(completed.ok()) << completed.status().ToString();
  EXPECT_EQ(completed.ValueOrDie().outcome, CommitOutcome::kIndeterminate);
  EXPECT_TRUE(completed.ValueOrDie().status.IsIndeterminate());
  std::filesystem::remove_all(path);
}

// F6a: publication is terminal-result fan-out, not part of the append
// bookkeeping that gates the next collection. The F3 placement called
// EpochCompletion::Publish inside append_commit_mutex, so every waiter woken by
// the publication immediately contended with the bookkeeping lock and the next
// queue admission had to wait for all post-publication bookkeeping. This gate
// parks the publication and then observes whether another commit can still be
// admitted (admission needs the same bookkeeping lock).
class EpochPublicationGate {
 public:
  void Hook() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!armed_) return;
    armed_ = false;
    entered_ = true;
    changed_.notify_all();
    changed_.wait(lock, [this] { return released_; });
  }

  void Arm() {
    std::lock_guard<std::mutex> lock(mutex_);
    armed_ = true;
    entered_ = false;
    released_ = false;
  }

  bool WaitUntilEntered(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mutex_);
    return changed_.wait_for(lock, timeout, [this] { return entered_; });
  }

  void Release() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      released_ = true;
    }
    changed_.notify_all();
  }

 private:
  std::mutex mutex_;
  std::condition_variable changed_;
  bool armed_ = false;
  bool entered_ = false;
  bool released_ = false;
};

TEST(KernelGroupCommitTest, EpochPublicationDoesNotSerializeAppendAdmission) {
  char pattern[] = "/tmp/cedar_kernel_publication_admission_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  EpochPublicationGate gate;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 1,
      .group_commit_window_us = 0,
      .collection_latency_slo_us_for_testing = 60'000'000,
      .epoch_publication_observer_for_testing = [&gate] { gate.Hook(); },
      .runtime_pressure_override_for_testing = PinHealthyPressure});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();

  auto first_transaction = database->BeginTransaction();
  ASSERT_TRUE(first_transaction.ok()) << first_transaction.status().ToString();
  ASSERT_TRUE(first_transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{9'700}}),
                           ValidTime{1})
                  .ok());
  gate.Arm();
  auto first_submission = std::async(std::launch::async, [&] {
    return first_transaction.ValueOrDie()->CommitAsyncPipelined();
  });
  const bool parked = gate.WaitUntilEntered(std::chrono::seconds(5));
  if (!parked) gate.Release();
  ASSERT_TRUE(parked) << "the append worker never reached the publication boundary";
  ASSERT_EQ(first_submission.wait_for(std::chrono::seconds(5)),
            std::future_status::ready);
  auto first_accepted = first_submission.get();
  ASSERT_TRUE(first_accepted.ok()) << first_accepted.status().ToString();

  // The worker is parked at publication. This second admission only needs the
  // append bookkeeping lock, which must therefore not be held across publish.
  auto second_transaction = database->BeginTransaction();
  ASSERT_TRUE(second_transaction.ok()) << second_transaction.status().ToString();
  ASSERT_TRUE(second_transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{9'701}}),
                           ValidTime{2})
                  .ok());
  auto second_submission = std::async(std::launch::async, [&] {
    return second_transaction.ValueOrDie()->CommitAsyncPipelined();
  });
  const bool admitted_while_parked =
      second_submission.wait_for(std::chrono::seconds(2)) ==
      std::future_status::ready;
  if (!admitted_while_parked) gate.Release();
  auto second_accepted = second_submission.get();
  EXPECT_TRUE(admitted_while_parked)
      << "append queue admission blocked behind a parked epoch publication";
  ASSERT_TRUE(second_accepted.ok()) << second_accepted.status().ToString();
  gate.Release();

  const auto first_completed = first_accepted.ValueOrDie().Wait();
  ASSERT_TRUE(first_completed.ok()) << first_completed.status().ToString();
  EXPECT_EQ(first_completed.ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_EQ(first_completed.ValueOrDie().commit_seq, CommitSeq{1});
  const auto second_completed = second_accepted.ValueOrDie().Wait();
  ASSERT_TRUE(second_completed.ok()) << second_completed.status().ToString();
  EXPECT_EQ(second_completed.ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_EQ(second_completed.ValueOrDie().commit_seq, CommitSeq{2});
  EXPECT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

// F6a: publishing outside the bookkeeping lock must not open a drain window.
// The drained barrier is what lets Close proceed to the executor sweep and to
// store teardown, so it may only fire once the epoch's terminal results are
// published. The pending-publication count keeps the drain predicate closed
// until the worker finishes the out-of-lock publication, and no epoch may be
// published twice.
TEST(KernelGroupCommitTest,
     CloseDrainWaitsForEpochPublicationAndObservesItPublished) {
  char pattern[] = "/tmp/cedar_kernel_publication_drain_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  EpochPublicationGate gate;
  std::mutex observations_mutex;
  std::vector<bool> published_at_drain;
  std::atomic<uint32_t> publications{0};
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 1,
      .group_commit_window_us = 0,
      .collection_latency_slo_us_for_testing = 60'000'000,
      .epoch_publication_observer_for_testing =
          [&] {
            ++publications;
            gate.Hook();
          },
      .append_drained_observer_for_testing = [&](bool epoch_published) {
        std::lock_guard<std::mutex> lock(observations_mutex);
        published_at_drain.push_back(epoch_published);
      },
      .runtime_pressure_override_for_testing = PinHealthyPressure});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
  auto transaction = database->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{9'710}}),
                           ValidTime{1})
                  .ok());
  gate.Arm();
  auto accepted = transaction.ValueOrDie()->CommitAsyncPipelined();
  ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();
  CommitHandle handle = std::move(accepted).ValueOrDie();
  const bool parked = gate.WaitUntilEntered(std::chrono::seconds(5));
  if (!parked) gate.Release();
  ASSERT_TRUE(parked) << "the append worker never reached the publication boundary";

  std::atomic<bool> close_done{false};
  Status close_status = Status::OK();
  std::thread closing([&] {
    close_status = database->Close();
    close_done.store(true);
  });
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  const bool closed_while_publication_parked = close_done.load();
  gate.Release();
  closing.join();
  EXPECT_FALSE(closed_while_publication_parked)
      << "Close drained an epoch whose terminal results were not published";
  EXPECT_TRUE(close_status.ok()) << close_status.ToString();
  {
    std::lock_guard<std::mutex> lock(observations_mutex);
    ASSERT_FALSE(published_at_drain.empty())
        << "the drained barrier was never observed";
    for (bool epoch_published : published_at_drain) {
      EXPECT_TRUE(epoch_published)
          << "the drained barrier was signalled before the epoch terminal "
             "result was published";
    }
  }
  EXPECT_EQ(publications.load(), 1U)
      << "the epoch terminal result was published more than once";
  const auto completed = handle.Wait();
  ASSERT_TRUE(completed.ok()) << completed.status().ToString();
  EXPECT_EQ(completed.ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_EQ(completed.ValueOrDie().commit_seq, CommitSeq{1});
  std::filesystem::remove_all(path);
}

// F6b: the transaction path registers commit calls without taking
// Database::Impl::mutex. These tests pin the registration protocol against the
// Close drain: Close must reach its drain wait while a commit is registered
// (nothing serializes it behind the transaction path), and it may not finish
// before that registration withdraws or the commit ends.
class CommitRegistrationGate {
 public:
  void Hook() {
    std::unique_lock<std::mutex> lock(mutex_);
    if (!armed_) return;
    armed_ = false;
    entered_ = true;
    changed_.notify_all();
    changed_.wait(lock, [this] { return released_; });
  }

  void Arm() {
    std::lock_guard<std::mutex> lock(mutex_);
    armed_ = true;
    entered_ = false;
    released_ = false;
  }

  bool WaitUntilEntered(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lock(mutex_);
    return changed_.wait_for(lock, timeout, [this] { return entered_; });
  }

  void Release() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      released_ = true;
    }
    changed_.notify_all();
  }

 private:
  std::mutex mutex_;
  std::condition_variable changed_;
  bool armed_ = false;
  bool entered_ = false;
  bool released_ = false;
};

TEST(KernelTransactionCommitPathTest,
     CloseReachesDrainWhileCommitRegistrationIsParked) {
  char pattern[] = "/tmp/cedar_kernel_commit_registration_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  CommitRegistrationGate registration;
  std::mutex drain_mutex;
  std::condition_variable drain_cv;
  bool drain_wait_entered = false;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 1,
      .group_commit_window_us = 0,
      .transaction_commit_registration_observer_for_testing =
          [&registration] { registration.Hook(); },
      .active_commit_drain_wait_observer_for_testing = [&] {
        {
          std::lock_guard<std::mutex> lock(drain_mutex);
          drain_wait_entered = true;
        }
        drain_cv.notify_all();
      }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
  auto transaction = database->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{9'720}}),
                           ValidTime{1})
                  .ok());
  registration.Arm();
  Status commit_status = Status::OK();
  std::jthread committing([&] {
    commit_status = transaction.ValueOrDie()->Commit().status();
  });
  if (!registration.WaitUntilEntered(std::chrono::seconds(5))) {
    registration.Release();
    FAIL() << "the commit never reached the registration boundary";
  }
  Status close_status = Status::OK();
  std::jthread closing([&] { close_status = database->Close(); });
  bool observed_drain_wait = false;
  {
    std::unique_lock<std::mutex> lock(drain_mutex);
    observed_drain_wait = drain_cv.wait_for(lock, std::chrono::seconds(5), [&] {
      return drain_wait_entered;
    });
  }
  registration.Release();
  committing.join();
  closing.join();
  ASSERT_TRUE(observed_drain_wait)
      << "Close could not reach its commit drain while a transaction commit was "
         "registering; the transaction path still serializes on a global lock";
  // The commit observed the close transition and withdrew without touching the
  // store, and EndCommit released the drain.
  EXPECT_TRUE(commit_status.IsShutdownInProgress()) << commit_status.ToString();
  // The transaction still owns its snapshot pin, so the first Close may report
  // SnapshotPinned; after the transaction dies the retry must succeed.
  if (!close_status.ok()) {
    EXPECT_TRUE(close_status.IsSnapshotPinned()) << close_status.ToString();
    transaction.ValueOrDie().reset();
    EXPECT_TRUE(database->Close().ok());
  }
  std::filesystem::remove_all(path);
}

TEST(KernelTransactionCommitPathTest, CloseWaitsForAdmittedCommitToFinish) {
  char pattern[] = "/tmp/cedar_kernel_commit_admitted_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  CommitRegistrationGate admitted;
  std::mutex drain_mutex;
  std::condition_variable drain_cv;
  bool drain_wait_entered = false;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 1,
      .group_commit_window_us = 0,
      .transaction_commit_admitted_observer_for_testing =
          [&admitted] { admitted.Hook(); },
      .active_commit_drain_wait_observer_for_testing = [&] {
        {
          std::lock_guard<std::mutex> lock(drain_mutex);
          drain_wait_entered = true;
        }
        drain_cv.notify_all();
      }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
  auto transaction = database->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{9'721}}),
                           ValidTime{1})
                  .ok());
  admitted.Arm();
  StatusOr<CommitResult> committed = Status::InvalidArgument("test", "not run");
  std::jthread committing([&] {
    committed = transaction.ValueOrDie()->Commit();
  });
  if (!admitted.WaitUntilEntered(std::chrono::seconds(5))) {
    admitted.Release();
    FAIL() << "the commit was never admitted";
  }
  Status close_status = Status::OK();
  std::jthread closing([&] { close_status = database->Close(); });
  bool observed_drain_wait = false;
  {
    std::unique_lock<std::mutex> lock(drain_mutex);
    observed_drain_wait = drain_cv.wait_for(lock, std::chrono::seconds(5), [&] {
      return drain_wait_entered;
    });
  }
  admitted.Release();
  committing.join();
  closing.join();
  ASSERT_TRUE(observed_drain_wait)
      << "Close never reached its commit drain for a registered commit";
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  EXPECT_EQ(committed.ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_TRUE(close_status.ok()) << close_status.ToString();
  std::filesystem::remove_all(path);
}

// F6b: a transaction without as_of is by construction a current-vintage pin, so
// BeginTransaction must use the lock-free BeginCurrentSnapshot path instead of
// the registered BeginSnapshot path (which takes lifecycle_mutex_ and
// epoch_mutex). The registered path fires snapshot_open_observer_for_testing;
// after the current epoch exists, BeginTransaction must not fire it.
TEST(KernelTransactionCommitPathTest, BeginTransactionUsesLockFreeCurrentSnapshot) {
  char pattern[] = "/tmp/cedar_kernel_commit_fast_begin_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::atomic<uint32_t> registered_opens{0};
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .snapshot_open_observer_for_testing = [&registered_opens] {
        registered_opens.fetch_add(1, std::memory_order_relaxed);
      }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
  // Establish the shared current epoch through one registered open.
  {
    auto warm = database->BeginSnapshot();
    ASSERT_TRUE(warm.ok()) << warm.status().ToString();
    Snapshot snapshot = std::move(warm).ConsumeValueOrDie();
    EXPECT_EQ(snapshot.commit_seq(), CommitSeq{0});
  }
  ASSERT_GT(registered_opens.load(), 0U);
  registered_opens.store(0, std::memory_order_relaxed);

  auto transaction = database->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  EXPECT_EQ(registered_opens.load(), 0U)
      << "BeginTransaction still took the registered BeginSnapshot path";
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{9'722}}),
                           ValidTime{1})
                  .ok());
  const auto committed = transaction.ValueOrDie()->Commit();
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  EXPECT_EQ(committed.ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

// The fast path falls back to the registered path when the store is closing;
// the observable contract (a ShutdownInProgress error from BeginTransaction) is
// unchanged.
TEST(KernelTransactionCommitPathTest, BeginTransactionReportsCloseInProgress) {
  char pattern[] = "/tmp/cedar_kernel_commit_close_begin_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::mutex stage_mutex;
  std::condition_variable stage_cv;
  bool admission_closed = false;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .shutdown_stage_observer_for_testing = [&](const char* stage) {
        if (std::strcmp(stage, "queue_admission_closed") != 0) return;
        {
          std::lock_guard<std::mutex> lock(stage_mutex);
          admission_closed = true;
        }
        stage_cv.notify_all();
      }});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
  Status close_status = Status::OK();
  std::jthread closing([&] { close_status = database->Close(); });
  bool observed = false;
  {
    std::unique_lock<std::mutex> lock(stage_mutex);
    observed = stage_cv.wait_for(lock, std::chrono::seconds(5),
                                 [&] { return admission_closed; });
  }
  const auto begun = database->BeginTransaction();
  closing.join();
  ASSERT_TRUE(observed) << "Close never published its admission-closed stage";
  ASSERT_FALSE(begun.ok());
  EXPECT_TRUE(begun.status().IsShutdownInProgress())
      << begun.status().ToString();
  EXPECT_TRUE(close_status.ok()) << close_status.ToString();
  std::filesystem::remove_all(path);
}

TEST_F(KernelCommitTest, PipelinedReportsValidationAbortConsistentWithLegacy) {
  std::unique_ptr<Transaction> winner = Begin();
  std::unique_ptr<Transaction> pipelined = Begin();
  std::unique_ptr<Transaction> legacy = Begin();
  const EntityFact contested =
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{9'601}});
  ASSERT_TRUE(winner->Assert(contested, ValidTime{10}).ok());
  ASSERT_TRUE(pipelined->Assert(contested, ValidTime{10}).ok());
  ASSERT_TRUE(legacy->Assert(contested, ValidTime{10}).ok());
  const auto committed = winner->Commit();
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();

  auto pipelined_accepted = pipelined->CommitAsyncPipelined();
  ASSERT_TRUE(pipelined_accepted.ok()) << pipelined_accepted.status().ToString();
  const auto pipelined_result = pipelined_accepted.ValueOrDie().Wait();
  ASSERT_TRUE(pipelined_result.ok()) << pipelined_result.status().ToString();
  EXPECT_EQ(pipelined_result.ValueOrDie().outcome, CommitOutcome::kAborted);
  EXPECT_TRUE(pipelined_result.ValueOrDie().status.IsConflict());

  auto legacy_accepted = legacy->CommitAsync();
  ASSERT_TRUE(legacy_accepted.ok()) << legacy_accepted.status().ToString();
  const auto legacy_result = legacy_accepted.ValueOrDie().Wait();
  ASSERT_TRUE(legacy_result.ok()) << legacy_result.status().ToString();
  EXPECT_EQ(legacy_result.ValueOrDie().outcome,
            pipelined_result.ValueOrDie().outcome);
  EXPECT_TRUE(legacy_result.ValueOrDie().status.IsConflict());
  EXPECT_EQ(legacy_result.ValueOrDie().commit_seq,
            pipelined_result.ValueOrDie().commit_seq);
}

TEST_F(KernelCommitTest, RejectsPipelinedCommitWithNonzeroDeadline) {
  std::unique_ptr<Transaction> transaction = Begin(TransactionOptions{
      .isolation = IsolationLevel::kSnapshot, .commit_deadline_us = 100'000});
  ASSERT_TRUE(transaction->Assert(
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{9'701}}), ValidTime{10}).ok());
  const auto accepted = transaction->CommitAsyncPipelined();
  ASSERT_FALSE(accepted.ok());
  EXPECT_TRUE(accepted.status().IsInvalidArgument())
      << accepted.status().ToString();
}

// Pins the predecided-slot growth fallback: the predecessor write is parked,
// preflight decides a one-request slot from the first pipeline admission, and
// the queue then grows past that snapshot. Pipelined mode must discard the
// prediction, select the enlarged group through the general path, and keep
// every accepted request with one WAL sync per group. In legacy mode the same
// growth must NOT discard the slot (the default CommitAsync path is unchanged).
enum class SlotGrowthMode { kPipelined, kLegacy };

void ExercisePredecidedSlotGrowth(SlotGrowthMode mode,
                                  CommitPipelineMetrics* metrics,
                                  std::vector<CommitSeq>* sequences) {
  constexpr size_t kTransactions = 5;
  char pattern[] = "/tmp/cedar_kernel_slot_growth_XXXXXX";
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
      .group_commit_max_batch_size = 8,
      .group_commit_window_us = 0,
      .predicted_preflight_min_bytes = 0,
      .commit_prewrite_fault_injector_for_testing =
          [&] {
            if (!inject_first.exchange(false, std::memory_order_acq_rel)) {
              return Status::OK();
            }
            std::unique_lock<std::mutex> lock(prewrite_mutex);
            first_prewrite_entered = true;
            prewrite_cv.notify_all();
            prewrite_cv.wait(lock, [&] { return release_first_prewrite; });
            return Status::OK();
          },
      .group_commit_max_queue_requests = 16,
      .group_commit_max_queue_bytes = 1ULL << 20,
      .append_commit_enqueued_observer_for_testing = [&] {
        std::lock_guard<std::mutex> lock(enqueue_mutex);
        ++enqueued;
        enqueue_cv.notify_all();
      },
      .collection_latency_slo_us_for_testing = 60'000'000,
      .runtime_pressure_override_for_testing = PinHealthyPressure});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();

  std::vector<std::unique_ptr<Transaction>> transactions;
  transactions.reserve(kTransactions);
  for (size_t index = 0; index < kTransactions; ++index) {
    auto transaction = database->BeginTransaction();
    ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
    ASSERT_TRUE(transaction.ValueOrDie()
                    ->Assert(EntityFact::Vertex(VertexRef{
                                  PartId{0}, VertexId{9'810 + index}}),
                             ValidTime{static_cast<uint64_t>(index + 1)})
                    .ok());
    transactions.push_back(std::move(transaction).ConsumeValueOrDie());
  }

  std::vector<std::optional<StatusOr<CommitHandle>>> accepted(kTransactions);
  std::vector<std::thread> threads;
  threads.emplace_back([&] { accepted[0].emplace(transactions[0]->CommitAsync()); });
  {
    std::unique_lock<std::mutex> lock(prewrite_mutex);
    ASSERT_TRUE(prewrite_cv.wait_for(
        lock, std::chrono::seconds(2), [&] { return first_prewrite_entered; }));
  }
  {
    std::unique_lock<std::mutex> lock(enqueue_mutex);
    ASSERT_TRUE(enqueue_cv.wait_for(lock, std::chrono::seconds(2),
                                    [&] { return enqueued == 1; }));
  }
  for (size_t index = 1; index < kTransactions; ++index) {
    if (mode == SlotGrowthMode::kPipelined) {
      accepted[index].emplace(transactions[index]->CommitAsyncPipelined());
    } else {
      threads.emplace_back([&, index] {
        accepted[index].emplace(transactions[index]->CommitAsync());
      });
    }
    {
      std::unique_lock<std::mutex> lock(enqueue_mutex);
      ASSERT_TRUE(enqueue_cv.wait_for(
          lock, std::chrono::seconds(2),
          [&] { return enqueued == index + 1; }));
    }
    if (index == 1) {
      // Let preflight decide the one-request slot from this first pipeline
      // admission before growing the queue with the remaining submissions.
      const auto eligible_deadline = std::chrono::steady_clock::now() +
                                     std::chrono::seconds(2);
      while (std::chrono::steady_clock::now() < eligible_deadline &&
             database->GetCommitPipelineMetrics().n_plus_one_eligible == 0) {
        std::this_thread::yield();
      }
      ASSERT_GE(database->GetCommitPipelineMetrics().n_plus_one_eligible, 1U);
    }
  }

  {
    std::lock_guard<std::mutex> lock(prewrite_mutex);
    release_first_prewrite = true;
  }
  prewrite_cv.notify_all();
  for (auto& thread : threads) thread.join();

  sequences->clear();
  for (auto& result : accepted) {
    ASSERT_TRUE(result.has_value());
    ASSERT_TRUE(result->ok()) << result->status().ToString();
    const auto completed = result->ValueOrDie().Wait();
    ASSERT_TRUE(completed.ok()) << completed.status().ToString();
    EXPECT_EQ(completed.ValueOrDie().outcome, CommitOutcome::kCommitted);
    sequences->push_back(completed.ValueOrDie().commit_seq);
  }
  *metrics = database->GetCommitPipelineMetrics();
  ASSERT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest,
     PipelinedDiscardsPredecidedSlotWhenQueueGrowsPastPrediction) {
  CommitPipelineMetrics metrics;
  std::vector<CommitSeq> sequences;
  ExercisePredecidedSlotGrowth(SlotGrowthMode::kPipelined, &metrics, &sequences);
  ASSERT_EQ(sequences.size(), 5U);
  for (size_t index = 0; index < sequences.size(); ++index) {
    EXPECT_EQ(sequences[index], CommitSeq{index + 1});
  }
  const size_t reason_index =
      static_cast<size_t>(NPlusOneDiscardReason::kGenerationMismatch);
  ASSERT_LT(reason_index, metrics.n_plus_one_discarded_by_reason.size());
  EXPECT_GE(metrics.n_plus_one_discarded_by_reason[reason_index], 1U);
  EXPECT_GE(metrics.n_plus_one_discards, 1U);
  EXPECT_EQ(metrics.n_plus_one_promoted_epochs, 0U);
  // One group per WAL sync, every accepted request accounted for exactly once.
  EXPECT_EQ(metrics.group_fill.groups, 2U);
  EXPECT_EQ(metrics.epochs, metrics.group_fill.groups);
  EXPECT_EQ(metrics.latency.wal_sync.count, metrics.group_fill.groups);
  EXPECT_EQ(metrics.group_fill.total_transactions, 5U);
  EXPECT_EQ(metrics.submitted, 5U);
  EXPECT_EQ(metrics.published, 5U);
  EXPECT_EQ(metrics.durably_accepted, 5U);
  EXPECT_EQ(metrics.rejected, 0U);
}

TEST(KernelGroupCommitTest,
     LegacyOnlyDatabasePromotesPredecidedSlotPrefixOnQueueGrowth) {
  CommitPipelineMetrics metrics;
  std::vector<CommitSeq> sequences;
  ExercisePredecidedSlotGrowth(SlotGrowthMode::kLegacy, &metrics, &sequences);
  ASSERT_EQ(sequences.size(), 5U);
  for (size_t index = 0; index < sequences.size(); ++index) {
    EXPECT_EQ(sequences[index], CommitSeq{index + 1});
  }
  const size_t reason_index =
      static_cast<size_t>(NPlusOneDiscardReason::kGenerationMismatch);
  ASSERT_LT(reason_index, metrics.n_plus_one_discarded_by_reason.size());
  // No pipelined admission ever happened, so the growth fallback must stay
  // disabled and the predicted prefix must be promoted as before Task 13.
  EXPECT_EQ(metrics.n_plus_one_discarded_by_reason[reason_index], 0U);
  EXPECT_EQ(metrics.n_plus_one_discards, 0U);
  EXPECT_EQ(metrics.n_plus_one_discarded_epochs, 0U);
  EXPECT_GE(metrics.n_plus_one_promoted, 1U);
  // The promoted prefix is one request; the other queued requests cannot join
  // it, so the remaining groups are at least two.
  EXPECT_GE(metrics.group_fill.groups, 3U);
  EXPECT_EQ(metrics.epochs, metrics.group_fill.groups);
  EXPECT_EQ(metrics.latency.wal_sync.count, metrics.group_fill.groups);
  EXPECT_EQ(metrics.group_fill.total_transactions, 5U);
  EXPECT_EQ(metrics.submitted, 5U);
  EXPECT_EQ(metrics.published, 5U);
  EXPECT_EQ(metrics.durably_accepted, 5U);
  EXPECT_EQ(metrics.rejected, 0U);
}

TEST(KernelGroupCommitTest, PipelinedGroupsMultipleCallersIntoOneWalWrite) {
  constexpr uint32_t kCallers = 2;
  constexpr uint32_t kPerCaller = 2;
  constexpr uint32_t kRequests = kCallers * kPerCaller;
  char pattern[] = "/tmp/cedar_kernel_pipelined_multicaller_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  std::mutex gate_mutex;
  std::condition_variable gate_cv;
  uint32_t enqueued = 0;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 8,
      .group_commit_window_us = 20'000,
      .predicted_preflight_min_bytes = 0,
      .append_commit_enqueued_observer_for_testing = [&] {
        std::lock_guard<std::mutex> lock(gate_mutex);
        ++enqueued;
        gate_cv.notify_all();
      },
      .append_commit_collection_observer_for_testing = [&] {
        std::unique_lock<std::mutex> lock(gate_mutex);
        gate_cv.wait_for(lock, std::chrono::seconds(30),
                         [&] { return enqueued >= kRequests; });
      },
      .collection_latency_slo_us_for_testing = 60'000'000,
      .runtime_pressure_override_for_testing = PinHealthyPressure});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();

  std::vector<std::vector<StatusOr<CommitHandle>>> accepted(kCallers);
  std::vector<std::thread> callers;
  for (uint32_t caller = 0; caller < kCallers; ++caller) {
    callers.emplace_back([&database, &accepted, caller] {
      for (uint32_t index = 0; index < kPerCaller; ++index) {
        auto transaction = database->BeginTransaction();
        if (!transaction.ok()) return;
        if (!transaction.ValueOrDie()
                 ->Assert(EntityFact::Vertex(VertexRef{
                               PartId{0},
                               VertexId{9'900 + caller * 10 + index}}),
                          ValidTime{10})
                 .ok()) {
          return;
        }
        accepted[caller].push_back(
            transaction.ValueOrDie()->CommitAsyncPipelined());
      }
    });
  }
  for (auto& caller : callers) caller.join();

  std::vector<CommitSeq> sequences;
  sequences.reserve(kRequests);
  for (auto& caller : accepted) {
    ASSERT_EQ(caller.size(), kPerCaller);
    for (auto& handle : caller) {
      ASSERT_TRUE(handle.ok()) << handle.status().ToString();
      const auto completed = handle.ValueOrDie().Wait();
      ASSERT_TRUE(completed.ok()) << completed.status().ToString();
      EXPECT_EQ(completed.ValueOrDie().outcome, CommitOutcome::kCommitted);
      sequences.push_back(completed.ValueOrDie().commit_seq);
    }
  }
  std::sort(sequences.begin(), sequences.end(),
            [](CommitSeq left, CommitSeq right) {
              return left.value < right.value;
            });
  ASSERT_EQ(sequences.size(), kRequests);
  for (size_t index = 0; index < sequences.size(); ++index) {
    EXPECT_EQ(sequences[index], CommitSeq{index + 1});
  }
  const CommitPipelineMetrics metrics = database->GetCommitPipelineMetrics();
  EXPECT_EQ(metrics.submitted, kRequests);
  EXPECT_EQ(metrics.group_fill.groups, 1U);
  EXPECT_EQ(metrics.group_fill.total_transactions, kRequests);
  EXPECT_EQ(metrics.epochs, 1U);
  EXPECT_EQ(metrics.latency.wal_sync.count, 1U);
  EXPECT_EQ(metrics.durably_accepted, kRequests);
  EXPECT_EQ(metrics.published, kRequests);
  EXPECT_EQ(metrics.rejected, 0U);
  EXPECT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest, PipelinedWaitIsIdempotentAndAcceptanceIsSticky) {
  char pattern[] = "/tmp/cedar_kernel_pipelined_idempotent_wait_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 8,
      .group_commit_window_us = 0,
      .predicted_preflight_min_bytes = 0});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
  auto transaction = database->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{9'950}}),
                           ValidTime{10})
                  .ok());
  auto accepted = transaction.ValueOrDie()->CommitAsyncPipelined();
  ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();
  CommitHandle handle = std::move(accepted).ValueOrDie();
  EXPECT_EQ(handle.acceptance(), CommitAcceptance::kEnqueued);

  const auto first = handle.Wait();
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  EXPECT_EQ(first.ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_EQ(first.ValueOrDie().commit_seq, CommitSeq{1});
  // acceptance() records the creation-time value and Wait() is repeatable.
  EXPECT_EQ(handle.acceptance(), CommitAcceptance::kEnqueued);
  const auto second = handle.Wait();
  ASSERT_TRUE(second.ok()) << second.status().ToString();
  EXPECT_EQ(second.ValueOrDie().outcome, first.ValueOrDie().outcome);
  EXPECT_EQ(second.ValueOrDie().commit_seq, first.ValueOrDie().commit_seq);
  EXPECT_EQ(second.ValueOrDie().txn_id, first.ValueOrDie().txn_id);
  EXPECT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

// F3 review minor: Wait() is idempotent for a legacy (non-pipelined) async
// handle bound to an epoch. Repeated calls must keep returning the same terminal
// result/status and must not consume or corrupt the retained terminal state.
TEST(KernelGroupCommitTest, LegacyAsyncWaitIsRepeatableAndIdempotent) {
  char pattern[] = "/tmp/cedar_kernel_legacy_idempotent_wait_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 8,
      .group_commit_window_us = 0,
      .predicted_preflight_min_bytes = 0});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
  auto transaction = database->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{9'951}}),
                           ValidTime{10})
                  .ok());
  auto accepted = transaction.ValueOrDie()->CommitAsync();
  ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();
  CommitHandle handle = std::move(accepted).ValueOrDie();
  EXPECT_EQ(handle.acceptance(), CommitAcceptance::kAccepted);

  const auto first = handle.Wait();
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  EXPECT_EQ(first.ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_EQ(first.ValueOrDie().commit_seq, CommitSeq{1});

  const auto second = handle.Wait();
  const auto third = handle.Wait();
  for (const auto* repeated : {&second, &third}) {
    ASSERT_TRUE(repeated->ok()) << repeated->status().ToString();
    EXPECT_EQ(repeated->ValueOrDie().outcome, first.ValueOrDie().outcome);
    EXPECT_EQ(repeated->ValueOrDie().commit_seq, first.ValueOrDie().commit_seq);
    EXPECT_EQ(repeated->ValueOrDie().txn_id, first.ValueOrDie().txn_id);
    EXPECT_EQ(repeated->ValueOrDie().status.ToString(),
              first.ValueOrDie().status.ToString());
  }
  // Wait() does not mutate the handle's immutable identity/acceptance.
  EXPECT_EQ(handle.txn_id(), first.ValueOrDie().txn_id);
  EXPECT_EQ(handle.acceptance(), CommitAcceptance::kAccepted);

  // The repeated waits did not disturb the committed visibility: the entity
  // state is readable and the pipeline reports exactly one committed
  // transaction.
  {
    const EntityFact committed =
        EntityFact::Vertex(VertexRef{PartId{0}, VertexId{9'951}});
    const auto snapshot = database->BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    const auto exists = snapshot.ValueOrDie().Exists(committed, ValidTime{10});
    ASSERT_TRUE(exists.ok()) << exists.status().ToString();
    EXPECT_TRUE(exists.ValueOrDie());
  }
  const CommitPipelineMetrics metrics = database->GetCommitPipelineMetrics();
  EXPECT_EQ(metrics.submitted, 1U);
  EXPECT_EQ(metrics.published, 1U);
  EXPECT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(KernelGroupCommitTest,
     PipelinedHandleWaitIsSafeAfterDatabaseDestruction) {
  char pattern[] = "/tmp/cedar_kernel_pipelined_handle_lifetime_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto opened = Database::Open(DatabaseOptions{
      .path = path,
      .group_commit_max_batch_size = 8,
      .group_commit_window_us = 0,
      .predicted_preflight_min_bytes = 0});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
  auto transaction = database->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{9'960}}),
                           ValidTime{10})
                  .ok());
  auto accepted = transaction.ValueOrDie()->CommitAsyncPipelined();
  ASSERT_TRUE(accepted.ok()) << accepted.status().ToString();
  CommitHandle handle = std::move(accepted).ValueOrDie();
  ASSERT_TRUE(database->Close().ok());
  database.reset();
  // The handle retains the terminal state (shared EpochCompletion); Wait() is
  // safe after the Database object is gone.
  const auto completed = handle.Wait();
  ASSERT_TRUE(completed.ok()) << completed.status().ToString();
  EXPECT_EQ(completed.ValueOrDie().outcome, CommitOutcome::kCommitted);
  EXPECT_EQ(completed.ValueOrDie().commit_seq, CommitSeq{1});
  EXPECT_EQ(handle.acceptance(), CommitAcceptance::kEnqueued);
  std::filesystem::remove_all(path);
}

}  // namespace
}  // namespace cedar
