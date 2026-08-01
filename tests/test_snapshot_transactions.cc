// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <array>
#include <barrier>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cedar/fact/fact_store.h"
#include "kernel/temporal_validation.h"

namespace cedar {
namespace {

PendingFactMutation VertexPut(uint64_t vertex_id, uint64_t valid_from) {
  return {EntityFact::Vertex(VertexId{vertex_id}).ref(), ValidTime{valid_from},
          FactOperation::kPut, 0, std::nullopt};
}

PendingFactMutation EdgePut(uint64_t edge_id, uint64_t valid_from) {
  return {EntityFact::Edge(EdgeId{edge_id}).ref(), ValidTime{valid_from},
          FactOperation::kPut, 0, std::nullopt};
}

class SnapshotTransactionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_snapshot_transactions_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    path_ = pattern;
    store_ = std::make_unique<FactStore>(FactStoreOptions{path_});
    ASSERT_TRUE(store_->Open().ok());
  }

  void TearDown() override {
    store_.reset();
    std::filesystem::remove_all(path_);
  }

  StoreCommitBatch BatchFromSnapshot(TxnId txn_id, const StoreSnapshot& snapshot,
                                     std::vector<PendingFactMutation> mutations) {
    auto dependencies = DeriveSnapshotWriteDependencies(*store_, snapshot, mutations);
    EXPECT_TRUE(dependencies.ok()) << dependencies.status().ToString();
    return StoreCommitBatch{txn_id, txn_id.value, std::move(mutations), {},
                            std::move(dependencies).ConsumeValueOrDie()};
  }

  std::string path_;
  std::unique_ptr<FactStore> store_;
};

TEST_F(SnapshotTransactionTest, ConflictsWhenConcurrentWritesShareInterval) {
  ASSERT_TRUE(store_->Commit(StoreCommitBatch{
                         TxnId{1}, 1, {VertexPut(1, 10)}, {}, {}})
                  .ok());
  const auto snapshot = store_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();

  StoreCommitBatch first =
      BatchFromSnapshot(TxnId{2}, snapshot.ValueOrDie(), {VertexPut(1, 15)});
  StoreCommitBatch second =
      BatchFromSnapshot(TxnId{3}, snapshot.ValueOrDie(), {VertexPut(1, 16)});

  ASSERT_TRUE(store_->Commit(first).ok());
  const auto conflicted = store_->Commit(second);
  EXPECT_TRUE(conflicted.status().IsConflict()) << conflicted.status().ToString();
}

TEST_F(SnapshotTransactionTest, ConflictsWithLaterCorrectionInsideCapturedInterval) {
  ASSERT_TRUE(store_->Commit(StoreCommitBatch{
                         TxnId{1}, 1, {VertexPut(1, 10), VertexPut(1, 30)}, {}, {}})
                  .ok());
  const auto snapshot = store_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();

  StoreCommitBatch later =
      BatchFromSnapshot(TxnId{2}, snapshot.ValueOrDie(), {VertexPut(1, 20)});
  StoreCommitBatch correction =
      BatchFromSnapshot(TxnId{3}, snapshot.ValueOrDie(), {VertexPut(1, 15)});
  ASSERT_EQ(correction.snapshot_write_dependencies.size(), 1U);
  EXPECT_EQ(correction.snapshot_write_dependencies[0].predecessor,
            std::optional<ValidTime>{ValidTime{10}});
  EXPECT_EQ(correction.snapshot_write_dependencies[0].successor,
            std::optional<ValidTime>{ValidTime{30}});

  ASSERT_TRUE(store_->Commit(later).ok());
  const auto conflicted = store_->Commit(correction);
  EXPECT_TRUE(conflicted.status().IsConflict()) << conflicted.status().ToString();
}

TEST_F(SnapshotTransactionTest, AllowsDisjointValidTimeIntervalsForOneFact) {
  ASSERT_TRUE(store_->Commit(StoreCommitBatch{
                         TxnId{1}, 1, {VertexPut(1, 10), VertexPut(1, 30)}, {}, {}})
                  .ok());
  const auto snapshot = store_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();

  StoreCommitBatch first =
      BatchFromSnapshot(TxnId{2}, snapshot.ValueOrDie(), {VertexPut(1, 15)});
  StoreCommitBatch second =
      BatchFromSnapshot(TxnId{3}, snapshot.ValueOrDie(), {VertexPut(1, 35)});

  ASSERT_TRUE(store_->Commit(first).ok());
  EXPECT_TRUE(store_->Commit(second).ok());
}

TEST_F(SnapshotTransactionTest, PermitsWriteSkewForIndependentFacts) {
  const auto snapshot = store_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();

  StoreCommitBatch first =
      BatchFromSnapshot(TxnId{1}, snapshot.ValueOrDie(), {VertexPut(1, 10)});
  StoreCommitBatch second =
      BatchFromSnapshot(TxnId{2}, snapshot.ValueOrDie(), {VertexPut(2, 10)});

  ASSERT_TRUE(store_->Commit(first).ok());
  EXPECT_TRUE(store_->Commit(second).ok());
}

TEST_F(SnapshotTransactionTest, TreatsEndpointAndEdgeStateAsIndependentFacts) {
  const EdgeIdentity identity{EdgeId{9}, VertexId{1}, VertexId{2}, 7};
  ASSERT_TRUE(store_->Commit(StoreCommitBatch{
                         TxnId{1}, 1,
                         {VertexPut(1, 10), VertexPut(2, 10), EdgePut(9, 10)},
                         {identity}, {}})
                  .ok());
  const auto snapshot = store_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();

  StoreCommitBatch edge =
      BatchFromSnapshot(TxnId{2}, snapshot.ValueOrDie(), {EdgePut(9, 20)});
  StoreCommitBatch endpoint =
      BatchFromSnapshot(TxnId{3}, snapshot.ValueOrDie(), {VertexPut(1, 20)});

  ASSERT_TRUE(store_->Commit(edge).ok());
  EXPECT_TRUE(store_->Commit(endpoint).ok());
}

TEST_F(SnapshotTransactionTest, RepeatedConcurrentContendersHaveOneWinner) {
  for (uint64_t iteration = 0; iteration < 20; ++iteration) {
    const uint64_t vertex_id = 100 + iteration;
    const TxnId seed_id{1000 + iteration * 3};
    ASSERT_TRUE(store_->Commit(StoreCommitBatch{
                           seed_id, seed_id.value, {VertexPut(vertex_id, 10)}, {}, {}})
                    .ok());
    const auto snapshot = store_->BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    StoreCommitBatch first = BatchFromSnapshot(
        TxnId{seed_id.value + 1}, snapshot.ValueOrDie(), {VertexPut(vertex_id, 15)});
    StoreCommitBatch second = BatchFromSnapshot(
        TxnId{seed_id.value + 2}, snapshot.ValueOrDie(), {VertexPut(vertex_id, 16)});

    std::barrier start(3);
    std::array<StatusOr<StoreCommitResult>, 2> results;
    std::thread first_thread([&] {
      start.arrive_and_wait();
      results[0] = store_->Commit(first);
    });
    std::thread second_thread([&] {
      start.arrive_and_wait();
      results[1] = store_->Commit(second);
    });
    start.arrive_and_wait();
    first_thread.join();
    second_thread.join();

    const bool first_committed = results[0].ok();
    const bool second_committed = results[1].ok();
    EXPECT_NE(first_committed, second_committed) << "iteration " << iteration;
    EXPECT_TRUE((first_committed ? results[1].status() : results[0].status())
                    .IsConflict())
        << "iteration " << iteration;
  }
}

}  // namespace
}  // namespace cedar
