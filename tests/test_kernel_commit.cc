// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>

#include "cedar/database.h"

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
  ASSERT_TRUE(transaction->Assert(EntityFact::Vertex(VertexId{1}), ValidTime{10}).ok());
  ASSERT_TRUE(transaction->Assert(EntityFact::Vertex(VertexId{2}), ValidTime{20}).ok());

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
  EXPECT_TRUE(snapshot.ValueOrDie().Exists(EntityFact::Vertex(VertexId{1}), ValidTime{10})
                  .ValueOrDie());
  EXPECT_TRUE(snapshot.ValueOrDie().Exists(EntityFact::Vertex(VertexId{2}), ValidTime{20})
                  .ValueOrDie());
}

TEST_F(KernelCommitTest, BindsEdgeIdentityAtomicallyAndRejectsConflictingBinding) {
  const EdgeIdentity identity{EdgeId{9}, VertexId{1}, VertexId{2}, 7};
  std::unique_ptr<Transaction> first = Begin();
  ASSERT_TRUE(first->Assert(EntityFact::Vertex(VertexId{1}), ValidTime{10}).ok());
  ASSERT_TRUE(first->Assert(EntityFact::Vertex(VertexId{2}), ValidTime{10}).ok());
  ASSERT_TRUE(first->Assert(identity, ValidTime{10}).ok());
  const auto first_commit = first->Commit();
  ASSERT_TRUE(first_commit.ok()) << first_commit.status().ToString();
  EXPECT_EQ(first_commit.ValueOrDie().outcome, CommitOutcome::kCommitted);

  const auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  EXPECT_TRUE(snapshot.ValueOrDie().Exists(EntityFact::Edge(EdgeId{9}), ValidTime{10})
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
  ASSERT_TRUE(seed->Assert(EntityFact::Vertex(VertexId{1}), ValidTime{10}).ok());
  ASSERT_EQ(seed->Commit().ValueOrDie().outcome, CommitOutcome::kCommitted);

  std::unique_ptr<Transaction> strict = Begin(
      TransactionOptions{.isolation = IsolationLevel::kStrict});
  ASSERT_TRUE(strict->Exists(EntityFact::Vertex(VertexId{1}), ValidTime{10})
                  .ValueOrDie());
  ASSERT_TRUE(strict->Assert(EntityFact::Vertex(VertexId{2}), ValidTime{10}).ok());

  std::unique_ptr<Transaction> concurrent = Begin();
  ASSERT_TRUE(concurrent->Retract(EntityFact::Vertex(VertexId{1}), ValidTime{10}).ok());
  ASSERT_EQ(concurrent->Commit().ValueOrDie().outcome, CommitOutcome::kCommitted);

  const auto rejected = strict->Commit();
  ASSERT_TRUE(rejected.ok()) << rejected.status().ToString();
  EXPECT_EQ(rejected.ValueOrDie().outcome, CommitOutcome::kAborted);
  EXPECT_TRUE(rejected.ValueOrDie().status.IsConflict())
      << rejected.ValueOrDie().status.ToString();
}

TEST_F(KernelCommitTest, PersistsPublicCommitAndTransactionIdAcrossReopen) {
  std::unique_ptr<Transaction> transaction = Begin();
  ASSERT_TRUE(transaction->Assert(EntityFact::Vertex(VertexId{1}), ValidTime{10}).ok());
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
  EXPECT_TRUE(snapshot.ValueOrDie().Exists(EntityFact::Vertex(VertexId{1}), ValidTime{10})
                  .ValueOrDie());

  std::unique_ptr<Transaction> next = Begin();
  ASSERT_TRUE(next->Assert(EntityFact::Vertex(VertexId{2}), ValidTime{10}).ok());
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
  ASSERT_TRUE(first.ValueOrDie()->Assert(EntityFact::Vertex(VertexId{1}), ValidTime{10})
                  .ok());
  ASSERT_TRUE(first.ValueOrDie()->Assert(EntityFact::Vertex(VertexId{3}), ValidTime{10})
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
    EXPECT_TRUE(snapshot.ValueOrDie().Exists(EntityFact::Vertex(VertexId{1}), ValidTime{10})
                    .ValueOrDie());
    EXPECT_TRUE(snapshot.ValueOrDie().Exists(EntityFact::Vertex(VertexId{3}), ValidTime{10})
                    .ValueOrDie());
  }
  auto recovered = reopened.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(recovered.ok()) << recovered.status().ToString();
  ASSERT_TRUE(recovered.ValueOrDie()->Assert(EntityFact::Vertex(VertexId{2}), ValidTime{10})
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
  ASSERT_TRUE(first.ValueOrDie()->Assert(EntityFact::Vertex(VertexId{1}), ValidTime{10})
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
  ASSERT_TRUE(recovered.ValueOrDie()->Assert(EntityFact::Vertex(VertexId{2}), ValidTime{10})
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
    EXPECT_FALSE(snapshot.ValueOrDie().Exists(EntityFact::Vertex(VertexId{1}), ValidTime{10})
                     .ValueOrDie());
    EXPECT_TRUE(snapshot.ValueOrDie().Exists(EntityFact::Vertex(VertexId{2}), ValidTime{10})
                    .ValueOrDie());
  }
  reopened.ValueOrDie().reset();
  std::filesystem::remove_all(path);
}

}  // namespace
}  // namespace cedar
