// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "cedar/database.h"
#include "storage/facts/fact_store.h"
#include "kernel/temporal_validation.h"

namespace cedar {
namespace {

PendingFactMutation VertexPut(uint64_t vertex_id, uint64_t valid_from) {
  return {EntityFact::Vertex(VertexRef{PartId{0}, VertexId{vertex_id}}).ref(), ValidTime{valid_from},
          FactOperation::kPut, 0, std::nullopt};
}

PendingFactMutation EdgePut(uint64_t edge_id, uint64_t valid_from) {
  return {EntityFact::Edge(EdgeRef{PartId{0}, EdgeId{edge_id}}).ref(), ValidTime{valid_from},
          FactOperation::kPut, 0, std::nullopt};
}

PendingFactMutation VertexPropertyPut(uint64_t vertex_id, uint16_t property_id,
                                      uint64_t valid_from, Value value) {
  return {PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{vertex_id}}, PropertyId{property_id}).ref(),
          ValidTime{valid_from}, FactOperation::kPut, 1, std::move(value)};
}

class StrictTransactionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_strict_transactions_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    path_ = pattern;
    store_ = std::make_unique<FactStore>(FactStoreOptions{path_});
    ASSERT_TRUE(store_->Open().ok());
  }

  void TearDown() override {
    store_.reset();
    std::filesystem::remove_all(path_);
  }

  std::string path_;
  std::unique_ptr<FactStore> store_;
};

TEST_F(StrictTransactionTest, RejectsEmptyReadPhantomAtExactFactAndTime) {
  const auto snapshot = store_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  const FactRef observed = EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}).ref();
  const auto dependency =
      CaptureStrictReadDependency(*store_, snapshot.ValueOrDie(), observed, ValidTime{10});
  ASSERT_TRUE(dependency.ok()) << dependency.status().ToString();

  ASSERT_TRUE(store_->Commit(StoreCommitBatch{
                         TxnId{1}, 1, {VertexPut(1, 10)}, {}, {}, {}})
                  .ok());
  const auto strict = store_->Commit(StoreCommitBatch{
      TxnId{2}, 2, {VertexPut(2, 10)}, {}, {}, {dependency.ValueOrDie()}});
  EXPECT_TRUE(strict.status().IsConflict()) << strict.status().ToString();
}

TEST_F(StrictTransactionTest,
       RejectsWriteSkewWhenVisibleExactReadChangesAndWriteIsIndependent) {
  ASSERT_TRUE(store_->Commit(StoreCommitBatch{
                         TxnId{1}, 1, {VertexPut(1, 10)}, {}, {}, {}})
                  .ok());
  const auto snapshot = store_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  const auto dependency = CaptureStrictReadDependency(
      *store_, snapshot.ValueOrDie(), EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}).ref(),
      ValidTime{10});
  ASSERT_TRUE(dependency.ok()) << dependency.status().ToString();
  ASSERT_TRUE(dependency.ValueOrDie().observed_event.has_value());

  ASSERT_TRUE(store_->Commit(StoreCommitBatch{
                         TxnId{2}, 2, {VertexPut(1, 10)}, {}, {}, {}})
                  .ok());
  const auto strict = store_->Commit(StoreCommitBatch{
      TxnId{3}, 3, {VertexPut(2, 10)}, {}, {}, {dependency.ValueOrDie()}});
  EXPECT_TRUE(strict.status().IsConflict()) << strict.status().ToString();
}

TEST_F(StrictTransactionTest, RejectsPredecessorAndSuccessorFenceInsertions) {
  ASSERT_TRUE(store_->Commit(StoreCommitBatch{
                         TxnId{1}, 1, {VertexPut(1, 10), VertexPut(1, 30)}, {}, {}, {}})
                  .ok());
  const FactRef fact = EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}).ref();
  const auto before_predecessor = store_->BeginSnapshot();
  ASSERT_TRUE(before_predecessor.ok()) << before_predecessor.status().ToString();
  const auto predecessor_dependency = CaptureStrictReadDependency(
      *store_, before_predecessor.ValueOrDie(), fact, ValidTime{20});
  ASSERT_TRUE(predecessor_dependency.ok()) << predecessor_dependency.status().ToString();
  EXPECT_EQ(predecessor_dependency.ValueOrDie().predecessor,
            std::optional<ValidTime>{ValidTime{10}});
  EXPECT_EQ(predecessor_dependency.ValueOrDie().successor,
            std::optional<ValidTime>{ValidTime{30}});

  ASSERT_TRUE(store_->Commit(StoreCommitBatch{
                         TxnId{2}, 2, {VertexPut(1, 15)}, {}, {}, {}})
                  .ok());
  const auto predecessor_conflict = store_->Commit(StoreCommitBatch{
      TxnId{3}, 3, {VertexPut(2, 10)}, {}, {},
      {predecessor_dependency.ValueOrDie()}});
  EXPECT_TRUE(predecessor_conflict.status().IsConflict())
      << predecessor_conflict.status().ToString();

  const auto before_successor = store_->BeginSnapshot();
  ASSERT_TRUE(before_successor.ok()) << before_successor.status().ToString();
  const auto successor_dependency = CaptureStrictReadDependency(
      *store_, before_successor.ValueOrDie(), fact, ValidTime{20});
  ASSERT_TRUE(successor_dependency.ok()) << successor_dependency.status().ToString();
  ASSERT_TRUE(store_->Commit(StoreCommitBatch{
                         TxnId{4}, 4, {VertexPut(1, 25)}, {}, {}, {}})
                  .ok());
  const auto successor_conflict = store_->Commit(StoreCommitBatch{
      TxnId{5}, 5, {VertexPut(3, 10)}, {}, {}, {successor_dependency.ValueOrDie()}});
  EXPECT_TRUE(successor_conflict.status().IsConflict())
      << successor_conflict.status().ToString();
}

TEST_F(StrictTransactionTest, RejectsWhenAnEdgeReadDependencyIncludesEndpoints) {
  const EdgeIdentity identity{EdgeId{9}, VertexId{1}, VertexId{2}, 7};
  ASSERT_TRUE(store_->Commit(StoreCommitBatch{
                         TxnId{1}, 1,
                         {VertexPut(1, 10), VertexPut(2, 10), EdgePut(9, 10)},
                         {identity}, {}, {}})
                  .ok());
  const auto snapshot = store_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  std::vector<StrictReadDependency> dependencies;
  for (const FactRef& fact : {EntityFact::Edge(EdgeRef{PartId{0}, EdgeId{9}}).ref(),
                              EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}).ref(),
                              EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}).ref()}) {
    const auto dependency =
        CaptureStrictReadDependency(*store_, snapshot.ValueOrDie(), fact, ValidTime{10});
    ASSERT_TRUE(dependency.ok()) << dependency.status().ToString();
    dependencies.push_back(dependency.ValueOrDie());
  }

  ASSERT_TRUE(store_->Commit(StoreCommitBatch{
                         TxnId{2}, 2,
                         {{EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}).ref(), ValidTime{10},
                           FactOperation::kDelete, 0, std::nullopt}},
                         {}, {}, {}})
                  .ok());
  const auto strict = store_->Commit(StoreCommitBatch{
      TxnId{3}, 3, {VertexPut(3, 10)}, {}, {}, std::move(dependencies)});
  EXPECT_TRUE(strict.status().IsConflict()) << strict.status().ToString();
}

TEST_F(StrictTransactionTest, StrictTransactionReadsExactEntityAndProperty) {
  ASSERT_TRUE(store_->Commit(StoreCommitBatch{
                         TxnId{1}, 1,
                         {VertexPut(1, 10),
                          VertexPropertyPut(1, 7, 10, Value::Int64(42))},
                         {}, {}, {}})
                  .ok());
  ASSERT_TRUE(store_->Close().ok());
  auto database = Database::Open(DatabaseOptions{.path = path_});
  ASSERT_TRUE(database.ok()) << database.status().ToString();
  auto transaction = database.ValueOrDie()->BeginTransaction(
      TransactionOptions{.isolation = IsolationLevel::kStrict});
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();

  const auto exists = transaction.ValueOrDie()->Exists(
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10});
  ASSERT_TRUE(exists.ok()) << exists.status().ToString();
  EXPECT_TRUE(exists.ValueOrDie());
  const auto value = transaction.ValueOrDie()->Get(
      PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{7}), ValidTime{10});
  ASSERT_TRUE(value.ok()) << value.status().ToString();
  EXPECT_EQ(value.ValueOrDie(), std::optional<Value>{Value::Int64(42)});
  EXPECT_TRUE(transaction.ValueOrDie()->Rollback().ok());
  EXPECT_TRUE(database.ValueOrDie()->Close().ok());
}

TEST_F(StrictTransactionTest, StrictTransactionReadsEdgeAndBothEndpoints) {
  const EdgeIdentity identity{EdgeId{9}, VertexId{1}, VertexId{2}, 7};
  ASSERT_TRUE(store_->Commit(StoreCommitBatch{
                         TxnId{1}, 1,
                         {VertexPut(1, 10), VertexPut(2, 10), EdgePut(9, 10)},
                         {identity}, {}, {}})
                  .ok());
  ASSERT_TRUE(store_->Close().ok());
  auto database = Database::Open(DatabaseOptions{.path = path_});
  ASSERT_TRUE(database.ok()) << database.status().ToString();
  auto transaction = database.ValueOrDie()->BeginTransaction(
      TransactionOptions{.isolation = IsolationLevel::kStrict});
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();

  const auto visible = transaction.ValueOrDie()->Exists(
      EntityFact::Edge(EdgeRef{PartId{0}, EdgeId{9}}), ValidTime{10});
  ASSERT_TRUE(visible.ok()) << visible.status().ToString();
  EXPECT_TRUE(visible.ValueOrDie());
  EXPECT_TRUE(transaction.ValueOrDie()->Rollback().ok());
  EXPECT_TRUE(database.ValueOrDie()->Close().ok());
}

TEST_F(StrictTransactionTest, RejectsStrictRangeScansWithTypedStatus) {
  ASSERT_TRUE(store_->Close().ok());
  auto database = Database::Open(DatabaseOptions{.path = path_});
  ASSERT_TRUE(database.ok()) << database.status().ToString();
  auto transaction = database.ValueOrDie()->BeginTransaction(
      TransactionOptions{.isolation = IsolationLevel::kStrict});
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();

  const Status scanned = transaction.ValueOrDie()->Scan(
      FactFamily::kVertexState, PropertyId{}, [](const FactEvent&) {
        return Status::OK();
      });
  EXPECT_TRUE(scanned.IsUnsupportedSerializablePredicate()) << scanned.ToString();
  EXPECT_TRUE(transaction.ValueOrDie()->Rollback().ok());
  EXPECT_TRUE(database.ValueOrDie()->Close().ok());
}

TEST_F(StrictTransactionTest, SnapshotTransactionScansItsCapturedFactFamily) {
  ASSERT_TRUE(store_->Commit(StoreCommitBatch{
                         TxnId{1}, 1, {VertexPut(1, 10), VertexPut(2, 20)}, {}, {}, {}})
                  .ok());
  ASSERT_TRUE(store_->Close().ok());
  auto database = Database::Open(DatabaseOptions{.path = path_});
  ASSERT_TRUE(database.ok()) << database.status().ToString();
  auto transaction = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();

  std::vector<FactEvent> events;
  const Status scanned = transaction.ValueOrDie()->Scan(
      FactFamily::kVertexState, PropertyId{}, [&events](const FactEvent& event) {
        events.push_back(event);
        return Status::OK();
      });
  ASSERT_TRUE(scanned.ok()) << scanned.ToString();
  ASSERT_EQ(events.size(), 2U);
  EXPECT_NE(std::find_if(events.begin(), events.end(), [](const FactEvent& event) {
              return event.ref == EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}).ref() &&
                     event.valid_from == ValidTime{10};
            }),
            events.end());
  EXPECT_NE(std::find_if(events.begin(), events.end(), [](const FactEvent& event) {
              return event.ref == EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}).ref() &&
                     event.valid_from == ValidTime{20};
            }),
            events.end());
  EXPECT_TRUE(transaction.ValueOrDie()->Rollback().ok());
  EXPECT_TRUE(database.ValueOrDie()->Close().ok());
}

}  // namespace
}  // namespace cedar
