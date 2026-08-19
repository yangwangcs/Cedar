// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <map>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "cedar/fact/fact_store.h"

namespace cedar {
namespace {

PendingFactMutation Mutation(uint64_t valid_time, FactOperation operation = FactOperation::kPut) {
  return {EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}).ref(), ValidTime{valid_time}, operation,
          0, std::nullopt};
}

class TemporalNeighborhoodTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_temporal_neighborhood_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    path_ = pattern;
    store_ = std::make_unique<FactStore>(FactStoreOptions{path_});
    ASSERT_TRUE(store_->Open().ok());
  }

  void TearDown() override {
    store_.reset();
    std::filesystem::remove_all(path_);
  }

  void Commit(TxnId txn_id, std::vector<PendingFactMutation> mutations) {
    ASSERT_TRUE(store_->Commit(StoreCommitBatch{
                          txn_id, txn_id.value, std::move(mutations), {}, {}, {}})
                    .ok());
  }

  TemporalNeighborhood Oracle(const StoreSnapshot& snapshot, ValidTime query) {
    std::map<uint64_t, FactEvent> boundaries;
    const Status scanned = store_->Scan(
        snapshot, FactPrefix::Exact(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}).ref()),
        [&boundaries](const FactEvent& event) {
          const auto found = boundaries.find(event.valid_from.value);
          if (found == boundaries.end() ||
              found->second.commit_seq.value < event.commit_seq.value) {
            boundaries.insert_or_assign(event.valid_from.value, event);
          }
          return Status::OK();
        });
    EXPECT_TRUE(scanned.ok()) << scanned.ToString();
    TemporalNeighborhood expected;
    const auto successor = boundaries.upper_bound(query.value);
    if (successor != boundaries.end()) expected.successor = ValidTime{successor->first};
    if (successor != boundaries.begin()) {
      const auto observed = std::prev(successor);
      expected.observed = observed->second;
      if (observed->first < query.value) {
        expected.predecessor = ValidTime{observed->first};
      } else if (observed != boundaries.begin()) {
        expected.predecessor = ValidTime{std::prev(observed)->first};
      }
    }
    return expected;
  }

  void ExpectMatchesOracle(const StoreSnapshot& snapshot, ValidTime query) {
    const auto actual = store_->ReadTemporalNeighborhood(
        snapshot, EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}).ref(), query);
    ASSERT_TRUE(actual.ok()) << actual.status().ToString();
    const TemporalNeighborhood expected = Oracle(snapshot, query);
    ASSERT_EQ(actual.ValueOrDie().observed.has_value(), expected.observed.has_value());
    if (expected.observed.has_value()) {
      const FactEvent& actual_event = *actual.ValueOrDie().observed;
      const FactEvent& expected_event = *expected.observed;
      EXPECT_EQ(actual_event.ref, expected_event.ref);
      EXPECT_EQ(actual_event.valid_from, expected_event.valid_from);
      EXPECT_EQ(actual_event.commit_seq, expected_event.commit_seq);
      EXPECT_EQ(actual_event.operation, expected_event.operation);
      EXPECT_EQ(actual_event.schema_epoch, expected_event.schema_epoch);
      EXPECT_EQ(actual_event.value, expected_event.value);
    }
    EXPECT_EQ(actual.ValueOrDie().predecessor, expected.predecessor);
    EXPECT_EQ(actual.ValueOrDie().successor, expected.successor);
  }

  std::string path_;
  std::unique_ptr<FactStore> store_;
};

TEST_F(TemporalNeighborhoodTest, MatchesScanForEmptyAndSingleVersionHistory) {
  const auto empty = store_->BeginSnapshot();
  ASSERT_TRUE(empty.ok());
  ExpectMatchesOracle(empty.ValueOrDie(), ValidTime{10});
  Commit(TxnId{1}, {Mutation(10)});
  const auto snapshot = store_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  for (uint64_t query : {0U, 9U, 10U, 11U}) {
    ExpectMatchesOracle(snapshot.ValueOrDie(), ValidTime{query});
  }
}

TEST_F(TemporalNeighborhoodTest, MatchesScanForOutOfOrderValidTimes) {
  Commit(TxnId{1}, {Mutation(30)});
  Commit(TxnId{2}, {Mutation(10)});
  Commit(TxnId{3}, {Mutation(20)});
  const auto snapshot = store_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  for (uint64_t query : {0U, 10U, 15U, 20U, 25U, 30U, 35U}) {
    ExpectMatchesOracle(snapshot.ValueOrDie(), ValidTime{query});
  }
}

TEST_F(TemporalNeighborhoodTest, MatchesScanForSameTimeCorrections) {
  Commit(TxnId{1}, {Mutation(10)});
  const auto before_correction = store_->BeginSnapshot();
  ASSERT_TRUE(before_correction.ok());
  Commit(TxnId{2}, {Mutation(10, FactOperation::kDelete)});
  Commit(TxnId{3}, {Mutation(20)});
  const auto current = store_->BeginSnapshot();
  ASSERT_TRUE(current.ok());
  for (uint64_t query : {0U, 10U, 15U, 20U, 25U}) {
    ExpectMatchesOracle(before_correction.ValueOrDie(), ValidTime{query});
    ExpectMatchesOracle(current.ValueOrDie(), ValidTime{query});
  }
}

TEST_F(TemporalNeighborhoodTest, MatchesScanForDeletes) {
  Commit(TxnId{1}, {Mutation(10)});
  Commit(TxnId{2}, {Mutation(20, FactOperation::kDelete)});
  Commit(TxnId{3}, {Mutation(30)});
  const auto snapshot = store_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  for (uint64_t query : {0U, 10U, 15U, 20U, 25U, 30U, 35U}) {
    ExpectMatchesOracle(snapshot.ValueOrDie(), ValidTime{query});
  }
}

TEST_F(TemporalNeighborhoodTest, MatchesScanAtHistoricalCommitSequences) {
  Commit(TxnId{1}, {Mutation(10)});
  Commit(TxnId{2}, {Mutation(20)});
  Commit(TxnId{3}, {Mutation(10, FactOperation::kDelete)});
  for (uint64_t sequence : {1U, 2U, 3U}) {
    const auto snapshot = store_->BeginSnapshot(SnapshotOptions{CommitSeq{sequence}});
    ASSERT_TRUE(snapshot.ok());
    for (uint64_t query : {0U, 10U, 15U, 20U, 25U}) {
      ExpectMatchesOracle(snapshot.ValueOrDie(), ValidTime{query});
    }
  }
}

TEST_F(TemporalNeighborhoodTest, MatchesScanBeforeAndAfterReopen) {
  Commit(TxnId{1}, {Mutation(10), Mutation(30)});
  Commit(TxnId{2}, {Mutation(20)});
  auto snapshot = store_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  for (uint64_t query : {0U, 10U, 15U, 20U, 25U, 30U, 35U}) {
    ExpectMatchesOracle(snapshot.ValueOrDie(), ValidTime{query});
  }
  snapshot = Status::InvalidArgument("test", "release snapshot before reopen");
  ASSERT_TRUE(store_->Close().ok());
  ASSERT_TRUE(store_->Open().ok());
  snapshot = store_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  for (uint64_t query : {0U, 10U, 15U, 20U, 25U, 30U, 35U}) {
    ExpectMatchesOracle(snapshot.ValueOrDie(), ValidTime{query});
  }
}

}  // namespace
}  // namespace cedar
