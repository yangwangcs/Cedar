#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <thread>

#include "cedar/fact/fact_codec.h"
#include "query/projection/query_delta.h"
#include "storage/facts/fact_store.h"

namespace cedar::internal {
namespace {

FactEvent VertexEvent(uint64_t commit, uint64_t valid, uint64_t vertex = 7,
                      FactOperation operation = FactOperation::kPut) {
  return {EntityFact::Vertex(VertexRef{PartId{0}, VertexId{vertex}}).ref(),
          ValidTime{valid}, CommitSeq{commit}, operation, 0, std::nullopt,
          std::nullopt};
}

TEST(QueryDeltaTest, DoesNotAdvanceAcrossAMissingCommit) {
  QueryDelta delta({.base_seq = CommitSeq{10}, .queue_capacity = 1});
  EXPECT_TRUE(delta.ObservePublished(QueryDeltaCommit{CommitSeq{11}}).ok());
  EXPECT_TRUE(delta.ObservePublished(QueryDeltaCommit{CommitSeq{12}})
                  .IsResourceExhausted());
  EXPECT_EQ(delta.indexed_through(), CommitSeq{11});
  EXPECT_EQ(delta.first_missing(), CommitSeq{12});
  EXPECT_FALSE(delta.mergeable());
}

TEST(QueryDeltaTest, SnapshotCutExcludesFutureCorrections) {
  QueryDelta delta({.base_seq = CommitSeq{1}, .queue_capacity = 8});
  QueryDeltaCommit first{CommitSeq{2}};
  first.facts.push_back(VertexEvent(2, 10));
  ASSERT_TRUE(delta.ObservePublished(first).ok());
  QueryDeltaCommit second{CommitSeq{3}};
  second.facts.push_back(VertexEvent(3, 10));
  ASSERT_TRUE(delta.ObservePublished(second).ok());

  const auto at_two = delta.AcquireThrough(CommitSeq{2});
  ASSERT_TRUE(at_two.ok());
  ASSERT_EQ(at_two.ValueOrDie().facts.size(), 1U);
  EXPECT_EQ(at_two.ValueOrDie().facts.front().commit_seq, CommitSeq{2});
  const auto events = delta.EventsFor(at_two.ValueOrDie().facts.front().ref,
                                      CommitSeq{2});
  ASSERT_EQ(events.size(), 1U);
  EXPECT_EQ(events.front().commit_seq, CommitSeq{2});
}

TEST(QueryDeltaTest, MergesCorrectionAtAnOldValidTime) {
  const FactRef ref = EntityFact::Vertex(VertexRef{PartId{0}, VertexId{7}}).ref();
  const std::vector<CorrectedBoundary> base{{ValidTime{10}, CommitSeq{5},
                                              FactOperation::kPut, 0,
                                              std::nullopt, std::nullopt}};
  const std::vector<FactEvent> delta{
      {ref, ValidTime{10}, CommitSeq{6}, FactOperation::kDelete, 0,
       std::nullopt, std::nullopt}};
  const auto merged = QueryDelta::MergeBoundaries(base, delta, CommitSeq{6});
  ASSERT_TRUE(merged.ok());
  ASSERT_EQ(merged.ValueOrDie().size(), 1U);
  EXPECT_EQ(merged.ValueOrDie().front().commit_seq, CommitSeq{6});
  EXPECT_EQ(merged.ValueOrDie().front().operation, FactOperation::kDelete);
}

TEST(QueryDeltaTest, NewEdgeIdentityIsRetainedForBothDirections) {
  const EdgeIdentity identity{EdgeRef{PartId{2}, EdgeId{9}},
                              VertexRef{PartId{2}, VertexId{11}},
                              VertexRef{PartId{3}, VertexId{12}}, 7};
  QueryDelta delta({.base_seq = CommitSeq{0}, .queue_capacity = 8});
  QueryDeltaCommit commit{CommitSeq{1}};
  commit.edge_identities.push_back(identity);
  ASSERT_TRUE(delta.ObservePublished(commit).ok());
  const auto view = delta.AcquireThrough(CommitSeq{1});
  ASSERT_TRUE(view.ok());
  ASSERT_EQ(view.ValueOrDie().edge_identities.size(), 1U);
  const EdgeIdentity& got = view.ValueOrDie().edge_identities.front();
  EXPECT_EQ(got.source_ref(), identity.source_ref());
  EXPECT_EQ(got.target_ref(), identity.target_ref());
  EXPECT_EQ(got.edge_ref(), identity.edge_ref());
}

TEST(QueryDeltaTest, HardMemoryRetirementStopsMergeability) {
  QueryDelta delta({.base_seq = CommitSeq{0}, .queue_capacity = 8,
                    .soft_memory_bytes = 1, .hard_memory_bytes = 1,
                    .max_lag_commits = 8});
  EXPECT_TRUE(delta.ObservePublished(QueryDeltaCommit{CommitSeq{1}})
                  .IsResourceExhausted());
  EXPECT_TRUE(delta.hard_limit_reached());
  EXPECT_FALSE(delta.mergeable());
}

TEST(FactStoreBatchReadTest, ReadsContiguousSequencesAndExactFactsInOrder) {
  char pattern[] = "/tmp/cedar_query_delta_store_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  FactStore store(FactStoreOptions{path});
  ASSERT_TRUE(store.Open().ok());
  const auto vertex = EntityFact::Vertex(VertexRef{PartId{0}, VertexId{7}}).ref();
  ASSERT_TRUE(store.Commit(StoreCommitBatch{
      TxnId{1}, 100,
      {{vertex, ValidTime{10}, FactOperation::kPut, 0, std::nullopt}}, {}}).ok());
  const EdgeIdentity identity{EdgeRef{PartId{0}, EdgeId{9}},
                              VertexRef{PartId{0}, VertexId{7}},
                              VertexRef{PartId{0}, VertexId{8}}, 3};
  const auto edge = EntityFact::Edge(identity.edge_ref()).ref();
  ASSERT_TRUE(store.Commit(StoreCommitBatch{
      TxnId{2}, 100,
      {{edge, ValidTime{10}, FactOperation::kPut, 0, std::nullopt}},
      {identity}}).ok());
  {
    auto snapshot = store.BeginSnapshot();
    ASSERT_TRUE(snapshot.ok());
    const auto sequences = store.ReadSequenceRange(snapshot.ValueOrDie(),
                                                   CommitSeq{1}, CommitSeq{2});
    ASSERT_TRUE(sequences.ok()) << sequences.status().ToString();
    ASSERT_EQ(sequences.ValueOrDie().size(), 2U);
    const auto facts = store.ReadExactFacts(snapshot.ValueOrDie(),
                                            sequences.ValueOrDie()[1].fact_keys);
    ASSERT_TRUE(facts.ok()) << facts.status().ToString();
    ASSERT_EQ(facts.ValueOrDie().size(),
              sequences.ValueOrDie()[1].fact_keys.size());
    EXPECT_EQ(facts.ValueOrDie()[0].ref, edge);
  }
  ASSERT_TRUE(store.Close().ok());
  std::filesystem::remove_all(path);
}

TEST(QueryDeltaTest, RepairReleasesQueueForLaterPublicationAndRetirement) {
  char pattern[] = "/tmp/cedar_query_delta_repair_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  FactStore store(FactStoreOptions{pattern});
  ASSERT_TRUE(store.Open().ok());
  const FactRef ref = EntityFact::Vertex(VertexRef{PartId{0}, VertexId{7}}).ref();
  for (uint64_t txn = 1; txn <= 2; ++txn) {
    ASSERT_TRUE(store.Commit(StoreCommitBatch{
        TxnId{txn}, 100,
        {{ref, ValidTime{txn}, FactOperation::kPut, 0, std::nullopt}}, {}}).ok());
  }
  QueryDelta delta({.base_seq = CommitSeq{0}, .queue_capacity = 1});
  ASSERT_TRUE(delta.ObservePublished(QueryDeltaCommit{CommitSeq{1}}).ok());
  EXPECT_TRUE(delta.ObservePublished(QueryDeltaCommit{CommitSeq{2}})
                  .IsResourceExhausted());
  {
    auto snapshot = store.BeginSnapshot();
    ASSERT_TRUE(snapshot.ok());
    ASSERT_TRUE(delta.RepairThrough(store, snapshot.ValueOrDie(), CommitSeq{2},
                                    QueryDeltaRepairLimits{8, 1ULL << 20})
                    .ok());
  }
  EXPECT_EQ(delta.pending_commits(), 0U);
  ASSERT_TRUE(delta.ObservePublished(QueryDeltaCommit{CommitSeq{3}}).ok());
  ASSERT_TRUE(delta.RetireThrough(CommitSeq{2}).ok());
  ASSERT_TRUE(delta.ConsumeThrough(CommitSeq{3}).ok());
  EXPECT_TRUE(delta.ObservePublished(QueryDeltaCommit{CommitSeq{4}}).ok());
  ASSERT_TRUE(store.Close().ok());
  std::filesystem::remove_all(pattern);
}

TEST(QueryDeltaTest, MutationValueIsCountedAgainstHardMemory) {
  const FactRef ref = PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{7}},
                                           PropertyId{1}).ref();
  QueryDelta delta({.base_seq = CommitSeq{0}, .queue_capacity = 8,
                    .soft_memory_bytes = 512, .hard_memory_bytes = 1024});
  QueryDeltaCommit commit(
      CommitSeq{1},
      {{ref, ValidTime{1}, FactOperation::kPut, 1,
        Value::String(std::string(4096, 'x'))}});
  EXPECT_TRUE(delta.ObservePublished(commit).IsResourceExhausted());
  EXPECT_TRUE(delta.hard_limit_reached());
}

TEST(QueryDeltaTest, EnqueueUsesWorkerBeforeResetOrRepair) {
  QueryDelta delta({.base_seq = CommitSeq{0}, .queue_capacity = 8});
  ASSERT_TRUE(delta.EnqueuePublished(QueryDeltaCommit{CommitSeq{1}}).ok());
  for (int attempt = 0; attempt < 1000 &&
       delta.indexed_through() != CommitSeq{1}; ++attempt) {
    std::this_thread::yield();
  }
  EXPECT_EQ(delta.indexed_through(), CommitSeq{1});
  ASSERT_TRUE(delta.ResetBase(CommitSeq{1}).ok());
  EXPECT_EQ(delta.indexed_through(), CommitSeq{1});
  EXPECT_TRUE(delta.EnqueuePublished(QueryDeltaCommit{CommitSeq{2}}).ok());
}

}  // namespace
}  // namespace cedar::internal
