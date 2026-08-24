#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <vector>

#include "cedar/fact/fact_codec.h"
#include "cedar/fact/meta_codec.h"
#include "storage/rocks/commit_publisher.h"
#include "storage/facts/fact_store.h"

namespace cedar {
namespace {

PendingFactMutation VertexPut(uint64_t id, uint64_t valid_from) {
  return {EntityFact::Vertex(VertexRef{PartId{3}, VertexId{id}}).ref(),
          ValidTime{valid_from}, FactOperation::kPut, 0, std::nullopt};
}

StoreCommitBatch Batch(TxnId txn_id, std::vector<PendingFactMutation> mutations) {
  return {txn_id, 100, std::move(mutations), {}};
}

StoreCommitBatch EdgeBatch(TxnId txn_id, const EdgeIdentity& identity,
                           FactOperation operation) {
  return {txn_id,
          100,
          {{EntityFact::Edge(identity.edge_ref()).ref(), ValidTime{10},
            operation, 0, std::nullopt}},
          {identity}};
}

class FactEventCanonicalTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_fact_event_canonical_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    path_ = pattern;
    store_ = std::make_unique<FactStore>(FactStoreOptions{path_});
    ASSERT_TRUE(store_->Open().ok());
  }

  void TearDown() override {
    store_.reset();
    std::filesystem::remove_all(path_);
  }

  void Reopen() {
    ASSERT_TRUE(store_->Close().ok());
    ASSERT_TRUE(store_->Open().ok());
  }

  std::string path_;
  std::unique_ptr<FactStore> store_;
};

TEST_F(FactEventCanonicalTest, SequenceRangeReadsOneCanonicalPayloadPerFactKey) {
  ASSERT_TRUE(store_->Commit(Batch(TxnId{1}, {VertexPut(11, 10), VertexPut(7, 20)}))
                  .ok());
  const auto snapshot = store_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();

  const auto events = store_->ReadCanonicalEvents(snapshot.ValueOrDie(),
                                                   CommitSeq{1}, CommitSeq{1});
  ASSERT_TRUE(events.ok()) << events.status().ToString();
  ASSERT_EQ(events.ValueOrDie().size(), 2U);
  ASSERT_TRUE(events.ValueOrDie()[0].Validate().ok());
  ASSERT_TRUE(events.ValueOrDie()[1].Validate().ok());
  EXPECT_EQ(events.ValueOrDie()[0].ref.entity_id(), 7U);
  EXPECT_EQ(events.ValueOrDie()[1].ref.entity_id(), 11U);
  EXPECT_EQ(events.ValueOrDie()[0].commit_seq, CommitSeq{1});
  EXPECT_EQ(events.ValueOrDie()[1].commit_seq, CommitSeq{1});
  EXPECT_EQ(events.ValueOrDie()[0].schema_epoch, 0U);
  EXPECT_EQ(events.ValueOrDie()[1].schema_epoch, 0U);
}

TEST_F(FactEventCanonicalTest, CanonicalEventsSurviveReopenWithoutAnEventLogCopy) {
  ASSERT_TRUE(store_->Commit(Batch(TxnId{2}, {VertexPut(9, 30)})).ok());
  Reopen();

  const auto snapshot = store_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  const auto events = store_->ReadCanonicalEvents(snapshot.ValueOrDie(),
                                                   CommitSeq{1}, CommitSeq{1});
  ASSERT_TRUE(events.ok()) << events.status().ToString();
  ASSERT_EQ(events.ValueOrDie().size(), 1U);
  const FactEvent& event = events.ValueOrDie().front();
  EXPECT_EQ(event.ref,
            EntityFact::Vertex(VertexRef{PartId{3}, VertexId{9}}).ref());
  EXPECT_EQ(event.valid_from, ValidTime{30});
  EXPECT_EQ(event.operation, FactOperation::kPut);
  EXPECT_FALSE(event.edge_identity.has_value());
}

TEST_F(FactEventCanonicalTest, PreservesDeleteAndEdgeIdentityInTheSameEventStream) {
  const EdgeIdentity identity{
      EdgeRef{PartId{4}, EdgeId{19}},
      VertexRef{PartId{4}, VertexId{21}},
      VertexRef{PartId{8}, VertexId{22}},
      17};
  ASSERT_TRUE(store_->Commit(EdgeBatch(TxnId{3}, identity,
                                       FactOperation::kPut))
                  .ok());
  ASSERT_TRUE(store_->Commit(Batch(
                                TxnId{4},
                                {{EntityFact::Vertex(VertexRef{PartId{3},
                                                               VertexId{9}})
                                      .ref(),
                                  ValidTime{30}, FactOperation::kDelete, 0,
                                  std::nullopt}}))
                  .ok());

  const auto snapshot = store_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  const auto events = store_->ReadCanonicalEvents(snapshot.ValueOrDie(),
                                                   CommitSeq{1}, CommitSeq{2});
  ASSERT_TRUE(events.ok()) << events.status().ToString();
  ASSERT_EQ(events.ValueOrDie().size(), 3U);
  for (size_t i = 1; i < events.ValueOrDie().size(); ++i) {
    EXPECT_LE(events.ValueOrDie()[i - 1].commit_seq.value,
              events.ValueOrDie()[i].commit_seq.value);
  }
  const auto edge = std::find_if(
      events.ValueOrDie().begin(), events.ValueOrDie().end(),
      [](const FactEvent& event) {
        return event.ref.family() == FactFamily::kEdgeIdentity;
      });
  ASSERT_NE(edge, events.ValueOrDie().end());
  ASSERT_TRUE(edge->edge_identity.has_value());
  EXPECT_EQ(*edge->edge_identity, identity);
  const auto deleted = std::find_if(
      events.ValueOrDie().begin(), events.ValueOrDie().end(),
      [](const FactEvent& event) {
        return event.operation == FactOperation::kDelete;
      });
  ASSERT_NE(deleted, events.ValueOrDie().end());
  EXPECT_FALSE(deleted->value.has_value());
}

TEST(FactEventCanonicalContractTest, SequenceMetadataRejectsDuplicateFactKeys) {
  const FactRef ref =
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{41}}).ref();
  const std::string key = EncodeFactKey(ref, ValidTime{3}, CommitSeq{2});
  const SequenceRecord record{CommitSeq{2}, TxnId{7}, 11, {key, key}};

  const auto encoded = EncodeSequenceRecord(record);
  EXPECT_FALSE(encoded.ok());
}

}  // namespace
}  // namespace cedar
