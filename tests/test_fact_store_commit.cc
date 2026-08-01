#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "cedar/fact/fact_store.h"
#include "cedar/fact/fact_codec.h"
#include "cedar/fact/meta_codec.h"

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice_transform.h>

namespace cedar {
namespace {

PendingFactMutation VertexPut(uint64_t vertex_id, uint64_t valid_from) {
  return {EntityFact::Vertex(VertexId{vertex_id}).ref(), ValidTime{valid_from},
          FactOperation::kPut, 0, std::nullopt};
}

StoreCommitBatch Batch(TxnId txn_id,
                       std::vector<PendingFactMutation> mutations) {
  return {txn_id, 100, std::move(mutations), {}};
}

StoreCommitBatch EdgeBatch(TxnId txn_id, EdgeIdentity identity) {
  return {txn_id,
          100,
          {{EntityFact::Edge(identity.edge_id).ref(), ValidTime{10},
            FactOperation::kPut, 0, std::nullopt}},
          {identity}};
}

void EraseMetaRecord(const std::string& path, const std::string& key) {
  rocksdb::Options options;
  options.create_if_missing = false;
  options.atomic_flush = true;
  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
  descriptors.emplace_back(rocksdb::kDefaultColumnFamilyName,
                           rocksdb::ColumnFamilyOptions(options));
  rocksdb::ColumnFamilyOptions facts_options(options);
  facts_options.prefix_extractor = std::shared_ptr<const rocksdb::SliceTransform>(
      rocksdb::NewFixedPrefixTransform(12));
  descriptors.emplace_back("facts", std::move(facts_options));
  descriptors.emplace_back("meta", rocksdb::ColumnFamilyOptions(options));
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  std::unique_ptr<rocksdb::DB> database;
  ASSERT_TRUE(rocksdb::DB::Open(options, path, descriptors, &handles, &database).ok());
  ASSERT_TRUE(database->Delete(rocksdb::WriteOptions(), handles[2], key).ok());
  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    database->DestroyColumnFamilyHandle(handle);
  }
}

class FactStoreCommitTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_fact_store_commit_XXXXXX";
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
    const Status opened = store_->Open();
    ASSERT_TRUE(opened.ok()) << opened.ToString();
  }

  std::string path_;
  std::unique_ptr<FactStore> store_;
};

TEST_F(FactStoreCommitTest,
       CommitsFactsOutcomeSequenceAndWatermarkAtomicallyAcrossReopen) {
  const StoreCommitBatch batch =
      Batch(TxnId{9}, {VertexPut(7, 10), VertexPut(11, 20)});

  const auto result = store_->Commit(batch);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ(result.ValueOrDie().commit_seq, CommitSeq{1});
  EXPECT_EQ(store_->visible_seq(), CommitSeq{1});

  Reopen();
  EXPECT_EQ(store_->visible_seq(), CommitSeq{1});
  const auto outcome = store_->ResolveTransaction(TxnId{9});
  ASSERT_TRUE(outcome.ok()) << outcome.status().ToString();
  ASSERT_TRUE(outcome.ValueOrDie().has_value());
  EXPECT_EQ(outcome.ValueOrDie()->commit_seq, CommitSeq{1});

  rocksdb::Options raw_options;
  std::vector<std::string> column_families;
  ASSERT_TRUE(rocksdb::DB::ListColumnFamilies(raw_options, path_, &column_families).ok());
  ASSERT_EQ(column_families.size(), 3U);

  ASSERT_TRUE(store_->Close().ok());
  raw_options.create_if_missing = false;
  raw_options.atomic_flush = true;
  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
  descriptors.emplace_back(rocksdb::kDefaultColumnFamilyName,
                           rocksdb::ColumnFamilyOptions(raw_options));
  rocksdb::ColumnFamilyOptions facts_options(raw_options);
  facts_options.prefix_extractor = std::shared_ptr<const rocksdb::SliceTransform>(
      rocksdb::NewFixedPrefixTransform(12));
  descriptors.emplace_back("facts", std::move(facts_options));
  descriptors.emplace_back("meta", rocksdb::ColumnFamilyOptions(raw_options));
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  std::unique_ptr<rocksdb::DB> database;
  ASSERT_TRUE(rocksdb::DB::Open(raw_options, path_, descriptors, &handles, &database).ok());
  {
    std::unique_ptr<rocksdb::Iterator> default_iterator(
        database->NewIterator(rocksdb::ReadOptions(), handles[0]));
    default_iterator->SeekToFirst();
    EXPECT_FALSE(default_iterator->Valid());
  }
  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    database->DestroyColumnFamilyHandle(handle);
  }
  database.reset();

  ASSERT_TRUE(store_->Open().ok());

  auto snapshot = store_->BeginSnapshot({});
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  for (uint64_t vertex_id : {7, 11}) {
    const auto fact = store_->Read(snapshot.ValueOrDie(),
                                   EntityFact::Vertex(VertexId{vertex_id}).ref(),
                                   ValidTime{100});
    ASSERT_TRUE(fact.ok()) << fact.status().ToString();
    ASSERT_TRUE(fact.ValueOrDie().has_value());
    EXPECT_EQ(fact.ValueOrDie()->commit_seq, CommitSeq{1});
  }
}

TEST_F(FactStoreCommitTest, DuplicateTransactionIsIdempotentOnlyForSameBatch) {
  const StoreCommitBatch original = Batch(TxnId{9}, {VertexPut(7, 10)});
  ASSERT_TRUE(store_->Commit(original).ok());

  const auto repeated = store_->Commit(original);
  ASSERT_TRUE(repeated.ok()) << repeated.status().ToString();
  EXPECT_EQ(repeated.ValueOrDie().commit_seq, CommitSeq{1});
  EXPECT_EQ(store_->visible_seq(), CommitSeq{1});

  const auto conflicting = store_->Commit(Batch(TxnId{9}, {VertexPut(8, 10)}));
  EXPECT_TRUE(conflicting.status().IsConflict())
      << conflicting.status().ToString();
  EXPECT_EQ(store_->visible_seq(), CommitSeq{1});
}

TEST_F(FactStoreCommitTest, AllocatesContiguousSequencesAndBoundsNonemptyScans) {
  ASSERT_TRUE(store_->Commit(Batch(TxnId{1}, {VertexPut(7, 10)})).ok());
  const auto second = store_->Commit(Batch(TxnId{2}, {VertexPut(7, 20),
                                                       VertexPut(8, 10)}));
  ASSERT_TRUE(second.ok()) << second.status().ToString();
  EXPECT_EQ(second.ValueOrDie().commit_seq, CommitSeq{2});

  auto snapshot = store_->BeginSnapshot({});
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  std::vector<FactEvent> scanned;
  ASSERT_TRUE(store_->Scan(snapshot.ValueOrDie(),
                           FactPrefix::Exact(EntityFact::Vertex(VertexId{7}).ref()),
                           [&](const FactEvent& event) {
                             scanned.push_back(event);
                             return Status::OK();
                           })
                  .ok());
  ASSERT_EQ(scanned.size(), 2U);
  EXPECT_EQ(scanned[0].ref.entity_id(), 7U);
  EXPECT_EQ(scanned[1].ref.entity_id(), 7U);
  EXPECT_EQ(scanned[0].valid_from, ValidTime{20});
  EXPECT_EQ(scanned[1].valid_from, ValidTime{10});

  const auto resolved = store_->Read(snapshot.ValueOrDie(),
                                     EntityFact::Vertex(VertexId{7}).ref(),
                                     ValidTime{15});
  ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
  ASSERT_TRUE(resolved.ValueOrDie().has_value());
  EXPECT_EQ(resolved.ValueOrDie()->valid_from, ValidTime{10});
  EXPECT_EQ(resolved.ValueOrDie()->commit_seq, CommitSeq{1});
}

TEST_F(FactStoreCommitTest, RejectsInvalidAndDuplicateFactMutationsBeforeWriting) {
  const StoreCommitBatch empty = Batch(TxnId{1}, {});
  EXPECT_TRUE(store_->Commit(empty).status().IsInvalidArgument());

  StoreCommitBatch duplicate = Batch(TxnId{2}, {VertexPut(7, 10), VertexPut(7, 10)});
  EXPECT_TRUE(store_->Commit(duplicate).status().IsInvalidArgument());

  StoreCommitBatch invalid = Batch(TxnId{}, {VertexPut(7, 10)});
  EXPECT_TRUE(store_->Commit(invalid).status().IsInvalidArgument());
  EXPECT_EQ(store_->visible_seq(), CommitSeq{0});
  EXPECT_FALSE(store_->ResolveTransaction(TxnId{1}).ValueOrDie().has_value());
}

TEST_F(FactStoreCommitTest, RejectsConflictingImmutableEdgeIdentity) {
  const EdgeIdentity original{EdgeId{17}, VertexId{7}, VertexId{11}, 3};
  ASSERT_TRUE(store_->Commit(EdgeBatch(TxnId{1}, original)).ok());

  const EdgeIdentity conflicting{EdgeId{17}, VertexId{7}, VertexId{13}, 3};
  const auto result = store_->Commit(EdgeBatch(TxnId{2}, conflicting));
  EXPECT_TRUE(result.status().IsIdentityConflict()) << result.status().ToString();
  EXPECT_EQ(store_->visible_seq(), CommitSeq{1});
}

TEST_F(FactStoreCommitTest, RejectsDuplicateTransactionWithDifferentEdgeIdentity) {
  const EdgeIdentity original{EdgeId{17}, VertexId{7}, VertexId{11}, 3};
  ASSERT_TRUE(store_->Commit(EdgeBatch(TxnId{1}, original)).ok());

  const EdgeIdentity conflicting{EdgeId{17}, VertexId{7}, VertexId{13}, 3};
  const auto result = store_->Commit(EdgeBatch(TxnId{1}, conflicting));
  EXPECT_TRUE(result.status().IsConflict()) << result.status().ToString();
  EXPECT_EQ(store_->visible_seq(), CommitSeq{1});
}

TEST_F(FactStoreCommitTest, RequiresEdgeIdentityWithFirstEdgeStateAssertion) {
  const StoreCommitBatch missing_identity =
      Batch(TxnId{1}, {{EntityFact::Edge(EdgeId{17}).ref(), ValidTime{10},
                        FactOperation::kPut, 0, std::nullopt}});
  EXPECT_TRUE(store_->Commit(missing_identity).status().IsInvalidArgument());

  const StoreCommitBatch orphan_identity{
      TxnId{2}, 100, {VertexPut(7, 10)},
      {EdgeIdentity{EdgeId{17}, VertexId{7}, VertexId{11}, 3}}};
  EXPECT_TRUE(store_->Commit(orphan_identity).status().IsInvalidArgument());
  EXPECT_EQ(store_->visible_seq(), CommitSeq{0});
}

TEST_F(FactStoreCommitTest, AllowsExistingEdgeUpdateBesideNewEdgeIdentity) {
  const EdgeIdentity existing{EdgeId{17}, VertexId{7}, VertexId{11}, 3};
  ASSERT_TRUE(store_->Commit(EdgeBatch(TxnId{1}, existing)).ok());

  const EdgeIdentity added{EdgeId{19}, VertexId{7}, VertexId{13}, 3};
  const StoreCommitBatch batch{
      TxnId{2},
      100,
      {{EntityFact::Edge(existing.edge_id).ref(), ValidTime{20},
        FactOperation::kPut, 0, std::nullopt},
       {EntityFact::Edge(added.edge_id).ref(), ValidTime{10}, FactOperation::kPut,
        0, std::nullopt}},
      {added}};
  const auto committed = store_->Commit(batch);
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  EXPECT_EQ(committed.ValueOrDie().commit_seq, CommitSeq{2});
}

TEST_F(FactStoreCommitTest, RejectsReopenWithMissingVisibleWatermark) {
  ASSERT_TRUE(store_->Commit(Batch(TxnId{1}, {VertexPut(7, 10)})).ok());
  ASSERT_TRUE(store_->Close().ok());
  EraseMetaRecord(path_, "watermark/visible");

  const Status opened = store_->Open();
  EXPECT_TRUE(opened.IsCorruption()) << opened.ToString();
}

TEST_F(FactStoreCommitTest, RejectsReopenWithMissingSequenceRecord) {
  ASSERT_TRUE(store_->Commit(Batch(TxnId{1}, {VertexPut(7, 10)})).ok());
  ASSERT_TRUE(store_->Close().ok());
  EraseMetaRecord(path_, EncodeSequenceMetaKey(CommitSeq{1}).ValueOrDie());

  const Status opened = store_->Open();
  EXPECT_TRUE(opened.IsCorruption()) << opened.ToString();
}

}  // namespace
}  // namespace cedar
