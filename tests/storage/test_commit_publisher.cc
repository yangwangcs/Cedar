// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>

#include "cedar/fact/fact_codec.h"
#include "cedar/fact/meta_codec.h"
#include "storage/rocks/commit_publisher.h"
#include "storage/rocks/decided_epoch.h"

namespace cedar::internal {
namespace {

class CommitPublisherTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_commit_publisher_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    path_ = pattern;
    rocksdb::Options options;
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
    descriptors.emplace_back(rocksdb::kDefaultColumnFamilyName, options);
    descriptors.emplace_back("facts", options);
    descriptors.emplace_back("meta", options);
    ASSERT_TRUE(rocksdb::DB::Open(options, path_, descriptors, &handles_, &db_).ok());
  }

  void TearDown() override {
    for (rocksdb::ColumnFamilyHandle* handle : handles_) {
      db_->DestroyColumnFamilyHandle(handle);
    }
    db_.reset();
    std::filesystem::remove_all(path_);
  }

  std::string path_;
  std::unique_ptr<rocksdb::DB> db_;
  std::vector<rocksdb::ColumnFamilyHandle*> handles_;
};

TEST_F(CommitPublisherTest, AppendsAllFactsAndMetadataForOneCandidate) {
  const StoreCommitBatch batch{
      TxnId{7}, 17,
      {{EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}).ref(), ValidTime{10}, FactOperation::kPut,
        0, std::nullopt},
       {EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}).ref(), ValidTime{20}, FactOperation::kDelete,
        0, std::nullopt}},
      {}, {}, {}};
  const CommitSeq sequence{4};
  const std::vector<std::string> fact_keys{
      EncodeFactKey(batch.mutations[0].ref, batch.mutations[0].valid_from, sequence),
      EncodeFactKey(batch.mutations[1].ref, batch.mutations[1].valid_from, sequence)};
  rocksdb::WriteBatch write_batch;

  ASSERT_TRUE(AppendCandidateToWriteBatch(
                  CandidateCommit{&batch, sequence, fact_keys}, handles_[1],
                  handles_[2], &write_batch)
                  .ok());
  ASSERT_TRUE(db_->Write(rocksdb::WriteOptions(), &write_batch).ok());

  for (size_t index = 0; index < batch.mutations.size(); ++index) {
    std::string encoded;
    ASSERT_TRUE(db_->Get(rocksdb::ReadOptions(), handles_[1], fact_keys[index],
                         &encoded)
                    .ok());
    const auto event = DecodeFactValue(batch.mutations[index].ref,
                                       batch.mutations[index].valid_from,
                                       sequence, encoded);
    ASSERT_TRUE(event.ok()) << event.status().ToString();
    EXPECT_EQ(event.ValueOrDie().operation, batch.mutations[index].operation);
  }
  std::string encoded_outcome;
  ASSERT_TRUE(db_->Get(rocksdb::ReadOptions(), handles_[2],
                       EncodeTransactionMetaKey(batch.txn_id).ValueOrDie(),
                       &encoded_outcome)
                  .ok());
  const auto outcome = DecodeTransactionOutcome(encoded_outcome);
  ASSERT_TRUE(outcome.ok()) << outcome.status().ToString();
  EXPECT_EQ(outcome.ValueOrDie().commit_seq, sequence);
  std::string encoded_sequence;
  ASSERT_TRUE(db_->Get(rocksdb::ReadOptions(), handles_[2],
                       EncodeSequenceMetaKey(sequence).ValueOrDie(),
                       &encoded_sequence)
                  .ok());
  const auto record = DecodeSequenceRecord(encoded_sequence);
  ASSERT_TRUE(record.ok()) << record.status().ToString();
  EXPECT_EQ(record.ValueOrDie().fact_keys, fact_keys);
}

TEST_F(CommitPublisherTest, AppendsAuthoritativeEdgeIdentityFactBesideRoutingMetadata) {
  const EdgeIdentity identity{
      EdgeRef{PartId{7}, EdgeId{17}},
      VertexRef{PartId{7}, VertexId{101}},
      VertexRef{PartId{9}, VertexId{202}}, 3};
  const StoreCommitBatch batch{
      TxnId{8}, 18,
      {{EntityFact::Edge(identity.edge_ref()).ref(), ValidTime{10},
        FactOperation::kPut, 0, std::nullopt}},
      {identity}, {}, {}};
  const CommitSeq sequence{5};
  const std::vector<std::string> fact_keys{
      EncodeFactKey(batch.mutations[0].ref, batch.mutations[0].valid_from, sequence)};
  rocksdb::WriteBatch write_batch;

  ASSERT_TRUE(AppendCandidateToWriteBatch(
                  CandidateCommit{&batch, sequence, fact_keys}, handles_[1],
                  handles_[2], &write_batch)
                  .ok());
  ASSERT_TRUE(db_->Write(rocksdb::WriteOptions(), &write_batch).ok());

  const FactRef identity_ref(PartId{7}, FactFamily::kEdgeIdentity,
                             PropertyId{}, identity.edge_id.value);
  const std::string identity_key =
      EncodeFactKey(identity_ref, ValidTime{0}, sequence);
  std::string encoded;
  ASSERT_TRUE(db_->Get(rocksdb::ReadOptions(), handles_[1], identity_key,
                       &encoded)
                  .ok());
  const auto decoded = DecodeFactValue(identity_ref, ValidTime{0}, sequence,
                                       encoded);
  ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
  EXPECT_EQ(decoded.ValueOrDie().edge_identity,
            std::optional<EdgeIdentity>{identity});

  std::string encoded_sequence;
  ASSERT_TRUE(db_->Get(rocksdb::ReadOptions(), handles_[2],
                       EncodeSequenceMetaKey(sequence).ValueOrDie(),
                       &encoded_sequence)
                  .ok());
  const auto sequence_record = DecodeSequenceRecord(encoded_sequence);
  ASSERT_TRUE(sequence_record.ok()) << sequence_record.status().ToString();
  const std::vector<std::string> expected_fact_keys{fact_keys.front(), identity_key};
  EXPECT_EQ(sequence_record.ValueOrDie().fact_keys, expected_fact_keys);
}

TEST(DecidedEpochTest, ExposesOneImmutablePrebuiltBatchSubmission) {
  auto batch = std::make_unique<rocksdb::WriteBatch>();
  batch->Put("prebuilt-key", "prebuilt-value");
  const size_t encoded_bytes = batch->GetDataSize();
  StoreCommittedGroupResult result;
  result.results.emplace_back(StoreCommitResult{CommitSeq{8}, 17});

  DecidedEpoch epoch(CommitSeq{7}, CommitSeq{8}, 1, true, std::move(batch),
                     std::move(result));

  EXPECT_EQ(epoch.base_visible_seq(), CommitSeq{7});
  EXPECT_EQ(epoch.visible_seq_target(), CommitSeq{8});
  EXPECT_EQ(epoch.group_result().results.size(), 1U);
  EXPECT_EQ(epoch.batch().GetDataSize(), encoded_bytes);
  EXPECT_NE(epoch.ClaimBatchForWrite(), nullptr);
  EXPECT_EQ(epoch.ClaimBatchForWrite(), nullptr);
}

TEST(DecidedEpochTest, TransfersCommittedResultsWithoutRetainingACopy) {
  auto batch = std::make_unique<rocksdb::WriteBatch>();
  batch->Put("prebuilt-key", "prebuilt-value");
  StoreCommittedGroupResult result;
  result.results.emplace_back(StoreCommitResult{CommitSeq{8}, 17});
  result.results.emplace_back(StoreCommitResult{CommitSeq{9}, 18});

  DecidedEpoch epoch(CommitSeq{7}, CommitSeq{9}, 2, true, std::move(batch),
                     std::move(result));

  StoreCommittedGroupResult transferred = epoch.TakeGroupResult();
  ASSERT_EQ(transferred.results.size(), 2U);
  EXPECT_EQ(transferred.results[0].ValueOrDie().commit_seq, CommitSeq{8});
  EXPECT_EQ(transferred.results[1].ValueOrDie().commit_seq, CommitSeq{9});
  EXPECT_TRUE(epoch.group_result().results.empty());
}

TEST(DecidedEpochTest, DoesNotRetainABatchForANonDurableDecision) {
  auto batch = std::make_unique<rocksdb::WriteBatch>();
  batch->Put("must-not-be-written", "value");
  StoreCommittedGroupResult result;
  result.results.emplace_back(Status::Conflict("commit", "rejected"));

  DecidedEpoch epoch(CommitSeq{7}, CommitSeq{7}, 0, false, std::move(batch),
                     std::move(result));

  EXPECT_FALSE(epoch.requires_durable_write());
  EXPECT_EQ(epoch.ClaimBatchForWrite(), nullptr);
}

}  // namespace
}  // namespace cedar::internal
