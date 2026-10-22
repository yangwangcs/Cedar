// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>

#include "cedar/fact/fact_codec.h"
#include "cedar/fact/meta_codec.h"
#include "storage/facts/group_commit_planner.h"
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

  // Encodes the candidate batches exactly as the commit path does, including
  // the visible-watermark record the group write appends, and returns the real
  // RocksDB WriteBatch.
  rocksdb::WriteBatch EncodeGroup(
      const std::vector<const StoreCommitBatch*>& batches, CommitSeq base,
      uint32_t sequence_format = kMetaSequenceFormatV1) {
    rocksdb::WriteBatch write_batch;
    CommitSeq sequence = base;
    for (const StoreCommitBatch* batch : batches) {
      std::vector<std::string> fact_keys;
      fact_keys.reserve(batch->mutations.size());
      for (const PendingFactMutation& mutation : batch->mutations) {
        fact_keys.push_back(
            EncodeFactKey(mutation.ref, mutation.valid_from, sequence));
      }
      const Status appended = AppendCandidateToWriteBatch(
          CandidateCommit{batch, sequence, std::move(fact_keys)}, handles_[1],
          handles_[2], sequence_format, &write_batch);
      EXPECT_TRUE(appended.ok()) << appended.ToString();
      ++sequence.value;
    }
    const auto watermark = EncodeWatermark(sequence);
    EXPECT_TRUE(watermark.ok()) << watermark.status().ToString();
    write_batch.Put(handles_[2], EncodeVisibleWatermarkKey(),
                    watermark.ValueOrDie());
    return write_batch;
  }
};

PendingFactMutation VertexValueMutation(uint64_t entity, uint64_t valid_from,
                                       std::optional<Value> value) {
  return {PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{entity}},
                               PropertyId{7})
              .ref(),
          ValidTime{valid_from}, FactOperation::kPut, 1, std::move(value)};
}

StoreCommitBatch ForecastBatch(uint64_t txn_id, uint64_t first_entity,
                               size_t facts) {
  std::vector<PendingFactMutation> mutations;
  mutations.reserve(facts);
  for (size_t index = 0; index < facts; ++index) {
    mutations.push_back(VertexValueMutation(
        first_entity + index, 10 + index,
        Value::Int32(static_cast<int32_t>(index))));
  }
  return StoreCommitBatch{TxnId{txn_id}, txn_id, std::move(mutations), {}, {}, {}};
}

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
                  handles_[2], kMetaSequenceFormatV1, &write_batch)
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
                  handles_[2], kMetaSequenceFormatV1, &write_batch)
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

TEST_F(CommitPublisherTest,
       V2SequenceRecordDropsNineBytesPerFactAndFitsTheExactByteCap) {
  const StoreCommitBatch batch = ForecastBatch(42, 500, 7);
  const rocksdb::WriteBatch v1 =
      EncodeGroup({&batch}, CommitSeq{1}, kMetaSequenceFormatV1);
  rocksdb::WriteBatch v2 =
      EncodeGroup({&batch}, CommitSeq{1}, kMetaSequenceFormatV2);
  // v1 sequence payload is 28 + 32N; v2 is 22 + 23N. The record, and therefore
  // the assembled WriteBatch, shrinks by exactly 6 + 9 bytes per fact.
  EXPECT_EQ(v1.GetDataSize() - v2.GetDataSize(), 6U + 9U * 7U);
  // The generic guard measures the real encoded batch, so the smaller record is
  // accepted at the exact hard cap instead of being over-counted.
  EXPECT_TRUE(CheckCommittedBatchSize(v2, v2.GetDataSize()).ok());

  ASSERT_TRUE(db_->Write(rocksdb::WriteOptions(), &v2).ok());
  const auto sequence_key = EncodeSequenceMetaKey(CommitSeq{1});
  ASSERT_TRUE(sequence_key.ok()) << sequence_key.status().ToString();
  std::string encoded_sequence;
  ASSERT_TRUE(db_->Get(rocksdb::ReadOptions(), handles_[2],
                       sequence_key.ValueOrDie(), &encoded_sequence)
                  .ok());
  ASSERT_GE(encoded_sequence.size(), 2U);
  EXPECT_EQ(static_cast<uint8_t>(encoded_sequence[1]), 9);  // kSequenceV2
  const auto decoded = DecodeSequenceRecord(encoded_sequence, CommitSeq{1});
  ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
  EXPECT_EQ(decoded.ValueOrDie().fact_keys.size(), 7U);
}

TEST_F(CommitPublisherTest, RejectsDuplicateCanonicalFactKeysBeforeWriting) {
  const StoreCommitBatch batch{
      TxnId{9}, 19,
      {{EntityFact::Vertex(VertexRef{PartId{0}, VertexId{3}}).ref(),
        ValidTime{10}, FactOperation::kPut, 0, std::nullopt},
       {EntityFact::Vertex(VertexRef{PartId{0}, VertexId{3}}).ref(),
        ValidTime{10}, FactOperation::kDelete, 0, std::nullopt}},
      {}, {}, {}, {}};
  const CommitSeq sequence{6};
  const std::string key = EncodeFactKey(batch.mutations[0].ref,
                                        batch.mutations[0].valid_from,
                                        sequence);
  rocksdb::WriteBatch write_batch;

  const Status status = AppendCandidateToWriteBatch(
      CandidateCommit{&batch, sequence, {key, key}}, handles_[1], handles_[2],
      kMetaSequenceFormatV1, &write_batch);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(write_batch.Count(), 0U);
}

TEST_F(CommitPublisherTest, EstimateCommitBatchBytesMatchesEncodedBatchWithinTolerance) {
  std::vector<std::pair<std::string, StoreCommitBatch>> cases;
  {
    std::vector<PendingFactMutation> mutations;
    for (uint64_t index = 0; index < 5; ++index) {
      mutations.push_back(
          VertexValueMutation(100 + index, 10, Value::Int32(7)));
    }
    cases.emplace_back(
        "observation-int32-5",
        StoreCommitBatch{TxnId{1}, 1, std::move(mutations), {}, {}, {}});
  }
  cases.emplace_back("forecast-int32-182", ForecastBatch(2, 1000, 182));
  {
    std::vector<PendingFactMutation> mutations;
    for (uint64_t index = 0; index < 3; ++index) {
      mutations.push_back(VertexValueMutation(
          200 + index, 10, Value::String(std::string(4000, 'x'))));
    }
    cases.emplace_back(
        "strings-4000-3",
        StoreCommitBatch{TxnId{3}, 1, std::move(mutations), {}, {}, {}});
  }
  {
    std::vector<PendingFactMutation> mutations;
    mutations.push_back(VertexValueMutation(300, 10, Value::Int64(1)));
    mutations.push_back(VertexValueMutation(301, 11, Value::Bool(true)));
    mutations.push_back(VertexValueMutation(302, 12, std::nullopt));
    mutations.back().operation = FactOperation::kDelete;
    mutations.push_back(VertexValueMutation(303, 13, Value::String("tiny")));
    cases.emplace_back(
        "mixed-int64-bool-delete-string",
        StoreCommitBatch{TxnId{4}, 1, std::move(mutations), {}, {}, {}});
  }
  {
    std::vector<PendingFactMutation> mutations;
    mutations.push_back(VertexValueMutation(
        400, 10, Value::String(std::string(1024 * 1024, 'y'))));
    cases.emplace_back(
        "single-mebibyte-string",
        StoreCommitBatch{TxnId{5}, 1, std::move(mutations), {}, {}, {}});
  }
  {
    const EdgeIdentity identity{
        EdgeRef{PartId{3}, EdgeId{17}},
        VertexRef{PartId{3}, VertexId{500}},
        VertexRef{PartId{4}, VertexId{600}}, 9};
    std::vector<PendingFactMutation> mutations;
    mutations.push_back({EntityFact::Edge(identity.edge_ref()).ref(),
                         ValidTime{10}, FactOperation::kPut, 0, std::nullopt});
    cases.emplace_back(
        "edge-identity",
        StoreCommitBatch{TxnId{6}, 1, std::move(mutations), {identity}, {}, {}});
  }

  for (const auto& [name, batch] : cases) {
    SCOPED_TRACE(name);
    for (uint32_t format :
         {kMetaSequenceFormatV1, kMetaSequenceFormatV2}) {
      SCOPED_TRACE(::testing::Message() << "format=" << format);
      const std::vector<const StoreCommitBatch*> group{&batch};
      const size_t actual =
          EncodeGroup(group, CommitSeq{1}, format).GetDataSize();
      const size_t estimate = EstimateCommitBatchBytes(batch, format);
      EXPECT_GE(estimate, actual);
      // The calibrated estimator may only carry a small absolute safety margin
      // over the real encoder; the former 2x reservation is not allowed back.
      EXPECT_LE(estimate - actual, actual / 64 + 512)
          << "estimate=" << estimate << " actual=" << actual;
    }
  }
}

TEST_F(CommitPublisherTest, EstimateSequenceRecordBytesMatchesBothEncoders) {
  for (size_t fact_count : {size_t{1}, size_t{5}, size_t{182}}) {
    SCOPED_TRACE(::testing::Message() << "facts=" << fact_count);
    SequenceRecord record{CommitSeq{7}, TxnId{9}, 11, {}};
    record.fact_keys.reserve(fact_count);
    for (size_t index = 0; index < fact_count; ++index) {
      record.fact_keys.push_back(EncodeFactKey(
          EntityFact::Vertex(VertexRef{PartId{0}, VertexId{index + 1}}).ref(),
          ValidTime{10}, CommitSeq{7}));
    }
    const auto v1 = EncodeSequenceRecord(record);
    ASSERT_TRUE(v1.ok()) << v1.status().ToString();
    EXPECT_EQ(EstimateSequenceRecordBytes(kMetaSequenceFormatV1, fact_count),
              v1.ValueOrDie().size());
    const auto v2 = EncodeSequenceRecordV2(record);
    ASSERT_TRUE(v2.ok()) << v2.status().ToString();
    EXPECT_EQ(EstimateSequenceRecordBytes(kMetaSequenceFormatV2, fact_count),
              v2.ValueOrDie().size());
    // v2 replaces each duplicated 32-byte canonical key with a 23-byte
    // descriptor and drops the 8-byte commit_seq from the payload (it is the
    // record key), i.e. exactly 6 + 9 bytes per fact.
    EXPECT_EQ(v1.ValueOrDie().size() - v2.ValueOrDie().size(),
              6U + 9U * fact_count);
  }
  // The format parameter defaults to v1 so existing callers keep byte-identical
  // estimates.
  const StoreCommitBatch batch = ForecastBatch(3, 500, 4);
  EXPECT_EQ(EstimateCommitBatchBytes(batch),
            EstimateCommitBatchBytes(batch, kMetaSequenceFormatV1));
}

TEST_F(CommitPublisherTest, ForecastGroupAtTwoMebibyteCapIsNotCutEarly) {
  constexpr uint64_t kByteCap = 2ULL * 1024ULL * 1024ULL;
  constexpr size_t kCountCap = 128;
  std::vector<StoreCommitBatch> batches;
  batches.reserve(kCountCap);
  for (size_t index = 0; index < kCountCap; ++index) {
    batches.push_back(ForecastBatch(
        100 + index, 1'000'000 + index * 1000, 182));
  }

  // Replay the collector's byte-limit stop for these independent batches.
  uint64_t selected_bytes = 0;
  size_t selected = 0;
  for (const StoreCommitBatch& batch : batches) {
    const size_t estimate = EstimateCommitBatchBytes(batch);
    if (selected != 0 &&
        estimate > kByteCap - std::min<uint64_t>(selected_bytes, kByteCap)) {
      break;
    }
    selected_bytes += estimate;
    ++selected;
  }
  EXPECT_EQ(selected, kCountCap) << "estimate stopped the group at " << selected;
  EXPECT_LE(selected_bytes, kByteCap);

  std::vector<const StoreCommitBatch*> group;
  group.reserve(selected);
  for (size_t index = 0; index < selected; ++index) {
    group.push_back(&batches[index]);
  }
  const rocksdb::WriteBatch encoded = EncodeGroup(group, CommitSeq{1});
  const size_t actual = encoded.GetDataSize();
  EXPECT_LE(actual, kByteCap);
  EXPECT_TRUE(CheckCommittedBatchSize(encoded, kByteCap).ok());
  // The test is only meaningful if these 128 transactions really do nearly
  // fill the 2 MiB cap.
  EXPECT_GE(actual, kByteCap * 95 / 100);
}

TEST_F(CommitPublisherTest, CommitByteGuardRejectsActualOversizeGroup) {
  constexpr uint64_t kByteCap = 2ULL * 1024ULL * 1024ULL;
  std::vector<PendingFactMutation> mutations;
  mutations.reserve(182);
  for (uint64_t index = 0; index < 182; ++index) {
    mutations.push_back(VertexValueMutation(
        1'000'000 + index, 10 + index,
        Value::String(std::string(16 * 1024, 'z'))));
  }
  const StoreCommitBatch batch{TxnId{900}, 1, std::move(mutations), {}, {}, {}};
  const std::vector<const StoreCommitBatch*> group{&batch};
  const rocksdb::WriteBatch encoded = EncodeGroup(group, CommitSeq{1});
  ASSERT_GT(encoded.GetDataSize(), kByteCap);
  EXPECT_TRUE(CheckCommittedBatchSize(encoded, kByteCap).IsResourceExhausted());
}

TEST(DecidedEpochTest, ExposesOneImmutablePrebuiltBatchSubmission) {
  auto batch = std::make_unique<rocksdb::WriteBatch>();
  batch->Put("prebuilt-key", "prebuilt-value");
  const size_t encoded_bytes = batch->GetDataSize();
  StoreCommittedGroupResult result;
  result.results.emplace_back(StoreCommitResult{CommitSeq{8}, 17});

  // The alignment digest is irrelevant here: this test never calls
  // WriteDecidedGroup, which is the only verifier of it.
  DecidedEpoch epoch(CommitSeq{7}, CommitSeq{8}, 1, true, std::move(batch),
                     0, std::move(result));

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
                     0, std::move(result));

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
                     0, std::move(result));

  EXPECT_FALSE(epoch.requires_durable_write());
  EXPECT_EQ(epoch.ClaimBatchForWrite(), nullptr);
}

}  // namespace
}  // namespace cedar::internal
