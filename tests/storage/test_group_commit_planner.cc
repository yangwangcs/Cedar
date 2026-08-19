// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include "storage/facts/group_commit_planner.h"
#include "storage/facts/pending_version_overlay.h"

namespace cedar::internal {
namespace {

PendingFactMutation Mutation(FactRef ref, uint64_t valid_time) {
  return {ref, ValidTime{valid_time}, FactOperation::kPut, 0, std::nullopt};
}

StoreCommitBatch Batch(TxnId txn_id,
                       std::vector<PendingFactMutation> mutations) {
  return {txn_id, txn_id.value, std::move(mutations), {}, {}, {}};
}

TEST(GroupCommitPlannerTest, AllowsIndependentPropertyAndMultiFactBatches) {
  StoreCommitBatch left = Batch(
      TxnId{1},
      {Mutation(PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{7}).ref(), 10)});
  StoreCommitBatch right =
      Batch(TxnId{2}, {Mutation(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}).ref(), 10),
                       Mutation(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{3}}).ref(), 10)});

  EXPECT_TRUE(CanSharePhysicalWrite(BuildCommitFootprint(left),
                                    BuildCommitFootprint(right)));
}

TEST(GroupCommitPlannerTest, RejectsSameFactAtDifferentValidTimes) {
  const FactRef ref =
      PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{7}).ref();
  StoreCommitBatch left = Batch(TxnId{1}, {Mutation(ref, 10)});
  StoreCommitBatch right = Batch(TxnId{2}, {Mutation(ref, 20)});

  EXPECT_FALSE(CanSharePhysicalWrite(BuildCommitFootprint(left),
                                     BuildCommitFootprint(right)));
}

TEST(GroupCommitPlannerTest, RejectsStrictReadWriteIntersectionBothDirections) {
  const FactRef ref = EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}).ref();
  StoreCommitBatch reader =
      Batch(TxnId{1}, {Mutation(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}).ref(), 10)});
  reader.strict_read_dependencies.push_back(
      StrictReadDependency{ref, ValidTime{10}, CommitSeq{0}});
  StoreCommitBatch writer = Batch(TxnId{2}, {Mutation(ref, 20)});

  EXPECT_FALSE(CanSharePhysicalWrite(BuildCommitFootprint(reader),
                                     BuildCommitFootprint(writer)));
  EXPECT_FALSE(CanSharePhysicalWrite(BuildCommitFootprint(writer),
                                     BuildCommitFootprint(reader)));
}

TEST(GroupCommitPlannerTest, RejectsSharedEdgeIdentity) {
  const EdgeIdentity first{EdgeId{9}, VertexId{1}, VertexId{2}, 7};
  const EdgeIdentity second{EdgeId{9}, VertexId{1}, VertexId{3}, 7};
  StoreCommitBatch left =
      Batch(TxnId{1}, {Mutation(EntityFact::Edge(EdgeRef{PartId{0}, EdgeId{9}}).ref(), 10)});
  StoreCommitBatch right =
      Batch(TxnId{2}, {Mutation(EntityFact::Edge(EdgeRef{PartId{0}, EdgeId{10}}).ref(), 10)});
  left.edge_identities.push_back(first);
  right.edge_identities.push_back(second);

  EXPECT_FALSE(CanSharePhysicalWrite(BuildCommitFootprint(left),
                                     BuildCommitFootprint(right)));
}

TEST(GroupCommitPlannerTest, AllowsSameNumericIdsInDifferentPartitions) {
  const VertexRef left_vertex{PartId{1}, VertexId{9}};
  const VertexRef right_vertex{PartId{2}, VertexId{9}};
  StoreCommitBatch left =
      Batch(TxnId{1}, {Mutation(EntityFact::Vertex(left_vertex).ref(), 10)});
  StoreCommitBatch right =
      Batch(TxnId{2}, {Mutation(EntityFact::Vertex(right_vertex).ref(), 10)});

  EXPECT_TRUE(CanSharePhysicalWrite(BuildCommitFootprint(left),
                                    BuildCommitFootprint(right)));

  left.edge_identities.emplace_back(
      EdgeRef{PartId{1}, EdgeId{7}}, left_vertex,
      VertexRef{PartId{1}, VertexId{10}}, 1);
  right.edge_identities.emplace_back(
      EdgeRef{PartId{2}, EdgeId{7}}, right_vertex,
      VertexRef{PartId{2}, VertexId{10}}, 1);
  EXPECT_TRUE(CanSharePhysicalWrite(BuildCommitFootprint(left),
                                    BuildCommitFootprint(right)));
}

TEST(GroupCommitPlannerTest, RejectsDuplicateTransactionId) {
  StoreCommitBatch left =
      Batch(TxnId{1}, {Mutation(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}).ref(), 10)});
  StoreCommitBatch right =
      Batch(TxnId{1}, {Mutation(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}).ref(), 10)});

  EXPECT_FALSE(CanSharePhysicalWrite(BuildCommitFootprint(left),
                                     BuildCommitFootprint(right)));
}

TEST(GroupCommitPlannerTest, IgnoresSnapshotOnlyReadSkew) {
  StoreCommitBatch left =
      Batch(TxnId{1}, {Mutation(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}).ref(), 10)});
  StoreCommitBatch right =
      Batch(TxnId{2}, {Mutation(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}).ref(), 10)});
  left.snapshot_write_dependencies.push_back(SnapshotWriteDependency{
      left.mutations.front().ref, left.mutations.front().valid_from,
      std::nullopt, std::nullopt, CommitSeq{0}});
  right.snapshot_write_dependencies.push_back(SnapshotWriteDependency{
      right.mutations.front().ref, right.mutations.front().valid_from,
      std::nullopt, std::nullopt, CommitSeq{0}});

  EXPECT_TRUE(CanSharePhysicalWrite(BuildCommitFootprint(left),
                                    BuildCommitFootprint(right)));
}

TEST(GroupCommitPlannerTest, AccountsForStrictReadObservedValueBytes) {
  StoreCommitBatch batch = Batch(
      TxnId{1}, {Mutation(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}).ref(), 10)});
  const size_t without_dependency = EstimateCommitBatchBytes(batch);
  const FactRef ref = PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{2}}, PropertyId{7}).ref();
  batch.strict_read_dependencies.push_back(StrictReadDependency{
      ref, ValidTime{10}, CommitSeq{1},
      FactEvent{ref, ValidTime{10}, CommitSeq{1}, FactOperation::kPut, 1,
                Value::String(std::string(4096, 'x'))},
      std::nullopt, std::nullopt});

  EXPECT_GE(EstimateCommitBatchBytes(batch), without_dependency + 4096U);
}

TEST(GroupCommitPlannerTest, ClassifiesOnlyBlindBoundaryAppendsAsFastPath) {
  StoreCommitBatch append = Batch(
      TxnId{2}, {Mutation(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}).ref(), 10)});
  EXPECT_TRUE(CanUseAppendFastPath(append));

  StoreCommitBatch correction = Batch(
      TxnId{3}, {Mutation(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}).ref(), 10)});
  correction.snapshot_write_dependencies.push_back(SnapshotWriteDependency{
      correction.mutations.front().ref, correction.mutations.front().valid_from,
      ValidTime{1}, std::nullopt, CommitSeq{1}});
  EXPECT_FALSE(CanUseAppendFastPath(correction));

  StoreCommitBatch strict = Batch(
      TxnId{4}, {Mutation(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{4}}).ref(), 10)});
  strict.strict_read_dependencies.push_back(StrictReadDependency{
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{9}}).ref(), ValidTime{10}, CommitSeq{1}});
  EXPECT_FALSE(CanUseAppendFastPath(strict));
}

TEST(GroupCommitPlannerTest, ConflictIndexChecksEachCandidateAgainstAccumulatedEpoch) {
  CommitConflictIndex index;
  for (uint64_t value = 1; value <= 256; ++value) {
    StoreCommitBatch batch = Batch(
        TxnId{value},
        {Mutation(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{value}}).ref(), value)});
    const CommitFootprint footprint = BuildCommitFootprint(batch);
    EXPECT_TRUE(index.Insert(footprint));
  }

  StoreCommitBatch duplicate_write = Batch(
      TxnId{1000},
      {Mutation(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{128}}).ref(), 999)});
  EXPECT_FALSE(index.Insert(BuildCommitFootprint(duplicate_write)));

  StoreCommitBatch duplicate_transaction = Batch(
      TxnId{128},
      {Mutation(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1001}}).ref(), 999)});
  EXPECT_FALSE(index.CanInsert(BuildCommitFootprint(duplicate_transaction)));

  StoreCommitBatch strict_reader = Batch(
      TxnId{1002},
      {Mutation(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1002}}).ref(), 999)});
  strict_reader.strict_read_dependencies.push_back(
      StrictReadDependency{EntityFact::Vertex(VertexRef{PartId{0}, VertexId{64}}).ref(),
                           ValidTime{1}, CommitSeq{1}});
  EXPECT_FALSE(index.CanInsert(BuildCommitFootprint(strict_reader)));
  EXPECT_EQ(index.size(), 256U);
}

TEST(PendingVersionOverlayTest, DetectsAPlannedEpochWriteConflict) {
  StoreCommitBatch planned = Batch(
      TxnId{5}, {Mutation(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{5}}).ref(), 10)});
  PendingVersionOverlay overlay = PendingVersionOverlay::FromBatch(planned);
  EXPECT_EQ(overlay.size(), 1U);

  StoreCommitBatch independent = Batch(
      TxnId{6}, {Mutation(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{6}}).ref(), 10)});
  StoreCommitBatch conflict = Batch(
      TxnId{7}, {Mutation(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{5}}).ref(), 20)});
  EXPECT_FALSE(overlay.Conflicts(BuildCommitFootprint(independent)));
  EXPECT_TRUE(overlay.Conflicts(BuildCommitFootprint(conflict)));
}

TEST(PendingVersionOverlayTest, DetectsStrictReadWriteIntersection) {
  const FactRef observed =
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{51}}).ref();
  StoreCommitBatch planned = Batch(
      TxnId{8}, {Mutation(EntityFact::Vertex(
                       VertexRef{PartId{0}, VertexId{52}})
                       .ref(),
                   10)});
  planned.strict_read_dependencies.push_back(
      StrictReadDependency{observed, ValidTime{10}, CommitSeq{0}});
  PendingVersionOverlay overlay = PendingVersionOverlay::FromBatch(planned);

  StoreCommitBatch writer = Batch(TxnId{9}, {Mutation(observed, 20)});
  EXPECT_TRUE(overlay.Conflicts(BuildCommitFootprint(writer)));
}

}  // namespace
}  // namespace cedar::internal
