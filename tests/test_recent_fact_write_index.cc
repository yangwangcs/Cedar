// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include "fact/recent_fact_write_index.h"

namespace cedar::internal {
namespace {

TEST(RecentFactWriteIndexTest, ProvesUnchangedOnlyAfterCoveredSnapshot) {
  RecentFactWriteIndex index(4096, CommitSeq{7});
  const FactRef ref = EntityFact::Vertex(VertexRef{PartId{0}, VertexId{71}}).ref();

  EXPECT_TRUE(index.CanProveUnchanged(ref, CommitSeq{7}));
  index.Publish(ref, CommitSeq{8});
  EXPECT_FALSE(index.CanProveUnchanged(ref, CommitSeq{7}));
  EXPECT_TRUE(index.CanProveUnchanged(ref, CommitSeq{8}));
  EXPECT_EQ(index.metrics().hits, 2U);
  EXPECT_EQ(index.metrics().misses, 1U);
}

TEST(RecentFactWriteIndexTest, UsesFixedBytesForArbitrarilyManyIdentities) {
  RecentFactWriteIndex index(4096, CommitSeq{3}, 64);
  ASSERT_GT(index.resident_bytes(), 0U);
  const size_t initial_bytes = index.resident_bytes();

  for (uint64_t id = 0; id != 4096; ++id) {
    index.Publish(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1000 + id}}).ref(),
                  CommitSeq{4 + id});
  }

  EXPECT_EQ(index.resident_bytes(), initial_bytes);
  EXPECT_EQ(index.filter_count(), 4U);
  EXPECT_GT(index.metrics().resets, 0U);
}

TEST(RecentFactWriteIndexTest, DropsOldCoverageWhenRollingWindowIsReused) {
  RecentFactWriteIndex index(4096, CommitSeq{3}, 2);
  for (uint64_t sequence = 4; sequence != 13; ++sequence) {
    index.Publish(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2000 + sequence}}).ref(),
                  CommitSeq{sequence});
  }

  EXPECT_GT(index.metrics().resets, 0U);
  EXPECT_FALSE(index.CanProveUnchanged(
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{9000}}).ref(), CommitSeq{3}));
  EXPECT_TRUE(index.CanProveUnchanged(
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{9000}}).ref(), CommitSeq{5}));
}

}  // namespace
}  // namespace cedar::internal
