// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>
#include <random>
#include <unordered_map>

#include "storage/facts/recent_fact_write_index.h"

namespace cedar::internal {
namespace {

TEST(RecentFactWriteIndexTest, RepeatedIdentityBeforeSnapshotAvoidsBloomMaybe) {
  RecentFactWriteIndex index(4096, CommitSeq{7});
  const FactRef a = EntityFact::Vertex(VertexRef{PartId{0}, VertexId{71}}).ref();
  const FactRef b = EntityFact::Vertex(VertexRef{PartId{0}, VertexId{72}}).ref();
  index.Publish(a, CommitSeq{8});
  index.Publish(b, CommitSeq{9});
  EXPECT_TRUE(index.CanProveUnchanged(a, CommitSeq{8}));
  EXPECT_FALSE(index.CanProveUnchanged(a, CommitSeq{7}));
}

TEST(RecentFactWriteIndexTest, LongHistoryProofSurvivesBloomRotation) {
  RecentFactWriteIndex index(4096, CommitSeq{7}, 2);
  const FactRef a = EntityFact::Vertex(VertexRef{PartId{0}, VertexId{71}}).ref();
  for (uint64_t seq = 8; seq <= 520; ++seq) index.Publish(a, CommitSeq{seq});
  const FactRef b = EntityFact::Vertex(VertexRef{PartId{0}, VertexId{72}}).ref();
  index.Publish(b, CommitSeq{521});
  EXPECT_TRUE(index.CanProveUnchanged(a, CommitSeq{520}));
  EXPECT_FALSE(index.CanProveUnchanged(a, CommitSeq{519}));
}

TEST(RecentFactWriteIndexTest, NeverProvesUnchangedAcrossLaterWrites) {
  std::mt19937_64 rng(20261012);
  for (size_t budget : {size_t{0}, size_t{1}, size_t{32}, size_t{4096}}) {
    RecentFactWriteIndex index(budget, CommitSeq{7}, 8);
    std::unordered_map<uint64_t, uint64_t> latest;
    uint64_t seq = 7;
    for (size_t op = 0; op < 100000; ++op) {
      const uint64_t id = rng() % 256;
      const FactRef ref = EntityFact::Vertex(VertexRef{PartId{0}, VertexId{id+1}}).ref();
      if (rng() % 3 != 0) {
        ++seq;
        uint64_t publish = op % 97 == 0 && seq > 10 ? seq - 3 : seq;
        index.Publish(ref, CommitSeq{publish});
        latest[id] = std::max(latest[id], publish);
      } else {
        const uint64_t snapshot = rng() % (seq + 1);
        if (index.CanProveUnchanged(ref, CommitSeq{snapshot})) {
          ASSERT_LE(latest[id], snapshot) << "budget=" << budget << " op=" << op;
        }
      }
      ASSERT_LE(index.resident_bytes(), budget);
    }
  }
}

TEST(RecentFactWriteIndexTest, EqualSequenceAndUnexpectedOlderPublicationStaySafe) {
  RecentFactWriteIndex index(4096, CommitSeq{7});
  const FactRef a = EntityFact::Vertex(VertexRef{PartId{0}, VertexId{71}}).ref();
  const FactRef b = EntityFact::Vertex(VertexRef{PartId{0}, VertexId{72}}).ref();
  index.Publish(a, CommitSeq{10});
  index.Publish(b, CommitSeq{10});
  EXPECT_FALSE(index.CanProveUnchanged(a, CommitSeq{9}));
  EXPECT_TRUE(index.CanProveUnchanged(a, CommitSeq{10}));
  index.Publish(a, CommitSeq{8});
  EXPECT_FALSE(index.CanProveUnchanged(a, CommitSeq{9}));
  index.Publish(b, CommitSeq{11});
  EXPECT_TRUE(index.CanProveUnchanged(b, CommitSeq{11}));
}

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
