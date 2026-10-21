// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include "storage/facts/fact_identity_history.h"

namespace cedar::internal {
namespace {

TEST(FactIdentityHistoryTest, NeverReportsFalseNegativeAfterPublish) {
  FactIdentityHistory history(4096);
  const FactRef vertex =
      EntityFact::Vertex(VertexRef{PartId{3}, VertexId{42}}).ref();
  const FactRef property =
      PropertyFact::Vertex(VertexRef{PartId{3}, VertexId{42}}, PropertyId{7})
          .ref();
  EXPECT_FALSE(history.MightContain(vertex));
  EXPECT_FALSE(history.MightContain(property));
  history.Publish(vertex);
  EXPECT_TRUE(history.MightContain(vertex));
  EXPECT_FALSE(history.MightContain(property));
}

TEST(FactIdentityHistoryTest, KeepsResidentBytesBounded) {
  FactIdentityHistory history(4096);
  for (uint64_t id = 0; id < 100000; ++id) {
    history.Publish(
        EntityFact::Vertex(VertexRef{PartId{0}, VertexId{id + 1}}).ref());
  }
  EXPECT_LE(history.resident_bytes(), 4096U);
  EXPECT_GT(history.resident_bytes(), 0U);
}

TEST(FactIdentityHistoryTest, FallsBackConservativelyForInvalidRefs) {
  FactIdentityHistory history(1024);
  const FactRef invalid{PartId{0}, FactFamily::kVertexState, PropertyId{}, 0};
  EXPECT_TRUE(history.MightContain(invalid));
  history.Publish(invalid);
}

TEST(FactIdentityHistoryTest, CapacityIsTheBitsPerIdentityBudget) {
  // 4096 bytes -> 512 words (the power-of-two word count) -> 32,768 bits, i.e.
  // 4096 identities at the 8-bits-per-identity usefulness point.
  EXPECT_TRUE(FactIdentityHistory::CanRepresent(4096, 0));
  EXPECT_TRUE(FactIdentityHistory::CanRepresent(4096, 4096));
  EXPECT_FALSE(FactIdentityHistory::CanRepresent(4096, 4097));
  EXPECT_FALSE(FactIdentityHistory::CanRepresent(4096, 1'000'000));
  // The decision uses the same arithmetic the constructor allocates with, so a
  // filter that claims it can represent N identities really has the bits.
  FactIdentityHistory history(4096);
  EXPECT_EQ(history.bit_count(), 32768U);
  EXPECT_EQ(history.resident_bytes(), 4096U);
  EXPECT_EQ(history.bit_count() / FactIdentityHistory::kUsefulBitsPerIdentity,
            4096U);
}

TEST(FactIdentityHistoryTest, DisableLeavesTheFilterInItsAlwaysTrueState) {
  FactIdentityHistory history(4096);
  const FactRef published =
      EntityFact::Vertex(VertexRef{PartId{3}, VertexId{42}}).ref();
  const FactRef never_written =
      EntityFact::Vertex(VertexRef{PartId{3}, VertexId{43}}).ref();
  history.Publish(published);
  EXPECT_TRUE(history.MightContain(published));
  EXPECT_FALSE(history.MightContain(never_written));

  history.Disable();

  EXPECT_TRUE(history.disabled());
  EXPECT_EQ(history.resident_bytes(), 0U);
  EXPECT_EQ(history.bit_count(), 0U);
  // No false negatives: a skip must never turn into an empty filter that
  // answers "definitely not" for identities that exist (or for any identity).
  EXPECT_TRUE(history.MightContain(published));
  EXPECT_TRUE(history.MightContain(never_written));
  // Publish stays a no-op, so the state cannot silently become a real filter
  // again with a zero word count.
  history.Publish(never_written);
  EXPECT_TRUE(history.MightContain(never_written));
  EXPECT_EQ(history.resident_bytes(), 0U);
}

}  // namespace
}  // namespace cedar::internal
