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

}  // namespace
}  // namespace cedar::internal
