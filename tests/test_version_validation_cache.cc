// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <map>

#include "fact/version_validation_cache.h"

namespace cedar::internal {
namespace {

std::map<uint64_t, uint64_t> AsMap(
    const std::vector<ValidationBoundary>& boundaries) {
  std::map<uint64_t, uint64_t> result;
  for (const ValidationBoundary& boundary : boundaries) {
    result.emplace(boundary.valid_from.value, boundary.commit_seq.value);
  }
  return result;
}

TEST(VersionValidationCacheTest, PublishesNewBoundaryIntoResidentChain) {
  VersionValidationCache cache(4096);
  const FactRef ref = PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{7}}, PropertyId{1}).ref();
  cache.Prime(ref, {{ValidTime{10}, CommitSeq{1}},
                    {ValidTime{20}, CommitSeq{2}}});

  const auto initial = cache.Lookup(ref);
  ASSERT_TRUE(initial.has_value());
  EXPECT_EQ(AsMap(*initial),
            (std::map<uint64_t, uint64_t>{{10, 1}, {20, 2}}));

  cache.Publish(PendingFactMutation{ref, ValidTime{30}, FactOperation::kPut,
                                    1, Value::Int64(3)},
                CommitSeq{3});
  const auto updated = cache.Lookup(ref);
  ASSERT_TRUE(updated.has_value());
  EXPECT_EQ(AsMap(*updated),
            (std::map<uint64_t, uint64_t>{{10, 1}, {20, 2}, {30, 3}}));
}

TEST(VersionValidationCacheTest, TreatsCapacityMissAsUnknownRatherThanAbsent) {
  VersionValidationCache cache(0);
  const FactRef ref = EntityFact::Vertex(VertexRef{PartId{0}, VertexId{8}}).ref();
  cache.Prime(ref, {{ValidTime{10}, CommitSeq{1}}});

  EXPECT_FALSE(cache.Lookup(ref).has_value());
  EXPECT_EQ(cache.resident_chains(), 0U);
  EXPECT_EQ(cache.resident_bytes(), 0U);
}

TEST(VersionValidationCacheTest, ReportsHitsAndUnknownMisses) {
  VersionValidationCache cache(4096);
  const FactRef resident = EntityFact::Vertex(VertexRef{PartId{0}, VertexId{9}}).ref();
  const FactRef absent = EntityFact::Vertex(VertexRef{PartId{0}, VertexId{10}}).ref();
  cache.Prime(resident, {{ValidTime{10}, CommitSeq{1}}});

  ASSERT_TRUE(cache.Lookup(resident).has_value());
  ASSERT_FALSE(cache.Lookup(absent).has_value());

  const auto metrics = cache.metrics();
  EXPECT_EQ(metrics.hits, 1U);
  EXPECT_EQ(metrics.misses, 1U);
}

TEST(VersionValidationCacheTest, DropsOversizedResidentChainToCanonicalFallback) {
  VersionValidationCache cache(4096);
  const FactRef ref = EntityFact::Vertex(VertexRef{PartId{0}, VertexId{11}}).ref();
  std::vector<ValidationBoundary> boundaries;
  for (uint64_t time = 1; time != 10; ++time) {
    boundaries.push_back(ValidationBoundary{ValidTime{time}, CommitSeq{time}});
  }

  cache.Prime(ref, std::move(boundaries));

  EXPECT_FALSE(cache.Lookup(ref).has_value());
  EXPECT_EQ(cache.resident_chains(), 0U);
}

TEST(VersionValidationCacheTest, DropsResidentChainWhenPublicationExceedsBound) {
  VersionValidationCache cache(4096);
  const FactRef ref = EntityFact::Vertex(VertexRef{PartId{0}, VertexId{12}}).ref();
  for (uint64_t time = 1; time != 9; ++time) {
    cache.Publish(PendingFactMutation{ref, ValidTime{time}, FactOperation::kPut,
                                      0, std::nullopt},
                  CommitSeq{time});
  }
  // Publication does not admit cold identities. Prime the canonical chain once
  // so the ninth subsequent boundary exercises the resident update path.
  std::vector<ValidationBoundary> initial;
  for (uint64_t time = 1; time != 9; ++time) {
    initial.push_back(ValidationBoundary{ValidTime{time}, CommitSeq{time}});
  }
  cache.Prime(ref, std::move(initial));
  ASSERT_TRUE(cache.Lookup(ref).has_value());

  cache.Publish(PendingFactMutation{ref, ValidTime{9}, FactOperation::kPut,
                                    0, std::nullopt},
                CommitSeq{9});

  EXPECT_FALSE(cache.Lookup(ref).has_value());
  EXPECT_EQ(cache.resident_chains(), 0U);
}

TEST(VersionValidationCacheTest, UsesAFixedAssociativeSlotBudget) {
  constexpr size_t kBudgetBytes = 4096;
  VersionValidationCache cache(kBudgetBytes);
  ASSERT_GT(cache.slot_capacity(), 0U);
  EXPECT_LE(cache.reserved_bytes(), kBudgetBytes);

  for (uint64_t id = 1; id <= cache.slot_capacity() * 4; ++id) {
    const FactRef ref = EntityFact::Vertex(VertexRef{PartId{0}, VertexId{100 + id}}).ref();
    cache.Prime(ref, {{ValidTime{10}, CommitSeq{id}}});
  }

  EXPECT_LE(cache.resident_chains(), cache.slot_capacity());
  EXPECT_LE(cache.resident_bytes(), cache.reserved_bytes());
}

}  // namespace
}  // namespace cedar::internal
