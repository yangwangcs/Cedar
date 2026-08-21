// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <optional>
#include <vector>

#include "query/temporal/corrected_chain.h"
#include "query/temporal/interval.h"
#include "tests/model/bitemporal_fact_oracle.h"

namespace cedar {
namespace {

TEST(TemporalModelTest, PreservesAFormerlyRedundantBoundary) {
  const FactRef ref =
      PropertyFact::Vertex({PartId{0}, VertexId{1}}, PropertyId{7}).ref();
  const std::vector<FactEvent> events = {
      {ref, ValidTime{0}, CommitSeq{1}, FactOperation::kPut, 1,
       Value::Int64(7)},
      {ref, ValidTime{10}, CommitSeq{2}, FactOperation::kPut, 1,
       Value::Int64(7)},
      {ref, ValidTime{5}, CommitSeq{3}, FactOperation::kDelete, 1,
       std::nullopt},
  };

  auto boundaries = internal::ResolveCorrectedBoundaries(events, CommitSeq{3});
  ASSERT_TRUE(boundaries.ok()) << boundaries.status().ToString();
  const std::vector<internal::StateInterval> state =
      internal::MaterializePresentState(boundaries.ValueOrDie());

  ASSERT_EQ(state.size(), 2U);
  EXPECT_EQ(state[0].interval,
            (ValidTimeInterval{ValidTime{0}, ValidTime{5}}));
  EXPECT_EQ(state[0].value, std::optional<Value>(Value::Int64(7)));
  EXPECT_EQ(state[1].interval,
            (ValidTimeInterval{ValidTime{10}, std::nullopt}));
  EXPECT_EQ(state[1].value, std::optional<Value>(Value::Int64(7)));
}

TEST(TemporalModelTest, PicksTheLatestVisibleCorrectionAtOneBoundary) {
  const FactRef ref =
      PropertyFact::Vertex({PartId{0}, VertexId{1}}, PropertyId{7}).ref();
  const std::vector<FactEvent> events = {
      {ref, ValidTime{5}, CommitSeq{1}, FactOperation::kPut, 1,
       Value::Int64(7)},
      {ref, ValidTime{5}, CommitSeq{2}, FactOperation::kDelete, 1,
       std::nullopt},
      {ref, ValidTime{5}, CommitSeq{3}, FactOperation::kPut, 1,
       Value::Int64(9)},
  };

  auto at_two = internal::ResolveCorrectedBoundaries(events, CommitSeq{2});
  ASSERT_TRUE(at_two.ok()) << at_two.status().ToString();
  ASSERT_EQ(at_two.ValueOrDie().size(), 1U);
  EXPECT_EQ(at_two.ValueOrDie()[0].operation, FactOperation::kDelete);

  auto at_three = internal::ResolveCorrectedBoundaries(events, CommitSeq{3});
  ASSERT_TRUE(at_three.ok()) << at_three.status().ToString();
  ASSERT_EQ(at_three.ValueOrDie().size(), 1U);
  EXPECT_EQ(at_three.ValueOrDie()[0].operation, FactOperation::kPut);
  EXPECT_EQ(at_three.ValueOrDie()[0].value,
            std::optional<Value>(Value::Int64(9)));
}

TEST(TemporalModelTest, KeepsValuelessEntityPresenceSeparateFromDelete) {
  const FactRef ref =
      EntityFact::Vertex({PartId{0}, VertexId{1}}).ref();
  const std::vector<FactEvent> events = {
      {ref, ValidTime{0}, CommitSeq{1}, FactOperation::kPut, 0, std::nullopt},
      {ref, ValidTime{5}, CommitSeq{2}, FactOperation::kDelete, 0,
       std::nullopt},
  };

  auto boundaries = internal::ResolveCorrectedBoundaries(events, CommitSeq{2});
  ASSERT_TRUE(boundaries.ok()) << boundaries.status().ToString();
  const auto state = internal::MaterializePresentState(boundaries.ValueOrDie());
  ASSERT_EQ(state.size(), 1U);
  EXPECT_EQ(state[0].interval,
            (ValidTimeInterval{ValidTime{0}, ValidTime{5}}));
  EXPECT_EQ(state[0].value, std::nullopt);
}

TEST(TemporalModelTest, TouchingIntervalsDoNotOverlap) {
  EXPECT_FALSE(internal::Intersect(
                   {ValidTime{1}, ValidTime{2}},
                   {ValidTime{2}, ValidTime{3}})
                   .has_value());
}

TEST(TemporalModelTest, CoalescesOnlyAdjacentEqualPresentStates) {
  std::vector<internal::StateInterval> states = {
      {{ValidTime{0}, ValidTime{5}}, Value::Int64(7)},
      {{ValidTime{5}, ValidTime{10}}, Value::Int64(7)},
      {{ValidTime{10}, std::nullopt}, Value::Int64(9)},
  };

  const auto coalesced = internal::Coalesce(std::move(states));
  ASSERT_EQ(coalesced.size(), 2U);
  EXPECT_EQ(coalesced[0].interval,
            (ValidTimeInterval{ValidTime{0}, ValidTime{10}}));
  EXPECT_EQ(coalesced[1].interval,
            (ValidTimeInterval{ValidTime{10}, std::nullopt}));
}

TEST(TemporalModelTest, MaterializesMissingOnlyInsideEntityExistence) {
  const std::vector<internal::CorrectedBoundary> boundaries = {
      {ValidTime{0}, CommitSeq{1}, FactOperation::kPut, 1, Value::Int64(7),
       std::nullopt},
      {ValidTime{5}, CommitSeq{2}, FactOperation::kDelete, 1, std::nullopt,
       std::nullopt},
      {ValidTime{10}, CommitSeq{3}, FactOperation::kPut, 1, Value::Int64(9),
       std::nullopt},
  };

  const auto missing = internal::MaterializeMissingState(
      boundaries, {ValidTime{0}, ValidTime{15}});
  ASSERT_EQ(missing.size(), 1U);
  EXPECT_EQ(missing[0].interval,
            (ValidTimeInterval{ValidTime{5}, ValidTime{10}}));
  EXPECT_EQ(missing[0].value, std::nullopt);
}

TEST(TemporalModelTest, RejectsMixedFactChains) {
  const FactRef first =
      PropertyFact::Vertex({PartId{0}, VertexId{1}}, PropertyId{7}).ref();
  const FactRef second =
      PropertyFact::Vertex({PartId{0}, VertexId{2}}, PropertyId{7}).ref();
  const std::vector<FactEvent> events = {
      {first, ValidTime{0}, CommitSeq{1}, FactOperation::kPut, 1,
       Value::Int64(7)},
      {second, ValidTime{0}, CommitSeq{2}, FactOperation::kPut, 1,
       Value::Int64(9)},
  };

  EXPECT_TRUE(internal::ResolveCorrectedBoundaries(events, CommitSeq{2})
                  .status()
                  .IsInvalidArgument());
}

TEST(TemporalModelTest, OracleKeepsItsOwnCorrectedHistorySemantics) {
  const FactRef ref =
      PropertyFact::Vertex({PartId{0}, VertexId{1}}, PropertyId{7}).ref();
  test::BitemporalFactOracle oracle;
  oracle.Add({ref, ValidTime{0}, CommitSeq{1}, FactOperation::kPut, 1,
              Value::Int64(7)});
  oracle.Add({ref, ValidTime{10}, CommitSeq{2}, FactOperation::kPut, 1,
              Value::Int64(7)});
  oracle.Add({ref, ValidTime{5}, CommitSeq{3}, FactOperation::kDelete, 1,
              std::nullopt});

  const auto history = oracle.History(ref, CommitSeq{3});
  ASSERT_EQ(history.size(), 2U);
  EXPECT_EQ(history[0].interval,
            (ValidTimeInterval{ValidTime{0}, ValidTime{5}}));
  EXPECT_EQ(history[1].interval,
            (ValidTimeInterval{ValidTime{10}, std::nullopt}));

  const auto changes = oracle.Changes(ref, CommitSeq{3});
  ASSERT_EQ(changes.size(), 3U);
  EXPECT_EQ(changes[1].before, std::optional<Value>(Value::Int64(7)));
  EXPECT_EQ(changes[1].after, std::nullopt);
}

TEST(TemporalModelTest, OracleRetainsValuelessEntityHistory) {
  const FactRef ref =
      EntityFact::Vertex({PartId{0}, VertexId{1}}).ref();
  test::BitemporalFactOracle oracle;
  oracle.Add({ref, ValidTime{0}, CommitSeq{1}, FactOperation::kPut, 0,
              std::nullopt});
  oracle.Add({ref, ValidTime{5}, CommitSeq{2}, FactOperation::kDelete, 0,
              std::nullopt});

  const auto history = oracle.History(ref, CommitSeq{2});
  ASSERT_EQ(history.size(), 1U);
  EXPECT_EQ(history[0].interval,
            (ValidTimeInterval{ValidTime{0}, ValidTime{5}}));
  EXPECT_EQ(history[0].value, std::nullopt);
}

}  // namespace
}  // namespace cedar
