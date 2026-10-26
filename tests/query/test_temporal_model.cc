// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <optional>
#include <vector>

#include "query/temporal/corrected_chain.h"
#include "query/temporal/interval.h"
#include "query/temporal/state_reader.h"
#include "query/resource/query_read_memory.h"
#include "tests/model/bitemporal_fact_oracle.h"

namespace cedar {
namespace {

TEST(TemporalModelTest, StateStreamBudgetsOwnedHistoryAndReleasesAfterCancellation) {
  const FactRef ref = EntityFact::Vertex({PartId{0}, VertexId{1}}).ref();
  const FactEventBatch input{{
      {ref, ValidTime{0}, CommitSeq{1}, FactOperation::kPut, 0, std::nullopt}}};
  CanonicalStateReadSpec spec;
  spec.snapshot_seq = CommitSeq{1};
  spec.valid_time = ValidTime{5};
  internal::QueryReservation tiny(1);
  spec.facts.read_memory = std::make_shared<internal::QueryReadMemory>(tiny);
  {
    internal::StateRowStream stream(spec, [](const auto&) { return Status::OK(); });
    EXPECT_TRUE(stream.Consume(input).IsResourceExhausted());
  }
  EXPECT_EQ(tiny.used_bytes(), 0U);

  internal::QueryReservation adequate(1 << 20);
  spec.facts.read_memory = std::make_shared<internal::QueryReadMemory>(adequate);
  size_t calls = 0;
  {
    internal::StateRowStream stream(spec, [&](const auto& rows) {
      ++calls;
      EXPECT_EQ(rows.size(), 1U);
      EXPECT_GE(adequate.used_bytes(), sizeof(CanonicalStateRow));
      return Status::Conflict("test", "stop visitor");
    });
    ASSERT_TRUE(stream.Consume(input).ok());
    EXPECT_GE(adequate.used_bytes(), sizeof(FactEvent));
    EXPECT_TRUE(stream.Finish().IsConflict());
  }
  EXPECT_EQ(calls, 1U);
  EXPECT_EQ(adequate.used_bytes(), 0U);
}

TEST(TemporalModelTest, StateStreamRejectsCorrectionWorkspaceBeforeVisiting) {
  const FactRef ref = PropertyFact::Vertex({PartId{0}, VertexId{1}}, PropertyId{7}).ref();
  const FactEventBatch input{{
      {ref, ValidTime{0}, CommitSeq{1}, FactOperation::kPut, 1,
       Value::String(std::string(4096, 'x'))}}};
  CanonicalStateReadSpec spec;
  spec.snapshot_seq = CommitSeq{1};
  spec.valid_time = ValidTime{5};
  internal::QueryReservation budget(12 * 1024);
  spec.facts.read_memory = std::make_shared<internal::QueryReadMemory>(budget);
  size_t calls = 0;
  {
    internal::StateRowStream stream(spec, [&](const auto&) {
      ++calls;
      return Status::OK();
    });
    ASSERT_TRUE(stream.Consume(input).ok());
    EXPECT_GE(budget.used_bytes(), 4096U);
    const auto status = stream.Finish();
    EXPECT_TRUE(status.IsResourceExhausted()) << status.ToString();
    EXPECT_EQ(calls, 0U);
  }
  EXPECT_EQ(budget.used_bytes(), 0U);
}

TEST(TemporalModelTest, StateRowLimitCountsPreviouslyDeliveredBatches) {
  FactEventBatch input;
  for (uint64_t id : {1, 2, 3}) {
    input.events.push_back({EntityFact::Vertex({PartId{0}, VertexId{id}}).ref(),
                            ValidTime{0}, CommitSeq{1}, FactOperation::kPut, 0,
                            std::nullopt});
  }
  CanonicalStateReadSpec spec;
  spec.snapshot_seq = CommitSeq{1};
  spec.valid_time = ValidTime{5};
  spec.facts.batch_row_limit = 1;
  spec.max_rows = 2;
  std::vector<uint64_t> delivered;
  internal::StateRowStream stream(spec, [&](const auto& rows) {
    EXPECT_EQ(rows.size(), 1U);
    delivered.push_back(rows.front().ref.entity_id());
    return Status::OK();
  });
  const auto consumed = stream.Consume(input);
  EXPECT_TRUE(consumed.ok() || consumed.IsQueryCancelled()) << consumed.ToString();
  EXPECT_TRUE(stream.Finish().ok());
  EXPECT_EQ(delivered, (std::vector<uint64_t>{1, 2}));
}

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

TEST(TemporalModelTest,
     IgnoresFutureInvalidAndDifferentFactEventsForOlderSnapshots) {
  const FactRef ref =
      PropertyFact::Vertex({PartId{0}, VertexId{1}}, PropertyId{7}).ref();
  const FactRef different_ref =
      PropertyFact::Vertex({PartId{0}, VertexId{2}}, PropertyId{7}).ref();
  const std::vector<FactEvent> events = {
      {ref, ValidTime{5}, CommitSeq{1}, FactOperation::kPut, 1,
       Value::Int64(7)},
      {different_ref, ValidTime{10}, CommitSeq{2}, FactOperation::kPut, 1,
       Value::Int64(9)},
      {ref, ValidTime{15}, CommitSeq{3}, FactOperation::kPut, 1,
       std::nullopt},
  };

  const auto boundaries =
      internal::ResolveCorrectedBoundaries(events, CommitSeq{1});

  ASSERT_TRUE(boundaries.ok()) << boundaries.status().ToString();
  ASSERT_EQ(boundaries.ValueOrDie().size(), 1U);
  EXPECT_EQ(boundaries.ValueOrDie()[0].valid_from, ValidTime{5});
  EXPECT_EQ(boundaries.ValueOrDie()[0].commit_seq, CommitSeq{1});
  EXPECT_EQ(boundaries.ValueOrDie()[0].operation, FactOperation::kPut);
  EXPECT_EQ(boundaries.ValueOrDie()[0].value,
            std::optional<Value>(Value::Int64(7)));
}

TEST(TemporalModelTest, ClipsFiniteIntervalsToTheirIntersection) {
  EXPECT_EQ(internal::Clip({ValidTime{1}, ValidTime{8}},
                           {ValidTime{3}, ValidTime{6}}),
            (std::optional<ValidTimeInterval>(
                ValidTimeInterval{ValidTime{3}, ValidTime{6}})));
}

TEST(TemporalModelTest, ClipsUnboundedIntervalsWithoutSentinelArithmetic) {
  EXPECT_EQ(internal::Clip({ValidTime{1}, std::nullopt},
                           {ValidTime{3}, ValidTime{7}}),
            (std::optional<ValidTimeInterval>(
                ValidTimeInterval{ValidTime{3}, ValidTime{7}})));
}

TEST(TemporalModelTest, ClipRejectsEndpointTouchingIntervals) {
  EXPECT_FALSE(internal::Clip({ValidTime{1}, ValidTime{2}},
                              {ValidTime{2}, std::nullopt})
                   .has_value());
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
