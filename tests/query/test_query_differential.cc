#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <filesystem>
#include <random>
#include <sstream>
#include <string>
#include <tuple>
#include <unistd.h>
#include <vector>

#include "cedar/database.h"
#include "cedar/core/crc32c.h"
#include "cedar/query.h"
#include "cedar/transaction.h"
#include "cedar/storage_options.h"
#include "query/projection/projection_store.h"
#include "query/projection/query_delta.h"
#include "query/temporal/corrected_chain.h"
#include "query/runtime/graph_frontier.h"
#include "query/runtime/journey.h"
#include "storage/facts/fact_store.h"
#include "tests/model/bitemporal_fact_oracle.h"

namespace cedar {
namespace {

FactRef VertexRefFor(uint64_t id) {
  return EntityFact::Vertex(VertexRef{PartId{0}, VertexId{id}}).ref();
}

test::BitemporalFactOracle MakeHistory(uint32_t seed) {
  test::BitemporalFactOracle oracle;
  std::mt19937_64 random(seed);
  const FactRef ref = VertexRefFor(1 + seed % 8);
  for (uint64_t commit = 1; commit <= 12; ++commit) {
    const uint64_t valid = (commit % 4 == 0) ? 0 : random() % 32;
    const bool put = (random() & 1) != 0;
    oracle.Add({ref, ValidTime{valid}, CommitSeq{commit},
                put ? FactOperation::kPut : FactOperation::kDelete, 0,
                put ? std::optional<Value>(Value::Int64(
                         static_cast<int64_t>(random() % 100)))
                    : std::nullopt,
                std::nullopt});
    // Same-time corrections, empty/touching intervals, Missing values, and
    // schema epochs are deliberate: the greatest visible commit wins.
    if ((seed + commit) % 5 == 0) {
      oracle.Add({ref, ValidTime{valid}, CommitSeq{commit + 8},
                  FactOperation::kPut, static_cast<uint16_t>(commit % 3),
                  (commit & 1) ? std::optional<Value>(Value::Int64(7))
                               : std::nullopt,
                  std::nullopt});
    }
  }
  // Include self-loop, parallel, cross-partition, and cycle edges in every
  // serialized case.  These are consumed by the oracle path/journey suites
  // and make replay failures carry the complete graph shape.
  const VertexRef a{PartId{0}, VertexId{1 + seed % 4}};
  const VertexRef b{PartId{1}, VertexId{2 + seed % 4}};
  const VertexRef c{PartId{0}, VertexId{3 + seed % 4}};
  const std::array<EdgeIdentity, 4> edges = {
      EdgeIdentity{{PartId{0}, EdgeId{seed + 1}}, a, a, seed % 3},
      EdgeIdentity{{PartId{0}, EdgeId{seed + 2}}, a, b, seed % 3},
      EdgeIdentity{{PartId{1}, EdgeId{seed + 3}}, a, b, (seed + 1) % 3},
      EdgeIdentity{{PartId{0}, EdgeId{seed + 4}}, b, c, (seed + 2) % 3}};
  for (size_t index = 0; index < edges.size(); ++index) {
    const EdgeIdentity& edge = edges[index];
    const FactRef identity = FactRef(edge.home_part_id, FactFamily::kEdgeIdentity,
                                     PropertyId{}, edge.edge_id.value);
    const uint64_t from = index == 0 ? 5 : index * 3;
    const uint64_t to = index == 1 ? from : from + 7;
    oracle.Add({identity, ValidTime{0}, CommitSeq{20 + index},
                FactOperation::kPut, static_cast<uint16_t>(index % 2),
                std::nullopt, edge});
    oracle.Add({EntityFact::Edge(edge.edge_ref()).ref(), ValidTime{from},
                CommitSeq{20 + index}, FactOperation::kPut, 0, std::nullopt,
                std::nullopt});
    if (to != from) {
      oracle.Add({EntityFact::Edge(edge.edge_ref()).ref(), ValidTime{to},
                  CommitSeq{24 + index}, FactOperation::kDelete, 0,
                  std::nullopt, std::nullopt});
    }
  }
  return oracle;
}

Status CollectVertexRows(QueryCursor* cursor, const Slot<VertexRef>& slot,
                         std::vector<VertexRef>* rows) {
  for (;;) {
    auto batch = cursor->Next();
    if (!batch.ok()) return batch.status();
    if (!batch.ValueOrDie().has_value()) return Status::OK();
    const QueryBatch& value = *batch.ValueOrDie();
    for (size_t row = 0; row < value.row_count(); ++row) {
      rows->push_back(value.Get<VertexRef>(slot, row));
    }
  }
}

std::vector<VertexRef> VertexRowsFromOracle(const test::OracleRows& rows) {
  std::vector<VertexRef> result;
  for (const auto& row : rows) {
    if (row.ref.family() == FactFamily::kVertexState) {
      result.push_back(VertexRef{row.ref.part_id(), VertexId{row.ref.entity_id()}});
    }
  }
  std::sort(result.begin(), result.end(), [](const VertexRef& left,
                                             const VertexRef& right) {
    return std::tie(left.part_id.value, left.vertex_id.value) <
           std::tie(right.part_id.value, right.vertex_id.value);
  });
  return result;
}

TEST(QueryDifferentialTest, SmokeOracleMatchesDirectEnumeration) {
  const FactRef ref = VertexRefFor(1);
  test::BitemporalFactOracle oracle;
  oracle.Add({ref, ValidTime{0}, CommitSeq{1}, FactOperation::kPut, 0,
              std::nullopt, std::nullopt});
  oracle.Add({ref, ValidTime{4}, CommitSeq{2}, FactOperation::kDelete, 0,
              std::nullopt, std::nullopt});
  oracle.Add({ref, ValidTime{4}, CommitSeq{3}, FactOperation::kPut, 0,
              std::nullopt, std::nullopt});
  const auto rows = oracle.Evaluate({{ref}, std::nullopt}, CommitSeq{3});
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows.front().interval, (ValidTimeInterval{ValidTime{0}, std::nullopt}));
  EXPECT_FALSE(rows.front().value.has_value());
}

TEST(QueryDifferentialTest, ProjectionAndDeltaTopologiesMatchOracle) {
  char pattern[] = "/tmp/cedar_query_topology_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::filesystem::path root(pattern);
  const std::filesystem::path projection_path = root / "projections";
  const std::filesystem::path facts_path = root / "facts";

  const FactRef ref = PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{7}},
                                           PropertyId{1})
                           .ref();
  const auto event = [&](uint64_t commit, uint64_t valid,
                         FactOperation operation, int64_t value) {
    return FactEvent{ref,
                     ValidTime{valid},
                     CommitSeq{commit},
                     operation,
                     1,
                     operation == FactOperation::kPut
                         ? std::optional<Value>(Value::Int64(value))
                         : std::nullopt,
                     std::nullopt};
  };
  const std::vector<FactEvent> tail = {
      event(11, 5, FactOperation::kPut, 3),
      event(12, 15, FactOperation::kDelete, 0),
      event(13, 25, FactOperation::kPut, 4),
      event(14, 30, FactOperation::kDelete, 0),
      event(15, 35, FactOperation::kPut, 5),
      event(16, 40, FactOperation::kDelete, 0),
  };
  test::BitemporalFactOracle oracle;
  oracle.Add({ref, ValidTime{0}, CommitSeq{10}, FactOperation::kPut, 1,
              Value::Int64(1), std::nullopt});
  oracle.Add({ref, ValidTime{10}, CommitSeq{10}, FactOperation::kPut, 1,
              Value::Int64(2), std::nullopt});
  oracle.Add({ref, ValidTime{20}, CommitSeq{10}, FactOperation::kDelete, 1,
              std::nullopt, std::nullopt});
  for (const auto& fact : tail) oracle.Add(fact);

  auto store_result = internal::QueryProjectionStore::Open(
      internal::ProjectionStoreOptions{projection_path.string(), "topology-db", {}});
  ASSERT_TRUE(store_result.ok()) << store_result.status().ToString();
  auto projection = std::move(store_result).ConsumeValueOrDie();
  internal::ProjectionBuild build;
  build.manifest.database_identity = "topology-db";
  build.manifest.generation_id = 10;
  build.manifest.base_seq = CommitSeq{10};
  internal::CoverageRegion region;
  region.kind = internal::ProjectionKind::kState;
  region.part_id = PartId{0};
  region.property_id = PropertyId{1};
  region.schema_epoch = 1;
  region.entity_min = 7;
  region.entity_max_exclusive = 8;
  region.valid_time = {ValidTime{0}, std::nullopt};
  internal::SegmentDescriptor descriptor;
  descriptor.segment_id = "topology-base";
  descriptor.filename = "topology-base.csegment";
  descriptor.header.kind = internal::ProjectionKind::kState;
  descriptor.header.generation_id = 10;
  descriptor.header.base_seq = CommitSeq{10};
  descriptor.header.part_id = PartId{0};
  descriptor.header.property_id = PropertyId{1};
  descriptor.header.schema_epoch = 1;
  descriptor.header.entity_min = 7;
  descriptor.header.entity_max_exclusive = 8;
  descriptor.header.valid_from_min = ValidTime{0};
  internal::ProjectionChain chain;
  chain.header = descriptor.header;
  chain.intervals = {
      {{ValidTime{0}, ValidTime{10}}, Value::Int64(1), 7},
      {{ValidTime{10}, ValidTime{20}}, Value::Int64(2), 7},
  };
  chain.boundaries = {
      {ValidTime{0}, FactOperation::kPut, Value::Int64(1), 7},
      {ValidTime{10}, FactOperation::kPut, Value::Int64(2), 7},
      {ValidTime{20}, FactOperation::kDelete, Value::Int64(0), 7},
  };
  auto encoded = internal::EncodeProjectionPage(
      chain, internal::CompressionCodec::kNone);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  descriptor.file_bytes = encoded.ValueOrDie().size();
  descriptor.checksum = crc32c::Value(encoded.ValueOrDie().data(),
                                      encoded.ValueOrDie().size());
  region.segments.push_back(descriptor);
  build.manifest.regions.push_back(region);
  build.segments.push_back({descriptor, encoded.ValueOrDie()});
  ASSERT_TRUE(projection->Build(build).ok());

  internal::CoverageRequest covered;
  covered.part_id = PartId{0};
  covered.property_id = PropertyId{1};
  covered.schema_epoch = 1;
  covered.entity_min = 7;
  covered.entity_max_exclusive = 8;
  covered.valid_time = {ValidTime{0}, std::nullopt};
  covered.snapshot_seq = CommitSeq{10};
  covered.generation_id = 10;
  covered.expected_base_seq = CommitSeq{10};
  covered.database_identity = "topology-db";
  auto pin = projection->Acquire(covered);
  ASSERT_TRUE(pin.has_value());
  auto base = projection->ReadChains(covered, *pin);
  ASSERT_TRUE(base.ok()) << base.status().ToString();
  ASSERT_EQ(base.ValueOrDie().size(), 1U);
  ASSERT_EQ(base.ValueOrDie().front().boundaries.size(), 3U);
  EXPECT_EQ(base.ValueOrDie().front().intervals, chain.intervals);

  internal::CoverageRequest uncovered = covered;
  uncovered.entity_min = 8;
  uncovered.entity_max_exclusive = 9;
  EXPECT_FALSE(projection->Acquire(uncovered).has_value());
  EXPECT_TRUE(projection->ReadChains(uncovered).status().IsNotFound());
  EXPECT_TRUE(projection->projections_enabled());

  internal::QueryDelta delta({.base_seq = CommitSeq{10}, .queue_capacity = 64});
  for (const FactEvent& fact : tail) {
    internal::QueryDeltaCommit commit(fact.commit_seq);
    commit.facts.push_back(fact);
    const Status observed = delta.ObservePublished(commit);
    ASSERT_TRUE(observed.ok()) << observed.ToString();
  }
  EXPECT_EQ(delta.indexed_through(), CommitSeq{16});
  EXPECT_EQ(delta.first_missing(), CommitSeq{});
  EXPECT_TRUE(delta.mergeable());

  const auto merge = [&](const internal::QueryDeltaView& view) {
    std::vector<internal::CorrectedBoundary> boundaries;
    for (const auto& boundary : base.ValueOrDie().front().boundaries) {
      boundaries.push_back({boundary.time, CommitSeq{10}, boundary.operation,
                             0,
                             boundary.operation == FactOperation::kPut
                                 ? std::optional<Value>(boundary.value)
                                 : std::nullopt,
                             std::nullopt});
    }
    return internal::QueryDelta::MergeBoundaries(boundaries, view.facts,
                                                  view.through);
  };
  auto short_view = delta.AcquireThrough(CommitSeq{11});
  ASSERT_TRUE(short_view.ok()) << short_view.status().ToString();
  ASSERT_EQ(short_view.ValueOrDie().facts.size(), 1U);
  auto short_boundaries = merge(short_view.ValueOrDie());
  ASSERT_TRUE(short_boundaries.ok()) << short_boundaries.status().ToString();
  const auto short_intervals =
      internal::MaterializePresentState(short_boundaries.ValueOrDie());
  ASSERT_EQ(short_intervals.size(), 3U);
  EXPECT_EQ(short_intervals[0].interval,
            (ValidTimeInterval{ValidTime{0}, ValidTime{5}}));
  EXPECT_EQ(short_intervals[1].interval,
            (ValidTimeInterval{ValidTime{5}, ValidTime{10}}));
  EXPECT_EQ(short_intervals[2].interval,
            (ValidTimeInterval{ValidTime{10}, ValidTime{20}}));
  EXPECT_EQ(std::get<int64_t>(short_intervals[1].value->data()), 3);

  auto long_view = delta.AcquireThrough(CommitSeq{16});
  ASSERT_TRUE(long_view.ok()) << long_view.status().ToString();
  ASSERT_EQ(long_view.ValueOrDie().facts.size(), tail.size());
  for (size_t i = 0; i < tail.size(); ++i) {
    EXPECT_EQ(long_view.ValueOrDie().facts[i].ref, tail[i].ref);
    EXPECT_EQ(long_view.ValueOrDie().facts[i].valid_from, tail[i].valid_from);
    EXPECT_EQ(long_view.ValueOrDie().facts[i].commit_seq, tail[i].commit_seq);
    EXPECT_EQ(long_view.ValueOrDie().facts[i].operation, tail[i].operation);
    EXPECT_EQ(long_view.ValueOrDie().facts[i].value, tail[i].value);
  }
  auto long_boundaries = merge(long_view.ValueOrDie());
  ASSERT_TRUE(long_boundaries.ok()) << long_boundaries.status().ToString();
  const auto long_intervals =
      internal::MaterializePresentState(long_boundaries.ValueOrDie());
  ASSERT_EQ(long_intervals.size(), 5U);
  EXPECT_EQ(long_intervals[0].interval,
            (ValidTimeInterval{ValidTime{0}, ValidTime{5}}));
  EXPECT_EQ(long_intervals[1].interval,
            (ValidTimeInterval{ValidTime{5}, ValidTime{10}}));
  EXPECT_EQ(long_intervals[2].interval,
            (ValidTimeInterval{ValidTime{10}, ValidTime{15}}));
  EXPECT_EQ(long_intervals[3].interval,
            (ValidTimeInterval{ValidTime{25}, ValidTime{30}}));
  EXPECT_EQ(long_intervals[4].interval,
            (ValidTimeInterval{ValidTime{35}, ValidTime{40}}));
  EXPECT_EQ(std::get<int64_t>(long_intervals[3].value->data()), 4);
  EXPECT_EQ(std::get<int64_t>(long_intervals[4].value->data()), 5);
  const auto oracle_rows = oracle.Evaluate({{ref}, std::nullopt}, CommitSeq{16});
  ASSERT_EQ(oracle_rows.size(), long_intervals.size());
  for (size_t i = 0; i < oracle_rows.size(); ++i) {
    EXPECT_EQ(oracle_rows[i].interval, long_intervals[i].interval);
    EXPECT_EQ(oracle_rows[i].value, long_intervals[i].value);
  }

  FactStore facts(FactStoreOptions{facts_path.string()});
  ASSERT_TRUE(facts.Open().ok());
  for (uint64_t commit = 1; commit <= 10; ++commit) {
    const FactEvent fact = event(commit, commit, FactOperation::kPut, 1);
    ASSERT_TRUE(facts.Commit(StoreCommitBatch{
        TxnId{commit}, 100,
        {PendingFactMutation{fact.ref, fact.valid_from, fact.operation,
                              fact.schema_epoch, fact.value}},
        {}})
                    .ok());
  }
  for (const FactEvent& fact : tail) {
    const PendingFactMutation mutation{fact.ref, fact.valid_from, fact.operation,
                                       fact.schema_epoch, fact.value};
    ASSERT_TRUE(facts.Commit(StoreCommitBatch{
        TxnId{fact.commit_seq.value}, 100, {mutation}, {}})
                    .ok());
  }
  internal::QueryDelta rebuilt({.base_seq = CommitSeq{10}, .queue_capacity = 64});
  auto snapshot = facts.BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  ASSERT_TRUE(rebuilt.RepairThrough(facts, snapshot.ValueOrDie(), CommitSeq{16})
                  .ok());
  auto repaired = rebuilt.AcquireThrough(CommitSeq{16});
  ASSERT_TRUE(repaired.ok()) << repaired.status().ToString();
  ASSERT_EQ(repaired.ValueOrDie().facts.size(), tail.size());
  for (size_t i = 0; i < tail.size(); ++i) {
    EXPECT_EQ(repaired.ValueOrDie().facts[i].ref, tail[i].ref);
    EXPECT_EQ(repaired.ValueOrDie().facts[i].valid_from, tail[i].valid_from);
    EXPECT_EQ(repaired.ValueOrDie().facts[i].commit_seq, tail[i].commit_seq);
    EXPECT_EQ(repaired.ValueOrDie().facts[i].operation, tail[i].operation);
    EXPECT_EQ(repaired.ValueOrDie().facts[i].value, tail[i].value);
  }
  EXPECT_TRUE(rebuilt.mergeable());
  ASSERT_TRUE(rebuilt.ResetBase(CommitSeq{16}).ok());
  EXPECT_EQ(rebuilt.base_seq(), CommitSeq{16});
  EXPECT_TRUE(rebuilt.AcquireThrough(CommitSeq{16}).ValueOrDie().facts.empty());
  snapshot = StatusOr<StoreSnapshot>(Status::InvalidArgument("test", "release"));
  ASSERT_TRUE(facts.Close().ok());
  projection.reset();
  std::filesystem::remove_all(root);
}

TEST(QueryDifferentialTest, OracleExpandUsesHalfOpenEdgeStateIntervals) {
  test::BitemporalFactOracle oracle;
  const EdgeRef edge_ref{PartId{1}, EdgeId{17}};
  const EdgeIdentity edge{edge_ref, VertexRef{PartId{0}, VertexId{1}},
                          VertexRef{PartId{2}, VertexId{2}}, 9};
  oracle.Add({FactRef(edge_ref.home_part_id, FactFamily::kEdgeIdentity,
                     PropertyId{}, edge_ref.edge_id.value), ValidTime{0}, CommitSeq{1},
              FactOperation::kPut, 3, std::nullopt, edge});
  oracle.Add({EntityFact::Edge(edge_ref).ref(), ValidTime{0}, CommitSeq{1},
              FactOperation::kPut, 3, std::nullopt, std::nullopt});
  oracle.Add({EntityFact::Edge(edge_ref).ref(), ValidTime{5}, CommitSeq{2},
              FactOperation::kDelete, 3, std::nullopt, std::nullopt});
  const auto rows = oracle.Expand(
      {VertexRef{PartId{0}, VertexId{1}},
       ValidTimeInterval{ValidTime{2}, ValidTime{8}}, ExpandDirection::kOut,
       std::nullopt, 1},
      CommitSeq{2});
  ASSERT_EQ(rows.size(), 1U);
  EXPECT_EQ(rows.front().interval,
            (ValidTimeInterval{ValidTime{2}, ValidTime{5}}));
}

TEST(QueryDifferentialTest, OraclePathAndJourneyApplyIndependentTieObjectives) {
  test::BitemporalFactOracle oracle;
  const VertexRef a{PartId{0}, VertexId{1}};
  const VertexRef b{PartId{0}, VertexId{2}};
  const VertexRef c{PartId{0}, VertexId{3}};
  const EdgeIdentity ab{{PartId{0}, EdgeId{11}}, a, b, 0};
  const EdgeIdentity bc{{PartId{0}, EdgeId{12}}, b, c, 0};
  const EdgeIdentity ac{{PartId{0}, EdgeId{13}}, a, c, 0};
  for (const EdgeIdentity& edge : {ab, bc, ac}) {
    oracle.Add({FactRef(edge.home_part_id, FactFamily::kEdgeIdentity,
                        PropertyId{}, edge.edge_id.value), ValidTime{0},
                CommitSeq{1}, FactOperation::kPut, 0, std::nullopt, edge});
    oracle.Add({EntityFact::Edge(edge.edge_ref()).ref(), ValidTime{0},
                CommitSeq{1}, FactOperation::kPut, 0, std::nullopt,
                std::nullopt});
  }
  const PropertyId duration{7};
  oracle.Add({PropertyFact::Edge(ab.edge_ref(), duration).ref(), ValidTime{0},
              CommitSeq{1}, FactOperation::kPut, 0, Value::Int64(2),
              std::nullopt});
  oracle.Add({PropertyFact::Edge(bc.edge_ref(), duration).ref(), ValidTime{0},
              CommitSeq{1}, FactOperation::kPut, 0, Value::Int64(2),
              std::nullopt});
  oracle.Add({PropertyFact::Edge(ac.edge_ref(), duration).ref(), ValidTime{0},
              CommitSeq{1}, FactOperation::kPut, 0, Value::Int64(9),
              std::nullopt});
  const test::OraclePathSpec path{a, c, {ValidTime{0}, ValidTime{20}},
                                  ExpandDirection::kOut, std::nullopt, 3};
  const auto shortest = oracle.CoexistingShortestPath(path, CommitSeq{1});
  ASSERT_EQ(shortest.edges.size(), 1U);
  EXPECT_EQ(shortest.edges.front().edge_id.value, 13U);
  const test::OracleJourneySpec journey{a, c, {ValidTime{0}, ValidTime{20}},
                                        ExpandDirection::kOut, std::nullopt, 3,
                                        duration};
  const auto earliest = oracle.EarliestArrival(journey, CommitSeq{1});
  ASSERT_EQ(earliest.edges.size(), 2U);
  EXPECT_EQ(earliest.final_arrival.value, 4U);
  const auto fastest = oracle.FastestDuration(journey, CommitSeq{1});
  EXPECT_EQ(fastest.duration.value, 4U);
  const auto latest = oracle.LatestDeparture(journey, CommitSeq{1});
  EXPECT_EQ(latest.initial_departure.value, 16U);
}

TEST(QueryDifferentialTest, ProductionPathAndJourneyMatchIndependentOracle) {
  char pattern[] = "/tmp/cedar_query_differential_graph_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  auto opened = Database::Open(DatabaseOptions{.path = pattern});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  const VertexRef a{PartId{0}, VertexId{1}};
  const VertexRef b{PartId{0}, VertexId{2}};
  const VertexRef c{PartId{0}, VertexId{3}};
  const EdgeIdentity ab{{PartId{0}, EdgeId{101}}, a, b, 1};
  const EdgeIdentity bc{{PartId{0}, EdgeId{102}}, b, c, 1};
  const PropertyId duration{7};
  auto txn = database->BeginTransaction();
  ASSERT_TRUE(txn.ok());
  for (const VertexRef vertex : {a, b, c}) {
    ASSERT_TRUE(txn.ValueOrDie()->Assert(EntityFact::Vertex(vertex), ValidTime{0}).ok());
  }
  ASSERT_TRUE(txn.ValueOrDie()->Assert(ab, ValidTime{0}).ok());
  ASSERT_TRUE(txn.ValueOrDie()->Assert(bc, ValidTime{0}).ok());
  ASSERT_TRUE(txn.ValueOrDie()->Retract(EntityFact::Edge(ab.edge_ref()), ValidTime{20}).ok());
  ASSERT_TRUE(txn.ValueOrDie()->Retract(EntityFact::Edge(bc.edge_ref()), ValidTime{20}).ok());
  auto committed = txn.ValueOrDie()->Commit();
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  const CommitSeq cut = committed.ValueOrDie().commit_seq;

  test::BitemporalFactOracle oracle;
  for (const VertexRef vertex : {a, b, c}) {
    oracle.Add({EntityFact::Vertex(vertex).ref(), ValidTime{0}, cut,
                FactOperation::kPut, 0, std::nullopt, std::nullopt});
  }
  for (const EdgeIdentity& edge : {ab, bc}) {
    oracle.Add({FactRef(edge.home_part_id, FactFamily::kEdgeIdentity,
                        PropertyId{}, edge.edge_id.value), ValidTime{0}, cut,
                FactOperation::kPut, 0, std::nullopt, edge});
    oracle.Add({EntityFact::Edge(edge.edge_ref()).ref(), ValidTime{0}, cut,
                FactOperation::kPut, 0, std::nullopt, std::nullopt});
    oracle.Add({EntityFact::Edge(edge.edge_ref()).ref(), ValidTime{20}, cut,
                FactOperation::kDelete, 0, std::nullopt, std::nullopt});
  }
  auto snapshot = database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  const test::OraclePathSpec path_spec{
      a, c, {ValidTime{0}, ValidTime{20}}, ExpandDirection::kOut, std::nullopt, 2};
  const auto expected_path = oracle.CoexistingShortestPath(path_spec, cut);
  const auto expected_journey = oracle.EarliestArrival(
      {a, c, {ValidTime{0}, ValidTime{20}}, ExpandDirection::kOut, std::nullopt,
       2, std::nullopt},
      cut);
  const internal::GraphExpansionRequest request{
      {a}, {ValidTime{0}, ValidTime{20}}, ExpandDirection::kOut, std::nullopt};
  internal::GraphFrontierOptions graph_options;
  graph_options.max_hops = 2;
  auto actual_path = internal::CoexistingShortestPath(
      snapshot.ValueOrDie(), request, c, graph_options);
  ASSERT_TRUE(actual_path.ok()) << actual_path.status().ToString();
  ASSERT_FALSE(actual_path.ValueOrDie().paths.empty());
  EXPECT_EQ(actual_path.ValueOrDie().paths.front(), expected_path);
  internal::JourneyRequest journey_request{
      a, c, {ValidTime{0}, ValidTime{20}},
      internal::JourneyObjective::kEarliestArrival, std::nullopt,
      [](EdgeRef edge, ValidTime) {
                                    return StatusOr<std::optional<ValidDuration>>(
                                        std::optional<ValidDuration>{ValidDuration{1}});
                                  }, std::nullopt, 2, ExpandDirection::kOut,
                                  std::nullopt};
  auto actual_journey = internal::EarliestArrival(snapshot.ValueOrDie(),
                                                  journey_request);
  ASSERT_TRUE(actual_journey.ok()) << actual_journey.status().ToString();
  EXPECT_EQ(actual_journey.ValueOrDie(), expected_journey);
  // Release the pinned snapshot before exercising Database::Close.  This
  // makes the cleanup assertion observe the real pin/lifetime contract.
  snapshot = StatusOr<Snapshot>(Status::InvalidArgument("test", "release"));
  EXPECT_TRUE(database->Close().ok());
  std::filesystem::remove_all(pattern);
}

TEST(QueryDifferentialTest, BoundedRandomizedProductionPathJourneyMatchOracle) {
  for (uint32_t seed = 0; seed < 8; ++seed) {
    char pattern[] = "/tmp/cedar_query_bounded_graph_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    const VertexRef source{PartId{0}, VertexId{1}};
    const VertexRef target{PartId{0}, VertexId{2 + seed}};
    const EdgeIdentity edge{{PartId{0}, EdgeId{1000 + seed}}, source, target,
                             seed % 3 + 1};
    auto opened = Database::Open(DatabaseOptions{.path = pattern});
    ASSERT_TRUE(opened.ok()) << opened.status().ToString();
    auto database = std::move(opened).ConsumeValueOrDie();
    auto tx = database->BeginTransaction();
    ASSERT_TRUE(tx.ok());
    ASSERT_TRUE(tx.ValueOrDie()->Assert(EntityFact::Vertex(source), ValidTime{0}).ok());
    ASSERT_TRUE(tx.ValueOrDie()->Assert(EntityFact::Vertex(target), ValidTime{0}).ok());
    const Status edge_status = tx.ValueOrDie()->Assert(edge, ValidTime{0});
    ASSERT_TRUE(edge_status.ok()) << edge_status.ToString() << " seed=" << seed;
    ASSERT_TRUE(tx.ValueOrDie()->Retract(EntityFact::Edge(edge.edge_ref()),
                                         ValidTime{50}).ok());
    auto committed = tx.ValueOrDie()->Commit();
    ASSERT_TRUE(committed.ok()) << committed.status().ToString();
    const CommitSeq cut = committed.ValueOrDie().commit_seq;
    test::BitemporalFactOracle oracle;
    oracle.Add({EntityFact::Vertex(source).ref(), ValidTime{0}, cut,
                FactOperation::kPut, 0, std::nullopt, std::nullopt});
    oracle.Add({EntityFact::Vertex(target).ref(), ValidTime{0}, cut,
                FactOperation::kPut, 0, std::nullopt, std::nullopt});
    oracle.Add({FactRef(edge.home_part_id, FactFamily::kEdgeIdentity,
                        PropertyId{}, edge.edge_id.value), ValidTime{0}, cut,
                FactOperation::kPut, 0, std::nullopt, edge});
    oracle.Add({EntityFact::Edge(edge.edge_ref()).ref(), ValidTime{0}, cut,
                FactOperation::kPut, 0, std::nullopt, std::nullopt});
    oracle.Add({EntityFact::Edge(edge.edge_ref()).ref(), ValidTime{50}, cut,
                FactOperation::kDelete, 0, std::nullopt, std::nullopt});
    auto snapshot = database->BeginSnapshot();
    ASSERT_TRUE(snapshot.ok());
    const test::OraclePathSpec path_spec{
        source, target, {ValidTime{0}, ValidTime{50}}, ExpandDirection::kOut,
        std::nullopt, 2};
    const auto expected_path = oracle.CoexistingShortestPath(path_spec, cut);
    auto actual_path = internal::CoexistingShortestPath(
        snapshot.ValueOrDie(), {{source}, {ValidTime{0}, ValidTime{50}},
                                ExpandDirection::kOut, std::nullopt},
        target, internal::GraphFrontierOptions{.max_hops = 2});
    ASSERT_TRUE(actual_path.ok()) << actual_path.status().ToString();
    ASSERT_FALSE(actual_path.ValueOrDie().paths.empty());
    EXPECT_EQ(actual_path.ValueOrDie().paths.front(), expected_path);
    internal::JourneyRequest journey_request{
        source, target, {ValidTime{0}, ValidTime{50}},
        internal::JourneyObjective::kEarliestArrival, std::nullopt,
        [](EdgeRef, ValidTime) {
          return StatusOr<std::optional<ValidDuration>>(
              std::optional<ValidDuration>{ValidDuration{1}});
        },
        std::nullopt, 2, ExpandDirection::kOut, std::nullopt};
    auto actual_journey = internal::EarliestArrival(snapshot.ValueOrDie(),
                                                    journey_request);
    ASSERT_TRUE(actual_journey.ok()) << actual_journey.status().ToString();
    EXPECT_EQ(actual_journey.ValueOrDie(), oracle.EarliestArrival(
        {source, target, {ValidTime{0}, ValidTime{50}}, ExpandDirection::kOut,
         std::nullopt, 2, std::nullopt}, cut));
    snapshot = StatusOr<Snapshot>(Status::InvalidArgument("test", "release"));
    ASSERT_TRUE(database->Close().ok());
    std::filesystem::remove_all(pattern);
  }
}

TEST(QueryDifferentialTest, OracleReplaySerializationIsStable) {
  const auto first = MakeHistory(43);
  const auto second = MakeHistory(43);
  EXPECT_EQ(first.Serialize(), second.Serialize());
  EXPECT_FALSE(first.Serialize().empty());
}

TEST(QueryDifferentialTest, SmokeDebugThresholdsAreExactAndCapacityOnly) {
  EXPECT_TRUE(UsesCedarKernelProfile(StorageProfile::kDebugSmallThresholds));
}

TEST(QueryDifferentialTest, DebugProfileExercisesRealFlushQueryAndVacuumLifecycle) {
  char pattern[] = "/tmp/cedar_query_debug_lifecycle_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  DatabaseOptions options;
  options.path = pattern;
  options.storage_profile = StorageProfile::kDebugSmallThresholds;
  auto opened = Database::Open(std::move(options));
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  for (uint64_t id = 1; id <= 256; ++id) {
    auto transaction = database->BeginTransaction();
    ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
    ASSERT_TRUE(transaction.ValueOrDie()
                    ->Assert(EntityFact::Vertex(
                                 VertexRef{PartId{0}, VertexId{id}}),
                             ValidTime{0})
                    .ok());
    ASSERT_TRUE(transaction.ValueOrDie()->Commit().ok());
  }
  const CommitSeq vacuum_seq = [&]() {
    auto snapshot = database->BeginSnapshot();
    EXPECT_TRUE(snapshot.ok());
    EXPECT_GE(snapshot.ValueOrDie().commit_seq().value, 256U);
    return snapshot.ValueOrDie().commit_seq();
  }();
  EXPECT_TRUE(database->Vacuum(vacuum_seq).ok());
  EXPECT_TRUE(database->Close().ok());
  std::filesystem::remove_all(pattern);
}

TEST(QueryDifferentialTest, SmokeCanonicalRowsEqualIndependentOracle) {
  char pattern[] = "/tmp/cedar_query_differential_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  DatabaseOptions options;
  options.path = path;
  auto opened = Database::Open(std::move(options));
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  const VertexRef vertex_ref{PartId{0}, VertexId{1}};
  auto transaction = database->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()->Assert(EntityFact::Vertex(vertex_ref),
                                                ValidTime{0}).ok());
  auto committed = transaction.ValueOrDie()->Commit();
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  test::BitemporalFactOracle oracle;
  oracle.Add({EntityFact::Vertex(vertex_ref).ref(), ValidTime{0},
              committed.ValueOrDie().commit_seq, FactOperation::kPut, 0,
              std::nullopt, std::nullopt});
  Slot<VertexRef> vertex = Slot<VertexRef>::Named("vertex");
  auto query = Query::Vertices(vertex, At{ValidTime{1}});
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  auto projected = query.ValueOrDie().Select({Project(vertex)});
  ASSERT_TRUE(projected.ok()) << projected.status().ToString();
  auto prepared = database->PrepareQuery(projected.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto snapshot = database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  const CommitSeq cut = snapshot.ValueOrDie().commit_seq();
  const auto expected = oracle.Evaluate(
      {{EntityFact::Vertex(vertex_ref).ref()}}, cut);
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  std::vector<VertexRef> actual;
  ASSERT_TRUE(CollectVertexRows(&cursor.ValueOrDie(), vertex, &actual).ok());
  EXPECT_EQ(actual, VertexRowsFromOracle(expected));
  EXPECT_TRUE(cursor.ValueOrDie().terminal_info().complete);
  EXPECT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(QueryDifferentialTest, CanonicalMatchesIndependentOracleAcrossExecutionLanes) {
  char pattern[] = "/tmp/cedar_query_differential_lanes_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  DatabaseOptions options;
  options.path = path;
  auto opened = Database::Open(std::move(options));
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  auto database = std::move(opened).ConsumeValueOrDie();
  const VertexRef first{PartId{0}, VertexId{1}};
  const VertexRef second{PartId{0}, VertexId{2}};
  auto txn = database->BeginTransaction();
  ASSERT_TRUE(txn.ok());
  ASSERT_TRUE(txn.ValueOrDie()->Assert(EntityFact::Vertex(first), ValidTime{0}).ok());
  ASSERT_TRUE(txn.ValueOrDie()->Assert(EntityFact::Vertex(second), ValidTime{0}).ok());
  auto committed = txn.ValueOrDie()->Commit();
  ASSERT_TRUE(committed.ok());
  test::BitemporalFactOracle oracle;
  oracle.Add({EntityFact::Vertex(first).ref(), ValidTime{0},
              committed.ValueOrDie().commit_seq, FactOperation::kPut, 0,
              std::nullopt, std::nullopt});
  oracle.Add({EntityFact::Vertex(second).ref(), ValidTime{0},
              committed.ValueOrDie().commit_seq, FactOperation::kPut, 0,
              std::nullopt, std::nullopt});
  Slot<VertexRef> vertex = Slot<VertexRef>::Named("lane_vertex");
  auto query = Query::Vertices(vertex, At{ValidTime{1}});
  ASSERT_TRUE(query.ok());
  auto selected = query.ValueOrDie().Select({Project(vertex)});
  ASSERT_TRUE(selected.ok());
  auto prepared = database->PrepareQuery(selected.ValueOrDie());
  ASSERT_TRUE(prepared.ok());
  const CommitSeq cut = [&]() {
    auto cut_snapshot = database->BeginSnapshot();
    EXPECT_TRUE(cut_snapshot.ok());
    return cut_snapshot.ValueOrDie().commit_seq();
  }();
  const auto expected = oracle.Evaluate(
      {{EntityFact::Vertex(first).ref(), EntityFact::Vertex(second).ref()}},
      cut);
  for (QueryExecutionMode mode : {QueryExecutionMode::kInteractive,
                                  QueryExecutionMode::kAnalytical,
                                  QueryExecutionMode::kAuto}) {
    QueryOptions query_options;
    query_options.mode = mode;
    query_options.capture_profile = true;
    auto lane_snapshot = database->BeginSnapshot();
    ASSERT_TRUE(lane_snapshot.ok());
    auto cursor = prepared.ValueOrDie().Execute(
        std::move(lane_snapshot).ConsumeValueOrDie(), Bindings{}, query_options);
    ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
    std::vector<VertexRef> actual;
    ASSERT_TRUE(CollectVertexRows(&cursor.ValueOrDie(), vertex, &actual).ok());
    EXPECT_EQ(actual, VertexRowsFromOracle(expected))
        << "mode=" << static_cast<int>(mode);
    EXPECT_TRUE(cursor.ValueOrDie().terminal_info().complete)
        << "mode=" << static_cast<int>(mode);
  }
  EXPECT_TRUE(database->Close().ok());
  std::filesystem::remove_all(path);
}

TEST(QueryDifferentialTest, FaultBudgetsAndCancellationOnlyEmitSnapshotPrefixes) {
  char pattern[] = "/tmp/cedar_query_differential_faults_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  auto opened = Database::Open(DatabaseOptions{.path = pattern});
  ASSERT_TRUE(opened.ok());
  auto database = std::move(opened).ConsumeValueOrDie();
  auto transaction = database->BeginTransaction();
  ASSERT_TRUE(transaction.ok());
  for (uint64_t id = 1; id <= 8; ++id) {
    ASSERT_TRUE(transaction.ValueOrDie()
                    ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{id}}),
                             ValidTime{0})
                    .ok());
  }
  ASSERT_TRUE(transaction.ValueOrDie()->Commit().ok());
  Slot<VertexRef> vertex = Slot<VertexRef>::Named("fault_vertex");
  auto query = Query::Vertices(vertex, At{ValidTime{1}});
  ASSERT_TRUE(query.ok());
  auto selected = query.ValueOrDie().Select({Project(vertex)});
  ASSERT_TRUE(selected.ok());
  auto prepared = database->PrepareQuery(selected.ValueOrDie());
  ASSERT_TRUE(prepared.ok());
  const auto expected = std::vector<VertexRef>{
      VertexRef{PartId{0}, VertexId{1}}, VertexRef{PartId{0}, VertexId{2}},
      VertexRef{PartId{0}, VertexId{3}}, VertexRef{PartId{0}, VertexId{4}},
      VertexRef{PartId{0}, VertexId{5}}, VertexRef{PartId{0}, VertexId{6}},
      VertexRef{PartId{0}, VertexId{7}}, VertexRef{PartId{0}, VertexId{8}}};
  for (const QueryOptions& options : {
           QueryOptions{.mode = QueryExecutionMode::kInteractive,
                        .budget = QueryBudget{.output_rows = 1}},
           QueryOptions{.mode = QueryExecutionMode::kInteractive,
                        .budget = QueryBudget{.cpu_us = 1}}}) {
    auto fault_snapshot = database->BeginSnapshot();
    ASSERT_TRUE(fault_snapshot.ok());
    auto cursor = prepared.ValueOrDie().Execute(
        std::move(fault_snapshot).ConsumeValueOrDie(), Bindings{}, options);
    ASSERT_TRUE(cursor.ok());
    std::vector<VertexRef> emitted;
    const Status run_status = CollectVertexRows(&cursor.ValueOrDie(), vertex,
                                                 &emitted);
    ASSERT_TRUE(run_status.ok() || run_status.IsResourceExhausted() ||
                run_status.IsDeadlineExceeded())
        << run_status.ToString();
    ASSERT_LE(emitted.size(), expected.size());
    EXPECT_TRUE(std::equal(emitted.begin(), emitted.end(), expected.begin()));
    if (options.budget.output_rows != 0) {
      EXPECT_LE(emitted.size(), options.budget.output_rows);
    }
    EXPECT_FALSE(cursor.ValueOrDie().terminal_info().complete);
  }
  auto cancel_snapshot = database->BeginSnapshot();
  ASSERT_TRUE(cancel_snapshot.ok());
  auto cancelled = prepared.ValueOrDie().Execute(
      std::move(cancel_snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  ASSERT_TRUE(cancelled.ok());
  EXPECT_TRUE(cancelled.ValueOrDie().Cancel().ok());
  std::vector<VertexRef> cancelled_rows;
  const Status cancel_status = CollectVertexRows(&cancelled.ValueOrDie(), vertex,
                                                 &cancelled_rows);
  EXPECT_TRUE(cancel_status.IsQueryCancelled());
  EXPECT_TRUE(cancelled_rows.empty());
  EXPECT_FALSE(cancelled.ValueOrDie().terminal_info().complete);
  EXPECT_TRUE(database->Close().ok());
  std::filesystem::remove_all(pattern);
}

TEST(QueryDifferentialTest, FullRandomHistorySeedsRemainReplayable) {
  for (uint32_t seed = 0; seed < 5000; ++seed) {
    const auto oracle = MakeHistory(seed);
    const auto rows = oracle.Evaluate(
        {{VertexRefFor(1 + seed % 8)}}, CommitSeq{16});
    for (const auto& row : rows) {
      ASSERT_TRUE(!row.interval.to ||
                  row.interval.from.value < row.interval.to->value)
          << "seed=" << seed << " replay=" << oracle.Serialize();
    }
    for (size_t index = 1; index < rows.size(); ++index) {
      if (rows[index - 1].ref == rows[index].ref &&
          rows[index - 1].interval.to &&
          rows[index].interval.from.value <
              rows[index - 1].interval.to->value) {
        FAIL() << "overlapping oracle intervals seed=" << seed
               << " replay=" << oracle.Serialize();
      }
    }
  }
}

TEST(QueryDifferentialTest, PathSeedsAreDeterministic) {
  for (uint32_t seed = 10000; seed < 11000; ++seed) {
    test::BitemporalFactOracle oracle;
    const VertexRef source{PartId{0}, VertexId{1}};
    const VertexRef target{PartId{0}, VertexId{2 + seed % 6}};
    const EdgeIdentity edge{EdgeRef{PartId{0}, EdgeId{seed}}, source, target, seed % 3};
    const FactRef ref = EntityFact::Edge(edge.edge_ref()).ref();
    oracle.Add({ref, ValidTime{0}, CommitSeq{1}, FactOperation::kPut, 0,
                std::nullopt, edge});
    const auto path = oracle.CoexistingShortestPath(
        {source, target, {ValidTime{0}, std::nullopt}, ExpandDirection::kOut,
         std::nullopt, 2}, CommitSeq{1});
    ASSERT_TRUE(path.vertices.empty() || path.vertices.front() == source)
        << "seed=" << seed;
  }
}

TEST(QueryDifferentialTest, JourneySeedsAreDeterministic) {
  for (uint32_t seed = 20000; seed < 21000; ++seed) {
    test::BitemporalFactOracle oracle;
    const VertexRef source{PartId{0}, VertexId{1}};
    const VertexRef target{PartId{0}, VertexId{2}};
    const EdgeIdentity edge{EdgeRef{PartId{0}, EdgeId{seed}}, source, target, 0};
    oracle.Add({EntityFact::Edge(edge.edge_ref()).ref(), ValidTime{0},
                CommitSeq{1}, FactOperation::kPut, 0, std::nullopt, edge});
    const auto journey = oracle.EarliestArrival(
        {source, target, {ValidTime{0}, ValidTime{100}}, ExpandDirection::kOut,
         std::nullopt, 2, std::nullopt}, CommitSeq{1});
    ASSERT_TRUE(journey.vertices.empty() || journey.final_arrival.value >= 1)
        << "seed=" << seed;
  }
}

TEST(QueryDifferentialTest, ProjectionAndDeltaTopologiesRemainSnapshotCorrect) {
  char pattern[] = "/tmp/cedar_query_topology_components_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::filesystem::path root(pattern);
  const std::filesystem::path projection_path = root / "projections";

  const FactRef ref = PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}},
                                           PropertyId{1}).ref();
  test::BitemporalFactOracle oracle;
  oracle.Add({ref, ValidTime{0}, CommitSeq{1}, FactOperation::kPut, 1,
              Value::Int64(10), std::nullopt});
  oracle.Add({ref, ValidTime{5}, CommitSeq{2}, FactOperation::kPut, 1,
              Value::Int64(20), std::nullopt});
  oracle.Add({ref, ValidTime{10}, CommitSeq{3}, FactOperation::kDelete, 1,
              std::nullopt, std::nullopt});

  internal::ProjectionBuild build;
  build.manifest.database_identity = root.string();
  build.manifest.generation_id = 1;
  build.manifest.base_seq = CommitSeq{1};
  internal::CoverageRegion region;
  region.kind = internal::ProjectionKind::kState;
  region.part_id = PartId{0};
  region.schema_epoch = 1;
  region.entity_min = 1;
  region.entity_max_exclusive = 3;
  region.valid_time = {ValidTime{0}, std::nullopt};
  internal::SegmentDescriptor descriptor;
  descriptor.segment_id = "topology-state";
  descriptor.filename = "topology-state.csegment";
  descriptor.header.kind = internal::ProjectionKind::kState;
  descriptor.header.generation_id = 1;
  descriptor.header.base_seq = CommitSeq{1};
  descriptor.header.part_id = PartId{0};
  descriptor.header.schema_epoch = 1;
  descriptor.header.entity_min = 1;
  descriptor.header.entity_max_exclusive = 3;
  descriptor.header.valid_from_min = ValidTime{0};
  internal::ProjectionChain chain;
  chain.header = descriptor.header;
  chain.intervals.push_back(
      {{ValidTime{0}, std::nullopt}, Value::Int64(10), 1});
  auto encoded = internal::EncodeProjectionPage(chain, internal::CompressionCodec::kNone);
  ASSERT_TRUE(encoded.ok()) << encoded.status().ToString();
  descriptor.file_bytes = encoded.ValueOrDie().size();
  descriptor.checksum = crc32c::Value(encoded.ValueOrDie().data(),
                                      encoded.ValueOrDie().size());
  region.segments.push_back(descriptor);
  build.manifest.regions.push_back(region);
  build.segments.push_back({descriptor, encoded.ValueOrDie()});

  auto store = internal::QueryProjectionStore::Open(
      internal::ProjectionStoreOptions{projection_path.string(), root.string(), {}});
  ASSERT_TRUE(store.ok()) << store.status().ToString();
  ASSERT_TRUE(store.ValueOrDie()->Build(build).ok());
  internal::CoverageRequest covered;
  covered.kind = internal::ProjectionKind::kState;
  covered.part_id = PartId{0};
  covered.schema_epoch = 1;
  covered.entity_min = 1;
  covered.entity_max_exclusive = 2;
  covered.valid_time = {ValidTime{0}, std::nullopt};
  covered.snapshot_seq = CommitSeq{1};
  covered.database_identity = root.string();
  auto base = store.ValueOrDie()->ReadChains(covered);
  ASSERT_TRUE(base.ok()) << base.status().ToString();
  ASSERT_EQ(base.ValueOrDie().size(), 1U);
  EXPECT_EQ(base.ValueOrDie().front().intervals.front().value,
            Value::Int64(10));
  const auto base_rows = oracle.Evaluate({{ref}, std::nullopt}, CommitSeq{1});
  ASSERT_EQ(base_rows.size(), base.ValueOrDie().front().intervals.size());
  for (size_t i = 0; i < base_rows.size(); ++i) {
    EXPECT_EQ(base_rows[i].ref, ref);
    EXPECT_EQ(base_rows[i].interval,
              base.ValueOrDie().front().intervals[i].effective);
    EXPECT_EQ(base_rows[i].value,
              base.ValueOrDie().front().intervals[i].value);
  }
  covered.entity_min = 3;
  covered.entity_max_exclusive = 4;
  EXPECT_TRUE(store.ValueOrDie()->ReadChains(covered).status().IsNotFound());

  internal::QueryDelta delta({.base_seq = CommitSeq{1}, .queue_capacity = 8,
                    .soft_memory_bytes = 1ULL << 20,
                    .hard_memory_bytes = 2ULL << 20,
                    .max_lag_commits = 8});
  internal::QueryDeltaCommit short_commit{CommitSeq{2}};
  short_commit.facts.push_back(
      {ref, ValidTime{5}, CommitSeq{2}, FactOperation::kPut, 1,
       Value::Int64(20), std::nullopt});
  const Status short_status = delta.ObservePublished(short_commit);
  ASSERT_TRUE(short_status.ok()) << short_status.ToString();
  auto short_view = delta.AcquireThrough(CommitSeq{2});
  ASSERT_TRUE(short_view.ok());
  auto short_merged = internal::QueryDelta::MergeBoundaries(
      {{ValidTime{0}, CommitSeq{1}, FactOperation::kPut, 0,
        Value::Int64(10), std::nullopt}},
      short_view.ValueOrDie().facts, CommitSeq{2});
  ASSERT_TRUE(short_merged.ok());
  auto short_intervals = MaterializePresentState(short_merged.ValueOrDie());
  const auto short_rows = oracle.Evaluate({{ref}, std::nullopt}, CommitSeq{2});
  ASSERT_EQ(short_rows.size(), short_intervals.size());
  ASSERT_EQ(short_intervals.size(), 2U);
  EXPECT_EQ(short_intervals[0].interval,
            (ValidTimeInterval{ValidTime{0}, ValidTime{5}}));
  EXPECT_EQ(short_intervals[1].value, Value::Int64(20));
  for (size_t i = 0; i < short_rows.size(); ++i) {
    EXPECT_EQ(short_rows[i].interval, short_intervals[i].interval);
    EXPECT_EQ(short_rows[i].value, short_intervals[i].value);
  }

  internal::QueryDeltaCommit long_commit{CommitSeq{3}};
  long_commit.facts.push_back(
      {ref, ValidTime{10}, CommitSeq{3}, FactOperation::kDelete, 1,
       std::nullopt, std::nullopt});
  ASSERT_TRUE(delta.ObservePublished(long_commit).ok());
  auto long_view = delta.AcquireThrough(CommitSeq{3});
  ASSERT_TRUE(long_view.ok());
  auto long_merged = internal::QueryDelta::MergeBoundaries(
      {{ValidTime{0}, CommitSeq{1}, FactOperation::kPut, 0,
        Value::Int64(10), std::nullopt}},
      long_view.ValueOrDie().facts, CommitSeq{3});
  ASSERT_TRUE(long_merged.ok());
  auto long_intervals = MaterializePresentState(long_merged.ValueOrDie());
  const auto long_rows = oracle.Evaluate({{ref}, std::nullopt}, CommitSeq{3});
  ASSERT_EQ(long_rows.size(), long_intervals.size());
  ASSERT_EQ(long_intervals.size(), 2U);
  EXPECT_EQ(long_intervals[0].value, Value::Int64(10));
  EXPECT_EQ(long_intervals[1].value, Value::Int64(20));
  EXPECT_EQ(long_intervals[1].interval,
            (ValidTimeInterval{ValidTime{5}, ValidTime{10}}));
  for (size_t i = 0; i < long_rows.size(); ++i) {
    EXPECT_EQ(long_rows[i].interval, long_intervals[i].interval);
    EXPECT_EQ(long_rows[i].value, long_intervals[i].value);
  }
  ASSERT_TRUE(delta.RetireThrough(CommitSeq{3}).ok());
  ASSERT_TRUE(delta.ResetBase(CommitSeq{3}).ok());
  EXPECT_EQ(delta.base_seq(), CommitSeq{3});
  EXPECT_TRUE(delta.mergeable());

  store.ValueOrDie().reset();
  std::filesystem::remove_all(root);
}

}  // namespace
}  // namespace cedar
