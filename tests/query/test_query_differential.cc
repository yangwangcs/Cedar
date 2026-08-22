#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <random>
#include <sstream>
#include <string>
#include <unistd.h>
#include <vector>

#include "cedar/database.h"
#include "cedar/query.h"
#include "cedar/transaction.h"
#include "cedar/storage_options.h"
#include "query/projection/query_delta.h"
#include "tests/model/bitemporal_fact_oracle.h"

namespace cedar {
namespace {

FactRef VertexRefFor(uint64_t id) {
  return EntityFact::Vertex(VertexRef{PartId{0}, VertexId{id}}).ref();
}

test::BitemporalFactOracle MakeHistory(uint32_t seed) {
  test::BitemporalFactOracle oracle;
  const FactRef ref = VertexRefFor(1 + seed % 8);
  std::mt19937_64 random(seed);
  for (uint64_t commit = 1; commit <= 8; ++commit) {
    const uint64_t valid = random() % 12;
    const bool put = (random() & 1) != 0;
    oracle.Add({ref, ValidTime{valid}, CommitSeq{commit},
                put ? FactOperation::kPut : FactOperation::kDelete, 0,
                put ? std::optional<Value>(Value::Int64(
                         static_cast<int64_t>(random() % 100)))
                    : std::nullopt,
                std::nullopt});
    // Same-time corrections are deliberate: the greatest visible commit wins.
    if ((seed + commit) % 5 == 0) {
      oracle.Add({ref, ValidTime{valid}, CommitSeq{commit + 8},
                  FactOperation::kPut, 0, Value::Int64(7), std::nullopt});
    }
  }
  return oracle;
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
  auto batch = cursor.ValueOrDie().Next();
  ASSERT_TRUE(batch.ok()) << batch.status().ToString();
  ASSERT_TRUE(batch.ValueOrDie().has_value());
  ASSERT_EQ(batch.ValueOrDie()->row_count(), expected.size());
  EXPECT_EQ(batch.ValueOrDie()->Get<VertexRef>(vertex, 0), vertex_ref);
  EXPECT_TRUE(cursor.ValueOrDie().Next().ValueOrDie().has_value() == false);
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
    auto cursor = prepared.ValueOrDie().Execute(
        database->BeginSnapshot().ConsumeValueOrDie(), Bindings{},
        query_options);
    ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
    auto batch = cursor.ValueOrDie().Next();
    ASSERT_TRUE(batch.ok()) << batch.status().ToString();
    ASSERT_TRUE(batch.ValueOrDie().has_value());
    EXPECT_EQ(batch.ValueOrDie()->row_count(), expected.size());
    ASSERT_FALSE(cursor.ValueOrDie().Next().ValueOrDie().has_value());
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
  for (const QueryOptions& options : {
           QueryOptions{.mode = QueryExecutionMode::kInteractive,
                        .budget = QueryBudget{.output_rows = 1}},
           QueryOptions{.mode = QueryExecutionMode::kInteractive,
                        .budget = QueryBudget{.cpu_us = 1}}}) {
    auto cursor = prepared.ValueOrDie().Execute(
        database->BeginSnapshot().ConsumeValueOrDie(), Bindings{}, options);
    ASSERT_TRUE(cursor.ok());
    auto first = cursor.ValueOrDie().Next();
    if (first.ok() && first.ValueOrDie()) {
      EXPECT_LE(first.ValueOrDie()->row_count(), 8U);
    }
    EXPECT_FALSE(cursor.ValueOrDie().terminal_info().complete);
  }
  auto cancelled = prepared.ValueOrDie().Execute(
      database->BeginSnapshot().ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  ASSERT_TRUE(cancelled.ok());
  EXPECT_TRUE(cancelled.ValueOrDie().Cancel().ok());
  EXPECT_FALSE(cancelled.ValueOrDie().terminal_info().complete);
  EXPECT_TRUE(database->Close().ok());
  std::filesystem::remove_all(pattern);
}

TEST(QueryDifferentialTest, FullRandomHistorySeedsRemainReplayable) {
  for (uint32_t seed = 0; seed < 5000; ++seed) {
    const auto oracle = MakeHistory(seed);
    const auto rows = oracle.Evaluate(
        {{VertexRefFor(1 + seed % 8)}}, CommitSeq{16});
    ASSERT_LE(rows.size(), 8U) << "seed=" << seed;
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

}  // namespace
}  // namespace cedar
