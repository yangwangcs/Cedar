#include <gtest/gtest.h>

#include <filesystem>
#include <functional>
#include <memory>
#include <cstdlib>

#include "cedar/database.h"
#include "cedar/query/query.h"
#include "cedar/query/result.h"
#include "cedar/transaction.h"
#include "query/runtime/graph_frontier.h"

namespace cedar::internal {
namespace {
struct GraphFixture {
  std::string path;
  std::unique_ptr<Database> database;
  VertexRef a{PartId{0}, VertexId{1}};
  VertexRef b{PartId{0}, VertexId{2}};
  VertexRef c{PartId{0}, VertexId{3}};
  GraphFixture() {
    char pattern[] = "/tmp/cedar_coexisting_path_XXXXXX";
    if (mkdtemp(pattern) == nullptr) std::abort();
    path = pattern;
    auto opened = Database::Open(DatabaseOptions{.path = path});
    if (!opened.ok()) std::abort();
    database = std::move(opened).ConsumeValueOrDie();
  }
  ~GraphFixture() {
    if (database) database->Close().IgnoreError();
    std::filesystem::remove_all(path);
  }
  void Commit(const std::function<Status(Transaction&)>& stage) {
    auto txn = database->BeginTransaction();
    ASSERT_TRUE(txn.ok()) << txn.status().ToString();
    ASSERT_TRUE(stage(*txn.ValueOrDie()).ok());
    ASSERT_TRUE(txn.ValueOrDie()->Commit().ok());
  }
  void Vertex(const VertexRef& v) {
    Commit([&](Transaction& txn) {
      return txn.Assert(EntityFact::Vertex(v), ValidTime{0});
    });
  }
  void Edge(const EdgeIdentity& identity, uint64_t from, uint64_t to) {
    Commit([&](Transaction& txn) {
      auto status = txn.Assert(identity, ValidTime{from});
      if (!status.ok()) return status;
      return txn.Retract(EntityFact::Edge(identity.edge_ref()), ValidTime{to});
    });
  }
  void EdgePeriod(const EdgeIdentity& identity, uint64_t from, uint64_t to) {
    Commit([&](Transaction& txn) {
      return txn.Assert(identity, ValidTime{from});
    });
    Commit([&](Transaction& txn) {
      return txn.Retract(EntityFact::Edge(identity.edge_ref()), ValidTime{to});
    });
  }
};
}  // namespace

TEST(CoexistingPathTest, RejectsAPathWithoutACommonInstant) {
  GraphFixture graph;
  graph.Vertex(graph.a); graph.Vertex(graph.b); graph.Vertex(graph.c);
  graph.Edge({EdgeRef{PartId{0}, EdgeId{11}}, graph.a, graph.b, 1}, 0, 10);
  graph.Edge({EdgeRef{PartId{0}, EdgeId{12}}, graph.b, graph.c, 1}, 10, 20);
  auto snapshot = graph.database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  GraphExpansionRequest request{{graph.a}, {ValidTime{0}, ValidTime{20}},
                                ExpandDirection::kOut, std::nullopt};
  GraphFrontierOptions options; options.max_hops = 2;
  auto result = CoexistingShortestPath(snapshot.ValueOrDie(), request, graph.c,
                                       options);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_TRUE(result.ValueOrDie().paths.empty());
}

TEST(CoexistingPathTest, ShallowerSupersetDominatesDeeperSubset) {
  GraphFixture graph;
  graph.Vertex(graph.a); graph.Vertex(graph.b); graph.Vertex(graph.c);
  graph.Edge({EdgeRef{PartId{0}, EdgeId{21}}, graph.a, graph.c, 1}, 0, 20);
  graph.Edge({EdgeRef{PartId{0}, EdgeId{22}}, graph.a, graph.b, 1}, 0, 10);
  graph.Edge({EdgeRef{PartId{0}, EdgeId{23}}, graph.b, graph.c, 1}, 0, 10);
  auto snapshot = graph.database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  GraphExpansionRequest request{{graph.a}, {ValidTime{0}, ValidTime{20}},
                                ExpandDirection::kOut, std::nullopt};
  GraphFrontierOptions options; options.max_hops = 2;
  auto result = CoexistingShortestPath(snapshot.ValueOrDie(), request, graph.c,
                                       options);
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result.ValueOrDie().paths.size(), 1U);
  EXPECT_EQ(result.ValueOrDie().paths.front().edges.size(), 1U);
}

TEST(CoexistingPathTest, BudgetExhaustionIsResourceExhausted) {
  GraphFixture graph;
  graph.Vertex(graph.a); graph.Vertex(graph.b);
  graph.Edge({EdgeRef{PartId{0}, EdgeId{31}}, graph.a, graph.b, 1}, 0, 20);
  auto snapshot = graph.database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  GraphExpansionRequest request{{graph.a}, {ValidTime{0}, ValidTime{20}},
                                ExpandDirection::kOut, std::nullopt};
  std::array<uint64_t, static_cast<size_t>(ResourceDimension::kCount)> limits;
  limits.fill(UINT64_MAX);
  limits[static_cast<size_t>(ResourceDimension::kGraphLabels)] = 1;
  QueryReservation reservation(limits);
  GraphFrontierOptions options; options.max_hops = 1; options.reservation = &reservation;
  auto result = CoexistingShortestPath(snapshot.ValueOrDie(), request, graph.b,
                                       options);
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsResourceExhausted());
}

TEST(CoexistingPathTest, KeepsDisjointMaximalWitnessIntervals) {
  GraphFixture graph;
  graph.Vertex(graph.a); graph.Vertex(graph.b); graph.Vertex(graph.c);
  graph.EdgePeriod({EdgeRef{PartId{0}, EdgeId{41}}, graph.a, graph.b, 1}, 0, 5);
  graph.EdgePeriod({EdgeRef{PartId{0}, EdgeId{42}}, graph.b, graph.c, 1}, 0, 5);
  graph.EdgePeriod({EdgeRef{PartId{0}, EdgeId{43}}, graph.a, graph.b, 1}, 20, 30);
  graph.EdgePeriod({EdgeRef{PartId{0}, EdgeId{44}}, graph.b, graph.c, 1}, 20, 30);
  auto snapshot = graph.database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  GraphExpansionRequest request{{graph.a}, {ValidTime{0}, ValidTime{30}},
                                ExpandDirection::kOut, std::nullopt};
  GraphFrontierOptions options; options.max_hops = 2;
  auto result = CoexistingShortestPath(snapshot.ValueOrDie(), request, graph.c,
                                       options);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  ASSERT_EQ(result.ValueOrDie().paths.size(), 2U);
  EXPECT_EQ(result.ValueOrDie().paths[0].common.from.value, 0U);
  EXPECT_EQ(result.ValueOrDie().paths[1].common.from.value, 20U);
}

TEST(CoexistingPathTest, EqualHopUsesLexicographicEdgeSequence) {
  GraphFixture graph;
  graph.Vertex(graph.a); graph.Vertex(graph.b); graph.Vertex(graph.c);
  graph.Edge({EdgeRef{PartId{0}, EdgeId{52}}, graph.a, graph.c, 1}, 0, 20);
  graph.Edge({EdgeRef{PartId{0}, EdgeId{51}}, graph.a, graph.b, 1}, 0, 20);
  graph.Edge({EdgeRef{PartId{0}, EdgeId{53}}, graph.b, graph.c, 1}, 0, 20);
  auto snapshot = graph.database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  GraphExpansionRequest request{{graph.a}, {ValidTime{0}, ValidTime{20}},
                                ExpandDirection::kOut, std::nullopt};
  GraphFrontierOptions options; options.max_hops = 2;
  auto result = CoexistingShortestPath(snapshot.ValueOrDie(), request, graph.c,
                                       options);
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result.ValueOrDie().paths.size(), 1U);
  EXPECT_EQ(result.ValueOrDie().paths.front().edges.front().edge_id.value, 52U);
}

TEST(CoexistingPathTest, PathColumnOffsetsDecodeUnequalVertexAndEdgeLengths) {
  PathValue one{{VertexRef{PartId{0}, VertexId{1}}}, {},
                ValidTimeInterval{ValidTime{0}, ValidTime{4}}};
  PathValue two{{VertexRef{PartId{0}, VertexId{1}},
                 VertexRef{PartId{0}, VertexId{2}},
                 VertexRef{PartId{0}, VertexId{3}}},
                {EdgeRef{PartId{0}, EdgeId{71}}, EdgeRef{PartId{0}, EdgeId{72}}},
                ValidTimeInterval{ValidTime{8}, ValidTime{12}}};
  const PathColumn column = PathColumn::FromValues({one, two});
  ASSERT_EQ(column.vertex_offsets, (std::vector<uint32_t>{0, 1, 4}));
  ASSERT_EQ(column.edge_offsets, (std::vector<uint32_t>{0, 0, 2}));
  EXPECT_EQ(column.row_offsets, column.vertex_offsets);
  EXPECT_EQ(column.Value(0), one);
  EXPECT_EQ(column.Value(1), two);
}

TEST(CoexistingPathTest, PublicQueryMaterializesPathAndChargesSurvivorsOnce) {
  GraphFixture graph;
  graph.Vertex(graph.a);
  graph.Vertex(graph.b);
  const EdgeRef edge{PartId{0}, EdgeId{81}};
  graph.Edge({edge, graph.a, graph.b, 1}, 0, 20);

  const auto vertex = Slot<VertexRef>::Named("vertex");
  const auto edge_slot = Slot<EdgeRef>::Named("edge");
  const auto destination = Slot<VertexRef>::Named("destination");
  const auto path = Slot<PathValue>::Named("path");
  auto source = Query::Vertices(
      vertex, History{ValidTimeInterval{ValidTime{0}, ValidTime{20}}});
  ASSERT_TRUE(source.ok());
  auto query = source.ValueOrDie().CoexistingShortestPath(
      ExpandSpec{vertex, edge_slot, destination, ExpandDirection::kOut}, 1,
      path);
  ASSERT_TRUE(query.ok());
  auto prepared = graph.database->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto snapshot = graph.database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  QueryOptions options;
  options.budget.graph_labels = 2;
  options.budget.interval_fragments = 2;
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{}, options);
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  auto batch = cursor.ValueOrDie().Next();
  ASSERT_TRUE(batch.ok()) << batch.status().ToString();
  ASSERT_TRUE(batch.ValueOrDie().has_value());
  ASSERT_EQ(batch.ValueOrDie()->row_count(), 1U);
  const PathValue expected{{graph.a, graph.b}, {edge},
                           ValidTimeInterval{ValidTime{0}, ValidTime{20}}};
  EXPECT_EQ(batch.ValueOrDie()->Get<PathValue>(path, 0), expected);
  EXPECT_THROW(
      batch.ValueOrDie()->Get<PathValue>(
          Slot<PathValue>::WithId(vertex.id(), "wrong_type"), 0),
      std::invalid_argument);
}

TEST(CoexistingPathTest, PublicQueryReturnsEmptyCursorWhenTargetIsAbsent) {
  GraphFixture graph;
  graph.Vertex(graph.a);
  graph.Vertex(graph.b);
  const auto vertex = Slot<VertexRef>::Named("vertex");
  const auto edge_slot = Slot<EdgeRef>::Named("edge");
  const auto destination = Slot<VertexRef>::Named("destination");
  const auto path = Slot<PathValue>::Named("path");
  auto source = Query::Vertices(vertex, At{ValidTime{1}});
  ASSERT_TRUE(source.ok());
  auto query = source.ValueOrDie().CoexistingShortestPath(
      ExpandSpec{vertex, edge_slot, destination, ExpandDirection::kOut}, 1,
      path);
  ASSERT_TRUE(query.ok());
  auto prepared = graph.database->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto snapshot = graph.database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  auto batch = cursor.ValueOrDie().Next();
  ASSERT_TRUE(batch.ok()) << batch.status().ToString();
  EXPECT_FALSE(batch.ValueOrDie().has_value());
}

}  // namespace cedar::internal
