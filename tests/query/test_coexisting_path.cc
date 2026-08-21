#include <gtest/gtest.h>

#include <filesystem>
#include <functional>
#include <memory>
#include <cstdlib>

#include "cedar/database.h"
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

}  // namespace cedar::internal
