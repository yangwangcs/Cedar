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
#include "query/runtime/journey.h"

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

TEST(CoexistingPathTest, ExpansionHistoryRespectsMemoryBudgetAndReleasesScratch) {
  for (const bool delta_history : {false, true}) {
    SCOPED_TRACE(delta_history ? "delta history" : "canonical history");
    GraphFixture graph;
    graph.Vertex(graph.b);
    graph.Edge({EdgeRef{PartId{0}, EdgeId{32}}, graph.a, graph.b, 1}, 0, 1024);
    if (!delta_history) {
      graph.Commit([&](Transaction& txn) {
        for (uint64_t time = 0; time < 512; ++time) {
          const auto fact = EntityFact::Vertex(graph.a);
          const Status status = time % 2 == 0
              ? txn.Assert(fact, ValidTime{time})
              : txn.Retract(fact, ValidTime{time});
          if (!status.ok()) return status;
        }
        return Status::OK();
      });
    }
    auto snapshot = graph.database->BeginSnapshot();
    ASSERT_TRUE(snapshot.ok());
    QueryDeltaView delta;
    if (delta_history) {
      for (uint64_t time = 0; time < 512; ++time) {
        delta.facts.push_back(FactEvent{
            EntityFact::Vertex(graph.a).ref(), ValidTime{time},
            snapshot.ValueOrDie().commit_seq(),
            time % 2 == 0 ? FactOperation::kPut : FactOperation::kDelete,
            0, std::nullopt});
      }
    }
    GraphExpansionRequest request{{graph.a}, {ValidTime{0}, ValidTime{512}},
                                  ExpandDirection::kOut, std::nullopt};
    for (const uint64_t bytes : {uint64_t{32 << 10}, uint64_t{8 << 20}}) {
      SCOPED_TRACE(bytes);
      QueryReservation reservation(bytes);
      GraphFrontierOptions options;
      options.reservation = &reservation;
      options.delta = delta_history ? &delta : nullptr;
      auto result = ExpandTemporal(snapshot.ValueOrDie(), request, options);
      if (bytes == (32 << 10)) {
        ASSERT_FALSE(result.ok());
        EXPECT_TRUE(result.status().IsResourceExhausted()) << result.status().ToString();
      } else {
        ASSERT_TRUE(result.ok()) << result.status().ToString();
        ASSERT_EQ(result.ValueOrDie().size(), 256U);
        EXPECT_EQ(result.ValueOrDie().front().effective,
                  (ValidTimeInterval{ValidTime{0}, ValidTime{1}}));
        EXPECT_EQ(result.ValueOrDie().back().effective,
                  (ValidTimeInterval{ValidTime{510}, ValidTime{511}}));
        EXPECT_GT(reservation.used(ResourceDimension::kMemory), 0U);
      }
      result = Status::NotFound("test", "released expansion result");
      EXPECT_EQ(reservation.used(ResourceDimension::kMemory), 0U);
    }
    for (const uint64_t bytes : {uint64_t{32 << 10}, uint64_t{8 << 20}}) {
      SCOPED_TRACE(bytes);
      QueryReservation reservation(bytes);
      GraphFrontierOptions options;
      options.reservation = &reservation;
      options.delta = delta_history ? &delta : nullptr;
      auto result = CoexistingShortestPath(snapshot.ValueOrDie(), request,
                                           graph.b, options);
      if (bytes == (32 << 10)) {
        EXPECT_FALSE(result.ok());
        EXPECT_TRUE(result.status().IsResourceExhausted()) << result.status().ToString();
      } else {
        ASSERT_TRUE(result.ok()) << result.status().ToString();
        ASSERT_EQ(result.ValueOrDie().paths.size(), 256U);
        EXPECT_EQ(result.ValueOrDie().paths.front().common,
                  (ValidTimeInterval{ValidTime{0}, ValidTime{1}}));
        EXPECT_EQ(result.ValueOrDie().paths.back().common,
                  (ValidTimeInterval{ValidTime{510}, ValidTime{511}}));
        EXPECT_GT(reservation.used(ResourceDimension::kMemory), 0U);
      }
      result = Status::NotFound("test", "released path result");
      EXPECT_EQ(reservation.used(ResourceDimension::kMemory), 0U);
      QueryReservation journey_reservation(bytes);
      JourneyRequest journey_request{graph.a, graph.b, request.interval};
      journey_request.max_hops = 1;
      journey_request.duration_at = [](EdgeRef, ValidTime)
          -> StatusOr<std::optional<ValidDuration>> {
        return std::optional<ValidDuration>{ValidDuration{0}};
      };
      JourneyOptions journey_options;
      journey_options.reservation = &journey_reservation;
      journey_options.delta = options.delta;
      auto journey = EarliestArrival(snapshot.ValueOrDie(), journey_request,
                                      journey_options);
      if (bytes == (32 << 10)) {
        EXPECT_FALSE(journey.ok());
        EXPECT_TRUE(journey.status().IsResourceExhausted()) << journey.status().ToString();
      } else {
        ASSERT_TRUE(journey.ok()) << journey.status().ToString();
        EXPECT_EQ(journey.ValueOrDie().edges,
                  (std::vector<EdgeRef>{{PartId{0}, EdgeId{32}}}));
        EXPECT_EQ(journey.ValueOrDie().arrival, ValidTime{0});
        EXPECT_GT(journey_reservation.used(ResourceDimension::kMemory), 0U);
      }
      journey = Status::NotFound("test", "released journey result");
      EXPECT_EQ(journey_reservation.used(ResourceDimension::kMemory), 0U);
    }
    if (delta_history) {
      QueryReservation reservation(8 << 20);
      GraphFrontierOptions options;
      options.reservation = &reservation;
      options.delta = &delta;
      size_t checks = 0;
      options.check_abort = [&checks] {
        return ++checks == 64 ? Status::QueryCancelled("graph history test") : Status::OK();
      };
      auto cancelled = ExpandTemporal(snapshot.ValueOrDie(), request, options);
      ASSERT_FALSE(cancelled.ok());
      EXPECT_TRUE(cancelled.status().IsQueryCancelled());
      EXPECT_EQ(reservation.used(ResourceDimension::kMemory), 0U);
    }
  }
}

TEST(CoexistingPathTest, GraphResultsKeepMemoryThroughMoveAndReplacement) {
  GraphFixture graph;
  graph.Vertex(graph.a); graph.Vertex(graph.b); graph.Vertex(graph.c);
  const EdgeRef first{PartId{0}, EdgeId{801}};
  const EdgeRef second{PartId{0}, EdgeId{802}};
  graph.Edge({first, graph.a, graph.b, 1}, 0, 20);
  graph.Edge({second, graph.b, graph.c, 1}, 0, 20);
  auto snapshot = graph.database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  GraphExpansionRequest request{{graph.a}, {ValidTime{0}, ValidTime{20}},
                                  ExpandDirection::kOut, std::nullopt};
  QueryReservation reservation(8 << 20);
  GraphFrontierOptions options;
  options.reservation = &reservation;
  options.max_hops = 2;
  auto hops = KHopExpand(snapshot.ValueOrDie(), request, options);
  ASSERT_TRUE(hops.ok()) << hops.status().ToString();
  ASSERT_EQ(hops.ValueOrDie().traversals.size(), 2U);
  const uint64_t hop_bytes = reservation.used(ResourceDimension::kMemory);
  EXPECT_GT(hop_bytes, 0U);
  KHopResult moved_hops = std::move(hops).ConsumeValueOrDie();
  EXPECT_EQ(reservation.used(ResourceDimension::kMemory), hop_bytes);
  moved_hops = KHopResult{};
  EXPECT_EQ(reservation.used(ResourceDimension::kMemory), 0U);

  auto paths = CoexistingShortestPath(snapshot.ValueOrDie(), request, graph.c, options);
  ASSERT_TRUE(paths.ok()) << paths.status().ToString();
  ASSERT_EQ(paths.ValueOrDie().paths.size(), 1U);
  EXPECT_EQ(paths.ValueOrDie().paths.front().edges, (std::vector<EdgeRef>{first, second}));
  const uint64_t path_bytes = reservation.used(ResourceDimension::kMemory);
  EXPECT_GT(path_bytes, 0U);
  CoexistingPathResult moved_paths = std::move(paths).ConsumeValueOrDie();
  EXPECT_EQ(reservation.used(ResourceDimension::kMemory), path_bytes);
  moved_paths = CoexistingPathResult{};
  EXPECT_EQ(reservation.used(ResourceDimension::kMemory), 0U);

  QueryReservation tiny(1);
  options.reservation = &tiny;
  auto rejected_hops = KHopExpand(snapshot.ValueOrDie(), request, options);
  ASSERT_FALSE(rejected_hops.ok());
  EXPECT_TRUE(rejected_hops.status().IsResourceExhausted());
  auto rejected_paths = CoexistingShortestPath(snapshot.ValueOrDie(), request, graph.c, options);
  ASSERT_FALSE(rejected_paths.ok());
  EXPECT_TRUE(rejected_paths.status().IsResourceExhausted());
  EXPECT_EQ(tiny.used(ResourceDimension::kMemory), 0U);
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

TEST(CoexistingPathTest, CandidateBeatingMultipleOverlappingWinnersReplacesAll) {
  GraphFixture graph;
  graph.Vertex(graph.a); graph.Vertex(graph.c);
  graph.EdgePeriod({EdgeRef{PartId{0}, EdgeId{61}}, graph.a, graph.c, 1}, 0, 10);
  graph.EdgePeriod({EdgeRef{PartId{0}, EdgeId{62}}, graph.a, graph.c, 1}, 20, 30);
  graph.EdgePeriod({EdgeRef{PartId{0}, EdgeId{63}}, graph.a, graph.c, 1}, 5, 25);
  auto snapshot = graph.database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  GraphExpansionRequest request{{graph.a}, {ValidTime{0}, ValidTime{30}},
                                ExpandDirection::kOut, std::nullopt};
  GraphFrontierOptions options; options.max_hops = 1;
  auto result = CoexistingShortestPath(snapshot.ValueOrDie(), request, graph.c,
                                       options);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  ASSERT_EQ(result.ValueOrDie().paths.size(), 1U);
  EXPECT_EQ(result.ValueOrDie().paths.front().common,
            (ValidTimeInterval{ValidTime{5}, ValidTime{25}}));
  EXPECT_EQ(result.ValueOrDie().paths.front().edges.front().edge_id.value, 63U);
}

TEST(CoexistingPathTest, GlobalWinnerFilterKeepsChainEndpointWitnesses) {
  GraphFixture graph;
  graph.Vertex(graph.a); graph.Vertex(graph.c);
  // C is initially rejected by A. Once B replaces A, C is disjoint from the
  // final winner and must be reconsidered by a global (rather than one-pass)
  // filter. Intervals are half-open, so C and B only touch at 20.
  graph.EdgePeriod({EdgeRef{PartId{0}, EdgeId{64}}, graph.a, graph.c, 1}, 0, 100);
  graph.EdgePeriod({EdgeRef{PartId{0}, EdgeId{65}}, graph.a, graph.c, 1}, 10, 20);
  graph.EdgePeriod({EdgeRef{PartId{0}, EdgeId{66}}, graph.a, graph.c, 1}, 20, 200);
  auto snapshot = graph.database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  GraphExpansionRequest request{{graph.a}, {ValidTime{0}, ValidTime{200}},
                                ExpandDirection::kOut, std::nullopt};
  GraphFrontierOptions options; options.max_hops = 1;
  auto result = CoexistingShortestPath(snapshot.ValueOrDie(), request, graph.c,
                                       options);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  ASSERT_EQ(result.ValueOrDie().paths.size(), 2U);
  EXPECT_EQ(result.ValueOrDie().paths[0].common,
            (ValidTimeInterval{ValidTime{10}, ValidTime{20}}));
  EXPECT_EQ(result.ValueOrDie().paths[1].common,
            (ValidTimeInterval{ValidTime{20}, ValidTime{200}}));
  EXPECT_EQ(result.ValueOrDie().paths[0].edges.front().edge_id.value, 65U);
  EXPECT_EQ(result.ValueOrDie().paths[1].edges.front().edge_id.value, 66U);
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

TEST(CoexistingPathTest, PublicQueryBothFindsIncomingOnlyTarget) {
  GraphFixture graph;
  graph.Vertex(graph.a); graph.Vertex(graph.b);
  const EdgeRef edge{PartId{0}, EdgeId{91}};
  graph.Edge({edge, graph.b, graph.a, 1}, 0, 20);
  const auto vertex = Slot<VertexRef>::Named("vertex");
  const auto edge_slot = Slot<EdgeRef>::Named("edge");
  const auto destination = Slot<VertexRef>::Named("destination");
  const auto path = Slot<PathValue>::Named("path");
  auto source = Query::Vertices(vertex, At{ValidTime{1}});
  ASSERT_TRUE(source.ok());
  auto query = source.ValueOrDie().CoexistingShortestPath(
      ExpandSpec{vertex, edge_slot, destination, ExpandDirection::kBoth}, 1,
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
  ASSERT_TRUE(batch.ValueOrDie().has_value());
  ASSERT_EQ(batch.ValueOrDie()->row_count(), 2U);
  bool found_incoming = false;
  for (size_t index = 0; index < batch.ValueOrDie()->row_count(); ++index) {
    const PathValue value = batch.ValueOrDie()->Get<PathValue>(path, index);
    if (value.vertices == std::vector<VertexRef>{graph.a, graph.b}) {
      found_incoming = value.edges == std::vector<EdgeRef>{edge};
    }
  }
  EXPECT_TRUE(found_incoming);
}

TEST(CoexistingPathTest, PublicQueryDiscoversTargetsBeyondOneHop) {
  GraphFixture graph;
  graph.Vertex(graph.a); graph.Vertex(graph.b); graph.Vertex(graph.c);
  const EdgeRef first{PartId{0}, EdgeId{92}};
  const EdgeRef second{PartId{0}, EdgeId{93}};
  graph.Edge({first, graph.a, graph.b, 1}, 0, 20);
  graph.Edge({second, graph.b, graph.c, 1}, 0, 20);
  const auto vertex = Slot<VertexRef>::Named("vertex");
  const auto edge_slot = Slot<EdgeRef>::Named("edge");
  const auto destination = Slot<VertexRef>::Named("destination");
  const auto path = Slot<PathValue>::Named("path");
  auto source = Query::Vertices(vertex, At{ValidTime{1}});
  ASSERT_TRUE(source.ok());
  auto query = source.ValueOrDie().CoexistingShortestPath(
      ExpandSpec{vertex, edge_slot, destination, ExpandDirection::kOut}, 2,
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
  ASSERT_TRUE(batch.ValueOrDie().has_value());
  ASSERT_EQ(batch.ValueOrDie()->row_count(), 3U);
  bool found_two_hop = false;
  for (size_t index = 0; index < batch.ValueOrDie()->row_count(); ++index) {
    if (batch.ValueOrDie()->Get<PathValue>(path, index).vertices ==
        std::vector<VertexRef>{graph.a, graph.b, graph.c}) {
      found_two_hop = true;
    }
  }
  EXPECT_TRUE(found_two_hop);
}

}  // namespace cedar::internal
