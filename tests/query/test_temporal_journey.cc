#include <gtest/gtest.h>

#include <filesystem>
#include <functional>
#include <memory>
#include <cstdlib>

#include "cedar/database.h"
#include "cedar/query/result.h"
#include "cedar/transaction.h"
#include "query/runtime/journey.h"

namespace cedar::internal {
namespace {

struct JourneyFixture {
  std::string path;
  std::unique_ptr<Database> database;
  VertexRef a{PartId{0}, VertexId{1}};
  VertexRef b{PartId{0}, VertexId{2}};
  VertexRef c{PartId{0}, VertexId{3}};
  VertexRef d{PartId{0}, VertexId{4}};

  JourneyFixture() {
    char pattern[] = "/tmp/cedar_temporal_journey_XXXXXX";
    if (mkdtemp(pattern) == nullptr) std::abort();
    path = pattern;
    auto opened = Database::Open(DatabaseOptions{.path = path});
    if (!opened.ok()) std::abort();
    database = std::move(opened).ConsumeValueOrDie();
  }
  ~JourneyFixture() {
    if (database) database->Close().IgnoreError();
    std::filesystem::remove_all(path);
  }
  void Commit(const std::function<Status(Transaction&)>& stage) {
    auto txn = database->BeginTransaction();
    ASSERT_TRUE(txn.ok()) << txn.status().ToString();
    ASSERT_TRUE(stage(*txn.ValueOrDie()).ok());
    ASSERT_TRUE(txn.ValueOrDie()->Commit().ok());
  }
  void Vertex(VertexRef v, uint64_t from = 0, uint64_t to = 100) {
    Commit([&](Transaction& txn) {
      auto status = txn.Assert(EntityFact::Vertex(v), ValidTime{from});
      if (!status.ok()) return status;
      return txn.Retract(EntityFact::Vertex(v), ValidTime{to});
    });
  }
  void Edge(EdgeRef e, VertexRef source, VertexRef target, uint64_t from,
            uint64_t to) {
    Commit([&](Transaction& txn) {
      auto status = txn.Assert(EdgeIdentity{e, source, target, 1},
                               ValidTime{from});
      if (!status.ok()) return status;
      return txn.Retract(EntityFact::Edge(e), ValidTime{to});
    });
  }
};

TEST(TemporalJourneyTest, ZeroDurationStillRequiresInstantaneousVisibility) {
  JourneyRequest request;
  request.interval = {ValidTime{0}, ValidTime{10}};
  EXPECT_TRUE(TraversalFits({ValidTime{2}, ValidTime{5}}, ValidTime{4},
                             ValidDuration{0}));
  EXPECT_FALSE(TraversalFits({ValidTime{2}, ValidTime{5}}, ValidTime{5},
                              ValidDuration{1}));
}

TEST(TemporalJourneyTest, PositiveDurationCannotReachExclusiveUpperBound) {
  EXPECT_FALSE(TraversalFits({ValidTime{0}, ValidTime{10}}, ValidTime{8},
                              ValidDuration{2}));
  EXPECT_TRUE(TraversalFits({ValidTime{0}, ValidTime{10}}, ValidTime{7},
                             ValidDuration{2}));
}

TEST(TemporalJourneyTest, DurationAdditionOverflowIsNumericOverflow) {
  auto arrival = AddDuration(ValidTime{UINT64_MAX - 1}, ValidDuration{2});
  ASSERT_FALSE(arrival.ok());
  EXPECT_TRUE(arrival.status().IsNumericOverflow());
}

TEST(TemporalJourneyTest, EarliestArrivalMaterializesOrderedJourney) {
  JourneyFixture graph;
  graph.Vertex(graph.a); graph.Vertex(graph.b); graph.Vertex(graph.d);
  graph.Edge(EdgeRef{PartId{0}, EdgeId{11}}, graph.a, graph.b, 0, 20);
  graph.Edge(EdgeRef{PartId{0}, EdgeId{12}}, graph.b, graph.d, 5, 30);
  auto snapshot = graph.database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  JourneyRequest request{graph.a, graph.d, {ValidTime{0}, ValidTime{30}},
                         JourneyObjective::kEarliestArrival};
  request.duration_at = [](EdgeRef edge, ValidTime) -> StatusOr<std::optional<ValidDuration>> {
    return std::optional<ValidDuration>{ValidDuration{static_cast<uint64_t>(edge.edge_id.value == 11 ? 5 : 7)}};
  };
  auto journey = EarliestArrival(snapshot.ValueOrDie(), request);
  ASSERT_TRUE(journey.ok()) << journey.status().ToString();
  EXPECT_EQ(journey.ValueOrDie().vertices, (std::vector<VertexRef>{graph.a, graph.b, graph.d}));
  EXPECT_EQ(journey.ValueOrDie().edges.size(), 2U);
  EXPECT_EQ(journey.ValueOrDie().final_arrival, (ValidTime{12}));
}

TEST(TemporalJourneyTest, LatestDepartureUsesReverseSearch) {
  JourneyFixture graph;
  graph.Vertex(graph.a, 0, 1'000'000);
  graph.Vertex(graph.b, 0, 1'000'000);
  graph.Edge(EdgeRef{PartId{0}, EdgeId{31}}, graph.a, graph.b, 0, 1'000'000);
  auto snapshot = graph.database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  JourneyRequest request{graph.a, graph.b, {ValidTime{0}, ValidTime{1'000'000}},
                         JourneyObjective::kLatestDeparture};
  request.arrival_deadline = ValidTime{500'000};
  request.duration_at = [](EdgeRef, ValidTime) -> StatusOr<std::optional<ValidDuration>> {
    return std::optional<ValidDuration>{ValidDuration{5}};
  };
  auto journey = LatestDeparture(snapshot.ValueOrDie(), request);
  ASSERT_TRUE(journey.ok()) << journey.status().ToString();
  EXPECT_EQ(journey.ValueOrDie().initial_departure, (ValidTime{499'995}));
}

TEST(TemporalJourneyTest, LatestDepartureReconstructsMultiHopOrder) {
  JourneyFixture graph;
  graph.Vertex(graph.a, 0, 100);
  graph.Vertex(graph.b, 0, 100);
  graph.Vertex(graph.d, 0, 100);
  graph.Edge(EdgeRef{PartId{0}, EdgeId{32}}, graph.a, graph.b, 0, 100);
  graph.Edge(EdgeRef{PartId{0}, EdgeId{33}}, graph.b, graph.d, 0, 100);
  auto snapshot = graph.database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  JourneyRequest request{graph.a, graph.d, {ValidTime{0}, ValidTime{100}},
                         JourneyObjective::kLatestDeparture};
  request.arrival_deadline = ValidTime{20};
  request.duration_at = [](EdgeRef, ValidTime)
      -> StatusOr<std::optional<ValidDuration>> {
    return std::optional<ValidDuration>{ValidDuration{5}};
  };
  auto journey = LatestDeparture(snapshot.ValueOrDie(), request);
  ASSERT_TRUE(journey.ok()) << journey.status().ToString();
  ASSERT_EQ(journey.ValueOrDie().edges.size(), 2U);
  EXPECT_EQ(journey.ValueOrDie().edges[0].edge_id.value, 32U);
  EXPECT_EQ(journey.ValueOrDie().edges[1].edge_id.value, 33U);
}

TEST(TemporalJourneyTest, FastestDurationRetainsDepartureArrivalParetoLabels) {
  JourneyFixture graph;
  graph.Vertex(graph.a, 0, 1'000'000);
  graph.Vertex(graph.b, 0, 1'000'000);
  graph.Edge(EdgeRef{PartId{0}, EdgeId{41}}, graph.a, graph.b, 0, 1'000'000);
  graph.Edge(EdgeRef{PartId{0}, EdgeId{42}}, graph.a, graph.b, 500'000, 1'000'000);
  auto snapshot = graph.database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  JourneyRequest request{graph.a, graph.b, {ValidTime{0}, ValidTime{1'000'000}},
                         JourneyObjective::kFastestDuration};
  request.duration_at = [](EdgeRef edge, ValidTime) -> StatusOr<std::optional<ValidDuration>> {
    return std::optional<ValidDuration>{ValidDuration{static_cast<uint64_t>(edge.edge_id.value == 41 ? 100 : 1)}};
  };
  auto journey = FastestDuration(snapshot.ValueOrDie(), request);
  ASSERT_TRUE(journey.ok()) << journey.status().ToString();
  EXPECT_EQ(journey.ValueOrDie().initial_departure, (ValidTime{500'000}));
  EXPECT_EQ(journey.ValueOrDie().duration, (ValidDuration{1}));
}

TEST(TemporalJourneyTest, PrepareRejectsUnprovenRegisteredDurationFifo) {
  JourneyFixture graph;
  ASSERT_TRUE(graph.database->RegisterProperty(PropertyDefinition{
      PropertyId{7}, 0, "duration", PropertyEntityKind::kEdge,
      PhysicalType::kInt64, 4096}).ok());
  graph.Vertex(graph.a); graph.Vertex(graph.b);
  const EdgeRef edge{PartId{0}, EdgeId{21}};
  graph.Edge(edge, graph.a, graph.b, 0, 20);
  graph.Commit([&](Transaction& txn) {
    return txn.Set(PropertyFact::Edge(edge, PropertyId{7}), ValidTime{0},
                    Value::Int64(5));
  });
  Slot<VertexRef> source = Slot<VertexRef>::Named("source");
  Slot<EdgeRef> edge_slot = Slot<EdgeRef>::Named("edge");
  Slot<VertexRef> target = Slot<VertexRef>::Named("target");
  Slot<JourneyValue> journey = Slot<JourneyValue>::Named("journey");
  auto source_query = Query::Vertices(source,
      History{ValidTimeInterval{ValidTime{0}, ValidTime{20}}});
  ASSERT_TRUE(source_query.ok());
  auto built = source_query.ValueOrDie().EarliestArrival(
      ExpandSpec{source, edge_slot, target, ExpandDirection::kOut}, 2,
      PropertyId{7}, journey);
  ASSERT_TRUE(built.ok()) << built.status().ToString();
  auto selected = built.ValueOrDie().Select({Project(source), Project(edge_slot),
                                             Project(target), Project(journey)});
  ASSERT_TRUE(selected.ok()) << selected.status().ToString();
  auto prepared = graph.database->PrepareQuery(selected.ValueOrDie());
  ASSERT_FALSE(prepared.ok());
  EXPECT_TRUE(prepared.status().IsNotSupportedError())
      << prepared.status().ToString();
}

TEST(TemporalJourneyTest, CallbackNonFifoIsRejectedBeforeEarliestSearch) {
  JourneyFixture graph;
  graph.Vertex(graph.a); graph.Vertex(graph.b);
  graph.Edge(EdgeRef{PartId{0}, EdgeId{31}}, graph.a, graph.b, 0, 10);
  auto snapshot = graph.database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  JourneyRequest request{graph.a, graph.b, {ValidTime{0}, ValidTime{10}},
                         JourneyObjective::kEarliestArrival};
  request.duration_at = [](EdgeRef, ValidTime time)
      -> StatusOr<std::optional<ValidDuration>> {
    return std::optional<ValidDuration>{ValidDuration{
        static_cast<uint64_t>(time.value < 5 ? 9 : 0)}};
  };
  auto journey = EarliestArrival(snapshot.ValueOrDie(), request);
  ASSERT_FALSE(journey.ok());
  EXPECT_TRUE(journey.status().IsNotSupportedError())
      << journey.status().ToString();
}

TEST(TemporalJourneyTest, WaitingCannotCrossSourceVisibilityGap) {
  JourneyFixture graph;
  graph.Vertex(graph.b); graph.Vertex(graph.d);
  graph.Commit([&](Transaction& txn) {
    auto status = txn.Assert(EntityFact::Vertex(graph.a), ValidTime{0});
    if (!status.ok()) return status;
    status = txn.Retract(EntityFact::Vertex(graph.a), ValidTime{3});
    if (!status.ok()) return status;
    status = txn.Assert(EntityFact::Vertex(graph.a), ValidTime{5});
    if (!status.ok()) return status;
    return txn.Retract(EntityFact::Vertex(graph.a), ValidTime{20});
  });
  graph.Edge(EdgeRef{PartId{0}, EdgeId{41}}, graph.a, graph.b, 5, 20);
  auto snapshot = graph.database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  JourneyRequest request{graph.a, graph.b, {ValidTime{0}, ValidTime{20}},
                         JourneyObjective::kEarliestArrival};
  request.duration_at = [](EdgeRef, ValidTime)
      -> StatusOr<std::optional<ValidDuration>> {
    return std::optional<ValidDuration>{ValidDuration{1}};
  };
  auto journey = EarliestArrival(snapshot.ValueOrDie(), request);
  ASSERT_FALSE(journey.ok());
  EXPECT_TRUE(journey.status().IsNotFound())
      << journey.status().ToString();
}

}  // namespace
}  // namespace cedar::internal
