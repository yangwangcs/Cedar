#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <limits>
#include <memory>
#include <map>
#include <random>
#include <cstdlib>
#include <tuple>

#include "cedar/database.h"
#include "cedar/query/query.h"
#include "cedar/query/result.h"
#include "cedar/transaction.h"
#include "query/runtime/journey.h"
#include "query/runtime/query_runtime.h"
#include "query/logical/logical_plan.h"

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

struct OracleEdge {
  EdgeRef edge;
  VertexRef source;
  VertexRef target;
  uint64_t from = 0;
  uint64_t to = 0;
  uint64_t duration = 1;
  bool missing_before_three = false;
};

struct OracleCase {
  VertexRef source;
  VertexRef target;
  std::vector<VertexRef> vertices;
  std::vector<OracleEdge> edges;
  ValidTimeInterval interval;
  uint32_t max_hops = 1;
  bool overflow = false;
};

struct OracleCandidate {
  uint64_t departure = 0;
  uint64_t arrival = 0;
  std::vector<uint64_t> edges;
};

void EnumerateOracle(const OracleCase& graph, VertexRef vertex,
                     uint64_t arrival, uint64_t initial_departure,
                     uint32_t depth, std::vector<uint64_t>* path,
                     std::vector<OracleCandidate>* output,
                     bool enumerate_all_departures) {
  if (depth >= graph.max_hops) return;
  const uint64_t interval_end = graph.interval.to->value;
  for (const OracleEdge& edge : graph.edges) {
    if (edge.source != vertex) continue;
    const uint64_t first = std::max(arrival, edge.from);
    const uint64_t last = std::min(edge.to, interval_end);
    const uint64_t departure_limit = enumerate_all_departures ? last : first + 1;
    for (uint64_t departure = first; departure < departure_limit; ++departure) {
      if (edge.missing_before_three && departure < 3) continue;
      if (edge.duration > std::numeric_limits<uint64_t>::max() - departure)
        continue;
      const uint64_t next_arrival = departure + edge.duration;
      // Edge and query intervals are half-open.  Waiting is legal in this
      // oracle because all generated vertices are visible for the horizon.
      if (next_arrival >= edge.to || next_arrival >= interval_end) continue;
      path->push_back(edge.edge.edge_id.value);
      const uint64_t initial = depth == 0 ? departure : initial_departure;
      if (edge.target == graph.target) {
        output->push_back({initial, next_arrival, *path});
      } else {
        EnumerateOracle(graph, edge.target, next_arrival, initial, depth + 1,
                        path, output, enumerate_all_departures);
      }
      path->pop_back();
    }
  }
}

std::vector<OracleCandidate> EnumerateOracle(const OracleCase& graph,
                                             bool enumerate_all_departures) {
  std::vector<OracleCandidate> output;
  std::vector<uint64_t> path;
  EnumerateOracle(graph, graph.source, graph.interval.from.value,
                  graph.interval.from.value, 0, &path, &output,
                  enumerate_all_departures);
  return output;
}

const OracleCandidate* OracleEarliest(const std::vector<OracleCandidate>& all) {
  if (all.empty()) return nullptr;
  return &*std::min_element(
      all.begin(), all.end(), [](const OracleCandidate& a,
                                 const OracleCandidate& b) {
        return std::tie(a.arrival, a.departure, a.edges) <
               std::tie(b.arrival, b.departure, b.edges);
      });
}

const OracleCandidate* OracleLatest(const std::vector<OracleCandidate>& all) {
  if (all.empty()) return nullptr;
  return &*std::max_element(
      all.begin(), all.end(), [](const OracleCandidate& a,
                                 const OracleCandidate& b) {
        return std::tie(a.departure, a.arrival, a.edges) <
               std::tie(b.departure, b.arrival, b.edges);
      });
}

const OracleCandidate* OracleFastest(const std::vector<OracleCandidate>& all) {
  if (all.empty()) return nullptr;
  return &*std::min_element(
      all.begin(), all.end(), [](const OracleCandidate& a,
                                 const OracleCandidate& b) {
        const uint64_t ad = a.arrival - a.departure;
        const uint64_t bd = b.arrival - b.departure;
        return std::tie(ad, a.departure, a.arrival, a.edges) <
               std::tie(bd, b.departure, b.arrival, b.edges);
      });
}

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

TEST(TemporalJourneyTest, EarliestArrivalRetainsLabelsAtDifferentHopDepths) {
  JourneyFixture graph;
  graph.Vertex(graph.a); graph.Vertex(graph.b); graph.Vertex(graph.c);
  graph.Vertex(graph.d);
  graph.Edge(EdgeRef{PartId{0}, EdgeId{51}}, graph.a, graph.b, 0, 100);
  graph.Edge(EdgeRef{PartId{0}, EdgeId{52}}, graph.a, graph.c, 0, 100);
  graph.Edge(EdgeRef{PartId{0}, EdgeId{53}}, graph.c, graph.b, 0, 100);
  graph.Edge(EdgeRef{PartId{0}, EdgeId{54}}, graph.b, graph.d, 5, 100);
  auto snapshot = graph.database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  JourneyRequest request{graph.a, graph.d, {ValidTime{0}, ValidTime{100}},
                         JourneyObjective::kEarliestArrival};
  request.max_hops = 2;
  request.duration_at = [](EdgeRef edge, ValidTime)
      -> StatusOr<std::optional<ValidDuration>> {
    uint64_t duration = edge.edge_id.value == 54 ? 1 :
                        (edge.edge_id.value == 51 ? 5 : 1);
    return std::optional<ValidDuration>{ValidDuration{duration}};
  };
  auto journey = EarliestArrival(snapshot.ValueOrDie(), request);
  ASSERT_TRUE(journey.ok()) << journey.status().ToString();
  EXPECT_EQ(journey.ValueOrDie().vertices,
            (std::vector<VertexRef>{graph.a, graph.b, graph.d}));
  EXPECT_EQ(journey.ValueOrDie().final_arrival, (ValidTime{6}));
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

TEST(TemporalJourneyTest, LatestDepartureAcceptsCallbackMissingAtLowerBound) {
  JourneyFixture graph;
  graph.Vertex(graph.a, 0, 20); graph.Vertex(graph.b, 0, 20);
  graph.Edge(EdgeRef{PartId{0}, EdgeId{55}}, graph.a, graph.b, 0, 20);
  auto snapshot = graph.database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  JourneyRequest request{graph.a, graph.b, {ValidTime{0}, ValidTime{20}},
                         JourneyObjective::kLatestDeparture};
  request.arrival_deadline = ValidTime{10};
  request.duration_at = [](EdgeRef, ValidTime time)
      -> StatusOr<std::optional<ValidDuration>> {
    if (time.value < 5) return std::optional<ValidDuration>{};
    return std::optional<ValidDuration>{ValidDuration{1}};
  };
  auto journey = LatestDeparture(snapshot.ValueOrDie(), request);
  ASSERT_TRUE(journey.ok()) << journey.status().ToString();
  EXPECT_EQ(journey.ValueOrDie().initial_departure, (ValidTime{9}));
}

TEST(TemporalJourneyTest, LatestDepartureOnlyRequiresFinalTargetAtArrival) {
  JourneyFixture graph;
  graph.Vertex(graph.a, 0, 20);
  // The destination disappears well before the arrival deadline.  It is
  // still valid at the selected arrival instant and must not be required to
  // remain visible while the reverse search considers earlier departures.
  graph.Vertex(graph.b, 0, 5);
  graph.Edge(EdgeRef{PartId{0}, EdgeId{56}}, graph.a, graph.b, 0, 20);
  auto snapshot = graph.database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  JourneyRequest request{graph.a, graph.b, {ValidTime{0}, ValidTime{20}},
                         JourneyObjective::kLatestDeparture};
  request.arrival_deadline = ValidTime{10};
  request.duration_at = [](EdgeRef, ValidTime)
      -> StatusOr<std::optional<ValidDuration>> {
    return std::optional<ValidDuration>{ValidDuration{1}};
  };
  auto journey = LatestDeparture(snapshot.ValueOrDie(), request);
  ASSERT_TRUE(journey.ok()) << journey.status().ToString();
  EXPECT_EQ(journey.ValueOrDie().initial_departure, (ValidTime{3}));
  EXPECT_EQ(journey.ValueOrDie().final_arrival, (ValidTime{4}));
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

TEST(TemporalJourneyTest, LatestDepartureRejectsWaitingAcrossTargetGap) {
  JourneyFixture graph;
  graph.Vertex(graph.a, 0, 100);
  graph.Vertex(graph.d, 0, 100);
  graph.Commit([&](Transaction& txn) {
    auto status = txn.Assert(EntityFact::Vertex(graph.b), ValidTime{0});
    if (!status.ok()) return status;
    status = txn.Retract(EntityFact::Vertex(graph.b), ValidTime{3});
    if (!status.ok()) return status;
    status = txn.Assert(EntityFact::Vertex(graph.b), ValidTime{16});
    if (!status.ok()) return status;
    return txn.Retract(EntityFact::Vertex(graph.b), ValidTime{20});
  });
  graph.Edge(EdgeRef{PartId{0}, EdgeId{34}}, graph.a, graph.b, 0, 20);
  graph.Edge(EdgeRef{PartId{0}, EdgeId{35}}, graph.b, graph.d, 10, 15);
  auto snapshot = graph.database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  JourneyRequest request{graph.a, graph.d, {ValidTime{0}, ValidTime{20}},
                         JourneyObjective::kLatestDeparture};
  request.arrival_deadline = ValidTime{20};
  request.duration_at = [](EdgeRef, ValidTime)
      -> StatusOr<std::optional<ValidDuration>> {
    return std::optional<ValidDuration>{ValidDuration{1}};
  };
  auto journey = LatestDeparture(snapshot.ValueOrDie(), request);
  ASSERT_FALSE(journey.ok());
  EXPECT_TRUE(journey.status().IsNotFound()) << journey.status().ToString();
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

TEST(TemporalJourneyTest, FastestDurationRetainsLabelsAtDifferentHopDepths) {
  JourneyFixture graph;
  graph.Vertex(graph.a); graph.Vertex(graph.b); graph.Vertex(graph.c);
  graph.Vertex(graph.d);
  graph.Edge(EdgeRef{PartId{0}, EdgeId{61}}, graph.a, graph.b, 0, 100);
  graph.Edge(EdgeRef{PartId{0}, EdgeId{62}}, graph.a, graph.c, 0, 100);
  graph.Edge(EdgeRef{PartId{0}, EdgeId{63}}, graph.c, graph.b, 0, 100);
  graph.Edge(EdgeRef{PartId{0}, EdgeId{64}}, graph.b, graph.d, 5, 100);
  auto snapshot = graph.database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  JourneyRequest request{graph.a, graph.d, {ValidTime{0}, ValidTime{100}},
                         JourneyObjective::kFastestDuration};
  request.max_hops = 2;
  request.duration_at = [](EdgeRef edge, ValidTime)
      -> StatusOr<std::optional<ValidDuration>> {
    return std::optional<ValidDuration>{ValidDuration{
        static_cast<uint64_t>(edge.edge_id.value == 64 ? 1 :
                              (edge.edge_id.value == 61 ? 5 : 1))}};
  };
  auto journey = FastestDuration(snapshot.ValueOrDie(), request);
  ASSERT_TRUE(journey.ok()) << journey.status().ToString();
  EXPECT_EQ(journey.ValueOrDie().vertices,
            (std::vector<VertexRef>{graph.a, graph.b, graph.d}));
  EXPECT_EQ(journey.ValueOrDie().duration, (ValidDuration{6}));
}

TEST(TemporalJourneyTest, IntervalFragmentBudgetAccumulatesAcrossExpansions) {
  JourneyFixture graph;
  graph.Vertex(graph.a); graph.Vertex(graph.b); graph.Vertex(graph.d);
  graph.Edge(EdgeRef{PartId{0}, EdgeId{71}}, graph.a, graph.b, 0, 100);
  graph.Edge(EdgeRef{PartId{0}, EdgeId{72}}, graph.b, graph.d, 0, 100);
  auto snapshot = graph.database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  JourneyRequest request{graph.a, graph.d, {ValidTime{0}, ValidTime{100}},
                         JourneyObjective::kEarliestArrival};
  request.duration_at = [](EdgeRef, ValidTime)
      -> StatusOr<std::optional<ValidDuration>> {
    return std::optional<ValidDuration>{ValidDuration{1}};
  };
  JourneyOptions options;
  options.max_interval_fragments = 1;
  auto journey = EarliestArrival(snapshot.ValueOrDie(), request, options);
  ASSERT_FALSE(journey.ok());
  EXPECT_TRUE(journey.status().IsResourceExhausted())
      << journey.status().ToString();
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

TEST(TemporalJourneyTest, PublicLatestDeparturePlanPreservesInAndBothDirection) {
  JourneyFixture graph;
  const auto source = Slot<VertexRef>::Named("source_direction");
  const auto edge = Slot<EdgeRef>::Named("edge_direction");
  const auto target = Slot<VertexRef>::Named("target_direction");
  const auto journey = Slot<JourneyValue>::Named("journey_direction");
  auto scan = Query::Vertices(
      source, History{ValidTimeInterval{ValidTime{0}, ValidTime{16}}});
  ASSERT_TRUE(scan.ok()) << scan.status().ToString();
  for (ExpandDirection direction : {ExpandDirection::kIn,
                                    ExpandDirection::kBoth}) {
    auto query = scan.ValueOrDie().LatestDeparture(
        ExpandSpec{source, edge, target, direction}, 2, PropertyId{911},
        journey);
    ASSERT_TRUE(query.ok()) << query.status().ToString();
    auto analyzed = AnalyzeQuery(query.ValueOrDie());
    ASSERT_TRUE(analyzed.ok()) << analyzed.status().ToString();
    ASSERT_TRUE(analyzed.ValueOrDie().graph_expand.has_value());
    EXPECT_EQ(analyzed.ValueOrDie().graph_expand->direction, direction);
    EXPECT_EQ(analyzed.ValueOrDie().graph_journey, 2U);
  }
}

TEST(TemporalJourneyTest, ExhaustiveOracleMatchesThreeObjectivesFor200Seeds) {
  JourneyFixture graph;
  constexpr uint32_t kSeeds = 200;
  constexpr uint64_t kHorizon = 16;
  std::mt19937 rng(0xCEDA1234u);
  std::vector<OracleCase> cases;
  cases.reserve(kSeeds);
  for (uint32_t seed = 0; seed < kSeeds; ++seed) {
    OracleCase test;
    test.max_hops = 1 + (rng() % 3);
    const uint64_t vertex_base = 1'000 + static_cast<uint64_t>(seed) * 8;
    for (uint32_t index = 0; index < 4; ++index) {
      test.vertices.push_back(
          VertexRef{PartId{0}, VertexId{vertex_base + index}});
    }
    test.source = test.vertices[0];
    test.target = test.vertices[3];
    test.interval = {ValidTime{0}, ValidTime{kHorizon}};
    const uint64_t edge_base = 10'000 + static_cast<uint64_t>(seed) * 16;
    const uint32_t edge_count = 3 + (rng() % 5);
    for (uint32_t index = 0; index < edge_count; ++index) {
      const uint32_t source = rng() % 4;
      const uint32_t target = (source + 1 + (rng() % 3)) % 4;
      const bool gap = ((seed + index) % 5) == 0;
      test.edges.push_back(OracleEdge{
          EdgeRef{PartId{0}, EdgeId{edge_base + index}},
          test.vertices[source], test.vertices[target],
          static_cast<uint64_t>(gap ? 3 : 0),
          kHorizon, 1 + (rng() % 4), gap});
    }
    // The final seed exercises callback addition overflow independently of
    // the ordinary bounded integer-time cases.
    if (seed == kSeeds - 1) {
      test.overflow = true;
      test.interval = {ValidTime{std::numeric_limits<uint64_t>::max() - 2},
                       ValidTime{std::numeric_limits<uint64_t>::max()}};
      test.vertices.clear();
      for (uint32_t index = 0; index < 2; ++index) {
        test.vertices.push_back(VertexRef{PartId{0},
                                          VertexId{vertex_base + index}});
      }
      test.source = test.vertices[0];
      test.target = test.vertices[1];
      test.edges = {OracleEdge{
          EdgeRef{PartId{0}, EdgeId{edge_base}}, test.source, test.target,
          test.interval.from.value, test.interval.to->value, 2, false}};
    }
    cases.push_back(std::move(test));
  }

  for (const OracleCase& test : cases) {
    graph.Commit([&](Transaction& txn) {
      for (const VertexRef& vertex : test.vertices) {
        auto status = txn.Assert(EntityFact::Vertex(vertex),
                                 test.interval.from);
        if (!status.ok()) return status;
        status = txn.Retract(EntityFact::Vertex(vertex), *test.interval.to);
        if (!status.ok()) return status;
      }
      for (const OracleEdge& edge : test.edges) {
        auto status = txn.Assert(
            EdgeIdentity{edge.edge, edge.source, edge.target, 1},
            ValidTime{edge.from});
        if (!status.ok()) return status;
        status = txn.Retract(EntityFact::Edge(edge.edge), ValidTime{edge.to});
        if (!status.ok()) return status;
      }
      return Status::OK();
    });
  }
  auto snapshot = graph.database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();

  for (uint32_t seed = 0; seed < cases.size(); ++seed) {
    const OracleCase& test = cases[seed];
    std::map<uint64_t, OracleEdge> by_edge;
    for (const OracleEdge& edge : test.edges) by_edge.emplace(edge.edge.edge_id.value, edge);
    auto duration_at = [by_edge](EdgeRef edge, ValidTime time)
        -> StatusOr<std::optional<ValidDuration>> {
      const auto found = by_edge.find(edge.edge_id.value);
      if (found == by_edge.end()) {
        return Status::NotFound("oracle", "unknown edge");
      }
      if (found->second.missing_before_three && time.value < 3)
        return std::optional<ValidDuration>{};
      return std::optional<ValidDuration>{
          ValidDuration{found->second.duration}};
    };
    for (JourneyObjective objective : {JourneyObjective::kEarliestArrival,
                                       JourneyObjective::kLatestDeparture,
                                       JourneyObjective::kFastestDuration}) {
      JourneyRequest request{test.source, test.target, test.interval, objective};
      request.max_hops = test.max_hops;
      request.duration_at = duration_at;
      if (objective == JourneyObjective::kLatestDeparture)
        request.arrival_deadline = test.interval.to;
      StatusOr<JourneyValue> actual = FindJourney(
          snapshot.ValueOrDie(), request);
      if (test.overflow) {
        ASSERT_FALSE(actual.ok()) << "seed " << seed << " objective "
                                  << static_cast<int>(objective);
        EXPECT_TRUE(actual.status().IsNumericOverflow())
            << "seed " << seed << ": " << actual.status().ToString();
        continue;
      }
      const std::vector<OracleCandidate> expected_all = EnumerateOracle(
          test, objective == JourneyObjective::kLatestDeparture);
      const OracleCandidate* expected =
          objective == JourneyObjective::kEarliestArrival
              ? OracleEarliest(expected_all)
              : objective == JourneyObjective::kLatestDeparture
                    ? OracleLatest(expected_all)
                    : OracleFastest(expected_all);
      if (expected == nullptr) {
        ASSERT_FALSE(actual.ok()) << "seed " << seed << " objective "
                                  << static_cast<int>(objective);
        EXPECT_TRUE(actual.status().IsNotFound())
            << "seed " << seed << ": " << actual.status().ToString();
        continue;
      }
      ASSERT_TRUE(actual.ok()) << "seed " << seed << " objective "
                               << static_cast<int>(objective) << ": "
                               << actual.status().ToString();
      const JourneyValue& value = actual.ValueOrDie();
      if (objective == JourneyObjective::kEarliestArrival) {
        EXPECT_EQ(value.final_arrival.value, expected->arrival) << "seed " << seed;
      } else if (objective == JourneyObjective::kLatestDeparture) {
        EXPECT_EQ(value.initial_departure.value, expected->departure)
            << "seed " << seed;
      } else {
        EXPECT_EQ(value.duration.value, expected->arrival - expected->departure)
            << "seed " << seed;
      }
    }
  }
}

}  // namespace
}  // namespace cedar::internal
