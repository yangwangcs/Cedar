#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <vector>

#include "cedar/database.h"
#include "cedar/transaction.h"
#include "query/runtime/graph_frontier.h"

namespace cedar::internal {

TEST(TemporalExpandTest, TraversalRetainsCrossPartitionIdentity) {
  const TemporalTraversal traversal{
      VertexRef{PartId{1}, VertexId{10}}, EdgeRef{PartId{1}, EdgeId{90}},
      VertexRef{PartId{2}, VertexId{20}}, 7,
      ValidTimeInterval{ValidTime{30}, ValidTime{40}}};
  EXPECT_EQ(traversal.source, (VertexRef{PartId{1}, VertexId{10}}));
  EXPECT_EQ(traversal.edge, (EdgeRef{PartId{1}, EdgeId{90}}));
  EXPECT_EQ(traversal.target, (VertexRef{PartId{2}, VertexId{20}}));
  EXPECT_EQ(traversal.edge_type, 7U);
  EXPECT_EQ(traversal.effective,
            (ValidTimeInterval{ValidTime{30}, ValidTime{40}}));
}

TEST(TemporalExpandTest, IntersectsSourceEdgeAndTargetStateAcrossPartitions) {
  char pattern[] = "/tmp/cedar_temporal_expand_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok()) << database.status().ToString();
  auto commit = [&](auto&& stage) {
    auto txn = database.ValueOrDie()->BeginTransaction();
    ASSERT_TRUE(txn.ok()) << txn.status().ToString();
    ASSERT_TRUE(stage(*txn.ValueOrDie()).ok());
    auto result = txn.ValueOrDie()->Commit();
    ASSERT_TRUE(result.ok()) << result.status().ToString();
  };
  const VertexRef source{PartId{1}, VertexId{10}};
  const VertexRef target{PartId{2}, VertexId{20}};
  const EdgeRef edge{PartId{1}, EdgeId{90}};
  const EdgeIdentity identity{edge, source, target, 7};
  commit([&](Transaction& txn) { return txn.Assert(EntityFact::Vertex(source), ValidTime{10}); });
  commit([&](Transaction& txn) { return txn.Assert(EntityFact::Vertex(source), ValidTime{50}); });
  commit([&](Transaction& txn) { return txn.Retract(EntityFact::Vertex(source), ValidTime{50}); });
  commit([&](Transaction& txn) { return txn.Assert(EntityFact::Vertex(target), ValidTime{20}); });
  commit([&](Transaction& txn) { return txn.Retract(EntityFact::Vertex(target), ValidTime{40}); });
  commit([&](Transaction& txn) { return txn.Assert(identity, ValidTime{25}); });
  commit([&](Transaction& txn) { return txn.Retract(EntityFact::Edge(edge), ValidTime{45}); });
  auto snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  GraphExpansionRequest request{{source}, ValidTimeInterval{ValidTime{0}, ValidTime{100}},
                                ExpandDirection::kOut, 7};
  auto rows = ExpandTemporal(snapshot.ValueOrDie(), request);
  ASSERT_TRUE(rows.ok()) << rows.status().ToString();
  ASSERT_EQ(rows.ValueOrDie().size(), 1U);
  EXPECT_EQ(rows.ValueOrDie().front(),
            (TemporalTraversal{source, edge, target, 7,
                               ValidTimeInterval{ValidTime{25}, ValidTime{40}}}));
  request.direction = ExpandDirection::kIn;
  request.frontier = {target};
  rows = ExpandTemporal(snapshot.ValueOrDie(), request);
  ASSERT_TRUE(rows.ok());
  ASSERT_EQ(rows.ValueOrDie().size(), 1U);
  EXPECT_EQ(rows.ValueOrDie().front().source, source);
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(path);
}

TEST(TemporalExpandTest, PublicExpandExecutesFrontierRows) {
  char pattern[] = "/tmp/cedar_temporal_expand_query_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok());
  const VertexRef source{PartId{0}, VertexId{10}};
  const VertexRef target{PartId{0}, VertexId{20}};
  const EdgeRef edge{PartId{0}, EdgeId{90}};
  const EdgeIdentity identity{edge, source, target, 7};
  auto txn = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(txn.ok());
  ASSERT_TRUE(txn.ValueOrDie()->Assert(EntityFact::Vertex(source), ValidTime{0}).ok());
  ASSERT_TRUE(txn.ValueOrDie()->Assert(EntityFact::Vertex(target), ValidTime{0}).ok());
  ASSERT_TRUE(txn.ValueOrDie()->Assert(identity, ValidTime{0}).ok());
  ASSERT_TRUE(txn.ValueOrDie()->Commit().ok());
  const auto vertex = Slot<VertexRef>::Named("vertex");
  const auto edge_slot = Slot<EdgeRef>::Named("edge");
  const auto destination = Slot<VertexRef>::Named("destination");
  auto source_query = Query::Vertices(vertex, At{ValidTime{1}});
  ASSERT_TRUE(source_query.ok());
  auto expanded = source_query.ValueOrDie().Expand(
      ExpandSpec{vertex, edge_slot, destination, ExpandDirection::kOut});
  ASSERT_TRUE(expanded.ok());
  auto prepared = database.ValueOrDie()->PrepareQuery(expanded.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  auto batch = cursor.ValueOrDie().Next();
  ASSERT_TRUE(batch.ok()) << batch.status().ToString();
  ASSERT_TRUE(batch.ValueOrDie().has_value());
  ASSERT_EQ(batch.ValueOrDie()->row_count(), 1U);
  EXPECT_EQ(batch.ValueOrDie()->Get(vertex, 0), source);
  EXPECT_EQ(batch.ValueOrDie()->Get(edge_slot, 0), edge);
  EXPECT_EQ(batch.ValueOrDie()->Get(destination, 0), target);
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(path);
}

TEST(TemporalExpandTest, AnalyticalGraphFallbackAllowsLargeCanonicalFamily) {
  char pattern[] = "/tmp/cedar_temporal_expand_analytical_fallback_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok()) << database.status().ToString();

  const VertexRef source{PartId{0}, VertexId{1}};
  auto txn = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(txn.ok()) << txn.status().ToString();
  ASSERT_TRUE(txn.ValueOrDie()
                  ->Assert(EntityFact::Vertex(source), ValidTime{0})
                  .ok());
  constexpr uint64_t kEdges = 4097;
  std::vector<EdgeIdentity> identities;
  identities.reserve(kEdges);
  for (uint64_t i = 0; i < kEdges; ++i) {
    const VertexRef target{PartId{0}, VertexId{i + 2}};
    const EdgeRef edge{PartId{0}, EdgeId{i + 2}};
    ASSERT_TRUE(txn.ValueOrDie()
                    ->Assert(EntityFact::Vertex(target), ValidTime{0})
                    .ok());
    const EdgeIdentity identity{edge, source, target, 7};
    identities.push_back(identity);
    ASSERT_TRUE(txn.ValueOrDie()->Assert(identity, ValidTime{0}).ok());
  }
  ASSERT_TRUE(txn.ValueOrDie()->Commit().ok());

  auto snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  QueryDeltaView delta{CommitSeq{0}, snapshot.ValueOrDie().commit_seq(),
                       {}, identities, {}, CommitSeq{}};
  GraphExpansionRequest request{{source}, ValidTimeInterval{ValidTime{0}, ValidTime{10}},
                                ExpandDirection::kOut, 7};
  QueryReservation reservation(64ULL << 20);
  GraphFrontierOptions analytical{&reservation, &delta, 1};
  // Analytical graph execution leaves the canonical fallback unlimited even
  // when it has an active resource reservation.
  analytical.fallback_candidate_limit = 0;
  uint64_t candidates_examined = 0;
  analytical.candidates_examined = &candidates_examined;
  auto rows = ExpandTemporal(snapshot.ValueOrDie(), request, analytical);
  ASSERT_TRUE(rows.ok()) << rows.status().ToString();
  EXPECT_GE(candidates_examined, kEdges);
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(path);
}

TEST(TemporalExpandTest, KHopDeduplicatesAtMinimumDepthAndHonorsBound) {
  char pattern[] = "/tmp/cedar_temporal_khop_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok());
  const VertexRef one{PartId{0}, VertexId{1}};
  const VertexRef two{PartId{0}, VertexId{2}};
  const VertexRef three{PartId{0}, VertexId{3}};
  const EdgeIdentity first{EdgeRef{PartId{0}, EdgeId{11}}, one, two, 1};
  const EdgeIdentity second{EdgeRef{PartId{0}, EdgeId{12}}, two, three, 1};
  auto add = [&](const EdgeIdentity& identity) {
    auto txn = database.ValueOrDie()->BeginTransaction();
    EXPECT_TRUE(txn.ok());
    EXPECT_TRUE(txn.ValueOrDie()->Assert(EntityFact::Vertex(identity.source_ref()), ValidTime{0}).ok());
    EXPECT_TRUE(txn.ValueOrDie()->Assert(EntityFact::Vertex(identity.target_ref()), ValidTime{0}).ok());
    EXPECT_TRUE(txn.ValueOrDie()->Assert(identity, ValidTime{0}).ok());
    EXPECT_TRUE(txn.ValueOrDie()->Commit().ok());
  };
  add(first);
  add(second);
  auto snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  const QueryDeltaView delta{CommitSeq{0}, snapshot.ValueOrDie().commit_seq(),
                             {}, {first, second}, {}, CommitSeq{}};
  GraphExpansionRequest request{{one}, ValidTimeInterval{ValidTime{0}, ValidTime{10}},
                                ExpandDirection::kOut, std::nullopt};
  uint64_t candidates = 0;
  GraphFrontierOptions bounded{nullptr, &delta, 2};
  bounded.adjacency_seek = [&delta](const std::vector<VertexRef>&,
                                    ExpandDirection,
                                    std::optional<uint64_t>) {
    return StatusOr<std::vector<EdgeIdentity>>(delta.edge_identities);
  };
  bounded.candidates_examined = &candidates;
  auto result = KHopExpand(snapshot.ValueOrDie(), request, bounded);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  ASSERT_EQ(result.ValueOrDie().labels.size(), 2U);
  EXPECT_EQ(result.ValueOrDie().labels[0].vertex, two);
  EXPECT_EQ(result.ValueOrDie().labels[0].depth, 1U);
  EXPECT_EQ(result.ValueOrDie().labels[1].vertex, three);
  EXPECT_EQ(result.ValueOrDie().labels[1].depth, 2U);
  EXPECT_EQ(candidates, 4U);
  result = KHopExpand(snapshot.ValueOrDie(), request,
                      GraphFrontierOptions{nullptr, &delta, 1});
  ASSERT_TRUE(result.ok());
  ASSERT_EQ(result.ValueOrDie().labels.size(), 1U);
  EXPECT_EQ(result.ValueOrDie().labels.front().vertex, two);
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(path);
}

TEST(TemporalExpandTest, AdjacencyIndexBoundsSelectedPostingAndPreservesIdentity) {
  const VertexRef selected{PartId{7}, VertexId{42}};
  std::vector<EdgeIdentity> identities;
  identities.reserve(100000);
  for (uint64_t i = 0; i < 99990; ++i) {
    identities.emplace_back(EdgeRef{PartId{1}, EdgeId{i + 1}},
                            VertexRef{PartId{1}, VertexId{1000 + i}},
                            VertexRef{PartId{2}, VertexId{2000 + i}}, 7);
  }
  for (uint64_t i = 0; i < 10; ++i) {
    identities.emplace_back(EdgeRef{PartId{7}, EdgeId{900000 + i}}, selected,
                            VertexRef{PartId{8}, VertexId{100 + i}}, 7);
  }
  identities.emplace_back(EdgeRef{PartId{7}, EdgeId{910000}}, selected, selected,
                          9);
  identities.emplace_back(EdgeRef{PartId{7}, EdgeId{910001}}, selected, selected,
                          9);
  QueryDeltaView delta{CommitSeq{0}, CommitSeq{7}, {}, identities, {}, CommitSeq{}};
  auto index = std::make_shared<AdjacencyIndex>();
  ASSERT_TRUE(index->ApplyDelta(delta, 3).ok());

  auto outgoing = index->Seek({selected}, ExpandDirection::kOut, 7,
                              CommitSeq{7}, 3, nullptr);
  ASSERT_TRUE(outgoing.ok()) << outgoing.status().ToString();
  EXPECT_EQ(outgoing.ValueOrDie().size(), 10U);
  auto incoming = index->Seek({selected}, ExpandDirection::kIn, 7,
                              CommitSeq{7}, 3, nullptr);
  ASSERT_TRUE(incoming.ok());
  EXPECT_TRUE(incoming.ValueOrDie().empty());
  auto both = index->Seek({selected}, ExpandDirection::kBoth, 9,
                          CommitSeq{7}, 3, nullptr);
  ASSERT_TRUE(both.ok());
  ASSERT_EQ(both.ValueOrDie().size(), 2U);
  EXPECT_NE(both.ValueOrDie()[0].edge_ref(), both.ValueOrDie()[1].edge_ref());
}

TEST(TemporalExpandTest, KHopBothUsesFrontierEndpointAndDeduplicatesDiamond) {
  char pattern[] = "/tmp/cedar_temporal_khop_both_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok());
  const VertexRef one{PartId{0}, VertexId{1}};
  const VertexRef two{PartId{0}, VertexId{2}};
  const VertexRef three{PartId{0}, VertexId{3}};
  const VertexRef four{PartId{0}, VertexId{4}};
  const std::vector<VertexRef> vertices{one, two, three, four};
  const EdgeIdentity e12{EdgeRef{PartId{0}, EdgeId{12}}, one, two, 1};
  const EdgeIdentity e13{EdgeRef{PartId{0}, EdgeId{13}}, one, three, 1};
  const EdgeIdentity e24{EdgeRef{PartId{0}, EdgeId{24}}, two, four, 1};
  const EdgeIdentity e34{EdgeRef{PartId{0}, EdgeId{34}}, three, four, 1};
  auto txn = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(txn.ok());
  for (const auto& vertex : vertices)
    ASSERT_TRUE(txn.ValueOrDie()->Assert(EntityFact::Vertex(vertex), ValidTime{0}).ok());
  for (const auto& edge : {e12, e13, e24, e34})
    ASSERT_TRUE(txn.ValueOrDie()->Assert(edge, ValidTime{0}).ok());
  ASSERT_TRUE(txn.ValueOrDie()->Commit().ok());
  auto snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  const QueryDeltaView delta{CommitSeq{0}, snapshot.ValueOrDie().commit_seq(),
                             {}, {e12, e13, e24, e34}, {}, CommitSeq{}};
  GraphFrontierOptions options{nullptr, &delta, 1};
  options.adjacency_seek = [&delta](const std::vector<VertexRef>&,
                                    ExpandDirection,
                                    std::optional<uint64_t>) {
    return StatusOr<std::vector<EdgeIdentity>>(delta.edge_identities);
  };
  GraphExpansionRequest incoming{{two}, ValidTimeInterval{ValidTime{0}, ValidTime{10}},
                                 ExpandDirection::kBoth, std::nullopt};
  auto result = KHopExpand(snapshot.ValueOrDie(), incoming, options);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  ASSERT_EQ(result.ValueOrDie().labels.size(), 2U);
  EXPECT_EQ(result.ValueOrDie().labels[0].vertex, one);
  EXPECT_EQ(result.ValueOrDie().labels[0].predecessor, two);
  EXPECT_EQ(result.ValueOrDie().labels[1].vertex, four);
  EXPECT_EQ(result.ValueOrDie().labels[1].predecessor, two);

  options.max_hops = 2;
  GraphExpansionRequest diamond{{one}, ValidTimeInterval{ValidTime{0}, ValidTime{10}},
                                ExpandDirection::kOut, std::nullopt};
  result = KHopExpand(snapshot.ValueOrDie(), diamond, options);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  ASSERT_EQ(result.ValueOrDie().labels.size(), 3U);
  size_t four_count = 0;
  for (const auto& label : result.ValueOrDie().labels) {
    if (label.vertex == four) {
      ++four_count;
      EXPECT_EQ(label.depth, 2U);
    }
  }
  EXPECT_EQ(four_count, 1U);
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(path);
}

TEST(TemporalExpandTest, KHopCarriesCommonIntervalAcrossLayers) {
  char pattern[] = "/tmp/cedar_temporal_khop_interval_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok());
  const VertexRef one{PartId{0}, VertexId{1}};
  const VertexRef two{PartId{0}, VertexId{2}};
  const VertexRef three{PartId{0}, VertexId{3}};
  const EdgeIdentity first{EdgeRef{PartId{0}, EdgeId{12}}, one, two, 1};
  const EdgeIdentity second{EdgeRef{PartId{0}, EdgeId{23}}, two, three, 1};
  auto txn = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(txn.ok());
  for (const auto& vertex : {one, two, three})
    ASSERT_TRUE(txn.ValueOrDie()->Assert(EntityFact::Vertex(vertex), ValidTime{0}).ok());
  ASSERT_TRUE(txn.ValueOrDie()->Assert(first, ValidTime{0}).ok());
  ASSERT_TRUE(txn.ValueOrDie()->Retract(EntityFact::Edge(first.edge_ref()), ValidTime{10}).ok());
  ASSERT_TRUE(txn.ValueOrDie()->Assert(second, ValidTime{5}).ok());
  ASSERT_TRUE(txn.ValueOrDie()->Retract(EntityFact::Edge(second.edge_ref()), ValidTime{15}).ok());
  ASSERT_TRUE(txn.ValueOrDie()->Commit().ok());
  auto snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  const QueryDeltaView delta{CommitSeq{0}, snapshot.ValueOrDie().commit_seq(),
                             {}, {first, second}, {}, CommitSeq{}};
  GraphFrontierOptions options{nullptr, &delta, 2};
  options.adjacency_seek = [&delta](const std::vector<VertexRef>&,
                                    ExpandDirection,
                                    std::optional<uint64_t>) {
    return StatusOr<std::vector<EdgeIdentity>>(delta.edge_identities);
  };
  GraphExpansionRequest request{{one}, ValidTimeInterval{ValidTime{0}, ValidTime{20}},
                                ExpandDirection::kOut, std::nullopt};
  auto result = KHopExpand(snapshot.ValueOrDie(), request, options);
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  ASSERT_EQ(result.ValueOrDie().labels.size(), 2U);
  EXPECT_EQ(result.ValueOrDie().labels[1].vertex, three);
  EXPECT_EQ(result.ValueOrDie().labels[1].effective,
            (ValidTimeInterval{ValidTime{5}, ValidTime{10}}));
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(path);
}

TEST(TemporalExpandTest, AdjacencyGenerationMismatchFallsBackInsteadOfEmpty) {
  const VertexRef source{PartId{0}, VertexId{1}};
  const VertexRef target{PartId{0}, VertexId{2}};
  const EdgeIdentity identity{EdgeRef{PartId{0}, EdgeId{12}}, source, target, 1};
  QueryDeltaView delta{CommitSeq{0}, CommitSeq{7}, {}, {identity}, {}, CommitSeq{}};
  auto index = std::make_shared<AdjacencyIndex>();
  ASSERT_TRUE(index->ApplyDelta(delta, 3).ok());
  auto result = index->Seek({source}, ExpandDirection::kOut, std::nullopt,
                            CommitSeq{7}, 4, nullptr);
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsNotFound());
  ASSERT_TRUE(index->ApplyDelta(delta, 4).ok());
  result = index->Seek({source}, ExpandDirection::kOut, std::nullopt,
                       CommitSeq{7}, 4, nullptr);
  ASSERT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsNotFound());
}

TEST(TemporalExpandTest, GraphMaterializesSourceEdgeAndDestinationProperties) {
  char pattern[] = "/tmp/cedar_temporal_graph_properties_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok());
  ASSERT_TRUE(database.ValueOrDie()
                  ->RegisterProperty(PropertyDefinition{
                      PropertyId{7}, 0, "source_score", PropertyEntityKind::kVertex,
                      PhysicalType::kInt64, 4096})
                  .ok());
  ASSERT_TRUE(database.ValueOrDie()
                  ->RegisterProperty(PropertyDefinition{
                      PropertyId{8}, 0, "weight", PropertyEntityKind::kEdge,
                      PhysicalType::kInt64, 4096})
                  .ok());
  ASSERT_TRUE(database.ValueOrDie()
                  ->RegisterProperty(PropertyDefinition{
                      PropertyId{9}, 0, "destination_score", PropertyEntityKind::kVertex,
                      PhysicalType::kInt64, 4096})
                  .ok());
  const VertexRef source{PartId{0}, VertexId{1}};
  const VertexRef target{PartId{0}, VertexId{2}};
  const EdgeRef edge{PartId{0}, EdgeId{12}};
  auto txn = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(txn.ok());
  ASSERT_TRUE(txn.ValueOrDie()->Assert(EntityFact::Vertex(source), ValidTime{0}).ok());
  ASSERT_TRUE(txn.ValueOrDie()->Assert(EntityFact::Vertex(target), ValidTime{0}).ok());
  ASSERT_TRUE(txn.ValueOrDie()->Assert(EdgeIdentity{edge, source, target, 1},
                                       ValidTime{0}).ok());
  ASSERT_TRUE(txn.ValueOrDie()->Set(PropertyFact::Vertex(source, PropertyId{7}),
                                    ValidTime{0}, Value::Int64(11)).ok());
  ASSERT_TRUE(txn.ValueOrDie()->Set(PropertyFact::Edge(edge, PropertyId{8}),
                                    ValidTime{0}, Value::Int64(22)).ok());
  ASSERT_TRUE(txn.ValueOrDie()->Set(PropertyFact::Vertex(target, PropertyId{9}),
                                    ValidTime{0}, Value::Int64(33)).ok());
  ASSERT_TRUE(txn.ValueOrDie()->Commit().ok());
  Slot<VertexRef> source_slot = Slot<VertexRef>::Named("source");
  Slot<EdgeRef> edge_slot = Slot<EdgeRef>::Named("edge");
  Slot<VertexRef> target_slot = Slot<VertexRef>::Named("target");
  OptionalSlot<int64_t> source_value = OptionalSlot<int64_t>::Named("source_value");
  OptionalSlot<int64_t> edge_value = OptionalSlot<int64_t>::Named("edge_value");
  OptionalSlot<int64_t> target_value = OptionalSlot<int64_t>::Named("target_value");
  auto source_query = Query::Vertices(
      source_slot, History{ValidTimeInterval{ValidTime{0}, ValidTime{10}}});
  ASSERT_TRUE(source_query.ok());
  auto expanded = source_query.ValueOrDie().Expand(
      ExpandSpec{source_slot, edge_slot, target_slot, ExpandDirection::kOut});
  ASSERT_TRUE(expanded.ok());
  auto with_source = expanded.ValueOrDie().BindVertexProperty(
      source_slot, PropertyId{7}, source_value);
  ASSERT_TRUE(with_source.ok());
  auto with_edge = with_source.ValueOrDie().BindEdgeProperty(
      edge_slot, PropertyId{8}, edge_value);
  ASSERT_TRUE(with_edge.ok());
  auto with_target = with_edge.ValueOrDie().BindVertexProperty(
      target_slot, PropertyId{9}, target_value);
  ASSERT_TRUE(with_target.ok());
  auto query = with_target.ValueOrDie().Select(
      {Project(source_slot), Project(edge_slot), Project(target_slot),
       Project(source_value), Project(edge_value), Project(target_value)});
  ASSERT_TRUE(query.ok());
  auto prepared = database.ValueOrDie()->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  auto batch = cursor.ValueOrDie().Next();
  ASSERT_TRUE(batch.ok()) << batch.status().ToString();
  ASSERT_TRUE(batch.ValueOrDie().has_value());
  ASSERT_EQ(batch.ValueOrDie()->row_count(), 1U);
  EXPECT_EQ(batch.ValueOrDie()->Get<int64_t>(source_value, 0), 11);
  EXPECT_EQ(batch.ValueOrDie()->Get<int64_t>(edge_value, 0), 22);
  EXPECT_EQ(batch.ValueOrDie()->Get<int64_t>(target_value, 0), 33);
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(path);
}

TEST(TemporalExpandTest,
     ConnectedSequenceMaterializesLaterSegmentPropertiesAndMetadata) {
  char pattern[] = "/tmp/cedar_temporal_graph_sequence_properties_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok()) << database.status().ToString();
  ASSERT_TRUE(database.ValueOrDie()
                  ->RegisterProperty(PropertyDefinition{
                      PropertyId{20}, 0, "destination_score",
                      PropertyEntityKind::kVertex, PhysicalType::kInt64, 4096})
                  .ok());
  const VertexRef a{PartId{0}, VertexId{1}};
  const VertexRef b{PartId{0}, VertexId{2}};
  const VertexRef c{PartId{0}, VertexId{3}};
  const EdgeRef e{PartId{0}, EdgeId{20}};
  const EdgeRef f{PartId{0}, EdgeId{21}};
  auto transaction = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(transaction.ok());
  for (const VertexRef vertex : {a, b, c}) {
    ASSERT_TRUE(transaction.ValueOrDie()
                    ->Assert(EntityFact::Vertex(vertex), ValidTime{0})
                    .ok());
  }
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EdgeIdentity{e, a, b, 1}, ValidTime{0})
                  .ok());
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EdgeIdentity{f, b, c, 2}, ValidTime{0})
                  .ok());
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Set(PropertyFact::Vertex(c, PropertyId{20}), ValidTime{0},
                        Value::Int64(303))
                  .ok());
  ASSERT_TRUE(transaction.ValueOrDie()->Commit().ok());

  Slot<VertexRef> a_slot = Slot<VertexRef>::Named("a");
  Slot<EdgeRef> e_slot = Slot<EdgeRef>::Named("e");
  Slot<VertexRef> b_slot = Slot<VertexRef>::Named("b");
  Slot<EdgeRef> f_slot = Slot<EdgeRef>::Named("f");
  Slot<VertexRef> c_slot = Slot<VertexRef>::Named("c");
  OptionalSlot<int64_t> c_score = OptionalSlot<int64_t>::Named("c_score");
  Slot<ValidTime> c_valid_from = Slot<ValidTime>::Named("c_valid_from");
  auto source = Query::Vertices(a_slot, At{ValidTime{1}});
  ASSERT_TRUE(source.ok());
  auto first = source.ValueOrDie().Expand(
      ExpandSpec{a_slot, e_slot, b_slot, ExpandDirection::kOut, 1});
  ASSERT_TRUE(first.ok());
  auto second = first.ValueOrDie().Expand(
      ExpandSpec{b_slot, f_slot, c_slot, ExpandDirection::kOut, 2});
  ASSERT_TRUE(second.ok());
  auto with_property = second.ValueOrDie().BindVertexProperty(
      c_slot, PropertyId{20}, c_score);
  ASSERT_TRUE(with_property.ok());
  auto with_metadata = with_property.ValueOrDie().ProjectMetadata(
      c_slot.id(), MetadataKind::kValidFrom, Project(c_valid_from));
  ASSERT_TRUE(with_metadata.ok());
  auto query = with_metadata.ValueOrDie().Select(
      {Project(a_slot), Project(e_slot), Project(b_slot), Project(f_slot),
       Project(c_slot), Project(c_score), Project(c_valid_from)});
  ASSERT_TRUE(query.ok());
  auto prepared = database.ValueOrDie()->PrepareQuery(query.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  auto batch = cursor.ValueOrDie().Next();
  ASSERT_TRUE(batch.ok()) << batch.status().ToString();
  ASSERT_TRUE(batch.ValueOrDie().has_value());
  ASSERT_EQ(batch.ValueOrDie()->row_count(), 1U);
  EXPECT_EQ(batch.ValueOrDie()->Get<VertexRef>(c_slot, 0), c);
  EXPECT_EQ(batch.ValueOrDie()->Get<EdgeRef>(f_slot, 0), f);
  EXPECT_EQ(batch.ValueOrDie()->Get<int64_t>(c_score, 0), 303);
  EXPECT_EQ(batch.ValueOrDie()->Get<ValidTime>(c_valid_from, 0), ValidTime{0});
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(path);
}

TEST(TemporalExpandTest, SourcePropertyPredicateDoesNotDuplicateTemporalExpansion) {
  char pattern[] = "/tmp/cedar_temporal_expand_property_predicate_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  const std::string path = pattern;
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok()) << database.status().ToString();
  ASSERT_TRUE(database.ValueOrDie()->RegisterProperty(PropertyDefinition{
      PropertyId{7}, 0, "score", PropertyEntityKind::kVertex,
      PhysicalType::kInt64, 4096}).ok());
  const VertexRef source{PartId{0}, VertexId{1}};
  const VertexRef target{PartId{0}, VertexId{2}};
  const EdgeRef edge{PartId{0}, EdgeId{9}};
  auto transaction = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(transaction.ok());
  ASSERT_TRUE(transaction.ValueOrDie()->Assert(EntityFact::Vertex(source),
                                               ValidTime{0}).ok());
  ASSERT_TRUE(transaction.ValueOrDie()->Assert(EntityFact::Vertex(target),
                                               ValidTime{0}).ok());
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EdgeIdentity{edge, source, target, 1}, ValidTime{0})
                  .ok());
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Set(PropertyFact::Vertex(source, PropertyId{7}),
                        ValidTime{0}, Value::Int64(11))
                  .ok());
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Set(PropertyFact::Vertex(source, PropertyId{7}),
                        ValidTime{5}, Value::Int64(22))
                  .ok());
  ASSERT_TRUE(transaction.ValueOrDie()->Commit().ok());

  Slot<VertexRef> source_slot = Slot<VertexRef>::Named("source_predicate");
  Slot<EdgeRef> edge_slot = Slot<EdgeRef>::Named("edge_predicate");
  Slot<VertexRef> target_slot = Slot<VertexRef>::Named("target_predicate");
  OptionalSlot<int64_t> score = OptionalSlot<int64_t>::Named("score_predicate");
  auto source_query = Query::Vertices(
      source_slot, History{ValidTimeInterval{ValidTime{0}, ValidTime{10}}});
  ASSERT_TRUE(source_query.ok());
  auto expanded = source_query.ValueOrDie().Expand(
      ExpandSpec{source_slot, edge_slot, target_slot, ExpandDirection::kOut});
  ASSERT_TRUE(expanded.ok());
  auto bound = expanded.ValueOrDie().BindVertexProperty(
      source_slot, PropertyId{7}, score);
  ASSERT_TRUE(bound.ok());
  auto filtered = bound.ValueOrDie().Where(
      GreaterThan(ValueOf(score), Literal<int64_t>(0)));
  ASSERT_TRUE(filtered.ok());
  auto projected = filtered.ValueOrDie().Select(
      {Project(source_slot), Project(score)});
  ASSERT_TRUE(projected.ok());
  auto prepared = database.ValueOrDie()->PrepareQuery(projected.ValueOrDie());
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  auto batch = cursor.ValueOrDie().Next();
  ASSERT_TRUE(batch.ok()) << batch.status().ToString();
  ASSERT_TRUE(batch.ValueOrDie().has_value());
  EXPECT_EQ(batch.ValueOrDie()->row_count(), 2U);
  EXPECT_EQ(batch.ValueOrDie()->Get<int64_t>(score, 0), 11);
  EXPECT_EQ(batch.ValueOrDie()->Get<int64_t>(score, 1), 22);
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(path);
}

}  // namespace cedar::internal
