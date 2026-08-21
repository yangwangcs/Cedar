#include <gtest/gtest.h>

#include <filesystem>
#include <memory>

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

}  // namespace cedar::internal
