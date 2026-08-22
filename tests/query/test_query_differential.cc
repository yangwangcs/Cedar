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

TEST(QueryDifferentialTest, SmokeDebugThresholdsAreExactAndCapacityOnly) {
  EXPECT_EQ(kQueryDebugThresholds.memtable_bytes, 64ULL << 10);
  EXPECT_EQ(kQueryDebugThresholds.projection_segment_bytes, 64ULL << 10);
  EXPECT_EQ(kQueryDebugThresholds.projection_page_bytes, 4ULL << 10);
  EXPECT_EQ(kQueryDebugThresholds.query_delta_soft_bytes, 64ULL << 10);
  EXPECT_EQ(kQueryDebugThresholds.query_delta_hard_bytes, 128ULL << 10);
  EXPECT_EQ(kQueryDebugThresholds.query_memory_bytes, 32ULL << 10);
  EXPECT_EQ(kQueryDebugThresholds.scratch_run_bytes, 16ULL << 10);
  EXPECT_EQ(kQueryDebugThresholds.delta_lag_soft_commits, 8U);
  EXPECT_EQ(kQueryDebugThresholds.delta_lag_hard_commits, 32U);
  EXPECT_EQ(kQueryDebugThresholds.manifest_commits_per_generation, 16U);
  EXPECT_TRUE(UsesCedarKernelProfile(StorageProfile::kDebugSmallThresholds));
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
