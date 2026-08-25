#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>

#include "cedar/cypher/session.h"
#include "cedar/database.h"

namespace cedar::cypher {
namespace {

TEST(CypherSessionTest, ExecutesWithSessionOwnedSnapshotAndAccessProfile) {
  char path[] = "/tmp/cedar_cypher_session_XXXXXX";
  ASSERT_NE(mkdtemp(path), nullptr);
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok()) << database.status().ToString();
  CypherSession session(*database.ValueOrDie(), SchemaCatalog{});
  auto prepared = session.Prepare(
      "FOR VALID_TIME BETWEEN 1 AND 9 MATCH (v) RETURN v");
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto cursor = session.Execute(prepared.ValueOrDie(), CypherRequest{});
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  EXPECT_TRUE(cursor.ValueOrDie().Next().ok());
  EXPECT_TRUE(cursor.ValueOrDie().Close().ok());
  auto explain = session.Explain(prepared.ValueOrDie());
  ASSERT_TRUE(explain.ok()) << explain.status().ToString();
  EXPECT_EQ(explain.ValueOrDie().profile, AccessProfile::kValidTimeRange);
  EXPECT_EQ(explain.ValueOrDie().mode, QueryExecutionMode::kAnalytical);
  EXPECT_FALSE(explain.ValueOrDie().source.empty());
  if (explain.ValueOrDie().source == "canonical") {
    EXPECT_NE(explain.ValueOrDie().physical.find("canonical_fallback"),
              std::string::npos);
  } else {
    EXPECT_NE(explain.ValueOrDie().physical.find("coverage="), std::string::npos);
  }
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(path);
}

TEST(CypherSessionTest, ExplainClassifiesCanonicalFallbackWithoutProjectionClaim) {
  char path[] = "/tmp/cedar_cypher_session_source_XXXXXX";
  ASSERT_NE(mkdtemp(path), nullptr);
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok());
  CypherSession session(*database.ValueOrDie(), SchemaCatalog{});
  auto prepared = session.Prepare("FOR VALID_TIME AS OF 1 MATCH (v) RETURN v");
  ASSERT_TRUE(prepared.ok());
  auto explain = session.Explain(prepared.ValueOrDie());
  ASSERT_TRUE(explain.ok()) << explain.status().ToString();
  EXPECT_EQ(explain.ValueOrDie().source, "canonical");
  EXPECT_EQ(explain.ValueOrDie().projection_generation, std::nullopt);
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(path);
}

TEST(CypherSessionTest, ExplainClassifiesPlannerCanonicalCoverage) {
  char path[] = "/tmp/cedar_cypher_session_coverage_XXXXXX";
  ASSERT_NE(mkdtemp(path), nullptr);
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok());
  auto tx = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(tx.ok()) << tx.status().ToString();
  auto vertex = database.ValueOrDie()->AllocateVertexId();
  ASSERT_TRUE(vertex.ok()) << vertex.status().ToString();
  ASSERT_TRUE(tx.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, vertex.ValueOrDie()}),
                           ValidTime{1})
                  .ok());
  ASSERT_TRUE(tx.ValueOrDie()->Commit().ok());

  CypherSession session(*database.ValueOrDie(), SchemaCatalog{});
  auto prepared = session.Prepare(
      "FOR VALID_TIME BETWEEN 1 AND 9 MATCH (v) RETURN v");
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto explain = session.Explain(prepared.ValueOrDie());
  ASSERT_TRUE(explain.ok()) << explain.status().ToString();
  EXPECT_EQ(explain.ValueOrDie().source, "canonical");
  EXPECT_NE(explain.ValueOrDie().physical.find("coverage=canonical"),
            std::string::npos);
  EXPECT_EQ(explain.ValueOrDie().projection_generation, std::nullopt);
  EXPECT_EQ(explain.ValueOrDie().projection_base, std::nullopt);
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(path);
}

TEST(CypherSessionTest, AppliesSystemTimeCeilingWithoutPreparedDatabasePointer) {
  char path[] = "/tmp/cedar_cypher_session_system_XXXXXX";
  ASSERT_NE(mkdtemp(path), nullptr);
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok());
  CypherSession session(*database.ValueOrDie(), SchemaCatalog{});
  auto prepared = session.Prepare(
      "FOR SYSTEM_TIME AS OF 3 MATCH (v) RETURN v");
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto cursor = session.Execute(prepared.ValueOrDie(), CypherRequest{});
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  EXPECT_TRUE(cursor.ValueOrDie().Next().ok());
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(path);
}

TEST(CypherSessionTest, AcceptsSystemTimeRangeAtSessionBoundary) {
  char path[] = "/tmp/cedar_cypher_session_range_XXXXXX";
  ASSERT_NE(mkdtemp(path), nullptr);
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok());
  CypherSession session(*database.ValueOrDie(), SchemaCatalog{});
  auto prepared = session.Prepare(
      "FOR SYSTEM_TIME BETWEEN 2 AND 8 MATCH (v) RETURN v");
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto cursor = session.Execute(prepared.ValueOrDie(), CypherRequest{});
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  EXPECT_TRUE(cursor.ValueOrDie().Close().ok());
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(path);
}

TEST(CypherSessionTest, RejectsSystemTimeRangeStartingAfterSnapshot) {
  char path[] = "/tmp/cedar_cypher_session_range_state_XXXXXX";
  ASSERT_NE(mkdtemp(path), nullptr);
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok()) << database.status().ToString();
  auto tx = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(tx.ok()) << tx.status().ToString();
  auto vertex = database.ValueOrDie()->AllocateVertexId();
  ASSERT_TRUE(vertex.ok()) << vertex.status().ToString();
  ASSERT_TRUE(tx.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, vertex.ValueOrDie()}),
                           ValidTime{1})
                  .ok());
  auto committed = tx.ValueOrDie()->Commit();
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  ASSERT_EQ(committed.ValueOrDie().commit_seq, CommitSeq{1});

  CypherSession session(*database.ValueOrDie(), SchemaCatalog{});
  auto prepared = session.Prepare(
      "FOR VALID_TIME AS OF 1 FOR SYSTEM_TIME BETWEEN 2 AND 2 "
      "MATCH (v) RETURN v");
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto cursor = session.Execute(prepared.ValueOrDie(), CypherRequest{});
  ASSERT_FALSE(cursor.ok());
  EXPECT_TRUE(cursor.status().IsInvalidArgument()) << cursor.status().ToString();

  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(path);
}

TEST(CypherSessionTest, ReadsStagedVertexThroughTransactionOverlay) {
  char path[] = "/tmp/cedar_cypher_session_overlay_XXXXXX";
  ASSERT_NE(mkdtemp(path), nullptr);
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok()) << database.status().ToString();
  auto transaction = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  auto vertex = database.ValueOrDie()->AllocateVertexId();
  ASSERT_TRUE(vertex.ok()) << vertex.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{0}, vertex.ValueOrDie()}),
                           ValidTime{1})
                  .ok());
  CypherSession session(*database.ValueOrDie(), SchemaCatalog{});
  auto prepared = session.Prepare("FOR VALID_TIME AS OF 1 MATCH (v) RETURN v");
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto cursor = session.Execute(prepared.ValueOrDie(), *transaction.ValueOrDie());
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  auto batch = cursor.ValueOrDie().Next();
  ASSERT_TRUE(batch.ok()) << batch.status().ToString();
  ASSERT_TRUE(batch.ValueOrDie().has_value());
  EXPECT_EQ(batch.ValueOrDie()->row_count(), 1U);
  auto historical = session.Prepare(
      "FOR VALID_TIME AS OF 1 FOR SYSTEM_TIME AS OF 0 MATCH (v) RETURN v");
  ASSERT_TRUE(historical.ok()) << historical.status().ToString();
  auto historical_cursor =
      session.Execute(historical.ValueOrDie(), *transaction.ValueOrDie());
  ASSERT_TRUE(historical_cursor.ok()) << historical_cursor.status().ToString();
  auto historical_batch = historical_cursor.ValueOrDie().Next();
  ASSERT_TRUE(historical_batch.ok()) << historical_batch.status().ToString();
  EXPECT_FALSE(historical_batch.ValueOrDie().has_value());
  EXPECT_TRUE(transaction.ValueOrDie()->Rollback().ok());
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(path);
}

TEST(CypherSessionTest, ReadsStagedEdgeIdentityThroughTransactionOverlay) {
  char path[] = "/tmp/cedar_cypher_session_edge_overlay_XXXXXX";
  ASSERT_NE(mkdtemp(path), nullptr);
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok());
  auto transaction = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(transaction.ok());
  auto source = database.ValueOrDie()->AllocateVertexId();
  auto target = database.ValueOrDie()->AllocateVertexId();
  auto edge = database.ValueOrDie()->AllocateEdgeId();
  ASSERT_TRUE(source.ok() && target.ok() && edge.ok());
  const VertexRef source_ref{PartId{0}, source.ValueOrDie()};
  const VertexRef target_ref{PartId{0}, target.ValueOrDie()};
  ASSERT_TRUE(transaction.ValueOrDie()->Assert(EntityFact::Vertex(source_ref), ValidTime{1}).ok());
  ASSERT_TRUE(transaction.ValueOrDie()->Assert(EntityFact::Vertex(target_ref), ValidTime{1}).ok());
  ASSERT_TRUE(transaction.ValueOrDie()->Assert(
      EdgeIdentity{EdgeRef{PartId{0}, edge.ValueOrDie()}, source_ref, target_ref,
                   0xd4a9c8114d237029ULL}, ValidTime{1}).ok());
  CypherSession session(*database.ValueOrDie(), SchemaCatalog{});
  auto prepared = session.Prepare(
      "FOR VALID_TIME AS OF 1 MATCH (a)-[e:KNOWS]->(b) RETURN a, e, b");
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto cursor = session.Execute(prepared.ValueOrDie(), *transaction.ValueOrDie());
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  auto batch = cursor.ValueOrDie().Next();
  ASSERT_TRUE(batch.ok()) << batch.status().ToString();
  ASSERT_TRUE(batch.ValueOrDie().has_value());
  EXPECT_EQ(batch.ValueOrDie()->row_count(), 1U);
  EXPECT_TRUE(transaction.ValueOrDie()->Rollback().ok());
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(path);
}

TEST(CypherSessionTest, DoesNotReuseCacheEntryAcrossStatementKinds) {
  char path[] = "/tmp/cedar_cypher_session_cache_XXXXXX";
  ASSERT_NE(mkdtemp(path), nullptr);
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok());
  CypherSession session(*database.ValueOrDie(), SchemaCatalog{});
  auto create = session.Prepare("CREATE (a)");
  auto match = session.Prepare("MATCH (v) RETURN v");
  ASSERT_TRUE(create.ok()) << create.status().ToString();
  ASSERT_TRUE(match.ok()) << match.status().ToString();
  EXPECT_NE(create.ValueOrDie().fingerprint(), match.ValueOrDie().fingerprint());
  EXPECT_EQ(match.ValueOrDie().bound_statement().kind, StatementKind::kRead);
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(path);
}

TEST(CypherSessionTest, BoundedLruEvictsInteriorEntriesWithoutChangingSemantics) {
  char path[] = "/tmp/cedar_cypher_session_lru_XXXXXX";
  ASSERT_NE(mkdtemp(path), nullptr);
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok());
  CypherSession session(*database.ValueOrDie(), SchemaCatalog{});
  for (uint64_t valid_time = 1; valid_time <= 65; ++valid_time) {
    auto prepared = session.Prepare(
        "FOR VALID_TIME AS OF " + std::to_string(valid_time) +
        " MATCH (v) RETURN v");
    ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  }
  auto evicted = session.Prepare("FOR VALID_TIME AS OF 1 MATCH (v) RETURN v");
  ASSERT_TRUE(evicted.ok()) << evicted.status().ToString();
  EXPECT_EQ(evicted.ValueOrDie().bound_statement().valid_time->as_of,
            std::optional<uint64_t>{1});
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(path);
}

}  // namespace
}  // namespace cedar::cypher
