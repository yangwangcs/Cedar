#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <atomic>
#include <barrier>
#include <thread>
#include <vector>

#include "cedar/cypher/session.h"
#include "cedar/database.h"
#include "test_util/sync_point.h"

namespace cedar::cypher {
namespace {

#ifndef NDEBUG
TEST(CypherSessionTest, PreparedHashCollisionDoesNotReuseStatement) {
  char path[] = "/tmp/cedar_cypher_collision_XXXXXX";
  ASSERT_NE(mkdtemp(path), nullptr);
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok()) << database.status().ToString();
  auto transaction = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(transaction.ok());
  for (uint64_t id : {1, 2, 3}) {
    ASSERT_TRUE(transaction.ValueOrDie()->Assert(
        EntityFact::Vertex({PartId{0}, VertexId{id}}), ValidTime{1}).ok());
  }
  ASSERT_TRUE(transaction.ValueOrDie()->Commit().ok());
  auto* sync = rocksdb::SyncPoint::GetInstance();
  struct ResetCallbacks {
    ~ResetCallbacks() {
      auto* sync = rocksdb::SyncPoint::GetInstance();
      sync->DisableProcessing();
      sync->ClearCallBack("CypherSession::Prepare:Fingerprint");
      sync->ClearCallBack("CypherSession::Prepare:Compile");
    }
  } reset;
  size_t compilations = 0;
  sync->SetCallBack("CypherSession::Prepare:Fingerprint", [](void* value) {
    *static_cast<uint64_t*>(value) = 42;
  });
  sync->SetCallBack("CypherSession::Prepare:Compile", [&](void*) { ++compilations; });
  sync->EnableProcessing();
  {
    CypherSession session(*database.ValueOrDie(), SchemaCatalog{});
    const std::string prefix = "FOR VALID_TIME AS OF 1 MATCH (v) RETURN v LIMIT ";
    auto one = session.Prepare(prefix + "1");
    auto two = session.Prepare(prefix + "2");
    ASSERT_TRUE(one.ok() && two.ok());
    EXPECT_EQ(one.ValueOrDie().fingerprint(), 42U);
    EXPECT_EQ(two.ValueOrDie().fingerprint(), 42U);
    EXPECT_NE(one.ValueOrDie().bound_statement().semantic_key,
              two.ValueOrDie().bound_statement().semantic_key);
    EXPECT_EQ(compilations, 2U);
    auto reused = session.Prepare(prefix + "2");
    ASSERT_TRUE(reused.ok());
    EXPECT_EQ(compilations, 2U);
    auto check_rows = [&](const PreparedCypher& prepared, size_t expected) {
      auto cursor = session.Execute(prepared);
      ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
      size_t rows = 0;
      for (;;) {
        auto batch = cursor.ValueOrDie().Next();
        ASSERT_TRUE(batch.ok()) << batch.status().ToString();
        if (!batch.ValueOrDie()) break;
        rows += batch.ValueOrDie()->row_count();
      }
      EXPECT_EQ(rows, expected);
    };
    check_rows(one.ValueOrDie(), 1);
    check_rows(two.ValueOrDie(), 2);
    // All keys still collide diagnostically while exercising bounded eviction.
    for (size_t i = 0; i < 65; ++i) {
      auto prepared = session.Prepare("FOR VALID_TIME AS OF " +
          std::to_string(i + 2) + " MATCH (v) RETURN v");
      ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
    }
    EXPECT_EQ(compilations, 67U);
    auto reloaded = session.Prepare(prefix + "1");
    ASSERT_TRUE(reloaded.ok());
    EXPECT_EQ(compilations, 68U);
    check_rows(reloaded.ValueOrDie(), 1);
    check_rows(two.ValueOrDie(), 2);
  }
  EXPECT_TRUE(database.ValueOrDie()->Close().ok());
  std::filesystem::remove_all(path);
}
#endif

TEST(CypherSessionTest, ConcurrentPreparePreservesCacheEntriesAcrossEviction) {
  char path[] = "/tmp/cedar_cypher_session_concurrent_XXXXXX";
  ASSERT_NE(mkdtemp(path), nullptr);
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok()) << database.status().ToString();
  {
    CypherSession session(*database.ValueOrDie(), SchemaCatalog{});
    std::vector<std::string> sources;
    std::vector<uint64_t> fingerprints;
    for (size_t i = 0; i < 96; ++i) {
      sources.push_back("FOR VALID_TIME AS OF " + std::to_string(i + 1) +
                        " MATCH (v) RETURN v");
      auto prepared = session.Prepare(sources.back());
      ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
      fingerprints.push_back(prepared.ValueOrDie().fingerprint());
    }
    constexpr size_t kWorkers = 8;
    std::barrier start(static_cast<std::ptrdiff_t>(kWorkers));
    std::atomic<size_t> mismatches{0};
    std::vector<std::thread> workers;
    for (size_t worker = 0; worker < kWorkers; ++worker) {
      workers.emplace_back([&, worker] {
        start.arrive_and_wait();
        for (size_t iteration = 0; iteration < 512; ++iteration) {
          // Shared hot keys exercise LRU touches amid distinct-key eviction.
          const size_t index = iteration % 2 == 0
                                   ? 0 : (iteration + worker * 13) % sources.size();
          auto prepared = session.Prepare(sources[index]);
          if (!prepared.ok() ||
              prepared.ValueOrDie().fingerprint() != fingerprints[index]) {
            mismatches.fetch_add(1, std::memory_order_relaxed);
          }
        }
      });
    }
    for (auto& worker : workers) worker.join();
    EXPECT_EQ(mismatches.load(), 0U);
  }
  EXPECT_TRUE(database.ValueOrDie()->Close().ok());
  std::filesystem::remove_all(path);
}

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

TEST(CypherSessionTest, DistinctLimitsKeepTheirBoundValuesAndResults) {
  char path[] = "/tmp/cedar_cypher_session_limits_XXXXXX";
  ASSERT_NE(mkdtemp(path), nullptr);
  auto database = Database::Open(DatabaseOptions{.path = path});
  ASSERT_TRUE(database.ok());
  auto transaction = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(transaction.ok());
  for (uint64_t id : {1ULL, 2ULL}) {
    ASSERT_TRUE(transaction.ValueOrDie()->Assert(
        EntityFact::Vertex(VertexRef{PartId{0}, VertexId{id}}), ValidTime{1}).ok());
  }
  ASSERT_TRUE(transaction.ValueOrDie()->Commit().ok());
  CypherSession session(*database.ValueOrDie(), SchemaCatalog{});
  for (size_t limit : {1U, 2U, 1U}) {
    SCOPED_TRACE(limit);
    auto prepared = session.Prepare(
        "FOR VALID_TIME AS OF 1 MATCH (v) RETURN v LIMIT " +
        std::to_string(limit));
    ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
    EXPECT_EQ(prepared.ValueOrDie().bound_statement().limit_count, limit);
    auto cursor = session.Execute(prepared.ValueOrDie());
    ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
    size_t rows = 0;
    while (true) {
      auto batch = cursor.ValueOrDie().Next();
      ASSERT_TRUE(batch.ok()) << batch.status().ToString();
      if (!batch.ValueOrDie()) break;
      rows += batch.ValueOrDie()->row_count();
    }
    EXPECT_EQ(rows, limit);
    EXPECT_TRUE(cursor.ValueOrDie().Close().ok());
  }
  EXPECT_TRUE(database.ValueOrDie()->Close().ok());
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
