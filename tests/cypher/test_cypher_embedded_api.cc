#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>

#include "cedar/cypher.h"
#include "cedar/cypher/session.h"
#include "cedar/database.h"

namespace cedar::cypher {
namespace {

TEST(CypherEmbeddedApiTest, PreparesAndExecutesThroughDatabaseRuntime) {
  char pattern[] = "/tmp/cedar_cypher_api_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  DatabaseOptions options;
  options.path = pattern;
  auto database = Database::Open(std::move(options));
  ASSERT_TRUE(database.ok()) << database.status().ToString();

  auto prepared = PrepareCypher(*database.ValueOrDie(),
                                "MATCH (v) RETURN v", SchemaCatalog{});
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{}, QueryOptions{});
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  EXPECT_TRUE(cursor.ValueOrDie().Next().ok());
  EXPECT_TRUE(cursor.ValueOrDie().Close().ok());
  EXPECT_TRUE(database.ValueOrDie()->Close().ok());
  std::filesystem::remove_all(pattern);
}

TEST(CypherEmbeddedApiTest, ExposesFingerprintWithoutQueryText) {
  char pattern[] = "/tmp/cedar_cypher_api_fp_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  DatabaseOptions options;
  options.path = pattern;
  auto database = Database::Open(std::move(options));
  ASSERT_TRUE(database.ok());
  auto prepared = PrepareCypher(*database.ValueOrDie(),
                                "MATCH (v) RETURN v", SchemaCatalog{});
  ASSERT_TRUE(prepared.ok());
  EXPECT_NE(prepared.ValueOrDie().fingerprint(), 0U);
  EXPECT_EQ(prepared.ValueOrDie().source_text(), "");
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(pattern);
}

TEST(CypherEmbeddedApiTest, AppliesSystemTimeAsSnapshotCeiling) {
  char pattern[] = "/tmp/cedar_cypher_system_ceiling_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  auto database = Database::Open(DatabaseOptions{.path = pattern});
  ASSERT_TRUE(database.ok()) << database.status().ToString();
  auto first = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(first.ok());
  auto v1 = database.ValueOrDie()->AllocateVertexId();
  ASSERT_TRUE(v1.ok());
  ASSERT_TRUE(first.ValueOrDie()->Assert(
      EntityFact::Vertex(VertexRef{PartId{0}, v1.ValueOrDie()}), ValidTime{1}).ok());
  ASSERT_TRUE(first.ValueOrDie()->Commit().ok());
  auto second = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(second.ok());
  auto v2 = database.ValueOrDie()->AllocateVertexId();
  ASSERT_TRUE(v2.ok());
  ASSERT_TRUE(second.ValueOrDie()->Assert(
      EntityFact::Vertex(VertexRef{PartId{0}, v2.ValueOrDie()}), ValidTime{1}).ok());
  ASSERT_TRUE(second.ValueOrDie()->Commit().ok());

  CypherSession session(*database.ValueOrDie(), SchemaCatalog{});
  auto prepared = session.Prepare(
      "FOR SYSTEM_TIME AS OF 1 FOR VALID_TIME AS OF 1 MATCH (v) RETURN v");
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  auto cursor = session.Execute(prepared.ValueOrDie(), CypherRequest{});
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  size_t rows = 0;
  while (true) {
    auto batch = cursor.ValueOrDie().Next();
    ASSERT_TRUE(batch.ok()) << batch.status().ToString();
    if (!batch.ValueOrDie().has_value()) break;
    rows += batch.ValueOrDie()->row_count();
  }
  EXPECT_EQ(rows, 1U);
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(pattern);
}

TEST(CypherEmbeddedApiTest, ReturnsCanonicalFactMetadataColumns) {
  char pattern[] = "/tmp/cedar_cypher_metadata_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  auto database = Database::Open(DatabaseOptions{.path = pattern});
  ASSERT_TRUE(database.ok());
  auto tx = database.ValueOrDie()->BeginTransaction();
  ASSERT_TRUE(tx.ok());
  auto vertex = database.ValueOrDie()->AllocateVertexId();
  ASSERT_TRUE(vertex.ok());
  ASSERT_TRUE(tx.ValueOrDie()->Assert(
      EntityFact::Vertex(VertexRef{PartId{0}, vertex.ValueOrDie()}), ValidTime{7}).ok());
  ASSERT_TRUE(tx.ValueOrDie()->Commit().ok());
  auto prepared = PrepareCypher(
      *database.ValueOrDie(),
      "FOR VALID_TIME AS OF 7 MATCH (v) RETURN v, valid_from(v), commit_seq(v)",
      SchemaCatalog{});
  ASSERT_TRUE(prepared.ok()) << prepared.status().ToString();
  auto snapshot = database.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  auto cursor = prepared.ValueOrDie().Execute(
      std::move(snapshot).ConsumeValueOrDie(), Bindings{});
  ASSERT_TRUE(cursor.ok()) << cursor.status().ToString();
  auto batch = cursor.ValueOrDie().Next();
  ASSERT_TRUE(batch.ok()) << batch.status().ToString();
  ASSERT_TRUE(batch.ValueOrDie().has_value());
  ASSERT_EQ(batch.ValueOrDie()->row_count(), 1U);
  ASSERT_EQ(batch.ValueOrDie()->columns().size(), 3U);
  EXPECT_EQ(std::get<std::vector<ValidTime>>(
                batch.ValueOrDie()->columns()[1].values)[0], ValidTime{7});
  EXPECT_EQ(std::get<std::vector<CommitSeq>>(
                batch.ValueOrDie()->columns()[2].values)[0], CommitSeq{1});
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(pattern);
}

}  // namespace
}  // namespace cedar::cypher
