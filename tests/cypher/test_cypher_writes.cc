#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>

#include "cedar/cypher.h"
#include "cedar/cypher/parser.h"
#include "cedar/cypher/write.h"
#include "cedar/database.h"

namespace cedar::cypher {
namespace {

TEST(CypherWriteTest, CreatePublishesCanonicalVertexEdgeFactsAndReopens) {
  char pattern[] = "/tmp/cedar_cypher_write_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  DatabaseOptions options;
  options.path = pattern;
  auto database = Database::Open(std::move(options));
  ASSERT_TRUE(database.ok()) << database.status().ToString();
  const auto parsed = Parse("CREATE (a)-[e:KNOWS]->(b)");
  ASSERT_TRUE(parsed.ok()) << parsed.status().ToString();
  const auto bound = Bind(parsed.ValueOrDie(), SchemaCatalog{}, BinderOptions{});
  ASSERT_TRUE(bound.ok()) << bound.status().ToString();
  const auto result = ExecuteWrite(*database.ValueOrDie(), bound.ValueOrDie(),
                                   Bindings{}, ValidTime{10});
  ASSERT_TRUE(result.ok()) << result.status().ToString();
  EXPECT_EQ(result.ValueOrDie().outcome, CommitOutcome::kCommitted);
  ASSERT_TRUE(database.ValueOrDie()->Close().ok());

  auto reopened = Database::Open(DatabaseOptions{.path = pattern});
  ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
  auto snapshot = reopened.ValueOrDie()->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  size_t vertices = 0;
  ASSERT_TRUE(snapshot.ValueOrDie().ScanFamily(
      FactFamily::kVertexState,
      [&vertices](const FactEvent&) {
        ++vertices;
        return Status::OK();
      }).ok());
  EXPECT_EQ(vertices, 2U);
  reopened.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(pattern);
}

TEST(CypherWriteTest, RejectsHistoricalSystemScope) {
  const auto parsed = Parse(
      "FOR SYSTEM_TIME AS OF 3 CREATE (a)");
  ASSERT_TRUE(parsed.ok());
  const auto bound = Bind(parsed.ValueOrDie(), SchemaCatalog{}, BinderOptions{});
  ASSERT_TRUE(bound.ok());
  char pattern[] = "/tmp/cedar_cypher_write_reject_XXXXXX";
  ASSERT_NE(mkdtemp(pattern), nullptr);
  auto database = Database::Open(DatabaseOptions{.path = pattern});
  ASSERT_TRUE(database.ok());
  const auto result = ExecuteWrite(*database.ValueOrDie(), bound.ValueOrDie(),
                                   Bindings{}, ValidTime{10});
  EXPECT_FALSE(result.ok());
  EXPECT_TRUE(result.status().IsInvalidArgument());
  database.ValueOrDie()->Close().IgnoreError();
  std::filesystem::remove_all(pattern);
}

}  // namespace
}  // namespace cedar::cypher
