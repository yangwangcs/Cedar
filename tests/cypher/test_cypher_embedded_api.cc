#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>

#include "cedar/cypher.h"
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

}  // namespace
}  // namespace cedar::cypher
