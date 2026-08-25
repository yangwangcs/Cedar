#include <gtest/gtest.h>

#include "cedar/cypher/binder.h"
#include "cedar/cypher/compiler.h"
#include "cedar/cypher/parser.h"

namespace cedar::cypher {
namespace {

BoundStatement Bound(const char* text) {
  const auto parsed = Parse(text);
  EXPECT_TRUE(parsed.ok()) << parsed.status().ToString();
  return Bind(parsed.ValueOrDie(), SchemaCatalog{}, BinderOptions{}).ValueOrDie();
}

TEST(CypherCompilerTest, LowersVertexScanAndChangesToCedarQuery) {
  const auto query = Compile(Bound(
      "CHANGES FOR VALID_TIME BETWEEN 1 AND 9 MATCH (v) RETURN v"));
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  ASSERT_EQ(query.ValueOrDie().schema().columns().size(), 1U);
  EXPECT_EQ(query.ValueOrDie().schema().columns().front().name, "v");
}

TEST(CypherCompilerTest, LowersBoundedPathToExistingExpandOperator) {
  const auto query = Compile(Bound(
      "MATCH (a)-[e:KNOWS*1..3]->(b) RETURN a, e, b"));
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  EXPECT_EQ(query.ValueOrDie().schema().columns().size(), 3U);
}

TEST(CypherCompilerTest, LowersConnectedMixedPathToLeftDeepExpansions) {
  const auto query = Compile(Bound(
      "MATCH (a)-[e:KNOWS]->(b)-[f:LIKES*1..2]->(c) RETURN a, e, b, f, c"));
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  EXPECT_EQ(query.ValueOrDie().schema().columns().size(), 5U);
}

TEST(CypherCompilerTest, LowersFactMetadataFunctionsToTypedColumns) {
  const auto query = Compile(Bound(
      "MATCH (v) RETURN v, valid_from(v), commit_seq(v)"));
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  ASSERT_EQ(query.ValueOrDie().schema().columns().size(), 3U);
  EXPECT_EQ(query.ValueOrDie().schema().columns()[1].type, QueryType::kValidTime);
  EXPECT_EQ(query.ValueOrDie().schema().columns()[2].type, QueryType::kCommitSeq);
}

TEST(CypherCompilerTest, LowersGraphEndpointMetadataFunctions) {
  const auto query = Compile(Bound(
      "MATCH (a)-[e:KNOWS]->(b) RETURN a, valid_from(e), b"));
  ASSERT_TRUE(query.ok()) << query.status().ToString();
  ASSERT_EQ(query.ValueOrDie().schema().columns().size(), 3U);
  EXPECT_EQ(query.ValueOrDie().schema().columns()[1].type, QueryType::kValidTime);
}

TEST(CypherCompilerTest, DoesNotTreatSystemTimeAsValidTime) {
  const auto parsed = Parse(
      "FOR SYSTEM_TIME AS OF 7 MATCH (v) RETURN v");
  ASSERT_TRUE(parsed.ok()) << parsed.status().ToString();
  const auto bound = Bind(parsed.ValueOrDie(), SchemaCatalog{}, BinderOptions{});
  ASSERT_TRUE(bound.ok()) << bound.status().ToString();
  const auto query = Compile(bound.ValueOrDie());
  ASSERT_TRUE(query.ok()) << query.status().ToString();
}

TEST(CypherCompilerTest, RejectsMixedPatternsInsteadOfDroppingTheRemainder) {
  const auto parsed = Parse("MATCH (a), (b) RETURN a, b");
  ASSERT_TRUE(parsed.ok()) << parsed.status().ToString();
  const auto bound = Bind(parsed.ValueOrDie(), SchemaCatalog{}, BinderOptions{});
  ASSERT_TRUE(bound.ok()) << bound.status().ToString();
  const auto query = Compile(bound.ValueOrDie());
  EXPECT_FALSE(query.ok());
  EXPECT_TRUE(query.status().IsNotSupportedError());
  EXPECT_NE(query.status().ToString().find("product operator"), std::string::npos);
}

}  // namespace
}  // namespace cedar::cypher
