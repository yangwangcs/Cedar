#include <gtest/gtest.h>

#include "cedar/cypher/parser.h"

namespace cedar::cypher {
namespace {

TEST(CypherParserTest, ParsesScopedBoundedTrailMatch) {
  const auto parsed = Parse(
      "USE social FOR VALID_TIME BETWEEN 10 AND 20 "
      "FOR SYSTEM_TIME AS OF 7 MATCH (a:Person)-[e:KNOWS*1..3]->(b) "
      "TRAIL RETURN a, e, b, valid_from(e)");
  ASSERT_TRUE(parsed.ok()) << parsed.status().ToString();
  EXPECT_EQ(parsed.ValueOrDie().graph, "social");
  ASSERT_TRUE(parsed.ValueOrDie().valid_time.has_value());
  EXPECT_EQ(parsed.ValueOrDie().valid_time->from, 10U);
  EXPECT_EQ(parsed.ValueOrDie().valid_time->to, 20U);
  ASSERT_TRUE(parsed.ValueOrDie().system_time.has_value());
  EXPECT_EQ(parsed.ValueOrDie().system_time->as_of, 7U);
  EXPECT_EQ(parsed.ValueOrDie().patterns.size(), 1U);
  EXPECT_EQ(parsed.ValueOrDie().patterns.front().min_hops, 1U);
  EXPECT_EQ(parsed.ValueOrDie().patterns.front().max_hops, 3U);
  EXPECT_TRUE(parsed.ValueOrDie().patterns.front().trail);
  EXPECT_EQ(parsed.ValueOrDie().projections.size(), 4U);
  EXPECT_EQ(parsed.ValueOrDie().projections.back().function, "valid_from");
}

TEST(CypherParserTest, ParsesChangesAndParameterizedWrite) {
  const auto changes = Parse(
      "CHANGES FOR VALID_TIME BETWEEN 1 AND 9 MATCH (v) RETURN v");
  ASSERT_TRUE(changes.ok()) << changes.status().ToString();
  EXPECT_TRUE(changes.ValueOrDie().changes);

  const auto write = Parse(
      "USE g CREATE (a:Person)-[e:KNOWS]->(b) SET a.name = $name "
      "DELETE e");
  ASSERT_TRUE(write.ok()) << write.status().ToString();
  EXPECT_EQ(write.ValueOrDie().kind, StatementKind::kWrite);
  EXPECT_EQ(write.ValueOrDie().parameters.size(), 1U);
  EXPECT_EQ(write.ValueOrDie().assignments.size(), 1U);
  EXPECT_EQ(write.ValueOrDie().assignments.front().parameter, "name");
  EXPECT_EQ(write.ValueOrDie().deletions.size(), 1U);
}

TEST(CypherParserTest, RejectsLegacyTemporalAndDiffSyntax) {
  for (const char* query : {"MATCH (v) AT VALID_TIME 4 RETURN v",
                            "DIFF GRAPH old, new MATCH (v) RETURN v",
                            "MATCH (v) RETURN v trailing"}) {
    const auto parsed = Parse(query);
    EXPECT_FALSE(parsed.ok()) << query;
    EXPECT_TRUE(parsed.status().IsParseError()) << parsed.status().ToString();
  }
}

TEST(CypherParserTest, RejectsUnboundedPath) {
  const auto parsed = Parse("MATCH (a)-[*]->(b) RETURN a");
  EXPECT_FALSE(parsed.ok());
  EXPECT_TRUE(parsed.status().IsParseError());
}

TEST(CypherParserTest, ParsesConnectedMixedPathSequence) {
  const auto parsed = Parse("MATCH (a)-[e:KNOWS]->(b)-[f:LIKES*1..2]->(c) RETURN a");
  ASSERT_TRUE(parsed.ok()) << parsed.status().ToString();
  ASSERT_EQ(parsed.ValueOrDie().patterns.size(), 2U);
  EXPECT_EQ(parsed.ValueOrDie().patterns[0].destination, "b");
  EXPECT_EQ(parsed.ValueOrDie().patterns[1].source, "b");
  EXPECT_EQ(parsed.ValueOrDie().patterns[1].max_hops, 2U);
}

TEST(CypherParserTest, ParsesPointReferenceAndLimit) {
  const auto parsed = Parse(
      "MATCH (v {part_id: 3, id: 42}) RETURN v LIMIT 10");
  ASSERT_TRUE(parsed.ok()) << parsed.status().ToString();
  ASSERT_EQ(parsed.ValueOrDie().patterns.size(), 1U);
  EXPECT_EQ(parsed.ValueOrDie().patterns.front().source_part_id, 3U);
  EXPECT_EQ(parsed.ValueOrDie().patterns.front().source_vertex_id, 42U);
  EXPECT_EQ(parsed.ValueOrDie().limit_count, 10U);
}

TEST(CypherParserTest, ParsesTypedWherePredicates) {
  auto parsed = Parse("MATCH (v) WHERE v.country = 'CN' AND v.load_mw >= 10 "
                      "RETURN v LIMIT 5");
  ASSERT_TRUE(parsed.ok()) << parsed.status().ToString();
  ASSERT_EQ(parsed.ValueOrDie().predicates.size(), 2U);
  EXPECT_EQ(parsed.ValueOrDie().predicates[0].variable, "v");
  EXPECT_EQ(parsed.ValueOrDie().predicates[0].property, "country");
  EXPECT_EQ(parsed.ValueOrDie().predicates[0].literal, "CN");
  EXPECT_EQ(parsed.ValueOrDie().predicates[1].op,
            PredicateOperator::kGreaterEqual);
  ASSERT_TRUE(parsed.ValueOrDie().limit_count.has_value());
  EXPECT_EQ(*parsed.ValueOrDie().limit_count, 5U);
}

TEST(CypherParserTest, RejectsMalformedMixedPathSequence) {
  const auto parsed = Parse("MATCH (a)-[e:KNOWS]->(b)-[f:LIKES*1..2 RETURN a");
  EXPECT_FALSE(parsed.ok());
  EXPECT_TRUE(parsed.status().IsParseError());
}

}  // namespace
}  // namespace cedar::cypher
