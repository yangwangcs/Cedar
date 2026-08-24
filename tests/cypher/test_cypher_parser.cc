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

}  // namespace
}  // namespace cedar::cypher
