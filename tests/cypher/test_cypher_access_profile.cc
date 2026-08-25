#include <gtest/gtest.h>

#include "cedar/cypher/access_profile.h"
#include "cedar/cypher/binder.h"
#include "cedar/cypher/parser.h"

namespace cedar::cypher {
namespace {

StatusOr<BoundStatement> Bound(const char* source) {
  const auto parsed = Parse(source);
  if (!parsed.ok()) return parsed.status();
  return Bind(parsed.ValueOrDie(), SchemaCatalog{}, BinderOptions{});
}

TEST(CypherAccessProfileTest, SelectsInteractiveForTypedGraphExpand) {
  const auto bound = Bound("MATCH (a)-[e:KNOWS]->(b) RETURN a, e, b");
  ASSERT_TRUE(bound.ok()) << bound.status().ToString();
  const auto decision = ChooseAccess(bound.ValueOrDie(), QueryOptions{});
  ASSERT_TRUE(decision.ok()) << decision.status().ToString();
  EXPECT_EQ(decision.ValueOrDie().profile, AccessProfile::kGraphExpand);
  EXPECT_EQ(decision.ValueOrDie().options.mode, QueryExecutionMode::kInteractive);
}

TEST(CypherAccessProfileTest, SelectsAnalyticalForChanges) {
  const auto bound = Bound(
      "CHANGES FOR VALID_TIME BETWEEN 1 AND 9 MATCH (v) RETURN v");
  ASSERT_TRUE(bound.ok()) << bound.status().ToString();
  const auto decision = ChooseAccess(bound.ValueOrDie(), QueryOptions{});
  ASSERT_TRUE(decision.ok()) << decision.status().ToString();
  EXPECT_EQ(decision.ValueOrDie().profile, AccessProfile::kValidTimeChanges);
  EXPECT_EQ(decision.ValueOrDie().options.mode, QueryExecutionMode::kAnalytical);
}

}  // namespace
}  // namespace cedar::cypher
