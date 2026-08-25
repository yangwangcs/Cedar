#include <gtest/gtest.h>

#include "cedar/cypher/binder.h"
#include "cedar/cypher/parser.h"

namespace cedar::cypher {
namespace {

SchemaCatalog Catalog() {
  SchemaCatalog catalog;
  EXPECT_TRUE(catalog.Add(PropertyDefinition{PropertyId{7}, 1, "name",
                                             PropertyEntityKind::kVertex,
                                             PhysicalType::kString, 64})
                          .ok());
  return catalog;
}

TEST(CypherBinderTest, BindsScopePartAndDemandWithoutParameterValues) {
  const auto parsed = Parse(
      "USE social FOR VALID_TIME BETWEEN 10 AND 20 MATCH "
      "(a)-[e:KNOWS*1..2]->(b) RETURN a, valid_from(e)");
  ASSERT_TRUE(parsed.ok()) << parsed.status().ToString();
  BinderOptions options;
  options.graph = "social";
  options.part_id = PartId{3};
  const auto bound = Bind(parsed.ValueOrDie(), Catalog(), options);
  ASSERT_TRUE(bound.ok()) << bound.status().ToString();
  EXPECT_EQ(bound.ValueOrDie().part_id, PartId{3});
  EXPECT_EQ(bound.ValueOrDie().demand.vertex_families, 1U);
  EXPECT_EQ(bound.ValueOrDie().demand.edge_families, 1U);
  EXPECT_EQ(bound.ValueOrDie().fingerprint,
            Bind(parsed.ValueOrDie(), Catalog(), options).ValueOrDie().fingerprint);
}

TEST(CypherBinderTest, ResolvesPropertyNamesAndStableParameterIds) {
  const auto parsed = Parse("USE g CREATE (a) SET a.name = $name");
  ASSERT_TRUE(parsed.ok()) << parsed.status().ToString();
  BinderOptions options;
  options.graph = "g";
  const auto bound = Bind(parsed.ValueOrDie(), Catalog(), options);
  ASSERT_TRUE(bound.ok()) << bound.status().ToString();
  ASSERT_EQ(bound.ValueOrDie().parameters.size(), 1U);
  EXPECT_EQ(bound.ValueOrDie().parameters.front().name, "name");
  EXPECT_EQ(bound.ValueOrDie().parameters.front().id, ParameterId{0});
  EXPECT_EQ(bound.ValueOrDie().assignments.front().property_id, PropertyId{7});
}

TEST(CypherBinderTest, BindsRepeatedNamedParameterOnlyOnce) {
  const auto parsed = Parse(
      "CREATE (a) SET a.name = $name SET a.name = $name");
  ASSERT_TRUE(parsed.ok()) << parsed.status().ToString();
  const auto bound = Bind(parsed.ValueOrDie(), Catalog(), BinderOptions{});
  ASSERT_TRUE(bound.ok()) << bound.status().ToString();
  EXPECT_EQ(bound.ValueOrDie().parameters.size(), 1U);
  EXPECT_EQ(bound.ValueOrDie().assignments[0].parameter,
            bound.ValueOrDie().assignments[1].parameter);
}

TEST(CypherBinderTest, RejectsGraphAndPropertyMismatches) {
  const auto parsed = Parse("USE other CREATE (a) SET a.name = $name");
  ASSERT_TRUE(parsed.ok());
  BinderOptions options;
  options.graph = "g";
  EXPECT_TRUE(Bind(parsed.ValueOrDie(), Catalog(), options).status().IsBindError());

  const auto unknown = Parse("CREATE (a) SET a.missing = $x");
  ASSERT_TRUE(unknown.ok());
  options.graph.clear();
  EXPECT_TRUE(Bind(unknown.ValueOrDie(), Catalog(), options).status().IsBindError());
}

}  // namespace
}  // namespace cedar::cypher
