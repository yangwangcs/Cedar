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

TEST(CypherBinderTest, RejectsUnsupportedMinimumHopCount) {
  for (const auto* query : {
           "MATCH (a)-[e*2..2]->(b) RETURN b",
           "MATCH (a)-[e*0..2]->(b) RETURN b"}) {
    const auto parsed = Parse(query);
    ASSERT_TRUE(parsed.ok()) << parsed.status().ToString();
    const auto bound = Bind(parsed.ValueOrDie(), Catalog());
    ASSERT_FALSE(bound.ok());
    EXPECT_TRUE(bound.status().IsNotSupportedError()) << bound.status().ToString();
  }
  for (const auto* query : {
           "MATCH (a)-[e*1..1]->(b) RETURN b",
           "MATCH (a)-[e*1..2]->(b) RETURN b"}) {
    const auto parsed = Parse(query);
    ASSERT_TRUE(parsed.ok());
    EXPECT_TRUE(Bind(parsed.ValueOrDie(), Catalog()).ok());
  }
}

TEST(CypherBinderTest, FingerprintDistinguishesExecutionSemantics) {
  const std::vector<std::pair<std::string, std::string>> pairs{
      {"MATCH (v) RETURN v LIMIT 1", "MATCH (v) RETURN v LIMIT 2"},
      {"MATCH (v) WHERE v.name = 'CN' RETURN v",
       "MATCH (v) WHERE v.name = 'US' RETURN v"},
      {"MATCH (v) WHERE v.name = 'CN' RETURN v",
       "MATCH (v) WHERE v.name > 'CN' RETURN v"},
      {"MATCH (v) WHERE v.name = $a RETURN v",
       "MATCH (v) WHERE v.name = $b RETURN v"},
      {"MATCH (v {part_id: 0, id: 1}) RETURN v",
       "MATCH (v {part_id: 0, id: 2}) RETURN v"},
      {"CREATE (a), (b) DELETE a", "CREATE (a), (b) DELETE b"},
      {"MATCH (a)-[e*1..1]->(b) RETURN b",
       "MATCH (a)-[e*1..2]->(b) RETURN b"},
      {"MATCH (a)-[e]->(b) RETURN a", "MATCH (a)-[e]->(b) RETURN b"},
      {"MATCH (a)-[e]->(b) RETURN e",
       "MATCH (a)-[e]->(b) RETURN valid_from(e)"},
      {"FOR VALID_TIME BETWEEN 10 AND 20 MATCH (v) RETURN v",
       "FOR VALID_TIME BETWEEN 10 AND 30 MATCH (v) RETURN v"},
      {"FOR SYSTEM_TIME AS OF 7 MATCH (v) RETURN v",
       "FOR SYSTEM_TIME AS OF 8 MATCH (v) RETURN v"},
      {"MATCH (v {part_id: 0, id: 1}) RETURN v",
       "MATCH (v {part_id: 1, id: 1}) RETURN v"},
  };
  for (const auto& [first, second] : pairs) {
    SCOPED_TRACE(first + " vs " + second);
    const auto first_ast = Parse(first);
    const auto second_ast = Parse(second);
    ASSERT_TRUE(first_ast.ok()) << first_ast.status().ToString();
    ASSERT_TRUE(second_ast.ok()) << second_ast.status().ToString();
    const auto left = Bind(first_ast.ValueOrDie(), Catalog());
    const auto right = Bind(second_ast.ValueOrDie(), Catalog());
    ASSERT_TRUE(left.ok()) << left.status().ToString();
    ASSERT_TRUE(right.ok()) << right.status().ToString();
    EXPECT_NE(left.ValueOrDie().semantic_key, right.ValueOrDie().semantic_key);
    EXPECT_NE(left.ValueOrDie().fingerprint, right.ValueOrDie().fingerprint);
  }
}

TEST(CypherBinderTest, SemanticIdentityFramesBinaryLiteralsAndSchema) {
  auto parsed = Parse("MATCH (v) WHERE v.name = 'x' RETURN v");
  ASSERT_TRUE(parsed.ok());
  auto first_ast = parsed.ValueOrDie();
  first_ast.predicates.front().literal = std::string("x\0|where=", 9);
  auto second_ast = first_ast;
  second_ast.predicates.front().literal = "x";
  const auto first = Bind(first_ast, Catalog());
  const auto second = Bind(second_ast, Catalog());
  ASSERT_TRUE(first.ok());
  ASSERT_TRUE(second.ok());
  EXPECT_NE(first.ValueOrDie().semantic_key, second.ValueOrDie().semantic_key);

  SchemaCatalog newer;
  ASSERT_TRUE(newer.Add(PropertyDefinition{PropertyId{7}, 2, "name",
      PropertyEntityKind::kVertex, PhysicalType::kString, 64}).ok());
  const auto changed_schema = Bind(first_ast, newer);
  ASSERT_TRUE(changed_schema.ok());
  EXPECT_NE(first.ValueOrDie().semantic_key,
            changed_schema.ValueOrDie().semantic_key);

  const auto whitespace = Parse("  MATCH (v) WHERE v.name = 'x' RETURN v  ");
  ASSERT_TRUE(whitespace.ok());
  const auto equivalent = Bind(whitespace.ValueOrDie(), Catalog());
  const auto original = Bind(parsed.ValueOrDie(), Catalog());
  ASSERT_TRUE(equivalent.ok());
  ASSERT_TRUE(original.ok());
  EXPECT_EQ(equivalent.ValueOrDie().semantic_key,
            original.ValueOrDie().semantic_key);
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

TEST(CypherBinderTest, BindsTypedWherePredicatesToSchemaProperties) {
  const auto parsed = Parse("MATCH (v) WHERE v.name = 'CN' RETURN v");
  ASSERT_TRUE(parsed.ok()) << parsed.status().ToString();
  const auto bound = Bind(parsed.ValueOrDie(), Catalog(), BinderOptions{});
  ASSERT_TRUE(bound.ok()) << bound.status().ToString();
  ASSERT_EQ(bound.ValueOrDie().predicates.size(), 1U);
  EXPECT_EQ(bound.ValueOrDie().predicates.front().property_id, PropertyId{7});
  EXPECT_EQ(bound.ValueOrDie().predicates.front().physical_type,
            PhysicalType::kString);
  EXPECT_EQ(bound.ValueOrDie().predicates.front().literal, "CN");
}

}  // namespace
}  // namespace cedar::cypher
