#include <gtest/gtest.h>

#include "cedar/cypher/binder.h"
#include "cedar/cypher/parser.h"

namespace cedar::cypher {
namespace {

TEST(SchemaProviderTest, CatalogExposesTypedStableLookupAndFingerprint) {
  SchemaCatalog catalog;
  ASSERT_TRUE(catalog.Add(PropertyDefinition{PropertyId{7}, 3, "score",
                                             PropertyEntityKind::kVertex,
                                             PhysicalType::kInt64, 64})
                  .ok());
  auto by_id = catalog.Lookup(PropertyId{7});
  ASSERT_TRUE(by_id.ok());
  ASSERT_TRUE(by_id.ValueOrDie().has_value());
  EXPECT_EQ(by_id.ValueOrDie()->name, "score");
  auto by_name = catalog.LookupByName("score", PropertyEntityKind::kVertex);
  ASSERT_TRUE(by_name.ok());
  EXPECT_TRUE(by_name.ValueOrDie().has_value());
  EXPECT_FALSE(catalog.Fingerprint().empty());
  auto parsed = Parse("CREATE (v) SET v.score = $value");
  ASSERT_TRUE(parsed.ok());
  auto bound = Bind(parsed.ValueOrDie(), static_cast<const SchemaProvider&>(catalog));
  EXPECT_TRUE(bound.ok()) << bound.status().ToString();
}

}  // namespace
}  // namespace cedar::cypher
