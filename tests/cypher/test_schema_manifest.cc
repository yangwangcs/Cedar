#include <gtest/gtest.h>

#include "cedar/cypher/schema_manifest.h"

namespace cedar::cypher {
namespace {

TEST(SchemaManifestTest, ParsesGraphPartAndProperties) {
  const auto parsed = ParseSchemaManifest(
      "graph=social\npart_id=7\nproperty=1,vertex,string,3,name\n");
  ASSERT_TRUE(parsed.ok()) << parsed.status().ToString();
  EXPECT_EQ(parsed.ValueOrDie().graph, "social");
  EXPECT_EQ(parsed.ValueOrDie().part_id, PartId{7});
  EXPECT_TRUE(parsed.ValueOrDie().catalog.Find("name", PropertyEntityKind::kVertex).ok());
}

TEST(SchemaManifestTest, RejectsUnknownDuplicateAndMalformedFields) {
  for (const std::string& text : {
           "part_id=1\n",
           "graph=g\nunknown=x\n",
           "graph=g\ngraph=h\n",
           "graph=g\nproperty=1,vertex,wrong,1,name\n",
           "graph=g\nproperty=1,vertex,string,1,name\nproperty=1,vertex,string,1,other\n"}) {
    const auto parsed = ParseSchemaManifest(text);
    EXPECT_FALSE(parsed.ok()) << text;
  }
}

}  // namespace
}  // namespace cedar::cypher
