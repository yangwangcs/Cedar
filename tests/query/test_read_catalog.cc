#include <gtest/gtest.h>

#include "query/read/read_catalog.h"

namespace cedar::internal {
namespace {

TEST(ReadCatalog, ImmutableSchemaAndCoverageLookups) {
  ReadCatalogBuilder builder;
  ASSERT_TRUE(builder.AddSchema(PropertyDefinition{
      PropertyId{7}, 1, "score", PropertyEntityKind::kVertex,
      PhysicalType::kInt64, 4096}).ok());
  CoverageRegion region;
  region.kind = ProjectionKind::kState;
  region.part_id = PartId{3};
  region.schema_epoch = 1;
  region.entity_min = 10;
  region.entity_max_exclusive = 20;
  region.valid_time = {ValidTime{0}, ValidTime{100}};
  ASSERT_TRUE(builder.AddCoverage(ReadCatalogKey{ProjectionKind::kState, PartId{3},
                                                  std::nullopt, 1},
                                  region).ok());
  auto catalog = std::move(builder).Finish();
  ASSERT_TRUE(catalog.ok()) << catalog.status().ToString();
  auto property = catalog.ValueOrDie()->LookupProperty(PropertyId{7});
  ASSERT_TRUE(property.ok() && property.ValueOrDie().has_value());
  EXPECT_EQ(property.ValueOrDie()->name, "score");
  auto by_name = catalog.ValueOrDie()->LookupPropertyByName(
      "score", PropertyEntityKind::kVertex);
  ASSERT_TRUE(by_name.ok() && by_name.ValueOrDie().has_value());
  EXPECT_EQ(by_name.ValueOrDie()->property_id, PropertyId{7});
  EXPECT_NE(catalog.ValueOrDie()->FindCoverage(
                ReadCatalogKey{ProjectionKind::kState, PartId{3}, std::nullopt, 1},
                11, 12, ValidTimeInterval{ValidTime{10}, ValidTime{20}}), nullptr);
}

TEST(ReadCatalog, ScalesSchemaLookupAndIndexesTypedAdjacency) {
  ReadCatalogBuilder builder;
  for (uint16_t i = 1; i <= 10000; ++i) {
    auto status = builder.AddSchema(PropertyDefinition{
        PropertyId{i}, 1, "p" + std::to_string(i),
        PropertyEntityKind::kVertex, PhysicalType::kInt64, 4096});
    ASSERT_TRUE(status.ok()) << status.ToString() << " i=" << i;
  }
  const VertexRef source{PartId{4}, VertexId{9}};
  const VertexRef target{PartId{4}, VertexId{10}};
  const EdgeIdentity edge{EdgeRef{PartId{4}, EdgeId{77}}, source, target, 42};
  ASSERT_TRUE(builder.AddAdjacency(source, edge, ExpandDirection::kOut).ok());
  ASSERT_TRUE(builder.AddAdjacency(source, edge, ExpandDirection::kOut, 42).ok());
  auto catalog = std::move(builder).Finish();
  ASSERT_TRUE(catalog.ok()) << catalog.status().ToString();
  auto property = catalog.ValueOrDie()->LookupProperty(PropertyId{9999});
  ASSERT_TRUE(property.ok() && property.ValueOrDie().has_value());
  EXPECT_EQ(property.ValueOrDie()->name, "p9999");
  const auto* all = catalog.ValueOrDie()->SeekAdjacency(
      source, ExpandDirection::kOut, std::nullopt);
  ASSERT_NE(all, nullptr);
  ASSERT_EQ(all->size(), 1U);
  EXPECT_EQ(all->front().edge_id, EdgeId{77});
  const auto* typed = catalog.ValueOrDie()->SeekAdjacency(
      source, ExpandDirection::kOut, 42);
  ASSERT_NE(typed, nullptr);
  EXPECT_EQ(typed->size(), 1U);
}

TEST(ReadCatalog, PublishesTypedPropertyPostingCapability) {
  ReadCatalogBuilder builder;
  const VertexRef vertex{PartId{0}, VertexId{11}};
  ASSERT_TRUE(builder.AddPropertyIndex(
      PropertyId{7}, PropertyIndexPosting{vertex,
                                          {ValidTime{0}, std::nullopt},
                                          CommitSeq{9}, Value::String("CN")})
                  .ok());
  auto catalog = std::move(builder).Finish();
  ASSERT_TRUE(catalog.ok()) << catalog.status().ToString();
  const auto* postings = catalog.ValueOrDie()->SeekPropertyIndex(
      PropertyId{7}, PropertyIndexOperator::kEqual, Value::String("CN"));
  ASSERT_NE(postings, nullptr);
  ASSERT_EQ(postings->size(), 1U);
  EXPECT_EQ(postings->front().vertex, vertex);
}

}  // namespace
}  // namespace cedar::internal
