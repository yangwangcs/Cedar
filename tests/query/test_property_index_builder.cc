#include <gtest/gtest.h>

#include "query/projection/property_index_builder.h"

namespace cedar::internal {
namespace {

class Reader final : public CanonicalFactReader {
 public:
  StatusOr<std::optional<FactEvent>> ReadStateAt(
      const FactReadSpec&, ValidTime, CommitSeq) const override {
    return std::optional<FactEvent>{};
  }
  Status ReadStateRows(const CanonicalStateReadSpec& spec,
                       const CanonicalStateBatchVisitor& visitor) const override {
    EXPECT_EQ(spec.snapshot_seq, CommitSeq{9});
    return visitor({{
        CanonicalStateRow{FactRef{PartId{2}, FactFamily::kVertexProperty,
                                  PropertyId{7}, 8},
                          {ValidTime{10}, std::nullopt}, CommitSeq{9},
                          Value::String("US")},
        CanonicalStateRow{FactRef{PartId{1}, FactFamily::kVertexProperty,
                                  PropertyId{7}, 4},
                          {ValidTime{10}, std::nullopt}, CommitSeq{9},
                          Value::String("CN")},
    }});
  }
  Status ReadEvents(const FactReadSpec&, const CanonicalFactBatchVisitor&) const override {
    return Status::NotSupported("test", "unused");
  }
  Status ReadColumnar(const FactReadSpec&, const CanonicalColumnarBatchVisitor&) const override {
    return Status::NotSupported("test", "unused");
  }
  StatusOr<std::vector<FactEvent>> ReadExact(
      const std::vector<std::string>&) const override {
    return std::vector<FactEvent>{};
  }
};

TEST(PropertyIndexBuilderTest, GroupsSortsAndPreservesPartAndSnapshot) {
  Reader reader;
  const PropertyDefinition definition{PropertyId{7}, 1, "country",
                                      PropertyEntityKind::kVertex,
                                      PhysicalType::kString, 4096};
  auto build = BuildPropertyIndexProjection(
      reader, {definition}, PartScope::All(),
      ValidTimeInterval{ValidTime{10}, ValidTime{11}}, CommitSeq{1},
      CommitSeq{9}, 3, "db");
  ASSERT_TRUE(build.ok()) << build.status().ToString();
  ASSERT_EQ(build.ValueOrDie().manifest.regions.size(), 2U);
  ASSERT_EQ(build.ValueOrDie().segments.size(), 2U);
  EXPECT_EQ(build.ValueOrDie().manifest.regions[0].part_id, PartId{1});
  EXPECT_EQ(build.ValueOrDie().manifest.regions[1].part_id, PartId{2});
  EXPECT_EQ(build.ValueOrDie().segments[0].descriptor.header.kind,
            ProjectionKind::kPropertyIndex);
  EXPECT_EQ(build.ValueOrDie().segments[0].descriptor.header.generation_id, 3U);
}

}  // namespace
}  // namespace cedar::internal
