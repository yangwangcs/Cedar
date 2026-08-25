#include <gtest/gtest.h>

#include "cedar/cypher/compiler.h"
#include "cedar/cypher/parser.h"
#include "cedar/fact/read_spec.h"

namespace cedar {

TEST(QueryPartScope, ExactZeroIsARealPartition) {
  const PartScope scope = PartScope::Exact(PartId{0});
  EXPECT_EQ(scope.kind, PartScopeKind::kExact);
  ASSERT_EQ(scope.parts.size(), 1U);
  EXPECT_EQ(scope.parts.front(), PartId{0});
  EXPECT_TRUE(scope.Validate().ok());
}

TEST(QueryPartScope, SetIsSortedAndDeduplicated) {
  const PartScope scope = PartScope::Set({PartId{7}, PartId{2}, PartId{7}, PartId{3}});
  ASSERT_EQ(scope.parts.size(), 3U);
  EXPECT_EQ(scope.parts[0], PartId{2});
  EXPECT_EQ(scope.parts[1], PartId{3});
  EXPECT_EQ(scope.parts[2], PartId{7});
  EXPECT_TRUE(scope.Validate().ok());
}

TEST(QueryPartScope, EmptySetIsRejectedAndAllIsExplicit) {
  EXPECT_FALSE(PartScope::Set({}).Validate().ok());
  const PartScope all = PartScope::All();
  EXPECT_EQ(all.kind, PartScopeKind::kAll);
  EXPECT_TRUE(all.parts.empty());
  EXPECT_TRUE(all.Validate().ok());
}

TEST(FactReadSpec, RejectsReversedAndDuplicateProjectionRanges) {
  FactReadSpec spec;
  spec.part_scope = PartScope::Exact(PartId{1});
  spec.entity_range.min = 10;
  spec.entity_range.max_exclusive = 2;
  EXPECT_FALSE(spec.Validate().ok());

  spec.entity_range.max_exclusive = 20;
  spec.projection = {FactColumnId::kEntityId, FactColumnId::kEntityId};
  EXPECT_FALSE(spec.Validate().ok());
}

TEST(ExecutionScope, RequiresBoundedValidTimeAndSnapshotCeiling) {
  ExecutionScope scope;
  scope.part_scope = PartScope::Exact(PartId{4});
  scope.valid_time = ValidTimeInterval{ValidTime{20}, ValidTime{10}};
  EXPECT_FALSE(scope.Validate().ok());

  scope.valid_time = ValidTimeInterval{ValidTime{10}, ValidTime{20}};
  scope.system_time_as_of = CommitSeq{3};
  scope.snapshot_seq = CommitSeq{2};
  EXPECT_FALSE(scope.Validate().ok());
}

TEST(QueryPartScope, BinderAndCompilerCarryExactPartIntoLogicalQuery) {
  const auto parsed = cypher::Parse("MATCH (v) RETURN v");
  ASSERT_TRUE(parsed.ok()) << parsed.status().ToString();
  cypher::BinderOptions options;
  options.part_id = PartId{9};
  const auto bound = cypher::Bind(parsed.ValueOrDie(), cypher::SchemaCatalog(), options);
  ASSERT_TRUE(bound.ok()) << bound.status().ToString();
  const auto compiled = cypher::Compile(bound.ValueOrDie());
  ASSERT_TRUE(compiled.ok()) << compiled.status().ToString();
  ASSERT_TRUE(compiled.ValueOrDie().execution_scope().has_value());
  const ExecutionScope& scope = *compiled.ValueOrDie().execution_scope();
  EXPECT_EQ(scope.part_scope.kind, PartScopeKind::kExact);
  ASSERT_EQ(scope.part_scope.parts.size(), 1U);
  EXPECT_EQ(scope.part_scope.parts.front(), PartId{9});
}

}  // namespace cedar
