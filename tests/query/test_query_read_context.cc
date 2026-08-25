#include <gtest/gtest.h>

#include "cedar/fact/canonical_reader.h"
#include "query/runtime/read_context.h"

namespace cedar::internal {
namespace {

class RejectingReader final : public CanonicalFactReader {
 public:
  StatusOr<std::optional<FactEvent>> ReadStateAt(
      const FactReadSpec&, ValidTime, CommitSeq) const override {
    return Status::NotSupported("test reader", "not used");
  }
  Status ReadEvents(const FactReadSpec& spec,
                    const CanonicalFactBatchVisitor&) const override {
    return spec.part_scope.kind == PartScopeKind::kAll
               ? Status::InvalidArgument("test reader", "wildcard not allowed")
               : Status::OK();
  }
  Status ReadColumnar(const FactReadSpec&,
                      const CanonicalColumnarBatchVisitor&) const override {
    return Status::OK();
  }
  StatusOr<std::vector<FactEvent>> ReadExact(
      const std::vector<std::string>&) const override {
    return std::vector<FactEvent>{};
  }
};

TEST(QueryReadContext, CarriesSnapshotAndExactPartWithoutStorageHandles) {
  RejectingReader reader;
  QueryReadContext context{reader, CommitSeq{17},
                           PartScope::Exact(PartId{3}), {}, {}};
  EXPECT_EQ(context.snapshot_seq, CommitSeq{17});
  EXPECT_EQ(context.part_scope.kind, PartScopeKind::kExact);
  EXPECT_EQ(context.part_scope.parts.front(), PartId{3});
  FactReadSpec spec;
  spec.part_scope = context.part_scope;
  EXPECT_TRUE(context.facts.ReadEvents(spec, [](const FactEventBatch&) {
    return Status::OK();
  }).ok());
}

}  // namespace
}  // namespace cedar::internal
