// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "cedar/database.h"
#include "kernel/transaction_mutation.h"

namespace cedar {
namespace {

PropertyDefinition Property(PropertyId property_id, std::string name,
                            PropertyEntityKind entity_kind,
                            PhysicalType physical_type) {
  return {property_id, 0, std::move(name), entity_kind, physical_type, 4096};
}

class KernelMutationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_kernel_mutations_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    path_ = pattern;
    auto opened = Database::Open(DatabaseOptions{.path = path_});
    ASSERT_TRUE(opened.ok()) << opened.status().ToString();
    database_ = std::move(opened).ConsumeValueOrDie();
  }

  void TearDown() override {
    database_.reset();
    std::filesystem::remove_all(path_);
  }

  std::unique_ptr<Transaction> Begin() {
    auto transaction = database_->BeginTransaction();
    EXPECT_TRUE(transaction.ok()) << transaction.status().ToString();
    return std::move(transaction).ConsumeValueOrDie();
  }

  std::string path_;
  std::unique_ptr<Database> database_;
};

TEST_F(KernelMutationTest, StagesEntityStateWithoutPropertySchemaOrValue) {
  std::unique_ptr<Transaction> transaction = Begin();
  ASSERT_TRUE(transaction->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10}).ok());
  ASSERT_TRUE(transaction->Retract(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{2}}), ValidTime{10}).ok());

  const auto before_rollback = database_->BeginSnapshot();
  ASSERT_TRUE(before_rollback.ok()) << before_rollback.status().ToString();
  const auto exists = before_rollback.ValueOrDie().Exists(
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10});
  ASSERT_TRUE(exists.ok()) << exists.status().ToString();
  EXPECT_FALSE(exists.ValueOrDie());

  EXPECT_TRUE(transaction->Rollback().ok());
  const auto after_rollback = database_->BeginSnapshot();
  ASSERT_TRUE(after_rollback.ok()) << after_rollback.status().ToString();
  const auto still_absent = after_rollback.ValueOrDie().Exists(
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10});
  ASSERT_TRUE(still_absent.ok()) << still_absent.status().ToString();
  EXPECT_FALSE(still_absent.ValueOrDie());
}

TEST_F(KernelMutationTest, UsesSchemaVisibleWhenTransactionBegan) {
  ASSERT_TRUE(database_->RegisterProperty(
                        Property(PropertyId{7}, "name", PropertyEntityKind::kVertex,
                                 PhysicalType::kString))
                  .ok());
  std::unique_ptr<Transaction> transaction = Begin();

  ASSERT_TRUE(database_->RegisterProperty(
                        Property(PropertyId{7}, "display_name",
                                 PropertyEntityKind::kVertex,
                                 PhysicalType::kString))
                  .ok());
  ASSERT_TRUE(database_->RegisterProperty(
                        Property(PropertyId{7}, "age", PropertyEntityKind::kVertex,
                                 PhysicalType::kInt64))
                  .ok());

  EXPECT_TRUE(transaction->Set(
                  PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{7}), ValidTime{10},
                  Value::String("Ada"))
                  .ok());
}

TEST_F(KernelMutationTest, RejectsPropertyWithWrongTypeOrMissingSchema) {
  ASSERT_TRUE(database_->RegisterProperty(
                        Property(PropertyId{7}, "name", PropertyEntityKind::kVertex,
                                 PhysicalType::kString))
                  .ok());
  std::unique_ptr<Transaction> transaction = Begin();

  EXPECT_TRUE(transaction->Set(
                  PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{7}), ValidTime{10},
                  Value::Int64(1))
                  .IsSchemaMismatch());
  EXPECT_TRUE(transaction->Unset(
                  PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{8}), ValidTime{10})
                  .IsSchemaMismatch());
}

TEST_F(KernelMutationTest, RejectsContradictoryDuplicateFactAtOneValidTime) {
  std::unique_ptr<Transaction> transaction = Begin();
  ASSERT_TRUE(transaction->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10}).ok());
  EXPECT_TRUE(transaction->Retract(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10})
                  .IsInvalidArgument());
  EXPECT_TRUE(transaction->Rollback().ok());
}

TEST_F(KernelMutationTest,
       StagesSameVertexIdInDistinctPartitionsIndependently) {
  std::unique_ptr<Transaction> transaction = Begin();
  ASSERT_TRUE(transaction
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{1}, VertexId{1}}),
                           ValidTime{10})
                  .ok());
  ASSERT_TRUE(transaction
                  ->Assert(EntityFact::Vertex(VertexRef{PartId{2}, VertexId{1}}),
                           ValidTime{10})
                  .ok());
}

TEST(KernelMutationOrderTest, SortsValidTimeDescendingWithinOneFact) {
  const FactRef ref = EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}).ref();
  std::vector<PendingFactMutation> mutations = {
      {ref, ValidTime{10}, FactOperation::kPut, 0, std::nullopt},
      {ref, ValidTime{30}, FactOperation::kPut, 0, std::nullopt},
      {ref, ValidTime{20}, FactOperation::kPut, 0, std::nullopt},
  };

  std::sort(mutations.begin(), mutations.end(), CanonicalMutationLess);
  EXPECT_EQ(mutations[0].valid_from, ValidTime{30});
  EXPECT_EQ(mutations[1].valid_from, ValidTime{20});
  EXPECT_EQ(mutations[2].valid_from, ValidTime{10});
}

TEST_F(KernelMutationTest, KeepsMixedTimeAndPartitionStagingReadable) {
  ASSERT_TRUE(database_->RegisterProperty(Property(
      PropertyId{7}, "value", PropertyEntityKind::kVertex,
      PhysicalType::kInt32)).ok());
  auto transaction = Begin();
  const auto first = PropertyFact::Vertex({PartId{1}, VertexId{11}}, PropertyId{7});
  const auto second = PropertyFact::Vertex({PartId{2}, VertexId{11}}, PropertyId{7});
  ASSERT_TRUE(transaction->Set(first, ValidTime{30}, Value::Int32(30)).ok());
  ASSERT_TRUE(transaction->Set(second, ValidTime{20}, Value::Int32(200)).ok());
  ASSERT_TRUE(transaction->Set(first, ValidTime{10}, Value::Int32(10)).ok());
  ASSERT_TRUE(transaction->Set(first, ValidTime{20}, Value::Int32(20)).ok());
  EXPECT_TRUE(transaction->Set(first, ValidTime{20}, Value::Int32(999)).IsInvalidArgument());
  const auto committed = transaction->Commit();
  ASSERT_TRUE(committed.ok());
  ASSERT_TRUE(committed.ValueOrDie().status.ok());
  auto after = Begin();
  for (const auto& [property, time, expected] :
       std::vector<std::tuple<PropertyFact, ValidTime, int32_t>>{
           {first, ValidTime{15}, 10}, {first, ValidTime{25}, 20},
           {first, ValidTime{35}, 30}, {second, ValidTime{25}, 200}}) {
    const auto value = after->Get(property, time);
    ASSERT_TRUE(value.ok());
    ASSERT_TRUE(value.ValueOrDie().has_value());
    EXPECT_EQ(std::get<int32_t>(value.ValueOrDie()->data()), expected);
  }
}

}  // namespace
}  // namespace cedar
