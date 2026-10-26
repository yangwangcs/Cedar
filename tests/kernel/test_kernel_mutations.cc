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

TEST_F(KernelMutationTest, SameEdgeIdInDifferentHomePartsIsIndependent) {
  auto transaction = Begin();
  for (uint32_t part : {1U, 2U}) {
    for (uint64_t id : {11ULL, 12ULL}) {
      ASSERT_TRUE(transaction->Assert(
          EntityFact::Vertex(VertexRef{PartId{part}, VertexId{id}}),
          ValidTime{10}).ok());
    }
    const EdgeIdentity identity{EdgeRef{PartId{part}, EdgeId{7}},
        VertexRef{PartId{part}, VertexId{11}},
        VertexRef{PartId{part}, VertexId{12}}, 1};
    const auto status = transaction->Assert(identity, ValidTime{10});
    ASSERT_TRUE(status.ok()) << status.ToString();
  }
  ASSERT_TRUE(transaction->Commit().ok());
  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok());
  for (uint32_t part : {1U, 2U}) {
    const auto exists = snapshot.ValueOrDie().Exists(
        EntityFact::Edge(EdgeRef{PartId{part}, EdgeId{7}}), ValidTime{10});
    ASSERT_TRUE(exists.ok());
    EXPECT_TRUE(exists.ValueOrDie());
  }
}

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

TEST_F(KernelMutationTest, UsesSchemaRefreshedAfterTransactionBegan) {
  // A transaction's snapshot epoch is shared per visible sequence and is
  // repointed at the current catalog when a property is registered (registration
  // does not move the visible sequence). The first lookup therefore sees the
  // latest schema even when it was registered after BeginTransaction; that
  // lookup is then cached for the rest of the transaction.
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
                        Property(PropertyId{8}, "age", PropertyEntityKind::kVertex,
                                 PhysicalType::kInt64))
                  .ok());

  EXPECT_TRUE(transaction->Set(
                  PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{8}), ValidTime{10},
                  Value::Int64(42))
                  .ok());
  // The resolved definition is reused within the transaction: a value of the
  // wrong type is rejected without re-reading the catalog.
  EXPECT_TRUE(transaction->Set(
                  PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{2}}, PropertyId{8}), ValidTime{10},
                  Value::String("Ada"))
                  .IsSchemaMismatch());
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

TEST_F(KernelMutationTest, SetReusesSchemaWithinTransaction) {
  ASSERT_TRUE(database_
                  ->RegisterProperty(Property(PropertyId{7}, "score",
                                              PropertyEntityKind::kVertex,
                                              PhysicalType::kInt64))
                  .ok());
  auto transaction = Begin();
  for (uint64_t index = 0; index < 8; ++index) {
    ASSERT_TRUE(transaction
                    ->Set(PropertyFact::Vertex(
                              VertexRef{PartId{0}, VertexId{100 + index}},
                              PropertyId{7}),
                          ValidTime{10}, Value::Int64(static_cast<int64_t>(index)))
                    .ok());
  }
  const auto committed = transaction->Commit();
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  EXPECT_EQ(committed.ValueOrDie().outcome, CommitOutcome::kCommitted);
}

TEST_F(KernelMutationTest,
       UsesPropertyRegisteredAfterTheLastCommitInTheNextTransaction) {
  // Regression: BeginTransaction used to reuse the snapshot epoch created for
  // the last commit. RegisterProperty does not advance the visible sequence, so
  // that epoch kept the pre-registration schema catalog and the transaction
  // failed with "property is not registered" (P0 final-acceptance regression).
  auto seed = Begin();
  ASSERT_TRUE(seed->Assert(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}),
                           ValidTime{1})
                  .ok());
  const auto seeded = seed->Commit();
  ASSERT_TRUE(seeded.ok()) << seeded.status().ToString();
  ASSERT_EQ(seeded.ValueOrDie().outcome, CommitOutcome::kCommitted);

  ASSERT_TRUE(database_
                  ->RegisterProperty(Property(PropertyId{11}, "label",
                                              PropertyEntityKind::kVertex,
                                              PhysicalType::kString))
                  .ok());

  auto transaction = Begin();
  const PropertyFact label =
      PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{11});
  const auto staged = transaction->Set(label, ValidTime{2}, Value::String("new"));
  ASSERT_TRUE(staged.ok()) << staged.ToString();
  const auto committed = transaction->Commit();
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  ASSERT_EQ(committed.ValueOrDie().outcome, CommitOutcome::kCommitted);
  ASSERT_TRUE(committed.ValueOrDie().status.ok())
      << committed.ValueOrDie().status.ToString();

  const auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  const auto value = snapshot.ValueOrDie().Get(label, ValidTime{2});
  ASSERT_TRUE(value.ok()) << value.status().ToString();
  ASSERT_TRUE(value.ValueOrDie().has_value());
  EXPECT_EQ(std::get<std::string>(value.ValueOrDie()->data()), "new");
}

TEST_F(KernelMutationTest, DuplicateAssertionFailsAtAssertTime) {
  auto transaction = Begin();
  const EntityFact vertex =
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}});
  ASSERT_TRUE(transaction->Assert(vertex, ValidTime{10}).ok());
  EXPECT_TRUE(transaction->Assert(vertex, ValidTime{10}).IsInvalidArgument());
  EXPECT_TRUE(transaction->Rollback().ok());
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
