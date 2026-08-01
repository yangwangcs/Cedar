// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "cedar/database.h"
#include "cedar/fact/fact_store.h"
#include "model/bitemporal_fact_oracle.h"

#include <rocksdb/db.h>
#include <rocksdb/options.h>

namespace cedar {
namespace {

PendingFactMutation VertexMutation(uint64_t vertex_id, uint64_t valid_from,
                                   FactOperation operation) {
  return PendingFactMutation{EntityFact::Vertex(VertexId{vertex_id}).ref(),
                             ValidTime{valid_from}, operation, 0, std::nullopt};
}

PendingFactMutation PropertyMutation(uint64_t vertex_id, uint16_t property_id,
                                     uint64_t valid_from,
                                     FactOperation operation,
                                     std::optional<Value> value = std::nullopt) {
  return PendingFactMutation{
      PropertyFact::Vertex(VertexId{vertex_id}, PropertyId{property_id}).ref(),
      ValidTime{valid_from}, operation, 1, std::move(value)};
}

PendingFactMutation EdgeMutation(uint64_t edge_id, uint64_t valid_from,
                                 FactOperation operation) {
  return PendingFactMutation{EntityFact::Edge(EdgeId{edge_id}).ref(),
                             ValidTime{valid_from}, operation, 0, std::nullopt};
}

StoreCommitBatch Batch(TxnId txn_id, std::vector<PendingFactMutation> mutations,
                       std::vector<EdgeIdentity> identities = {}) {
  return StoreCommitBatch{txn_id, txn_id.value, std::move(mutations),
                          std::move(identities)};
}

class KernelSnapshotTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_kernel_snapshot_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    path_ = pattern;
  }

  void TearDown() override { std::filesystem::remove_all(path_); }

  void Seed(const std::vector<StoreCommitBatch>& batches) {
    FactStore store(FactStoreOptions{path_});
    ASSERT_TRUE(store.Open().ok());
    for (const StoreCommitBatch& batch : batches) {
      ASSERT_TRUE(store.Commit(batch).ok());
    }
    ASSERT_TRUE(store.Close().ok());
  }

  std::unique_ptr<Database> Open() {
    auto database = Database::Open(DatabaseOptions{.path = path_});
    EXPECT_TRUE(database.ok()) << database.status().ToString();
    return std::move(database).ConsumeValueOrDie();
  }

  std::string path_;
};

TEST_F(KernelSnapshotTest, ResolvesBitemporalStateAndPropertiesAgainstOracle) {
  const FactRef vertex = EntityFact::Vertex(VertexId{1}).ref();
  const FactRef property =
      PropertyFact::Vertex(VertexId{1}, PropertyId{7}).ref();
  test::BitemporalFactOracle oracle;
  const std::vector<StoreCommitBatch> batches = {
      Batch(TxnId{1}, {VertexMutation(1, 10, FactOperation::kPut),
                       PropertyMutation(1, 7, 10, FactOperation::kPut,
                                        Value::Int64(10))}),
      Batch(TxnId{2}, {VertexMutation(1, 30, FactOperation::kPut),
                       PropertyMutation(1, 7, 30, FactOperation::kPut,
                                        Value::Int64(30))}),
      Batch(TxnId{3}, {VertexMutation(1, 20, FactOperation::kDelete),
                       PropertyMutation(1, 7, 20, FactOperation::kDelete)}),
      Batch(TxnId{4}, {VertexMutation(1, 20, FactOperation::kPut),
                       PropertyMutation(1, 7, 20, FactOperation::kPut,
                                        Value::Int64(20))}),
  };
  for (const StoreCommitBatch& batch : batches) {
    for (const PendingFactMutation& mutation : batch.mutations) {
      oracle.Add(FactEvent{mutation.ref, mutation.valid_from,
                           CommitSeq{batch.txn_id.value}, mutation.operation,
                           mutation.schema_epoch, mutation.value});
    }
  }
  Seed(batches);
  const std::unique_ptr<Database> database = Open();

  for (const CommitSeq sequence : {CommitSeq{1}, CommitSeq{3}, CommitSeq{4}}) {
    const auto snapshot = database->BeginSnapshot(SnapshotOptions{sequence});
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    const auto expected_vertex = oracle.Read(vertex, ValidTime{25}, sequence);
    const auto expected_property = oracle.Read(property, ValidTime{25}, sequence);
    const auto exists = snapshot.ValueOrDie().Exists(EntityFact::Vertex(VertexId{1}),
                                                      ValidTime{25});
    ASSERT_TRUE(exists.ok()) << exists.status().ToString();
    EXPECT_EQ(exists.ValueOrDie(), expected_vertex.has_value());
    const auto value = snapshot.ValueOrDie().Get(
        PropertyFact::Vertex(VertexId{1}, PropertyId{7}), ValidTime{25});
    ASSERT_TRUE(value.ok()) << value.status().ToString();
    EXPECT_EQ(value.ValueOrDie(),
              expected_property.has_value() ? expected_property->value
                                            : std::optional<Value>{});
  }

  const auto latest = database->BeginSnapshot();
  ASSERT_TRUE(latest.ok()) << latest.status().ToString();
  const auto value = latest.ValueOrDie().Get(
      PropertyFact::Vertex(VertexId{1}, PropertyId{7}), ValidTime{35});
  ASSERT_TRUE(value.ok()) << value.status().ToString();
  EXPECT_EQ(value.ValueOrDie(), std::optional<Value>{Value::Int64(30)});
}

TEST_F(KernelSnapshotTest, ScansOnlyFactsVisibleAtTheCapturedSequence) {
  Seed({Batch(TxnId{1}, {PropertyMutation(1, 7, 10, FactOperation::kPut,
                                          Value::Int64(10))}),
        Batch(TxnId{2}, {PropertyMutation(1, 7, 20, FactOperation::kPut,
                                          Value::Int64(20))}),
        Batch(TxnId{3}, {PropertyMutation(1, 7, 20, FactOperation::kDelete)})});
  const std::unique_ptr<Database> database = Open();
  const auto snapshot = database->BeginSnapshot(SnapshotOptions{CommitSeq{2}});
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();

  std::vector<FactEvent> events;
  ASSERT_TRUE(snapshot.ValueOrDie()
                  .Scan(FactFamily::kVertexProperty, PropertyId{7},
                        [&](const FactEvent& event) {
                          events.push_back(event);
                          return Status::OK();
                        })
                  .ok());
  ASSERT_EQ(events.size(), 2U);
  for (const FactEvent& event : events) {
    EXPECT_LE(event.commit_seq.value, 2U);
  }
}

TEST_F(KernelSnapshotTest, RequiresBothEndpointStatesForEdgeVisibility) {
  const EdgeIdentity identity{EdgeId{9}, VertexId{1}, VertexId{2}, 7};
  Seed({Batch(TxnId{1}, {VertexMutation(1, 10, FactOperation::kPut),
                         VertexMutation(2, 10, FactOperation::kPut),
                         EdgeMutation(9, 10, FactOperation::kPut),
                         PendingFactMutation{
                             PropertyFact::Edge(EdgeId{9}, PropertyId{7}).ref(),
                             ValidTime{10}, FactOperation::kPut, 1,
                             Value::Int64(99)}},
              {identity}),
        Batch(TxnId{2}, {VertexMutation(1, 20, FactOperation::kDelete)}),
        Batch(TxnId{3}, {VertexMutation(1, 30, FactOperation::kPut)}),
        Batch(TxnId{4}, {VertexMutation(2, 40, FactOperation::kDelete)}),
        Batch(TxnId{5}, {VertexMutation(2, 45, FactOperation::kPut)}),
        Batch(TxnId{6}, {EdgeMutation(9, 50, FactOperation::kDelete)})});
  const std::unique_ptr<Database> database = Open();
  const auto snapshot = database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();

  for (const auto [valid_time, expected] :
       {std::pair{ValidTime{15}, true}, std::pair{ValidTime{25}, false},
        std::pair{ValidTime{35}, true}, std::pair{ValidTime{42}, false},
        std::pair{ValidTime{47}, true}, std::pair{ValidTime{55}, false}}) {
    const auto exists = snapshot.ValueOrDie().Exists(EntityFact::Edge(EdgeId{9}),
                                                      valid_time);
    ASSERT_TRUE(exists.ok()) << exists.status().ToString();
    EXPECT_EQ(exists.ValueOrDie(), expected);
  }
  const auto visible_property = snapshot.ValueOrDie().Get(
      PropertyFact::Edge(EdgeId{9}, PropertyId{7}), ValidTime{15});
  ASSERT_TRUE(visible_property.ok()) << visible_property.status().ToString();
  EXPECT_EQ(visible_property.ValueOrDie(), std::optional<Value>{Value::Int64(99)});
  const auto hidden_property = snapshot.ValueOrDie().Get(
      PropertyFact::Edge(EdgeId{9}, PropertyId{7}), ValidTime{25});
  ASSERT_TRUE(hidden_property.ok()) << hidden_property.status().ToString();
  EXPECT_FALSE(hidden_property.ValueOrDie().has_value());

  const auto historical = database->BeginSnapshot(SnapshotOptions{CommitSeq{1}});
  ASSERT_TRUE(historical.ok()) << historical.status().ToString();
  const auto historical_edge = historical.ValueOrDie().Exists(
      EntityFact::Edge(EdgeId{9}), ValidTime{55});
  ASSERT_TRUE(historical_edge.ok()) << historical_edge.status().ToString();
  EXPECT_TRUE(historical_edge.ValueOrDie());
}

TEST_F(KernelSnapshotTest, PreservesBitemporalReadsAfterFlushCompactionAndReopen) {
  Seed({Batch(TxnId{1}, {VertexMutation(1, 10, FactOperation::kPut),
                         PropertyMutation(1, 7, 10, FactOperation::kPut,
                                          Value::Int64(10))}),
        Batch(TxnId{2}, {PropertyMutation(1, 7, 20, FactOperation::kPut,
                                          Value::Int64(20))}),
        Batch(TxnId{3}, {PropertyMutation(1, 7, 20, FactOperation::kDelete)})});

  rocksdb::Options options;
  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors = {
      {rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions(options)},
      {"facts", rocksdb::ColumnFamilyOptions(options)},
      {"meta", rocksdb::ColumnFamilyOptions(options)},
  };
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  std::unique_ptr<rocksdb::DB> raw_database;
  ASSERT_TRUE(rocksdb::DB::Open(options, path_, descriptors, &handles, &raw_database)
                  .ok());
  ASSERT_EQ(handles.size(), 3U);
  rocksdb::FlushOptions flush_options;
  flush_options.wait = true;
  ASSERT_TRUE(raw_database->Flush(flush_options, handles[1]).ok());
  ASSERT_TRUE(raw_database->CompactRange(rocksdb::CompactRangeOptions(), handles[1],
                                         nullptr, nullptr)
                  .ok());
  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    raw_database->DestroyColumnFamilyHandle(handle);
  }
  raw_database.reset();

  const std::unique_ptr<Database> database = Open();
  const auto before_delete = database->BeginSnapshot(SnapshotOptions{CommitSeq{2}});
  ASSERT_TRUE(before_delete.ok()) << before_delete.status().ToString();
  const auto before_value = before_delete.ValueOrDie().Get(
      PropertyFact::Vertex(VertexId{1}, PropertyId{7}), ValidTime{25});
  ASSERT_TRUE(before_value.ok()) << before_value.status().ToString();
  EXPECT_EQ(before_value.ValueOrDie(), std::optional<Value>{Value::Int64(20)});

  const auto latest = database->BeginSnapshot();
  ASSERT_TRUE(latest.ok()) << latest.status().ToString();
  const auto deleted_value = latest.ValueOrDie().Get(
      PropertyFact::Vertex(VertexId{1}, PropertyId{7}), ValidTime{25});
  ASSERT_TRUE(deleted_value.ok()) << deleted_value.status().ToString();
  EXPECT_FALSE(deleted_value.ValueOrDie().has_value());
}

}  // namespace
}  // namespace cedar
