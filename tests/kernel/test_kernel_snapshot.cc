// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "cedar/database.h"
#include "storage/facts/fact_store.h"
#include "model/bitemporal_fact_oracle.h"

#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include "storage/rocks/rocksdb_config.h"

namespace cedar {
namespace {

PendingFactMutation VertexMutation(uint64_t vertex_id, uint64_t valid_from,
                                   FactOperation operation) {
  return PendingFactMutation{EntityFact::Vertex(VertexRef{PartId{0}, VertexId{vertex_id}}).ref(),
                             ValidTime{valid_from}, operation, 0, std::nullopt};
}

PendingFactMutation PropertyMutation(uint64_t vertex_id, uint16_t property_id,
                                     uint64_t valid_from,
                                     FactOperation operation,
                                     std::optional<Value> value = std::nullopt) {
  return PendingFactMutation{
      PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{vertex_id}}, PropertyId{property_id}).ref(),
      ValidTime{valid_from}, operation, 1, std::move(value)};
}

PendingFactMutation EdgeMutation(uint64_t edge_id, uint64_t valid_from,
                                 FactOperation operation) {
  return PendingFactMutation{EntityFact::Edge(EdgeRef{PartId{0}, EdgeId{edge_id}}).ref(),
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
  const FactRef vertex = EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}).ref();
  const FactRef property =
      PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{7}).ref();
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
    const auto exists = snapshot.ValueOrDie().Exists(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}),
                                                      ValidTime{25});
    ASSERT_TRUE(exists.ok()) << exists.status().ToString();
    EXPECT_EQ(exists.ValueOrDie(), expected_vertex.has_value());
    const auto value = snapshot.ValueOrDie().Get(
        PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{7}), ValidTime{25});
    ASSERT_TRUE(value.ok()) << value.status().ToString();
    EXPECT_EQ(value.ValueOrDie(),
              expected_property.has_value() ? expected_property->value
                                            : std::optional<Value>{});
  }

  const auto latest = database->BeginSnapshot();
  ASSERT_TRUE(latest.ok()) << latest.status().ToString();
  const auto value = latest.ValueOrDie().Get(
      PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{7}), ValidTime{35});
  ASSERT_TRUE(value.ok()) << value.status().ToString();
  EXPECT_EQ(value.ValueOrDie(), std::optional<Value>{Value::Int64(30)});
}

TEST_F(KernelSnapshotTest, MultiExistsPreservesRequestOrder) {
  Seed({Batch(TxnId{1}, {VertexMutation(1, 1, FactOperation::kPut),
                         VertexMutation(3, 1, FactOperation::kPut)})});
  auto database = Open();
  auto snapshot = database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  const auto values = snapshot.ValueOrDie().MultiExists(
      {EntityFact::Vertex({PartId{0}, VertexId{3}}),
       EntityFact::Vertex({PartId{0}, VertexId{2}}),
       EntityFact::Vertex({PartId{0}, VertexId{1}})},
      ValidTime{1});
  ASSERT_TRUE(values.ok()) << values.status().ToString();
  ASSERT_EQ(values.ValueOrDie().size(), 3U);
  EXPECT_TRUE(values.ValueOrDie()[0]);
  EXPECT_FALSE(values.ValueOrDie()[1]);
  EXPECT_TRUE(values.ValueOrDie()[2]);
  const auto metrics = database->SampleRuntimeMetrics();
  ASSERT_TRUE(metrics.ok()) << metrics.status().ToString();
  EXPECT_GE(metrics.ValueOrDie().multi_get_operations, 3U);
  EXPECT_GE(metrics.ValueOrDie().point_read_operations, 3U);
  EXPECT_GT(metrics.ValueOrDie().canonical_read_physical_bytes, 0U);
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
  const Status scanned = snapshot.ValueOrDie().Scan(
      FactFamily::kVertexProperty, PropertyId{7}, [&](const FactEvent& event) {
        events.push_back(event);
        return Status::OK();
      });
  ASSERT_TRUE(scanned.ok()) << scanned.ToString();
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
                             PropertyFact::Edge(EdgeRef{PartId{0}, EdgeId{9}}, PropertyId{7}).ref(),
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
    const auto exists = snapshot.ValueOrDie().Exists(EntityFact::Edge(EdgeRef{PartId{0}, EdgeId{9}}),
                                                      valid_time);
    ASSERT_TRUE(exists.ok()) << exists.status().ToString();
    EXPECT_EQ(exists.ValueOrDie(), expected);
  }
  const auto visible_property = snapshot.ValueOrDie().Get(
      PropertyFact::Edge(EdgeRef{PartId{0}, EdgeId{9}}, PropertyId{7}), ValidTime{15});
  ASSERT_TRUE(visible_property.ok()) << visible_property.status().ToString();
  EXPECT_EQ(visible_property.ValueOrDie(), std::optional<Value>{Value::Int64(99)});
  const auto hidden_property = snapshot.ValueOrDie().Get(
      PropertyFact::Edge(EdgeRef{PartId{0}, EdgeId{9}}, PropertyId{7}), ValidTime{25});
  ASSERT_TRUE(hidden_property.ok()) << hidden_property.status().ToString();
  EXPECT_FALSE(hidden_property.ValueOrDie().has_value());

  const auto historical = database->BeginSnapshot(SnapshotOptions{CommitSeq{1}});
  ASSERT_TRUE(historical.ok()) << historical.status().ToString();
  const auto historical_edge = historical.ValueOrDie().Exists(
      EntityFact::Edge(EdgeRef{PartId{0}, EdgeId{9}}), ValidTime{55});
  ASSERT_TRUE(historical_edge.ok()) << historical_edge.status().ToString();
  EXPECT_TRUE(historical_edge.ValueOrDie());
}

TEST_F(KernelSnapshotTest, GetPointAtReadsLatestValueLikeSnapshotGet) {
  Seed({Batch(TxnId{1}, {VertexMutation(1, 10, FactOperation::kPut),
                         PropertyMutation(1, 7, 10, FactOperation::kPut,
                                          Value::Int64(10))}),
        Batch(TxnId{2}, {PropertyMutation(1, 7, 20, FactOperation::kPut,
                                          Value::Int64(20))})});
  const std::unique_ptr<Database> database = Open();
  const PropertyFact property =
      PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{7});

  const auto snapshot = database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  const auto expected = snapshot.ValueOrDie().Get(property, ValidTime{25});
  ASSERT_TRUE(expected.ok()) << expected.status().ToString();

  const auto actual = database->GetPointAt(property.ref(), ValidTime{25});
  ASSERT_TRUE(actual.ok()) << actual.status().ToString();
  EXPECT_EQ(actual.ValueOrDie(), expected.ValueOrDie());
  EXPECT_EQ(actual.ValueOrDie(), std::optional<Value>{Value::Int64(20)});
}

TEST_F(KernelSnapshotTest, GetPointAtReturnsMissingForAbsentFact) {
  Seed({Batch(TxnId{1}, {VertexMutation(1, 10, FactOperation::kPut)})});
  const std::unique_ptr<Database> database = Open();
  const PropertyFact absent =
      PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{7});

  const auto snapshot = database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  const auto expected = snapshot.ValueOrDie().Get(absent, ValidTime{15});
  ASSERT_TRUE(expected.ok()) << expected.status().ToString();
  EXPECT_FALSE(expected.ValueOrDie().has_value());

  const auto actual = database->GetPointAt(absent.ref(), ValidTime{15});
  ASSERT_TRUE(actual.ok()) << actual.status().ToString();
  EXPECT_FALSE(actual.ValueOrDie().has_value());
  EXPECT_EQ(actual.ValueOrDie(), expected.ValueOrDie());
}

TEST_F(KernelSnapshotTest, GetPointAtReadsStagedDeleteAfterCommit) {
  Seed({Batch(TxnId{1}, {VertexMutation(1, 10, FactOperation::kPut),
                         PropertyMutation(1, 7, 10, FactOperation::kPut,
                                          Value::Int64(10))}),
        Batch(TxnId{2}, {PropertyMutation(1, 7, 20, FactOperation::kDelete)})});
  const std::unique_ptr<Database> database = Open();
  const PropertyFact property =
      PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{7});

  const auto before_delete = database->GetPointAt(property.ref(), ValidTime{15});
  ASSERT_TRUE(before_delete.ok()) << before_delete.status().ToString();
  EXPECT_EQ(before_delete.ValueOrDie(), std::optional<Value>{Value::Int64(10)});

  const auto snapshot = database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  const auto expected = snapshot.ValueOrDie().Get(property, ValidTime{25});
  ASSERT_TRUE(expected.ok()) << expected.status().ToString();
  EXPECT_FALSE(expected.ValueOrDie().has_value());

  const auto after_delete = database->GetPointAt(property.ref(), ValidTime{25});
  ASSERT_TRUE(after_delete.ok()) << after_delete.status().ToString();
  EXPECT_FALSE(after_delete.ValueOrDie().has_value());
  EXPECT_EQ(after_delete.ValueOrDie(), expected.ValueOrDie());
}

TEST_F(KernelSnapshotTest, GetPointAtRejectsInvalidRef) {
  Seed({Batch(TxnId{1}, {VertexMutation(1, 10, FactOperation::kPut)})});
  const std::unique_ptr<Database> database = Open();
  const PropertyFact invalid =
      PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{});
  ASSERT_TRUE(invalid.ref().Validate().IsInvalidArgument());

  const auto snapshot = database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  const auto expected = snapshot.ValueOrDie().Get(invalid, ValidTime{15});
  ASSERT_FALSE(expected.ok());
  EXPECT_TRUE(expected.status().IsInvalidArgument());

  const auto actual = database->GetPointAt(invalid.ref(), ValidTime{15});
  ASSERT_FALSE(actual.ok());
  EXPECT_TRUE(actual.status().IsInvalidArgument());
  EXPECT_EQ(actual.status().ToString(), expected.status().ToString());
}

TEST_F(KernelSnapshotTest, GetPointAtMatchesSnapshotGetForVertexProperty) {
  Seed({Batch(TxnId{1}, {VertexMutation(1, 10, FactOperation::kPut),
                         PropertyMutation(1, 7, 10, FactOperation::kPut,
                                          Value::Int64(10))}),
        Batch(TxnId{2}, {PropertyMutation(1, 7, 20, FactOperation::kPut,
                                          Value::Int64(20))})});
  const std::unique_ptr<Database> database = Open();
  const PropertyFact property =
      PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{7});

  const auto snapshot = database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  for (const ValidTime valid_time :
       {ValidTime{5}, ValidTime{15}, ValidTime{25}, ValidTime{35}}) {
    const auto expected = snapshot.ValueOrDie().Get(property, valid_time);
    ASSERT_TRUE(expected.ok()) << expected.status().ToString();
    const auto actual = database->GetPointAt(property.ref(), valid_time);
    ASSERT_TRUE(actual.ok()) << actual.status().ToString();
    EXPECT_EQ(actual.ValueOrDie(), expected.ValueOrDie())
        << "valid_time=" << valid_time.value;
  }
}

TEST_F(KernelSnapshotTest, GetPointAtMatchesSnapshotGetForEdgeProperty) {
  const EdgeIdentity identity{EdgeId{9}, VertexId{1}, VertexId{2}, 7};
  Seed({Batch(TxnId{1}, {VertexMutation(1, 10, FactOperation::kPut),
                         VertexMutation(2, 10, FactOperation::kPut),
                         EdgeMutation(9, 10, FactOperation::kPut),
                         PendingFactMutation{
                             PropertyFact::Edge(EdgeRef{PartId{0}, EdgeId{9}},
                                                PropertyId{7}).ref(),
                             ValidTime{10}, FactOperation::kPut, 1,
                             Value::Int64(99)}},
              {identity}),
        Batch(TxnId{2}, {VertexMutation(1, 20, FactOperation::kDelete)}),
        Batch(TxnId{3}, {VertexMutation(1, 30, FactOperation::kPut)}),
        Batch(TxnId{4}, {VertexMutation(2, 40, FactOperation::kDelete)}),
        Batch(TxnId{5}, {VertexMutation(2, 45, FactOperation::kPut)}),
        Batch(TxnId{6}, {EdgeMutation(9, 50, FactOperation::kDelete)})});
  const std::unique_ptr<Database> database = Open();
  const PropertyFact property =
      PropertyFact::Edge(EdgeRef{PartId{0}, EdgeId{9}}, PropertyId{7});

  const auto snapshot = database->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  bool saw_visible = false;
  bool saw_hidden = false;
  for (const ValidTime valid_time : {ValidTime{15}, ValidTime{25},
                                     ValidTime{35}, ValidTime{42},
                                     ValidTime{47}, ValidTime{55}}) {
    const auto expected = snapshot.ValueOrDie().Get(property, valid_time);
    ASSERT_TRUE(expected.ok()) << expected.status().ToString();
    const auto actual = database->GetPointAt(property.ref(), valid_time);
    ASSERT_TRUE(actual.ok()) << actual.status().ToString();
    EXPECT_EQ(actual.ValueOrDie(), expected.ValueOrDie())
        << "valid_time=" << valid_time.value;
    saw_visible = saw_visible || expected.ValueOrDie().has_value();
    saw_hidden = saw_hidden || !expected.ValueOrDie().has_value();
  }
  // The committed endpoint/edge deletes must exercise both sides of the filter,
  // otherwise this test would not distinguish the shared path from a bare read.
  EXPECT_TRUE(saw_visible);
  EXPECT_TRUE(saw_hidden);
}

TEST_F(KernelSnapshotTest, GetPointAtRecordsPointReadMetric) {
  Seed({Batch(TxnId{1}, {VertexMutation(1, 10, FactOperation::kPut),
                         PropertyMutation(1, 7, 10, FactOperation::kPut,
                                          Value::Int64(10))})});
  const std::unique_ptr<Database> database = Open();
  const PropertyFact property =
      PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{7});

  const auto before = database->SampleRuntimeMetrics();
  ASSERT_TRUE(before.ok()) << before.status().ToString();
  const auto actual = database->GetPointAt(property.ref(), ValidTime{15});
  ASSERT_TRUE(actual.ok()) << actual.status().ToString();
  const auto after = database->SampleRuntimeMetrics();
  ASSERT_TRUE(after.ok()) << after.status().ToString();
  EXPECT_EQ(after.ValueOrDie().point_read_operations,
            before.ValueOrDie().point_read_operations + 1);
}

TEST_F(KernelSnapshotTest, GetPointAtAtMatchesSnapshotGetForHistoricalVintage) {
  Seed({Batch(TxnId{1}, {VertexMutation(1, 10, FactOperation::kPut),
                         PropertyMutation(1, 7, 10, FactOperation::kPut,
                                          Value::Int64(10))}),
        Batch(TxnId{2}, {PropertyMutation(1, 7, 20, FactOperation::kPut,
                                          Value::Int64(20))})});
  const std::unique_ptr<Database> database = Open();
  const PropertyFact property =
      PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{7});

  const auto latest = database->BeginSnapshot();
  ASSERT_TRUE(latest.ok()) << latest.status().ToString();
  const CommitSeq visible = latest.ValueOrDie().commit_seq();
  EXPECT_EQ(visible, CommitSeq{2});
  const auto latest_expected = latest.ValueOrDie().Get(property, ValidTime{25});
  ASSERT_TRUE(latest_expected.ok()) << latest_expected.status().ToString();
  EXPECT_EQ(latest_expected.ValueOrDie(), std::optional<Value>{Value::Int64(20)});

  // as_of at the captured visible sequence is the latest-vintage read and must
  // agree with GetPointAt, which pins the current epoch.
  const auto latest_actual =
      database->GetPointAtAt(visible, property.ref(), ValidTime{25});
  ASSERT_TRUE(latest_actual.ok()) << latest_actual.status().ToString();
  EXPECT_EQ(latest_actual.ValueOrDie(), latest_expected.ValueOrDie());
  const auto latest_point = database->GetPointAt(property.ref(), ValidTime{25});
  ASSERT_TRUE(latest_point.ok()) << latest_point.status().ToString();
  EXPECT_EQ(latest_actual.ValueOrDie(), latest_point.ValueOrDie());

  // A historical vintage must not observe the later revision.
  const auto historical = database->BeginSnapshot(SnapshotOptions{CommitSeq{1}});
  ASSERT_TRUE(historical.ok()) << historical.status().ToString();
  const auto historical_expected =
      historical.ValueOrDie().Get(property, ValidTime{25});
  ASSERT_TRUE(historical_expected.ok())
      << historical_expected.status().ToString();
  EXPECT_EQ(historical_expected.ValueOrDie(),
            std::optional<Value>{Value::Int64(10)});
  for (int repeat = 0; repeat < 3; ++repeat) {
    // Repeated reads exercise the bounded per-sequence as_of epoch cache.
    const auto actual =
        database->GetPointAtAt(CommitSeq{1}, property.ref(), ValidTime{25});
    ASSERT_TRUE(actual.ok()) << actual.status().ToString();
    EXPECT_EQ(actual.ValueOrDie(), historical_expected.ValueOrDie());
  }
}

TEST_F(KernelSnapshotTest, GetPointAtAtAppliesEdgeVisibilityAtHistoricalSequence) {
  const EdgeIdentity identity{EdgeId{9}, VertexId{1}, VertexId{2}, 7};
  Seed({Batch(TxnId{1}, {VertexMutation(1, 10, FactOperation::kPut),
                         VertexMutation(2, 10, FactOperation::kPut),
                         EdgeMutation(9, 10, FactOperation::kPut),
                         PendingFactMutation{
                             PropertyFact::Edge(EdgeRef{PartId{0}, EdgeId{9}},
                                                PropertyId{7}).ref(),
                             ValidTime{10}, FactOperation::kPut, 1,
                             Value::Int64(99)}},
               {identity}),
        Batch(TxnId{2}, {VertexMutation(1, 20, FactOperation::kDelete)})});
  const std::unique_ptr<Database> database = Open();
  const PropertyFact property =
      PropertyFact::Edge(EdgeRef{PartId{0}, EdgeId{9}}, PropertyId{7});

  struct Probe {
    CommitSeq sequence;
    ValidTime valid_time;
  };
  bool saw_visible = false;
  bool saw_hidden = false;
  for (const Probe probe : {Probe{CommitSeq{1}, ValidTime{15}},
                            Probe{CommitSeq{1}, ValidTime{25}},
                            Probe{CommitSeq{2}, ValidTime{15}},
                            Probe{CommitSeq{2}, ValidTime{25}}}) {
    const auto snapshot =
        database->BeginSnapshot(SnapshotOptions{probe.sequence});
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    const auto expected =
        snapshot.ValueOrDie().Get(property, probe.valid_time);
    ASSERT_TRUE(expected.ok()) << expected.status().ToString();
    const auto actual = database->GetPointAtAt(probe.sequence, property.ref(),
                                               probe.valid_time);
    ASSERT_TRUE(actual.ok()) << actual.status().ToString();
    EXPECT_EQ(actual.ValueOrDie(), expected.ValueOrDie())
        << "sequence=" << probe.sequence.value
        << " valid_time=" << probe.valid_time.value;
    saw_visible = saw_visible || expected.ValueOrDie().has_value();
    saw_hidden = saw_hidden || !expected.ValueOrDie().has_value();
  }
  // Endpoint visibility at the historical sequence must be exercised in both
  // directions, otherwise the as_of point path is not sharing the edge filter.
  EXPECT_TRUE(saw_visible);
  EXPECT_TRUE(saw_hidden);
}

TEST_F(KernelSnapshotTest, GetPointAtAtReportsExpiredAndInvalidSequences) {
  Seed({Batch(TxnId{1}, {VertexMutation(1, 10, FactOperation::kPut),
                         PropertyMutation(1, 7, 10, FactOperation::kPut,
                                          Value::Int64(10))}),
        Batch(TxnId{2}, {VertexMutation(1, 20, FactOperation::kPut)})});
  {
    FactStore store(FactStoreOptions{path_});
    ASSERT_TRUE(store.Open().ok());
    ASSERT_TRUE(store.Vacuum(CommitSeq{1}).ok());
    ASSERT_TRUE(store.Close().ok());
  }
  const std::unique_ptr<Database> database = Open();
  const PropertyFact property =
      PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{7});

  const auto expired_snapshot =
      database->BeginSnapshot(SnapshotOptions{CommitSeq{0}});
  ASSERT_FALSE(expired_snapshot.ok());
  EXPECT_TRUE(expired_snapshot.status().IsSnapshotExpired());
  const auto expired =
      database->GetPointAtAt(CommitSeq{0}, property.ref(), ValidTime{15});
  ASSERT_FALSE(expired.ok());
  EXPECT_TRUE(expired.status().IsSnapshotExpired());
  EXPECT_EQ(expired.status().ToString(), expired_snapshot.status().ToString());

  const auto invalid_snapshot =
      database->BeginSnapshot(SnapshotOptions{CommitSeq{3}});
  ASSERT_FALSE(invalid_snapshot.ok());
  EXPECT_TRUE(invalid_snapshot.status().IsInvalidArgument());
  const auto invalid =
      database->GetPointAtAt(CommitSeq{3}, property.ref(), ValidTime{15});
  ASSERT_FALSE(invalid.ok());
  EXPECT_TRUE(invalid.status().IsInvalidArgument());
  EXPECT_EQ(invalid.status().ToString(), invalid_snapshot.status().ToString());

  const PropertyFact invalid_ref =
      PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{});
  ASSERT_TRUE(invalid_ref.ref().Validate().IsInvalidArgument());
  const auto invalid_ref_snapshot =
      database->BeginSnapshot(SnapshotOptions{CommitSeq{1}});
  ASSERT_TRUE(invalid_ref_snapshot.ok())
      << invalid_ref_snapshot.status().ToString();
  const auto invalid_ref_expected =
      invalid_ref_snapshot.ValueOrDie().Get(invalid_ref, ValidTime{15});
  ASSERT_FALSE(invalid_ref_expected.ok());
  const auto invalid_ref_actual =
      database->GetPointAtAt(CommitSeq{1}, invalid_ref.ref(), ValidTime{15});
  ASSERT_FALSE(invalid_ref_actual.ok());
  EXPECT_TRUE(invalid_ref_actual.status().IsInvalidArgument());
  EXPECT_EQ(invalid_ref_actual.status().ToString(),
            invalid_ref_expected.status().ToString());
}

TEST_F(KernelSnapshotTest, GetPointAtFastPathTracksConcurrentCommits) {
  {
    // Register the schema and seed the first revision in one store lifetime so
    // the first snapshot epoch created by the Database already sees the
    // property catalog (an epoch caches the catalog at creation).
    FactStore store(FactStoreOptions{path_});
    ASSERT_TRUE(store.Open().ok());
    ASSERT_TRUE(store.RegisterProperty(PropertyDefinition{
                                        PropertyId{7}, 0, "value",
                                        PropertyEntityKind::kVertex,
                                        PhysicalType::kInt64, 4096})
                    .ok());
    ASSERT_TRUE(store.Commit(Batch(TxnId{1}, {VertexMutation(1, 10, FactOperation::kPut),
                                             PropertyMutation(1, 7, 10, FactOperation::kPut,
                                                              Value::Int64(0))}))
                    .ok());
    ASSERT_TRUE(store.Close().ok());
  }
  auto opened = Database::Open(DatabaseOptions{.path = path_});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
  const PropertyFact property =
      PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{7});
  constexpr int kReaders = 8;
  constexpr int kReadsPerReader = 10000;

  std::atomic<bool> done{false};
  std::atomic<bool> failed{false};
  std::atomic<uint64_t> commits{0};
  std::atomic<uint64_t> reads{0};
  // Highest value whose commit has *returned* to the writer. Values are
  // published monotonically, so any value above this bound would be a read of an
  // uncommitted vintage.
  std::atomic<int64_t> committed_max{0};
  std::mutex failure_mutex;
  std::string failure;
  const auto fail = [&](const std::string& message) {
    std::lock_guard<std::mutex> lock(failure_mutex);
    if (!failed.exchange(true)) failure = message;
  };

  std::thread writer([&] {
    int64_t next = 0;
    while (!done.load(std::memory_order_relaxed)) {
      const auto transaction = database->BeginTransaction();
      if (!transaction.ok()) {
        fail("begin: " + transaction.status().ToString());
        return;
      }
      const Status asserted = transaction.ValueOrDie()->Assert(
          EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10});
      if (!asserted.ok()) {
        fail("assert: " + asserted.ToString());
        return;
      }
      const int64_t value = ++next;
      const Status set = transaction.ValueOrDie()->Set(
          property, ValidTime{10}, Value::Int64(value));
      if (!set.ok()) {
        fail("set: " + set.ToString());
        return;
      }
      const auto committed = transaction.ValueOrDie()->Commit();
      if (!committed.ok()) {
        fail("commit: " + committed.status().ToString());
        return;
      }
      committed_max.store(value, std::memory_order_release);
      commits.fetch_add(1, std::memory_order_relaxed);
    }
  });

  std::vector<std::thread> readers;
  readers.reserve(kReaders);
  for (int reader = 0; reader < kReaders; ++reader) {
    readers.emplace_back([&] {
      int64_t last_observed = 0;
      for (int index = 0; index < kReadsPerReader; ++index) {
        const auto value = database->GetPointAt(property.ref(), ValidTime{10});
        if (!value.ok()) {
          fail("read: " + value.status().ToString());
          return;
        }
        if (!value.ValueOrDie().has_value()) {
          fail("read: committed value is missing");
          return;
        }
        const int64_t observed = std::get<int64_t>(value.ValueOrDie()->data());
        // The writer is a single thread that publishes values in increasing
        // order. A commit is published before Commit() returns, so at most one
        // already-published value can still be missing from committed_max; any
        // larger value would be uncommitted.
        if (observed < 0 ||
            observed > committed_max.load(std::memory_order_acquire) + 1) {
          fail("read: observed uncommitted value " +
               std::to_string(observed));
          return;
        }
        // Successive point reads at the same valid time must be monotonic: the
        // visible watermark only advances, so a later read can never resurrect
        // an older revision.
        if (observed < last_observed) {
          fail("read: observed value went backwards " +
               std::to_string(observed) + " after " +
               std::to_string(last_observed));
          return;
        }
        last_observed = observed;
        reads.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  for (std::thread& reader : readers) reader.join();
  done.store(true, std::memory_order_relaxed);
  writer.join();

  EXPECT_FALSE(failed.load()) << failure;
  EXPECT_EQ(reads.load(), kReaders * kReadsPerReader);
  EXPECT_GT(commits.load(), 0U);
}

TEST_F(KernelSnapshotTest, CloseRacingFastPathLeavesNoLivePins) {
  Seed({Batch(TxnId{1}, {VertexMutation(1, 10, FactOperation::kPut),
                         PropertyMutation(1, 7, 10, FactOperation::kPut,
                                          Value::Int64(10))})});
  const PropertyFact property =
      PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{7});

  for (int round = 0; round < 5; ++round) {
    auto opened = Database::Open(DatabaseOptions{.path = path_});
    ASSERT_TRUE(opened.ok()) << opened.status().ToString();
    std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
    // Deterministic pinned-close coverage: a live handle must make Close report
    // SnapshotPinned (exercising the retry path) and leave the store usable.
    {
      auto held = database->BeginSnapshot();
      ASSERT_TRUE(held.ok()) << held.status().ToString();
      const Status pinned = database->Close();
      EXPECT_TRUE(pinned.IsSnapshotPinned()) << pinned.ToString();
      const auto still_readable =
          database->GetPointAt(property.ref(), ValidTime{15});
      ASSERT_TRUE(still_readable.ok()) << still_readable.status().ToString();
      EXPECT_EQ(still_readable.ValueOrDie(), std::optional<Value>{Value::Int64(10)});
    }
    std::atomic<bool> stop{false};
    std::atomic<uint64_t> succeeded{0};
    std::atomic<uint64_t> rejected{0};
    std::atomic<uint64_t> unexpected{0};
    std::vector<std::thread> readers;
    readers.reserve(4);
    for (int reader = 0; reader < 4; ++reader) {
      readers.emplace_back([&] {
        while (!stop.load(std::memory_order_relaxed)) {
          const auto value =
              database->GetPointAt(property.ref(), ValidTime{15});
          if (value.ok()) {
            if (value.ValueOrDie().has_value()) {
              succeeded.fetch_add(1, std::memory_order_relaxed);
            } else {
              unexpected.fetch_add(1, std::memory_order_relaxed);
            }
          } else if (value.status().IsShutdownInProgress() ||
                     value.status().IsInvalidArgument()) {
            rejected.fetch_add(1, std::memory_order_relaxed);
          } else {
            unexpected.fetch_add(1, std::memory_order_relaxed);
          }
        }
      });
    }
    // Let the readers enter the lock-free path, then close underneath them.
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    Status closed;
    for (int attempt = 0; attempt < 500; ++attempt) {
      closed = database->Close();
      if (closed.ok()) break;
      EXPECT_TRUE(closed.IsSnapshotPinned()) << closed.ToString();
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(closed.ok()) << closed.ToString();
    // A successful close implies zero live pins: the store is gone, so a new
    // read is rejected instead of returning stale data.
    const auto after_close =
        database->GetPointAt(property.ref(), ValidTime{15});
    ASSERT_FALSE(after_close.ok());
    EXPECT_TRUE(after_close.status().IsInvalidArgument());
    stop.store(true, std::memory_order_relaxed);
    for (std::thread& reader : readers) reader.join();
    EXPECT_EQ(unexpected.load(), 0U);
    EXPECT_GT(succeeded.load(), 0U);
    EXPECT_GT(succeeded.load() + rejected.load(), 0U);
    // Reopening proves the close handshake actually released RocksDB.
    auto reopened = Database::Open(DatabaseOptions{.path = path_});
    ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
    const auto value =
        reopened.ValueOrDie()->GetPointAt(property.ref(), ValidTime{15});
    ASSERT_TRUE(value.ok()) << value.status().ToString();
    EXPECT_EQ(value.ValueOrDie(), std::optional<Value>{Value::Int64(10)});
    ASSERT_TRUE(reopened.ValueOrDie()->Close().ok());
  }
}

// C1 regression: a writer commits continuously while reader threads call
// GetPointAtAt(old_seq). Every as_of read must equal the vintage of the
// requested sequence (Snapshot::Get at that sequence) and can never observe a
// post-old_seq value: a publish that races the pin must not move the read to a
// newer epoch.
TEST_F(KernelSnapshotTest,
       GetPointAtAtNeverObservesNewerVintageUnderContinuousCommits) {
  {
    // Register the schema and seed the first revision in one store lifetime so
    // the epochs created by the Database below see the property catalog.
    FactStore store(FactStoreOptions{path_});
    ASSERT_TRUE(store.Open().ok());
    ASSERT_TRUE(store.RegisterProperty(PropertyDefinition{
                                        PropertyId{7}, 0, "value",
                                        PropertyEntityKind::kVertex,
                                        PhysicalType::kInt64, 4096})
                    .ok());
    ASSERT_TRUE(store.Commit(Batch(TxnId{1}, {VertexMutation(1, 10, FactOperation::kPut),
                                             PropertyMutation(1, 7, 10, FactOperation::kPut,
                                                              Value::Int64(0))}))
                    .ok());
    ASSERT_TRUE(store.Close().ok());
  }
  auto opened = Database::Open(DatabaseOptions{.path = path_});
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
  const PropertyFact property =
      PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{7});
  constexpr int kReaders = 4;
  constexpr int kIterations = 150;

  std::atomic<bool> done{false};
  std::atomic<uint64_t> committed{0};
  std::atomic<uint64_t> as_of_reads{0};
  std::atomic<bool> failed{false};
  std::mutex failure_mutex;
  std::string failure;
  const auto fail = [&](const std::string& message) {
    std::lock_guard<std::mutex> lock(failure_mutex);
    if (!failed.exchange(true)) failure = message;
  };

  std::thread writer([&] {
    int64_t next = 0;
    while (!done.load(std::memory_order_relaxed)) {
      const auto transaction = database->BeginTransaction();
      if (!transaction.ok()) {
        fail("begin: " + transaction.status().ToString());
        return;
      }
      const Status asserted = transaction.ValueOrDie()->Assert(
          EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10});
      if (!asserted.ok()) {
        fail("assert: " + asserted.ToString());
        return;
      }
      const Status set = transaction.ValueOrDie()->Set(
          property, ValidTime{10}, Value::Int64(++next));
      if (!set.ok()) {
        fail("set: " + set.ToString());
        return;
      }
      const auto result = transaction.ValueOrDie()->Commit();
      if (!result.ok()) {
        fail("commit: " + result.status().ToString());
        return;
      }
      committed.store(static_cast<uint64_t>(next),
                      std::memory_order_release);
    }
  });

  std::vector<std::thread> readers;
  readers.reserve(kReaders);
  for (int reader = 0; reader < kReaders; ++reader) {
    readers.emplace_back([&] {
      for (int iteration = 0; iteration < kIterations; ++iteration) {
        // Capture an as_of vintage and its expected value through the snapshot
        // API, then re-read the same vintage through the point API while the
        // writer keeps publishing newer revisions.
        const auto snapshot = database->BeginSnapshot();
        if (!snapshot.ok()) {
          fail("snapshot: " + snapshot.status().ToString());
          return;
        }
        const CommitSeq vintage = snapshot.ValueOrDie().commit_seq();
        const auto expected =
            snapshot.ValueOrDie().Get(property, ValidTime{10});
        if (!expected.ok() || !expected.ValueOrDie().has_value()) {
          fail("expected: " + expected.status().ToString());
          return;
        }
        const int64_t expected_value =
            std::get<int64_t>(expected.ValueOrDie()->data());
        const auto actual =
            database->GetPointAtAt(vintage, property.ref(), ValidTime{10});
        if (!actual.ok()) {
          fail("as_of read: " + actual.status().ToString());
          return;
        }
        if (!actual.ValueOrDie().has_value()) {
          fail("as_of read at " + std::to_string(vintage.value) +
               " returned no value");
          return;
        }
        const int64_t actual_value =
            std::get<int64_t>(actual.ValueOrDie()->data());
        // Values increase with every commit, so inequality means the as_of read
        // observed a vintage other than the requested one (a newer commit
        // leaking into the read).
        if (actual_value != expected_value) {
          fail("as_of read at " + std::to_string(vintage.value) +
               " returned value " + std::to_string(actual_value) +
               " instead of " + std::to_string(expected_value));
          return;
        }
        as_of_reads.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  for (std::thread& reader : readers) reader.join();
  done.store(true, std::memory_order_relaxed);
  writer.join();

  EXPECT_FALSE(failed.load()) << failure;
  EXPECT_EQ(as_of_reads.load(), kReaders * kIterations);
  EXPECT_GT(committed.load(), 0U);
}

TEST_F(KernelSnapshotTest, PreservesBitemporalReadsAfterFlushCompactionAndReopen) {
  Seed({Batch(TxnId{1}, {VertexMutation(1, 10, FactOperation::kPut),
                         PropertyMutation(1, 7, 10, FactOperation::kPut,
                                          Value::Int64(10))}),
        Batch(TxnId{2}, {PropertyMutation(1, 7, 20, FactOperation::kPut,
                                          Value::Int64(20))}),
        Batch(TxnId{3}, {PropertyMutation(1, 7, 20, FactOperation::kDelete)})});

  FactStoreOptions store_options;
  store_options.path = path_;
  const rocksdb::Options options = internal::MakeRocksDbOptions(
      store_options, false);
  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors =
      internal::MakeRocksDbColumnFamilyDescriptors(store_options, options);
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
      PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{7}), ValidTime{25});
  ASSERT_TRUE(before_value.ok()) << before_value.status().ToString();
  EXPECT_EQ(before_value.ValueOrDie(), std::optional<Value>{Value::Int64(20)});

  const auto latest = database->BeginSnapshot();
  ASSERT_TRUE(latest.ok()) << latest.status().ToString();
  const auto deleted_value = latest.ValueOrDie().Get(
      PropertyFact::Vertex(VertexRef{PartId{0}, VertexId{1}}, PropertyId{7}), ValidTime{25});
  ASSERT_TRUE(deleted_value.ok()) << deleted_value.status().ToString();
  EXPECT_FALSE(deleted_value.ValueOrDie().has_value());
}

}  // namespace
}  // namespace cedar
