#include <gtest/gtest.h>

#include <array>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "cedar/database.h"
#include "cedar/fact/fact_codec.h"
#include "db/cedar_columnar_scan.h"
#include "storage/rocks/rocksdb_config.h"

#include <rocksdb/db.h>
#include <rocksdb/options.h>

namespace cedar {
namespace {

class ColumnarFactScanTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_columnar_fact_scan_XXXXXX";
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

  void Commit(FactOperation operation, ValidTime valid_time) {
    auto transaction = database_->BeginTransaction();
    ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
    const EntityFact vertex = EntityFact::Vertex(VertexRef{PartId{7}, VertexId{9}});
    ASSERT_TRUE((operation == FactOperation::kPut
                     ? transaction.ValueOrDie()->Assert(vertex, valid_time)
                     : transaction.ValueOrDie()->Retract(vertex, valid_time))
                    .ok());
    const auto committed = transaction.ValueOrDie()->Commit();
    ASSERT_TRUE(committed.ok()) << committed.status().ToString();
    ASSERT_EQ(committed.ValueOrDie().outcome, CommitOutcome::kCommitted)
        << committed.ValueOrDie().status.ToString();
  }

  void CommitVertex(VertexId vertex_id, ValidTime valid_time) {
    auto transaction = database_->BeginTransaction();
    ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
    const EntityFact vertex = EntityFact::Vertex(VertexRef{PartId{7}, vertex_id});
    ASSERT_TRUE(transaction.ValueOrDie()->Assert(vertex, valid_time).ok());
    const auto committed = transaction.ValueOrDie()->Commit();
    ASSERT_TRUE(committed.ok()) << committed.status().ToString();
    ASSERT_EQ(committed.ValueOrDie().outcome, CommitOutcome::kCommitted)
        << committed.ValueOrDie().status.ToString();
  }

  void FlushFactsAndReopen() {
    ASSERT_TRUE(database_->Close().ok());
    database_.reset();
    FactStoreOptions store_options;
    store_options.path = path_;
    const rocksdb::Options options =
        internal::MakeRocksDbOptions(store_options, false);
    std::unique_ptr<rocksdb::DB> raw_database;
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    ASSERT_TRUE(rocksdb::DB::Open(
                    options, path_,
                    internal::MakeRocksDbColumnFamilyDescriptors(store_options, options),
                    &handles, &raw_database)
                    .ok());
    ASSERT_EQ(handles.size(), 3U);
    rocksdb::FlushOptions flush_options;
    flush_options.wait = true;
    ASSERT_TRUE(raw_database->Flush(flush_options, handles[1]).ok());
    for (rocksdb::ColumnFamilyHandle* handle : handles) {
      ASSERT_TRUE(raw_database->DestroyColumnFamilyHandle(handle).ok());
    }
    raw_database.reset();
    auto reopened = Database::Open(DatabaseOptions{.path = path_});
    ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
    database_ = std::move(reopened).ConsumeValueOrDie();
  }

  std::string path_;
  std::unique_ptr<Database> database_;
};

TEST_F(ColumnarFactScanTest, EventAndStateScansUseThePinnedSnapshot) {
  Commit(FactOperation::kPut, ValidTime{10});
  Commit(FactOperation::kDelete, ValidTime{20});
  Commit(FactOperation::kPut, ValidTime{30});

  auto snapshot = database_->BeginSnapshot(SnapshotOptions{CommitSeq{2}});
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  const FactScanSpec spec{PartId{7}, FactFamily::kVertexState, PropertyId{},
                          ValidTime{35}, 1};

  std::vector<FactEvent> events;
  ASSERT_TRUE(snapshot.ValueOrDie()
                  .EventScan(spec, [&](const FactEventBatch& batch) {
                    events.insert(events.end(), batch.events.begin(), batch.events.end());
                    return Status::OK();
                  })
                  .ok());
  ASSERT_EQ(events.size(), 2U);
  EXPECT_EQ(events[0].commit_seq, CommitSeq{2});
  EXPECT_EQ(events[1].commit_seq, CommitSeq{1});

  std::vector<FactEvent> state;
  ASSERT_TRUE(snapshot.ValueOrDie()
                  .StateScan(spec, [&](const FactEventBatch& batch) {
                    state.insert(state.end(), batch.events.begin(), batch.events.end());
                    return Status::OK();
                  })
                  .ok());
  EXPECT_TRUE(state.empty());
}

TEST_F(ColumnarFactScanTest,
       PersistedPropertyColumnarScansPreserveEveryPhysicalValueLane) {
  struct PropertyCase {
    PropertyId property_id;
    PhysicalType physical_type;
    Value value;
  };
  const std::vector<PropertyCase> properties = {
      {PropertyId{1}, PhysicalType::kBool, Value::Bool(true)},
      {PropertyId{2}, PhysicalType::kInt32, Value::Int32(-7)},
      {PropertyId{3}, PhysicalType::kInt64, Value::Int64(-9)},
      {PropertyId{4}, PhysicalType::kFloat32, Value::Float32(1.5F)},
      {PropertyId{5}, PhysicalType::kFloat64, Value::Float64(-2.25)},
      {PropertyId{6}, PhysicalType::kTimestamp64, Value::Timestamp(42)},
      {PropertyId{7}, PhysicalType::kString, Value::String("cedar")},
      {PropertyId{8}, PhysicalType::kBinary, Value::Binary(std::string("a\0b", 3))},
  };
  for (const PropertyCase& property : properties) {
    ASSERT_TRUE(database_->RegisterProperty(PropertyDefinition{
                                            property.property_id, 0,
                                            "property" +
                                                std::to_string(property.property_id.value),
                                            PropertyEntityKind::kVertex,
                                            property.physical_type, 4096})
                    .ok());
  }

  auto transaction = database_->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  for (const PropertyCase& property : properties) {
    ASSERT_TRUE(transaction.ValueOrDie()
                    ->Set(PropertyFact::Vertex(VertexRef{PartId{7}, VertexId{9}},
                                                property.property_id),
                          ValidTime{10}, property.value)
                    .ok());
  }
  const auto committed = transaction.ValueOrDie()->Commit();
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  ASSERT_EQ(committed.ValueOrDie().outcome, CommitOutcome::kCommitted)
      << committed.ValueOrDie().status.ToString();

  FlushFactsAndReopen();
  const std::vector<FactColumnId> projection = {
      FactColumnId::kPhysicalType, FactColumnId::kBoolValue,
      FactColumnId::kInt32Value, FactColumnId::kInt64Value,
      FactColumnId::kFloat32Value, FactColumnId::kFloat64Value,
      FactColumnId::kTimestamp64Value, FactColumnId::kBytesValue};
  const auto assert_projected_lanes =
      [&projection](const FactColumnarBatch& batch, const PropertyCase& property) {
        ASSERT_EQ(batch.row_count(), 1U);
        ASSERT_EQ(batch.columns.size(), projection.size());
        ASSERT_EQ(batch.columns[0].id, FactColumnId::kPhysicalType);
        EXPECT_EQ(batch.columns[0].present, std::vector<uint8_t>({1}));
        EXPECT_EQ(std::get<std::vector<uint32_t>>(batch.columns[0].values),
                  std::vector<uint32_t>(
                      {static_cast<uint32_t>(property.physical_type)}));

        const size_t matching_lane =
            property.physical_type == PhysicalType::kBinary
                ? 7
                : static_cast<size_t>(property.physical_type);
        for (size_t lane = 1; lane < batch.columns.size(); ++lane) {
          EXPECT_EQ(batch.columns[lane].id, projection[lane]);
          EXPECT_EQ(batch.columns[lane].present,
                    std::vector<uint8_t>(
                        {static_cast<uint8_t>(lane == matching_lane ? 1 : 0)}));
        }
        switch (property.physical_type) {
          case PhysicalType::kBool:
            EXPECT_EQ(std::get<std::vector<uint8_t>>(batch.columns[1].values),
                      std::vector<uint8_t>({1}));
            break;
          case PhysicalType::kInt32:
            EXPECT_EQ(std::get<std::vector<int32_t>>(batch.columns[2].values),
                      std::vector<int32_t>({-7}));
            break;
          case PhysicalType::kInt64:
            EXPECT_EQ(std::get<std::vector<int64_t>>(batch.columns[3].values),
                      std::vector<int64_t>({-9}));
            break;
          case PhysicalType::kFloat32:
            EXPECT_EQ(std::get<std::vector<float>>(batch.columns[4].values),
                      std::vector<float>({1.5F}));
            break;
          case PhysicalType::kFloat64:
            EXPECT_EQ(std::get<std::vector<double>>(batch.columns[5].values),
                      std::vector<double>({-2.25}));
            break;
          case PhysicalType::kTimestamp64:
            EXPECT_EQ(std::get<std::vector<uint64_t>>(batch.columns[6].values),
                      std::vector<uint64_t>({42}));
            break;
          case PhysicalType::kString:
            EXPECT_EQ(std::get<std::vector<std::string>>(batch.columns[7].values),
                      std::vector<std::string>({"cedar"}));
            break;
          case PhysicalType::kBinary:
            EXPECT_EQ(std::get<std::vector<std::string>>(batch.columns[7].values),
                      std::vector<std::string>({std::string("a\0b", 3)}));
            break;
        }
      };

  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  for (const PropertyCase& property : properties) {
    const FactScanSpec spec{PartId{7}, FactFamily::kVertexProperty,
                            property.property_id, ValidTime{10}, 1};
    uint32_t event_batches = 0;
    ASSERT_TRUE(snapshot.ValueOrDie()
                    .EventColumnarScan(
                        spec, projection,
                        [&](const FactColumnarBatch& batch) {
                          ++event_batches;
                          assert_projected_lanes(batch, property);
                          return Status::OK();
                        })
                    .ok());
    EXPECT_EQ(event_batches, 1U);

    uint32_t state_batches = 0;
    ASSERT_TRUE(snapshot.ValueOrDie()
                    .StateColumnarScan(
                        spec, projection,
                        [&](const FactColumnarBatch& batch) {
                          ++state_batches;
                          assert_projected_lanes(batch, property);
                          return Status::OK();
                        })
                    .ok());
    EXPECT_EQ(state_batches, 1U);
  }
}

TEST_F(ColumnarFactScanTest, EventScanBoundsBatchesAndPropagatesVisitorFailure) {
  Commit(FactOperation::kPut, ValidTime{10});
  Commit(FactOperation::kPut, ValidTime{20});

  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  const FactScanSpec spec{PartId{7}, FactFamily::kVertexState, PropertyId{},
                          ValidTime{20}, 1};

  uint32_t batches = 0;
  const Status scan = snapshot.ValueOrDie().EventScan(
      spec, [&batches](const FactEventBatch& batch) {
        ++batches;
        EXPECT_EQ(batch.events.size(), 1U);
        return Status::InvalidArgument("test visitor", "stop");
      });
  EXPECT_TRUE(scan.IsInvalidArgument()) << scan.ToString();
  EXPECT_EQ(batches, 1U);
}

TEST_F(ColumnarFactScanTest, EventScanAppliesEntityTimeAndCommitRanges) {
  CommitVertex(VertexId{8}, ValidTime{10});
  CommitVertex(VertexId{9}, ValidTime{20});
  CommitVertex(VertexId{10}, ValidTime{30});

  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  FactScanSpec spec{PartId{7}, FactFamily::kVertexState, PropertyId{},
                    ValidTime{}, 8};
  spec.entity_id_min = 9;
  spec.entity_id_max = 10;
  spec.event_valid_from_min = ValidTime{20};
  spec.event_valid_from_max = ValidTime{30};
  spec.event_commit_seq_min = CommitSeq{2};
  spec.event_commit_seq_max = CommitSeq{2};

  std::vector<FactEvent> events;
  ASSERT_TRUE(snapshot.ValueOrDie()
                  .EventScan(spec, [&](const FactEventBatch& batch) {
                    events.insert(events.end(), batch.events.begin(), batch.events.end());
                    return Status::OK();
                  })
                  .ok());
  ASSERT_EQ(events.size(), 1U);
  EXPECT_EQ(events.front().ref.entity_id(), 9U);
  EXPECT_EQ(events.front().valid_from, ValidTime{20});
  EXPECT_EQ(events.front().commit_seq, CommitSeq{2});
}

TEST_F(ColumnarFactScanTest, EventScanSeeksToEntityLowerBoundAndStopsAtUpperBound) {
  CommitVertex(VertexId{8}, ValidTime{10});
  CommitVertex(VertexId{9}, ValidTime{20});
  CommitVertex(VertexId{10}, ValidTime{30});
  CommitVertex(VertexId{11}, ValidTime{40});

  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  FactScanSpec spec{PartId{7}, FactFamily::kVertexState, PropertyId{},
                    ValidTime{100}, 16};
  spec.entity_id_min = 9;
  spec.entity_id_max = 10;

  std::vector<FactEvent> events;
  ASSERT_TRUE(snapshot.ValueOrDie()
                  .EventScan(spec, [&](const FactEventBatch& batch) {
                    events.insert(events.end(), batch.events.begin(), batch.events.end());
                    return Status::OK();
                  })
                  .ok());
  ASSERT_EQ(events.size(), 2U);
  EXPECT_EQ(events[0].ref.entity_id(), 9U);
  EXPECT_EQ(events[1].ref.entity_id(), 10U);
}

TEST_F(ColumnarFactScanTest, EventScanUsesCedarParquetFilesAfterFlushAndReopen) {
  CommitVertex(VertexId{8}, ValidTime{10});
  CommitVertex(VertexId{9}, ValidTime{20});
  CommitVertex(VertexId{10}, ValidTime{30});
  FlushFactsAndReopen();

  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  FactScanSpec spec{PartId{7}, FactFamily::kVertexState, PropertyId{},
                    ValidTime{100}, 16};
  spec.entity_id_min = 9;
  spec.entity_id_max = 10;
  std::vector<FactEvent> events;
  ASSERT_TRUE(snapshot.ValueOrDie()
                  .EventScan(spec, [&](const FactEventBatch& batch) {
                    events.insert(events.end(), batch.events.begin(), batch.events.end());
                    return Status::OK();
                  })
                  .ok());
  ASSERT_EQ(events.size(), 2U);
  EXPECT_EQ(events[0].ref.entity_id(), 9U);
  EXPECT_EQ(events[1].ref.entity_id(), 10U);
}

TEST_F(ColumnarFactScanTest,
       EventColumnarScanExposesCedarOwnedProjectedVectorsFromPinnedSnapshot) {
  CommitVertex(VertexId{8}, ValidTime{10});
  CommitVertex(VertexId{9}, ValidTime{20});
  FlushFactsAndReopen();

  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  FactScanSpec spec{PartId{7}, FactFamily::kVertexState, PropertyId{},
                    ValidTime{100}, 1};

  std::vector<uint64_t> entities;
  const Status scan = snapshot.ValueOrDie()
                          .EventColumnarScan(
                              spec,
                              {FactColumnId::kPartId, FactColumnId::kEntityId,
                               FactColumnId::kValidFrom},
                              [&entities](const FactColumnarBatch& batch) -> Status {
                        EXPECT_EQ(batch.row_count(), 1U);
                        EXPECT_EQ(batch.columns.size(), 3U);
                        const auto& part_ids =
                            std::get<std::vector<uint32_t>>(batch.columns[0].values);
                        const auto& entity_ids =
                            std::get<std::vector<uint64_t>>(batch.columns[1].values);
                        const auto& valid_from =
                            std::get<std::vector<uint64_t>>(batch.columns[2].values);
                        EXPECT_EQ(part_ids, std::vector<uint32_t>({7}));
                        EXPECT_EQ(valid_from.size(), entity_ids.size());
                        entities.insert(entities.end(), entity_ids.begin(), entity_ids.end());
                                return Status::OK();
                              });
  ASSERT_TRUE(scan.ok()) << scan.ToString();
  EXPECT_EQ(entities, std::vector<uint64_t>({8, 9}));
}

TEST_F(ColumnarFactScanTest,
       EventColumnarScanAppliesPersistedEntityTimeAndCommitRanges) {
  CommitVertex(VertexId{8}, ValidTime{10});
  CommitVertex(VertexId{9}, ValidTime{20});
  CommitVertex(VertexId{10}, ValidTime{30});
  FlushFactsAndReopen();
  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  FactScanSpec spec{PartId{7}, FactFamily::kVertexState, PropertyId{}, ValidTime{}, 8};
  spec.entity_id_min = 9;
  spec.entity_id_max = 10;
  spec.event_valid_from_min = ValidTime{20};
  spec.event_valid_from_max = ValidTime{30};
  spec.event_commit_seq_min = CommitSeq{2};
  spec.event_commit_seq_max = CommitSeq{2};
  std::vector<uint64_t> entities;
  ASSERT_TRUE(snapshot.ValueOrDie().EventColumnarScan(
      spec, {FactColumnId::kEntityId}, [&entities](const FactColumnarBatch& batch) {
        const auto& values = std::get<std::vector<uint64_t>>(batch.columns[0].values);
        entities.insert(entities.end(), values.begin(), values.end());
        return Status::OK();
      }).ok());
  EXPECT_EQ(entities, std::vector<uint64_t>({9}));
}

TEST_F(ColumnarFactScanTest,
       PersistedEventAndStateProjectionSubsetsMatchCanonicalRangeOracles) {
  CommitVertex(VertexId{8}, ValidTime{10});
  CommitVertex(VertexId{9}, ValidTime{20});
  CommitVertex(VertexId{10}, ValidTime{30});
  FlushFactsAndReopen();

  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  FactScanSpec spec{PartId{7}, FactFamily::kVertexState, PropertyId{},
                    ValidTime{30}, 1};
  spec.entity_id_min = 8;
  spec.entity_id_max = 9;
  spec.event_valid_from_min = ValidTime{10};
  spec.event_valid_from_max = ValidTime{20};
  spec.event_commit_seq_min = CommitSeq{1};
  spec.event_commit_seq_max = CommitSeq{2};

  const std::vector<FactColumnId> available_columns = {
      FactColumnId::kPartId, FactColumnId::kFactFamily,
      FactColumnId::kEntityId, FactColumnId::kValidFrom,
      FactColumnId::kCedarCommitSeq, FactColumnId::kOperation};
  const auto expected_rows = [](const std::vector<FactEvent>& events,
                                const std::vector<FactColumnId>& projection) {
    std::vector<std::vector<uint64_t>> rows;
    for (const FactEvent& event : events) {
      std::vector<uint64_t> row;
      row.reserve(projection.size());
      for (FactColumnId column : projection) {
        switch (column) {
          case FactColumnId::kPartId:
            row.push_back(event.ref.part_id().value);
            break;
          case FactColumnId::kFactFamily:
            row.push_back(static_cast<uint64_t>(event.ref.family()));
            break;
          case FactColumnId::kEntityId:
            row.push_back(event.ref.entity_id());
            break;
          case FactColumnId::kValidFrom:
            row.push_back(event.valid_from.value);
            break;
          case FactColumnId::kCedarCommitSeq:
            row.push_back(event.commit_seq.value);
            break;
          case FactColumnId::kOperation:
            row.push_back(static_cast<uint64_t>(event.operation));
            break;
          default:
            ADD_FAILURE() << "unexpected projection column";
            return std::vector<std::vector<uint64_t>>{};
        }
      }
      rows.push_back(std::move(row));
    }
    return rows;
  };
  const auto collect_rows = [](const FactColumnarBatch& batch,
                               std::vector<std::vector<uint64_t>>* rows) {
    for (size_t row = 0; row < batch.row_count(); ++row) {
      std::vector<uint64_t> output;
      output.reserve(batch.columns.size());
      for (const FactColumn& column : batch.columns) {
        EXPECT_EQ(column.present[row], 1);
        if (const auto* values = std::get_if<std::vector<uint32_t>>(&column.values)) {
          output.push_back((*values)[row]);
        } else if (const auto* values =
                       std::get_if<std::vector<uint64_t>>(&column.values)) {
          output.push_back((*values)[row]);
        } else {
          ADD_FAILURE() << "unexpected vector type";
          return Status::Corruption("test", "unexpected projection vector");
        }
      }
      rows->push_back(std::move(output));
    }
    return Status::OK();
  };

  std::vector<FactEvent> canonical_events;
  ASSERT_TRUE(snapshot.ValueOrDie().EventScan(
      spec, [&canonical_events](const FactEventBatch& batch) {
        canonical_events.insert(canonical_events.end(), batch.events.begin(),
                                batch.events.end());
        return Status::OK();
      }).ok());
  std::vector<FactEvent> canonical_state;
  ASSERT_TRUE(snapshot.ValueOrDie().StateScan(
      spec, [&canonical_state](const FactEventBatch& batch) {
        canonical_state.insert(canonical_state.end(), batch.events.begin(),
                               batch.events.end());
        return Status::OK();
      }).ok());

  struct ScanMode {
    const std::vector<FactEvent>* oracle;
    bool state;
  };
  const std::array<ScanMode, 2> modes = {{{&canonical_events, false},
                                           {&canonical_state, true}}};
  for (uint32_t subset = 1; subset < (1U << available_columns.size()); ++subset) {
    std::vector<FactColumnId> projection;
    for (size_t column = 0; column < available_columns.size(); ++column) {
      if ((subset & (1U << column)) != 0) {
        projection.push_back(available_columns[column]);
      }
    }
    std::vector<std::vector<uint64_t>> columnar_events;
    ASSERT_TRUE(snapshot.ValueOrDie().EventColumnarScan(
        spec, projection, [&collect_rows, &columnar_events](const FactColumnarBatch& batch) {
          return collect_rows(batch, &columnar_events);
        }).ok());
    EXPECT_EQ(columnar_events, expected_rows(canonical_events, projection));

    std::vector<std::vector<uint64_t>> columnar_state;
    ASSERT_TRUE(snapshot.ValueOrDie().StateColumnarScan(
        spec, projection, [&collect_rows, &columnar_state](const FactColumnarBatch& batch) {
          return collect_rows(batch, &columnar_state);
        }).ok());
    EXPECT_EQ(columnar_state, expected_rows(canonical_state, projection));
  }
}

TEST_F(ColumnarFactScanTest,
       PersistedPropertyProjectionSubsetsMatchCanonicalEventAndStateOracles) {
  struct PropertyCase {
    PropertyId property_id;
    PhysicalType physical_type;
    Value initial_value;
    Value replacement_value;
  };
  const std::vector<PropertyCase> properties = {
      {PropertyId{1}, PhysicalType::kBool, Value::Bool(true), Value::Bool(false)},
      {PropertyId{2}, PhysicalType::kInt32, Value::Int32(-7), Value::Int32(8)},
      {PropertyId{3}, PhysicalType::kInt64, Value::Int64(-9), Value::Int64(10)},
      {PropertyId{4}, PhysicalType::kFloat32, Value::Float32(1.5F),
       Value::Float32(-2.5F)},
      {PropertyId{5}, PhysicalType::kFloat64, Value::Float64(-2.25),
       Value::Float64(3.75)},
      {PropertyId{6}, PhysicalType::kTimestamp64, Value::Timestamp(42),
       Value::Timestamp(84)},
      {PropertyId{7}, PhysicalType::kString, Value::String("before"),
       Value::String("after")},
      {PropertyId{8}, PhysicalType::kBinary, Value::Binary(std::string("a\0b", 3)),
       Value::Binary(std::string("c\0d", 3))},
  };
  for (const PropertyCase& property : properties) {
    ASSERT_TRUE(database_->RegisterProperty(PropertyDefinition{
                                            property.property_id, 0,
                                            "subset" +
                                                std::to_string(property.property_id.value),
                                            PropertyEntityKind::kVertex,
                                            property.physical_type, 4096})
                    .ok());
  }

  const VertexRef vertex{PartId{7}, VertexId{9}};
  auto transaction = database_->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  for (const PropertyCase& property : properties) {
    ASSERT_TRUE(transaction.ValueOrDie()
                    ->Set(PropertyFact::Vertex(vertex, property.property_id), ValidTime{10},
                          property.initial_value)
                    .ok());
  }
  auto committed = transaction.ValueOrDie()->Commit();
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  ASSERT_EQ(committed.ValueOrDie().outcome, CommitOutcome::kCommitted)
      << committed.ValueOrDie().status.ToString();

  transaction = database_->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  for (const PropertyCase& property : properties) {
    if (property.property_id == PropertyId{8}) {
      ASSERT_TRUE(transaction.ValueOrDie()
                      ->Unset(PropertyFact::Vertex(vertex, property.property_id),
                              ValidTime{20})
                      .ok());
    } else {
      ASSERT_TRUE(transaction.ValueOrDie()
                      ->Set(PropertyFact::Vertex(vertex, property.property_id), ValidTime{20},
                            property.replacement_value)
                      .ok());
    }
  }
  committed = transaction.ValueOrDie()->Commit();
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  ASSERT_EQ(committed.ValueOrDie().outcome, CommitOutcome::kCommitted)
      << committed.ValueOrDie().status.ToString();

  FlushFactsAndReopen();
  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();

  const std::vector<FactColumnId> available_columns = {
      FactColumnId::kPhysicalType, FactColumnId::kBoolValue,
      FactColumnId::kInt32Value, FactColumnId::kInt64Value,
      FactColumnId::kFloat32Value, FactColumnId::kFloat64Value,
      FactColumnId::kTimestamp64Value, FactColumnId::kBytesValue,
  };
  const auto expected_presence = [](const std::vector<FactEvent>& events,
                                    FactColumnId column) {
    std::vector<uint8_t> present;
    present.reserve(events.size());
    for (const FactEvent& event : events) {
      const std::optional<Value>& value = event.value;
      bool has_value = column == FactColumnId::kPhysicalType;
      if (value.has_value()) {
        switch (column) {
          case FactColumnId::kBoolValue:
            has_value = value->type() == PhysicalType::kBool;
            break;
          case FactColumnId::kInt32Value:
            has_value = value->type() == PhysicalType::kInt32;
            break;
          case FactColumnId::kInt64Value:
            has_value = value->type() == PhysicalType::kInt64;
            break;
          case FactColumnId::kFloat32Value:
            has_value = value->type() == PhysicalType::kFloat32;
            break;
          case FactColumnId::kFloat64Value:
            has_value = value->type() == PhysicalType::kFloat64;
            break;
          case FactColumnId::kTimestamp64Value:
            has_value = value->type() == PhysicalType::kTimestamp64;
            break;
          case FactColumnId::kBytesValue:
            has_value = value->type() == PhysicalType::kString ||
                        value->type() == PhysicalType::kBinary;
            break;
          case FactColumnId::kPhysicalType:
            break;
          default:
            ADD_FAILURE() << "unexpected physical projection column";
            break;
        }
      }
      present.push_back(has_value ? 1 : 0);
    }
    return present;
  };
  const auto scan_all_properties = [&](auto scan, std::vector<FactEvent>* events) {
    for (const PropertyCase& property : properties) {
      const FactScanSpec spec{PartId{7}, FactFamily::kVertexProperty,
                              property.property_id, ValidTime{20}, 1};
      const Status status = scan(spec, [&events](const FactEventBatch& batch) {
        events->insert(events->end(), batch.events.begin(), batch.events.end());
        return Status::OK();
      });
      EXPECT_TRUE(status.ok()) << status.ToString();
    }
  };

  std::vector<FactEvent> canonical_events;
  scan_all_properties(
      [&snapshot](const FactScanSpec& spec, const FactEventBatchVisitor& visitor) {
        return snapshot.ValueOrDie().EventScan(spec, visitor);
      },
      &canonical_events);
  std::vector<FactEvent> canonical_state;
  scan_all_properties(
      [&snapshot](const FactScanSpec& spec, const FactEventBatchVisitor& visitor) {
        return snapshot.ValueOrDie().StateScan(spec, visitor);
      },
      &canonical_state);

  struct ScanMode {
    const std::vector<FactEvent>* oracle;
    bool state;
  };
  const std::array<ScanMode, 2> modes = {{{&canonical_events, false},
                                           {&canonical_state, true}}};
  for (uint32_t subset = 1; subset < (1U << available_columns.size()); ++subset) {
    std::vector<FactColumnId> projection;
    for (size_t column = 0; column < available_columns.size(); ++column) {
      if ((subset & (1U << column)) != 0) projection.push_back(available_columns[column]);
    }
    for (const ScanMode& mode : modes) {
      std::vector<std::vector<uint8_t>> observed_presence(projection.size());
      for (const PropertyCase& property : properties) {
        const FactScanSpec spec{PartId{7}, FactFamily::kVertexProperty,
                                property.property_id, ValidTime{20}, 1};
        const auto collect = [&](const FactColumnarBatch& batch) -> Status {
          if (batch.columns.size() != projection.size()) {
            return Status::Corruption("test", "unexpected physical projection width");
          }
          for (size_t column = 0; column < batch.columns.size(); ++column) {
            EXPECT_EQ(batch.columns[column].id, projection[column]);
            observed_presence[column].insert(observed_presence[column].end(),
                                             batch.columns[column].present.begin(),
                                             batch.columns[column].present.end());
          }
          return Status::OK();
        };
        const Status status = mode.state
                                  ? snapshot.ValueOrDie().StateColumnarScan(spec, projection,
                                                                             collect)
                                  : snapshot.ValueOrDie().EventColumnarScan(spec, projection,
                                                                             collect);
        ASSERT_TRUE(status.ok()) << status.ToString();
      }
      for (size_t column = 0; column < projection.size(); ++column) {
        EXPECT_EQ(observed_presence[column],
                  expected_presence(*mode.oracle, projection[column]));
      }
    }
  }
}

TEST_F(ColumnarFactScanTest,
       EventColumnarScanProjectsCrossPartAuthoritativeEdgeTopologyAfterFlush) {
  const EdgeIdentity identity{
      EdgeRef{PartId{8}, EdgeId{17}}, VertexRef{PartId{8}, VertexId{101}},
      VertexRef{PartId{9}, VertexId{202}}, 42};
  auto transaction = database_->BeginTransaction();
  ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
  ASSERT_TRUE(transaction.ValueOrDie()->Assert(identity, ValidTime{}).ok());
  const auto committed = transaction.ValueOrDie()->Commit();
  ASSERT_TRUE(committed.ok()) << committed.status().ToString();
  ASSERT_EQ(committed.ValueOrDie().outcome, CommitOutcome::kCommitted);
  FlushFactsAndReopen();

  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  const FactScanSpec spec{PartId{8}, FactFamily::kEdgeIdentity, PropertyId{},
                          ValidTime{}, 16};
  uint32_t rows = 0;
  const Status scan = snapshot.ValueOrDie().EventColumnarScan(
      spec,
      {FactColumnId::kPartId, FactColumnId::kEntityId, FactColumnId::kSourcePartId,
       FactColumnId::kSourceVertexId, FactColumnId::kTargetPartId,
       FactColumnId::kTargetVertexId, FactColumnId::kEdgeType},
      [&rows](const FactColumnarBatch& batch) -> Status {
        if (batch.row_count() != 1U || batch.columns.size() != 7U) {
          return Status::Corruption("test", "unexpected edge topology batch shape");
        }
        const auto& home_parts = std::get<std::vector<uint32_t>>(batch.columns[0].values);
        const auto& edge_ids = std::get<std::vector<uint64_t>>(batch.columns[1].values);
        const auto& source_parts = std::get<std::vector<uint32_t>>(batch.columns[2].values);
        const auto& source_vertices = std::get<std::vector<uint64_t>>(batch.columns[3].values);
        const auto& target_parts = std::get<std::vector<uint32_t>>(batch.columns[4].values);
        const auto& target_vertices = std::get<std::vector<uint64_t>>(batch.columns[5].values);
        const auto& edge_types = std::get<std::vector<uint64_t>>(batch.columns[6].values);
        for (size_t row = 0; row < batch.row_count(); ++row) {
          EXPECT_EQ(home_parts[row], 8U);
          EXPECT_EQ(edge_ids[row], 17U);
          EXPECT_EQ(source_parts[row], 8U);
          EXPECT_EQ(source_vertices[row], 101U);
          EXPECT_EQ(target_parts[row], 9U);
          EXPECT_EQ(target_vertices[row], 202U);
          EXPECT_EQ(edge_types[row], 42U);
          for (const FactColumn& column : batch.columns) EXPECT_EQ(column.present[row], 1);
          ++rows;
        }
        return Status::OK();
      });
  ASSERT_TRUE(scan.ok()) << scan.ToString();
  EXPECT_EQ(rows, 1U);
}

TEST_F(ColumnarFactScanTest,
       StateColumnarScanResolvesTemporalDeletesBeforeReturningProjectedRows) {
  CommitVertex(VertexId{8}, ValidTime{10});
  Commit(FactOperation::kPut, ValidTime{10});
  Commit(FactOperation::kDelete, ValidTime{20});

  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  FactScanSpec spec{PartId{7}, FactFamily::kVertexState, PropertyId{},
                    ValidTime{20}, 16};

  std::vector<uint64_t> entities;
  const Status scan = snapshot.ValueOrDie().StateColumnarScan(
      spec, {FactColumnId::kEntityId, FactColumnId::kOperation},
      [&entities](const FactColumnarBatch& batch) -> Status {
        const auto& entity_ids =
            std::get<std::vector<uint64_t>>(batch.columns[0].values);
        const auto& operation =
            std::get<std::vector<uint32_t>>(batch.columns[1].values);
        EXPECT_EQ(entity_ids.size(), operation.size());
        entities.insert(entities.end(), entity_ids.begin(), entity_ids.end());
        return Status::OK();
      });
  ASSERT_TRUE(scan.ok()) << scan.ToString();
  EXPECT_EQ(entities, std::vector<uint64_t>({8}));
}

TEST_F(ColumnarFactScanTest,
       StateColumnarScanRejectsStorageSequenceProjectionForAnEmptyResult) {
  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  const FactScanSpec spec{PartId{7}, FactFamily::kVertexState, PropertyId{},
                          ValidTime{}, 1};
  const Status scan = snapshot.ValueOrDie().StateColumnarScan(
      spec, {FactColumnId::kStorageSequence},
      [](const FactColumnarBatch&) { return Status::OK(); });
  EXPECT_TRUE(scan.IsNotSupportedError()) << scan.ToString();
}

TEST_F(ColumnarFactScanTest,
       StateColumnarScanResolvesPersistedOverrideBeforeApplyingEventFilters) {
  CommitVertex(VertexId{8}, ValidTime{10});
  Commit(FactOperation::kPut, ValidTime{10});
  Commit(FactOperation::kDelete, ValidTime{20});
  Commit(FactOperation::kPut, ValidTime{30});
  FlushFactsAndReopen();

  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  FactScanSpec spec{PartId{7}, FactFamily::kVertexState, PropertyId{},
                    ValidTime{35}, 1};
  spec.event_valid_from_min = ValidTime{10};
  spec.event_valid_from_max = ValidTime{10};

  std::vector<uint64_t> entities;
  std::vector<uint64_t> valid_from;
  const Status scan = snapshot.ValueOrDie().StateColumnarScan(
      spec, {FactColumnId::kEntityId, FactColumnId::kValidFrom},
      [&entities, &valid_from](const FactColumnarBatch& batch) -> Status {
        EXPECT_EQ(batch.row_count(), 1U);
        const auto& entity_ids =
            std::get<std::vector<uint64_t>>(batch.columns[0].values);
        const auto& valid_times =
            std::get<std::vector<uint64_t>>(batch.columns[1].values);
        entities.insert(entities.end(), entity_ids.begin(), entity_ids.end());
        valid_from.insert(valid_from.end(), valid_times.begin(), valid_times.end());
        return Status::OK();
      });
  ASSERT_TRUE(scan.ok()) << scan.ToString();
  EXPECT_EQ(entities, std::vector<uint64_t>({8}));
  EXPECT_EQ(valid_from, std::vector<uint64_t>({10}));
}

TEST_F(ColumnarFactScanTest,
       ColumnarScansPreserveVacuumBoundaryAcrossFlushAndCompaction) {
  Commit(FactOperation::kPut, ValidTime{10});
  FlushFactsAndReopen();
  Commit(FactOperation::kDelete, ValidTime{10});
  FlushFactsAndReopen();

  {
    auto pinned = database_->BeginSnapshot(SnapshotOptions{CommitSeq{1}});
    ASSERT_TRUE(pinned.ok()) << pinned.status().ToString();
    EXPECT_TRUE(database_->Vacuum(CommitSeq{2}).IsSnapshotPinned());
  }
  ASSERT_TRUE(database_->Vacuum(CommitSeq{2}).ok());
  FlushFactsAndReopen();

  ASSERT_TRUE(database_->Close().ok());
  database_.reset();
  FactStoreOptions store_options;
  store_options.path = path_;
  rocksdb::Options options = internal::MakeRocksDbOptions(store_options, false);
  options.disable_auto_compactions = true;
  std::unique_ptr<rocksdb::DB> raw_database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_TRUE(rocksdb::DB::Open(
                  options, path_,
                  internal::MakeRocksDbColumnFamilyDescriptors(store_options, options),
                  &handles, &raw_database)
                  .ok());
  std::vector<rocksdb::LiveFileMetaData> live_files;
  raw_database->GetLiveFilesMetaData(&live_files);
  std::vector<std::string> level_zero_inputs;
  for (const auto& file : live_files) {
    if (file.column_family_name == "facts" && file.level == 0) {
      level_zero_inputs.push_back(
          std::filesystem::path(file.relative_filename).filename().string());
    }
  }
  ASSERT_GE(level_zero_inputs.size(), 3U);
  ASSERT_TRUE(raw_database
                  ->CompactFiles(rocksdb::CompactionOptions(), handles[1],
                                 level_zero_inputs, 1)
                  .ok());
  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    ASSERT_TRUE(raw_database->DestroyColumnFamilyHandle(handle).ok());
  }
  raw_database.reset();

  auto reopened = Database::Open(DatabaseOptions{.path = path_});
  ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
  database_ = std::move(reopened).ConsumeValueOrDie();
  EXPECT_TRUE(database_->BeginSnapshot(SnapshotOptions{CommitSeq{1}})
                  .status()
                  .IsSnapshotExpired());
  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  const FactScanSpec spec{PartId{7}, FactFamily::kVertexState, PropertyId{},
                          ValidTime{10}, 1};

  std::vector<uint32_t> operations;
  ASSERT_TRUE(snapshot.ValueOrDie().EventColumnarScan(
      spec, {FactColumnId::kOperation}, [&operations](const FactColumnarBatch& batch) {
        const auto& values = std::get<std::vector<uint32_t>>(batch.columns[0].values);
        operations.insert(operations.end(), values.begin(), values.end());
        return Status::OK();
      }).ok());
  EXPECT_EQ(operations,
            std::vector<uint32_t>({static_cast<uint32_t>(FactOperation::kDelete)}));

  uint32_t state_rows = 0;
  ASSERT_TRUE(snapshot.ValueOrDie().StateColumnarScan(
      spec, {FactColumnId::kOperation}, [&state_rows](const FactColumnarBatch& batch) {
        state_rows += batch.row_count();
        return Status::OK();
      }).ok());
  EXPECT_EQ(state_rows, 0U);
}

TEST_F(ColumnarFactScanTest, EventColumnarScanPropagatesVisitorCancellation) {
  CommitVertex(VertexId{8}, ValidTime{10});
  auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  FactScanSpec spec{PartId{7}, FactFamily::kVertexState, PropertyId{},
                    ValidTime{10}, 1};

  const Status scan = snapshot.ValueOrDie().EventColumnarScan(
      spec, {FactColumnId::kEntityId},
      [](const FactColumnarBatch&) {
        return Status::InvalidArgument("test visitor", "cancelled");
      });
  EXPECT_TRUE(scan.IsInvalidArgument()) << scan.ToString();
}

TEST_F(ColumnarFactScanTest,
       PinnedSuperVersionAdapterReturnsTypedProjectionFromFlushedFacts) {
  CommitVertex(VertexId{8}, ValidTime{10});
  CommitVertex(VertexId{9}, ValidTime{20});
  CommitVertex(VertexId{10}, ValidTime{30});

  ASSERT_TRUE(database_->Close().ok());
  database_.reset();
  FactStoreOptions store_options;
  store_options.path = path_;
  const rocksdb::Options options = internal::MakeRocksDbOptions(store_options, false);
  std::unique_ptr<rocksdb::DB> raw_database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_TRUE(rocksdb::DB::Open(
                  options, path_,
                  internal::MakeRocksDbColumnFamilyDescriptors(store_options, options),
                  &handles, &raw_database)
                  .ok());
  rocksdb::FlushOptions flush_options;
  flush_options.wait = true;
  ASSERT_TRUE(raw_database->Flush(flush_options, handles[1]).ok());

  rocksdb::cedar_parquet::CedarParquetScanSpec spec;
  spec.batch_row_limit = 2;
  spec.projection = {rocksdb::cedar_parquet::CedarParquetColumnId::kEntityId,
                     rocksdb::cedar_parquet::CedarParquetColumnId::kFactFamily};
  std::vector<uint64_t> entities;
  ASSERT_TRUE(rocksdb::ScanCedarParquetFacts(
      raw_database.get(), handles[1], rocksdb::ReadOptions(), spec,
      [&entities](const rocksdb::cedar_parquet::CedarParquetColumnarBatch& batch) {
        const auto& entity = batch.columns[0];
        const auto& family = batch.columns[1];
        const auto& entity_values = std::get<std::vector<uint64_t>>(entity.values);
        for (size_t index = 0; index < batch.row_count(); ++index) {
          if (entity.present[index] != 0 && family.present[index] != 0) {
            entities.push_back(entity_values[index]);
          }
        }
        return rocksdb::Status::OK();
      })
          .ok());
  EXPECT_EQ(entities, std::vector<uint64_t>({8, 9, 10}));

  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    ASSERT_TRUE(raw_database->DestroyColumnFamilyHandle(handle).ok());
  }
}

TEST_F(ColumnarFactScanTest,
       PinnedSuperVersionAdapterMergesActiveMemtableAndPointDeletion) {
  CommitVertex(VertexId{8}, ValidTime{10});
  CommitVertex(VertexId{9}, ValidTime{20});
  CommitVertex(VertexId{10}, ValidTime{30});

  ASSERT_TRUE(database_->Close().ok());
  database_.reset();
  FactStoreOptions store_options;
  store_options.path = path_;
  const rocksdb::Options options = internal::MakeRocksDbOptions(store_options, false);
  std::unique_ptr<rocksdb::DB> raw_database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_TRUE(rocksdb::DB::Open(
                  options, path_,
                  internal::MakeRocksDbColumnFamilyDescriptors(store_options, options),
                  &handles, &raw_database)
                  .ok());

  std::unique_ptr<rocksdb::Iterator> iterator(
      raw_database->NewIterator(rocksdb::ReadOptions(), handles[1]));
  iterator->SeekToFirst();
  ASSERT_TRUE(iterator->Valid()) << iterator->status().ToString();
  const std::string erased_key = iterator->key().ToString();
  std::string active_memtable_key = erased_key;
  ASSERT_EQ(active_memtable_key.size(), 32U);
  active_memtable_key[15] = 11;
  ASSERT_TRUE(raw_database
                  ->Put(rocksdb::WriteOptions(), handles[1], active_memtable_key,
                        iterator->value())
                  .ok());
  iterator.reset();

  rocksdb::cedar_parquet::CedarParquetScanSpec spec;
  spec.projection = {rocksdb::cedar_parquet::CedarParquetColumnId::kEntityId};
  std::vector<uint64_t> entities_before_delete;
  const rocksdb::Snapshot* before_delete = raw_database->GetSnapshot();
  ASSERT_NE(before_delete, nullptr);
  rocksdb::ReadOptions before_delete_options;
  before_delete_options.snapshot = before_delete;
  ASSERT_TRUE(rocksdb::ScanCedarParquetFacts(
      raw_database.get(), handles[1], before_delete_options, spec,
      [&entities_before_delete](
          const rocksdb::cedar_parquet::CedarParquetColumnarBatch& batch) {
        const auto& entity_values =
            std::get<std::vector<uint64_t>>(batch.columns[0].values);
        entities_before_delete.insert(entities_before_delete.end(),
                                      entity_values.begin(), entity_values.end());
        return rocksdb::Status::OK();
      })
                  .ok());
  EXPECT_EQ(entities_before_delete, std::vector<uint64_t>({8, 9, 10, 11}));

  ASSERT_TRUE(raw_database->Delete(rocksdb::WriteOptions(), handles[1], erased_key).ok());
  std::vector<uint64_t> entities_after_delete;
  ASSERT_TRUE(rocksdb::ScanCedarParquetFacts(
      raw_database.get(), handles[1], rocksdb::ReadOptions(), spec,
      [&entities_after_delete](
          const rocksdb::cedar_parquet::CedarParquetColumnarBatch& batch) {
        const auto& entity_values =
            std::get<std::vector<uint64_t>>(batch.columns[0].values);
        entities_after_delete.insert(entities_after_delete.end(),
                                     entity_values.begin(), entity_values.end());
        return rocksdb::Status::OK();
      })
                  .ok());
  EXPECT_EQ(entities_after_delete, std::vector<uint64_t>({9, 10, 11}));
  raw_database->ReleaseSnapshot(before_delete);

  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    ASSERT_TRUE(raw_database->DestroyColumnFamilyHandle(handle).ok());
  }
}

TEST_F(ColumnarFactScanTest,
       PinnedSuperVersionAdapterMergesImmutableMemtableAndActivePointDeletion) {
  CommitVertex(VertexId{8}, ValidTime{10});

  ASSERT_TRUE(database_->Close().ok());
  database_.reset();
  FactStoreOptions store_options;
  store_options.path = path_;
  const rocksdb::Options options = internal::MakeRocksDbOptions(store_options, false);
  std::unique_ptr<rocksdb::DB> raw_database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_TRUE(rocksdb::DB::Open(
                  options, path_,
                  internal::MakeRocksDbColumnFamilyDescriptors(store_options, options),
                  &handles, &raw_database)
                  .ok());
  rocksdb::FlushOptions flush_options;
  flush_options.wait = true;
  ASSERT_TRUE(raw_database->Flush(flush_options, handles[1]).ok());

  std::unique_ptr<rocksdb::Iterator> iterator(
      raw_database->NewIterator(rocksdb::ReadOptions(), handles[1]));
  iterator->SeekToFirst();
  ASSERT_TRUE(iterator->Valid()) << iterator->status().ToString();
  const std::string persisted_key = iterator->key().ToString();
  std::string immutable_key = persisted_key;
  ASSERT_EQ(immutable_key.size(), 32U);
  immutable_key[15] = 9;
  const std::string encoded_value = iterator->value().ToString();
  iterator.reset();

  ASSERT_TRUE(raw_database->PauseBackgroundWork().ok());
  struct BackgroundWorkResume {
    rocksdb::DB* database;
    ~BackgroundWorkResume() { EXPECT_TRUE(database->ContinueBackgroundWork().ok()); }
  } resume_background_work{raw_database.get()};

  ASSERT_TRUE(raw_database
                  ->Put(rocksdb::WriteOptions(), handles[1], immutable_key,
                        encoded_value)
                  .ok());
  ASSERT_TRUE(raw_database
                  ->SetOptions(handles[1], {{"write_buffer_size", "65536"},
                                            {"arena_block_size", "4096"},
                                            {"max_write_buffer_number", "16"}})
                  .ok());
  // Use a legal MemTable capacity and enough versions to cross it. Rewriting
  // one user key rolls earlier versions into immutable MemTables without
  // changing the snapshot-visible entity set.
  for (int version = 0; version < 2048; ++version) {
    ASSERT_TRUE(raw_database
                    ->Put(rocksdb::WriteOptions(), handles[1], immutable_key,
                          encoded_value)
                    .ok());
  }
  const rocksdb::Snapshot* before_delete = raw_database->GetSnapshot();
  ASSERT_NE(before_delete, nullptr);
  rocksdb::ReadOptions before_delete_options;
  before_delete_options.snapshot = before_delete;
  ASSERT_TRUE(raw_database
                  ->Delete(rocksdb::WriteOptions(), handles[1], persisted_key)
                  .ok());
  uint64_t immutable_memtable_count = 0;
  ASSERT_TRUE(raw_database->GetIntProperty(handles[1], "rocksdb.num-immutable-mem-table",
                                           &immutable_memtable_count));
  ASSERT_GE(immutable_memtable_count, 1U);

  rocksdb::cedar_parquet::CedarParquetScanSpec spec;
  spec.projection = {rocksdb::cedar_parquet::CedarParquetColumnId::kEntityId};
  const auto collect_entities = [&](const rocksdb::ReadOptions& read_options) {
    std::vector<uint64_t> entities;
    const rocksdb::Status status = rocksdb::ScanCedarParquetFacts(
        raw_database.get(), handles[1], read_options, spec,
        [&entities](const rocksdb::cedar_parquet::CedarParquetColumnarBatch& batch) {
          const auto& entity_values =
              std::get<std::vector<uint64_t>>(batch.columns[0].values);
          entities.insert(entities.end(), entity_values.begin(), entity_values.end());
          return rocksdb::Status::OK();
        });
    EXPECT_TRUE(status.ok()) << status.ToString();
    return entities;
  };

  EXPECT_EQ(collect_entities(before_delete_options), std::vector<uint64_t>({8, 9}));
  EXPECT_EQ(collect_entities(rocksdb::ReadOptions()), std::vector<uint64_t>({9}));
  raw_database->ReleaseSnapshot(before_delete);

  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    ASSERT_TRUE(raw_database->DestroyColumnFamilyHandle(handle).ok());
  }
}

TEST_F(ColumnarFactScanTest,
       PinnedSuperVersionAdapterMergesMultipleFlushedFactsFilesWithProjectedCursors) {
  CommitVertex(VertexId{8}, ValidTime{10});
  FlushFactsAndReopen();
  CommitVertex(VertexId{9}, ValidTime{20});
  FlushFactsAndReopen();

  ASSERT_TRUE(database_->Close().ok());
  database_.reset();
  FactStoreOptions store_options;
  store_options.path = path_;
  const rocksdb::Options options = internal::MakeRocksDbOptions(store_options, false);
  std::unique_ptr<rocksdb::DB> raw_database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_TRUE(rocksdb::DB::Open(
                  options, path_,
                  internal::MakeRocksDbColumnFamilyDescriptors(store_options, options),
                  &handles, &raw_database)
                  .ok());

  std::vector<rocksdb::LiveFileMetaData> live_files;
  raw_database->GetLiveFilesMetaData(&live_files);
  size_t flushed_fact_files = 0;
  for (const auto& file : live_files) {
    if (file.column_family_name == "facts") ++flushed_fact_files;
  }
  ASSERT_GE(flushed_fact_files, 2U);

  rocksdb::cedar_parquet::CedarParquetScanSpec spec;
  spec.projection = {rocksdb::cedar_parquet::CedarParquetColumnId::kEntityId};
  std::vector<uint64_t> entities;
  const rocksdb::Status scan = rocksdb::ScanCedarParquetFacts(
      raw_database.get(), handles[1], rocksdb::ReadOptions(), spec,
      [&entities](const rocksdb::cedar_parquet::CedarParquetColumnarBatch& batch) {
        const auto& entity_values =
            std::get<std::vector<uint64_t>>(batch.columns[0].values);
        entities.insert(entities.end(), entity_values.begin(), entity_values.end());
        return rocksdb::Status::OK();
      });
  ASSERT_TRUE(scan.ok()) << scan.ToString();
  EXPECT_EQ(entities, std::vector<uint64_t>({8, 9}));

  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    ASSERT_TRUE(raw_database->DestroyColumnFamilyHandle(handle).ok());
  }
}

TEST_F(ColumnarFactScanTest,
       PinnedSuperVersionAdapterMergesCompactedLevelAndActiveMemtable) {
  CommitVertex(VertexId{8}, ValidTime{10});
  FlushFactsAndReopen();
  CommitVertex(VertexId{9}, ValidTime{20});
  FlushFactsAndReopen();
  CommitVertex(VertexId{10}, ValidTime{30});

  ASSERT_TRUE(database_->Close().ok());
  database_.reset();
  FactStoreOptions store_options;
  store_options.path = path_;
  rocksdb::Options options = internal::MakeRocksDbOptions(store_options, false);
  options.disable_auto_compactions = true;
  std::unique_ptr<rocksdb::DB> raw_database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_TRUE(rocksdb::DB::Open(
                  options, path_,
                  internal::MakeRocksDbColumnFamilyDescriptors(store_options, options),
                  &handles, &raw_database)
                  .ok());

  std::vector<rocksdb::LiveFileMetaData> live_files;
  raw_database->GetLiveFilesMetaData(&live_files);
  std::vector<std::string> level_zero_inputs;
  for (const auto& file : live_files) {
    if (file.column_family_name == "facts" && file.level == 0) {
      level_zero_inputs.push_back(std::filesystem::path(file.relative_filename)
                                      .filename()
                                      .string());
    }
  }
  ASSERT_GE(level_zero_inputs.size(), 2U);
  ASSERT_TRUE(raw_database
                  ->CompactFiles(rocksdb::CompactionOptions(), handles[1],
                                 level_zero_inputs, 1)
                  .ok());

  live_files.clear();
  raw_database->GetLiveFilesMetaData(&live_files);
  bool has_level_one_fact_file = false;
  for (const auto& file : live_files) {
    has_level_one_fact_file = has_level_one_fact_file ||
                              (file.column_family_name == "facts" && file.level == 1);
  }
  ASSERT_TRUE(has_level_one_fact_file);

  rocksdb::cedar_parquet::CedarParquetScanSpec spec;
  spec.projection = {rocksdb::cedar_parquet::CedarParquetColumnId::kEntityId};
  std::vector<uint64_t> entities;
  ASSERT_TRUE(rocksdb::ScanCedarParquetFacts(
      raw_database.get(), handles[1], rocksdb::ReadOptions(), spec,
      [&entities](const rocksdb::cedar_parquet::CedarParquetColumnarBatch& batch) {
        const auto& entity_values =
            std::get<std::vector<uint64_t>>(batch.columns[0].values);
        entities.insert(entities.end(), entity_values.begin(), entity_values.end());
        return rocksdb::Status::OK();
      })
                  .ok());
  EXPECT_EQ(entities, std::vector<uint64_t>({8, 9, 10}));

  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    ASSERT_TRUE(raw_database->DestroyColumnFamilyHandle(handle).ok());
  }
}

TEST_F(ColumnarFactScanTest,
       ColumnarScansMergeOverlappingL0AndLowerLevelFactsAfterFlushAndReopen) {
  CommitVertex(VertexId{8}, ValidTime{10});
  FlushFactsAndReopen();

  ASSERT_TRUE(database_->Close().ok());
  database_.reset();
  FactStoreOptions store_options;
  store_options.path = path_;
  const rocksdb::Options options = internal::MakeRocksDbOptions(store_options, false);
  std::unique_ptr<rocksdb::DB> raw_database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_TRUE(rocksdb::DB::Open(
                  options, path_,
                  internal::MakeRocksDbColumnFamilyDescriptors(store_options, options),
                  &handles, &raw_database)
                  .ok());
  std::vector<rocksdb::LiveFileMetaData> live_files;
  raw_database->GetLiveFilesMetaData(&live_files);
  std::vector<std::string> level_zero_inputs;
  for (const auto& file : live_files) {
    if (file.column_family_name == "facts" && file.level == 0) {
      level_zero_inputs.push_back(
          std::filesystem::path(file.relative_filename).filename().string());
    }
  }
  ASSERT_EQ(level_zero_inputs.size(), 1U);
  ASSERT_TRUE(raw_database
                  ->CompactFiles(rocksdb::CompactionOptions(), handles[1],
                                 level_zero_inputs, 1)
                  .ok());
  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    ASSERT_TRUE(raw_database->DestroyColumnFamilyHandle(handle).ok());
  }
  raw_database.reset();

  auto reopened = Database::Open(DatabaseOptions{.path = path_});
  ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
  database_ = std::move(reopened).ConsumeValueOrDie();
  CommitVertex(VertexId{8}, ValidTime{20});
  CommitVertex(VertexId{9}, ValidTime{30});
  FlushFactsAndReopen();

  ASSERT_TRUE(database_->Close().ok());
  database_.reset();
  ASSERT_TRUE(rocksdb::DB::Open(
                  options, path_,
                  internal::MakeRocksDbColumnFamilyDescriptors(store_options, options),
                  &handles, &raw_database)
                  .ok());
  live_files.clear();
  raw_database->GetLiveFilesMetaData(&live_files);
  bool has_l0_facts = false;
  bool has_lower_level_facts = false;
  for (const auto& file : live_files) {
    if (file.column_family_name != "facts") continue;
    has_l0_facts = has_l0_facts || file.level == 0;
    has_lower_level_facts = has_lower_level_facts || file.level > 0;
  }
  ASSERT_TRUE(has_l0_facts);
  ASSERT_TRUE(has_lower_level_facts);
  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    ASSERT_TRUE(raw_database->DestroyColumnFamilyHandle(handle).ok());
  }
  raw_database.reset();

  reopened = Database::Open(DatabaseOptions{.path = path_});
  ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
  database_ = std::move(reopened).ConsumeValueOrDie();
  const auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  const FactScanSpec spec{PartId{7}, FactFamily::kVertexState, PropertyId{},
                          ValidTime{}, 1};
  std::vector<std::pair<uint64_t, uint64_t>> events;
  ASSERT_TRUE(snapshot.ValueOrDie().EventColumnarScan(
      spec, {FactColumnId::kEntityId, FactColumnId::kValidFrom},
      [&events](const FactColumnarBatch& batch) -> Status {
        const auto& entities =
            std::get<std::vector<uint64_t>>(batch.columns[0].values);
        const auto& valid_from =
            std::get<std::vector<uint64_t>>(batch.columns[1].values);
        EXPECT_EQ(entities.size(), valid_from.size());
        for (size_t row = 0; row < entities.size(); ++row) {
          events.emplace_back(entities[row], valid_from[row]);
        }
        return Status::OK();
      })
                  .ok());
  EXPECT_EQ(events, (std::vector<std::pair<uint64_t, uint64_t>>{
                        {8, 20}, {8, 10}, {9, 30}}));

  FactScanSpec state_spec{PartId{7}, FactFamily::kVertexState, PropertyId{},
                          ValidTime{35}, 1};
  std::vector<std::pair<uint64_t, uint64_t>> state;
  ASSERT_TRUE(snapshot.ValueOrDie().StateColumnarScan(
      state_spec, {FactColumnId::kEntityId, FactColumnId::kValidFrom},
      [&state](const FactColumnarBatch& batch) -> Status {
        const auto& entities =
            std::get<std::vector<uint64_t>>(batch.columns[0].values);
        const auto& valid_from =
            std::get<std::vector<uint64_t>>(batch.columns[1].values);
        EXPECT_EQ(entities.size(), valid_from.size());
        for (size_t row = 0; row < entities.size(); ++row) {
          state.emplace_back(entities[row], valid_from[row]);
        }
        return Status::OK();
      })
                  .ok());
  EXPECT_EQ(state, (std::vector<std::pair<uint64_t, uint64_t>>{{8, 20}, {9, 30}}));
}

TEST_F(ColumnarFactScanTest,
       PublicColumnarScansMatchCanonicalIteratorOracleAcrossL0LowerLevelAndActiveMemtable) {
  CommitVertex(VertexId{8}, ValidTime{10});
  FlushFactsAndReopen();

  ASSERT_TRUE(database_->Close().ok());
  database_.reset();
  FactStoreOptions store_options;
  store_options.path = path_;
  rocksdb::Options options = internal::MakeRocksDbOptions(store_options, false);
  options.disable_auto_compactions = true;
  std::unique_ptr<rocksdb::DB> raw_database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_TRUE(rocksdb::DB::Open(
                  options, path_,
                  internal::MakeRocksDbColumnFamilyDescriptors(store_options, options),
                  &handles, &raw_database)
                  .ok());
  std::vector<rocksdb::LiveFileMetaData> live_files;
  raw_database->GetLiveFilesMetaData(&live_files);
  std::vector<std::string> level_zero_inputs;
  for (const auto& file : live_files) {
    if (file.column_family_name == "facts" && file.level == 0) {
      level_zero_inputs.push_back(
          std::filesystem::path(file.relative_filename).filename().string());
    }
  }
  ASSERT_EQ(level_zero_inputs.size(), 1U);
  ASSERT_TRUE(raw_database
                  ->CompactFiles(rocksdb::CompactionOptions(), handles[1],
                                 level_zero_inputs, 1)
                  .ok());
  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    ASSERT_TRUE(raw_database->DestroyColumnFamilyHandle(handle).ok());
  }
  raw_database.reset();

  auto reopened = Database::Open(DatabaseOptions{.path = path_});
  ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
  database_ = std::move(reopened).ConsumeValueOrDie();
  CommitVertex(VertexId{8}, ValidTime{20});
  CommitVertex(VertexId{9}, ValidTime{30});
  FlushFactsAndReopen();

  ASSERT_TRUE(database_->Close().ok());
  database_.reset();
  ASSERT_TRUE(rocksdb::DB::Open(
                  options, path_,
                  internal::MakeRocksDbColumnFamilyDescriptors(store_options, options),
                  &handles, &raw_database)
                  .ok());
  live_files.clear();
  raw_database->GetLiveFilesMetaData(&live_files);
  bool has_l0_facts = false;
  bool has_lower_level_facts = false;
  for (const auto& file : live_files) {
    if (file.column_family_name != "facts") continue;
    has_l0_facts = has_l0_facts || file.level == 0;
    has_lower_level_facts = has_lower_level_facts || file.level > 0;
  }
  ASSERT_TRUE(has_l0_facts);
  ASSERT_TRUE(has_lower_level_facts);
  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    ASSERT_TRUE(raw_database->DestroyColumnFamilyHandle(handle).ok());
  }
  raw_database.reset();

  reopened = Database::Open(DatabaseOptions{.path = path_});
  ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
  database_ = std::move(reopened).ConsumeValueOrDie();
  CommitVertex(VertexId{10}, ValidTime{40});

  const auto snapshot = database_->BeginSnapshot();
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  FactScanSpec event_spec{PartId{7}, FactFamily::kVertexState, PropertyId{},
                          ValidTime{35}, 1};
  const std::vector<FactColumnId> projection = {
      FactColumnId::kPartId, FactColumnId::kEntityId, FactColumnId::kValidFrom,
      FactColumnId::kCedarCommitSeq, FactColumnId::kOperation};
  const auto event_rows = [](const std::vector<FactEvent>& events) {
    std::vector<std::array<uint64_t, 5>> rows;
    for (const FactEvent& event : events) {
      rows.push_back({event.ref.part_id().value, event.ref.entity_id(),
                      event.valid_from.value, event.commit_seq.value,
                      static_cast<uint64_t>(event.operation)});
    }
    return rows;
  };
  const auto collect_columnar = [](const FactColumnarBatch& batch,
                                   std::vector<std::array<uint64_t, 5>>* rows) {
    const auto& parts = std::get<std::vector<uint32_t>>(batch.columns[0].values);
    const auto& entities = std::get<std::vector<uint64_t>>(batch.columns[1].values);
    const auto& valid_from = std::get<std::vector<uint64_t>>(batch.columns[2].values);
    const auto& commits = std::get<std::vector<uint64_t>>(batch.columns[3].values);
    const auto& operations = std::get<std::vector<uint32_t>>(batch.columns[4].values);
    for (size_t row = 0; row < batch.row_count(); ++row) {
      EXPECT_EQ(batch.columns[0].present[row], 1);
      EXPECT_EQ(batch.columns[1].present[row], 1);
      EXPECT_EQ(batch.columns[2].present[row], 1);
      EXPECT_EQ(batch.columns[3].present[row], 1);
      EXPECT_EQ(batch.columns[4].present[row], 1);
      rows->push_back({parts[row], entities[row], valid_from[row], commits[row],
                       operations[row]});
    }
    return Status::OK();
  };

  std::vector<FactEvent> canonical_events;
  ASSERT_TRUE(snapshot.ValueOrDie().EventScan(
      event_spec, [&canonical_events](const FactEventBatch& batch) {
        canonical_events.insert(canonical_events.end(), batch.events.begin(), batch.events.end());
        return Status::OK();
      }).ok());
  std::vector<std::array<uint64_t, 5>> columnar_events;
  ASSERT_TRUE(snapshot.ValueOrDie().EventColumnarScan(
      event_spec, projection,
      [&collect_columnar, &columnar_events](const FactColumnarBatch& batch) {
        return collect_columnar(batch, &columnar_events);
      }).ok());
  EXPECT_EQ(columnar_events, event_rows(canonical_events));

  std::vector<FactEvent> canonical_state;
  ASSERT_TRUE(snapshot.ValueOrDie().StateScan(
      event_spec, [&canonical_state](const FactEventBatch& batch) {
        canonical_state.insert(canonical_state.end(), batch.events.begin(), batch.events.end());
        return Status::OK();
      }).ok());
  std::vector<std::array<uint64_t, 5>> columnar_state;
  ASSERT_TRUE(snapshot.ValueOrDie().StateColumnarScan(
      event_spec, projection,
      [&collect_columnar, &columnar_state](const FactColumnarBatch& batch) {
        return collect_columnar(batch, &columnar_state);
      }).ok());
  EXPECT_EQ(columnar_state, event_rows(canonical_state));
}

TEST_F(ColumnarFactScanTest,
       PinnedSuperVersionAdapterPreservesSnapshotVisibilityAcrossFlushAndCompaction) {
  CommitVertex(VertexId{8}, ValidTime{10});
  FlushFactsAndReopen();
  CommitVertex(VertexId{9}, ValidTime{20});
  FlushFactsAndReopen();

  ASSERT_TRUE(database_->Close().ok());
  database_.reset();
  FactStoreOptions store_options;
  store_options.path = path_;
  const rocksdb::Options options = internal::MakeRocksDbOptions(store_options, false);
  std::unique_ptr<rocksdb::DB> raw_database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_TRUE(rocksdb::DB::Open(
                  options, path_,
                  internal::MakeRocksDbColumnFamilyDescriptors(store_options, options),
                  &handles, &raw_database)
                  .ok());

  std::unique_ptr<rocksdb::Iterator> iterator(
      raw_database->NewIterator(rocksdb::ReadOptions(), handles[1]));
  iterator->SeekToFirst();
  ASSERT_TRUE(iterator->Valid()) << iterator->status().ToString();
  const std::string deleted_key = iterator->key().ToString();
  iterator.reset();

  const rocksdb::Snapshot* before_delete = raw_database->GetSnapshot();
  ASSERT_NE(before_delete, nullptr);
  rocksdb::ReadOptions before_delete_options;
  before_delete_options.snapshot = before_delete;
  ASSERT_TRUE(raw_database->Delete(rocksdb::WriteOptions(), handles[1], deleted_key).ok());
  rocksdb::FlushOptions flush_options;
  flush_options.wait = true;
  ASSERT_TRUE(raw_database->Flush(flush_options, handles[1]).ok());

  std::vector<rocksdb::LiveFileMetaData> live_files;
  raw_database->GetLiveFilesMetaData(&live_files);
  std::vector<std::string> level_zero_inputs;
  for (const auto& file : live_files) {
    if (file.column_family_name == "facts" && file.level == 0) {
      level_zero_inputs.push_back(
          std::filesystem::path(file.relative_filename).filename().string());
    }
  }
  ASSERT_GE(level_zero_inputs.size(), 3U);
  ASSERT_TRUE(raw_database
                  ->CompactFiles(rocksdb::CompactionOptions(), handles[1],
                                 level_zero_inputs, 1)
                  .ok());

  rocksdb::cedar_parquet::CedarParquetScanSpec spec;
  spec.projection = {rocksdb::cedar_parquet::CedarParquetColumnId::kEntityId};
  const auto collect_entities = [&](const rocksdb::ReadOptions& read_options) {
    std::vector<uint64_t> entities;
    const rocksdb::Status status = rocksdb::ScanCedarParquetFacts(
        raw_database.get(), handles[1], read_options, spec,
        [&entities](const rocksdb::cedar_parquet::CedarParquetColumnarBatch& batch) {
          const auto& entity_values =
              std::get<std::vector<uint64_t>>(batch.columns[0].values);
          entities.insert(entities.end(), entity_values.begin(), entity_values.end());
          return rocksdb::Status::OK();
        });
    EXPECT_TRUE(status.ok()) << status.ToString();
    return entities;
  };

  EXPECT_EQ(collect_entities(before_delete_options), std::vector<uint64_t>({8, 9}));
  EXPECT_EQ(collect_entities(rocksdb::ReadOptions()), std::vector<uint64_t>({9}));
  raw_database->ReleaseSnapshot(before_delete);

  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    ASSERT_TRUE(raw_database->DestroyColumnFamilyHandle(handle).ok());
  }
}

TEST_F(ColumnarFactScanTest,
       PinnedSuperVersionAdapterSeeksPastCorruptRowsBelowProjectedKeyRange) {
  CommitVertex(VertexId{8}, ValidTime{10});
  CommitVertex(VertexId{9}, ValidTime{20});
  FlushFactsAndReopen();

  ASSERT_TRUE(database_->Close().ok());
  database_.reset();
  FactStoreOptions store_options;
  store_options.path = path_;
  const rocksdb::Options options = internal::MakeRocksDbOptions(store_options, false);
  std::unique_ptr<rocksdb::DB> raw_database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_TRUE(rocksdb::DB::Open(
                  options, path_,
                  internal::MakeRocksDbColumnFamilyDescriptors(store_options, options),
                  &handles, &raw_database)
                  .ok());

  const FactRef corrupt_ref{PartId{7}, FactFamily::kVertexState, PropertyId{}, 8};
  const std::string corrupt_key =
      EncodeFactKey(corrupt_ref, ValidTime{10}, CommitSeq{1});
  ASSERT_FALSE(corrupt_key.empty());
  ASSERT_TRUE(raw_database
                  ->Put(rocksdb::WriteOptions(), handles[1], corrupt_key, "corrupt")
                  .ok());

  const FactRef selected_ref{PartId{7}, FactFamily::kVertexState, PropertyId{}, 9};
  const std::string lower_user_key = EncodeFactKey(
      selected_ref, ValidTime{std::numeric_limits<uint64_t>::max()},
      CommitSeq{std::numeric_limits<uint64_t>::max()});
  const std::string upper_user_key =
      EncodeFactKey(selected_ref, ValidTime{}, CommitSeq{1});
  ASSERT_EQ(lower_user_key.size(), 32U);
  ASSERT_EQ(upper_user_key.size(), 32U);

  rocksdb::cedar_parquet::CedarParquetScanSpec spec;
  spec.sort_key_lower = lower_user_key + std::string(7, '\0') + '\xfe';
  spec.sort_key_upper = upper_user_key + std::string(8, '\xff');
  spec.projection = {rocksdb::cedar_parquet::CedarParquetColumnId::kEntityId};
  std::vector<uint64_t> entities;
  const rocksdb::Status bounded_scan = rocksdb::ScanCedarParquetFacts(
      raw_database.get(), handles[1], rocksdb::ReadOptions(), spec,
      [&entities](const rocksdb::cedar_parquet::CedarParquetColumnarBatch& batch) {
        const auto& entity_values =
            std::get<std::vector<uint64_t>>(batch.columns[0].values);
        entities.insert(entities.end(), entity_values.begin(), entity_values.end());
        return rocksdb::Status::OK();
      });
  ASSERT_TRUE(bounded_scan.ok()) << bounded_scan.ToString();
  EXPECT_EQ(entities, std::vector<uint64_t>({9}));

  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    ASSERT_TRUE(raw_database->DestroyColumnFamilyHandle(handle).ok());
  }
}

TEST_F(ColumnarFactScanTest,
       PinnedSuperVersionAdapterPropagatesCorruptSelectedActiveMemtableValue) {
  CommitVertex(VertexId{8}, ValidTime{10});
  FlushFactsAndReopen();

  ASSERT_TRUE(database_->Close().ok());
  database_.reset();
  FactStoreOptions store_options;
  store_options.path = path_;
  const rocksdb::Options options = internal::MakeRocksDbOptions(store_options, false);
  std::unique_ptr<rocksdb::DB> raw_database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_TRUE(rocksdb::DB::Open(
                  options, path_,
                  internal::MakeRocksDbColumnFamilyDescriptors(store_options, options),
                  &handles, &raw_database)
                  .ok());

  const FactRef corrupt_ref{PartId{7}, FactFamily::kVertexState, PropertyId{}, 9};
  const std::string corrupt_key =
      EncodeFactKey(corrupt_ref, ValidTime{20}, CommitSeq{2});
  ASSERT_FALSE(corrupt_key.empty());
  ASSERT_TRUE(raw_database
                  ->Put(rocksdb::WriteOptions(), handles[1], corrupt_key, "corrupt")
                  .ok());

  std::vector<rocksdb::LiveFileMetaData> live_files;
  raw_database->GetLiveFilesMetaData(&live_files);
  std::filesystem::path facts_file;
  for (const auto& file : live_files) {
    if (file.column_family_name == "facts") {
      facts_file = std::filesystem::path(path_) /
                   std::filesystem::path(file.relative_filename).filename();
      break;
    }
  }
  ASSERT_FALSE(facts_file.empty());
  std::ifstream facts_stream(facts_file, std::ios::binary);
  ASSERT_TRUE(facts_stream.is_open()) << facts_file;
  std::array<char, 4> magic{};
  facts_stream.read(magic.data(), static_cast<std::streamsize>(magic.size()));
  ASSERT_EQ(facts_stream.gcount(), static_cast<std::streamsize>(magic.size()));
  EXPECT_EQ(std::string(magic.data(), magic.size()), "PAR1");

  rocksdb::cedar_parquet::CedarParquetScanSpec spec;
  spec.batch_row_limit = 1;
  spec.projection = {rocksdb::cedar_parquet::CedarParquetColumnId::kEntityId};
  std::vector<uint64_t> delivered_entities;
  const rocksdb::Status scan = rocksdb::ScanCedarParquetFacts(
      raw_database.get(), handles[1], rocksdb::ReadOptions(), spec,
      [&delivered_entities](
          const rocksdb::cedar_parquet::CedarParquetColumnarBatch& batch) {
        const auto& entities =
            std::get<std::vector<uint64_t>>(batch.columns[0].values);
        delivered_entities.insert(delivered_entities.end(), entities.begin(), entities.end());
        return rocksdb::Status::OK();
      });
  EXPECT_TRUE(scan.IsCorruption()) << scan.ToString();
  EXPECT_EQ(delivered_entities, std::vector<uint64_t>({8}));

  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    ASSERT_TRUE(raw_database->DestroyColumnFamilyHandle(handle).ok());
  }
}

TEST_F(ColumnarFactScanTest,
       PinnedSuperVersionAdapterPropagatesCorruptSelectedImmutableMemtableValue) {
  CommitVertex(VertexId{8}, ValidTime{10});
  FlushFactsAndReopen();

  ASSERT_TRUE(database_->Close().ok());
  database_.reset();
  FactStoreOptions store_options;
  store_options.path = path_;
  const rocksdb::Options options = internal::MakeRocksDbOptions(store_options, false);
  std::unique_ptr<rocksdb::DB> raw_database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_TRUE(rocksdb::DB::Open(
                  options, path_,
                  internal::MakeRocksDbColumnFamilyDescriptors(store_options, options),
                  &handles, &raw_database)
                  .ok());

  std::unique_ptr<rocksdb::Iterator> iterator(
      raw_database->NewIterator(rocksdb::ReadOptions(), handles[1]));
  iterator->SeekToFirst();
  ASSERT_TRUE(iterator->Valid()) << iterator->status().ToString();
  const std::string encoded_value = iterator->value().ToString();
  iterator.reset();

  const FactRef corrupt_ref{PartId{7}, FactFamily::kVertexState, PropertyId{}, 9};
  const std::string corrupt_key =
      EncodeFactKey(corrupt_ref, ValidTime{20}, CommitSeq{2});
  ASSERT_FALSE(corrupt_key.empty());
  const FactRef immutable_filler_ref{PartId{7}, FactFamily::kVertexState,
                                     PropertyId{}, 10};
  const std::string immutable_filler_key =
      EncodeFactKey(immutable_filler_ref, ValidTime{30}, CommitSeq{3});
  ASSERT_FALSE(immutable_filler_key.empty());
  const FactRef active_ref{PartId{7}, FactFamily::kVertexState, PropertyId{}, 11};
  const std::string active_key =
      EncodeFactKey(active_ref, ValidTime{40}, CommitSeq{4});
  ASSERT_FALSE(active_key.empty());

  ASSERT_TRUE(raw_database->PauseBackgroundWork().ok());
  struct BackgroundWorkResume {
    rocksdb::DB* database;
    ~BackgroundWorkResume() { EXPECT_TRUE(database->ContinueBackgroundWork().ok()); }
  } resume_background_work{raw_database.get()};

  ASSERT_TRUE(raw_database
                  ->Put(rocksdb::WriteOptions(), handles[1], corrupt_key, "corrupt")
                  .ok());
  ASSERT_TRUE(raw_database
                  ->SetOptions(handles[1], { {"write_buffer_size", "65536"},
                                             {"arena_block_size", "4096"},
                                             {"max_write_buffer_number", "16"} })
                  .ok());
  // The repeated filler writes cross the reduced capacity and rotate the
  // current table containing the malformed value.
  for (int version = 0; version < 2048; ++version) {
    ASSERT_TRUE(raw_database
                    ->Put(rocksdb::WriteOptions(), handles[1], immutable_filler_key,
                          encoded_value)
                    .ok());
  }
  ASSERT_TRUE(raw_database
                  ->Put(rocksdb::WriteOptions(), handles[1], active_key, encoded_value)
                  .ok());
  uint64_t immutable_memtable_count = 0;
  ASSERT_TRUE(raw_database->GetIntProperty(handles[1], "rocksdb.num-immutable-mem-table",
                                           &immutable_memtable_count));
  ASSERT_GE(immutable_memtable_count, 1U);

  std::vector<rocksdb::LiveFileMetaData> live_files;
  raw_database->GetLiveFilesMetaData(&live_files);
  std::filesystem::path facts_file;
  for (const auto& file : live_files) {
    if (file.column_family_name == "facts") {
      facts_file = std::filesystem::path(path_) /
                   std::filesystem::path(file.relative_filename).filename();
      break;
    }
  }
  ASSERT_FALSE(facts_file.empty());
  std::ifstream facts_stream(facts_file, std::ios::binary);
  ASSERT_TRUE(facts_stream.is_open()) << facts_file;
  std::array<char, 4> magic{};
  facts_stream.read(magic.data(), static_cast<std::streamsize>(magic.size()));
  ASSERT_EQ(facts_stream.gcount(), static_cast<std::streamsize>(magic.size()));
  EXPECT_EQ(std::string(magic.data(), magic.size()), "PAR1");

  rocksdb::cedar_parquet::CedarParquetScanSpec spec;
  spec.batch_row_limit = 1;
  spec.projection = {rocksdb::cedar_parquet::CedarParquetColumnId::kEntityId};
  std::vector<uint64_t> delivered_entities;
  const rocksdb::Status scan = rocksdb::ScanCedarParquetFacts(
      raw_database.get(), handles[1], rocksdb::ReadOptions(), spec,
      [&delivered_entities](
          const rocksdb::cedar_parquet::CedarParquetColumnarBatch& batch) {
        const auto& entities =
            std::get<std::vector<uint64_t>>(batch.columns[0].values);
        delivered_entities.insert(delivered_entities.end(), entities.begin(), entities.end());
        return rocksdb::Status::OK();
      });
  EXPECT_TRUE(scan.IsCorruption()) << scan.ToString();
  EXPECT_EQ(delivered_entities, std::vector<uint64_t>({8}));

  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    ASSERT_TRUE(raw_database->DestroyColumnFamilyHandle(handle).ok());
  }
}

TEST_F(ColumnarFactScanTest,
       PinnedSuperVersionAdapterSkipsUnprojectedCanonicalValuesInSingleFactsTable) {
  CommitVertex(VertexId{8}, ValidTime{10});

  ASSERT_TRUE(database_->Close().ok());
  database_.reset();
  FactStoreOptions store_options;
  store_options.path = path_;
  const rocksdb::Options options = internal::MakeRocksDbOptions(store_options, false);
  std::unique_ptr<rocksdb::DB> raw_database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_TRUE(rocksdb::DB::Open(
                  options, path_,
                  internal::MakeRocksDbColumnFamilyDescriptors(store_options, options),
                  &handles, &raw_database)
                  .ok());
  rocksdb::FlushOptions flush_options;
  flush_options.wait = true;
  ASSERT_TRUE(raw_database->Flush(flush_options, handles[1]).ok());

  std::unique_ptr<rocksdb::Iterator> iterator(
      raw_database->NewIterator(rocksdb::ReadOptions(), handles[1]));
  iterator->SeekToFirst();
  ASSERT_TRUE(iterator->Valid()) << iterator->status().ToString();
  const std::string encoded_value = iterator->value().ToString();
  ASSERT_FALSE(encoded_value.empty());

  std::vector<rocksdb::LiveFileMetaData> live_files;
  raw_database->GetLiveFilesMetaData(&live_files);
  std::filesystem::path facts_file;
  for (const auto& file : live_files) {
    if (file.column_family_name == "facts") {
      facts_file = std::filesystem::path(path_) /
                   std::filesystem::path(file.relative_filename).filename();
      break;
    }
  }
  ASSERT_FALSE(facts_file.empty());
  iterator.reset();
  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    ASSERT_TRUE(raw_database->DestroyColumnFamilyHandle(handle).ok());
  }
  raw_database.reset();

  std::fstream file(facts_file, std::ios::in | std::ios::out | std::ios::binary);
  ASSERT_TRUE(file.is_open()) << facts_file;
  file.seekg(0, std::ios::end);
  const std::streamoff size = file.tellg();
  ASSERT_GT(size, 0);
  std::string bytes(static_cast<size_t>(size), '\0');
  file.seekg(0);
  file.read(bytes.data(), size);
  ASSERT_TRUE(file.good());
  const size_t encoded_offset = bytes.find(encoded_value);
  ASSERT_NE(encoded_offset, std::string::npos);
  file.seekp(static_cast<std::streamoff>(encoded_offset + encoded_value.size() - 1));
  const char corrupted = static_cast<char>(
      static_cast<unsigned char>(encoded_value.back()) ^ static_cast<unsigned char>(1));
  file.write(&corrupted, 1);
  ASSERT_TRUE(file.good());
  file.close();

  ASSERT_TRUE(rocksdb::DB::Open(
                  options, path_,
                  internal::MakeRocksDbColumnFamilyDescriptors(store_options, options),
                  &handles, &raw_database)
                  .ok());
  rocksdb::cedar_parquet::CedarParquetScanSpec spec;
  spec.projection = {rocksdb::cedar_parquet::CedarParquetColumnId::kEntityId};
  std::vector<uint64_t> entities;
  const rocksdb::Status scan = rocksdb::ScanCedarParquetFacts(
      raw_database.get(), handles[1], rocksdb::ReadOptions(), spec,
      [&entities](const rocksdb::cedar_parquet::CedarParquetColumnarBatch& batch) {
        const auto& entity_values =
            std::get<std::vector<uint64_t>>(batch.columns[0].values);
        entities.insert(entities.end(), entity_values.begin(), entity_values.end());
        return rocksdb::Status::OK();
      });
  ASSERT_TRUE(scan.ok()) << scan.ToString();
  EXPECT_EQ(entities, std::vector<uint64_t>({8}));

  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    ASSERT_TRUE(raw_database->DestroyColumnFamilyHandle(handle).ok());
  }
}

TEST_F(ColumnarFactScanTest,
       PinnedSuperVersionAdapterMergesActiveMemtableWithProjectedFactsTable) {
  CommitVertex(VertexId{8}, ValidTime{10});

  ASSERT_TRUE(database_->Close().ok());
  database_.reset();
  FactStoreOptions store_options;
  store_options.path = path_;
  const rocksdb::Options options = internal::MakeRocksDbOptions(store_options, false);
  std::unique_ptr<rocksdb::DB> raw_database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_TRUE(rocksdb::DB::Open(
                  options, path_,
                  internal::MakeRocksDbColumnFamilyDescriptors(store_options, options),
                  &handles, &raw_database)
                  .ok());
  rocksdb::FlushOptions flush_options;
  flush_options.wait = true;
  ASSERT_TRUE(raw_database->Flush(flush_options, handles[1]).ok());

  std::unique_ptr<rocksdb::Iterator> iterator(
      raw_database->NewIterator(rocksdb::ReadOptions(), handles[1]));
  iterator->SeekToFirst();
  ASSERT_TRUE(iterator->Valid()) << iterator->status().ToString();
  const std::string encoded_value = iterator->value().ToString();
  ASSERT_FALSE(encoded_value.empty());

  std::vector<rocksdb::LiveFileMetaData> live_files;
  raw_database->GetLiveFilesMetaData(&live_files);
  std::filesystem::path facts_file;
  for (const auto& file : live_files) {
    if (file.column_family_name == "facts") {
      facts_file = std::filesystem::path(path_) /
                   std::filesystem::path(file.relative_filename).filename();
      break;
    }
  }
  ASSERT_FALSE(facts_file.empty());
  iterator.reset();
  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    ASSERT_TRUE(raw_database->DestroyColumnFamilyHandle(handle).ok());
  }
  raw_database.reset();

  const std::string active_source_path = path_ + "-active-source";
  auto active_source = Database::Open(DatabaseOptions{.path = active_source_path});
  ASSERT_TRUE(active_source.ok()) << active_source.status().ToString();
  {
    auto transaction = active_source.ValueOrDie()->BeginTransaction();
    ASSERT_TRUE(transaction.ok()) << transaction.status().ToString();
    ASSERT_TRUE(transaction.ValueOrDie()
                    ->Assert(EntityFact::Vertex(VertexRef{PartId{7}, VertexId{9}}),
                             ValidTime{20})
                    .ok());
    const auto committed = transaction.ValueOrDie()->Commit();
    ASSERT_TRUE(committed.ok()) << committed.status().ToString();
    ASSERT_EQ(committed.ValueOrDie().outcome, CommitOutcome::kCommitted);
  }
  ASSERT_TRUE(active_source.ValueOrDie()->Close().ok());

  std::unique_ptr<rocksdb::DB> active_raw_database;
  std::vector<rocksdb::ColumnFamilyHandle*> active_handles;
  ASSERT_TRUE(rocksdb::DB::Open(
                  options, active_source_path,
                  internal::MakeRocksDbColumnFamilyDescriptors(store_options, options),
                  &active_handles, &active_raw_database)
                  .ok());
  std::unique_ptr<rocksdb::Iterator> active_iterator(
      active_raw_database->NewIterator(rocksdb::ReadOptions(), active_handles[1]));
  active_iterator->SeekToFirst();
  ASSERT_TRUE(active_iterator->Valid()) << active_iterator->status().ToString();
  const std::string active_user_key = active_iterator->key().ToString();
  const std::string active_encoded_value = active_iterator->value().ToString();
  active_iterator.reset();
  for (rocksdb::ColumnFamilyHandle* handle : active_handles) {
    ASSERT_TRUE(active_raw_database->DestroyColumnFamilyHandle(handle).ok());
  }
  active_raw_database.reset();
  std::filesystem::remove_all(active_source_path);

  std::fstream file(facts_file, std::ios::in | std::ios::out | std::ios::binary);
  ASSERT_TRUE(file.is_open()) << facts_file;
  file.seekg(0, std::ios::end);
  const std::streamoff size = file.tellg();
  ASSERT_GT(size, 0);
  std::string bytes(static_cast<size_t>(size), '\0');
  file.seekg(0);
  file.read(bytes.data(), size);
  ASSERT_TRUE(file.good());
  const size_t encoded_offset = bytes.find(encoded_value);
  ASSERT_NE(encoded_offset, std::string::npos);
  file.seekp(static_cast<std::streamoff>(encoded_offset + encoded_value.size() - 1));
  const char corrupted = static_cast<char>(
      static_cast<unsigned char>(encoded_value.back()) ^ static_cast<unsigned char>(1));
  file.write(&corrupted, 1);
  ASSERT_TRUE(file.good());
  file.close();

  ASSERT_TRUE(rocksdb::DB::Open(
                  options, path_,
                  internal::MakeRocksDbColumnFamilyDescriptors(store_options, options),
                  &handles, &raw_database)
                  .ok());
  ASSERT_TRUE(raw_database
                  ->Put(rocksdb::WriteOptions(), handles[1], active_user_key,
                        active_encoded_value)
                  .ok());

  rocksdb::cedar_parquet::CedarParquetScanSpec spec;
  spec.projection = {rocksdb::cedar_parquet::CedarParquetColumnId::kEntityId};
  std::vector<uint64_t> entities;
  const rocksdb::Status scan = rocksdb::ScanCedarParquetFacts(
      raw_database.get(), handles[1], rocksdb::ReadOptions(), spec,
      [&entities](const rocksdb::cedar_parquet::CedarParquetColumnarBatch& batch) {
        const auto& entity_values =
            std::get<std::vector<uint64_t>>(batch.columns[0].values);
        entities.insert(entities.end(), entity_values.begin(), entity_values.end());
        return rocksdb::Status::OK();
      });
  ASSERT_TRUE(scan.ok()) << scan.ToString();
  EXPECT_EQ(entities, std::vector<uint64_t>({8, 9}));
  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    ASSERT_TRUE(raw_database->DestroyColumnFamilyHandle(handle).ok());
  }
}

TEST_F(ColumnarFactScanTest,
       PinnedSuperVersionAdapterMergesAllSourceKindsWithProjectionAndSnapshotDelete) {
  CommitVertex(VertexId{8}, ValidTime{10});
  FlushFactsAndReopen();

  ASSERT_TRUE(database_->Close().ok());
  database_.reset();
  FactStoreOptions store_options;
  store_options.path = path_;
  rocksdb::Options options = internal::MakeRocksDbOptions(store_options, false);
  options.disable_auto_compactions = true;
  std::unique_ptr<rocksdb::DB> raw_database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_TRUE(rocksdb::DB::Open(
                  options, path_,
                  internal::MakeRocksDbColumnFamilyDescriptors(store_options, options),
                  &handles, &raw_database)
                  .ok());
  std::vector<rocksdb::LiveFileMetaData> live_files;
  raw_database->GetLiveFilesMetaData(&live_files);
  std::vector<std::string> level_zero_inputs;
  for (const auto& file : live_files) {
    if (file.column_family_name == "facts" && file.level == 0) {
      level_zero_inputs.push_back(
          std::filesystem::path(file.relative_filename).filename().string());
    }
  }
  ASSERT_EQ(level_zero_inputs.size(), 1U);
  ASSERT_TRUE(raw_database
                  ->CompactFiles(rocksdb::CompactionOptions(), handles[1],
                                 level_zero_inputs, 1)
                  .ok());
  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    ASSERT_TRUE(raw_database->DestroyColumnFamilyHandle(handle).ok());
  }
  raw_database.reset();

  auto reopened = Database::Open(DatabaseOptions{.path = path_});
  ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
  database_ = std::move(reopened).ConsumeValueOrDie();
  CommitVertex(VertexId{8}, ValidTime{20});
  CommitVertex(VertexId{9}, ValidTime{30});
  FlushFactsAndReopen();

  ASSERT_TRUE(database_->Close().ok());
  database_.reset();
  ASSERT_TRUE(rocksdb::DB::Open(
                  options, path_,
                  internal::MakeRocksDbColumnFamilyDescriptors(store_options, options),
                  &handles, &raw_database)
                  .ok());
  live_files.clear();
  raw_database->GetLiveFilesMetaData(&live_files);
  bool has_l0_facts = false;
  bool has_lower_level_facts = false;
  for (const auto& file : live_files) {
    if (file.column_family_name != "facts") continue;
    has_l0_facts = has_l0_facts || file.level == 0;
    has_lower_level_facts = has_lower_level_facts || file.level > 0;
  }
  ASSERT_TRUE(has_l0_facts);
  ASSERT_TRUE(has_lower_level_facts);

  std::unique_ptr<rocksdb::Iterator> iterator(
      raw_database->NewIterator(rocksdb::ReadOptions(), handles[1]));
  iterator->SeekToFirst();
  ASSERT_TRUE(iterator->Valid()) << iterator->status().ToString();
  const std::string copied_key = iterator->key().ToString();
  const std::string encoded_value = iterator->value().ToString();
  ASSERT_EQ(copied_key.size(), 32U);
  ASSERT_FALSE(encoded_value.empty());
  std::string deleted_l0_key;
  for (; iterator->Valid(); iterator->Next()) {
    const std::string candidate = iterator->key().ToString();
    if (candidate.size() == 32U && static_cast<unsigned char>(candidate[15]) == 9U) {
      deleted_l0_key = candidate;
      break;
    }
  }
  ASSERT_FALSE(deleted_l0_key.empty());
  ASSERT_TRUE(iterator->status().ok()) << iterator->status().ToString();
  iterator.reset();

  std::string immutable_key = copied_key;
  immutable_key[15] = 10;
  std::string active_key = copied_key;
  active_key[15] = 11;
  ASSERT_TRUE(raw_database->PauseBackgroundWork().ok());
  struct BackgroundWorkResume {
    rocksdb::DB* database;
    ~BackgroundWorkResume() { EXPECT_TRUE(database->ContinueBackgroundWork().ok()); }
  } resume_background_work{raw_database.get()};

  ASSERT_TRUE(raw_database
                  ->Put(rocksdb::WriteOptions(), handles[1], immutable_key, encoded_value)
                  .ok());
  ASSERT_TRUE(raw_database
                  ->SetOptions(handles[1], {{"write_buffer_size", "65536"},
                                            {"arena_block_size", "4096"},
                                            {"max_write_buffer_number", "16"}})
                  .ok());
  for (int version = 0; version < 2048; ++version) {
    ASSERT_TRUE(raw_database
                    ->Put(rocksdb::WriteOptions(), handles[1], immutable_key,
                          encoded_value)
                    .ok());
  }
  uint64_t immutable_memtable_count = 0;
  ASSERT_TRUE(raw_database->GetIntProperty(handles[1], "rocksdb.num-immutable-mem-table",
                                           &immutable_memtable_count));
  ASSERT_GE(immutable_memtable_count, 1U);
  ASSERT_TRUE(raw_database
                  ->SetOptions(handles[1], {{"write_buffer_size", "134217728"}})
                  .ok());
  ASSERT_TRUE(raw_database
                  ->Put(rocksdb::WriteOptions(), handles[1], active_key, encoded_value)
                  .ok());

  const rocksdb::Snapshot* before_delete = raw_database->GetSnapshot();
  ASSERT_NE(before_delete, nullptr);
  rocksdb::ReadOptions before_delete_options;
  before_delete_options.snapshot = before_delete;
  ASSERT_TRUE(raw_database
                  ->Delete(rocksdb::WriteOptions(), handles[1], deleted_l0_key)
                  .ok());

  rocksdb::cedar_parquet::CedarParquetScanSpec spec;
  spec.projection = {rocksdb::cedar_parquet::CedarParquetColumnId::kPartId,
                     rocksdb::cedar_parquet::CedarParquetColumnId::kFactFamily,
                     rocksdb::cedar_parquet::CedarParquetColumnId::kEntityId,
                     rocksdb::cedar_parquet::CedarParquetColumnId::kValidFrom};
  const auto collect_rows = [&](const rocksdb::ReadOptions& read_options) {
    std::vector<std::array<uint64_t, 4>> rows;
    const rocksdb::Status status = rocksdb::ScanCedarParquetFacts(
        raw_database.get(), handles[1], read_options, spec,
        [&rows](const rocksdb::cedar_parquet::CedarParquetColumnarBatch& batch) {
          const auto& parts = std::get<std::vector<uint32_t>>(batch.columns[0].values);
          const auto& families = std::get<std::vector<uint32_t>>(batch.columns[1].values);
          const auto& entities = std::get<std::vector<uint64_t>>(batch.columns[2].values);
          const auto& valid_from = std::get<std::vector<uint64_t>>(batch.columns[3].values);
          EXPECT_EQ(parts.size(), batch.row_count());
          EXPECT_EQ(families.size(), batch.row_count());
          EXPECT_EQ(entities.size(), batch.row_count());
          EXPECT_EQ(valid_from.size(), batch.row_count());
          for (size_t row = 0; row < batch.row_count(); ++row) {
            EXPECT_EQ(batch.columns[0].present[row], 1);
            EXPECT_EQ(batch.columns[1].present[row], 1);
            EXPECT_EQ(batch.columns[2].present[row], 1);
            EXPECT_EQ(batch.columns[3].present[row], 1);
            rows.push_back({parts[row], families[row], entities[row], valid_from[row]});
          }
          return rocksdb::Status::OK();
        });
    EXPECT_TRUE(status.ok()) << status.ToString();
    return rows;
  };

  const uint64_t vertex_state = static_cast<uint64_t>(FactFamily::kVertexState);
  EXPECT_EQ(collect_rows(before_delete_options),
            (std::vector<std::array<uint64_t, 4>>{
                {7, vertex_state, 8, 20}, {7, vertex_state, 8, 10},
                {7, vertex_state, 9, 30}, {7, vertex_state, 10, 20},
                {7, vertex_state, 11, 20}}));
  EXPECT_EQ(collect_rows(rocksdb::ReadOptions()),
            (std::vector<std::array<uint64_t, 4>>{
                {7, vertex_state, 8, 20}, {7, vertex_state, 8, 10},
                {7, vertex_state, 10, 20}, {7, vertex_state, 11, 20}}));
  raw_database->ReleaseSnapshot(before_delete);

  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    ASSERT_TRUE(raw_database->DestroyColumnFamilyHandle(handle).ok());
  }
}

}  // namespace
}  // namespace cedar
