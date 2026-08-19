#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "cedar/database.h"
#include "cedar/fact/meta_codec.h"
#include "storage/rocks/rocksdb_config.h"

#include <rocksdb/db.h>
#include <rocksdb/options.h>

namespace cedar {
namespace {

class VacuumTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_vacuum_XXXXXX";
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

  CommitSeq CommitVertex(VertexId vertex_id, ValidTime valid_time) {
    auto begun = database_->BeginTransaction();
    EXPECT_TRUE(begun.ok()) << begun.status().ToString();
    std::unique_ptr<Transaction> transaction =
        std::move(begun).ConsumeValueOrDie();
    EXPECT_TRUE(transaction
                    ->Assert(EntityFact::Vertex(
                                 VertexRef{PartId{0}, vertex_id}),
                             valid_time)
                    .ok());
    const auto committed = transaction->Commit();
    EXPECT_TRUE(committed.ok()) << committed.status().ToString();
    EXPECT_EQ(committed.ValueOrDie().outcome, CommitOutcome::kCommitted)
        << committed.ValueOrDie().status.ToString();
    return committed.ValueOrDie().commit_seq;
  }

  CommitSeq CommitVertexBatch(uint64_t first_vertex_id, uint64_t count,
                              bool retract) {
    auto begun = database_->BeginTransaction();
    EXPECT_TRUE(begun.ok()) << begun.status().ToString();
    std::unique_ptr<Transaction> transaction =
        std::move(begun).ConsumeValueOrDie();
    for (uint64_t offset = 0; offset < count; ++offset) {
      const EntityFact vertex =
          EntityFact::Vertex(VertexRef{PartId{0}, VertexId{first_vertex_id + offset}});
      const Status staged = retract
                                ? transaction->Retract(vertex, ValidTime{10})
                                : transaction->Assert(vertex, ValidTime{10});
      EXPECT_TRUE(staged.ok()) << staged.ToString();
    }
    const auto committed = transaction->Commit();
    EXPECT_TRUE(committed.ok()) << committed.status().ToString();
    EXPECT_EQ(committed.ValueOrDie().outcome, CommitOutcome::kCommitted)
        << committed.ValueOrDie().status.ToString();
    return committed.ValueOrDie().commit_seq;
  }

  void ReopenWithoutFault() {
    database_.reset();
    auto reopened = Database::Open(DatabaseOptions{.path = path_});
    ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
    database_ = std::move(reopened).ConsumeValueOrDie();
  }

  size_t CountCanonicalFactsAfterClose() {
    EXPECT_TRUE(database_->Close().ok());
    database_.reset();
    FactStoreOptions store_options;
    store_options.path = path_;
    rocksdb::Options options = internal::MakeRocksDbOptions(store_options, false);
    options.create_if_missing = false;
    options.create_missing_column_families = false;
    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors =
        internal::MakeRocksDbColumnFamilyDescriptors(store_options, options);
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    std::unique_ptr<rocksdb::DB> raw;
    const rocksdb::Status opened =
        rocksdb::DB::Open(options, path_, descriptors, &handles, &raw);
    EXPECT_TRUE(opened.ok()) << opened.ToString();
    if (!opened.ok()) return 0;
    size_t count = 0;
    {
      std::unique_ptr<rocksdb::Iterator> iterator(
          raw->NewIterator(rocksdb::ReadOptions(), handles[1]));
      for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) ++count;
      EXPECT_TRUE(iterator->status().ok());
    }
    for (rocksdb::ColumnFamilyHandle* handle : handles) {
      raw->DestroyColumnFamilyHandle(handle);
    }
    return count;
  }

  void PersistPreparedVacuumState(CommitSeq target) {
    ASSERT_TRUE(database_->Close().ok());
    database_.reset();
    FactStoreOptions store_options;
    store_options.path = path_;
    rocksdb::Options options = internal::MakeRocksDbOptions(store_options, false);
    options.create_if_missing = false;
    options.create_missing_column_families = false;
    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors =
        internal::MakeRocksDbColumnFamilyDescriptors(store_options, options);
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    std::unique_ptr<rocksdb::DB> raw;
    ASSERT_TRUE(rocksdb::DB::Open(options, path_, descriptors, &handles, &raw).ok());
    const auto state = EncodeVacuumState(
        VacuumState{target, VacuumPhase::kPrepared, {}});
    const auto watermark = EncodeWatermark(target);
    ASSERT_TRUE(state.ok()) << state.status().ToString();
    ASSERT_TRUE(watermark.ok()) << watermark.status().ToString();
    rocksdb::WriteBatch batch;
    batch.Put(handles[2], EncodeVacuumStateKey(), state.ValueOrDie());
    batch.Put(handles[2], EncodeOldestReadableWatermarkKey(),
              watermark.ValueOrDie());
    rocksdb::WriteOptions write_options;
    write_options.sync = true;
    ASSERT_TRUE(raw->Write(write_options, &batch).ok());
    for (rocksdb::ColumnFamilyHandle* handle : handles) {
      raw->DestroyColumnFamilyHandle(handle);
    }
  }

  std::string path_;
  std::unique_ptr<Database> database_;
};

TEST_F(VacuumTest, AdvancesDurableBoundaryAndExpiresOlderSnapshots) {
  EXPECT_EQ(CommitVertex(VertexId{1}, ValidTime{1}), CommitSeq{1});
  EXPECT_EQ(CommitVertex(VertexId{2}, ValidTime{1}), CommitSeq{2});

  ASSERT_TRUE(database_->Vacuum(CommitSeq{1}).ok());
  EXPECT_TRUE(database_->BeginSnapshot(SnapshotOptions{CommitSeq{0}})
                  .status().IsSnapshotExpired());
  const auto retained = database_->BeginSnapshot(SnapshotOptions{CommitSeq{1}});
  ASSERT_TRUE(retained.ok()) << retained.status().ToString();
  EXPECT_EQ(retained.ValueOrDie().oldest_readable_seq(), CommitSeq{1});

  EXPECT_TRUE(database_->Vacuum(CommitSeq{0}).IsInvalidArgument());
  EXPECT_TRUE(database_->Vacuum(CommitSeq{3}).IsInvalidArgument());
}

TEST_F(VacuumTest, RefusesToExpireAnActiveOlderSnapshot) {
  EXPECT_EQ(CommitVertex(VertexId{1}, ValidTime{1}), CommitSeq{1});
  auto old = database_->BeginSnapshot(SnapshotOptions{CommitSeq{0}});
  ASSERT_TRUE(old.ok()) << old.status().ToString();
  EXPECT_EQ(CommitVertex(VertexId{2}, ValidTime{1}), CommitSeq{2});

  EXPECT_TRUE(database_->Vacuum(CommitSeq{1}).IsSnapshotPinned());
  std::optional<Snapshot> release_old(
      std::move(old).ConsumeValueOrDie());
  release_old.reset();
  EXPECT_TRUE(database_->Vacuum(CommitSeq{1}).ok());
}

TEST_F(VacuumTest, TracksDuplicateSnapshotSequencesUntilEveryHandleIsReleased) {
  EXPECT_EQ(CommitVertex(VertexId{1}, ValidTime{1}), CommitSeq{1});
  auto first = database_->BeginSnapshot(SnapshotOptions{CommitSeq{0}});
  auto second = database_->BeginSnapshot(SnapshotOptions{CommitSeq{0}});
  ASSERT_TRUE(first.ok()) << first.status().ToString();
  ASSERT_TRUE(second.ok()) << second.status().ToString();
  std::optional<Snapshot> moved_first(
      std::move(first).ConsumeValueOrDie());
  EXPECT_EQ(CommitVertex(VertexId{2}, ValidTime{1}), CommitSeq{2});

  std::optional<Snapshot> second_handle(
      std::move(second).ConsumeValueOrDie());
  second_handle.reset();
  EXPECT_TRUE(database_->Vacuum(CommitSeq{1}).IsSnapshotPinned());
  moved_first.reset();
  EXPECT_TRUE(database_->Vacuum(CommitSeq{1}).ok());
}

TEST_F(VacuumTest, PreservesBoundaryAcrossReopen) {
  EXPECT_EQ(CommitVertex(VertexId{1}, ValidTime{1}), CommitSeq{1});
  EXPECT_EQ(CommitVertex(VertexId{2}, ValidTime{1}), CommitSeq{2});
  ASSERT_TRUE(database_->Vacuum(CommitSeq{2}).ok());
  ASSERT_TRUE(database_->Close().ok());
  database_.reset();

  auto reopened = Database::Open(DatabaseOptions{.path = path_});
  ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
  database_ = std::move(reopened).ConsumeValueOrDie();
  EXPECT_TRUE(database_->BeginSnapshot(SnapshotOptions{CommitSeq{1}})
                  .status().IsSnapshotExpired());
  EXPECT_TRUE(database_->BeginSnapshot(SnapshotOptions{CommitSeq{2}}).ok());
}

TEST_F(VacuumTest, RetainsOneBaselinePerValidTimeAndAllNewerCorrections) {
  EXPECT_EQ(CommitVertex(VertexId{1}, ValidTime{10}), CommitSeq{1});
  EXPECT_EQ(CommitVertex(VertexId{1}, ValidTime{20}), CommitSeq{2});
  auto retraction = database_->BeginTransaction();
  ASSERT_TRUE(retraction.ok()) << retraction.status().ToString();
  std::unique_ptr<Transaction> retract = std::move(retraction).ConsumeValueOrDie();
  ASSERT_TRUE(retract->Retract(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10}).ok());
  EXPECT_EQ(retract->Commit().ValueOrDie().commit_seq, CommitSeq{3});
  EXPECT_EQ(CommitVertex(VertexId{1}, ValidTime{10}), CommitSeq{4});

  ASSERT_TRUE(database_->Vacuum(CommitSeq{3}).ok());
  EXPECT_EQ(CountCanonicalFactsAfterClose(), 3U);
}

TEST_F(VacuumTest, ReopenResumesPreparedVacuumBeforeAnyNewOperation) {
  EXPECT_EQ(CommitVertex(VertexId{1}, ValidTime{10}), CommitSeq{1});
  auto retraction = database_->BeginTransaction();
  ASSERT_TRUE(retraction.ok()) << retraction.status().ToString();
  std::unique_ptr<Transaction> retract = std::move(retraction).ConsumeValueOrDie();
  ASSERT_TRUE(retract->Retract(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10}).ok());
  EXPECT_EQ(retract->Commit().ValueOrDie().commit_seq, CommitSeq{2});
  EXPECT_EQ(CommitVertex(VertexId{1}, ValidTime{10}), CommitSeq{3});

  PersistPreparedVacuumState(CommitSeq{2});
  auto reopened = Database::Open(DatabaseOptions{.path = path_});
  ASSERT_TRUE(reopened.ok()) << reopened.status().ToString();
  database_ = std::move(reopened).ConsumeValueOrDie();
  EXPECT_EQ(CountCanonicalFactsAfterClose(), 2U);
}

TEST_F(VacuumTest, ReopenAfterBoundaryWriteFaultResumesVacuum) {
  EXPECT_EQ(CommitVertex(VertexId{1}, ValidTime{10}), CommitSeq{1});
  auto retraction = database_->BeginTransaction();
  ASSERT_TRUE(retraction.ok()) << retraction.status().ToString();
  std::unique_ptr<Transaction> retract = std::move(retraction).ConsumeValueOrDie();
  ASSERT_TRUE(retract->Retract(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10}).ok());
  EXPECT_EQ(retract->Commit().ValueOrDie().commit_seq, CommitSeq{2});
  EXPECT_EQ(CommitVertex(VertexId{1}, ValidTime{10}), CommitSeq{3});

  ASSERT_TRUE(database_->Close().ok());
  database_.reset();
  auto faulted = Database::Open(DatabaseOptions{
      .path = path_,
      .vacuum_fault_injector_for_testing = [](VacuumFaultPoint point) {
        if (point == VacuumFaultPoint::kAfterBoundaryWrite) {
          return Status::Indeterminate("test", "crash after vacuum boundary");
        }
        return Status::OK();
      },
  });
  ASSERT_TRUE(faulted.ok()) << faulted.status().ToString();
  std::unique_ptr<Database> faulted_database =
      std::move(faulted).ConsumeValueOrDie();
  EXPECT_TRUE(faulted_database->Vacuum(CommitSeq{2}).IsIndeterminate());
  faulted_database.reset();

  ReopenWithoutFault();
  EXPECT_TRUE(database_->BeginSnapshot(SnapshotOptions{CommitSeq{1}})
                  .status().IsSnapshotExpired());
  EXPECT_EQ(CountCanonicalFactsAfterClose(), 2U);
}

TEST_F(VacuumTest, ReopenAfterPreBoundaryFaultLeavesHistoryUntouched) {
  EXPECT_EQ(CommitVertex(VertexId{1}, ValidTime{10}), CommitSeq{1});
  auto retraction = database_->BeginTransaction();
  ASSERT_TRUE(retraction.ok()) << retraction.status().ToString();
  std::unique_ptr<Transaction> retract = std::move(retraction).ConsumeValueOrDie();
  ASSERT_TRUE(retract->Retract(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10}).ok());
  EXPECT_EQ(retract->Commit().ValueOrDie().commit_seq, CommitSeq{2});
  EXPECT_EQ(CommitVertex(VertexId{1}, ValidTime{10}), CommitSeq{3});

  ASSERT_TRUE(database_->Close().ok());
  database_.reset();
  auto faulted = Database::Open(DatabaseOptions{
      .path = path_,
      .vacuum_fault_injector_for_testing = [](VacuumFaultPoint point) {
        if (point == VacuumFaultPoint::kBeforeBoundaryWrite) {
          return Status::IOError("test", "crash before vacuum boundary");
        }
        return Status::OK();
      },
  });
  ASSERT_TRUE(faulted.ok()) << faulted.status().ToString();
  std::unique_ptr<Database> faulted_database =
      std::move(faulted).ConsumeValueOrDie();
  EXPECT_TRUE(faulted_database->Vacuum(CommitSeq{2}).IsIOError());
  faulted_database.reset();

  ReopenWithoutFault();
  EXPECT_TRUE(database_->BeginSnapshot(SnapshotOptions{CommitSeq{1}}).ok());
  EXPECT_EQ(CountCanonicalFactsAfterClose(), 3U);
}

TEST_F(VacuumTest, ReopenAfterRunningCursorFaultResumesVacuum) {
  constexpr uint64_t kFactCount = 1025;
  EXPECT_EQ(CommitVertexBatch(1, kFactCount, false), CommitSeq{1});
  EXPECT_EQ(CommitVertexBatch(1, kFactCount, true), CommitSeq{2});
  EXPECT_EQ(CommitVertexBatch(1, kFactCount, false), CommitSeq{3});

  ASSERT_TRUE(database_->Close().ok());
  database_.reset();
  bool injected = false;
  auto faulted = Database::Open(DatabaseOptions{
      .path = path_,
      .vacuum_fault_injector_for_testing = [&injected](VacuumFaultPoint point) {
        if (point == VacuumFaultPoint::kAfterCleanupBatch && !injected) {
          injected = true;
          return Status::Indeterminate("test", "crash after vacuum cursor");
        }
        return Status::OK();
      },
  });
  ASSERT_TRUE(faulted.ok()) << faulted.status().ToString();
  std::unique_ptr<Database> faulted_database =
      std::move(faulted).ConsumeValueOrDie();
  EXPECT_TRUE(faulted_database->Vacuum(CommitSeq{2}).IsIndeterminate());
  EXPECT_TRUE(injected);
  faulted_database.reset();

  ReopenWithoutFault();
  EXPECT_TRUE(database_->BeginSnapshot(SnapshotOptions{CommitSeq{1}})
                  .status().IsSnapshotExpired());
  EXPECT_EQ(CountCanonicalFactsAfterClose(), 2U * kFactCount);
}

TEST_F(VacuumTest, ReopenAfterFinalStateRemovalFaultResumesVacuum) {
  EXPECT_EQ(CommitVertex(VertexId{1}, ValidTime{10}), CommitSeq{1});
  auto retraction = database_->BeginTransaction();
  ASSERT_TRUE(retraction.ok()) << retraction.status().ToString();
  std::unique_ptr<Transaction> retract = std::move(retraction).ConsumeValueOrDie();
  ASSERT_TRUE(retract->Retract(EntityFact::Vertex(VertexRef{PartId{0}, VertexId{1}}), ValidTime{10}).ok());
  EXPECT_EQ(retract->Commit().ValueOrDie().commit_seq, CommitSeq{2});
  EXPECT_EQ(CommitVertex(VertexId{1}, ValidTime{10}), CommitSeq{3});

  ASSERT_TRUE(database_->Close().ok());
  database_.reset();
  auto faulted = Database::Open(DatabaseOptions{
      .path = path_,
      .vacuum_fault_injector_for_testing = [](VacuumFaultPoint point) {
        if (point == VacuumFaultPoint::kBeforeCompletion) {
          return Status::Indeterminate("test", "crash before vacuum completion");
        }
        return Status::OK();
      },
  });
  ASSERT_TRUE(faulted.ok()) << faulted.status().ToString();
  std::unique_ptr<Database> faulted_database =
      std::move(faulted).ConsumeValueOrDie();
  EXPECT_TRUE(faulted_database->Vacuum(CommitSeq{2}).IsIndeterminate());
  faulted_database.reset();

  ReopenWithoutFault();
  EXPECT_TRUE(database_->BeginSnapshot(SnapshotOptions{CommitSeq{1}})
                  .status().IsSnapshotExpired());
  EXPECT_EQ(CountCanonicalFactsAfterClose(), 2U);
}

}  // namespace
}  // namespace cedar
