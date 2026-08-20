// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "rocksdb/cedar_commit.h"
#include "rocksdb/cedar_maintenance.h"
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/write_buffer_manager.h"
#include "rocksdb/write_batch.h"
#include "test_util/sync_point.h"

namespace {

void CountWalDurable(void* context) noexcept {
  static_cast<std::atomic<uint32_t>*>(context)->fetch_add(
      1, std::memory_order_relaxed);
}

class TestWbmStall final : public rocksdb::StallInterface {
 public:
  void Block() override { blocked = true; }
  void Signal() override { signalled = true; }

  bool blocked = false;
  bool signalled = false;
};

class CedarKernelMaintenanceTest : public ::testing::Test {
 protected:
  void SetUp() override {
    path_ = std::filesystem::temp_directory_path() /
            ("cedar_kernel_maintenance_" +
             std::to_string(
                 std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::remove_all(path_);
    options_.create_if_missing = true;
    options_.create_missing_column_families = true;
    options_.write_buffer_size = 64 * 1024;
    options_.max_write_buffer_number = 8;
    options_.level0_file_num_compaction_trigger = 2;
    options_.level0_slowdown_writes_trigger = 20;
    options_.level0_stop_writes_trigger = 24;
    options_.cedar_kernel_mode = true;
    const std::vector<rocksdb::ColumnFamilyDescriptor> descriptors = {
        {rocksdb::kDefaultColumnFamilyName,
         rocksdb::ColumnFamilyOptions(options_)},
        {"facts", rocksdb::ColumnFamilyOptions(options_)},
        {"meta", rocksdb::ColumnFamilyOptions(options_)},
    };
    ASSERT_TRUE(rocksdb::DB::Open(options_, path_.string(), descriptors,
                                  &handles_, &db_)
                    .ok());
  }

  void TearDown() override {
    if (db_ != nullptr) {
      for (auto* handle : handles_) db_->DestroyColumnFamilyHandle(handle);
      ASSERT_TRUE(db_->Close().ok());
    }
    EXPECT_TRUE(rocksdb::DestroyDB(path_.string(), options_).ok());
  }

  rocksdb::CedarMaintenanceSnapshot WriteDebt(
      rocksdb::ColumnFamilyHandle* handle, const std::string& key) {
    rocksdb::WriteOptions write_options;
    write_options.no_slowdown = true;
    for (uint32_t index = 0; index < 8; ++index) {
      EXPECT_TRUE(db_->Put(write_options, handle,
                           key + std::to_string(index),
                           std::string(16 * 1024, 'x'))
                      .ok());
    }
    rocksdb::CedarMaintenanceSnapshot snapshot;
    EXPECT_TRUE(rocksdb::PollCedarMaintenance(db_.get(), &snapshot).ok());
    return snapshot;
  }

  rocksdb::CedarMaintenanceGrant FlushGrant(
      const rocksdb::CedarMaintenanceSnapshot& snapshot) const {
    rocksdb::CedarMaintenanceGrant grant;
    grant.snapshot_generation = snapshot.generation;
    grant.kind = rocksdb::CedarMaintenanceKind::kFlush;
    grant.priority = rocksdb::CedarMaintenancePriority::kEmergency;
    grant.max_input_bytes = 64ULL * 1024ULL * 1024ULL;
    grant.max_output_bytes = 64ULL * 1024ULL * 1024ULL;
    grant.deadline_us = 5ULL * 1000ULL * 1000ULL;
    return grant;
  }

  rocksdb::CedarMaintenanceGrant CompactionGrant(
      const rocksdb::CedarMaintenanceSnapshot& snapshot) const {
    rocksdb::CedarMaintenanceGrant grant;
    grant.snapshot_generation = snapshot.generation;
    grant.kind = rocksdb::CedarMaintenanceKind::kCompaction;
    grant.priority = rocksdb::CedarMaintenancePriority::kNormal;
    grant.max_input_bytes = 64ULL * 1024ULL * 1024ULL;
    grant.max_output_bytes = 64ULL * 1024ULL * 1024ULL;
    grant.deadline_us = 5ULL * 1000ULL * 1000ULL;
    return grant;
  }

  void FlushQueuedDebt(const rocksdb::CedarMaintenanceSnapshot& snapshot) {
    rocksdb::CedarMaintenanceResult result;
    const rocksdb::Status status =
        rocksdb::RunCedarMaintenance(db_.get(), FlushGrant(snapshot), &result);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  rocksdb::CedarMaintenanceSnapshot CreateFactsCompactionDebt() {
    for (uint32_t batch = 0; batch < 3; ++batch) {
      const rocksdb::CedarMaintenanceSnapshot pending =
          WriteDebt(handles_[1], "facts-compaction-" + std::to_string(batch));
      FlushQueuedDebt(pending);
    }
    rocksdb::CedarMaintenanceSnapshot snapshot;
    EXPECT_TRUE(rocksdb::PollCedarMaintenance(db_.get(), &snapshot).ok());
    return snapshot;
  }

  rocksdb::CedarMaintenanceSnapshot CreateOverlappingFactsCompactionDebt() {
    for (uint32_t batch = 0; batch < 3; ++batch) {
      const rocksdb::CedarMaintenanceSnapshot pending =
          WriteDebt(handles_[1], "facts-overlap");
      FlushQueuedDebt(pending);
    }
    rocksdb::CedarMaintenanceSnapshot snapshot;
    EXPECT_TRUE(rocksdb::PollCedarMaintenance(db_.get(), &snapshot).ok());
    return snapshot;
  }

  std::filesystem::path path_;
  rocksdb::Options options_;
  std::unique_ptr<rocksdb::DB> db_;
  std::vector<rocksdb::ColumnFamilyHandle*> handles_;
};

TEST(RocksDbCedarKernelTest, WritesOneDurableEpochThroughNamedSeam) {
  const std::filesystem::path path =
      std::filesystem::temp_directory_path() / "cedar_rocksdb_kernel_test";
  std::filesystem::remove_all(path);
  rocksdb::Options options;
  options.create_if_missing = true;
  std::unique_ptr<rocksdb::DB> db;
  ASSERT_TRUE(rocksdb::DB::Open(options, path.string(), &db).ok());

  rocksdb::WriteBatch batch;
  ASSERT_TRUE(batch.Put("key", "value").ok());
  rocksdb::CedarEpochOptions epoch_options;
  std::atomic<uint32_t> callback_count{0};
  ASSERT_TRUE(rocksdb::WriteCedarEpoch(
                  db.get(), epoch_options, &batch, CountWalDurable,
                  &callback_count)
                  .ok());
  EXPECT_EQ(callback_count.load(std::memory_order_relaxed), 1U);

  std::string value;
  ASSERT_TRUE(db->Get(rocksdb::ReadOptions(), "key", &value).ok());
  EXPECT_EQ(value, "value");
  ASSERT_TRUE(db->Close().ok());
  EXPECT_TRUE(rocksdb::DestroyDB(path.string(), options).ok());
}

TEST(RocksDbCedarKernelTest,
     DbWideSnapshotIncludesFactsMetaAndDefaultDebt) {
  const std::filesystem::path path =
      std::filesystem::temp_directory_path() /
      "cedar_rocksdb_db_wide_maintenance_snapshot";
  std::filesystem::remove_all(path);
  rocksdb::Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  options.write_buffer_size = 4 * 1024;
  options.max_write_buffer_number = 3;
  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors = {
      {rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions(options)},
      {"facts", rocksdb::ColumnFamilyOptions(options)},
      {"meta", rocksdb::ColumnFamilyOptions(options)},
  };
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  std::unique_ptr<rocksdb::DB> db;
  ASSERT_TRUE(rocksdb::DB::Open(options, path.string(), descriptors, &handles,
                                &db)
                  .ok());
  ASSERT_EQ(handles.size(), 3U);

  rocksdb::WriteOptions write_options;
  write_options.no_slowdown = true;
  ASSERT_TRUE(db->Put(write_options, handles[2], "meta-key",
                      std::string(16 * 1024, 'm'))
                  .ok());

  rocksdb::CedarMaintenanceSnapshot snapshot;
  ASSERT_TRUE(rocksdb::PollCedarMaintenance(db.get(), &snapshot).ok());
  EXPECT_EQ(snapshot.column_families.size(), 3U);
  const auto meta = std::find_if(
      snapshot.column_families.begin(), snapshot.column_families.end(),
      [](const rocksdb::CedarColumnFamilyDebt& debt) {
        return debt.role == rocksdb::CedarColumnFamilyRole::kMeta;
      });
  ASSERT_NE(meta, snapshot.column_families.end());
  EXPECT_GT(meta->active_memtable_bytes + meta->immutable_memtable_bytes, 0U);
  EXPECT_GT(snapshot.total_active_memtable_bytes +
                snapshot.total_immutable_memtable_bytes,
            0U);

  for (auto* handle : handles) db->DestroyColumnFamilyHandle(handle);
  ASSERT_TRUE(db->Close().ok());
  EXPECT_TRUE(rocksdb::DestroyDB(path.string(), options).ok());
}

TEST(RocksDbCedarKernelTest, SnapshotReportsWbmWriteStopAndClearsIt) {
  const std::filesystem::path path =
      std::filesystem::temp_directory_path() /
      "cedar_rocksdb_wbm_write_stop_snapshot";
  std::filesystem::remove_all(path);
  rocksdb::Options options;
  options.create_if_missing = true;
  auto wbm = std::make_shared<rocksdb::WriteBufferManager>(1024, nullptr, true);
  options.write_buffer_manager = wbm;
  std::unique_ptr<rocksdb::DB> db;
  ASSERT_TRUE(rocksdb::DB::Open(options, path.string(), &db).ok());

  TestWbmStall stall;
  wbm->ReserveMem(2048);
  wbm->BeginWriteStall(&stall);
  rocksdb::CedarMaintenanceSnapshot stopped;
  ASSERT_TRUE(rocksdb::PollCedarMaintenance(db.get(), &stopped).ok());
  EXPECT_EQ(stopped.write_stopped, 1U);
  EXPECT_EQ(stopped.write_buffer_manager_limit_bytes, 1024U);

  wbm->SetAllowStall(false);
  wbm->MaybeEndWriteStall();
  rocksdb::CedarMaintenanceSnapshot running;
  ASSERT_TRUE(rocksdb::PollCedarMaintenance(db.get(), &running).ok());
  EXPECT_EQ(running.write_stopped, 0U);

  ASSERT_TRUE(db->Close().ok());
  EXPECT_TRUE(rocksdb::DestroyDB(path.string(), options).ok());
}

TEST(RocksDbCedarKernelTest, RealWbmBlockedWriterReportsActiveOnlyWriteStop) {
#if !defined(CEDAR_ROCKSDB_SYNC_POINTS)
  GTEST_SKIP() << "RocksDB SyncPoint processing is unavailable in this build";
#else
  const std::filesystem::path path =
      std::filesystem::temp_directory_path() /
      "cedar_rocksdb_real_wbm_blocked_writer";
  std::filesystem::remove_all(path);
  rocksdb::Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 512 * 1024;
  options.max_write_buffer_number = 8;
  options.level0_file_num_compaction_trigger = 1000;
  options.cedar_kernel_mode = true;
  auto wbm = std::make_shared<rocksdb::WriteBufferManager>(100 * 1024, nullptr, true);
  options.write_buffer_manager = wbm;
  std::unique_ptr<rocksdb::DB> db;
  ASSERT_TRUE(rocksdb::DB::Open(options, path.string(), &db).ok());

  rocksdb::WriteOptions seed_options;
  seed_options.no_slowdown = true;
  ASSERT_TRUE(db->Put(seed_options, "seed-a", std::string(80 * 1024, 'a')).ok());
  ASSERT_TRUE(db->Put(seed_options, "seed-b", std::string(80 * 1024, 'b')).ok());

  std::atomic<bool> entered{false};
  rocksdb::SyncPoint* sync = rocksdb::SyncPoint::GetInstance();
  sync->SetCallBack("WBMStallInterface::BlockDB", [&](void*) {
    // This callback runs while WBMStallInterface holds its state mutex.  It
    // must only acknowledge the blocked state; SetAllowStall() signals that
    // same interface and would self-deadlock here.
    entered.store(true, std::memory_order_release);
  });
  sync->EnableProcessing();

  rocksdb::Status writer_status;
  std::thread writer([&] {
    rocksdb::WriteOptions write_options;
    writer_status = db->Put(write_options, "blocked", "value");
  });

  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(5);
  while (!entered.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  const bool observed_block = entered.load(std::memory_order_acquire);
  rocksdb::CedarMaintenanceSnapshot stopped;
  if (observed_block) {
    ASSERT_TRUE(rocksdb::PollCedarMaintenance(db.get(), &stopped).ok());
  }
  // Release from outside the SyncPoint callback, after observing the real
  // active WBM stall. This mirrors RocksDB's own two-thread test pattern.
  wbm->SetAllowStall(false);
  writer.join();
  sync->ClearAllCallBacks();
  sync->DisableProcessing();

  ASSERT_TRUE(writer_status.ok()) << writer_status.ToString();
  ASSERT_TRUE(observed_block);
  EXPECT_EQ(stopped.write_stopped, 1U);
  rocksdb::CedarMaintenanceSnapshot running;
  ASSERT_TRUE(rocksdb::PollCedarMaintenance(db.get(), &running).ok());
  EXPECT_EQ(running.write_stopped, 0U);

  ASSERT_TRUE(db->Close().ok());
  EXPECT_TRUE(rocksdb::DestroyDB(path.string(), options).ok());
#endif
}

TEST_F(CedarKernelMaintenanceTest,
       KernelGateQueuesFlushDebtWithoutSubmittingAJob) {
  const rocksdb::CedarMaintenanceSnapshot snapshot =
      WriteDebt(handles_[1], "facts-debt");

  EXPECT_GT(snapshot.total_immutable_memtable_bytes, 0U);
  EXPECT_EQ(snapshot.background_flush_calls, 0U);
}

TEST_F(CedarKernelMaintenanceTest, FlushGrantYieldsWhileWalSyncIsCritical) {
  const rocksdb::CedarMaintenanceSnapshot before =
      WriteDebt(handles_[1], "facts-wal-critical");
  std::atomic<bool> wal_sync_critical{true};
  rocksdb::CedarMaintenanceGrant grant = FlushGrant(before);
  grant.wal_sync_critical = &wal_sync_critical;
  rocksdb::CedarMaintenanceResult result;

  const rocksdb::Status status =
      rocksdb::RunCedarMaintenance(db_.get(), grant, &result);
  EXPECT_TRUE(status.IsTryAgain()) << status.ToString();
  EXPECT_EQ(result.yield, rocksdb::CedarMaintenanceYield::kWalSync);

  rocksdb::CedarMaintenanceSnapshot after;
  ASSERT_TRUE(rocksdb::PollCedarMaintenance(db_.get(), &after).ok());
  EXPECT_EQ(after.background_flush_calls, before.background_flush_calls);
}

TEST_F(CedarKernelMaintenanceTest, UnconsumedGrantReportsDeadlineYield) {
  rocksdb::CedarMaintenanceSnapshot snapshot;
  ASSERT_TRUE(rocksdb::PollCedarMaintenance(db_.get(), &snapshot).ok());
  rocksdb::CedarMaintenanceGrant grant = FlushGrant(snapshot);
  grant.deadline_us = 1;
  rocksdb::CedarMaintenanceResult result;

  const rocksdb::Status status =
      rocksdb::RunCedarMaintenance(db_.get(), grant, &result);
  EXPECT_TRUE(status.IsTimedOut()) << status.ToString();
  EXPECT_EQ(result.yield, rocksdb::CedarMaintenanceYield::kDeadline);
}

TEST_F(CedarKernelMaintenanceTest,
       OneFlushGrantSubmitsExactlyOneNativeBackgroundJob) {
  const rocksdb::CedarMaintenanceSnapshot before =
      WriteDebt(handles_[1], "facts-debt");
  rocksdb::CedarMaintenanceResult result;

  const rocksdb::Status status =
      rocksdb::RunCedarMaintenance(db_.get(), FlushGrant(before), &result);
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_GT(result.input_bytes, 0U);

  rocksdb::CedarMaintenanceSnapshot after;
  ASSERT_TRUE(rocksdb::PollCedarMaintenance(db_.get(), &after).ok());
  EXPECT_EQ(after.background_flush_calls, 1U);
}

TEST_F(CedarKernelMaintenanceTest,
       FlushGrantSurvivesActiveMemtableTelemetryChange) {
  rocksdb::WriteOptions write_options;
  write_options.no_slowdown = true;
  for (uint32_t index = 0; index < 5; ++index) {
    ASSERT_TRUE(db_->Put(write_options, handles_[1],
                         "facts-generation-debt-" + std::to_string(index),
                         std::string(16 * 1024, 'x'))
                    .ok());
  }
  rocksdb::CedarMaintenanceSnapshot before;
  ASSERT_TRUE(rocksdb::PollCedarMaintenance(db_.get(), &before).ok());
  ASSERT_GT(before.total_immutable_memtable_bytes, 0U);
  const rocksdb::CedarMaintenanceGrant grant = FlushGrant(before);

  ASSERT_TRUE(db_->Put(write_options, handles_[1], "facts-active-only", "x").ok());

  rocksdb::CedarMaintenanceSnapshot after_active_write;
  ASSERT_TRUE(
      rocksdb::PollCedarMaintenance(db_.get(), &after_active_write).ok());
  ASSERT_GT(after_active_write.total_active_memtable_bytes, 0U);
  EXPECT_EQ(after_active_write.total_immutable_memtable_bytes,
            before.total_immutable_memtable_bytes);
  EXPECT_EQ(after_active_write.total_immutable_memtable_count,
            before.total_immutable_memtable_count);
  EXPECT_EQ(after_active_write.total_l0_files, before.total_l0_files);
  EXPECT_EQ(after_active_write.total_pending_compaction_bytes,
            before.total_pending_compaction_bytes);
  EXPECT_EQ(after_active_write.generation, before.generation);

  rocksdb::CedarMaintenanceResult result;
  const rocksdb::Status status =
      rocksdb::RunCedarMaintenance(db_.get(), grant, &result);
  ASSERT_TRUE(status.ok()) << status.ToString();

  rocksdb::CedarMaintenanceSnapshot after_flush;
  ASSERT_TRUE(rocksdb::PollCedarMaintenance(db_.get(), &after_flush).ok());
  EXPECT_EQ(after_flush.background_flush_calls,
            before.background_flush_calls + 1U);
}

TEST_F(CedarKernelMaintenanceTest,
       DbWideFlushGrantSelectsMetaDebtBeforeUnpressuredFacts) {
  WriteDebt(handles_[1], "facts-debt");
  const rocksdb::CedarMaintenanceSnapshot before =
      WriteDebt(handles_[2], "meta-debt");
  rocksdb::CedarMaintenanceResult result;

  const rocksdb::Status status =
      rocksdb::RunCedarMaintenance(db_.get(), FlushGrant(before), &result);
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(result.selected_column_family_id, handles_[2]->GetID());
}

TEST_F(CedarKernelMaintenanceTest,
       KernelGateQueuesCompactionDebtWithoutSubmittingAJob) {
  const rocksdb::CedarMaintenanceSnapshot snapshot =
      CreateFactsCompactionDebt();

  EXPECT_GE(snapshot.total_l0_files, 2U);
  EXPECT_EQ(snapshot.running_compactions, 0U);
}

TEST_F(CedarKernelMaintenanceTest,
       OneCompactionGrantSubmitsOneNativeBackgroundJob) {
  const rocksdb::CedarMaintenanceSnapshot before =
      CreateFactsCompactionDebt();
  ASSERT_GE(before.total_l0_files, 2U);
  rocksdb::CedarMaintenanceResult result;

  const rocksdb::Status status =
      rocksdb::RunCedarMaintenance(db_.get(), CompactionGrant(before), &result);
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_GT(result.input_bytes, 0U);

  rocksdb::CedarMaintenanceSnapshot after;
  ASSERT_TRUE(rocksdb::PollCedarMaintenance(db_.get(), &after).ok());
  EXPECT_LT(after.total_l0_files, before.total_l0_files);
}

TEST_F(CedarKernelMaintenanceTest,
       CompactionGrantRejectsAnIncompleteOverlapSetBeforeSubmission) {
  const rocksdb::CedarMaintenanceSnapshot before =
      CreateFactsCompactionDebt();
  ASSERT_GE(before.total_l0_files, 2U);
  rocksdb::CedarMaintenanceGrant grant = CompactionGrant(before);
  grant.max_input_bytes = 1;
  rocksdb::CedarMaintenanceResult result;

  const rocksdb::Status status =
      rocksdb::RunCedarMaintenance(db_.get(), grant, &result);
  ASSERT_TRUE(status.ok()) << status.ToString();
  EXPECT_EQ(result.yield, rocksdb::CedarMaintenanceYield::kInputBudget);
  EXPECT_TRUE(result.input_budget_exceeded);
  EXPECT_GT(result.remaining_smallest_complete_unit_bytes,
            grant.max_input_bytes);

  rocksdb::CedarMaintenanceSnapshot after;
  ASSERT_TRUE(rocksdb::PollCedarMaintenance(db_.get(), &after).ok());
  EXPECT_EQ(after.total_l0_files, before.total_l0_files);
  EXPECT_EQ(after.running_compactions, 0U);
}

TEST_F(CedarKernelMaintenanceTest,
       CompactionGrantReportsAtomicOutputOverrunAfterInstall) {
  const rocksdb::CedarMaintenanceSnapshot before =
      CreateOverlappingFactsCompactionDebt();
  ASSERT_GE(before.total_l0_files, 2U);
  rocksdb::CedarMaintenanceGrant grant = CompactionGrant(before);
  grant.max_output_bytes = 1;
  rocksdb::CedarMaintenanceResult result;

  ASSERT_TRUE(rocksdb::RunCedarMaintenance(db_.get(), grant, &result).ok());
  EXPECT_GT(result.output_bytes, grant.max_output_bytes);
  EXPECT_EQ(result.yield, rocksdb::CedarMaintenanceYield::kOutputBudget);
  EXPECT_EQ(result.atomic_overrun_bytes,
            result.output_bytes - grant.max_output_bytes);
}

}  // namespace
