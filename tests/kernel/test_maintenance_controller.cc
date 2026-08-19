// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <mutex>

#include "kernel/maintenance_controller.h"

namespace cedar {
namespace {

class FakeMaintenanceAdapter final : public MaintenanceAdapter {
 public:
  void BlockCompaction() {
    std::lock_guard<std::mutex> lock(mutex_);
    block_compaction_ = true;
  }

  void ReleaseCompaction() {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      block_compaction_ = false;
    }
    completed_.notify_all();
  }

  bool WaitForCompactionStart() {
    std::unique_lock<std::mutex> lock(mutex_);
    return completed_.wait_for(lock, std::chrono::seconds(2), [this] {
      return compaction_started_ != 0;
    });
  }

  bool WaitForFlushCompletion(uint64_t count = 1) {
    std::unique_lock<std::mutex> lock(mutex_);
    return completed_.wait_for(lock, std::chrono::seconds(2), [this, count] {
      return flush_completed_ >= count;
    });
  }

  uint64_t flush_started() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return flush_started_;
  }
  uint64_t compaction_started() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return compaction_started_;
  }
  uint64_t flush_completed() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return flush_completed_;
  }
  uint64_t compaction_completed() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return compaction_completed_;
  }

  StatusOr<CedarMaintenanceCompletion> RunFlush(
      const CedarMaintenanceDecision& decision,
      const std::atomic<bool>*) override {
    std::lock_guard<std::mutex> lock(mutex_);
    ++flush_started_;
    ++flush_completed_;
    completed_.notify_all();
    return CedarMaintenanceCompletion{.kind = decision.kind};
  }

  StatusOr<CedarMaintenanceCompletion> RunCompaction(
      const CedarMaintenanceDecision& decision,
      const std::atomic<bool>*) override {
    std::unique_lock<std::mutex> lock(mutex_);
    ++compaction_started_;
    completed_.notify_all();
    completed_.wait(lock, [this] { return !block_compaction_; });
    ++compaction_completed_;
    completed_.notify_all();
    return CedarMaintenanceCompletion{.kind = decision.kind};
  }

 private:
  mutable std::mutex mutex_;
  std::condition_variable completed_;
  bool block_compaction_ = false;
  uint64_t flush_started_ = 0;
  uint64_t compaction_started_ = 0;
  uint64_t flush_completed_ = 0;
  uint64_t compaction_completed_ = 0;
};

CedarRuntimeSnapshot Snapshot(uint64_t generation, uint64_t immutable,
                              uint64_t l0_files) {
  CedarRuntimeSnapshot snapshot;
  snapshot.generation = generation;
  snapshot.rocksdb.total_immutable_memtable_bytes = immutable;
  snapshot.rocksdb.total_l0_files = l0_files;
  snapshot.rocksdb.column_families = {
      {.role = RocksDbRuntimeMetrics::ColumnFamilyRole::kDefault},
      {.role = RocksDbRuntimeMetrics::ColumnFamilyRole::kFacts,
       .immutable_memtable_bytes = immutable,
       .l0_files = l0_files},
      {.role = RocksDbRuntimeMetrics::ColumnFamilyRole::kMeta},
  };
  return snapshot;
}

TEST(MaintenanceControllerTest, BlockedCompactionDoesNotPreventEmergencyFlush) {
  FakeMaintenanceAdapter adapter;
  adapter.BlockCompaction();
  MaintenanceController controller(&adapter);
  ASSERT_TRUE(controller.Start().ok());
  controller.PublishSnapshot(Snapshot(1, 0, 4));
  ASSERT_TRUE(adapter.WaitForCompactionStart());

  controller.PublishSnapshot(Snapshot(2, 64ULL << 20, 4));
  EXPECT_TRUE(adapter.WaitForFlushCompletion());
  EXPECT_EQ(adapter.compaction_completed(), 0U);

  adapter.ReleaseCompaction();
  controller.Stop();
}

TEST(MaintenanceControllerTest, EachLaneHasAtMostOneOutstandingGrant) {
  FakeMaintenanceAdapter adapter;
  adapter.BlockCompaction();
  MaintenanceController controller(&adapter);
  ASSERT_TRUE(controller.Start().ok());
  controller.PublishSnapshot(Snapshot(1, 64ULL << 20, 4));
  ASSERT_TRUE(adapter.WaitForCompactionStart());
  ASSERT_TRUE(adapter.WaitForFlushCompletion());
  controller.PublishSnapshot(Snapshot(2, 64ULL << 20, 4));
  ASSERT_TRUE(adapter.WaitForFlushCompletion(2));
  EXPECT_EQ(adapter.compaction_started(), 1U);
  EXPECT_EQ(adapter.flush_started(), 2U);

  adapter.ReleaseCompaction();
  controller.Stop();
}

TEST(MaintenanceControllerTest, WalCriticalStatePreventsNormalSubmission) {
  FakeMaintenanceAdapter adapter;
  MaintenanceController controller(&adapter);
  ASSERT_TRUE(controller.Start().ok());
  controller.SetWalSyncCritical(true);
  controller.PublishSnapshot(Snapshot(1, 0, 4));
  std::this_thread::sleep_for(std::chrono::milliseconds(25));
  EXPECT_EQ(adapter.compaction_started(), 0U);
  controller.Stop();
}

}  // namespace
}  // namespace cedar
