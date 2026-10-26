#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

#include "table/cedar_parquet/cedar_parquet_read_budget.h"

namespace ROCKSDB_NAMESPACE::cedar_parquet {
namespace {

class LimitedQueryMemory final : public CedarParquetQueryMemory {
 public:
  uint64_t used = 0;
  Status Reserve(uint64_t bytes) override {
    if (bytes > 256 - used) return Status::MemoryLimit("query exhausted");
    used += bytes;
    return Status::OK();
  }
  void Release(uint64_t bytes) override { used -= bytes; }
  Status Pin(const void*, uint64_t, std::shared_ptr<const void>*) override {
    return Status::NotSupported();
  }
};

TEST(CedarParquetReadBudgetTest, QueryRejectionRollsBackDbReservationAndGrow) {
  CedarParquetReadBudget pool(1024);
  auto query = std::make_shared<LimitedQueryMemory>();
  std::unique_ptr<CedarParquetMemoryLease> lease;
  EXPECT_TRUE(pool.Acquire(257, &lease, query).IsMemoryLimit());
  EXPECT_EQ(pool.current_bytes(), 0U);
  EXPECT_FALSE(lease);
  lease.reset();
  ASSERT_TRUE(pool.Acquire(128, &lease, query).ok());
  EXPECT_EQ(query->used, 128U);
  EXPECT_TRUE(lease->Grow(129).IsMemoryLimit());
  EXPECT_EQ(pool.current_bytes(), 128U);
  EXPECT_EQ(query->used, 128U);
  ASSERT_TRUE(lease->Grow(128).ok());
  lease->Shrink(64);
  EXPECT_EQ(query->used, 192U);
  lease.reset();
  EXPECT_EQ(query->used, 0U);
  EXPECT_EQ(pool.current_bytes(), 0U);
}

TEST(CedarParquetReadBudgetTest, RejectsBeforeAllocationAndReleases) {
  CedarParquetReadBudget pool(1024);
  std::unique_ptr<CedarParquetMemoryLease> lease;
  ASSERT_TRUE(pool.Acquire(768, &lease).ok());
  EXPECT_EQ(pool.current_bytes(), 768U);
  std::unique_ptr<CedarParquetMemoryLease> rejected;
  EXPECT_TRUE(pool.Acquire(257, &rejected).IsMemoryLimit());
  EXPECT_EQ(rejected, nullptr);
  EXPECT_EQ(pool.current_bytes(), 768U);
  EXPECT_EQ(pool.rejections(), 1U);
  lease.reset();
  EXPECT_EQ(pool.current_bytes(), 0U);
  EXPECT_EQ(pool.peak_bytes(), 768U);
}

TEST(CedarParquetReadBudgetTest, FailedGrowKeepsOriginalLease) {
  CedarParquetReadBudget pool(1024);
  std::unique_ptr<CedarParquetMemoryLease> lease;
  ASSERT_TRUE(pool.Acquire(768, &lease).ok());
  EXPECT_TRUE(lease->Grow(257).IsMemoryLimit());
  EXPECT_EQ(lease->bytes(), 768U);
  ASSERT_TRUE(lease->Grow(256).ok());
  EXPECT_EQ(pool.current_bytes(), 1024U);
  lease->Shrink(512);
  EXPECT_EQ(lease->bytes(), 512U);
  EXPECT_EQ(pool.current_bytes(), 512U);
  EXPECT_TRUE(lease->Grow(UINT64_MAX).IsMemoryLimit());
  EXPECT_EQ(lease->bytes(), 512U);
}

TEST(CedarParquetReadBudgetTest, RejectionNamesTheExhaustedPoolKind) {
  CedarParquetReadBudget working(1024, CedarParquetReadBudgetKind::kWorking);
  CedarParquetReadBudget metadata(1024, CedarParquetReadBudgetKind::kMetadata);
  // The single-argument constructor keeps the page-decode working kind.
  CedarParquetReadBudget default_kind(1024);
  std::unique_ptr<CedarParquetMemoryLease> lease;
  std::unique_ptr<CedarParquetMemoryLease> rejected;

  ASSERT_TRUE(working.Acquire(1024, &lease).ok());
  const Status working_status = working.Acquire(1, &rejected);
  ASSERT_TRUE(working_status.IsMemoryLimit()) << working_status.ToString();
  const std::string working_message = working_status.ToString();
  EXPECT_NE(working_message.find("working"), std::string::npos) << working_message;
  EXPECT_EQ(working_message.find("metadata"), std::string::npos) << working_message;
  lease.reset();

  ASSERT_TRUE(default_kind.Acquire(1024, &lease).ok());
  const Status default_status = default_kind.Acquire(1, &rejected);
  ASSERT_TRUE(default_status.IsMemoryLimit()) << default_status.ToString();
  EXPECT_EQ(default_status.ToString(), working_message);
  lease.reset();

  ASSERT_TRUE(metadata.Acquire(1024, &lease).ok());
  const Status metadata_status = metadata.Acquire(1, &rejected);
  ASSERT_TRUE(metadata_status.IsMemoryLimit()) << metadata_status.ToString();
  const std::string metadata_message = metadata_status.ToString();
  EXPECT_NE(metadata_message.find("metadata"), std::string::npos)
      << metadata_message;
  EXPECT_EQ(metadata_message.find("working"), std::string::npos)
      << metadata_message;
  EXPECT_NE(metadata_message, working_message);

  // A failed Grow reports the same pool that the lease was acquired from.
  const Status grow_status = lease->Grow(1);
  ASSERT_TRUE(grow_status.IsMemoryLimit()) << grow_status.ToString();
  EXPECT_EQ(grow_status.ToString(), metadata_message);
  EXPECT_EQ(lease->bytes(), 1024U);
  lease.reset();

  EXPECT_EQ(working.rejections(), 1U);
  EXPECT_EQ(default_kind.rejections(), 1U);
  EXPECT_EQ(metadata.rejections(), 2U);
  EXPECT_EQ(working.current_bytes(), 0U);
  EXPECT_EQ(metadata.current_bytes(), 0U);
}

TEST(CedarParquetReadBudgetTest, AggregatesConcurrentLeases) {
  CedarParquetReadBudget pool(1024);
  std::atomic<bool> overflow{false};
  std::vector<std::thread> workers;
  for (int index = 0; index < 8; ++index) {
    workers.emplace_back([&] {
      for (int repeat = 0; repeat < 200; ++repeat) {
        std::unique_ptr<CedarParquetMemoryLease> lease;
        const auto status = pool.Acquire(256, &lease);
        if (!status.ok()) {
          if (!status.IsMemoryLimit()) overflow.store(true);
          continue;
        }
        if (pool.current_bytes() > 1024) overflow.store(true);
        std::this_thread::yield();
      }
    });
  }
  for (auto& worker : workers) worker.join();
  EXPECT_FALSE(overflow.load());
  EXPECT_EQ(pool.current_bytes(), 0U);
  EXPECT_LE(pool.peak_bytes(), 1024U);
}

TEST(CedarParquetReadBudgetTest, LeaseOutlivesPoolFacade) {
  auto pool = std::make_unique<CedarParquetReadBudget>(1024);
  std::unique_ptr<CedarParquetMemoryLease> lease;
  ASSERT_TRUE(pool->Acquire(256, &lease).ok());
  pool.reset();
  ASSERT_TRUE(lease->Grow(256).ok());
  EXPECT_EQ(lease->bytes(), 512U);
  lease.reset();
}

}  // namespace
}  // namespace ROCKSDB_NAMESPACE::cedar_parquet
