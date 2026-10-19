#include <gtest/gtest.h>
#include "rocksdb/advanced_cache.h"

#include <algorithm>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>

#include <unistd.h>

#include "storage/facts/fact_store.h"
#include "rocksdb/db.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/table.h"
#include "storage/rocks/rocksdb_config.h"

namespace cedar::internal {
namespace {

class MetaCompressionTest : public testing::TestWithParam<StorageProfile> {
 protected:
  void SetUp() override {
    path_ = (std::filesystem::temp_directory_path() /
             ("cedar-meta-compression-" + std::to_string(getpid()) + "-" +
              std::to_string(static_cast<int>(GetParam())))).string();
    std::filesystem::remove_all(path_);
  }
  void Close() {
    for (auto* handle : handles_) db_->DestroyColumnFamilyHandle(handle);
    handles_.clear();
    db_.reset();
  }
  void TearDown() override {
    Close();
    std::filesystem::remove_all(path_);
  }
  std::string path_;
  std::unique_ptr<rocksdb::DB> db_;
  std::vector<rocksdb::ColumnFamilyHandle*> handles_;
};

// Catches silently inheriting kNoCompression when Snappy is not built, and
// verifies actual compressed files rather than just the requested options.
TEST_P(MetaCompressionTest, CompressesMetaSstAndPreservesValuesAfterReopen) {
  FactStoreOptions store;
  store.storage_profile = GetParam();
  store.production.memory_budget_bytes = 1ULL << 30;
  const auto resolved = ResolveStorageProfile(store);
  ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
  auto options = MakeRocksDbOptions(store, true, &resolved.ValueOrDie());
  // Raw offline fixture: no live kernel grant controller is attached.
  options.cedar_kernel_mode = false;
  options.atomic_flush = false;
  auto descriptors = MakeRocksDbColumnFamilyDescriptors(
      store, options, &resolved.ValueOrDie());
  descriptors[2].options.disable_auto_compactions = true;
  descriptors[2].options.write_buffer_size = 4ULL << 20;
  ASSERT_TRUE(rocksdb::DB::Open(options, path_, descriptors, &handles_, &db_).ok());
  const std::string value(2048, 'm');
  for (int i = 0; i < 256; ++i) {
    ASSERT_TRUE(db_->Put(rocksdb::WriteOptions(), handles_[2],
                        "meta-key-" + std::to_string(i), value).ok());
  }
  rocksdb::FlushOptions flush;
  flush.wait = true;
  ASSERT_TRUE(db_->Flush(flush, handles_[2]).ok());
  auto expect_compressed = [&](const std::string& codec_hex) {
    rocksdb::TablePropertiesCollection tables;
    ASSERT_TRUE(db_->GetPropertiesOfAllTables(handles_[2], &tables).ok());
    ASSERT_FALSE(tables.empty());
    uint64_t entries = 0;
    for (const auto& [file, props] : tables) {
      // RocksDB format >= 7 reports actual types as hex: 04=LZ4, 07=Zstd.
      EXPECT_NE(props->compression_name.find(";" + codec_hex + ";"),
                std::string::npos) << props->compression_name << " " << file;
      EXPECT_LT(props->data_size, props->raw_value_size / 4) << file;
      entries += props->num_entries;
    }
    EXPECT_EQ(entries, 256U);
  };
  expect_compressed("04");
  rocksdb::CompactRangeOptions compact;
  compact.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForce;
  ASSERT_TRUE(db_->CompactRange(compact, handles_[2], nullptr, nullptr).ok());
  expect_compressed("07");
  Close();
  options.create_if_missing = false;
  ASSERT_TRUE(rocksdb::DB::Open(options, path_, descriptors, &handles_, &db_).ok());
  for (int i = 0; i < 256; ++i) {
    std::string found;
    ASSERT_TRUE(db_->Get(rocksdb::ReadOptions(), handles_[2],
                        "meta-key-" + std::to_string(i), &found).ok());
    EXPECT_EQ(found, value);
  }
}

INSTANTIATE_TEST_SUITE_P(AllProfiles, MetaCompressionTest,
    testing::Values(StorageProfile::kDeveloper, StorageProfile::kProductionAppend,
                    StorageProfile::kKernelTest, StorageProfile::kDebugSmallThresholds));

TEST(RocksDbProfileTest, RejectsProductionBudgetBelowOneGiB) {
  FactStoreOptions options;
  options.storage_profile = StorageProfile::kProductionAppend;
  options.production.memory_budget_bytes = 512ULL * 1024ULL * 1024ULL;
  const auto resolved = ResolveStorageProfile(options);
  ASSERT_FALSE(resolved.ok());
  EXPECT_TRUE(resolved.status().IsInvalidArgument())
      << resolved.status().ToString();
}

TEST(RocksDbProfileTest, AcceptsProductionBudgetAtFortyFivePercentOfHostMemory) {
  const long pages = sysconf(_SC_PHYS_PAGES);
  const long page_size = sysconf(_SC_PAGESIZE);
  ASSERT_GT(pages, 0);
  ASSERT_GT(page_size, 0);
  const uint64_t host_memory =
      static_cast<uint64_t>(pages) * static_cast<uint64_t>(page_size);

  FactStoreOptions options;
  options.storage_profile = StorageProfile::kProductionAppend;
  options.production.memory_budget_bytes = host_memory * 45 / 100;

  const auto resolved = ResolveStorageProfile(options);
  ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
  EXPECT_EQ(resolved.ValueOrDie().memory_budget_bytes,
            options.production.memory_budget_bytes);
}

TEST(RocksDbProfileTest, DefaultsProductionBudgetToFiftyPercentOfHostMemory) {
  const long pages = sysconf(_SC_PHYS_PAGES);
  const long page_size = sysconf(_SC_PAGESIZE);
  ASSERT_GT(pages, 0);
  ASSERT_GT(page_size, 0);
  const uint64_t host_memory =
      static_cast<uint64_t>(pages) * static_cast<uint64_t>(page_size);

  FactStoreOptions options;
  options.storage_profile = StorageProfile::kProductionAppend;

  const auto resolved = ResolveStorageProfile(options);
  ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
  EXPECT_EQ(resolved.ValueOrDie().memory_budget_bytes,
            host_memory * 50 / 100);
}

TEST(RocksDbProfileTest, RejectsProductionBudgetAboveFiftyPercentOfHostMemory) {
  const long pages = sysconf(_SC_PHYS_PAGES);
  const long page_size = sysconf(_SC_PAGESIZE);
  ASSERT_GT(pages, 0);
  ASSERT_GT(page_size, 0);
  const uint64_t host_memory =
      static_cast<uint64_t>(pages) * static_cast<uint64_t>(page_size);

  FactStoreOptions options;
  options.storage_profile = StorageProfile::kProductionAppend;
  options.production.memory_budget_bytes = host_memory * 51 / 100;

  const auto resolved = ResolveStorageProfile(options);
  ASSERT_FALSE(resolved.ok());
  EXPECT_TRUE(resolved.status().IsInvalidArgument())
      << resolved.status().ToString();
}

TEST(RocksDbProfileTest, ExposesExistingCacheWithoutAllocatingSecondBudget) {
  FactStoreOptions store;
  store.storage_profile=StorageProfile::kProductionAppend;
  store.production.memory_budget_bytes=4ULL<<30;
  const auto resolved=ResolveStorageProfile(store);
  ASSERT_TRUE(resolved.ok());
  std::shared_ptr<rocksdb::Cache> cache;
  const auto options=MakeRocksDbOptions(store,true,&resolved.ValueOrDie(),&cache);
  ASSERT_NE(cache,nullptr);
  EXPECT_EQ(cache->GetCapacity(),resolved.ValueOrDie().block_cache_bytes);
  const auto* block=options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
  ASSERT_NE(block,nullptr);
  EXPECT_EQ(block->block_cache.get(),cache.get());
  auto page_metrics=std::make_shared<rocksdb::cedar_parquet::CanonicalPageCacheMetrics>();
  const auto descriptors=MakeRocksDbColumnFamilyDescriptors(
      store,options,&resolved.ValueOrDie(),{cache,page_metrics});
  const auto* meta=descriptors[2].options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
  ASSERT_NE(meta,nullptr);
  EXPECT_EQ(meta->block_cache.get(),cache.get());
  EXPECT_EQ(cache->GetCapacity(),resolved.ValueOrDie().block_cache_bytes);
}

TEST(RocksDbProfileTest, KernelTestProfileAcceleratesMaintenanceLifecycle) {
  FactStoreOptions options;
  options.storage_profile = StorageProfile::kKernelTest;
  const auto resolved = ResolveStorageProfile(options);
  ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
  EXPECT_EQ(resolved.ValueOrDie().memory_budget_bytes, 1ULL * 1024ULL * 1024ULL);
  EXPECT_EQ(resolved.ValueOrDie().block_cache_bytes, 256ULL * 1024ULL);
  EXPECT_EQ(resolved.ValueOrDie().facts_write_buffer_bytes,
            32ULL * 1024ULL);
  EXPECT_EQ(resolved.ValueOrDie().meta_write_buffer_bytes, 8ULL * 1024ULL);
  EXPECT_EQ(resolved.ValueOrDie().default_write_buffer_bytes, 8ULL * 1024ULL);
  EXPECT_EQ(resolved.ValueOrDie().max_background_jobs, 2U);

  const rocksdb::Options db_options = MakeRocksDbOptions(
      options, true, &resolved.ValueOrDie());
  EXPECT_TRUE(db_options.cedar_kernel_mode);
  EXPECT_TRUE(db_options.cedar_disable_periodic_tasks);
  EXPECT_EQ(db_options.max_background_flushes, 1);
  EXPECT_EQ(db_options.max_background_compactions, 2);
  ASSERT_NE(db_options.write_buffer_manager, nullptr);
  // Small enough to force frequent 32 KiB facts flushes, while retaining
  // enough Cedar-owned backlog for the emergency flush worker to drain.
  EXPECT_EQ(db_options.write_buffer_manager->buffer_size(), 1ULL * 1024ULL * 1024ULL);

  const auto descriptors = MakeRocksDbColumnFamilyDescriptors(
      options, db_options, &resolved.ValueOrDie());
  ASSERT_EQ(descriptors.size(), 3U);
  EXPECT_EQ(descriptors[1].options.write_buffer_size, 32ULL * 1024ULL);
  EXPECT_EQ(descriptors[1].options.max_write_buffer_number, 6);
  EXPECT_EQ(descriptors[1].options.min_write_buffer_number_to_merge, 1);
  EXPECT_EQ(descriptors[1].options.level0_file_num_compaction_trigger, 2);
  EXPECT_EQ(descriptors[1].options.level0_slowdown_writes_trigger, 12);
  EXPECT_EQ(descriptors[1].options.level0_stop_writes_trigger, 24);
  EXPECT_EQ(descriptors[2].options.write_buffer_size, 8ULL * 1024ULL);
  EXPECT_EQ(descriptors[2].options.max_write_buffer_number, 6);
  EXPECT_EQ(descriptors[2].options.min_write_buffer_number_to_merge, 1);
  EXPECT_EQ(descriptors[2].options.level0_file_num_compaction_trigger, 2);
  EXPECT_EQ(descriptors[2].options.level0_slowdown_writes_trigger, 12);
  EXPECT_EQ(descriptors[2].options.level0_stop_writes_trigger, 24);
}

TEST(RocksDbProfileTest, RejectsPipelineLimitsAboveProductionProfileCap) {
  FactStoreOptions options;
  options.storage_profile = StorageProfile::kProductionAppend;
  options.production.memory_budget_bytes = 1ULL * 1024ULL * 1024ULL * 1024ULL;
  options.production.max_commit_batch_count = 64;
  options.group_commit_max_batch_size = 65;
  const auto resolved = ResolveStorageProfile(options);
  ASSERT_FALSE(resolved.ok());
  EXPECT_TRUE(resolved.status().IsInvalidArgument())
      << resolved.status().ToString();
}

TEST(RocksDbProfileTest, ResolvesProductionBudgetSplitAndBaseline) {
  FactStoreOptions options;
  options.storage_profile = StorageProfile::kProductionAppend;
  options.production.memory_budget_bytes = 4ULL * 1024ULL * 1024ULL * 1024ULL;
  options.production.max_background_jobs = 6;
  const auto resolved = ResolveStorageProfile(options);
  ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
  EXPECT_EQ(resolved.ValueOrDie().block_cache_bytes,
            4ULL * 1024ULL * 1024ULL * 1024ULL * 55 / 100);
  EXPECT_EQ(resolved.ValueOrDie().facts_write_buffer_bytes, 1024ULL * 1024ULL * 1024ULL);
  EXPECT_EQ(resolved.ValueOrDie().meta_write_buffer_bytes,
            4ULL * 1024ULL * 1024ULL * 1024ULL * 5 / 100);
  EXPECT_EQ(resolved.ValueOrDie().max_background_jobs, 6U);

  const rocksdb::Options db_options = MakeRocksDbOptions(
      options, true, &resolved.ValueOrDie());
  EXPECT_FALSE(db_options.atomic_flush);
  EXPECT_TRUE(db_options.paranoid_checks);
  EXPECT_TRUE(db_options.track_and_verify_wals_in_manifest);
  EXPECT_TRUE(db_options.allow_concurrent_memtable_write);
  EXPECT_TRUE(db_options.enable_write_thread_adaptive_yield);
  EXPECT_FALSE(db_options.enable_pipelined_write);
  EXPECT_FALSE(db_options.unordered_write);
  EXPECT_FALSE(db_options.two_write_queues);
  EXPECT_EQ(db_options.max_write_batch_group_size_bytes, 2ULL * 1024ULL * 1024ULL);
  EXPECT_EQ(db_options.max_background_jobs, 6);
  EXPECT_EQ(db_options.max_background_flushes, 1);
  EXPECT_EQ(db_options.max_background_compactions, 2);
  EXPECT_EQ(db_options.max_subcompactions, 2U);
  EXPECT_EQ(db_options.bytes_per_sync, 1ULL << 20);
  EXPECT_EQ(db_options.wal_bytes_per_sync, 0U);
}

TEST(RocksDbProfileTest, AppliesExplicitProductionCapacityToEveryColumnFamily) {
  FactStoreOptions options;
  options.storage_profile = StorageProfile::kProductionAppend;
  // The production profile rejects budgets above 50% of host memory. Keep the
  // requested 12 GiB on large hosts but stay within the host limit on smaller
  // development machines so this capacity mapping test is host-independent.
  const uint64_t host_memory =
      static_cast<uint64_t>(::sysconf(_SC_PHYS_PAGES)) *
      static_cast<uint64_t>(::sysconf(_SC_PAGE_SIZE));
  ASSERT_GT(host_memory, 2ULL << 30);
  options.production.memory_budget_bytes = std::min<uint64_t>(
      12ULL << 30, host_memory / 2 - (256ULL << 20));
  ASSERT_GE(options.production.memory_budget_bytes, 1ULL << 30);
  // Scale the explicit capacities to the resolved budget so the profile's
  // derived allocations (25% facts, 5% meta+default) still hold on any host.
  constexpr uint64_t kMiB = 1ULL << 20;
  const uint64_t budget = options.production.memory_budget_bytes;
  options.production.facts_memtable_bytes = (budget / 16) / kMiB * kMiB;
  options.production.meta_memtable_bytes = (budget / 100) / kMiB * kMiB;
  options.production.default_memtable_bytes = (budget / 200) / kMiB * kMiB;
  options.production.facts_memtable_count = 4;
  options.production.meta_memtable_count = 3;
  options.production.default_memtable_count = 3;
  options.production.flush_worker_capacity = 2;
  options.production.compaction_worker_capacity = 4;

  const auto resolved = ResolveStorageProfile(options);
  ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
  EXPECT_EQ(resolved.ValueOrDie().facts_memtable_bytes,
            options.production.facts_memtable_bytes);
  EXPECT_EQ(resolved.ValueOrDie().meta_memtable_bytes,
            options.production.meta_memtable_bytes);
  EXPECT_EQ(resolved.ValueOrDie().default_memtable_bytes,
            options.production.default_memtable_bytes);
  EXPECT_EQ(resolved.ValueOrDie().facts_write_buffer_count, 4U);
  EXPECT_EQ(resolved.ValueOrDie().meta_write_buffer_count, 3U);
  EXPECT_EQ(resolved.ValueOrDie().default_write_buffer_count, 3U);
  EXPECT_EQ(resolved.ValueOrDie().flush_worker_capacity, 2U);
  EXPECT_EQ(resolved.ValueOrDie().compaction_worker_capacity, 4U);
  EXPECT_EQ(resolved.ValueOrDie().max_background_jobs, 6U);

  const rocksdb::Options db_options =
      MakeRocksDbOptions(options, true, &resolved.ValueOrDie());
  EXPECT_EQ(db_options.max_background_jobs, 6);
  EXPECT_EQ(db_options.max_background_flushes, 2);
  EXPECT_EQ(db_options.max_background_compactions, 4);

  const auto descriptors = MakeRocksDbColumnFamilyDescriptors(
      options, db_options, &resolved.ValueOrDie());
  ASSERT_EQ(descriptors.size(), 3U);
  EXPECT_EQ(descriptors[0].options.write_buffer_size,
            options.production.default_memtable_bytes);
  EXPECT_EQ(descriptors[0].options.max_write_buffer_number, 3);
  EXPECT_EQ(descriptors[0].options.level0_file_num_compaction_trigger, 8);
  EXPECT_EQ(descriptors[0].options.level0_slowdown_writes_trigger, 24);
  EXPECT_EQ(descriptors[0].options.level0_stop_writes_trigger, 36);
  EXPECT_EQ(descriptors[1].options.write_buffer_size,
            options.production.facts_memtable_bytes);
  EXPECT_EQ(descriptors[1].options.max_write_buffer_number, 4);
  EXPECT_EQ(descriptors[1].options.min_write_buffer_number_to_merge, 2);
  EXPECT_EQ(descriptors[1].options.target_file_size_base,
            options.production.facts_memtable_bytes);
  EXPECT_EQ(descriptors[1].options.level0_file_num_compaction_trigger, 8);
  EXPECT_EQ(descriptors[1].options.level0_slowdown_writes_trigger, 24);
  EXPECT_EQ(descriptors[1].options.level0_stop_writes_trigger, 36);
  EXPECT_EQ(descriptors[2].options.write_buffer_size,
            options.production.meta_memtable_bytes);
  EXPECT_EQ(descriptors[2].options.max_write_buffer_number, 3);
  EXPECT_EQ(descriptors[2].options.level0_file_num_compaction_trigger, 8);
  EXPECT_EQ(descriptors[2].options.level0_slowdown_writes_trigger, 24);
  EXPECT_EQ(descriptors[2].options.level0_stop_writes_trigger, 36);
}

TEST(RocksDbProfileTest, RejectsProductionCapacityThatExceedsItsBudget) {
  FactStoreOptions options;
  options.storage_profile = StorageProfile::kProductionAppend;
  options.production.memory_budget_bytes = 4ULL << 30;
  options.production.facts_memtable_bytes = 1ULL << 30;
  options.production.facts_memtable_count = 4;
  options.production.meta_memtable_bytes = 128ULL << 20;
  options.production.meta_memtable_count = 3;

  const auto resolved = ResolveStorageProfile(options);
  ASSERT_FALSE(resolved.ok());
  EXPECT_TRUE(resolved.status().IsInvalidArgument())
      << resolved.status().ToString();
}

TEST(RocksDbProfileTest, RejectsCombinedMetaAndDefaultCapacityAboveBudget) {
  FactStoreOptions options;
  options.storage_profile = StorageProfile::kProductionAppend;
  options.production.memory_budget_bytes = 4ULL << 30;
  options.production.meta_memtable_bytes = 64ULL << 20;
  options.production.meta_memtable_count = 2;
  options.production.default_memtable_bytes = 64ULL << 20;
  options.production.default_memtable_count = 2;

  const auto resolved = ResolveStorageProfile(options);
  ASSERT_FALSE(resolved.ok());
  EXPECT_TRUE(resolved.status().IsInvalidArgument())
      << resolved.status().ToString();
}

TEST(RocksDbProfileTest, RejectsBackgroundWorkerCapacityOverflow) {
  FactStoreOptions options;
  options.storage_profile = StorageProfile::kProductionAppend;
  options.production.memory_budget_bytes = 4ULL << 30;
  options.production.flush_worker_capacity =
      std::numeric_limits<uint32_t>::max();
  options.production.compaction_worker_capacity = 2;

  const auto resolved = ResolveStorageProfile(options);
  ASSERT_FALSE(resolved.ok());
  EXPECT_TRUE(resolved.status().IsInvalidArgument())
      << resolved.status().ToString();
}

TEST(RocksDbProfileTest, RejectsMemtableCountAboveRocksDbLimit) {
  FactStoreOptions options;
  options.storage_profile = StorageProfile::kProductionAppend;
  options.production.facts_memtable_bytes = 1;
  options.production.facts_memtable_count =
      std::numeric_limits<uint32_t>::max();

  const auto resolved = ResolveStorageProfile(options);
  ASSERT_FALSE(resolved.ok());
  EXPECT_TRUE(resolved.status().IsInvalidArgument())
      << resolved.status().ToString();
}

TEST(RocksDbProfileTest, RejectsFactsMemtableCountBelowFlushMergeWidth) {
  FactStoreOptions options;
  options.storage_profile = StorageProfile::kProductionAppend;
  options.production.facts_memtable_count = 1;

  const auto resolved = ResolveStorageProfile(options);
  ASSERT_FALSE(resolved.ok());
  EXPECT_TRUE(resolved.status().IsInvalidArgument())
      << resolved.status().ToString();
}

TEST(RocksDbProfileTest, RejectsBackgroundJobCountAboveRocksDbLimit) {
  FactStoreOptions options;
  options.storage_profile = StorageProfile::kProductionAppend;
  options.production.max_background_jobs =
      std::numeric_limits<uint32_t>::max();

  const auto resolved = ResolveStorageProfile(options);
  ASSERT_FALSE(resolved.ok());
  EXPECT_TRUE(resolved.status().IsInvalidArgument())
      << resolved.status().ToString();
}

TEST(RocksDbProfileTest, ProductionProfileUsesCedarWalDefaults) {
  FactStoreOptions options;
  options.storage_profile = StorageProfile::kProductionAppend;
  options.production.memory_budget_bytes = 1ULL * 1024ULL * 1024ULL * 1024ULL;
  const auto resolved = ResolveStorageProfile(options);
  ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
  EXPECT_EQ(resolved.ValueOrDie().max_background_jobs, 2U);

  const rocksdb::Options db_options = MakeRocksDbOptions(
      options, true, &resolved.ValueOrDie());
  EXPECT_FALSE(db_options.manual_wal_flush);
  EXPECT_FALSE(db_options.use_fsync);
  EXPECT_EQ(db_options.wal_bytes_per_sync, 0U);
  EXPECT_EQ(db_options.recycle_log_file_num, 0U);
  EXPECT_EQ(db_options.stats_dump_period_sec, 0U);
  EXPECT_EQ(db_options.stats_persist_period_sec, 0U);
  EXPECT_EQ(db_options.statistics, nullptr);
  EXPECT_TRUE(db_options.cedar_disable_periodic_tasks);
}

TEST(RocksDbProfileTest, PassesExplicitWalDirectoryToRocksDb) {
  FactStoreOptions options;
  options.storage_profile = StorageProfile::kProductionAppend;
  options.production.memory_budget_bytes = 1ULL * 1024ULL * 1024ULL * 1024ULL;
  options.production.wal_directory = "/volumes/cedar-wal";
  const auto resolved = ResolveStorageProfile(options);
  ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();

  const rocksdb::Options db_options = MakeRocksDbOptions(
      options, true, &resolved.ValueOrDie());
  EXPECT_EQ(db_options.wal_dir, "/volumes/cedar-wal");
}

TEST(RocksDbProfileTest, RequiresAnExplicitWalDirectoryForSeparateDevice) {
  FactStoreOptions options;
  options.storage_profile = StorageProfile::kProductionAppend;
  options.path = "/var/lib/cedar/data";
  options.production.require_separate_wal_device = true;

  const Status status = ValidateProductionWalPlacement(options);
  EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
}

TEST(RocksDbProfileTest, RejectsSameDeviceForRequiredWalIsolation) {
  char root_template[] = "/tmp/cedar_wal_placement_XXXXXX";
  ASSERT_NE(mkdtemp(root_template), nullptr);
  const std::filesystem::path root(root_template);
  const std::filesystem::path data = root / "data";
  const std::filesystem::path wal = root / "wal";
  ASSERT_TRUE(std::filesystem::create_directory(data));
  ASSERT_TRUE(std::filesystem::create_directory(wal));

  FactStoreOptions options;
  options.storage_profile = StorageProfile::kProductionAppend;
  options.path = data.string();
  options.production.wal_directory = wal.string();
  options.production.require_separate_wal_device = true;
  const Status status = ValidateProductionWalPlacement(options);
  EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();

  std::filesystem::remove_all(root);
}

TEST(RocksDbProfileTest, DiagnosticProfileExplicitlyEnablesStatistics) {
  FactStoreOptions options;
  options.storage_profile = StorageProfile::kProductionAppend;
  options.production.memory_budget_bytes = 1ULL * 1024ULL * 1024ULL * 1024ULL;
  options.production.diagnostic_periodic_tasks = true;
  const auto resolved = ResolveStorageProfile(options);
  ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();

  const rocksdb::Options db_options = MakeRocksDbOptions(
      options, true, &resolved.ValueOrDie());
  EXPECT_NE(db_options.statistics, nullptr);
  EXPECT_GT(db_options.stats_dump_period_sec, 0U);
  EXPECT_GT(db_options.stats_persist_period_sec, 0U);
  EXPECT_FALSE(db_options.cedar_disable_periodic_tasks);
}

TEST(RocksDbProfileTest, RejectsUnqualifiedWalRecyclingCounts) {
  FactStoreOptions options;
  options.storage_profile = StorageProfile::kProductionAppend;
  options.production.memory_budget_bytes = 1ULL * 1024ULL * 1024ULL * 1024ULL;
  for (const uint32_t count : {1U, 5U}) {
    options.production.recycle_log_file_num = count;
    const auto resolved = ResolveStorageProfile(options);
    ASSERT_FALSE(resolved.ok());
    EXPECT_TRUE(resolved.status().IsInvalidArgument())
        << resolved.status().ToString();
  }
}

TEST(RocksDbProfileTest, AcceptsQualifiedWalRecyclingCounts) {
  FactStoreOptions options;
  options.storage_profile = StorageProfile::kProductionAppend;
  options.production.memory_budget_bytes = 1ULL * 1024ULL * 1024ULL * 1024ULL;
  for (const uint32_t count : {2U, 3U, 4U}) {
    options.production.recycle_log_file_num = count;
    const auto resolved = ResolveStorageProfile(options);
    ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
    const rocksdb::Options db_options = MakeRocksDbOptions(
        options, true, &resolved.ValueOrDie());
    EXPECT_EQ(db_options.recycle_log_file_num, count);
  }
}

TEST(RocksDbProfileTest, BuildsDistinctFactsAndMetaColumnFamilyPolicies) {
  FactStoreOptions options;
  options.storage_profile = StorageProfile::kProductionAppend;
  options.production.memory_budget_bytes = 1ULL * 1024ULL * 1024ULL * 1024ULL;
  const auto resolved = ResolveStorageProfile(options);
  ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
  const auto db_options = MakeRocksDbOptions(options, true, &resolved.ValueOrDie());
  const auto descriptors = MakeRocksDbColumnFamilyDescriptors(
      options, db_options, &resolved.ValueOrDie());
  ASSERT_EQ(descriptors.size(), 3U);
  ASSERT_NE(descriptors[1].options.table_factory, nullptr);
  ASSERT_NE(descriptors[2].options.table_factory, nullptr);
  EXPECT_STREQ(descriptors[1].options.table_factory->Name(),
               "CedarParquetFactTable");
  ASSERT_NE(descriptors[1].options.memtable_factory, nullptr);
  EXPECT_STREQ(descriptors[1].options.memtable_factory->Name(),
               "PartitionedVersionRadixFactory");
  ASSERT_NE(descriptors[2].options.memtable_factory, nullptr);
  EXPECT_STRNE(descriptors[2].options.memtable_factory->Name(),
               "PartitionedVersionRadixFactory");
  EXPECT_STREQ(descriptors[2].options.table_factory->Name(),
               rocksdb::TableFactory::kBlockBasedTableName());
  EXPECT_EQ(descriptors[1].options.write_buffer_size, 128ULL * 1024ULL * 1024ULL);
  EXPECT_EQ(descriptors[1].options.max_write_buffer_number, 4);
  EXPECT_EQ(descriptors[1].options.min_write_buffer_number_to_merge, 2);
  EXPECT_EQ(descriptors[1].options.level0_file_num_compaction_trigger, 8);
  EXPECT_EQ(descriptors[1].options.level0_slowdown_writes_trigger, 24);
  EXPECT_EQ(descriptors[1].options.level0_stop_writes_trigger, 36);
  EXPECT_EQ(descriptors[1].options.soft_pending_compaction_bytes_limit,
            8ULL * 1024ULL * 1024ULL * 1024ULL);
  EXPECT_EQ(descriptors[1].options.hard_pending_compaction_bytes_limit,
            32ULL * 1024ULL * 1024ULL * 1024ULL);
  EXPECT_EQ(descriptors[1].options.compression, rocksdb::kNoCompression);
  EXPECT_EQ(descriptors[1].options.bottommost_compression,
            rocksdb::kNoCompression);
  EXPECT_NE(descriptors[1].options.table_factory->GetPrintableOptions().find(
                "page_compression=LZ4_RAW"),
            std::string::npos);
  EXPECT_EQ(descriptors[1].options.prefix_extractor, nullptr);
  EXPECT_FALSE(descriptors[1].options.enable_blob_files);
  EXPECT_EQ(descriptors[2].options.write_buffer_size, 32ULL * 1024ULL * 1024ULL);
  EXPECT_EQ(descriptors[2].options.max_write_buffer_number, 3);
  EXPECT_EQ(descriptors[2].options.level0_file_num_compaction_trigger, 4);
  EXPECT_EQ(descriptors[2].options.level0_slowdown_writes_trigger, 12);
  EXPECT_EQ(descriptors[2].options.level0_stop_writes_trigger, 20);
  EXPECT_FALSE(descriptors[2].options.enable_blob_files);
}

TEST(RocksDbProfileTest, UsesCedarParquetFactsWithoutAnOptInFallback) {
  FactStoreOptions options;
  options.storage_profile = StorageProfile::kProductionAppend;
  options.production.memory_budget_bytes = 1ULL * 1024ULL * 1024ULL * 1024ULL;
  const auto resolved = ResolveStorageProfile(options);
  ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
  const auto db_options = MakeRocksDbOptions(options, true, &resolved.ValueOrDie());
  const auto descriptors = MakeRocksDbColumnFamilyDescriptors(
      options, db_options, &resolved.ValueOrDie());
  ASSERT_EQ(descriptors.size(), 3U);
  EXPECT_STREQ(descriptors[1].options.table_factory->Name(),
               "CedarParquetFactTable");
  EXPECT_EQ(descriptors[1].options.prefix_extractor, nullptr);
  EXPECT_FALSE(descriptors[1].options.enable_blob_files);
}

TEST(RocksDbProfileTest, InstallsRequestedBackgroundCompactionRateLimit) {
  FactStoreOptions options;
  options.storage_profile = StorageProfile::kProductionAppend;
  options.production.memory_budget_bytes = 1ULL * 1024ULL * 1024ULL * 1024ULL;
  options.production.compaction_rate_limit_bytes_per_sec = 32ULL * 1024ULL * 1024ULL;
  const auto resolved = ResolveStorageProfile(options);
  ASSERT_TRUE(resolved.ok()) << resolved.status().ToString();
  EXPECT_EQ(resolved.ValueOrDie().compaction_rate_limit_bytes_per_sec,
            32ULL * 1024ULL * 1024ULL);

  const rocksdb::Options db_options = MakeRocksDbOptions(
      options, true, &resolved.ValueOrDie());
  ASSERT_NE(db_options.rate_limiter, nullptr);
  EXPECT_EQ(db_options.rate_limiter->GetBytesPerSecond(),
            32LL * 1024LL * 1024LL);
}

}  // namespace
}  // namespace cedar::internal
