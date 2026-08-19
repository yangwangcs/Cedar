#include <gtest/gtest.h>

#include <filesystem>
#include <limits>

#include <unistd.h>

#include "storage/facts/fact_store.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/table.h"
#include "storage/rocks/rocksdb_config.h"

namespace cedar::internal {
namespace {

TEST(RocksDbProfileTest, RejectsProductionBudgetBelowOneGiB) {
  FactStoreOptions options;
  options.storage_profile = StorageProfile::kProductionAppend;
  options.production.memory_budget_bytes = 512ULL * 1024ULL * 1024ULL;
  const auto resolved = ResolveStorageProfile(options);
  ASSERT_FALSE(resolved.ok());
  EXPECT_TRUE(resolved.status().IsInvalidArgument())
      << resolved.status().ToString();
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
  EXPECT_FALSE(db_options.enable_pipelined_write);
  EXPECT_EQ(db_options.max_background_jobs, 6);
  EXPECT_EQ(db_options.max_subcompactions, 2U);
  EXPECT_EQ(db_options.bytes_per_sync, 1ULL << 20);
  EXPECT_EQ(db_options.wal_bytes_per_sync, 0U);
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
  EXPECT_EQ(descriptors[1].options.compression, rocksdb::kNoCompression);
  EXPECT_EQ(descriptors[1].options.bottommost_compression,
            rocksdb::kNoCompression);
  EXPECT_NE(descriptors[1].options.table_factory->GetPrintableOptions().find(
                "page_compression=LZ4_RAW"),
            std::string::npos);
  EXPECT_EQ(descriptors[1].options.prefix_extractor, nullptr);
  EXPECT_FALSE(descriptors[1].options.enable_blob_files);
  EXPECT_EQ(descriptors[2].options.write_buffer_size, 32ULL * 1024ULL * 1024ULL);
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
