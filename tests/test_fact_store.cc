#include <gtest/gtest.h>

#include <cstdlib>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <fstream>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "cedar/fact/fact_store.h"
#include "cedar/format.h"

#include <rocksdb/db.h>
#include <rocksdb/options.h>

namespace cedar {
namespace {

class FactStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_fact_store_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    path_ = pattern;
  }

  void TearDown() override { std::filesystem::remove_all(path_); }

  std::string path_;
};

TEST_F(FactStoreTest, OpensTwoColumnFamiliesAndReturnsEmptySnapshot) {
  EXPECT_EQ(kCedarFormatVersion, 2U);
  FactStore store(FactStoreOptions{path_});
  const Status opened = store.Open();
  ASSERT_TRUE(opened.ok()) << opened.ToString();

  {
    auto snapshot = store.BeginSnapshot({});
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    EXPECT_EQ(snapshot.ValueOrDie().commit_seq(), CommitSeq{0});
    EXPECT_EQ(snapshot.ValueOrDie().oldest_readable_seq(), CommitSeq{0});

    const FactRef ref = EntityFact::Vertex(VertexRef{PartId{0}, VertexId{7}}).ref();
    const auto read = store.Read(snapshot.ValueOrDie(), ref, ValidTime{10});
    ASSERT_TRUE(read.ok()) << read.status().ToString();
    EXPECT_FALSE(read.ValueOrDie().has_value());

    size_t scanned = 0;
    ASSERT_TRUE(store.Scan(snapshot.ValueOrDie(), FactPrefix::Exact(ref),
                           [&](const FactEvent&) {
                             ++scanned;
                             return Status::OK();
                           })
                    .ok());
    EXPECT_EQ(scanned, 0U);
  }
  EXPECT_TRUE(store.Close().ok());

  rocksdb::Options options;
  std::vector<std::string> column_families;
  ASSERT_TRUE(rocksdb::DB::ListColumnFamilies(options, path_, &column_families).ok());
  EXPECT_EQ(column_families,
            (std::vector<std::string>{rocksdb::kDefaultColumnFamilyName,
                                      "facts", "meta"}));
}

TEST_F(FactStoreTest, OpensConstrainedProductionAppendProfile) {
  FactStoreOptions options;
  options.path = path_;
  options.storage_profile = StorageProfile::kProductionAppend;
  options.production.memory_budget_bytes = 1ULL * 1024ULL * 1024ULL * 1024ULL;
  FactStore store(options);
  ASSERT_TRUE(store.Open().ok());
  ASSERT_TRUE(store.Close().ok());
}

TEST_F(FactStoreTest, PlacesProductionWalInExplicitRocksDbDirectory) {
  const std::filesystem::path root(path_);
  const std::filesystem::path data_directory = root / "data";
  const std::filesystem::path wal_directory = root / "wal";
  ASSERT_TRUE(std::filesystem::create_directory(wal_directory));

  FactStoreOptions options;
  options.path = data_directory.string();
  options.storage_profile = StorageProfile::kProductionAppend;
  options.production.memory_budget_bytes = 1ULL * 1024ULL * 1024ULL * 1024ULL;
  options.production.wal_directory = wal_directory.string();
  FactStore store(std::move(options));
  ASSERT_TRUE(store.Open().ok());
  ASSERT_TRUE(store.Close().ok());

  bool contains_wal = false;
  for (const auto& entry : std::filesystem::directory_iterator(wal_directory)) {
    contains_wal = contains_wal || entry.path().extension() == ".log";
  }
  EXPECT_TRUE(contains_wal);
}

TEST_F(FactStoreTest, SamplesRocksDbRuntimeMetricsWithoutExposingRocksDbTypes) {
  FactStore store(FactStoreOptions{path_});
  ASSERT_TRUE(store.Open().ok());
  const auto metrics = store.SampleRuntimeMetrics();
  ASSERT_TRUE(metrics.ok()) << metrics.status().ToString();
  EXPECT_EQ(metrics.ValueOrDie().l0_files, 0U);
  EXPECT_EQ(metrics.ValueOrDie().background_errors, 0U);
  EXPECT_GT(metrics.ValueOrDie().retained_wal_bytes, 0U);
  ASSERT_TRUE(store.Close().ok());
}

TEST_F(FactStoreTest, SnapshotCreationDoesNotBlockPressureSampling) {
  std::mutex snapshot_mutex;
  std::condition_variable snapshot_cv;
  bool snapshot_entered = false;
  bool release_snapshot = false;
  FactStoreOptions options;
  options.path = path_;
  options.snapshot_open_observer_for_testing = [&] {
    std::unique_lock<std::mutex> lock(snapshot_mutex);
    snapshot_entered = true;
    snapshot_cv.notify_all();
    snapshot_cv.wait(lock, [&] { return release_snapshot; });
  };
  FactStore store(std::move(options));
  ASSERT_TRUE(store.Open().ok());

  std::optional<StatusOr<StoreSnapshot>> snapshot;
  std::thread snapshot_thread([&] { snapshot.emplace(store.BeginSnapshot()); });
  {
    std::unique_lock<std::mutex> lock(snapshot_mutex);
    ASSERT_TRUE(snapshot_cv.wait_for(lock, std::chrono::seconds(2), [&] {
      return snapshot_entered;
    }));
  }
  auto pressure = std::async(std::launch::async, [&] { return store.SamplePressure(); });
  const bool sampled =
      pressure.wait_for(std::chrono::milliseconds(250)) == std::future_status::ready;
  {
    std::lock_guard<std::mutex> lock(snapshot_mutex);
    release_snapshot = true;
  }
  snapshot_cv.notify_all();
  snapshot_thread.join();
  ASSERT_TRUE(sampled);
  ASSERT_TRUE(pressure.get().ok());
  ASSERT_TRUE(snapshot.has_value());
  ASSERT_TRUE(snapshot->ok()) << snapshot->status().ToString();
  snapshot.reset();
  ASSERT_TRUE(store.Close().ok());
}

TEST_F(FactStoreTest, SamplesFilesystemHeadroomForPressureAdmission) {
  FactStore store(FactStoreOptions{path_});
  ASSERT_TRUE(store.Open().ok());
  const auto pressure = store.SamplePressure();
  ASSERT_TRUE(pressure.ok()) << pressure.status().ToString();
  EXPECT_NE(pressure.ValueOrDie().free_disk_bytes, UINT64_MAX);
  EXPECT_LE(pressure.ValueOrDie().free_disk_percent, 100U);
  ASSERT_TRUE(store.Close().ok());
}

TEST_F(FactStoreTest, RejectsSnapshotsOutsideDurableWindow) {
  FactStore store(FactStoreOptions{path_});
  const Status opened = store.Open();
  ASSERT_TRUE(opened.ok()) << opened.ToString();

  EXPECT_TRUE(store.BeginSnapshot(SnapshotOptions{CommitSeq{1}})
                  .status()
                  .IsInvalidArgument());
  EXPECT_TRUE(store.Close().ok());
}

TEST_F(FactStoreTest, CloseRejectsLiveSnapshotThenSucceedsAfterRelease) {
  FactStore store(FactStoreOptions{path_});
  ASSERT_TRUE(store.Open().ok());

  {
    auto snapshot = store.BeginSnapshot({});
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();

    const Status closed = store.Close();
    EXPECT_TRUE(closed.IsSnapshotPinned()) << closed.ToString();

    const auto still_open = store.BeginSnapshot({});
    EXPECT_TRUE(still_open.ok()) << still_open.status().ToString();
  }

  EXPECT_TRUE(store.Close().ok());
}

TEST_F(FactStoreTest, ReopensAfterSnapshotIsReleasedBeforeClose) {
  FactStore store(FactStoreOptions{path_});
  ASSERT_TRUE(store.Open().ok());
  {
    auto snapshot = store.BeginSnapshot({});
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  }
  ASSERT_TRUE(store.Close().ok());
  ASSERT_TRUE(store.Open().ok());
  EXPECT_TRUE(store.Close().ok());
}

TEST_F(FactStoreTest, RejectsNonRocksDirectoryWithoutMutation) {
  const std::filesystem::path legacy_marker =
      std::filesystem::path(path_) / "FORMAT";
  std::ofstream(legacy_marker) << "CEDAR-LEGACY";

  FactStore store(FactStoreOptions{path_});
  const Status opened = store.Open();
  EXPECT_TRUE(opened.IsNotSupportedError()) << opened.ToString();
  EXPECT_TRUE(std::filesystem::exists(legacy_marker));
  EXPECT_FALSE(std::filesystem::exists(std::filesystem::path(path_) / "CURRENT"));
}

TEST_F(FactStoreTest, ReopensWithInitializedWatermarks) {
  FactStore store(FactStoreOptions{path_});
  ASSERT_TRUE(store.Open().ok());
  ASSERT_TRUE(store.Close().ok());
  ASSERT_TRUE(store.Open().ok());

  const auto snapshot = store.BeginSnapshot({});
  ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
  EXPECT_EQ(snapshot.ValueOrDie().commit_seq(), CommitSeq{0});
  EXPECT_EQ(snapshot.ValueOrDie().oldest_readable_seq(), CommitSeq{0});
}

TEST_F(FactStoreTest, RejectsDefaultOnlyRocksDbWithoutCreatingColumnFamilies) {
  rocksdb::Options options;
  options.create_if_missing = true;
  std::unique_ptr<rocksdb::DB> database;
  ASSERT_TRUE(rocksdb::DB::Open(options, path_, &database).ok());
  database.reset();

  FactStore store(FactStoreOptions{path_});
  const Status opened = store.Open();
  EXPECT_TRUE(opened.IsNotSupportedError()) << opened.ToString();

  std::vector<std::string> column_families;
  ASSERT_TRUE(rocksdb::DB::ListColumnFamilies(options, path_, &column_families).ok());
  EXPECT_EQ(column_families,
            std::vector<std::string>{rocksdb::kDefaultColumnFamilyName});
}

TEST_F(FactStoreTest, RejectsUninitializedCedarColumnFamiliesWithoutMutation) {
  rocksdb::Options options;
  options.create_if_missing = true;
  options.create_missing_column_families = true;
  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors;
  descriptors.emplace_back(rocksdb::kDefaultColumnFamilyName,
                           rocksdb::ColumnFamilyOptions(options));
  descriptors.emplace_back("facts", rocksdb::ColumnFamilyOptions(options));
  descriptors.emplace_back("meta", rocksdb::ColumnFamilyOptions(options));
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  std::unique_ptr<rocksdb::DB> database;
  ASSERT_TRUE(
      rocksdb::DB::Open(options, path_, descriptors, &handles, &database).ok());
  for (rocksdb::ColumnFamilyHandle* handle : handles) {
    database->DestroyColumnFamilyHandle(handle);
  }
  database.reset();

  FactStore first(FactStoreOptions{path_});
  const Status first_open = first.Open();
  EXPECT_TRUE(first_open.IsCorruption()) << first_open.ToString();
  ASSERT_TRUE(first.Close().ok());

  FactStore second(FactStoreOptions{path_});
  const Status second_open = second.Open();
  EXPECT_TRUE(second_open.IsCorruption()) << second_open.ToString();
}

}  // namespace
}  // namespace cedar
