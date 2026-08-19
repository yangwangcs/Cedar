// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "cedar/fact/fact_codec.h"
#include "cedar/fact/fact_store.h"
#include "fact/rocksdb_config.h"

#include <rocksdb/db.h>
#include <rocksdb/cedar_maintenance.h>
#include <rocksdb/file_system.h>
#include <rocksdb/metadata.h>
#include <rocksdb/options.h>
#include <rocksdb/utilities/backup_engine.h>
#include <rocksdb/utilities/checkpoint.h>

namespace cedar {
namespace {

PendingFactMutation VertexPut(uint64_t vertex_id, uint64_t valid_from) {
  return {EntityFact::Vertex(VertexRef{PartId{0}, VertexId{vertex_id}}).ref(),
          ValidTime{valid_from}, FactOperation::kPut, 0, std::nullopt};
}

StoreCommitBatch Batch(uint64_t transaction_id, uint64_t vertex_id) {
  return {TxnId{transaction_id}, 100,
          {VertexPut(vertex_id, transaction_id)}, {}};
}

rocksdb::Status OpenRawCedarDatabase(
    const std::string& path, std::unique_ptr<rocksdb::DB>* database,
    std::vector<rocksdb::ColumnFamilyHandle*>* handles,
    bool avoid_flush_during_recovery = false,
    rocksdb::Env* env = nullptr, size_t facts_write_buffer_size = 0,
    bool force_new_manifest = false) {
  FactStoreOptions store_options;
  store_options.path = path;
  rocksdb::Options options = internal::MakeRocksDbOptions(store_options, false);
  options.create_if_missing = false;
  options.create_missing_column_families = false;
  options.avoid_flush_during_recovery = avoid_flush_during_recovery;
  if (env != nullptr) options.env = env;
  if (force_new_manifest) {
    options.max_manifest_file_size = 0;
    options.max_manifest_space_amp_pct = 0;
  }
  auto descriptors = internal::MakeRocksDbColumnFamilyDescriptors(store_options,
                                                                    options);
  if (facts_write_buffer_size != 0) {
    descriptors[1].options.write_buffer_size = facts_write_buffer_size;
  }
  return rocksdb::DB::Open(
      options, path, descriptors, handles, database);
}

void CloseRawCedarDatabase(std::unique_ptr<rocksdb::DB>* database,
                           std::vector<rocksdb::ColumnFamilyHandle*>* handles) {
  for (rocksdb::ColumnFamilyHandle* handle : *handles) {
    (*database)->DestroyColumnFamilyHandle(handle);
  }
  handles->clear();
  database->reset();
}

std::set<std::filesystem::path> LiveFactsFiles(
    rocksdb::DB* database, const std::string& database_path) {
  std::vector<rocksdb::LiveFileMetaData> files;
  database->GetLiveFilesMetaData(&files);
  std::set<std::filesystem::path> facts_files;
  for (const auto& file : files) {
    if (file.column_family_name == "facts") {
      facts_files.insert(std::filesystem::path(database_path) /
                         file.relative_filename);
    }
  }
  return facts_files;
}

std::set<std::filesystem::path> LiveFiles(
    rocksdb::DB* database, const std::string& database_path,
    const std::string& column_family_name) {
  std::vector<rocksdb::LiveFileMetaData> files;
  database->GetLiveFilesMetaData(&files);
  std::set<std::filesystem::path> result;
  for (const auto& file : files) {
    if (file.column_family_name == column_family_name) {
      result.insert(std::filesystem::path(database_path) / file.relative_filename);
    }
  }
  return result;
}

void ExpectCedarParquetMagic(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  ASSERT_TRUE(input.is_open()) << path;
  std::string magic(4, '\0');
  input.read(magic.data(), static_cast<std::streamsize>(magic.size()));
  ASSERT_EQ(input.gcount(), static_cast<std::streamsize>(magic.size())) << path;
  EXPECT_EQ(magic, "PAR1") << path;
}

void ExpectNotCedarParquetMagic(const std::filesystem::path& path) {
  std::ifstream input(path, std::ios::binary);
  ASSERT_TRUE(input.is_open()) << path;
  std::string magic(4, '\0');
  input.read(magic.data(), static_cast<std::streamsize>(magic.size()));
  ASSERT_EQ(input.gcount(), static_cast<std::streamsize>(magic.size())) << path;
  EXPECT_NE(magic, "PAR1") << path;
}

bool IsDecimalRange(const std::string& text, size_t start, size_t end) {
  return start < end && end <= text.size() &&
         std::all_of(text.begin() + static_cast<std::ptrdiff_t>(start),
                     text.begin() + static_cast<std::ptrdiff_t>(end),
                     [](unsigned char byte) { return byte >= '0' && byte <= '9'; });
}

bool IsRocksDbOwnedDatabaseEntry(const std::filesystem::directory_entry& entry) {
  const std::string name = entry.path().filename().string();
  if (name == "CURRENT" || name == "IDENTITY" || name == "LOCK" ||
      name == "LOG" || name == "LOG.old") {
    return true;
  }
  if (name.starts_with("LOG.old.")) return IsDecimalRange(name, 8, name.size());
  if (entry.is_directory()) return name == "archive";
  if (name.starts_with("MANIFEST-")) return IsDecimalRange(name, 9, name.size());
  if (name.starts_with("OPTIONS-")) return IsDecimalRange(name, 8, name.size());
  if (name.ends_with(".sst")) return IsDecimalRange(name, 0, name.size() - 4);
  if (name.ends_with(".log")) return IsDecimalRange(name, 0, name.size() - 4);
  return false;
}

void ExpectManifestOnlyRocksDbOwnership(const std::string& database_path) {
  size_t manifest_count = 0;
  for (const auto& entry : std::filesystem::directory_iterator(database_path)) {
    const std::string name = entry.path().filename().string();
    EXPECT_TRUE(IsRocksDbOwnedDatabaseEntry(entry))
        << "unexpected facts sidecar or secondary manifest: " << entry.path();
    if (name.starts_with("MANIFEST-")) ++manifest_count;
  }
  EXPECT_EQ(manifest_count, 1U);
}

class FailingFactsSstWritableFile final : public rocksdb::FSWritableFileOwnerWrapper {
 public:
  FailingFactsSstWritableFile(std::unique_ptr<rocksdb::FSWritableFile>&& target,
                              std::atomic_bool* fail_appends)
      : rocksdb::FSWritableFileOwnerWrapper(std::move(target)),
        fail_appends_(fail_appends) {}

  rocksdb::IOStatus Append(const rocksdb::Slice& data,
                           const rocksdb::IOOptions& options,
                           rocksdb::IODebugContext* debug_context) override {
    if (fail_appends_->load(std::memory_order_relaxed)) {
      return rocksdb::IOStatus::IOError("injected facts SST append failure");
    }
    return target()->Append(data, options, debug_context);
  }

 private:
  std::atomic_bool* fail_appends_;
};

class FailingFactsSstFileSystem final : public rocksdb::FileSystemWrapper {
 public:
  FailingFactsSstFileSystem(const std::shared_ptr<rocksdb::FileSystem>& target,
                            std::atomic_bool* fail_appends)
      : rocksdb::FileSystemWrapper(target), fail_appends_(fail_appends) {}

  const char* Name() const override { return "FailingFactsSstFileSystem"; }

  rocksdb::IOStatus NewWritableFile(
      const std::string& path, const rocksdb::FileOptions& options,
      std::unique_ptr<rocksdb::FSWritableFile>* file,
      rocksdb::IODebugContext* debug_context) override {
    std::unique_ptr<rocksdb::FSWritableFile> target;
    rocksdb::IOStatus status = FileSystemWrapper::NewWritableFile(
        path, options, &target, debug_context);
    if (status.ok() && std::filesystem::path(path).extension() == ".sst") {
      file->reset(new FailingFactsSstWritableFile(std::move(target),
                                                  fail_appends_));
    } else if (status.ok()) {
      *file = std::move(target);
    }
    return status;
  }

 private:
  std::atomic_bool* fail_appends_;
};

class FactsSstAppendGate {
 public:
  bool WaitUntilEntered() {
    std::unique_lock<std::mutex> lock(mutex_);
    return entered_.wait_for(lock, std::chrono::seconds(10),
                             [this] { return entered_builder_; });
  }

  void BlockBuilderAppend() {
    std::unique_lock<std::mutex> lock(mutex_);
    entered_builder_ = true;
    entered_.notify_all();
    released_.wait(lock, [this] { return release_builder_; });
  }

  void ReleaseBuilder() {
    std::lock_guard<std::mutex> lock(mutex_);
    release_builder_ = true;
    released_.notify_all();
  }

 private:
  std::mutex mutex_;
  std::condition_variable entered_;
  std::condition_variable released_;
  bool entered_builder_ = false;
  bool release_builder_ = false;
};

class BlockingFactsSstWritableFile final : public rocksdb::FSWritableFileOwnerWrapper {
 public:
  BlockingFactsSstWritableFile(std::unique_ptr<rocksdb::FSWritableFile>&& target,
                               FactsSstAppendGate* gate)
      : rocksdb::FSWritableFileOwnerWrapper(std::move(target)), gate_(gate) {}

  rocksdb::IOStatus Append(const rocksdb::Slice& data,
                           const rocksdb::IOOptions& options,
                           rocksdb::IODebugContext* debug_context) override {
    gate_->BlockBuilderAppend();
    return target()->Append(data, options, debug_context);
  }

 private:
  FactsSstAppendGate* gate_;
};

class BlockingFactsSstFileSystem final : public rocksdb::FileSystemWrapper {
 public:
  BlockingFactsSstFileSystem(const std::shared_ptr<rocksdb::FileSystem>& target,
                             FactsSstAppendGate* gate)
      : rocksdb::FileSystemWrapper(target), gate_(gate) {}

  const char* Name() const override { return "BlockingFactsSstFileSystem"; }

  rocksdb::IOStatus NewWritableFile(
      const std::string& path, const rocksdb::FileOptions& options,
      std::unique_ptr<rocksdb::FSWritableFile>* file,
      rocksdb::IODebugContext* debug_context) override {
    std::unique_ptr<rocksdb::FSWritableFile> target;
    rocksdb::IOStatus status = FileSystemWrapper::NewWritableFile(
        path, options, &target, debug_context);
    if (status.ok() && std::filesystem::path(path).extension() == ".sst") {
      file->reset(new BlockingFactsSstWritableFile(std::move(target), gate_));
    } else if (status.ok()) {
      *file = std::move(target);
    }
    return status;
  }

 private:
  FactsSstAppendGate* gate_;
};

class FailingManifestWritableFile final : public rocksdb::FSWritableFileOwnerWrapper {
 public:
  FailingManifestWritableFile(std::unique_ptr<rocksdb::FSWritableFile>&& target,
                              std::atomic_bool* fail_appends)
      : rocksdb::FSWritableFileOwnerWrapper(std::move(target)),
        fail_appends_(fail_appends) {}

  rocksdb::IOStatus Append(const rocksdb::Slice& data,
                           const rocksdb::IOOptions& options,
                           rocksdb::IODebugContext* debug_context) override {
    if (fail_appends_->load(std::memory_order_relaxed)) {
      return rocksdb::IOStatus::IOError("injected MANIFEST append failure");
    }
    return target()->Append(data, options, debug_context);
  }

 private:
  std::atomic_bool* fail_appends_;
};

class FailingManifestFileSystem final : public rocksdb::FileSystemWrapper {
 public:
  FailingManifestFileSystem(const std::shared_ptr<rocksdb::FileSystem>& target,
                            std::atomic_bool* fail_appends)
      : rocksdb::FileSystemWrapper(target), fail_appends_(fail_appends) {}

  const char* Name() const override { return "FailingManifestFileSystem"; }

  rocksdb::IOStatus NewWritableFile(
      const std::string& path, const rocksdb::FileOptions& options,
      std::unique_ptr<rocksdb::FSWritableFile>* file,
      rocksdb::IODebugContext* debug_context) override {
    std::unique_ptr<rocksdb::FSWritableFile> target;
    rocksdb::IOStatus status = FileSystemWrapper::NewWritableFile(
        path, options, &target, debug_context);
    if (status.ok() && std::filesystem::path(path).filename().string().starts_with(
                           "MANIFEST-")) {
      file->reset(new FailingManifestWritableFile(std::move(target),
                                                  fail_appends_));
    } else if (status.ok()) {
      *file = std::move(target);
    }
    return status;
  }

 private:
  std::atomic_bool* fail_appends_;
};

void ExpectBlockBasedTables(rocksdb::DB* database,
                            rocksdb::ColumnFamilyHandle* handle,
                            const std::string& column_family_name,
                            const std::string& database_path) {
  rocksdb::TablePropertiesCollection properties;
  ASSERT_TRUE(database->GetPropertiesOfAllTables(handle, &properties).ok());
  ASSERT_FALSE(properties.empty());
  for (const auto& [filename, table] : properties) {
    EXPECT_EQ(table->column_family_name, column_family_name) << filename;
    EXPECT_FALSE(table->user_collected_properties.contains("cedar.parquet.format"))
        << filename;
  }
  for (const auto& path : LiveFiles(database, database_path, column_family_name)) {
    ExpectNotCedarParquetMagic(path);
  }
}

void DeleteCurrentManifest(const std::string& path) {
  for (const auto& entry : std::filesystem::directory_iterator(path)) {
    const std::string name = entry.path().filename().string();
    if (name.starts_with("MANIFEST-")) {
      ASSERT_TRUE(std::filesystem::remove(entry.path()));
      return;
    }
  }
  FAIL() << "database did not contain a MANIFEST";
}

class RocksDbLifecycleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_rocksdb_lifecycle_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    root_path_ = pattern;
    database_path_ = (std::filesystem::path(root_path_) / "database").string();
  }

  void TearDown() override { std::filesystem::remove_all(root_path_); }

  void PopulateSource() {
    FactStore source(FactStoreOptions{database_path_});
    ASSERT_TRUE(source.Open().ok());
    ASSERT_TRUE(source.Commit(Batch(1, 101)).ok());
    ASSERT_TRUE(source.Commit(Batch(2, 202)).ok());
    ASSERT_TRUE(source.Close().ok());
  }

  void ExpectFactsReadable(const std::string& path) {
  FactStore restored(FactStoreOptions{path});
  const auto opened = restored.Open();
  ASSERT_TRUE(opened.ok()) << opened.ToString();
    {
      const auto snapshot = restored.BeginSnapshot();
      ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
      EXPECT_EQ(snapshot.ValueOrDie().commit_seq(), CommitSeq{2});
      for (uint64_t vertex_id : {101, 202}) {
        const auto event = restored.Read(
            snapshot.ValueOrDie(),
            EntityFact::Vertex(VertexRef{PartId{0}, VertexId{vertex_id}}).ref(),
            ValidTime{1000});
        ASSERT_TRUE(event.ok()) << event.status().ToString();
        ASSERT_TRUE(event.ValueOrDie().has_value());
        EXPECT_EQ(event.ValueOrDie()->ref.entity_id(), vertex_id);
      }
    }
    ASSERT_TRUE(restored.Close().ok());
  }

  std::string root_path_;
  std::string database_path_;
};

TEST_F(RocksDbLifecycleTest,
       CheckpointReopensCedarParquetFactsWithAnUnflushedWal) {
  PopulateSource();
  std::unique_ptr<rocksdb::DB> database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_TRUE(OpenRawCedarDatabase(database_path_, &database, &handles).ok());

  rocksdb::Checkpoint* checkpoint_raw = nullptr;
  ASSERT_TRUE(rocksdb::Checkpoint::Create(database.get(), &checkpoint_raw).ok());
  std::unique_ptr<rocksdb::Checkpoint> checkpoint(checkpoint_raw);
  const std::string checkpoint_path =
      (std::filesystem::path(root_path_) / "checkpoint").string();
  ASSERT_TRUE(checkpoint->CreateCheckpoint(
      checkpoint_path, std::numeric_limits<uint64_t>::max()).ok());
  checkpoint.reset();
  CloseRawCedarDatabase(&database, &handles);

  ExpectFactsReadable(checkpoint_path);
}

TEST_F(RocksDbLifecycleTest,
       ReopenReplaysUnflushedFactsWalIntoTheActiveVersionRadixMemTable) {
  const pid_t child = fork();
  ASSERT_NE(child, -1);
  if (child == 0) {
    FactStore source(FactStoreOptions{database_path_});
    if (!source.Open().ok()) _exit(2);
    if (!source.Commit(Batch(1, 101)).ok()) _exit(3);
    _exit(0);
  }
  int child_status = 0;
  ASSERT_EQ(waitpid(child, &child_status, 0), child);
  ASSERT_TRUE(WIFEXITED(child_status));
  ASSERT_EQ(WEXITSTATUS(child_status), 0);

  std::unique_ptr<rocksdb::DB> database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_TRUE(OpenRawCedarDatabase(database_path_, &database, &handles, true).ok());
  ASSERT_EQ(handles.size(), 3U);
  EXPECT_TRUE(LiveFactsFiles(database.get(), database_path_).empty());

  std::string active_entries;
  ASSERT_TRUE(database->GetProperty(handles[1],
                                    "rocksdb.num-entries-active-mem-table",
                                    &active_entries));
  ASSERT_GT(std::stoull(active_entries), 0U);

  const FactRef ref =
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{101}}).ref();
  const std::string key = EncodeFactKey(ref, ValidTime{1}, CommitSeq{1});
  ASSERT_FALSE(key.empty());
  std::string encoded_value;
  ASSERT_TRUE(database
                  ->Get(rocksdb::ReadOptions(), handles[1], key, &encoded_value)
                  .ok());
  const auto decoded = DecodeFactValue(ref, ValidTime{1}, CommitSeq{1},
                                       encoded_value);
  ASSERT_TRUE(decoded.ok()) << decoded.status().ToString();
  EXPECT_EQ(decoded.ValueOrDie().ref, ref);
  EXPECT_EQ(decoded.ValueOrDie().commit_seq, CommitSeq{1});
  CloseRawCedarDatabase(&database, &handles);

  FactStore restored(FactStoreOptions{database_path_});
  ASSERT_TRUE(restored.Open().ok());
  {
    const auto snapshot = restored.BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    EXPECT_EQ(snapshot.ValueOrDie().commit_seq(), CommitSeq{1});
    const auto event = restored.Read(snapshot.ValueOrDie(), ref, ValidTime{1});
    ASSERT_TRUE(event.ok()) << event.status().ToString();
    ASSERT_TRUE(event.ValueOrDie().has_value());
    EXPECT_EQ(event.ValueOrDie()->ref, ref);
  }
  ASSERT_TRUE(restored.Close().ok());
}

TEST_F(RocksDbLifecycleTest,
       BackupEngineRestoreReopensCedarParquetFactsWithAnActiveWal) {
  PopulateSource();
  std::unique_ptr<rocksdb::DB> database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_TRUE(OpenRawCedarDatabase(database_path_, &database, &handles).ok());

  const std::string backup_path =
      (std::filesystem::path(root_path_) / "backup").string();
  rocksdb::BackupEngineOptions backup_options(backup_path);
  rocksdb::BackupEngine* backup_engine_raw = nullptr;
  ASSERT_TRUE(rocksdb::BackupEngine::Open(
                  backup_options, rocksdb::Env::Default(), &backup_engine_raw)
                  .ok());
  std::unique_ptr<rocksdb::BackupEngine> backup_engine(backup_engine_raw);
  rocksdb::CreateBackupOptions create_options;
  create_options.flush_before_backup = false;
  rocksdb::BackupID backup_id = 0;
  const rocksdb::IOStatus created = backup_engine->CreateNewBackup(
      create_options, database.get(), &backup_id);
  ASSERT_TRUE(created.ok()) << created.ToString();
  ASSERT_TRUE(backup_engine->VerifyBackup(backup_id, true).ok());
  CloseRawCedarDatabase(&database, &handles);

  const std::string restored_path =
      (std::filesystem::path(root_path_) / "restored").string();
  ASSERT_TRUE(backup_engine
                  ->RestoreDBFromBackup(rocksdb::RestoreOptions(), backup_id,
                                        restored_path, restored_path)
                  .ok());
  backup_engine.reset();

  ExpectFactsReadable(restored_path);
}

TEST_F(RocksDbLifecycleTest,
       RepairDbReconstructsCedarParquetFactsAfterTheManifestIsLost) {
  PopulateSource();
  std::unique_ptr<rocksdb::DB> database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_TRUE(OpenRawCedarDatabase(database_path_, &database, &handles).ok());
  ASSERT_EQ(handles.size(), 3U);
  ASSERT_TRUE(database->Flush(rocksdb::FlushOptions(), handles[1]).ok());
  CloseRawCedarDatabase(&database, &handles);

  DeleteCurrentManifest(database_path_);
  FactStoreOptions store_options;
  store_options.path = database_path_;
  rocksdb::Options options = internal::MakeRocksDbOptions(store_options, false);
  ASSERT_TRUE(rocksdb::RepairDB(
                  database_path_, rocksdb::DBOptions(options),
                  internal::MakeRocksDbColumnFamilyDescriptors(store_options,
                                                                options))
                  .ok());

  ExpectFactsReadable(database_path_);
}

TEST_F(RocksDbLifecycleTest,
       CompactionReclaimsObsoleteCedarParquetFactsFilesWithoutLosingFacts) {
  for (uint64_t transaction_id = 1; transaction_id <= 3; ++transaction_id) {
    FactStore source(FactStoreOptions{database_path_});
    ASSERT_TRUE(source.Open().ok());
    ASSERT_TRUE(source.Commit(Batch(transaction_id, 101)).ok());
    ASSERT_TRUE(source.Close().ok());

    std::unique_ptr<rocksdb::DB> database;
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    ASSERT_TRUE(OpenRawCedarDatabase(database_path_, &database, &handles).ok());
    rocksdb::FlushOptions flush_options;
    flush_options.wait = true;
    ASSERT_TRUE(database->Flush(flush_options, handles[1]).ok());
    CloseRawCedarDatabase(&database, &handles);
  }

  std::unique_ptr<rocksdb::DB> database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_TRUE(OpenRawCedarDatabase(database_path_, &database, &handles).ok());
  ASSERT_TRUE(database
                  ->Put(rocksdb::WriteOptions(), handles[0], "default-control",
                        "block-based")
                  .ok());
  rocksdb::FlushOptions flush_options;
  flush_options.wait = true;
  ASSERT_TRUE(database->Flush(flush_options, handles[0]).ok());
  ASSERT_TRUE(database->Flush(flush_options, handles[2]).ok());
  ExpectBlockBasedTables(database.get(), handles[0], "default", database_path_);
  ExpectBlockBasedTables(database.get(), handles[2], "meta", database_path_);
  const std::set<std::filesystem::path> obsolete_candidates =
      LiveFactsFiles(database.get(), database_path_);
  ASSERT_GE(obsolete_candidates.size(), 3U);
  for (const auto& path : obsolete_candidates) ExpectCedarParquetMagic(path);
  ASSERT_TRUE(database
                  ->SetDBOptions(
                      {{"delete_obsolete_files_period_micros", "0"}})
                  .ok());

  std::vector<std::string> input_files;
  input_files.reserve(obsolete_candidates.size());
  for (const auto& path : obsolete_candidates) {
    input_files.push_back(path.filename().string());
  }
  ASSERT_TRUE(database
                  ->CompactFiles(rocksdb::CompactionOptions(), handles[1],
                                 input_files, 1)
                  .ok());
  rocksdb::WaitForCompactOptions wait_options;
  wait_options.wait_for_purge = true;
  ASSERT_TRUE(database->WaitForCompact(wait_options).ok());
  const std::set<std::filesystem::path> compacted_files =
      LiveFactsFiles(database.get(), database_path_);
  ASSERT_FALSE(compacted_files.empty());
  bool replaced_input = false;
  for (const auto& path : obsolete_candidates) {
    replaced_input = replaced_input || !compacted_files.contains(path);
  }
  EXPECT_TRUE(replaced_input);
  for (const auto& path : compacted_files) ExpectCedarParquetMagic(path);
  CloseRawCedarDatabase(&database, &handles);

  for (const auto& path : obsolete_candidates) {
    EXPECT_FALSE(std::filesystem::exists(path)) << path;
  }
  ExpectManifestOnlyRocksDbOwnership(database_path_);

  FactStore restored(FactStoreOptions{database_path_});
  ASSERT_TRUE(restored.Open().ok());
  {
    const auto snapshot = restored.BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    EXPECT_EQ(snapshot.ValueOrDie().commit_seq(), CommitSeq{3});
    for (uint64_t valid_from = 1; valid_from <= 3; ++valid_from) {
      const auto event = restored.Read(
          snapshot.ValueOrDie(),
          EntityFact::Vertex(VertexRef{PartId{0}, VertexId{101}}).ref(),
          ValidTime{valid_from});
      ASSERT_TRUE(event.ok()) << event.status().ToString();
      ASSERT_TRUE(event.ValueOrDie().has_value());
      EXPECT_EQ(event.ValueOrDie()->ref.entity_id(), 101U);
      EXPECT_EQ(event.ValueOrDie()->valid_from, ValidTime{valid_from});
      EXPECT_EQ(event.ValueOrDie()->commit_seq, CommitSeq{valid_from});
    }
  }
  ASSERT_TRUE(restored.Close().ok());
}

TEST_F(RocksDbLifecycleTest,
       ShutdownWaitsForActiveCedarParquetBuilderIoAndReopensCommittedFacts) {
  FactStore source(FactStoreOptions{database_path_});
  ASSERT_TRUE(source.Open().ok());
  ASSERT_TRUE(source.Commit(Batch(1, 101)).ok());
  ASSERT_TRUE(source.Close().ok());

  FactsSstAppendGate gate;
  auto file_system = std::make_shared<BlockingFactsSstFileSystem>(
      rocksdb::Env::Default()->GetFileSystem(), &gate);
  std::unique_ptr<rocksdb::Env> env = rocksdb::NewCompositeEnv(file_system);
  std::unique_ptr<rocksdb::DB> database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_TRUE(OpenRawCedarDatabase(database_path_, &database, &handles, true,
                                   env.get())
                  .ok());
  ASSERT_EQ(handles.size(), 3U);
  ASSERT_TRUE(LiveFactsFiles(database.get(), database_path_).empty());

  rocksdb::FlushOptions flush_options;
  flush_options.wait = false;
  ASSERT_TRUE(database->Flush(flush_options, handles[1]).ok());
  ASSERT_TRUE(gate.WaitUntilEntered());

  std::mutex close_mutex;
  std::condition_variable close_started;
  bool close_has_started = false;
  std::atomic_bool close_finished{false};
  std::thread closer([&] {
    {
      std::lock_guard<std::mutex> lock(close_mutex);
      close_has_started = true;
    }
    close_started.notify_one();
    CloseRawCedarDatabase(&database, &handles);
    close_finished.store(true, std::memory_order_release);
  });
  {
    std::unique_lock<std::mutex> lock(close_mutex);
    ASSERT_TRUE(close_started.wait_for(lock, std::chrono::seconds(10),
                                       [&] { return close_has_started; }));
  }
  EXPECT_FALSE(close_finished.load(std::memory_order_acquire));
  gate.ReleaseBuilder();
  closer.join();
  EXPECT_TRUE(close_finished.load(std::memory_order_acquire));

  FactStore restored(FactStoreOptions{database_path_});
  ASSERT_TRUE(restored.Open().ok());
  {
    const auto snapshot = restored.BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    EXPECT_EQ(snapshot.ValueOrDie().commit_seq(), CommitSeq{1});
    const auto event = restored.Read(
        snapshot.ValueOrDie(),
        EntityFact::Vertex(VertexRef{PartId{0}, VertexId{101}}).ref(),
        ValidTime{1});
    ASSERT_TRUE(event.ok()) << event.status().ToString();
    ASSERT_TRUE(event.ValueOrDie().has_value());
    EXPECT_EQ(event.ValueOrDie()->ref.entity_id(), 101U);
  }
  ASSERT_TRUE(restored.Close().ok());
}

TEST_F(RocksDbLifecycleTest,
       FlushBuilderIoFailureReturnsErrorAndRecoversFromWal) {
  FactStore source(FactStoreOptions{database_path_});
  ASSERT_TRUE(source.Open().ok());
  ASSERT_TRUE(source.Commit(Batch(1, 101)).ok());
  ASSERT_TRUE(source.Close().ok());

  std::atomic_bool fail_appends{false};
  auto file_system = std::make_shared<FailingFactsSstFileSystem>(
      rocksdb::Env::Default()->GetFileSystem(), &fail_appends);
  std::unique_ptr<rocksdb::Env> env = rocksdb::NewCompositeEnv(file_system);
  std::unique_ptr<rocksdb::DB> database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_TRUE(OpenRawCedarDatabase(database_path_, &database, &handles, true,
                                   env.get())
                  .ok());
  ASSERT_EQ(handles.size(), 3U);
  ASSERT_TRUE(LiveFactsFiles(database.get(), database_path_).empty());

  fail_appends.store(true, std::memory_order_relaxed);
  rocksdb::FlushOptions flush_options;
  flush_options.wait = true;
  const rocksdb::Status flush_status = database->Flush(flush_options, handles[1]);
  EXPECT_TRUE(flush_status.IsIOError()) << flush_status.ToString();
  EXPECT_TRUE(LiveFactsFiles(database.get(), database_path_).empty());
  CloseRawCedarDatabase(&database, &handles);

  for (const auto& entry : std::filesystem::directory_iterator(database_path_)) {
    EXPECT_NE(entry.path().extension(), ".sst") << entry.path();
  }
  ExpectManifestOnlyRocksDbOwnership(database_path_);

  ASSERT_TRUE(OpenRawCedarDatabase(database_path_, &database, &handles, true).ok());
  ASSERT_TRUE(LiveFactsFiles(database.get(), database_path_).empty());
  std::string active_entries;
  ASSERT_TRUE(database->GetProperty(handles[1],
                                    "rocksdb.num-entries-active-mem-table",
                                    &active_entries));
  EXPECT_GT(std::stoull(active_entries), 0U);
  CloseRawCedarDatabase(&database, &handles);

  FactStore restored(FactStoreOptions{database_path_});
  ASSERT_TRUE(restored.Open().ok());
  {
    const auto snapshot = restored.BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    const auto event = restored.Read(
        snapshot.ValueOrDie(),
        EntityFact::Vertex(VertexRef{PartId{0}, VertexId{101}}).ref(),
        ValidTime{1});
    ASSERT_TRUE(event.ok()) << event.status().ToString();
    ASSERT_TRUE(event.ValueOrDie().has_value());
    EXPECT_EQ(event.ValueOrDie()->ref.entity_id(), 101U);
  }
  const Status close_status = restored.Close();
  ASSERT_TRUE(close_status.ok()) << close_status.ToString();
}

TEST_F(RocksDbLifecycleTest,
       AutomaticFlushBuilderIoFailureIncrementsBackgroundErrorCount) {
  FactStore source(FactStoreOptions{database_path_});
  ASSERT_TRUE(source.Open().ok());
  ASSERT_TRUE(source.Close().ok());

  std::atomic_bool fail_appends{false};
  auto file_system = std::make_shared<FailingFactsSstFileSystem>(
      rocksdb::Env::Default()->GetFileSystem(), &fail_appends);
  std::unique_ptr<rocksdb::Env> env = rocksdb::NewCompositeEnv(file_system);
  std::unique_ptr<rocksdb::DB> database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  const rocksdb::Status open_status = OpenRawCedarDatabase(
      database_path_, &database, &handles, false, env.get(), 1024);
  ASSERT_TRUE(open_status.ok()) << open_status.ToString();
  fail_appends.store(true, std::memory_order_relaxed);

  rocksdb::Status write_status;
  for (uint64_t vertex_id = 1; vertex_id <= 2048; ++vertex_id) {
    const FactRef ref =
        EntityFact::Vertex(VertexRef{PartId{0}, VertexId{vertex_id}}).ref();
    const FactEvent event{ref, ValidTime{vertex_id}, CommitSeq{vertex_id},
                          FactOperation::kPut, 0, std::nullopt};
    const auto encoded_value = EncodeFactValue(event);
    ASSERT_TRUE(encoded_value.ok()) << encoded_value.status().ToString();
    write_status = database->Put(
        rocksdb::WriteOptions(), handles[1],
        EncodeFactKey(ref, event.valid_from, event.commit_seq),
        encoded_value.ValueOrDie());
    if (!write_status.ok()) break;
  }

  EXPECT_TRUE(write_status.IsIOError()) << write_status.ToString();
  uint64_t background_errors = 0;
  ASSERT_TRUE(database->GetIntProperty(rocksdb::DB::Properties::kBackgroundErrors,
                                       &background_errors));
  EXPECT_GT(background_errors, 0U);
  rocksdb::CedarMaintenanceSnapshot maintenance;
  ASSERT_TRUE(rocksdb::PollCedarMaintenance(database.get(), &maintenance).ok());
  EXPECT_GT(maintenance.background_errors, 0U);
  EXPECT_TRUE(LiveFactsFiles(database.get(), database_path_).empty());
  CloseRawCedarDatabase(&database, &handles);
}

TEST_F(RocksDbLifecycleTest,
       FlushManifestInstallFailureDoesNotPublishFactsSstAndRecoversFromWal) {
  FactStore source(FactStoreOptions{database_path_});
  ASSERT_TRUE(source.Open().ok());
  ASSERT_TRUE(source.Commit(Batch(1, 101)).ok());
  ASSERT_TRUE(source.Close().ok());

  std::atomic_bool fail_appends{false};
  auto file_system = std::make_shared<FailingManifestFileSystem>(
      rocksdb::Env::Default()->GetFileSystem(), &fail_appends);
  std::unique_ptr<rocksdb::Env> env = rocksdb::NewCompositeEnv(file_system);
  std::unique_ptr<rocksdb::DB> database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  const rocksdb::Status open_status = OpenRawCedarDatabase(
      database_path_, &database, &handles, true, env.get(), 0, true);
  ASSERT_TRUE(open_status.ok()) << open_status.ToString();
  ASSERT_TRUE(LiveFactsFiles(database.get(), database_path_).empty());

  fail_appends.store(true, std::memory_order_relaxed);
  rocksdb::FlushOptions flush_options;
  flush_options.wait = true;
  const rocksdb::Status flush_status = database->Flush(flush_options, handles[1]);
  EXPECT_TRUE(flush_status.IsIOError()) << flush_status.ToString();
  EXPECT_TRUE(LiveFactsFiles(database.get(), database_path_).empty());
  CloseRawCedarDatabase(&database, &handles);

  for (const auto& entry : std::filesystem::directory_iterator(database_path_)) {
    EXPECT_NE(entry.path().extension(), ".sst") << entry.path();
  }
  ExpectManifestOnlyRocksDbOwnership(database_path_);

  ASSERT_TRUE(OpenRawCedarDatabase(database_path_, &database, &handles, true).ok());
  ASSERT_TRUE(LiveFactsFiles(database.get(), database_path_).empty());
  std::string active_entries;
  ASSERT_TRUE(database->GetProperty(handles[1],
                                    "rocksdb.num-entries-active-mem-table",
                                    &active_entries));
  EXPECT_GT(std::stoull(active_entries), 0U);
  CloseRawCedarDatabase(&database, &handles);

  FactStore restored(FactStoreOptions{database_path_});
  ASSERT_TRUE(restored.Open().ok());
  {
    const auto snapshot = restored.BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    const auto event = restored.Read(
        snapshot.ValueOrDie(),
        EntityFact::Vertex(VertexRef{PartId{0}, VertexId{101}}).ref(),
        ValidTime{1});
    ASSERT_TRUE(event.ok()) << event.status().ToString();
    ASSERT_TRUE(event.ValueOrDie().has_value());
    EXPECT_EQ(event.ValueOrDie()->ref.entity_id(), 101U);
  }
  ASSERT_TRUE(restored.Close().ok());
}

TEST_F(RocksDbLifecycleTest,
       CompactionOutputIoFailureRetainsInstalledFactsTables) {
  for (uint64_t transaction_id = 1; transaction_id <= 2; ++transaction_id) {
    FactStore source(FactStoreOptions{database_path_});
    ASSERT_TRUE(source.Open().ok());
    ASSERT_TRUE(source.Commit(Batch(transaction_id, transaction_id * 101)).ok());
    ASSERT_TRUE(source.Close().ok());

    std::unique_ptr<rocksdb::DB> database;
    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    ASSERT_TRUE(OpenRawCedarDatabase(database_path_, &database, &handles).ok());
    rocksdb::FlushOptions flush_options;
    flush_options.wait = true;
    ASSERT_TRUE(database->Flush(flush_options, handles[1]).ok());
    CloseRawCedarDatabase(&database, &handles);
  }

  std::atomic_bool fail_appends{false};
  auto file_system = std::make_shared<FailingFactsSstFileSystem>(
      rocksdb::Env::Default()->GetFileSystem(), &fail_appends);
  std::unique_ptr<rocksdb::Env> env = rocksdb::NewCompositeEnv(file_system);
  std::unique_ptr<rocksdb::DB> database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_TRUE(OpenRawCedarDatabase(database_path_, &database, &handles, false,
                                   env.get())
                  .ok());
  const std::set<std::filesystem::path> installed_files =
      LiveFactsFiles(database.get(), database_path_);
  ASSERT_EQ(installed_files.size(), 2U);
  std::vector<std::string> input_files;
  for (const auto& path : installed_files) {
    input_files.push_back(path.filename().string());
  }

  fail_appends.store(true, std::memory_order_relaxed);
  const rocksdb::Status compaction_status = database->CompactFiles(
      rocksdb::CompactionOptions(), handles[1], input_files, 1);
  EXPECT_TRUE(compaction_status.IsIOError()) << compaction_status.ToString();
  EXPECT_EQ(LiveFactsFiles(database.get(), database_path_), installed_files);
  CloseRawCedarDatabase(&database, &handles);

  FactStore restored(FactStoreOptions{database_path_});
  ASSERT_TRUE(restored.Open().ok());
  {
    const auto snapshot = restored.BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    EXPECT_EQ(snapshot.ValueOrDie().commit_seq(), CommitSeq{2});
    for (uint64_t vertex_id : {101, 202}) {
      const auto event = restored.Read(
          snapshot.ValueOrDie(),
          EntityFact::Vertex(VertexRef{PartId{0}, VertexId{vertex_id}}).ref(),
          ValidTime{vertex_id / 101});
      ASSERT_TRUE(event.ok()) << event.status().ToString();
      ASSERT_TRUE(event.ValueOrDie().has_value());
      EXPECT_EQ(event.ValueOrDie()->ref.entity_id(), vertex_id);
    }
  }
  ASSERT_TRUE(restored.Close().ok());
}

}  // namespace
}  // namespace cedar
