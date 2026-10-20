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
#include <optional>
#include <set>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <utility>
#include <unistd.h>
#include <vector>

#include "cedar/fact/fact_codec.h"
#include "cedar/storage_files.h"
#include "storage/facts/fact_store.h"
#include "storage/rocks/rocksdb_config.h"

#include <rocksdb/db.h>
#include <rocksdb/cedar_maintenance.h>
#include <rocksdb/file_system.h>
#include <rocksdb/metadata.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>
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

using RawEntries = std::vector<std::pair<std::string, std::string>>;

RawEntries CollectRawEntries(rocksdb::DB* database,
                             rocksdb::ColumnFamilyHandle* handle,
                             bool reverse) {
  std::unique_ptr<rocksdb::Iterator> iterator(
      database->NewIterator(rocksdb::ReadOptions(), handle));
  RawEntries entries;
  reverse ? iterator->SeekToLast() : iterator->SeekToFirst();
  while (iterator->Valid()) {
    entries.emplace_back(iterator->key().ToString(),
                         iterator->value().ToString());
    reverse ? iterator->Prev() : iterator->Next();
  }
  EXPECT_TRUE(iterator->status().ok()) << iterator->status().ToString();
  return entries;
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

TEST_F(RocksDbLifecycleTest, InspectsLiveCedarFilesReadOnly) {
  PopulateSource();
  std::unique_ptr<rocksdb::DB> database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_TRUE(OpenRawCedarDatabase(database_path_, &database, &handles).ok());
  ASSERT_EQ(handles.size(), 3U);
  ASSERT_TRUE(database->Flush(rocksdb::FlushOptions(), handles[1]).ok());
  CloseRawCedarDatabase(&database, &handles);
  std::map<std::string, uintmax_t> before;
  for (const auto& entry : std::filesystem::directory_iterator(database_path_)) {
    if (entry.is_regular_file()) before.emplace(entry.path().filename().string(),
                                                 entry.file_size());
  }

  const auto inspected = InspectStorageFiles({.path = database_path_});
  ASSERT_TRUE(inspected.ok()) << inspected.status().ToString();
  ASSERT_FALSE(inspected.ValueOrDie().empty());
  bool saw_facts = false;
  for (size_t i = 0; i < inspected.ValueOrDie().size(); ++i) {
    const auto& file = inspected.ValueOrDie()[i];
    if (file.column_family_name == "facts") {
      saw_facts = true;
      EXPECT_EQ(file.role, StorageFileRole::kAuthoritativeFacts);
      EXPECT_EQ(file.table_format, StorageTableFormat::kCedarParquet);
    }
    if (i > 0) {
      const auto& previous = inspected.ValueOrDie()[i - 1];
      EXPECT_LE(std::tie(previous.column_family_name, previous.level,
                         previous.relative_filename),
                std::tie(file.column_family_name, file.level,
                         file.relative_filename));
    }
  }
  EXPECT_TRUE(saw_facts);

  std::map<std::string, uintmax_t> after;
  for (const auto& entry : std::filesystem::directory_iterator(database_path_)) {
    if (entry.is_regular_file()) after.emplace(entry.path().filename().string(),
                                                entry.file_size());
  }
  EXPECT_EQ(before, after);
}

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
       RadixFlushSstReopenPreservesPointReadsAndBidirectionalOrder) {
  FactStore initializer(FactStoreOptions{database_path_});
  ASSERT_TRUE(initializer.Open().ok());
  ASSERT_TRUE(initializer.Close().ok());

  std::unique_ptr<rocksdb::DB> database;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  ASSERT_TRUE(OpenRawCedarDatabase(database_path_, &database, &handles).ok());
  ASSERT_EQ(handles.size(), 3U);

  const FactRef deleted_ref =
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{101}}).ref();
  const FactRef surviving_ref =
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{202}}).ref();
  const std::string put_key =
      EncodeFactKey(deleted_ref, ValidTime{7}, CommitSeq{8});
  const std::string delete_key =
      EncodeFactKey(deleted_ref, ValidTime{7}, CommitSeq{9});
  const std::string surviving_key =
      EncodeFactKey(surviving_ref, ValidTime{8}, CommitSeq{10});
  const auto put_value = EncodeFactValue(
      FactEvent{deleted_ref, ValidTime{7}, CommitSeq{8}, FactOperation::kPut,
                0, std::nullopt});
  const auto delete_value = EncodeFactValue(
      FactEvent{deleted_ref, ValidTime{7}, CommitSeq{9}, FactOperation::kDelete,
                0, std::nullopt});
  const auto surviving_value = EncodeFactValue(
      FactEvent{surviving_ref, ValidTime{8}, CommitSeq{10},
                FactOperation::kPut, 0, std::nullopt});
  ASSERT_TRUE(put_value.ok()) << put_value.status().ToString();
  ASSERT_TRUE(delete_value.ok()) << delete_value.status().ToString();
  ASSERT_TRUE(surviving_value.ok()) << surviving_value.status().ToString();

  rocksdb::WriteBatch batch;
  ASSERT_TRUE(batch.Put(handles[1], put_key, put_value.ValueOrDie()).ok());
  ASSERT_TRUE(batch.Put(handles[1], delete_key,
                        delete_value.ValueOrDie()).ok());
  ASSERT_TRUE(batch.Put(handles[1], surviving_key,
                        surviving_value.ValueOrDie()).ok());
  ASSERT_TRUE(database->Write(rocksdb::WriteOptions(), &batch).ok());

  std::string value;
  ASSERT_TRUE(database->Get(rocksdb::ReadOptions(), handles[1], put_key,
                            &value).ok());
  EXPECT_EQ(value, put_value.ValueOrDie());
  ASSERT_TRUE(database->Get(rocksdb::ReadOptions(), handles[1], delete_key,
                            &value).ok());
  EXPECT_EQ(value, delete_value.ValueOrDie());
  ASSERT_TRUE(database->Get(rocksdb::ReadOptions(), handles[1], surviving_key,
                            &value).ok());
  EXPECT_EQ(value, surviving_value.ValueOrDie());
  const RawEntries active_forward =
      CollectRawEntries(database.get(), handles[1], false);
  const RawEntries active_reverse =
      CollectRawEntries(database.get(), handles[1], true);
  ASSERT_EQ(active_forward.size(), 3U);
  ASSERT_EQ(active_reverse.size(), 3U);
  EXPECT_EQ(active_reverse, RawEntries(active_forward.rbegin(),
                                       active_forward.rend()));

  rocksdb::FlushOptions flush_options;
  flush_options.wait = true;
  ASSERT_TRUE(database->Flush(flush_options, handles[1]).ok());
  const auto facts_files = LiveFactsFiles(database.get(), database_path_);
  ASSERT_FALSE(facts_files.empty());
  for (const auto& file : facts_files) ExpectCedarParquetMagic(file);
  CloseRawCedarDatabase(&database, &handles);

  ASSERT_TRUE(OpenRawCedarDatabase(database_path_, &database, &handles).ok());
  ASSERT_TRUE(database->Get(rocksdb::ReadOptions(), handles[1], put_key,
                            &value).ok());
  EXPECT_EQ(value, put_value.ValueOrDie());
  ASSERT_TRUE(database->Get(rocksdb::ReadOptions(), handles[1], delete_key,
                            &value).ok());
  EXPECT_EQ(value, delete_value.ValueOrDie());
  ASSERT_TRUE(database->Get(rocksdb::ReadOptions(), handles[1], surviving_key,
                            &value).ok());
  EXPECT_EQ(value, surviving_value.ValueOrDie());
  EXPECT_EQ(CollectRawEntries(database.get(), handles[1], false),
            active_forward);
  EXPECT_EQ(CollectRawEntries(database.get(), handles[1], true),
            active_reverse);
  CloseRawCedarDatabase(&database, &handles);
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

// Task 3: Close must gate on live epoch references, including a handle whose
// epoch was already retired by a later commit. A failed close keeps the store
// usable (accepting snapshots again); the same handle must still read its
// sequence and every handle must be released before close can succeed.
TEST_F(RocksDbLifecycleTest,
       CloseReportsRetiredEpochUntilEveryHandleIsReleased) {
  {
    FactStore seed(FactStoreOptions{database_path_});
    ASSERT_TRUE(seed.Open().ok());
    ASSERT_TRUE(seed.Commit(Batch(1, 101)).ok());
    ASSERT_TRUE(seed.Close().ok());
  }
  FactStore store(FactStoreOptions{database_path_});
  ASSERT_TRUE(store.Open().ok());
  const FactRef ref =
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{101}}).ref();

  auto begun = store.BeginSnapshot();
  ASSERT_TRUE(begun.ok()) << begun.status().ToString();
  std::optional<StoreSnapshot> retired_epoch(
      std::move(begun).ConsumeValueOrDie());
  EXPECT_EQ(retired_epoch->commit_seq(), CommitSeq{1});
  ASSERT_TRUE(store.Commit(Batch(2, 202)).ok());

  EXPECT_TRUE(store.Close().IsSnapshotPinned());
  // A failed close must leave the store open for new snapshots.
  auto current = store.BeginSnapshot();
  ASSERT_TRUE(current.ok()) << current.status().ToString();
  std::optional<StoreSnapshot> current_epoch(
      std::move(current).ConsumeValueOrDie());
  EXPECT_EQ(current_epoch->commit_seq(), CommitSeq{2});

  // The still-live retired epoch keeps serving its own sequence.
  const auto event = store.Read(*retired_epoch, ref, ValidTime{1});
  ASSERT_TRUE(event.ok()) << event.status().ToString();
  ASSERT_TRUE(event.ValueOrDie().has_value());
  EXPECT_EQ(event.ValueOrDie()->commit_seq, CommitSeq{1});

  retired_epoch.reset();
  EXPECT_TRUE(store.Close().IsSnapshotPinned());
  current_epoch.reset();
  EXPECT_TRUE(store.Close().ok());
}

// Task 3 stress: snapshot opens race Close. Every successful open owns a live
// epoch reference until its handle is destroyed, so Close may only report
// success when no handle is outstanding; rejected opens must fail with the
// documented transient/invalid statuses. Bounded: readers exit after a fixed
// number of iterations and close is only retried while handles drain.
TEST_F(RocksDbLifecycleTest,
       ConcurrentSnapshotOpenAndCloseKeepsEpochAccountingConsistent) {
  {
    FactStore seed(FactStoreOptions{database_path_});
    ASSERT_TRUE(seed.Open().ok());
    ASSERT_TRUE(seed.Commit(Batch(1, 101)).ok());
    ASSERT_TRUE(seed.Close().ok());
  }
  FactStore store(FactStoreOptions{database_path_});
  ASSERT_TRUE(store.Open().ok());
  const FactRef ref =
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{101}}).ref();

  constexpr int kReaders = 4;
  constexpr int kIterations = 2000;
  std::atomic<int> started{0};
  std::atomic<bool> close_succeeded{false};
  std::atomic<uint64_t> opened{0};
  std::atomic<uint64_t> unexpected{0};
  std::atomic<uint64_t> live_handles{0};
  std::vector<std::thread> readers;
  readers.reserve(kReaders);
  for (int reader = 0; reader < kReaders; ++reader) {
    readers.emplace_back([&] {
      started.fetch_add(1, std::memory_order_release);
      while (started.load(std::memory_order_acquire) != kReaders) {
        std::this_thread::yield();
      }
      for (int iteration = 0; iteration < kIterations; ++iteration) {
        if (close_succeeded.load(std::memory_order_acquire)) break;
        auto begun = store.BeginSnapshot();
        if (!begun.ok()) {
          if (!begun.status().IsShutdownInProgress() &&
              !begun.status().IsInvalidArgument()) {
            unexpected.fetch_add(1, std::memory_order_relaxed);
          }
          continue;
        }
        StoreSnapshot snapshot = std::move(begun).ConsumeValueOrDie();
        live_handles.fetch_add(1, std::memory_order_acq_rel);
        const auto event = store.Read(snapshot, ref, ValidTime{1});
        if (!event.ok() || !event.ValueOrDie().has_value()) {
          unexpected.fetch_add(1, std::memory_order_relaxed);
        }
        live_handles.fetch_sub(1, std::memory_order_acq_rel);
        opened.fetch_add(1, std::memory_order_relaxed);
        std::this_thread::yield();
      }
    });
  }

  // Wait (bounded) until every reader has opened at least one snapshot so the
  // close attempt is guaranteed to race real epoch references.
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (opened.load(std::memory_order_acquire) <
             static_cast<uint64_t>(kReaders) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::yield();
  }

  Status closed;
  const auto close_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (std::chrono::steady_clock::now() < close_deadline) {
    closed = store.Close();
    if (closed.ok()) break;
    if (!closed.IsSnapshotPinned()) break;
    std::this_thread::yield();
  }
  // A successful close must not have raced a live handle into the teardown:
  // any handle alive at that moment would still own an epoch reference.
  if (closed.ok()) {
    EXPECT_EQ(live_handles.load(std::memory_order_acquire), 0U);
  }
  close_succeeded.store(true, std::memory_order_release);
  for (std::thread& reader : readers) reader.join();

  EXPECT_TRUE(closed.ok()) << closed.ToString();
  EXPECT_EQ(unexpected.load(std::memory_order_acquire), 0U);
  EXPECT_GT(opened.load(std::memory_order_acquire), 0U);
  EXPECT_EQ(live_handles.load(std::memory_order_acquire), 0U);
  // Idempotent close on an already-closed store.
  EXPECT_TRUE(store.Close().ok());
}

// C2 regression: repeated Open/Close on the same FactStore must not accumulate
// retained closed-store shells, and every retained shell must be inert - no
// validation caches, write index, identity history, statistics, property
// schemas, RocksDB handle, or snapshot lease. The single retained shell exists
// only so a lock-free pin that loaded the raw store pointer before the close
// gate can read accepting_fast and fail safely.
TEST_F(RocksDbLifecycleTest, RepeatedOpenCloseBoundsRetainedShells) {
  PopulateSource();
  FactStore store(FactStoreOptions{database_path_});
  for (int iteration = 0; iteration < 25; ++iteration) {
    const auto opened = store.Open();
    ASSERT_TRUE(opened.ok()) << opened.ToString();
    {
      const auto snapshot = store.BeginSnapshot();
      ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
      EXPECT_EQ(snapshot.ValueOrDie().commit_seq(), CommitSeq{2});
    }
    const Status closed = store.Close();
    ASSERT_TRUE(closed.ok()) << closed.ToString();
    // Bounded retention: at most the most recently closed shell is kept, and it
    // must have released everything heavy.
    EXPECT_LE(store.RetainedShellCountForTesting(), 1U)
        << "iteration " << iteration;
    EXPECT_TRUE(store.RetainedShellsAreInertForTesting())
        << "iteration " << iteration;
  }
  // Reopening after the bounded-retention closes still serves reads.
  const auto reopened = store.Open();
  ASSERT_TRUE(reopened.ok()) << reopened.ToString();
  const auto event = store.ReadCurrent(
      EntityFact::Vertex(VertexRef{PartId{0}, VertexId{101}}).ref(),
      ValidTime{1000});
  ASSERT_TRUE(event.ok()) << event.status().ToString();
  ASSERT_TRUE(event.ValueOrDie().has_value());
  EXPECT_EQ(event.ValueOrDie()->ref.entity_id(), 101U);
  EXPECT_EQ(event.ValueOrDie()->commit_seq, CommitSeq{1});
  EXPECT_TRUE(store.Close().ok());
  EXPECT_LE(store.RetainedShellCountForTesting(), 1U);
  EXPECT_TRUE(store.RetainedShellsAreInertForTesting());
}

// UAF regression: Close used to destroy `db`, `statistics`,
// `canonical_page_metrics`, and the property schema catalog as soon as the epoch
// reference count was zero. Storage-level non-epoch users - CreateCheckpoint,
// SampleRuntime, SampleValidationCacheMetrics, RunNativeMaintenance - hold only a
// shared_ptr<FactStoreImpl> and are invisible to the epoch registry, so they
// could still be reading that state when it was released (5d2a3af behavior).
//
// The drain handshake registers such an operation under lifecycle_mutex_, the
// same mutex Close holds for its entire handshake. The observer below parks the
// operation after registration; Close must therefore stay blocked until the
// registration is dropped. No new operation can register meanwhile (it would
// need lifecycle_mutex_), so a single zero observation is final.
TEST_F(RocksDbLifecycleTest,
       CloseDrainsStorageLevelMetricsSampleBeforeTeardown) {
  FactStoreOptions options{database_path_};
  std::atomic<bool> op_registered{false};
  std::atomic<bool> release_op{false};
  std::atomic<bool> close_started{false};
  std::atomic<bool> close_finished{false};
  options.non_epoch_op_observer_for_testing = [&] {
    op_registered.store(true, std::memory_order_release);
    while (!release_op.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  };
  FactStore store(options);
  ASSERT_TRUE(store.Open().ok());
  ASSERT_TRUE(store.Commit(Batch(1, 101)).ok());

  Status sample_status;
  std::thread sampler([&] {
    const auto sampled = store.SampleValidationCacheMetrics();
    sample_status = sampled.status();
  });
  const auto registration_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!op_registered.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < registration_deadline) {
    std::this_thread::yield();
  }
  ASSERT_TRUE(op_registered.load(std::memory_order_acquire));
  ASSERT_EQ(store.NonEpochUsersForTesting(), 1U);

  Status close_status;
  std::thread closer([&] {
    close_started.store(true, std::memory_order_release);
    close_status = store.Close();
    close_finished.store(true, std::memory_order_release);
  });
  const auto close_start_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!close_started.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < close_start_deadline) {
    std::this_thread::yield();
  }
  ASSERT_TRUE(close_started.load(std::memory_order_acquire));

  // Bounded liveness check: a close without the drain finishes in microseconds;
  // this close must not finish while the operation is registered. The window is
  // only a bound against a hang, not the synchronization itself.
  const auto drained_deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(200);
  while (!close_finished.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < drained_deadline) {
    std::this_thread::yield();
  }
  EXPECT_FALSE(close_finished.load(std::memory_order_acquire))
      << "Close released store state while a non-epoch sample was in flight";

  release_op.store(true, std::memory_order_release);
  sampler.join();
  closer.join();

  EXPECT_TRUE(sample_status.ok()) << sample_status.ToString();
  EXPECT_TRUE(close_status.ok()) << close_status.ToString();
  EXPECT_EQ(store.NonEpochUsersForTesting(), 0U);
  // Once the op drained, the teardown completed normally and idempotent close
  // still succeeds.
  EXPECT_TRUE(store.Close().ok());
  EXPECT_TRUE(store.RetainedShellsAreInertForTesting());
}

// Task 12 final re-review: Database::BeginTransaction starts with
// FactStore::AllocateTransactionId, whose lease slow path writes allocator
// metadata through store->db. That path was invisible to Close (no epoch
// reference and no non-epoch registration), so a close racing a transaction
// start could destroy db before the allocation wrote through it.
//
// The observer parks the allocation after its registration; with the guard,
// Close cannot finish until the allocation drains. Disabling the drain (or the
// registration) makes Close finish while the allocation is parked, so this
// test fails; the resumed allocation then uses the freed RocksDB handle under
// ASan.
TEST_F(RocksDbLifecycleTest,
       CloseDrainsTransactionIdAllocationBeforeTeardown) {
  FactStoreOptions options{database_path_};
  std::atomic<bool> op_registered{false};
  std::atomic<bool> release_op{false};
  std::atomic<bool> close_started{false};
  std::atomic<bool> close_finished{false};
  options.non_epoch_op_observer_for_testing = [&] {
    op_registered.store(true, std::memory_order_release);
    while (!release_op.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
  };
  FactStore store(options);
  ASSERT_TRUE(store.Open().ok());

  Status allocate_status;
  std::optional<TxnId> allocated;
  std::thread starter([&] {
    auto result = store.AllocateTransactionId();
    allocate_status = result.status();
    if (result.ok()) allocated = std::move(result).ConsumeValueOrDie();
  });
  const auto registration_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!op_registered.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < registration_deadline) {
    std::this_thread::yield();
  }
  ASSERT_TRUE(op_registered.load(std::memory_order_acquire));
  ASSERT_EQ(store.NonEpochUsersForTesting(), 1U);

  Status close_status;
  std::thread closer([&] {
    close_started.store(true, std::memory_order_release);
    close_status = store.Close();
    close_finished.store(true, std::memory_order_release);
  });
  const auto close_start_deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!close_started.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < close_start_deadline) {
    std::this_thread::yield();
  }
  ASSERT_TRUE(close_started.load(std::memory_order_acquire));

  // Bounded liveness check: a close without the drain finishes in
  // microseconds; this close must not finish while the allocation is
  // registered. The window is only a bound against a hang, not the
  // synchronization itself.
  const auto drained_deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(200);
  while (!close_finished.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < drained_deadline) {
    std::this_thread::yield();
  }
  EXPECT_FALSE(close_finished.load(std::memory_order_acquire))
      << "Close released RocksDB state while a transaction-ID allocation was "
         "in flight";

  release_op.store(true, std::memory_order_release);
  starter.join();
  closer.join();

  EXPECT_TRUE(allocate_status.ok()) << allocate_status.ToString();
  ASSERT_TRUE(allocated.has_value());
  EXPECT_NE(allocated->value, 0U);
  EXPECT_TRUE(close_status.ok()) << close_status.ToString();
  EXPECT_EQ(store.NonEpochUsersForTesting(), 0U);
  // A successful close is final: a transaction start that arrives afterwards
  // must fail without touching store state.
  const auto late = store.AllocateTransactionId();
  EXPECT_TRUE(late.status().IsInvalidArgument()) << late.status().ToString();
  EXPECT_TRUE(store.RetainedShellsAreInertForTesting());
}

}  // namespace
}  // namespace cedar
