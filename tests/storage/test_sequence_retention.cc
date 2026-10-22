// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

// F14: sequence-record retention below the Vacuum watermark.
//
// The tests pin four contracts:
//  1. Default behavior is unchanged: no watermark record, no delete, and a
//     contiguous [1..visible] reopen for every database that never opts in.
//  2. With the option enabled, Vacuum persists the durable
//     `watermark/sequence-retention` boundary before the first delete, removes
//     only records below that boundary in bounded (<=1024-key) batches, and is
//     idempotent.
//  3. Reopen validation starts at the durable watermark: a missing prefix below
//     it is accepted, while any gap at or above it is still corruption.
//  4. Crash windows around the prune (before the watermark, after it, inside a
//     delete batch, after fact cleanup) all reopen into a consistent state and
//     a later open or Vacuum finishes the interrupted prune.

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "cedar/database.h"
#include "cedar/fact/fact_codec.h"
#include "cedar/fact/meta_codec.h"
#include "storage/facts/fact_store.h"
#include "storage/rocks/rocksdb_config.h"

#include <rocksdb/db.h>
#include <rocksdb/options.h>

namespace cedar {
namespace {

constexpr char kSequenceKeyPrefix[] = "sequence/";
constexpr size_t kSequenceKeyPrefixBytes = sizeof(kSequenceKeyPrefix) - 1;
constexpr size_t kSequenceKeyBytes = kSequenceKeyPrefixBytes + sizeof(uint64_t);
constexpr int kPruneCrashExitCode = 93;

StoreCommitBatch VertexStateBatch(TxnId txn_id, uint64_t vertex_id,
                                  uint64_t valid_time) {
  return StoreCommitBatch{
      txn_id,
      valid_time,
      {{EntityFact::Vertex(VertexRef{PartId{0}, VertexId{vertex_id}}).ref(),
        ValidTime{valid_time}, FactOperation::kPut, 0, std::nullopt}},
      {},
      {},
      {},
      false};
}

StoreCommitBatch VertexFactsBatch(TxnId txn_id, uint64_t first_vertex,
                                  uint64_t fact_count, uint64_t valid_time,
                                  bool retract) {
  StoreCommitBatch batch{txn_id, valid_time, {}, {}, {}, {}, false};
  batch.mutations.reserve(fact_count);
  for (uint64_t offset = 0; offset < fact_count; ++offset) {
    batch.mutations.push_back(PendingFactMutation{
        EntityFact::Vertex(VertexRef{PartId{0}, VertexId{first_vertex + offset}})
            .ref(),
        ValidTime{valid_time},
        retract ? FactOperation::kDelete : FactOperation::kPut, 0,
        std::nullopt});
  }
  return batch;
}

// Vanilla inspection handle. Cedar's facts SSTs use the Cedar Parquet table
// factory, so reuse the store's column-family options; the flush-suppression
// flags keep the inspection from rewriting the files the next Cedar open sees.
struct RawDatabase {
  std::unique_ptr<rocksdb::DB> db;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;

  ~RawDatabase() {
    for (rocksdb::ColumnFamilyHandle* handle : handles) {
      if (db) db->DestroyColumnFamilyHandle(handle);
    }
  }
};

bool OpenRawDatabase(const std::string& path, RawDatabase* out) {
  FactStoreOptions store_options;
  store_options.path = path;
  std::shared_ptr<rocksdb::Cache> cache;
  rocksdb::Options options = internal::MakeRocksDbOptions(
      store_options, /*is_new_database=*/false, /*resolved=*/nullptr, &cache);
  options.create_if_missing = false;
  options.create_missing_column_families = false;
  options.atomic_flush = true;
  options.avoid_flush_during_shutdown = true;
  options.avoid_flush_during_recovery = true;
  auto page_metrics =
      std::make_shared<rocksdb::cedar_parquet::CanonicalPageCacheMetrics>();
  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors =
      internal::MakeRocksDbColumnFamilyDescriptors(
          store_options, options, /*resolved=*/nullptr, {cache, page_metrics});
  const rocksdb::Status opened = rocksdb::DB::Open(
      options, path, descriptors, &out->handles, &out->db);
  if (!opened.ok()) {
    ADD_FAILURE() << "raw database open failed: " << opened.ToString();
    return false;
  }
  return true;
}

std::optional<std::string> ReadRawMetaRecord(const std::string& path,
                                             const std::string& key) {
  RawDatabase raw;
  if (!OpenRawDatabase(path, &raw)) return std::nullopt;
  std::string encoded;
  const rocksdb::Status got =
      raw.db->Get(rocksdb::ReadOptions(), raw.handles[2], key, &encoded);
  if (got.IsNotFound()) return std::nullopt;
  if (!got.ok()) {
    ADD_FAILURE() << got.ToString();
    return std::nullopt;
  }
  return encoded;
}

void PutRawMetaRecord(const std::string& path, const std::string& key,
                      const std::string& value) {
  RawDatabase raw;
  if (!OpenRawDatabase(path, &raw)) return;
  ASSERT_TRUE(
      raw.db->Put(rocksdb::WriteOptions(), raw.handles[2], key, value).ok());
}

void DeleteRawMetaRecord(const std::string& path, const std::string& key) {
  RawDatabase raw;
  if (!OpenRawDatabase(path, &raw)) return;
  ASSERT_TRUE(
      raw.db->Delete(rocksdb::WriteOptions(), raw.handles[2], key).ok());
}

uint64_t ParseSequenceKey(rocksdb::Slice key) {
  uint64_t sequence = 0;
  for (size_t index = kSequenceKeyPrefixBytes; index < kSequenceKeyBytes;
       ++index) {
    sequence = (sequence << 8) | static_cast<uint8_t>(key[index]);
  }
  return sequence;
}

// Every surviving `sequence/<seq>` key in ascending order.
std::vector<uint64_t> SequenceKeys(const std::string& path) {
  std::vector<uint64_t> keys;
  RawDatabase raw;
  if (!OpenRawDatabase(path, &raw)) return keys;
  std::unique_ptr<rocksdb::Iterator> iterator(
      raw.db->NewIterator(rocksdb::ReadOptions(), raw.handles[2]));
  for (iterator->Seek(kSequenceKeyPrefix);
       iterator->Valid() &&
       std::string_view(iterator->key().data(), kSequenceKeyPrefixBytes) ==
           kSequenceKeyPrefix;
       iterator->Next()) {
    if (iterator->key().size() != kSequenceKeyBytes) {
      ADD_FAILURE() << "non-canonical sequence key";
      return {};
    }
    keys.push_back(ParseSequenceKey(iterator->key()));
  }
  EXPECT_TRUE(iterator->status().ok());
  return keys;
}

// Key plus value bytes of every surviving sequence record. This is the logical
// "deleted bytes" accounting the F14 acceptance asks for; it deliberately does
// not include RocksDB block/SST overhead.
size_t SequenceRecordBytes(const std::string& path) {
  RawDatabase raw;
  if (!OpenRawDatabase(path, &raw)) return 0;
  size_t bytes = 0;
  std::unique_ptr<rocksdb::Iterator> iterator(
      raw.db->NewIterator(rocksdb::ReadOptions(), raw.handles[2]));
  for (iterator->Seek(kSequenceKeyPrefix);
       iterator->Valid() &&
       std::string_view(iterator->key().data(), kSequenceKeyPrefixBytes) ==
           kSequenceKeyPrefix;
       iterator->Next()) {
    bytes += iterator->key().size() + iterator->value().size();
  }
  return bytes;
}

std::optional<CommitSeq> ReadRetentionWatermark(const std::string& path) {
  const auto encoded =
      ReadRawMetaRecord(path, EncodeSequenceRetentionWatermarkKey());
  if (!encoded.has_value()) return std::nullopt;
  const auto decoded = DecodeWatermark(*encoded);
  if (!decoded.ok()) {
    ADD_FAILURE() << decoded.status().ToString();
    return std::nullopt;
  }
  return decoded.ValueOrDie();
}

// Key plus value bytes of one sequence record, for exact deleted-byte accounting.
std::optional<size_t> SequenceRecordBytesAt(const std::string& path,
                                            uint64_t sequence) {
  const auto key = EncodeSequenceMetaKey(CommitSeq{sequence});
  if (!key.ok()) {
    ADD_FAILURE() << key.status().ToString();
    return std::nullopt;
  }
  RawDatabase raw;
  if (!OpenRawDatabase(path, &raw)) return std::nullopt;
  std::string value;
  const rocksdb::Status got =
      raw.db->Get(rocksdb::ReadOptions(), raw.handles[2], key.ValueOrDie(),
                  &value);
  if (!got.ok()) {
    ADD_FAILURE() << got.ToString();
    return std::nullopt;
  }
  return key.ValueOrDie().size() + value.size();
}

size_t CountFactKeys(const std::string& path) {
  RawDatabase raw;
  if (!OpenRawDatabase(path, &raw)) return 0;
  size_t count = 0;
  std::unique_ptr<rocksdb::Iterator> iterator(
      raw.db->NewIterator(rocksdb::ReadOptions(), raw.handles[1]));
  for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) ++count;
  EXPECT_TRUE(iterator->status().ok());
  return count;
}

class SequenceRetentionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char pattern[] = "/tmp/cedar_sequence_retention_XXXXXX";
    ASSERT_NE(mkdtemp(pattern), nullptr);
    path_ = pattern;
  }

  void TearDown() override { std::filesystem::remove_all(path_); }

  std::string SubPath(const std::string& name) const {
    return path_ + "/" + name;
  }

  std::unique_ptr<FactStore> OpenStore(const FactStoreOptions& options) {
    auto store = std::make_unique<FactStore>(options);
    const Status opened = store->Open();
    EXPECT_TRUE(opened.ok()) << opened.ToString();
    if (!opened.ok()) return nullptr;
    return store;
  }

  // Opens a store whose failure is part of the assertion (for example a pruned
  // history with a gap at the watermark). The status is returned through
  // `status` so the caller can pin the corruption message.
  std::unique_ptr<FactStore> TryOpenStore(const FactStoreOptions& options,
                                          Status* status = nullptr) {
    auto store = std::make_unique<FactStore>(options);
    const Status opened = store->Open();
    if (status != nullptr) *status = opened;
    if (!opened.ok()) return nullptr;
    return store;
  }

  FactStoreOptions PruningOptions(const std::string& path) const {
    FactStoreOptions options;
    options.path = path;
    options.prune_sequence_records_on_vacuum = true;
    return options;
  }

  void CommitVertices(FactStore* store, uint64_t first_txn,
                      uint64_t first_vertex, uint64_t count) {
    for (uint64_t index = 0; index < count; ++index) {
      ASSERT_TRUE(store
                      ->Commit(VertexStateBatch(TxnId{first_txn + index},
                                                first_vertex + index, 10))
                      .ok());
    }
  }

  std::string path_;
};

TEST_F(SequenceRetentionTest, DefaultVacuumKeepsHistoryAndWritesNoWatermark) {
  const std::string path = SubPath("default");
  auto store = OpenStore(FactStoreOptions{path});
  ASSERT_NE(store, nullptr);
  CommitVertices(store.get(), 1, 1, 4);
  ASSERT_TRUE(store->Vacuum(CommitSeq{2}).ok());
  ASSERT_TRUE(store->Close().ok());
  store.reset();

  EXPECT_EQ(SequenceKeys(path).size(), 4U);
  EXPECT_FALSE(ReadRetentionWatermark(path).has_value());

  // The default reopen contract still validates the full contiguous history,
  // and sequence reads below the oldest-readable boundary remain available
  // through the explicit snapshot path (Vacuum only removed facts there).
  store = OpenStore(FactStoreOptions{path});
  ASSERT_NE(store, nullptr);
  {
    const auto snapshot = store->BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    const auto first =
        store->ReadSequence(snapshot.ValueOrDie(), CommitSeq{1});
    ASSERT_TRUE(first.ok()) << first.status().ToString();
    EXPECT_EQ(first.ValueOrDie().fact_keys.size(), 1U);
  }
  ASSERT_TRUE(store->Close().ok());
}

TEST_F(SequenceRetentionTest, VacuumPrunesBelowWatermarkAndReopenToleratesPrefix) {
  const std::string path = SubPath("functional");
  auto store = OpenStore(FactStoreOptions{path});
  ASSERT_NE(store, nullptr);
  CommitVertices(store.get(), 1, 1, 4);
  ASSERT_TRUE(store->Close().ok());
  store.reset();
  const size_t bytes_before = SequenceRecordBytes(path);
  const auto pruned_one_bytes = SequenceRecordBytesAt(path, 1);
  const auto pruned_two_bytes = SequenceRecordBytesAt(path, 2);
  ASSERT_TRUE(pruned_one_bytes.has_value());
  ASSERT_TRUE(pruned_two_bytes.has_value());

  store = OpenStore(PruningOptions(path));
  ASSERT_NE(store, nullptr);
  ASSERT_TRUE(store->Vacuum(CommitSeq{3}).ok());
  ASSERT_TRUE(store->Close().ok());
  store.reset();

  const std::vector<uint64_t> surviving = SequenceKeys(path);
  ASSERT_EQ(surviving.size(), 2U);
  EXPECT_EQ(surviving[0], 3U);
  EXPECT_EQ(surviving[1], 4U);
  const auto watermark = ReadRetentionWatermark(path);
  ASSERT_TRUE(watermark.has_value());
  EXPECT_EQ(watermark->value, 3U);
  // The logical bytes Vacuum removed are exactly the two pruned records.
  EXPECT_EQ(bytes_before - SequenceRecordBytes(path),
            *pruned_one_bytes + *pruned_two_bytes);

  // A default-policy open must accept the pruned prefix: the durable watermark,
  // not the caller's option, is what excuses the missing records.
  store = OpenStore(FactStoreOptions{path});
  ASSERT_NE(store, nullptr);
  EXPECT_EQ(store->visible_seq(), CommitSeq{4});
  {
    const auto snapshot = store->BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    const auto pruned =
        store->ReadSequence(snapshot.ValueOrDie(), CommitSeq{1});
    EXPECT_TRUE(pruned.status().IsSnapshotExpired())
        << pruned.status().ToString();
    const auto at_boundary =
        store->ReadSequence(snapshot.ValueOrDie(), CommitSeq{3});
    ASSERT_TRUE(at_boundary.ok()) << at_boundary.status().ToString();
    const auto below_range = store->ReadSequenceRange(
        snapshot.ValueOrDie(), CommitSeq{2}, CommitSeq{3});
    EXPECT_TRUE(below_range.status().IsSnapshotExpired())
        << below_range.status().ToString();
    const auto retained_range = store->ReadSequenceRange(
        snapshot.ValueOrDie(), CommitSeq{3}, CommitSeq{4});
    ASSERT_TRUE(retained_range.ok()) << retained_range.status().ToString();
    EXPECT_EQ(retained_range.ValueOrDie().size(), 2U);
  }
  ASSERT_TRUE(store->Close().ok());
}

TEST_F(SequenceRetentionTest, ReopenRequiresContiguityAtOrAboveWatermark) {
  // Gap at the watermark itself: still corruption.
  {
    const std::string path = SubPath("gap_at_watermark");
    auto store = OpenStore(PruningOptions(path));
    ASSERT_NE(store, nullptr);
    CommitVertices(store.get(), 1, 1, 4);
    ASSERT_TRUE(store->Vacuum(CommitSeq{3}).ok());
    ASSERT_TRUE(store->Close().ok());
    store.reset();
    const auto key = EncodeSequenceMetaKey(CommitSeq{3});
    ASSERT_TRUE(key.ok());
    DeleteRawMetaRecord(path, key.ValueOrDie());

    Status opened;
    store = TryOpenStore(FactStoreOptions{path}, &opened);
    EXPECT_EQ(store.get(), nullptr);
    EXPECT_TRUE(opened.IsCorruption()) << opened.ToString();
    EXPECT_NE(opened.ToString().find("missing durable sequence record"),
              std::string::npos)
        << opened.ToString();
  }

  // A watermark that declares the prefix pruned makes exactly those missing
  // records acceptable, even when more than the prune would have removed is
  // absent (the watermark is a lower bound, not an exact delete log).
  {
    const std::string path = SubPath("declared_prefix");
    // Reach the retention boundary without pruning, then declare the prefix
    // pruned by hand and delete records 1 and 2 raw.
    auto store = OpenStore(FactStoreOptions{path});
    ASSERT_NE(store, nullptr);
    CommitVertices(store.get(), 1, 1, 4);
    ASSERT_TRUE(store->Vacuum(CommitSeq{3}).ok());
    ASSERT_TRUE(store->Close().ok());
    store.reset();
    const auto watermark = EncodeWatermark(CommitSeq{3});
    ASSERT_TRUE(watermark.ok());
    PutRawMetaRecord(path, EncodeSequenceRetentionWatermarkKey(),
                     watermark.ValueOrDie());
    for (uint64_t sequence = 1; sequence <= 2; ++sequence) {
      const auto key = EncodeSequenceMetaKey(CommitSeq{sequence});
      ASSERT_TRUE(key.ok());
      DeleteRawMetaRecord(path, key.ValueOrDie());
    }

    store = OpenStore(FactStoreOptions{path});
    ASSERT_NE(store, nullptr);
    {
      const auto snapshot = store->BeginSnapshot();
      ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
      EXPECT_TRUE(store->ReadSequence(snapshot.ValueOrDie(), CommitSeq{2})
                      .status()
                      .IsSnapshotExpired());
      ASSERT_TRUE(store->ReadSequence(snapshot.ValueOrDie(), CommitSeq{3}).ok());
    }
    ASSERT_TRUE(store->Close().ok());
  }

  // A watermark above the oldest-readable boundary is an inconsistent durable
  // state: reject it instead of guessing which contract wins.
  {
    const std::string path = SubPath("watermark_above_boundary");
    auto store = OpenStore(FactStoreOptions{path});
    ASSERT_NE(store, nullptr);
    CommitVertices(store.get(), 1, 1, 2);
    ASSERT_TRUE(store->Close().ok());
    store.reset();
    const auto watermark = EncodeWatermark(CommitSeq{2});
    ASSERT_TRUE(watermark.ok());
    PutRawMetaRecord(path, EncodeSequenceRetentionWatermarkKey(),
                     watermark.ValueOrDie());

    store = TryOpenStore(FactStoreOptions{path});
    EXPECT_EQ(store.get(), nullptr);
  }
}

TEST_F(SequenceRetentionTest, PruneIsBatchedIdempotentAndBounded) {
  // A small batch override lets the test cross two real batch boundaries with
  // few commits; the production bound (1024) is clamped inside the store.
  constexpr uint64_t kCommits = 40;
  constexpr uint64_t kBoundary = 37;
  const std::string path = SubPath("batched");
  FactStoreOptions options = PruningOptions(path);
  options.sequence_prune_batch_size_for_testing = 8;
  int prune_batches = 0;
  options.vacuum_fault_injector_for_testing =
      [&prune_batches](VacuumFaultPoint point) {
        if (point == VacuumFaultPoint::kAfterSequencePruneBatch) {
          ++prune_batches;
        }
        return Status::OK();
      };
  auto store = OpenStore(options);
  ASSERT_NE(store, nullptr);
  CommitVertices(store.get(), 1, 1, kCommits);
  ASSERT_TRUE(store->Vacuum(CommitSeq{kBoundary}).ok());
  ASSERT_TRUE(store->Close().ok());
  store.reset();
  // 36 records are below the boundary, i.e. five batches of eight.
  EXPECT_EQ(prune_batches, 5);
  std::vector<uint64_t> surviving = SequenceKeys(path);
  ASSERT_EQ(surviving.size(), kCommits - kBoundary + 1);
  EXPECT_EQ(surviving.front(), kBoundary);
  EXPECT_EQ(surviving.back(), kCommits);

  // A repeated Vacuum at the same target is a no-op, and so is a reopen: there
  // is nothing left below the watermark, so no delete batch runs.
  prune_batches = 0;
  store = OpenStore(options);
  ASSERT_NE(store, nullptr);
  ASSERT_TRUE(store->Vacuum(CommitSeq{kBoundary}).ok());
  ASSERT_TRUE(store->Close().ok());
  store.reset();
  EXPECT_EQ(prune_batches, 0);
  EXPECT_EQ(SequenceKeys(path), surviving);
}

TEST_F(SequenceRetentionTest, PinnedSnapshotRefusalDoesNotPrune) {
  const std::string path = SubPath("pinned");
  FactStoreOptions options = PruningOptions(path);
  auto store = OpenStore(options);
  ASSERT_NE(store, nullptr);
  CommitVertices(store.get(), 1, 1, 3);
  {
    auto pinned = store->BeginSnapshot(SnapshotOptions{CommitSeq{1}});
    ASSERT_TRUE(pinned.ok()) << pinned.status().ToString();
    const Status refused = store->Vacuum(CommitSeq{2});
    EXPECT_TRUE(refused.IsSnapshotPinned()) << refused.ToString();
    // The refused Vacuum must not have advanced the boundary or pruned; the
    // pinned snapshot is released only here, so Close below can succeed.
  }
  ASSERT_TRUE(store->Close().ok());
  store.reset();

  // The refused Vacuum published neither the boundary nor the watermark and
  // deleted nothing.
  EXPECT_FALSE(ReadRetentionWatermark(path).has_value());
  EXPECT_EQ(SequenceKeys(path).size(), 3U);
}

TEST_F(SequenceRetentionTest, CrashBeforePruneWatermarkLeavesHistoryContiguous) {
  const std::string path = SubPath("crash_before_watermark");
  FactStoreOptions options = PruningOptions(path);
  bool injected = false;
  options.vacuum_fault_injector_for_testing =
      [&injected](VacuumFaultPoint point) {
        if (point == VacuumFaultPoint::kBeforeSequencePruneWatermark &&
            !injected) {
          injected = true;
          return Status::Indeterminate("test", "crash before prune watermark");
        }
        return Status::OK();
      };
  auto store = OpenStore(options);
  ASSERT_NE(store, nullptr);
  CommitVertices(store.get(), 1, 1, 4);
  const Status vacuumed = store->Vacuum(CommitSeq{2});
  EXPECT_TRUE(vacuumed.IsIndeterminate()) << vacuumed.ToString();
  EXPECT_TRUE(injected);
  ASSERT_TRUE(store->Close().ok());
  store.reset();

  // The oldest-readable boundary is durable but the sequence watermark is not
  // and no record was deleted, so the default contiguous reopen still passes.
  EXPECT_FALSE(ReadRetentionWatermark(path).has_value());
  EXPECT_EQ(SequenceKeys(path).size(), 4U);
  store = OpenStore(FactStoreOptions{path});
  ASSERT_NE(store, nullptr);
  {
    const auto snapshot = store->BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    ASSERT_TRUE(store->ReadSequence(snapshot.ValueOrDie(), CommitSeq{1}).ok());
  }
  ASSERT_TRUE(store->Close().ok());
  store.reset();

  // An opted-in open resumes the interrupted prune at the published boundary.
  store = OpenStore(PruningOptions(path));
  ASSERT_NE(store, nullptr);
  ASSERT_TRUE(store->Close().ok());
  store.reset();
  const auto watermark = ReadRetentionWatermark(path);
  ASSERT_TRUE(watermark.has_value());
  EXPECT_EQ(watermark->value, 2U);
  const std::vector<uint64_t> surviving = SequenceKeys(path);
  ASSERT_EQ(surviving.size(), 3U);
  EXPECT_EQ(surviving.front(), 2U);
}

TEST_F(SequenceRetentionTest, CrashAfterPruneWatermarkToleratesMissingPrefix) {
  const std::string path = SubPath("crash_after_watermark");
  FactStoreOptions options = PruningOptions(path);
  bool injected = false;
  options.vacuum_fault_injector_for_testing =
      [&injected](VacuumFaultPoint point) {
        if (point == VacuumFaultPoint::kAfterSequencePruneWatermark &&
            !injected) {
          injected = true;
          return Status::Indeterminate("test", "crash after prune watermark");
        }
        return Status::OK();
      };
  auto store = OpenStore(options);
  ASSERT_NE(store, nullptr);
  CommitVertices(store.get(), 1, 1, 4);
  const Status vacuumed = store->Vacuum(CommitSeq{3});
  EXPECT_TRUE(vacuumed.IsIndeterminate()) << vacuumed.ToString();
  ASSERT_TRUE(store->Close().ok());
  store.reset();

  // The watermark is durable but the deletes have not run: extra history below
  // it is allowed but reads below it are already expired.
  const auto watermark = ReadRetentionWatermark(path);
  ASSERT_TRUE(watermark.has_value());
  EXPECT_EQ(watermark->value, 3U);
  EXPECT_EQ(SequenceKeys(path).size(), 4U);
  store = OpenStore(FactStoreOptions{path});
  ASSERT_NE(store, nullptr);
  {
    const auto snapshot = store->BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    EXPECT_TRUE(store->ReadSequence(snapshot.ValueOrDie(), CommitSeq{1})
                    .status()
                    .IsSnapshotExpired());
    ASSERT_TRUE(store->ReadSequence(snapshot.ValueOrDie(), CommitSeq{3}).ok());
  }
  ASSERT_TRUE(store->Close().ok());
  store.reset();

  // The opted-in open finishes the prune; the surviving history is unchanged
  // above the boundary.
  store = OpenStore(PruningOptions(path));
  ASSERT_NE(store, nullptr);
  ASSERT_TRUE(store->Close().ok());
  store.reset();
  const std::vector<uint64_t> surviving = SequenceKeys(path);
  ASSERT_EQ(surviving.size(), 2U);
  EXPECT_EQ(surviving.front(), 3U);
}

// A real process death in the first delete batch: the durable watermark (written
// with Sync=true before any delete) and the first batch are replayed by reopen.
TEST_F(SequenceRetentionTest, ProcessCrashDuringFirstPruneBatchReopensSafely) {
  constexpr uint64_t kCommits = 40;
  constexpr uint64_t kBoundary = 30;
  constexpr uint64_t kBatch = 8;
  const std::string path = SubPath("process_crash_batch");
  auto store = OpenStore(FactStoreOptions{path});
  ASSERT_NE(store, nullptr);
  CommitVertices(store.get(), 1, 1, kCommits);
  ASSERT_TRUE(store->Close().ok());
  store.reset();

  const pid_t child = fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    FactStoreOptions child_options;
    child_options.path = path;
    child_options.prune_sequence_records_on_vacuum = true;
    child_options.sequence_prune_sync = true;
    child_options.sequence_prune_batch_size_for_testing = kBatch;
    child_options.vacuum_fault_injector_for_testing =
        [](VacuumFaultPoint point) {
          if (point == VacuumFaultPoint::kAfterSequencePruneBatch) {
            _exit(kPruneCrashExitCode);
          }
          return Status::OK();
        };
    FactStore child_store(child_options);
    if (!child_store.Open().ok()) _exit(94);
    const Status vacuumed = child_store.Vacuum(CommitSeq{kBoundary});
    // The first batch is one of four; the fault must have killed this process.
    _exit(vacuumed.ok() ? 95 : 96);
  }
  int status = 0;
  ASSERT_EQ(waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), kPruneCrashExitCode);

  // Reopen under the default policy first: validation starts at the durable
  // watermark, so the eight already-deleted records do not look like a gap.
  const auto watermark = ReadRetentionWatermark(path);
  ASSERT_TRUE(watermark.has_value());
  EXPECT_EQ(watermark->value, kBoundary);
  EXPECT_EQ(SequenceKeys(path).size(), kCommits - kBatch);
  store = OpenStore(FactStoreOptions{path});
  ASSERT_NE(store, nullptr);
  EXPECT_EQ(store->visible_seq(), CommitSeq{kCommits});
  {
    const auto snapshot = store->BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    // The surviving records below the watermark are already expired to readers.
    EXPECT_TRUE(store->ReadSequence(snapshot.ValueOrDie(),
                                    CommitSeq{kBoundary - 1})
                    .status()
                    .IsSnapshotExpired());
    ASSERT_TRUE(store->ReadSequence(snapshot.ValueOrDie(),
                                    CommitSeq{kBoundary})
                    .ok());
  }
  ASSERT_TRUE(store->Close().ok());
  store.reset();

  // An opted-in open finishes the records below the watermark that the crash
  // left behind.
  store = OpenStore(PruningOptions(path));
  ASSERT_NE(store, nullptr);
  ASSERT_TRUE(store->Close().ok());
  store.reset();
  const std::vector<uint64_t> surviving = SequenceKeys(path);
  ASSERT_EQ(surviving.size(), kCommits - kBoundary + 1);
  EXPECT_EQ(surviving.front(), kBoundary);
}

// A crash between the durable watermark write and the first delete leaves every
// record in place; both the tolerant default reopen and the resuming opted-in
// reopen must accept it.
TEST_F(SequenceRetentionTest, ProcessCrashAfterPruneWatermarkReopensSafely) {
  const std::string path = SubPath("process_crash_watermark");
  auto store = OpenStore(FactStoreOptions{path});
  ASSERT_NE(store, nullptr);
  CommitVertices(store.get(), 1, 1, 4);
  ASSERT_TRUE(store->Close().ok());
  store.reset();

  const pid_t child = fork();
  ASSERT_GE(child, 0);
  if (child == 0) {
    FactStoreOptions child_options;
    child_options.path = path;
    child_options.prune_sequence_records_on_vacuum = true;
    child_options.sequence_prune_sync = true;
    child_options.vacuum_fault_injector_for_testing =
        [](VacuumFaultPoint point) {
          if (point == VacuumFaultPoint::kAfterSequencePruneWatermark) {
            _exit(kPruneCrashExitCode);
          }
          return Status::OK();
        };
    FactStore child_store(child_options);
    if (!child_store.Open().ok()) _exit(94);
    const Status vacuumed = child_store.Vacuum(CommitSeq{3});
    _exit(vacuumed.ok() ? 95 : 96);
  }
  int status = 0;
  ASSERT_EQ(waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), kPruneCrashExitCode);

  const auto watermark = ReadRetentionWatermark(path);
  ASSERT_TRUE(watermark.has_value());
  EXPECT_EQ(watermark->value, 3U);
  EXPECT_EQ(SequenceKeys(path).size(), 4U);

  store = OpenStore(FactStoreOptions{path});
  ASSERT_NE(store, nullptr);
  {
    const auto snapshot = store->BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    EXPECT_TRUE(store->ReadSequence(snapshot.ValueOrDie(), CommitSeq{2})
                    .status()
                    .IsSnapshotExpired());
    ASSERT_TRUE(store->ReadSequence(snapshot.ValueOrDie(), CommitSeq{4}).ok());
  }
  ASSERT_TRUE(store->Close().ok());
  store.reset();

  store = OpenStore(PruningOptions(path));
  ASSERT_NE(store, nullptr);
  ASSERT_TRUE(store->Close().ok());
  store.reset();
  EXPECT_EQ(SequenceKeys(path).size(), 2U);
}

// Interleaving with the fact-cleanup phase: a crash after one fact-delete batch
// leaves a durable Vacuum cursor. The next opted-in open must first resume the
// fact cleanup and then prune sequence records at the same target.
TEST_F(SequenceRetentionTest, FactCleanupCrashThenReopenResumesVacuumAndPrune) {
  constexpr uint64_t kFactCount = 1025;
  const std::string path = SubPath("fact_cleanup_crash");
  FactStoreOptions options = PruningOptions(path);
  bool injected = false;
  options.vacuum_fault_injector_for_testing =
      [&injected](VacuumFaultPoint point) {
        if (point == VacuumFaultPoint::kAfterCleanupBatch && !injected) {
          injected = true;
          return Status::Indeterminate("test", "crash after cleanup batch");
        }
        return Status::OK();
      };
  auto store = OpenStore(options);
  ASSERT_NE(store, nullptr);
  ASSERT_TRUE(store->Commit(VertexFactsBatch(TxnId{1}, 1, kFactCount, 10, false)).ok());
  ASSERT_TRUE(store->Commit(VertexFactsBatch(TxnId{2}, 1, kFactCount, 10, true)).ok());
  ASSERT_TRUE(store->Commit(VertexFactsBatch(TxnId{3}, 1, kFactCount, 10, false)).ok());
  const Status vacuumed = store->Vacuum(CommitSeq{2});
  EXPECT_TRUE(vacuumed.IsIndeterminate()) << vacuumed.ToString();
  EXPECT_TRUE(injected);
  ASSERT_TRUE(store->Close().ok());
  store.reset();

  EXPECT_FALSE(ReadRetentionWatermark(path).has_value());
  ASSERT_EQ(SequenceKeys(path).size(), 3U);

  store = OpenStore(PruningOptions(path));
  ASSERT_NE(store, nullptr);
  {
    const auto snapshot = store->BeginSnapshot();
    ASSERT_TRUE(snapshot.ok()) << snapshot.status().ToString();
    EXPECT_TRUE(store->ReadSequence(snapshot.ValueOrDie(), CommitSeq{1})
                    .status()
                    .IsSnapshotExpired());
    ASSERT_TRUE(store->ReadSequenceRange(snapshot.ValueOrDie(), CommitSeq{2},
                                         CommitSeq{3})
                    .ok());
  }
  ASSERT_TRUE(store->Close().ok());
  store.reset();

  const auto watermark = ReadRetentionWatermark(path);
  ASSERT_TRUE(watermark.has_value());
  EXPECT_EQ(watermark->value, 2U);
  const std::vector<uint64_t> surviving = SequenceKeys(path);
  ASSERT_EQ(surviving.size(), 2U);
  EXPECT_EQ(surviving[0], 2U);
  // Both Vacuum phases finished: one baseline per valid-time chain plus the
  // newer correction remain.
  EXPECT_EQ(CountFactKeys(path), 2U * kFactCount);
}

// Kernel plumbing: DatabaseOptions forwards the option, Database::Vacuum drives
// the prune, and the reopened Database still serves reads above the boundary.
TEST_F(SequenceRetentionTest, DatabaseVacuumPrunesThroughPublicOption) {
  const std::string path = SubPath("kernel");
  DatabaseOptions options;
  options.path = path;
  options.prune_sequence_records_on_vacuum = true;
  auto opened = Database::Open(std::move(options));
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  std::unique_ptr<Database> database = std::move(opened).ConsumeValueOrDie();
  const auto registered = database->RegisterProperty(PropertyDefinition{
      PropertyId{7}, 0, "value", PropertyEntityKind::kVertex,
      PhysicalType::kInt64, 4096});
  ASSERT_TRUE(registered.ok()) << registered.status().ToString();
  const PropertyFact property = PropertyFact::Vertex(
      VertexRef{PartId{0}, VertexId{4}}, PropertyId{7});
  for (uint64_t vertex = 1; vertex <= 4; ++vertex) {
    auto begun = database->BeginTransaction();
    ASSERT_TRUE(begun.ok()) << begun.status().ToString();
    std::unique_ptr<Transaction> transaction =
        std::move(begun).ConsumeValueOrDie();
    ASSERT_TRUE(transaction
                    ->Set(PropertyFact::Vertex(
                              VertexRef{PartId{0}, VertexId{vertex}},
                              PropertyId{7}),
                          ValidTime{10}, Value::Int64(vertex))
                    .ok());
    const auto committed = transaction->Commit();
    ASSERT_TRUE(committed.ok()) << committed.status().ToString();
    EXPECT_EQ(committed.ValueOrDie().outcome, CommitOutcome::kCommitted);
  }
  ASSERT_TRUE(database->Vacuum(CommitSeq{3}).ok());
  ASSERT_TRUE(database->Close().ok());
  database.reset();

  EXPECT_EQ(SequenceKeys(path).size(), 2U);
  const auto watermark = ReadRetentionWatermark(path);
  ASSERT_TRUE(watermark.has_value());
  EXPECT_EQ(watermark->value, 3U);

  DatabaseOptions reopen_options;
  reopen_options.path = path;
  opened = Database::Open(std::move(reopen_options));
  ASSERT_TRUE(opened.ok()) << opened.status().ToString();
  database = std::move(opened).ConsumeValueOrDie();
  EXPECT_TRUE(database->BeginSnapshot(SnapshotOptions{CommitSeq{2}})
                  .status()
                  .IsSnapshotExpired());
  {
    const auto current = database->BeginSnapshot();
    ASSERT_TRUE(current.ok()) << current.status().ToString();
    EXPECT_EQ(current.ValueOrDie().oldest_readable_seq(), CommitSeq{3});
    const auto point = database->GetPointAt(property.ref(), ValidTime{10});
    ASSERT_TRUE(point.ok()) << point.status().ToString();
    EXPECT_EQ(point.ValueOrDie(), std::optional<Value>{Value::Int64(4)});
  }
  ASSERT_TRUE(database->Close().ok());
}

}  // namespace
}  // namespace cedar
