// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/fact/meta_v2_reclaim.h"

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/write_batch.h>

#include "cedar/fact/fact_codec.h"
#include "cedar/fact/meta_codec.h"
#include "storage/facts/fact_store.h"
#include "storage/rocks/rocksdb_config.h"

namespace cedar {
namespace {

// Mirrors MetaRecordKind::kSequenceV2 in meta_codec.cc. Record framing is
// meta version(1) + kind(1), so the kind byte is enough to skip an
// already-reclaimed record without materializing its fact keys.
constexpr uint8_t kSequenceV2RecordKind = 9;

constexpr std::string_view kSequenceKeyPrefix = "sequence/";
constexpr size_t kCommitSeqKeyBytes = 8;

Status FromEngineStatus(const rocksdb::Status& status, const char* context) {
  if (status.ok()) return Status::OK();
  const std::string message = status.ToString();
  if (status.IsNotFound()) return Status::NotFound(context, message);
  if (status.IsCorruption()) return Status::Corruption(context, message);
  if (status.IsInvalidArgument()) {
    return Status::InvalidArgument(context, message);
  }
  if (status.IsNotSupported()) return Status::NotSupported(context, message);
  return Status::IOError(context, message);
}

// Owns the engine handles for the duration of one offline reclamation. The
// database is opened with Cedar's storage configuration so an existing facts
// column family (Cedar Parquet) can be recovered even though reclamation only
// reads and rewrites the meta column family. The caller supplies the storage
// profile and WAL placement of the database being reclaimed: recovering a
// database whose WAL lives on a separate device without that directory would
// silently miss every WAL-resident record, including the format marker.
struct ReclaimDatabase {
  std::unique_ptr<rocksdb::DB> db;
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  rocksdb::ColumnFamilyHandle* meta = nullptr;

  ~ReclaimDatabase() {
    if (!db) return;
    for (rocksdb::ColumnFamilyHandle* handle : handles) {
      if (handle != nullptr) db->DestroyColumnFamilyHandle(handle);
    }
  }
};

Status ValidateReclaimWalDirectory(const std::string& wal_directory) {
  if (wal_directory.empty()) return Status::OK();
  // Rules that FactStore applies to WAL placement (production-profile device
  // requirements, relative paths) must hold for the offline open as well.
  if (!std::filesystem::path(wal_directory).is_absolute()) {
    return Status::InvalidArgument("meta v2 reclaim",
                                   "WAL directory must be absolute");
  }
  return Status::OK();
}

Status OpenReclaimDatabase(const std::string& db_path,
                           const MetaV2ReclaimOptions& reclaim_options,
                           ReclaimDatabase* out) {
  FactStoreOptions store_options;
  store_options.path = db_path;
  store_options.storage_profile = reclaim_options.storage_profile;
  store_options.production = reclaim_options.production;
  const Status wal_placement = internal::ValidateProductionWalPlacement(store_options);
  if (!wal_placement.ok()) return wal_placement;
  const auto resolved_profile = internal::ResolveStorageProfile(store_options);
  if (!resolved_profile.ok()) return resolved_profile.status();
  const internal::ResolvedStorageProfile* resolved_ptr =
      UsesCedarKernelProfile(store_options.storage_profile)
          ? &resolved_profile.ValueOrDie()
          : nullptr;
  std::shared_ptr<rocksdb::Cache> cache;
  rocksdb::Options options = internal::MakeRocksDbOptions(
      store_options, /*is_new_database=*/false, resolved_ptr, &cache);
  options.create_if_missing = false;
  options.create_missing_column_families = false;
  // The WAL directory describes where the database physically keeps its log,
  // not a profile capacity, so honor it for every profile - including the
  // default Developer profile, whose engine options never place a WAL outside
  // the database directory. An empty value keeps the historical behavior.
  options.wal_dir = reclaim_options.production.wal_directory;
  // Reclamation must not rewrite the files the next Cedar open sees. Recovered
  // memtables stay in the WAL-backed memtable instead of being flushed through
  // this inspection configuration.
  options.atomic_flush = true;
  options.avoid_flush_during_shutdown = true;
  options.avoid_flush_during_recovery = true;
  auto page_metrics =
      std::make_shared<rocksdb::cedar_parquet::CanonicalPageCacheMetrics>();
  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors =
      internal::MakeRocksDbColumnFamilyDescriptors(
          store_options, options, resolved_ptr, {cache, page_metrics});
  const rocksdb::Status opened = rocksdb::DB::Open(
      options, db_path, descriptors, &out->handles, &out->db);
  if (!opened.ok()) return FromEngineStatus(opened, "open meta v2 reclaim");
  if (out->handles.size() != descriptors.size()) {
    return Status::Corruption("meta v2 reclaim",
                              "missing required column family handle");
  }
  for (size_t index = 0; index < descriptors.size(); ++index) {
    if (descriptors[index].name == "meta") out->meta = out->handles[index];
  }
  if (out->meta == nullptr) {
    return Status::Corruption("meta v2 reclaim", "database has no meta column family");
  }
  return Status::OK();
}

Status ParseSequenceCommitSeq(rocksdb::Slice key, CommitSeq* commit_seq) {
  const std::string_view bytes(key.data(), key.size());
  if (bytes.size() != kSequenceKeyPrefix.size() + kCommitSeqKeyBytes) {
    return Status::Corruption("meta v2 reclaim",
                              "malformed sequence metadata key");
  }
  uint64_t value = 0;
  for (size_t index = 0; index < kCommitSeqKeyBytes; ++index) {
    value = (value << 8) |
            static_cast<uint8_t>(bytes[kSequenceKeyPrefix.size() + index]);
  }
  const CommitSeq parsed{value};
  const auto canonical = EncodeSequenceMetaKey(parsed);
  if (!canonical.ok() ||
      std::string_view(canonical.ValueOrDie()) != bytes) {
    return Status::Corruption("meta v2 reclaim",
                              "non-canonical sequence metadata key");
  }
  *commit_seq = parsed;
  return Status::OK();
}

class SnapshotGuard {
 public:
  SnapshotGuard(rocksdb::DB* db, const rocksdb::Snapshot* snapshot)
      : db_(db), snapshot_(snapshot) {}
  ~SnapshotGuard() { Release(); }
  SnapshotGuard(const SnapshotGuard&) = delete;
  SnapshotGuard& operator=(const SnapshotGuard&) = delete;

  void Release() {
    if (db_ != nullptr && snapshot_ != nullptr) {
      db_->ReleaseSnapshot(snapshot_);
      snapshot_ = nullptr;
    }
  }

 private:
  rocksdb::DB* db_ = nullptr;
  const rocksdb::Snapshot* snapshot_ = nullptr;
};

struct PendingRewrite {
  std::string key;
  std::string value;
  uint64_t new_size = 0;
};

// Verifies that the compact encoding round-trips to the same SequenceRecord,
// including every rebuilt 32-byte fact key.
Status VerifyRewrite(const SequenceRecord& v1, const std::string& encoded_v2,
                     CommitSeq commit_seq) {
  const auto decoded_v2 = DecodeSequenceRecord(encoded_v2, commit_seq);
  if (!decoded_v2.ok()) return decoded_v2.status();
  const SequenceRecord& rewritten = decoded_v2.ValueOrDie();
  if (!(rewritten == v1)) {
    return Status::Corruption(
        "meta v2 reclaim",
        "rewritten sequence record does not match the source record");
  }
  if (rewritten.fact_keys.size() != v1.fact_keys.size()) {
    return Status::Corruption("meta v2 reclaim",
                              "rewritten sequence record changed its fact count");
  }
  for (size_t index = 0; index < v1.fact_keys.size(); ++index) {
    const std::string& rebuilt = rewritten.fact_keys[index];
    if (rebuilt.size() != kEncodedFactKeyBytes ||
        rebuilt != v1.fact_keys[index]) {
      return Status::Corruption(
          "meta v2 reclaim",
          "rewritten sequence record rebuilt a non-identical fact key");
    }
  }
  return Status::OK();
}

}  // namespace

Status ReclaimSequenceRecordsV2(const std::string& db_path,
                                const MetaV2ReclaimOptions& options,
                                MetaV2ReclaimStats* stats) {
  MetaV2ReclaimStats local_stats;
  MetaV2ReclaimStats& out = stats != nullptr ? *stats : local_stats;
  out = MetaV2ReclaimStats{};
  if (db_path.empty()) {
    return Status::InvalidArgument("meta v2 reclaim", "missing database path");
  }
  if (options.batch_records == 0) {
    return Status::InvalidArgument("meta v2 reclaim",
                                   "batch_records must be positive");
  }
  const Status wal_directory =
      ValidateReclaimWalDirectory(options.production.wal_directory);
  if (!wal_directory.ok()) return wal_directory;

  ReclaimDatabase database;
  const Status opened = OpenReclaimDatabase(db_path, options, &database);
  if (!opened.ok()) return opened;

  // Reclamation is only meaningful for a database whose durable marker already
  // claims format 2. Reading it before the first write is also the safety gate:
  // a missing or v1 marker leaves every record untouched.
  std::string encoded_format;
  const rocksdb::Status marker_status =
      database.db->Get(rocksdb::ReadOptions(), database.meta,
                       EncodeSequenceFormatKey(), &encoded_format);
  if (marker_status.IsNotFound()) {
    return Status::InvalidArgument(
        "meta v2 reclaim",
        "database has no durable meta sequence format marker; enable format 2 "
        "and reopen it before reclaiming");
  }
  if (!marker_status.ok()) {
    return FromEngineStatus(marker_status, "read meta sequence format marker");
  }
  const auto decoded_format = DecodeSequenceFormat(encoded_format);
  if (!decoded_format.ok()) return decoded_format.status();
  if (decoded_format.ValueOrDie() != kMetaSequenceFormatV2) {
    return Status::InvalidArgument(
        "meta v2 reclaim", "database meta sequence format is " +
                               std::to_string(decoded_format.ValueOrDie()) +
                               ", reclamation requires 2");
  }

  std::vector<PendingRewrite> pending;
  pending.reserve(options.batch_records);
  size_t batch_index = 0;
  bool wrote_any = false;

  const auto write_pending_batch = [&]() -> Status {
    if (pending.empty()) return Status::OK();
    rocksdb::WriteBatch batch;
    for (const PendingRewrite& rewrite : pending) {
      batch.Put(database.meta, rewrite.key, rewrite.value);
    }
    rocksdb::WriteOptions write_options;
    write_options.sync = options.sync_each_batch;
    const rocksdb::Status written = database.db->Write(write_options, &batch);
    if (!written.ok()) {
      return FromEngineStatus(written, "rewrite meta sequence record");
    }
    wrote_any = true;
    for (const PendingRewrite& rewrite : pending) {
      ++out.rewritten;
      out.bytes_after += rewrite.new_size;
    }
    pending.clear();
    if (options.fault_for_testing) options.fault_for_testing(batch_index);
    ++batch_index;
    return Status::OK();
  };

  // Each batch is read from a fresh snapshot that is released before the batch
  // is written. The records in one committed batch are atomic and the next
  // batch re-seeks past the last examined key, so a stopped run always leaves a
  // prefix of v2 records followed by untouched v1 records. Keeping the snapshot
  // scoped to one batch also lets the engine retire the replaced v1 versions
  // between batches instead of retaining the whole history until the end.
  std::string cursor(kSequenceKeyPrefix);
  for (;;) {
    rocksdb::ReadOptions read_options;
    read_options.snapshot = database.db->GetSnapshot();
    SnapshotGuard snapshot_guard(database.db.get(), read_options.snapshot);
    std::unique_ptr<rocksdb::Iterator> iterator(
        database.db->NewIterator(read_options, database.meta));
    if (iterator == nullptr) {
      return Status::IOError(
          "meta v2 reclaim", "failed to create meta column family iterator");
    }

    pending.clear();
    bool exhausted = true;
    std::string last_key;
    for (iterator->Seek(cursor); iterator->Valid(); iterator->Next()) {
      const rocksdb::Slice key = iterator->key();
      if (!std::string_view(key.data(), key.size())
               .starts_with(kSequenceKeyPrefix)) {
        break;
      }
      CommitSeq commit_seq;
      const Status parsed = ParseSequenceCommitSeq(key, &commit_seq);
      if (!parsed.ok()) return parsed;

      const rocksdb::Slice value = iterator->value();
      ++out.scanned;
      out.bytes_before += value.size();
      last_key = key.ToString();
      if (value.size() >= 2 &&
          static_cast<uint8_t>(value[1]) == kSequenceV2RecordKind) {
        ++out.already_v2;
        out.bytes_after += value.size();
        continue;
      }

      const std::string encoded_v1 = value.ToString();
      const auto record = DecodeSequenceRecord(encoded_v1, commit_seq);
      if (!record.ok()) return record.status();
      if (record.ValueOrDie().commit_seq != commit_seq) {
        return Status::Corruption(
            "meta v2 reclaim",
            "sequence record commit sequence disagrees with its metadata key");
      }
      const auto encoded_v2 = EncodeSequenceRecordV2(record.ValueOrDie());
      if (!encoded_v2.ok()) return encoded_v2.status();
      if (options.verify) {
        const Status verified = VerifyRewrite(
            record.ValueOrDie(), encoded_v2.ValueOrDie(), commit_seq);
        if (!verified.ok()) return verified;
      }
      pending.push_back(PendingRewrite{
          key.ToString(), encoded_v2.ValueOrDie(),
          static_cast<uint64_t>(encoded_v2.ValueOrDie().size())});
      if (pending.size() >= options.batch_records) {
        exhausted = false;
        break;
      }
    }

    const rocksdb::Status iteration_status = iterator->status();
    iterator.reset();
    snapshot_guard.Release();
    if (!iteration_status.ok()) {
      return FromEngineStatus(iteration_status, "scan meta sequence records");
    }
    const Status flushed = write_pending_batch();
    if (!flushed.ok()) return flushed;
    if (exhausted) break;
    if (last_key.empty()) {
      return Status::Corruption("meta v2 reclaim",
                                "failed to advance the sequence scan");
    }
    // Successor of the last examined key under the bytewise comparator; the
    // next snapshot resumes at the first record not yet examined.
    cursor = last_key;
    cursor.push_back('\0');
  }

  if (wrote_any && !options.sync_each_batch) {
    const rocksdb::Status synced = database.db->SyncWAL();
    if (!synced.ok()) {
      return FromEngineStatus(synced, "sync reclaimed meta sequence records");
    }
  }
  return Status::OK();
}

}  // namespace cedar
