// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/fact/fact_store.h"

#include <algorithm>
#include <filesystem>
#include <limits>
#include <mutex>
#include <utility>
#include <vector>

#include <rocksdb/comparator.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/write_batch.h>

#include "cedar/fact/fact_codec.h"
#include "cedar/fact/meta_codec.h"

namespace cedar {
namespace {

constexpr uint32_t kCedarFactStoreFormatVersion = 1;
constexpr size_t kFactIdentityPrefixBytes = 12;

Status FromRocksDb(const rocksdb::Status& status, const char* context) {
  if (status.ok()) return Status::OK();
  const std::string message = status.ToString();
  if (status.IsNotFound()) return Status::NotFound(context, message);
  if (status.IsCorruption()) return Status::Corruption(context, message);
  if (status.IsInvalidArgument()) return Status::InvalidArgument(context, message);
  if (status.IsNotSupported()) return Status::NotSupported(context, message);
  return Status::IOError(context, message);
}

void AppendU16(std::string* out, uint16_t value) {
  out->push_back(static_cast<char>(value >> 8));
  out->push_back(static_cast<char>(value));
}

void AppendU64(std::string* out, uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    out->push_back(static_cast<char>(value >> shift));
  }
}

bool StartsWith(const rocksdb::Slice& value, const std::string& prefix) {
  return value.size() >= prefix.size() &&
         std::equal(prefix.begin(), prefix.end(), value.data());
}

std::string EncodeFactIdentityPrefix(FactFamily family, PropertyId property_id,
                                     std::optional<uint64_t> entity_id) {
  std::string prefix;
  prefix.reserve(entity_id.has_value() ? kFactIdentityPrefixBytes : 4);
  prefix.push_back(1);
  prefix.push_back(static_cast<char>(family));
  AppendU16(&prefix, property_id.value);
  if (entity_id.has_value()) AppendU64(&prefix, *entity_id);
  return prefix;
}

rocksdb::Options MakeRocksDbOptions(const FactStoreOptions& options,
                                    bool is_new_database) {
  rocksdb::Options result;
  result.create_if_missing = true;
  result.create_missing_column_families = is_new_database;
  result.write_buffer_size = options.write_buffer_bytes;
  result.atomic_flush = true;
  result.comparator = rocksdb::BytewiseComparator();
  result.enable_blob_files = true;
  result.min_blob_size = options.blob_threshold_bytes;
  return result;
}

std::vector<rocksdb::ColumnFamilyDescriptor> MakeColumnFamilyDescriptors(
    const rocksdb::Options& options) {
  rocksdb::ColumnFamilyOptions default_options(options);
  rocksdb::ColumnFamilyOptions facts_options(options);
  facts_options.prefix_extractor =
      std::shared_ptr<const rocksdb::SliceTransform>(
          rocksdb::NewFixedPrefixTransform(kFactIdentityPrefixBytes));
  rocksdb::ColumnFamilyOptions meta_options(options);
  return {{rocksdb::kDefaultColumnFamilyName, std::move(default_options)},
          {"facts", std::move(facts_options)},
          {"meta", std::move(meta_options)}};
}

}  // namespace

class FactStoreImpl {
 public:
  ~FactStoreImpl() {
    if (!db) return;
    if (default_cf != nullptr) db->DestroyColumnFamilyHandle(default_cf);
    if (facts_cf != nullptr) db->DestroyColumnFamilyHandle(facts_cf);
    if (meta_cf != nullptr) db->DestroyColumnFamilyHandle(meta_cf);
  }

  std::mutex publisher_mutex;
  std::unique_ptr<rocksdb::DB> db;
  rocksdb::ColumnFamilyHandle* default_cf = nullptr;
  rocksdb::ColumnFamilyHandle* facts_cf = nullptr;
  rocksdb::ColumnFamilyHandle* meta_cf = nullptr;
  CommitSeq visible_seq;
  CommitSeq oldest_readable_seq;
  size_t active_snapshots = 0;
};

class StoreSnapshot::State {
 public:
  State(std::shared_ptr<FactStoreImpl> store, const rocksdb::Snapshot* snapshot,
        CommitSeq commit_seq, CommitSeq oldest_readable_seq)
      : store(std::move(store)),
        snapshot(snapshot),
        commit_seq(commit_seq),
        oldest_readable_seq(oldest_readable_seq) {}

  ~State() {
    if (!store) return;
    std::lock_guard<std::mutex> lock(store->publisher_mutex);
    if (snapshot != nullptr && store->db) {
      store->db->ReleaseSnapshot(snapshot);
    }
    --store->active_snapshots;
  }

  std::shared_ptr<FactStoreImpl> store;
  const rocksdb::Snapshot* snapshot = nullptr;
  CommitSeq commit_seq;
  CommitSeq oldest_readable_seq;
};

FactPrefix FactPrefix::Exact(FactRef ref) {
  return FactPrefix(ref.family(), ref.property_id(), ref.entity_id());
}

FactPrefix FactPrefix::Family(FactFamily family, PropertyId property_id) {
  return FactPrefix(family, property_id, std::nullopt);
}

Status FactPrefix::Validate() const {
  const FactRef representative(family_, property_id_, entity_id_.value_or(1));
  if (!representative.Validate().ok()) {
    return Status::InvalidArgument("fact prefix", "invalid family or property ID");
  }
  if (entity_id_.has_value() && *entity_id_ == 0) {
    return Status::InvalidArgument("fact prefix", "zero entity ID");
  }
  return Status::OK();
}

StoreSnapshot::StoreSnapshot(std::unique_ptr<State> state) : state_(std::move(state)) {}
StoreSnapshot::~StoreSnapshot() = default;
StoreSnapshot::StoreSnapshot(StoreSnapshot&&) noexcept = default;
StoreSnapshot& StoreSnapshot::operator=(StoreSnapshot&&) noexcept = default;

CommitSeq StoreSnapshot::commit_seq() const {
  return state_ == nullptr ? CommitSeq{} : state_->commit_seq;
}

CommitSeq StoreSnapshot::oldest_readable_seq() const {
  return state_ == nullptr ? CommitSeq{} : state_->oldest_readable_seq;
}

FactStore::FactStore(FactStoreOptions options) : options_(std::move(options)) {}
FactStore::~FactStore() { Close().IgnoreError(); }

Status FactStore::Open() {
  std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
  if (impl_) return Status::InvalidArgument("fact store", "store is already open");
  if (options_.path.empty()) {
    return Status::InvalidArgument("fact store", "missing database path");
  }
  std::error_code filesystem_error;
  std::filesystem::create_directories(options_.path, filesystem_error);
  if (filesystem_error) {
    return Status::IOError("fact store", filesystem_error.message());
  }
  const std::filesystem::path current_path =
      std::filesystem::path(options_.path) / "CURRENT";
  const bool has_current = std::filesystem::exists(current_path, filesystem_error);
  if (filesystem_error) {
    return Status::IOError("fact store", filesystem_error.message());
  }
  const bool is_new_database = !has_current;
  if (is_new_database &&
      !std::filesystem::is_empty(options_.path, filesystem_error)) {
    return Status::NotSupported("fact store",
                                "directory does not contain a RocksDB database");
  }
  if (filesystem_error) {
    return Status::IOError("fact store", filesystem_error.message());
  }

  rocksdb::Options options = MakeRocksDbOptions(options_, is_new_database);

  std::vector<std::string> existing_column_families;
  if (!is_new_database) {
    const rocksdb::Status listed = rocksdb::DB::ListColumnFamilies(
        options, options_.path, &existing_column_families);
    if (!listed.ok()) {
      return FromRocksDb(listed, "list fact store column families");
    }
    const std::vector<std::string> expected = {rocksdb::kDefaultColumnFamilyName,
                                               "facts", "meta"};
    std::sort(existing_column_families.begin(), existing_column_families.end());
    std::vector<std::string> sorted_expected = expected;
    std::sort(sorted_expected.begin(), sorted_expected.end());
    if (existing_column_families != sorted_expected) {
      return Status::NotSupported("fact store",
                                  "database has an incompatible column family layout");
    }
  }

  std::vector<rocksdb::ColumnFamilyDescriptor> descriptors =
      MakeColumnFamilyDescriptors(options);
  std::vector<rocksdb::ColumnFamilyHandle*> handles;
  std::unique_ptr<rocksdb::DB> db;
  const rocksdb::Status opened =
      rocksdb::DB::Open(options, options_.path, descriptors, &handles, &db);
  if (!opened.ok()) return FromRocksDb(opened, "open fact store");
  if (handles.size() != descriptors.size()) {
    for (rocksdb::ColumnFamilyHandle* handle : handles) {
      db->DestroyColumnFamilyHandle(handle);
    }
    return Status::Corruption("fact store", "missing required column family handle");
  }

  auto store = std::make_shared<FactStoreImpl>();
  store->db = std::move(db);
  store->default_cf = handles[0];
  store->facts_cf = handles[1];
  store->meta_cf = handles[2];

  std::string encoded_format;
  const rocksdb::Status got_format = store->db->Get(
      rocksdb::ReadOptions(), store->meta_cf, EncodeCurrentFormatKey(),
      &encoded_format);
  if (got_format.IsNotFound()) {
    if (!is_new_database) {
      return Status::Corruption("fact store", "missing durable format record");
    }
    const auto format = EncodeFormatVersion(kCedarFactStoreFormatVersion);
    const auto empty_watermark = EncodeWatermark(CommitSeq{});
    if (!format.ok()) return format.status();
    if (!empty_watermark.ok()) return empty_watermark.status();
    rocksdb::WriteBatch batch;
    batch.Put(store->meta_cf, EncodeCurrentFormatKey(), format.ValueOrDie());
    batch.Put(store->meta_cf, EncodeVisibleWatermarkKey(),
              empty_watermark.ValueOrDie());
    batch.Put(store->meta_cf, EncodeOldestReadableWatermarkKey(),
              empty_watermark.ValueOrDie());
    rocksdb::WriteOptions write_options;
    write_options.sync = true;
    const rocksdb::Status initialized = store->db->Write(write_options, &batch);
    if (!initialized.ok()) return FromRocksDb(initialized, "initialize fact store");
    store->visible_seq = CommitSeq{};
    store->oldest_readable_seq = CommitSeq{};
  } else {
    if (!got_format.ok()) return FromRocksDb(got_format, "read fact store format");
    const auto format = DecodeFormatVersion(encoded_format);
    if (!format.ok()) return format.status();
    if (format.ValueOrDie() != kCedarFactStoreFormatVersion) {
      return Status::NotSupported("fact store", "unsupported durable format version");
    }
    std::string encoded_visible;
    std::string encoded_oldest;
    const rocksdb::Status got_visible = store->db->Get(
        rocksdb::ReadOptions(), store->meta_cf, EncodeVisibleWatermarkKey(),
        &encoded_visible);
    const rocksdb::Status got_oldest = store->db->Get(
        rocksdb::ReadOptions(), store->meta_cf,
        EncodeOldestReadableWatermarkKey(), &encoded_oldest);
    if (!got_visible.ok() || !got_oldest.ok()) {
      return Status::Corruption("fact store", "missing durable watermark");
    }
    const auto visible = DecodeWatermark(encoded_visible);
    const auto oldest = DecodeWatermark(encoded_oldest);
    if (!visible.ok()) return visible.status();
    if (!oldest.ok()) return oldest.status();
    if (oldest.ValueOrDie().value > visible.ValueOrDie().value) {
      return Status::Corruption("fact store",
                                "retention watermark exceeds visible sequence");
    }
    store->visible_seq = visible.ValueOrDie();
    store->oldest_readable_seq = oldest.ValueOrDie();
  }
  impl_ = std::move(store);
  return Status::OK();
}

Status FactStore::Close() {
  std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
  if (!impl_) return Status::OK();
  std::lock_guard<std::mutex> publisher_lock(impl_->publisher_mutex);
  if (impl_->active_snapshots != 0) {
    return Status::SnapshotPinned("fact store", "active snapshots prevent close");
  }
  impl_.reset();
  return Status::OK();
}

StatusOr<StoreSnapshot> FactStore::BeginSnapshot(SnapshotOptions options) const {
  std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
  if (!impl_) return Status::InvalidArgument("fact store", "store is not open");
  std::lock_guard<std::mutex> lock(impl_->publisher_mutex);
  const CommitSeq selected = options.as_of.value_or(impl_->visible_seq);
  if (selected.value < impl_->oldest_readable_seq.value) {
    return Status::SnapshotExpired("snapshot", "sequence is below retention boundary");
  }
  if (selected.value > impl_->visible_seq.value) {
    return Status::InvalidArgument("snapshot", "sequence exceeds visible watermark");
  }
  const rocksdb::Snapshot* snapshot = impl_->db->GetSnapshot();
  if (snapshot == nullptr) return Status::IOError("snapshot", "RocksDB refused snapshot");
  auto state = std::make_unique<StoreSnapshot::State>(
      impl_, snapshot, selected, impl_->oldest_readable_seq);
  ++impl_->active_snapshots;
  return StoreSnapshot(std::move(state));
}

StatusOr<std::optional<FactEvent>> FactStore::Read(
    const StoreSnapshot& snapshot, const FactRef& ref,
    ValidTime valid_time) const {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store || snapshot.state_ == nullptr || snapshot.state_->store != store) {
      return Status::InvalidArgument("fact read", "snapshot belongs to another store");
    }
  }
  const Status valid = ref.Validate();
  if (!valid.ok()) return valid;
  const std::string seek = EncodeFactKey(
      ref, valid_time, CommitSeq{std::numeric_limits<uint64_t>::max()});
  if (seek.empty()) return Status::InvalidArgument("fact read", "invalid fact key");
  const std::string prefix = seek.substr(0, kFactIdentityPrefixBytes);
  rocksdb::ReadOptions options;
  options.snapshot = snapshot.state_->snapshot;
  std::unique_ptr<rocksdb::Iterator> iterator(
      store->db->NewIterator(options, store->facts_cf));
  for (iterator->Seek(seek); iterator->Valid() && StartsWith(iterator->key(), prefix);
       iterator->Next()) {
    const auto decoded_key = DecodeFactKey(iterator->key().ToString());
    if (!decoded_key.ok()) return decoded_key.status();
    if (decoded_key.ValueOrDie().commit_seq.value > snapshot.commit_seq().value) {
      continue;
    }
    auto decoded_value = DecodeFactValue(
        decoded_key.ValueOrDie().ref, decoded_key.ValueOrDie().valid_from,
        decoded_key.ValueOrDie().commit_seq, iterator->value().ToString());
    if (!decoded_value.ok()) return decoded_value.status();
    if (decoded_value.ValueOrDie().operation == FactOperation::kDelete) {
      return std::optional<FactEvent>{};
    }
    return std::optional<FactEvent>{decoded_value.ConsumeValueOrDie()};
  }
  if (!iterator->status().ok()) return FromRocksDb(iterator->status(), "iterate fact read");
  return std::optional<FactEvent>{};
}

Status FactStore::Scan(const StoreSnapshot& snapshot, const FactPrefix& prefix,
                       const FactVisitor& visitor) const {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store || snapshot.state_ == nullptr || snapshot.state_->store != store) {
      return Status::InvalidArgument("fact scan", "snapshot belongs to another store");
    }
  }
  const Status valid = prefix.Validate();
  if (!valid.ok()) return valid;
  if (!visitor) return Status::InvalidArgument("fact scan", "missing visitor");
  const std::string encoded_prefix = EncodeFactIdentityPrefix(
      prefix.family(), prefix.property_id(), prefix.entity_id());
  rocksdb::ReadOptions options;
  options.snapshot = snapshot.state_->snapshot;
  std::unique_ptr<rocksdb::Iterator> iterator(
      store->db->NewIterator(options, store->facts_cf));
  for (iterator->Seek(encoded_prefix);
       iterator->Valid() && StartsWith(iterator->key(), encoded_prefix);
       iterator->Next()) {
    const auto decoded_key = DecodeFactKey(iterator->key().ToString());
    if (!decoded_key.ok()) return decoded_key.status();
    if (decoded_key.ValueOrDie().commit_seq.value > snapshot.commit_seq().value) {
      continue;
    }
    auto decoded_value = DecodeFactValue(
        decoded_key.ValueOrDie().ref, decoded_key.ValueOrDie().valid_from,
        decoded_key.ValueOrDie().commit_seq, iterator->value().ToString());
    if (!decoded_value.ok()) return decoded_value.status();
    const Status visited = visitor(decoded_value.ConsumeValueOrDie());
    if (!visited.ok()) return visited;
  }
  return iterator->status().ok() ? Status::OK()
                                 : FromRocksDb(iterator->status(), "iterate fact scan");
}

}  // namespace cedar
