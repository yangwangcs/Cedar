// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/fact/fact_store.h"

#include <algorithm>
#include <filesystem>
#include <limits>
#include <map>
#include <mutex>
#include <set>
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
constexpr uint64_t kDefaultIdLeaseSize = 4096;
constexpr size_t kFactIdentityPrefixBytes = 12;
constexpr char kSchemaMetaPrefix[] = "schema/";
constexpr char kSequenceMetaPrefix[] = "sequence/";

Status FromRocksDb(const rocksdb::Status& status, const char* context) {
  if (status.ok()) return Status::OK();
  const std::string message = status.ToString();
  if (status.IsNotFound()) return Status::NotFound(context, message);
  if (status.IsCorruption()) return Status::Corruption(context, message);
  if (status.IsInvalidArgument()) return Status::InvalidArgument(context, message);
  if (status.IsNotSupported()) return Status::NotSupported(context, message);
  return Status::IOError(context, message);
}

Status FromCommitWriteFailure(const rocksdb::Status& status) {
  if (status.IsInvalidArgument() || status.IsNotSupported()) {
    return FromRocksDb(status, "commit fact store batch");
  }
  return Status::Indeterminate("commit fact store batch", status.ToString());
}

Status FromMetadataWriteFailure(const rocksdb::Status& status) {
  if (status.IsInvalidArgument() || status.IsNotSupported()) {
    return FromRocksDb(status, "persist fact store metadata");
  }
  return Status::Indeterminate("persist fact store metadata", status.ToString());
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

bool SamePropertyDefinition(const PropertyDefinition& left,
                            const PropertyDefinition& right) {
  return left.property_id == right.property_id && left.name == right.name &&
         left.entity_kind == right.entity_kind &&
         left.physical_type == right.physical_type &&
         left.blob_threshold_bytes == right.blob_threshold_bytes;
}

Status ValidatePropertyRequest(const PropertyDefinition& definition) {
  if (definition.schema_epoch != 0) {
    return Status::InvalidArgument("property definition",
                                   "registration request has a schema epoch");
  }
  PropertyDefinition persisted = definition;
  persisted.schema_epoch = 1;
  return persisted.Validate();
}

bool HasIncompatiblePropertyName(
    const std::vector<PropertyDefinition>& definitions,
    const PropertyDefinition& candidate) {
  for (const PropertyDefinition& definition : definitions) {
    if (definition.name == candidate.name &&
        (definition.entity_kind != candidate.entity_kind ||
         definition.physical_type != candidate.physical_type)) {
      return true;
    }
  }
  return false;
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
  using PropertySchemas = std::map<uint16_t, std::vector<PropertyDefinition>>;

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
  IdAllocatorState vertex_allocator{IdKind::kVertex, 1};
  IdAllocatorState edge_allocator{IdKind::kEdge, 1};
  std::shared_ptr<const PropertySchemas> property_schemas =
      std::make_shared<const PropertySchemas>();
  size_t active_snapshots = 0;
  bool recovery_required = false;
};

Status LoadAllocatorState(FactStoreImpl* store, IdKind kind,
                          IdAllocatorState* state) {
  std::string encoded;
  const rocksdb::Status got = store->db->Get(
      rocksdb::ReadOptions(), store->meta_cf, EncodeAllocatorMetaKey(kind), &encoded);
  if (got.IsNotFound()) {
    *state = IdAllocatorState{kind, 1};
    return Status::OK();
  }
  if (!got.ok()) return FromRocksDb(got, "read ID allocator metadata");
  const auto decoded = DecodeIdAllocatorState(encoded);
  if (!decoded.ok()) return decoded.status();
  if (decoded.ValueOrDie().kind != kind) {
    return Status::Corruption("fact store", "allocator record has wrong ID kind");
  }
  *state = decoded.ValueOrDie();
  return Status::OK();
}

Status LoadPropertySchemas(FactStoreImpl* store) {
  auto schemas = std::make_shared<FactStoreImpl::PropertySchemas>();
  std::unique_ptr<rocksdb::Iterator> iterator(
      store->db->NewIterator(rocksdb::ReadOptions(), store->meta_cf));
  for (iterator->Seek(kSchemaMetaPrefix);
       iterator->Valid() && StartsWith(iterator->key(), kSchemaMetaPrefix);
       iterator->Next()) {
    const std::string key = iterator->key().ToString();
    auto definition = DecodePropertyDefinition(iterator->value().ToString());
    if (!definition.ok()) return definition.status();
    const auto expected_key = EncodeSchemaMetaKey(definition.ValueOrDie().property_id,
                                                  definition.ValueOrDie().schema_epoch);
    if (!expected_key.ok()) return expected_key.status();
    if (key != expected_key.ValueOrDie()) {
      return Status::Corruption("fact store", "schema record key disagrees with value");
    }
    auto& epochs = (*schemas)[definition.ValueOrDie().property_id.value];
    if (definition.ValueOrDie().schema_epoch != epochs.size() + 1) {
      return Status::Corruption("fact store", "schema epochs are not contiguous");
    }
    if (HasIncompatiblePropertyName(epochs, definition.ValueOrDie())) {
      return Status::Corruption("fact store", "schema type changes for a property name");
    }
    epochs.push_back(definition.ConsumeValueOrDie());
  }
  if (!iterator->status().ok()) {
    return FromRocksDb(iterator->status(), "iterate property schemas");
  }
  store->property_schemas = std::move(schemas);
  return Status::OK();
}

Status ValidateCommittedSequence(FactStoreImpl* store, CommitSeq commit_seq) {
  const auto sequence_key = EncodeSequenceMetaKey(commit_seq);
  if (!sequence_key.ok()) return sequence_key.status();
  std::string encoded_sequence;
  const rocksdb::Status got_sequence = store->db->Get(
      rocksdb::ReadOptions(), store->meta_cf, sequence_key.ValueOrDie(),
      &encoded_sequence);
  if (!got_sequence.ok()) {
    return Status::Corruption("fact store", "missing durable sequence record");
  }
  const auto sequence = DecodeSequenceRecord(encoded_sequence);
  if (!sequence.ok()) return sequence.status();
  if (sequence.ValueOrDie().commit_seq != commit_seq) {
    return Status::Corruption("fact store", "sequence record has wrong commit sequence");
  }
  const auto transaction_key = EncodeTransactionMetaKey(sequence.ValueOrDie().txn_id);
  if (!transaction_key.ok()) return transaction_key.status();
  std::string encoded_outcome;
  const rocksdb::Status got_outcome = store->db->Get(
      rocksdb::ReadOptions(), store->meta_cf, transaction_key.ValueOrDie(),
      &encoded_outcome);
  if (!got_outcome.ok()) {
    return Status::Corruption("fact store", "sequence record is missing outcome");
  }
  const auto outcome = DecodeTransactionOutcome(encoded_outcome);
  if (!outcome.ok()) return outcome.status();
  if (outcome.ValueOrDie().txn_id != sequence.ValueOrDie().txn_id ||
      outcome.ValueOrDie().commit_seq != commit_seq) {
    return Status::Corruption("fact store", "outcome and sequence disagree");
  }
  for (const std::string& fact_key : sequence.ValueOrDie().fact_keys) {
    const auto decoded_key = DecodeFactKey(fact_key);
    if (!decoded_key.ok() || decoded_key.ValueOrDie().commit_seq != commit_seq) {
      return Status::Corruption("fact store", "sequence contains invalid fact key");
    }
    std::string encoded_fact;
    const rocksdb::Status got_fact = store->db->Get(
        rocksdb::ReadOptions(), store->facts_cf, fact_key, &encoded_fact);
    if (!got_fact.ok()) {
      return Status::Corruption("fact store", "sequence record is missing fact");
    }
    const auto fact = DecodeFactValue(decoded_key.ValueOrDie().ref,
                                      decoded_key.ValueOrDie().valid_from,
                                      commit_seq, encoded_fact);
    if (!fact.ok()) return fact.status();
  }
  return Status::OK();
}

Status ValidateCommittedSequences(FactStoreImpl* store, CommitSeq visible_seq) {
  if (visible_seq.value == 0) return Status::OK();
  rocksdb::ReadOptions options;
  std::unique_ptr<rocksdb::Iterator> iterator(
      store->db->NewIterator(options, store->meta_cf));
  uint64_t expected = 1;
  for (iterator->Seek(kSequenceMetaPrefix);
       iterator->Valid() && StartsWith(iterator->key(), kSequenceMetaPrefix);
       iterator->Next()) {
    const Status valid = ValidateCommittedSequence(store, CommitSeq{expected});
    if (!valid.ok()) return valid;
    if (expected == visible_seq.value) {
      ++expected;
      iterator->Next();
      break;
    }
    ++expected;
  }
  if (!iterator->status().ok()) {
    return FromRocksDb(iterator->status(), "iterate durable sequence records");
  }
  if (expected != visible_seq.value + 1) {
    return Status::Corruption("fact store", "visible watermark lacks contiguous sequences");
  }
  if (iterator->Valid() && StartsWith(iterator->key(), kSequenceMetaPrefix)) {
    return Status::Corruption("fact store", "sequence exceeds visible watermark");
  }
  return Status::OK();
}

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

Status StoreCommitBatch::Validate() const {
  if (!txn_id.valid() || system_hlc == 0 || mutations.empty()) {
    return Status::InvalidArgument("commit batch", "missing transaction, time, or facts");
  }
  std::set<std::string> facts;
  for (const PendingFactMutation& mutation : mutations) {
    const Status valid = mutation.Validate();
    if (!valid.ok()) return valid;
    const std::string key = EncodeFactKey(mutation.ref, mutation.valid_from,
                                          CommitSeq{1});
    if (key.empty() || !facts.emplace(key).second) {
      return Status::InvalidArgument("commit batch", "duplicate fact mutation");
    }
  }
  std::set<uint64_t> edge_ids;
  for (const EdgeIdentity& identity : edge_identities) {
    const Status valid = identity.Validate();
    if (!valid.ok()) return valid;
    if (!edge_ids.emplace(identity.edge_id.value).second) {
      return Status::InvalidArgument("commit batch", "duplicate edge identity");
    }
  }
  if (!edge_ids.empty()) {
    for (uint64_t edge_id : edge_ids) {
      bool has_state_assertion = false;
      for (const PendingFactMutation& mutation : mutations) {
        if (mutation.ref.family() == FactFamily::kEdgeState &&
            mutation.ref.entity_id() == edge_id &&
            mutation.operation == FactOperation::kPut) {
          has_state_assertion = true;
          break;
        }
      }
      if (!has_state_assertion) {
        return Status::InvalidArgument("commit batch", "edge identity lacks state assertion");
      }
    }
  }
  return Status::OK();
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
    const Status sequences_valid =
        ValidateCommittedSequences(store.get(), store->visible_seq);
    if (!sequences_valid.ok()) return sequences_valid;
  }
  const Status vertex_allocator =
      LoadAllocatorState(store.get(), IdKind::kVertex, &store->vertex_allocator);
  if (!vertex_allocator.ok()) return vertex_allocator;
  const Status edge_allocator =
      LoadAllocatorState(store.get(), IdKind::kEdge, &store->edge_allocator);
  if (!edge_allocator.ok()) return edge_allocator;
  const Status schemas = LoadPropertySchemas(store.get());
  if (!schemas.ok()) return schemas;
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

StatusOr<StoreCommitResult> FactStore::Commit(const StoreCommitBatch& batch) {
  const Status valid = batch.Validate();
  if (!valid.ok()) return valid;
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("commit", "store is not open");
  }
  std::lock_guard<std::mutex> lock(store->publisher_mutex);
  if (store->recovery_required) {
    return Status::RecoveryRequired("commit", "reopen required after indeterminate write");
  }

  const auto transaction_key = EncodeTransactionMetaKey(batch.txn_id);
  if (!transaction_key.ok()) return transaction_key.status();
  std::string encoded_outcome;
  const rocksdb::Status got_outcome = store->db->Get(
      rocksdb::ReadOptions(), store->meta_cf, transaction_key.ValueOrDie(),
      &encoded_outcome);
  if (got_outcome.ok()) {
    const auto outcome = DecodeTransactionOutcome(encoded_outcome);
    if (!outcome.ok()) return outcome.status();
    const auto sequence_key = EncodeSequenceMetaKey(outcome.ValueOrDie().commit_seq);
    if (!sequence_key.ok()) return sequence_key.status();
    std::string encoded_sequence;
    const rocksdb::Status got_sequence = store->db->Get(
        rocksdb::ReadOptions(), store->meta_cf, sequence_key.ValueOrDie(),
        &encoded_sequence);
    if (!got_sequence.ok()) {
      return Status::Corruption("commit", "outcome is missing sequence record");
    }
    const auto sequence = DecodeSequenceRecord(encoded_sequence);
    if (!sequence.ok()) return sequence.status();
    if (sequence.ValueOrDie().txn_id != batch.txn_id ||
        sequence.ValueOrDie().system_hlc != batch.system_hlc ||
        sequence.ValueOrDie().fact_keys.size() != batch.mutations.size()) {
      return Status::Conflict("commit", "transaction ID belongs to a different batch");
    }
    std::set<uint64_t> committed_edge_ids;
    for (const std::string& fact_key : sequence.ValueOrDie().fact_keys) {
      const auto decoded_key = DecodeFactKey(fact_key);
      if (!decoded_key.ok()) return decoded_key.status();
      if (decoded_key.ValueOrDie().ref.family() == FactFamily::kEdgeState ||
          decoded_key.ValueOrDie().ref.family() == FactFamily::kEdgeProperty) {
        committed_edge_ids.emplace(decoded_key.ValueOrDie().ref.entity_id());
      }
    }
    if (committed_edge_ids.size() != batch.edge_identities.size()) {
      return Status::Conflict("commit", "transaction ID belongs to a different batch");
    }
    for (const EdgeIdentity& identity : batch.edge_identities) {
      if (!committed_edge_ids.contains(identity.edge_id.value)) {
        return Status::Conflict("commit", "transaction ID belongs to a different batch");
      }
      const auto identity_key = EncodeEdgeIdentityMetaKey(identity.edge_id);
      if (!identity_key.ok()) return identity_key.status();
      std::string encoded_identity;
      const rocksdb::Status got_identity = store->db->Get(
          rocksdb::ReadOptions(), store->meta_cf, identity_key.ValueOrDie(),
          &encoded_identity);
      if (!got_identity.ok()) {
        return Status::Corruption("commit", "committed edge is missing identity");
      }
      const auto committed_identity = DecodeEdgeIdentity(encoded_identity);
      if (!committed_identity.ok()) return committed_identity.status();
      if (committed_identity.ValueOrDie() != identity) {
        return Status::Conflict("commit", "transaction ID belongs to a different batch");
      }
    }
    for (size_t index = 0; index < batch.mutations.size(); ++index) {
      const PendingFactMutation& mutation = batch.mutations[index];
      const std::string expected_key = EncodeFactKey(
          mutation.ref, mutation.valid_from, outcome.ValueOrDie().commit_seq);
      if (expected_key != sequence.ValueOrDie().fact_keys[index]) {
        return Status::Conflict("commit", "transaction ID belongs to a different batch");
      }
      std::string encoded_fact;
      const rocksdb::Status got_fact = store->db->Get(
          rocksdb::ReadOptions(), store->facts_cf, expected_key, &encoded_fact);
      if (!got_fact.ok()) return FromRocksDb(got_fact, "read committed fact");
      const auto actual = DecodeFactValue(mutation.ref, mutation.valid_from,
                                          outcome.ValueOrDie().commit_seq,
                                          encoded_fact);
      if (!actual.ok()) return actual.status();
      const FactEvent expected{mutation.ref, mutation.valid_from,
                               outcome.ValueOrDie().commit_seq,
                               mutation.operation, mutation.schema_epoch,
                               mutation.value};
      if (actual.ValueOrDie().operation != expected.operation ||
          actual.ValueOrDie().schema_epoch != expected.schema_epoch ||
          actual.ValueOrDie().value != expected.value) {
        return Status::Conflict("commit", "transaction ID belongs to a different batch");
      }
    }
    return StoreCommitResult{outcome.ValueOrDie().commit_seq,
                             sequence.ValueOrDie().system_hlc};
  }
  if (!got_outcome.IsNotFound()) return FromRocksDb(got_outcome, "read transaction outcome");

  for (const PendingFactMutation& mutation : batch.mutations) {
    if (mutation.ref.family() != FactFamily::kEdgeState ||
        mutation.operation != FactOperation::kPut) {
      continue;
    }
    const auto identity_key = EncodeEdgeIdentityMetaKey(EdgeId{mutation.ref.entity_id()});
    if (!identity_key.ok()) return identity_key.status();
    std::string encoded_identity;
    const rocksdb::Status got_identity = store->db->Get(
        rocksdb::ReadOptions(), store->meta_cf, identity_key.ValueOrDie(),
        &encoded_identity);
    if (got_identity.IsNotFound()) {
      bool supplied_identity = false;
      for (const EdgeIdentity& identity : batch.edge_identities) {
        if (identity.edge_id.value == mutation.ref.entity_id()) {
          supplied_identity = true;
          break;
        }
      }
      if (!supplied_identity) {
        return Status::InvalidArgument("commit", "first edge assertion requires identity");
      }
    } else if (!got_identity.ok()) {
      return FromRocksDb(got_identity, "read edge identity");
    }
  }

  for (const EdgeIdentity& identity : batch.edge_identities) {
    const auto identity_key = EncodeEdgeIdentityMetaKey(identity.edge_id);
    if (!identity_key.ok()) return identity_key.status();
    std::string encoded_identity;
    const rocksdb::Status got_identity = store->db->Get(
        rocksdb::ReadOptions(), store->meta_cf, identity_key.ValueOrDie(),
        &encoded_identity);
    if (got_identity.IsNotFound()) continue;
    if (!got_identity.ok()) {
      return FromRocksDb(got_identity, "read edge identity");
    }
    const auto existing_identity = DecodeEdgeIdentity(encoded_identity);
    if (!existing_identity.ok()) return existing_identity.status();
    if (existing_identity.ValueOrDie() != identity) {
      return Status::IdentityConflict("commit", "edge ID has a different identity");
    }
  }

  if (store->visible_seq.value == std::numeric_limits<uint64_t>::max()) {
    return Status::ResourceExhausted("commit", "commit sequence exhausted");
  }
  const CommitSeq commit_seq{store->visible_seq.value + 1};
  std::vector<std::string> fact_keys;
  fact_keys.reserve(batch.mutations.size());
  rocksdb::WriteBatch write_batch;
  for (const PendingFactMutation& mutation : batch.mutations) {
    FactEvent event{mutation.ref, mutation.valid_from, commit_seq,
                    mutation.operation, mutation.schema_epoch, mutation.value};
    const auto encoded_value = EncodeFactValue(event);
    if (!encoded_value.ok()) return encoded_value.status();
    std::string key = EncodeFactKey(mutation.ref, mutation.valid_from, commit_seq);
    if (key.empty()) return Status::InvalidArgument("commit", "invalid fact key");
    write_batch.Put(store->facts_cf, key, encoded_value.ValueOrDie());
    fact_keys.push_back(std::move(key));
  }
  for (const EdgeIdentity& identity : batch.edge_identities) {
    const auto key = EncodeEdgeIdentityMetaKey(identity.edge_id);
    const auto value = EncodeEdgeIdentity(identity);
    if (!key.ok()) return key.status();
    if (!value.ok()) return value.status();
    write_batch.Put(store->meta_cf, key.ValueOrDie(), value.ValueOrDie());
  }
  const TransactionOutcomeRecord outcome{batch.txn_id, commit_seq,
                                         TransactionOutcome::kCommitted};
  const SequenceRecord sequence{commit_seq, batch.txn_id, batch.system_hlc,
                                fact_keys};
  const auto encoded_new_outcome = EncodeTransactionOutcome(outcome);
  const auto encoded_new_sequence = EncodeSequenceRecord(sequence);
  const auto encoded_watermark = EncodeWatermark(commit_seq);
  const auto sequence_key = EncodeSequenceMetaKey(commit_seq);
  if (!encoded_new_outcome.ok()) return encoded_new_outcome.status();
  if (!encoded_new_sequence.ok()) return encoded_new_sequence.status();
  if (!encoded_watermark.ok()) return encoded_watermark.status();
  if (!sequence_key.ok()) return sequence_key.status();
  write_batch.Put(store->meta_cf, transaction_key.ValueOrDie(),
                  encoded_new_outcome.ValueOrDie());
  write_batch.Put(store->meta_cf, sequence_key.ValueOrDie(),
                  encoded_new_sequence.ValueOrDie());
  write_batch.Put(store->meta_cf, EncodeVisibleWatermarkKey(),
                  encoded_watermark.ValueOrDie());
  rocksdb::WriteOptions options;
  options.sync = true;
  const rocksdb::Status written = store->db->Write(options, &write_batch);
  if (!written.ok()) {
    const Status status = FromCommitWriteFailure(written);
    if (status.IsIndeterminate()) store->recovery_required = true;
    return status;
  }
  store->visible_seq = commit_seq;
  return StoreCommitResult{commit_seq, batch.system_hlc};
}

StatusOr<IdLease> FactStore::LeaseIds(IdKind kind, uint64_t count) {
  if (kind != IdKind::kVertex && kind != IdKind::kEdge) {
    return Status::InvalidArgument("ID lease", "unknown ID kind");
  }
  if (count == 0) count = kDefaultIdLeaseSize;
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("ID lease", "store is not open");
  }
  std::lock_guard<std::mutex> lock(store->publisher_mutex);
  if (store->recovery_required) {
    return Status::RecoveryRequired("ID lease",
                                    "reopen required after indeterminate write");
  }
  IdAllocatorState* allocator = kind == IdKind::kVertex
      ? &store->vertex_allocator
      : &store->edge_allocator;
  if (count > std::numeric_limits<uint64_t>::max() - allocator->next_id) {
    return Status::ResourceExhausted("ID lease", "ID space exhausted");
  }
  const uint64_t next_id = allocator->next_id + count;
  const IdAllocatorState updated{kind, next_id};
  const auto encoded = EncodeIdAllocatorState(updated);
  if (!encoded.ok()) return encoded.status();
  rocksdb::WriteBatch batch;
  batch.Put(store->meta_cf, EncodeAllocatorMetaKey(kind), encoded.ValueOrDie());
  rocksdb::WriteOptions options;
  options.sync = true;
  const rocksdb::Status written = store->db->Write(options, &batch);
  if (!written.ok()) {
    const Status status = FromMetadataWriteFailure(written);
    if (status.IsIndeterminate()) store->recovery_required = true;
    return status;
  }
  const IdLease lease{kind, allocator->next_id, count};
  *allocator = updated;
  return lease;
}

StatusOr<PropertyDefinition> FactStore::RegisterProperty(
    PropertyDefinition definition) {
  const Status valid = ValidatePropertyRequest(definition);
  if (!valid.ok()) return valid;
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("property definition", "store is not open");
  }
  std::lock_guard<std::mutex> lock(store->publisher_mutex);
  if (store->recovery_required) {
    return Status::RecoveryRequired("property definition",
                                    "reopen required after indeterminate write");
  }
  const auto found = store->property_schemas->find(definition.property_id.value);
  if (found != store->property_schemas->end() && !found->second.empty()) {
    const PropertyDefinition& latest = found->second.back();
    if (SamePropertyDefinition(latest, definition)) return latest;
    if (HasIncompatiblePropertyName(found->second, definition)) {
      return Status::SchemaMismatch("property definition",
                                    "property name has an incompatible type");
    }
    if (latest.schema_epoch == std::numeric_limits<uint32_t>::max()) {
      return Status::ResourceExhausted("property definition", "schema epochs exhausted");
    }
    definition.schema_epoch = latest.schema_epoch + 1;
  } else {
    definition.schema_epoch = 1;
  }
  const auto key = EncodeSchemaMetaKey(definition.property_id, definition.schema_epoch);
  const auto encoded = EncodePropertyDefinition(definition);
  if (!key.ok()) return key.status();
  if (!encoded.ok()) return encoded.status();
  rocksdb::WriteBatch batch;
  batch.Put(store->meta_cf, key.ValueOrDie(), encoded.ValueOrDie());
  rocksdb::WriteOptions options;
  options.sync = true;
  const rocksdb::Status written = store->db->Write(options, &batch);
  if (!written.ok()) {
    const Status status = FromMetadataWriteFailure(written);
    if (status.IsIndeterminate()) store->recovery_required = true;
    return status;
  }
  auto schemas = std::make_shared<FactStoreImpl::PropertySchemas>(
      *store->property_schemas);
  (*schemas)[definition.property_id.value].push_back(definition);
  store->property_schemas = std::move(schemas);
  return definition;
}

StatusOr<std::optional<PropertyDefinition>> FactStore::LookupProperty(
    PropertyId property_id, uint32_t schema_epoch) const {
  if (!property_id.valid()) {
    return Status::InvalidArgument("property definition", "zero property ID");
  }
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("property definition", "store is not open");
  }
  std::lock_guard<std::mutex> lock(store->publisher_mutex);
  const auto found = store->property_schemas->find(property_id.value);
  if (found == store->property_schemas->end() || found->second.empty() ||
      schema_epoch > found->second.size()) {
    return std::optional<PropertyDefinition>{};
  }
  if (schema_epoch == 0) return std::optional<PropertyDefinition>{found->second.back()};
  return std::optional<PropertyDefinition>{found->second[schema_epoch - 1]};
}

CommitSeq FactStore::visible_seq() const {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
  }
  if (!store) return CommitSeq{};
  std::lock_guard<std::mutex> lock(store->publisher_mutex);
  return store->visible_seq;
}

StatusOr<std::optional<StoreCommitResult>> FactStore::ResolveTransaction(
    TxnId txn_id) const {
  if (!txn_id.valid()) return Status::InvalidArgument("transaction", "zero transaction ID");
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("transaction", "store is not open");
  }
  const auto transaction_key = EncodeTransactionMetaKey(txn_id);
  if (!transaction_key.ok()) return transaction_key.status();
  std::string encoded_outcome;
  const rocksdb::Status got_outcome = store->db->Get(
      rocksdb::ReadOptions(), store->meta_cf, transaction_key.ValueOrDie(),
      &encoded_outcome);
  if (got_outcome.IsNotFound()) return std::optional<StoreCommitResult>{};
  if (!got_outcome.ok()) return FromRocksDb(got_outcome, "read transaction outcome");
  const auto outcome = DecodeTransactionOutcome(encoded_outcome);
  if (!outcome.ok()) return outcome.status();
  const auto sequence_key = EncodeSequenceMetaKey(outcome.ValueOrDie().commit_seq);
  if (!sequence_key.ok()) return sequence_key.status();
  std::string encoded_sequence;
  const rocksdb::Status got_sequence = store->db->Get(
      rocksdb::ReadOptions(), store->meta_cf, sequence_key.ValueOrDie(),
      &encoded_sequence);
  if (!got_sequence.ok()) {
    return Status::Corruption("transaction", "outcome is missing sequence record");
  }
  const auto sequence = DecodeSequenceRecord(encoded_sequence);
  if (!sequence.ok()) return sequence.status();
  if (sequence.ValueOrDie().txn_id != txn_id ||
      sequence.ValueOrDie().commit_seq != outcome.ValueOrDie().commit_seq) {
    return Status::Corruption("transaction", "outcome and sequence disagree");
  }
  return std::optional<StoreCommitResult>{
      StoreCommitResult{outcome.ValueOrDie().commit_seq,
                        sequence.ValueOrDie().system_hlc}};
}

}  // namespace cedar
