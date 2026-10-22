// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "storage/rocks/rocks_adapter.h"

#include <algorithm>
#include <array>
#include <numeric>
#include <sstream>
#include <tuple>
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <filesystem>
#include <limits>
#include <map>
#include <mutex>
#include <set>
#include <sys/statvfs.h>
#include <type_traits>
#include <unordered_set>
#include <utility>
#include <vector>

#include <rocksdb/comparator.h>
#include <rocksdb/cache.h>
#include <rocksdb/cedar_commit.h>
#include <rocksdb/cedar_maintenance.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/perf_context.h>
#include <rocksdb/utilities/checkpoint.h>
#include <rocksdb/slice_transform.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/write_batch.h>

#include "db/cedar_columnar_scan.h"
#include "cedar/core/crc32c.h"
#include "cedar/fact/fact_codec.h"
#include "cedar/fact/meta_codec.h"
#include "cedar/format.h"
#include "storage/rocks/commit_publisher.h"
#include "storage/rocks/decided_epoch.h"
#include "storage/facts/fact_identity_history.h"
#include "storage/facts/group_commit_planner.h"
#include "storage/facts/prepared_commit_codec.h"
#include "storage/facts/recent_fact_write_index.h"
#include "storage/rocks/rocksdb_config.h"
#include "storage/facts/vacuum.h"
#include "storage/facts/version_validation_cache.h"

namespace cedar {
namespace {

// One durable lease extension every 65,536 transaction IDs keeps the
// publisher-mutex fsync off the critical path for ~65k transaction starts.
// IDs stay opaque: a crash may skip at most the unused tail of one lease (the
// durable allocator record already reserves the whole range before any ID from
// it is used).
constexpr uint64_t kDefaultIdLeaseSize = 65536;
// Vertex/edge IDs are handed out by explicit Allocate{Vertex,Edge}Id calls, not
// on the commit hot path, so their default lease keeps the historical 4,096
// range (raising it would only widen the crash ID gap).
constexpr uint64_t kDefaultEntityIdLeaseSize = 4096;
constexpr size_t kFactIdentityPrefixBytes = 16;
constexpr char kSchemaMetaPrefix[] = "schema/";
constexpr char kSequenceMetaPrefix[] = "sequence/";
// `sequence/` + big-endian commit sequence, i.e. the key EncodeSequenceMetaKey()
// produces. Reopen validation rebuilds it in place to compare the key the
// meta-CF iterator already holds without allocating or re-reading it.
constexpr size_t kSequenceMetaPrefixBytes = sizeof(kSequenceMetaPrefix) - 1;
constexpr size_t kSequenceMetaKeyBytes =
    kSequenceMetaPrefixBytes + sizeof(uint64_t);

void AssignSequenceMetaKey(uint64_t commit_seq, std::string* out) {
  out->assign(kSequenceMetaPrefix);
  for (int shift = 56; shift >= 0; shift -= 8) {
    out->push_back(static_cast<char>(commit_seq >> shift));
  }
}

// Parses the commit sequence out of a canonical `sequence/<seq>` key. Compact
// v2a sequence records do not embed commit_seq (it is the record key), so a
// reader that decoded one needs the number the iterator key already holds.
uint64_t ParseSequenceMetaKey(rocksdb::Slice key) {
  uint64_t commit_seq = 0;
  for (size_t index = kSequenceMetaPrefixBytes; index < kSequenceMetaKeyBytes;
       ++index) {
    commit_seq = (commit_seq << 8) | static_cast<uint8_t>(key[index]);
  }
  return commit_seq;
}

uint64_t TransactionIdLeaseSize(const FactStoreOptions& options) {
  return options.transaction_id_lease_size_for_testing == 0
             ? kDefaultIdLeaseSize
             : options.transaction_id_lease_size_for_testing;
}

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

Status CheckCommittedBatchSize(const rocksdb::WriteBatch& batch,
                               uint64_t max_bytes) {
  return internal::CheckCommittedBatchSize(batch, max_bytes);
}

void AppendU16(std::string* out, uint16_t value) {
  out->push_back(static_cast<char>(value >> 8));
  out->push_back(static_cast<char>(value));
}

void AppendU32(std::string* out, uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    out->push_back(static_cast<char>(value >> shift));
  }
}

void AppendU64(std::string* out, uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    out->push_back(static_cast<char>(value >> shift));
  }
}

// Prefix test over borrowed key bytes. Kept as `Slice` (not `std::string`) so a
// point-read prefix can be compared without materializing another string;
// `std::string` and NUL-terminated constants convert implicitly.
bool StartsWith(const rocksdb::Slice& value, const rocksdb::Slice& prefix) {
  return value.size() >= prefix.size() &&
         std::equal(prefix.data(), prefix.data() + prefix.size(), value.data());
}

uint64_t SaturatingAdd(uint64_t left, uint64_t right) {
  return right > UINT64_MAX - left ? UINT64_MAX : left + right;
}

uint64_t CounterDelta(uint64_t current, uint64_t before) {
  // PerfContext counters are monotonic for the lifetime of a thread. Treat a
  // reset defensively as zero rather than allowing unsigned underflow into a
  // bogus physical-read total.
  return current >= before ? current - before : 0;
}

class CanonicalReadPerfTracker {
 public:
  explicit CanonicalReadPerfTracker(std::atomic<uint64_t>* destination)
      : destination_(destination), context_(rocksdb::get_perf_context()) {
    get_read_bytes_ = context_->get_read_bytes;
    multiget_read_bytes_ = context_->multiget_read_bytes;
    iter_read_bytes_ = context_->iter_read_bytes;
  }

  ~CanonicalReadPerfTracker() {
    if (destination_ == nullptr) return;
    uint64_t delta = CounterDelta(context_->get_read_bytes, get_read_bytes_);
    delta = SaturatingAdd(
        delta, CounterDelta(context_->multiget_read_bytes, multiget_read_bytes_));
    delta = SaturatingAdd(
        delta, CounterDelta(context_->iter_read_bytes, iter_read_bytes_));
    if (delta != 0) destination_->fetch_add(delta, std::memory_order_relaxed);
  }

 private:
  std::atomic<uint64_t>* destination_;
  rocksdb::PerfContext* context_;
  uint64_t get_read_bytes_ = 0;
  uint64_t multiget_read_bytes_ = 0;
  uint64_t iter_read_bytes_ = 0;
};

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

std::string EncodeFactIdentityPrefix(PartId part_id, FactFamily family,
                                     PropertyId property_id,
                                     std::optional<uint64_t> entity_id) {
  std::string prefix;
  prefix.reserve(entity_id.has_value() ? kFactIdentityPrefixBytes : 8);
  prefix.push_back(2);
  AppendU32(&prefix, part_id.value);
  prefix.push_back(static_cast<char>(family));
  AppendU16(&prefix, property_id.value);
  if (entity_id.has_value()) AppendU64(&prefix, *entity_id);
  return prefix;
}

Status MakeFactColumn(FactColumnId id, FactColumn* column) {
  column->id = id;
  switch (id) {
    case FactColumnId::kPartId:
    case FactColumnId::kFactFamily:
    case FactColumnId::kPropertyId:
    case FactColumnId::kOperation:
    case FactColumnId::kSchemaEpoch:
    case FactColumnId::kPhysicalType:
    case FactColumnId::kSourcePartId:
    case FactColumnId::kTargetPartId:
      column->values = std::vector<uint32_t>{};
      return Status::OK();
    case FactColumnId::kEntityId:
    case FactColumnId::kValidFrom:
    case FactColumnId::kCedarCommitSeq:
    case FactColumnId::kStorageSequence:
    case FactColumnId::kTimestamp64Value:
    case FactColumnId::kSourceVertexId:
    case FactColumnId::kTargetVertexId:
    case FactColumnId::kEdgeType:
      column->values = std::vector<uint64_t>{};
      return Status::OK();
    case FactColumnId::kBoolValue:
      column->values = std::vector<uint8_t>{};
      return Status::OK();
    case FactColumnId::kInt32Value:
      column->values = std::vector<int32_t>{};
      return Status::OK();
    case FactColumnId::kInt64Value:
      column->values = std::vector<int64_t>{};
      return Status::OK();
    case FactColumnId::kFloat32Value:
      column->values = std::vector<float>{};
      return Status::OK();
    case FactColumnId::kFloat64Value:
      column->values = std::vector<double>{};
      return Status::OK();
    case FactColumnId::kBytesValue:
      column->values = std::vector<std::string>{};
      return Status::OK();
  }
  return Status::InvalidArgument("columnar scan", "unknown projected Cedar column");
}

rocksdb::cedar_parquet::CedarParquetColumnId ToRocksColumnId(FactColumnId id) {
  return static_cast<rocksdb::cedar_parquet::CedarParquetColumnId>(id);
}

Status AppendSelectedColumnarValues(
    const rocksdb::cedar_parquet::CedarParquetColumnVector& source,
    const std::vector<size_t>& selected_rows, size_t begin, size_t end,
    FactColumn* destination) {
  if (begin > end || end > selected_rows.size()) {
    return Status::InvalidArgument("columnar scan", "invalid selection vector range");
  }
  for (size_t offset = begin; offset < end; ++offset) {
    const size_t row = selected_rows[offset];
    if (source.present.size() <= row) {
      return Status::Corruption("columnar scan", "source null bitmap is shorter than rows");
    }
  }
  return std::visit(
      [&, destination](const auto& source_values) -> Status {
        using Vector = std::decay_t<decltype(source_values)>;
        auto* destination_values = std::get_if<Vector>(&destination->values);
        if (destination_values == nullptr) {
          return Status::Corruption("columnar scan", "projected vector type mismatch");
        }
        destination_values->reserve(destination_values->size() + (end - begin));
        destination->present.reserve(destination->present.size() + (end - begin));
        for (size_t offset = begin; offset < end; ++offset) {
          const size_t row = selected_rows[offset];
          if (source_values.size() <= row) {
            return Status::Corruption("columnar scan",
                                      "source vector is shorter than rows");
          }
          destination_values->push_back(source_values[row]);
          destination->present.push_back(source.present[row]);
        }
        return Status::OK();
      },
      source.values);
}

uint64_t ColumnarBatchBytes(
    const rocksdb::cedar_parquet::CedarParquetColumnarBatch& batch) {
  uint64_t bytes = 0;
  auto add = [&bytes](uint64_t value) {
    bytes = value > UINT64_MAX - bytes ? UINT64_MAX : bytes + value;
  };
  for (const auto& column : batch.columns) {
    add(column.present.size());
    std::visit(
        [&add](const auto& values) {
          using Vector = std::decay_t<decltype(values)>;
          if constexpr (std::is_same_v<Vector, std::vector<std::string>>) {
            for (const auto& value : values) add(value.size());
          } else {
            add(values.size() * sizeof(typename Vector::value_type));
          }
        },
        column.values);
  }
  return bytes;
}

bool MatchesColumnarPrefix(const FactPrefix& prefix, const FactRef& ref) {
  return prefix.part_id() == ref.part_id() && prefix.family() == ref.family() &&
         prefix.property_id() == ref.property_id() &&
         (!prefix.entity_id().has_value() || *prefix.entity_id() == ref.entity_id());
}

StatusOr<std::string> MakeEntitySortBound(const FactPrefix& prefix,
                                          uint64_t entity_id, bool lower) {
  if (entity_id == 0) {
    return Status::InvalidArgument("columnar scan", "zero entity key bound");
  }
  const std::string user_key = EncodeFactKey(
      FactRef(prefix.part_id(), prefix.family(), prefix.property_id(), entity_id),
      lower ? ValidTime{std::numeric_limits<uint64_t>::max()} : ValidTime{},
      lower ? CommitSeq{std::numeric_limits<uint64_t>::max()} : CommitSeq{1});
  if (user_key.empty()) {
    return Status::InvalidArgument("columnar scan", "invalid entity key bound");
  }
  if (lower) {
    std::string sort_key;
    const rocksdb::Status status =
        rocksdb::MakeCedarParquetSortLowerBound(user_key, &sort_key);
    if (!status.ok()) {
      return Status::Corruption("columnar scan", status.ToString());
    }
    return sort_key;
  }
  std::string sort_key = user_key;
  {
    // sequence zero plus point deletion is the largest valid internal suffix.
    sort_key.append(8, static_cast<char>(0xff));
  }
  return sort_key;
}

}  // namespace

class SnapshotLease;
struct SnapshotEpoch;

class FactStoreImpl {
 public:
  struct RecoveryState {
    RecoveryState() = default;
    RecoveryState(const RecoveryState&) = delete;
    RecoveryState& operator=(const RecoveryState&) = delete;

    RecoveryState& operator=(bool required) noexcept {
      value = required;
      fast.store(required, std::memory_order_release);
      return *this;
    }

    operator bool() const noexcept { return value; }
    bool LoadFast() const noexcept {
      return fast.load(std::memory_order_acquire);
    }

    bool value = false;
    std::atomic<bool> fast{false};
  };

  struct GroupCommitRequest {
    StoreCommitBatch batch;
    std::optional<std::string> prepared_key;
    bool sync = true;
    bool selected = false;
    std::optional<StatusOr<StoreCommitResult>> result;
  };

  using PropertySchemas = std::map<uint16_t, std::vector<PropertyDefinition>>;

  // Defined after SnapshotEpoch so owned epochs (and their snapshot leases)
  // are released before the RocksDB handle is destroyed.
  ~FactStoreImpl();

  std::mutex publisher_mutex;
  std::mutex transaction_lease_mutex;
  // Opens that passed the lifecycle check and are still creating/registering
  // their epoch. Close flips accepting_snapshots under the lifecycle lock and
  // waits only for this bounded creation window before deciding whether live
  // epoch references permit the close.
  std::atomic<size_t> snapshot_opens_in_flight{0};
  std::atomic<bool> accepting_snapshots{true};
  // Gate for the lock-free current-epoch pin. Close clears it (and then drains
  // pin_attempts) before it can tear the epoch registry down.
  std::atomic<bool> accepting_fast{true};
  std::mutex group_commit_mutex;
  std::condition_variable group_commit_cv;
  std::deque<std::shared_ptr<GroupCommitRequest>> group_commit_requests;
  bool group_commit_leader = false;
  std::unique_ptr<rocksdb::DB> db;
  rocksdb::ColumnFamilyHandle* default_cf = nullptr;
  rocksdb::ColumnFamilyHandle* facts_cf = nullptr;
  rocksdb::ColumnFamilyHandle* meta_cf = nullptr;
  std::shared_ptr<rocksdb::Statistics> statistics;
  uint32_t maintenance_flush_worker_capacity = 1;
  uint32_t maintenance_compaction_worker_capacity = 1;
  uint32_t maintenance_normal_flush_credits = 1;
  uint32_t maintenance_emergency_flush_credits = 1;
  uint32_t maintenance_compaction_credits = 1;
  mutable std::mutex pressure_sample_mutex;
  bool pressure_sample_initialized = false;
  std::chrono::steady_clock::time_point pressure_last_sample_at;
  uint64_t pressure_last_bytes_written = 0;
  uint64_t pressure_last_flush_bytes = 0;
  uint64_t pressure_last_compact_bytes = 0;
  CommitSeq visible_seq;
  CommitSeq oldest_readable_seq;
  // Durable boundary below which committed `sequence/*` records may be absent
  // (F14). 0 means no retention prune has ever been published, so reopen
  // validation requires every record from 1 through visible_seq. A value W >= 1
  // requires records [W..visible_seq] to be contiguous and tolerates a missing
  // prefix below W. Only the opt-in Vacuum prune advances it (after a durable,
  // Sync=true write), and it never exceeds the oldest-readable boundary.
  std::atomic<uint64_t> sequence_retention_watermark{0};
  std::atomic<uint64_t> published_visible_seq{0};
  std::atomic<uint64_t> published_oldest_readable_seq{0};
  std::atomic<uint64_t> point_read_operations{0};
  std::atomic<uint64_t> multi_get_operations{0};
  std::atomic<uint64_t> multi_get_batches{0};
  std::atomic<uint64_t> projected_scan_rows{0};
  std::atomic<uint64_t> projected_scan_bytes_read{0};
  std::atomic<uint64_t> projected_scan_pages_skipped{0};
  std::atomic<uint64_t> projected_scan_pages_read{0};
  std::atomic<uint64_t> projected_scan_physical_bytes_read{0};
  std::atomic<uint64_t> canonical_scan_bytes_read{0};
  std::atomic<uint64_t> canonical_validation_scans{0};
  std::atomic<uint64_t> canonical_validation_boundaries{0};
  std::atomic<uint64_t> canonical_read_physical_bytes{0};
  std::atomic<uint64_t> logical_facts_bytes{0};
  IdAllocatorState vertex_allocator{IdKind::kVertex, 1};
  IdAllocatorState edge_allocator{IdKind::kEdge, 1};
  IdAllocatorState transaction_allocator{IdKind::kTransaction, 1};
  std::atomic<uint64_t> next_transaction_id{1};
  std::atomic<uint64_t> transaction_lease_limit{1};
  std::shared_ptr<const PropertySchemas> property_schemas =
      std::make_shared<const PropertySchemas>();
  std::atomic<const PropertySchemas*> current_schemas{
      property_schemas.get()};
  mutable std::mutex retired_schemas_mutex;
  std::vector<std::shared_ptr<const PropertySchemas>> retired_schemas;

  // Readers on the hot path load a raw pointer; retired schema versions are
  // kept alive while the store is open (a successful close drains every epoch
  // reference before ReleaseRetainedStateForClose drops them), so the hot path
  // never touches a refcount.
  //
  // A property registration does not advance the visible sequence, so every
  // snapshot epoch opened so far (the current epoch, retired epochs, and the
  // as_of cache's retired-from-birth epochs) must be repointed at the new
  // catalog. Otherwise a reader that reuses an epoch created before the
  // registration would resolve properties against a stale catalog (the
  // transaction path hit this: SnapshotEpoch::schemas was captured at epoch
  // creation only). Lock order: retired_schemas_mutex -> epoch_mutex. No path
  // takes them in the opposite order, and every publisher is additionally
  // serialized by publisher_mutex at the FactStore level.
  void PublishSchemas(std::shared_ptr<const PropertySchemas> next) {
    std::lock_guard<std::mutex> lock(retired_schemas_mutex);
    if (property_schemas) retired_schemas.push_back(property_schemas);
    property_schemas = std::move(next);
    const PropertySchemas* published = property_schemas.get();
    current_schemas.store(published, std::memory_order_release);
    {
      std::lock_guard<std::mutex> epoch_lock(epoch_mutex);
      RefreshLiveEpochSchemasLocked(published);
    }
  }
  std::shared_ptr<const PropertySchemas> SchemasSnapshot() const {
    std::lock_guard<std::mutex> lock(retired_schemas_mutex);
    return property_schemas;
  }
  std::unique_ptr<internal::VersionValidationCache> version_validation_cache;
  std::unique_ptr<internal::RecentFactWriteIndex> recent_fact_write_index;
  std::unique_ptr<internal::FactIdentityHistory> identity_history;
  std::shared_ptr<rocksdb::cedar_parquet::CanonicalPageCacheMetrics> canonical_page_metrics;
  std::function<void()> validation_scan_observer_for_testing;
  // S1: reports the source of every committed sequence record consumed by
  // reopen validation. See FactStoreOptions for the contract.
  std::function<void(bool)> sequence_validation_read_source_observer_for_testing;
  // Reopen outcome validation batch size (production 1024). Tests override it
  // to exercise MultiGet batch boundaries with a handful of transactions.
  size_t sequence_validation_outcome_batch_size = 1024;
  // Reopen fact cross-check policy; see FactStoreOptions.
  bool deep_recovery_validation = true;
  uint64_t recovery_validation_recent_sequences = 4096;
  // Effective meta sequence writer format resolved by Open from the durable
  // format marker and FactStoreOptions::meta_sequence_format. Fixed for the
  // lifetime of this open store; every commit consults it.
  uint32_t meta_sequence_format = kMetaSequenceFormatV1;
  // Reports one completed reopen validation pass; see FactStoreOptions.
  std::function<void(const ReopenValidationPhaseCounters&)>
      reopen_validation_counters_for_testing;
  std::function<void()> snapshot_epoch_created_observer_for_testing;
  std::function<void()> snapshot_epoch_destroyed_observer_for_testing;
  // Fired by PublishVisible after the new visible watermark is stored and
  // before the previous epoch is retired. Runs under epoch_mutex; the observer
  // must not call back into the store.
  std::function<void()> publish_visible_before_retire_observer_for_testing;
  // Fired by ForceDetachEpochsLocked at the start of its epoch_mutex critical
  // section, before the epoch registry is collected. Test-only: lets a regression
  // test park a concurrent last-reader release in the zero-count window the
  // destructor force-detach has to tolerate. Must not call back into the store.
  std::function<void()> force_detach_epochs_observer_for_testing;
  // Fired by ReleaseEpochRef when the last reference of a retired epoch reaches
  // zero, after the decrement and before epoch_mutex is taken. Test-only: the
  // release thread parks there while a destructor force-detach holds epoch_mutex.
  // Must not call back into the store.
  std::function<void()> epoch_release_before_reclaim_observer_for_testing;
  RecoveryState recovery_required;
  // One SnapshotEpoch per visible sequence is shared by every reader opened at
  // that sequence. Creation, publication, retirement, and reclamation are
  // serialized by epoch_mutex. Hot-path readers never take that mutex: they
  // load current_epoch_fast and increment one atomic reference word with a
  // DEAD bit (see TryPinCurrentEpochLockFree and ReleaseEpochRef).
  //
  // Every registered epoch carries one owner reference in addition to reader
  // references: the store's own reference for the current epoch, or the as_of
  // cache's reference for a cached historical epoch. That makes a zero
  // reference count unambiguous: only a retired epoch can reach it, and exactly
  // one thread observes the final decrement.
  static constexpr size_t kEpochRefShards = 32;
  static constexpr uint32_t kEpochDeadBit = 1u << 31;
  // One padded attempt counter per shard so the lock-free pin registration does
  // not bounce a cache line between workers.
  struct alignas(64) EpochAttemptCounter {
    std::atomic<uint32_t> value{0};
  };
  static constexpr size_t kAsOfCacheSlots = 8;
  struct AsOfCacheEntry {
    CommitSeq seq;
    SnapshotEpoch* epoch = nullptr;
  };

  std::mutex epoch_mutex;
  SnapshotEpoch* current_epoch = nullptr;
  std::atomic<SnapshotEpoch*> current_epoch_fast{nullptr};
  std::vector<SnapshotEpoch*> retired_epochs;
  // Retention target of a Vacuum that has scanned the epoch registry and is
  // about to publish its durable boundary. Guarded by epoch_mutex. Readers
  // validate against max(published_oldest_readable_seq, pending_vacuum_oldest)
  // in the same critical section that registers their epoch, so a reader either
  // makes its epoch visible to the Vacuum scan (and the Vacuum refuses to clean
  // below it) or observes this target and reports SnapshotExpired.
  //
  // Sentinel: 0 means "no Vacuum between its registry scan and its boundary
  // publication". 0 is unambiguous because a real target is always
  // >= oldest_readable_seq, and Vacuum early-returns before reserving when the
  // target equals the current durable boundary; the initial durable boundary is
  // 0, so a target of 0 can never reach the reservation. Every reservation is
  // owned by a scope guard (see Vacuum) that clears it on every exit path that
  // does not publish the boundary.
  uint64_t pending_vacuum_oldest = 0;
  // Retired epochs that are DEAD but whose memory cannot be freed yet because a
  // lock-free pin attempt may still hold the pointer. Freed once every attempt
  // counter has drained.
  std::vector<SnapshotEpoch*> dead_epochs_pending_free;
  std::atomic<bool> pending_reclaim{false};
  // Epochs detached by a successful Close. Their memory is kept (leases are
  // already released) until the store is destroyed so that a last-reader
  // release that raced the close can never touch freed memory.
  std::vector<SnapshotEpoch*> close_graveyard;
  std::array<EpochAttemptCounter, kEpochRefShards> pin_attempts{};
  // In-flight epoch reference releases. A release registers here before it
  // decrements the reference count and unregisters only after its last access
  // to the store, so a Close that drains this total proves a previously closed
  // shell is quiescent and can be destroyed (see FreeEarlierRetainedShells).
  // Kept separate from pin_attempts so the reclamation protocol stays exactly
  // the same as before: only pin attempts can still hold a parked DEAD epoch.
  std::array<EpochAttemptCounter, kEpochRefShards> reclaim_attempts{};

  // Bounded per-sequence cache for historical point reads (GetPointAtAt). Each
  // entry owns one epoch reference; the epoch is retired from birth and is
  // released when the entry is evicted or the cache is cleared.
  std::mutex as_of_cache_mutex;
  std::array<AsOfCacheEntry, kAsOfCacheSlots> as_of_cache{};

  // Lock-free current-epoch pin. Returns true with one reference pinned for the
  // caller plus the store shared_ptr that the epoch owns, or false when the
  // caller must use the lifecycle/epoch-mutex slow path.
  bool TryPinCurrentEpochLockFree(std::shared_ptr<FactStoreImpl>* store_owner,
                                  SnapshotEpoch** pinned);
  // Slow path: returns the epoch for the current visible sequence (creating it
  // on first use) with one reference pinned for the caller and the store's own
  // owner reference held. Returns nullptr when RocksDB refuses a snapshot. Must
  // be paired with ReleaseEpochRef. Caller must hold epoch_mutex: the boundary
  // validation and the registration must be atomic with Vacuum's scan.
  SnapshotEpoch* PinCurrentEpochLocked(
      const std::shared_ptr<FactStoreImpl>& self);
  // Creates a non-cached epoch at an arbitrary readable sequence, retired from
  // birth, with exactly one reference owned by the caller. Used by as_of reads
  // and by the as_of cache. `durable_oldest_readable` is the published retention
  // boundary the epoch reports (not the effective pending boundary used for
  // validation). Returns nullptr when RocksDB refuses a snapshot. Caller must
  // hold epoch_mutex (same open/vacuum atomicity as above).
  SnapshotEpoch* CreateUncachedEpochLocked(
      const std::shared_ptr<FactStoreImpl>& self, CommitSeq seq,
      CommitSeq durable_oldest_readable);
  // Smallest sequence a new epoch may be opened at. It is the durable retention
  // boundary, raised by the target of an in-flight Vacuum that has already
  // scanned the epoch registry but has not published its durable boundary yet.
  // Caller must hold epoch_mutex. This is the reader-side half of the
  // open/vacuum handshake (see Vacuum).
  uint64_t EffectiveOldestReadableLocked() const;
  // Durable retention boundary, excluding an in-flight Vacuum reservation. This
  // is the value recorded on newly created snapshot epochs (and therefore the
  // value reported by StoreSnapshot::oldest_readable_seq); validation still uses
  // EffectiveOldestReadableLocked. Recording the published value keeps a failed
  // Vacuum (which releases its reservation without publishing) from making an
  // already-open or subsequently opened snapshot look expired to
  // ValidatePreparedQuery. Caller must hold epoch_mutex.
  CommitSeq PublishedOldestReadableLocked() const;
  // Drops one reference. The last reference of a retired epoch marks it DEAD,
  // unlinks it, releases its lease, and frees its memory once the lock-free
  // pin-attempt protocol proves no reader can still reach it.
  void ReleaseEpochRef(SnapshotEpoch* epoch);

  // Publishes a new visible watermark and retires the previous current epoch.
  void PublishVisible(CommitSeq sequence);

  // Reports whether any registered epoch still has a live reader reference.
  // The current epoch's owner reference is not a reader reference. Caller must
  // hold epoch_mutex.
  bool HasActiveEpochRefsLocked() const;
  // Smallest sequence among epochs with a live reader reference, or nullopt
  // when no epoch is pinned. Caller must hold epoch_mutex.
  std::optional<uint64_t> OldestActiveEpochSeqLocked() const;
  // Total number of in-flight lock-free pin attempts. Draining it proves that
  // no thread is between loading current_epoch_fast and deciding its reference.
  uint32_t PinAttemptsTotal() const;
  // Total number of in-flight epoch reference releases.
  uint32_t ReclaimAttemptsTotal() const;
  // Detaches every registered epoch and releases its RocksDB lease without
  // firing the destroyed observer (teardown contract). Epoch memory is kept in
  // close_graveyard until the store is destroyed. Caller must hold epoch_mutex
  // and PinAttemptsTotal() must be zero.
  void CloseEpochsLocked();
  // Destructor-only teardown variant of CloseEpochsLocked that does not require
  // a zero reader count: a StoreSnapshot handle may outlive the FactStore that
  // owns it (Close already reported SnapshotPinned). Every epoch is retired,
  // unlinked, its payload (RocksDB lease + strong store back-pointer) is
  // released, and its memory is claimed into close_graveyard so the later
  // ReleaseEpochRef from those handles is a harmless no-op. The owner/cache
  // references are dropped first; refs that remain work exactly like the normal
  // last-reader reclamation. Tolerates a concurrent last-reader release: such a
  // release can reach a zero count before it blocks on epoch_mutex, so a
  // reader-held epoch is preserved (payload + memory claimed) without asserting
  // on the count, and the blocked reclaimer becomes a no-op. Never fires the
  // destroyed observer (notify=false). Reading a handle concurrently with
  // destruction is not supported (see ~FactStore). Caller must hold epoch_mutex.
  void ForceDetachEpochsLocked();
  // Releases every remaining epoch and graveyard entry. Exclusive ownership.
  void DestroyEpochsLocked();
  // Destroys the RocksDB handle and column families while keeping the store
  // object (and its graveyard) alive for a lock-free pin that raced the close.
  void DestroyDatabaseHandles();
  // Releases the heavy, per-open state of a successfully closed store so the
  // retained shell (kept only for a pin that raced the close gate) is inert:
  // validation caches, statistics, property schemas, and the canonical page
  // metrics. Must be called after DestroyDatabaseHandles() and only when no
  // reader reference is live.
  void ReleaseRetainedStateForClose();
  // True when nothing heavy or RocksDB-owning is left in this closed shell:
  // used by the lifecycle regression test that bounds Close retention.
  bool RetainedShellIsInertForTesting();
  // Drops every cached as_of epoch reference (may reclaim epochs).
  void ClearAsOfCache();
  // Deletes DEAD epochs once no lock-free pin attempt can still reference them.
  void MaybeFreeDeadEpochs();

 private:
  // Repoints every live snapshot epoch (the current epoch, every retired epoch,
  // and therefore every as_of cache epoch, which is retired from birth and
  // registered in retired_epochs while alive) at `schemas`. Caller must hold
  // epoch_mutex so no epoch can be created or unlinked between the scan and the
  // store, and must keep the catalog alive (see PublishSchemas).
  void RefreshLiveEpochSchemasLocked(const PropertySchemas* schemas);
  SnapshotEpoch* CreateEpochLocked(const std::shared_ptr<FactStoreImpl>& self,
                                   CommitSeq seq,
                                   CommitSeq durable_oldest_readable);
  void RetireCurrentEpochLocked();
  void DropOwnerReferenceLocked(SnapshotEpoch* epoch);
  // Caller observed the reference count fall to zero. Caller must hold
  // epoch_mutex. `notify` fires the destroyed observer through
  // ReleaseEpochPayloadLocked; teardown paths pass false (contract: epochs torn
  // down with the store never fire it).
  void ReclaimEpochLocked(SnapshotEpoch* epoch, bool notify = true);
  void UnlinkEpochLocked(SnapshotEpoch* epoch);
  // Releases the lease and the store shared_ptr exactly once. `notify` fires
  // the destroyed observer; teardown paths pass false (contract).
  void ReleaseEpochPayloadLocked(SnapshotEpoch* epoch, bool notify);
  bool ClaimEpochMemoryLocked(SnapshotEpoch* epoch);
  void FreeDeadEpochsLocked();
  static uint32_t EpochRefs(const SnapshotEpoch* epoch);
};

// Releases the shared RocksDB snapshot when the last reader drops it. Holds a
// raw store pointer to avoid a FactStoreImpl -> lease -> FactStoreImpl cycle;
// every open StoreSnapshot handle owns a shared_ptr to the store for lifetime.
class SnapshotLease {
 public:
  SnapshotLease(FactStoreImpl* store, const rocksdb::Snapshot* snapshot)
      : store_(store), snapshot_(snapshot) {}
  ~SnapshotLease() {
    if (snapshot_ != nullptr && store_ != nullptr && store_->db) {
      store_->db->ReleaseSnapshot(snapshot_);
    }
  }
  SnapshotLease(const SnapshotLease&) = delete;
  SnapshotLease& operator=(const SnapshotLease&) = delete;
  const rocksdb::Snapshot* get() const { return snapshot_; }

 private:
  FactStoreImpl* store_ = nullptr;
  const rocksdb::Snapshot* snapshot_ = nullptr;
};

// Lock-free pin attempts register in one padded counter per thread, and the
// as_of cache uses the same shard count. Pin a stable shard per thread.
size_t SnapshotEpochShardIndex() {
  static std::atomic<size_t> next{0};
  thread_local const size_t index =
      next.fetch_add(1, std::memory_order_relaxed) %
      FactStoreImpl::kEpochRefShards;
  return index;
}

FactStore::FastPinAttempt::FastPinAttempt(const FactStore& store)
    : store_(store), shard_(SnapshotEpochShardIndex()) {
  store_.fast_pin_attempts_[shard_].value.fetch_add(
      1, std::memory_order_seq_cst);
}

FactStore::FastPinAttempt::~FastPinAttempt() {
  store_.fast_pin_attempts_[shard_].value.fetch_sub(
      1, std::memory_order_seq_cst);
}

uint32_t FactStore::FastPinAttemptsTotal() const {
  uint32_t total = 0;
  for (const FastPinAttemptCounter& counter : fast_pin_attempts_) {
    total += counter.value.load(std::memory_order_seq_cst);
  }
  return total;
}

uint32_t FactStore::FastPinAttemptsSkippedForTesting() const {
  return fast_pin_attempts_skipped_.load(std::memory_order_acquire);
}

FactStore::NonEpochUseGuard::~NonEpochUseGuard() {
  if (store_ == nullptr) return;
  store_->non_epoch_users_.fetch_sub(1, std::memory_order_acq_rel);
}

FactStore::NonEpochUseGuard::NonEpochUseGuard(
    NonEpochUseGuard&& other) noexcept
    : store_(other.store_) {
  other.store_ = nullptr;
}

FactStore::NonEpochUseGuard& FactStore::NonEpochUseGuard::operator=(
    NonEpochUseGuard&& other) noexcept {
  if (this == &other) return *this;
  if (store_ != nullptr) {
    store_->non_epoch_users_.fetch_sub(1, std::memory_order_acq_rel);
  }
  store_ = other.store_;
  other.store_ = nullptr;
  return *this;
}

FactStore::NonEpochUseGuard FactStore::RegisterNonEpochUseLocked() const {
  non_epoch_users_.fetch_add(1, std::memory_order_acq_rel);
  return NonEpochUseGuard(this);
}

void FactStore::RunNonEpochOpObserverForTesting() const {
  if (options_.non_epoch_op_observer_for_testing) {
    options_.non_epoch_op_observer_for_testing();
  }
}

size_t FactStore::NonEpochUsersForTesting() const {
  return non_epoch_users_.load(std::memory_order_acquire);
}

// One shared RocksDB snapshot per visible commit sequence. Readers opened at
// the current visible sequence pin this epoch instead of taking their own
// RocksDB snapshot. The reference count is a single atomic word; the highest
// bit marks the epoch DEAD, which makes a pin that raced the last release see a
// value it can never increment. Every registered epoch also carries one owner
// reference (the store's current reference or the as_of cache's reference), so
// a zero count unambiguously means "retired and no reader".
struct SnapshotEpoch {
  SnapshotEpoch() = default;
  SnapshotEpoch(const SnapshotEpoch&) = delete;
  SnapshotEpoch& operator=(const SnapshotEpoch&) = delete;

  // The owning store outlives every epoch on the slow path. `store_owner` is a
  // strong reference (not just this raw back-pointer): a lock-free pin copies it
  // into the StoreSnapshot handle, which keeps the store alive for the read even
  // though the pin does not hold lifecycle_mutex_. It does not form an ownership
  // cycle in the close path because Close releases every epoch's payload (lease
  // and store_owner) before the shell is retained, and the retained shell owns
  // only DEAD epochs with no back-reference.
  FactStoreImpl* store = nullptr;
  std::shared_ptr<SnapshotLease> lease;
  std::shared_ptr<FactStoreImpl> store_owner;
  // The property catalog this epoch resolves against. Refreshed in place by
  // PublishSchemas for every live epoch because property registration is not
  // tied to a visible sequence; readers load it with acquire. Old catalog
  // versions are retained in FactStoreImpl::retired_schemas while the store is
  // open; a successful close drains every epoch reference before it releases
  // them (ReleaseRetainedStateForClose).
  std::atomic<const FactStoreImpl::PropertySchemas*> schemas{nullptr};
  CommitSeq seq;
  // Published durable retention boundary captured when the epoch was created.
  // It intentionally excludes an in-flight Vacuum reservation so a failed
  // Vacuum cannot make this epoch look expired to ValidatePreparedQuery.
  CommitSeq oldest_readable;
  std::atomic<uint32_t> refs{0};
  // True once the epoch is detached from the current publication: retired by a
  // PublishVisible, or a one-off as_of epoch created retired-from-birth. Only a
  // retired epoch can reach a zero reference count, which the reclamation path
  // asserts before it claims the epoch.
  std::atomic<bool> retired{false};
  // Set exactly once when the lease and the store shared_ptr are released.
  std::atomic<bool> reclaimed{false};
  // Set exactly once by whichever owner takes responsibility for the epoch's
  // memory (the last reader, or a close that detached it into its graveyard).
  std::atomic<bool> memory_owned{false};
};

void FactStoreImpl::RefreshLiveEpochSchemasLocked(
    const PropertySchemas* schemas) {
  // DEAD epochs (dead_epochs_pending_free / close_graveyard) have no reader
  // reference, so no lookup can still load their catalog pointer; leaving them
  // alone keeps the close path's "parked memory is inert" contract unchanged.
  if (current_epoch != nullptr) {
    current_epoch->schemas.store(schemas, std::memory_order_release);
  }
  for (SnapshotEpoch* epoch : retired_epochs) {
    epoch->schemas.store(schemas, std::memory_order_release);
  }
}

uint32_t FactStoreImpl::EpochRefs(const SnapshotEpoch* epoch) {
  return epoch->refs.load(std::memory_order_acquire);
}

uint32_t FactStoreImpl::PinAttemptsTotal() const {
  // seq_cst: the fence participates in the pin attempt / reclamation protocol
  // (a pin raises its counter before loading current_epoch_fast, and the
  // reclaimer reads this total only after it has made the epoch unreachable).
  uint32_t total = 0;
  for (const EpochAttemptCounter& counter : pin_attempts) {
    total += counter.value.load(std::memory_order_seq_cst);
  }
  return total;
}

uint32_t FactStoreImpl::ReclaimAttemptsTotal() const {
  uint32_t total = 0;
  for (const EpochAttemptCounter& counter : reclaim_attempts) {
    total += counter.value.load(std::memory_order_seq_cst);
  }
  return total;
}

bool FactStoreImpl::HasActiveEpochRefsLocked() const {
  // The current epoch's owner reference is held by the store, so only counts
  // above it are reader references.
  if (current_epoch != nullptr && EpochRefs(current_epoch) > 1) return true;
  for (const SnapshotEpoch* epoch : retired_epochs) {
    if (EpochRefs(epoch) != 0) return true;
  }
  return false;
}

std::optional<uint64_t> FactStoreImpl::OldestActiveEpochSeqLocked() const {
  std::optional<uint64_t> oldest;
  const auto consider = [&oldest](const SnapshotEpoch* epoch, bool current) {
    if (epoch == nullptr) return;
    const uint32_t refs = EpochRefs(epoch);
    if (current ? refs <= 1 : refs == 0) return;
    if (!oldest.has_value() || epoch->seq.value < *oldest) {
      oldest = epoch->seq.value;
    }
  };
  consider(current_epoch, true);
  for (const SnapshotEpoch* epoch : retired_epochs) consider(epoch, false);
  return oldest;
}

void FactStoreImpl::UnlinkEpochLocked(SnapshotEpoch* epoch) {
  if (current_epoch == epoch) current_epoch = nullptr;
  const auto found = std::find(retired_epochs.begin(), retired_epochs.end(),
                               epoch);
  if (found != retired_epochs.end()) retired_epochs.erase(found);
}

void FactStoreImpl::ReleaseEpochPayloadLocked(SnapshotEpoch* epoch,
                                              bool notify) {
  bool expected = false;
  if (!epoch->reclaimed.compare_exchange_strong(expected, true)) return;
  // Release the RocksDB snapshot before the observer can run; observers must
  // never observe a lease that is still registered.
  epoch->lease.reset();
  epoch->store_owner.reset();
  if (notify && snapshot_epoch_destroyed_observer_for_testing) {
    snapshot_epoch_destroyed_observer_for_testing();
  }
}

bool FactStoreImpl::ClaimEpochMemoryLocked(SnapshotEpoch* epoch) {
  bool expected = false;
  return epoch->memory_owned.compare_exchange_strong(expected, true);
}

void FactStoreImpl::FreeDeadEpochsLocked() {
  for (SnapshotEpoch* epoch : dead_epochs_pending_free) delete epoch;
  dead_epochs_pending_free.clear();
  pending_reclaim.store(false, std::memory_order_seq_cst);
}

void FactStoreImpl::MaybeFreeDeadEpochs() {
  // seq_cst on both the flag load and the attempt-total reads: the reclaimer
  // publishes dead_epochs_pending_free with a seq_cst flag store, and the freeing
  // decision must be ordered against the seq_cst pin-attempt registration. An
  // acquire load would be sufficient only if every path that touches a DEAD
  // epoch also had a release fence here; the total-order guarantee is what makes
  // the "no attempt in flight" proof formal, so keep seq_cst throughout.
  if (!pending_reclaim.load(std::memory_order_seq_cst)) return;
  if (PinAttemptsTotal() != 0) return;
  std::lock_guard<std::mutex> lock(epoch_mutex);
  if (PinAttemptsTotal() != 0) return;
  FreeDeadEpochsLocked();
}

void FactStoreImpl::ReclaimEpochLocked(SnapshotEpoch* epoch, bool notify) {
  // The caller observed the reference count fall to zero, so no reader holds a
  // reference. A lock-free pin that loaded this epoch before it became
  // unreachable may still try to increment the count: the DEAD bit makes that
  // compare-exchange fail, and the epoch's memory is only freed once every pin
  // attempt has drained (see TryPinCurrentEpochLockFree).
  uint32_t expected = 0;
  if (!epoch->refs.compare_exchange_strong(expected, kEpochDeadBit,
                                           std::memory_order_seq_cst,
                                           std::memory_order_relaxed)) {
    // A racing reader re-pinned the retired epoch, or a teardown path already
    // claimed it (DEAD); either way its own path owns the reclamation.
    return;
  }
  UnlinkEpochLocked(epoch);
  ReleaseEpochPayloadLocked(epoch, /*notify=*/notify);
  if (!ClaimEpochMemoryLocked(epoch)) return;
  if (PinAttemptsTotal() == 0) {
    delete epoch;
    return;
  }
  dead_epochs_pending_free.push_back(epoch);
  // Publish the pending flag before the final check so the attempt that is
  // still in flight either frees it on exit or is observed by a later check.
  pending_reclaim.store(true, std::memory_order_seq_cst);
  if (PinAttemptsTotal() == 0) {
    FreeDeadEpochsLocked();
  }
}

void FactStoreImpl::DropOwnerReferenceLocked(SnapshotEpoch* epoch) {
  const uint32_t previous =
      epoch->refs.fetch_sub(1, std::memory_order_acq_rel);
  assert(previous >= 1 && (previous & kEpochDeadBit) == 0);
  if (previous == 1) ReclaimEpochLocked(epoch);
}

void FactStoreImpl::RetireCurrentEpochLocked() {
  SnapshotEpoch* retired = current_epoch;
  if (retired == nullptr) return;
  // Readers must stop discovering this epoch before its owner reference can be
  // dropped: publish the cleared fast pointer first. That ordering is what
  // makes a retired epoch's zero reference count imply "no reader can pin".
  current_epoch_fast.store(nullptr, std::memory_order_seq_cst);
  current_epoch = nullptr;
  retired->retired.store(true, std::memory_order_release);
  retired_epochs.push_back(retired);
  DropOwnerReferenceLocked(retired);
}

void FactStoreImpl::PublishVisible(CommitSeq sequence) {
  std::lock_guard<std::mutex> lock(epoch_mutex);
  visible_seq = sequence;
  published_visible_seq.store(sequence.value, std::memory_order_release);
  // Test-only window: the new sequence is visible here while the previous epoch
  // may still be published as current. An exact-sequence as_of read that loaded
  // the new watermark must not use that older epoch; see
  // BeginCurrentSnapshotAt/BeginSnapshotAtCached.
  if (publish_visible_before_retire_observer_for_testing) {
    publish_visible_before_retire_observer_for_testing();
  }
  // Retiring next drops the store's owner reference. If that was the last
  // reference the epoch is released right here (the immediate-release
  // contract); otherwise the last reader's release reclaims it.
  if (current_epoch != nullptr && current_epoch->seq.value != sequence.value) {
    RetireCurrentEpochLocked();
  }
}

SnapshotEpoch* FactStoreImpl::CreateEpochLocked(
    const std::shared_ptr<FactStoreImpl>& self, CommitSeq seq,
    CommitSeq durable_oldest_readable) {
  const rocksdb::Snapshot* raw = db != nullptr ? db->GetSnapshot() : nullptr;
  if (raw == nullptr) return nullptr;
  auto epoch = std::make_unique<SnapshotEpoch>();
  epoch->store = this;
  epoch->lease = std::make_shared<SnapshotLease>(this, raw);
  epoch->store_owner = self;
  epoch->schemas.store(current_schemas.load(std::memory_order_acquire),
                       std::memory_order_relaxed);
  epoch->seq = seq;
  epoch->oldest_readable = durable_oldest_readable;
  // The returned owner reference: the store's current reference, or the
  // caller's reference for a one-off epoch.
  epoch->refs.store(1, std::memory_order_relaxed);
  return epoch.release();
}

uint64_t FactStoreImpl::EffectiveOldestReadableLocked() const {
  const uint64_t durable =
      published_oldest_readable_seq.load(std::memory_order_acquire);
  // The pending target can only be >= the durable boundary: Vacuum validates its
  // target against the durable range before it publishes it here. max() keeps
  // the reader side correct even if a future path ever lowered the durable
  // published value first.
  return std::max(durable, pending_vacuum_oldest);
}

CommitSeq FactStoreImpl::PublishedOldestReadableLocked() const {
  return CommitSeq{
      published_oldest_readable_seq.load(std::memory_order_acquire)};
}

SnapshotEpoch* FactStoreImpl::PinCurrentEpochLocked(
    const std::shared_ptr<FactStoreImpl>& self) {
  // Read the watermark under the epoch lock so a concurrent publish cannot
  // make a stale sequence look like the epoch to cache.
  const CommitSeq current{
      published_visible_seq.load(std::memory_order_acquire)};
  if (current_epoch != nullptr && current_epoch->seq.value == current.value) {
    current_epoch->refs.fetch_add(1, std::memory_order_relaxed);
    return current_epoch;
  }
  if (current_epoch != nullptr) {
    // Publishes retire under this lock, so this is only a consistency guard.
    RetireCurrentEpochLocked();
  }
  // Record the *published* boundary, not the effective one: a Vacuum that
  // reserved a pending target and then fails must not leave the epoch reporting
  // a boundary it never made durable.
  SnapshotEpoch* published = CreateEpochLocked(
      self, current, PublishedOldestReadableLocked());
  if (published == nullptr) return nullptr;
  current_epoch = published;
  current_epoch_fast.store(published, std::memory_order_seq_cst);
  if (snapshot_epoch_created_observer_for_testing) {
    snapshot_epoch_created_observer_for_testing();
  }
  published->refs.fetch_add(1, std::memory_order_relaxed);  // caller reference
  return published;
}

SnapshotEpoch* FactStoreImpl::CreateUncachedEpochLocked(
    const std::shared_ptr<FactStoreImpl>& self, CommitSeq seq,
    CommitSeq durable_oldest_readable) {
  SnapshotEpoch* uncached =
      CreateEpochLocked(self, seq, durable_oldest_readable);
  if (uncached == nullptr) return nullptr;
  // A one-off epoch is never published as current and is never pinned by the
  // lock-free path; its owner (a reader or the as_of cache) releases it.
  uncached->retired.store(true, std::memory_order_release);
  retired_epochs.push_back(uncached);
  if (snapshot_epoch_created_observer_for_testing) {
    snapshot_epoch_created_observer_for_testing();
  }
  return uncached;
}

bool FactStoreImpl::TryPinCurrentEpochLockFree(
    std::shared_ptr<FactStoreImpl>* store_owner, SnapshotEpoch** pinned) {
  const size_t shard = SnapshotEpochShardIndex();
  EpochAttemptCounter& attempts = pin_attempts[shard];
  // Raise the attempt counter before the epoch pointer is loaded and clear it
  // only after the reference decision is final. A reclaimer that observes a
  // zero total therefore cannot free an epoch this attempt may still touch.
  attempts.value.fetch_add(1, std::memory_order_seq_cst);
  SnapshotEpoch* epoch = nullptr;
  for (int attempt = 0; attempt < 4 && epoch == nullptr; ++attempt) {
    if (!accepting_fast.load(std::memory_order_seq_cst)) break;
    SnapshotEpoch* candidate =
        current_epoch_fast.load(std::memory_order_seq_cst);
    if (candidate == nullptr) break;
    // No pre-CAS revalidation load: a retirement between the load above and the
    // reference increment is handled by the post-CAS check below, which is the
    // authoritative one. The retired epoch cannot be freed while this attempt
    // is registered (see the attempt counter above), so touching `refs` is
    // safe; if the CAS lands on a retired epoch the post-CAS load observes the
    // cleared fast pointer and the reference is released. A reclaimer that
    // already claimed the epoch is rejected by the DEAD bit, which the CAS
    // itself cannot overwrite. This removes one seq_cst load per pin without
    // weakening either invariant.
    uint32_t word = candidate->refs.load(std::memory_order_relaxed);
    if ((word & kEpochDeadBit) != 0) continue;
    if (!candidate->refs.compare_exchange_weak(
            word, word + 1, std::memory_order_seq_cst,
            std::memory_order_relaxed)) {
      continue;
    }
    // The reference now protects the epoch from reclamation.
    if (current_epoch_fast.load(std::memory_order_seq_cst) != candidate) {
      // Retired between the load above and the increment: drop it and try again
      // (or fall back to the slow path when the retries are exhausted).
      ReleaseEpochRef(candidate);
      continue;
    }
    epoch = candidate;
  }
  if (epoch != nullptr) {
    // Safe: the pinned reference keeps the epoch and its store owner alive.
    *store_owner = epoch->store_owner;
    *pinned = epoch;
  }
  attempts.value.fetch_sub(1, std::memory_order_seq_cst);
  if (pending_reclaim.load(std::memory_order_seq_cst)) {
    MaybeFreeDeadEpochs();
  }
  return epoch != nullptr;
}

void FactStoreImpl::ReleaseEpochRef(SnapshotEpoch* epoch) {
  if (epoch == nullptr) return;
  // The caller holds a reference, so the epoch cannot be freed concurrently:
  // every reclamation path starts from the same zero-count transition and only
  // the thread that observes the final decrement proceeds.
  //
  // Register this release in the sharded reclaim counters *before* the
  // decrement. A last-reader release may be the thread that observes refs == 0
  // and then blocks on epoch_mutex to claim the epoch; a close that drains a
  // previously closed shell must observe an in-flight reclaimer instead of
  // freeing memory this thread is about to touch. Registering before the
  // decrement makes that ordering unconditional for every release.
  const size_t shard = SnapshotEpochShardIndex();
  EpochAttemptCounter& attempts = reclaim_attempts[shard];
  attempts.value.fetch_add(1, std::memory_order_seq_cst);
  const uint32_t previous =
      epoch->refs.fetch_sub(1, std::memory_order_acq_rel);
  assert(previous >= 1 && (previous & kEpochDeadBit) == 0);
  if (previous == 1) {
    // A zero reference count is only reachable for a retired epoch: the store's
    // owner reference keeps the current epoch above zero and every as_of epoch
    // is retired from birth. The reclamation path asserts that invariant.
    assert(epoch->retired.load(std::memory_order_relaxed));
    if (epoch_release_before_reclaim_observer_for_testing) {
      // Test-only window: the count is already zero and this thread has not yet
      // taken epoch_mutex, which is exactly the state a destructor force-detach
      // must tolerate (see ForceDetachEpochsLocked). Cold path: only the last
      // reference of a retired epoch reaches here, and it immediately takes
      // epoch_mutex and releases a snapshot lease. Must not call back into the
      // store.
      epoch_release_before_reclaim_observer_for_testing();
    }
    std::lock_guard<std::mutex> lock(epoch_mutex);
    ReclaimEpochLocked(epoch);
  }
  // Free any DEAD epoch parked by this reclaimer *before* the attempt counter is
  // dropped: the decrement must be the last access to the store, so a Close that
  // drains the counter can free a previously closed shell without racing a
  // thread that still touches it. Every access to the parker's own epoch ended
  // with ReclaimEpochLocked above, so freeing it here is safe.
  if (pending_reclaim.load(std::memory_order_seq_cst)) {
    MaybeFreeDeadEpochs();
  }
  attempts.value.fetch_sub(1, std::memory_order_seq_cst);
}

void FactStoreImpl::CloseEpochsLocked() {
  // Called by a successful close: no lock-free pin attempt is in flight and no
  // registered epoch has a reader reference (the current epoch keeps only the
  // store's owner reference). Epoch memory is deliberately not freed here: a
  // last-reader release that raced this close may still hold the pointer, so it
  // is handed to the close graveyard and freed with the store.
  for (SnapshotEpoch* epoch : retired_epochs) {
    assert(EpochRefs(epoch) == 0);
    epoch->refs.store(kEpochDeadBit, std::memory_order_seq_cst);
    ReleaseEpochPayloadLocked(epoch, /*notify=*/false);
    if (ClaimEpochMemoryLocked(epoch)) close_graveyard.push_back(epoch);
  }
  retired_epochs.clear();
  if (current_epoch != nullptr) {
    assert(EpochRefs(current_epoch) <= 1);
    current_epoch_fast.store(nullptr, std::memory_order_seq_cst);
    current_epoch->refs.store(kEpochDeadBit, std::memory_order_seq_cst);
    ReleaseEpochPayloadLocked(current_epoch, /*notify=*/false);
    if (ClaimEpochMemoryLocked(current_epoch)) {
      close_graveyard.push_back(current_epoch);
    }
    current_epoch = nullptr;
  }
}

void FactStoreImpl::ForceDetachEpochsLocked() {
  // Destructor-only teardown. Unlike CloseEpochsLocked this must tolerate live
  // reader references because the FactStore owner is going away while a
  // StoreSnapshot handle may still exist (Close reported SnapshotPinned). We
  // release every epoch payload (RocksDB snapshot lease + the strong store
  // back-reference) so the database handle can actually be closed, but keep the
  // epoch memory alive in the close graveyard and mark it retired: a later
  // ReleaseEpochRef from an outliving handle then decrements to zero, takes the
  // DEAD bit and finds both the payload and the memory already claimed, which is
  // a no-op instead of a use-after-free or a double free.
  //
  // The claim below also tolerates a *concurrent* last-reader release: such a
  // release registers its reclaim attempt and decrements the count to zero
  // before it blocks on epoch_mutex, so a reader-held epoch may already be at
  // zero by the time this function runs. Claiming payload+memory here makes the
  // blocked reclaimer a no-op (its ReclaimEpochLocked compare-exchange for the
  // payload fails, so it neither fires the destroyed observer nor frees the
  // memory). Reading through a handle concurrently with destruction is still not
  // supported; that requires the owner/handle serialization documented at
  // ~FactStore.
  if (force_detach_epochs_observer_for_testing) {
    // Test-only window: runs under epoch_mutex, before the registry is collected,
    // so a regression test can hold a last-reader release in the zero-count
    // window this function has to tolerate. Must not call back into the store.
    force_detach_epochs_observer_for_testing();
  }
  std::vector<SnapshotEpoch*> owner_held;
  // as_of cache epochs are registered in retired_epochs as well, but their cache
  // entry owns one reference. Track them so they are handled exactly once (as
  // owner-held) and never also as a reader-only epoch.
  std::unordered_set<SnapshotEpoch*> cache_owned;
  for (AsOfCacheEntry& entry : as_of_cache) {
    if (entry.epoch == nullptr) continue;
    owner_held.push_back(entry.epoch);
    cache_owned.insert(entry.epoch);
    entry.epoch = nullptr;
    entry.seq = CommitSeq{};
  }
  // Retired epochs have already dropped their owner reference; only reader
  // references can keep them alive.
  std::vector<SnapshotEpoch*> reader_held;
  for (SnapshotEpoch* epoch : retired_epochs) {
    if (cache_owned.count(epoch) == 0) reader_held.push_back(epoch);
  }
  retired_epochs.clear();
  if (current_epoch != nullptr) {
    owner_held.push_back(current_epoch);
    current_epoch = nullptr;
    current_epoch_fast.store(nullptr, std::memory_order_seq_cst);
  }
  const auto preserve = [this](SnapshotEpoch* epoch) {
    epoch->retired.store(true, std::memory_order_release);
    ReleaseEpochPayloadLocked(epoch, /*notify=*/false);
    if (ClaimEpochMemoryLocked(epoch)) close_graveyard.push_back(epoch);
  };
  for (SnapshotEpoch* epoch : owner_held) {
    const uint32_t previous =
        epoch->refs.fetch_sub(1, std::memory_order_acq_rel);
    assert(previous >= 1 && (previous & kEpochDeadBit) == 0);
    if (previous == 1) {
      // No reader is left: the normal reclaimer releases the payload, claims the
      // memory and frees the epoch immediately (or parks it while a pin attempt
      // drains). Pass notify=false: this is teardown, so the destroyed observer
      // must not fire (the owner - and possibly its test observers - is going
      // away).
      ReclaimEpochLocked(epoch, /*notify=*/false);
    } else {
      preserve(epoch);
    }
  }
  for (SnapshotEpoch* epoch : reader_held) {
    // The count can legitimately be zero here: a last-reader release registers
    // its attempt, decrements to zero and only then takes epoch_mutex, so it can
    // be parked behind this critical section. Do not assert on the count (a
    // Debug-only assert would abort the destructor in that window); claim the
    // payload and the epoch memory instead. The blocked reclaimer then finds the
    // payload claimed and the memory owned by the graveyard, and completes as a
    // harmless no-op.
    preserve(epoch);
  }
}

void FactStoreImpl::DestroyEpochsLocked() {
  // Exclusive ownership (store destruction): every remaining reference is the
  // store's own, and no other thread may touch the cache or registry. Observers
  // are not fired here (teardown contract).
  for (AsOfCacheEntry& entry : as_of_cache) {
    if (entry.epoch == nullptr) continue;
    const uint32_t previous =
        entry.epoch->refs.fetch_sub(1, std::memory_order_acq_rel);
    assert(previous >= 1 && (previous & kEpochDeadBit) == 0);
    entry.epoch = nullptr;
    entry.seq = CommitSeq{};
  }
  for (SnapshotEpoch* epoch : close_graveyard) delete epoch;
  close_graveyard.clear();
  for (SnapshotEpoch* epoch : dead_epochs_pending_free) delete epoch;
  dead_epochs_pending_free.clear();
  for (SnapshotEpoch* epoch : retired_epochs) {
    assert(EpochRefs(epoch) == 0);
    epoch->refs.store(kEpochDeadBit, std::memory_order_seq_cst);
    ReleaseEpochPayloadLocked(epoch, /*notify=*/false);
    delete epoch;
  }
  retired_epochs.clear();
  if (current_epoch != nullptr) {
    assert(EpochRefs(current_epoch) <= 1);
    current_epoch->refs.store(kEpochDeadBit, std::memory_order_seq_cst);
    ReleaseEpochPayloadLocked(current_epoch, /*notify=*/false);
    delete current_epoch;
    current_epoch = nullptr;
  }
  current_epoch_fast.store(nullptr, std::memory_order_seq_cst);
  pending_reclaim.store(false, std::memory_order_relaxed);
}

FactStoreImpl::~FactStoreImpl() {
  // The store cannot be destroyed while a StoreSnapshot handle exists (handles
  // own a shared_ptr to it), so every remaining epoch has at most the store's
  // own reference. Observers are not fired here: test lambdas may already be
  // gone by the time the owning FactStore is torn down.
  {
    std::lock_guard<std::mutex> lock(epoch_mutex);
    DestroyEpochsLocked();
  }
  DestroyDatabaseHandles();
}

void FactStoreImpl::DestroyDatabaseHandles() {
  if (!db) return;
  if (default_cf != nullptr) db->DestroyColumnFamilyHandle(default_cf);
  if (facts_cf != nullptr) db->DestroyColumnFamilyHandle(facts_cf);
  if (meta_cf != nullptr) db->DestroyColumnFamilyHandle(meta_cf);
  default_cf = nullptr;
  facts_cf = nullptr;
  meta_cf = nullptr;
  db.reset();
}

void FactStoreImpl::ReleaseRetainedStateForClose() {
  // A successful close leaves the store object alive only so that a lock-free
  // pin that loaded the raw pointer before the close gate can read
  // accepting_fast and fail. Everything expensive or RocksDB-owned must go:
  // otherwise every Open->Close would retain a full store image (the validation
  // caches alone are `validation_cache_bytes`, default 8 MiB, plus the RocksDB
  // statistics object and the property schema catalog).
  version_validation_cache.reset();
  recent_fact_write_index.reset();
  identity_history.reset();
  statistics.reset();
  canonical_page_metrics.reset();
  {
    std::lock_guard<std::mutex> lock(retired_schemas_mutex);
    // Releasing the catalog is safe for both callers: FinishClose drained every
    // epoch reference first, and the force-detach path released every epoch
    // payload under epoch_mutex first, so no *supported* reader can still resolve
    // its cached `schemas` pointer through the store. A handle that outlives the
    // force-detach is detached and must not be read (see ~FactStore); the
    // destruction contract serializes destruction with users of such a handle, so
    // reading concurrently with this release is a contract violation, not a
    // supported path.
    property_schemas.reset();
    retired_schemas.clear();
    current_schemas.store(nullptr, std::memory_order_release);
  }
}

bool FactStoreImpl::RetainedShellIsInertForTesting() {
  if (db || default_cf != nullptr || facts_cf != nullptr ||
      meta_cf != nullptr) {
    return false;
  }
  if (version_validation_cache || recent_fact_write_index || identity_history) {
    return false;
  }
  if (statistics || canonical_page_metrics) return false;
  {
    std::lock_guard<std::mutex> lock(retired_schemas_mutex);
    if (property_schemas) return false;
    if (!retired_schemas.empty()) return false;
  }
  std::lock_guard<std::mutex> lock(epoch_mutex);
  // A successful close detaches every registered epoch into the graveyard and
  // releases its payload (RocksDB lease + store owner), so nothing left in the
  // shell can hold a RocksDB snapshot.
  if (current_epoch != nullptr || !retired_epochs.empty()) return false;
  if (!dead_epochs_pending_free.empty()) return false;
  for (const SnapshotEpoch* epoch : close_graveyard) {
    if (epoch->lease || epoch->store_owner) return false;
  }
  return true;
}

void FactStoreImpl::ClearAsOfCache() {
  std::lock_guard<std::mutex> cache_lock(as_of_cache_mutex);
  std::vector<SnapshotEpoch*> released;
  for (AsOfCacheEntry& entry : as_of_cache) {
    if (entry.epoch == nullptr) continue;
    released.push_back(entry.epoch);
    entry.epoch = nullptr;
    entry.seq = CommitSeq{};
  }
  for (SnapshotEpoch* epoch : released) ReleaseEpochRef(epoch);
}

namespace {

struct CurrentFactInterval {
  ValidTime valid_from;
  std::optional<ValidTime> valid_to;
  CommitSeq commit_seq;
};

struct StrictReadIdentity {
  std::optional<FactEvent> observed_event;
  std::optional<ValidTime> predecessor;
  std::optional<ValidTime> successor;
};

bool SameFactEvent(const FactEvent& left, const FactEvent& right) {
  return left.ref == right.ref && left.valid_from == right.valid_from &&
         left.commit_seq == right.commit_seq && left.operation == right.operation &&
         left.schema_epoch == right.schema_epoch && left.value == right.value;
}

bool SameFactEvent(const std::optional<FactEvent>& left,
                   const std::optional<FactEvent>& right) {
  return left.has_value() == right.has_value() &&
         (!left.has_value() || SameFactEvent(*left, *right));
}

bool SameStrictReadIdentity(const StrictReadIdentity& left,
                            const StrictReadIdentity& right) {
  return SameFactEvent(left.observed_event, right.observed_event) &&
         left.predecessor == right.predecessor && left.successor == right.successor;
}

bool MatchesStrictReadDependency(const StrictReadIdentity& identity,
                                 const StrictReadDependency& dependency) {
  return SameFactEvent(identity.observed_event, dependency.observed_event) &&
         identity.predecessor == dependency.predecessor &&
         identity.successor == dependency.successor;
}

bool IntervalsOverlap(ValidTime left_from, std::optional<ValidTime> left_to,
                      ValidTime right_from, std::optional<ValidTime> right_to) {
  const bool left_before_right_end =
      !right_to.has_value() || left_from.value < right_to->value;
  const bool right_before_left_end =
      !left_to.has_value() || right_from.value < left_to->value;
  return left_before_right_end && right_before_left_end;
}

Status RunVacuumFaultInjector(const FactStoreOptions& options,
                              VacuumFaultPoint point) {
  if (!options.vacuum_fault_injector_for_testing) return Status::OK();
  return options.vacuum_fault_injector_for_testing(point);
}

StatusOr<bool> DeleteObsoleteFactVersions(FactStoreImpl* store,
                                          const FactStoreOptions& fact_store_options,
                                          const VacuumState& state) {
  rocksdb::ReadOptions read_options;
  std::unique_ptr<rocksdb::Iterator> iterator(
      store->db->NewIterator(read_options, store->facts_cf));
  if (!state.cursor.empty()) {
    iterator->Seek(state.cursor);
    if (iterator->Valid() && iterator->key().ToString() == state.cursor) {
      iterator->Next();
    }
  } else {
    iterator->SeekToFirst();
  }
  rocksdb::WriteBatch deletes;
  VacuumCleanupPlanner planner(state.target);
  for (; iterator->Valid(); iterator->Next()) {
    const std::string key = iterator->key().ToString();
    const auto decision = planner.Consider(key);
    if (!decision.ok()) return decision.status();
    if (decision.ValueOrDie().stop_before_key) break;
    if (decision.ValueOrDie().delete_key) {
      deletes.Delete(store->facts_cf, key);
    }
  }
  if (!iterator->status().ok()) {
    return FromRocksDb(iterator->status(), "iterate facts for vacuum");
  }
  const bool completed = !iterator->Valid();
  if (completed) {
    const Status injected =
        RunVacuumFaultInjector(fact_store_options,
                                VacuumFaultPoint::kBeforeCompletion);
    if (!injected.ok()) return injected;
  }
  if (completed) {
    deletes.Delete(store->meta_cf, EncodeVacuumStateKey());
  } else {
    const auto next = EncodeVacuumState(
        VacuumState{state.target, VacuumPhase::kRunning, planner.last_key()});
    if (!next.ok()) return next.status();
    deletes.Put(store->meta_cf, EncodeVacuumStateKey(), next.ValueOrDie());
  }
  rocksdb::WriteOptions options;
  options.sync = true;
  const rocksdb::Status written = store->db->Write(options, &deletes);
  if (!written.ok()) return FromMetadataWriteFailure(written);
  if (!completed) {
    const Status injected =
        RunVacuumFaultInjector(fact_store_options,
                                VacuumFaultPoint::kAfterCleanupBatch);
    if (!injected.ok()) return injected;
  }
  return completed;
}

Status ResumeVacuum(FactStoreImpl* store, const FactStoreOptions& options,
                    VacuumState state) {
  while (true) {
    const auto completed = DeleteObsoleteFactVersions(store, options, state);
    if (!completed.ok()) return completed.status();
    if (completed.ValueOrDie()) return Status::OK();
    std::string encoded;
    const rocksdb::Status got = store->db->Get(
        rocksdb::ReadOptions(), store->meta_cf, EncodeVacuumStateKey(), &encoded);
    if (!got.ok()) return FromRocksDb(got, "read vacuum cursor");
    auto next = DecodeVacuumState(encoded);
    if (!next.ok()) return next.status();
    state = next.ConsumeValueOrDie();
  }
}

// Deletes committed `sequence/<seq>` records below `boundary` after making the
// durable retention watermark cover them (F14). Crash-safety order:
//   1. persist watermark/sequence-retention = boundary with Sync=true;
//   2. only then delete, in bounded WriteBatches.
// A crash between (1) and (2) (or during the deletes) therefore reopens onto a
// watermark that excuses exactly the records that may be missing. Reopen
// validation starts at the watermark; it never sees a gap it cannot explain.
//
// Idempotent and resumable: every call re-scans from the sequence prefix and
// deletes whatever is still below the boundary, so a crash-interrupted prune is
// finished by the next Vacuum or by an open with the option enabled. `boundary`
// is the Vacuum target (= the durable oldest-readable boundary), so this never
// deletes a record a live snapshot can read: a snapshot below the boundary is
// already rejected by BeginSnapshot.
// Never invoked unless FactStoreOptions::prune_sequence_records_on_vacuum is
// set, so the default meta layout and reopen contract are unchanged.
Status PruneSequenceRecords(FactStoreImpl* store,
                            const FactStoreOptions& options,
                            CommitSeq boundary) {
  if (boundary.value <= 1) return Status::OK();
  const uint64_t durable = store->sequence_retention_watermark.load(
      std::memory_order_acquire);
  if (durable < boundary.value) {
    const Status before = RunVacuumFaultInjector(
        options, VacuumFaultPoint::kBeforeSequencePruneWatermark);
    if (!before.ok()) return before;
    const auto watermark = EncodeWatermark(boundary);
    if (!watermark.ok()) return watermark.status();
    rocksdb::WriteBatch batch;
    batch.Put(store->meta_cf, EncodeSequenceRetentionWatermarkKey(),
              watermark.ValueOrDie());
    rocksdb::WriteOptions write_options;
    write_options.sync = true;
    const rocksdb::Status written = store->db->Write(write_options, &batch);
    if (!written.ok()) return FromMetadataWriteFailure(written);
    store->sequence_retention_watermark.store(boundary.value,
                                              std::memory_order_release);
    const Status after = RunVacuumFaultInjector(
        options, VacuumFaultPoint::kAfterSequencePruneWatermark);
    if (!after.ok()) return after;
  }
  constexpr size_t kMaxKeysPerBatch = 1024;
  const size_t keys_per_batch =
      options.sequence_prune_batch_size_for_testing == 0
          ? kMaxKeysPerBatch
          : std::min(kMaxKeysPerBatch,
                     options.sequence_prune_batch_size_for_testing);
  while (true) {
    std::unique_ptr<rocksdb::Iterator> iterator(
        store->db->NewIterator(rocksdb::ReadOptions(), store->meta_cf));
    iterator->Seek(kSequenceMetaPrefix);
    rocksdb::WriteBatch deletes;
    size_t batch_keys = 0;
    for (; iterator->Valid() &&
           StartsWith(iterator->key(), kSequenceMetaPrefix);
         iterator->Next()) {
      if (batch_keys == keys_per_batch) break;
      if (iterator->key().size() != kSequenceMetaKeyBytes) {
        return Status::Corruption(
            "sequence retention", "invalid durable sequence record key");
      }
      const uint64_t sequence = ParseSequenceMetaKey(iterator->key());
      if (sequence >= boundary.value) break;
      deletes.Delete(store->meta_cf, iterator->key());
      ++batch_keys;
    }
    if (!iterator->status().ok()) {
      return FromRocksDb(iterator->status(),
                         "iterate sequence records for retention pruning");
    }
    if (batch_keys == 0) break;
    rocksdb::WriteOptions write_options;
    write_options.sync = options.sequence_prune_sync;
    const rocksdb::Status written = store->db->Write(write_options, &deletes);
    if (!written.ok()) return FromMetadataWriteFailure(written);
    const Status after_batch = RunVacuumFaultInjector(
        options, VacuumFaultPoint::kAfterSequencePruneBatch);
    if (!after_batch.ok()) return after_batch;
  }
  return RunVacuumFaultInjector(
      options, VacuumFaultPoint::kBeforeSequencePruneCompletion);
}

std::optional<std::vector<CurrentFactInterval>> CachedFactIntervals(
    FactStoreImpl* store, const FactRef& ref) {
  if (store->version_validation_cache == nullptr) return std::nullopt;
  const auto cached = store->version_validation_cache->Lookup(ref);
  if (!cached.has_value()) return std::nullopt;
  std::vector<CurrentFactInterval> intervals;
  intervals.reserve(cached->size());
  for (const internal::ValidationBoundary& boundary : *cached) {
    intervals.push_back(CurrentFactInterval{boundary.valid_from, std::nullopt,
                                            boundary.commit_seq});
  }
  for (size_t index = 1; index < intervals.size(); ++index) {
    intervals[index].valid_to = intervals[index - 1].valid_from;
  }
  return intervals;
}

StatusOr<std::vector<CurrentFactInterval>> ScanCurrentFactIntervals(
    FactStoreImpl* store, const FactRef& ref, rocksdb::Iterator* iterator,
    uint64_t observed_publish_generation) {
  if (iterator == nullptr) {
    return Status::InvalidArgument("validation scan", "missing fact iterator");
  }
  const std::string prefix = EncodeFactIdentityPrefix(
      ref.part_id(), ref.family(), ref.property_id(), ref.entity_id());
  store->canonical_validation_scans.fetch_add(1, std::memory_order_relaxed);
  if (store->validation_scan_observer_for_testing) {
    store->validation_scan_observer_for_testing();
  }
  std::vector<CurrentFactInterval> intervals;
  std::optional<ValidTime> previous_boundary;
  for (iterator->Seek(prefix);
       iterator->Valid() && StartsWith(iterator->key(), prefix);
       iterator->Next()) {
    const auto decoded = DecodeFactKey(iterator->key().ToString());
    if (!decoded.ok()) return decoded.status();
    if (previous_boundary.has_value() &&
        decoded.ValueOrDie().valid_from == *previous_boundary) {
      continue;
    }
    store->canonical_validation_boundaries.fetch_add(1, std::memory_order_relaxed);
    intervals.push_back(CurrentFactInterval{decoded.ValueOrDie().valid_from,
                                             previous_boundary,
                                             decoded.ValueOrDie().commit_seq});
    previous_boundary = decoded.ValueOrDie().valid_from;
  }
  if (!iterator->status().ok()) {
    return FromRocksDb(iterator->status(), "iterate current fact intervals");
  }
  if (store->version_validation_cache != nullptr) {
    std::vector<internal::ValidationBoundary> boundaries;
    boundaries.reserve(intervals.size());
    for (const CurrentFactInterval& interval : intervals) {
      boundaries.push_back(
          internal::ValidationBoundary{interval.valid_from, interval.commit_seq});
    }
    store->version_validation_cache->Prime(ref, std::move(boundaries),
                                           observed_publish_generation);
  }
  return intervals;
}

StatusOr<std::vector<CurrentFactInterval>> CurrentFactIntervals(
    FactStoreImpl* store, const FactRef& ref) {
  if (auto cached = CachedFactIntervals(store, ref); cached.has_value()) {
    return std::move(*cached);
  }
  const uint64_t observed_publish_generation =
      store->version_validation_cache != nullptr
          ? store->version_validation_cache->publish_generation()
          : 0;
  rocksdb::ReadOptions options;
  std::unique_ptr<rocksdb::Iterator> iterator(
      store->db->NewIterator(options, store->facts_cf));
  return ScanCurrentFactIntervals(store, ref, iterator.get(),
                                  observed_publish_generation);
}

void PublishValidationCache(FactStoreImpl* store,
                            const std::vector<PendingFactMutation>& mutations,
                            CommitSeq commit_seq) {
  if (store->version_validation_cache == nullptr) return;
  for (const PendingFactMutation& mutation : mutations) {
    store->version_validation_cache->Publish(mutation, commit_seq);
  }
}

void PublishValidationCache(FactStoreImpl* store,
                            const StoreCommitBatch& batch,
                            CommitSeq commit_seq) {
  PublishValidationCache(store, batch.mutations, commit_seq);
}

void PublishRecentFactWriteIndex(FactStoreImpl* store,
                                 const std::vector<PendingFactMutation>& mutations,
                                 CommitSeq commit_seq) {
  if (store->recent_fact_write_index == nullptr &&
      store->identity_history == nullptr) {
    return;
  }
  for (const PendingFactMutation& mutation : mutations) {
    if (store->recent_fact_write_index != nullptr) {
      store->recent_fact_write_index->Publish(mutation.ref, commit_seq);
    }
    if (store->identity_history != nullptr) {
      store->identity_history->Publish(mutation.ref);
    }
  }
}

void PublishRecentFactWriteIndex(FactStoreImpl* store,
                                 const StoreCommitBatch& batch,
                                 CommitSeq commit_seq) {
  PublishRecentFactWriteIndex(store, batch.mutations, commit_seq);
}

StatusOr<TemporalNeighborhood> ReadTemporalNeighborhoodWithIterator(
    FactStoreImpl* store, const FactRef& ref, ValidTime valid_time,
    CommitSeq commit_limit, rocksdb::Iterator* iterator) {
  if (iterator == nullptr) {
    return Status::InvalidArgument("temporal neighborhood",
                                   "missing fact iterator");
  }
  const Status valid = ref.Validate();
  if (!valid.ok()) return valid;
  if (store->identity_history != nullptr &&
      !store->identity_history->MightContain(ref)) {
    // No fact of this identity was ever written, so the chain is empty at every
    // snapshot. Skip the canonical iterator setup entirely.
    return TemporalNeighborhood{};
  }
  store->point_read_operations.fetch_add(1, std::memory_order_relaxed);
  CanonicalReadPerfTracker physical_read(&store->canonical_read_physical_bytes);
  const std::string seek = EncodeFactKey(
      ref, valid_time, CommitSeq{std::numeric_limits<uint64_t>::max()});
  if (seek.empty()) {
    return Status::InvalidArgument("temporal neighborhood", "invalid fact key");
  }
  const std::string prefix = seek.substr(0, kFactIdentityPrefixBytes);

  TemporalNeighborhood neighborhood;
  rocksdb::Iterator* forward = iterator;
  std::optional<ValidTime> current_boundary;
  bool current_boundary_visible = false;
  forward->Seek(seek);
  for (; forward->Valid() && StartsWith(forward->key(), prefix); forward->Next()) {
    const auto decoded_key = DecodeFactKey(forward->key().ToString());
    if (!decoded_key.ok()) return decoded_key.status();
    const DecodedFactKey& key = decoded_key.ValueOrDie();
    if (key.commit_seq.value > commit_limit.value) continue;
    if (!current_boundary.has_value() || current_boundary != key.valid_from) {
      current_boundary = key.valid_from;
      current_boundary_visible = false;
    }
    if (current_boundary_visible) continue;
    const auto event = DecodeFactValue(key.ref, key.valid_from, key.commit_seq,
                                       forward->value().ToString());
    if (!event.ok()) return event.status();
    current_boundary_visible = true;
    if (!neighborhood.observed.has_value()) {
      neighborhood.observed = event.ValueOrDie();
      if (key.valid_from.value < valid_time.value) {
        neighborhood.predecessor = key.valid_from;
        break;
      }
      continue;
    }
    neighborhood.predecessor = key.valid_from;
    break;
  }
  if (!forward->status().ok()) {
    return FromRocksDb(forward->status(), "iterate temporal neighborhood");
  }

  // Both scans use the same RocksDB snapshot and options. Reposition the
  // iterator after checking the forward scan's status; no borrowed Slice is
  // retained in the decoded neighborhood.
  rocksdb::Iterator* backward = forward;
  std::optional<ValidTime> successor_boundary;
  bool successor_boundary_visible = false;
  backward->SeekForPrev(seek);
  for (; backward->Valid() && StartsWith(backward->key(), prefix); backward->Prev()) {
    const auto decoded_key = DecodeFactKey(backward->key().ToString());
    if (!decoded_key.ok()) return decoded_key.status();
    const DecodedFactKey& key = decoded_key.ValueOrDie();
    if (!successor_boundary.has_value() || successor_boundary != key.valid_from) {
      if (successor_boundary_visible) {
        neighborhood.successor = successor_boundary;
        break;
      }
      successor_boundary = key.valid_from;
      successor_boundary_visible = false;
    }
    if (key.commit_seq.value <= commit_limit.value) {
      successor_boundary_visible = true;
    }
  }
  if (!neighborhood.successor.has_value() && successor_boundary_visible) {
    neighborhood.successor = successor_boundary;
  }
  if (!backward->status().ok()) {
    return FromRocksDb(backward->status(), "iterate temporal neighborhood");
  }
  return neighborhood;
}

StatusOr<TemporalNeighborhood> ReadTemporalNeighborhood(
    FactStoreImpl* store, const FactRef& ref, ValidTime valid_time,
    CommitSeq commit_limit, const rocksdb::Snapshot* rocks_snapshot) {
  rocksdb::ReadOptions options;
  options.snapshot = rocks_snapshot;
  std::unique_ptr<rocksdb::Iterator> iterator(
      store->db->NewIterator(options, store->facts_cf));
  return ReadTemporalNeighborhoodWithIterator(store, ref, valid_time,
                                              commit_limit, iterator.get());
}

StatusOr<StrictReadIdentity> CurrentStrictReadIdentity(
    FactStoreImpl* store, const FactRef& ref, ValidTime valid_time,
    CommitSeq commit_limit) {
  const auto neighborhood = ReadTemporalNeighborhood(
      store, ref, valid_time, commit_limit, nullptr);
  if (!neighborhood.ok()) return neighborhood.status();
  return StrictReadIdentity{neighborhood.ValueOrDie().observed,
                            neighborhood.ValueOrDie().predecessor,
                            neighborhood.ValueOrDie().successor};
}

Status ValidateSnapshotWriteDependencies(
    FactStoreImpl* store,
    const std::vector<SnapshotWriteDependency>& dependencies) {
  const auto ref_less = [](const FactRef& left, const FactRef& right) {
    return std::tuple{left.part_id().value,
                      static_cast<uint8_t>(left.family()),
                      left.property_id().value, left.entity_id()} <
           std::tuple{right.part_id().value,
                      static_cast<uint8_t>(right.family()),
                      right.property_id().value, right.entity_id()};
  };
  std::vector<size_t> pending;
  pending.reserve(dependencies.size());
  for (size_t index = 0; index < dependencies.size(); ++index) {
    const SnapshotWriteDependency& dependency = dependencies[index];
    if (dependency.snapshot_seq.value > store->visible_seq.value) {
      return Status::InvalidArgument("commit",
                                     "snapshot dependency exceeds visible watermark");
    }
    if (store->recent_fact_write_index != nullptr &&
        store->recent_fact_write_index->CanProveUnchanged(
            dependency.ref, dependency.snapshot_seq)) {
      continue;
    }
    if (store->identity_history != nullptr &&
        !store->identity_history->MightContain(dependency.ref)) {
      // The identity was never written, so no canonical event can conflict.
      continue;
    }
    pending.push_back(index);
  }
  // Sort by fact identity and group in place. One iterator serves every group;
  // canonical order keeps each Seek at or after the previous prefix, and no
  // per-dependency prefix string is allocated.
  std::sort(pending.begin(), pending.end(),
            [&dependencies, &ref_less](size_t left, size_t right) {
              return ref_less(dependencies[left].ref, dependencies[right].ref);
            });
  // Capture the publication generation before the shared scan iterator is
  // created: a commit published after this point may be invisible to the scan,
  // so any chain primed from it must not replace a newer canonical view.
  const uint64_t observed_publish_generation =
      store->version_validation_cache != nullptr
          ? store->version_validation_cache->publish_generation()
          : 0;
  std::unique_ptr<rocksdb::Iterator> iterator;
  for (size_t begin = 0; begin < pending.size();) {
    const FactRef& ref = dependencies[pending[begin]].ref;
    size_t end = begin + 1;
    while (end < pending.size() && dependencies[pending[end]].ref == ref) {
      ++end;
    }
    std::optional<std::vector<CurrentFactInterval>> intervals =
        CachedFactIntervals(store, ref);
    if (!intervals.has_value()) {
      if (iterator == nullptr) {
        iterator.reset(
            store->db->NewIterator(rocksdb::ReadOptions(), store->facts_cf));
      }
      auto scanned = ScanCurrentFactIntervals(
          store, ref, iterator.get(), observed_publish_generation);
      if (!scanned.ok()) return scanned.status();
      intervals = scanned.ConsumeValueOrDie();
    }
    for (size_t position = begin; position < end; ++position) {
      const SnapshotWriteDependency& dependency = dependencies[pending[position]];
      for (const CurrentFactInterval& interval : *intervals) {
        if (interval.commit_seq.value <= dependency.snapshot_seq.value) continue;
        if (IntervalsOverlap(dependency.valid_from, dependency.successor,
                             interval.valid_from, interval.valid_to)) {
          return Status::Conflict(
              "commit", "snapshot write conflicts with a later overlapping event");
        }
      }
    }
    begin = end;
  }
  return Status::OK();
}

Status ValidateStrictReadDependencies(
    FactStoreImpl* store,
    const std::vector<StrictReadDependency>& dependencies) {
  for (const StrictReadDependency& dependency : dependencies) {
    if (dependency.snapshot_seq.value > store->visible_seq.value) {
      return Status::InvalidArgument("commit",
                                     "strict dependency exceeds visible watermark");
    }
    const auto at_snapshot = CurrentStrictReadIdentity(
        store, dependency.ref, dependency.valid_time, dependency.snapshot_seq);
    if (!at_snapshot.ok()) return at_snapshot.status();
    if (!MatchesStrictReadDependency(at_snapshot.ValueOrDie(), dependency)) {
      return Status::Conflict("commit",
                              "strict read identity does not match its snapshot");
    }
    const auto current = CurrentStrictReadIdentity(
        store, dependency.ref, dependency.valid_time, store->visible_seq);
    if (!current.ok()) return current.status();
    if (!SameStrictReadIdentity(at_snapshot.ValueOrDie(), current.ValueOrDie())) {
      return Status::Conflict("commit", "strict read was changed after its snapshot");
    }
  }
  return Status::OK();
}

StatusOr<std::optional<EdgeIdentity>> ReadLatestEdgeIdentity(
    FactStoreImpl* store, EdgeRef edge) {
  const FactRef ref(edge.home_part_id, FactFamily::kEdgeIdentity,
                    PropertyId{}, edge.edge_id.value);
  const std::string seek = EncodeFactKey(
      ref, ValidTime{0}, CommitSeq{std::numeric_limits<uint64_t>::max()});
  if (seek.empty()) return Status::InvalidArgument("edge identity", "invalid edge key");
  const std::string prefix = seek.substr(0, kFactIdentityPrefixBytes);
  std::unique_ptr<rocksdb::Iterator> iterator(
      store->db->NewIterator(rocksdb::ReadOptions(), store->facts_cf));
  for (iterator->Seek(seek); iterator->Valid() && StartsWith(iterator->key(), prefix);
       iterator->Next()) {
    const auto decoded_key = DecodeFactKey(iterator->key().ToString());
    if (!decoded_key.ok()) return decoded_key.status();
    const auto decoded_value = DecodeFactValue(
        decoded_key.ValueOrDie().ref, decoded_key.ValueOrDie().valid_from,
        decoded_key.ValueOrDie().commit_seq, iterator->value().ToString());
    if (!decoded_value.ok()) return decoded_value.status();
    if (decoded_value.ValueOrDie().operation == FactOperation::kDelete) {
      return std::optional<EdgeIdentity>{};
    }
    if (!decoded_value.ValueOrDie().edge_identity.has_value()) {
      return Status::Corruption("edge identity", "identity fact lacks identity payload");
    }
    return decoded_value.ValueOrDie().edge_identity;
  }
  if (!iterator->status().ok()) {
    return FromRocksDb(iterator->status(), "read edge identity fact");
  }
  return std::optional<EdgeIdentity>{};
}

Status ValidateEdgeIdentitiesForCommit(FactStoreImpl* store,
                                       const StoreCommitBatch& batch) {
  for (const PendingFactMutation& mutation : batch.mutations) {
    if (mutation.ref.family() != FactFamily::kEdgeState ||
        mutation.operation != FactOperation::kPut) {
      continue;
    }
    const EdgeRef edge{mutation.ref.part_id(), EdgeId{mutation.ref.entity_id()}};
    const auto existing_identity = ReadLatestEdgeIdentity(store, edge);
    if (!existing_identity.ok()) return existing_identity.status();
    if (!existing_identity.ValueOrDie().has_value()) {
      const bool supplied_identity = std::any_of(
          batch.edge_identities.begin(), batch.edge_identities.end(),
          [&mutation](const EdgeIdentity& identity) {
            return identity.home_part_id == mutation.ref.part_id() &&
                   identity.edge_id.value == mutation.ref.entity_id();
          });
      if (!supplied_identity) {
        return Status::InvalidArgument("commit",
                                       "first edge assertion requires identity");
      }
    }
  }

  for (const EdgeIdentity& identity : batch.edge_identities) {
    const auto existing_identity = ReadLatestEdgeIdentity(store, identity.edge_ref());
    if (!existing_identity.ok()) return existing_identity.status();
    if (existing_identity.ValueOrDie().has_value() &&
        *existing_identity.ValueOrDie() != identity) {
      return Status::IdentityConflict("commit",
                                      "edge ID has a different identity");
    }
  }
  return Status::OK();
}

}  // namespace

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

Status LoadTransactionAllocatorState(FactStoreImpl* store,
                                     IdAllocatorState* state) {
  std::string encoded;
  const rocksdb::Status got = store->db->Get(
      rocksdb::ReadOptions(), store->meta_cf,
      EncodeAllocatorMetaKey(IdKind::kTransaction), &encoded);
  if (got.ok()) {
    const auto decoded = DecodeIdAllocatorState(encoded);
    if (!decoded.ok()) return decoded.status();
    if (decoded.ValueOrDie().kind != IdKind::kTransaction) {
      return Status::Corruption("fact store", "transaction allocator has wrong ID kind");
    }
    *state = decoded.ValueOrDie();
    return Status::OK();
  }
  if (!got.IsNotFound()) return FromRocksDb(got, "read transaction ID allocator");

  uint64_t highest = 0;
  // A pruned database no longer has the full history; the committed
  // transaction IDs still live in the surviving records, so the cold-path
  // rebuild starts at the durable sequence-retention watermark instead of
  // sequence 1. IDs allocated to transactions that never committed are not in
  // either scan (that was already true before pruning).
  const uint64_t first_sequence =
      std::max<uint64_t>(1, store->sequence_retention_watermark.load(
                                std::memory_order_acquire));
  std::string first_key;
  AssignSequenceMetaKey(first_sequence, &first_key);
  std::unique_ptr<rocksdb::Iterator> iterator(
      store->db->NewIterator(rocksdb::ReadOptions(), store->meta_cf));
  for (iterator->Seek(first_key);
       iterator->Valid() && StartsWith(iterator->key(), kSequenceMetaPrefix);
       iterator->Next()) {
    if (iterator->key().size() != kSequenceMetaKeyBytes) {
      return Status::Corruption("fact store",
                                "invalid durable sequence record key");
    }
    const auto sequence = DecodeSequenceRecord(
        iterator->value().ToString(),
        CommitSeq{ParseSequenceMetaKey(iterator->key())});
    if (!sequence.ok()) return sequence.status();
    highest = std::max(highest, sequence.ValueOrDie().txn_id.value);
  }
  if (!iterator->status().ok()) {
    return FromRocksDb(iterator->status(), "iterate transaction IDs");
  }
  if (highest == std::numeric_limits<uint64_t>::max()) {
    return Status::ResourceExhausted("transaction", "transaction ID space exhausted");
  }
  *state = IdAllocatorState{IdKind::kTransaction, highest + 1};
  return Status::OK();
}

// Reads the durable meta sequence-format marker. std::nullopt means the
// database predates the marker (every record so far is v1 or was written by a
// dual-read writer without the opt-in). A marker that fails framing/CRC or
// records an unsupported format fails Open as Corruption.
StatusOr<std::optional<uint32_t>> LoadMetaSequenceFormatMarker(
    FactStoreImpl* store) {
  std::string encoded;
  const rocksdb::Status got = store->db->Get(
      rocksdb::ReadOptions(), store->meta_cf, EncodeSequenceFormatKey(),
      &encoded);
  if (got.IsNotFound()) return std::optional<uint32_t>{};
  if (!got.ok()) {
    return FromRocksDb(got, "read meta sequence format marker");
  }
  const auto decoded = DecodeSequenceFormat(encoded);
  if (!decoded.ok()) return decoded.status();
  const uint32_t format = decoded.ValueOrDie();
  if (format != kMetaSequenceFormatV1 && format != kMetaSequenceFormatV2) {
    return Status::Corruption(
        "fact store",
        "unsupported meta sequence format marker: " + std::to_string(format));
  }
  return std::optional<uint32_t>{format};
}

// Durably records the meta sequence format. Sync=true is the crash-safety
// contract: Open must not accept a v2 commit until the marker that makes v2
// authoritative on every later open has reached the WAL. A failed write fails
// Open; the caller has not accepted any commit yet.
Status WriteMetaSequenceFormatMarker(FactStoreImpl* store,
                                     const FactStoreOptions& options,
                                     uint32_t format) {
  const auto encoded = EncodeSequenceFormat(format);
  if (!encoded.ok()) return encoded.status();
  rocksdb::WriteOptions write_options;
  write_options.sync = true;
  if (options.meta_sequence_format_marker_write_observer_for_testing) {
    options.meta_sequence_format_marker_write_observer_for_testing(
        write_options.sync);
  }
  if (options.meta_sequence_format_marker_write_fault_injector_for_testing) {
    const Status injected =
        options.meta_sequence_format_marker_write_fault_injector_for_testing();
    if (!injected.ok()) return injected;
  }
  const rocksdb::Status written =
      store->db->Put(write_options, store->meta_cf, EncodeSequenceFormatKey(),
                     encoded.ValueOrDie());
  if (!written.ok()) {
    return FromRocksDb(written, "write meta sequence format marker");
  }
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
  std::shared_ptr<const FactStoreImpl::PropertySchemas> immutable_schemas =
      std::move(schemas);
  store->PublishSchemas(std::move(immutable_schemas));
  return Status::OK();
}

// Validates one committed sequence record from bytes the reopen scan already
// holds. The scan owns the record's key and value through its meta-CF iterator,
// so re-reading the sequence key (the previous behaviour) was one extra meta Get
// for every committed sequence: 42.5M duplicate Gets per reopen on the
// production dataset. The transaction outcome record is still read: it lives
// under a different key, and its *existence* is what this check pins, which the
// sequence record cannot prove.
// Validates one committed sequence record from the bytes the reopen scan
// already holds (structure, embedded fact keys and, within the retention
// window, the facts themselves). The transaction outcome is validated in
// batches by ValidateSequenceOutcomeBatch; see ValidateCommittedSequences.
Status ValidateSequenceRecordContent(FactStoreImpl* store, CommitSeq commit_seq,
                                     const SequenceRecordView& sequence,
                                     bool require_facts,
                                     uint64_t* fact_gets) {
  if (sequence.commit_seq != commit_seq) {
    return Status::Corruption("fact store",
                              "sequence record has wrong commit sequence");
  }
  const Status structure = sequence.Validate();
  if (!structure.ok()) {
    return Status::Corruption("sequence record", structure.ToString());
  }
  for (uint32_t index = 0; index < sequence.fact_count; ++index) {
    const std::string_view fact_key = sequence.identities.substr(
        static_cast<size_t>(index) * kEncodedFactKeyBytes,
        kEncodedFactKeyBytes);
    const auto decoded_key = DecodeFactKey(fact_key);
    if (!decoded_key.ok() || decoded_key.ValueOrDie().commit_seq != commit_seq) {
      return Status::Corruption("fact store",
                                "sequence contains invalid fact key");
    }
    if (!require_facts) continue;
    if (fact_gets != nullptr) ++*fact_gets;
    std::string encoded_fact;
    const rocksdb::Status got_fact = store->db->Get(
        rocksdb::ReadOptions(), store->facts_cf,
        rocksdb::Slice(fact_key.data(), fact_key.size()), &encoded_fact);
    if (!got_fact.ok()) {
      return Status::Corruption("fact store",
                                "sequence record is missing fact");
    }
    const auto fact = DecodeFactValue(decoded_key.ValueOrDie().ref,
                                      decoded_key.ValueOrDie().valid_from,
                                      commit_seq, encoded_fact);
    if (!fact.ok()) return fact.status();
  }
  return Status::OK();
}

// Batch of outcome checks: one MultiGet per 1024 committed sequences replaces
// one meta-CF Get per sequence (42.5M Gets per reopen on the production
// dataset).
Status ValidateSequenceOutcomeBatch(FactStoreImpl* store,
                                    const std::vector<std::string>& keys,
                                    const std::vector<TxnId>& txn_ids,
                                    const std::vector<CommitSeq>& commit_seqs,
                                    uint64_t* meta_gets) {
  if (keys.empty()) return Status::OK();
  if (meta_gets != nullptr) ++*meta_gets;
  std::vector<rocksdb::Slice> slices;
  slices.reserve(keys.size());
  for (const std::string& key : keys) slices.emplace_back(key);
  std::vector<rocksdb::PinnableSlice> values(keys.size());
  std::vector<rocksdb::Status> statuses(keys.size());
  store->db->MultiGet(rocksdb::ReadOptions(), store->meta_cf, keys.size(),
                      slices.data(), values.data(), statuses.data());
  for (size_t index = 0; index < keys.size(); ++index) {
    if (!statuses[index].ok()) {
      return Status::Corruption("fact store",
                                "sequence record is missing outcome");
    }
    const auto outcome = DecodeTransactionOutcome(
        std::string(values[index].data(), values[index].size()));
    if (!outcome.ok()) return outcome.status();
    if (outcome.ValueOrDie().txn_id != txn_ids[index] ||
        outcome.ValueOrDie().commit_seq != commit_seqs[index]) {
      return Status::Corruption("fact store", "outcome and sequence disagree");
    }
  }
  return Status::OK();
}

// Reports one completed reopen validation pass to the test-only observer. The
// hook is a no-op when unset and never calls back into the store itself.
void ReportReopenValidationPhase(
    FactStoreImpl* store, std::string_view phase, uint64_t records,
    uint64_t fact_gets, uint64_t meta_gets,
    std::chrono::steady_clock::time_point started) {
  if (!store->reopen_validation_counters_for_testing) return;
  ReopenValidationPhaseCounters counters;
  counters.phase = phase;
  counters.records = records;
  counters.fact_gets = fact_gets;
  counters.meta_gets = meta_gets;
  counters.elapsed_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - started)
          .count());
  store->reopen_validation_counters_for_testing(counters);
}

Status ValidateCommittedSequences(FactStoreImpl* store, CommitSeq visible_seq,
                                  CommitSeq oldest_readable_seq,
                                  CommitSeq sequence_retention_watermark) {
  const auto phase_started = std::chrono::steady_clock::now();
  const auto report_phase = [&](uint64_t records, uint64_t fact_gets,
                                uint64_t meta_gets) {
    ReportReopenValidationPhase(store, "P1_sequence_validation", records,
                                fact_gets, meta_gets, phase_started);
  };
  if (visible_seq.value == 0) {
    report_phase(0, 0, 0);
    return Status::OK();
  }
  // Records below the durable sequence-retention watermark were pruned by an
  // opted-in Vacuum and are allowed to be absent. Everything from the watermark
  // through the visible sequence must still be contiguous; that is the whole
  // point of persisting the boundary before the first delete.
  const uint64_t first_expected =
      std::max<uint64_t>(1, sequence_retention_watermark.value);
  rocksdb::ReadOptions options;
  std::unique_ptr<rocksdb::Iterator> iterator(
      store->db->NewIterator(options, store->meta_cf));
  // Canonical key of the next expected sequence, rebuilt in place. Comparing it
  // against the iterated key proves the record being validated is the one it
  // claims to be (and still bounds the scan at the watermark) without the meta
  // Get the old loop issued per sequence.
  std::string expected_key;
  expected_key.reserve(kSequenceMetaKeyBytes);
  const size_t kOutcomeBatch = store->sequence_validation_outcome_batch_size;
  // The recent-fact cross-check window: facts at or above this floor are read
  // and decoded; older facts are covered by the Vacuum ordering invariant and
  // are detected on read. deep_recovery_validation disables the bound.
  const uint64_t recent_sequences =
      std::max<uint64_t>(1, store->recovery_validation_recent_sequences);
  const uint64_t recent_floor =
      visible_seq.value > recent_sequences
          ? visible_seq.value - recent_sequences + 1
          : 1;
  const uint64_t fact_validation_floor =
      std::max(oldest_readable_seq.value, recent_floor);
  std::vector<std::string> outcome_keys;
  std::vector<TxnId> outcome_txn_ids;
  std::vector<CommitSeq> outcome_commit_seqs;
  outcome_keys.reserve(kOutcomeBatch);
  outcome_txn_ids.reserve(kOutcomeBatch);
  outcome_commit_seqs.reserve(kOutcomeBatch);
  uint64_t fact_gets = 0;
  uint64_t meta_gets = 0;
  uint64_t expected = first_expected;
  AssignSequenceMetaKey(expected, &expected_key);
  for (iterator->Seek(expected_key);
       iterator->Valid() && StartsWith(iterator->key(), kSequenceMetaPrefix) &&
       expected <= visible_seq.value;
       iterator->Next(), ++expected) {
    if (store->sequence_validation_read_source_observer_for_testing) {
      // S1: the record comes from the iterator's own key/value pair.
      store->sequence_validation_read_source_observer_for_testing(true);
    }
    AssignSequenceMetaKey(expected, &expected_key);
    if (iterator->key().compare(rocksdb::Slice(expected_key)) != 0) {
      // The canonical record for this sequence is missing (or the iterated key
      // is not canonical), exactly what the previous per-sequence Get reported
      // when it returned NotFound. Because the scan starts at the durable
      // sequence-retention watermark, reaching this from a pruned database means
      // a gap at or above the watermark, which is still corruption.
      return Status::Corruption("fact store", "missing durable sequence record");
    }
    const auto sequence = DecodeSequenceRecordView(
        std::string_view(iterator->value().data(), iterator->value().size()),
        CommitSeq{expected});
    if (!sequence.ok()) return sequence.status();
    // deep restores the pre-F9 coverage (every fact at or above the retention
    // boundary); it must not re-check facts Vacuum has legitimately removed.
    const bool require_facts =
        expected >= (store->deep_recovery_validation
                         ? oldest_readable_seq.value
                         : fact_validation_floor);
    const Status valid = ValidateSequenceRecordContent(
        store, CommitSeq{expected}, sequence.ValueOrDie(), require_facts,
        &fact_gets);
    if (!valid.ok()) return valid;
    auto transaction_key = EncodeTransactionMetaKey(sequence.ValueOrDie().txn_id);
    if (!transaction_key.ok()) return transaction_key.status();
    outcome_keys.push_back(transaction_key.ConsumeValueOrDie());
    outcome_txn_ids.push_back(sequence.ValueOrDie().txn_id);
    outcome_commit_seqs.push_back(CommitSeq{expected});
    if (outcome_keys.size() == kOutcomeBatch) {
      const Status outcomes = ValidateSequenceOutcomeBatch(
          store, outcome_keys, outcome_txn_ids, outcome_commit_seqs,
          &meta_gets);
      if (!outcomes.ok()) return outcomes;
      outcome_keys.clear();
      outcome_txn_ids.clear();
      outcome_commit_seqs.clear();
    }
  }
  const Status outcomes = ValidateSequenceOutcomeBatch(
      store, outcome_keys, outcome_txn_ids, outcome_commit_seqs, &meta_gets);
  if (!outcomes.ok()) return outcomes;
  if (!iterator->status().ok()) {
    return FromRocksDb(iterator->status(), "iterate durable sequence records");
  }
  if (expected != visible_seq.value + 1) {
    return Status::Corruption("fact store", "visible watermark lacks contiguous sequences");
  }
  if (iterator->Valid() && StartsWith(iterator->key(), kSequenceMetaPrefix)) {
    return Status::Corruption("fact store", "sequence exceeds visible watermark");
  }
  report_phase(expected - first_expected, fact_gets, meta_gets);
  return Status::OK();
}

StoreSnapshot::StoreSnapshot(std::shared_ptr<FactStoreImpl> store,
                             const FactStore* owner, SnapshotEpoch* epoch)
    : store_(std::move(store)), owner_(owner), epoch_(epoch) {}

StoreSnapshot::~StoreSnapshot() { Release(); }

StoreSnapshot::StoreSnapshot(StoreSnapshot&& other) noexcept
    : store_(std::move(other.store_)),
      owner_(other.owner_),
      epoch_(other.epoch_) {
  other.owner_ = nullptr;
  other.epoch_ = nullptr;
}

StoreSnapshot& StoreSnapshot::operator=(StoreSnapshot&& other) noexcept {
  if (this != &other) {
    Release();
    store_ = std::move(other.store_);
    owner_ = other.owner_;
    epoch_ = other.epoch_;
    other.owner_ = nullptr;
    other.epoch_ = nullptr;
  }
  return *this;
}

void StoreSnapshot::Release() {
  if (epoch_ == nullptr) return;
  if (store_) {
    // This may reclaim the epoch (and release its lease) when the handle was
    // the last reader of a retired epoch. The epoch reference count is the only
    // liveness registry: Close and Vacuum read it directly.
    store_->ReleaseEpochRef(epoch_);
  }
  epoch_ = nullptr;
  owner_ = nullptr;
}

CommitSeq StoreSnapshot::commit_seq() const {
  return epoch_ == nullptr ? CommitSeq{} : epoch_->seq;
}

CommitSeq StoreSnapshot::oldest_readable_seq() const {
  return epoch_ == nullptr ? CommitSeq{} : epoch_->oldest_readable;
}

StatusOr<std::optional<PropertyDefinition>> LookupPropertyInSchemas(
    const FactStoreImpl::PropertySchemas& schemas, PropertyId property_id,
    uint32_t schema_epoch) {
  if (!property_id.valid()) {
    return Status::InvalidArgument("property definition", "zero property ID");
  }
  const auto found = schemas.find(property_id.value);
  if (found == schemas.end() || found->second.empty() ||
      schema_epoch > found->second.size()) {
    return std::optional<PropertyDefinition>{};
  }
  if (schema_epoch == 0) return std::optional<PropertyDefinition>{found->second.back()};
  return std::optional<PropertyDefinition>{found->second[schema_epoch - 1]};
}

FactPrefix FactPrefix::Exact(FactRef ref) {
  return FactPrefix(ref.part_id(), ref.family(), ref.property_id(),
                    ref.entity_id());
}

Status SnapshotWriteDependency::Validate() const {
  const Status valid = ref.Validate();
  if (!valid.ok()) return valid;
  if (predecessor.has_value() && predecessor->value >= valid_from.value) {
    return Status::InvalidArgument("snapshot write dependency",
                                   "predecessor is not before mutation");
  }
  if (successor.has_value() && successor->value <= valid_from.value) {
    return Status::InvalidArgument("snapshot write dependency",
                                   "successor is not after mutation");
  }
  return Status::OK();
}

Status StrictReadDependency::Validate() const {
  const Status valid = ref.Validate();
  if (!valid.ok()) return valid;
  if (predecessor.has_value() && predecessor->value > valid_time.value) {
    return Status::InvalidArgument("strict read dependency",
                                   "predecessor is after read time");
  }
  if (successor.has_value() && successor->value <= valid_time.value) {
    return Status::InvalidArgument("strict read dependency",
                                   "successor is not after read time");
  }
  if (observed_event.has_value()) {
    const Status observed_valid = observed_event->Validate();
    if (!observed_valid.ok()) return observed_valid;
    if (observed_event->ref != ref ||
        observed_event->valid_from.value > valid_time.value ||
        observed_event->commit_seq.value > snapshot_seq.value ||
        (predecessor.has_value() && predecessor->value >= valid_time.value)) {
      return Status::InvalidArgument("strict read dependency",
                                     "observed event disagrees with read identity");
    }
  } else if (predecessor.has_value()) {
    return Status::InvalidArgument("strict read dependency",
                                   "empty read has a predecessor fence");
  }
  return Status::OK();
}

Status StoreCommitBatch::Validate() const {
  if (!txn_id.valid() || system_hlc == 0 || mutations.empty()) {
    return Status::InvalidArgument("commit batch", "missing transaction, time, or facts");
  }
  // Batches can carry thousands of mutations; hash membership keeps this
  // validation linear instead of a red-black-tree insert per fact. The sets are
  // sized up front because this runs on the append and preflight hot paths.
  std::unordered_set<std::string> facts;
  facts.reserve(mutations.size());
  for (const PendingFactMutation& mutation : mutations) {
    const Status valid = mutation.Validate();
    if (!valid.ok()) return valid;
    const std::string key = EncodeFactKey(mutation.ref, mutation.valid_from,
                                          CommitSeq{1});
    if (key.empty() || !facts.emplace(key).second) {
      return Status::InvalidArgument("commit batch", "duplicate fact mutation");
    }
  }
  std::unordered_set<std::string> dependencies;
  dependencies.reserve(snapshot_write_dependencies.size());
  for (const SnapshotWriteDependency& dependency : snapshot_write_dependencies) {
    const Status dependency_valid = dependency.Validate();
    if (!dependency_valid.ok()) return dependency_valid;
    const std::string key = EncodeFactKey(dependency.ref, dependency.valid_from,
                                          CommitSeq{1});
    if (!facts.contains(key) || !dependencies.emplace(key).second) {
      return Status::InvalidArgument("commit batch",
                                     "invalid snapshot write dependency");
    }
  }
  std::unordered_set<std::string> strict_reads;
  strict_reads.reserve(strict_read_dependencies.size());
  for (const StrictReadDependency& dependency : strict_read_dependencies) {
    const Status dependency_valid = dependency.Validate();
    if (!dependency_valid.ok()) return dependency_valid;
    std::string key = EncodeFactKey(dependency.ref, ValidTime{0}, CommitSeq{1});
    if (key.empty()) {
      return Status::InvalidArgument("commit batch",
                                     "duplicate strict read dependency");
    }
    for (int shift = 56; shift >= 0; shift -= 8) {
      key.push_back(static_cast<char>(dependency.valid_time.value >> shift));
    }
    if (!strict_reads.emplace(std::move(key)).second) {
      return Status::InvalidArgument("commit batch",
                                     "duplicate strict read dependency");
    }
  }
  std::unordered_set<uint64_t> edge_ids;
  edge_ids.reserve(edge_identities.size());
  for (const EdgeIdentity& identity : edge_identities) {
    const Status valid = identity.Validate();
    if (!valid.ok()) return valid;
    if (!edge_ids.emplace(identity.edge_id.value).second) {
      return Status::InvalidArgument("commit batch", "duplicate edge identity");
    }
  }
  if (!edge_ids.empty()) {
    std::unordered_set<uint64_t> edge_state_assertions;
    edge_state_assertions.reserve(edge_ids.size());
    for (const PendingFactMutation& mutation : mutations) {
      if (mutation.ref.family() == FactFamily::kEdgeState &&
          mutation.operation == FactOperation::kPut) {
        edge_state_assertions.insert(mutation.ref.entity_id());
      }
    }
    for (uint64_t edge_id : edge_ids) {
      if (!edge_state_assertions.contains(edge_id)) {
        return Status::InvalidArgument("commit batch",
                                       "edge identity lacks state assertion");
      }
    }
  }
  return Status::OK();
}

FactPrefix FactPrefix::Family(PartId part_id, FactFamily family,
                              PropertyId property_id) {
  return FactPrefix(part_id, family, property_id, std::nullopt);
}

Status FactPrefix::Validate() const {
  const FactRef representative(part_id_, family_, property_id_,
                               entity_id_.value_or(1));
  if (!representative.Validate().ok()) {
    return Status::InvalidArgument("fact prefix", "invalid family or property ID");
  }
  if (entity_id_.has_value() && *entity_id_ == 0) {
    return Status::InvalidArgument("fact prefix", "zero entity ID");
  }
  return Status::OK();
}

FactStore::FactStore(FactStoreOptions options) : options_(std::move(options)) {}
FactStore::~FactStore() {
  // Destruction is the final close and cannot report SnapshotPinned to a caller.
  // Try the normal close first (unchanged semantics); if a StoreSnapshot still
  // pins an epoch, force the teardown instead of letting the epoch's strong store
  // back-reference keep the RocksDB handle, LOCK/WAL files and heavy state alive
  // after the owner is destroyed.
  std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
  if (!close_prepared_) {
    const Status prepared = PrepareCloseLocked();
    if (!prepared.ok()) {
      ForceCloseLocked();
      return;
    }
  }
  FinishCloseLocked().IgnoreError();
}

Status FactStore::Open() {
  std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
  if (impl_) return Status::InvalidArgument("fact store", "store is already open");
  if (options_.path.empty()) {
    return Status::InvalidArgument("fact store", "missing database path");
  }
  if (options_.meta_sequence_format != kMetaSequenceFormatV1 &&
      options_.meta_sequence_format != kMetaSequenceFormatV2) {
    return Status::InvalidArgument(
        "fact store", "meta sequence format must be 1 (v1) or 2 (v2a)");
  }
  if (options_.group_commit_max_batch_size == 0 ||
      options_.group_commit_max_batch_size > kMaximumGroupCommitBatchCount) {
    return Status::InvalidArgument("fact store",
                                   "group commit count is outside [1, 512]");
  }
  if (options_.group_commit_max_batch_bytes == 0 ||
      options_.group_commit_max_batch_bytes > kMaximumGroupCommitBatchBytes) {
    return Status::InvalidArgument("fact store",
                                   "group commit bytes exceed 2 MiB hard limit");
  }
  std::error_code filesystem_error;
  std::filesystem::create_directories(options_.path, filesystem_error);
  if (filesystem_error) {
    return Status::IOError("fact store", filesystem_error.message());
  }
  const Status wal_placement =
      internal::ValidateProductionWalPlacement(options_);
  if (!wal_placement.ok()) return wal_placement;
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

  const auto resolved_profile = internal::ResolveStorageProfile(options_);
  if (!resolved_profile.ok()) return resolved_profile.status();
  const internal::ResolvedStorageProfile* resolved_ptr =
      UsesCedarKernelProfile(options_.storage_profile)
          ? &resolved_profile.ValueOrDie()
          : nullptr;
  std::shared_ptr<rocksdb::Cache> shared_cache;
  rocksdb::Options options = internal::MakeRocksDbOptions(
      options_, is_new_database, resolved_ptr, &shared_cache);
  auto page_metrics = std::make_shared<rocksdb::cedar_parquet::CanonicalPageCacheMetrics>();

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
      internal::MakeRocksDbColumnFamilyDescriptors(options_, options, resolved_ptr,
          {shared_cache, page_metrics});
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
  if (resolved_ptr != nullptr) {
    store->maintenance_flush_worker_capacity =
        resolved_ptr->flush_worker_capacity;
    store->maintenance_compaction_worker_capacity =
        resolved_ptr->compaction_worker_capacity;
    store->maintenance_normal_flush_credits =
        resolved_ptr->normal_flush_credits;
    store->maintenance_emergency_flush_credits =
        resolved_ptr->emergency_flush_credits;
    store->maintenance_compaction_credits = resolved_ptr->compaction_credits;
  }
  store->canonical_page_metrics = std::move(page_metrics);
  store->db = std::move(db);
  store->default_cf = handles[0];
  store->facts_cf = handles[1];
  store->meta_cf = handles[2];
  store->statistics = options.statistics;
  store->validation_scan_observer_for_testing =
      options_.validation_scan_observer_for_testing;
  store->sequence_validation_read_source_observer_for_testing =
      options_.sequence_validation_read_source_observer_for_testing;
  store->sequence_validation_outcome_batch_size =
      options_.sequence_validation_outcome_batch_size_for_testing == 0
          ? 1024
          : options_.sequence_validation_outcome_batch_size_for_testing;
  store->deep_recovery_validation = options_.deep_recovery_validation;
  store->recovery_validation_recent_sequences =
      std::max<uint64_t>(1, options_.recovery_validation_recent_sequences);
  store->reopen_validation_counters_for_testing =
      options_.reopen_validation_counters_for_testing;
  store->snapshot_epoch_created_observer_for_testing =
      options_.snapshot_epoch_created_observer_for_testing;
  store->snapshot_epoch_destroyed_observer_for_testing =
      options_.snapshot_epoch_destroyed_observer_for_testing;
  store->publish_visible_before_retire_observer_for_testing =
      options_.publish_visible_before_retire_observer_for_testing;
  store->force_detach_epochs_observer_for_testing =
      options_.force_detach_epochs_observer_for_testing;
  store->epoch_release_before_reclaim_observer_for_testing =
      options_.epoch_release_before_reclaim_observer_for_testing;

  std::string encoded_format;
  const rocksdb::Status got_format = store->db->Get(
      rocksdb::ReadOptions(), store->meta_cf, EncodeCurrentFormatKey(),
      &encoded_format);
  if (got_format.IsNotFound()) {
    if (!is_new_database) {
      return Status::Corruption("fact store", "missing durable format record");
    }
    const auto identity = EncodeSystemIdentity(SystemIdentity{
        "cedar.authoritative-columnar", 1, "part32.fact.v2",
        "cedar.parquet.facts.v3", "cedar.v2.internal-key.bytewise.v1"});
    const auto empty_watermark = EncodeWatermark(CommitSeq{});
    if (!identity.ok()) return identity.status();
    if (!empty_watermark.ok()) return empty_watermark.status();
    rocksdb::WriteBatch batch;
    batch.Put(store->meta_cf, EncodeCurrentFormatKey(), identity.ValueOrDie());
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
    store->sequence_retention_watermark.store(0, std::memory_order_relaxed);
  } else {
    if (!got_format.ok()) return FromRocksDb(got_format, "read fact store format");
    const auto identity = DecodeSystemIdentity(encoded_format);
    if (!identity.ok()) return identity.status();
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
    // Optional durable sequence-retention boundary. Databases that never pruned
    // sequence history do not have the record at all; 0 keeps the historical
    // contract that every record from 1 through the visible watermark exists.
    std::string encoded_sequence_retention;
    const rocksdb::Status got_sequence_retention = store->db->Get(
        rocksdb::ReadOptions(), store->meta_cf,
        EncodeSequenceRetentionWatermarkKey(), &encoded_sequence_retention);
    CommitSeq sequence_retention{};
    if (!got_sequence_retention.IsNotFound()) {
      if (!got_sequence_retention.ok()) {
        return FromRocksDb(got_sequence_retention,
                           "read sequence retention watermark");
      }
      const auto decoded_retention = DecodeWatermark(encoded_sequence_retention);
      if (!decoded_retention.ok()) return decoded_retention.status();
      sequence_retention = decoded_retention.ValueOrDie();
    }
    if (sequence_retention.value > store->oldest_readable_seq.value) {
      return Status::Corruption(
          "fact store",
          "sequence retention watermark exceeds the retention boundary");
    }
    store->sequence_retention_watermark.store(sequence_retention.value,
                                              std::memory_order_relaxed);
  }
  // Resolve the effective meta sequence writer format before reopen validation
  // and before the store can accept any commit. The durable marker is
  // authoritative on reopen: once it records 2, this and every later open write
  // v2 even if the caller asks for v1. Missing marker + v1 option keeps today's
  // byte-identical v1 behavior; missing marker + v2 option writes the marker
  // durably below, before any v2 record can exist.
  const auto marker = LoadMetaSequenceFormatMarker(store.get());
  if (!marker.ok()) return marker.status();
  const std::optional<uint32_t> marker_format = marker.ValueOrDie();
  const bool marker_is_v2 =
      marker_format.has_value() && *marker_format == kMetaSequenceFormatV2;
  store->meta_sequence_format =
      marker_is_v2 ? kMetaSequenceFormatV2 : options_.meta_sequence_format;
  if (!is_new_database) {
    // Marker loaded, then validation: the sequence scanner is dual-read, but the
    // open contract is that the durable format is known first. Validation starts
    // at the durable sequence-retention watermark, so pruned history below it is
    // accepted while any gap at or above it still fails the open.
    const Status sequences_valid = ValidateCommittedSequences(
        store.get(), store->visible_seq, store->oldest_readable_seq,
        CommitSeq{store->sequence_retention_watermark.load(
            std::memory_order_relaxed)});
    if (!sequences_valid.ok()) return sequences_valid;
  }
  if (store->meta_sequence_format == kMetaSequenceFormatV2 && !marker_is_v2) {
    // Opt-in upgrade (or a marker that still records v1): durably record 2
    // before accepting commits. Sync=true makes a crash-safe ordering guarantee:
    // no v2 record can be durable without the marker that claims v2.
    const Status marked =
        WriteMetaSequenceFormatMarker(store.get(), options_,
                                      kMetaSequenceFormatV2);
    if (!marked.ok()) return marked;
  }
  const Status vertex_allocator =
      LoadAllocatorState(store.get(), IdKind::kVertex, &store->vertex_allocator);
  if (!vertex_allocator.ok()) return vertex_allocator;
  const Status edge_allocator =
      LoadAllocatorState(store.get(), IdKind::kEdge, &store->edge_allocator);
  if (!edge_allocator.ok()) return edge_allocator;
  const Status transaction_allocator = LoadTransactionAllocatorState(
      store.get(), &store->transaction_allocator);
  if (!transaction_allocator.ok()) return transaction_allocator;
  store->next_transaction_id.store(store->transaction_allocator.next_id,
                                   std::memory_order_relaxed);
  store->transaction_lease_limit.store(store->transaction_allocator.next_id,
                                       std::memory_order_relaxed);
  store->published_visible_seq.store(store->visible_seq.value,
                                     std::memory_order_relaxed);
  store->published_oldest_readable_seq.store(store->oldest_readable_seq.value,
                                              std::memory_order_relaxed);
  const Status schemas = LoadPropertySchemas(store.get());
  if (!schemas.ok()) return schemas;
  std::string encoded_vacuum_state;
  const rocksdb::Status got_vacuum_state = store->db->Get(
      rocksdb::ReadOptions(), store->meta_cf, EncodeVacuumStateKey(),
      &encoded_vacuum_state);
  if (!got_vacuum_state.IsNotFound()) {
    if (!got_vacuum_state.ok()) {
      return FromRocksDb(got_vacuum_state, "read vacuum state");
    }
    const auto vacuum_state = DecodeVacuumState(encoded_vacuum_state);
    if (!vacuum_state.ok()) return vacuum_state.status();
    if (vacuum_state.ValueOrDie().target != store->oldest_readable_seq) {
      return Status::Corruption("vacuum", "state target disagrees with watermark");
    }
    const Status resumed =
        ResumeVacuum(store.get(), options_, vacuum_state.ValueOrDie());
    if (!resumed.ok()) return resumed;
  }
  // Opt-in retention resume: a crash after the durable watermark write (but
  // before/inside the bounded deletes) leaves records below the watermark that
  // this open removes. The operation is idempotent, so a clean database pays one
  // bounded seek and no writes.
  if (options_.prune_sequence_records_on_vacuum) {
    const Status pruned = PruneSequenceRecords(
        store.get(), options_, store->oldest_readable_seq);
    if (!pruned.ok()) return pruned;
  }
  // Split the fixed validation budget: recent-write Bloom windows, the
  // ever-written identity Bloom, and the version validation cache.
  const uint64_t write_index_bytes = options_.validation_cache_bytes / 8;
  const uint64_t identity_history_bytes =
      options_.validation_cache_bytes * 3 / 8;
  const uint64_t validation_cache_bytes =
      options_.validation_cache_bytes - write_index_bytes -
      identity_history_bytes;
  store->version_validation_cache =
      std::make_unique<internal::VersionValidationCache>(
          validation_cache_bytes);
  store->recent_fact_write_index =
      std::make_unique<internal::RecentFactWriteIndex>(
          write_index_bytes, store->visible_seq);
  store->identity_history =
      std::make_unique<internal::FactIdentityHistory>(
          identity_history_bytes);
  // The identity-history Bloom is a write/read-path optimization: "has this
  // identity ever been written" lets the write path skip canonical neighborhood
  // reads for brand-new identities, and lets point reads skip the chain scan for
  // them. Rebuilding it at open costs a full facts-CF scan (792M rows on the
  // production dataset), so skip the rebuild when the facts CF holds more
  // identities than the fixed filter budget can usefully represent.
  //
  // Capacity check, deliberately conservative:
  //  * The capacity math is exact and shared with the constructor
  //    (FactIdentityHistory::WordCountForBytes), so CanRepresent() means "the
  //    filter keeps at least 8 bits per identity", the point where 3 probes
  //    still give a low (~3%) false-positive rate. Below that a rebuild spends
  //    the scan only to answer "maybe" for nearly every probe.
  //  * rocksdb.estimate-num-keys counts *keys*, and one identity has one key per
  //    version, so it can only over-count distinct identities. An over-count can
  //    only trigger an unnecessary skip, never a missed one, so a database whose
  //    chain history floods the filter cannot evade the check.
  //  * If the property cannot be read, the estimate is unreliable: keep today's
  //    rebuild. Rebuilding is always safe; skipping is only safe because the
  //    skip leaves the filter in its always-true state (next paragraph).
  //
  // The skip must leave the filter *always true* (word_count_ == 0, so
  // MightContain returns true and Publish is a no-op). Leaving a zeroed/empty
  // filter would break the no-false-negative contract: the write path would
  // skip the canonical read that detects prior versions of an existing identity,
  // and point reads of that identity would return no facts at all.
  const auto identity_phase_started = std::chrono::steady_clock::now();
  uint64_t identity_records = 0;
  bool skip_identity_history_rebuild =
      options_.identity_history_skip_rebuild_for_testing;
  if (!skip_identity_history_rebuild) {
    uint64_t estimated_fact_keys = 0;
    if (store->db->GetIntProperty(store->facts_cf,
                                  rocksdb::DB::Properties::kEstimateNumKeys,
                                  &estimated_fact_keys)) {
      skip_identity_history_rebuild =
          !internal::FactIdentityHistory::CanRepresent(identity_history_bytes,
                                                       estimated_fact_keys);
    }
  }
  if (skip_identity_history_rebuild) {
    store->identity_history->Disable();
  } else {
    rocksdb::ReadOptions options;
    std::unique_ptr<rocksdb::Iterator> iterator(
        store->db->NewIterator(options, store->facts_cf));
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
      const auto decoded = DecodeFactKey(iterator->key().ToString());
      if (!decoded.ok()) return decoded.status();
      store->identity_history->Publish(decoded.ValueOrDie().ref);
      ++identity_records;
    }
    if (!iterator->status().ok()) {
      return FromRocksDb(iterator->status(), "scan fact identities");
    }
  }
  // S3: a skipped rebuild reports records == 0; the elapsed time still covers
  // the capacity decision, so the two outcomes stay comparable.
  ReportReopenValidationPhase(store.get(), "P2_identity_history",
                              identity_records, 0, 0,
                              identity_phase_started);
  impl_ = std::move(store);
  fast_impl_.store(impl_.get(), std::memory_order_seq_cst);
  return Status::OK();
}

Status FactStore::Close() {
  std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
  const Status prepared = PrepareCloseLocked();
  if (!prepared.ok()) return prepared;
  return FinishCloseLocked();
}

Status FactStore::PrepareClose() {
  std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
  return PrepareCloseLocked();
}

Status FactStore::FinishClose() {
  std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
  return FinishCloseLocked();
}

Status FactStore::PrepareCloseLocked() {
  if (!impl_) return Status::OK();
  if (close_prepared_) return Status::OK();
  if (options_.prepare_close_locked_observer_for_testing) {
    // Test-only window: lifecycle_mutex_ is held (the caller locked it), exactly
    // the handshake that must not serialize lock-free snapshot lookups.
    options_.prepare_close_locked_observer_for_testing();
  }
  auto store = impl_;
  store->accepting_snapshots.store(false, std::memory_order_release);
  store->accepting_fast.store(false, std::memory_order_seq_cst);
  // Unpublish the raw store pointer before draining. A lock-free pin registers
  // its attempt (in fast_pin_attempts_) *before* it loads this pointer, so once
  // every attempt has drained no thread can still hold a raw pointer into this
  // store or into an older closed shell. This is what allows the retention below
  // to be bounded to one inert shell.
  fast_impl_.store(nullptr, std::memory_order_seq_cst);
  // Wait only for opens that already registered themselves as in flight (see
  // BeginSnapshot, which increments under lifecycle_mutex_): their remaining
  // work is one RocksDB GetSnapshot plus registry insertion under epoch_mutex,
  // never a user-visible read. Reader-held epochs are accounted below as
  // references, so a close pinned by a reader is reported instead of waited
  // for.
  while (store->snapshot_opens_in_flight.load(std::memory_order_acquire) != 0) {
    std::this_thread::yield();
  }
  // Drain lock-free pin attempts that loaded the store before the unpublish
  // above. Such an attempt either finishes here (and is then accounted as a live
  // reference below) or falls back to the slow path this close serializes with.
  while (FastPinAttemptsTotal() != 0) {
    std::this_thread::yield();
  }
  // Belt-and-braces: a lock-free pin registers in pin_attempts inside the gate
  // attempt above, so the gate drain already implies zero in-flight pin
  // attempts. Drain the epoch-side counters explicitly so the close contract
  // does not depend on that implication.
  while (store->PinAttemptsTotal() != 0) {
    std::this_thread::yield();
  }
  // Drain storage-level non-epoch users: checkpoint, runtime sampling,
  // validation-cache metrics, and native maintenance hold a shared_ptr to the
  // store but no epoch reference, so nothing above accounts for them. They
  // register under lifecycle_mutex_ (which this close holds for the whole
  // handshake), therefore no new user can register once the count is zero here;
  // a user that arrives later blocks on lifecycle_mutex_ and then observes the
  // prepared/closed state. Because the epoch side is already quiescent, these
  // users are the last ones that can touch the heavy state torn down below.
  while (non_epoch_users_.load(std::memory_order_acquire) != 0) {
    std::this_thread::yield();
  }
  bool pinned = false;
  {
    // Serialize with a publisher so no visible-sequence advance retires or
    // reclaims an epoch between the reference test and the teardown.
    std::lock_guard<std::mutex> publisher_lock(store->publisher_mutex);
    // Evict the rebuildable as_of epoch cache before the reference test. Eviction
    // is one-way for this attempt: a pinned (failed) close does not repopulate
    // it, and later as_of reads rebuild entries lazily. Cache entries hold only
    // rebuildable epoch references and never pin the retention boundary, so this
    // cannot affect correctness; the rollback below restores the publication
    // gates, not the cache.
    store->ClearAsOfCache();
    {
      std::lock_guard<std::mutex> epoch_lock(store->epoch_mutex);
      pinned = store->HasActiveEpochRefsLocked();
    }
  }
  if (pinned) {
    // Semantics unchanged: a failed close leaves the store open and usable.
    // Nothing heavy has been released and no live reader reference is dropped
    // (a reader inside a cached as_of read holds its own reference); the
    // publication gates are the only state this attempt changed, so restoring
    // them restores usability. The as_of cache stays empty (see above): it is
    // rebuildable and its eviction has no correctness effect.
    fast_impl_.store(store.get(), std::memory_order_seq_cst);
    store->accepting_fast.store(true, std::memory_order_seq_cst);
    store->accepting_snapshots.store(true, std::memory_order_release);
    return Status::SnapshotPinned("fact store", "active snapshots prevent close");
  }
  // Irreversible decision: no epoch reference exists and none can be created
  // because accepting_snapshots is false and fast_impl_ is null. Keep the
  // handshake quiescent and hand the store to FinishClose, which is where the
  // heavy state is actually released.
  close_prepared_ = true;
  closing_impl_ = std::move(store);
  return Status::OK();
}

Status FactStore::FinishCloseLocked() {
  if (!close_prepared_) {
    // Idempotent close of an already closed store (and the tail of a
    // single-call Close that found no open store).
    return impl_ ? Status::InvalidArgument("fact store", "close was not prepared")
                 : Status::OK();
  }
  auto store = std::move(closing_impl_);
  close_prepared_ = false;
  // The store is already committed to closing, so the second quiescence pass
  // cannot spin on new arrivals: snapshot opens and fast pins are rejected
  // before they can create an epoch reference, and new non-epoch users block on
  // lifecycle_mutex_ until the shell is gone. The caller stops its producers
  // (sampler, maintenance, commit workers) between PrepareClose and this call.
  while (store->snapshot_opens_in_flight.load(std::memory_order_acquire) != 0) {
    std::this_thread::yield();
  }
  while (FastPinAttemptsTotal() != 0) {
    std::this_thread::yield();
  }
  while (store->PinAttemptsTotal() != 0) {
    std::this_thread::yield();
  }
  while (non_epoch_users_.load(std::memory_order_acquire) != 0) {
    std::this_thread::yield();
  }
  {
    std::lock_guard<std::mutex> publisher_lock(store->publisher_mutex);
    {
      std::lock_guard<std::mutex> epoch_lock(store->epoch_mutex);
      // Release the current/retired epoch leases before the store is closed.
      // A successful close implies zero live references; teardown does not fire
      // the destroyed observer, as documented in fact_store.h.
      store->CloseEpochsLocked();
    }
    // Destroy RocksDB resources and release every heavy member, then keep only
    // this inert shell (bounded retention) for a pin that loaded the pointer
    // before the unpublish and is guaranteed to fail.
    store->DestroyDatabaseHandles();
    store->ReleaseRetainedStateForClose();
    FreeEarlierRetainedShells();
    retained_impls_.push_back(std::move(store));
    impl_.reset();
  }
  return Status::OK();
}

void FactStore::ForceCloseLocked() {
  if (!impl_) return;
  // The destructor owns the store: a live StoreSnapshot handle may still pin an
  // epoch (Close reported SnapshotPinned), but it must not keep the RocksDB
  // handle, LOCK/WAL files and heavy state alive after the owner is gone.
  //
  // Contract (see ~FactStore): destruction must be serialized with the users of
  // an outliving handle - no thread may read through it while this runs, because
  // the epoch payload (snapshot lease) and the heavy state are released here.
  // Releasing the handle itself is tolerated (the last-reader reclamation claims
  // payload/memory under epoch_mutex and becomes a no-op); this function drains
  // the in-flight release windows below and ForceDetachEpochsLocked claims every
  // payload and epoch memory so such a release can neither free nor fire
  // anything the owner is tearing down.
  auto store = std::move(impl_);
  store->accepting_snapshots.store(false, std::memory_order_release);
  store->accepting_fast.store(false, std::memory_order_seq_cst);
  fast_impl_.store(nullptr, std::memory_order_seq_cst);
  // No new arrival can register (accepting_snapshots is off and the fast path is
  // unpublished); drain only the already-registered windows. Reader-held epoch
  // references are tolerated below, but a reader may also be *releasing* its
  // handle concurrently: a last-reader release registers a reclaim attempt before
  // it decrements and keeps it until after its last access to this store, so it
  // must be drained before the store's heavy state is released. Unlike a pin
  // attempt it is not gated by the close (a live handle can start one at any
  // time), so this drain is a best-effort window closer: the authoritative
  // argument for a release that starts later is that ForceDetachEpochsLocked
  // claims every payload and epoch memory under epoch_mutex (see its contract),
  // making the late reclaimer a no-op on graveyard-owned memory.
  while (store->snapshot_opens_in_flight.load(std::memory_order_acquire) != 0) {
    std::this_thread::yield();
  }
  while (FastPinAttemptsTotal() != 0) {
    std::this_thread::yield();
  }
  while (store->PinAttemptsTotal() != 0) {
    std::this_thread::yield();
  }
  while (store->ReclaimAttemptsTotal() != 0) {
    std::this_thread::yield();
  }
  while (non_epoch_users_.load(std::memory_order_acquire) != 0) {
    std::this_thread::yield();
  }
  {
    std::lock_guard<std::mutex> publisher_lock(store->publisher_mutex);
    {
      std::lock_guard<std::mutex> epoch_lock(store->epoch_mutex);
      // Release and unlink every epoch payload before the RocksDB handle is
      // destroyed; the epoch memory stays valid for an outliving handle's later
      // ReleaseEpochRef. Must run while db is still alive (SnapshotLease
      // destructor releases through it).
      store->ForceDetachEpochsLocked();
    }
    store->DestroyDatabaseHandles();
    store->ReleaseRetainedStateForClose();
  }
  // Do not retain the shell here: a live StoreSnapshot already owns a strong
  // reference to the store (which keeps the graveyard alive until the last
  // handle is released); otherwise dropping the local reference destroys the
  // store and its remaining epoch memory now.
}

void FactStore::FreeEarlierRetainedShells() {
  for (const std::shared_ptr<FactStoreImpl>& shell : retained_impls_) {
    // A last-reader release may still be inside ReleaseEpochRef: it registered
    // its reclaim attempt before decrementing and keeps it until after its last
    // access to the shell. No new attempt can start for a closed shell
    // (fast_impl_ no longer points at it and no snapshot handle can reference
    // it), so draining the counters proves the shell is quiescent and can be
    // destroyed.
    while (shell->ReclaimAttemptsTotal() != 0) std::this_thread::yield();
  }
  retained_impls_.clear();
}

size_t FactStore::RetainedShellCountForTesting() const {
  std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
  return retained_impls_.size();
}

size_t FactStore::SnapshotOpensInFlightForTesting() const {
  std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
  return impl_ == nullptr
             ? 0U
             : impl_->snapshot_opens_in_flight.load(std::memory_order_acquire);
}

bool FactStore::RetainedShellsAreInertForTesting() const {
  std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
  for (const std::shared_ptr<FactStoreImpl>& shell : retained_impls_) {
    if (!shell->RetainedShellIsInertForTesting()) return false;
  }
  return true;
}

Status FactStore::CreateCheckpoint(const std::string& checkpoint_path) const {
  if (checkpoint_path.empty() ||
      !std::filesystem::path(checkpoint_path).is_absolute()) {
    return Status::InvalidArgument("fact store checkpoint",
                                   "checkpoint path must be absolute");
  }
  std::shared_ptr<FactStoreImpl> store;
  NonEpochUseGuard non_epoch_use;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) {
      return Status::InvalidArgument("fact store checkpoint", "store is not open");
    }
    non_epoch_use = RegisterNonEpochUseLocked();
  }
  RunNonEpochOpObserverForTesting();
  std::error_code filesystem_error;
  if (std::filesystem::exists(checkpoint_path, filesystem_error)) {
    return Status::InvalidArgument("fact store checkpoint",
                                   "destination already exists");
  }
  if (filesystem_error) {
    return Status::IOError("fact store checkpoint", filesystem_error.message());
  }
  rocksdb::Checkpoint* raw_checkpoint = nullptr;
  const rocksdb::Status created =
      rocksdb::Checkpoint::Create(store->db.get(), &raw_checkpoint);
  if (!created.ok()) return FromRocksDb(created, "create fact store checkpoint");
  std::unique_ptr<rocksdb::Checkpoint> checkpoint(raw_checkpoint);
  const rocksdb::Status checkpointed = checkpoint->CreateCheckpoint(
      checkpoint_path, std::numeric_limits<uint64_t>::max());
  return FromRocksDb(checkpointed, "create fact store checkpoint");
}

namespace {

// RAII registration for FactStoreImpl::snapshot_opens_in_flight. Every exit
// path of an as_of open - including a throwing test observer, a bad_alloc while
// building a Status, or any future early return - must drop the registration,
// otherwise a later Close would spin forever in its open drain while holding
// lifecycle_mutex_. The caller keeps the store alive with a shared_ptr for the
// whole open, so the guard only touches the atomic counter (constructed after
// the registration under lifecycle_mutex_; the manual decrement on the
// accepting_snapshots rejection path happens before the guard exists).
class SnapshotOpenGuard {
 public:
  explicit SnapshotOpenGuard(FactStoreImpl* store) : store_(store) {}
  ~SnapshotOpenGuard() {
    if (store_ != nullptr) {
      store_->snapshot_opens_in_flight.fetch_sub(1, std::memory_order_acq_rel);
    }
  }
  SnapshotOpenGuard(const SnapshotOpenGuard&) = delete;
  SnapshotOpenGuard& operator=(const SnapshotOpenGuard&) = delete;

 private:
  FactStoreImpl* store_;
};

}  // namespace

StatusOr<StoreSnapshot> FactStore::BeginSnapshot(SnapshotOptions options) const {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("fact store", "store is not open");
    // Register the open while still holding the lifecycle lock that Close
    // holds for its whole handshake. A concurrent close therefore either sees
    // this open in flight (and waits for its bounded epoch creation) or flips
    // accepting_snapshots before this open registers; there is no window in
    // which a close can miss an epoch that is about to be pinned.
    store->snapshot_opens_in_flight.fetch_add(1, std::memory_order_acq_rel);
    if (!store->accepting_snapshots.load(std::memory_order_acquire)) {
      store->snapshot_opens_in_flight.fetch_sub(1, std::memory_order_acq_rel);
      return Status::ShutdownInProgress("snapshot", "fact store is closing");
    }
  }
  // The registration taken above is released by scope, including on an
  // exception thrown by a test observer below (see SnapshotOpenGuard). Construct
  // the guard before the observer so no user/test callback can leak it.
  const SnapshotOpenGuard open_registration(store.get());
  if (options_.snapshot_open_observer_for_testing) {
    options_.snapshot_open_observer_for_testing();
  }
  // Cheap preview: reject clearly invalid requests before taking epoch_mutex.
  // It is not the correctness check; the authoritative boundary re-check and the
  // epoch registration happen in one epoch_mutex critical section below.
  const CommitSeq visible_preview{store->published_visible_seq.load(
      std::memory_order_acquire)};
  const CommitSeq oldest_preview{store->published_oldest_readable_seq.load(
      std::memory_order_acquire)};
  const CommitSeq requested = options.as_of.value_or(visible_preview);
  if (requested.value < oldest_preview.value) {
    return Status::SnapshotExpired("snapshot", "sequence is below retention boundary");
  }
  if (requested.value > visible_preview.value) {
    return Status::InvalidArgument("snapshot", "sequence exceeds visible watermark");
  }
  // Test-only window: the retention watermarks have been loaded and the cheap
  // checks passed, but no epoch has been registered yet.
  if (options_.snapshot_boundary_loaded_observer_for_testing) {
    options_.snapshot_boundary_loaded_observer_for_testing();
  }
  // Open/vacuum handshake: the retention-boundary re-check and the epoch
  // registration are one epoch_mutex critical section. Vacuum performs its
  // active-epoch scan and publishes its new boundary (the pending target) under
  // the same mutex, so a concurrent Vacuum either observes this epoch in its
  // scan (and refuses to clean at or below the epoch's sequence) or this open
  // observes the pending/advanced boundary and reports SnapshotExpired. Without
  // the shared critical section a reader could validate against the old
  // boundary and register its epoch after the scan, then silently read versions
  // the Vacuum had already deleted.
  const bool current_request = !options.as_of.has_value();
  SnapshotEpoch* epoch = nullptr;
  {
    std::lock_guard<std::mutex> epoch_lock(store->epoch_mutex);
    const CommitSeq visible{store->published_visible_seq.load(
        std::memory_order_acquire)};
    const CommitSeq sequence = current_request ? visible : requested;
    const CommitSeq boundary{store->EffectiveOldestReadableLocked()};
    if (sequence.value < boundary.value) {
      return Status::SnapshotExpired("snapshot",
                                     "sequence is below retention boundary");
    }
    if (sequence.value > visible.value) {
      return Status::InvalidArgument("snapshot",
                                     "sequence exceeds visible watermark");
    }
    if (current_request) {
      // Every reader at the current visible sequence shares one epoch (and one
      // RocksDB snapshot). A cache hit only pins a reference; the created
      // observer fires once when the epoch is first constructed.
      epoch = store->PinCurrentEpochLocked(store);
    } else {
      // Historical as_of reads use a one-off, uncached epoch: they must never
      // share the current epoch's lease. Validation used the effective boundary;
      // the epoch reports the published one (see PublishedOldestReadableLocked).
      epoch = store->CreateUncachedEpochLocked(
          store, sequence, store->PublishedOldestReadableLocked());
    }
  }
  if (epoch == nullptr) {
    return Status::IOError("snapshot", "RocksDB refused snapshot");
  }
  // The epoch reference is the only liveness registry read by Vacuum/Close: it
  // was registered under epoch_mutex before this open is reported finished. The
  // scope guard drops the opens-in-flight registration as the handle is
  // constructed, so a close can never observe a registered open without its epoch
  // reference.
  return StoreSnapshot(std::move(store), this, epoch);
}

StatusOr<StoreSnapshot> FactStore::BeginCurrentSnapshot() const {
  // No-new-arrivals gate: a null fast_impl_ means PrepareClose already
  // unpublished the fast path, so registering an attempt here could only feed
  // the Close drain after it decided to tear down. Skip registration and use the
  // registered slow path, which reports the closing state. See the invariant
  // documented at FastPinAttempt in fact_store.h.
  if (fast_impl_.load(std::memory_order_seq_cst) == nullptr) {
    fast_pin_attempts_skipped_.fetch_add(1, std::memory_order_relaxed);
    return BeginSnapshot();
  }
  {
    // The attempt is registered before the raw store pointer is loaded, and it
    // is dropped again before the slow path below acquires lifecycle_mutex_
    // (which Close holds while draining these attempts). See FastPinAttempt.
    FastPinAttempt attempt(*this);
    FactStoreImpl* store = fast_impl_.load(std::memory_order_seq_cst);
    if (store != nullptr) {
      std::shared_ptr<FactStoreImpl> owner;
      SnapshotEpoch* epoch = nullptr;
      if (store->TryPinCurrentEpochLockFree(&owner, &epoch)) {
        return StoreSnapshot(std::move(owner), this, epoch);
      }
    }
  }
  // Cold cases only: no current epoch yet, a retire/close race, or a closed
  // store. The slow path registers the open with the close handshake.
  return BeginSnapshot();
}

StatusOr<StoreSnapshot> FactStore::BeginCurrentSnapshotAt(
    CommitSeq sequence) const {
  // Same no-new-arrivals gate as BeginCurrentSnapshot: once the fast path is
  // unpublished (store closing), do not register a pin attempt; the registered
  // slow path re-validates the watermarks and reports the close state.
  if (fast_impl_.load(std::memory_order_seq_cst) == nullptr) {
    fast_pin_attempts_skipped_.fetch_add(1, std::memory_order_relaxed);
    return BeginSnapshotAtCached(sequence);
  }
  {
    FastPinAttempt attempt(*this);
    FactStoreImpl* store = fast_impl_.load(std::memory_order_seq_cst);
    if (store != nullptr) {
      const CommitSeq visible{store->published_visible_seq.load(
          std::memory_order_acquire)};
      const CommitSeq oldest{store->published_oldest_readable_seq.load(
          std::memory_order_acquire)};
      if (sequence.value < oldest.value) {
        return Status::SnapshotExpired("snapshot",
                                       "sequence is below retention boundary");
      }
      if (sequence.value > visible.value) {
        return Status::InvalidArgument("snapshot",
                                       "sequence exceeds visible watermark");
      }
      if (sequence.value == visible.value) {
        // Test-only window: the watermark has been loaded, the epoch pin has not
        // happened yet. A concurrent publish here must not leak a newer vintage
        // into the read (see the exact-sequence check below).
        if (options_.current_sequence_loaded_observer_for_testing) {
          options_.current_sequence_loaded_observer_for_testing();
        }
        // The latest vintage is the same lock-free current-epoch pin, but only
        // when the pinned epoch really is the requested sequence. A publish that
        // landed between the watermark load above and this pin makes the fast
        // pin return a *newer* epoch (or the slow path create one); using it
        // would serve a future commit inside an as_of read.
        std::shared_ptr<FactStoreImpl> owner;
        SnapshotEpoch* epoch = nullptr;
        if (store->TryPinCurrentEpochLockFree(&owner, &epoch)) {
          if (epoch->seq.value == sequence.value) {
            return StoreSnapshot(std::move(owner), this, epoch);
          }
          // The pinned epoch is newer than the requested sequence (a publish
          // landed between the watermark load and the pin). Drop it and use the
          // exact-sequence slow path below; never hand it to the caller.
          store->ReleaseEpochRef(epoch);
          owner.reset();
        }
        // Fall through to the exact-sequence slow path, which re-validates the
        // watermarks and pins (or creates) an epoch at exactly `sequence`.
      } else {
        // Historical vintage: use the bounded per-sequence epoch cache. The
        // cache mutex keeps the entry's owner reference alive while it is
        // pinned; every entry was created at exactly its recorded sequence.
        std::lock_guard<std::mutex> cache_lock(store->as_of_cache_mutex);
        for (const FactStoreImpl::AsOfCacheEntry& entry : store->as_of_cache) {
          if (entry.epoch == nullptr || entry.seq.value != sequence.value) {
            continue;
          }
          assert(entry.epoch->seq.value == sequence.value);
          entry.epoch->refs.fetch_add(1, std::memory_order_relaxed);
          std::shared_ptr<FactStoreImpl> owner = entry.epoch->store_owner;
          return StoreSnapshot(std::move(owner), this, entry.epoch);
        }
      }
    }
  }
  return BeginSnapshotAtCached(sequence);
}

StatusOr<StoreSnapshot> FactStore::BeginSnapshotAtCached(
    CommitSeq sequence) const {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("fact store", "store is not open");
    store->snapshot_opens_in_flight.fetch_add(1, std::memory_order_acq_rel);
    if (!store->accepting_snapshots.load(std::memory_order_acquire)) {
      store->snapshot_opens_in_flight.fetch_sub(1, std::memory_order_acq_rel);
      return Status::ShutdownInProgress("snapshot", "fact store is closing");
    }
  }
  // The registration taken above is released by scope, including on an
  // exception thrown by a test observer below (see SnapshotOpenGuard). Construct
  // the guard before the observer so no user/test callback can leak it.
  const SnapshotOpenGuard open_registration(store.get());
  if (options_.snapshot_open_observer_for_testing) {
    options_.snapshot_open_observer_for_testing();
  }
  // Cheap preview, then the authoritative boundary re-check and epoch
  // registration under epoch_mutex (see BeginSnapshot for the handshake
  // rationale). The check may not pass here and register later: that window is
  // exactly where a Vacuum could advance the boundary and clean the sequence.
  const CommitSeq visible_preview{store->published_visible_seq.load(
      std::memory_order_acquire)};
  const CommitSeq oldest_preview{store->published_oldest_readable_seq.load(
      std::memory_order_acquire)};
  if (sequence.value < oldest_preview.value) {
    return Status::SnapshotExpired("snapshot",
                                   "sequence is below retention boundary");
  }
  if (sequence.value > visible_preview.value) {
    return Status::InvalidArgument("snapshot",
                                   "sequence exceeds visible watermark");
  }
  // Test-only window: same as BeginSnapshot (boundary read done, epoch not yet
  // registered).
  if (options_.snapshot_boundary_loaded_observer_for_testing) {
    options_.snapshot_boundary_loaded_observer_for_testing();
  }
  SnapshotEpoch* epoch = nullptr;
  bool shared_current = false;
  {
    std::lock_guard<std::mutex> epoch_lock(store->epoch_mutex);
    const CommitSeq visible{store->published_visible_seq.load(
        std::memory_order_acquire)};
    const CommitSeq boundary{store->EffectiveOldestReadableLocked()};
    if (sequence.value < boundary.value) {
      return Status::SnapshotExpired("snapshot",
                                     "sequence is below retention boundary");
    }
    if (sequence.value > visible.value) {
      return Status::InvalidArgument("snapshot",
                                     "sequence exceeds visible watermark");
    }
    if (sequence.value == visible.value) {
      // The current epoch is created/reused from this same visible watermark
      // under this same lock, so it is pinned at exactly `sequence`.
      epoch = store->PinCurrentEpochLocked(store);
      shared_current = true;
    } else {
      // Validation used the effective boundary; the epoch reports the durable
      // published one (see PublishedOldestReadableLocked).
      epoch = store->CreateUncachedEpochLocked(
          store, sequence, store->PublishedOldestReadableLocked());
    }
  }
  if (epoch == nullptr) {
    return Status::IOError("snapshot", "RocksDB refused snapshot");
  }
  assert(epoch->seq.value == sequence.value);
  if (shared_current) {
    return StoreSnapshot(std::move(store), this, epoch);
  }
  // Insert into the bounded cache. The created epoch owns the insertion
  // reference; a concurrent miss for the same sequence may have won, in which
  // case the winner's epoch is reused and ours is released.
  SnapshotEpoch* chosen = epoch;
  SnapshotEpoch* released = nullptr;
  std::shared_ptr<FactStoreImpl> owner;
  {
    std::lock_guard<std::mutex> cache_lock(store->as_of_cache_mutex);
    bool inserted = false;
    for (const FactStoreImpl::AsOfCacheEntry& entry : store->as_of_cache) {
      if (entry.epoch != nullptr && entry.seq.value == sequence.value) {
        chosen = entry.epoch;
        break;
      }
    }
    if (chosen == epoch) {
      size_t slot = store->as_of_cache.size();
      for (size_t index = 0; index < store->as_of_cache.size(); ++index) {
        if (store->as_of_cache[index].epoch == nullptr) {
          slot = index;
          break;
        }
      }
      if (slot == store->as_of_cache.size()) {
        slot = 0;
        for (size_t index = 1; index < store->as_of_cache.size(); ++index) {
          if (store->as_of_cache[index].seq.value <
              store->as_of_cache[slot].seq.value) {
            slot = index;
          }
        }
        released = store->as_of_cache[slot].epoch;
      }
      store->as_of_cache[slot] =
          FactStoreImpl::AsOfCacheEntry{sequence, epoch};
      inserted = true;
    }
    if (!inserted) {
      // A concurrent miss for the same sequence won the insertion; drop the
      // reference we created for the cache and use the winner's epoch.
      released = epoch;
    }
    // Pin the chosen epoch and copy its store owner before releasing the cache
    // mutex: the entry reference keeps it alive while we pin.
    chosen->refs.fetch_add(1, std::memory_order_relaxed);
    owner = chosen->store_owner;
  }
  if (released != nullptr && released != chosen) {
    store->ReleaseEpochRef(released);
  }
  return StoreSnapshot(std::move(owner), this, chosen);
}

StatusOr<std::optional<FactEvent>> FactStore::Read(
    const StoreSnapshot& snapshot, const FactRef& ref,
    ValidTime valid_time) const {
  if (snapshot.epoch_ == nullptr || snapshot.owner_ != this) {
    return Status::InvalidArgument("fact read", "snapshot belongs to another store");
  }
  const std::shared_ptr<FactStoreImpl>& store = snapshot.store_;
  const Status valid = ref.Validate();
  if (!valid.ok()) return valid;
  const std::string seek = EncodeFactKey(
      ref, valid_time, CommitSeq{std::numeric_limits<uint64_t>::max()});
  if (seek.empty()) return Status::InvalidArgument("fact read", "invalid fact key");
  // Borrow the identity prefix from the encoded seek key: no substring copy.
  const rocksdb::Slice prefix(seek.data(), kFactIdentityPrefixBytes);
  CanonicalReadPerfTracker physical_read(&store->canonical_read_physical_bytes);
  rocksdb::ReadOptions options;
  options.snapshot = snapshot.epoch_->lease->get();
  std::unique_ptr<rocksdb::Iterator> iterator(
      store->db->NewIterator(options, store->facts_cf));
  for (iterator->Seek(seek); iterator->Valid() && StartsWith(iterator->key(), prefix);
       iterator->Next()) {
    // Decode the iterator's borrowed key/value slices in place. `ToString()`
    // here used to allocate a string for both records on every point read.
    const auto decoded_key = DecodeFactKey(iterator->key().ToStringView());
    if (!decoded_key.ok()) return decoded_key.status();
    if (decoded_key.ValueOrDie().commit_seq.value > snapshot.commit_seq().value) {
      continue;
    }
    auto decoded_value = DecodeFactValue(
        decoded_key.ValueOrDie().ref, decoded_key.ValueOrDie().valid_from,
        decoded_key.ValueOrDie().commit_seq, iterator->value().ToStringView());
    if (!decoded_value.ok()) return decoded_value.status();
    if (decoded_value.ValueOrDie().operation == FactOperation::kDelete) {
      return std::optional<FactEvent>{};
    }
    return std::optional<FactEvent>{decoded_value.ConsumeValueOrDie()};
  }
  if (!iterator->status().ok()) return FromRocksDb(iterator->status(), "iterate fact read");
  return std::optional<FactEvent>{};
}

StatusOr<std::optional<FactEvent>> FactStore::ReadCurrent(
    const FactRef& ref, ValidTime valid_time) const {
  const auto snapshot = BeginCurrentSnapshot();
  if (!snapshot.ok()) return snapshot.status();
  return Read(snapshot.ValueOrDie(), ref, valid_time);
}

StatusOr<std::optional<FactEvent>> FactStore::ReadCurrentAt(
    CommitSeq sequence, const FactRef& ref, ValidTime valid_time) const {
  const auto snapshot = BeginCurrentSnapshotAt(sequence);
  if (!snapshot.ok()) return snapshot.status();
  // The as_of pin is exact by construction (see BeginCurrentSnapshotAt).
  assert(snapshot.ValueOrDie().commit_seq().value == sequence.value);
  return Read(snapshot.ValueOrDie(), ref, valid_time);
}

StatusOr<TemporalNeighborhood> FactStore::ReadTemporalNeighborhood(
    const StoreSnapshot& snapshot, const FactRef& ref,
    ValidTime valid_time) const {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store || snapshot.epoch_ == nullptr ||
        snapshot.epoch_->store != store.get()) {
      return Status::InvalidArgument("temporal neighborhood",
                                     "snapshot belongs to another store");
    }
  }
  return ::cedar::ReadTemporalNeighborhood(store.get(), ref, valid_time,
                                           snapshot.commit_seq(),
                                           snapshot.epoch_->lease->get());
}

Status TemporalNeighborhoodQuery::Validate() const { return ref.Validate(); }

Status FactStore::ReadTemporalNeighborhoods(
    const StoreSnapshot& snapshot,
    const std::vector<TemporalNeighborhoodQuery>& queries,
    std::vector<TemporalNeighborhood>* neighborhoods) const {
  if (neighborhoods == nullptr) {
    return Status::InvalidArgument("temporal neighborhood",
                                   "missing output vector");
  }
  neighborhoods->clear();
  if (queries.empty()) return Status::OK();
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store || snapshot.epoch_ == nullptr ||
        snapshot.epoch_->store != store.get()) {
      return Status::InvalidArgument("temporal neighborhood",
                                     "snapshot belongs to another store");
    }
  }
  for (const TemporalNeighborhoodQuery& query : queries) {
    const Status valid = query.Validate();
    if (!valid.ok()) return valid;
  }
  rocksdb::ReadOptions options;
  options.snapshot = snapshot.epoch_->lease->get();
  // One iterator serves the whole batch. Queries are visited in canonical fact
  // order so each Seek starts at or after the previous prefix.
  std::unique_ptr<rocksdb::Iterator> iterator(
      store->db->NewIterator(options, store->facts_cf));
  std::vector<size_t> order(queries.size());
  std::iota(order.begin(), order.end(), size_t{0});
  std::stable_sort(order.begin(), order.end(),
                   [&queries](size_t left, size_t right) {
                     const FactRef& a = queries[left].ref;
                     const FactRef& b = queries[right].ref;
                     return std::tuple{a.part_id().value,
                                       static_cast<uint8_t>(a.family()),
                                       a.property_id().value, a.entity_id()} <
                            std::tuple{b.part_id().value,
                                       static_cast<uint8_t>(b.family()),
                                       b.property_id().value, b.entity_id()};
                   });
  neighborhoods->resize(queries.size());
  for (size_t index : order) {
    auto neighborhood = ::cedar::ReadTemporalNeighborhoodWithIterator(
        store.get(), queries[index].ref, queries[index].valid_time,
        snapshot.commit_seq(), iterator.get());
    if (!neighborhood.ok()) return neighborhood.status();
    (*neighborhoods)[index] = neighborhood.ConsumeValueOrDie();
  }
  return Status::OK();
}

Status FactStore::Scan(const StoreSnapshot& snapshot, const FactPrefix& prefix,
                       const FactVisitor& visitor) const {
  return Scan(snapshot, prefix, FactScanBounds{}, visitor);
}

Status FactStore::ScanFamily(const StoreSnapshot& snapshot, FactFamily family,
                             const FactVisitor& visitor) const {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store || snapshot.epoch_ == nullptr ||
        snapshot.epoch_->store != store.get())
      return Status::InvalidArgument("fact scan", "snapshot belongs to another store");
  }
  if (!visitor) return Status::InvalidArgument("fact scan", "missing visitor");
  CanonicalReadPerfTracker physical_read(&store->canonical_read_physical_bytes);
  rocksdb::ReadOptions options;
  options.snapshot = snapshot.epoch_->lease->get();
  std::unique_ptr<rocksdb::Iterator> iterator(
      store->db->NewIterator(options, store->facts_cf));
  for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
    auto decoded_key = DecodeFactKey(iterator->key().ToString());
    if (!decoded_key.ok()) return decoded_key.status();
    if (decoded_key.ValueOrDie().ref.family() != family ||
        decoded_key.ValueOrDie().commit_seq.value > snapshot.commit_seq().value)
      continue;
    auto decoded_value = DecodeFactValue(
        decoded_key.ValueOrDie().ref, decoded_key.ValueOrDie().valid_from,
        decoded_key.ValueOrDie().commit_seq, iterator->value().ToString());
    if (!decoded_value.ok()) return decoded_value.status();
    if (Status status = visitor(decoded_value.ValueOrDie()); !status.ok()) return status;
  }
  return iterator->status().ok() ? Status::OK()
                                 : FromRocksDb(iterator->status(), "iterate family scan");
}

Status FactStore::Scan(const StoreSnapshot& snapshot, const FactPrefix& prefix,
                       const FactScanBounds& bounds,
                       const FactVisitor& visitor) const {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store || snapshot.epoch_ == nullptr ||
        snapshot.epoch_->store != store.get()) {
      return Status::InvalidArgument("fact scan", "snapshot belongs to another store");
    }
  }
  const Status valid = prefix.Validate();
  if (!valid.ok()) return valid;
  if (!visitor) return Status::InvalidArgument("fact scan", "missing visitor");
  if (bounds.entity_id_min.has_value() && bounds.entity_id_max.has_value() &&
      *bounds.entity_id_min > *bounds.entity_id_max) {
    return Status::InvalidArgument("fact scan", "invalid entity range");
  }
  if (prefix.entity_id().has_value() &&
      ((bounds.entity_id_min.has_value() &&
        *prefix.entity_id() < *bounds.entity_id_min) ||
       (bounds.entity_id_max.has_value() &&
        *prefix.entity_id() > *bounds.entity_id_max))) {
    return Status::OK();
  }
  const std::optional<uint64_t> seek_entity =
      prefix.entity_id().has_value() ? prefix.entity_id() : bounds.entity_id_min;
  const std::string match_prefix = EncodeFactIdentityPrefix(
      prefix.part_id(), prefix.family(), prefix.property_id(), prefix.entity_id());
  const std::string seek_key = EncodeFactIdentityPrefix(
      prefix.part_id(), prefix.family(), prefix.property_id(), seek_entity);
  CanonicalReadPerfTracker physical_read(&store->canonical_read_physical_bytes);
  rocksdb::ReadOptions options;
  options.snapshot = snapshot.epoch_->lease->get();
  std::unique_ptr<rocksdb::Iterator> iterator(
      store->db->NewIterator(options, store->facts_cf));
  for (iterator->Seek(seek_key);
       iterator->Valid() && StartsWith(iterator->key(), match_prefix);
       iterator->Next()) {
    const auto decoded_key = DecodeFactKey(iterator->key().ToString());
    if (!decoded_key.ok()) return decoded_key.status();
    if (bounds.entity_id_min.has_value() &&
        decoded_key.ValueOrDie().ref.entity_id() < *bounds.entity_id_min) {
      continue;
    }
    if (bounds.entity_id_max.has_value() &&
        decoded_key.ValueOrDie().ref.entity_id() > *bounds.entity_id_max) {
      break;
    }
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

Status FactStore::ScanColumnar(const StoreSnapshot& snapshot,
                               const FactPrefix& prefix,
                               const FactScanBounds& bounds,
                               const FactColumnarScanOptions& options,
                               const FactColumnarBatchVisitor& visitor) const {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store || snapshot.epoch_ == nullptr ||
        snapshot.epoch_->store != store.get()) {
      return Status::InvalidArgument("columnar scan", "snapshot belongs to another store");
    }
  }
  const Status prefix_status = prefix.Validate();
  if (!prefix_status.ok()) return prefix_status;
  if (!visitor) return Status::InvalidArgument("columnar scan", "missing visitor");
  if (options.batch_row_limit == 0) {
    return Status::InvalidArgument("columnar scan", "zero batch row limit");
  }
  if (options.projection.empty()) {
    return Status::InvalidArgument("columnar scan", "missing projection");
  }
  if ((bounds.entity_id_min.has_value() && bounds.entity_id_max.has_value() &&
       *bounds.entity_id_min > *bounds.entity_id_max) ||
      (options.event_valid_from_min.has_value() &&
       options.event_valid_from_max.has_value() &&
       options.event_valid_from_min->value > options.event_valid_from_max->value) ||
      (options.event_commit_seq_min.has_value() &&
       options.event_commit_seq_max.has_value() &&
       options.event_commit_seq_min->value > options.event_commit_seq_max->value)) {
    return Status::InvalidArgument("columnar scan", "invalid inclusive range");
  }

  rocksdb::cedar_parquet::CedarParquetScanSpec rocks_spec;
  rocks_spec.batch_row_limit = options.batch_row_limit;
  rocksdb::cedar_parquet::CedarParquetScanStats scan_stats;
  rocks_spec.stats = &scan_stats;
  if (options.event_valid_from_min.has_value()) {
    rocks_spec.valid_from_min = options.event_valid_from_min->value;
  }
  if (options.event_valid_from_max.has_value()) {
    rocks_spec.valid_from_max = options.event_valid_from_max->value;
  }
  if (options.event_commit_seq_min.has_value()) {
    rocks_spec.cedar_commit_seq_min = options.event_commit_seq_min->value;
  }
  if (options.event_commit_seq_max.has_value()) {
  rocks_spec.cedar_commit_seq_max = options.event_commit_seq_max->value;
  }
  rocks_spec.max_rows = options.max_rows;
  const std::optional<uint64_t> lower_entity =
      prefix.entity_id().has_value() ? prefix.entity_id() : bounds.entity_id_min;
  const std::optional<uint64_t> upper_entity =
      prefix.entity_id().has_value() ? prefix.entity_id() : bounds.entity_id_max;
  if (lower_entity.has_value() && *lower_entity != 0) {
    const auto lower = MakeEntitySortBound(prefix, *lower_entity, true);
    if (!lower.ok()) return lower.status();
    rocks_spec.sort_key_lower = lower.ValueOrDie();
  }
  if (upper_entity.has_value()) {
    if (*upper_entity == 0) return Status::OK();
    const auto upper = MakeEntitySortBound(prefix, *upper_entity, false);
    if (!upper.ok()) return upper.status();
    rocks_spec.sort_key_upper = upper.ValueOrDie();
  }
  rocks_spec.projection.reserve(options.projection.size());
  FactColumnarBatch output;
  output.columns.reserve(options.projection.size());
  for (FactColumnId id : options.projection) {
    for (const FactColumn& existing : output.columns) {
      if (existing.id == id) {
        return Status::InvalidArgument("columnar scan", "duplicate projection column");
      }
    }
    FactColumn column;
    const Status column_status = MakeFactColumn(id, &column);
    if (!column_status.ok()) return column_status;
    rocks_spec.projection.push_back(ToRocksColumnId(id));
    output.columns.push_back(std::move(column));
  }

  const auto new_output = [&]() -> FactColumnarBatch {
    FactColumnarBatch next;
    next.columns.reserve(options.projection.size());
    for (FactColumnId id : options.projection) {
      FactColumn column;
      const Status column_status = MakeFactColumn(id, &column);
      assert(column_status.ok());
      next.columns.push_back(std::move(column));
    }
    return next;
  };
  std::optional<Status> visitor_error;
  const auto flush = [&]() -> Status {
    if (output.row_count() == 0) return Status::OK();
    const Status callback_status = visitor(output);
    if (!callback_status.ok()) {
      visitor_error = callback_status;
      return callback_status;
    }
    output = new_output();
    return Status::OK();
  };
  const auto selected = [&](const DecodedFactKey& decoded) {
    return MatchesColumnarPrefix(prefix, decoded.ref) &&
           (!bounds.entity_id_min.has_value() ||
            decoded.ref.entity_id() >= *bounds.entity_id_min) &&
           (!bounds.entity_id_max.has_value() ||
            decoded.ref.entity_id() <= *bounds.entity_id_max) &&
           decoded.commit_seq.value <= snapshot.commit_seq().value &&
           (!options.event_valid_from_min.has_value() ||
            decoded.valid_from.value >= options.event_valid_from_min->value) &&
           (!options.event_valid_from_max.has_value() ||
            decoded.valid_from.value <= options.event_valid_from_max->value) &&
           (!options.event_commit_seq_min.has_value() ||
            decoded.commit_seq.value >= options.event_commit_seq_min->value) &&
           (!options.event_commit_seq_max.has_value() ||
            decoded.commit_seq.value <= options.event_commit_seq_max->value);
  };

  rocksdb::ReadOptions read_options;
  read_options.snapshot = snapshot.epoch_->lease->get();
  const rocksdb::Status scanned = rocksdb::ScanCedarParquetFacts(
      store->db.get(), store->facts_cf, read_options, rocks_spec,
      [&](const rocksdb::cedar_parquet::CedarParquetColumnarBatch& source) {
        const uint64_t source_bytes = ColumnarBatchBytes(source);
        store->projected_scan_rows.fetch_add(
            source.internal_keys.size(), std::memory_order_relaxed);
        if (options.projection.size() >= 21) {
          store->canonical_scan_bytes_read.fetch_add(
              source_bytes, std::memory_order_relaxed);
          store->logical_facts_bytes.fetch_add(
              source_bytes, std::memory_order_relaxed);
        } else {
          store->projected_scan_bytes_read.fetch_add(
              source_bytes, std::memory_order_relaxed);
        }
        if (source.columns.size() != output.columns.size()) {
          return rocksdb::Status::Corruption("columnar scan projection width mismatch");
        }
        for (size_t column = 0; column < source.columns.size(); ++column) {
          if (source.columns[column].id != ToRocksColumnId(output.columns[column].id)) {
            return rocksdb::Status::Corruption("columnar scan projection order mismatch");
          }
        }
        std::vector<size_t> selected_rows;
        selected_rows.reserve(source.internal_keys.size());
        for (size_t row = 0; row < source.internal_keys.size(); ++row) {
          if (source.internal_keys[row].size() < 8) {
            return rocksdb::Status::Corruption("columnar scan received short internal key");
          }
          const auto decoded = DecodeFactKey(
              source.internal_keys[row].substr(0, source.internal_keys[row].size() - 8));
          if (!decoded.ok()) return rocksdb::Status::Corruption(decoded.status().ToString());
          if (!selected(decoded.ValueOrDie())) continue;
          selected_rows.push_back(row);
        }
        size_t selected_begin = 0;
        while (selected_begin < selected_rows.size()) {
          const size_t room = options.batch_row_limit - output.row_count();
          const size_t selected_end = std::min(
              selected_rows.size(), selected_begin + room);
          for (size_t column = 0; column < source.columns.size(); ++column) {
            const Status appended = AppendSelectedColumnarValues(
                source.columns[column], selected_rows, selected_begin, selected_end,
                &output.columns[column]);
            if (!appended.ok()) return rocksdb::Status::Corruption(appended.ToString());
          }
          selected_begin = selected_end;
          if (output.row_count() == options.batch_row_limit) {
            const Status flushed = flush();
            if (!flushed.ok()) return rocksdb::Status::Incomplete(
                "columnar scan visitor stopped");
          }
        }
        return rocksdb::Status::OK();
      });
  store->projected_scan_pages_skipped.fetch_add(
      scan_stats.pages_skipped, std::memory_order_relaxed);
  store->projected_scan_pages_read.fetch_add(
      scan_stats.pages_read, std::memory_order_relaxed);
  store->projected_scan_physical_bytes_read.fetch_add(
      scan_stats.bytes_read, std::memory_order_relaxed);
  if (visitor_error.has_value()) return *visitor_error;
  if (!scanned.ok()) return FromRocksDb(scanned, "scan projected facts");
  return flush();
}

Status FactStore::ScanColumnarFamily(
    const StoreSnapshot& snapshot, FactFamily family, PropertyId property,
    const FactColumnarScanOptions& options,
    const FactColumnarBatchVisitor& visitor) const {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store || snapshot.epoch_ == nullptr ||
        snapshot.epoch_->store != store.get()) {
      return Status::InvalidArgument("columnar scan", "snapshot belongs to another store");
    }
  }
  if (!visitor) return Status::InvalidArgument("columnar scan", "missing visitor");
  std::set<uint32_t> partitions;
  rocksdb::ReadOptions read_options;
  read_options.snapshot = snapshot.epoch_->lease->get();
  std::unique_ptr<rocksdb::Iterator> iterator(
      store->db->NewIterator(read_options, store->facts_cf));
  for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
    auto key = DecodeFactKey(iterator->key().ToString());
    if (!key.ok()) return key.status();
    if (key.ValueOrDie().commit_seq.value > snapshot.commit_seq().value ||
        key.ValueOrDie().ref.family() != family ||
        key.ValueOrDie().ref.property_id() != property) {
      continue;
    }
    partitions.insert(key.ValueOrDie().ref.part_id().value);
  }
  if (!iterator->status().ok()) {
    return FromRocksDb(iterator->status(), "enumerate columnar fact partitions");
  }
  for (uint32_t part : partitions) {
    const Status scanned = ScanColumnar(
        snapshot, FactPrefix::Family(PartId{part}, family, property),
        FactScanBounds{}, options, visitor);
    if (!scanned.ok()) return scanned;
  }
  return Status::OK();
}

StatusOr<SequenceRecord> FactStore::ReadSequence(
    const StoreSnapshot& snapshot, CommitSeq commit_seq) const {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store || snapshot.epoch_ == nullptr ||
        snapshot.epoch_->store != store.get()) {
      return Status::InvalidArgument("scan sequence", "snapshot belongs to another store");
    }
  }
  if (commit_seq.value == 0 || commit_seq.value > snapshot.commit_seq().value) {
    return Status::InvalidArgument("scan sequence", "outside snapshot range");
  }
  const uint64_t sequence_retention =
      store->sequence_retention_watermark.load(std::memory_order_acquire);
  if (commit_seq.value < sequence_retention) {
    return Status::SnapshotExpired(
        "scan sequence",
        "sequence record was pruned below the sequence retention watermark");
  }
  const auto key = EncodeSequenceMetaKey(commit_seq);
  if (!key.ok()) return key.status();
  std::string encoded;
  rocksdb::ReadOptions options;
  options.snapshot = snapshot.epoch_->lease->get();
  const rocksdb::Status got = store->db->Get(options, store->meta_cf,
                                              key.ValueOrDie(), &encoded);
  if (got.IsNotFound()) {
    // Below the watermark a NotFound is the documented pruned-history outcome
    // and is handled above; at or above it a missing record is corruption.
    return Status::Corruption(
        "scan sequence",
        "missing durable sequence record at or above the sequence retention "
        "watermark");
  }
  if (!got.ok()) return FromRocksDb(got, "read scan sequence");
  auto record = DecodeSequenceRecord(encoded, commit_seq);
  if (!record.ok()) return record.status();
  if (record.ValueOrDie().commit_seq != commit_seq) {
    return Status::Corruption("scan sequence", "commit sequence disagrees with key");
  }
  return record;
}

StatusOr<std::vector<SequenceRecord>> FactStore::ReadSequenceRange(
    const StoreSnapshot& snapshot, CommitSeq first, CommitSeq last) const {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store || snapshot.epoch_ == nullptr ||
        snapshot.epoch_->store != store.get()) {
      return Status::InvalidArgument("scan sequence range",
                                     "snapshot belongs to another store");
    }
  }
  if (first.value == 0 || last.value < first.value ||
      last.value > snapshot.commit_seq().value) {
    return Status::InvalidArgument("scan sequence range", "invalid snapshot range");
  }
  // A pruned range must fail loudly instead of silently returning the surviving
  // suffix: the caller asked for history that the retention policy removed.
  const uint64_t sequence_retention =
      store->sequence_retention_watermark.load(std::memory_order_acquire);
  if (first.value < sequence_retention) {
    return Status::SnapshotExpired(
        "scan sequence range",
        "sequence record range starts below the sequence retention watermark");
  }
  const auto first_key = EncodeSequenceMetaKey(first);
  if (!first_key.ok()) return first_key.status();
  std::vector<SequenceRecord> records;
  records.reserve(static_cast<size_t>(last.value - first.value + 1));
  rocksdb::ReadOptions options;
  options.snapshot = snapshot.epoch_->lease->get();
  std::unique_ptr<rocksdb::Iterator> iterator(
      store->db->NewIterator(options, store->meta_cf));
  iterator->Seek(first_key.ValueOrDie());
  CommitSeq expected = first;
  while (iterator->Valid() && expected.value <= last.value) {
    if (!StartsWith(iterator->key(), kSequenceMetaPrefix)) break;
    const auto expected_key = EncodeSequenceMetaKey(expected);
    if (!expected_key.ok()) return expected_key.status();
    if (iterator->key().ToString() != expected_key.ValueOrDie()) {
      return Status::Corruption("scan sequence range", "sequence key disagrees with requested range");
    }
    const auto decoded =
        DecodeSequenceRecord(iterator->value().ToString(), expected);
    if (!decoded.ok()) {
      return decoded.status();
    }
    if (decoded.ValueOrDie().commit_seq != expected) {
      return Status::Corruption("scan sequence range", "non-contiguous sequence metadata");
    }
    records.push_back(decoded.ValueOrDie());
    if (expected.value == last.value) break;
    ++expected.value;
    iterator->Next();
  }
  const rocksdb::Status iterator_status = iterator->status();
  if (!iterator_status.ok()) return FromRocksDb(iterator_status, "read sequence range");
  if (records.size() != static_cast<size_t>(last.value - first.value + 1)) {
    return Status::Corruption("scan sequence range", "missing sequence metadata");
  }
  return records;
}

StatusOr<std::vector<FactEvent>> FactStore::ReadCanonicalEvents(
    const StoreSnapshot& snapshot, CommitSeq first, CommitSeq last) const {
  const auto records = ReadSequenceRange(snapshot, first, last);
  if (!records.ok()) return records.status();

  std::vector<std::string> fact_keys;
  for (const SequenceRecord& record : records.ValueOrDie()) {
    fact_keys.insert(fact_keys.end(), record.fact_keys.begin(),
                     record.fact_keys.end());
  }
  // Sequence records preserve commit membership. Sort by commit first and
  // canonical key second so the event stream is deterministic even when a
  // transaction assembled facts from independent mutation vectors.
  std::sort(fact_keys.begin(), fact_keys.end(),
            [](const std::string& left, const std::string& right) {
              const auto left_key = DecodeFactKey(left);
              const auto right_key = DecodeFactKey(right);
              if (!left_key.ok() || !right_key.ok()) {
                return left < right;
              }
              if (left_key.ValueOrDie().commit_seq !=
                  right_key.ValueOrDie().commit_seq) {
                return left_key.ValueOrDie().commit_seq.value <
                       right_key.ValueOrDie().commit_seq.value;
              }
              return left < right;
            });
  return ReadExactFacts(snapshot, fact_keys);
}

StatusOr<FactEvent> FactStore::ReadExactFact(
    const StoreSnapshot& snapshot, const std::string& encoded_fact_key) const {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store || snapshot.epoch_ == nullptr ||
        snapshot.epoch_->store != store.get()) {
      return Status::InvalidArgument("scan fact", "snapshot belongs to another store");
    }
  }
  const auto key = DecodeFactKey(encoded_fact_key);
  if (!key.ok()) return key.status();
  store->point_read_operations.fetch_add(1, std::memory_order_relaxed);
  if (key.ValueOrDie().commit_seq.value > snapshot.commit_seq().value) {
    return Status::InvalidArgument("scan fact", "outside snapshot range");
  }
  std::string encoded;
  CanonicalReadPerfTracker physical_read(&store->canonical_read_physical_bytes);
  rocksdb::ReadOptions options;
  options.snapshot = snapshot.epoch_->lease->get();
  const rocksdb::Status got = store->db->Get(options, store->facts_cf,
                                              encoded_fact_key, &encoded);
  if (!got.ok()) return FromRocksDb(got, "read scan fact");
  return DecodeFactValue(key.ValueOrDie().ref, key.ValueOrDie().valid_from,
                         key.ValueOrDie().commit_seq, encoded);
}

StatusOr<std::vector<FactEvent>> FactStore::ReadExactFacts(
    const StoreSnapshot& snapshot,
    const std::vector<std::string>& encoded_fact_keys) const {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store || snapshot.epoch_ == nullptr ||
        snapshot.epoch_->store != store.get()) {
      return Status::InvalidArgument("scan facts", "snapshot belongs to another store");
    }
  }
  if (encoded_fact_keys.empty()) return std::vector<FactEvent>{};
  std::vector<DecodedFactKey> decoded_keys;
  decoded_keys.reserve(encoded_fact_keys.size());
  std::vector<rocksdb::Slice> keys;
  keys.reserve(encoded_fact_keys.size());
  for (const std::string& encoded : encoded_fact_keys) {
    const auto decoded = DecodeFactKey(encoded);
    if (!decoded.ok()) return decoded.status();
    if (decoded.ValueOrDie().commit_seq.value > snapshot.commit_seq().value) {
      return Status::InvalidArgument("scan facts", "outside snapshot range");
    }
    decoded_keys.push_back(decoded.ValueOrDie());
    keys.emplace_back(encoded);
  }
  std::vector<rocksdb::ColumnFamilyHandle*> families(keys.size(), store->facts_cf);
  std::vector<std::string> values;
  CanonicalReadPerfTracker physical_read(&store->canonical_read_physical_bytes);
  rocksdb::ReadOptions options;
  options.snapshot = snapshot.epoch_->lease->get();
  const std::vector<rocksdb::Status> statuses =
      store->db->MultiGet(options, families, keys, &values);
  store->multi_get_operations.fetch_add(encoded_fact_keys.size(),
                                        std::memory_order_relaxed);
  store->multi_get_batches.fetch_add(1, std::memory_order_relaxed);
  std::vector<FactEvent> events;
  events.reserve(encoded_fact_keys.size());
  for (size_t i = 0; i < statuses.size(); ++i) {
    if (!statuses[i].ok()) {
      if (statuses[i].IsNotFound()) {
        return Status::Corruption("scan facts", "sequence references missing fact");
      }
      return FromRocksDb(statuses[i], "read exact facts");
    }
    const auto event = DecodeFactValue(decoded_keys[i].ref,
                                       decoded_keys[i].valid_from,
                                       decoded_keys[i].commit_seq, values[i]);
    if (!event.ok()) return event.status();
    events.push_back(event.ValueOrDie());
  }
  return events;
}

StatusOr<StoreCommitResult> FactStore::Commit(const StoreCommitBatch& batch) {
  if (options_.group_commit_max_batch_size > 1 &&
      options_.group_commit_window_us > 0) {
    return CommitGrouped(batch, std::nullopt, true);
  }
  return CommitDirect(batch, std::nullopt, true);
}

StatusOr<StoreCommitResult> FactStore::CommitWithWalCallback(
    const StoreCommitBatch& batch, WalDurableCallback on_wal_durable,
    void* callback_context) {
  if (on_wal_durable == nullptr) {
    return Status::InvalidArgument("commit", "missing WAL durable callback");
  }
  return CommitWithWalCallbackDirect(batch, on_wal_durable, callback_context);
}

StatusOr<StoreCommittedGroupResult> FactStore::CommitGroupWithWalCallback(
    const std::vector<StoreCommitGroupRequest>& requests,
    WalDurableCallback on_wal_durable, void* callback_context,
    std::atomic<bool>* wal_sync_critical) {
  if (requests.empty()) {
    return Status::InvalidArgument("commit", "missing committed group batches");
  }
  if (on_wal_durable == nullptr) {
    return Status::InvalidArgument("commit", "missing WAL durable callback");
  }
  return CommitGroupWithWalCallbackDirect(requests, on_wal_durable,
                                           callback_context, wal_sync_critical);
}

Status FactStore::PersistPreparedCommit(const StoreCommitBatch& batch) {
  return PersistPreparedCommits({batch});
}

Status FactStore::PersistPreparedCommits(
    const std::vector<StoreCommitBatch>& batches) {
  if (batches.empty()) {
    return Status::InvalidArgument("async prepare", "missing prepared batches");
  }
  for (const StoreCommitBatch& batch : batches) {
    const Status valid = batch.Validate();
    if (!valid.ok()) return valid;
  }
  std::set<uint64_t> txn_ids;
  for (const StoreCommitBatch& batch : batches) {
    if (!txn_ids.emplace(batch.txn_id.value).second) {
      return Status::InvalidArgument("async prepare", "duplicate transaction ID");
    }
  }
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("async prepare", "store is not open");
  }
  std::lock_guard<std::mutex> lock(store->publisher_mutex);
  if (store->recovery_required) {
    return Status::RecoveryRequired("async prepare", "reopen required after indeterminate write");
  }
  rocksdb::WriteBatch write_batch;
  for (const StoreCommitBatch& batch : batches) {
    const auto key = internal::EncodePreparedCommitKey(batch.txn_id);
    const auto value = internal::EncodePreparedCommit(batch);
    if (!key.ok()) return key.status();
    if (!value.ok()) return value.status();
    std::string existing;
    const rocksdb::Status got = store->db->Get(
        rocksdb::ReadOptions(), store->meta_cf, key.ValueOrDie(), &existing);
    if (got.ok()) {
      if (existing == value.ValueOrDie()) continue;
      return Status::Conflict("async prepare", "transaction ID belongs to a different batch");
    }
    if (!got.IsNotFound()) return FromRocksDb(got, "read async prepare");
    const auto transaction_key = EncodeTransactionMetaKey(batch.txn_id);
    if (!transaction_key.ok()) return transaction_key.status();
    std::string committed_outcome;
    const rocksdb::Status got_outcome = store->db->Get(
        rocksdb::ReadOptions(), store->meta_cf, transaction_key.ValueOrDie(),
        &committed_outcome);
    if (got_outcome.ok()) {
      return Status::Conflict("async prepare", "transaction ID is already committed");
    }
    if (!got_outcome.IsNotFound()) {
      return FromRocksDb(got_outcome, "read async transaction outcome");
    }
    write_batch.Put(store->meta_cf, key.ValueOrDie(), value.ValueOrDie());
  }
  if (write_batch.Count() == 0) return Status::OK();
  if (options_.async_prepare_prewrite_fault_injector_for_testing) {
    const Status injected =
        options_.async_prepare_prewrite_fault_injector_for_testing();
    if (!injected.ok()) {
      if (injected.IsIndeterminate()) store->recovery_required = true;
      return injected;
    }
  }
  rocksdb::WriteOptions options;
  options.sync = true;
  const rocksdb::Status written = store->db->Write(options, &write_batch);
  if (!written.ok()) {
    const Status status = FromMetadataWriteFailure(written);
    if (status.IsIndeterminate()) store->recovery_required = true;
    return status;
  }
  return Status::OK();
}

StatusOr<std::vector<StoreCommitBatch>> FactStore::ListPreparedCommits() const {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("async prepare", "store is not open");
  }
  const std::string prefix = internal::PreparedCommitPrefix();
  std::vector<StoreCommitBatch> batches;
  std::lock_guard<std::mutex> lock(store->publisher_mutex);
  std::unique_ptr<rocksdb::Iterator> iterator(
      store->db->NewIterator(rocksdb::ReadOptions(), store->meta_cf));
  for (iterator->Seek(prefix); iterator->Valid() && StartsWith(iterator->key(), prefix);
       iterator->Next()) {
    const auto decoded = internal::DecodePreparedCommit(iterator->value().ToString());
    if (!decoded.ok()) return decoded.status();
    const auto key = internal::EncodePreparedCommitKey(decoded.ValueOrDie().txn_id);
    if (!key.ok() || key.ValueOrDie() != iterator->key().ToString()) {
      return Status::Corruption("async prepare", "prepare key disagrees with record");
    }
    batches.push_back(decoded.ValueOrDie());
  }
  if (!iterator->status().ok()) return FromRocksDb(iterator->status(), "scan async prepares");
  return batches;
}

StatusOr<StoreCommitResult> FactStore::FinalizePreparedCommit(
    const StoreCommitBatch& batch, bool sync) {
  const auto key = internal::EncodePreparedCommitKey(batch.txn_id);
  if (!key.ok()) return key.status();
  if (options_.group_commit_max_batch_size > 1 &&
      options_.group_commit_window_us > 0) {
    return CommitGrouped(batch, key.ValueOrDie(), sync);
  }
  return CommitDirect(batch, key.ValueOrDie(), sync);
}

Status FactStore::SynchronizeWal() {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) {
      return Status::InvalidArgument("sync fact store WAL",
                                     "store is not open");
    }
  }
  return FromRocksDb(store->db->SyncWAL(), "sync fact store WAL");
}

Status FactStore::AbortPreparedCommit(TxnId txn_id) {
  const auto prepare_key = internal::EncodePreparedCommitKey(txn_id);
  const auto terminal_key = internal::EncodeAsyncTerminalKey(txn_id);
  const auto terminal_value = internal::EncodeAsyncAbortTerminal(txn_id);
  if (!prepare_key.ok()) return prepare_key.status();
  if (!terminal_key.ok()) return terminal_key.status();
  if (!terminal_value.ok()) return terminal_value.status();
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("async terminal", "store is not open");
  }
  std::lock_guard<std::mutex> lock(store->publisher_mutex);
  rocksdb::WriteBatch write_batch;
  write_batch.Put(store->meta_cf, terminal_key.ValueOrDie(), terminal_value.ValueOrDie());
  write_batch.Delete(store->meta_cf, prepare_key.ValueOrDie());
  rocksdb::WriteOptions options;
  options.sync = true;
  const rocksdb::Status written = store->db->Write(options, &write_batch);
  if (!written.ok()) {
    const Status status = FromMetadataWriteFailure(written);
    if (status.IsIndeterminate()) store->recovery_required = true;
    return status;
  }
  return Status::OK();
}

Status FactStore::PersistPreparedDecision(
    const StorePreparedDecision& decision) {
  const auto decision_key = internal::EncodePreparedDecisionKey(decision.txn_id);
  const auto decision_value = internal::EncodePreparedDecision(decision);
  if (!decision_key.ok()) return decision_key.status();
  if (!decision_value.ok()) return decision_value.status();
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) {
      return Status::InvalidArgument("prepared decision", "store is not open");
    }
  }
  std::lock_guard<std::mutex> lock(store->publisher_mutex);
  std::string existing;
  const rocksdb::Status got = store->db->Get(
      rocksdb::ReadOptions(), store->meta_cf, decision_key.ValueOrDie(),
      &existing);
  if (got.ok()) {
    return existing == decision_value.ValueOrDie()
               ? Status::OK()
               : Status::Conflict("prepared decision",
                                  "terminal certificate differs");
  }
  if (!got.IsNotFound()) return FromRocksDb(got, "read prepared decision");

  const auto prepare_key = internal::EncodePreparedCommitKey(decision.txn_id);
  if (!prepare_key.ok()) return prepare_key.status();
  std::string prepared;
  const rocksdb::Status got_prepare = store->db->Get(
      rocksdb::ReadOptions(), store->meta_cf, prepare_key.ValueOrDie(),
      &prepared);
  if (got_prepare.IsNotFound()) {
    return Status::NotFound("prepared decision", "transaction is not prepared");
  }
  if (!got_prepare.ok()) {
    return FromRocksDb(got_prepare, "read prepared transaction");
  }
  rocksdb::WriteOptions options;
  options.sync = true;
  const rocksdb::Status written = store->db->Put(
      options, store->meta_cf, decision_key.ValueOrDie(),
      decision_value.ValueOrDie());
  if (!written.ok()) {
    const Status status = FromMetadataWriteFailure(written);
    if (status.IsIndeterminate()) store->recovery_required = true;
    return status;
  }
  return Status::OK();
}

StatusOr<std::optional<StorePreparedDecision>>
FactStore::ResolvePreparedDecision(TxnId txn_id) const {
  const auto key = internal::EncodePreparedDecisionKey(txn_id);
  if (!key.ok()) return key.status();
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) {
      return Status::InvalidArgument("prepared decision", "store is not open");
    }
  }
  std::lock_guard<std::mutex> lock(store->publisher_mutex);
  std::string encoded;
  const rocksdb::Status got = store->db->Get(
      rocksdb::ReadOptions(), store->meta_cf, key.ValueOrDie(), &encoded);
  if (got.IsNotFound()) return std::optional<StorePreparedDecision>{};
  if (!got.ok()) return FromRocksDb(got, "read prepared decision");
  auto decoded = internal::DecodePreparedDecision(encoded);
  if (!decoded.ok()) return decoded.status();
  if (decoded.ValueOrDie().txn_id != txn_id) {
    return Status::Corruption("prepared decision",
                              "decision key disagrees with record");
  }
  return std::optional<StorePreparedDecision>{decoded.ValueOrDie()};
}

StatusOr<StoreCommitResult> FactStore::CommitGrouped(
    const StoreCommitBatch& batch, const std::optional<std::string>& prepared_key,
    bool sync) {
  const Status valid = batch.Validate();
  if (!valid.ok()) return valid;
  if (internal::EstimateCommitBatchBytes(batch) >
      options_.group_commit_max_batch_bytes) {
    return Status::ResourceExhausted("commit", "encoded batch exceeds hard byte limit");
  }
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("commit", "store is not open");
  }
  auto request = std::make_shared<FactStoreImpl::GroupCommitRequest>();
  request->batch = batch;
  request->prepared_key = prepared_key;
  request->sync = sync;
  std::vector<std::shared_ptr<FactStoreImpl::GroupCommitRequest>> requests;
  {
    std::unique_lock<std::mutex> lock(store->group_commit_mutex);
    store->group_commit_requests.push_back(request);
    while (store->group_commit_leader || request->selected ||
           store->group_commit_requests.front() != request) {
      store->group_commit_cv.notify_one();
      store->group_commit_cv.wait(lock, [&] {
        return request->result.has_value() ||
               (!request->selected && !store->group_commit_leader &&
                store->group_commit_requests.front() == request);
      });
      if (request->result.has_value()) return std::move(*request->result);
    }
    store->group_commit_leader = true;
    const auto deadline = std::chrono::steady_clock::now() +
        std::chrono::microseconds(options_.group_commit_window_us);
    store->group_commit_cv.wait_until(lock, deadline, [&] {
      return store->group_commit_requests.size() >= options_.group_commit_max_batch_size;
    });
    internal::CommitConflictIndex conflict_index;
    size_t selected_count = 0;
    for (; selected_count < store->group_commit_requests.size();
         ++selected_count) {
      const auto& candidate = store->group_commit_requests[selected_count];
      const internal::CommitFootprint footprint =
          internal::BuildCommitFootprint(candidate->batch);
      const bool compatible = candidate->sync == request->sync &&
                              selected_count < options_.group_commit_max_batch_size &&
                              conflict_index.Insert(footprint);
      // Preserve FIFO fairness: an incompatible request is a physical group
      // boundary, so newer requests never bypass it.
      if (!compatible) break;
    }
    for (size_t index = 0; index < selected_count; ++index) {
      store->group_commit_requests.front()->selected = true;
      requests.push_back(std::move(store->group_commit_requests.front()));
      store->group_commit_requests.pop_front();
    }
    store->group_commit_leader = false;
    store->group_commit_cv.notify_all();
  }

  const auto finish_all = [&](const StatusOr<StoreCommitResult>& result) {
    std::lock_guard<std::mutex> lock(store->group_commit_mutex);
    for (const auto& candidate : requests) candidate->result.emplace(result);
    store->group_commit_cv.notify_all();
  };
  if (requests.size() == 1) {
    const auto result = CommitDirect(request->batch, request->prepared_key,
                                     request->sync);
    finish_all(result);
    return result;
  }

  std::unique_lock<std::mutex> publisher_lock(store->publisher_mutex);
  if (store->recovery_required) {
    const Status result = Status::RecoveryRequired("commit", "reopen required after indeterminate write");
    finish_all(result);
    return result;
  }
  for (const auto& candidate : requests) {
    if (candidate->prepared_key.has_value()) {
      std::string encoded_prepare;
      const rocksdb::Status got_prepare = store->db->Get(
          rocksdb::ReadOptions(), store->meta_cf, *candidate->prepared_key,
          &encoded_prepare);
      if (got_prepare.IsNotFound()) {
        const Status result = Status::NotFound("async prepare",
                                               "prepared transaction is missing");
        finish_all(result);
        return result;
      }
      if (!got_prepare.ok()) {
        const Status result = FromRocksDb(got_prepare, "read async prepare");
        finish_all(result);
        return result;
      }
      const auto expected_prepare = internal::EncodePreparedCommit(candidate->batch);
      if (!expected_prepare.ok()) {
        finish_all(expected_prepare.status());
        return expected_prepare.status();
      }
      if (encoded_prepare != expected_prepare.ValueOrDie()) {
        const Status result = Status::Conflict(
            "async prepare", "prepared transaction differs from final batch");
        finish_all(result);
        return result;
      }
    }
    std::string outcome;
    const auto transaction_key = EncodeTransactionMetaKey(candidate->batch.txn_id);
    if (!transaction_key.ok()) {
      finish_all(transaction_key.status());
      return transaction_key.status();
    }
    const auto existing = store->db->Get(rocksdb::ReadOptions(), store->meta_cf,
                                         transaction_key.ValueOrDie(), &outcome);
    const Status snapshot_dependencies = ValidateSnapshotWriteDependencies(
        store.get(), candidate->batch.snapshot_write_dependencies);
    const Status strict_dependencies = ValidateStrictReadDependencies(
        store.get(), candidate->batch.strict_read_dependencies);
    const Status edge_identities =
        ValidateEdgeIdentitiesForCommit(store.get(), candidate->batch);
    if (!existing.IsNotFound() || !snapshot_dependencies.ok() ||
        !strict_dependencies.ok() || !edge_identities.ok()) {
      publisher_lock.unlock();
      for (const auto& fallback : requests) {
        const auto result = CommitDirect(fallback->batch, fallback->prepared_key,
                                         fallback->sync);
        std::lock_guard<std::mutex> lock(store->group_commit_mutex);
        fallback->result.emplace(result);
      }
      store->group_commit_cv.notify_all();
      return std::move(*request->result);
    }
  }
  if (requests.size() > std::numeric_limits<uint64_t>::max() - store->visible_seq.value) {
    const Status result = Status::ResourceExhausted("commit", "commit sequence exhausted");
    finish_all(result);
    return result;
  }
  rocksdb::WriteBatch write_batch;
  std::vector<StoreCommitResult> results;
  results.reserve(requests.size());
  for (const auto& candidate : requests) {
    const CommitSeq commit_seq{store->visible_seq.value + results.size() + 1};
    std::vector<std::string> fact_keys;
    fact_keys.reserve(candidate->batch.mutations.size());
    for (const PendingFactMutation& mutation : candidate->batch.mutations) {
      std::string key = EncodeFactKey(mutation.ref, mutation.valid_from, commit_seq);
      if (key.empty()) {
        const Status invalid = Status::InvalidArgument("commit", "invalid fact key");
        finish_all(invalid);
        return invalid;
      }
      fact_keys.push_back(std::move(key));
    }
    const Status appended = internal::AppendCandidateToWriteBatch(
        internal::CandidateCommit{&candidate->batch, commit_seq,
                                  std::move(fact_keys)},
        store->facts_cf, store->meta_cf, store->meta_sequence_format,
        &write_batch);
    if (!appended.ok()) {
      finish_all(appended);
      return appended;
    }
    if (candidate->prepared_key.has_value()) {
      write_batch.Delete(store->meta_cf, *candidate->prepared_key);
    }
    results.push_back(StoreCommitResult{commit_seq, candidate->batch.system_hlc});
  }
  const auto watermark = EncodeWatermark(results.back().commit_seq);
  if (!watermark.ok()) { finish_all(watermark.status()); return watermark.status(); }
  write_batch.Put(store->meta_cf, EncodeVisibleWatermarkKey(), watermark.ValueOrDie());
  const Status batch_size = CheckCommittedBatchSize(
      write_batch, options_.group_commit_max_batch_bytes);
  if (!batch_size.ok()) { finish_all(batch_size); return batch_size; }
  if (options_.commit_prewrite_fault_injector_for_testing) {
    const Status injected = options_.commit_prewrite_fault_injector_for_testing();
    if (!injected.ok()) { if (injected.IsIndeterminate()) store->recovery_required = true; finish_all(injected); return injected; }
  }
  rocksdb::WriteOptions options;
  options.sync = request->sync;
  if (options_.commit_write_options_observer_for_testing) {
    options_.commit_write_options_observer_for_testing(options.sync);
  }
  const rocksdb::Status written = store->db->Write(options, &write_batch);
  if (!written.ok()) { const Status result = FromCommitWriteFailure(written); if (result.IsIndeterminate()) store->recovery_required = true; finish_all(result); return result; }
  if (options_.commit_fault_injector_for_testing) {
    const Status injected = options_.commit_fault_injector_for_testing();
    if (!injected.ok()) {
      store->recovery_required = true;
      const Status result = injected.IsIndeterminate()
          ? injected : Status::Indeterminate("commit", injected.ToString());
      finish_all(result);
      return result;
    }
  }
  for (size_t index = 0; index < requests.size(); ++index) {
    PublishRecentFactWriteIndex(store.get(), requests[index]->batch,
                                results[index].commit_seq);
    PublishValidationCache(store.get(), requests[index]->batch,
                           results[index].commit_seq);
  }
  store->PublishVisible(results.back().commit_seq);
  {
    std::lock_guard<std::mutex> lock(store->group_commit_mutex);
    for (size_t index = 0; index < requests.size(); ++index) requests[index]->result.emplace(results[index]);
    store->group_commit_cv.notify_all();
  }
  return *request->result;
}

StatusOr<StoreCommitResult> FactStore::CommitDirect(
    const StoreCommitBatch& batch, const std::optional<std::string>& prepared_key,
    bool sync) {
  const Status valid = batch.Validate();
  if (!valid.ok()) return valid;
  if (internal::EstimateCommitBatchBytes(batch) >
      options_.group_commit_max_batch_bytes) {
    return Status::ResourceExhausted("commit", "encoded batch exceeds hard byte limit");
  }
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
  if (prepared_key.has_value()) {
    std::string encoded_prepare;
    const rocksdb::Status got_prepare = store->db->Get(
        rocksdb::ReadOptions(), store->meta_cf, *prepared_key, &encoded_prepare);
    if (got_prepare.IsNotFound()) {
      return Status::NotFound("async prepare", "prepared transaction is missing");
    }
    if (!got_prepare.ok()) return FromRocksDb(got_prepare, "read async prepare");
    const auto decoded_prepare = internal::DecodePreparedCommit(encoded_prepare);
    if (!decoded_prepare.ok()) return decoded_prepare.status();
    const auto expected_prepare = internal::EncodePreparedCommit(batch);
    if (!expected_prepare.ok()) return expected_prepare.status();
    if (encoded_prepare != expected_prepare.ValueOrDie()) {
      return Status::Conflict("async prepare", "prepared transaction differs from final batch");
    }
  }

  const auto transaction_key = EncodeTransactionMetaKey(batch.txn_id);
  if (!transaction_key.ok()) return transaction_key.status();
  std::string encoded_outcome;
  rocksdb::Status got_outcome = rocksdb::Status::NotFound();
  if (!batch.fresh_transaction_id) {
    if (options_.commit_transaction_lookup_observer_for_testing) {
      options_.commit_transaction_lookup_observer_for_testing();
    }
    got_outcome = store->db->Get(
        rocksdb::ReadOptions(), store->meta_cf, transaction_key.ValueOrDie(),
        &encoded_outcome);
  }
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
    const auto sequence = DecodeSequenceRecord(
        encoded_sequence, outcome.ValueOrDie().commit_seq);
    if (!sequence.ok()) return sequence.status();
    if (sequence.ValueOrDie().txn_id != batch.txn_id ||
        sequence.ValueOrDie().system_hlc != batch.system_hlc ||
        sequence.ValueOrDie().fact_keys.size() !=
            batch.mutations.size() + batch.edge_identities.size()) {
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
      const auto committed_identity =
          ReadLatestEdgeIdentity(store.get(), identity.edge_ref());
      if (!committed_identity.ok()) return committed_identity.status();
      if (!committed_identity.ValueOrDie().has_value()) {
        return Status::Corruption("commit", "committed edge lacks an authoritative identity fact");
      }
      if (*committed_identity.ValueOrDie() != identity) {
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

  const Status snapshot_dependencies = ValidateSnapshotWriteDependencies(
      store.get(), batch.snapshot_write_dependencies);
  if (!snapshot_dependencies.ok()) return snapshot_dependencies;
  const Status strict_dependencies = ValidateStrictReadDependencies(
      store.get(), batch.strict_read_dependencies);
  if (!strict_dependencies.ok()) return strict_dependencies;

  const Status edge_identities = ValidateEdgeIdentitiesForCommit(store.get(), batch);
  if (!edge_identities.ok()) return edge_identities;

  if (store->visible_seq.value == std::numeric_limits<uint64_t>::max()) {
    return Status::ResourceExhausted("commit", "commit sequence exhausted");
  }
  const CommitSeq commit_seq{store->visible_seq.value + 1};
  std::vector<std::string> fact_keys;
  fact_keys.reserve(batch.mutations.size());
  rocksdb::WriteBatch write_batch;
  for (const PendingFactMutation& mutation : batch.mutations) {
    std::string key = EncodeFactKey(mutation.ref, mutation.valid_from, commit_seq);
    if (key.empty()) return Status::InvalidArgument("commit", "invalid fact key");
    fact_keys.push_back(std::move(key));
  }
  const auto encoded_watermark = EncodeWatermark(commit_seq);
  if (!encoded_watermark.ok()) return encoded_watermark.status();
  const Status appended = internal::AppendCandidateToWriteBatch(
      internal::CandidateCommit{&batch, commit_seq, std::move(fact_keys)},
      store->facts_cf, store->meta_cf, store->meta_sequence_format,
      &write_batch);
  if (!appended.ok()) return appended;
  write_batch.Put(store->meta_cf, EncodeVisibleWatermarkKey(),
                  encoded_watermark.ValueOrDie());
  if (prepared_key.has_value()) write_batch.Delete(store->meta_cf, *prepared_key);
  const Status batch_size = CheckCommittedBatchSize(
      write_batch, options_.group_commit_max_batch_bytes);
  if (!batch_size.ok()) return batch_size;
  rocksdb::WriteOptions options;
  options.sync = sync;
  if (options_.commit_write_options_observer_for_testing) {
    options_.commit_write_options_observer_for_testing(options.sync);
  }
  if (options_.commit_prewrite_fault_injector_for_testing) {
    const Status injected = options_.commit_prewrite_fault_injector_for_testing();
    if (!injected.ok()) {
      if (injected.IsIndeterminate()) store->recovery_required = true;
      return injected;
    }
  }
  const rocksdb::Status written = store->db->Write(options, &write_batch);
  if (!written.ok()) {
    const Status status = FromCommitWriteFailure(written);
    if (status.IsIndeterminate()) store->recovery_required = true;
    return status;
  }
  if (options_.commit_fault_injector_for_testing) {
    const Status injected = options_.commit_fault_injector_for_testing();
    if (!injected.ok()) {
      store->recovery_required = true;
      return injected.IsIndeterminate()
                 ? injected
                 : Status::Indeterminate("commit", injected.ToString());
    }
  }
  PublishRecentFactWriteIndex(store.get(), batch, commit_seq);
  PublishValidationCache(store.get(), batch, commit_seq);
  store->PublishVisible(commit_seq);
  return StoreCommitResult{commit_seq, batch.system_hlc};
}

StatusOr<StoreCommitResult> FactStore::CommitWithWalCallbackDirect(
    const StoreCommitBatch& batch, WalDurableCallback on_wal_durable,
    void* callback_context) {
  const Status valid = batch.Validate();
  if (!valid.ok()) return valid;
  if (internal::EstimateCommitBatchBytes(batch) >
      options_.group_commit_max_batch_bytes) {
    return Status::ResourceExhausted("commit", "encoded batch exceeds hard byte limit");
  }
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
  std::string existing_outcome;
  rocksdb::Status got_outcome = rocksdb::Status::NotFound();
  if (!batch.fresh_transaction_id) {
    if (options_.commit_transaction_lookup_observer_for_testing) {
      options_.commit_transaction_lookup_observer_for_testing();
    }
    got_outcome = store->db->Get(
        rocksdb::ReadOptions(), store->meta_cf, transaction_key.ValueOrDie(),
        &existing_outcome);
  }
  if (got_outcome.ok()) {
    return Status::Conflict("commit", "transaction ID is already committed");
  }
  if (!got_outcome.IsNotFound()) {
    return FromRocksDb(got_outcome, "read transaction outcome");
  }

  const Status snapshot_dependencies = ValidateSnapshotWriteDependencies(
      store.get(), batch.snapshot_write_dependencies);
  if (!snapshot_dependencies.ok()) return snapshot_dependencies;
  const Status strict_dependencies = ValidateStrictReadDependencies(
      store.get(), batch.strict_read_dependencies);
  if (!strict_dependencies.ok()) return strict_dependencies;
  const Status edge_identities = ValidateEdgeIdentitiesForCommit(store.get(), batch);
  if (!edge_identities.ok()) return edge_identities;
  if (store->visible_seq.value == std::numeric_limits<uint64_t>::max()) {
    return Status::ResourceExhausted("commit", "commit sequence exhausted");
  }

  const CommitSeq commit_seq{store->visible_seq.value + 1};
  std::vector<std::string> fact_keys;
  fact_keys.reserve(batch.mutations.size());
  for (const PendingFactMutation& mutation : batch.mutations) {
    std::string key = EncodeFactKey(mutation.ref, mutation.valid_from, commit_seq);
    if (key.empty()) return Status::InvalidArgument("commit", "invalid fact key");
    fact_keys.push_back(std::move(key));
  }
  rocksdb::WriteBatch write_batch;
  const Status appended = internal::AppendCandidateToWriteBatch(
      internal::CandidateCommit{&batch, commit_seq, std::move(fact_keys)},
      store->facts_cf, store->meta_cf, store->meta_sequence_format,
      &write_batch);
  if (!appended.ok()) return appended;
  const auto watermark = EncodeWatermark(commit_seq);
  if (!watermark.ok()) return watermark.status();
  write_batch.Put(store->meta_cf, EncodeVisibleWatermarkKey(), watermark.ValueOrDie());
  const Status batch_size = CheckCommittedBatchSize(
      write_batch, options_.group_commit_max_batch_bytes);
  if (!batch_size.ok()) return batch_size;

  if (options_.commit_prewrite_fault_injector_for_testing) {
    const Status injected = options_.commit_prewrite_fault_injector_for_testing();
    if (!injected.ok()) {
      if (injected.IsIndeterminate()) store->recovery_required = true;
      return injected;
    }
  }
  rocksdb::CedarEpochOptions epoch_options;
  epoch_options.full_fsync = options_.wal_sync_mode == WalSyncMode::kFullDeviceFlush;
  if (options_.commit_write_options_observer_for_testing) {
    options_.commit_write_options_observer_for_testing(epoch_options.sync);
  }
  const rocksdb::Status written = rocksdb::WriteCedarEpoch(
      store->db.get(), epoch_options, &write_batch, on_wal_durable,
      callback_context);
  if (!written.ok()) {
    const Status status = FromCommitWriteFailure(written);
    store->recovery_required = true;
    return status.IsIndeterminate()
               ? status
               : Status::RecoveryRequired("commit", status.ToString());
  }
  if (options_.commit_fault_injector_for_testing) {
    const Status injected = options_.commit_fault_injector_for_testing();
    if (!injected.ok()) {
      store->recovery_required = true;
      return injected.IsIndeterminate()
                 ? injected
                 : Status::RecoveryRequired("commit", injected.ToString());
    }
  }
  PublishRecentFactWriteIndex(store.get(), batch, commit_seq);
  PublishValidationCache(store.get(), batch, commit_seq);
  store->PublishVisible(commit_seq);
  return StoreCommitResult{commit_seq, batch.system_hlc};
}

namespace {

// Order-sensitive digest of the request descriptors a decision was built from.
// Publication reads the caller's live batches, so the writer recomputes this
// from those batches and rejects a mismatch before the WAL write. It hashes only
// per-request scalar metadata and payload counts (never mutation payloads), so
// it stays O(requests) with no allocation and no copy of the batches themselves.
constexpr uint64_t kRequestAlignmentSeed = 0x9e3779b97f4a7c15ULL;

uint64_t MixRequestAlignment(uint64_t digest, uint64_t value) {
  digest ^= value + 0x9e3779b97f4a7c15ULL + (digest << 6) + (digest >> 2);
  return digest;
}

uint64_t RequestAlignmentDigest(const StoreCommitBatch& batch,
                                bool persist_async_abort) {
  uint64_t digest = MixRequestAlignment(kRequestAlignmentSeed, 1);
  digest = MixRequestAlignment(digest, batch.txn_id.value);
  digest = MixRequestAlignment(digest, persist_async_abort ? 1 : 0);
  digest = MixRequestAlignment(digest, batch.system_hlc);
  digest = MixRequestAlignment(digest, batch.mutations.size());
  digest = MixRequestAlignment(digest, batch.edge_identities.size());
  digest = MixRequestAlignment(digest,
                               batch.snapshot_write_dependencies.size());
  digest = MixRequestAlignment(digest, batch.strict_read_dependencies.size());
  return digest;
}

uint64_t RequestAlignmentDigest(
    const std::vector<StoreCommitGroupRequest>& requests) {
  uint64_t digest = kRequestAlignmentSeed;
  for (const StoreCommitGroupRequest& request : requests) {
    digest = MixRequestAlignment(
        digest, RequestAlignmentDigest(request.batch,
                                       request.persist_async_abort));
  }
  return digest;
}

uint64_t RequestAlignmentDigest(
    const std::vector<StoreCommitGroupRequestView>& requests) {
  uint64_t digest = kRequestAlignmentSeed;
  for (const StoreCommitGroupRequestView& request : requests) {
    digest = MixRequestAlignment(
        digest, RequestAlignmentDigest(*request.batch,
                                       request.persist_async_abort));
  }
  return digest;
}

// The caller holds FactStoreImpl::publisher_mutex. This stage has no RocksDB
// write side effect: it fixes terminal outcomes, assigns Cedar sequences, and
// constructs the one final WriteBatch before the writer receives it.
StatusOr<internal::DecidedEpoch> DecideAndEncodeGroupLocked(
    FactStoreImpl* store, const FactStoreOptions& options,
    const std::vector<StoreCommitGroupRequest>& requests) {
  const CommitSeq base_visible_seq = store->visible_seq;
  std::set<uint64_t> transaction_ids;
  auto write_batch = std::make_unique<rocksdb::WriteBatch>();
  StoreCommittedGroupResult group;
  group.results.reserve(requests.size());
  const auto validation_started_at = std::chrono::steady_clock::now();
  uint64_t committed_count = 0;
  bool has_durable_terminal = false;
  for (const StoreCommitGroupRequest& request : requests) {
    const StoreCommitBatch& batch = request.batch;
    Status validation = batch.Validate();
    bool may_persist_abort = request.persist_async_abort;
    if (validation.ok() && internal::EstimateCommitBatchBytes(batch) >
                               options.group_commit_max_batch_bytes) {
      validation = Status::ResourceExhausted(
          "commit", "encoded batch exceeds hard byte limit");
    }
    if (validation.ok() && !transaction_ids.emplace(batch.txn_id.value).second) {
      validation = Status::Conflict("commit", "duplicate transaction ID in group");
      may_persist_abort = false;
    }
    if (validation.ok()) {
      const auto transaction_key = EncodeTransactionMetaKey(batch.txn_id);
      if (!transaction_key.ok()) {
        validation = transaction_key.status();
      } else {
        std::string existing_outcome;
        rocksdb::Status got_outcome = rocksdb::Status::NotFound();
        if (!batch.fresh_transaction_id) {
          if (options.commit_transaction_lookup_observer_for_testing) {
            options.commit_transaction_lookup_observer_for_testing();
          }
          got_outcome = store->db->Get(
              rocksdb::ReadOptions(), store->meta_cf,
              transaction_key.ValueOrDie(), &existing_outcome);
        }
        if (got_outcome.ok()) {
          validation = Status::Conflict("commit", "transaction ID is already committed");
          may_persist_abort = false;
        } else if (!got_outcome.IsNotFound()) {
          return FromRocksDb(got_outcome, "read transaction outcome");
        }
      }
    }
    if (validation.ok()) {
      validation = ValidateSnapshotWriteDependencies(
          store, batch.snapshot_write_dependencies);
    }
    if (validation.ok()) {
      validation = ValidateStrictReadDependencies(
          store, batch.strict_read_dependencies);
    }
    if (validation.ok()) {
      validation = ValidateEdgeIdentitiesForCommit(store, batch);
    }
    if (!validation.ok()) {
      if (may_persist_abort) {
        const auto terminal_key = internal::EncodeAsyncTerminalKey(batch.txn_id);
        const auto terminal_value = internal::EncodeAsyncAbortTerminal(batch.txn_id);
        if (!terminal_key.ok()) return terminal_key.status();
        if (!terminal_value.ok()) return terminal_value.status();
        write_batch->Put(store->meta_cf, terminal_key.ValueOrDie(),
                         terminal_value.ValueOrDie());
        has_durable_terminal = true;
      }
      group.results.emplace_back(validation);
      continue;
    }
    if (store->visible_seq.value == std::numeric_limits<uint64_t>::max() ||
        committed_count == std::numeric_limits<uint64_t>::max() -
                               store->visible_seq.value) {
      return Status::ResourceExhausted("commit", "commit sequence exhausted");
    }
    const CommitSeq commit_seq{store->visible_seq.value + committed_count + 1};
    std::vector<std::string> fact_keys;
    fact_keys.reserve(batch.mutations.size());
    for (const PendingFactMutation& mutation : batch.mutations) {
      std::string key = EncodeFactKey(mutation.ref, mutation.valid_from, commit_seq);
      if (key.empty()) return Status::InvalidArgument("commit", "invalid fact key");
      fact_keys.push_back(std::move(key));
    }
    const Status appended = internal::AppendCandidateToWriteBatch(
        internal::CandidateCommit{&batch, commit_seq, std::move(fact_keys)},
        store->facts_cf, store->meta_cf, store->meta_sequence_format,
        write_batch.get());
    if (!appended.ok()) return appended;
    ++committed_count;
    group.results.emplace_back(StoreCommitResult{commit_seq, batch.system_hlc});
  }
  const auto validation_finished_at = std::chrono::steady_clock::now();
  group.validation_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          validation_finished_at - validation_started_at)
          .count());
  const auto assembly_started_at = validation_finished_at;
  const uint64_t request_alignment = RequestAlignmentDigest(requests);
  if (committed_count == 0 && !has_durable_terminal) {
    group.assembly_us = 0;
    return internal::DecidedEpoch(
        base_visible_seq, base_visible_seq, 0, false, nullptr,
        request_alignment, std::move(group));
  }
  if (committed_count != 0) {
    const auto watermark = EncodeWatermark(
        CommitSeq{store->visible_seq.value + committed_count});
    if (!watermark.ok()) return watermark.status();
    write_batch->Put(store->meta_cf, EncodeVisibleWatermarkKey(),
                     watermark.ValueOrDie());
  }
  const Status batch_size = CheckCommittedBatchSize(
      *write_batch, options.group_commit_max_batch_bytes);
  if (!batch_size.ok()) return batch_size;
  group.assembly_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - assembly_started_at)
          .count());
  return internal::DecidedEpoch(
      base_visible_seq,
      CommitSeq{base_visible_seq.value + committed_count}, committed_count,
      committed_count != 0 || has_durable_terminal, std::move(write_batch),
      request_alignment, std::move(group));
}

// The caller holds FactStoreImpl::publisher_mutex. This stage intentionally
// performs no validation, sequence assignment, or encoding; ClaimBatchForWrite
// makes an already decided epoch a one-shot writer submission.
StatusOr<StoreCommittedGroupResult> WriteDecidedGroupLocked(
    FactStoreImpl* store, const FactStoreOptions& options,
    internal::DecidedEpoch* epoch,
    const std::vector<StoreCommitGroupRequest>& requests,
    WalDurableCallback on_wal_durable, void* callback_context,
    std::atomic<bool>* wal_sync_critical) {
  if (epoch == nullptr) {
    return Status::InvalidArgument("commit", "missing decided epoch");
  }
  if (!epoch->requires_durable_write()) {
    return epoch->TakeGroupResult();
  }
  if (store->visible_seq != epoch->base_visible_seq()) {
    return Status::Conflict("commit", "decided epoch base is no longer current");
  }
  // Publication below reads the caller's live batches. The decision and the
  // request list must describe the same transactions, in the same order; a
  // matching count alone does not prove that (a permutation or a same-size
  // replacement would publish recent-write/validation entries under the wrong
  // sequences), so compare the order-sensitive alignment digest the decision
  // retained. Checked before the write so a contract violation cannot produce a
  // durable commit with no publication, or the wrong publication.
  const std::vector<StatusOr<StoreCommitResult>>& decisions =
      epoch->group_result().results;
  if (decisions.size() != requests.size() ||
      epoch->request_alignment() != RequestAlignmentDigest(requests)) {
    return Status::Corruption(
        "commit", "decided epoch requests do not match its decision");
  }

  if (options.commit_prewrite_fault_injector_for_testing) {
    const Status injected = options.commit_prewrite_fault_injector_for_testing();
    if (!injected.ok()) {
      if (injected.IsIndeterminate()) store->recovery_required = true;
      return injected;
    }
  }
  rocksdb::CedarEpochOptions kernel_options;
  kernel_options.full_fsync = options.wal_sync_mode == WalSyncMode::kFullDeviceFlush;
  if (options.commit_write_options_observer_for_testing) {
    options.commit_write_options_observer_for_testing(kernel_options.sync);
  }
  std::unique_ptr<rocksdb::WriteBatch> decided_batch = epoch->ClaimBatchForWrite();
  if (decided_batch == nullptr) {
    return Status::Corruption("commit", "decided epoch batch was already claimed");
  }
  rocksdb::Status written;
  rocksdb::CedarEpochMetrics kernel_metrics;
  if (options.kernel_write_observer_for_testing) {
    options.kernel_write_observer_for_testing(true);
  }
  kernel_options.wal_sync_critical = wal_sync_critical;
  written = rocksdb::WriteCedarEpoch(
      store->db.get(), kernel_options, decided_batch.get(), on_wal_durable,
      callback_context, &kernel_metrics);
  if (!written.ok()) {
    store->recovery_required = true;
    const Status status = FromCommitWriteFailure(written);
    return status.IsIndeterminate()
               ? status
               : Status::RecoveryRequired("commit", status.ToString());
  }
  if (options.commit_fault_injector_for_testing) {
    const Status injected = options.commit_fault_injector_for_testing();
    if (!injected.ok()) {
      store->recovery_required = true;
      return injected.IsIndeterminate()
                 ? injected
                 : Status::RecoveryRequired("commit", injected.ToString());
    }
  }
  const auto publication_started_at = std::chrono::steady_clock::now();
  if (epoch->committed_count() != 0) {
    // Publication consumes the caller's live request batches: no per-mutation
    // publication copy is retained across the decision/predecision window. The
    // result vector is aligned with `requests` (verified above); aborted
    // decisions have no commit sequence and are skipped.
    for (size_t index = 0; index < requests.size(); ++index) {
      if (!decisions[index].ok()) continue;
      const CommitSeq commit_seq = decisions[index].ValueOrDie().commit_seq;
      PublishRecentFactWriteIndex(store, requests[index].batch, commit_seq);
      PublishValidationCache(store, requests[index].batch, commit_seq);
    }
    store->PublishVisible(epoch->visible_seq_target());
  }
  StoreCommittedGroupResult result = epoch->TakeGroupResult();
  result.wal_append_us = kernel_metrics.wal_append_us;
  result.wal_sync_us = kernel_metrics.wal_sync_us;
  result.manifest_us = kernel_metrics.manifest_us;
  result.memtable_insert_us = kernel_metrics.memtable_insert_us;
  result.wal_rotations = kernel_metrics.wal_rotations;
  result.has_kernel_stage_metrics = true;
  result.publication_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - publication_started_at)
          .count());
  return result;
}

}  // namespace

StatusOr<std::unique_ptr<internal::DecidedEpoch>>
FactStore::DecideAndEncodeGroup(
    const std::vector<StoreCommitGroupRequest>& requests) {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("commit", "store is not open");
  }
  std::lock_guard<std::mutex> lock(store->publisher_mutex);
  if (store->recovery_required) {
    return Status::RecoveryRequired("commit",
                                    "reopen required after indeterminate write");
  }
  auto epoch = DecideAndEncodeGroupLocked(store.get(), options_, requests);
  if (!epoch.ok()) return epoch.status();
  return std::make_unique<internal::DecidedEpoch>(
      epoch.ConsumeValueOrDie());
}

StatusOr<std::unique_ptr<internal::DecidedEpoch>>
FactStore::DecideIndependentAppendGroup(
    CommitSeq base_visible_seq, CommitSeq required_snapshot_seq,
    const std::vector<StoreCommitGroupRequest>& requests) {
  // The planning-view overload keeps no reference past the call, so building the
  // temporary view vector is enough: the batches themselves are never copied.
  std::vector<StoreCommitGroupRequestView> views;
  views.reserve(requests.size());
  for (const StoreCommitGroupRequest& request : requests) {
    views.push_back(StoreCommitGroupRequestView{&request.batch,
                                                request.persist_async_abort});
  }
  return DecideIndependentAppendGroup(base_visible_seq, required_snapshot_seq,
                                      views);
}

StatusOr<std::unique_ptr<internal::DecidedEpoch>>
FactStore::DecideIndependentAppendGroup(
    CommitSeq base_visible_seq, CommitSeq required_snapshot_seq,
    const std::vector<StoreCommitGroupRequestView>& requests) {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("commit", "store is not open");
  }
  if (store->recovery_required.LoadFast()) {
    return Status::RecoveryRequired("commit",
                                    "reopen required after indeterminate write");
  }
  if (requests.empty()) {
    return Status::InvalidArgument("commit",
                                   "missing independent append batches");
  }
  if (base_visible_seq.value == std::numeric_limits<uint64_t>::max()) {
    return Status::ResourceExhausted("commit", "commit sequence exhausted");
  }

  const auto validation_started_at = std::chrono::steady_clock::now();
  internal::CommitConflictIndex conflict_index;
  auto write_batch = std::make_unique<rocksdb::WriteBatch>();
  StoreCommittedGroupResult group;
  group.results.reserve(requests.size());
  uint64_t committed_count = 0;
  for (const StoreCommitGroupRequestView& request : requests) {
    if (request.batch == nullptr) {
      return Status::InvalidArgument("commit",
                                     "missing independent append batch");
    }
    const StoreCommitBatch& batch = *request.batch;
    if (!batch.fresh_transaction_id ||
        !internal::CanUseAppendFastPath(batch)) {
      return Status::InvalidArgument(
          "commit",
          "independent append decision requires a fresh blind put batch");
    }
    // Snapshot-write dependencies are validated at promotion time (see
    // ValidatePredictedGroup); this decision only encodes the predicted
    // sequences. A stale prediction is discarded before the write.
    (void)required_snapshot_seq;
    const Status validation = batch.Validate();
    if (!validation.ok()) return validation;
    if (internal::EstimateCommitBatchBytes(batch) >
        options_.group_commit_max_batch_bytes) {
      return Status::ResourceExhausted(
          "commit", "encoded batch exceeds hard byte limit");
    }
    if (!conflict_index.Insert(internal::BuildCommitFootprint(batch))) {
      return Status::Conflict("commit",
                              "independent append group has a conflict");
    }
    if (committed_count ==
        std::numeric_limits<uint64_t>::max() - base_visible_seq.value) {
      return Status::ResourceExhausted("commit", "commit sequence exhausted");
    }
    const CommitSeq commit_seq{base_visible_seq.value + committed_count + 1};
    std::vector<std::string> fact_keys;
    fact_keys.reserve(batch.mutations.size());
    for (const PendingFactMutation& mutation : batch.mutations) {
      std::string key = EncodeFactKey(mutation.ref, mutation.valid_from, commit_seq);
      if (key.empty()) return Status::InvalidArgument("commit", "invalid fact key");
      fact_keys.push_back(std::move(key));
    }
    const Status appended = internal::AppendCandidateToWriteBatch(
        internal::CandidateCommit{&batch, commit_seq, std::move(fact_keys)},
        store->facts_cf, store->meta_cf, store->meta_sequence_format,
        write_batch.get());
    if (!appended.ok()) return appended;
    ++committed_count;
    group.results.emplace_back(StoreCommitResult{commit_seq, batch.system_hlc});
  }
  const auto validation_finished_at = std::chrono::steady_clock::now();
  group.validation_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          validation_finished_at - validation_started_at)
          .count());
  const auto watermark =
      EncodeWatermark(CommitSeq{base_visible_seq.value + committed_count});
  if (!watermark.ok()) return watermark.status();
  write_batch->Put(store->meta_cf, EncodeVisibleWatermarkKey(),
                   watermark.ValueOrDie());
  const Status batch_size =
      CheckCommittedBatchSize(*write_batch, options_.group_commit_max_batch_bytes);
  if (!batch_size.ok()) return batch_size;
  group.assembly_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - validation_finished_at)
          .count());
  // All views were validated non-null above; the digest is the writer's
  // alignment contract for the live batches passed to WriteDecidedGroup.
  const uint64_t request_alignment = RequestAlignmentDigest(requests);
  return std::make_unique<internal::DecidedEpoch>(
      base_visible_seq,
      CommitSeq{base_visible_seq.value + committed_count}, committed_count,
      true, std::move(write_batch), request_alignment, std::move(group));
}

Status FactStore::ValidatePredictedGroup(
    const std::vector<StoreCommitGroupRequest>& requests) const {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("commit", "store is not open");
  }
  if (store->recovery_required.LoadFast()) {
    return Status::RecoveryRequired("commit",
                                    "reopen required after indeterminate write");
  }
  for (const StoreCommitGroupRequest& request : requests) {
    const Status valid = ValidateSnapshotWriteDependencies(
        store.get(), request.batch.snapshot_write_dependencies);
    if (!valid.ok()) return valid;
  }
  return Status::OK();
}

StatusOr<StoreCommittedGroupResult> FactStore::WriteDecidedGroup(
    internal::DecidedEpoch* epoch,
    const std::vector<StoreCommitGroupRequest>& requests,
    WalDurableCallback on_wal_durable, void* callback_context,
    std::atomic<bool>* wal_sync_critical) {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("commit", "store is not open");
  }
  std::lock_guard<std::mutex> lock(store->publisher_mutex);
  if (store->recovery_required) {
    return Status::RecoveryRequired("commit",
                                    "reopen required after indeterminate write");
  }
  return WriteDecidedGroupLocked(store.get(), options_, epoch, requests,
                                 on_wal_durable, callback_context,
                                 wal_sync_critical);
}

StatusOr<StoreCommittedGroupResult> FactStore::CommitGroupWithWalCallbackDirect(
    const std::vector<StoreCommitGroupRequest>& requests,
    WalDurableCallback on_wal_durable, void* callback_context,
    std::atomic<bool>* wal_sync_critical) {
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
  auto epoch = DecideAndEncodeGroupLocked(store.get(), options_, requests);
  if (!epoch.ok()) return epoch.status();
  return WriteDecidedGroupLocked(store.get(), options_, &epoch.ValueOrDie(),
                                 requests, on_wal_durable, callback_context,
                                 wal_sync_critical);
}

StatusOr<TxnId> FactStore::AllocateTransactionId() {
  std::shared_ptr<FactStoreImpl> store;
  NonEpochUseGuard non_epoch_use;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("transaction", "store is not open");
    // Transaction start reads only atomics on the common path, but the lease
    // slow path writes allocator state through store->db. It therefore counts as
    // a non-epoch user: registration happens under lifecycle_mutex_, which Close
    // holds for its whole handshake, so Close either waits for this allocation
    // or starts after it finished and sees no open store.
    non_epoch_use = RegisterNonEpochUseLocked();
  }
  RunNonEpochOpObserverForTesting();
  for (;;) {
    if (store->recovery_required.LoadFast()) {
      return Status::RecoveryRequired(
          "transaction", "reopen required after indeterminate write");
    }
    uint64_t next = store->next_transaction_id.load(std::memory_order_relaxed);
    const uint64_t limit = store->transaction_lease_limit.load(
        std::memory_order_acquire);
    if (next < limit && store->next_transaction_id.compare_exchange_weak(
                            next, next + 1, std::memory_order_relaxed,
                            std::memory_order_relaxed)) {
      // The ID came from a previously synchronized lease. No publisher lock
      // is needed, so callers can continue staging N+1 during an epoch write.
      return TxnId{next};
    }

    std::lock_guard<std::mutex> lease_lock(store->transaction_lease_mutex);
    next = store->next_transaction_id.load(std::memory_order_relaxed);
    const uint64_t current_limit = store->transaction_lease_limit.load(
        std::memory_order_acquire);
    if (next < current_limit) continue;

    // Lease extension is rare (one synchronized metadata write per lease). Keep
    // the existing recovery and allocator ordering for this slow path while the
    // common path remains lock-free.
    std::lock_guard<std::mutex> publisher_lock(store->publisher_mutex);
    if (store->recovery_required) {
      return Status::RecoveryRequired(
          "transaction", "reopen required after indeterminate write");
    }
    const uint64_t lease_size = TransactionIdLeaseSize(options_);
    const uint64_t lease_start = store->transaction_allocator.next_id;
    if (lease_size >
        std::numeric_limits<uint64_t>::max() - lease_start) {
      return Status::ResourceExhausted("transaction", "transaction ID space exhausted");
    }
    const IdAllocatorState updated{IdKind::kTransaction,
                                   lease_start + lease_size};
    const auto encoded = EncodeIdAllocatorState(updated);
    if (!encoded.ok()) return encoded.status();
    rocksdb::WriteBatch batch;
    batch.Put(store->meta_cf, EncodeAllocatorMetaKey(IdKind::kTransaction),
              encoded.ValueOrDie());
    rocksdb::WriteOptions options;
    options.sync = true;
    if (options_.transaction_lease_write_observer_for_testing) {
      options_.transaction_lease_write_observer_for_testing(options.sync);
    }
    const rocksdb::Status written = store->db->Write(options, &batch);
    if (!written.ok()) {
      const Status status = FromMetadataWriteFailure(written);
      if (status.IsIndeterminate()) store->recovery_required = true;
      return status;
    }
    store->transaction_allocator = updated;
    store->next_transaction_id.store(lease_start, std::memory_order_relaxed);
    store->transaction_lease_limit.store(updated.next_id,
                                         std::memory_order_release);
  }
}

StatusOr<IdLease> FactStore::LeaseIds(IdKind kind, uint64_t count) {
  if (kind != IdKind::kVertex && kind != IdKind::kEdge) {
    return Status::InvalidArgument("ID lease", "unknown ID kind");
  }
  if (count == 0) count = kDefaultEntityIdLeaseSize;
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
  const auto schemas = store->SchemasSnapshot();
  const auto found = schemas->find(definition.property_id.value);
  if (found != schemas->end() && !found->second.empty()) {
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
  auto updated_schemas = std::make_shared<FactStoreImpl::PropertySchemas>(*schemas);
  (*updated_schemas)[definition.property_id.value].push_back(definition);
  std::shared_ptr<const FactStoreImpl::PropertySchemas> immutable_schemas =
      std::move(updated_schemas);
  store->PublishSchemas(std::move(immutable_schemas));
  return definition;
}

StatusOr<std::optional<PropertyDefinition>> FactStore::LookupProperty(
    PropertyId property_id, uint32_t schema_epoch) const {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("property definition", "store is not open");
  }
  std::lock_guard<std::mutex> lock(store->publisher_mutex);
  const auto schemas = store->SchemasSnapshot();
  return LookupPropertyInSchemas(*schemas, property_id,
                                 schema_epoch);
}

StatusOr<std::vector<PropertyDefinition>> FactStore::ListProperties() const {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("property definition", "store is not open");
  }
  const auto schemas = store->SchemasSnapshot();
  std::vector<PropertyDefinition> result;
  result.reserve(schemas->size());
  for (const auto& [property_id, epochs] : *schemas) {
    (void)property_id;
    if (!epochs.empty()) result.push_back(epochs.back());
  }
  std::sort(result.begin(), result.end(),
            [](const PropertyDefinition& left, const PropertyDefinition& right) {
              return left.property_id.value < right.property_id.value;
            });
  return result;
}

StatusOr<std::string> FactStore::SchemaFingerprint() const {
  // The fingerprint reads the property schema catalog, which a successful Close
  // releases (ReleaseRetainedStateForClose resets property_schemas). Register
  // with the close drain so a close that starts after the handle copy waits for
  // this read instead of releasing the catalog between the two steps.
  std::shared_ptr<FactStoreImpl> store;
  NonEpochUseGuard non_epoch_use;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("property definition", "store is not open");
    non_epoch_use = RegisterNonEpochUseLocked();
  }
  RunNonEpochOpObserverForTesting();
  const auto schemas = store->SchemasSnapshot();
  if (!schemas) {
    // Unreachable while the non-epoch registration is held: Close drains it
    // before releasing the catalog. Keep a contract violation from becoming a
    // null dereference instead of masking one (the registration is the fix).
    return Status::ShutdownInProgress("property definition",
                                      "store is closing or closed");
  }
  std::ostringstream canonical;
  for (const auto& [property_id, epochs] : *schemas) {
    if (epochs.empty()) continue;
    const auto& definition = epochs.back();
    canonical << property_id << ':' << definition.schema_epoch << ':'
              << static_cast<unsigned>(definition.entity_kind) << ':'
              << static_cast<unsigned>(definition.physical_type) << ':'
              << definition.blob_threshold_bytes << ':' << definition.name << ';';
  }
  const std::string bytes = canonical.str();
  const uint32_t digest = crc32c::Value(bytes.data(), bytes.size());
  std::ostringstream fingerprint;
  fingerprint << std::hex << digest << ':' << bytes.size();
  return fingerprint.str();
}

StatusOr<std::optional<PropertyDefinition>> FactStore::LookupProperty(
    const StoreSnapshot& snapshot, PropertyId property_id,
    uint32_t schema_epoch) const {
  // Lock-free ownership validation, same check as Read: the snapshot records
  // the FactStore it was opened from, and every snapshot handle owns an epoch
  // reference. A successful Close requires the epoch registry to have no live
  // references (otherwise it reports SnapshotPinned and rolls back), so a live
  // snapshot implies the schema catalogs are still retained: catalogs are
  // retired while the store is open and a successful close drains every epoch
  // reference before ReleaseRetainedStateForClose frees them. A handle that
  // outlives a force-detach must not be read, and destruction is documented to
  // be serialized with users of such a handle (~FactStore), so reading
  // concurrently with that release is a contract violation, not a supported
  // path. No lifecycle_mutex_ is needed here, which removes the last
  // store-global lock from first-property resolution on the transaction path.
  if (snapshot.epoch_ == nullptr || snapshot.owner_ != this) {
    return Status::InvalidArgument("property definition",
                                   "snapshot belongs to another store");
  }
  const FactStoreImpl::PropertySchemas* schemas =
      snapshot.epoch_->schemas.load(std::memory_order_acquire);
  if (schemas == nullptr) {
    return Status::InvalidArgument("property definition",
                                   "snapshot schema catalog is unavailable");
  }
  return LookupPropertyInSchemas(*schemas, property_id, schema_epoch);
}

namespace {

// Reads the immutable edge identity fact for `edge`. The caller has already
// established that a visible edge-state event exists in this snapshot (either by
// scanning the edge-state prefix or by observing one through Read).
StatusOr<std::optional<EdgeIdentity>> ReadEdgeIdentity(
    const FactStore& store, const StoreSnapshot& snapshot, EdgeRef edge) {
  const FactRef identity_ref(edge.home_part_id, FactFamily::kEdgeIdentity,
                             PropertyId{}, edge.edge_id.value);
  const auto identity_fact = store.Read(snapshot, identity_ref, ValidTime{0});
  if (!identity_fact.ok()) return identity_fact.status();
  if (!identity_fact.ValueOrDie().has_value() ||
      !identity_fact.ValueOrDie()->edge_identity.has_value()) {
    return Status::Corruption("edge identity",
                              "edge state lacks an authoritative identity fact");
  }
  const EdgeIdentity& identity = *identity_fact.ValueOrDie()->edge_identity;
  if (identity.edge_ref() != edge) {
    return Status::Corruption("edge identity", "identity fact disagrees with key");
  }
  return std::optional<EdgeIdentity>{identity};
}

}  // namespace

StatusOr<std::optional<EdgeIdentity>> FactStore::LookupEdgeIdentity(
    const StoreSnapshot& snapshot, EdgeRef edge) const {
  if (!edge.valid()) {
    return Status::InvalidArgument("edge identity", "zero edge ID");
  }

  // Identity is bound atomically with the first EdgeState PUT. Requiring that
  // state in the logical snapshot prevents a future immutable identity from
  // leaking into an older as-of view.
  bool has_visible_edge_state = false;
  const Status scanned = Scan(
      snapshot, FactPrefix::Exact(EntityFact::Edge(edge).ref()),
      [&has_visible_edge_state](const FactEvent&) {
        has_visible_edge_state = true;
        return Status::OK();
      });
  if (!scanned.ok()) return scanned;
  if (!has_visible_edge_state) return std::optional<EdgeIdentity>{};
  return ReadEdgeIdentity(*this, snapshot, edge);
}

StatusOr<std::optional<EdgeIdentity>> FactStore::LookupEdgeIdentity(
    const StoreSnapshot& snapshot, EdgeRef edge,
    const FactEvent& observed_edge_state) const {
  if (!edge.valid()) {
    return Status::InvalidArgument("edge identity", "zero edge ID");
  }
  // The observation must be the visible edge-state PUT the caller read from this
  // same snapshot. A mismatch is a programming error: it would mean the
  // visibility walk and the identity lookup could disagree about which edge they
  // are resolving, so reject instead of silently trusting it.
  if (observed_edge_state.ref != EntityFact::Edge(edge).ref() ||
      observed_edge_state.operation == FactOperation::kDelete) {
    return Status::InvalidArgument(
        "edge identity", "observed edge state does not match the lookup key");
  }
  // Locally re-check the one fact this overload can verify without the scan:
  // Read never returns an event above the snapshot's commit sequence, so a newer
  // observation cannot be the visible edge-state PUT. The remaining trust
  // boundary is deliberate - proving that this event is the version visible at
  // the caller's valid_time is exactly the edge-state scan this overload exists
  // to avoid. The caller must pass the event returned by
  // Read(snapshot, EntityFact::Edge(edge).ref(), valid_time) on this same
  // snapshot; a caller that cannot promise that must use the scanning overload.
  if (observed_edge_state.commit_seq.value > snapshot.commit_seq().value) {
    return Status::InvalidArgument(
        "edge identity", "observed edge state is newer than the snapshot");
  }
  // Derivation of the scan above: the scanning overload refuses the lookup
  // unless some visible event exists under the edge-state identity prefix.
  // `observed_edge_state` is exactly such an event (Read returned it from this
  // snapshot at the same prefix, so its commit_seq is visible and its
  // valid_from chain exists), therefore the scan can only report true and the
  // edge fact does not need to be read a second time. Everything after the scan
  // is unchanged.
  return ReadEdgeIdentity(*this, snapshot, edge);
}

namespace {

// Owns one Vacuum pending-boundary reservation (FactStoreImpl::
// pending_vacuum_oldest). It clears the reservation under epoch_mutex on every
// exit path that does not publish the boundary, including exceptions from the
// state encoders, the test fault injector, or an allocation failure while
// building a Status. Without the guard such a failure would leave readers
// rejecting sequences below a boundary that was never made durable. The guard
// only holds a raw pointer: Vacuum keeps the store alive with a shared_ptr.
class PendingVacuumReservation {
 public:
  explicit PendingVacuumReservation(FactStoreImpl* store) : store_(store) {}
  ~PendingVacuumReservation() {
    if (store_ == nullptr) return;
    std::lock_guard<std::mutex> epoch_lock(store_->epoch_mutex);
    store_->pending_vacuum_oldest = 0;
  }
  PendingVacuumReservation(const PendingVacuumReservation&) = delete;
  PendingVacuumReservation& operator=(const PendingVacuumReservation&) = delete;
  // Phase 2 already published the boundary and cleared the reservation in the
  // same epoch_mutex critical section; no further clearing is needed.
  void Dismiss() { store_ = nullptr; }

 private:
  FactStoreImpl* store_;
};

}  // namespace

Status FactStore::Vacuum(CommitSeq oldest_readable) {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("vacuum", "store is not open");
  }
  std::lock_guard<std::mutex> lock(store->publisher_mutex);
  if (store->recovery_required) {
    return Status::RecoveryRequired("vacuum",
                                    "reopen required after indeterminate write");
  }
  if (oldest_readable.value < store->oldest_readable_seq.value ||
      oldest_readable.value > store->visible_seq.value) {
    return Status::InvalidArgument("vacuum", "target is outside readable range");
  }
  // Opt-in retention (F14): every successful Vacuum finishes any sequence prune
  // up to its target, including the no-op Vacuums that repeat the current
  // boundary after a crash interrupted the deletes. The helper is a no-op when
  // the option is off, so the default durable layout and behavior are unchanged.
  const auto run_sequence_prune = [&](CommitSeq boundary) {
    if (!options_.prune_sequence_records_on_vacuum) return Status::OK();
    const Status pruned = PruneSequenceRecords(store.get(), options_, boundary);
    if (pruned.IsIndeterminate()) store->recovery_required = true;
    return pruned;
  };
  if (oldest_readable == store->oldest_readable_seq) {
    return run_sequence_prune(store->oldest_readable_seq);
  }
  // Cached historical epochs hold references that would otherwise pin the
  // retention boundary forever. Evict them first; readers still inside a cached
  // as_of read keep their own reference and are visible to the scan below.
  store->ClearAsOfCache();
  // Open/vacuum handshake, phase 1: scan the live epoch registry and reserve the
  // new retention boundary in one epoch_mutex critical section. The registry scan
  // alone is not enough: pins, creation and retirement do happen under
  // epoch_mutex, but an open validates the boundary *before* it registers its
  // epoch, so a scan can miss an open that validated against the old boundary and
  // registers afterwards. Epoch opens now repeat the boundary check and register
  // in a single critical section against pending_vacuum_oldest, so an open is
  // either visible to this scan (and makes the vacuum fail with SnapshotPinned)
  // or observes the reserved target (and reports SnapshotExpired). The target is
  // reserved before the durable write (not after) so readers never have to wait
  // for disk I/O while holding epoch_mutex; it is cleared on every path that does
  // not publish the boundary by the PendingVacuumReservation guard below.
  PendingVacuumReservation reservation(store.get());
  {
    std::lock_guard<std::mutex> epoch_lock(store->epoch_mutex);
    const std::optional<uint64_t> oldest_active =
        store->OldestActiveEpochSeqLocked();
    if (oldest_active.has_value() && *oldest_active < oldest_readable.value) {
      return Status::SnapshotPinned("vacuum", "active snapshot predates target");
    }
    store->pending_vacuum_oldest = oldest_readable.value;
  }
  const Status before_boundary = RunVacuumFaultInjector(
      options_, VacuumFaultPoint::kBeforeBoundaryWrite);
  if (!before_boundary.ok()) return before_boundary;
  const auto state = EncodeVacuumState(VacuumState{oldest_readable});
  const auto watermark = EncodeWatermark(oldest_readable);
  if (!state.ok()) return state.status();
  if (!watermark.ok()) return watermark.status();
  rocksdb::WriteBatch batch;
  batch.Put(store->meta_cf, EncodeVacuumStateKey(), state.ValueOrDie());
  batch.Put(store->meta_cf, EncodeOldestReadableWatermarkKey(),
            watermark.ValueOrDie());
  rocksdb::WriteOptions options;
  options.sync = true;
  const rocksdb::Status written = store->db->Write(options, &batch);
  if (!written.ok()) {
    // The durable boundary was not published, so no cleanup will run and the
    // older versions are still intact. An indeterminate result may or may not
    // have advanced the durable watermark; reopen resumes the persisted vacuum
    // rule either way, so the in-memory reservation is dropped here too (writes
    // stay blocked by recovery_required). The scope guard clears it.
    const Status status = FromMetadataWriteFailure(written);
    if (status.IsIndeterminate()) store->recovery_required = true;
    return status;
  }
  // Phase 2: publish the durable boundary and release the reservation in one
  // critical section. Any epoch registered from now on is at or above the new
  // boundary, and every epoch below it was either rejected in phase 1 or belongs
  // to a live reader (which made the scan above fail). The cleanup pass below can
  // therefore delete versions <= target without breaking a reachable read.
  {
    std::lock_guard<std::mutex> epoch_lock(store->epoch_mutex);
    store->oldest_readable_seq = oldest_readable;
    store->published_oldest_readable_seq.store(oldest_readable.value,
                                               std::memory_order_release);
    store->pending_vacuum_oldest = 0;
  }
  reservation.Dismiss();
  const Status after_boundary = RunVacuumFaultInjector(
      options_, VacuumFaultPoint::kAfterBoundaryWrite);
  if (!after_boundary.ok()) {
    if (after_boundary.IsIndeterminate()) store->recovery_required = true;
    return after_boundary;
  }
  const Status cleaned = ResumeVacuum(
      store.get(), options_,
      VacuumState{oldest_readable, VacuumPhase::kPrepared, {}});
  if (!cleaned.ok()) {
    if (cleaned.IsIndeterminate()) store->recovery_required = true;
    return cleaned;
  }
  return run_sequence_prune(oldest_readable);
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
  {
    std::lock_guard<std::mutex> lock(store->publisher_mutex);
    if (outcome.ValueOrDie().commit_seq.value > store->visible_seq.value) {
      return std::optional<StoreCommitResult>{};
    }
  }
  // A committed transaction whose sequence history was pruned is below the
  // durable retention boundary. Report the retention outcome explicitly instead
  // of the generic "outcome is missing sequence record" corruption below.
  if (outcome.ValueOrDie().commit_seq.value <
      store->sequence_retention_watermark.load(std::memory_order_acquire)) {
    return Status::SnapshotExpired(
        "transaction",
        "sequence record was pruned below the sequence retention watermark");
  }
  const auto sequence_key = EncodeSequenceMetaKey(outcome.ValueOrDie().commit_seq);
  if (!sequence_key.ok()) return sequence_key.status();
  std::string encoded_sequence;
  const rocksdb::Status got_sequence = store->db->Get(
      rocksdb::ReadOptions(), store->meta_cf, sequence_key.ValueOrDie(),
      &encoded_sequence);
  if (!got_sequence.ok()) {
    return Status::Corruption("transaction", "outcome is missing sequence record");
  }
  const auto sequence = DecodeSequenceRecord(
      encoded_sequence, outcome.ValueOrDie().commit_seq);
  if (!sequence.ok()) return sequence.status();
  if (sequence.ValueOrDie().txn_id != txn_id ||
      sequence.ValueOrDie().commit_seq != outcome.ValueOrDie().commit_seq) {
    return Status::Corruption("transaction", "outcome and sequence disagree");
  }
  return std::optional<StoreCommitResult>{
      StoreCommitResult{outcome.ValueOrDie().commit_seq,
                        sequence.ValueOrDie().system_hlc}};
}

StatusOr<std::optional<StoreAsyncCommitResult>> FactStore::ResolveAsyncTransaction(
    TxnId txn_id) const {
  const auto committed = ResolveTransaction(txn_id);
  if (!committed.ok()) return committed.status();
  if (committed.ValueOrDie().has_value()) {
    return std::optional<StoreAsyncCommitResult>{StoreAsyncCommitResult{
        StoreAsyncCommitOutcome::kCommitted, committed.ValueOrDie()->commit_seq, txn_id}};
  }
  const auto terminal_key = internal::EncodeAsyncTerminalKey(txn_id);
  if (!terminal_key.ok()) return terminal_key.status();
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("async terminal", "store is not open");
  }
  std::string encoded;
  const rocksdb::Status got = store->db->Get(
      rocksdb::ReadOptions(), store->meta_cf, terminal_key.ValueOrDie(), &encoded);
  if (got.IsNotFound()) return std::optional<StoreAsyncCommitResult>{};
  if (!got.ok()) return FromRocksDb(got, "read async terminal");
  const auto decoded = internal::DecodeAsyncAbortTerminal(encoded);
  if (!decoded.ok()) return decoded.status();
  if (decoded.ValueOrDie() != txn_id) {
    return Status::Corruption("async terminal", "terminal key disagrees with record");
  }
  return std::optional<StoreAsyncCommitResult>{StoreAsyncCommitResult{
      StoreAsyncCommitOutcome::kAborted, CommitSeq{}, txn_id}};
}

StatusOr<PressureSample> FactStore::SamplePressure() const {
  const auto sampled = SampleRuntime();
  if (!sampled.ok()) return sampled.status();
  return sampled.ValueOrDie().pressure;
}

StatusOr<RocksDbRuntimeMetrics> FactStore::SampleRuntimeMetrics() const {
  const auto sampled = SampleRuntime();
  if (!sampled.ok()) return sampled.status();
  return sampled.ValueOrDie().metrics;
}

void FactStore::RecordPointRead() const {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
  }
  if (store != nullptr) {
    store->point_read_operations.fetch_add(1, std::memory_order_relaxed);
  }
}

void FactStore::RecordPointRead(const StoreSnapshot& snapshot) const {
  // The snapshot owns a reference to the same store instance, so liveness is
  // already established; no lifecycle lock is needed to touch the counter.
  if (snapshot.epoch_ == nullptr || snapshot.owner_ != this ||
      snapshot.store_ == nullptr) {
    return;
  }
  snapshot.store_->point_read_operations.fetch_add(1,
                                                   std::memory_order_relaxed);
}

void FactStore::RecordMultiGet(uint64_t requests) const {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
  }
  if (store != nullptr) {
    store->multi_get_operations.fetch_add(requests, std::memory_order_relaxed);
  }
}

StatusOr<FactStoreMaintenanceResult> FactStore::RunNativeMaintenance(
    const FactStoreMaintenanceRequest& request,
    const std::atomic<bool>* wal_sync_critical) {
  std::shared_ptr<FactStoreImpl> store;
  NonEpochUseGuard non_epoch_use;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) {
      return Status::ShutdownInProgress("maintenance", "store is not open");
    }
    non_epoch_use = RegisterNonEpochUseLocked();
  }
  RunNonEpochOpObserverForTesting();

  rocksdb::CedarMaintenanceGrant grant;
  grant.snapshot_generation = request.snapshot_generation;
  grant.kind = request.kind == FactStoreMaintenanceKind::kFlush
                   ? rocksdb::CedarMaintenanceKind::kFlush
                   : rocksdb::CedarMaintenanceKind::kCompaction;
  grant.priority = request.emergency ? rocksdb::CedarMaintenancePriority::kEmergency
                                     : rocksdb::CedarMaintenancePriority::kNormal;
  grant.max_input_bytes = request.max_input_bytes;
  grant.max_output_bytes = request.max_output_bytes;
  grant.deadline_us = request.deadline_us;
  grant.wal_sync_critical = request.yield_for_wal_sync ? wal_sync_critical : nullptr;

  rocksdb::CedarMaintenanceResult native;
  const rocksdb::Status status =
      rocksdb::RunCedarMaintenance(store->db.get(), grant, &native);

  FactStoreMaintenanceResult result;
  result.grant_id = native.grant_id;
  result.kind = request.kind;
  result.input_bytes = native.input_bytes;
  result.output_bytes = native.output_bytes;
  result.elapsed_us = native.elapsed_us;
  result.remaining_smallest_complete_unit_bytes =
      native.remaining_smallest_complete_unit_bytes;
  result.atomic_overrun_bytes = native.atomic_overrun_bytes;
  result.flush_queue_depth = native.flush_queue_depth;
  result.unscheduled_flushes = native.unscheduled_flushes;
  result.scheduled_flushes = native.scheduled_flushes;
  result.running_flushes = native.running_flushes;
  result.selected_column_family_id = native.selected_column_family_id;
  switch (native.yield) {
    case rocksdb::CedarMaintenanceYield::kNone:
      result.yield = FactStoreMaintenanceYield::kNone;
      break;
    case rocksdb::CedarMaintenanceYield::kNoDebt:
      result.yield = FactStoreMaintenanceYield::kNoDebt;
      break;
    case rocksdb::CedarMaintenanceYield::kInputBudget:
      result.yield = FactStoreMaintenanceYield::kInputBudget;
      break;
    case rocksdb::CedarMaintenanceYield::kOutputBudget:
      result.yield = FactStoreMaintenanceYield::kOutputBudget;
      break;
    case rocksdb::CedarMaintenanceYield::kDeadline:
      result.yield = FactStoreMaintenanceYield::kDeadline;
      break;
    case rocksdb::CedarMaintenanceYield::kWalSync:
      result.yield = FactStoreMaintenanceYield::kWalSync;
      break;
    case rocksdb::CedarMaintenanceYield::kManualConflict:
      result.yield = FactStoreMaintenanceYield::kManualConflict;
      break;
    case rocksdb::CedarMaintenanceYield::kRecovery:
      result.yield = FactStoreMaintenanceYield::kRecovery;
      break;
    case rocksdb::CedarMaintenanceYield::kShutdown:
      result.yield = FactStoreMaintenanceYield::kShutdown;
      break;
    case rocksdb::CedarMaintenanceYield::kError:
      result.yield = FactStoreMaintenanceYield::kInvariantViolation;
      result.status = Status::IOError("native RocksDB maintenance", "job failed");
      break;
  }
  if (!status.ok() && result.status.ok()) {
    if (status.IsTryAgain() && native.yield == rocksdb::CedarMaintenanceYield::kNone) {
      result.yield = FactStoreMaintenanceYield::kStaleGeneration;
    }
    if (status.IsShutdownInProgress()) {
      result.status = Status::ShutdownInProgress("native RocksDB maintenance",
                                                 status.ToString());
    } else if (status.IsTimedOut()) {
      result.status = Status::MaintenanceBackoff("native RocksDB maintenance",
                                                  status.ToString());
    } else if (status.IsTryAgain() || status.IsBusy()) {
      result.status = Status::MaintenanceBackoff("native RocksDB maintenance",
                                                  status.ToString());
    } else {
      result.status = FromRocksDb(status, "run native RocksDB maintenance");
    }
  }
  return result;
}

StatusOr<FactStoreRuntimeSample> FactStore::SampleRuntime() const {
  if (options_.runtime_sample_observer_for_testing) {
    options_.runtime_sample_observer_for_testing();
  }
  std::shared_ptr<FactStoreImpl> store;
  NonEpochUseGuard non_epoch_use;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("pressure", "store is not open");
    non_epoch_use = RegisterNonEpochUseLocked();
  }
  RunNonEpochOpObserverForTesting();
  FactStoreRuntimeSample runtime_sample;
  PressureSample& sample = runtime_sample.pressure;
  RocksDbRuntimeMetrics& metrics = runtime_sample.metrics;
  RuntimeSamplingTiming timing;
  const auto pressure_started_at = std::chrono::steady_clock::now();
  rocksdb::CedarMaintenanceSnapshot maintenance;
  const rocksdb::Status runtime_status = rocksdb::PollCedarMaintenance(
      store->db.get(), &maintenance);
  if (!runtime_status.ok()) {
    return FromRocksDb(runtime_status, "sample Cedar maintenance snapshot");
  }
  const auto facts_it = std::find_if(
      maintenance.column_families.begin(), maintenance.column_families.end(),
      [](const rocksdb::CedarColumnFamilyDebt& debt) {
        return debt.role == rocksdb::CedarColumnFamilyRole::kFacts;
      });
  if (facts_it != maintenance.column_families.end()) {
    sample.l0_files = facts_it->l0_files;
    sample.pending_compaction_bytes = facts_it->pending_compaction_bytes;
    sample.immutable_memtable_count = facts_it->immutable_memtable_count;
    sample.columnar_backlog_buffers = facts_it->flush_pending ? 1 : 0;
    sample.columnar_flush_pending_bytes = facts_it->immutable_memtable_bytes;
    metrics.immutable_memtable_bytes = facts_it->immutable_memtable_bytes;
    metrics.active_memtable_bytes = facts_it->active_memtable_bytes;
  }
  sample.write_stopped = maintenance.write_stopped ? 1 : 0;
  sample.background_error = maintenance.background_errors;
  if (maintenance.write_buffer_manager_limit_bytes != 0) {
    sample.immutable_memtable_percent = std::min<uint64_t>(
        100, maintenance.total_immutable_memtable_bytes * 100 /
                 maintenance.write_buffer_manager_limit_bytes);
  }
  metrics.l0_files = sample.l0_files;
  metrics.maintenance_generation = maintenance.generation;
  metrics.pending_compaction_bytes = sample.pending_compaction_bytes;
  // Pressure occupancy includes both active and immutable MemTables, but the
  // maintenance debt field must remain immutable-only. Cedar uses that field
  // to decide whether a flush is already owed.
  metrics.block_cache_usage_bytes = maintenance.block_cache_usage_bytes;
  metrics.block_cache_pinned_bytes = maintenance.block_cache_pinned_bytes;
  metrics.running_flushes = maintenance.running_flushes;
  metrics.running_compactions = maintenance.running_compactions;
  metrics.flush_queue_depth = maintenance.flush_queue_depth;
  metrics.unscheduled_flushes = maintenance.unscheduled_flushes;
  metrics.scheduled_flushes = maintenance.scheduled_flushes;
  metrics.background_errors = maintenance.background_errors;
  metrics.background_errors_total = maintenance.background_errors;
  metrics.live_sst_bytes = maintenance.live_sst_bytes;
  metrics.blob_file_bytes = maintenance.blob_file_bytes;
  metrics.write_stopped = sample.write_stopped;
  metrics.manual_conflict = maintenance.manual_conflict;
  metrics.recovery_in_progress = maintenance.recovery_in_progress;
  metrics.shutting_down = maintenance.shutting_down;
  metrics.immutable_memtable_count = facts_it == maintenance.column_families.end()
                                         ? 0
                                         : facts_it->immutable_memtable_count;
  metrics.total_active_memtable_bytes = maintenance.total_active_memtable_bytes;
  metrics.total_immutable_memtable_bytes =
      maintenance.total_immutable_memtable_bytes;
  metrics.total_immutable_memtable_count =
      maintenance.total_immutable_memtable_count;
  metrics.total_l0_files = maintenance.total_l0_files;
  metrics.total_pending_compaction_bytes =
      maintenance.total_pending_compaction_bytes;
  metrics.write_buffer_manager_bytes = maintenance.write_buffer_manager_bytes;
  metrics.write_buffer_manager_limit_bytes =
      maintenance.write_buffer_manager_limit_bytes;
  metrics.delayed_write_rate_bytes_per_sec =
      maintenance.delayed_write_rate_bytes_per_sec;
  metrics.maintenance_flush_worker_capacity =
      store->maintenance_flush_worker_capacity;
  metrics.maintenance_compaction_worker_capacity =
      store->maintenance_compaction_worker_capacity;
  metrics.maintenance_normal_flush_credits =
      store->maintenance_normal_flush_credits;
  metrics.maintenance_emergency_flush_credits =
      store->maintenance_emergency_flush_credits;
  metrics.maintenance_compaction_credits =
      store->maintenance_compaction_credits;
  for (const auto& debt : maintenance.column_families) {
    RocksDbRuntimeMetrics::ColumnFamilyMetrics cf_metrics;
    cf_metrics.id = debt.id;
    switch (debt.role) {
      case rocksdb::CedarColumnFamilyRole::kDefault:
        cf_metrics.role = RocksDbRuntimeMetrics::ColumnFamilyRole::kDefault;
        break;
      case rocksdb::CedarColumnFamilyRole::kFacts:
        cf_metrics.role = RocksDbRuntimeMetrics::ColumnFamilyRole::kFacts;
        break;
      case rocksdb::CedarColumnFamilyRole::kMeta:
        cf_metrics.role = RocksDbRuntimeMetrics::ColumnFamilyRole::kMeta;
        break;
      case rocksdb::CedarColumnFamilyRole::kOther:
        cf_metrics.role = RocksDbRuntimeMetrics::ColumnFamilyRole::kOther;
        break;
    }
    cf_metrics.active_memtable_bytes = debt.active_memtable_bytes;
    cf_metrics.immutable_memtable_bytes = debt.immutable_memtable_bytes;
    cf_metrics.immutable_memtable_count = debt.immutable_memtable_count;
    cf_metrics.oldest_immutable_age_us = debt.oldest_immutable_age_us;
    cf_metrics.l0_files = debt.l0_files;
    cf_metrics.pending_compaction_bytes = debt.pending_compaction_bytes;
    cf_metrics.flush_pending = debt.flush_pending;
    cf_metrics.compaction_pending = debt.compaction_pending;
    metrics.column_families.push_back(cf_metrics);
  }
  timing.pressure_properties_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - pressure_started_at)
          .count());

  const auto wal_started_at = std::chrono::steady_clock::now();
  sample.retained_wal_bytes = maintenance.retained_wal_bytes;
  metrics.retained_wal_bytes = sample.retained_wal_bytes;
  const uint64_t sampled_now_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
  metrics.maintenance_snapshot_age_us =
      sampled_now_us >= maintenance.sampled_at_us
          ? sampled_now_us - maintenance.sampled_at_us
          : 0;
  metrics.background_flush_calls = maintenance.background_flush_calls;
  metrics.manual_compaction_calls = maintenance.manual_compaction_calls;
  metrics.periodic_task_registrations = maintenance.periodic_task_registrations;
  metrics.recovery_flush_exceptions = maintenance.recovery_flush_exceptions;
  timing.recovery_wal_bytes_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - wal_started_at)
          .count());

  sample.storage_debt_bytes = sample.pending_compaction_bytes;
  struct statvfs filesystem_stats {};
  if (statvfs(options_.path.c_str(), &filesystem_stats) == 0 &&
      filesystem_stats.f_frsize != 0) {
    const uint64_t free_blocks = static_cast<uint64_t>(filesystem_stats.f_bavail);
    const uint64_t block_size = static_cast<uint64_t>(filesystem_stats.f_frsize);
    if (free_blocks <= UINT64_MAX / block_size) {
      sample.free_disk_bytes = free_blocks * block_size;
      const uint64_t total_blocks = static_cast<uint64_t>(filesystem_stats.f_blocks);
      if (total_blocks != 0) {
        sample.free_disk_percent = std::min<uint64_t>(
            100, free_blocks * 100 / total_blocks);
      }
    }
  }
  if (store->statistics != nullptr) {
    const uint64_t bytes_written =
        store->statistics->getTickerCount(rocksdb::BYTES_WRITTEN);
    const uint64_t flush_bytes =
        store->statistics->getTickerCount(rocksdb::FLUSH_WRITE_BYTES);
    const uint64_t compact_bytes =
        store->statistics->getTickerCount(rocksdb::COMPACT_WRITE_BYTES);
    const auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(store->pressure_sample_mutex);
    if (store->pressure_sample_initialized) {
      const uint64_t interval_us = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(
              now - store->pressure_last_sample_at)
              .count());
      const auto rate = [interval_us](uint64_t current, uint64_t previous) {
        if (interval_us == 0 || current < previous) return uint64_t{0};
        const uint64_t delta = current - previous;
        return delta > UINT64_MAX / 1'000'000
                   ? UINT64_MAX
                   : delta * 1'000'000 / interval_us;
      };
      sample.sample_interval_us = interval_us;
      sample.admitted_facts_bytes_per_sec =
          rate(bytes_written, store->pressure_last_bytes_written);
      const uint64_t flush_rate = rate(flush_bytes, store->pressure_last_flush_bytes);
      const uint64_t compact_rate =
          rate(compact_bytes, store->pressure_last_compact_bytes);
      sample.completed_background_bytes_per_sec =
          flush_rate > UINT64_MAX - compact_rate ? UINT64_MAX
                                                  : flush_rate + compact_rate;
    }
    store->pressure_sample_initialized = true;
    store->pressure_last_sample_at = now;
    store->pressure_last_bytes_written = bytes_written;
    store->pressure_last_flush_bytes = flush_bytes;
    store->pressure_last_compact_bytes = compact_bytes;
    metrics.block_cache_hits =
        store->statistics->getTickerCount(rocksdb::BLOCK_CACHE_HIT);
    metrics.block_cache_misses =
        store->statistics->getTickerCount(rocksdb::BLOCK_CACHE_MISS);
    metrics.blocks_compressed =
        store->statistics->getTickerCount(rocksdb::NUMBER_BLOCK_COMPRESSED);
    metrics.compression_input_bytes =
        store->statistics->getTickerCount(rocksdb::BYTES_COMPRESSED_FROM);
    metrics.compression_output_bytes =
        store->statistics->getTickerCount(rocksdb::BYTES_COMPRESSED_TO);
  }
  metrics.point_read_operations =
      store->point_read_operations.load(std::memory_order_relaxed);
  metrics.multi_get_operations =
      store->multi_get_operations.load(std::memory_order_relaxed);
  metrics.multi_get_batches =
      store->multi_get_batches.load(std::memory_order_relaxed);
  metrics.projected_scan_rows =
      store->projected_scan_rows.load(std::memory_order_relaxed);
  metrics.projected_scan_bytes_read =
      store->projected_scan_bytes_read.load(std::memory_order_relaxed);
  metrics.projected_scan_pages_skipped =
      store->projected_scan_pages_skipped.load(std::memory_order_relaxed);
  metrics.projected_scan_pages_read =
      store->projected_scan_pages_read.load(std::memory_order_relaxed);
  metrics.projected_scan_physical_bytes_read =
      store->projected_scan_physical_bytes_read.load(std::memory_order_relaxed);
  metrics.canonical_scan_bytes_read =
      store->canonical_scan_bytes_read.load(std::memory_order_relaxed);
  metrics.canonical_read_physical_bytes =
      store->canonical_read_physical_bytes.load(std::memory_order_relaxed);
  metrics.logical_facts_bytes =
      store->logical_facts_bytes.load(std::memory_order_relaxed);
  std::unordered_set<std::string> live_files;
  std::vector<rocksdb::LiveFileMetaData> live_metadata;
  store->db->GetLiveFilesMetaData(&live_metadata);
  for (const auto& file : live_metadata) {
    live_files.insert(std::filesystem::path(file.name).filename().string());
  }
  std::error_code directory_error;
  for (const auto& entry : std::filesystem::directory_iterator(
           options_.path, directory_error)) {
    if (directory_error || !entry.is_regular_file(directory_error)) continue;
    const std::string name = entry.path().filename().string();
    const uint64_t bytes = entry.file_size(directory_error);
    if (directory_error) continue;
    if ((entry.path().extension() == ".sst" ||
         entry.path().extension() == ".ldb") &&
        !live_files.contains(name)) {
      metrics.obsolete_sst_bytes = metrics.obsolete_sst_bytes > UINT64_MAX - bytes
                                       ? UINT64_MAX
                                       : metrics.obsolete_sst_bytes + bytes;
    }
    if (entry.path().extension() == ".tmp" || name.find(".tmp.") != std::string::npos) {
      metrics.temporary_output_bytes =
          metrics.temporary_output_bytes > UINT64_MAX - bytes
              ? UINT64_MAX
              : metrics.temporary_output_bytes + bytes;
    }
  }
  metrics.free_disk_bytes = sample.free_disk_bytes;
  metrics.free_disk_percent = sample.free_disk_percent;
  timing.runtime_metrics_properties_us = timing.pressure_properties_us;
  runtime_sample.timing = timing;
  return runtime_sample;
}

StatusOr<ValidationCacheMetrics> FactStore::SampleValidationCacheMetrics() const {
  std::shared_ptr<FactStoreImpl> store;
  NonEpochUseGuard non_epoch_use;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("metrics", "store is not open");
    non_epoch_use = RegisterNonEpochUseLocked();
  }
  RunNonEpochOpObserverForTesting();
  ValidationCacheMetrics metrics;
  if (store->canonical_page_metrics) {
    metrics.canonical_page_hits = store->canonical_page_metrics->hits.load(std::memory_order_relaxed);
    metrics.canonical_page_misses = store->canonical_page_metrics->misses.load(std::memory_order_relaxed);
    metrics.canonical_page_decodes = store->canonical_page_metrics->decodes.load(std::memory_order_relaxed);
    metrics.canonical_page_decoded_bytes = store->canonical_page_metrics->decoded_bytes.load(std::memory_order_relaxed);
    metrics.canonical_page_resident_charge_bytes = store->canonical_page_metrics->resident_charge_bytes.load(std::memory_order_relaxed);
    metrics.canonical_page_pinned_charge_bytes = store->canonical_page_metrics->pinned_charge_bytes.load(std::memory_order_relaxed);
    metrics.canonical_page_uncached_live_bytes = store->canonical_page_metrics->uncached_live_bytes.load(std::memory_order_relaxed);
    metrics.canonical_page_insert_failures = store->canonical_page_metrics->insert_failures.load(std::memory_order_relaxed);
  }
  metrics.canonical_validation_scans = store->canonical_validation_scans.load(std::memory_order_relaxed);
  metrics.canonical_validation_boundaries = store->canonical_validation_boundaries.load(std::memory_order_relaxed);
  if (store->identity_history != nullptr) {
    metrics.identity_history_resident_bytes =
        store->identity_history->resident_bytes();
    metrics.identity_history_disabled = store->identity_history->disabled();
  }
  if (store->version_validation_cache == nullptr) return metrics;
  const auto cache_metrics = store->version_validation_cache->metrics();
  metrics.hits = cache_metrics.hits;
  metrics.misses = cache_metrics.misses;
  metrics.resident_chains = store->version_validation_cache->resident_chains();
  metrics.resident_bytes = store->version_validation_cache->resident_bytes();
  metrics.slot_capacity = store->version_validation_cache->slot_capacity();
  metrics.reserved_bytes = store->version_validation_cache->reserved_bytes();
  if (store->recent_fact_write_index != nullptr) {
    const auto index_metrics = store->recent_fact_write_index->metrics();
    metrics.recent_write_exact_hits = index_metrics.exact_hits;
    metrics.recent_write_bloom_hits = index_metrics.bloom_hits;
    metrics.recent_write_exact_absent = index_metrics.exact_absent;
    metrics.recent_write_exact_newer = index_metrics.exact_newer;
    metrics.recent_write_exact_evictions = index_metrics.exact_evictions;
    metrics.recent_write_exact_resets = index_metrics.exact_resets;
    metrics.recent_write_coverage_misses = index_metrics.coverage_misses;
    metrics.recent_write_exact_capacity = store->recent_fact_write_index->exact_capacity();
    metrics.recent_write_exact_resident_bytes = store->recent_fact_write_index->exact_resident_bytes();
    metrics.recent_write_index_hits = index_metrics.hits;
    metrics.recent_write_index_misses = index_metrics.misses;
    metrics.recent_write_index_resets = index_metrics.resets;
    metrics.recent_write_index_capacity =
        store->recent_fact_write_index->capacity();
    metrics.recent_write_index_resident_bytes =
        store->recent_fact_write_index->resident_bytes();
  }
  return metrics;
}

}  // namespace cedar
