// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "storage/rocks/rocks_adapter.h"

#include <algorithm>
#include <atomic>
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
#include <rocksdb/slice_transform.h>
#include <rocksdb/statistics.h>
#include <rocksdb/table.h>
#include <rocksdb/write_batch.h>

#include "db/cedar_columnar_scan.h"
#include "cedar/fact/fact_codec.h"
#include "cedar/fact/meta_codec.h"
#include "cedar/format.h"
#include "storage/rocks/commit_publisher.h"
#include "storage/rocks/decided_epoch.h"
#include "storage/facts/group_commit_planner.h"
#include "storage/facts/prepared_commit_codec.h"
#include "storage/facts/recent_fact_write_index.h"
#include "storage/rocks/rocksdb_config.h"
#include "storage/facts/vacuum.h"
#include "storage/facts/version_validation_cache.h"

namespace cedar {
namespace {

constexpr uint64_t kDefaultIdLeaseSize = 4096;
constexpr size_t kFactIdentityPrefixBytes = 16;
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

Status CheckCommittedBatchSize(const rocksdb::WriteBatch& batch,
                               uint64_t max_bytes) {
  if (batch.GetDataSize() > max_bytes) {
    return Status::ResourceExhausted("commit", "encoded batch exceeds hard byte limit");
  }
  return Status::OK();
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

  ~FactStoreImpl() {
    if (!db) return;
    if (default_cf != nullptr) db->DestroyColumnFamilyHandle(default_cf);
    if (facts_cf != nullptr) db->DestroyColumnFamilyHandle(facts_cf);
    if (meta_cf != nullptr) db->DestroyColumnFamilyHandle(meta_cf);
  }

  std::mutex publisher_mutex;
  std::mutex transaction_lease_mutex;
  std::mutex snapshot_mutex;
  bool accepting_snapshots = true;
  std::mutex group_commit_mutex;
  std::condition_variable group_commit_cv;
  std::deque<std::shared_ptr<GroupCommitRequest>> group_commit_requests;
  bool group_commit_leader = false;
  std::unique_ptr<rocksdb::DB> db;
  rocksdb::ColumnFamilyHandle* default_cf = nullptr;
  rocksdb::ColumnFamilyHandle* facts_cf = nullptr;
  rocksdb::ColumnFamilyHandle* meta_cf = nullptr;
  std::shared_ptr<rocksdb::Statistics> statistics;
  mutable std::mutex pressure_sample_mutex;
  bool pressure_sample_initialized = false;
  std::chrono::steady_clock::time_point pressure_last_sample_at;
  uint64_t pressure_last_bytes_written = 0;
  uint64_t pressure_last_flush_bytes = 0;
  uint64_t pressure_last_compact_bytes = 0;
  CommitSeq visible_seq;
  CommitSeq oldest_readable_seq;
  std::atomic<uint64_t> published_visible_seq{0};
  std::atomic<uint64_t> published_oldest_readable_seq{0};
  std::atomic<uint64_t> point_read_operations{0};
  std::atomic<uint64_t> multi_get_operations{0};
  std::atomic<uint64_t> projected_scan_rows{0};
  std::atomic<uint64_t> projected_scan_bytes_read{0};
  std::atomic<uint64_t> projected_scan_pages_skipped{0};
  std::atomic<uint64_t> projected_scan_pages_read{0};
  std::atomic<uint64_t> projected_scan_physical_bytes_read{0};
  std::atomic<uint64_t> canonical_scan_bytes_read{0};
  std::atomic<uint64_t> logical_facts_bytes{0};
  IdAllocatorState vertex_allocator{IdKind::kVertex, 1};
  IdAllocatorState edge_allocator{IdKind::kEdge, 1};
  IdAllocatorState transaction_allocator{IdKind::kTransaction, 1};
  std::atomic<uint64_t> next_transaction_id{1};
  std::atomic<uint64_t> transaction_lease_limit{1};
  std::shared_ptr<const PropertySchemas> property_schemas =
      std::make_shared<const PropertySchemas>();
  std::unique_ptr<internal::VersionValidationCache> version_validation_cache;
  std::unique_ptr<internal::RecentFactWriteIndex> recent_fact_write_index;
  std::function<void()> validation_scan_observer_for_testing;
  size_t active_snapshots = 0;
  std::multiset<uint64_t> active_snapshot_sequences;
  RecoveryState recovery_required;

  void PublishVisible(CommitSeq sequence) {
    visible_seq = sequence;
    published_visible_seq.store(sequence.value, std::memory_order_release);
  }
};

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

StatusOr<std::vector<CurrentFactInterval>> CurrentFactIntervals(
    FactStoreImpl* store, const FactRef& ref) {
  if (store->version_validation_cache != nullptr) {
    const auto cached = store->version_validation_cache->Lookup(ref);
    if (cached.has_value()) {
      std::vector<CurrentFactInterval> intervals;
      intervals.reserve(cached->size());
      for (const internal::ValidationBoundary& boundary : *cached) {
        intervals.push_back(CurrentFactInterval{boundary.valid_from,
                                                std::nullopt,
                                                boundary.commit_seq});
      }
      for (size_t index = 1; index < intervals.size(); ++index) {
        intervals[index].valid_to = intervals[index - 1].valid_from;
      }
      return intervals;
    }
  }
  const std::string prefix = EncodeFactIdentityPrefix(
      ref.part_id(), ref.family(), ref.property_id(), ref.entity_id());
  if (store->validation_scan_observer_for_testing) {
    store->validation_scan_observer_for_testing();
  }
  rocksdb::ReadOptions options;
  std::unique_ptr<rocksdb::Iterator> iterator(
      store->db->NewIterator(options, store->facts_cf));
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
    store->version_validation_cache->Prime(ref, std::move(boundaries));
  }
  return intervals;
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
  if (store->recent_fact_write_index == nullptr) return;
  for (const PendingFactMutation& mutation : mutations) {
    store->recent_fact_write_index->Publish(mutation.ref, commit_seq);
  }
}

void PublishRecentFactWriteIndex(FactStoreImpl* store,
                                 const StoreCommitBatch& batch,
                                 CommitSeq commit_seq) {
  PublishRecentFactWriteIndex(store, batch.mutations, commit_seq);
}

StatusOr<TemporalNeighborhood> ReadTemporalNeighborhood(
    FactStoreImpl* store, const FactRef& ref, ValidTime valid_time,
    CommitSeq commit_limit, const rocksdb::Snapshot* rocks_snapshot) {
  const Status valid = ref.Validate();
  if (!valid.ok()) return valid;
  store->point_read_operations.fetch_add(1, std::memory_order_relaxed);
  const std::string seek = EncodeFactKey(
      ref, valid_time, CommitSeq{std::numeric_limits<uint64_t>::max()});
  if (seek.empty()) {
    return Status::InvalidArgument("temporal neighborhood", "invalid fact key");
  }
  const std::string prefix = seek.substr(0, kFactIdentityPrefixBytes);
  rocksdb::ReadOptions options;
  options.snapshot = rocks_snapshot;

  TemporalNeighborhood neighborhood;
  std::unique_ptr<rocksdb::Iterator> forward(
      store->db->NewIterator(options, store->facts_cf));
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

  std::unique_ptr<rocksdb::Iterator> backward(
      store->db->NewIterator(options, store->facts_cf));
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
  for (const SnapshotWriteDependency& dependency : dependencies) {
    if (dependency.snapshot_seq.value > store->visible_seq.value) {
      return Status::InvalidArgument("commit",
                                     "snapshot dependency exceeds visible watermark");
    }
    if (store->recent_fact_write_index != nullptr &&
        store->recent_fact_write_index->CanProveUnchanged(
            dependency.ref, dependency.snapshot_seq)) {
      continue;
    }
    const auto intervals = CurrentFactIntervals(store, dependency.ref);
    if (!intervals.ok()) return intervals.status();
    for (const CurrentFactInterval& interval : intervals.ValueOrDie()) {
      if (interval.commit_seq.value <= dependency.snapshot_seq.value) continue;
      if (IntervalsOverlap(dependency.valid_from, dependency.successor,
                           interval.valid_from, interval.valid_to)) {
        return Status::Conflict(
            "commit", "snapshot write conflicts with a later overlapping event");
      }
    }
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
  std::unique_ptr<rocksdb::Iterator> iterator(
      store->db->NewIterator(rocksdb::ReadOptions(), store->meta_cf));
  for (iterator->Seek(kSequenceMetaPrefix);
       iterator->Valid() && StartsWith(iterator->key(), kSequenceMetaPrefix);
       iterator->Next()) {
    const auto sequence = DecodeSequenceRecord(iterator->value().ToString());
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
  std::atomic_store(&store->property_schemas, std::move(immutable_schemas));
  return Status::OK();
}

Status ValidateCommittedSequence(FactStoreImpl* store, CommitSeq commit_seq,
                                 bool require_facts) {
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
    if (!require_facts) continue;
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

Status ValidateCommittedSequences(FactStoreImpl* store, CommitSeq visible_seq,
                                  CommitSeq oldest_readable_seq) {
  if (visible_seq.value == 0) return Status::OK();
  rocksdb::ReadOptions options;
  std::unique_ptr<rocksdb::Iterator> iterator(
      store->db->NewIterator(options, store->meta_cf));
  uint64_t expected = 1;
  for (iterator->Seek(kSequenceMetaPrefix);
       iterator->Valid() && StartsWith(iterator->key(), kSequenceMetaPrefix);
       iterator->Next()) {
    const Status valid = ValidateCommittedSequence(
        store, CommitSeq{expected}, expected >= oldest_readable_seq.value);
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
        CommitSeq commit_seq, CommitSeq oldest_readable_seq,
        std::shared_ptr<const FactStoreImpl::PropertySchemas> property_schemas)
      : store(std::move(store)),
        snapshot(snapshot),
        commit_seq(commit_seq),
        oldest_readable_seq(oldest_readable_seq),
        property_schemas(std::move(property_schemas)) {}

  ~State() {
    if (!store) return;
    std::lock_guard<std::mutex> lock(store->snapshot_mutex);
    if (snapshot != nullptr && store->db) {
      store->db->ReleaseSnapshot(snapshot);
    }
    --store->active_snapshots;
    const auto registered = store->active_snapshot_sequences.find(commit_seq.value);
    if (registered != store->active_snapshot_sequences.end()) {
      store->active_snapshot_sequences.erase(registered);
    }
  }

  std::shared_ptr<FactStoreImpl> store;
  const rocksdb::Snapshot* snapshot = nullptr;
  CommitSeq commit_seq;
  CommitSeq oldest_readable_seq;
  std::shared_ptr<const FactStoreImpl::PropertySchemas> property_schemas;
};

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
  std::set<std::string> dependencies;
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
  std::set<std::pair<std::string, uint64_t>> strict_reads;
  for (const StrictReadDependency& dependency : strict_read_dependencies) {
    const Status dependency_valid = dependency.Validate();
    if (!dependency_valid.ok()) return dependency_valid;
    const std::string ref_key = EncodeFactKey(dependency.ref, ValidTime{0},
                                              CommitSeq{1});
    if (ref_key.empty() ||
        !strict_reads.emplace(ref_key, dependency.valid_time.value).second) {
      return Status::InvalidArgument("commit batch", "duplicate strict read dependency");
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
      options_.storage_profile == StorageProfile::kProductionAppend
          ? &resolved_profile.ValueOrDie()
          : nullptr;
  rocksdb::Options options = internal::MakeRocksDbOptions(
      options_, is_new_database, resolved_ptr);

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
      internal::MakeRocksDbColumnFamilyDescriptors(options_, options, resolved_ptr);
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
  store->statistics = options.statistics;
  store->validation_scan_observer_for_testing =
      options_.validation_scan_observer_for_testing;

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
    const Status sequences_valid =
        ValidateCommittedSequences(store.get(), store->visible_seq,
                                   store->oldest_readable_seq);
    if (!sequences_valid.ok()) return sequences_valid;
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
  const uint64_t write_index_bytes = options_.validation_cache_bytes / 4;
  store->version_validation_cache =
      std::make_unique<internal::VersionValidationCache>(
          options_.validation_cache_bytes - write_index_bytes);
  store->recent_fact_write_index =
      std::make_unique<internal::RecentFactWriteIndex>(
          write_index_bytes, store->visible_seq);
  impl_ = std::move(store);
  return Status::OK();
}

Status FactStore::Close() {
  std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
  if (!impl_) return Status::OK();
  std::lock_guard<std::mutex> publisher_lock(impl_->publisher_mutex);
  std::lock_guard<std::mutex> snapshot_lock(impl_->snapshot_mutex);
  if (impl_->active_snapshots != 0) {
    return Status::SnapshotPinned("fact store", "active snapshots prevent close");
  }
  impl_->accepting_snapshots = false;
  impl_.reset();
  return Status::OK();
}

StatusOr<StoreSnapshot> FactStore::BeginSnapshot(SnapshotOptions options) const {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("fact store", "store is not open");
  }
  if (options_.snapshot_open_observer_for_testing) {
    options_.snapshot_open_observer_for_testing();
  }
  std::lock_guard<std::mutex> snapshot_lock(store->snapshot_mutex);
  if (!store->accepting_snapshots) {
    return Status::ShutdownInProgress("snapshot", "fact store is closing");
  }
  const CommitSeq visible{store->published_visible_seq.load(
      std::memory_order_acquire)};
  const CommitSeq oldest{store->published_oldest_readable_seq.load(
      std::memory_order_acquire)};
  const CommitSeq selected = options.as_of.value_or(visible);
  if (selected.value < oldest.value) {
    return Status::SnapshotExpired("snapshot", "sequence is below retention boundary");
  }
  if (selected.value > visible.value) {
    return Status::InvalidArgument("snapshot", "sequence exceeds visible watermark");
  }
  const rocksdb::Snapshot* snapshot = store->db->GetSnapshot();
  if (snapshot == nullptr) return Status::IOError("snapshot", "RocksDB refused snapshot");
  const auto schemas = std::atomic_load(&store->property_schemas);
  auto state = std::make_unique<StoreSnapshot::State>(
      store, snapshot, selected, oldest, schemas);
  ++store->active_snapshots;
  store->active_snapshot_sequences.insert(selected.value);
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

StatusOr<TemporalNeighborhood> FactStore::ReadTemporalNeighborhood(
    const StoreSnapshot& snapshot, const FactRef& ref,
    ValidTime valid_time) const {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store || snapshot.state_ == nullptr || snapshot.state_->store != store) {
      return Status::InvalidArgument("temporal neighborhood",
                                     "snapshot belongs to another store");
    }
  }
  return ::cedar::ReadTemporalNeighborhood(store.get(), ref, valid_time,
                                           snapshot.commit_seq(),
                                           snapshot.state_->snapshot);
}

Status FactStore::Scan(const StoreSnapshot& snapshot, const FactPrefix& prefix,
                       const FactVisitor& visitor) const {
  return Scan(snapshot, prefix, FactScanBounds{}, visitor);
}

Status FactStore::Scan(const StoreSnapshot& snapshot, const FactPrefix& prefix,
                       const FactScanBounds& bounds,
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
  rocksdb::ReadOptions options;
  options.snapshot = snapshot.state_->snapshot;
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
    if (!store || snapshot.state_ == nullptr || snapshot.state_->store != store) {
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
  read_options.snapshot = snapshot.state_->snapshot;
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
  if (visitor_error.has_value()) return *visitor_error;
  if (!scanned.ok()) return FromRocksDb(scanned, "scan projected facts");
  store->projected_scan_pages_skipped.fetch_add(
      scan_stats.pages_skipped, std::memory_order_relaxed);
  store->projected_scan_pages_read.fetch_add(
      scan_stats.pages_read, std::memory_order_relaxed);
  store->projected_scan_physical_bytes_read.fetch_add(
      scan_stats.bytes_read, std::memory_order_relaxed);
  return flush();
}

StatusOr<SequenceRecord> FactStore::ReadSequence(
    const StoreSnapshot& snapshot, CommitSeq commit_seq) const {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store || snapshot.state_ == nullptr || snapshot.state_->store != store) {
      return Status::InvalidArgument("scan sequence", "snapshot belongs to another store");
    }
  }
  if (commit_seq.value == 0 || commit_seq.value > snapshot.commit_seq().value) {
    return Status::InvalidArgument("scan sequence", "outside snapshot range");
  }
  const auto key = EncodeSequenceMetaKey(commit_seq);
  if (!key.ok()) return key.status();
  std::string encoded;
  rocksdb::ReadOptions options;
  options.snapshot = snapshot.state_->snapshot;
  const rocksdb::Status got = store->db->Get(options, store->meta_cf,
                                              key.ValueOrDie(), &encoded);
  if (!got.ok()) return FromRocksDb(got, "read scan sequence");
  auto record = DecodeSequenceRecord(encoded);
  if (!record.ok()) return record.status();
  if (record.ValueOrDie().commit_seq != commit_seq) {
    return Status::Corruption("scan sequence", "commit sequence disagrees with key");
  }
  return record;
}

StatusOr<FactEvent> FactStore::ReadExactFact(
    const StoreSnapshot& snapshot, const std::string& encoded_fact_key) const {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store || snapshot.state_ == nullptr || snapshot.state_->store != store) {
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
  rocksdb::ReadOptions options;
  options.snapshot = snapshot.state_->snapshot;
  const rocksdb::Status got = store->db->Get(options, store->facts_cf,
                                              encoded_fact_key, &encoded);
  if (!got.ok()) return FromRocksDb(got, "read scan fact");
  return DecodeFactValue(key.ValueOrDie().ref, key.ValueOrDie().valid_from,
                         key.ValueOrDie().commit_seq, encoded);
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
    const StoreCommitBatch& batch) {
  const auto key = internal::EncodePreparedCommitKey(batch.txn_id);
  if (!key.ok()) return key.status();
  if (options_.group_commit_max_batch_size > 1 &&
      options_.group_commit_window_us > 0) {
    return CommitGrouped(batch, key.ValueOrDie(), false);
  }
  return CommitDirect(batch, key.ValueOrDie(), false);
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
        store->facts_cf, store->meta_cf, &write_batch);
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
    const auto sequence = DecodeSequenceRecord(encoded_sequence);
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
      store->facts_cf, store->meta_cf, &write_batch);
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
      store->facts_cf, store->meta_cf, &write_batch);
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
  std::vector<internal::DecidedEpoch::PublishedCommit> publications;
  group.results.reserve(requests.size());
  publications.reserve(requests.size());
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
        store->facts_cf, store->meta_cf, write_batch.get());
    if (!appended.ok()) return appended;
    ++committed_count;
    group.results.emplace_back(StoreCommitResult{commit_seq, batch.system_hlc});
    publications.push_back(
        internal::DecidedEpoch::PublishedCommit{commit_seq, batch.mutations});
  }
  const auto validation_finished_at = std::chrono::steady_clock::now();
  group.validation_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          validation_finished_at - validation_started_at)
          .count());
  const auto assembly_started_at = validation_finished_at;
  if (committed_count == 0 && !has_durable_terminal) {
    group.assembly_us = 0;
    return internal::DecidedEpoch(
        base_visible_seq, base_visible_seq, 0, false, nullptr,
        std::move(group));
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
      std::move(group), std::move(publications));
}

// The caller holds FactStoreImpl::publisher_mutex. This stage intentionally
// performs no validation, sequence assignment, or encoding; ClaimBatchForWrite
// makes an already decided epoch a one-shot writer submission.
StatusOr<StoreCommittedGroupResult> WriteDecidedGroupLocked(
    FactStoreImpl* store, const FactStoreOptions& options,
    internal::DecidedEpoch* epoch, WalDurableCallback on_wal_durable,
    void* callback_context, std::atomic<bool>* wal_sync_critical) {
  if (epoch == nullptr) {
    return Status::InvalidArgument("commit", "missing decided epoch");
  }
  if (!epoch->requires_durable_write()) {
    return epoch->TakeGroupResult();
  }
  if (store->visible_seq != epoch->base_visible_seq()) {
    return Status::Conflict("commit", "decided epoch base is no longer current");
  }

  if (options.commit_prewrite_fault_injector_for_testing) {
    const Status injected = options.commit_prewrite_fault_injector_for_testing();
    if (!injected.ok()) {
      if (injected.IsIndeterminate()) store->recovery_required = true;
      return injected;
    }
  }
  rocksdb::CedarEpochOptions kernel_options;
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
    for (const internal::DecidedEpoch::PublishedCommit& publication :
         epoch->publications()) {
      PublishRecentFactWriteIndex(store, publication.mutations,
                                  publication.commit_seq);
      PublishValidationCache(store, publication.mutations,
                             publication.commit_seq);
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
  std::vector<internal::DecidedEpoch::PublishedCommit> publications;
  group.results.reserve(requests.size());
  publications.reserve(requests.size());
  uint64_t committed_count = 0;
  for (const StoreCommitGroupRequest& request : requests) {
    const StoreCommitBatch& batch = request.batch;
    if (!batch.fresh_transaction_id ||
        !internal::CanUseAppendFastPath(batch)) {
      return Status::InvalidArgument(
          "commit",
          "independent append decision requires a fresh blind put batch");
    }
    for (const SnapshotWriteDependency& dependency :
         batch.snapshot_write_dependencies) {
      if (dependency.snapshot_seq != required_snapshot_seq ||
          dependency.predecessor.has_value() ||
          dependency.successor.has_value()) {
        return Status::Conflict(
            "commit",
            "independent append snapshot is not the stable predecessor");
      }
    }
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
        store->facts_cf, store->meta_cf, write_batch.get());
    if (!appended.ok()) return appended;
    ++committed_count;
    group.results.emplace_back(StoreCommitResult{commit_seq, batch.system_hlc});
    publications.push_back(
        internal::DecidedEpoch::PublishedCommit{commit_seq, batch.mutations});
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
  return std::make_unique<internal::DecidedEpoch>(
      base_visible_seq,
      CommitSeq{base_visible_seq.value + committed_count}, committed_count,
      true, std::move(write_batch), std::move(group), std::move(publications));
}

StatusOr<StoreCommittedGroupResult> FactStore::WriteDecidedGroup(
    internal::DecidedEpoch* epoch, WalDurableCallback on_wal_durable,
    void* callback_context, std::atomic<bool>* wal_sync_critical) {
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
  return WriteDecidedGroupLocked(store.get(), options_, epoch, on_wal_durable,
                                 callback_context, wal_sync_critical);
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
                                 on_wal_durable, callback_context,
                                 wal_sync_critical);
}

StatusOr<TxnId> FactStore::AllocateTransactionId() {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("transaction", "store is not open");
  }
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

    // Lease extension is rare (one synchronized metadata write per 4096
    // transactions). Keep the existing recovery and allocator ordering for
    // this slow path while the common path remains lock-free.
    std::lock_guard<std::mutex> publisher_lock(store->publisher_mutex);
    if (store->recovery_required) {
      return Status::RecoveryRequired(
          "transaction", "reopen required after indeterminate write");
    }
    const uint64_t lease_start = store->transaction_allocator.next_id;
    if (kDefaultIdLeaseSize >
        std::numeric_limits<uint64_t>::max() - lease_start) {
      return Status::ResourceExhausted("transaction", "transaction ID space exhausted");
    }
    const IdAllocatorState updated{IdKind::kTransaction,
                                   lease_start + kDefaultIdLeaseSize};
    const auto encoded = EncodeIdAllocatorState(updated);
    if (!encoded.ok()) return encoded.status();
    rocksdb::WriteBatch batch;
    batch.Put(store->meta_cf, EncodeAllocatorMetaKey(IdKind::kTransaction),
              encoded.ValueOrDie());
    rocksdb::WriteOptions options;
    options.sync = true;
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
  const auto schemas = std::atomic_load(&store->property_schemas);
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
  std::atomic_store(&store->property_schemas, std::move(immutable_schemas));
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
  const auto schemas = std::atomic_load(&store->property_schemas);
  return LookupPropertyInSchemas(*schemas, property_id,
                                 schema_epoch);
}

StatusOr<std::optional<PropertyDefinition>> FactStore::LookupProperty(
    const StoreSnapshot& snapshot, PropertyId property_id,
    uint32_t schema_epoch) const {
  std::shared_ptr<FactStoreImpl> store;
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store || snapshot.state_ == nullptr || snapshot.state_->store != store) {
      return Status::InvalidArgument("property definition",
                                     "snapshot belongs to another store");
    }
  }
  return LookupPropertyInSchemas(*snapshot.state_->property_schemas, property_id,
                                 schema_epoch);
}

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

  const FactRef identity_ref(edge.home_part_id, FactFamily::kEdgeIdentity,
                             PropertyId{}, edge.edge_id.value);
  const auto identity_fact = Read(snapshot, identity_ref, ValidTime{0});
  if (!identity_fact.ok()) return identity_fact.status();
  if (!identity_fact.ValueOrDie().has_value() ||
      !identity_fact.ValueOrDie()->edge_identity.has_value()) {
    return Status::Corruption("edge identity", "edge state lacks an authoritative identity fact");
  }
  const EdgeIdentity& identity = *identity_fact.ValueOrDie()->edge_identity;
  if (identity.edge_ref() != edge) {
    return Status::Corruption("edge identity", "identity fact disagrees with key");
  }
  return std::optional<EdgeIdentity>{identity};
}

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
  if (oldest_readable == store->oldest_readable_seq) return Status::OK();
  std::lock_guard<std::mutex> snapshot_lock(store->snapshot_mutex);
  if (!store->active_snapshot_sequences.empty() &&
      *store->active_snapshot_sequences.begin() < oldest_readable.value) {
    return Status::SnapshotPinned("vacuum", "active snapshot predates target");
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
    const Status status = FromMetadataWriteFailure(written);
    if (status.IsIndeterminate()) store->recovery_required = true;
    return status;
  }
  store->oldest_readable_seq = oldest_readable;
  store->published_oldest_readable_seq.store(oldest_readable.value,
                                             std::memory_order_release);
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
  return Status::OK();
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
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) {
      return Status::ShutdownInProgress("maintenance", "store is not open");
    }
  }

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
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("pressure", "store is not open");
  }
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
  const uint64_t all_memtable_bytes =
      maintenance.total_immutable_memtable_bytes >
              UINT64_MAX - maintenance.total_active_memtable_bytes
          ? UINT64_MAX
          : maintenance.total_immutable_memtable_bytes +
                maintenance.total_active_memtable_bytes;
  const auto resolved = internal::ResolveStorageProfile(options_);
  if (resolved.ok() && resolved.ValueOrDie().facts_write_buffer_bytes != 0) {
    sample.immutable_memtable_percent = std::min<uint64_t>(
        100, all_memtable_bytes * 100 /
                 resolved.ValueOrDie().facts_write_buffer_bytes);
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
  {
    std::lock_guard<std::mutex> lifecycle_lock(lifecycle_mutex_);
    store = impl_;
    if (!store) return Status::InvalidArgument("metrics", "store is not open");
  }
  ValidationCacheMetrics metrics;
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
