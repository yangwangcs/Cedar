// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#include "cedar/transaction/transaction_coordinator.h"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <map>
#include <set>
#include <utility>

#include "cedar/storage/sst_flush.h"
#include "cedar/storage/sst_compaction.h"
#include "cedar/blob/blob_gc.h"
#include "cedar/cache/cache_manager.h"
#include "cedar/columnar/sst.h"
#include "cedar/index/index_sidecar.h"
#include "cedar/columnar/temporal_read_merger.h"
#include "cedar/runtime/io_governor.h"
#include "cedar/runtime/resource_profile.h"
#include "cedar/runtime/work_execution_service.h"
#include "cedar/runtime/work_scheduler.h"
#include "cedar/storage/storage_layout.h"
#include "cedar/tcypher/executor.h"
#include "cedar/transaction/database_format.h"

namespace cedar {
namespace {

constexpr uint64_t kBlobSegmentTargetBytes = 256ULL * 1024 * 1024;
constexpr uint64_t kDiskSafetyReserveBytes = 1024ULL * 1024 * 1024;

struct CommittedInterval {
  uint64_t valid_from;
  uint64_t valid_to;
  uint64_t commit_seq;
};

constexpr uint64_t kInfiniteValidTime = UINT64_MAX;

uint64_t SaturatingAdd(uint64_t left, uint64_t right) {
  return left > UINT64_MAX - right ? UINT64_MAX : left + right;
}

uint64_t ElapsedNs(std::chrono::steady_clock::time_point start) {
  const auto elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>(
      std::chrono::steady_clock::now() - start).count();
  return elapsed <= 0 ? 0 : static_cast<uint64_t>(elapsed);
}

std::string MeasurementReason(const Status& status) {
  if (status.IsInvalidArgument()) return "invalid_argument";
  if (status.IsSchemaMismatch()) return "schema_mismatch";
  if (status.IsConflict()) return "serialization_conflict";
  if (status.IsWriteStalled()) return "write_stalled";
  if (status.IsResourceExhausted()) return "resource_exhausted";
  if (status.IsRecoveryRequired()) return "recovery_required";
  if (status.IsIOError()) return "io_error";
  if (status.IsCorruption() || status.IsBlobCorruption()) return "corruption";
  if (status.IsNotSupportedError()) return "not_supported";
  return "other";
}

StatusOr<ResourceProfile> AddMaintenanceResources(
    const ResourceProfile& left, const ResourceProfile& right) {
  const auto add = [](uint64_t a, uint64_t b,
                      const char* dimension) -> StatusOr<uint64_t> {
    if (b > UINT64_MAX - a) {
      return Status::InvalidArgument(
          "maintenance estimate", std::string(dimension) + " overflow");
    }
    return a + b;
  };
  ResourceProfile result;
#define CEDAR_ADD_RESOURCE(field)                                      \
  do {                                                                 \
    const auto value = add(left.field, right.field, #field);           \
    if (!value.ok()) return value.status();                            \
    result.field = value.ValueOrDie();                                 \
  } while (false)
  CEDAR_ADD_RESOURCE(memory_bytes);
  CEDAR_ADD_RESOURCE(io_tokens);
  CEDAR_ADD_RESOURCE(temporary_bytes);
  CEDAR_ADD_RESOURCE(sequential_read_bytes);
  CEDAR_ADD_RESOURCE(random_read_ops);
  CEDAR_ADD_RESOURCE(write_bytes);
  CEDAR_ADD_RESOURCE(metadata_ops);
#undef CEDAR_ADD_RESOURCE
  result.descriptors = std::max(left.descriptors, right.descriptors);
  result.cpu_slots = std::max(left.cpu_slots, right.cpu_slots);
  return result;
}

uint64_t LogicalEventBytes(const PendingEvent& event) {
  // Canonical logical identity + valid/system time + schema/op/value framing.
  uint64_t bytes = 2 + 8 + 8 + 2 + 2 + 8 + 8 + 8 + 4 + 1 + 1;
  if (event.operation == TemporalOperation::kDelete) return bytes;
  const uint64_t value_bytes = event.blob_ref.has_value()
      ? SaturatingAdd(5, event.blob_ref->raw_length)
      : static_cast<uint64_t>(event.value.Encode().size());
  return SaturatingAdd(bytes, value_bytes);
}

uint64_t LogicalEventBytes(const TemporalEvent& event) {
  uint64_t bytes = 2 + 8 + 8 + 2 + 2 + 8 + 8 + 8 + 4 + 1 + 1;
  if (event.is_delete()) return bytes;
  const uint64_t value_bytes = event.is_blob_reference()
      ? SaturatingAdd(5, event.blob_ref()->raw_length)
      : static_cast<uint64_t>(event.value().Encode().size());
  return SaturatingAdd(bytes, value_bytes);
}

uint64_t LogicalEventBytes(const std::vector<PendingEvent>& events) {
  uint64_t bytes = 0;
  for (const PendingEvent& event : events) {
    bytes = SaturatingAdd(bytes, LogicalEventBytes(event));
  }
  return bytes;
}

uint64_t LogicalEventBytes(const std::vector<TemporalEvent>& events) {
  uint64_t bytes = 0;
  for (const TemporalEvent& event : events) {
    bytes = SaturatingAdd(bytes, LogicalEventBytes(event));
  }
  return bytes;
}

void AtomicSaturatingAdd(std::atomic<uint64_t>* value, uint64_t delta) {
  uint64_t current = value->load(std::memory_order_relaxed);
  for (;;) {
    const uint64_t next = SaturatingAdd(current, delta);
    if (value->compare_exchange_weak(current, next, std::memory_order_relaxed,
                                     std::memory_order_relaxed)) {
      return;
    }
  }
}

void AtomicMax(std::atomic<uint64_t>* value, uint64_t candidate) {
  uint64_t current = value->load(std::memory_order_relaxed);
  while (current < candidate &&
         !value->compare_exchange_weak(current, candidate,
                                       std::memory_order_relaxed,
                                       std::memory_order_relaxed)) {
  }
}

uint64_t EstimateFlushWriteBytes(const std::vector<TemporalEvent>& events) {
  uint64_t bytes = 0;
  for (const TemporalEvent& event : events) {
    constexpr uint64_t kEventOverheadBytes = 96;
    const uint64_t value_bytes = event.is_blob_reference()
        ? 64 : static_cast<uint64_t>(event.value().Encode().size());
    bytes = SaturatingAdd(bytes, SaturatingAdd(kEventOverheadBytes, value_bytes));
  }
  return bytes;
}

using CompactionDebtKey =
    std::tuple<std::string, uint8_t, uint16_t, uint32_t, uint8_t, uint16_t,
               uint8_t, uint8_t, uint32_t, uint16_t>;

std::optional<CompactionDebtKey> MakeCompactionDebtKey(const SstFileMeta& file) {
  const std::filesystem::path relative(file.relative_path);
  const std::filesystem::path shard = relative.parent_path().parent_path();
  if (relative.parent_path().filename() != "sst" || shard.parent_path().filename() != "shards") {
    return std::nullopt;
  }
  return CompactionDebtKey{shard.filename().string(), static_cast<uint8_t>(file.partition.entity_type),
                           file.partition.column_id, file.partition.schema_epoch,
                           static_cast<uint8_t>(file.partition.physical_type),
                           file.partition.edge_type,
                           static_cast<uint8_t>(file.partition.compression_id),
                           static_cast<uint8_t>(file.partition.key_kind),
                           file.partition.storage_shard_id,
                           file.partition.logical_type_id};
}

uint64_t CompactionDebtBytes(const VersionSnapshot& snapshot, size_t minimum_files) {
  std::map<CompactionDebtKey, std::pair<size_t, uint64_t>> groups;
  for (const SstFileMeta& file : snapshot.files) {
    const auto key = MakeCompactionDebtKey(file);
    if (!key.has_value()) continue;
    auto& group = groups[*key];
    ++group.first;
    group.second = SaturatingAdd(group.second, file.file_size);
  }
  uint64_t debt = 0;
  for (const auto& group : groups) {
    if (group.second.first >= minimum_files) debt = SaturatingAdd(debt, group.second.second);
  }
  return debt;
}

uint64_t MonotonicNowNs() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now().time_since_epoch()).count());
}

std::vector<CommittedInterval> CurrentIntervalsForKey(const std::vector<TemporalEvent>& events,
                                                       const LogicalKey& key) {
  std::vector<TemporalEvent> candidates;
  for (const TemporalEvent& event : events) {
    if (event.logical_key() == key) candidates.push_back(event);
  }
  std::sort(candidates.begin(), candidates.end(), [](const TemporalEvent& left,
                                                       const TemporalEvent& right) {
    if (left.valid_from() != right.valid_from()) return left.valid_from() < right.valid_from();
    return left.commit_seq() < right.commit_seq();
  });
  std::vector<TemporalEvent> selected;
  for (const TemporalEvent& event : candidates) {
    if (!selected.empty() && selected.back().valid_from() == event.valid_from()) {
      selected.back() = event;
    } else {
      selected.push_back(event);
    }
  }
  std::vector<CommittedInterval> intervals;
  intervals.reserve(selected.size());
  for (size_t index = 0; index < selected.size(); ++index) {
    intervals.push_back(CommittedInterval{selected[index].valid_from(),
        index + 1 == selected.size() ? kInfiniteValidTime : selected[index + 1].valid_from(),
        selected[index].commit_seq()});
  }
  return intervals;
}

bool IntervalsOverlap(uint64_t left_from, uint64_t left_to,
                      uint64_t right_from, uint64_t right_to) {
  return left_from < right_to && right_from < left_to;
}

TransactionCoordinator::StrictReadPoint BuildStrictReadPoint(
    const std::vector<TemporalEvent>& committed, const LogicalKey& key,
    uint64_t valid_time, uint64_t snapshot_seq) {
  TransactionCoordinator::StrictReadPoint point{key, valid_time};
  point.observed_event = ResolveVisibleEvent(
      committed, key, valid_time, snapshot_seq);
  if (point.observed_event.has_value()) {
    point.predecessor_fence = point.observed_event->valid_from();
  }
  for (const TemporalEvent& event : committed) {
    if (event.logical_key() != key || event.commit_seq() > snapshot_seq ||
        event.valid_from() <= valid_time) {
      continue;
    }
    if (!point.successor_fence.has_value() ||
        event.valid_from() < *point.successor_fence) {
      point.successor_fence = event.valid_from();
    }
  }
  return point;
}

bool SameObservedEvent(const std::optional<TemporalEvent>& left,
                       const std::optional<TemporalEvent>& right) {
  if (left.has_value() != right.has_value()) return false;
  return !left.has_value() || SameTemporalEventContent(*left, *right);
}

bool SamePartition(const BlockPartition& left, const BlockPartition& right) {
  return left.entity_type == right.entity_type && left.column_id == right.column_id &&
         left.schema_epoch == right.schema_epoch && left.physical_type == right.physical_type &&
         left.edge_type == right.edge_type && left.compression_id == right.compression_id &&
         left.key_kind == right.key_kind &&
         left.storage_shard_id == right.storage_shard_id &&
         left.logical_type_id == right.logical_type_id;
}

Status ValidateSnapshotWrites(const std::vector<TemporalEvent>& committed,
                              const std::vector<PendingEvent>& pending,
                              uint64_t snapshot_seq) {
  std::map<LogicalKey, std::vector<CommittedInterval>> intervals;
  for (const PendingEvent& event : pending) {
    if (intervals.count(event.logical_key) == 0) {
      intervals.emplace(event.logical_key, CurrentIntervalsForKey(committed, event.logical_key));
    }
  }
  for (const PendingEvent& event : pending) {
    const auto found = intervals.find(event.logical_key);
    uint64_t valid_to = kInfiniteValidTime;
    for (const CommittedInterval& interval : found->second) {
      if (interval.valid_from > event.valid_from) {
        valid_to = interval.valid_from;
        break;
      }
    }
    for (const CommittedInterval& interval : found->second) {
      if (interval.commit_seq > snapshot_seq &&
          IntervalsOverlap(event.valid_from, valid_to, interval.valid_from, interval.valid_to)) {
        return Status::Conflict("transaction", "snapshot write conflicts with a later overlapping event");
      }
    }
  }
  return Status::OK();
}

Status ValidateStrictReads(const std::vector<TemporalEvent>& committed,
                           const std::vector<TransactionCoordinator::StrictReadPoint>& reads,
                           uint64_t snapshot_seq) {
  for (const TransactionCoordinator::StrictReadPoint& read : reads) {
    const auto at_snapshot = BuildStrictReadPoint(
        committed, read.logical_key, read.valid_time, snapshot_seq);
    const bool has_captured_identity = read.observed_event.has_value() ||
        read.predecessor_fence.has_value() || read.successor_fence.has_value();
    if (has_captured_identity &&
        (!SameObservedEvent(read.observed_event, at_snapshot.observed_event) ||
         read.predecessor_fence != at_snapshot.predecessor_fence ||
         read.successor_fence != at_snapshot.successor_fence)) {
      return Status::Conflict(
          "transaction", "strict read identity does not match its snapshot");
    }
    const auto current = ResolveVisibleEvent(committed, read.logical_key, read.valid_time,
                                             UINT64_MAX);
    if (!SameObservedEvent(at_snapshot.observed_event, current)) {
      return Status::Conflict("transaction", "strict read was changed after its snapshot");
    }
  }
  return Status::OK();
}

}  // namespace

uint64_t BenchmarkStorageStats::physical_durable_bytes_written() const {
  uint64_t total = 0;
  for (uint64_t component : {
           wal_bytes_written, decision_log_bytes_written,
           sst_flush_bytes_written, compaction_bytes_written,
           blob_bytes_written, manifest_bytes_written}) {
    total = SaturatingAdd(total, component);
  }
  return total;
}

TransactionCoordinator::TransactionCoordinator(std::string db_path,
                                               uint32_t shard_count,
                                               uint64_t hash_seed)
    : db_path_(std::move(db_path)),
      shard_directory_(shard_count, hash_seed),
      blob_store_(db_path_ + "/blobs", shard_count),
      decision_log_(db_path_ + "/decision/DECISION"),
      commit_timeline_(db_path_ + "/manifest/COMMIT-TIMELINE"),
      version_set_(db_path_ + "/" + storage_layout::kManifestRelativePath),
      stats_snapshot_store_(db_path_ + "/manifest/STATS"),
      vertex_id_allocator_(db_path_ + "/manifest/VERTEX-ID"),
      transaction_id_allocator_(db_path_ + "/manifest/TXN-ID") {
  for (uint32_t shard = 0; shard < shard_count; ++shard) {
    prepare_logs_.push_back(std::make_unique<ShardPrepareLog>(
        db_path_ + "/shards/" + std::to_string(shard) + "/wal/PREPARE", shard));
    storage_shards_.push_back(std::make_unique<StorageShard>(shard));
  }
}

TransactionCoordinator::~TransactionCoordinator() {
  if (owned_work_execution_service_) {
    owned_work_execution_service_->Stop().IgnoreError();
  }
}

Status TransactionCoordinator::SetWorkExecutionService(
    WorkExecutionService* execution_service) {
  if (execution_service == nullptr) {
    return Status::InvalidArgument(
        "work execution service", "execution service must not be null");
  }
  if (opened_) {
    return Status::InvalidArgument(
        "work execution service", "execution service must be bound before open");
  }
  if (work_execution_service_ != nullptr &&
      work_execution_service_ != execution_service) {
    return Status::InvalidArgument(
        "work execution service", "execution service is already bound");
  }
  if (work_execution_service_ == execution_service) return Status::OK();
  const Status configured = maintenance_executor_.Configure(
      execution_service, resource_governor_, io_governor_);
  if (!configured.ok()) return configured;
  work_execution_service_ = execution_service;
  return Status::OK();
}

Status TransactionCoordinator::EnsureWorkExecutionService() {
  if (work_execution_service_ != nullptr) return Status::OK();
  owned_work_scheduler_ = std::make_shared<WorkScheduler>();
  const size_t worker_count = static_cast<size_t>(std::max<uint32_t>(
      1, std::min<uint32_t>(shard_directory_.shard_count(), 4)));
  owned_work_execution_service_ = std::make_unique<WorkExecutionService>(
      owned_work_scheduler_, worker_count);
  if (resource_governor_ != nullptr) {
    const Status configured =
        owned_work_execution_service_->ConfigureResourceGovernor(
            resource_governor_);
    if (!configured.ok()) {
      owned_work_execution_service_.reset();
      owned_work_scheduler_.reset();
      return configured;
    }
  }
  const Status started = owned_work_execution_service_->Start();
  if (!started.ok()) {
    owned_work_execution_service_.reset();
    owned_work_scheduler_.reset();
    return started;
  }
  work_execution_service_ = owned_work_execution_service_.get();
  const Status configured = ConfigureMaintenanceExecutor();
  if (!configured.ok()) {
    work_execution_service_ = nullptr;
    owned_work_execution_service_->Stop().IgnoreError();
    owned_work_execution_service_.reset();
    owned_work_scheduler_.reset();
    maintenance_executor_
        .Configure(nullptr, resource_governor_, io_governor_)
        .IgnoreError();
    return configured;
  }
  return Status::OK();
}

Status TransactionCoordinator::RunCommitCriticalTasks(
    std::vector<std::function<Status()>> tasks) {
  if (tasks.empty()) return Status::OK();
  if (work_execution_service_ == nullptr) {
    return Status::InvalidArgument(
        "transaction", "commit-critical execution service is unavailable");
  }
  std::vector<WorkTaskHandle> handles;
  handles.reserve(tasks.size());
  Status submission_status = Status::OK();
  for (std::function<Status()>& task : tasks) {
    auto submitted = work_execution_service_->Submit(
        WorkTaskRequest{WorkClass::kCommitCritical, {}, true, 0},
        std::move(task));
    if (!submitted.ok()) {
      submission_status = submitted.status();
      break;
    }
    handles.push_back(std::move(submitted).ConsumeValueOrDie());
  }
  if (!submission_status.ok()) {
    for (const WorkTaskHandle& handle : handles) {
      work_execution_service_->Cancel(handle.id());
    }
  }
  Status first_failure = submission_status;
  for (const WorkTaskHandle& handle : handles) {
    const Status completed = work_execution_service_->WaitForTask(handle);
    if (first_failure.ok() && !completed.ok()) first_failure = completed;
  }
  return first_failure;
}

std::vector<StorageShard::WriteReservation>
TransactionCoordinator::BuildWriteIntervals(
    const std::vector<TemporalEvent>& committed,
    const std::vector<PendingEvent>& pending) const {
  std::map<LogicalKey, std::set<uint64_t>> boundaries;
  for (const TemporalEvent& event : committed) {
    boundaries[event.logical_key()].insert(event.valid_from());
  }
  for (const PendingEvent& event : pending) {
    boundaries[event.logical_key].insert(event.valid_from);
  }

  std::vector<StorageShard::WriteReservation> intervals;
  intervals.reserve(pending.size());
  for (const PendingEvent& event : pending) {
    const auto& key_boundaries = boundaries[event.logical_key];
    const auto successor = key_boundaries.upper_bound(event.valid_from);
    intervals.push_back(StorageShard::WriteReservation{
        event.logical_key, event.valid_from,
        successor == key_boundaries.end() ? kInfiniteValidTime : *successor});
  }
  return intervals;
}

Status TransactionCoordinator::Open() {
  opened_ = false;
  const Status status = EnsureWorkExecutionService();
  if (!status.ok()) return status;
  auto submitted = work_execution_service_->Submit(
      WorkTaskRequest{
          WorkClass::kRecovery, ResourceProfile{0, 0, 0, 0, 1}, false, 0},
      [this] { return OpenInternal(); });
  if (!submitted.ok()) return submitted.status();
  return work_execution_service_->WaitForTask(submitted.ValueOrDie());
}

Status TransactionCoordinator::OpenInternal() {
  std::function<void()> recovery_hook;
  {
    std::lock_guard<std::mutex> lock(recovery_mutex_);
    recovery_hook = recovery_execution_hook_;
  }
  if (recovery_hook) recovery_hook();
  Status status;
  for (const auto& shard : storage_shards_) shard->ClearReservationsForRecovery();
  status = CreateOrValidateDatabaseFormat(
      db_path_, MakeDatabaseFormat(shard_directory_.shard_count(),
                                     shard_directory_.hash_seed()));
  if (!status.ok()) return status;
  const std::filesystem::path legacy_schema =
      std::filesystem::path(db_path_) / "manifest" / "SCHEMA";
  std::error_code legacy_error;
  const bool has_legacy_schema =
      std::filesystem::exists(legacy_schema, legacy_error);
  if (legacy_error) {
    return Status::IOError(legacy_schema.string(), legacy_error.message());
  }
  if (has_legacy_schema) {
    return Status::NotSupported(
        "schema", "standalone SCHEMA catalog is not supported");
  }
  status = version_set_.Open();
  if (!status.ok()) return status;
  status = schema_registry_.Install(version_set_.Snapshot()->schemas);
  if (!status.ok()) {
    return Status::Corruption("manifest", status.ToString());
  }
  status = blob_store_.Open();
  if (!status.ok()) return status;
  status = vertex_id_allocator_.Open();
  if (!status.ok()) return status;
  status = transaction_id_allocator_.Open();
  if (!status.ok()) return status;
  status = CleanupOrphanSsts();
  if (!status.ok()) return status;
  status = ReconcileBlobSegments();
  if (!status.ok()) return status;
  status = stats_snapshot_store_.Open();
  if (!status.ok()) return status;
  status = LoadCheckpointOutcomes();
  if (!status.ok()) return status;
  status = RestorePublishedSstEvents();
  if (!status.ok()) return status;
  blob_reference_catalog_.RebuildLiveSstSources(*version_set_.Snapshot());
  for (const SstFileMeta& file : version_set_.Snapshot()->files) {
    next_sst_file_number_ = std::max(next_sst_file_number_, file.file_number + 1);
  }
  for (const auto& log : prepare_logs_) {
    status = log->Open();
    if (!status.ok()) return status;
  }
  status = decision_log_.Open(version_set_.Snapshot()->checkpoint.checkpoint_seq);
  if (!status.ok()) return status;
  status = commit_timeline_.Open();
  if (!status.ok()) return status;
  status = commit_timeline_.RestoreFromOutcomes(checkpoint_outcomes_);
  if (!status.ok()) return status;
  status = commit_timeline_.RestoreFromDecisions(decision_log_.commits());
  if (!status.ok()) return status;

  visible_prefix_.RestorePersistedPrefix(version_set_.Snapshot()->checkpoint.checkpoint_seq);

  std::vector<ShardPrepareLog*> logs;
  for (const auto& log : prepare_logs_) logs.push_back(log.get());
  std::vector<RecoveredTransaction> recovered;
  status = RecoverCommittedTransactions(decision_log_, logs, &recovered);
  if (!status.ok()) return status;
  for (const CommitDecision& decision : decision_log_.commits()) {
    status = InstallDecision(decision);
    if (!status.ok()) return status;
  }
  opened_ = true;
  recovery_required_.store(false, std::memory_order_release);
  // Index fragments and statistics are rebuildable accelerators. Reconcile
  // them after recovery; any failure leaves the Manifest-backed base path
  // authoritative and does not prevent the database from opening.
  const std::vector<SstFileMeta> recovered_files = version_set_.Snapshot()->files;
  ScheduleStatsMerge(recovered_files).IgnoreError();
  ScheduleIndexBuild(recovered_files).IgnoreError();
  return Status::OK();
}

Status TransactionCoordinator::CleanupOrphanSsts() {
  std::set<std::string> live_paths;
  for (const SstFileMeta& file : version_set_.Snapshot()->files) {
    live_paths.insert(file.relative_path);
  }
  std::set<std::string> live_sidecars;
  for (const IndexFragment& fragment : version_set_.Snapshot()->index_fragments) {
    live_sidecars.insert(fragment.relative_path);
  }
  const std::filesystem::path shards = std::filesystem::path(db_path_) / "shards";
  std::error_code error;
  const bool shards_exist = std::filesystem::exists(shards, error);
  if (error) return Status::IOError(shards.string(), error.message());
  if (shards_exist) {
    for (std::filesystem::directory_iterator shard(shards, error), end;
         !error && shard != end; shard.increment(error)) {
      const std::filesystem::path sst_directory = shard->path() / "sst";
      if (!std::filesystem::exists(sst_directory, error)) {
        if (error) break;
        continue;
      }
      for (std::filesystem::directory_iterator file(sst_directory, error), file_end;
           !error && file != file_end; file.increment(error)) {
        if (!file->is_regular_file(error)) {
          if (error) break;
          continue;
        }
        const std::string filename = file->path().filename().string();
        const bool old_incomplete = filename.size() > 9 &&
            filename.compare(filename.size() - 9, 9,
                            std::string(storage_layout::kOldSstExtension) +
                                storage_layout::kTemporarySuffix) == 0;
        if (old_incomplete) {
          return Status::NotSupported("orphan SST cleanup",
                                      "old .sst2 temporary layout is not supported");
        }
        const bool incomplete = filename.size() > 8 &&
            filename.compare(filename.size() - 8, 8,
                            std::string(storage_layout::kSstExtension) +
                                storage_layout::kTemporarySuffix) == 0;
        if (incomplete) {
          std::filesystem::remove(file->path(), error);
          if (error) break;
          continue;
        }
        if (file->path().extension() == storage_layout::kOldSstExtension) {
          return Status::NotSupported("orphan SST cleanup",
                                      "old .sst2 layout is not supported");
        }
        if (file->path().extension() != storage_layout::kSstExtension) continue;
        const std::string relative =
            file->path().lexically_relative(db_path_).generic_string();
        if (live_paths.count(relative) != 0) continue;
        std::filesystem::remove(file->path(), error);
        if (error) break;
      }
    }
  }
  if (error) return Status::IOError("orphan SST cleanup", error.message());
  const std::filesystem::path indexes = std::filesystem::path(db_path_) / "indexes";
  if (!std::filesystem::exists(indexes, error)) {
    return error ? Status::IOError(indexes.string(), error.message()) : Status::OK();
  }
  for (std::filesystem::recursive_directory_iterator file(indexes, error), end;
       !error && file != end; file.increment(error)) {
    if (!file->is_regular_file(error)) {
      if (error) break;
      continue;
    }
    const std::string filename = file->path().filename().string();
    const bool old_incomplete = filename.size() > 9 &&
        filename.compare(filename.size() - 9, 9,
                        std::string(storage_layout::kOldIndexExtension) +
                            storage_layout::kTemporarySuffix) == 0;
    if (old_incomplete) {
      return Status::NotSupported("orphan index cleanup",
                                  "old .idx1 temporary layout is not supported");
    }
    const bool incomplete = filename.size() > 8 &&
        filename.compare(filename.size() - 8, 8,
                        std::string(storage_layout::kIndexExtension) +
                            storage_layout::kTemporarySuffix) == 0;
    if (incomplete) {
      std::filesystem::remove(file->path(), error);
      if (error) break;
      continue;
    }
    if (file->path().extension() == storage_layout::kOldIndexExtension) {
      return Status::NotSupported("orphan index cleanup",
                                  "old .idx1 layout is not supported");
    }
    if (file->path().extension() != storage_layout::kIndexExtension) continue;
    const std::string relative = file->path().lexically_relative(db_path_).generic_string();
    if (live_sidecars.count(relative) != 0) continue;
    std::filesystem::remove(file->path(), error);
    if (error) break;
  }
  return error ? Status::IOError("orphan SST cleanup", error.message()) : Status::OK();
}

Status TransactionCoordinator::LoadCheckpointOutcomes() {
  const DurableCheckpoint& checkpoint = version_set_.Snapshot()->checkpoint;
  checkpoint_outcomes_.clear();
  if (checkpoint.checkpoint_seq == 0) return Status::OK();
  if (checkpoint.wal_safe_lsns.size() != prepare_logs_.size()) {
    return Status::Corruption("checkpoint", "per-shard WAL safe positions are incomplete");
  }
  const std::filesystem::path relative(checkpoint.outcome_index_relative_path);
  if (relative.empty() || relative.is_absolute() ||
      std::find(relative.begin(), relative.end(), std::filesystem::path("..")) != relative.end()) {
    return Status::Corruption("checkpoint", "invalid outcome index path");
  }
  const auto outcomes = ReadTransactionOutcomeIndex(
      db_path_ + "/" + checkpoint.outcome_index_relative_path,
      checkpoint.outcome_index_checksum, checkpoint.checkpoint_seq);
  if (!outcomes.ok()) return outcomes.status();
  checkpoint_outcomes_ = outcomes.ValueOrDie();
  return Status::OK();
}

Status TransactionCoordinator::ReconcileBlobSegments() {
  const std::vector<BlobSegmentId> physical = blob_store_.SegmentIds();
  const std::shared_ptr<const VersionSnapshot> snapshot = version_set_.Snapshot();
  std::map<std::pair<uint32_t, uint64_t>, BlobSegmentMeta> manifest_segments;
  std::map<uint32_t, BlobSegmentMeta> manifest_active;
  for (const BlobSegmentMeta& segment : snapshot->blob_segments) {
    const std::pair<uint32_t, uint64_t> key{segment.shard_id, segment.segment_id};
    if (!manifest_segments.emplace(key, segment).second) {
      return Status::Corruption("blob manifest", "duplicate segment identity");
    }
    if (segment.active && !manifest_active.emplace(segment.shard_id, segment).second) {
      return Status::Corruption("blob manifest", "multiple active segments for shard");
    }
  }
  std::map<std::pair<uint32_t, uint64_t>, BlobSegmentId> physical_segments;
  for (const BlobSegmentId& segment : physical) {
    physical_segments.emplace(std::make_pair(segment.shard_id, segment.segment_id), segment);
  }
  for (const auto& segment : manifest_segments) {
    if (physical_segments.count(segment.first) == 0) {
      return Status::Corruption("blob manifest", "Manifest references missing segment");
    }
  }

  VersionEdit edit;
  for (const BlobSegmentId& segment : physical) {
    const std::pair<uint32_t, uint64_t> key{segment.shard_id, segment.segment_id};
    const std::string relative = "blobs/shard-" + std::to_string(segment.shard_id) +
        "/segment-" + std::to_string(segment.segment_id) + ".blob";
    const auto existing = manifest_segments.find(key);
    if (existing == manifest_segments.end()) {
      if (!segment.active) {
        const bool pending_reader_epoch = std::any_of(
            retired_blob_segments_.begin(), retired_blob_segments_.end(),
            [&segment](const RetiredBlobSegmentSet& retired) {
              return std::any_of(
                  retired.segments.begin(), retired.segments.end(),
                  [&segment](const BlobSegmentId& candidate) {
                    return candidate.shard_id == segment.shard_id &&
                           candidate.segment_id == segment.segment_id;
                  });
            });
        if (pending_reader_epoch) continue;
        const Status orphan_removed =
            TrackBlobMutation(blob_store_.DeleteRetiredSegments({segment}));
        if (!orphan_removed.ok()) {
          return Status::Corruption("blob manifest", "unmanifested sealed segment remains referenced");
        }
        continue;
      }
      const auto old_active = manifest_active.find(segment.shard_id);
      if (old_active != manifest_active.end()) {
        BlobSegmentMeta sealed = old_active->second;
        sealed.active = false;
        edit.blob_segment_updates.push_back(std::move(sealed));
      }
      edit.blob_segment_adds.push_back(
          BlobSegmentMeta{segment.shard_id, segment.segment_id, relative, segment.active});
      continue;
    }
    if (existing->second.relative_path != relative || existing->second.active != segment.active) {
      BlobSegmentMeta updated = existing->second;
      updated.relative_path = relative;
      updated.active = segment.active;
      edit.blob_segment_updates.push_back(std::move(updated));
    }
  }
  if (edit.blob_segment_adds.empty() && edit.blob_segment_updates.empty()) return Status::OK();
  return version_set_.ApplyEdit(edit);
}

Status TransactionCoordinator::EnsureBlobSegmentsManifested() {
  const Status provisioned =
      TrackBlobMutation(blob_store_.EnsureActiveSegments());
  if (!provisioned.ok()) return provisioned;
  return ReconcileBlobSegments();
}

Status TransactionCoordinator::TrackBlobMutation(Status status) {
  if (!status.ok() && blob_store_.requires_reopen()) {
    recovery_required_.store(true, std::memory_order_release);
  }
  return status;
}

Status TransactionCoordinator::RestorePublishedSstEvents() {
  published_commit_watermarks_.assign(
      storage_shards_.size(), version_set_.Snapshot()->checkpoint.checkpoint_seq);
  for (const SstFileMeta& file : version_set_.Snapshot()->files) {
    const std::string path = db_path_ + "/" + file.relative_path;
    std::error_code error;
    if (!std::filesystem::exists(path, error)) {
      if (error) return Status::IOError(path, error.message());
      return Status::Corruption("flush recovery", "Manifest references missing SST: " +
                                                       file.relative_path);
    }
    const uint64_t actual_size = std::filesystem::file_size(path, error);
    if (error) return Status::IOError(path, error.message());
    if (actual_size != file.file_size) {
      return Status::Corruption("flush recovery", "Manifest SST size does not match file: " +
                                                       file.relative_path);
    }
    const auto read_metadata = ReadSstFileMetadata(path);
    if (!read_metadata.ok()) return read_metadata.status();
    const SstMetadata& metadata = read_metadata.ValueOrDie();
    if (!SamePartition(metadata.partition, file.partition)) {
      return Status::Corruption("flush recovery", "Manifest SST partition does not match file: " +
                                                       file.relative_path);
    }
    if (metadata.blob_refs != file.blob_refs) {
      return Status::Corruption("flush recovery", "Manifest BlobRefSet does not match SST: " +
                                                       file.relative_path);
    }
    if (metadata.format != file.format || metadata.identity != file.identity ||
        metadata.statistics != file.statistics ||
        metadata.statistics_crc32c != file.statistics_crc32c) {
      return Status::Corruption(
          "flush recovery",
          "Manifest SST ownership metadata does not match file: " +
              file.relative_path);
    }
    const std::filesystem::path relative(file.relative_path);
    const std::filesystem::path shard = relative.parent_path().parent_path();
    uint32_t shard_id = 0;
    try {
      size_t parsed = 0;
      const std::string identity = shard.filename().string();
      const unsigned long value = std::stoul(identity, &parsed);
      if (parsed != identity.size() || value >= storage_shards_.size()) {
        return Status::Corruption("flush recovery", "SST shard identity is out of range");
      }
      shard_id = static_cast<uint32_t>(value);
    } catch (const std::exception&) {
      return Status::Corruption("flush recovery", "invalid SST shard identity");
    }
    published_commit_watermarks_[shard_id] = std::max(
        published_commit_watermarks_[shard_id], metadata.max_commit_seq);
  }
  return Status::OK();
}

Status TransactionCoordinator::RegisterColumn(const ColumnSchema& schema,
                                              ColumnSchema* registered) {
  std::lock_guard<std::mutex> publication_lock(flush_mutex_);
  const Status allowed = CheckMutationAllowed("schema registration");
  if (!allowed.ok()) return allowed;
  if (registered == nullptr) {
    return Status::InvalidArgument("schema", "missing registration output");
  }
  for (size_t attempt = 0; attempt < 16; ++attempt) {
    const std::shared_ptr<const VersionSnapshot> snapshot = version_set_.Snapshot();
    const Status installed = schema_registry_.Install(snapshot->schemas);
    if (!installed.ok()) return Status::Corruption("manifest", installed.ToString());
    ColumnSchema proposed;
    const Status proposal = schema_registry_.Propose(schema, &proposed);
    if (!proposal.ok()) return proposal;
    const auto existing = schema_registry_.Lookup(
        proposed.entity_type, proposed.column_id, proposed.schema_epoch);
    if (existing.has_value() &&
        SameColumnSchemaDefinition(*existing, proposed)) {
      *registered = *existing;
      return Status::OK();
    }
    VersionEdit edit;
    edit.schema_adds.push_back(proposed);
    edit.expected_generation = snapshot->generation;
    std::optional<ResourceLease> manifest_lease;
    const Status published = version_set_.ApplyEditWithAdmission(
        edit, [&](uint64_t projected_rewrite_bytes) {
          const ResourceProfile resources{
              0, 0, 1, 0, 0, 0, 0, projected_rewrite_bytes, 1};
          if (resource_governor_ != nullptr) {
            auto acquired = resource_governor_->Acquire(resources, false);
            if (!acquired.ok()) return acquired.status();
            manifest_lease.emplace(
                std::move(acquired).ConsumeValueOrDie());
          }
          if (io_governor_ != nullptr) {
            const Status io = io_governor_->TryAcquire(
                IoTokenRequest{0, 0, projected_rewrite_bytes, 1, false},
                MonotonicNowNs());
            if (!io.ok()) return io;
          }
          return Status::OK();
        });
    if (published.IsConflict()) continue;
    if (!published.ok()) {
      if (published.IsIndeterminate()) {
        recovery_required_.store(true, std::memory_order_release);
      }
      return published;
    }
    const Status refreshed =
        schema_registry_.Install(version_set_.Snapshot()->schemas);
    if (!refreshed.ok()) {
      recovery_required_.store(true, std::memory_order_release);
      return Status::Corruption("manifest", refreshed.ToString());
    }
    *registered = proposed;
    return Status::OK();
  }
  return Status::Conflict("schema", "Manifest generation kept changing");
}

Status TransactionCoordinator::CheckMutationAllowed(const char* context) const {
  if (!opened_) {
    return Status::InvalidArgument(context, "coordinator is not open");
  }
  if (recovery_required()) {
    return Status::RecoveryRequired(
        context, "reopen database before accepting another mutation");
  }
  return Status::OK();
}

Status TransactionCoordinator::RegisterIndex(IndexDefinition definition, uint64_t* index_id) {
  const Status allowed = CheckMutationAllowed("index registration");
  if (!allowed.ok()) return allowed;
  IndexCatalog catalog(&version_set_, &schema_registry_);
  return catalog.RegisterIndex(std::move(definition), index_id);
}

Status TransactionCoordinator::SetIndexState(uint64_t index_id, IndexState state) {
  std::lock_guard<std::mutex> flush_lock(flush_mutex_);
  const Status allowed = CheckMutationAllowed("index state");
  if (!allowed.ok()) return allowed;
  IndexCatalog catalog(&version_set_, &schema_registry_);
  const Status updated = catalog.SetIndexState(index_id, state);
  if (!updated.ok()) return updated;
  if (state != IndexState::kBuilding && state != IndexState::kActive) return Status::OK();
  // Existing SSTs predate this definition. Build their immutable sidecars as
  // best-effort maintenance; missing or corrupt fragments remain uncovered
  // and therefore force the query planner onto its base path.
  ScheduleIndexBuild(version_set_.Snapshot()->files).IgnoreError();
  return Status::OK();
}

Status TransactionCoordinator::RepairIndexes() {
  std::lock_guard<std::mutex> flush_lock(flush_mutex_);
  const Status allowed = CheckMutationAllowed("index repair");
  if (!allowed.ok()) return allowed;
  return ScheduleIndexBuild(version_set_.Snapshot()->files);
}

Status TransactionCoordinator::ReportIndexHealthEvent(
    uint64_t index_id, uint64_t source_sst_id, uint64_t catalog_generation,
    IndexHealthFailureClass failure_class) {
  if (index_id == 0 || source_sst_id == 0 || catalog_generation == 0) {
    return Status::InvalidArgument("index health", "invalid health event identity");
  }
  const IndexHealthEventKey key{
      index_id, source_sst_id, catalog_generation, failure_class};
  {
    std::lock_guard<std::mutex> lock(index_health_mutex_);
    const auto existing = index_health_events_.find(key);
    if (existing != index_health_events_.end() &&
        existing->second != IndexHealthRepairState::kFailed) {
      return Status::OK();
    }
    if (existing == index_health_events_.end() &&
        index_health_events_.size() >= kMaximumIndexHealthEvents) {
      const auto evictable = std::find_if(
          index_health_events_.begin(), index_health_events_.end(),
          [](const auto& event) {
            return event.second != IndexHealthRepairState::kPending;
          });
      if (evictable == index_health_events_.end()) {
        return Status::ResourceExhausted(
            "index health", "all bounded health-event slots are pending");
      }
      index_health_events_.erase(evictable);
    }
    index_health_events_[key] = IndexHealthRepairState::kPending;
    ++index_health_repair_schedule_count_;
  }

  Status repair = Status::OK();
  std::lock_guard<std::mutex> flush_lock(flush_mutex_);
  const auto snapshot = version_set_.Snapshot();
  const auto definition = std::find_if(
      snapshot->index_definitions.begin(), snapshot->index_definitions.end(),
      [index_id](const IndexDefinition& candidate) {
        return candidate.index_id == index_id;
      });
  const auto file = std::find_if(
      snapshot->files.begin(), snapshot->files.end(),
      [source_sst_id](const SstFileMeta& candidate) {
        return candidate.file_number == source_sst_id;
      });
  const auto fragment = std::find_if(
      snapshot->index_fragments.begin(), snapshot->index_fragments.end(),
      [index_id, source_sst_id](const IndexFragment& candidate) {
        return candidate.index_id == index_id &&
            candidate.source_sst_id == source_sst_id;
      });
  if (definition != snapshot->index_definitions.end() &&
      file != snapshot->files.end() &&
      (definition->state == IndexState::kBuilding ||
       definition->state == IndexState::kActive) &&
      (fragment == snapshot->index_fragments.end() ||
       fragment->catalog_generation <= catalog_generation)) {
    repair = ScheduleIndexBuild({*file});
  }

  {
    std::lock_guard<std::mutex> lock(index_health_mutex_);
    const auto event = index_health_events_.find(key);
    if (event != index_health_events_.end()) {
      event->second = repair.ok() ? IndexHealthRepairState::kCompleted
                                  : IndexHealthRepairState::kFailed;
    }
    if (!repair.ok()) ++index_health_repair_failure_count_;
  }
  return repair;
}

IndexHealthStats TransactionCoordinator::index_health_stats() const {
  std::lock_guard<std::mutex> lock(index_health_mutex_);
  uint64_t pending = 0;
  for (const auto& event : index_health_events_) {
    if (event.second == IndexHealthRepairState::kPending) ++pending;
  }
  return IndexHealthStats{
      static_cast<uint64_t>(index_health_events_.size()),
      index_health_repair_schedule_count_, index_health_repair_failure_count_,
      pending};
}

Status TransactionCoordinator::DropIndex(uint64_t index_id) {
  const Status allowed = CheckMutationAllowed("index drop");
  if (!allowed.ok()) return allowed;
  std::lock_guard<std::mutex> flush_lock(flush_mutex_);
  if (!opened_) return Status::InvalidArgument("index drop", "coordinator is not open");
  const std::shared_ptr<const VersionSnapshot> input_snapshot = version_set_.Snapshot();
  const auto definition = std::find_if(input_snapshot->index_definitions.begin(),
                                       input_snapshot->index_definitions.end(),
                                       [index_id](const IndexDefinition& candidate) {
                                         return candidate.index_id == index_id;
                                       });
  if (definition == input_snapshot->index_definitions.end()) {
    return Status::NotFound("index drop", "index definition is not present");
  }
  std::vector<std::string> sidecar_paths;
  for (const IndexFragment& fragment : input_snapshot->index_fragments) {
    if (fragment.index_id == index_id) sidecar_paths.push_back(fragment.relative_path);
  }
  IndexCatalog catalog(&version_set_, &schema_registry_);
  Status status = catalog.DropIndex(index_id);
  if (!status.ok()) return status;
  if (!sidecar_paths.empty()) {
    const std::string source = "retired-index:" + std::to_string(index_id) + ":" +
        std::to_string(input_snapshot->generation);
    blob_reference_catalog_.ReplaceSource(source, {});
    retired_ssts_.push_back(RetiredSstSet{input_snapshot, std::move(sidecar_paths), source});
  }
  return ReclaimRetiredSsts();
}

IndexCatalogSnapshot TransactionCoordinator::index_catalog_snapshot() const {
  const auto snapshot = version_set_.Snapshot();
  uint64_t coverage_generation = 0;
  for (const IndexFragment& fragment : snapshot->index_fragments) {
    coverage_generation = std::max(coverage_generation, fragment.catalog_generation);
  }
  return IndexCatalogSnapshot{snapshot->generation, snapshot->index_definitions,
                              snapshot->index_fragments, coverage_generation,
                              stats_snapshot_store_.generation()};
}

StatusOr<StatsSnapshot> TransactionCoordinator::StatsFor(
    EntityType entity_type, uint16_t column_id) const {
  return stats_snapshot_store_.SnapshotFor(*version_set_.Snapshot(), entity_type, column_id);
}

Status TransactionCoordinator::FlushFrozenShard(
    StorageShard* shard, const std::vector<TemporalEvent>& events,
    uint64_t estimated_write_bytes) {
  if (shard == nullptr) {
    return Status::InvalidArgument("flush", "missing storage shard");
  }
  if (flush_execution_hook_) flush_execution_hook_();
  if (io_governor_ != nullptr) {
    const Status io = io_governor_->TryAcquire(
        IoTokenRequest{0, 0, estimated_write_bytes, 1, false},
        MonotonicNowNs());
    if (!io.ok()) return io;
  }
  const auto result = FlushEventsToSst(
      db_path_, shard->shard_id(), events, next_sst_file_number_,
      schema_registry_, &version_set_, sst_publication_fault_injector_);
  if (!result.ok()) {
    if (result.status().IsIndeterminate()) {
      recovery_required_.store(true, std::memory_order_release);
    }
    return result.status();
  }
  uint64_t flushed_bytes = 0;
  for (const SstFileMeta& file : result.ValueOrDie().files) {
    flushed_bytes = SaturatingAdd(flushed_bytes, file.file_size);
  }
  AtomicSaturatingAdd(&sst_flush_bytes_written_, flushed_bytes);
  for (size_t slot = 0; slot < kPageTypeMetricSlots; ++slot) {
    AtomicSaturatingAdd(
        &page_uncompressed_bytes_written_[slot],
        result.ValueOrDie().compression.uncompressed_bytes[slot]);
    AtomicSaturatingAdd(
        &page_stored_bytes_written_[slot],
        result.ValueOrDie().compression.stored_bytes[slot]);
  }
  next_sst_file_number_ = result.ValueOrDie().next_file_number;
  uint64_t published_watermark = 0;
  for (const TemporalEvent& event : events) {
    published_watermark = std::max(published_watermark, event.commit_seq());
  }
  if (shard->shard_id() >= published_commit_watermarks_.size()) {
    return Status::Corruption(
        "flush", "published shard is outside checkpoint watermark table");
  }
  published_commit_watermarks_[shard->shard_id()] = std::max(
      published_commit_watermarks_[shard->shard_id()], published_watermark);
  shard->CompleteFlushPublished();
  // Publish the Manifest-live SST source before releasing the frozen MemTable
  // source so a BlobRef never loses its liveness protection.
  blob_reference_catalog_.RebuildLiveSstSources(*version_set_.Snapshot());
  RefreshMemtableBlobReferences(shard->shard_id());
  return Status::OK();
}

Status TransactionCoordinator::Flush() {
  std::unique_lock<std::mutex> flush_lock(flush_mutex_);
  const Status allowed = CheckMutationAllowed("flush");
  if (!allowed.ok()) return allowed;
  for (const auto& shard : storage_shards_) {
    std::vector<TemporalEvent> frozen_events = shard->FreezeForFlush();
    if (frozen_events.empty()) continue;
    const uint64_t estimated_write_bytes =
        EstimateFlushWriteBytes(frozen_events);
    const ResourceProfile resources{
        0, 0, 1, estimated_write_bytes, 1, 0, 0,
        estimated_write_bytes, 1};
    auto callback = [this, shard_ptr = shard.get(),
                     events = std::move(frozen_events),
                     estimated_write_bytes]() mutable {
      return FlushFrozenShard(shard_ptr, events, estimated_write_bytes);
    };
    if (work_execution_service_ != nullptr) {
      const auto submitted = work_execution_service_->Submit(
          WorkTaskRequest{WorkClass::kFlush, resources, false, 0},
          std::move(callback));
      if (!submitted.ok()) {
        RefreshPressure();
        return submitted.status();
      }
      const Status completed =
          work_execution_service_->WaitForTask(submitted.ValueOrDie());
      if (!completed.ok()) {
        RefreshPressure();
        return completed;
      }
    } else {
      std::optional<ResourceLease> maintenance_lease;
      if (resource_governor_ != nullptr) {
        auto acquired = resource_governor_->Acquire(resources);
        if (!acquired.ok()) {
          RefreshPressure();
          return acquired.status();
        }
        maintenance_lease.emplace(std::move(acquired).ConsumeValueOrDie());
      }
      const Status completed = callback();
      if (!completed.ok()) {
        RefreshPressure();
        return completed;
      }
    }
  }
  blob_reference_catalog_.RebuildLiveSstSources(*version_set_.Snapshot());
  const Status reclaimed = ReclaimRetiredSsts();
  if (!reclaimed.ok()) return reclaimed;
  const PressureController::Decision post_flush_pressure = RefreshPressure();
  const bool has_compaction_debt = CompactionDebtBytes(*version_set_.Snapshot(), 4) != 0;
  flush_lock.unlock();
  if (has_compaction_debt &&
      (post_flush_pressure.require_urgent_compaction ||
       post_flush_pressure.admit_optional_maintenance)) {
    // Compaction is derived from the published VersionSet, not from this
    // transient request. A maintenance failure leaves all inputs live and is
    // retried by the next flush or explicit Compact call.
    CompactWithClass(post_flush_pressure.require_urgent_compaction
                         ? WorkClass::kCompactionUrgent
                         : WorkClass::kCompactionNormal)
        .IgnoreError();
  }
  // Indexes and statistics are advisory optional maintenance.  Submit them
  // only after all essential flush work has left the scheduler queue.
  const std::vector<SstFileMeta> live_files = version_set_.Snapshot()->files;
  if (post_flush_pressure.admit_optional_maintenance) {
    ScheduleIndexBuild(live_files).IgnoreError();
    ScheduleStatsMerge(live_files).IgnoreError();
  }
  // A checkpoint is only an optimization after the SST Manifest has made the
  // events durable.  A failed checkpoint therefore retains the logs and is
  // retried by the next completed flush or explicit checkpoint request.
  CheckpointDurableLogs().IgnoreError();
  return Status::OK();
}

Status TransactionCoordinator::Compact() {
  const Status allowed = CheckMutationAllowed("compaction");
  if (!allowed.ok()) return allowed;
  const PressureController::Decision pressure = RefreshPressure();
  if (pressure.require_urgent_compaction) {
    return CompactWithClass(WorkClass::kCompactionUrgent);
  }
  if (!pressure.admit_optional_maintenance) {
    return Status::ResourceExhausted(
        "compaction", "normal compaction deferred by pressure policy");
  }
  return CompactWithClass(WorkClass::kCompactionNormal);
}

Status TransactionCoordinator::CompactWithClass(WorkClass work_class) {
  return maintenance_executor_.SubmitAndRun(MaintenanceTaskSpec{
      work_class,
      ResourceProfile{0, 0, 1, 0, 1, 0, 0, 0, 1},
      IoTokenRequest{0, 0, 0, 1, false}, false,
      work_class == WorkClass::kCompactionNormal,
      [this](const std::shared_ptr<WorkCancellation>& cancellation) {
        return CompactInternal(cancellation);
      }});
}

Status TransactionCoordinator::CompactInternal(
    const std::shared_ptr<WorkCancellation>& cancellation) {
  std::lock_guard<std::mutex> flush_lock(flush_mutex_);
  const Status allowed = CheckMutationAllowed("compaction");
  if (!allowed.ok()) return allowed;
  const Status reclaimed = ReclaimRetiredSsts();
  if (!reclaimed.ok()) return reclaimed;

  const std::shared_ptr<const VersionSnapshot> input_snapshot = version_set_.Snapshot();
  using CandidateKey = std::tuple<std::string, uint8_t, uint16_t, uint32_t, uint8_t, uint16_t,
                                  uint8_t, uint8_t>;
  std::map<CandidateKey, std::vector<SstFileMeta>> candidates;
  for (const SstFileMeta& file : input_snapshot->files) {
    const std::filesystem::path relative(file.relative_path);
    const std::filesystem::path shard = relative.parent_path().parent_path();
    if (relative.parent_path().filename() != "sst" || shard.parent_path().filename() != "shards") {
      return Status::Corruption("compaction", "Manifest contains a non-canonical shard SST path");
    }
    candidates[{shard.filename().string(), static_cast<uint8_t>(file.partition.entity_type),
                file.partition.column_id, file.partition.schema_epoch,
                static_cast<uint8_t>(file.partition.physical_type), file.partition.edge_type,
                static_cast<uint8_t>(file.partition.compression_id),
                static_cast<uint8_t>(file.partition.key_kind)}]
        .push_back(file);
  }

  for (const auto& candidate : candidates) {
    if (cancellation != nullptr) {
      const Status checkpoint = cancellation->Checkpoint("compaction");
      if (!checkpoint.ok()) return checkpoint;
    }
    const std::vector<SstFileMeta>& inputs = candidate.second;
    if (inputs.size() < 2) continue;
    uint64_t input_bytes = 0;
    for (const SstFileMeta& input : inputs) {
      input_bytes = SaturatingAdd(input_bytes, input.file_size);
    }
    std::optional<ResourceLease> maintenance_lease;
    if (resource_governor_ != nullptr) {
      auto acquired = resource_governor_->Acquire(ResourceProfile{
          0, 0, static_cast<uint64_t>(inputs.size()), input_bytes, 1,
          input_bytes, 0, input_bytes, 1});
      if (!acquired.ok()) return acquired.status();
      maintenance_lease.emplace(std::move(acquired).ConsumeValueOrDie());
    }
    if (io_governor_ != nullptr) {
      const Status io = io_governor_->TryAcquire(
          IoTokenRequest{input_bytes, 0, input_bytes, 1, false}, MonotonicNowNs());
      if (!io.ok()) return io;
    }

    const auto compacted = CompactSstPartition(
        db_path_, inputs, next_sst_file_number_, &version_set_, cancellation);
    if (!compacted.ok()) return compacted.status();
    AtomicSaturatingAdd(&compaction_input_bytes_,
                        compacted.ValueOrDie().input_bytes_read);
    AtomicSaturatingAdd(&compaction_output_bytes_,
                        compacted.ValueOrDie().output_bytes_written);
    AtomicSaturatingAdd(&compaction_blob_payload_bytes_read_,
                        compacted.ValueOrDie().blob_payload_bytes_read);
    AtomicMax(&compaction_peak_buffered_events_,
              compacted.ValueOrDie().peak_buffered_events);
    AtomicMax(&compaction_peak_buffered_bytes_,
              compacted.ValueOrDie().peak_buffered_bytes);
    for (size_t slot = 0; slot < kPageTypeMetricSlots; ++slot) {
      AtomicSaturatingAdd(
          &page_uncompressed_bytes_written_[slot],
          compacted.ValueOrDie().compression.uncompressed_bytes[slot]);
      AtomicSaturatingAdd(
          &page_stored_bytes_written_[slot],
          compacted.ValueOrDie().compression.stored_bytes[slot]);
    }
    next_sst_file_number_ = compacted.ValueOrDie().next_file_number;
    RetireCompactionInputs(input_snapshot, compacted.ValueOrDie().inputs,
                           compacted.ValueOrDie().output.file_number);
    // Sidecars are advisory. Removing the old fragments in the compaction
    // edit is correct; failure to build the replacement simply leaves a base
    // scan coverage gap until maintenance retries.
    blob_reference_catalog_.RebuildLiveSstSources(*version_set_.Snapshot());
  }
  RefreshPressure();
  return Status::OK();
}

Status TransactionCoordinator::RotateBlobSegments() {
  ResourceProfile resources;
  {
    std::scoped_lock<std::mutex, std::mutex> publication_lock(
        commit_mutex_, flush_mutex_);
    const Status allowed = CheckMutationAllowed("blob rotation");
    if (!allowed.ok()) return allowed;
    const auto estimated = EstimateBlobRotationResourcesLocked();
    if (!estimated.ok()) return estimated.status();
    resources = estimated.ValueOrDie();
  }
  return maintenance_executor_.SubmitAndRun(MaintenanceTaskSpec{
      WorkClass::kForegroundWrite, resources,
      IoTokenRequest{0, 0, resources.write_bytes, resources.metadata_ops, false},
      false, false,
      [this](const std::shared_ptr<WorkCancellation>&) {
        std::scoped_lock<std::mutex, std::mutex> publication_lock(
            commit_mutex_, flush_mutex_);
        const Status allowed = CheckMutationAllowed("blob rotation");
        if (!allowed.ok()) return allowed;
        if (blob_rotation_execution_hook_) blob_rotation_execution_hook_();
        const Status rotated =
            TrackBlobMutation(blob_store_.RotateActiveSegments());
        if (!rotated.ok()) return rotated;
        return ReconcileBlobSegments();
      }});
}

StatusOr<ResourceProfile>
TransactionCoordinator::EstimateBlobRotationResourcesLocked() const {
  const std::vector<BlobSegmentId> segments = blob_store_.SegmentIds();
  uint64_t active_bytes = 0;
  uint64_t active_count = 0;
  for (const BlobSegmentId& segment : segments) {
    if (!segment.active) continue;
    ++active_count;
    const std::string path = db_path_ + "/blobs/shard-" +
        std::to_string(segment.shard_id) + "/segment-" +
        std::to_string(segment.segment_id) + ".blob";
    std::error_code error;
    const uint64_t bytes = std::filesystem::file_size(path, error);
    if (error) return Status::IOError(path, error.message());
    active_bytes = SaturatingAdd(active_bytes, bytes);
  }
  if (active_count == 0) {
    // Rotation also provisions the first active segment on a newly opened
    // database, so reserve a bounded minimum even when BlobStore has not yet
    // materialized a segment entry.
    return ResourceProfile{0, 0, 1, 4096, 1, 0, 0, 4096, 1};
  }
  const uint64_t metadata_ops = SaturatingAdd(active_count, 2);
  const uint64_t descriptors = SaturatingAdd(active_count, 1);
  const uint64_t write_bytes = SaturatingAdd(active_bytes, 4096);
  return ResourceProfile{0, 0, descriptors, active_bytes, 1, active_bytes,
                         0, write_bytes, metadata_ops};
}

Status TransactionCoordinator::CollectBlobGarbage() {
  std::vector<BlobHash> live_hashes;
  ResourceProfile resources;
  {
    std::scoped_lock<std::mutex, std::mutex> publication_lock(
        commit_mutex_, flush_mutex_);
    const Status allowed = CheckMutationAllowed("blob gc");
    if (!allowed.ok()) return allowed;
    live_hashes = blob_reference_catalog_.LiveHashes();
    const auto estimated =
        EstimateBlobGarbageCollectionResourcesLocked(live_hashes);
    if (!estimated.ok()) return estimated.status();
    resources = estimated.ValueOrDie();
  }
  return maintenance_executor_.SubmitAndRun(MaintenanceTaskSpec{
      WorkClass::kBlobGc, resources,
      IoTokenRequest{0, 0, resources.write_bytes,
                     resources.metadata_ops, false}, false, true,
      [this, live_hashes = std::move(live_hashes), resources](
          const std::shared_ptr<WorkCancellation>& cancellation) {
        return CollectBlobGarbageInternal(live_hashes, resources,
                                          cancellation);
      }});
}

StatusOr<ResourceProfile>
TransactionCoordinator::EstimateBlobGarbageCollectionResources() {
  std::scoped_lock<std::mutex, std::mutex> publication_lock(
      commit_mutex_, flush_mutex_);
  if (!opened_) {
    return Status::InvalidArgument("blob gc", "coordinator is not open");
  }
  return EstimateBlobGarbageCollectionResourcesLocked(
      blob_reference_catalog_.LiveHashes());
}

StatusOr<ResourceProfile>
TransactionCoordinator::EstimateBlobGarbageCollectionResourcesLocked(
    const std::vector<BlobHash>& live_hashes) const {
  const auto blob = blob_store_.EstimateGarbageCollectionWrites(live_hashes);
  if (!blob.ok()) return blob.status();
  const auto manifest = version_set_.EstimateManifestRewriteBytes(
      blob.ValueOrDie().manifest_rewrites, 0);
  if (!manifest.ok()) return manifest.status();

  const auto checked_add = [](uint64_t left, uint64_t right,
                              const char* dimension) -> StatusOr<uint64_t> {
    if (left > UINT64_MAX - right) {
      return Status::ResourceExhausted(
          "blob gc estimate", std::string(dimension) + " estimate overflow");
    }
    return left + right;
  };
  const auto total_bytes = checked_add(
      blob.ValueOrDie().total_bytes, manifest.ValueOrDie(), "write bytes");
  if (!total_bytes.ok()) return total_bytes.status();
  const uint64_t manifest_descriptors =
      blob.ValueOrDie().manifest_rewrites == 0 ? 0 : 2;
  const auto descriptors = checked_add(
      blob.ValueOrDie().descriptors, manifest_descriptors, "descriptors");
  if (!descriptors.ok()) return descriptors.status();
  const uint64_t manifest_metadata =
      blob.ValueOrDie().manifest_rewrites == 0 ? 0 : 3;
  auto metadata_ops = checked_add(
      blob.ValueOrDie().metadata_ops, manifest_metadata, "metadata operations");
  if (!metadata_ops.ok()) return metadata_ops.status();
  uint64_t reclaim_descriptors = 0;
  for (const RetiredBlobSegmentSet& retired : retired_blob_segments_) {
    if (!retired.pinned_snapshot.expired()) continue;
    reclaim_descriptors = 1;
    metadata_ops = checked_add(metadata_ops.ValueOrDie(),
                               retired.segments.size(),
                               "metadata operations");
    if (!metadata_ops.ok()) return metadata_ops.status();
    std::set<uint32_t> reclaim_shards;
    for (const BlobSegmentId& segment : retired.segments) {
      reclaim_shards.insert(segment.shard_id);
    }
    metadata_ops = checked_add(metadata_ops.ValueOrDie(),
                               reclaim_shards.size(),
                               "metadata operations");
    if (!metadata_ops.ok()) return metadata_ops.status();
  }
  const auto all_descriptors = checked_add(
      descriptors.ValueOrDie(), reclaim_descriptors, "descriptors");
  if (!all_descriptors.ok()) return all_descriptors.status();
  return ResourceProfile{0, 0, all_descriptors.ValueOrDie(),
                         total_bytes.ValueOrDie(), 1, 0, 0,
                         total_bytes.ValueOrDie(),
                         metadata_ops.ValueOrDie()};
}

Status TransactionCoordinator::CollectBlobGarbageInternal(
    const std::vector<BlobHash>& live_hashes,
    const ResourceProfile& admitted_resources,
    const std::shared_ptr<WorkCancellation>& cancellation) {
  std::scoped_lock<std::mutex, std::mutex> publication_lock(commit_mutex_, flush_mutex_);
  if (cancellation != nullptr) {
    const Status checkpoint = cancellation->Checkpoint("blob gc");
    if (!checkpoint.ok()) return checkpoint;
  }
  const Status allowed = CheckMutationAllowed("blob gc");
  if (!allowed.ok()) return allowed;
  // commit_mutex_ protects the externalization -> PREPARE interval, while
  // flush_mutex_ protects catalog handoff from MemTable to published SST.
  std::vector<BlobHash> current_live_hashes =
      blob_reference_catalog_.LiveHashes();
  const auto by_hash = [](const BlobHash& left, const BlobHash& right) {
    return left.bytes < right.bytes;
  };
  std::vector<BlobHash> captured_live_hashes = live_hashes;
  std::sort(captured_live_hashes.begin(), captured_live_hashes.end(), by_hash);
  captured_live_hashes.erase(
      std::unique(captured_live_hashes.begin(), captured_live_hashes.end()),
      captured_live_hashes.end());
  std::sort(current_live_hashes.begin(), current_live_hashes.end(), by_hash);
  current_live_hashes.erase(
      std::unique(current_live_hashes.begin(), current_live_hashes.end()),
      current_live_hashes.end());
  if (current_live_hashes != captured_live_hashes) {
    return Status::Conflict("blob gc", "live hash set changed during collection");
  }
  const auto required_resources =
      EstimateBlobGarbageCollectionResourcesLocked(captured_live_hashes);
  if (!required_resources.ok()) return required_resources.status();
  const ResourceProfile& required = required_resources.ValueOrDie();
  if (required.descriptors > admitted_resources.descriptors ||
      required.temporary_bytes > admitted_resources.temporary_bytes ||
      required.cpu_slots > admitted_resources.cpu_slots ||
      required.write_bytes > admitted_resources.write_bytes ||
      required.metadata_ops > admitted_resources.metadata_ops) {
    return Status::QueryMemoryLimit(
        "blob gc", "physical writes exceed admitted task grant");
  }
  const Status reclaimed = ReclaimRetiredBlobSegments();
  if (!reclaimed.ok()) return reclaimed;
  const std::shared_ptr<const VersionSnapshot> input_snapshot = version_set_.Snapshot();
  const Status relocated =
      TrackBlobMutation(blob_store_.RelocateLiveHashes(
          captured_live_hashes, cancellation));
  if (!relocated.ok()) return relocated;
  if (cancellation != nullptr) {
    const Status checkpoint = cancellation->Checkpoint("blob gc");
    if (!checkpoint.ok()) return checkpoint;
  }
  const auto retired =
      blob_store_.RetireUnreferencedSealedSegments(captured_live_hashes);
  if (!retired.ok()) return TrackBlobMutation(retired.status());
  std::vector<BlobSegmentId> newly_retired;
  for (const BlobSegmentId& candidate : retired.ValueOrDie()) {
    const bool already_pending = std::any_of(
        retired_blob_segments_.begin(), retired_blob_segments_.end(),
        [&candidate](const RetiredBlobSegmentSet& pending) {
          return std::any_of(
              pending.segments.begin(), pending.segments.end(),
              [&candidate](const BlobSegmentId& segment) {
                return segment.shard_id == candidate.shard_id &&
                       segment.segment_id == candidate.segment_id;
              });
        });
    if (!already_pending) newly_retired.push_back(candidate);
  }
  if (newly_retired.empty()) return Status::OK();
  if (cancellation != nullptr) {
    const Status checkpoint = cancellation->Checkpoint("blob gc");
    if (!checkpoint.ok()) return checkpoint;
  }
  VersionEdit edit;
  edit.expected_generation = input_snapshot->generation;
  for (const BlobSegmentId& segment : newly_retired) {
    edit.blob_segment_deletes.push_back(BlobSegmentKey{segment.shard_id, segment.segment_id});
  }
  const Status published = version_set_.ApplyEdit(edit);
  if (!published.ok()) return published;
  retired_blob_segments_.push_back(
      RetiredBlobSegmentSet{input_snapshot, std::move(newly_retired)});
  return Status::OK();
}

void TransactionCoordinator::RetireCompactionInputs(
    const std::shared_ptr<const VersionSnapshot>& snapshot,
    const std::vector<SstFileMeta>& inputs, uint64_t output_file_number) {
  std::set<uint64_t> input_ids;
  std::vector<BlobHash> blob_refs;
  std::vector<std::string> paths;
  for (const SstFileMeta& input : inputs) {
    input_ids.insert(input.file_number);
    paths.push_back(input.relative_path);
    blob_refs.insert(blob_refs.end(), input.blob_refs.begin(), input.blob_refs.end());
  }
  for (const IndexFragment& fragment : snapshot->index_fragments) {
    if (input_ids.count(fragment.source_sst_id) != 0) paths.push_back(fragment.relative_path);
  }
  const std::string source = "retired-sst:" + std::to_string(output_file_number);
  blob_reference_catalog_.ReplaceSource(source, blob_refs);
  retired_ssts_.push_back(RetiredSstSet{snapshot, std::move(paths), source});
}

Status TransactionCoordinator::ReclaimRetiredSsts() {
  for (auto retired = retired_ssts_.begin(); retired != retired_ssts_.end();) {
    if (!retired->pinned_snapshot.expired()) {
      ++retired;
      continue;
    }
    for (const std::string& relative : retired->relative_paths) {
      std::error_code error;
      std::filesystem::remove(db_path_ + "/" + relative, error);
      if (error) return Status::IOError(relative, error.message());
    }
    blob_reference_catalog_.RemoveSource(retired->blob_catalog_source);
    retired = retired_ssts_.erase(retired);
  }
  return Status::OK();
}

Status TransactionCoordinator::ReclaimRetiredBlobSegments() {
  for (auto retired = retired_blob_segments_.begin(); retired != retired_blob_segments_.end();) {
    if (!retired->pinned_snapshot.expired()) {
      ++retired;
      continue;
    }
    BlobGarbageCollector collector(&blob_store_);
    const Status removed =
        TrackBlobMutation(collector.FinishCollection(retired->segments));
    if (!removed.ok()) return removed;
    retired = retired_blob_segments_.erase(retired);
  }
  return Status::OK();
}

Status TransactionCoordinator::BuildIndexFragmentsForFiles(
    const std::vector<SstFileMeta>& files,
    const std::shared_ptr<WorkCancellation>& cancellation,
    uint64_t expected_generation) {
  for (const SstFileMeta& file : files) {
    if (cancellation != nullptr) {
      const Status checkpoint = cancellation->Checkpoint("index build");
      if (!checkpoint.ok()) return checkpoint;
    }
    if (file.partition.key_kind != LogicalKeyKind::kProperty) continue;
    const auto snapshot = version_set_.Snapshot();
    if (expected_generation != UINT64_MAX &&
        snapshot->generation != expected_generation) {
      return Status::Conflict("index build", "Manifest generation changed");
    }
    const auto live_file = std::find_if(
        snapshot->files.begin(), snapshot->files.end(),
        [&file](const SstFileMeta& candidate) {
          return candidate.file_number == file.file_number &&
                 candidate.relative_path == file.relative_path &&
                 candidate.identity == file.identity;
        });
    if (live_file == snapshot->files.end()) {
      return Status::Conflict("index build", "source SST identity changed");
    }
    const auto source_events = ReadSstFile(db_path_ + "/" + file.relative_path);
    if (!source_events.ok()) return source_events.status();
    std::vector<std::string> published_paths;
    VersionEdit publication;
    publication.expected_generation = snapshot->generation;
    for (const IndexDefinition& definition : snapshot->index_definitions) {
      if ((definition.state != IndexState::kBuilding && definition.state != IndexState::kActive) ||
          definition.entity_type != file.partition.entity_type ||
          definition.column_id != file.partition.column_id ||
          definition.schema_epoch != file.partition.schema_epoch) {
        continue;
      }
      const auto existing = std::find_if(
          snapshot->index_fragments.begin(), snapshot->index_fragments.end(),
          [&definition, &file](const IndexFragment& fragment) {
            return fragment.index_id == definition.index_id &&
                   fragment.source_sst_id == file.file_number;
          });
      bool healthy = false;
      if (existing != snapshot->index_fragments.end() && existing->usable &&
          existing->source_row_count == file.statistics.row_count &&
          existing->indexed_put_count <= file.statistics.put_count) {
        const auto schema = schema_registry_.Lookup(
            definition.entity_type, definition.column_id,
            definition.schema_epoch);
        if (!schema.has_value()) {
          return Status::SchemaMismatch("index build", "indexed schema is missing");
        }
        const auto bound = EstimateIndexSidecarEncodedBytes(
            definition, file.statistics, file.file_size, *schema);
        if (!bound.ok()) return bound.status();
        healthy = ReadVerifiedIndexSidecarFile(
            db_path_ + "/" + existing->relative_path, definition,
            file.file_number, existing->identity_checksum,
            bound.ValueOrDie()).ok();
      }
      if (healthy) continue;
      const auto sidecar = BuildIndexCandidateSidecar(
          file.file_number, definition, source_events.ValueOrDie(),
          cancellation);
      if (!sidecar.ok()) return sidecar.status();
      const auto encoded = BuildIndexSidecar(definition, file.file_number, sidecar.ValueOrDie().postings);
      if (!encoded.ok()) return encoded.status();
      if (cancellation != nullptr) {
        const Status checkpoint = cancellation->Checkpoint("index build");
        if (!checkpoint.ok()) return checkpoint;
      }
      const std::string relative = "indexes/" + std::to_string(definition.index_id) + "/" +
          std::to_string(file.file_number) + storage_layout::kIndexExtension;
      const std::string path = db_path_ + "/" + relative;
      const Status written = WriteIndexSidecarFile(
          path, definition, file.file_number, sidecar.ValueOrDie().postings,
          index_sidecar_publication_fault_injector_);
      if (!written.ok()) return written;
      published_paths.push_back(path);
      if (cancellation != nullptr) {
        const Status checkpoint = cancellation->Checkpoint("index build");
        if (!checkpoint.ok()) {
          std::error_code error;
          std::filesystem::remove(path, error);
          return checkpoint;
        }
      }
      const BlobHash checksum = Blake3Hash(encoded.ValueOrDie());
      IndexFragment fragment;
      fragment.index_id = definition.index_id;
      fragment.source_sst_id = file.file_number;
      fragment.relative_path = relative;
      fragment.source_row_count = source_events.ValueOrDie().size();
      fragment.indexed_put_count = sidecar.ValueOrDie().postings.size();
      fragment.format_version = 1;
      fragment.usable = true;
      fragment.identity_checksum = checksum.bytes;
      fragment.catalog_generation = snapshot->generation + 1;
      if (cancellation != nullptr) {
        const Status checkpoint = cancellation->Checkpoint("index build");
        if (!checkpoint.ok()) {
          std::error_code error;
          std::filesystem::remove(path, error);
          return checkpoint;
        }
      }
      publication.index_fragment_adds.push_back(std::move(fragment));
    }
    if (!publication.index_fragment_adds.empty()) {
      const Status attached = version_set_.ApplyEdit(publication);
      if (!attached.ok()) {
        if (!attached.IsIndeterminate()) {
          for (const std::string& path : published_paths) {
            std::error_code error;
            std::filesystem::remove(path, error);
          }
        }
        return attached;
      }
    }
  }
  return Status::OK();
}

Status TransactionCoordinator::BuildStatsFragmentsForFiles(
    const std::vector<SstFileMeta>& files,
    const std::shared_ptr<WorkCancellation>& cancellation,
    uint64_t expected_generation) {
  for (const SstFileMeta& file : files) {
    if (cancellation != nullptr) {
      const Status checkpoint = cancellation->Checkpoint("stats merge");
      if (!checkpoint.ok()) return checkpoint;
    }
    if (file.partition.key_kind != LogicalKeyKind::kProperty) continue;
    const auto snapshot = version_set_.Snapshot();
    const auto live_file = std::find_if(
        snapshot->files.begin(), snapshot->files.end(),
        [&file](const SstFileMeta& candidate) {
          return candidate.file_number == file.file_number &&
                 candidate.relative_path == file.relative_path &&
                 candidate.identity == file.identity;
        });
    if (live_file == snapshot->files.end()) {
      return Status::Conflict("stats merge", "source SST identity changed");
    }
    const auto events = ReadSstFile(db_path_ + "/" + file.relative_path);
    if (!events.ok()) return events.status();
    const auto fragment = BuildStatsFragment(
        file.file_number, file.partition.entity_type, file.partition.column_id,
        events.ValueOrDie(), cancellation);
    if (!fragment.ok()) return fragment.status();
    if (cancellation != nullptr) {
      const Status checkpoint = cancellation->Checkpoint("stats merge");
      if (!checkpoint.ok()) return checkpoint;
    }
    const Status stored = expected_generation == UINT64_MAX
        ? stats_snapshot_store_.Upsert(fragment.ValueOrDie())
        : stats_snapshot_store_.UpsertExpected(
              fragment.ValueOrDie(), expected_generation);
    if (!stored.ok()) return stored;
  }
  return Status::OK();
}

Status TransactionCoordinator::ScheduleIndexBuild(
    const std::vector<SstFileMeta>& files) {
  if (files.empty()) return Status::OK();
  for (const SstFileMeta& requested_file : files) {
    if (requested_file.partition.key_kind != LogicalKeyKind::kProperty) continue;
    if (!RefreshPressure().admit_optional_maintenance) {
      return Status::MaintenanceBackoff(
          "index build", "optional maintenance yielded under pressure");
    }
    const auto snapshot = version_set_.Snapshot();
    const auto live = std::find_if(
        snapshot->files.begin(), snapshot->files.end(),
        [&requested_file](const SstFileMeta& file) {
          return file.file_number == requested_file.file_number &&
                 file.identity == requested_file.identity;
        });
    if (live == snapshot->files.end()) continue;
    const auto schema = schema_registry_.Lookup(
        live->partition.entity_type, live->partition.column_id,
        live->partition.schema_epoch);
    if (!schema.has_value()) {
      return Status::SchemaMismatch("index build", "source schema is missing");
    }
    auto resources = EstimateSstDecodeResources(
        live->statistics, live->file_size, *schema);
    if (!resources.ok()) return resources.status();
    VersionEdit publication;
    publication.expected_generation = snapshot->generation;
    for (const IndexDefinition& definition : snapshot->index_definitions) {
      if ((definition.state != IndexState::kBuilding &&
           definition.state != IndexState::kActive) ||
          definition.entity_type != live->partition.entity_type ||
          definition.column_id != live->partition.column_id ||
          definition.schema_epoch != live->partition.schema_epoch) {
        continue;
      }
      const auto encoded = EstimateIndexSidecarEncodedBytes(
          definition, live->statistics, live->file_size, *schema);
      if (!encoded.ok()) return encoded.status();
      const uint64_t bytes = encoded.ValueOrDie();
      const bool existing_fragment = std::any_of(
          snapshot->index_fragments.begin(),
          snapshot->index_fragments.end(),
          [&definition, &live](const IndexFragment& fragment) {
            return fragment.index_id == definition.index_id &&
                   fragment.source_sst_id == live->file_number;
          });
      const auto combined = AddMaintenanceResources(
          resources.ValueOrDie(),
          ResourceProfile{SaturatingAdd(bytes, bytes), 0, 2, bytes, 1,
                          existing_fragment ? bytes : 0, 0, bytes,
                          existing_fragment ? 4U : 3U});
      if (!combined.ok()) return combined.status();
      resources = combined.ValueOrDie();
      publication.index_fragment_adds.push_back(IndexFragment{
          definition.index_id, live->file_number,
          "indexes/" + std::to_string(definition.index_id) + "/" +
              std::to_string(live->file_number) + storage_layout::kIndexExtension,
          live->statistics.row_count, live->statistics.put_count,
          snapshot->generation + 1, 1, true, {}});
    }
    if (publication.index_fragment_adds.empty()) continue;
    const auto manifest =
        version_set_.EstimateManifestEditRewriteBytes(publication);
    if (!manifest.ok()) return manifest.status();
    const uint64_t manifest_bytes = manifest.ValueOrDie();
    const auto complete = AddMaintenanceResources(
        resources.ValueOrDie(),
        ResourceProfile{manifest_bytes, 0, 2, manifest_bytes, 1,
                        0, 0, manifest_bytes, 4});
    if (!complete.ok()) return complete.status();
    const ResourceProfile grant = complete.ValueOrDie();
    const SstFileMeta file = *live;
    const Status status = maintenance_executor_.SubmitAndRun(
        MaintenanceTaskSpec{
            WorkClass::kIndexBuild, grant,
            IoTokenRequest{grant.sequential_read_bytes,
                           grant.random_read_ops, grant.write_bytes,
                           grant.metadata_ops, false},
            false, true,
            [this, file, generation = snapshot->generation](
                const std::shared_ptr<WorkCancellation>& cancellation) {
              return BuildIndexFragmentsForFiles(
                  {file}, cancellation, generation);
            }});
    if (!status.ok()) return status;
  }
  return Status::OK();
}

Status TransactionCoordinator::ScheduleStatsMerge(
    const std::vector<SstFileMeta>& files) {
  if (files.empty()) return Status::OK();
  for (const SstFileMeta& requested_file : files) {
    if (requested_file.partition.key_kind != LogicalKeyKind::kProperty) continue;
    if (!RefreshPressure().admit_optional_maintenance) {
      return Status::MaintenanceBackoff(
          "stats merge", "optional maintenance yielded under pressure");
    }
    const auto snapshot = version_set_.Snapshot();
    const auto live = std::find_if(
        snapshot->files.begin(), snapshot->files.end(),
        [&requested_file](const SstFileMeta& file) {
          return file.file_number == requested_file.file_number &&
                 file.identity == requested_file.identity;
        });
    if (live == snapshot->files.end()) continue;
    const auto schema = schema_registry_.Lookup(
        live->partition.entity_type, live->partition.column_id,
        live->partition.schema_epoch);
    if (!schema.has_value()) {
      return Status::SchemaMismatch("stats merge", "source schema is missing");
    }
    auto resources = EstimateSstDecodeResources(
        live->statistics, live->file_size, *schema);
    if (!resources.ok()) return resources.status();
    const auto fragment_resources = EstimateStatsFragmentResources(
        live->statistics, *schema);
    if (!fragment_resources.ok()) return fragment_resources.status();
    auto combined = AddMaintenanceResources(
        resources.ValueOrDie(), fragment_resources.ValueOrDie());
    if (!combined.ok()) return combined.status();
    const uint64_t stats_generation = stats_snapshot_store_.generation();
    const StatsFragment projected{
        live->file_number, live->partition.entity_type,
        live->partition.column_id, live->statistics.row_count,
        live->statistics.put_count, live->statistics.delete_count,
        live->statistics.put_count, live->statistics.min_valid_from,
        live->statistics.max_valid_from, live->statistics.min_commit_seq,
        live->statistics.max_commit_seq};
    const auto checkpoint = stats_snapshot_store_.EstimateUpsertResources(
        projected, stats_generation);
    if (!checkpoint.ok()) return checkpoint.status();
    combined = AddMaintenanceResources(
        combined.ValueOrDie(), checkpoint.ValueOrDie());
    if (!combined.ok()) return combined.status();
    const ResourceProfile grant = combined.ValueOrDie();
    const SstFileMeta file = *live;
    const Status status = maintenance_executor_.SubmitAndRun(
        MaintenanceTaskSpec{
            WorkClass::kStatsMerge, grant,
            IoTokenRequest{grant.sequential_read_bytes,
                           grant.random_read_ops, grant.write_bytes,
                           grant.metadata_ops, false},
            false, true,
            [this, file, stats_generation](
                const std::shared_ptr<WorkCancellation>& cancellation) {
              return BuildStatsFragmentsForFiles(
                  {file}, cancellation, stats_generation);
            }});
    if (!status.ok()) return status;
  }
  return Status::OK();
}

void TransactionCoordinator::RefreshMemtableBlobReferences(uint32_t shard_id) {
  std::vector<BlobHash> references;
  for (const TemporalEvent& event : storage_shards_[shard_id]->SnapshotUnflushedEvents()) {
    if (event.blob_ref().has_value()) references.push_back(event.blob_ref()->content_hash);
  }
  blob_reference_catalog_.ReplaceSource("memtable:" + std::to_string(shard_id), references);
}

Status TransactionCoordinator::CheckpointCommitTimeline() {
  return CheckpointDurableLogs();
}

uint64_t TransactionCoordinator::LargestCheckpointableSequence() const {
  uint64_t checkpoint_seq = version_set_.Snapshot()->checkpoint.checkpoint_seq;
  for (const CommitDecision& decision : decision_log_.commits()) {
    if (decision.commit_seq != checkpoint_seq + 1) break;
    bool covered = true;
    for (const PrepareReference& reference : decision.prepares) {
      if (reference.shard_id >= published_commit_watermarks_.size() ||
          published_commit_watermarks_[reference.shard_id] < decision.commit_seq) {
        covered = false;
        break;
      }
    }
    if (!covered) break;
    checkpoint_seq = decision.commit_seq;
  }
  return checkpoint_seq;
}

Status TransactionCoordinator::CheckpointDurableLogs() {
  const Status allowed = CheckMutationAllowed("checkpoint");
  if (!allowed.ok()) return allowed;
  return maintenance_executor_.SubmitAndRun(MaintenanceTaskSpec{
      WorkClass::kCommitCritical,
      ResourceProfile{0, 0, 1, 0, 1, 0, 0, 0, 1},
      IoTokenRequest{0, 0, 0, 1, true}, true, false,
      [this](const std::shared_ptr<WorkCancellation>&) {
        return CheckpointDurableLogsInternal();
      }});
}

Status TransactionCoordinator::CheckpointDurableLogsInternal() {
  std::scoped_lock<std::mutex, std::mutex> publication_lock(commit_mutex_, flush_mutex_);
  const Status allowed = CheckMutationAllowed("checkpoint");
  if (!allowed.ok()) return allowed;
  const Status blob_index_checkpoint =
      TrackBlobMutation(blob_store_.CheckpointIndex());
  if (!blob_index_checkpoint.ok()) return blob_index_checkpoint;
  const std::shared_ptr<const VersionSnapshot> snapshot = version_set_.Snapshot();
  const uint64_t previous_checkpoint = snapshot->checkpoint.checkpoint_seq;
  const uint64_t target_checkpoint = LargestCheckpointableSequence();
  if (target_checkpoint == previous_checkpoint) return commit_timeline_.Checkpoint();

  std::vector<TransactionOutcome> outcomes = checkpoint_outcomes_;
  if (outcomes.size() != previous_checkpoint) {
    return Status::Corruption("checkpoint", "outcome index does not match Manifest prefix");
  }
  std::vector<uint64_t> wal_safe_lsns = snapshot->checkpoint.wal_safe_lsns;
  if (wal_safe_lsns.empty()) wal_safe_lsns.assign(prepare_logs_.size(), 0);
  if (wal_safe_lsns.size() != prepare_logs_.size()) {
    return Status::Corruption("checkpoint", "per-shard WAL safe positions are incomplete");
  }
  const std::vector<uint64_t> previous_wal_safe_lsns = wal_safe_lsns;
  for (const CommitDecision& decision : decision_log_.commits()) {
    if (decision.commit_seq > target_checkpoint) break;
    if (decision.commit_seq != outcomes.size() + 1) {
      return Status::Corruption("checkpoint", "non-contiguous retained decision prefix");
    }
    outcomes.push_back(TransactionOutcome{
        decision.txn_id, decision.commit_seq, decision.system_time_hlc});
    for (const PrepareReference& reference : decision.prepares) {
      if (reference.shard_id >= prepare_logs_.size()) {
        return Status::Corruption("checkpoint", "decision references unknown shard");
      }
      const auto end_lsn = prepare_logs_[reference.shard_id]->EndLsn(reference);
      if (!end_lsn.ok()) return end_lsn.status();
      wal_safe_lsns[reference.shard_id] = std::max(
          wal_safe_lsns[reference.shard_id], end_lsn.ValueOrDie());
    }
  }
  // Commit serialization keeps uncommitted prepares out of this window, but
  // a decision in the retained suffix can still refer to an older physical
  // prepare segment. Do not advance a shard's reclamation boundary past the
  // first such segment.
  std::vector<uint32_t> retained_prepare_segments(
      prepare_logs_.size(), UINT32_MAX);
  for (const CommitDecision& decision : decision_log_.commits()) {
    if (decision.commit_seq <= target_checkpoint) continue;
    for (const PrepareReference& reference : decision.prepares) {
      if (reference.shard_id >= retained_prepare_segments.size()) {
        return Status::Corruption("checkpoint", "decision references unknown shard");
      }
      retained_prepare_segments[reference.shard_id] = std::min(
          retained_prepare_segments[reference.shard_id],
          static_cast<uint32_t>(reference.lsn >> 32));
    }
  }
  for (size_t shard = 0; shard < wal_safe_lsns.size(); ++shard) {
    const uint32_t candidate_segment = static_cast<uint32_t>(wal_safe_lsns[shard] >> 32);
    const uint32_t retained_segment = retained_prepare_segments[shard];
    if (retained_segment < candidate_segment) {
      const uint64_t retained_segment_start =
          static_cast<uint64_t>(retained_segment) << 32;
      if (previous_wal_safe_lsns[shard] > retained_segment_start) {
        return Status::Corruption("checkpoint", "retained decision precedes WAL safety boundary");
      }
      wal_safe_lsns[shard] = std::max(
          previous_wal_safe_lsns[shard], retained_segment_start);
    }
  }
  if (outcomes.size() != target_checkpoint) {
    return Status::Corruption("checkpoint", "incomplete checkpoint outcome prefix");
  }

  const std::string relative_path = "checkpoints/TXN-OUTCOMES-" +
      std::to_string(target_checkpoint) + ".toi";
  std::array<uint8_t, 32> checksum{};
  const Status outcome_written = WriteTransactionOutcomeIndex(
      db_path_ + "/" + relative_path, outcomes, &checksum);
  if (!outcome_written.ok()) return outcome_written;

  DurableCheckpoint checkpoint;
  checkpoint.checkpoint_seq = target_checkpoint;
  checkpoint.decision_safe_seq = target_checkpoint;
  checkpoint.outcome_index_relative_path = relative_path;
  checkpoint.outcome_index_checksum = checksum;
  checkpoint.wal_safe_lsns = std::move(wal_safe_lsns);
  VersionEdit edit;
  edit.checkpoint = std::move(checkpoint);
  const Status published = version_set_.ApplyEdit(edit);
  if (!published.ok()) return published;

  checkpoint_outcomes_ = std::move(outcomes);
  const Status decision_rewritten = decision_log_.CheckpointThrough(target_checkpoint);
  if (!decision_rewritten.ok()) return decision_rewritten;
  // The Manifest checkpoint now durably records each shard's safe prepare
  // position, and the DecisionLog suffix no longer references the covered
  // prefix.  Removing only older whole segments keeps suffix references
  // stable while allowing durable WAL space reclamation.
  const std::vector<uint64_t>& published_safe_lsns =
      version_set_.Snapshot()->checkpoint.wal_safe_lsns;
  for (size_t shard = 0; shard < prepare_logs_.size(); ++shard) {
    const Status truncated = prepare_logs_[shard]->TruncateThrough(
        published_safe_lsns[shard]);
    if (!truncated.ok()) return truncated;
  }
  return commit_timeline_.Checkpoint();
}

StatusOr<uint64_t> TransactionCoordinator::AllocateVertexId() {
  std::lock_guard<std::mutex> publication_lock(flush_mutex_);
  const Status allowed = CheckMutationAllowed("vertex id");
  if (!allowed.ok()) return allowed;
  return vertex_id_allocator_.Allocate();
}

Status TransactionCoordinator::PopulateTcypherContext(TcypherExecutionContext* context) const {
  if (context == nullptr) {
    return Status::InvalidArgument("T-Cypher snapshot", "missing execution context");
  }
  std::scoped_lock<std::mutex, std::mutex> publication_lock(commit_mutex_, flush_mutex_);
  context->visible_seq_ceiling = visible_prefix_.visible_seq();
  context->schema_snapshot = schema_registry_.Snapshot();
  context->version_snapshot = version_set_.Snapshot();
  context->statistics_snapshot = stats_snapshot_store_.Pin();
  context->blob_reader_epoch = context->version_snapshot->generation;
  const uint64_t wall_clock_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::system_clock::now().time_since_epoch()).count());
  const Status statement_hlc = commit_timeline_.Allocate(
      wall_clock_us, &context->statement_start_hlc);
  if (!statement_hlc.ok()) return statement_hlc;
  context->runtime_sources_from_snapshot = true;
  context->committed_events.clear();
  context->session_overlay_events.clear();
  context->sst_event_sources.clear();
  context->memtable_event_sources.clear();
  for (const SstFileMeta& file : context->version_snapshot->files) {
    context->sst_event_sources.push_back(
        PinnedSstSource{file, db_path_ + "/" + file.relative_path});
  }
  for (const auto& shard : storage_shards_) {
    std::vector<std::shared_ptr<const TemporalMemTable>> pinned =
        shard->PinUnflushedMemtables();
    context->memtable_event_sources.insert(
        context->memtable_event_sources.end(), pinned.begin(), pinned.end());
  }
  uint64_t coverage_generation = 0;
  for (const IndexFragment& fragment : context->version_snapshot->index_fragments) {
    coverage_generation = std::max(coverage_generation, fragment.catalog_generation);
  }
  context->index_catalog_snapshot = std::make_shared<IndexCatalogSnapshot>(
      IndexCatalogSnapshot{context->version_snapshot->generation,
                           context->version_snapshot->index_definitions,
                           context->version_snapshot->index_fragments,
                           coverage_generation,
                           context->statistics_snapshot->statistics_snapshot_id()});
  context->index_sources.clear();
  context->delta_index_sources.clear();
  for (const IndexDefinition& definition :
       context->index_catalog_snapshot->definitions) {
    if (definition.state != IndexState::kActive ||
        !IsSupportedIndexCanonicalEncoding(
            definition.canonical_encoding_id)) {
      continue;
    }
    for (const IndexFragment& fragment :
         context->index_catalog_snapshot->fragments) {
      if (!fragment.usable || fragment.index_id != definition.index_id) continue;
      const auto file = std::find_if(
          context->sst_event_sources.begin(), context->sst_event_sources.end(),
          [&fragment](const PinnedSstSource& source) {
            return source.metadata.file_number == fragment.source_sst_id;
          });
      if (file == context->sst_event_sources.end()) continue;
      TcypherIndexSource source;
      source.index_id = definition.index_id;
      source.source_sst_id = fragment.source_sst_id;
      source.pinned_sst_source = *file;
      source.definition = definition;
      source.fragment = fragment;
      source.sidecar_path = db_path_ + "/" + fragment.relative_path;
      context->index_sources.push_back(std::move(source));
    }
    for (const auto& memtable : context->memtable_event_sources) {
      if (!memtable) continue;
      TcypherDeltaIndexSource source;
      source.index_id = definition.index_id;
      source.source_generation = context->visible_seq_ceiling;
      source.pinned_memtable = memtable;
      source.definition = definition;
      context->delta_index_sources.push_back(std::move(source));
    }
  }
  return Status::OK();
}

Status TransactionCoordinator::InstallDecision(
    const CommitDecision& decision,
    bool respect_published_watermarks,
    const std::function<void(uint64_t, uint32_t)>& participant_hook,
    const std::function<Status(uint64_t, uint32_t)>& participant_fault) {
  if (decision.prepares.empty()) {
    return Status::Corruption("decision", "commit has no participant prepares");
  }
  struct ValidatedParticipant {
    uint32_t shard_id = 0;
    bool covered_by_published_sst = false;
    PrepareRecord prepare;
  };
  std::vector<ValidatedParticipant> participants;
  participants.reserve(decision.prepares.size());
  std::set<uint32_t> unique_shards;
  for (const PrepareReference& ref : decision.prepares) {
    if (ref.shard_id >= prepare_logs_.size() ||
        ref.shard_id >= storage_shards_.size() ||
        ref.shard_id >= published_commit_watermarks_.size()) {
      return Status::Corruption("decision", "shard reference is out of range");
    }
    if (!unique_shards.insert(ref.shard_id).second) {
      return Status::Corruption("decision", "duplicate participant shard reference");
    }
    PrepareRecord prepare;
    Status status = prepare_logs_[ref.shard_id]->Read(ref, &prepare);
    if (!status.ok()) return status;
    if (prepare.txn_id != decision.txn_id)
      return Status::Corruption("decision", "prepare transaction mismatch");
    participants.push_back(ValidatedParticipant{
        ref.shard_id,
        respect_published_watermarks &&
            decision.commit_seq <= published_commit_watermarks_[ref.shard_id],
        std::move(prepare)});
  }

  std::vector<std::function<Status()>> install_tasks;
  install_tasks.reserve(participants.size());
  for (size_t index = 0; index < participants.size(); ++index) {
    install_tasks.push_back([&, index]() {
      const ValidatedParticipant& participant = participants[index];
      if (participant_hook) {
        participant_hook(decision.commit_seq, participant.shard_id);
      }
      if (participant_fault) {
        const Status fault =
            participant_fault(decision.commit_seq, participant.shard_id);
        if (!fault.ok()) return fault;
      }
      if (!participant.covered_by_published_sst) {
        const Status installed =
            storage_shards_[participant.shard_id]->InstallCommitted(
                decision.txn_id, decision.commit_seq,
                participant.prepare.events);
        if (!installed.ok()) return installed;
      }
      RefreshMemtableBlobReferences(participant.shard_id);
      return Status::OK();
    });
  }
  const Status installed =
      RunCommitCriticalTasks(std::move(install_tasks));
  if (!installed.ok()) return installed;
  visible_prefix_.MarkInstalled(decision.commit_seq);
  return Status::OK();
}

Status TransactionCoordinator::Commit(uint64_t snapshot_seq,
                                      const std::vector<PendingEvent>& events,
                                      uint64_t* commit_seq) {
  if (commit_seq == nullptr) {
    return Status::InvalidArgument("transaction", "missing commit sequence output");
  }
  const CommitResult result = CommitWithResult(snapshot_seq, events);
  if (result.outcome == CommitOutcome::kCommitted) {
    *commit_seq = result.commit_seq;
    return Status::OK();
  }
  if (result.outcome == CommitOutcome::kAborted) return result.reason;
  return Status::Indeterminate(
      "transaction", "resolve transaction " + std::to_string(result.txn_id));
}

CommitResult TransactionCoordinator::CommitWithResult(
    uint64_t snapshot_seq, const std::vector<PendingEvent>& events) {
  const auto start = std::chrono::steady_clock::now();
  RecordTransactionMeasurement(TransactionMeasurementEvent{
      TransactionMeasurementKind::kStarted, TransactionMeasurementMode::kSnapshot});
  const CommitResult result = CommitInternal(snapshot_seq, events, {}, false);
  RecordTransactionMeasurement(TransactionMeasurementEvent{
      TransactionMeasurementKind::kTerminal, TransactionMeasurementMode::kSnapshot,
      result.outcome == CommitOutcome::kCommitted
          ? TransactionMeasurementOutcome::kCommitted
          : result.outcome == CommitOutcome::kIndeterminate
                ? TransactionMeasurementOutcome::kIndeterminate
                : TransactionMeasurementOutcome::kAborted,
      result.reason.ok() ? "" : MeasurementReason(result.reason), ElapsedNs(start)});
  return result;
}

Status TransactionCoordinator::CommitStrict(
    uint64_t snapshot_seq, const std::vector<PendingEvent>& events,
    const std::vector<StrictReadPoint>& reads, uint64_t* commit_seq) {
  if (commit_seq == nullptr) {
    return Status::InvalidArgument("transaction", "missing commit sequence output");
  }
  const CommitResult result = CommitStrictWithResult(snapshot_seq, events, reads);
  if (result.outcome == CommitOutcome::kCommitted) {
    *commit_seq = result.commit_seq;
    return Status::OK();
  }
  if (result.outcome == CommitOutcome::kAborted) return result.reason;
  return Status::Indeterminate(
      "transaction", "resolve transaction " + std::to_string(result.txn_id));
}

CommitResult TransactionCoordinator::CommitStrictWithResult(
    uint64_t snapshot_seq, const std::vector<PendingEvent>& events,
    const std::vector<StrictReadPoint>& reads) {
  const auto start = std::chrono::steady_clock::now();
  RecordTransactionMeasurement(TransactionMeasurementEvent{
      TransactionMeasurementKind::kStarted, TransactionMeasurementMode::kStrict});
  const CommitResult result = CommitInternal(snapshot_seq, events, reads, true);
  RecordTransactionMeasurement(TransactionMeasurementEvent{
      TransactionMeasurementKind::kTerminal, TransactionMeasurementMode::kStrict,
      result.outcome == CommitOutcome::kCommitted
          ? TransactionMeasurementOutcome::kCommitted
          : result.outcome == CommitOutcome::kIndeterminate
                ? TransactionMeasurementOutcome::kIndeterminate
                : TransactionMeasurementOutcome::kAborted,
      result.reason.ok() ? "" : MeasurementReason(result.reason), ElapsedNs(start)});
  return result;
}

TransactionMeasurementSnapshot TransactionCoordinator::transaction_measurements() const {
  std::lock_guard<std::mutex> lock(transaction_measurements_mutex_);
  return transaction_measurements_;
}

void TransactionCoordinator::SetTransactionMeasurementSink(
    TransactionMeasurementSink sink) {
  std::lock_guard<std::mutex> lock(transaction_measurements_mutex_);
  transaction_measurement_sink_ = std::move(sink);
}

void TransactionCoordinator::SetTransactionMeasurementsEnabled(bool enabled) {
  std::lock_guard<std::mutex> lock(transaction_measurements_mutex_);
  transaction_measurements_enabled_ = enabled;
  transaction_measurements_.available = enabled;
  transaction_measurements_.availability_reason = enabled
      ? "" : "minimal_instrumentation";
}

void TransactionCoordinator::RecordTransactionMeasurement(
    const TransactionMeasurementEvent& event) {
  TransactionMeasurementSink sink;
  {
    std::lock_guard<std::mutex> lock(transaction_measurements_mutex_);
    if (!transaction_measurements_enabled_) return;
    switch (event.kind) {
      case TransactionMeasurementKind::kStarted:
        transaction_measurements_.started = SaturatingAdd(
            transaction_measurements_.started, 1);
        break;
      case TransactionMeasurementKind::kTerminal:
        if (event.outcome == TransactionMeasurementOutcome::kCommitted) {
          transaction_measurements_.committed = SaturatingAdd(
              transaction_measurements_.committed, 1);
        } else if (event.outcome == TransactionMeasurementOutcome::kIndeterminate) {
          transaction_measurements_.indeterminate = SaturatingAdd(
              transaction_measurements_.indeterminate, 1);
        } else {
          transaction_measurements_.aborted = SaturatingAdd(
              transaction_measurements_.aborted, 1);
          if (event.reason == "serialization_conflict") {
            transaction_measurements_.conflicts = SaturatingAdd(
                transaction_measurements_.conflicts, 1);
          }
        }
        transaction_measurements_.commit_latency.Observe(event.duration_ns);
        break;
      case TransactionMeasurementKind::kPrepareLatency:
        transaction_measurements_.prepare_latency.Observe(event.duration_ns);
        break;
      case TransactionMeasurementKind::kDecisionLatency:
        transaction_measurements_.decision_latency.Observe(event.duration_ns);
        break;
      case TransactionMeasurementKind::kDecisionFsyncLatency:
        transaction_measurements_.decision_fsync_latency.Observe(event.duration_ns);
        break;
      case TransactionMeasurementKind::kVisiblePrefixWait:
        if (event.outcome == TransactionMeasurementOutcome::kSucceeded) {
          transaction_measurements_.visible_prefix_wait_success.Observe(event.duration_ns);
          const uint64_t sample_index =
              transaction_measurements_.visible_prefix_lag_sample_count;
          transaction_measurements_.visible_prefix_lag_samples[
              sample_index % kTransactionMeasurementLagSampleCapacity] = event.lag_seq;
          transaction_measurements_.visible_prefix_lag_sample_count = SaturatingAdd(
              transaction_measurements_.visible_prefix_lag_sample_count, 1);
          if (event.nonzero_stall) {
            transaction_measurements_.visible_prefix_nonzero_stalls = SaturatingAdd(
                transaction_measurements_.visible_prefix_nonzero_stalls, 1);
          }
        } else {
          transaction_measurements_.visible_prefix_wait_failure.Observe(event.duration_ns);
        }
        break;
    }
    sink = transaction_measurement_sink_;
  }
  if (sink) {
    try {
      sink(event);
    } catch (...) {
      // Observability is never permitted to alter a transaction outcome.
    }
  }
}

CommitResult TransactionCoordinator::CommitInternal(
    uint64_t snapshot_seq, const std::vector<PendingEvent>& events,
    const std::vector<StrictReadPoint>& strict_reads, bool strict_mode) {
  std::unique_lock<std::mutex> commit_lock(commit_mutex_);
  if (!opened_) {
    return CommitResult::Aborted(
        Status::InvalidArgument("transaction", "coordinator is not open"));
  }
  if (recovery_required()) {
    return CommitResult::Aborted(Status::RecoveryRequired(
        "transaction", "reopen database before accepting another commit"));
  }
  if (events.empty()) {
    return CommitResult::Aborted(
        Status::InvalidArgument("transaction", "missing events"));
  }
  if (snapshot_seq > visible_prefix_.visible_seq()) {
    return CommitResult::Aborted(
        Status::InvalidArgument("transaction", "snapshot exceeds visible prefix"));
  }
  std::set<std::pair<LogicalKey, uint64_t>> event_identities;
  for (const PendingEvent& event : events) {
    if (!event_identities.emplace(event.logical_key, event.valid_from).second) {
      return CommitResult::Aborted(Status::InvalidArgument(
          "transaction", "duplicate logical event identity in one commit"));
    }
  }

  // Admission happens before validation and durable PREPARE work. Once a
  // transaction crosses that boundary, its critical completion reserve keeps
  // it independent from subsequent pressure transitions.
  const PressureController::Decision pressure = RefreshPressure();
  if (!pressure.admit_writes) {
    commit_lock.unlock();
    ApplyPressureActions(pressure);
    return CommitResult::Aborted(Status::WriteStalled(
        "transaction", "write admission is stalled by storage pressure"));
  }

  std::vector<uint32_t> participants;
  participants.reserve(events.size());
  for (const PendingEvent& event : events) {
    const auto schema = schema_registry_.Lookup(event.logical_key.entity_type(),
                                                event.logical_key.schema_column_id(),
                                                event.schema_epoch);
    if (!schema.has_value())
      return CommitResult::Aborted(
          Status::SchemaMismatch("transaction", "unregistered schema epoch"));
    if (event.operation == TemporalOperation::kDelete && event.blob_ref.has_value()) {
      return CommitResult::Aborted(
          Status::InvalidArgument("transaction", "DELETE cannot carry a BlobRef"));
    }
    if (event.operation == TemporalOperation::kPut && event.blob_ref.has_value()) {
      if (schema->physical_type != PhysicalType::kString &&
          schema->physical_type != PhysicalType::kBinary) {
        return CommitResult::Aborted(Status::SchemaMismatch(
            "transaction", "BlobRef requires string or binary schema"));
      }
      const auto payload = blob_store_.Get(*event.blob_ref);
      if (!payload.ok()) return CommitResult::Aborted(payload.status());
      if (payload.ValueOrDie().size() != event.blob_ref->raw_length) {
        return CommitResult::Aborted(
            Status::BlobCorruption("transaction", "BlobRef length mismatch"));
      }
    } else if (event.operation == TemporalOperation::kPut) {
      const Status validation = schema_registry_.Validate(*schema, event.value);
      if (!validation.ok()) return CommitResult::Aborted(validation);
    }
    participants.push_back(shard_directory_.ShardFor(event.logical_key));
  }
  std::sort(participants.begin(), participants.end());
  participants.erase(std::unique(participants.begin(), participants.end()), participants.end());

  std::vector<PendingEvent> durable_events = events;
  const auto allocated_txn_id = transaction_id_allocator_.Allocate();
  if (!allocated_txn_id.ok()) return CommitResult::Aborted(allocated_txn_id.status());
  const uint64_t txn_id = allocated_txn_id.ValueOrDie();
  SystemHlc system_time_hlc;
  Status status = Status::OK();

  std::vector<PendingEvent> projected_durable_events = durable_events;
  std::vector<std::string> projected_blob_payloads;
  for (PendingEvent& event : projected_durable_events) {
    const auto schema = schema_registry_.Lookup(event.logical_key.entity_type(),
                                                event.logical_key.schema_column_id(),
                                                event.schema_epoch);
    if (schema.has_value() && event.operation == TemporalOperation::kPut &&
        !event.blob_ref.has_value() &&
        (schema->physical_type == PhysicalType::kString ||
         schema->physical_type == PhysicalType::kBinary)) {
      const std::string& payload = std::get<std::string>(event.value.data());
      if (payload.size() > schema->blob_threshold) {
        projected_blob_payloads.push_back(payload);
        event = PendingEvent::PutBlob(
            event.logical_key, event.valid_from, event.schema_epoch,
            BlobRef{Blake3Hash(payload), payload.size(), BlobLocation{0, 1, 0}});
      }
    }
  }
  const auto blob_write_estimate = blob_store_.EstimateProtectedPutWrites(
      projected_blob_payloads, kBlobSegmentTargetBytes);
  if (!blob_write_estimate.ok()) {
    return CommitResult::Aborted(blob_write_estimate.status());
  }

  std::map<uint32_t, std::vector<PendingEvent>> estimated_by_shard;
  for (const PendingEvent& event : projected_durable_events) {
    estimated_by_shard[shard_directory_.ShardFor(event.logical_key)].push_back(event);
  }
  std::vector<PrepareRecord> estimated_prepares;
  estimated_prepares.reserve(estimated_by_shard.size());
  for (const auto& entry : estimated_by_shard) {
    estimated_prepares.push_back(
        PrepareRecord{txn_id, snapshot_seq, participants, entry.second});
  }
  const auto durable_write_estimate =
      EstimateDurableCommitWriteBytes(estimated_prepares);
  if (!durable_write_estimate.ok()) {
    return CommitResult::Aborted(durable_write_estimate.status());
  }
  const auto manifest_write_bytes = version_set_.EstimateManifestRewriteBytes(
      blob_write_estimate.ValueOrDie().manifest_rewrites,
      blob_write_estimate.ValueOrDie().additional_manifest_segments);
  if (!manifest_write_bytes.ok()) {
    return CommitResult::Aborted(manifest_write_bytes.status());
  }

  PreparedCompletionGrant completion_grant;
  completion_grant.profile = ResourceProfile{
      0, 0,
      SaturatingAdd(SaturatingAdd(static_cast<uint64_t>(participants.size()), 1),
                    blob_write_estimate.ValueOrDie().descriptors),
      0, 1, 0, 0,
      SaturatingAdd(durable_write_estimate.ValueOrDie().total_bytes,
                    SaturatingAdd(blob_write_estimate.ValueOrDie().total_bytes,
                                  manifest_write_bytes.ValueOrDie())),
      SaturatingAdd(
          SaturatingAdd(static_cast<uint64_t>(participants.size()), 1),
          SaturatingAdd(blob_write_estimate.ValueOrDie().metadata_ops,
                        blob_write_estimate.ValueOrDie().manifest_rewrites))};
  if (resource_governor_ != nullptr) {
    auto acquired = resource_governor_->Acquire(
        completion_grant.profile, true);
    if (!acquired.ok()) return CommitResult::Aborted(acquired.status());
    completion_grant.resources =
        std::move(acquired).ConsumeValueOrDie();
  }
  if (io_governor_ != nullptr) {
    const Status io = io_governor_->TryAcquire(
        IoTokenRequest{0, 0, completion_grant.profile.write_bytes,
                       completion_grant.profile.metadata_ops, true},
        MonotonicNowNs());
    if (!io.ok()) return CommitResult::Aborted(io);
  }

  bool blob_segments_manifested = false;
  std::vector<size_t> blob_event_indexes;
  std::vector<std::string> blob_payloads;
  for (size_t event_index = 0; event_index < durable_events.size();
       ++event_index) {
    PendingEvent& event = durable_events[event_index];
    const auto schema = schema_registry_.Lookup(event.logical_key.entity_type(),
                                                event.logical_key.schema_column_id(),
                                                event.schema_epoch);
    if (!schema.has_value()) {
      return CommitResult::Aborted(
          Status::SchemaMismatch("transaction", "unregistered schema epoch"));
    }
    if (event.operation == TemporalOperation::kPut && !event.blob_ref.has_value()) {
      if ((schema->physical_type == PhysicalType::kString ||
           schema->physical_type == PhysicalType::kBinary) &&
          std::get<std::string>(event.value.data()).size() > schema->blob_threshold) {
        blob_event_indexes.push_back(event_index);
        blob_payloads.push_back(std::get<std::string>(event.value.data()));
      }
    }
  }
  if (!blob_payloads.empty()) {
    Status manifest = EnsureBlobSegmentsManifested();
    if (!manifest.ok()) return CommitResult::Aborted(manifest);
    blob_segments_manifested = true;
    const auto references = blob_store_.PutBatch(blob_payloads);
    if (!references.ok()) {
      if (blob_store_.requires_reopen()) {
        recovery_required_.store(true, std::memory_order_release);
      }
      return CommitResult::Aborted(references.status());
    }
    if (references.ValueOrDie().size() != blob_event_indexes.size()) {
      return CommitResult::Aborted(Status::Corruption(
          "transaction", "Blob batch result count mismatch"));
    }
    for (size_t index = 0; index < blob_event_indexes.size(); ++index) {
      PendingEvent& event = durable_events[blob_event_indexes[index]];
      event = PendingEvent::PutBlob(
          event.logical_key, event.valid_from, event.schema_epoch,
          references.ValueOrDie()[index]);
    }
  }

  // Blob segments are sealed only between transactions. The old active
  // segment was Manifest-live before any BlobRef was written; after rotating,
  // reconcile publishes the new active identity before another prepare can
  // reference it.
  if (blob_segments_manifested) {
    const auto rotate = blob_store_.ActiveSegmentsNeedRotation(kBlobSegmentTargetBytes);
    if (!rotate.ok()) return CommitResult::Aborted(rotate.status());
    if (rotate.ValueOrDie()) {
      const Status rotated =
          TrackBlobMutation(blob_store_.RotateActiveSegments());
      if (!rotated.ok()) return CommitResult::Aborted(rotated);
      const Status manifested = ReconcileBlobSegments();
      if (!manifested.ok()) return CommitResult::Aborted(manifested);
    }
  }

  std::map<uint32_t, std::vector<PendingEvent>> by_shard;
  for (const PendingEvent& event : durable_events) {
    by_shard[shard_directory_.ShardFor(event.logical_key)].push_back(event);
  }
  std::vector<std::pair<uint32_t, PrepareRecord>> prepare_records;
  prepare_records.reserve(by_shard.size());
  for (const auto& entry : by_shard) {
    prepare_records.emplace_back(
        entry.first,
        PrepareRecord{txn_id, snapshot_seq, participants, entry.second});
  }
  std::vector<PrepareReference> references;
  references.reserve(prepare_records.size());

  std::vector<uint32_t> reservation_shards = participants;
  for (const StrictReadPoint& read : strict_reads) {
    reservation_shards.push_back(
        shard_directory_.ShardFor(read.logical_key));
  }
  std::sort(reservation_shards.begin(), reservation_shards.end());
  reservation_shards.erase(
      std::unique(reservation_shards.begin(), reservation_shards.end()),
      reservation_shards.end());
  std::vector<StorageShard::ValidationGuard> reservation_guards;
  reservation_guards.reserve(reservation_shards.size());
  for (uint32_t shard : reservation_shards) {
    reservation_guards.push_back(storage_shards_[shard]->LockValidation());
  }
  const std::function<void(uint32_t)> validation_hook =
      validation_started_hook_;
  const std::function<void()> reservation_hook =
      reservation_installed_hook_;
  const std::function<Status(CommitFaultPoint)> commit_fault_injector =
      commit_fault_injector_;
  const std::function<void(uint64_t, bool)> decision_install_hook =
      decision_install_hook_;
  const std::function<Status(uint64_t)> decision_install_fault_injector =
      decision_install_fault_injector_;
  const std::function<void(uint64_t, uint32_t)> participant_install_hook =
      participant_install_hook_;
  const std::function<Status(uint64_t, uint32_t)>
      participant_install_fault_injector =
          participant_install_fault_injector_;
  commit_lock.unlock();
  if (validation_hook) {
    for (uint32_t shard : reservation_shards) validation_hook(shard);
  }

  const auto committed_snapshot = SnapshotCommittedEvents();
  if (!committed_snapshot.ok()) {
    return CommitResult::Aborted(committed_snapshot.status());
  }
  const std::vector<TemporalEvent>& committed = committed_snapshot.ValueOrDie();
  const Status conflict = ValidateSnapshotWrites(
      committed, durable_events, snapshot_seq);
  if (!conflict.ok()) return CommitResult::Aborted(conflict);
  const Status strict_conflict = ValidateStrictReads(
      committed, strict_reads, snapshot_seq);
  if (!strict_conflict.ok()) return CommitResult::Aborted(strict_conflict);
  const std::vector<StorageShard::WriteReservation> write_intervals =
      BuildWriteIntervals(committed, durable_events);

  struct ReservationBatch {
    std::vector<StorageShard::ReadReservation> reads;
    std::vector<StorageShard::WriteReservation> writes;
  };
  std::map<uint32_t, ReservationBatch> reservation_batches;
  for (const StrictReadPoint& read : strict_reads) {
    reservation_batches[shard_directory_.ShardFor(read.logical_key)]
        .reads.push_back(StorageShard::ReadReservation{
            read.logical_key, read.valid_time});
  }
  for (const StorageShard::WriteReservation& write : write_intervals) {
    reservation_batches[shard_directory_.ShardFor(write.logical_key)]
        .writes.push_back(write);
  }
  size_t reservation_index = 0;
  for (const auto& entry : reservation_batches) {
    const Status reservation_status =
        storage_shards_[entry.first]->ValidateReservationLocked(
            reservation_guards[reservation_index], txn_id,
            entry.second.reads, entry.second.writes);
    if (!reservation_status.ok()) {
      return CommitResult::Aborted(reservation_status);
    }
    ++reservation_index;
  }
  reservation_index = 0;
  for (auto& entry : reservation_batches) {
    storage_shards_[entry.first]->InstallReservationLocked(
        reservation_guards[reservation_index], txn_id,
        std::move(entry.second.reads), std::move(entry.second.writes));
    ++reservation_index;
  }
  reservation_guards.clear();
  const auto release_prepared_reservation = [&]() {
    std::vector<StorageShard::ValidationGuard> guards;
    guards.reserve(reservation_shards.size());
    for (uint32_t shard : reservation_shards) {
      guards.push_back(storage_shards_[shard]->LockValidation());
    }
    for (size_t index = 0; index < reservation_shards.size(); ++index) {
      storage_shards_[reservation_shards[index]]->ReleaseReservationLocked(
          guards[index], txn_id);
    }
  };
  if (reservation_hook) reservation_hook();

  references.resize(prepare_records.size());
  std::vector<Status> prepare_statuses(prepare_records.size(), Status::OK());
  std::vector<std::function<Status()>> prepare_tasks;
  prepare_tasks.reserve(prepare_records.size());
  for (size_t index = 0; index < prepare_records.size(); ++index) {
    prepare_tasks.push_back([&, index]() {
      prepare_statuses[index] =
          prepare_logs_[prepare_records[index].first]->Append(
              prepare_records[index].second, &references[index]);
      return prepare_statuses[index];
    });
  }
  const auto prepare_start = std::chrono::steady_clock::now();
  const Status prepared = RunCommitCriticalTasks(std::move(prepare_tasks));
  RecordTransactionMeasurement(TransactionMeasurementEvent{
      TransactionMeasurementKind::kPrepareLatency,
      strict_mode ? TransactionMeasurementMode::kStrict
                  : TransactionMeasurementMode::kSnapshot,
      prepared.ok() ? TransactionMeasurementOutcome::kSucceeded
                    : TransactionMeasurementOutcome::kFailed,
      "", ElapsedNs(prepare_start)});
  if (!prepared.ok()) {
    for (size_t index = 0; index < prepare_statuses.size(); ++index) {
      if (!prepare_statuses[index].ok() &&
          prepare_logs_[prepare_records[index].first]->requires_reopen()) {
        recovery_required_.store(true, std::memory_order_release);
      }
    }
    release_prepared_reservation();
    return CommitResult::Aborted(prepared);
  }
  for (size_t index = 0; index < prepare_statuses.size(); ++index) {
    status = prepare_statuses[index];
    if (!status.ok()) {
      if (prepare_logs_[prepare_records[index].first]->requires_reopen()) {
        recovery_required_.store(true, std::memory_order_release);
      }
      release_prepared_reservation();
      return CommitResult::Aborted(status);
    }
  }
  if (commit_fault_injector) {
    status = commit_fault_injector(CommitFaultPoint::kAfterPrepareDurable);
    if (!status.ok()) {
      release_prepared_reservation();
      return CommitResult::Aborted(status);
    }
  }
  commit_lock.lock();
  if (recovery_required()) {
    release_prepared_reservation();
    return CommitResult::Aborted(Status::RecoveryRequired(
        "transaction", "reopen database before creating a durable decision"));
  }
  const uint64_t wall_clock_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::system_clock::now().time_since_epoch()).count());
  status = commit_timeline_.Allocate(wall_clock_us, &system_time_hlc);
  if (!status.ok()) {
    release_prepared_reservation();
    return CommitResult::Aborted(status);
  }
  const auto require_recovery = [this, txn_id](Status failure) {
    recovery_required_.store(true, std::memory_order_release);
    {
      std::lock_guard<std::mutex> lock(installation_wait_mutex_);
      installation_cv_.notify_all();
    }
    return CommitResult::Indeterminate(txn_id, std::move(failure));
  };
  const auto decision_start = std::chrono::steady_clock::now();
  const DecisionAppendResult decision_append =
      decision_log_.AppendCommitWithResult(txn_id, references, system_time_hlc);
  const TransactionMeasurementMode measurement_mode = strict_mode
      ? TransactionMeasurementMode::kStrict
      : TransactionMeasurementMode::kSnapshot;
  const TransactionMeasurementOutcome decision_outcome = decision_append.status.ok()
      ? TransactionMeasurementOutcome::kSucceeded
      : TransactionMeasurementOutcome::kFailed;
  RecordTransactionMeasurement(TransactionMeasurementEvent{
      TransactionMeasurementKind::kDecisionLatency, measurement_mode,
      decision_outcome, "", ElapsedNs(decision_start)});
  if (decision_append.fsync_attempted) {
    RecordTransactionMeasurement(TransactionMeasurementEvent{
        TransactionMeasurementKind::kDecisionFsyncLatency, measurement_mode,
        decision_outcome, "", decision_append.fsync_latency_ns});
  }
  if (!decision_append.status.ok()) {
    if (decision_append.requires_reopen) {
      recovery_required_.store(true, std::memory_order_release);
      std::lock_guard<std::mutex> lock(installation_wait_mutex_);
      installation_cv_.notify_all();
    }
    if (decision_append.may_be_durable) {
      return require_recovery(decision_append.status);
    }
    release_prepared_reservation();
    return CommitResult::Aborted(decision_append.status);
  }
  const uint64_t assigned_commit_seq = decision_append.commit_seq;
  bool reservation_found = true;
  {
    std::vector<StorageShard::ValidationGuard> guards;
    guards.reserve(reservation_shards.size());
    for (uint32_t shard : reservation_shards) {
      guards.push_back(storage_shards_[shard]->LockValidation());
    }
    for (size_t index = 0; index < reservation_shards.size(); ++index) {
      reservation_found =
          storage_shards_[reservation_shards[index]]
              ->MarkReservationCommittedPendingLocked(guards[index], txn_id) &&
          reservation_found;
    }
  }
  if (!reservation_found) {
    return require_recovery(Status::Corruption(
        "transaction", "durable transaction lost its prepared reservation"));
  }

  if (commit_fault_injector) {
    status = commit_fault_injector(CommitFaultPoint::kAfterDecisionDurable);
    if (!status.ok()) return require_recovery(status);
  }
  status = commit_timeline_.AddDurableCommit(assigned_commit_seq, system_time_hlc);
  if (!status.ok()) return require_recovery(status);
  const CommitDecision durable_decision = decision_log_.commits().back();
  commit_lock.unlock();

  if (decision_install_hook) {
    decision_install_hook(assigned_commit_seq, true);
  }
  if (decision_install_fault_injector) {
    status = decision_install_fault_injector(assigned_commit_seq);
    if (!status.ok()) return require_recovery(status);
  }
  status = InstallDecision(durable_decision, false, participant_install_hook,
                           participant_install_fault_injector);
  if (!status.ok()) return require_recovery(status);
  if (decision_install_hook) {
    decision_install_hook(assigned_commit_seq, false);
  }
  {
    std::lock_guard<std::mutex> lock(installation_wait_mutex_);
    installation_cv_.notify_all();
  }
  const auto visible_prefix_wait_start = std::chrono::steady_clock::now();
  {
    std::unique_lock<std::mutex> lock(installation_wait_mutex_);
    installation_cv_.wait(lock, [&]() {
      return visible_prefix_.visible_seq() >= assigned_commit_seq ||
             recovery_required();
    });
  }
  const uint64_t visible_prefix_wait_ns = ElapsedNs(visible_prefix_wait_start);
  if (visible_prefix_.visible_seq() < assigned_commit_seq) {
    RecordTransactionMeasurement(TransactionMeasurementEvent{
        TransactionMeasurementKind::kVisiblePrefixWait, measurement_mode,
        TransactionMeasurementOutcome::kFailed, "", visible_prefix_wait_ns});
    return CommitResult::Indeterminate(
        txn_id, Status::RecoveryRequired(
                    "transaction", "visible prefix stopped during installation"));
  }
  const uint64_t visible_seq = visible_prefix_.visible_seq();
  RecordTransactionMeasurement(TransactionMeasurementEvent{
      TransactionMeasurementKind::kVisiblePrefixWait, measurement_mode,
      TransactionMeasurementOutcome::kSucceeded, "", visible_prefix_wait_ns,
      assigned_commit_seq >= visible_seq ? assigned_commit_seq - visible_seq : 0,
      visible_prefix_wait_ns != 0});
  release_prepared_reservation();

  AtomicSaturatingAdd(&wal_bytes_written_,
                      durable_write_estimate.ValueOrDie().prepare_bytes);
  AtomicSaturatingAdd(&decision_log_bytes_written_,
                      durable_write_estimate.ValueOrDie().decision_bytes);
  AtomicSaturatingAdd(&blob_durable_bytes_written_,
                      blob_write_estimate.ValueOrDie().total_bytes);
  const uint64_t logical_bytes = LogicalEventBytes(events);
  AtomicSaturatingAdd(&logical_committed_bytes_, logical_bytes);
  AtomicSaturatingAdd(&logical_live_bytes_, logical_bytes);

  const PressureController::Decision post_commit_pressure = RefreshPressure();
  // Remediation is advisory after the durable commit result is fixed. Failure
  // leaves its storage inputs live for a later retry and cannot revoke the
  // already published transaction.
  ApplyPressureActions(post_commit_pressure);
  return CommitResult::Committed(assigned_commit_seq, txn_id);
}

PressureSignals TransactionCoordinator::CollectPressureSignals() const {
  PressureSignals signals;
  for (const auto& shard : storage_shards_) {
    const MemtableUsage usage = shard->memtable_usage();
    signals.memtable_bytes = SaturatingAdd(signals.memtable_bytes, usage.total_bytes());
  }
  for (const auto& prepare_log : prepare_logs_) {
    signals.wal_backlog_bytes = SaturatingAdd(
        signals.wal_backlog_bytes, prepare_log->retained_bytes());
  }
  signals.compaction_debt_bytes = CompactionDebtBytes(*version_set_.Snapshot(), 4);
  if (cache_manager_ != nullptr) {
    const CacheStats cache = cache_manager_->stats();
    signals.cache_pressure_bytes = std::max(cache.resident_bytes, cache.pinned_bytes);
  }
  uint64_t disk_available_bytes = 0;
  if (disk_available_bytes_provider_) {
    disk_available_bytes = disk_available_bytes_provider_();
  } else {
    std::error_code error;
    disk_available_bytes = std::filesystem::space(db_path_, error).available;
    if (error) disk_available_bytes = 0;
  }
  signals.disk_pressure_bytes =
      disk_available_bytes >= kDiskSafetyReserveBytes
          ? 0
          : kDiskSafetyReserveBytes - disk_available_bytes;
  return signals;
}

PressureController::Decision TransactionCoordinator::RefreshPressure() {
  std::lock_guard<std::mutex> lock(pressure_mutex_);
  last_pressure_signals_ = CollectPressureSignals();
  if (pressure_controller_ == nullptr) {
    last_pressure_state_ = PressureState::kNormal;
    return PressureController::Decision{};
  }
  PressureController::Decision decision = pressure_controller_->Update(last_pressure_signals_);
  if (decision.state != PressureState::kNormal && cache_manager_ != nullptr) {
    cache_manager_->EvictAllUnpinned();
    last_pressure_signals_ = CollectPressureSignals();
    decision = pressure_controller_->Update(last_pressure_signals_);
  }
  last_pressure_state_ = decision.state;
  return decision;
}

PressureSignals TransactionCoordinator::pressure_signals() const {
  std::lock_guard<std::mutex> lock(pressure_mutex_);
  return CollectPressureSignals();
}

PressureState TransactionCoordinator::pressure_state() const {
  std::lock_guard<std::mutex> lock(pressure_mutex_);
  return last_pressure_state_;
}

void TransactionCoordinator::ApplyPressureActions(
    const PressureController::Decision& decision) {
  if (decision.cancel_queued_analytical && work_execution_service_ != nullptr) {
    work_execution_service_->CancelQueued(WorkClass::kAnalyticalQuery);
  }
  if (decision.require_flush) Flush().IgnoreError();
  if (decision.require_urgent_compaction) {
    CompactWithClass(WorkClass::kCompactionUrgent).IgnoreError();
  }
  if (decision.require_blob_gc) CollectBlobGarbage().IgnoreError();
}

Status TransactionCoordinator::AdmitQuery(bool analytical) {
  std::unique_lock<std::mutex> commit_lock(commit_mutex_);
  if (!opened_) return Status::InvalidArgument("query admission", "coordinator is not open");
  if (recovery_required()) {
    return Status::RecoveryRequired(
        "query admission", "reopen database before accepting another query");
  }
  const PressureController::Decision decision = RefreshPressure();
  Status admission = Status::OK();
  if (analytical && !decision.admit_analytical) {
    admission = Status::ResourceExhausted(
        "query admission", "analytical query rejected by current storage pressure");
  }
  commit_lock.unlock();
  ApplyPressureActions(decision);
  return admission;
}

std::optional<Value> TransactionCoordinator::Get(const LogicalKey& key,
                                                  uint64_t valid_time,
                                                  uint64_t snapshot_seq) const {
  const auto result = GetChecked(key, valid_time, snapshot_seq);
  return result.ok() ? result.ValueOrDie() : std::nullopt;
}

StatusOr<std::optional<Value>> TransactionCoordinator::GetChecked(
    const LogicalKey& key, uint64_t valid_time, uint64_t snapshot_seq) const {
  if (!opened_) return Status::InvalidArgument("read", "coordinator is not open");
  if (recovery_required()) {
    return Status::RecoveryRequired(
        "read", "reopen database before observing committed state");
  }
  const uint64_t cutoff = snapshot_seq == 0 ? visible_prefix_.visible_seq() : snapshot_seq;
  if (cutoff > visible_prefix_.visible_seq())
    return Status::InvalidArgument("read", "snapshot exceeds visible prefix");
  const auto read_event = [&](const LogicalKey& candidate)
      -> StatusOr<std::optional<TemporalEvent>> {
    SstReadStats stats;
    auto result = MergeTemporalReadEvent(
        db_path_, *version_set_.Snapshot(),
        storage_shards_[shard_directory_.ShardFor(candidate)]->SnapshotUnflushedEvents(),
        candidate, valid_time, cutoff, cache_manager_, &stats, io_governor_);
    RecordSstReadStats(stats);
    return result;
  };
  const bool is_edge = key.entity_type() == EntityType::EdgeOut ||
                       key.entity_type() == EntityType::EdgeIn;
  auto event = read_event(key);
  if (!event.ok()) return event.status();
  if (is_edge && !event.ValueOrDie().has_value()) {
    const EntityType peer_direction = key.entity_type() == EntityType::EdgeOut
        ? EntityType::EdgeIn : EntityType::EdgeOut;
    const LogicalKey peer = key.kind() == LogicalKeyKind::kExistence
        ? LogicalKey::EdgeExistence(key.entity_id(), key.target_id(), key.edge_type(),
                                    key.edge_id(), peer_direction)
        : LogicalKey::EdgeProperty(key.entity_id(), key.target_id(), key.edge_type(),
                                   key.edge_id(), key.column_id(), peer_direction);
    event = read_event(peer);
    if (!event.ok()) return event.status();
  }
  if (!event.ValueOrDie().has_value() || event.ValueOrDie()->is_delete()) {
    return std::optional<Value>{};
  }
  if (is_edge) {
    const LogicalKey& stored_key = event.ValueOrDie()->logical_key();
    if (stored_key.kind() == LogicalKeyKind::kProperty) {
      const LogicalKey existence = LogicalKey::EdgeExistence(
          stored_key.entity_id(), stored_key.target_id(), stored_key.edge_type(),
          stored_key.edge_id(), stored_key.entity_type());
      auto existence_event = read_event(existence);
      if (!existence_event.ok()) return existence_event.status();
      if (!existence_event.ValueOrDie().has_value()) {
        const EntityType peer_direction = stored_key.entity_type() == EntityType::EdgeOut
            ? EntityType::EdgeIn : EntityType::EdgeOut;
        existence_event = read_event(LogicalKey::EdgeExistence(
            stored_key.entity_id(), stored_key.target_id(), stored_key.edge_type(),
            stored_key.edge_id(), peer_direction));
        if (!existence_event.ok()) return existence_event.status();
      }
      if (!existence_event.ValueOrDie().has_value() ||
          existence_event.ValueOrDie()->is_delete()) {
        return std::optional<Value>{};
      }
    }
    for (uint64_t endpoint : {stored_key.entity_id(), stored_key.target_id()}) {
      const auto endpoint_event = read_event(LogicalKey::VertexExistence(endpoint));
      if (!endpoint_event.ok()) return endpoint_event.status();
      if (!endpoint_event.ValueOrDie().has_value() ||
          endpoint_event.ValueOrDie()->is_delete()) {
        return std::optional<Value>{};
      }
    }
  }
  if (!event.ValueOrDie()->is_blob_reference()) {
    return std::optional<Value>{event.ValueOrDie()->value()};
  }
  const auto materialized = MaterializeBlobValue(*event.ValueOrDie());
  if (!materialized.ok()) return materialized.status();
  if (!materialized.ValueOrDie().has_value()) {
    return Status::BlobCorruption("read", "BlobRef materialized no value");
  }
  return materialized.ValueOrDie();
}

StatusOr<TransactionCoordinator::StrictReadPoint>
TransactionCoordinator::CaptureStrictReadPoint(const LogicalKey& key,
                                                uint64_t valid_time,
                                                uint64_t snapshot_seq) const {
  if (!opened_) return Status::InvalidArgument("transaction", "coordinator is not open");
  if (recovery_required()) {
    return Status::RecoveryRequired(
        "transaction", "reopen database before capturing a strict read");
  }
  if (snapshot_seq > visible_prefix_.visible_seq()) {
    return Status::InvalidArgument(
        "transaction", "snapshot exceeds visible prefix");
  }
  const auto committed = SnapshotCommittedEvents();
  if (!committed.ok()) return committed.status();
  return BuildStrictReadPoint(committed.ValueOrDie(), key, valid_time,
                              snapshot_seq);
}

StatusOr<std::vector<TransactionCoordinator::StrictReadPoint>>
TransactionCoordinator::CaptureStrictEdgeReadSet(const LogicalKey& edge_key,
                                                 uint64_t valid_time,
                                                 uint64_t snapshot_seq) const {
  if (edge_key.entity_type() != EntityType::EdgeOut &&
      edge_key.entity_type() != EntityType::EdgeIn) {
    return Status::InvalidArgument(
        "transaction", "strict edge read requires an edge logical key");
  }
  if (!opened_) return Status::InvalidArgument("transaction", "coordinator is not open");
  if (recovery_required()) {
    return Status::RecoveryRequired(
        "transaction", "reopen database before capturing a strict edge read");
  }
  if (snapshot_seq > visible_prefix_.visible_seq()) {
    return Status::InvalidArgument(
        "transaction", "snapshot exceeds visible prefix");
  }
  const auto committed = SnapshotCommittedEvents();
  if (!committed.ok()) return committed.status();

  std::vector<StrictReadPoint> reads;
  reads.reserve(edge_key.kind() == LogicalKeyKind::kProperty ? 4 : 3);
  reads.push_back(BuildStrictReadPoint(
      committed.ValueOrDie(), edge_key, valid_time, snapshot_seq));
  if (edge_key.kind() == LogicalKeyKind::kProperty) {
    reads.push_back(BuildStrictReadPoint(
        committed.ValueOrDie(),
        LogicalKey::EdgeExistence(
            edge_key.entity_id(), edge_key.target_id(), edge_key.edge_type(),
            edge_key.edge_id(), edge_key.entity_type()),
        valid_time, snapshot_seq));
  }
  reads.push_back(BuildStrictReadPoint(
      committed.ValueOrDie(), LogicalKey::VertexExistence(edge_key.entity_id()),
      valid_time, snapshot_seq));
  if (edge_key.target_id() != edge_key.entity_id()) {
    reads.push_back(BuildStrictReadPoint(
        committed.ValueOrDie(),
        LogicalKey::VertexExistence(edge_key.target_id()), valid_time,
        snapshot_seq));
  }
  return reads;
}

StatusOr<std::optional<Value>> TransactionCoordinator::MaterializeBlobValue(
    const TemporalEvent& event) const {
  if (!event.is_blob_reference()) {
    return Status::InvalidArgument("read", "event is not a BlobRef");
  }
  const BlobRef& reference = *event.blob_ref();
  std::string payload_bytes;
  const CacheKey cache_key{CacheKind::kBlobValue, BlobHashHex(reference.content_hash)};
  const CacheHandle cached = cache_manager_ == nullptr ? CacheHandle{} : cache_manager_->Lookup(cache_key);
  if (cached) {
    payload_bytes = *cached.value();
  } else {
    const auto payload = blob_store_.Get(reference);
    if (!payload.ok()) return payload.status();
    payload_bytes = payload.ValueOrDie();
    if (cache_manager_ != nullptr) {
      const auto inserted = cache_manager_->Insert(
          cache_key, std::make_shared<const std::string>(payload_bytes), CacheAdmission::kPointRead);
      if (!inserted.ok() && !inserted.status().IsQueryMemoryLimit()) return inserted.status();
    }
  }
  if (payload_bytes.size() != reference.raw_length) {
    return Status::BlobCorruption("read", "cached BlobRef length mismatch");
  }
  const LogicalKey& stored_key = event.logical_key();
  const auto schema = schema_registry_.Lookup(stored_key.entity_type(), stored_key.schema_column_id(),
                                               event.schema_epoch());
  if (!schema.has_value()) return Status::SchemaMismatch("read", "BlobRef schema epoch missing");
  if (schema->physical_type == PhysicalType::kString) {
    return std::optional<Value>{Value::String(payload_bytes)};
  }
  if (schema->physical_type == PhysicalType::kBinary) {
    return std::optional<Value>{Value::Binary(payload_bytes)};
  }
  return Status::BlobCorruption("read", "BlobRef has non-variable-length schema");
}

void TransactionCoordinator::RecordSstReadStats(
    const SstReadStats& stats) const {
  AtomicSaturatingAdd(&page_bytes_decoded_, stats.page_bytes_decoded);
  AtomicSaturatingAdd(&page_bytes_skipped_, stats.page_bytes_skipped);
  AtomicSaturatingAdd(&sst_physical_bytes_read_, stats.bytes_read);
  AtomicSaturatingAdd(&page_decode_count_, stats.page_decode_count);
  AtomicSaturatingAdd(&page_decode_latency_ns_, stats.page_decode_latency_ns);
}

StorageRuntimeStats TransactionCoordinator::storage_stats() const {
  const BlobStoreStats blob = blob_store_.stats();
  StorageRuntimeStats stats;
  stats.page_bytes_decoded =
      page_bytes_decoded_.load(std::memory_order_relaxed);
  stats.page_bytes_skipped =
      page_bytes_skipped_.load(std::memory_order_relaxed);
  stats.sst_physical_bytes_read =
      sst_physical_bytes_read_.load(std::memory_order_relaxed);
  stats.page_decode_count = page_decode_count_.load(std::memory_order_relaxed);
  stats.page_decode_latency_ns =
      page_decode_latency_ns_.load(std::memory_order_relaxed);
  stats.blob_payload_bytes_read = blob.payload_bytes_read;
  stats.blob_payload_bytes_written = blob.payload_bytes_written;
  stats.blob_payload_bytes_deduplicated = blob.payload_bytes_deduplicated;
  stats.blob_lookup_count = blob.lookup_count;
  stats.blob_lookup_latency_ns = blob.lookup_latency_ns;
  stats.compaction_input_bytes =
      compaction_input_bytes_.load(std::memory_order_relaxed);
  stats.compaction_output_bytes =
      compaction_output_bytes_.load(std::memory_order_relaxed);
  stats.compaction_blob_payload_bytes_read =
      compaction_blob_payload_bytes_read_.load(std::memory_order_relaxed);
  stats.compaction_peak_buffered_events =
      compaction_peak_buffered_events_.load(std::memory_order_relaxed);
  stats.compaction_peak_buffered_bytes =
      compaction_peak_buffered_bytes_.load(std::memory_order_relaxed);
  stats.blob_gc_live_bytes = blob.gc_live_bytes;
  stats.blob_gc_rewritten_bytes = blob.gc_rewritten_bytes;
  for (size_t slot = 0; slot < kPageTypeMetricSlots; ++slot) {
    stats.page_uncompressed_bytes_written[slot] =
        page_uncompressed_bytes_written_[slot].load(std::memory_order_relaxed);
    stats.page_stored_bytes_written[slot] =
        page_stored_bytes_written_[slot].load(std::memory_order_relaxed);
  }
  return stats;
}

StatusOr<BenchmarkStorageStats>
TransactionCoordinator::benchmark_storage_stats(
    bool include_logical_live_bytes) const {
  uint64_t live_physical_bytes = 0;
  std::error_code error;
  const bool exists = std::filesystem::exists(db_path_, error);
  if (error) return Status::IOError(db_path_, error.message());
  if (exists) {
    for (std::filesystem::recursive_directory_iterator file(db_path_, error), end;
         !error && file != end; file.increment(error)) {
      if (!file->is_regular_file(error)) {
        if (error) break;
        continue;
      }
      const uint64_t size = file->file_size(error);
      if (error) break;
      live_physical_bytes = SaturatingAdd(live_physical_bytes, size);
    }
  }
  if (error) return Status::IOError(db_path_, error.message());
  uint64_t logical_live_bytes =
      logical_live_bytes_.load(std::memory_order_relaxed);
  if (include_logical_live_bytes && logical_live_bytes == 0) {
    const auto live_events = SnapshotCommittedEvents();
    if (!live_events.ok()) return live_events.status();
    logical_live_bytes = LogicalEventBytes(live_events.ValueOrDie());
  }
  return BenchmarkStorageStats{
      wal_bytes_written_.load(std::memory_order_relaxed),
      decision_log_bytes_written_.load(std::memory_order_relaxed),
      sst_flush_bytes_written_.load(std::memory_order_relaxed),
      compaction_output_bytes_.load(std::memory_order_relaxed),
      blob_durable_bytes_written_.load(std::memory_order_relaxed),
      version_set_.durable_bytes_written(),
      logical_committed_bytes_.load(std::memory_order_relaxed),
      live_physical_bytes,
      logical_live_bytes};
}

StatusOr<uint64_t> TransactionCoordinator::ResolveSystemTimeAsOf(
    uint64_t timestamp_us, uint64_t visible_seq_ceiling) const {
  if (!opened_) return Status::InvalidArgument("commit timeline", "coordinator is not open");
  const uint64_t ceiling = visible_seq_ceiling == 0 ? visible_prefix_.visible_seq()
                                                    : visible_seq_ceiling;
  if (ceiling > visible_prefix_.visible_seq()) {
    return Status::InvalidArgument("commit timeline", "snapshot exceeds visible prefix");
  }
  return commit_timeline_.ResolveAsOf(timestamp_us, ceiling);
}

StatusOr<std::optional<uint64_t>> TransactionCoordinator::ResolveTransaction(
    uint64_t txn_id) const {
  if (!opened_) return Status::InvalidArgument("transaction", "coordinator is not open");
  if (const auto live = decision_log_.Resolve(txn_id); live.has_value()) {
    return std::optional<uint64_t>{live->commit_seq};
  }
  const auto checkpointed = std::find_if(
      checkpoint_outcomes_.begin(), checkpoint_outcomes_.end(), [txn_id](const auto& outcome) {
        return outcome.txn_id == txn_id;
      });
  if (checkpointed == checkpoint_outcomes_.end()) return std::optional<uint64_t>{};
  return std::optional<uint64_t>{checkpointed->commit_seq};
}

StatusOr<std::vector<TemporalEvent>> TransactionCoordinator::SnapshotCommittedEvents() const {
  std::vector<TemporalEvent> events;
  const std::shared_ptr<const VersionSnapshot> snapshot = version_set_.Snapshot();
  for (const SstFileMeta& file : snapshot->files) {
    const auto persisted = ReadSstFile(db_path_ + "/" + file.relative_path);
    if (!persisted.ok()) return persisted.status();
    events.insert(events.end(), persisted.ValueOrDie().begin(),
                  persisted.ValueOrDie().end());
  }
  for (const auto& shard : storage_shards_) {
    std::vector<TemporalEvent> shard_events = shard->SnapshotUnflushedEvents();
    events.insert(events.end(), shard_events.begin(), shard_events.end());
  }
  return events;
}

}  // namespace cedar
#include "cedar/runtime/resource_profile.h"
