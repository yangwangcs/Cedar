// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#ifndef CEDAR_TRANSACTION_TRANSACTION_COORDINATOR_H_
#define CEDAR_TRANSACTION_TRANSACTION_COORDINATOR_H_

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "cedar/blob/blob_store.h"
#include "cedar/blob/blob_reference_catalog.h"
#include "cedar/columnar/sst.h"
#include "cedar/index/index_sidecar.h"
#include "cedar/storage/storage_shard.h"
#include "cedar/schema/schema_registry.h"
#include "cedar/transaction/decision_log.h"
#include "cedar/transaction/transaction_measurements.h"
#include "cedar/transaction/commit_timeline.h"
#include "cedar/transaction/logical_id_allocator.h"
#include "cedar/transaction/shard_directory.h"
#include "cedar/transaction/visible_prefix.h"
#include "cedar/storage/version_set.h"
#include "cedar/index/index_catalog.h"
#include "cedar/runtime/pressure_controller.h"
#include "cedar/runtime/maintenance_executor.h"
#include "cedar/runtime/resource_profile.h"
#include "cedar/statistics/stats_snapshot.h"

namespace cedar {

enum class CommitOutcome : uint8_t {
  kCommitted = 0,
  kAborted = 1,
  kIndeterminate = 2,
};

struct CommitResult {
  CommitOutcome outcome = CommitOutcome::kAborted;
  uint64_t commit_seq = 0;
  uint64_t txn_id = 0;
  Status reason = Status::InvalidArgument("transaction", "commit was not attempted");

  static CommitResult Committed(uint64_t commit_seq, uint64_t txn_id) {
    return CommitResult{CommitOutcome::kCommitted, commit_seq, txn_id, Status::OK()};
  }
  static CommitResult Aborted(Status reason) {
    return CommitResult{CommitOutcome::kAborted, 0, 0, std::move(reason)};
  }
  static CommitResult Indeterminate(uint64_t txn_id, Status reason) {
    return CommitResult{CommitOutcome::kIndeterminate, 0, txn_id, std::move(reason)};
  }
};

enum class CommitFaultPoint : uint8_t {
  kAfterPrepareDurable = 0,
  kAfterDecisionDurable = 1,
};

struct PreparedCompletionGrant {
  ResourceLease resources;
  ResourceProfile profile;
};

struct StorageRuntimeStats {
  uint64_t page_bytes_decoded = 0;
  uint64_t page_bytes_skipped = 0;
  uint64_t sst_physical_bytes_read = 0;
  uint64_t page_decode_count = 0;
  uint64_t page_decode_latency_ns = 0;
  uint64_t blob_payload_bytes_read = 0;
  uint64_t blob_payload_bytes_written = 0;
  uint64_t blob_payload_bytes_deduplicated = 0;
  uint64_t blob_lookup_count = 0;
  uint64_t blob_lookup_latency_ns = 0;
  uint64_t compaction_input_bytes = 0;
  uint64_t compaction_output_bytes = 0;
  uint64_t compaction_blob_payload_bytes_read = 0;
  uint64_t blob_gc_live_bytes = 0;
  uint64_t blob_gc_rewritten_bytes = 0;
  std::array<uint64_t, kPageTypeMetricSlots> page_uncompressed_bytes_written{};
  std::array<uint64_t, kPageTypeMetricSlots> page_stored_bytes_written{};
};

enum class IndexHealthFailureClass : uint8_t {
  kMissingSidecar = 0,
  kCorruptSidecar = 1,
  kPostingCountMismatch = 2,
};

struct IndexHealthStats {
  uint64_t event_count = 0;
  uint64_t repair_schedule_count = 0;
  uint64_t repair_failure_count = 0;
  uint64_t pending_repair_count = 0;
};

struct BenchmarkStorageStats {
  uint64_t wal_bytes_written = 0;
  uint64_t decision_log_bytes_written = 0;
  uint64_t sst_flush_bytes_written = 0;
  uint64_t compaction_bytes_written = 0;
  uint64_t blob_bytes_written = 0;
  uint64_t manifest_bytes_written = 0;
  uint64_t logical_committed_bytes = 0;
  uint64_t live_physical_bytes = 0;
  uint64_t logical_live_bytes = 0;

  uint64_t physical_durable_bytes_written() const;
};

// Canonical durable transaction entry point for the current event and
// decision-log implementation.
class TransactionCoordinator {
 public:
  TransactionCoordinator(std::string db_path, uint32_t shard_count,
                         uint64_t hash_seed);
  ~TransactionCoordinator();

  Status Open();
  Status RegisterColumn(const ColumnSchema& schema, ColumnSchema* registered);
  Status RegisterIndex(IndexDefinition definition, uint64_t* index_id);
  Status SetIndexState(uint64_t index_id, IndexState state);
  Status RepairIndexes();
  Status ReportIndexHealthEvent(uint64_t index_id, uint64_t source_sst_id,
                                uint64_t catalog_generation,
                                IndexHealthFailureClass failure_class);
  IndexHealthStats index_health_stats() const;
  Status DropIndex(uint64_t index_id);
  IndexCatalogSnapshot index_catalog_snapshot() const;
  StatusOr<StatsSnapshot> StatsFor(EntityType entity_type, uint16_t column_id) const;
  Status Flush();
  Status Compact();
  Status RotateBlobSegments();
  Status CollectBlobGarbage();
  StatusOr<ResourceProfile> EstimateBlobGarbageCollectionResources();
  Status CheckpointDurableLogs();
  Status CheckpointCommitTimeline();
  void SetResourceGovernor(class ResourceGovernor* governor) {
    resource_governor_ = governor;
    ConfigureMaintenanceExecutor().IgnoreError();
  }
  void SetIoGovernor(class IoGovernor* governor) {
    io_governor_ = governor;
    ConfigureMaintenanceExecutor().IgnoreError();
  }
  void SetCacheManager(class CacheManager* cache_manager) { cache_manager_ = cache_manager; }
  Status SetWorkExecutionService(
      class WorkExecutionService* execution_service);
  void SetPressureController(PressureController* controller) { pressure_controller_ = controller; }
  void SetDiskAvailableBytesProviderForTesting(
      std::function<uint64_t()> provider) {
    std::lock_guard<std::mutex> lock(pressure_mutex_);
    disk_available_bytes_provider_ = std::move(provider);
  }
  PressureSignals pressure_signals() const;
  PressureState pressure_state() const;
  Status AdmitQuery(bool analytical);
  StatusOr<uint64_t> AllocateVertexId();
  Status Commit(uint64_t snapshot_seq, const std::vector<PendingEvent>& events,
                uint64_t* commit_seq);
  CommitResult CommitWithResult(uint64_t snapshot_seq,
                                const std::vector<PendingEvent>& events);
  struct StrictReadPoint {
    StrictReadPoint() : logical_key(LogicalKey::VertexProperty(0, 0)) {}
    StrictReadPoint(LogicalKey key, uint64_t time)
        : logical_key(std::move(key)), valid_time(time) {}
    LogicalKey logical_key;
    uint64_t valid_time = 0;
    // The event and fences observed at the transaction snapshot.  Keeping the
    // immutable event identity here prevents a caller from reducing a read
    // dependency to only a key and valid-time pair.
    std::optional<TemporalEvent> observed_event;
    std::optional<uint64_t> predecessor_fence;
    std::optional<uint64_t> successor_fence;
  };
  StatusOr<StrictReadPoint> CaptureStrictReadPoint(const LogicalKey& key,
                                                   uint64_t valid_time,
                                                   uint64_t snapshot_seq) const;
  StatusOr<std::vector<StrictReadPoint>> CaptureStrictEdgeReadSet(
      const LogicalKey& edge_key, uint64_t valid_time,
      uint64_t snapshot_seq) const;
  Status CommitStrict(uint64_t snapshot_seq, const std::vector<PendingEvent>& events,
                      const std::vector<StrictReadPoint>& reads, uint64_t* commit_seq);
  CommitResult CommitStrictWithResult(uint64_t snapshot_seq,
                                      const std::vector<PendingEvent>& events,
                                      const std::vector<StrictReadPoint>& reads);
  TransactionMeasurementSnapshot transaction_measurements() const;
  void SetTransactionMeasurementSink(TransactionMeasurementSink sink);
  void SetTransactionMeasurementsEnabled(bool enabled);
  bool recovery_required() const {
    return recovery_required_.load(std::memory_order_acquire) ||
           version_set_.requires_reopen();
  }
  void SetCommitFaultInjectorForTesting(
      std::function<Status(CommitFaultPoint)> injector) {
    commit_fault_injector_ = std::move(injector);
  }
  void SetReservationInstalledHookForTesting(std::function<void()> hook) {
    std::lock_guard<std::mutex> lock(commit_mutex_);
    reservation_installed_hook_ = std::move(hook);
  }
  void SetValidationStartedHookForTesting(
      std::function<void(uint32_t)> hook) {
    std::lock_guard<std::mutex> lock(commit_mutex_);
    validation_started_hook_ = std::move(hook);
  }
  void SetFlushExecutionHookForTesting(std::function<void()> hook) {
    std::lock_guard<std::mutex> lock(flush_mutex_);
    flush_execution_hook_ = std::move(hook);
  }
  void SetBlobRotationExecutionHookForTesting(std::function<void()> hook) {
    std::lock_guard<std::mutex> lock(flush_mutex_);
    blob_rotation_execution_hook_ = std::move(hook);
  }
  void SetRecoveryExecutionHookForTesting(std::function<void()> hook) {
    std::lock_guard<std::mutex> lock(recovery_mutex_);
    recovery_execution_hook_ = std::move(hook);
  }
  void SetMaintenanceCancellationObserverForTesting(
      WorkCancellation::CheckpointObserverForTesting observer) {
    maintenance_executor_.SetCancellationObserverForTesting(
        std::move(observer));
  }
  void SetDecisionInstallHookForTesting(
      std::function<void(uint64_t, bool)> hook) {
    std::lock_guard<std::mutex> lock(commit_mutex_);
    decision_install_hook_ = std::move(hook);
  }
  void SetDecisionInstallFaultInjectorForTesting(
      std::function<Status(uint64_t)> injector) {
    std::lock_guard<std::mutex> lock(commit_mutex_);
    decision_install_fault_injector_ = std::move(injector);
  }
  void SetParticipantInstallHookForTesting(
      std::function<void(uint64_t, uint32_t)> hook) {
    std::lock_guard<std::mutex> lock(commit_mutex_);
    participant_install_hook_ = std::move(hook);
  }
  void SetParticipantInstallFaultInjectorForTesting(
      std::function<Status(uint64_t, uint32_t)> injector) {
    std::lock_guard<std::mutex> lock(commit_mutex_);
    participant_install_fault_injector_ = std::move(injector);
  }
  void SetDecisionLogFaultInjectorForTesting(
      std::function<Status(DecisionLogFaultPoint)> injector) {
    decision_log_.SetFaultInjectorForTesting(std::move(injector));
  }
  void SetPrepareLogFaultInjectorForTesting(
      const std::function<Status(DecisionLogFaultPoint)>& injector) {
    for (const auto& prepare_log : prepare_logs_) {
      prepare_log->SetFaultInjectorForTesting(injector);
    }
  }
  void SetBlobIndexFaultInjectorForTesting(
      std::function<Status(BlobStoreFaultPoint)> injector) {
    blob_store_.SetIndexFaultInjectorForTesting(std::move(injector));
  }
  void SetSstPublicationFaultInjectorForTesting(
      std::function<Status(SstPublicationFaultPoint)> injector) {
    sst_publication_fault_injector_ = std::move(injector);
  }
  void SetIndexSidecarPublicationFaultInjectorForTesting(
      std::function<Status(IndexSidecarPublicationFaultPoint)> injector) {
    index_sidecar_publication_fault_injector_ = std::move(injector);
  }
  void SetVersionSetFaultInjectorForTesting(
      std::function<Status(VersionSetFaultPoint)> injector) {
    version_set_.SetFaultInjectorForTesting(std::move(injector));
  }
  std::optional<Value> Get(const LogicalKey& key, uint64_t valid_time,
                           uint64_t snapshot_seq = 0) const;
  StatusOr<std::optional<Value>> GetChecked(const LogicalKey& key,
                                            uint64_t valid_time,
                                            uint64_t snapshot_seq = 0) const;
  StatusOr<std::optional<Value>> MaterializeBlobValue(
      const TemporalEvent& event) const;
  uint64_t visible_seq() const { return visible_prefix_.visible_seq(); }
  StatusOr<uint64_t> ResolveSystemTimeAsOf(uint64_t timestamp_us,
                                           uint64_t visible_seq_ceiling = 0) const;
  StatusOr<std::optional<uint64_t>> ResolveTransaction(uint64_t txn_id) const;
  // Captures every mutable input that a T-Cypher statement needs while commit
  // and flush publication are held stable. Invalid or incomplete sidecars are
  // omitted so the executor conservatively uses its base path.
  Status PopulateTcypherContext(struct TcypherExecutionContext* context) const;
  StatusOr<std::vector<TemporalEvent>> SnapshotCommittedEvents() const;
  const CommitTimeline& commit_timeline() const { return commit_timeline_; }
  std::shared_ptr<const SchemaSnapshot> schema_snapshot() const {
    return schema_registry_.Snapshot();
  }
  std::shared_ptr<const VersionSnapshot> version_snapshot() const {
    return version_set_.Snapshot();
  }
  StorageRuntimeStats storage_stats() const;
  StatusOr<BenchmarkStorageStats> benchmark_storage_stats(
      bool include_logical_live_bytes = false) const;

 private:
  CommitResult CommitInternal(uint64_t snapshot_seq,
                              const std::vector<PendingEvent>& events,
                              const std::vector<StrictReadPoint>& strict_reads,
                              bool strict_mode);
  void RecordTransactionMeasurement(const TransactionMeasurementEvent& event);
  std::vector<StorageShard::WriteReservation> BuildWriteIntervals(
      const std::vector<TemporalEvent>& committed,
      const std::vector<PendingEvent>& pending) const;
  Status BuildIndexFragmentsForFiles(
      const std::vector<SstFileMeta>& files,
      const std::shared_ptr<WorkCancellation>& cancellation = nullptr,
      uint64_t expected_generation = UINT64_MAX);
  Status BuildStatsFragmentsForFiles(
      const std::vector<SstFileMeta>& files,
      const std::shared_ptr<WorkCancellation>& cancellation = nullptr,
      uint64_t expected_generation = UINT64_MAX);
  Status ScheduleIndexBuild(const std::vector<SstFileMeta>& files);
  Status ScheduleStatsMerge(const std::vector<SstFileMeta>& files);
  Status CompactWithClass(WorkClass work_class);
  Status CompactInternal(
      const std::shared_ptr<WorkCancellation>& cancellation = nullptr);
  Status FlushFrozenShard(StorageShard* shard,
                          const std::vector<TemporalEvent>& events,
                          uint64_t estimated_write_bytes);
  Status CollectBlobGarbageInternal(
      const std::vector<BlobHash>& live_hashes,
      const ResourceProfile& admitted_resources,
      const std::shared_ptr<WorkCancellation>& cancellation = nullptr);
  StatusOr<ResourceProfile> EstimateBlobRotationResourcesLocked() const;
  StatusOr<ResourceProfile> EstimateBlobGarbageCollectionResourcesLocked(
      const std::vector<BlobHash>& live_hashes) const;
  Status CheckpointDurableLogsInternal();
  Status EnsureWorkExecutionService();
  Status OpenInternal();
  Status RunCommitCriticalTasks(
      std::vector<std::function<Status()>> tasks);
  Status ConfigureMaintenanceExecutor() {
    return maintenance_executor_.Configure(
        work_execution_service_, resource_governor_, io_governor_);
  }
  Status RestorePublishedSstEvents();
  Status CleanupOrphanSsts();
  Status InstallDecision(
      const CommitDecision& decision,
      bool respect_published_watermarks = true,
      const std::function<void(uint64_t, uint32_t)>& participant_hook = {},
      const std::function<Status(uint64_t, uint32_t)>& participant_fault = {});
  void RefreshMemtableBlobReferences(uint32_t shard_id);
  Status ReclaimRetiredSsts();
  Status TrackBlobMutation(Status status);
  Status ReconcileBlobSegments();
  Status EnsureBlobSegmentsManifested();
  Status ReclaimRetiredBlobSegments();
  Status LoadCheckpointOutcomes();
  uint64_t LargestCheckpointableSequence() const;
  void RetireCompactionInputs(const std::shared_ptr<const VersionSnapshot>& snapshot,
                              const std::vector<SstFileMeta>& inputs,
                              uint64_t output_file_number);
  void RecordSstReadStats(const struct SstReadStats& stats) const;
  PressureSignals CollectPressureSignals() const;
  PressureController::Decision RefreshPressure();
  void ApplyPressureActions(const PressureController::Decision& decision);
  Status CheckMutationAllowed(const char* context) const;

  std::string db_path_;
  ShardDirectory shard_directory_;
  BlobStore blob_store_;
  BlobReferenceCatalog blob_reference_catalog_;
  std::vector<std::unique_ptr<ShardPrepareLog>> prepare_logs_;
  std::vector<std::unique_ptr<StorageShard>> storage_shards_;
  DecisionLog decision_log_;
  CommitTimeline commit_timeline_;
  SchemaRegistry schema_registry_;
  VersionSet version_set_;
  StatsSnapshotStore stats_snapshot_store_;
  LogicalIdAllocator vertex_id_allocator_;
  LogicalIdAllocator transaction_id_allocator_;
  VisiblePrefix visible_prefix_;
  uint64_t next_sst_file_number_ = 1;
  uint64_t next_work_id_ = 1;
  std::vector<uint64_t> published_commit_watermarks_;
  std::vector<TransactionOutcome> checkpoint_outcomes_;
  struct RetiredSstSet {
    std::weak_ptr<const VersionSnapshot> pinned_snapshot;
    std::vector<std::string> relative_paths;
    std::string blob_catalog_source;
  };
  std::vector<RetiredSstSet> retired_ssts_;
  struct RetiredBlobSegmentSet {
    std::weak_ptr<const VersionSnapshot> pinned_snapshot;
    std::vector<BlobSegmentId> segments;
  };
  std::vector<RetiredBlobSegmentSet> retired_blob_segments_;
  struct IndexHealthEventKey {
    uint64_t index_id = 0;
    uint64_t source_sst_id = 0;
    uint64_t catalog_generation = 0;
    IndexHealthFailureClass failure_class =
        IndexHealthFailureClass::kCorruptSidecar;

    friend bool operator<(const IndexHealthEventKey& left,
                          const IndexHealthEventKey& right) {
      if (left.index_id != right.index_id) {
        return left.index_id < right.index_id;
      }
      if (left.source_sst_id != right.source_sst_id) {
        return left.source_sst_id < right.source_sst_id;
      }
      if (left.catalog_generation != right.catalog_generation) {
        return left.catalog_generation < right.catalog_generation;
      }
      return static_cast<uint8_t>(left.failure_class) <
          static_cast<uint8_t>(right.failure_class);
    }
  };
  enum class IndexHealthRepairState : uint8_t {
    kPending,
    kCompleted,
    kFailed,
  };
  static constexpr size_t kMaximumIndexHealthEvents = 1024;
  mutable std::mutex index_health_mutex_;
  std::map<IndexHealthEventKey, IndexHealthRepairState> index_health_events_;
  uint64_t index_health_repair_schedule_count_ = 0;
  uint64_t index_health_repair_failure_count_ = 0;
  mutable std::mutex commit_mutex_;
  mutable std::mutex transaction_measurements_mutex_;
  TransactionMeasurementSnapshot transaction_measurements_;
  TransactionMeasurementSink transaction_measurement_sink_;
  bool transaction_measurements_enabled_ = true;
  mutable std::mutex flush_mutex_;
  mutable std::mutex recovery_mutex_;
  mutable std::mutex pressure_mutex_;
  mutable std::mutex installation_wait_mutex_;
  std::condition_variable installation_cv_;
  class ResourceGovernor* resource_governor_ = nullptr;
  class IoGovernor* io_governor_ = nullptr;
  class CacheManager* cache_manager_ = nullptr;
  class WorkExecutionService* work_execution_service_ = nullptr;
  std::shared_ptr<class WorkScheduler> owned_work_scheduler_;
  std::unique_ptr<class WorkExecutionService> owned_work_execution_service_;
  MaintenanceExecutor maintenance_executor_;
  PressureController* pressure_controller_ = nullptr;
  PressureSignals last_pressure_signals_;
  PressureState last_pressure_state_ = PressureState::kNormal;
  std::function<uint64_t()> disk_available_bytes_provider_;
  std::function<void()> flush_execution_hook_;
  std::function<void()> blob_rotation_execution_hook_;
  std::function<void()> recovery_execution_hook_;
  bool opened_ = false;
  std::atomic<bool> recovery_required_{false};
  mutable std::atomic<uint64_t> page_bytes_decoded_{0};
  mutable std::atomic<uint64_t> page_bytes_skipped_{0};
  mutable std::atomic<uint64_t> sst_physical_bytes_read_{0};
  mutable std::atomic<uint64_t> page_decode_count_{0};
  mutable std::atomic<uint64_t> page_decode_latency_ns_{0};
  std::atomic<uint64_t> compaction_input_bytes_{0};
  std::atomic<uint64_t> compaction_output_bytes_{0};
  std::atomic<uint64_t> compaction_blob_payload_bytes_read_{0};
  std::array<std::atomic<uint64_t>, kPageTypeMetricSlots>
      page_uncompressed_bytes_written_{};
  std::array<std::atomic<uint64_t>, kPageTypeMetricSlots>
      page_stored_bytes_written_{};
  std::atomic<uint64_t> wal_bytes_written_{0};
  std::atomic<uint64_t> decision_log_bytes_written_{0};
  std::atomic<uint64_t> sst_flush_bytes_written_{0};
  std::atomic<uint64_t> blob_durable_bytes_written_{0};
  std::atomic<uint64_t> logical_committed_bytes_{0};
  std::atomic<uint64_t> logical_live_bytes_{0};
  std::function<Status(CommitFaultPoint)> commit_fault_injector_;
  std::function<Status(SstPublicationFaultPoint)>
      sst_publication_fault_injector_;
  std::function<Status(IndexSidecarPublicationFaultPoint)>
      index_sidecar_publication_fault_injector_;
  std::function<void()> reservation_installed_hook_;
  std::function<void(uint32_t)> validation_started_hook_;
  std::function<void(uint64_t, bool)> decision_install_hook_;
  std::function<Status(uint64_t)> decision_install_fault_injector_;
  std::function<void(uint64_t, uint32_t)> participant_install_hook_;
  std::function<Status(uint64_t, uint32_t)>
      participant_install_fault_injector_;
};

}  // namespace cedar

#endif  // CEDAR_TRANSACTION_TRANSACTION_COORDINATOR_H_
