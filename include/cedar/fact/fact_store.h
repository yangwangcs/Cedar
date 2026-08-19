// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_FACT_FACT_STORE_H_
#define CEDAR_FACT_FACT_STORE_H_

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/fact/fact.h"
#include "cedar/fact/meta_codec.h"
#include "cedar/runtime/pressure_controller.h"

namespace cedar {
namespace internal {
class DecidedEpoch;
}

inline constexpr uint32_t kMaximumGroupCommitBatchCount = 512;
inline constexpr uint64_t kMaximumGroupCommitBatchBytes = 2ULL * 1024ULL * 1024ULL;

enum class StorageProfile : uint8_t { kDeveloper, kProductionAppend };

struct ProductionStorageOptions {
  uint64_t memory_budget_bytes = 0;
  uint64_t block_cache_bytes = 0;
  uint32_t max_background_jobs = 0;
  uint32_t max_commit_batch_count = kMaximumGroupCommitBatchCount;
  uint64_t max_commit_batch_bytes = kMaximumGroupCommitBatchBytes;
  uint64_t compaction_rate_limit_bytes_per_sec = 0;
  bool kernel_mode = false;
  bool diagnostic_periodic_tasks = false;
  uint32_t recycle_log_file_num = 0;
  // Absolute directory for RocksDB-owned WAL files. Empty retains the data
  // directory default.
  std::string wal_directory;
  // Refuse startup unless wal_directory is provisioned on another device.
  bool require_separate_wal_device = false;
};

struct RuntimeSamplingTiming {
  uint64_t pressure_properties_us = 0;
  uint64_t recovery_wal_bytes_us = 0;
  uint64_t runtime_metrics_properties_us = 0;
  uint64_t snapshot_publish_us = 0;
  uint64_t refresh_total_us = 0;
};

class FactStoreImpl;

struct FactStoreOptions {
  std::string path;
  uint64_t write_buffer_bytes = 64ULL * 1024ULL * 1024ULL;
  uint64_t block_cache_bytes = 256ULL * 1024ULL * 1024ULL;
  uint64_t blob_threshold_bytes = 4096;
  uint32_t group_commit_max_batch_size = 128;
  uint64_t group_commit_window_us = 200;
  std::function<Status()> async_prepare_prewrite_fault_injector_for_testing;
  std::function<Status()> commit_prewrite_fault_injector_for_testing;
  std::function<Status()> commit_fault_injector_for_testing;
  std::function<void(bool sync)> commit_write_options_observer_for_testing;
  std::function<void()> commit_transaction_lookup_observer_for_testing;
  std::function<Status(VacuumFaultPoint)> vacuum_fault_injector_for_testing;
  uint64_t group_commit_max_batch_bytes = 2ULL * 1024ULL * 1024ULL;
  StorageProfile storage_profile = StorageProfile::kDeveloper;
  ProductionStorageOptions production;
  uint64_t validation_cache_bytes = 8ULL * 1024ULL * 1024ULL;
  std::function<void()> validation_scan_observer_for_testing;
  std::function<void()> runtime_sample_observer_for_testing;
  std::function<void()> snapshot_open_observer_for_testing;
  std::function<void(bool)> kernel_write_observer_for_testing;
};

struct SnapshotWriteDependency {
  FactRef ref;
  ValidTime valid_from;
  std::optional<ValidTime> predecessor;
  std::optional<ValidTime> successor;
  CommitSeq snapshot_seq;

  Status Validate() const;
};

struct StrictReadDependency {
  FactRef ref;
  ValidTime valid_time;
  CommitSeq snapshot_seq;
  std::optional<FactEvent> observed_event;
  std::optional<ValidTime> predecessor;
  std::optional<ValidTime> successor;

  Status Validate() const;
};

struct StoreCommitBatch {
  TxnId txn_id;
  uint64_t system_hlc = 0;
  std::vector<PendingFactMutation> mutations;
  std::vector<EdgeIdentity> edge_identities;
  std::vector<SnapshotWriteDependency> snapshot_write_dependencies;
  std::vector<StrictReadDependency> strict_read_dependencies;
  // Set only for a batch created from this process' durably leased transaction
  // ID range. It permits skipping the redundant persisted-outcome lookup; the
  // group still detects duplicates within the epoch.
  bool fresh_transaction_id = false;

  Status Validate() const;
};

struct StoreCommitResult {
  CommitSeq commit_seq;
  uint64_t system_hlc = 0;

  constexpr bool operator==(const StoreCommitResult&) const = default;
};

struct RocksDbRuntimeMetrics {
  enum class ColumnFamilyRole : uint8_t {
    kDefault,
    kFacts,
    kMeta,
    kOther,
  };

  struct ColumnFamilyMetrics {
    uint32_t id = 0;
    ColumnFamilyRole role = ColumnFamilyRole::kOther;
    uint64_t active_memtable_bytes = 0;
    uint64_t immutable_memtable_bytes = 0;
    uint64_t immutable_memtable_count = 0;
    uint64_t oldest_immutable_age_us = 0;
    uint64_t l0_files = 0;
    uint64_t pending_compaction_bytes = 0;
    bool flush_pending = false;
    bool compaction_pending = false;
  };

  uint64_t l0_files = 0;
  uint64_t maintenance_generation = 0;
  uint64_t pending_compaction_bytes = 0;
  uint64_t immutable_memtable_bytes = 0;
  uint64_t immutable_memtable_count = 0;
  uint64_t active_memtable_bytes = 0;
  uint64_t total_active_memtable_bytes = 0;
  uint64_t total_immutable_memtable_bytes = 0;
  uint64_t total_immutable_memtable_count = 0;
  uint64_t total_l0_files = 0;
  uint64_t total_pending_compaction_bytes = 0;
  uint64_t write_buffer_manager_bytes = 0;
  uint64_t write_buffer_manager_limit_bytes = 0;
  uint64_t background_errors_total = 0;
  uint64_t block_cache_usage_bytes = 0;
  uint64_t block_cache_pinned_bytes = 0;
  uint64_t running_flushes = 0;
  uint64_t running_compactions = 0;
  uint64_t background_errors = 0;
  uint64_t live_sst_bytes = 0;
  uint64_t blob_file_bytes = 0;
  uint64_t write_stopped = 0;
  bool manual_conflict = false;
  bool recovery_in_progress = false;
  bool shutting_down = false;
  uint64_t delayed_write_rate_bytes_per_sec = 0;
  uint64_t retained_wal_bytes = 0;
  uint64_t block_cache_hits = 0;
  uint64_t block_cache_misses = 0;
  uint64_t blocks_compressed = 0;
  uint64_t compression_input_bytes = 0;
  uint64_t compression_output_bytes = 0;
  // Cedar-owned foreground read counters sampled without foreground
  // RocksDB property calls.
  uint64_t point_read_operations = 0;
  uint64_t multi_get_operations = 0;
  uint64_t projected_scan_rows = 0;
  uint64_t projected_scan_bytes_read = 0;
  uint64_t canonical_scan_bytes_read = 0;
  uint64_t logical_facts_bytes = 0;
  uint64_t obsolete_sst_bytes = 0;
  uint64_t temporary_output_bytes = 0;
  uint64_t free_disk_bytes = UINT64_MAX;
  uint64_t free_disk_percent = 100;
  std::vector<ColumnFamilyMetrics> column_families;
};

struct FactStoreRuntimeSample {
  PressureSample pressure;
  RocksDbRuntimeMetrics metrics;
  RuntimeSamplingTiming timing;
};

enum class FactStoreMaintenanceKind : uint8_t { kFlush, kCompaction };

enum class FactStoreMaintenanceYield : uint8_t {
  kNone = 0,
  kNoDebt,
  kStaleGeneration,
  kInputBudget,
  kOutputBudget,
  kDeadline,
  kWalSync,
  kManualConflict,
  kRecovery,
  kShutdown,
  kInvariantViolation,
};

struct FactStoreMaintenanceRequest {
  FactStoreMaintenanceKind kind = FactStoreMaintenanceKind::kFlush;
  uint64_t snapshot_generation = 0;
  uint64_t max_input_bytes = 0;
  uint64_t max_output_bytes = 0;
  uint64_t deadline_us = 0;
  bool emergency = false;
  bool yield_for_wal_sync = true;
};

struct FactStoreMaintenanceResult {
  uint64_t grant_id = 0;
  FactStoreMaintenanceKind kind = FactStoreMaintenanceKind::kFlush;
  uint64_t input_bytes = 0;
  uint64_t output_bytes = 0;
  uint64_t elapsed_us = 0;
  uint64_t remaining_smallest_complete_unit_bytes = 0;
  uint64_t atomic_overrun_bytes = 0;
  uint32_t selected_column_family_id = 0;
  FactStoreMaintenanceYield yield = FactStoreMaintenanceYield::kNone;
  Status status = Status::OK();
};

struct ValidationCacheMetrics {
  uint64_t hits = 0;
  uint64_t misses = 0;
  uint64_t resident_chains = 0;
  uint64_t resident_bytes = 0;
  uint64_t slot_capacity = 0;
  uint64_t reserved_bytes = 0;
  uint64_t recent_write_index_hits = 0;
  uint64_t recent_write_index_misses = 0;
  uint64_t recent_write_index_resets = 0;
  uint64_t recent_write_index_capacity = 0;
  uint64_t recent_write_index_resident_bytes = 0;
};

struct StoreCommitGroupRequest {
  StoreCommitBatch batch;
  bool persist_async_abort = false;
};

struct StoreCommittedGroupResult {
  std::vector<StatusOr<StoreCommitResult>> results;
  // CPU time spent deciding terminal outcomes and constructing the immutable
  // RocksDB batch. These are measured separately from the physical DB write.
  uint64_t validation_us = 0;
  uint64_t assembly_us = 0;
  uint64_t publication_us = 0;
  uint64_t wal_append_us = 0;
  uint64_t wal_sync_us = 0;
  uint64_t manifest_us = 0;
  uint64_t memtable_insert_us = 0;
  uint64_t wal_rotations = 0;
  bool has_kernel_stage_metrics = false;
};

enum class StoreAsyncCommitOutcome : uint8_t { kCommitted = 1, kAborted = 2 };

struct StoreAsyncCommitResult {
  StoreAsyncCommitOutcome outcome = StoreAsyncCommitOutcome::kAborted;
  CommitSeq commit_seq;
  TxnId txn_id;
};

using WalDurableCallback = void (*)(void* context) noexcept;

struct IdLease {
  IdKind kind = IdKind::kVertex;
  uint64_t first_id = 0;
  uint64_t count = 0;

  constexpr bool operator==(const IdLease&) const = default;
};

struct TemporalNeighborhood {
  std::optional<FactEvent> observed;
  std::optional<ValidTime> predecessor;
  std::optional<ValidTime> successor;
};

class FactPrefix {
 public:
  static FactPrefix Exact(FactRef ref);
  static FactPrefix Family(PartId part_id, FactFamily family,
                           PropertyId property_id);

  PartId part_id() const { return part_id_; }
  FactFamily family() const { return family_; }
  PropertyId property_id() const { return property_id_; }
  const std::optional<uint64_t>& entity_id() const { return entity_id_; }
  Status Validate() const;

 private:
  FactPrefix(PartId part_id, FactFamily family, PropertyId property_id,
             std::optional<uint64_t> entity_id)
      : part_id_(part_id), family_(family), property_id_(property_id),
        entity_id_(entity_id) {}

  PartId part_id_;
  FactFamily family_;
  PropertyId property_id_;
  std::optional<uint64_t> entity_id_;
};

class StoreSnapshot {
 public:
  ~StoreSnapshot();
  StoreSnapshot(StoreSnapshot&&) noexcept;
  StoreSnapshot& operator=(StoreSnapshot&&) noexcept;

  StoreSnapshot(const StoreSnapshot&) = delete;
  StoreSnapshot& operator=(const StoreSnapshot&) = delete;

  CommitSeq commit_seq() const;
  CommitSeq oldest_readable_seq() const;

 private:
  class State;
  explicit StoreSnapshot(std::unique_ptr<State> state);

  std::unique_ptr<State> state_;

  friend class FactStore;
};

using FactVisitor = std::function<Status(const FactEvent&)>;

// Optional canonical-key bounds for a snapshot-pinned facts scan. The store
// applies these before decoding values; higher layers still apply any
// bitemporal/semantic predicates after visibility resolution.
struct FactScanBounds {
  std::optional<uint64_t> entity_id_min;
  std::optional<uint64_t> entity_id_max;
};

// Cedar-owned projected fact columns. They deliberately use ordinary Cedar
// vectors rather than exposing Arrow or RocksDB objects through the public API.
enum class FactColumnId : uint8_t {
  kPartId = 3,
  kFactFamily = 4,
  kPropertyId = 5,
  kEntityId = 6,
  kValidFrom = 7,
  kCedarCommitSeq = 8,
  kRocksdbSequence = 9,
  kOperation = 10,
  kSchemaEpoch = 11,
  kPhysicalType = 12,
  kBoolValue = 13,
  kInt32Value = 14,
  kInt64Value = 15,
  kFloat32Value = 16,
  kFloat64Value = 17,
  kTimestamp64Value = 18,
  kBytesValue = 19,
  kSourcePartId = 20,
  kSourceVertexId = 21,
  kTargetPartId = 22,
  kTargetVertexId = 23,
  kEdgeType = 24,
};

using FactColumnVector = std::variant<
    std::vector<uint32_t>, std::vector<uint64_t>, std::vector<int32_t>,
    std::vector<int64_t>, std::vector<float>, std::vector<double>,
    std::vector<uint8_t>, std::vector<std::string>>;

struct FactColumn {
  FactColumnId id;
  FactColumnVector values;
  // One byte per row: 1 means the corresponding typed value is present.
  std::vector<uint8_t> present;
};

struct FactColumnarBatch {
  std::vector<FactColumn> columns;

  size_t row_count() const {
    return columns.empty() ? 0 : columns.front().present.size();
  }
};

using FactColumnarBatchVisitor = std::function<Status(const FactColumnarBatch&)>;

struct FactColumnarScanOptions {
  std::optional<ValidTime> event_valid_from_min;
  std::optional<ValidTime> event_valid_from_max;
  std::optional<CommitSeq> event_commit_seq_min;
  std::optional<CommitSeq> event_commit_seq_max;
  std::vector<FactColumnId> projection;
  uint32_t batch_row_limit = 1024;
};

class FactStore {
 public:
  explicit FactStore(FactStoreOptions options);
  ~FactStore();

  FactStore(const FactStore&) = delete;
  FactStore& operator=(const FactStore&) = delete;

  Status Open();
  Status Close();
  StatusOr<StoreSnapshot> BeginSnapshot(SnapshotOptions options = {}) const;
  StatusOr<std::optional<FactEvent>> Read(const StoreSnapshot& snapshot,
                                          const FactRef& ref,
                                          ValidTime valid_time) const;
  StatusOr<TemporalNeighborhood> ReadTemporalNeighborhood(
      const StoreSnapshot& snapshot, const FactRef& ref,
      ValidTime valid_time) const;
  Status Scan(const StoreSnapshot& snapshot, const FactPrefix& prefix,
              const FactVisitor& visitor) const;
  Status Scan(const StoreSnapshot& snapshot, const FactPrefix& prefix,
              const FactScanBounds& bounds, const FactVisitor& visitor) const;
  Status ScanColumnar(const StoreSnapshot& snapshot, const FactPrefix& prefix,
                      const FactScanBounds& bounds,
                      const FactColumnarScanOptions& options,
                      const FactColumnarBatchVisitor& visitor) const;
  // Projection-only accessors. Both are bound to the supplied StoreSnapshot
  // and never expose RocksDB types to callers.
  StatusOr<SequenceRecord> ReadSequence(const StoreSnapshot& snapshot,
                                        CommitSeq commit_seq) const;
  StatusOr<FactEvent> ReadExactFact(const StoreSnapshot& snapshot,
                                    const std::string& encoded_fact_key) const;
  StatusOr<StoreCommitResult> Commit(const StoreCommitBatch& batch);
  StatusOr<StoreCommitResult> CommitWithWalCallback(
      const StoreCommitBatch& batch, WalDurableCallback on_wal_durable,
      void* callback_context);
  StatusOr<StoreCommittedGroupResult> CommitGroupWithWalCallback(
      const std::vector<StoreCommitGroupRequest>& requests,
      WalDurableCallback on_wal_durable, void* callback_context,
      std::atomic<bool>* wal_sync_critical = nullptr);
  // Pipeline-only hand-off. The returned epoch owns a final immutable batch
  // but has not entered RocksDB; WriteDecidedGroup is its only writer path.
  StatusOr<std::unique_ptr<internal::DecidedEpoch>> DecideAndEncodeGroup(
      const std::vector<StoreCommitGroupRequest>& requests);
  // This deliberately narrower operation is safe while a predecessor epoch
  // owns the RocksDB writer: it reads no mutable RocksDB state and accepts
  // only fresh, blind append batches against the supplied virtual base.
  StatusOr<std::unique_ptr<internal::DecidedEpoch>>
  DecideIndependentAppendGroup(
      CommitSeq base_visible_seq, CommitSeq required_snapshot_seq,
      const std::vector<StoreCommitGroupRequest>& requests);
  StatusOr<StoreCommittedGroupResult> WriteDecidedGroup(
      internal::DecidedEpoch* epoch, WalDurableCallback on_wal_durable,
      void* callback_context,
      std::atomic<bool>* wal_sync_critical = nullptr);
  Status PersistPreparedCommit(const StoreCommitBatch& batch);
  Status PersistPreparedCommits(const std::vector<StoreCommitBatch>& batches);
  StatusOr<std::vector<StoreCommitBatch>> ListPreparedCommits() const;
  StatusOr<StoreCommitResult> FinalizePreparedCommit(
      const StoreCommitBatch& batch);
  Status AbortPreparedCommit(TxnId txn_id);
  StatusOr<TxnId> AllocateTransactionId();
  StatusOr<IdLease> LeaseIds(IdKind kind, uint64_t count);
  StatusOr<PropertyDefinition> RegisterProperty(PropertyDefinition definition);
  StatusOr<std::optional<PropertyDefinition>> LookupProperty(
      PropertyId property_id, uint32_t schema_epoch = 0) const;
  StatusOr<std::optional<PropertyDefinition>> LookupProperty(
      const StoreSnapshot& snapshot, PropertyId property_id,
      uint32_t schema_epoch = 0) const;
  StatusOr<std::optional<EdgeIdentity>> LookupEdgeIdentity(
      const StoreSnapshot& snapshot, EdgeRef edge) const;
  Status Vacuum(CommitSeq oldest_readable);
  CommitSeq visible_seq() const;
  StatusOr<std::optional<StoreCommitResult>> ResolveTransaction(
      TxnId txn_id) const;
  StatusOr<std::optional<StoreAsyncCommitResult>> ResolveAsyncTransaction(
      TxnId txn_id) const;
  StatusOr<PressureSample> SamplePressure() const;
  StatusOr<RocksDbRuntimeMetrics> SampleRuntimeMetrics() const;
  StatusOr<FactStoreRuntimeSample> SampleRuntime() const;
  void RecordPointRead() const;
  void RecordMultiGet(uint64_t requests) const;
  StatusOr<FactStoreMaintenanceResult> RunNativeMaintenance(
      const FactStoreMaintenanceRequest& request,
      const std::atomic<bool>* wal_sync_critical);
  StatusOr<ValidationCacheMetrics> SampleValidationCacheMetrics() const;

 private:
  StatusOr<StoreCommitResult> CommitDirect(
      const StoreCommitBatch& batch,
      const std::optional<std::string>& prepared_key = std::nullopt,
      bool sync = true);
  StatusOr<StoreCommitResult> CommitGrouped(
      const StoreCommitBatch& batch,
      const std::optional<std::string>& prepared_key = std::nullopt,
      bool sync = true);
  StatusOr<StoreCommitResult> CommitWithWalCallbackDirect(
      const StoreCommitBatch& batch, WalDurableCallback on_wal_durable,
      void* callback_context);
  StatusOr<StoreCommittedGroupResult> CommitGroupWithWalCallbackDirect(
      const std::vector<StoreCommitGroupRequest>& requests,
      WalDurableCallback on_wal_durable, void* callback_context,
      std::atomic<bool>* wal_sync_critical = nullptr);

  FactStoreOptions options_;
  mutable std::mutex lifecycle_mutex_;
  std::shared_ptr<FactStoreImpl> impl_;
};

}  // namespace cedar

#endif  // CEDAR_FACT_FACT_STORE_H_
