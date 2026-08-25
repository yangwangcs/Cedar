// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_STORAGE_FACTS_FACT_STORE_H_
#define CEDAR_STORAGE_FACTS_FACT_STORE_H_

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/fact/fact.h"
#include "cedar/fact/fact_scan.h"
#include "cedar/fact/meta_codec.h"
#include "cedar/runtime/pressure_controller.h"
#include "cedar/runtime/runtime_metrics.h"
#include "cedar/storage_options.h"

namespace cedar {

// These names remain private to the storage module while the public options
// use Cedar-neutral terminology.
inline constexpr uint32_t kMaximumGroupCommitBatchCount =
    kMaximumCommitBatchCount;
inline constexpr uint64_t kMaximumGroupCommitBatchBytes =
    kMaximumCommitBatchBytes;

namespace internal {
class DecidedEpoch;
}

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
  uint64_t flush_queue_depth = 0;
  uint64_t unscheduled_flushes = 0;
  uint64_t scheduled_flushes = 0;
  uint64_t background_errors = 0;
  uint64_t live_sst_bytes = 0;
  uint64_t blob_file_bytes = 0;
  uint64_t write_stopped = 0;
  bool manual_conflict = false;
  bool recovery_in_progress = false;
  bool shutting_down = false;
  uint64_t delayed_write_rate_bytes_per_sec = 0;
  uint64_t retained_wal_bytes = 0;
  uint64_t maintenance_snapshot_age_us = 0;
  uint64_t background_flush_calls = 0;
  uint64_t manual_compaction_calls = 0;
  uint64_t periodic_task_registrations = 0;
  uint64_t recovery_flush_exceptions = 0;
  uint64_t block_cache_hits = 0;
  uint64_t block_cache_misses = 0;
  uint64_t blocks_compressed = 0;
  uint64_t compression_input_bytes = 0;
  uint64_t compression_output_bytes = 0;
  // Cedar-owned foreground read counters sampled without foreground
  // RocksDB property calls.
  uint64_t point_read_operations = 0;
  uint64_t multi_get_operations = 0;
  uint64_t multi_get_batches = 0;
  uint64_t projected_scan_rows = 0;
  uint64_t projected_scan_bytes_read = 0;
  uint64_t projected_scan_pages_skipped = 0;
  uint64_t projected_scan_pages_read = 0;
  uint64_t projected_scan_physical_bytes_read = 0;
  uint64_t canonical_scan_bytes_read = 0;
  // Foreground physical bytes decoded by canonical RocksDB Get, MultiGet,
  // and iterator reads. This is sampled from the storage adapter and does
  // not expose the backing engine's metric types to Cedar callers.
  uint64_t canonical_read_physical_bytes = 0;
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
  uint64_t flush_queue_depth = 0;
  uint64_t unscheduled_flushes = 0;
  uint64_t scheduled_flushes = 0;
  uint64_t running_flushes = 0;
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

struct FactColumnarScanOptions {
  std::optional<ValidTime> event_valid_from_min;
  std::optional<ValidTime> event_valid_from_max;
  std::optional<CommitSeq> event_commit_seq_min;
  std::optional<CommitSeq> event_commit_seq_max;
  std::vector<FactColumnId> projection;
  uint32_t batch_row_limit = 1024;
  std::optional<uint64_t> max_rows;
};

class FactStore {
 public:
  explicit FactStore(FactStoreOptions options);
  ~FactStore();

  FactStore(const FactStore&) = delete;
  FactStore& operator=(const FactStore&) = delete;

  Status Open();
  Status Close();
  // Creates a RocksDB-owned openable checkpoint without exposing RocksDB
  // handles or file-format details across the Cedar storage boundary.
  Status CreateCheckpoint(const std::string& checkpoint_path) const;
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
  // Scans one Cedar fact family across every home partition. This is used by
  // graph adjacency fallback because PartId{0} is a real partition, not a
  // wildcard.
  Status ScanFamily(const StoreSnapshot& snapshot, FactFamily family,
                    const FactVisitor& visitor) const;
  Status ScanColumnar(const StoreSnapshot& snapshot, const FactPrefix& prefix,
                      const FactScanBounds& bounds,
                      const FactColumnarScanOptions& options,
                      const FactColumnarBatchVisitor& visitor) const;
  Status ScanColumnarFamily(const StoreSnapshot& snapshot, FactFamily family,
                            PropertyId property,
                            const FactColumnarScanOptions& options,
                            const FactColumnarBatchVisitor& visitor) const;
  // Projection-only accessors. Both are bound to the supplied StoreSnapshot
  // and never expose RocksDB types to callers.
  StatusOr<SequenceRecord> ReadSequence(const StoreSnapshot& snapshot,
                                        CommitSeq commit_seq) const;
  // Reads an ordered, contiguous range of durable sequence records.  The
  // range is validated against the supplied Snapshot and a missing sequence
  // is canonical corruption rather than a silently shortened result.
  StatusOr<std::vector<SequenceRecord>> ReadSequenceRange(
      const StoreSnapshot& snapshot, CommitSeq first,
      CommitSeq last) const;
  // Returns the immutable canonical FactEvent rows referenced by a contiguous
  // commit range. Sequence metadata contributes keys only; payloads are read
  // from the facts CF through the same canonical decoder used by all scans.
  StatusOr<std::vector<FactEvent>> ReadCanonicalEvents(
      const StoreSnapshot& snapshot, CommitSeq first, CommitSeq last) const;
  StatusOr<FactEvent> ReadExactFact(const StoreSnapshot& snapshot,
                                    const std::string& encoded_fact_key) const;
  // Reads exact fact keys through one RocksDB MultiGet while preserving the
  // caller's key order.  Missing values are canonical corruption.
  StatusOr<std::vector<FactEvent>> ReadExactFacts(
      const StoreSnapshot& snapshot,
      const std::vector<std::string>& encoded_fact_keys) const;
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
  // Stable fingerprint of the latest registered definition for every property.
  // The value is derived from the authoritative schema catalog and is used to
  // bind derived query statistics to the schema that produced them.
  StatusOr<std::string> SchemaFingerprint() const;
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

#endif  // CEDAR_STORAGE_FACTS_FACT_STORE_H_
