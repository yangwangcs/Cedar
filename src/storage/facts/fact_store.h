// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_STORAGE_FACTS_FACT_STORE_H_
#define CEDAR_STORAGE_FACTS_FACT_STORE_H_

#include <array>
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
class FactStore;
struct SnapshotEpoch;

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
  // Kept before the appended epoch observers so positional FactStoreOptions
  // initializers keep their existing meaning.
  WalSyncMode wal_sync_mode = WalSyncMode::kFullDeviceFlush;
  // Observe snapshot-epoch lifecycle. The created observer fires once for each
  // newly constructed epoch (never for a reuse of the current one); the
  // destroyed observer fires once per retired epoch when its last reader
  // releases it. Epochs torn down with the store do not fire it. Both may run
  // while the store's epoch mutex is held, so they must not call back into
  // FactStore.
  std::function<void()> snapshot_epoch_created_observer_for_testing;
  std::function<void()> snapshot_epoch_destroyed_observer_for_testing;
  // Observe the two exact-sequence windows of the as_of point path.
  // `current_sequence_loaded` fires in BeginCurrentSnapshotAt after the
  // visible/retention watermark loads and validation but *before* the epoch pin,
  // so a test can advance the visible sequence inside that window.
  // `publish_visible_before_retire` fires inside PublishVisible after the new
  // visible watermark is published but before the previous epoch is retired, so
  // a test can pin an older epoch while the requested sequence is already
  // visible. It runs under the store's epoch mutex; neither observer may call
  // back into FactStore.
  std::function<void()> current_sequence_loaded_observer_for_testing;
  std::function<void()> publish_visible_before_retire_observer_for_testing;
  // Fires in every registered storage-level non-epoch operation (checkpoint,
  // transaction-ID allocation, runtime sampling, validation-cache metrics,
  // native maintenance, and SchemaFingerprint) after it has registered with the
  // close drain handshake and before it touches any store state that Close
  // releases. Tests use it to hold such an operation in flight while a Close is
  // attempted; it must not call back into FactStore.
  std::function<void()> non_epoch_op_observer_for_testing;
  // Fires in BeginSnapshot and BeginSnapshotAtCached after the retention
  // watermarks have been loaded and the cheap boundary checks passed, but before
  // the atomic boundary re-check and epoch registration. Tests use it to park a
  // reader in exactly the window that the open/vacuum handshake closes; it must
  // not call back into FactStore.
  std::function<void()> snapshot_boundary_loaded_observer_for_testing;
  // Destructor force-detach windows. `force_detach_epochs` fires inside
  // ForceDetachEpochsLocked at the start of its epoch_mutex critical section;
  // `epoch_release_before_reclaim` fires in a last-reader release after the
  // reference count reached zero and before that release takes epoch_mutex. A
  // test can park one release in the second window while the force-detach holds
  // the lock, which is the interleaving the destructor must tolerate. Both must
  // not call back into FactStore; the second one is only reached by the last
  // reference of a retired epoch (it is not on the current-epoch point-read
  // release path).
  std::function<void()> force_detach_epochs_observer_for_testing;
  std::function<void()> epoch_release_before_reclaim_observer_for_testing;
  // Test-only override of the durable transaction-ID lease size. Zero keeps the
  // production default. Small values let tests drive lease exhaustion and
  // extension without allocating tens of thousands of IDs. It intentionally
  // does not affect vertex/edge lease defaults (`LeaseIds(kind, 0)`).
  uint64_t transaction_id_lease_size_for_testing = 0;
  // Fires before every durable transaction-ID lease-extension write with the
  // Sync flag that write will use. Tests pin the durability contract here; it
  // must not call back into FactStore.
  std::function<void(bool)> transaction_lease_write_observer_for_testing;
  // Fires at the very top of FactStore::PrepareCloseLocked while
  // lifecycle_mutex_ is held. Tests park it to prove that an operation which no
  // longer needs the lifecycle lock (for example LookupProperty on a pinned
  // snapshot) is not serialized behind a concurrent close handshake. Must not
  // call back into FactStore. Production callers leave it unset.
  std::function<void()> prepare_close_locked_observer_for_testing;
  // Fires once per committed sequence record consumed by reopen validation with
  // `read_from_iterator == true` when the record came from the meta-CF iterator
  // the scan is already holding, or false when the validator had to re-read the
  // sequence key. S1 removed that second read, so the reopen scan reports true
  // for every record; a regression that re-reads the key is expected to report
  // false here. Must not call back into FactStore.
  std::function<void(bool)> sequence_validation_read_source_observer_for_testing;
  // Forces the reopen identity-history rebuild to be skipped even when the
  // facts CF could be represented by the filter. Tests use it to prove the skip
  // path leaves the filter in its always-true state on a small database.
  // Production takes the capacity decision from the facts CF estimate.
  bool identity_history_skip_rebuild_for_testing = false;
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

enum class StorePreparedDecisionOutcome : uint8_t {
  kCommit = 1,
  kAbort = 2,
};

struct StorePreparedDecision {
  TxnId txn_id;
  StorePreparedDecisionOutcome outcome =
      StorePreparedDecisionOutcome::kAbort;
  std::string certificate;

  bool operator==(const StorePreparedDecision&) const = default;
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
  uint32_t maintenance_flush_worker_capacity = 0;
  uint32_t maintenance_compaction_worker_capacity = 0;
  uint32_t maintenance_normal_flush_credits = 0;
  uint32_t maintenance_emergency_flush_credits = 0;
  uint32_t maintenance_compaction_credits = 0;
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
  uint64_t canonical_page_hits = 0;
  uint64_t canonical_page_misses = 0;
  uint64_t canonical_page_decodes = 0;
  uint64_t canonical_page_decoded_bytes = 0;
  uint64_t canonical_page_resident_charge_bytes = 0;
  uint64_t canonical_page_pinned_charge_bytes = 0;
  uint64_t canonical_page_uncached_live_bytes = 0;
  uint64_t canonical_page_insert_failures = 0;
  uint64_t canonical_validation_scans = 0;
  uint64_t canonical_validation_boundaries = 0;
  uint64_t recent_write_exact_hits = 0;
  uint64_t recent_write_bloom_hits = 0;
  uint64_t recent_write_exact_absent = 0;
  uint64_t recent_write_exact_newer = 0;
  uint64_t recent_write_exact_evictions = 0;
  uint64_t recent_write_exact_resets = 0;
  uint64_t recent_write_coverage_misses = 0;
  uint64_t recent_write_exact_capacity = 0;
  uint64_t recent_write_exact_resident_bytes = 0;
  // S3: state of the ever-written identity Bloom. `disabled` means the reopen
  // rebuild was skipped because the facts CF outgrew the filter budget; the
  // filter is then always-true (resident_bytes == 0) and every probe falls back
  // to the canonical read.
  uint64_t identity_history_resident_bytes = 0;
  bool identity_history_disabled = false;

};

struct StoreCommitGroupRequest {
  StoreCommitBatch batch;
  bool persist_async_abort = false;
};

// Lightweight non-owning planning view used by the N+1 preflight worker. It is
// valid only while the owning AppendCommitRequest batches stay alive; the
// preflight decision is built from these views and the decided epoch retains no
// reference to them, because publication reads the live batches at write time.
struct StoreCommitGroupRequestView {
  const StoreCommitBatch* batch = nullptr;
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
  // A benchmark may know sync cost without exposing RocksDB kernel stages.
  bool has_wal_sync_cost = false;
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

// A single point in one fact chain to resolve against a store snapshot.
struct TemporalNeighborhoodQuery {
  FactRef ref;
  ValidTime valid_time;

  Status Validate() const;
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
  // Published durable retention boundary captured when the epoch was created
  // (see the SnapshotEpoch comment in rocks_adapter.cc). A Vacuum's unpublished
  // pending reservation is deliberately not included.
  CommitSeq oldest_readable_seq() const;

 private:
  explicit StoreSnapshot(std::shared_ptr<FactStoreImpl> store,
                         const FactStore* owner, SnapshotEpoch* epoch);

  // Drops the one epoch reference held by this handle. The epoch reference
  // count is the liveness source of truth for Close and Vacuum; there is no
  // per-request bookkeeping to unregister. Safe to call only once.
  void Release();

  // The epoch outlives every read because it owns a RocksDB snapshot lease
  // and only the last released reference reclaims it. The shared_ptr keeps the
  // owning store (and therefore that registry) alive for the whole read. Each
  // epoch also holds its own strong `store_owner` back-pointer so a lock-free
  // pin can copy it after the handle is built; that does not create an
  // ownership cycle in the close path because Close releases every epoch's
  // owner reference before the closed shell is retained, and the retained
  // shell (bounded to one) holds the DEAD epoch memory until it is freed.
  //
  // This handle reference is not redundant with `epoch_->store_owner` and must
  // not be replaced by a raw `epoch_->store` pointer: `~FactStore` deliberately
  // supports an outliving handle (Close reports SnapshotPinned) and
  // ForceDetachEpochsLocked clears every epoch's `store_owner` while reader
  // references are still live. The later handle release then still has to
  // decrement the epoch reference and take `epoch_mutex`, i.e. call into a store
  // whose owner is already gone. This shared reference is what keeps that store
  // shell (and its close graveyard) alive until the last handle is released; see
  // RocksDbLifecycleTest.DestructorForceClosesStoreWithOutlivingSnapshot and
  // ForceDetachRetiresOutlivingHandlesAfterDestruction. The per-read RMW pair is
  // therefore a lifetime requirement, not an optimization opportunity.
  std::shared_ptr<FactStoreImpl> store_;
  const FactStore* owner_ = nullptr;
  SnapshotEpoch* epoch_ = nullptr;

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
  // Destroys the store. Unlike Close(), destruction cannot report
  // SnapshotPinned: a StoreSnapshot handle that still pins an epoch is
  // force-detached during destruction - its epoch payload (the storage-engine
  // snapshot lease and the strong store back-reference) is released and the
  // storage handle is destroyed, while the epoch memory itself is retained until
  // the handle is gone. Such an outliving handle cannot keep the database
  // (LOCK/WAL) open, but it is detached from a live store.
  //
  // Serialization contract (required for every outliving handle): destruction
  // must be serialized with every user of the handle. While ~FactStore runs, no
  // thread may read through the handle (Read*/Scan*/Lookup*), inspect it
  // (commit_seq/oldest_readable_seq), or release/destroy it: its payload and the
  // heavy state a read goes through are released here, so concurrent use is a
  // use-after-free, not a supported interleaving. The supported retirement is
  // sequential: destroy the FactStore, wait for ~FactStore to return, then
  // destroy the handle. Releasing the handle afterwards is safe and is the
  // required way to retire it. The implementation additionally tolerates a
  // last-reader release that races destruction (the epoch payload and memory are
  // claimed under the epoch lock, so the late release is a no-op), but that
  // tolerance does not extend to reads: reading through the handle concurrently
  // with destruction remains a contract violation. The normal path - Close()
  // before destruction, or destruction with no outliving handle - is unchanged.
  ~FactStore();

  FactStore(const FactStore&) = delete;
  FactStore& operator=(const FactStore&) = delete;

  Status Open();
  // Closes the store. Only epoch-pinned readers are a reason to fail
  // (SnapshotPinned); Close does not wait for them. Operations that are
  // registered with the close handshake may run concurrently with Close: they
  // observe either an open store or "store is not open". All other public
  // methods must be externally serialized with Close (see the concurrency
  // contract below).
  //
  // Concurrency contract:
  // * Registered epoch readers: BeginSnapshot/BeginCurrentSnapshot/
  //   BeginCurrentSnapshotAt register their open under lifecycle_mutex_ and the
  //   returned StoreSnapshot owns an epoch reference, so a live handle keeps
  //   the RocksDB snapshot alive and makes Close fail with SnapshotPinned.
  // * Registered non-epoch operations: CreateCheckpoint, SampleRuntime (and its
  //   SampleRuntimeMetrics/SamplePressure wrappers), SampleValidationCacheMetrics,
  //   RunNativeMaintenance, AllocateTransactionId, and SchemaFingerprint
  //   register in the in-flight count under lifecycle_mutex_ after copying the
  //   store. Close drains that count after draining snapshot opens and lock-free
  //   pins and before it releases any RocksDB handle or heavy state.
  //   Registration and the Close handshake both need lifecycle_mutex_, so a zero
  //   observation is final: an operation either registered before Close and is
  //   waited for, or blocks behind Close and then sees "store is not open".
  // * Unregistered operations: every public method that touches a RocksDB
  //   handle or mutable store metadata is not covered by that handshake. This
  //   includes Commit/CommitWithWalCallback/CommitGroupWithWalCallback,
  //   DecideAndEncodeGroup/DecideIndependentAppendGroup, WriteDecidedGroup,
  //   PersistPreparedCommit(s)/PersistPreparedDecision,
  //   FinalizePreparedCommit/AbortPreparedCommit/ResolveTransaction/
  //   ResolveAsyncTransaction/ResolvePreparedDecision, SynchronizeWal,
  //   ValidatePredictedGroup, LeaseIds, RegisterProperty, Vacuum and
  //   visible_seq. The caller must serialize these with Close (and with each
  //   other as their own contracts require) through a higher-level lifecycle
  //   lock; otherwise Close can free state they are still using. The kernel
  //   satisfies this with Database::Impl::mutex, which it holds across
  //   FactStore::Close and while issuing those calls. Read methods taking a
  //   StoreSnapshot stay safe because the handle pins its epoch; property
  //   catalog snapshots are returned by value/shared_ptr.
  // * Open and Close are serialized by lifecycle_mutex_ only for the duration
  //   of each call; they are not themselves registered operations.
  Status Close();
  // Two-phase close used by the kernel to keep a failed (SnapshotPinned) close
  // non-destructive. PrepareClose performs the irreversible *decision* part of
  // Close: it unpublishes the lock-free fast path, drains registered opens,
  // fast pins and non-epoch users, evicts the rebuildable as_of epoch cache,
  // and checks the epoch registry under the publisher/epoch locks. If an epoch
  // is still referenced it rolls the publication gates back (the lock-free fast
  // path is republished and snapshot/fast-pin acceptance is restored) and
  // returns SnapshotPinned; nothing heavy is destroyed, so the store stays open
  // and usable. The rollback deliberately does not repopulate the as_of cache:
  // the pin test runs after the eviction, and later as_of reads rebuild cache
  // entries lazily. Cache entries hold only rebuildable epoch references and
  // never pin the retention boundary, so a pinned close costs a cache refill,
  // not correctness. If no epoch is referenced, the store is committed to
  // closing: accepting_snapshots stays false and fast_impl_ stays null, so no
  // epoch reference can be created afterwards, but no heavy state has been
  // released yet. The caller may then perform its own irreversible teardown
  // (stop commit workers, detach derived state) knowing the close can no longer
  // fail for pinning, and finally call FinishClose. A successful PrepareClose
  // must be followed by FinishClose; Close() is the single-call composition of
  // both under one lifecycle critical section.
  Status PrepareClose();
  // Completes a successful PrepareClose: drains any remaining registered
  // operation (the caller has stopped its producers), releases the epoch
  // leases, destroys the RocksDB handles and retains the inert shell. It cannot
  // fail with SnapshotPinned because PrepareClose already excluded new epoch
  // references. Calling FinishClose without a prepared close on an open store
  // is a contract violation (InvalidArgument).
  Status FinishClose();
  // Creates a RocksDB-owned openable checkpoint without exposing RocksDB
  // handles or file-format details across the Cedar storage boundary.
  Status CreateCheckpoint(const std::string& checkpoint_path) const;
  // Opens a snapshot at the current visible sequence, or at `options.as_of`.
  // The retention-boundary check and the epoch registration are one atomic
  // section with respect to Vacuum (see Vacuum's retention/open invariant): a
  // concurrent Vacuum either cannot clean the pinned sequence or this call
  // reports SnapshotExpired. Reports SnapshotExpired below the retention
  // boundary and InvalidArgument above the visible watermark.
  StatusOr<StoreSnapshot> BeginSnapshot(SnapshotOptions options = {}) const;
  // Lock-free pin of the latest visible sequence for point reads. Reads no
  // mutex on the hot path; falls back to BeginSnapshot only when the current
  // epoch has not been created yet, a publication/retirement raced the pin, or
  // the store is closing. The returned handle owns one epoch reference and (like
  // BeginSnapshot) keeps the read pinned until it is destroyed. The lock-free
  // path never takes epoch_mutex, so the open/vacuum handshake adds no lock to
  // the current-vintage point-read hot path.
  StatusOr<StoreSnapshot> BeginCurrentSnapshot() const;
  // Historical as_of pin for point reads (GetPointAtAt). Reads at the exact
  // commit sequence with a small bounded per-sequence epoch cache (8 entries,
  // each holding its own reference). The pin is always exact: a current-epoch
  // pin whose sequence does not equal the requested sequence is released and
  // an epoch pinned at the requested sequence is created instead, so a commit
  // published concurrently with this call can never leak a newer vintage into
  // the read. Reports SnapshotExpired below the durable retention boundary and
  // InvalidArgument above the visible watermark, exactly like
  // BeginSnapshot(options.as_of); the cache-miss path repeats the boundary check
  // and registers its epoch atomically with Vacuum like BeginSnapshot, while a
  // cache hit is already covered because Vacuum evicts the cache before it scans
  // the live epoch registry.
  StatusOr<StoreSnapshot> BeginCurrentSnapshotAt(CommitSeq sequence) const;
  // One-shot point reads on top of the lock-free pins above. They return the
  // storage-level fact event; kernel callers that need metric/edge visibility
  // semantics use BeginCurrentSnapshot* together with the shared snapshot
  // helper.
  StatusOr<std::optional<FactEvent>> ReadCurrent(const FactRef& ref,
                                                 ValidTime valid_time) const;
  StatusOr<std::optional<FactEvent>> ReadCurrentAt(CommitSeq sequence,
                                                   const FactRef& ref,
                                                   ValidTime valid_time) const;
  StatusOr<std::optional<FactEvent>> Read(const StoreSnapshot& snapshot,
                                          const FactRef& ref,
                                          ValidTime valid_time) const;
  StatusOr<TemporalNeighborhood> ReadTemporalNeighborhood(
      const StoreSnapshot& snapshot, const FactRef& ref,
      ValidTime valid_time) const;
  // Reads several fact neighborhoods against one store snapshot. Results are
  // returned in query order; an empty query list is valid.
  Status ReadTemporalNeighborhoods(
      const StoreSnapshot& snapshot,
      const std::vector<TemporalNeighborhoodQuery>& queries,
      std::vector<TemporalNeighborhood>* neighborhoods) const;
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
  // but has not entered RocksDB; WriteDecidedGroup is its only writer path and
  // must receive the same request batches the epoch was decided from (same
  // order and size). Publication of the recent-write index and validation
  // cache reads those live batches at write time, so the epoch retains no
  // per-mutation publication copy.
  StatusOr<std::unique_ptr<internal::DecidedEpoch>> DecideAndEncodeGroup(
      const std::vector<StoreCommitGroupRequest>& requests);
  // This deliberately narrower operation is safe while a predecessor epoch
  // owns the RocksDB writer: it reads no mutable RocksDB state and accepts
  // only fresh, blind append batches against the supplied virtual base.
  StatusOr<std::unique_ptr<internal::DecidedEpoch>>
  DecideIndependentAppendGroup(
      CommitSeq base_visible_seq, CommitSeq required_snapshot_seq,
      const std::vector<StoreCommitGroupRequest>& requests);
  // Planning-view overload: the caller guarantees every `batch` pointer stays
  // valid and unmodified until this call returns. Used by the preflight worker
  // so planning a group does not copy every queued batch.
  StatusOr<std::unique_ptr<internal::DecidedEpoch>>
  DecideIndependentAppendGroup(
      CommitSeq base_visible_seq, CommitSeq required_snapshot_seq,
      const std::vector<StoreCommitGroupRequestView>& requests);
  // Validates snapshot-write dependencies for a predicted (predecided) group
  // immediately before its write. Returns Conflict when a dependency changed.
  Status ValidatePredictedGroup(
      const std::vector<StoreCommitGroupRequest>& requests) const;
  StatusOr<StoreCommittedGroupResult> WriteDecidedGroup(
      internal::DecidedEpoch* epoch,
      const std::vector<StoreCommitGroupRequest>& requests,
      WalDurableCallback on_wal_durable, void* callback_context,
      std::atomic<bool>* wal_sync_critical = nullptr);
  Status PersistPreparedCommit(const StoreCommitBatch& batch);
  Status PersistPreparedCommits(const std::vector<StoreCommitBatch>& batches);
  StatusOr<std::vector<StoreCommitBatch>> ListPreparedCommits() const;
  StatusOr<StoreCommitResult> FinalizePreparedCommit(
      const StoreCommitBatch& batch, bool sync = false);
  Status SynchronizeWal();
  Status AbortPreparedCommit(TxnId txn_id);
  Status PersistPreparedDecision(const StorePreparedDecision& decision);
  StatusOr<std::optional<StorePreparedDecision>> ResolvePreparedDecision(
      TxnId txn_id) const;
  // Registered non-epoch operation: the lease slow path writes allocator
  // metadata through the RocksDB handle, so Close drains it before teardown.
  StatusOr<TxnId> AllocateTransactionId();
  StatusOr<IdLease> LeaseIds(IdKind kind, uint64_t count);
  StatusOr<PropertyDefinition> RegisterProperty(PropertyDefinition definition);
  StatusOr<std::optional<PropertyDefinition>> LookupProperty(
      PropertyId property_id, uint32_t schema_epoch = 0) const;
  StatusOr<std::optional<PropertyDefinition>> LookupProperty(
      const StoreSnapshot& snapshot, PropertyId property_id,
      uint32_t schema_epoch = 0) const;
  StatusOr<std::vector<PropertyDefinition>> ListProperties() const;
  // Stable fingerprint of the latest registered definition for every property.
  // The value is derived from the authoritative schema catalog and is used to
  // bind derived query statistics to the schema that produced them. Registered
  // non-epoch operation: Close drains it before releasing the schema catalog, so
  // callers (including query planning) may invoke it without holding the kernel
  // lifecycle lock.
  StatusOr<std::string> SchemaFingerprint() const;
  StatusOr<std::optional<EdgeIdentity>> LookupEdgeIdentity(
      const StoreSnapshot& snapshot, EdgeRef edge) const;
  // Same lookup when the caller has already observed a visible edge-state PUT
  // for `edge` in this snapshot (the edge-visibility walk in
  // `EdgeVisibleAt`). The observation is exactly the condition the overload
  // above re-establishes by scanning the edge-state prefix, so the scan is
  // skipped and the edge fact is read once. Contract: `observed_edge_state`
  // must be the value returned by `Read(snapshot,
  // EntityFact::Edge(edge).ref(), valid_time)` on the same snapshot; a
  // mismatched observation (different fact, a delete, or a commit sequence above
  // the snapshot) is reported as InvalidArgument rather than trusted. The
  // overload deliberately does not re-prove valid-time-chain visibility: that
  // proof is the scan it removes, so the caller remains its sole owner (that
  // `EdgeVisibleAt` is the only caller is part of the contract).
  StatusOr<std::optional<EdgeIdentity>> LookupEdgeIdentity(
      const StoreSnapshot& snapshot, EdgeRef edge,
      const FactEvent& observed_edge_state) const;
  // Advances the durable retention boundary to `oldest_readable` and deletes
  // obsolete versions at or below it (one baseline per valid-time chain is
  // kept). Unregistered operation: the caller serializes it with Close.
  //
  // Retention/open invariant: an as_of open validates the retention boundary and
  // registers its epoch in one `epoch_mutex` critical section, and Vacuum scans
  // the live epoch registry and reserves the new boundary (before publishing it
  // durably) in one critical section of the same mutex. Therefore a concurrent
  // Vacuum either observes the opening epoch (a live reference at or below the
  // target makes it fail with SnapshotPinned) or the open observes the reserved
  // boundary and fails with SnapshotExpired. A successful open at a sequence is
  // always at or above the boundary that covers the data it can read, so a
  // reader can never be handed an epoch over versions the Vacuum already
  // deleted.
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
  // Lock-free variant for callers that already hold a pinned StoreSnapshot:
  // the snapshot keeps the underlying store instance alive, so the counter can
  // be bumped without re-acquiring lifecycle_mutex_.
  void RecordPointRead(const StoreSnapshot& snapshot) const;
  void RecordMultiGet(uint64_t requests) const;
  StatusOr<FactStoreMaintenanceResult> RunNativeMaintenance(
      const FactStoreMaintenanceRequest& request,
      const std::atomic<bool>* wal_sync_critical);
  StatusOr<ValidationCacheMetrics> SampleValidationCacheMetrics() const;
  // Test-only introspection of the closed-store shells retained for a lock-free
  // pin that raced `Close`. The contract under test is that the retention stays
  // bounded (at most one shell: the one closed most recently) and that every
  // retained shell released its heavy state (validation caches, statistics,
  // property schemas) and every RocksDB snapshot lease. Exposed for lifecycle
  // regression tests only; no production code may depend on it.
  size_t RetainedShellCountForTesting() const;
  bool RetainedShellsAreInertForTesting() const;
  // Number of as_of opens currently registered with the close handshake.
  // Test-only: proves that an exception path releases its registration (a leaked
  // registration makes every later Close drain forever).
  size_t SnapshotOpensInFlightForTesting() const;
  // Number of storage-level non-epoch operations currently registered with the
  // close drain handshake. Test-only: no production code may depend on it.
  size_t NonEpochUsersForTesting() const;
  // Number of fast-pin entry points that observed the unpublished fast_impl_ and
  // skipped attempt registration (the no-new-arrivals gate documented at
  // FastPinAttempt). Test-only: proves that a closing store cannot be kept busy
  // by new lock-free arrivals.
  uint32_t FastPinAttemptsSkippedForTesting() const;

 private:
  // Slow path of BeginCurrentSnapshotAt for a historical sequence whose epoch
  // is not cached: acquires the open store through the close handshake, creates
  // the one-off epoch, and inserts it into the bounded cache. Also used as the
  // exact-sequence fallback for the current vintage, in which case a mismatch
  // between the requested sequence and the pinned current epoch creates a
  // one-off epoch at exactly the requested sequence.
  StatusOr<StoreSnapshot> BeginSnapshotAtCached(CommitSeq sequence) const;
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

  // Lock-free point reads register one attempt here *before* they load
  // fast_impl_. A successful Close clears fast_impl_ and then drains these
  // sharded counters (seq_cst), which proves that no thread can still hold a raw
  // pointer into any previously closed store shell. That proof is what lets
  // Close release the previous shell instead of retaining one full store per
  // Open->Close; the newly closed shell is the only retained one and it is made
  // inert by releasing its heavy state.
  //
  // No-new-arrivals invariant (drain boundedness): both lock-free entry points
  // read fast_impl_ *before* constructing a FastPinAttempt and skip registration
  // when it is null. PrepareClose clears fast_impl_ before it drains the total,
  // so for the store that is closing, once that clear is visible only threads
  // that already passed the gate can still increment a counter; every later
  // arrival goes straight to the registered slow path (which reports the closing
  // state) and never touches the total again. The drain is therefore bounded by a
  // finite set of already in-flight attempts, not by the arrival rate.
  //
  // The seq_cst guarantee is one-directional: an attempt that observes a non-null
  // fast_impl_ registered its counter before the clear in the seq_cst total
  // order, so the drain is guaranteed to wait for it. The converse does not hold:
  // an attempt that registers after the clear is not guaranteed to load null (a
  // pinned close republishes the same store and a later Open publishes a new
  // one), but such an attempt only touches a store that is open again, never the
  // shell being torn down. Shell-lifetime safety therefore rests on the forward
  // direction - observing non-null implies the registration precedes the clear
  // and is drained - not on the drain observing zero before the shell is freed.
  static constexpr size_t kFastPinShardCount = 32;
  struct alignas(64) FastPinAttemptCounter {
    std::atomic<uint32_t> value{0};
  };
  class FastPinAttempt {
   public:
    explicit FastPinAttempt(const FactStore& store);
    ~FastPinAttempt();
    FastPinAttempt(const FastPinAttempt&) = delete;
    FastPinAttempt& operator=(const FastPinAttempt&) = delete;

   private:
    const FactStore& store_;
    size_t shard_ = 0;
  };
  uint32_t FastPinAttemptsTotal() const;
  // RAII marker for a storage-level non-epoch operation. Such an operation holds
  // a shared_ptr<FactStoreImpl> but no epoch reference, so the epoch registry and
  // the fast-pin attempts cannot make it visible to Close. Registration happens
  // under lifecycle_mutex_, which Close holds for its whole handshake, so a Close
  // either observes the registration (and waits for it) or starts after the
  // operation finished; a new operation that starts after the close began blocks
  // on lifecycle_mutex_ and then observes no open store.
  //
  // The guard must be destroyed before the operation's shared_ptr copy. Covered
  // operations must not call FactStore methods that take lifecycle_mutex_ while
  // registered (Close holds it during the drain).
  class NonEpochUseGuard {
   public:
    NonEpochUseGuard() = default;
    ~NonEpochUseGuard();
    NonEpochUseGuard(const NonEpochUseGuard&) = delete;
    NonEpochUseGuard& operator=(const NonEpochUseGuard&) = delete;
    NonEpochUseGuard(NonEpochUseGuard&& other) noexcept;
    NonEpochUseGuard& operator=(NonEpochUseGuard&& other) noexcept;

   private:
    friend class FactStore;
    explicit NonEpochUseGuard(const FactStore* store) : store_(store) {}
    const FactStore* store_ = nullptr;
  };
  // Caller must hold lifecycle_mutex_ and must already have copied impl_.
  NonEpochUseGuard RegisterNonEpochUseLocked() const;
  // Fires the test-only re-entrance observer for a covered non-epoch operation.
  void RunNonEpochOpObserverForTesting() const;
  // Frees every retained shell from an earlier close after draining its
  // last-reader reclaim attempts. Only a successful Close calls this: it holds
  // lifecycle_mutex_, fast_impl_ is null, and this close already drained every
  // fast pin attempt, so no thread can reference the older shells.
  void FreeEarlierRetainedShells();
  // The following two helpers require lifecycle_mutex_ to be held.
  Status PrepareCloseLocked();
  Status FinishCloseLocked();
  // Destructor-only fallback used when PrepareCloseLocked() reports
  // SnapshotPinned: force-detaches every epoch (see
  // FactStoreImpl::ForceDetachEpochsLocked), destroys the RocksDB handles and
  // releases the heavy state without requiring a zero reader count. Requires
  // lifecycle_mutex_ to be held.
  void ForceCloseLocked();

  FactStoreOptions options_;
  mutable std::mutex lifecycle_mutex_;
  std::shared_ptr<FactStoreImpl> impl_;
  // Store committed by a successful PrepareClose but not yet destroyed by
  // FinishClose. `close_prepared_` is guarded by lifecycle_mutex_; while it is
  // set, accepting_snapshots is false and fast_impl_ is null, so no new epoch
  // reference (and therefore no SnapshotPinned outcome) can appear.
  std::shared_ptr<FactStoreImpl> closing_impl_;
  bool close_prepared_ = false;
  // Storage-level non-epoch operations currently between registration and
  // teardown. They are invisible to the epoch registry, so Close drains this
  // count while holding lifecycle_mutex_ before it releases any heavy state.
  mutable std::atomic<uint32_t> non_epoch_users_{0};
  // Raw publication of impl_ for the lock-free point-read path. fast_impl_ is
  // cleared at the start of a close handshake and only republished when the
  // close reports SnapshotPinned. A thread that loaded the pointer before the
  // clear is covered by a FastPinAttempt that it registered before the load, so
  // the shell can be freed once close drained the attempts.
  std::atomic<FactStoreImpl*> fast_impl_{nullptr};
  // Counts fast-pin entry points rejected by the unpublished-fast_impl_ gate
  // (see FastPinAttempt's no-new-arrivals invariant). Only written on the
  // closing path, never by a successful lock-free pin.
  mutable std::atomic<uint32_t> fast_pin_attempts_skipped_{0};
  mutable std::array<FastPinAttemptCounter, kFastPinShardCount>
      fast_pin_attempts_{};
  // Closed shells kept for a lock-free pin that raced the close gate. Bounded
  // to the most recently closed shell; older shells are freed by the next
  // successful close. The shell itself is inert (no caches/statistics/schemas,
  // no RocksDB handle), and its epoch graveyard holds at most the epochs that
  // existed at close, with every RocksDB snapshot lease already released.
  std::vector<std::shared_ptr<FactStoreImpl>> retained_impls_;
};

}  // namespace cedar

#endif  // CEDAR_STORAGE_FACTS_FACT_STORE_H_
