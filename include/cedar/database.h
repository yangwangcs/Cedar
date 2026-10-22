// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_DATABASE_H_
#define CEDAR_DATABASE_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>

#include "cedar/core/status.h"
#include "cedar/fact/fact.h"
#include "cedar/query/result.h"
#include "cedar/schema.h"
#include "cedar/snapshot.h"
#include "cedar/runtime/pressure_controller.h"
#include "cedar/runtime/runtime_metrics.h"
#include "cedar/storage_options.h"
#include "cedar/storage_files.h"
#include "cedar/transaction.h"

namespace cedar {
namespace internal {
class QueryExecutionContextFactory;
}

class Query;

// Query metrics use fixed Cedar-owned dimensions so snapshots remain bounded
// and cannot accidentally acquire query-, property-, or value-level labels.
enum class QueryMetricOperator : uint8_t {
  kScan = 0, kExpand, kJoin, kFilter, kProject, kAggregate, kSort, kCount,
};
enum class QueryMetricTerminal : uint8_t {
  kComplete = 0, kCancelled, kFailed, kCount,
};
enum class QueryMetricFallback : uint8_t {
  kNone = 0, kCanonical, kDelta, kUntrustedStatistics, kCount,
};
enum class QueryMetricAdmission : uint8_t {
  kAdmitted = 0, kQueued, kRejected, kCount,
};
enum class QueryMetricProjection : uint8_t {
  kHit = 0, kFallback, kCount,
};
enum class QueryMetricProjectionHealth : uint8_t {
  kHealthy = 0, kHole, kCorrupt, kStale, kCount,
};
enum class QueryMetricAdjacencyPruning : uint8_t {
  kPruned = 0, kExpanded, kCount,
};
enum class QueryMetricLabelDominance : uint8_t {
  kBalanced = 0, kDominant, kCount,
};

constexpr size_t kQueryMetricHistogramBuckets = 16;

struct QueryMetricsSnapshot {
  std::array<uint64_t, static_cast<size_t>(QueryMetricOperator::kCount)> operator_rows{};
  std::array<uint64_t, static_cast<size_t>(QueryMetricTerminal::kCount)> terminal{};
  std::array<uint64_t, static_cast<size_t>(QueryMetricFallback::kCount)> fallback{};
  std::array<uint64_t, static_cast<size_t>(QueryMetricAdmission::kCount)> admission{};
  std::array<uint64_t, static_cast<size_t>(QueryMetricProjection::kCount)> projection{};
  std::array<uint64_t, static_cast<size_t>(QueryMetricProjectionHealth::kCount)> projection_health{};
  std::array<uint64_t, static_cast<size_t>(QueryMetricAdjacencyPruning::kCount)> adjacency_pruning{};
  std::array<uint64_t, static_cast<size_t>(QueryMetricLabelDominance::kCount)> label_dominance{};
  std::array<uint64_t, kQueryMetricHistogramBuckets> latency_us{};
  std::array<uint64_t, kQueryMetricHistogramBuckets> admission_wait_us{};
  std::array<uint64_t, kQueryMetricHistogramBuckets> worker_wait_us{};
  std::array<uint64_t, kQueryMetricHistogramBuckets> io_wait_us{};
  std::array<uint64_t, kQueryMetricHistogramBuckets> delta_lag{};
  uint64_t batches = 0;
  uint64_t physical_bytes = 0;
  uint64_t decoded_bytes = 0;
  uint64_t interval_fragments = 0;
  uint64_t spill_bytes = 0;
  uint64_t memory_bytes = 0;
  uint64_t scratch_bytes = 0;
};

class QueryMaintenanceHandle {
 public:
  QueryMaintenanceHandle() = default;
  QueryMaintenanceHandle(QueryMaintenanceHandle&&) noexcept;
  QueryMaintenanceHandle& operator=(QueryMaintenanceHandle&&) noexcept;
  QueryMaintenanceHandle(const QueryMaintenanceHandle&) = delete;
  QueryMaintenanceHandle& operator=(const QueryMaintenanceHandle&) = delete;
  ~QueryMaintenanceHandle();
  void Cancel();
  Status Await();

 private:
  class State;
  explicit QueryMaintenanceHandle(std::unique_ptr<State> state);
  std::unique_ptr<State> state_;
  friend class Database;
};

struct CommitLatencyHistogram {
  static constexpr size_t kBucketCount = 13;
  // Upper bounds in microseconds; the final bucket contains all larger values.
  std::array<uint64_t, kBucketCount> buckets{};
  uint64_t count = 0;
  uint64_t total_us = 0;
  uint64_t max_us = 0;

  uint64_t ApproximatePercentile(uint32_t percentile) const;
};

struct CommitGroupFillMetrics {
  static constexpr size_t kBucketCount = 9;
  // Buckets are <=1, <=2, <=4, <=8, <=16, <=32, <=64, <=128, and >128.
  std::array<uint64_t, kBucketCount> buckets{};
  uint64_t groups = 0;
  uint64_t total_transactions = 0;
  uint64_t max_transactions = 0;

  uint64_t ApproximatePercentile(uint32_t percentile) const;
};

constexpr size_t GroupFillBucket(size_t request_count) {
  if (request_count <= 1) return 0;
  if (request_count <= 2) return 1;
  if (request_count <= 4) return 2;
  if (request_count <= 8) return 3;
  if (request_count <= 16) return 4;
  if (request_count <= 32) return 5;
  if (request_count <= 64) return 6;
  if (request_count <= 128) return 7;
  return 8;
}

struct CommitPipelineLatencyMetrics {
  CommitLatencyHistogram collection;
  CommitLatencyHistogram queue;
  CommitLatencyHistogram validation;
  CommitLatencyHistogram assembly;
  CommitLatencyHistogram wal_append;
  CommitLatencyHistogram wal_sync;
  CommitLatencyHistogram wal_callback;
  CommitLatencyHistogram manifest;
  CommitLatencyHistogram memtable_insert;
  CommitLatencyHistogram db_write;
  CommitLatencyHistogram publication;
  CommitLatencyHistogram end_to_end;
};

struct AsyncExecutorOptions {
  uint32_t submission_workers = 1;
  uint32_t max_mailbox_requests = 512;
  uint64_t max_mailbox_bytes = 4ULL * 1024ULL * 1024ULL;
};

enum class PreparedCommitRecoveryPolicy : uint8_t {
  kFinalizeOnOpen = 0,
  kAwaitExternalDecision = 1,
};

struct PreparedCommitBatch {
  TxnId txn_id;
  uint64_t system_hlc = 0;
  std::vector<FactEvent> events;
};

enum class PreparedCommitDecisionOutcome : uint8_t {
  kCommit = 1,
  kAbort = 2,
};

enum class PreparedFinalizeDurability : uint8_t {
  kBuffered = 0,
  kWalSync = 1,
};

struct PreparedCommitDecision {
  PreparedCommitDecisionOutcome outcome =
      PreparedCommitDecisionOutcome::kAbort;
  std::string certificate;
};

struct QueryRuntimeOptions {
  uint32_t query_workers = 4;
  uint32_t reserved_interactive_workers = 1;
  uint64_t query_memory_bytes = 256ULL << 20;
  uint64_t projection_cache_bytes = 256ULL << 20;
  uint64_t query_delta_bytes = 256ULL << 20;
  uint64_t scratch_disk_bytes = 4ULL << 30;
  uint64_t scratch_free_space_reserve_bytes = 2ULL << 30;
  uint64_t read_bytes_per_second = 0;
  uint64_t scratch_bytes_per_second = 0;
  uint64_t max_prefetch_bytes = 8ULL << 20;
};

enum class NPlusOneDiscardReason : uint8_t {
  kPredecessorFailure = 0,
  kIndeterminate,
  kCancelled,
  kGenerationMismatch,
  kBaseMismatch,
  kShutdown,
  kCount,
};

constexpr size_t kNPlusOneDiscardReasonCount =
    static_cast<size_t>(NPlusOneDiscardReason::kCount);

struct DatabaseOptions {
  std::string path;
  uint64_t write_buffer_bytes = 64ULL * 1024ULL * 1024ULL;
  uint64_t block_cache_bytes = 256ULL * 1024ULL * 1024ULL;
  uint64_t blob_threshold_bytes = 4096;
  uint32_t group_commit_max_batch_size = 128;
  uint64_t group_commit_window_us = 200;
  // Group-commit settle: when observed WAL sync cost is at least the floor,
  // collection waits up to this extra age to fill the batch. 0 disables.
  uint64_t group_commit_settle_sync_floor_us = 0;
  uint64_t group_commit_settle_max_age_us = 8'000;
  uint32_t group_commit_settle_sync_divisor = 8;
  // Predicted (pre-encoded) epochs are only worthwhile when assembling a group
  // costs a meaningful fraction of the WAL sync. Below this encoded size the
  // pipeline collects a full group on the general path instead.
  uint64_t predicted_preflight_min_bytes = 32ULL * 1024ULL;
  WalSyncMode wal_sync_mode = WalSyncMode::kFullDeviceFlush;
  std::function<Status()> async_prepare_prewrite_fault_injector_for_testing;
  std::function<Status()> commit_prewrite_fault_injector_for_testing;
  std::function<Status()> commit_fault_injector_for_testing;
  std::function<void(bool sync)> commit_write_options_observer_for_testing;
  std::function<void()> commit_transaction_lookup_observer_for_testing;
  std::function<Status(VacuumFaultPoint)> vacuum_fault_injector_for_testing;
  // Deterministic Cedar-owned publication fault hook. The phase names are
  // segment_sync, manifest_sync, current_replace, delta_enqueue, and
  // scratch_write. Production callers leave this unset.
  std::function<Status(const char*)> query_crash_fault_injector_for_testing;
  uint64_t group_commit_max_batch_bytes = 2ULL * 1024ULL * 1024ULL;
  uint32_t group_commit_max_queue_requests = 1024;
  uint64_t group_commit_max_queue_bytes = 16ULL * 1024ULL * 1024ULL;
  std::function<void()> append_commit_enqueued_observer_for_testing;
  // Test-only hook after the append worker wakes and releases its queue lock,
  // before it selects a WAL group.
  std::function<void()> append_commit_collection_observer_for_testing;
  // Test-only override for the adaptive epoch controller's collection latency
  // SLO. Zero keeps the production default (5 ms).
  uint64_t collection_latency_slo_us_for_testing = 0;
  std::function<void()> commit_result_processing_observer_for_testing;
  // Test-only observer fired by the append worker immediately before it
  // publishes an epoch's terminal results (EpochCompletion::Publish). It runs
  // on the append worker and may park there deterministically; a conforming
  // implementation calls it outside the append bookkeeping lock so publication
  // never serializes collection or queue admission. It must not call back into
  // the database. Production callers leave this unset.
  std::function<void()> epoch_publication_observer_for_testing;
  // Test-only observer fired by the append worker immediately after it signals
  // `append_drained` for an epoch. The boolean reports whether that epoch's
  // terminal results were already published (EpochCompletion) when the drained
  // barrier was notified; shutdown sweeps may only observe published outcomes,
  // so a conforming implementation always passes true. It runs outside the
  // append bookkeeping lock and must not call back into the database.
  std::function<void(bool epoch_published_before_drain)>
      append_drained_observer_for_testing;
  // Test-only observer exposing the kernel commit handle once a legacy
  // (non-pipelined) CommitAsync() observed its terminal state. Legacy callers
  // receive failed terminals as an error status, so this is the only way to
  // assert that a later close sweep does not overwrite an already published
  // epoch-bound result with ShutdownInProgress. Runs after the handle mutex is
  // released, on the submitting thread, and must not call back into the
  // database.
  std::function<void(const std::shared_ptr<CommitHandle::State>&)>
      async_commit_handle_observer_for_testing;
  StorageProfile storage_profile = StorageProfile::kDeveloper;
  ProductionStorageOptions production;
  uint64_t validation_cache_bytes = 8ULL * 1024ULL * 1024ULL;
  // Opt-in durable writer format for committed meta sequence records (F10).
  // 1 (default, unchanged) keeps every new `kSequence` record byte-identical to
  // previous releases; 2 writes compact `kSequenceV2` records and durably
  // records format 2 in the meta format marker during open, before any commit
  // is accepted. The marker wins on reopen: a database upgraded to 2 keeps
  // writing v2 even when a later open leaves this at the default. Format 2 is a
  // one-way compatibility gate for older binaries; see docs/README.md.
  // Database::Open rejects values other than 1 or 2.
  uint32_t meta_sequence_format = 1;
  std::function<void()> validation_scan_observer_for_testing;
  std::function<void()> runtime_sample_observer_for_testing;
  std::function<void(const RuntimeSamplingTiming&)>
      runtime_sampling_timing_observer_for_testing;
  std::function<void()> runtime_snapshot_before_publish_observer_for_testing;
  std::function<void()> runtime_snapshot_published_observer_for_testing;
  std::function<void()> snapshot_open_observer_for_testing;
  // Test-only ordering hooks for the lock-free transaction commit registration.
  // `transaction_commit_registration` fires after Transaction::State::BeginCommit
  // has incremented the active-commit counter and before it re-checks the closing
  // gate; a test parks there to race a Close transition against the registration
  // (a correct implementation must let Close reach its drain wait while this
  // commit is registered). `transaction_commit_admitted` fires after that
  // re-check accepted the commit, before the caller stages/commits its batch.
  // Both run on the committing thread, may park, and must not call back into the
  // database. Production callers leave them unset.
  std::function<void()> transaction_commit_registration_observer_for_testing;
  std::function<void()> transaction_commit_admitted_observer_for_testing;
  // Test-only observer fired by Database::Close immediately before it waits for
  // registered commit calls to drain. It runs outside Database::Impl::mutex and
  // must not call back into the database.
  std::function<void()> active_commit_drain_wait_observer_for_testing;
  std::function<void(uint64_t)> runtime_sampler_interval_observer_for_testing;
  std::function<void()> runtime_sampler_thread_started_observer_for_testing;
  std::function<void(PressureSample*)> runtime_pressure_override_for_testing;
  std::function<void(bool)> kernel_write_observer_for_testing;
  std::function<void(const char*)> shutdown_stage_observer_for_testing;
  // Open-order observer used by recovery and query-planning tests. Names are
  // authoritative_recovery, query_delta_repaired, derived_loaded,
  // query_bind_state_captured (fired by query planning while Database::Impl::mutex
  // is still held, immediately after the synchronized reference capture of the
  // derived query state), and query_bind_state_acquired (fired after that mutex
  // has been released). Observers must not call back into the database; the
  // captured stage in particular runs under the lifecycle mutex.
  std::function<void(const char*)> query_open_stage_observer_for_testing;
  // Forwards the storage-level registered non-epoch operation observer. Tests
  // use it to park such an operation (for example SchemaFingerprint) after its
  // close-drain registration and prove Close waits for it.
  std::function<void()> non_epoch_op_observer_for_testing;
  // Recovery test hook exposing the configured QueryDelta repair byte cap.
  std::function<void(uint64_t)>
      query_delta_repair_budget_observer_for_testing;
  // Test-only lifecycle switch for exercising shutdown discard accounting.
  // Production keeps the normal drain-before-stop ordering.
  //
  // Caveat (test-only, unreachable from production): with this override the
  // append pipeline is stopped *before* the commit drain, so the executor Stop
  // sweep can run while a legacy (non-pipelined) CommitAsync request is not yet
  // bound to its epoch. That sweep may write a synthetic ShutdownInProgress
  // terminal into such a request, which can mask or race the result the append
  // worker publishes afterwards. The normal drain-before-stop path publishes
  // every epoch terminal before the sweep can observe the request, so production
  // (false) never has this window; tests must not combine this switch with
  // legacy submissions whose terminal result they assert.
  bool stop_pipeline_before_drain_for_testing = false;
  uint32_t foreground_admission_concurrency = 0;
  std::function<void()> foreground_admission_observer_for_testing;
  AsyncExecutorOptions async_executor;
  PreparedCommitRecoveryPolicy prepared_commit_recovery =
      PreparedCommitRecoveryPolicy::kFinalizeOnOpen;
  QueryRuntimeOptions query_runtime;
  // Forwards FactStoreOptions::snapshot_boundary_loaded_observer_for_testing.
  // Tests use it to park an as_of open between its retention-boundary read and
  // its epoch registration while Vacuum advances the boundary.
  std::function<void()> snapshot_boundary_loaded_observer_for_testing;
  // Opt-in sequence-record retention (F14). Default false keeps every durable
  // `sequence/*` record and the historical reopen contract unchanged. When
  // true, Database::Vacuum (and a later open) advances the durable
  // `watermark/sequence-retention` boundary to the Vacuum target and deletes
  // the older committed sequence records in bounded, idempotent batches.
  // Pruning below the oldest-readable boundary makes the missing sequence
  // history explicit: reads at or below it report SnapshotExpired instead of
  // silently shortening a range. See docs/README.md ("Sequence-record
  // retention") for the compatibility and crash-safety contract.
  bool prune_sequence_records_on_vacuum = false;
};

struct CommitPipelineMetrics {
  uint64_t submitted = 0;
  uint64_t durably_accepted = 0;
  uint64_t published = 0;
  uint64_t aborted = 0;
  uint64_t indeterminate = 0;
  uint64_t rejected = 0;
  uint64_t epochs = 0;
  uint64_t epoch_transactions = 0;
  uint64_t epoch_bytes = 0;
  uint64_t wal_rotations = 0;
  uint64_t append_fast_path = 0;
  uint64_t general_path = 0;
  uint64_t pending_overlay_peak = 0;
  uint64_t n_plus_one_preflight_transactions = 0;
  uint64_t n_plus_one_decided_transactions = 0;
  uint64_t n_plus_one_preflight_epochs = 0;
  uint64_t n_plus_one_decided_epochs = 0;
  uint64_t n_plus_one_eligible_epochs = 0;
  uint64_t n_plus_one_promoted_epochs = 0;
  uint64_t n_plus_one_discarded_epochs = 0;
  uint64_t n_plus_one_hidden_cpu_us = 0;
  uint64_t n_plus_one_discards = 0;
  uint64_t n_plus_one_eligible = 0;
  uint64_t n_plus_one_promoted = 0;
  std::array<uint64_t, kNPlusOneDiscardReasonCount>
      n_plus_one_discarded_by_reason{};
  uint64_t admission_wait_us = 0;
  uint64_t pressure_normal_us = 0;
  uint64_t pressure_soft_us = 0;
  uint64_t pressure_hard_us = 0;
  uint64_t validation_cache_hits = 0;
  uint64_t validation_cache_misses = 0;
  uint64_t validation_cache_resident_chains = 0;
  uint64_t validation_cache_resident_bytes = 0;
  uint64_t validation_cache_slot_capacity = 0;
  uint64_t validation_cache_reserved_bytes = 0;
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
  // S3: a reopen whose facts CF outgrew the fixed identity-history Bloom budget
  // skips the rebuild and leaves the filter always-true. `disabled` exposes that
  // state (resident_bytes is then zero and every probe falls back to the
  // canonical read).
  uint64_t identity_history_resident_bytes = 0;
  bool identity_history_disabled = false;

  uint64_t async_mailbox_accepted = 0;
  uint64_t async_mailbox_rejected = 0;
  uint64_t async_runtime_snapshot_stale_rejected = 0;
  uint64_t async_runtime_pressure_rejected = 0;
  uint64_t async_mailbox_requests_reserved = 0;
  uint64_t async_mailbox_bytes_reserved = 0;
  uint64_t async_mailbox_requests_reserved_peak = 0;
  uint64_t async_mailbox_bytes_reserved_peak = 0;
  CommitGroupFillMetrics group_fill;
  CommitPipelineLatencyMetrics latency;
  RuntimeMetrics runtime;
  PressureState pressure_state = PressureState::kNormal;
};

class Database {
 public:
  static StatusOr<std::unique_ptr<Database>> Open(DatabaseOptions options);

  // Destroys the database (attempting a normal Close first). A Snapshot handle
  // that still pins a storage epoch at destruction is force-detached: its epoch
  // payload (the storage snapshot lease and the store back-reference) is
  // released and the storage handles are destroyed, so it cannot keep the
  // database's on-disk open state alive.
  //
  // Serialization contract (required for every outliving handle): destruction
  // must be serialized with every user of the Snapshot. While the Database is
  // being destroyed, no thread may read through the Snapshot (Get/Exists/Multi*/
  // Scan*/EventScan/StateScan, canonical_reader/adjacency_index), inspect it
  // (commit_seq/oldest_readable_seq), or release/destroy it: a read goes through
  // the payload and state destroyed here. Destroy the Database, wait for its
  // destruction to finish, then destroy the Snapshot handle; destroying it
  // afterwards is safe and is the required way to retire it. Using it
  // concurrently with destruction is not supported. The normal path - Close()
  // before destruction, or destruction with no outliving handle - is unchanged.
  // See FactStore::~FactStore for the storage-level contract.
  ~Database();
  Database(const Database&) = delete;
  Database& operator=(const Database&) = delete;

  Status Close();
  // Creates an openable, point-in-time database snapshot owned by the
  // underlying storage engine. The snapshot uses Cedar's normal recovery
  // contract and does not expose implementation handles.
  Status CreateCheckpoint(const std::string& checkpoint_path) const;
  StatusOr<VertexId> AllocateVertexId();
  StatusOr<EdgeId> AllocateEdgeId();
  StatusOr<PropertyDefinition> RegisterProperty(PropertyDefinition definition);
  StatusOr<std::optional<PropertyDefinition>> LookupProperty(
      PropertyId property_id, uint32_t schema_epoch = 0) const;
  StatusOr<std::unique_ptr<Transaction>> BeginTransaction(
      TransactionOptions options = {});
  StatusOr<Snapshot> BeginSnapshot(SnapshotOptions options = {}) const;
  // Reads one fact point at the latest visible commit sequence without
  // materializing a Snapshot object. Each call pins the current snapshot epoch
  // for a single point-read operation through the same internal path as
  // Snapshot::Get (counted once in point-read metrics; an edge-property read
  // additionally checks both endpoint vertex states at valid_time), so it
  // observes the latest visible sequence and holds no cached handle between
  // calls. Semantics match BeginSnapshot() + Snapshot::Get for every property
  // fact: missing or deleted facts yield an empty optional, and edge-property
  // facts additionally require both endpoint vertex states to be visible at
  // valid_time.
  StatusOr<std::optional<Value>> GetPointAt(FactRef ref,
                                            ValidTime valid_time) const;
  // Reads one fact point at an explicit historical commit sequence (the as_of
  // vintage) without materializing a Snapshot object. The sequence must be
  // within the readable range: a sequence below the durable retention boundary
  // reports SnapshotExpired and one above the visible watermark reports
  // InvalidArgument, exactly like BeginSnapshot(SnapshotOptions{as_of}) followed
  // by Snapshot::Get. Every property fact - including edge-property endpoint
  // visibility at valid_time - resolves through the same shared point-read path
  // as GetPointAt; only the pinned commit sequence differs. The read is pinned
  // at exactly `sequence`: a commit that is published concurrently with the
  // call is never observed, even when its epoch is created before this call
  // pins one.
  StatusOr<std::optional<Value>> GetPointAtAt(CommitSeq sequence, FactRef ref,
                                              ValidTime valid_time) const;
  StatusOr<PreparedQuery> PrepareQuery(const Query& query) const;
  StatusOr<QueryMaintenanceHandle> RefreshQueryStatistics();
  // Builds a snapshot-bound typed property index in the derived-read lane.
  // The operation never runs on the commit/WAL critical path.
  StatusOr<QueryMaintenanceHandle> RefreshQueryIndexes(ValidTime valid_time);
  StatusOr<std::optional<CommitResult>> ResolveTransaction(TxnId txn_id) const;
  Status PersistPreparedCommit(const PreparedCommitBatch& batch);
  StatusOr<std::vector<PreparedCommitBatch>> ListPreparedCommits() const;
  StatusOr<std::optional<PreparedCommitDecision>>
  ResolvePreparedCommitDecision(TxnId txn_id) const;
  StatusOr<CommitResult> FinalizePreparedCommit(
      TxnId txn_id, std::string decision_certificate,
      PreparedFinalizeDurability durability =
          PreparedFinalizeDurability::kBuffered);
  Status AbortPreparedCommit(TxnId txn_id,
                             std::string decision_certificate);
  CommitPipelineMetrics GetCommitPipelineMetrics() const;
  StatusOr<RuntimeMetrics> SampleRuntimeMetrics() const;
  QueryMetricsSnapshot SampleQueryMetrics() const;
  Status Vacuum(CommitSeq oldest_readable);

 private:
  class Impl;
  explicit Database(std::shared_ptr<Impl> impl);
  Database(std::shared_ptr<Impl> impl, bool close_on_destroy);

  std::shared_ptr<Impl> impl_;
  bool close_on_destroy_ = true;

  friend class Snapshot;
  friend class Transaction;
  friend class PreparedQuery;
  friend class internal::QueryExecutionContextFactory;
};

}  // namespace cedar

#endif  // CEDAR_DATABASE_H_
