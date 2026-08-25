// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_DATABASE_H_
#define CEDAR_DATABASE_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
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
  std::function<void()> commit_result_processing_observer_for_testing;
  StorageProfile storage_profile = StorageProfile::kDeveloper;
  ProductionStorageOptions production;
  uint64_t validation_cache_bytes = 8ULL * 1024ULL * 1024ULL;
  std::function<void()> validation_scan_observer_for_testing;
  std::function<void()> runtime_sample_observer_for_testing;
  std::function<void(const RuntimeSamplingTiming&)>
      runtime_sampling_timing_observer_for_testing;
  std::function<void()> runtime_snapshot_before_publish_observer_for_testing;
  std::function<void()> runtime_snapshot_published_observer_for_testing;
  std::function<void()> snapshot_open_observer_for_testing;
  std::function<void(uint64_t)> runtime_sampler_interval_observer_for_testing;
  std::function<void()> runtime_sampler_thread_started_observer_for_testing;
  std::function<void(PressureSample*)> runtime_pressure_override_for_testing;
  std::function<void(bool)> kernel_write_observer_for_testing;
  std::function<void(const char*)> shutdown_stage_observer_for_testing;
  // Open-order observer used by recovery tests. Names are
  // authoritative_recovery, query_delta_repaired, and derived_loaded.
  std::function<void(const char*)> query_open_stage_observer_for_testing;
  // Recovery test hook exposing the configured QueryDelta repair byte cap.
  std::function<void(uint64_t)>
      query_delta_repair_budget_observer_for_testing;
  // Test-only lifecycle switch for exercising shutdown discard accounting.
  // Production keeps the normal drain-before-stop ordering.
  bool stop_pipeline_before_drain_for_testing = false;
  uint32_t foreground_admission_concurrency = 0;
  std::function<void()> foreground_admission_observer_for_testing;
  AsyncExecutorOptions async_executor;
  QueryRuntimeOptions query_runtime;
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
  StatusOr<std::unique_ptr<Transaction>> BeginTransaction(
      TransactionOptions options = {});
  StatusOr<Snapshot> BeginSnapshot(SnapshotOptions options = {}) const;
  StatusOr<PreparedQuery> PrepareQuery(const Query& query) const;
  StatusOr<QueryMaintenanceHandle> RefreshQueryStatistics();
  StatusOr<std::optional<CommitResult>> ResolveTransaction(TxnId txn_id) const;
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
