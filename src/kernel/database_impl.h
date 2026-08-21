// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_KERNEL_DATABASE_IMPL_H_
#define CEDAR_KERNEL_DATABASE_IMPL_H_

#include <algorithm>
#include <condition_variable>
#include <cstddef>
#include <chrono>
#include <atomic>
#include <limits>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cedar/database.h"
#include "storage/facts/fact_store.h"
#include "cedar/runtime/pressure_controller.h"
#include "storage/facts/group_commit_planner.h"
#include "storage/rocks/decided_epoch.h"
#include "storage/facts/pending_version_overlay.h"
#include "kernel/async_commit.h"
#include "kernel/adaptive_epoch_controller.h"
#include "kernel/epoch_completion.h"
#include "kernel/async_submission_executor.h"
#include "kernel/maintenance_controller.h"
#include "kernel/maintenance_policy.h"
#include "query/projection/projection_store.h"
#include "query/projection/query_delta.h"
#include "query/resource/query_resource_pool.h"

namespace cedar {

namespace internal {
class AdjacencyIndex;
}

inline AsyncSubmissionExecutor::Options ResolveAsyncExecutorOptions(
    const DatabaseOptions& options) {
  AsyncSubmissionExecutor::Options resolved{
      options.async_executor.submission_workers,
      options.async_executor.max_mailbox_requests,
      options.async_executor.max_mailbox_bytes};
  const AsyncExecutorOptions defaults;
  if (!UsesCedarKernelProfile(options.storage_profile) &&
      options.async_executor.submission_workers == defaults.submission_workers &&
      options.async_executor.max_mailbox_requests == defaults.max_mailbox_requests &&
      options.async_executor.max_mailbox_bytes == defaults.max_mailbox_bytes) {
    resolved.worker_count = 2;
    resolved.max_requests = std::max<uint32_t>(
        1024, options.group_commit_max_queue_requests);
    resolved.max_bytes = std::max<uint64_t>(
        16ULL * 1024ULL * 1024ULL, options.group_commit_max_queue_bytes);
  }
  return resolved;
}

class Database::Impl {
 public:
  class FactStoreMaintenanceAdapter final : public MaintenanceAdapter {
   public:
    FactStoreMaintenanceAdapter(FactStore* store,
                                std::atomic<bool>* wal_sync_critical)
        : store_(store), wal_sync_critical_(wal_sync_critical) {}
    StatusOr<CedarMaintenanceCompletion> RunFlush(
        const CedarMaintenanceDecision& decision,
        const std::atomic<bool>* wal_sync_critical) override;
    StatusOr<CedarMaintenanceCompletion> RunCompaction(
        const CedarMaintenanceDecision& decision,
        const std::atomic<bool>* wal_sync_critical) override;

   private:
    StatusOr<CedarMaintenanceCompletion> Run(
        const CedarMaintenanceDecision& decision,
        const std::atomic<bool>* wal_sync_critical);
    FactStore* store_;
    std::atomic<bool>* wal_sync_critical_;
  };
  enum class SlotState : uint8_t {
    kEmpty = 0,
    kEligible,
    kPromoted,
    kDiscarded,
  };

  struct AppendCommitRequest {
    StoreCommitBatch batch;
    TxnId txn_id;
    size_t estimated_bytes = 0;
    internal::CommitFootprint preflight_footprint;
    std::chrono::steady_clock::time_point enqueued_at;
    std::shared_ptr<CommitHandle::State> handle;
    // The mailbox and the active WAL epoch own the ticket. Keeping this as a
    // weak reference prevents the ticket callbacks from retaining the request
    // after terminal completion.
    std::weak_ptr<AsyncSubmissionExecutor::Ticket> executor_ticket;
    std::atomic<bool> cancelled{false};
    bool selected = false;
    std::optional<StatusOr<StoreCommitResult>> result;
  };

  struct DecidedEpochSlot {
    uint64_t generation = 0;
    uint64_t preparation_generation = 0;
    CommitSeq predecessor_base_visible_seq;
    CommitSeq successor_base_visible_seq;
    uint64_t preflight_transactions = 0;
    std::vector<std::shared_ptr<AppendCommitRequest>> requests;
    std::unique_ptr<internal::DecidedEpoch> epoch;
    SlotState state = SlotState::kEmpty;
  };
  struct WalDurabilityContext {
    std::shared_ptr<internal::EpochCompletion> completion;
    std::vector<std::shared_ptr<AsyncSubmissionExecutor::Ticket>> executor_tickets;
    AsyncSubmissionExecutor* executor = nullptr;
    std::chrono::steady_clock::time_point write_started_at;
    std::atomic<bool> wal_durable{false};
    std::atomic<uint64_t> wal_callback_us{0};
    std::atomic<uint64_t> wal_callback_duration_us{0};
  };
  class ForegroundAdmissionSlot {
   public:
    explicit ForegroundAdmissionSlot(Impl* database);
    ~ForegroundAdmissionSlot();

    ForegroundAdmissionSlot(const ForegroundAdmissionSlot&) = delete;
    ForegroundAdmissionSlot& operator=(const ForegroundAdmissionSlot&) = delete;

   private:
    Impl* database_;
    bool held_ = false;
  };
  explicit Impl(DatabaseOptions options)
      : append_commit_max_batch_size(options.group_commit_max_batch_size),
        append_commit_window_us(options.group_commit_window_us),
        append_commit_max_batch_bytes(options.group_commit_max_batch_bytes),
        append_commit_max_queue_requests(options.group_commit_max_queue_requests),
        append_commit_max_queue_bytes(options.group_commit_max_queue_bytes),
        adaptive_epoch_controller(internal::AdaptiveEpochController::Options{
            options.group_commit_max_batch_size,
            options.group_commit_max_batch_bytes,
            5'000,
            options.group_commit_window_us}),
        append_commit_enqueued_observer_for_testing(
            std::move(options.append_commit_enqueued_observer_for_testing)),
        append_commit_collection_observer_for_testing(
            std::move(options.append_commit_collection_observer_for_testing)),
        commit_result_processing_observer_for_testing(
            std::move(options.commit_result_processing_observer_for_testing)),
        runtime_sampler_interval_observer_for_testing(
            std::move(options.runtime_sampler_interval_observer_for_testing)),
        runtime_sampler_thread_started_observer_for_testing(
            std::move(options.runtime_sampler_thread_started_observer_for_testing)),
        runtime_sampling_timing_observer_for_testing(
            std::move(options.runtime_sampling_timing_observer_for_testing)),
        runtime_snapshot_before_publish_observer_for_testing(
            std::move(options.runtime_snapshot_before_publish_observer_for_testing)),
        runtime_snapshot_published_observer_for_testing(
            std::move(options.runtime_snapshot_published_observer_for_testing)),
        runtime_pressure_override_for_testing(
            std::move(options.runtime_pressure_override_for_testing)),
        shutdown_stage_observer_for_testing(
            std::move(options.shutdown_stage_observer_for_testing)),
        stop_pipeline_before_drain_for_testing(
            options.stop_pipeline_before_drain_for_testing),
        foreground_admission_concurrency(
            options.foreground_admission_concurrency != 0
                ? (UsesCedarKernelProfile(options.storage_profile)
                       ? std::min(options.foreground_admission_concurrency,
                                  std::max(1U, std::thread::hardware_concurrency()))
                       : options.foreground_admission_concurrency)
                : (UsesCedarKernelProfile(options.storage_profile)
                       ? std::max(1U, std::thread::hardware_concurrency())
                       : 0)),
        foreground_admission_observer_for_testing(
            std::move(options.foreground_admission_observer_for_testing)),
        enforce_disk_pressure(
            UsesCedarKernelProfile(options.storage_profile)),
        // Keep the database path available for query scratch ownership.
        store(FactStoreOptions{options.path,
                               options.write_buffer_bytes,
                               options.block_cache_bytes,
                               options.blob_threshold_bytes,
                               options.group_commit_max_batch_size,
                               options.group_commit_window_us,
                               std::move(options.async_prepare_prewrite_fault_injector_for_testing),
                               std::move(options.commit_prewrite_fault_injector_for_testing),
                               std::move(options.commit_fault_injector_for_testing),
                               std::move(options.commit_write_options_observer_for_testing),
                               std::move(options.commit_transaction_lookup_observer_for_testing),
                               std::move(options.vacuum_fault_injector_for_testing),
                               options.group_commit_max_batch_bytes,
                               options.storage_profile,
                               options.production,
                               options.validation_cache_bytes,
                               std::move(options.validation_scan_observer_for_testing),
                               std::move(options.runtime_sample_observer_for_testing),
                               std::move(options.snapshot_open_observer_for_testing),
                               std::move(options.kernel_write_observer_for_testing)}),
        async_executor(ResolveAsyncExecutorOptions(options)),
        query_runtime_options(options.query_runtime),
        query_database_path(options.path) {
    internal::QueryResourcePoolOptions pool_options;
    pool_options.memory_bytes = query_runtime_options.query_memory_bytes;
    pool_options.scratch_bytes = query_runtime_options.scratch_disk_bytes;
    pool_options.scratch_free_space_reserve_bytes =
        query_runtime_options.scratch_free_space_reserve_bytes;
    pool_options.read_bytes_per_second = query_runtime_options.read_bytes_per_second;
    pool_options.scratch_bytes_per_second = query_runtime_options.scratch_bytes_per_second;
    pool_options.scratch_root = query_database_path;
    pool_options.scratch_instance = "active";
    pool_options.read_bytes = UINT64_MAX;
    // Prefetch is a per-query reservation. Size the pool cap for all admitted
    // workers so independent default queries do not reject one another merely
    // because each consumes the full per-query allowance.
    pool_options.prefetch_bytes =
        query_runtime_options.max_prefetch_bytes >
                std::numeric_limits<uint64_t>::max() /
                    query_runtime_options.query_workers
            ? std::numeric_limits<uint64_t>::max()
            : query_runtime_options.max_prefetch_bytes *
                  query_runtime_options.query_workers;
    pool_options.decoded_rows = UINT64_MAX;
    pool_options.output_rows = UINT64_MAX;
    pool_options.output_bytes = UINT64_MAX;
    pool_options.interval_fragments = UINT64_MAX;
    pool_options.graph_labels = UINT64_MAX;
    pool_options.visited_vertices = UINT64_MAX;
    pool_options.cpu_us = UINT64_MAX;
    pool_options.max_parallelism = query_runtime_options.query_workers;
    pool_options.wal_sync_critical = &wal_sync_critical;
    query_resource_pool = std::make_unique<internal::QueryResourcePool>(pool_options);
  }

  ~Impl();

  Status StartAppendCommitPipeline();
  Status StartRuntimeSampler();
  void StopRuntimeSampler();
  CedarRuntimeSnapshot ReadRuntimeSnapshot() const;
  bool RuntimeSnapshotIsFresh() const;
  Status RefreshRuntimeSnapshot();
  void ObserveAppendPressure(const PressureSample& sample);
  void AccountPressureTime();
  static void NotifyWalDurable(void* context) noexcept;
  StatusOr<StoreCommitResult> SubmitSyncCommit(const StoreCommitBatch& batch,
                                               uint64_t deadline_us = 0);
  Status SubmitAsyncCommit(const StoreCommitBatch& batch,
                           std::shared_ptr<CommitHandle::State> handle,
                           uint64_t deadline_us = 0);
  Status SubmitAsyncCommitToAppendPipeline(
      const std::shared_ptr<AppendCommitRequest>& request);
  void CancelQueuedAsyncCommit(
      const std::shared_ptr<AppendCommitRequest>& request);
  internal::EpochLimits LimitsForQueueLocked() const;
  Status DrainAppendCommitPipeline();
  void StopAppendCommitPipeline();
  void ObserveShutdownStage(const char* stage) {
    if (shutdown_stage_observer_for_testing) {
      shutdown_stage_observer_for_testing(stage);
    }
  }
  Status ValidatePreparedQuery(
      CommitSeq snapshot_seq,
      const std::vector<PropertyDefinition>& schema_fingerprint) const;

  mutable std::mutex mutex;
  std::condition_variable commits_drained;
  std::string query_database_path;
  FactStore store;
  std::unique_ptr<internal::QueryProjectionStore> projection_store;
  // Derived, rebuildable commit tail.  It is deliberately independent from
  // the durable commit path; queue overflow records a gap but never rejects a
  // commit that RocksDB has already published.
  std::unique_ptr<internal::QueryDelta> query_delta;
  std::shared_ptr<internal::AdjacencyIndex> adjacency_index;
  AsyncSubmissionExecutor async_executor;
  QueryRuntimeOptions query_runtime_options;
  std::unique_ptr<internal::QueryResourcePool> query_resource_pool;
  uint64_t next_vertex_id = 0;
  uint64_t vertex_lease_limit = 0;
  uint64_t next_edge_id = 0;
  uint64_t edge_lease_limit = 0;
  size_t active_commit_calls = 0;
  uint32_t append_commit_max_batch_size = 1;
  uint64_t append_commit_window_us = 0;
  uint64_t append_commit_max_batch_bytes = 2ULL * 1024ULL * 1024ULL;
  uint32_t append_commit_max_queue_requests = 1024;
  uint64_t append_commit_max_queue_bytes = 16ULL * 1024ULL * 1024ULL;
  // Accessed only by the ordered append worker. Runtime pressure remains
  // separately published through atomics for the preflight worker.
  internal::AdaptiveEpochController adaptive_epoch_controller;
  std::function<void()> append_commit_enqueued_observer_for_testing;
  std::function<void()> append_commit_collection_observer_for_testing;
  std::function<void()> commit_result_processing_observer_for_testing;
  std::function<void(uint64_t)> runtime_sampler_interval_observer_for_testing;
  std::function<void()> runtime_sampler_thread_started_observer_for_testing;
  std::function<void(const RuntimeSamplingTiming&)>
      runtime_sampling_timing_observer_for_testing;
  std::function<void()> runtime_snapshot_before_publish_observer_for_testing;
  std::function<void()> runtime_snapshot_published_observer_for_testing;
  std::function<void(PressureSample*)> runtime_pressure_override_for_testing;
  std::function<void(const char*)> shutdown_stage_observer_for_testing;
  bool stop_pipeline_before_drain_for_testing = false;
  bool enforce_disk_pressure = false;
  std::mutex append_commit_mutex;
  std::condition_variable append_commit_cv;
  std::deque<std::shared_ptr<AppendCommitRequest>> append_commit_requests;
  std::thread append_commit_worker;
  std::thread append_preflight_worker;
  std::thread runtime_sampler_worker;
  std::mutex runtime_sampler_mutex;
  std::mutex runtime_refresh_mutex;
  std::condition_variable runtime_sampler_cv;
  size_t active_append_commit_requests = 0;
  uint64_t queued_append_commit_bytes = 0;
  uint64_t reserved_append_commit_bytes = 0;
  std::shared_ptr<const internal::PendingVersionOverlay>
      active_pending_version_overlay;
  uint64_t active_epoch_generation = 0;
  uint64_t active_n_plus_one_preflight_transactions = 0;
  uint64_t active_n_plus_one_decided_transactions = 0;
  uint64_t accounted_n_plus_one_preflight_generation = 0;
  uint64_t accounted_n_plus_one_decided_generation = 0;
  CommitSeq active_epoch_base_visible_seq;
  uint64_t active_epoch_committed_count_hint = 0;
  std::optional<DecidedEpochSlot> next_epoch_slot;
  uint64_t preflight_request_generation = 0;
  uint64_t preflight_completed_generation = 0;
  // The sampler owns the mutable controller. The commit path consumes only
  // these atomically published admission limits and never blocks publication.
  std::mutex runtime_pressure_mutex;
  PressureController append_commit_pressure_controller;
  std::chrono::steady_clock::time_point pressure_last_observed_at;
  bool pressure_clock_initialized = false;
  std::atomic<PressureState> runtime_pressure_state{PressureState::kNormal};
  std::atomic<uint32_t> runtime_target_count{128};
  std::atomic<uint64_t> runtime_target_bytes{2ULL * 1024ULL * 1024ULL};
  std::atomic<uint64_t> runtime_collection_window_us{200};
  std::atomic<bool> runtime_admission_permitted{true};
  std::atomic<uint64_t> runtime_pressure_normal_us{0};
  std::atomic<uint64_t> runtime_pressure_soft_us{0};
  std::atomic<uint64_t> runtime_pressure_hard_us{0};
  std::atomic<uint64_t> runtime_queued_request_count{0};
  CommitPipelineMetrics append_commit_metrics;
  uint32_t foreground_admission_concurrency = 0;
  std::mutex foreground_admission_mutex;
  std::condition_variable foreground_admission_cv;
  uint32_t foreground_admissions_inflight = 0;
  std::function<void()> foreground_admission_observer_for_testing;
  bool append_commit_stopping = false;
  bool runtime_sampler_stopping = false;
  std::atomic<bool> wal_sync_critical{false};
  std::shared_ptr<const CedarRuntimeSnapshot> runtime_snapshot;
  std::unique_ptr<FactStoreMaintenanceAdapter> maintenance_adapter;
  std::unique_ptr<MaintenanceController> maintenance_controller;
  bool closing = false;
  bool closed = false;
};

}  // namespace cedar

#endif  // CEDAR_KERNEL_DATABASE_IMPL_H_
