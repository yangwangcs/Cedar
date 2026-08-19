// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/database.h"

#include <algorithm>
#include <chrono>
#include <unordered_set>
#include <utility>

#if defined(__APPLE__)
#include <pthread/qos.h>
#endif

#include "storage/facts/group_commit_planner.h"
#include "kernel/database_impl.h"

namespace cedar {
namespace {

constexpr std::array<uint64_t, CommitLatencyHistogram::kBucketCount>
    kLatencyBucketUpperBounds = {
        10, 50, 100, 250, 500, 1'000, 2'000,
        5'000, 10'000, 25'000, 50'000, 100'000, UINT64_MAX};

constexpr uint64_t kRuntimeSnapshotStaleUs = 250'000;

void ApplyCedarSamplerScheduling() {
#if defined(__APPLE__)
  // A best-effort scheduler hint keeps Cedar's bounded-staleness safety loop
  // runnable under saturated foreground clients. Admission still fails closed
  // if the OS cannot schedule the sampler inside the stale-snapshot bound.
  pthread_set_qos_class_self_np(QOS_CLASS_USER_INTERACTIVE, 0);
#endif
}

uint64_t SteadyNowMicros() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
}

void RecordLatency(CommitLatencyHistogram* histogram, uint64_t elapsed_us) {
  ++histogram->count;
  histogram->total_us += elapsed_us;
  histogram->max_us = std::max(histogram->max_us, elapsed_us);
  for (size_t index = 0; index < kLatencyBucketUpperBounds.size(); ++index) {
    if (elapsed_us <= kLatencyBucketUpperBounds[index]) {
      ++histogram->buckets[index];
      return;
    }
  }
}

CommitResult ToCommitResult(TxnId txn_id,
                            const StatusOr<StoreCommitResult>& result) {
  if (result.ok()) {
    return CommitResult{CommitOutcome::kCommitted, result.ValueOrDie().commit_seq,
                        txn_id, Status::OK()};
  }
  const Status status = result.status();
  return CommitResult{status.IsIndeterminate() || status.IsRecoveryRequired()
                          ? CommitOutcome::kIndeterminate
                          : CommitOutcome::kAborted,
                      CommitSeq{}, txn_id, status};
}

void CompleteCommitHandle(const std::shared_ptr<CommitHandle::State>& handle,
                          CommitResult result) {
  if (!handle) return;
  std::lock_guard<std::mutex> lock(handle->mutex);
  handle->result.emplace(std::move(result));
  handle->completed.notify_all();
}

RuntimeMetrics ToRuntimeMetrics(const RocksDbRuntimeMetrics& source) {
  RuntimeMetrics metrics;
  metrics.retained_wal_bytes = source.retained_wal_bytes;
  metrics.active_fact_bytes = source.total_active_memtable_bytes;
  metrics.immutable_fact_bytes = source.total_immutable_memtable_bytes;
  metrics.immutable_fact_count = source.total_immutable_memtable_count;
  metrics.l0_file_count = source.total_l0_files;
  metrics.pending_compaction_bytes = source.total_pending_compaction_bytes;
  metrics.write_buffer_bytes = source.write_buffer_manager_bytes;
  metrics.write_buffer_limit_bytes = source.write_buffer_manager_limit_bytes;
  metrics.background_error_count = source.background_errors_total;
  metrics.cache_usage_bytes = source.block_cache_usage_bytes;
  metrics.cache_pinned_bytes = source.block_cache_pinned_bytes;
  metrics.running_flushes = source.running_flushes;
  metrics.running_compactions = source.running_compactions;
  metrics.live_fact_bytes = source.live_sst_bytes;
  metrics.write_stopped = source.write_stopped;
  metrics.delayed_write_rate_bytes_per_sec =
      source.delayed_write_rate_bytes_per_sec;
  metrics.cache_hits = source.block_cache_hits;
  metrics.cache_misses = source.block_cache_misses;
  metrics.compressed_block_count = source.blocks_compressed;
  metrics.compression_input_bytes = source.compression_input_bytes;
  metrics.compression_output_bytes = source.compression_output_bytes;
  metrics.point_read_operations = source.point_read_operations;
  metrics.multi_get_operations = source.multi_get_operations;
  metrics.projected_scan_rows = source.projected_scan_rows;
  metrics.projected_scan_bytes_read = source.projected_scan_bytes_read;
  metrics.canonical_scan_bytes_read = source.canonical_scan_bytes_read;
  metrics.logical_facts_bytes = source.logical_facts_bytes;
  metrics.obsolete_fact_bytes = source.obsolete_sst_bytes;
  metrics.temporary_output_bytes = source.temporary_output_bytes;
  metrics.free_disk_bytes = source.free_disk_bytes;
  metrics.free_disk_percent = source.free_disk_percent;
  return metrics;
}

bool CanDecideIndependentAppend(const StoreCommitBatch& batch,
                                CommitSeq required_snapshot_seq) {
  if (!batch.fresh_transaction_id ||
      !internal::CanUseAppendFastPath(batch)) {
    return false;
  }
  return std::all_of(
      batch.snapshot_write_dependencies.begin(),
      batch.snapshot_write_dependencies.end(),
      [required_snapshot_seq](const SnapshotWriteDependency& dependency) {
        return dependency.snapshot_seq == required_snapshot_seq &&
               !dependency.predecessor.has_value() &&
               !dependency.successor.has_value();
      });
}

void RecordNPlusOneDiscard(CommitPipelineMetrics* metrics,
                           NPlusOneDiscardReason reason, uint64_t count) {
  if (count == 0) return;
  metrics->n_plus_one_discards += count;
  ++metrics->n_plus_one_discarded_epochs;
  metrics->n_plus_one_discarded_by_reason[static_cast<size_t>(reason)] +=
      count;
}

NPlusOneDiscardReason DiscardReasonForStatus(const Status& status) {
  if (status.IsQueryCancelled()) return NPlusOneDiscardReason::kCancelled;
  if (status.IsIndeterminate() || status.IsRecoveryRequired()) {
    return NPlusOneDiscardReason::kIndeterminate;
  }
  return NPlusOneDiscardReason::kPredecessorFailure;
}

}  // namespace

StatusOr<CedarMaintenanceCompletion>
Database::Impl::FactStoreMaintenanceAdapter::RunFlush(
    const CedarMaintenanceDecision& decision,
    const std::atomic<bool>* wal_sync_critical) {
  return Run(decision, wal_sync_critical);
}

StatusOr<CedarMaintenanceCompletion>
Database::Impl::FactStoreMaintenanceAdapter::RunCompaction(
    const CedarMaintenanceDecision& decision,
    const std::atomic<bool>* wal_sync_critical) {
  return Run(decision, wal_sync_critical);
}

StatusOr<CedarMaintenanceCompletion>
Database::Impl::FactStoreMaintenanceAdapter::Run(
    const CedarMaintenanceDecision& decision,
    const std::atomic<bool>* wal_sync_critical) {
  FactStoreMaintenanceRequest request;
  request.kind = decision.kind == CedarMaintenanceKind::kFlush
                     ? FactStoreMaintenanceKind::kFlush
                     : FactStoreMaintenanceKind::kCompaction;
  request.snapshot_generation = decision.snapshot_generation;
  request.max_input_bytes = decision.max_input_bytes;
  request.max_output_bytes = decision.max_output_bytes;
  request.deadline_us = decision.deadline_us;
  request.emergency = decision.priority == CedarMaintenancePriority::kEmergency;
  request.yield_for_wal_sync = decision.yield_for_wal_sync;
  const auto native = store_->RunNativeMaintenance(request, wal_sync_critical_);
  if (!native.ok()) return native.status();
  const FactStoreMaintenanceResult& result = native.ValueOrDie();
  CedarMaintenanceCompletion completion;
  completion.grant_id = result.grant_id;
  completion.kind = decision.kind;
  completion.input_bytes = result.input_bytes;
  completion.output_bytes = result.output_bytes;
  completion.elapsed_us = result.elapsed_us;
  completion.remaining_smallest_complete_unit_bytes =
      result.remaining_smallest_complete_unit_bytes;
  completion.status = result.status;
  switch (result.yield) {
    case FactStoreMaintenanceYield::kNone:
      completion.yield = CedarMaintenanceYield::kNone;
      break;
    case FactStoreMaintenanceYield::kNoDebt:
      completion.yield = CedarMaintenanceYield::kNoDebt;
      break;
    case FactStoreMaintenanceYield::kStaleGeneration:
      completion.yield = CedarMaintenanceYield::kStaleGeneration;
      break;
    case FactStoreMaintenanceYield::kInputBudget:
      completion.yield = CedarMaintenanceYield::kInputBudget;
      break;
    case FactStoreMaintenanceYield::kOutputBudget:
      completion.yield = CedarMaintenanceYield::kOutputBudget;
      break;
    case FactStoreMaintenanceYield::kDeadline:
      completion.yield = CedarMaintenanceYield::kDeadline;
      break;
    case FactStoreMaintenanceYield::kWalSync:
      completion.yield = CedarMaintenanceYield::kWalSync;
      break;
    case FactStoreMaintenanceYield::kManualConflict:
      completion.yield = CedarMaintenanceYield::kManualConflict;
      break;
    case FactStoreMaintenanceYield::kRecovery:
      completion.yield = CedarMaintenanceYield::kRecovery;
      break;
    case FactStoreMaintenanceYield::kShutdown:
      completion.yield = CedarMaintenanceYield::kShutdown;
      break;
    case FactStoreMaintenanceYield::kInvariantViolation:
      completion.yield = CedarMaintenanceYield::kInvariantViolation;
      break;
  }
  return completion;
}

Database::Impl::ForegroundAdmissionSlot::ForegroundAdmissionSlot(
    Impl* database)
    : database_(database) {
  if (database_->foreground_admission_concurrency == 0) return;
  std::unique_lock<std::mutex> lock(database_->foreground_admission_mutex);
  database_->foreground_admission_cv.wait(lock, [this] {
    return database_->foreground_admissions_inflight <
           database_->foreground_admission_concurrency;
  });
  ++database_->foreground_admissions_inflight;
  held_ = true;
}

Database::Impl::ForegroundAdmissionSlot::~ForegroundAdmissionSlot() {
  if (!held_) return;
  {
    std::lock_guard<std::mutex> lock(database_->foreground_admission_mutex);
    --database_->foreground_admissions_inflight;
  }
  database_->foreground_admission_cv.notify_one();
}

uint64_t CommitLatencyHistogram::ApproximatePercentile(uint32_t percentile) const {
  if (count == 0) return 0;
  const uint64_t clamped = std::min<uint32_t>(percentile, 100);
  const uint64_t rank = std::max<uint64_t>(
      1, (count * clamped + 99) / 100);
  uint64_t cumulative = 0;
  for (size_t index = 0; index < buckets.size(); ++index) {
    cumulative += buckets[index];
    if (cumulative >= rank) {
      return index + 1 == buckets.size() ? max_us : kLatencyBucketUpperBounds[index];
    }
  }
  return max_us;
}

void Database::Impl::ObserveAppendPressure(const PressureSample& sample) {
  std::lock_guard<std::mutex> lock(runtime_pressure_mutex);
  PressureSample effective_sample = sample;
  if (!enforce_disk_pressure) {
    effective_sample.free_disk_bytes = UINT64_MAX;
    effective_sample.free_disk_percent = 100;
  }
  const auto now = std::chrono::steady_clock::now();
  uint64_t sample_interval_us = 0;
  if (!pressure_clock_initialized) {
    pressure_last_observed_at = now;
    pressure_clock_initialized = true;
  } else {
    const uint64_t elapsed = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            now - pressure_last_observed_at)
            .count());
    sample_interval_us = elapsed;
    switch (append_commit_pressure_controller.state()) {
      case PressureState::kNormal:
        runtime_pressure_normal_us.fetch_add(elapsed, std::memory_order_relaxed);
        break;
      case PressureState::kSoft:
        runtime_pressure_soft_us.fetch_add(elapsed, std::memory_order_relaxed);
        break;
      case PressureState::kHard:
        runtime_pressure_hard_us.fetch_add(elapsed, std::memory_order_relaxed);
        break;
    }
  }
  effective_sample.sample_interval_us = sample_interval_us;
  append_commit_pressure_controller.Observe(effective_sample);
  runtime_pressure_state.store(append_commit_pressure_controller.state(),
                               std::memory_order_release);
  runtime_target_count.store(append_commit_pressure_controller.target_count(),
                             std::memory_order_release);
  runtime_target_bytes.store(append_commit_pressure_controller.target_bytes(),
                             std::memory_order_release);
  runtime_collection_window_us.store(
      append_commit_pressure_controller.collection_window_us(),
      std::memory_order_release);
  runtime_admission_permitted.store(
      append_commit_pressure_controller.DecideAdmission(0, 0, 1).admit,
      std::memory_order_release);
  pressure_last_observed_at = now;
}

void Database::Impl::AccountPressureTime() {
  std::lock_guard<std::mutex> lock(runtime_pressure_mutex);
  if (!pressure_clock_initialized) return;
  const auto now = std::chrono::steady_clock::now();
  const uint64_t elapsed = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          now - pressure_last_observed_at)
          .count());
  switch (append_commit_pressure_controller.state()) {
    case PressureState::kNormal:
      runtime_pressure_normal_us.fetch_add(elapsed, std::memory_order_relaxed);
      break;
    case PressureState::kSoft:
      runtime_pressure_soft_us.fetch_add(elapsed, std::memory_order_relaxed);
      break;
    case PressureState::kHard:
      runtime_pressure_hard_us.fetch_add(elapsed, std::memory_order_relaxed);
      break;
  }
  pressure_last_observed_at = now;
}

CedarRuntimeSnapshot Database::Impl::ReadRuntimeSnapshot() const {
  const auto snapshot =
      std::atomic_load_explicit(&runtime_snapshot, std::memory_order_acquire);
  return snapshot == nullptr ? CedarRuntimeSnapshot{} : *snapshot;
}

bool Database::Impl::RuntimeSnapshotIsFresh() const {
  const CedarRuntimeSnapshot snapshot = ReadRuntimeSnapshot();
  if (snapshot.sampled_at_us == 0) return false;
  const uint64_t now = SteadyNowMicros();
  return now >= snapshot.sampled_at_us &&
         now - snapshot.sampled_at_us <= kRuntimeSnapshotStaleUs;
}

Status Database::Impl::RefreshRuntimeSnapshot() {
  std::lock_guard<std::mutex> refresh_lock(runtime_refresh_mutex);
  const auto refresh_started_at = std::chrono::steady_clock::now();
  const auto sampled_runtime = store.SampleRuntime();
  if (!sampled_runtime.ok()) return sampled_runtime.status();
  RuntimeSamplingTiming timing = sampled_runtime.ValueOrDie().timing;
  CedarRuntimeSnapshot snapshot;
  snapshot.pressure = sampled_runtime.ValueOrDie().pressure;
  if (runtime_pressure_override_for_testing) {
    runtime_pressure_override_for_testing(&snapshot.pressure);
  }
  snapshot.rocksdb = sampled_runtime.ValueOrDie().metrics;
  snapshot.generation = snapshot.rocksdb.maintenance_generation;
  snapshot.rocksdb.free_disk_bytes = snapshot.pressure.free_disk_bytes;
  snapshot.rocksdb.free_disk_percent = snapshot.pressure.free_disk_percent;
  if (runtime_snapshot_before_publish_observer_for_testing) {
    runtime_snapshot_before_publish_observer_for_testing();
  }
  const auto snapshot_publish_started_at = std::chrono::steady_clock::now();
  snapshot.pressure.queue_depth =
      runtime_queued_request_count.load(std::memory_order_acquire);
  ObserveAppendPressure(snapshot.pressure);
  snapshot.pressure_state = runtime_pressure_state.load(std::memory_order_acquire);
  snapshot.sampled_at_us = SteadyNowMicros();
  std::atomic_store_explicit(
      &runtime_snapshot, std::make_shared<const CedarRuntimeSnapshot>(snapshot),
      std::memory_order_release);
  if (maintenance_controller) maintenance_controller->PublishSnapshot(snapshot);
  if (runtime_snapshot_published_observer_for_testing) {
    runtime_snapshot_published_observer_for_testing();
  }
  timing.snapshot_publish_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - snapshot_publish_started_at)
          .count());
  timing.refresh_total_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - refresh_started_at)
          .count());
  if (runtime_sampling_timing_observer_for_testing) {
    runtime_sampling_timing_observer_for_testing(timing);
  }
  return Status::OK();
}

Status Database::Impl::StartRuntimeSampler() {
  const Status initial = RefreshRuntimeSnapshot();
  if (!initial.ok()) return initial;
  {
    std::lock_guard<std::mutex> lock(runtime_sampler_mutex);
    runtime_sampler_stopping = false;
  }
  runtime_sampler_worker = std::thread([this] {
    ApplyCedarSamplerScheduling();
    if (runtime_sampler_thread_started_observer_for_testing) {
      runtime_sampler_thread_started_observer_for_testing();
    }
    for (;;) {
      uint64_t interval_ms = 50;
      switch (runtime_pressure_state.load(std::memory_order_acquire)) {
        case PressureState::kNormal:
          interval_ms = 50;
          break;
        case PressureState::kSoft:
          interval_ms = 10;
          break;
        case PressureState::kHard:
          interval_ms = 5;
          break;
      }
      if (runtime_sampler_interval_observer_for_testing) {
        runtime_sampler_interval_observer_for_testing(interval_ms);
      }
      std::unique_lock<std::mutex> lock(runtime_sampler_mutex);
      if (runtime_sampler_cv.wait_for(
              lock, std::chrono::milliseconds(interval_ms), [this] {
                return runtime_sampler_stopping;
              })) {
        return;
      }
      lock.unlock();
      RefreshRuntimeSnapshot().IgnoreError();
    }
  });
  return Status::OK();
}

void Database::Impl::StopRuntimeSampler() {
  {
    std::lock_guard<std::mutex> lock(runtime_sampler_mutex);
    runtime_sampler_stopping = true;
  }
  runtime_sampler_cv.notify_all();
  if (runtime_sampler_worker.joinable()) runtime_sampler_worker.join();
  ObserveShutdownStage("sampler_join");
}

Database::Impl::~Impl() { StopAppendCommitPipeline(); }

void Database::Impl::NotifyWalDurable(void* context) noexcept {
  auto* durability = static_cast<WalDurabilityContext*>(context);
  if (durability == nullptr) return;
  const auto callback_started_at = std::chrono::steady_clock::now();
  if (durability->write_started_at.time_since_epoch().count() != 0) {
    const uint64_t elapsed = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - durability->write_started_at)
            .count());
    durability->wal_callback_us.store(elapsed, std::memory_order_relaxed);
  }
  for (const auto& handle : durability->handles) {
    std::lock_guard<std::mutex> lock(handle->mutex);
    handle->wal_durable = true;
    handle->completed.notify_all();
  }
  if (durability->executor != nullptr) {
    for (const auto& ticket : durability->executor_tickets) {
      if (ticket != nullptr) durability->executor->Release(ticket->id);
    }
  }
  durability->wal_callback_duration_us.store(
      static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - callback_started_at)
              .count()),
      std::memory_order_relaxed);
}

Status Database::Impl::StartAppendCommitPipeline() {
  const Status executor_started = async_executor.Start();
  if (!executor_started.ok()) return executor_started;
  const Status sampler_started = StartRuntimeSampler();
  if (!sampler_started.ok()) return sampler_started;
  maintenance_adapter = std::make_unique<FactStoreMaintenanceAdapter>(
      &store, &wal_sync_critical);
  maintenance_controller = std::make_unique<MaintenanceController>(
      maintenance_adapter.get());
  const Status maintenance_started = maintenance_controller->Start();
  if (!maintenance_started.ok()) return maintenance_started;
  maintenance_controller->PublishSnapshot(ReadRuntimeSnapshot());
  append_preflight_worker = std::thread([this] {
    for (;;) {
      std::shared_ptr<const internal::PendingVersionOverlay> overlay;
      std::vector<std::shared_ptr<AppendCommitRequest>> candidates;
      uint64_t epoch_generation = 0;
      uint64_t request_generation = 0;
      uint32_t maximum = 0;
      uint64_t target_bytes = 0;
      CommitSeq epoch_base;
      uint64_t epoch_committed_count = 0;
      {
        std::unique_lock<std::mutex> lock(append_commit_mutex);
        const bool requested = append_commit_cv.wait_for(
            lock, std::chrono::milliseconds(5), [this] {
              return append_commit_stopping ||
                 (active_pending_version_overlay != nullptr &&
                  (!next_epoch_slot.has_value() ||
                   next_epoch_slot->state != SlotState::kEligible) &&
                  preflight_completed_generation != preflight_request_generation);
        });
        if (append_commit_stopping) return;
        if (!requested) continue;
        if (active_pending_version_overlay == nullptr) {
          continue;
        }
        overlay = active_pending_version_overlay;
        epoch_generation = active_epoch_generation;
        request_generation = preflight_request_generation;
        epoch_base = active_epoch_base_visible_seq;
        epoch_committed_count = active_epoch_committed_count_hint;
        candidates.assign(append_commit_requests.begin(),
                          append_commit_requests.end());
        maximum = std::min<uint32_t>(append_commit_max_batch_size,
                                     runtime_target_count.load(std::memory_order_acquire));
        target_bytes = std::min<uint64_t>(
            append_commit_max_batch_bytes,
            runtime_target_bytes.load(std::memory_order_acquire));
      }

      if (epoch_committed_count == 0) {
        std::lock_guard<std::mutex> lock(append_commit_mutex);
        if (active_pending_version_overlay == overlay &&
            active_epoch_generation == epoch_generation) {
          active_n_plus_one_preflight_transactions = 0;
          active_n_plus_one_decided_transactions = 0;
          preflight_completed_generation = request_generation;
        }
        continue;
      }

      internal::CommitConflictIndex planned_index;
      uint64_t planned_bytes = 0;
      uint64_t planned_transactions = 0;
      std::vector<StoreCommitGroupRequest> planned_requests;
      std::vector<std::shared_ptr<AppendCommitRequest>> planned_candidates;
      planned_requests.reserve(maximum);
      planned_candidates.reserve(maximum);
      for (const auto& candidate : candidates) {
        if (planned_transactions == maximum ||
            candidate->estimated_bytes > target_bytes -
                std::min<uint64_t>(planned_bytes, target_bytes)) {
          break;
        }
        const internal::CommitFootprint& footprint =
            candidate->preflight_footprint;
        if (!CanDecideIndependentAppend(candidate->batch, epoch_base)) break;
        if (overlay->Conflicts(footprint)) break;
        if (!planned_index.Insert(footprint)) break;
        planned_bytes += candidate->estimated_bytes;
        ++planned_transactions;
        planned_requests.push_back(StoreCommitGroupRequest{
            candidate->batch, candidate->handle != nullptr});
        planned_candidates.push_back(candidate);
      }
      std::unique_ptr<internal::DecidedEpoch> predecided;
      const auto preflight_started_at = std::chrono::steady_clock::now();
      if (!planned_requests.empty() &&
          epoch_committed_count <=
              std::numeric_limits<uint64_t>::max() - epoch_base.value) {
        auto decided = store.DecideIndependentAppendGroup(
            CommitSeq{epoch_base.value + epoch_committed_count}, epoch_base,
            planned_requests);
        if (decided.ok()) predecided = decided.ConsumeValueOrDie();
      }

      std::lock_guard<std::mutex> lock(append_commit_mutex);
        if (active_pending_version_overlay == overlay &&
          active_epoch_generation == epoch_generation &&
          (!next_epoch_slot.has_value() ||
           next_epoch_slot->state != SlotState::kEligible)) {
          active_n_plus_one_preflight_transactions = planned_transactions;
          if (accounted_n_plus_one_preflight_generation != epoch_generation) {
            ++append_commit_metrics.n_plus_one_preflight_epochs;
            accounted_n_plus_one_preflight_generation = epoch_generation;
          }
          append_commit_metrics.n_plus_one_hidden_cpu_us += static_cast<uint64_t>(
              std::chrono::duration_cast<std::chrono::microseconds>(
                  std::chrono::steady_clock::now() - preflight_started_at)
                  .count());
          preflight_completed_generation = request_generation;
          if (predecided != nullptr && !planned_candidates.empty()) {
            active_n_plus_one_decided_transactions = planned_candidates.size();
            if (accounted_n_plus_one_decided_generation != epoch_generation) {
              ++append_commit_metrics.n_plus_one_decided_epochs;
              accounted_n_plus_one_decided_generation = epoch_generation;
            }
          DecidedEpochSlot slot;
          slot.generation = epoch_generation;
          slot.preparation_generation = request_generation;
          slot.predecessor_base_visible_seq = epoch_base;
          slot.successor_base_visible_seq = predecided->base_visible_seq();
          slot.preflight_transactions = planned_transactions;
          slot.requests = std::move(planned_candidates);
          slot.epoch = std::move(predecided);
          slot.state = SlotState::kEligible;
          append_commit_metrics.n_plus_one_eligible += slot.requests.size();
          ++append_commit_metrics.n_plus_one_eligible_epochs;
          next_epoch_slot = std::move(slot);
        }
      }
    }
  });
  append_commit_worker = std::thread([this] {
    for (;;) {
      std::vector<std::shared_ptr<AppendCommitRequest>> requests;
      std::unique_ptr<internal::DecidedEpoch> predecided_epoch;
      const auto collection_started_at = std::chrono::steady_clock::now();
      {
        std::unique_lock<std::mutex> lock(append_commit_mutex);
        append_commit_cv.wait(lock, [this] {
          return append_commit_stopping || !append_commit_requests.empty();
        });
        if (append_commit_stopping && append_commit_requests.empty()) {
          if (next_epoch_slot.has_value() &&
              next_epoch_slot->state == SlotState::kEligible) {
            next_epoch_slot->state = SlotState::kDiscarded;
            RecordNPlusOneDiscard(
                &append_commit_metrics, NPlusOneDiscardReason::kShutdown,
                next_epoch_slot->requests.size());
            next_epoch_slot.reset();
          }
          return;
        }
        bool can_promote_slot = false;
        if (next_epoch_slot.has_value() &&
            next_epoch_slot->state == SlotState::kEligible) {
          const size_t planned_size = next_epoch_slot->requests.size();
          const uint64_t preparation_generation =
              next_epoch_slot->preparation_generation;
          if (append_commit_requests.size() < planned_size &&
              !append_commit_stopping) {
            append_commit_cv.wait(
                lock, [this, planned_size, preparation_generation] {
              return append_commit_stopping ||
                     !next_epoch_slot.has_value() ||
                     next_epoch_slot->state != SlotState::kEligible ||
                     next_epoch_slot->preparation_generation !=
                         preparation_generation ||
                     append_commit_requests.size() >= planned_size;
            });
          }
          const bool same_slot =
              next_epoch_slot.has_value() &&
              next_epoch_slot->state == SlotState::kEligible &&
              next_epoch_slot->preparation_generation ==
                  preparation_generation;
          if (same_slot && append_commit_requests.size() >= planned_size) {
            can_promote_slot = std::equal(
                next_epoch_slot->requests.begin(),
                next_epoch_slot->requests.end(), append_commit_requests.begin(),
                [](const auto& planned, const auto& queued) {
                  return planned == queued;
                });
            if (!can_promote_slot) {
              next_epoch_slot->state = SlotState::kDiscarded;
              RecordNPlusOneDiscard(
                  &append_commit_metrics,
                  NPlusOneDiscardReason::kGenerationMismatch, planned_size);
              next_epoch_slot.reset();
            }
          } else if (same_slot && append_commit_stopping) {
            next_epoch_slot->state = SlotState::kDiscarded;
            RecordNPlusOneDiscard(&append_commit_metrics,
                                  NPlusOneDiscardReason::kShutdown,
                                  planned_size);
            next_epoch_slot.reset();
          }
        }
        if (can_promote_slot) {
          const size_t planned_size = next_epoch_slot->requests.size();
          requests.reserve(planned_size);
          for (size_t index = 0; index < planned_size; ++index) {
            requests.push_back(std::move(append_commit_requests.front()));
            append_commit_requests.pop_front();
            runtime_queued_request_count.fetch_sub(1, std::memory_order_release);
            queued_append_commit_bytes -= requests.back()->estimated_bytes;
          }
          predecided_epoch = std::move(next_epoch_slot->epoch);
          next_epoch_slot->state = SlotState::kPromoted;
          append_commit_metrics.n_plus_one_promoted += planned_size;
          ++append_commit_metrics.n_plus_one_promoted_epochs;
          next_epoch_slot.reset();
        } else {
          const auto deadline = std::chrono::steady_clock::now() +
              std::chrono::microseconds(std::min(
                  append_commit_window_us,
                  runtime_collection_window_us.load(std::memory_order_acquire)));
          append_commit_cv.wait_until(lock, deadline, [this] {
            return append_commit_stopping ||
                   append_commit_requests.size() >= std::min<uint32_t>(
                       append_commit_max_batch_size,
                       runtime_target_count.load(std::memory_order_acquire));
          });
          internal::CommitConflictIndex conflict_index;
          const size_t maximum = std::min<size_t>(
              append_commit_requests.size(), std::min<uint32_t>(
                  append_commit_max_batch_size,
                  runtime_target_count.load(std::memory_order_acquire)));
          const uint64_t target_bytes = std::min<uint64_t>(
              append_commit_max_batch_bytes,
              runtime_target_bytes.load(std::memory_order_acquire));
          uint64_t batch_bytes = 0;
          requests.reserve(maximum);
          for (size_t index = 0; index < maximum; ++index) {
            const auto& candidate = append_commit_requests.front();
            if (!requests.empty() &&
                (batch_bytes > target_bytes ||
                 candidate->estimated_bytes > target_bytes - batch_bytes)) {
              break;
            }
            const internal::CommitFootprint& footprint =
                candidate->preflight_footprint;
            if (!conflict_index.Insert(footprint)) break;
            batch_bytes += candidate->estimated_bytes;
            requests.push_back(std::move(append_commit_requests.front()));
            append_commit_requests.pop_front();
            runtime_queued_request_count.fetch_sub(1, std::memory_order_release);
            queued_append_commit_bytes -= requests.back()->estimated_bytes;
          }
        }
        active_append_commit_requests += requests.size();
        if (!requests.empty()) {
          const auto selected_at = std::chrono::steady_clock::now();
          RecordLatency(&append_commit_metrics.latency.collection,
              static_cast<uint64_t>(
                  std::chrono::duration_cast<std::chrono::microseconds>(
                      selected_at - collection_started_at)
                      .count()));
          for (const auto& request : requests) {
            RecordLatency(&append_commit_metrics.latency.queue,
                static_cast<uint64_t>(
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        selected_at - request->enqueued_at)
                        .count()));
          }
        }
      }

      std::vector<const StoreCommitBatch*> selected_batches;
      selected_batches.reserve(requests.size());
      for (const auto& request : requests) {
        selected_batches.push_back(&request->batch);
      }
      const auto overlay = std::make_shared<const internal::PendingVersionOverlay>(
          internal::PendingVersionOverlay::FromBatches(selected_batches));
      {
        std::lock_guard<std::mutex> lock(append_commit_mutex);
        active_pending_version_overlay = overlay;
        ++active_epoch_generation;
        active_n_plus_one_preflight_transactions = 0;
        active_n_plus_one_decided_transactions = 0;
        active_epoch_base_visible_seq = store.visible_seq();
        active_epoch_committed_count_hint =
            std::all_of(requests.begin(), requests.end(),
                        [this](const auto& request) {
                          return CanDecideIndependentAppend(
                              request->batch, active_epoch_base_visible_seq);
                        })
                ? requests.size()
                : 0;
        ++preflight_request_generation;
        append_commit_cv.notify_all();
      }

      std::vector<StoreCommitGroupRequest> store_requests;
      WalDurabilityContext durability;
      durability.executor = &async_executor;
      size_t fast_path_requests = 0;
      store_requests.reserve(requests.size());
      durability.handles.reserve(requests.size());
      durability.executor_tickets.reserve(requests.size());
      std::unordered_set<uint64_t> durable_txn_ids;
      size_t durably_accepted_transactions = 0;
      for (const auto& request : requests) {
        if (internal::CanUseAppendFastPath(request->batch)) {
          ++fast_path_requests;
        }
        const bool first_durable_transaction =
            durable_txn_ids.emplace(request->txn_id.value).second;
        if (first_durable_transaction) {
          ++durably_accepted_transactions;
        }
        store_requests.push_back(
            StoreCommitGroupRequest{std::move(request->batch),
                                    request->handle != nullptr &&
                                        first_durable_transaction});
        if (request->handle != nullptr && first_durable_transaction) {
          durability.handles.push_back(request->handle);
          durability.executor_tickets.push_back(request->executor_ticket);
        }
      }
      durability.write_started_at = std::chrono::steady_clock::now();
      if (maintenance_controller) {
        maintenance_controller->SetWalSyncCritical(true);
      }
      bool discarded_stale_predecision = false;
      StatusOr<StoreCommittedGroupResult> result;
      if (predecided_epoch != nullptr) {
        result = store.WriteDecidedGroup(predecided_epoch.get(), NotifyWalDurable,
                                         &durability, &wal_sync_critical);
        if (!result.ok() && result.status().IsConflict()) {
          discarded_stale_predecision = true;
          result = store.CommitGroupWithWalCallback(
              store_requests, NotifyWalDurable, &durability,
              &wal_sync_critical);
        }
      } else {
        result = store.CommitGroupWithWalCallback(store_requests, NotifyWalDurable,
                                                  &durability,
                                                  &wal_sync_critical);
      }
      wal_sync_critical.store(false, std::memory_order_release);
      if (maintenance_controller) {
        maintenance_controller->SetWalSyncCritical(false);
      }
      const auto write_finished_at = std::chrono::steady_clock::now();
      const CedarRuntimeSnapshot runtime_snapshot = ReadRuntimeSnapshot();
      {
        std::lock_guard<std::mutex> lock(append_commit_mutex);
        if (commit_result_processing_observer_for_testing) {
          commit_result_processing_observer_for_testing();
        }
        append_commit_metrics.epochs += 1;
        append_commit_metrics.epoch_transactions += requests.size();
        for (const auto& request : requests) {
          append_commit_metrics.epoch_bytes += request->estimated_bytes;
        }
        append_commit_metrics.append_fast_path += fast_path_requests;
        append_commit_metrics.general_path += requests.size() - fast_path_requests;
        append_commit_metrics.pending_overlay_peak = std::max<uint64_t>(
            append_commit_metrics.pending_overlay_peak, overlay->size());
        append_commit_metrics.pressure_state =
            runtime_pressure_state.load(std::memory_order_acquire);
        if (result.ok()) {
          const auto& group_metrics = result.ValueOrDie();
          append_commit_metrics.wal_rotations += group_metrics.wal_rotations;
          RecordLatency(&append_commit_metrics.latency.validation,
                        group_metrics.validation_us);
          RecordLatency(&append_commit_metrics.latency.assembly,
                        group_metrics.assembly_us);
          if (group_metrics.has_kernel_stage_metrics) {
            RecordLatency(&append_commit_metrics.latency.wal_append,
                          group_metrics.wal_append_us);
            RecordLatency(&append_commit_metrics.latency.wal_sync,
                          group_metrics.wal_sync_us);
            RecordLatency(&append_commit_metrics.latency.manifest,
                          group_metrics.manifest_us);
            RecordLatency(&append_commit_metrics.latency.memtable_insert,
                          group_metrics.memtable_insert_us);
          }
        }
        if (discarded_stale_predecision) {
          RecordNPlusOneDiscard(&append_commit_metrics,
                                NPlusOneDiscardReason::kBaseMismatch,
                                requests.size());
        }
        if (predecided_epoch == nullptr && next_epoch_slot.has_value() &&
            next_epoch_slot->state == SlotState::kEligible) {
          const size_t planned_size = next_epoch_slot->requests.size();
          if (!result.ok()) {
            next_epoch_slot->state = SlotState::kDiscarded;
            RecordNPlusOneDiscard(&append_commit_metrics,
                                  DiscardReasonForStatus(result.status()),
                                  planned_size);
            next_epoch_slot.reset();
          } else {
            const bool has_expected_successor =
                next_epoch_slot->generation == active_epoch_generation &&
                next_epoch_slot->predecessor_base_visible_seq ==
                    active_epoch_base_visible_seq &&
                next_epoch_slot->successor_base_visible_seq ==
                    store.visible_seq();
            if (!has_expected_successor) {
              next_epoch_slot->state = SlotState::kDiscarded;
              const NPlusOneDiscardReason reason =
                  next_epoch_slot->generation != active_epoch_generation
                      ? NPlusOneDiscardReason::kGenerationMismatch
                      : NPlusOneDiscardReason::kBaseMismatch;
              RecordNPlusOneDiscard(&append_commit_metrics, reason,
                                    planned_size);
              next_epoch_slot.reset();
            }
          }
        }
        RecordLatency(&append_commit_metrics.latency.db_write,
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(
                    write_finished_at - durability.write_started_at)
                    .count()));
        RecordLatency(&append_commit_metrics.latency.wal_callback,
                      durability.wal_callback_duration_us.load(
                          std::memory_order_relaxed));
        append_commit_metrics.runtime = ToRuntimeMetrics(runtime_snapshot.rocksdb);
        if (result.ok()) {
          append_commit_metrics.durably_accepted += durably_accepted_transactions;
        }
        for (size_t index = 0; index < requests.size(); ++index) {
          if (result.ok()) {
            requests[index]->result.emplace(
                std::move(result.ValueOrDie().results[index]));
            if (requests[index]->result->ok()) {
              ++append_commit_metrics.published;
            } else {
              ++append_commit_metrics.aborted;
              if (requests[index]->result->status().IsIndeterminate() ||
                  requests[index]->result->status().IsRecoveryRequired()) {
                ++append_commit_metrics.indeterminate;
              }
            }
          } else {
            requests[index]->result.emplace(result.status());
            ++append_commit_metrics.aborted;
            if (result.status().IsIndeterminate() ||
                result.status().IsRecoveryRequired()) {
              ++append_commit_metrics.indeterminate;
            }
          }
        }
        const auto publication_observed_at = std::chrono::steady_clock::now();
        active_append_commit_requests -= requests.size();
        append_commit_metrics.n_plus_one_preflight_transactions +=
            active_n_plus_one_preflight_transactions;
        append_commit_metrics.n_plus_one_decided_transactions +=
            active_n_plus_one_decided_transactions;
        if (result.ok()) {
          RecordLatency(&append_commit_metrics.latency.publication,
                        result.ValueOrDie().publication_us);
        }
        for (const auto& request : requests) {
          RecordLatency(&append_commit_metrics.latency.end_to_end,
              static_cast<uint64_t>(
                  std::chrono::duration_cast<std::chrono::microseconds>(
                      publication_observed_at - request->enqueued_at)
                      .count()));
        }
        active_pending_version_overlay.reset();
        ++preflight_request_generation;
        append_commit_cv.notify_all();
      }
      for (size_t index = 0; index < requests.size(); ++index) {
        if (requests[index]->handle) {
          CompleteCommitHandle(requests[index]->handle,
                               ToCommitResult(requests[index]->txn_id,
                                              *requests[index]->result));
        }
      }
      store_requests.clear();
      durability.handles.clear();
      uint64_t released_bytes = 0;
      for (const auto& request : requests) released_bytes += request->estimated_bytes;
      requests.clear();
      {
        std::lock_guard<std::mutex> lock(append_commit_mutex);
        reserved_append_commit_bytes -= released_bytes;
        append_commit_cv.notify_all();
      }
    }
  });
  return Status::OK();
}

StatusOr<StoreCommitResult> Database::Impl::SubmitSyncCommit(
    const StoreCommitBatch& batch, uint64_t deadline_us) {
  auto request = std::make_shared<AppendCommitRequest>();
  request->batch = batch;
  request->txn_id = batch.txn_id;
  request->estimated_bytes = internal::EstimateCommitBatchBytes(batch);
  request->preflight_footprint = internal::BuildCommitFootprint(batch);
  request->enqueued_at = std::chrono::steady_clock::now();
  if (request->estimated_bytes > append_commit_max_batch_bytes) {
    std::lock_guard<std::mutex> lock(append_commit_mutex);
    ++append_commit_metrics.rejected;
    return Status::ResourceExhausted("commit", "encoded batch exceeds hard byte limit");
  }
  {
    ForegroundAdmissionSlot admission(this);
    if (foreground_admission_observer_for_testing) {
      foreground_admission_observer_for_testing();
    }
    std::unique_lock<std::mutex> lock(append_commit_mutex);
    if (append_commit_stopping) {
      ++append_commit_metrics.rejected;
      return Status::ShutdownInProgress("commit", "append pipeline is stopping");
    }
    if (!RuntimeSnapshotIsFresh()) {
      ++append_commit_metrics.rejected;
      return Status::ResourceExhausted(
          "commit", "runtime pressure snapshot is stale");
    }
    if (!runtime_admission_permitted.load(std::memory_order_acquire)) {
      ++append_commit_metrics.rejected;
      return Status::ResourceExhausted("commit", "append admission is under pressure");
    }
    const auto has_capacity = [this, &request] {
      return append_commit_requests.size() < append_commit_max_queue_requests &&
             request->estimated_bytes <= append_commit_max_queue_bytes -
                 std::min<uint64_t>(reserved_append_commit_bytes,
                                    append_commit_max_queue_bytes) &&
             request->estimated_bytes <= append_commit_max_queue_bytes -
                 std::min<uint64_t>(queued_append_commit_bytes,
                                    append_commit_max_queue_bytes);
    };
    if (!has_capacity()) {
      if (deadline_us == 0) {
        ++append_commit_metrics.rejected;
        return Status::ResourceExhausted("commit", "append queue is full");
      }
      const auto deadline = std::chrono::steady_clock::now() +
                            std::chrono::microseconds(deadline_us);
      const auto started_wait = std::chrono::steady_clock::now();
      while (!has_capacity()) {
        if (append_commit_cv.wait_until(lock, deadline) ==
            std::cv_status::timeout) {
          ++append_commit_metrics.rejected;
          return Status::ResourceExhausted(
              "commit", "append admission deadline expired");
        }
        if (append_commit_stopping) {
          ++append_commit_metrics.rejected;
          return Status::ShutdownInProgress("commit",
                                            "append pipeline is stopping");
        }
      }
      append_commit_metrics.admission_wait_us += static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - started_wait)
              .count());
    }
    append_commit_requests.push_back(request);
    runtime_queued_request_count.fetch_add(1, std::memory_order_release);
    queued_append_commit_bytes += request->estimated_bytes;
    reserved_append_commit_bytes += request->estimated_bytes;
    if (active_pending_version_overlay != nullptr) {
      ++preflight_request_generation;
    }
    ++append_commit_metrics.submitted;
    if (append_commit_enqueued_observer_for_testing) {
      append_commit_enqueued_observer_for_testing();
    }
    append_commit_cv.notify_all();
  }
  std::unique_lock<std::mutex> lock(append_commit_mutex);
  append_commit_cv.wait(lock, [&] { return request->result.has_value(); });
  return std::move(*request->result);
}

Status Database::Impl::SubmitAsyncCommitToAppendPipeline(
    const std::shared_ptr<AppendCommitRequest>& request) {
  std::lock_guard<std::mutex> lock(append_commit_mutex);
  if (request->cancelled.load(std::memory_order_acquire)) {
    return Status::ResourceExhausted(
        "async commit", "submission was cancelled before append admission");
  }
  if (append_commit_stopping) {
    ++append_commit_metrics.rejected;
    return Status::ShutdownInProgress("async commit", "append pipeline is stopping");
  }
  if (!RuntimeSnapshotIsFresh()) {
    ++append_commit_metrics.rejected;
    ++append_commit_metrics.async_runtime_snapshot_stale_rejected;
    return Status::ResourceExhausted(
        "async commit", "runtime pressure snapshot is stale");
  }
  if (!runtime_admission_permitted.load(std::memory_order_acquire)) {
    ++append_commit_metrics.rejected;
    ++append_commit_metrics.async_runtime_pressure_rejected;
    return Status::ResourceExhausted("async commit",
                                     "append admission is under pressure");
  }
  if (append_commit_requests.size() >= append_commit_max_queue_requests ||
      request->estimated_bytes > append_commit_max_queue_bytes -
          std::min<uint64_t>(queued_append_commit_bytes,
                             append_commit_max_queue_bytes)) {
    ++append_commit_metrics.rejected;
    return Status::ResourceExhausted("async commit", "append queue is full");
  }
  if (request->estimated_bytes > append_commit_max_queue_bytes -
          std::min<uint64_t>(reserved_append_commit_bytes,
                             append_commit_max_queue_bytes)) {
    ++append_commit_metrics.rejected;
    return Status::ResourceExhausted("async commit", "append memory budget is full");
  }
  append_commit_requests.push_back(request);
  runtime_queued_request_count.fetch_add(1, std::memory_order_release);
  queued_append_commit_bytes += request->estimated_bytes;
  reserved_append_commit_bytes += request->estimated_bytes;
  if (active_pending_version_overlay != nullptr) {
    ++preflight_request_generation;
  }
  ++append_commit_metrics.submitted;
  if (append_commit_enqueued_observer_for_testing) {
    append_commit_enqueued_observer_for_testing();
  }
  append_commit_cv.notify_all();
  return Status::OK();
}

void Database::Impl::CancelQueuedAsyncCommit(
    const std::shared_ptr<AppendCommitRequest>& request) {
  if (request == nullptr ||
      request->cancelled.exchange(true, std::memory_order_acq_rel)) {
    return;
  }

  bool removed = false;
  {
    std::lock_guard<std::mutex> lock(append_commit_mutex);
    const auto queued = std::find(append_commit_requests.begin(),
                                  append_commit_requests.end(), request);
    if (queued != append_commit_requests.end()) {
      append_commit_requests.erase(queued);
      runtime_queued_request_count.fetch_sub(1, std::memory_order_release);
      queued_append_commit_bytes -= request->estimated_bytes;
      reserved_append_commit_bytes -= request->estimated_bytes;
      ++append_commit_metrics.rejected;
      removed = true;
    }
    if (next_epoch_slot.has_value() &&
        next_epoch_slot->state == SlotState::kEligible) {
      const auto planned = std::find(next_epoch_slot->requests.begin(),
                                     next_epoch_slot->requests.end(), request);
      if (planned != next_epoch_slot->requests.end()) {
        next_epoch_slot->state = SlotState::kDiscarded;
        RecordNPlusOneDiscard(&append_commit_metrics,
                              NPlusOneDiscardReason::kCancelled,
                              next_epoch_slot->requests.size());
        next_epoch_slot.reset();
      }
    }
    if (removed) append_commit_cv.notify_all();
  }

  if (removed) {
    CompleteCommitHandle(
        request->handle,
        CommitResult{CommitOutcome::kAborted, CommitSeq{}, request->txn_id,
                     Status::ResourceExhausted(
                         "async commit", "submission was cancelled")});
    if (request->executor_ticket != nullptr) {
      async_executor.Release(request->executor_ticket->id);
    }
  } else if (request->executor_ticket != nullptr) {
    // The ticket may still be in the bounded submission mailbox. WorkerMain
    // observes this bit and releases it without handing the request off.
    async_executor.Cancel(request->executor_ticket->id);
  }
}

Status Database::Impl::SubmitAsyncCommit(
    const StoreCommitBatch& batch, std::shared_ptr<CommitHandle::State> handle,
    uint64_t deadline_us) {
  auto request = std::make_shared<AppendCommitRequest>();
  request->batch = batch;
  request->txn_id = batch.txn_id;
  request->estimated_bytes = internal::EstimateCommitBatchBytes(batch);
  request->preflight_footprint = internal::BuildCommitFootprint(batch);
  request->enqueued_at = std::chrono::steady_clock::now();
  if (request->estimated_bytes > append_commit_max_batch_bytes) {
    std::lock_guard<std::mutex> lock(append_commit_mutex);
    ++append_commit_metrics.rejected;
    return Status::ResourceExhausted("async commit", "encoded batch exceeds hard byte limit");
  }
  request->handle = std::move(handle);
  auto ticket = std::make_shared<AsyncSubmissionExecutor::Ticket>();
  ticket->estimated_bytes = request->estimated_bytes;
  ticket->handoff = [this, request] {
    return SubmitAsyncCommitToAppendPipeline(request);
  };
  ticket->fail = [request](const Status& status) {
    CompleteCommitHandle(
        request->handle,
        CommitResult{status.IsIndeterminate() || status.IsRecoveryRequired()
                         ? CommitOutcome::kIndeterminate
                         : CommitOutcome::kAborted,
                     CommitSeq{}, request->txn_id, status});
  };
  ticket->release = [] {};
  request->executor_ticket = ticket;
  const Status submitted = async_executor.TrySubmit(ticket);
  {
    std::lock_guard<std::mutex> lock(append_commit_mutex);
    append_commit_metrics.async_mailbox_requests_reserved =
        async_executor.ReservedRequests();
    append_commit_metrics.async_mailbox_bytes_reserved =
        async_executor.ReservedBytes();
    append_commit_metrics.async_mailbox_requests_reserved_peak = std::max(
        append_commit_metrics.async_mailbox_requests_reserved_peak,
        append_commit_metrics.async_mailbox_requests_reserved);
    append_commit_metrics.async_mailbox_bytes_reserved_peak = std::max(
        append_commit_metrics.async_mailbox_bytes_reserved_peak,
        append_commit_metrics.async_mailbox_bytes_reserved);
    if (!submitted.ok()) {
      ++append_commit_metrics.async_mailbox_rejected;
      ++append_commit_metrics.rejected;
      return submitted;
    }
    ++append_commit_metrics.async_mailbox_accepted;
  }

  std::unique_lock<std::mutex> lock(request->handle->mutex);
  const auto accepted = [&] {
    return request->handle->wal_durable || request->handle->result.has_value();
  };
  if (deadline_us == 0) {
    request->handle->completed.wait(lock, accepted);
  } else if (!request->handle->completed.wait_for(
                 lock, std::chrono::microseconds(deadline_us), accepted)) {
    lock.unlock();
    CancelQueuedAsyncCommit(request);
    lock.lock();
    // Cancellation is determinate only while the request is still in a Cedar
    // mailbox or queue. Once the writer has selected it, wait for the real WAL
    // or terminal outcome instead of reporting a failure that may later commit.
    request->handle->completed.wait(lock, accepted);
  }
  if (!request->handle->wal_durable) return request->handle->result->status;
  return Status::OK();
}

Status Database::Impl::DrainAppendCommitPipeline() {
  std::unique_lock<std::mutex> lock(append_commit_mutex);
  append_commit_cv.wait(lock, [this] {
    return append_commit_requests.empty() && active_append_commit_requests == 0;
  });
  return Status::OK();
}

void Database::Impl::StopAppendCommitPipeline() {
  async_executor.Stop(
      Status::ShutdownInProgress("async commit", "append pipeline is stopping"));
  {
    std::lock_guard<std::mutex> lock(append_commit_mutex);
    if (append_commit_stopping) return;
    if (next_epoch_slot.has_value() &&
        next_epoch_slot->state == SlotState::kEligible) {
      next_epoch_slot->state = SlotState::kDiscarded;
      RecordNPlusOneDiscard(&append_commit_metrics,
                            NPlusOneDiscardReason::kShutdown,
                            next_epoch_slot->requests.size());
      next_epoch_slot.reset();
    }
    append_commit_stopping = true;
    append_commit_cv.notify_all();
  }
  ObserveShutdownStage("queue_worker_stop");
  if (append_preflight_worker.joinable()) append_preflight_worker.join();
  ObserveShutdownStage("preparation_join");
  if (append_commit_worker.joinable()) append_commit_worker.join();
  ObserveShutdownStage("commit_join");
  if (maintenance_controller) maintenance_controller->Stop();
  ObserveShutdownStage("maintenance_join");
  StopRuntimeSampler();
}

Database::Database(std::shared_ptr<Impl> impl) : impl_(std::move(impl)) {}
Database::~Database() { Close().IgnoreError(); }

StatusOr<std::unique_ptr<Database>> Database::Open(DatabaseOptions options) {
  if (options.path.empty()) return Status::InvalidArgument("database", "missing path");
  if (options.group_commit_max_batch_size == 0 ||
      options.group_commit_max_batch_size > kMaximumGroupCommitBatchCount) {
    return Status::InvalidArgument("database", "group commit count is outside [1, 512]");
  }
  if (options.group_commit_max_batch_bytes == 0 ||
      options.group_commit_max_batch_bytes > kMaximumGroupCommitBatchBytes) {
    return Status::InvalidArgument("database", "group commit bytes exceed 2 MiB hard limit");
  }
  if (options.group_commit_max_queue_requests == 0 ||
      options.group_commit_max_queue_bytes == 0) {
    return Status::InvalidArgument("database", "append queue bounds must be nonzero");
  }
  if (options.async_executor.submission_workers == 0 ||
      options.async_executor.submission_workers > 2 ||
      options.async_executor.max_mailbox_requests == 0 ||
      options.async_executor.max_mailbox_bytes == 0 ||
      options.async_executor.max_mailbox_bytes < options.group_commit_max_batch_bytes) {
    return Status::InvalidArgument("database", "async executor bounds are invalid");
  }
  auto impl = std::make_shared<Impl>(std::move(options));
  const Status opened = impl->store.Open();
  if (!opened.ok()) return opened;
  const auto prepared = impl->store.ListPreparedCommits();
  if (!prepared.ok()) {
    impl->store.Close().IgnoreError();
    return prepared.status();
  }
  for (const StoreCommitBatch& batch : prepared.ValueOrDie()) {
    const auto finalized = impl->store.FinalizePreparedCommit(batch);
    if (!finalized.ok()) {
      impl->store.Close().IgnoreError();
      return finalized.status();
    }
  }
  const Status pipeline_started = impl->StartAppendCommitPipeline();
  if (!pipeline_started.ok()) {
    impl->store.Close().IgnoreError();
    return pipeline_started;
  }
  return std::unique_ptr<Database>(new Database(std::move(impl)));
}

Status Database::Close() {
  if (!impl_) return Status::OK();
  std::unique_lock<std::mutex> lock(impl_->mutex);
  if (impl_->closed) return Status::OK();
  if (impl_->closing) {
    return Status::ShutdownInProgress("database", "database close is in progress");
  }
  impl_->closing = true;
  lock.unlock();
  impl_->ObserveShutdownStage("queue_admission_closed");
  lock.lock();
  if (!impl_->stop_pipeline_before_drain_for_testing) {
    impl_->commits_drained.wait(lock, [this] {
      return impl_->active_commit_calls == 0;
    });
  }
  lock.unlock();
  if (impl_->stop_pipeline_before_drain_for_testing) {
    impl_->StopAppendCommitPipeline();
  }
  const Status drained = impl_->DrainAppendCommitPipeline();
  if (!drained.ok()) {
    lock.lock();
    impl_->closing = false;
    return drained;
  }
  impl_->ObserveShutdownStage("active_commit_resolution");
  impl_->StopAppendCommitPipeline();
  impl_->RefreshRuntimeSnapshot().IgnoreError();
  impl_->ObserveShutdownStage("final_runtime_snapshot");
  impl_->ObserveShutdownStage("rocksdb_close");
  lock.lock();
  const Status closed = impl_->store.Close();
  if (!closed.ok()) {
    impl_->closing = false;
    return closed;
  }
  impl_->closed = true;
  impl_->closing = false;
  return Status::OK();
}

StatusOr<VertexId> Database::AllocateVertexId() {
  if (!impl_) return Status::InvalidArgument("database", "moved-from database");
  std::lock_guard<std::mutex> lock(impl_->mutex);
  if (impl_->closed) return Status::InvalidArgument("database", "database is closed");
  if (impl_->closing) {
    return Status::ShutdownInProgress("database", "database close is in progress");
  }
  if (impl_->next_vertex_id == impl_->vertex_lease_limit) {
    const auto lease = impl_->store.LeaseIds(IdKind::kVertex, 0);
    if (!lease.ok()) return lease.status();
    impl_->next_vertex_id = lease.ValueOrDie().first_id;
    impl_->vertex_lease_limit =
        lease.ValueOrDie().first_id + lease.ValueOrDie().count;
  }
  return VertexId{impl_->next_vertex_id++};
}

StatusOr<EdgeId> Database::AllocateEdgeId() {
  if (!impl_) return Status::InvalidArgument("database", "moved-from database");
  std::lock_guard<std::mutex> lock(impl_->mutex);
  if (impl_->closed) return Status::InvalidArgument("database", "database is closed");
  if (impl_->closing) {
    return Status::ShutdownInProgress("database", "database close is in progress");
  }
  if (impl_->next_edge_id == impl_->edge_lease_limit) {
    const auto lease = impl_->store.LeaseIds(IdKind::kEdge, 0);
    if (!lease.ok()) return lease.status();
    impl_->next_edge_id = lease.ValueOrDie().first_id;
    impl_->edge_lease_limit = lease.ValueOrDie().first_id + lease.ValueOrDie().count;
  }
  return EdgeId{impl_->next_edge_id++};
}

StatusOr<PropertyDefinition> Database::RegisterProperty(
    PropertyDefinition definition) {
  if (!impl_) return Status::InvalidArgument("database", "moved-from database");
  std::lock_guard<std::mutex> lock(impl_->mutex);
  if (impl_->closed) return Status::InvalidArgument("database", "database is closed");
  if (impl_->closing) {
    return Status::ShutdownInProgress("database", "database close is in progress");
  }
  return impl_->store.RegisterProperty(std::move(definition));
}

Status Database::Vacuum(CommitSeq oldest_readable) {
  if (!impl_) return Status::InvalidArgument("database", "moved-from database");
  std::lock_guard<std::mutex> lock(impl_->mutex);
  if (impl_->closed) return Status::InvalidArgument("database", "database is closed");
  if (impl_->closing) {
    return Status::ShutdownInProgress("database", "database close is in progress");
  }
  return impl_->store.Vacuum(oldest_readable);
}

StatusOr<std::optional<CommitResult>> Database::ResolveTransaction(
    TxnId txn_id) const {
  if (!impl_) return Status::InvalidArgument("database", "moved-from database");
  std::lock_guard<std::mutex> lock(impl_->mutex);
  if (impl_->closed) return Status::InvalidArgument("database", "database is closed");
  if (impl_->closing) {
    return Status::ShutdownInProgress("database", "database close is in progress");
  }
  const auto resolved = impl_->store.ResolveTransaction(txn_id);
  if (!resolved.ok()) return resolved.status();
  if (resolved.ValueOrDie().has_value()) {
    const StoreCommitResult stored = *resolved.ValueOrDie();
    return std::optional<CommitResult>{CommitResult{
        CommitOutcome::kCommitted, stored.commit_seq, txn_id, Status::OK()}};
  }
  const auto async = impl_->store.ResolveAsyncTransaction(txn_id);
  if (!async.ok()) return async.status();
  if (!async.ValueOrDie().has_value()) return std::optional<CommitResult>{};
  const StoreAsyncCommitResult stored = *async.ValueOrDie();
  return std::optional<CommitResult>{CommitResult{
      stored.outcome == StoreAsyncCommitOutcome::kCommitted
          ? CommitOutcome::kCommitted : CommitOutcome::kAborted,
      stored.commit_seq, txn_id,
      stored.outcome == StoreAsyncCommitOutcome::kCommitted
          ? Status::OK() : Status::Conflict("async commit", "validation failed")}};
}

CommitPipelineMetrics Database::GetCommitPipelineMetrics() const {
  if (!impl_) return {};
  CommitPipelineMetrics metrics;
  {
    std::lock_guard<std::mutex> lock(impl_->append_commit_mutex);
    impl_->AccountPressureTime();
    metrics = impl_->append_commit_metrics;
  }
  metrics.pressure_normal_us =
      impl_->runtime_pressure_normal_us.load(std::memory_order_acquire);
  metrics.pressure_soft_us =
      impl_->runtime_pressure_soft_us.load(std::memory_order_acquire);
  metrics.pressure_hard_us =
      impl_->runtime_pressure_hard_us.load(std::memory_order_acquire);
  metrics.async_mailbox_requests_reserved =
      impl_->async_executor.ReservedRequests();
  metrics.async_mailbox_bytes_reserved = impl_->async_executor.ReservedBytes();
  const auto cache_metrics = impl_->store.SampleValidationCacheMetrics();
  if (cache_metrics.ok()) {
    metrics.validation_cache_hits = cache_metrics.ValueOrDie().hits;
    metrics.validation_cache_misses = cache_metrics.ValueOrDie().misses;
    metrics.validation_cache_resident_chains =
        cache_metrics.ValueOrDie().resident_chains;
    metrics.validation_cache_resident_bytes =
        cache_metrics.ValueOrDie().resident_bytes;
    metrics.validation_cache_slot_capacity =
        cache_metrics.ValueOrDie().slot_capacity;
    metrics.validation_cache_reserved_bytes =
        cache_metrics.ValueOrDie().reserved_bytes;
    metrics.recent_write_index_hits =
        cache_metrics.ValueOrDie().recent_write_index_hits;
    metrics.recent_write_index_misses =
        cache_metrics.ValueOrDie().recent_write_index_misses;
    metrics.recent_write_index_resets =
        cache_metrics.ValueOrDie().recent_write_index_resets;
    metrics.recent_write_index_capacity =
        cache_metrics.ValueOrDie().recent_write_index_capacity;
    metrics.recent_write_index_resident_bytes =
        cache_metrics.ValueOrDie().recent_write_index_resident_bytes;
  }
  return metrics;
}

StatusOr<RuntimeMetrics> Database::SampleRuntimeMetrics() const {
  if (!impl_) return Status::InvalidArgument("database", "moved-from database");
  const auto sampled = impl_->store.SampleRuntimeMetrics();
  if (!sampled.ok()) return sampled.status();
  return ToRuntimeMetrics(sampled.ValueOrDie());
}

}  // namespace cedar
