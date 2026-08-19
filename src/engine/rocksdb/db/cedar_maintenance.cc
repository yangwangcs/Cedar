// Copyright 2026 The Cedar Authors

#include "rocksdb/cedar_maintenance.h"

#include <algorithm>
#include <chrono>
#include <limits>
#include <utility>

#include "rocksdb/db.h"
#include "rocksdb/table.h"

#include "db/db_impl/db_impl.h"

namespace ROCKSDB_NAMESPACE {
namespace {

uint64_t NowMicros() {
  return static_cast<uint64_t>(std::chrono::duration_cast<
      std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
}

uint64_t CedarSaturatingAdd(uint64_t left, uint64_t right) {
  return left > std::numeric_limits<uint64_t>::max() - right
             ? std::numeric_limits<uint64_t>::max()
             : left + right;
}

uint64_t UnixNowSeconds() {
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::system_clock::now().time_since_epoch()).count());
}

CedarColumnFamilyRole CedarRoleForName(const std::string& name) {
  if (name == kDefaultColumnFamilyName) return CedarColumnFamilyRole::kDefault;
  if (name == "facts") return CedarColumnFamilyRole::kFacts;
  if (name == "meta") return CedarColumnFamilyRole::kMeta;
  return CedarColumnFamilyRole::kOther;
}

void CedarHashCombine(uint64_t* hash, uint64_t value) {
  *hash ^= value + 0x9e3779b97f4a7c15ULL + (*hash << 6U) + (*hash >> 2U);
}

uint64_t CedarMaintenanceSignature(const CedarMaintenanceSnapshot& snapshot) {
  uint64_t hash = 0xcbf29ce484222325ULL;
  CedarHashCombine(&hash, snapshot.total_active_memtable_bytes);
  CedarHashCombine(&hash, snapshot.total_immutable_memtable_bytes);
  CedarHashCombine(&hash, snapshot.total_immutable_memtable_count);
  CedarHashCombine(&hash, snapshot.write_buffer_manager_bytes);
  CedarHashCombine(&hash, snapshot.write_buffer_manager_limit_bytes);
  CedarHashCombine(&hash, snapshot.retained_wal_bytes);
  CedarHashCombine(&hash, snapshot.total_l0_files);
  CedarHashCombine(&hash, snapshot.total_pending_compaction_bytes);
  CedarHashCombine(&hash, snapshot.running_flushes);
  CedarHashCombine(&hash, snapshot.running_compactions);
  CedarHashCombine(&hash, snapshot.background_errors);
  CedarHashCombine(&hash, snapshot.write_delayed);
  CedarHashCombine(&hash, snapshot.write_stopped);
  CedarHashCombine(&hash, snapshot.manual_conflict);
  CedarHashCombine(&hash, snapshot.recovery_in_progress);
  CedarHashCombine(&hash, snapshot.shutting_down);
  CedarHashCombine(&hash, snapshot.recovery_flush_exceptions);
  for (const auto& debt : snapshot.column_families) {
    CedarHashCombine(&hash, debt.id);
    CedarHashCombine(&hash, static_cast<uint64_t>(debt.role));
    CedarHashCombine(&hash, debt.active_memtable_bytes);
    CedarHashCombine(&hash, debt.immutable_memtable_bytes);
    CedarHashCombine(&hash, debt.immutable_memtable_count);
    CedarHashCombine(&hash, debt.oldest_immutable_age_us);
    CedarHashCombine(&hash, debt.l0_files);
    CedarHashCombine(&hash, debt.pending_compaction_bytes);
    CedarHashCombine(&hash, debt.flush_pending);
    CedarHashCombine(&hash, debt.compaction_pending);
  }
  return hash;
}

}  // namespace

Status PollCedarMaintenance(DB* db, CedarMaintenanceSnapshot* snapshot) {
  if (db == nullptr || snapshot == nullptr) {
    return Status::InvalidArgument("Cedar maintenance requires DB and snapshot");
  }
  auto* impl = dynamic_cast<DBImpl*>(db->GetRootDB());
  if (impl == nullptr) {
    return Status::NotSupported("Cedar maintenance requires DBImpl");
  }
  return impl->GetCedarMaintenanceSnapshot(snapshot);
}

Status RunCedarMaintenance(DB* db, const CedarMaintenanceGrant& grant,
                           CedarMaintenanceResult* result) {
  if (db == nullptr || result == nullptr) {
    return Status::InvalidArgument("Cedar maintenance requires DB and result");
  }
  auto* impl = dynamic_cast<DBImpl*>(db->GetRootDB());
  if (impl == nullptr) {
    return Status::NotSupported("Cedar maintenance requires DBImpl");
  }
  return impl->RunCedarMaintenance(grant, result);
}

Status DBImpl::RunCedarMaintenance(const CedarMaintenanceGrant& grant,
                                   CedarMaintenanceResult* result) {
  if (result == nullptr) {
    return Status::InvalidArgument("Cedar maintenance requires result");
  }
  if (!immutable_db_options_.cedar_kernel_mode) {
    return Status::InvalidArgument("Cedar maintenance requires kernel mode");
  }
  if (grant.kind != CedarMaintenanceKind::kFlush &&
      grant.kind != CedarMaintenanceKind::kCompaction) {
    return Status::InvalidArgument("unknown Cedar maintenance grant kind");
  }
  if (grant.deadline_us == 0) {
    return Status::InvalidArgument("Cedar maintenance grant requires deadline");
  }
  if (grant.wal_sync_critical != nullptr &&
      grant.wal_sync_critical->load(std::memory_order_acquire)) {
    result->yield = CedarMaintenanceYield::kWalSync;
    return Status::TryAgain("Cedar WAL sync is critical");
  }

  InstrumentedMutexLock lock(&mutex_);
  if (shutting_down_.load(std::memory_order_acquire)) {
    result->yield = CedarMaintenanceYield::kShutdown;
    return Status::ShutdownInProgress();
  }
  if (error_handler_.IsRecoveryInProgress()) {
    result->yield = CedarMaintenanceYield::kRecovery;
    return Status::TryAgain("Cedar maintenance is deferred during recovery");
  }
  if (grant.snapshot_generation !=
      cedar_maintenance_generation_.load(std::memory_order_relaxed)) {
    return Status::TryAgain("Cedar maintenance snapshot is stale");
  }
  std::optional<CedarGrantState>* grant_state =
      grant.kind == CedarMaintenanceKind::kFlush ? &cedar_flush_grant_
                                                  : &cedar_compaction_grant_;
  if (grant_state->has_value()) {
    return Status::Busy("Cedar maintenance grant already installed");
  }

  const uint64_t now_us = immutable_db_options_.clock->NowMicros();
  CedarGrantState state;
  state.id = next_cedar_grant_id_++;
  state.grant = grant;
  state.result.grant_id = state.id;
  state.status = Status::OK();
  state.expires_at_us =
      grant.deadline_us > std::numeric_limits<uint64_t>::max() - now_us
          ? std::numeric_limits<uint64_t>::max()
          : now_us + grant.deadline_us;
  state.installed = true;
  grant_state->emplace(std::move(state));
  MaybeScheduleFlushOrCompaction();

  while (!grant_state->value().completed &&
         !shutting_down_.load(std::memory_order_acquire)) {
    const uint64_t current_us = immutable_db_options_.clock->NowMicros();
    if (!grant_state->value().consumed &&
        current_us >= grant_state->value().expires_at_us) {
      grant_state->value().status =
          Status::TimedOut("Cedar maintenance grant expired");
      grant_state->value().result.yield = CedarMaintenanceYield::kDeadline;
      grant_state->value().completed = true;
      break;
    }
    if (grant_state->value().consumed) {
      cedar_maintenance_cv_.Wait();
    } else {
      cedar_maintenance_cv_.TimedWait(grant_state->value().expires_at_us);
    }
  }

  if (!grant_state->value().completed) {
    grant_state->value().status = Status::ShutdownInProgress();
    grant_state->value().completed = true;
  }
  *result = grant_state->value().result;
  const Status status = grant_state->value().status;
  grant_state->reset();
  return status;
}

Status DBImpl::GetCedarMaintenanceSnapshot(CedarMaintenanceSnapshot* snapshot) {
  if (snapshot == nullptr) {
    return Status::InvalidArgument("Cedar maintenance requires snapshot");
  }

  CedarMaintenanceSnapshot next;
  next.sampled_at_us = NowMicros();
  InstrumentedMutexLock mutex_lock(&mutex_);
  next.shutting_down = shutting_down_.load(std::memory_order_acquire);
  next.recovery_in_progress = error_handler_.IsRecoveryInProgress();
  next.running_flushes = static_cast<uint64_t>(
      std::max(0, num_running_flushes_.load(std::memory_order_relaxed)));
  next.running_compactions = static_cast<uint64_t>(
      std::max(0, num_running_compactions_.load(std::memory_order_relaxed)));
  next.retained_wal_bytes = wals_total_size_.LoadRelaxed();
  // WBM stalls are a database-wide write stop even when no compaction stop
  // token is held. Cedar must see both sources to admit an emergency flush.
  next.write_stopped = write_controller_.IsStopped() ||
                       (write_buffer_manager_ != nullptr &&
                        write_buffer_manager_->IsStallActive());
  next.write_delayed = write_controller_.NeedsDelay();
  next.delayed_write_rate_bytes_per_sec =
      next.write_delayed ? write_controller_.delayed_write_rate() : 0;
  next.manual_conflict = HasExclusiveManualCompaction();
  next.maybe_schedule_flush_or_compaction_calls =
      cedar_maybe_schedule_calls_.load(std::memory_order_relaxed);
  next.background_flush_calls =
      cedar_background_flush_calls_.load(std::memory_order_relaxed);
  next.manual_compaction_calls =
      cedar_manual_compaction_calls_.load(std::memory_order_relaxed);
  next.periodic_task_registrations =
      cedar_periodic_task_registrations_.load(std::memory_order_relaxed);
  next.recovery_flush_exceptions =
      cedar_recovery_flush_exceptions_.load(std::memory_order_relaxed);
  if (write_buffer_manager_ != nullptr) {
    next.write_buffer_manager_bytes = write_buffer_manager_->memory_usage();
    next.write_buffer_manager_limit_bytes = write_buffer_manager_->buffer_size();
  }

  const uint64_t now_seconds = UnixNowSeconds();
  bool block_cache_sampled = false;
  for (auto* cfd : *versions_->GetColumnFamilySet()) {
    if (cfd == nullptr || !cfd->initialized() || cfd->IsDropped()) continue;
    CedarColumnFamilyDebt debt;
    debt.id = cfd->GetID();
    debt.role = CedarRoleForName(cfd->GetName());
    debt.active_memtable_bytes = cfd->mem()->ApproximateMemoryUsageFast();
    debt.immutable_memtable_bytes = cfd->imm()->ApproximateMemoryUsage();
    debt.immutable_memtable_count = cfd->imm()->NumNotFlushed();
    const uint64_t oldest_key_time = cfd->imm()->ApproximateOldestKeyTime();
    if (oldest_key_time != std::numeric_limits<uint64_t>::max() &&
        now_seconds > oldest_key_time) {
      const uint64_t age_seconds = now_seconds - oldest_key_time;
      debt.oldest_immutable_age_us =
          age_seconds > std::numeric_limits<uint64_t>::max() / 1000000ULL
              ? std::numeric_limits<uint64_t>::max()
              : age_seconds * 1000000ULL;
    }
    const auto* storage = cfd->current()->storage_info();
    debt.l0_files = static_cast<uint64_t>(storage->NumLevelFiles(0));
    debt.pending_compaction_bytes = storage->estimated_compaction_needed_bytes();
    debt.flush_pending = cfd->imm()->IsFlushPending();
    debt.compaction_pending = cfd->queued_for_compaction() || cfd->NeedsCompaction();
    next.total_active_memtable_bytes = CedarSaturatingAdd(
        next.total_active_memtable_bytes, debt.active_memtable_bytes);
    next.total_immutable_memtable_bytes = CedarSaturatingAdd(
        next.total_immutable_memtable_bytes, debt.immutable_memtable_bytes);
    next.total_immutable_memtable_count = CedarSaturatingAdd(
        next.total_immutable_memtable_count, debt.immutable_memtable_count);
    next.total_l0_files = CedarSaturatingAdd(next.total_l0_files, debt.l0_files);
    next.total_pending_compaction_bytes = CedarSaturatingAdd(
        next.total_pending_compaction_bytes, debt.pending_compaction_bytes);
    next.background_errors = CedarSaturatingAdd(
        next.background_errors, cfd->internal_stats()->GetBackgroundErrorCount());
    next.live_sst_bytes = CedarSaturatingAdd(
        next.live_sst_bytes, cfd->current()->GetSstFilesSize());
    if (!block_cache_sampled) {
      auto* table_factory = cfd->GetCurrentMutableCFOptions().table_factory.get();
      if (table_factory != nullptr) {
        Cache* block_cache =
            table_factory->GetOptions<Cache>(TableFactory::kBlockCacheOpts());
        if (block_cache != nullptr) {
          next.block_cache_usage_bytes = block_cache->GetUsage();
          next.block_cache_pinned_bytes = block_cache->GetPinnedUsage();
          block_cache_sampled = true;
        }
      }
    }
    next.column_families.push_back(debt);
  }

  const uint64_t signature = CedarMaintenanceSignature(next);
  if (!cedar_maintenance_snapshot_initialized_ ||
      signature != cedar_maintenance_snapshot_signature_) {
    cedar_maintenance_snapshot_signature_ = signature;
    cedar_maintenance_snapshot_initialized_ = true;
    cedar_maintenance_generation_.fetch_add(1, std::memory_order_relaxed);
  }
  next.generation = cedar_maintenance_generation_.load(std::memory_order_relaxed);
  *snapshot = std::move(next);
  return Status::OK();
}

}  // namespace ROCKSDB_NAMESPACE
