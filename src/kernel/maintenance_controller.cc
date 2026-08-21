// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "kernel/maintenance_controller.h"

#include <algorithm>
#include <chrono>

namespace cedar {
namespace {

constexpr uint32_t kCedarFlushWorkerSlots = 2;
constexpr uint32_t kCedarCompactionWorkerSlots = 1;

}  // namespace

MaintenanceController::MaintenanceController(MaintenanceAdapter* adapter)
    : adapter_(adapter) {}

MaintenanceController::~MaintenanceController() { Stop(); }

Status MaintenanceController::Start() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (adapter_ == nullptr) {
    return Status::InvalidArgument("maintenance controller", "adapter is null");
  }
  if (started_) return Status::OK();
  stopping_ = false;
  started_ = true;
  flush_workers_.reserve(kCedarFlushWorkerSlots);
  for (uint32_t slot = 0; slot < kCedarFlushWorkerSlots; ++slot) {
    flush_workers_.emplace_back([this] { RunLane(Lane::kFlush); });
  }
  compaction_workers_.reserve(kCedarCompactionWorkerSlots);
  for (uint32_t slot = 0; slot < kCedarCompactionWorkerSlots; ++slot) {
    compaction_workers_.emplace_back([this] { RunLane(Lane::kCompaction); });
  }
  return Status::OK();
}

void MaintenanceController::PublishSnapshot(CedarRuntimeSnapshot snapshot) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (stopping_) return;
  snapshot_ = std::move(snapshot);
  ++published_sequence_;
  ++metrics_.snapshots_published;
  work_available_.notify_all();
}

void MaintenanceController::SetWalSyncCritical(bool critical) {
  wal_sync_critical_.store(critical, std::memory_order_release);
  work_available_.notify_all();
}

void MaintenanceController::Stop() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!started_) return;
    stopping_ = true;
  }
  work_available_.notify_all();
  for (std::thread& worker : compaction_workers_) {
    if (worker.joinable()) worker.join();
  }
  compaction_workers_.clear();
  for (std::thread& worker : flush_workers_) {
    if (worker.joinable()) worker.join();
  }
  flush_workers_.clear();
  std::lock_guard<std::mutex> lock(mutex_);
  started_ = false;
}

CedarMaintenanceMetrics MaintenanceController::metrics() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return metrics_;
}

std::optional<CedarMaintenanceDecision>
MaintenanceController::NextDecisionLocked(Lane lane) const {
  if (!snapshot_.has_value() || stopping_ || metrics_.first_error.has_value()) {
    return std::nullopt;
  }
  const CedarMaintenancePlan plan = SelectCedarMaintenance(
      *snapshot_, history_, wal_sync_critical_.load(std::memory_order_acquire));
  const std::optional<CedarMaintenanceDecision>& decision =
      lane == Lane::kFlush ? plan.flush : plan.compaction;
  if (!decision.has_value()) return std::nullopt;
  if (lane == Lane::kFlush) {
    const uint32_t dispatched = flush_dispatched_sequence_ == published_sequence_
                                    ? flush_dispatched_credits_
                                    : 0;
    if (dispatched >= plan.flush_credits) return std::nullopt;
  } else {
    const uint32_t dispatched =
        compaction_dispatched_sequence_ == published_sequence_
            ? compaction_dispatched_credits_
            : 0;
    if (dispatched >= plan.compaction_credits) return std::nullopt;
  }
  if (lane == Lane::kCompaction && decision->priority == CedarMaintenancePriority::kNormal &&
      wal_sync_critical_.load(std::memory_order_acquire)) {
    return std::nullopt;
  }
  return decision;
}

bool MaintenanceController::LaneHasWorkLocked(Lane lane) const {
  if (lane == Lane::kFlush && flush_outstanding_ >= kCedarFlushWorkerSlots) {
    return false;
  }
  if (lane == Lane::kCompaction &&
      compaction_outstanding_ >= kCedarCompactionWorkerSlots) {
    return false;
  }
  return NextDecisionLocked(lane).has_value();
}

void MaintenanceController::CompleteLocked(
    const CedarMaintenanceCompletion& completion) {
  ++metrics_.completed_grants;
  metrics_.input_bytes += completion.input_bytes;
  metrics_.output_bytes += completion.output_bytes;
  const size_t yield = static_cast<size_t>(completion.yield);
  if (yield < metrics_.yields.size()) ++metrics_.yields[yield];
  if (completion.kind == CedarMaintenanceKind::kFlush) {
    history_.last_flush = completion;
    metrics_.last_flush_completion = completion;
  } else {
    history_.last_compaction = completion;
  }
  const bool retryable_yield =
      completion.yield == CedarMaintenanceYield::kNoDebt ||
      completion.yield == CedarMaintenanceYield::kStaleGeneration ||
      completion.yield == CedarMaintenanceYield::kInputBudget ||
      completion.yield == CedarMaintenanceYield::kOutputBudget ||
      completion.yield == CedarMaintenanceYield::kDeadline ||
      completion.yield == CedarMaintenanceYield::kWalSync ||
      completion.yield == CedarMaintenanceYield::kManualConflict ||
      completion.yield == CedarMaintenanceYield::kRecovery ||
      completion.yield == CedarMaintenanceYield::kShutdown;
  if (!completion.status.ok() && !retryable_yield) {
    ++metrics_.maintenance_errors;
    if (!metrics_.first_error.has_value()) metrics_.first_error = completion.status;
  }
}

void MaintenanceController::RunLane(Lane lane) {
  for (;;) {
    CedarMaintenanceDecision decision;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      work_available_.wait(lock, [this, lane] {
        return stopping_ || LaneHasWorkLocked(lane);
      });
      if (stopping_) return;
      const std::optional<CedarMaintenanceDecision> next = NextDecisionLocked(lane);
      if (!next.has_value()) continue;
      decision = *next;
      if (lane == Lane::kFlush) {
        ++flush_outstanding_;
        if (flush_dispatched_sequence_ != published_sequence_) {
          flush_dispatched_sequence_ = published_sequence_;
          flush_dispatched_credits_ = 0;
        }
        flush_dispatched_sequence_ = published_sequence_;
        ++flush_dispatched_credits_;
        ++metrics_.flush_grants_requested;
      } else {
        ++compaction_outstanding_;
        if (compaction_dispatched_sequence_ != published_sequence_) {
          compaction_dispatched_sequence_ = published_sequence_;
          compaction_dispatched_credits_ = 0;
        }
        ++compaction_dispatched_credits_;
        ++metrics_.compaction_grants_requested;
      }
    }

    StatusOr<CedarMaintenanceCompletion> run =
        lane == Lane::kFlush
            ? adapter_->RunFlush(decision, &wal_sync_critical_)
            : adapter_->RunCompaction(decision, &wal_sync_critical_);
    CedarMaintenanceCompletion completion;
    if (run.ok()) {
      completion = run.ValueOrDie();
    } else {
      completion.kind = lane == Lane::kFlush ? CedarMaintenanceKind::kFlush
                                               : CedarMaintenanceKind::kCompaction;
      completion.status = run.status();
    }

    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (lane == Lane::kFlush) {
        --flush_outstanding_;
        if (run.ok()) ++metrics_.flush_grants_accepted;
      } else {
        --compaction_outstanding_;
        if (run.ok()) ++metrics_.compaction_grants_accepted;
      }
      CompleteLocked(completion);
    }
    work_available_.notify_all();
  }
}

}  // namespace cedar
