// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_RUNTIME_PRESSURE_CONTROLLER_H_
#define CEDAR_RUNTIME_PRESSURE_CONTROLLER_H_

#include <array>
#include <cstdint>
#include <limits>
#include <mutex>

#include "cedar/observability/metric_registry.h"

namespace cedar {

enum class PressureState : uint8_t {
  kNormal,
  kSoftPressure,
  kHardPressure,
  kWriteStall,
  kDiskEmergency,
  kRecovery,
  kShutdown,
};

enum class PressureCause : uint8_t {
  kNone,
  kMemtable,
  kWal,
  kCompaction,
  kCache,
  kDisk,
};

struct PressureSignals {
  uint64_t memtable_bytes = 0;
  uint64_t wal_backlog_bytes = 0;
  uint64_t compaction_debt_bytes = 0;
  uint64_t cache_pressure_bytes = 0;
  uint64_t disk_pressure_bytes = 0;

  uint64_t Maximum() const {
    uint64_t value = memtable_bytes;
    value = value > wal_backlog_bytes ? value : wal_backlog_bytes;
    value = value > compaction_debt_bytes ? value : compaction_debt_bytes;
    value = value > cache_pressure_bytes ? value : cache_pressure_bytes;
    return value > disk_pressure_bytes ? value : disk_pressure_bytes;
  }
};

struct PressureDecision {
  PressureState state = PressureState::kNormal;
  PressureCause cause = PressureCause::kNone;
  bool admit_writes = true;
  bool admit_analytical = true;
  bool admit_optional_maintenance = true;
  bool cancel_queued_analytical = false;
  bool require_flush = false;
  bool require_urgent_compaction = false;
  bool require_blob_gc = false;
};

class PressureController {
 public:
  using Decision = PressureDecision;

  PressureController(uint64_t high, uint64_t low, uint64_t emergency = 0)
      : high_(high), low_(low > high ? high : low),
        emergency_(emergency == 0
                       ? (high > std::numeric_limits<uint64_t>::max() - high
                              ? std::numeric_limits<uint64_t>::max()
                              : high * 2)
                       : (emergency < high ? high : emergency)) {}

  bool Update(uint64_t level) {
    return Update(PressureSignals{level}).admit_writes;
  }

  PressureDecision Update(const PressureSignals& signals) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ == PressureState::kRecovery ||
        state_ == PressureState::kShutdown) {
      return decision_;
    }

    const uint64_t level = signals.Maximum();
    PressureState next_state = PressureState::kNormal;
    PressureCause next_cause = PressureCause::kNone;
    if (level > low_ || level >= high_) {
      next_cause = SelectCause(signals);
      if (signals.disk_pressure_bytes >= emergency_) {
        next_state = PressureState::kDiskEmergency;
      } else if (signals.memtable_bytes >= high_ ||
                 signals.wal_backlog_bytes >= high_) {
        next_state = PressureState::kWriteStall;
      } else if (level >= high_) {
        next_state = PressureState::kHardPressure;
      } else {
        next_state = PressureState::kSoftPressure;
      }
    }

    const int next_rank = SafetyRank(next_state);
    const int current_rank = SafetyRank(state_);
    if (level > low_ && state_ != PressureState::kNormal &&
        (next_rank < current_rank ||
         (next_rank == current_rank && level < last_level_))) {
      next_state = state_;
      next_cause = cause_;
    }
    SetDecisionLocked(next_state, next_cause);
    last_level_ = level;
    return decision_;
  }

  PressureDecision EnterRecovery() {
    std::lock_guard<std::mutex> lock(mutex_);
    SetDecisionLocked(PressureState::kRecovery, PressureCause::kNone);
    return decision_;
  }

  PressureDecision EnterShutdown() {
    std::lock_guard<std::mutex> lock(mutex_);
    SetDecisionLocked(PressureState::kShutdown, PressureCause::kNone);
    return decision_;
  }

  PressureState state() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return state_;
  }
  PressureCause cause() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cause_;
  }
  PressureDecision decision() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return decision_;
  }

  Status ExportMetrics(MetricRegistry* metrics) const {
    if (metrics == nullptr) {
      return Status::InvalidArgument("pressure metrics", "missing metric registry");
    }
    static constexpr std::array<const char*, 6> kCauseLabels = {
        "none", "memtable", "wal", "compaction", "cache", "disk"};
    static constexpr std::array<const char*, 7> kStateLabels = {
        "normal", "soft_pressure", "hard_pressure", "write_stall",
        "disk_emergency", "recovery", "shutdown"};
    Status status = metrics->Register(MetricDefinition{
        "cedar_pressure_transitions_total", MetricType::kCounter, "count", 1});
    if (!status.ok()) return status;
    status = metrics->Register(MetricDefinition{
        "cedar_pressure_state", MetricType::kGauge, "state", 1});
    if (!status.ok()) return status;
    for (const char* label : kCauseLabels) {
      status = metrics->AddCounter(
          "cedar_pressure_transitions_total", label, 0);
      if (!status.ok()) return status;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    for (size_t index = 0; index < kCauseLabels.size(); ++index) {
      status = metrics->AddCounter(
          "cedar_pressure_transitions_total", kCauseLabels[index],
          transition_counts_[index] - exported_transition_counts_[index]);
      if (!status.ok()) return status;
      exported_transition_counts_[index] = transition_counts_[index];
    }
    const size_t current_state = static_cast<size_t>(state_);
    for (size_t index = 0; index < kStateLabels.size(); ++index) {
      status = metrics->SetGauge(
          "cedar_pressure_state", kStateLabels[index],
          index == current_state ? 1 : 0);
      if (!status.ok()) return status;
    }
    return Status::OK();
  }

 private:
  static int SafetyRank(PressureState state) {
    switch (state) {
      case PressureState::kNormal: return 0;
      case PressureState::kSoftPressure: return 1;
      case PressureState::kHardPressure: return 2;
      case PressureState::kWriteStall: return 3;
      case PressureState::kDiskEmergency: return 4;
      case PressureState::kRecovery: return 5;
      case PressureState::kShutdown: return 6;
    }
    return 0;
  }

  static PressureCause SelectCause(const PressureSignals& signals) {
    uint64_t maximum = signals.disk_pressure_bytes;
    PressureCause cause = PressureCause::kDisk;
    const auto select = [&maximum, &cause](uint64_t value,
                                           PressureCause candidate) {
      if (value > maximum) {
        maximum = value;
        cause = candidate;
      }
    };
    select(signals.wal_backlog_bytes, PressureCause::kWal);
    select(signals.memtable_bytes, PressureCause::kMemtable);
    select(signals.compaction_debt_bytes, PressureCause::kCompaction);
    select(signals.cache_pressure_bytes, PressureCause::kCache);
    return maximum == 0 ? PressureCause::kNone : cause;
  }

  static PressureDecision Actions(PressureState state, PressureCause cause) {
    PressureDecision decision;
    decision.state = state;
    decision.cause = cause;
    const bool flush = cause == PressureCause::kMemtable ||
                       cause == PressureCause::kWal;
    const bool compact = cause == PressureCause::kCompaction ||
                         cause == PressureCause::kDisk;
    const bool blob_gc = cause == PressureCause::kDisk;
    switch (state) {
      case PressureState::kNormal:
        return decision;
      case PressureState::kSoftPressure:
        decision.admit_optional_maintenance = false;
        decision.require_flush = flush;
        decision.require_urgent_compaction = compact;
        decision.require_blob_gc = blob_gc;
        return decision;
      case PressureState::kHardPressure:
        decision.admit_analytical = false;
        decision.admit_optional_maintenance = false;
        decision.cancel_queued_analytical = true;
        decision.require_flush = flush;
        decision.require_urgent_compaction = compact;
        decision.require_blob_gc = blob_gc;
        return decision;
      case PressureState::kWriteStall:
        decision.admit_writes = false;
        decision.admit_analytical = false;
        decision.admit_optional_maintenance = false;
        decision.cancel_queued_analytical = true;
        decision.require_flush = true;
        decision.require_urgent_compaction = compact;
        decision.require_blob_gc = blob_gc;
        return decision;
      case PressureState::kDiskEmergency:
        decision.admit_writes = false;
        decision.admit_analytical = false;
        decision.admit_optional_maintenance = false;
        decision.cancel_queued_analytical = true;
        decision.require_urgent_compaction = true;
        decision.require_blob_gc = true;
        return decision;
      case PressureState::kRecovery:
      case PressureState::kShutdown:
        decision.admit_writes = false;
        decision.admit_analytical = false;
        decision.admit_optional_maintenance = false;
        decision.cancel_queued_analytical = true;
        return decision;
    }
    return decision;
  }

  void SetDecisionLocked(PressureState state, PressureCause cause) {
    if (state != state_ || cause != cause_) {
      ++transition_counts_[static_cast<size_t>(cause)];
    }
    state_ = state;
    cause_ = cause;
    decision_ = Actions(state_, cause_);
  }

  uint64_t high_;
  uint64_t low_;
  uint64_t emergency_;
  uint64_t last_level_ = 0;
  PressureState state_ = PressureState::kNormal;
  PressureCause cause_ = PressureCause::kNone;
  PressureDecision decision_;
  std::array<uint64_t, 6> transition_counts_{};
  mutable std::array<uint64_t, 6> exported_transition_counts_{};
  mutable std::mutex mutex_;
};

}  // namespace cedar

#endif  // CEDAR_RUNTIME_PRESSURE_CONTROLLER_H_
