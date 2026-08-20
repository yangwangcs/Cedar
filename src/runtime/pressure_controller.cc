// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/runtime/pressure_controller.h"

#include <algorithm>

namespace cedar {
namespace {

constexpr uint64_t kGiB = 1024ULL * 1024ULL * 1024ULL;
constexpr uint64_t kSoftPendingCompactionBytes = 8ULL * kGiB;
constexpr uint64_t kHardPendingCompactionBytes = 32ULL * kGiB;
constexpr uint64_t kSoftRetainedWalBytes = 768ULL * 1024ULL * 1024ULL;
constexpr uint64_t kHardRetainedWalBytes = 1ULL * kGiB;
constexpr uint64_t kPressureHorizonSeconds = 5;

bool DiskSignalAvailable(const PressureSample& sample) {
  return sample.free_disk_bytes != UINT64_MAX;
}

bool LowDiskSoft(const PressureSample& sample) {
  return DiskSignalAvailable(sample) &&
         (sample.free_disk_bytes < 10 * kGiB || sample.free_disk_percent < 10);
}

bool LowDiskHard(const PressureSample& sample) {
  return DiskSignalAvailable(sample) &&
         (sample.free_disk_bytes < 4 * kGiB || sample.free_disk_percent < 5);
}

bool DiskRecoveredSoft(const PressureSample& sample) {
  return !DiskSignalAvailable(sample) ||
         (sample.free_disk_bytes >= 10 * kGiB && sample.free_disk_percent >= 10);
}

bool DiskRecoveredHard(const PressureSample& sample) {
  return !DiskSignalAvailable(sample) ||
         (sample.free_disk_bytes >= 4 * kGiB && sample.free_disk_percent >= 5);
}

uint64_t SaturatingAdd(uint64_t left, uint64_t right) {
  return left > UINT64_MAX - right ? UINT64_MAX : left + right;
}

uint64_t ProjectedDebt(uint64_t base, uint64_t admitted, uint64_t completed) {
  if (admitted <= completed) return base;
  const uint64_t deficit = admitted - completed;
  const uint64_t horizon = deficit > UINT64_MAX / kPressureHorizonSeconds
                               ? UINT64_MAX
                               : deficit * kPressureHorizonSeconds;
  return SaturatingAdd(base, horizon);
}

}  // namespace

uint64_t PressureController::Ewma(uint64_t old_value, uint64_t sample) {
  // alpha = 1/8; integer arithmetic keeps the controller deterministic.
  return old_value == 0 ? sample : old_value - old_value / 8 + sample / 8;
}

void PressureController::Observe(const PressureSample& sample) {
  arrival_rate_ewma_ = Ewma(arrival_rate_ewma_, sample.arrival_rate);
  bytes_per_txn_ewma_ = Ewma(bytes_per_txn_ewma_, sample.bytes_per_txn);
  wal_sync_us_ewma_ = Ewma(wal_sync_us_ewma_, sample.wal_sync_us);
  memtable_us_ewma_ = Ewma(memtable_us_ewma_, sample.memtable_us);
  queue_age_us_ewma_ = Ewma(queue_age_us_ewma_, sample.queue_age_us);

  // Rates are meaningful only when the sampler supplied a nonzero interval.
  // This prevents a zero-duration/reset sample from creating a false pressure
  // transition or corrupting the EWMA state.
  if (sample.sample_interval_us != 0) {
    admitted_rate_ewma_ = Ewma(admitted_rate_ewma_,
                               sample.admitted_facts_bytes_per_sec);
    background_rate_ewma_ = Ewma(background_rate_ewma_,
                                  sample.completed_background_bytes_per_sec);
  }
  const uint64_t observed_storage_debt = std::max(
      sample.storage_debt_bytes, sample.columnar_flush_pending_bytes);
  projected_debt_bytes_ = ProjectedDebt(observed_storage_debt,
                                        admitted_rate_ewma_,
                                        background_rate_ewma_);

  const bool valid_rate_sample = sample.sample_interval_us != 0;
  const bool rates_healthy =
      !valid_rate_sample || background_rate_ewma_ >= admitted_rate_ewma_;
  const bool deficit = valid_rate_sample &&
                       admitted_rate_ewma_ > background_rate_ewma_;
  if (deficit) {
    ++deficit_observations_;
    deficit_duration_us_ = SaturatingAdd(deficit_duration_us_,
                                         sample.sample_interval_us);
  } else {
    deficit_observations_ = 0;
    deficit_duration_us_ = 0;
  }

  const bool hard = sample.write_stopped != 0 || sample.background_error != 0 ||
                    LowDiskHard(sample) ||
                    sample.l0_files >= 24 ||
                    sample.pending_compaction_bytes >= kHardPendingCompactionBytes ||
                    sample.retained_wal_bytes >= kHardRetainedWalBytes ||
                    sample.immutable_memtable_count >= 4 ||
                    sample.columnar_backlog_buffers >= 2 ||
                    sample.immutable_memtable_percent >= 90 ||
                    (deficit_duration_us_ >= kPressureHorizonSeconds * 1'000'000 &&
                     observed_storage_debt >= kSoftPendingCompactionBytes &&
                     projected_debt_bytes_ >= kHardPendingCompactionBytes);
  const bool soft = LowDiskSoft(sample) || sample.l0_files >= 16 ||
                    sample.pending_compaction_bytes >= kSoftPendingCompactionBytes ||
                    sample.retained_wal_bytes >= kSoftRetainedWalBytes ||
                    sample.immutable_memtable_count >= 2 ||
                    sample.columnar_backlog_buffers >= 1 ||
                    sample.immutable_memtable_percent >= 75 ||
                    (deficit_observations_ >= 3 &&
                     projected_debt_bytes_ >= kSoftPendingCompactionBytes);
  if (state_ == PressureState::kHard) {
    if (hard || !rates_healthy || !DiskRecoveredHard(sample) ||
        sample.l0_files >= 16 ||
        sample.pending_compaction_bytes >= kSoftPendingCompactionBytes ||
        sample.retained_wal_bytes >= kSoftRetainedWalBytes ||
        sample.immutable_memtable_count >= 2 ||
        sample.columnar_backlog_buffers >= 1 ||
        sample.immutable_memtable_percent >= 70) {
      recovery_observations_ = 0;
    } else if (++recovery_observations_ >= 3) {
      state_ = soft ? PressureState::kSoft : PressureState::kNormal;
      recovery_observations_ = 0;
    }
  } else if (state_ == PressureState::kSoft) {
    if (hard) {
      state_ = PressureState::kHard;
      recovery_observations_ = 0;
    } else if (soft || !rates_healthy || !DiskRecoveredSoft(sample) ||
               sample.l0_files > 12 ||
               sample.pending_compaction_bytes >= kSoftPendingCompactionBytes / 2 ||
               sample.retained_wal_bytes >= kSoftRetainedWalBytes ||
               sample.immutable_memtable_count != 0 ||
               sample.columnar_backlog_buffers != 0 ||
               sample.immutable_memtable_percent != 0) {
      recovery_observations_ = 0;
    } else if (++recovery_observations_ >= 3) {
      state_ = PressureState::kNormal;
      recovery_observations_ = 0;
    }
  } else {
    state_ = hard ? PressureState::kHard
                  : (soft ? PressureState::kSoft : PressureState::kNormal);
    recovery_observations_ = 0;
  }

  const bool busy = sample.arrival_rate >= 256 || sample.queue_depth >= 64;
  if (state_ == PressureState::kNormal && busy) {
    if (++busy_observations_ >= 4) {
      target_count_ = std::min<uint32_t>(256, target_count_ + 64);
      busy_observations_ = 0;
    }
  } else if (state_ != PressureState::kNormal || !busy) {
    busy_observations_ = 0;
    if (state_ == PressureState::kNormal) target_count_ = 128;
  }
  if (state_ == PressureState::kSoft) {
    target_count_ = 64;
    target_bytes_ = 1ULL * 1024ULL * 1024ULL;
    collection_window_us_ = 200;
  } else if (state_ == PressureState::kHard) {
    target_count_ = 64;
    target_bytes_ = 512ULL * 1024ULL;
    collection_window_us_ = 0;
  } else {
    target_bytes_ = 2ULL * 1024ULL * 1024ULL;
    collection_window_us_ = 200;
  }
}

AdmissionDecision PressureController::DecideAdmission(
    uint64_t request_count, uint64_t request_bytes, uint64_t deadline_us) const {
  AdmissionDecision decision{state_ != PressureState::kHard, target_count_,
                             target_bytes_, collection_window_us_};
  if (state_ == PressureState::kSoft && deadline_us == 0 && request_count != 0) {
    decision.admit = false;
  }
  if (request_count >= decision.max_count || request_bytes >= decision.max_bytes) {
    decision.admit = false;
  }
  return decision;
}

}  // namespace cedar
