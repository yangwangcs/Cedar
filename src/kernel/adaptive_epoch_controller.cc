// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "kernel/adaptive_epoch_controller.h"

#include <algorithm>
#include <limits>

namespace cedar::internal {
namespace {

uint64_t SaturatingMultiply(uint64_t left, uint64_t right) {
  if (left == 0 || right == 0) return 0;
  return left > std::numeric_limits<uint64_t>::max() / right
             ? std::numeric_limits<uint64_t>::max()
             : left * right;
}

}  // namespace

AdaptiveEpochController::AdaptiveEpochController(Options options) : options_(options) {
  options_.max_transactions = std::max<uint32_t>(1, options_.max_transactions);
  options_.max_encoded_bytes = std::max<uint64_t>(1, options_.max_encoded_bytes);
  options_.latency_slo_us = std::max<uint64_t>(1, options_.latency_slo_us);
}

uint64_t AdaptiveEpochController::Ewma(uint64_t previous, uint64_t sample) {
  return previous == 0 ? sample : previous - previous / 8 + sample / 8;
}

void AdaptiveEpochController::Observe(const EpochObservation& observation) {
  wal_sync_us_ewma_ = Ewma(wal_sync_us_ewma_, observation.wal_sync_us);
  queue_p99_us_ewma_ = Ewma(queue_p99_us_ewma_, observation.queue_p99_us);
  if (observation.transactions != 0) {
    bytes_per_transaction_ewma_ = Ewma(
        bytes_per_transaction_ewma_, observation.encoded_bytes / observation.transactions);
  }
  if (observations_ != std::numeric_limits<uint32_t>::max()) ++observations_;
}

EpochLimits AdaptiveEpochController::NextLimits(
    const EpochQueueSnapshot& snapshot) const {
  EpochLimits limits{options_.max_transactions, options_.max_encoded_bytes,
                     options_.maximum_collection_age_us};
  if (snapshot.pressure_state == PressureState::kHard) {
    return EpochLimits{1, std::min<uint64_t>(options_.max_encoded_bytes,
                                             512ULL * 1024ULL), 0};
  }
  if (snapshot.pressure_state == PressureState::kSoft) {
    return EpochLimits{std::max<uint32_t>(1, options_.max_transactions / 2),
                       std::max<uint64_t>(1, options_.max_encoded_bytes / 2),
                       std::min<uint64_t>(options_.maximum_collection_age_us, 50)};
  }

  const bool enough_history = observations_ >= 4;
  const bool latency_limited = enough_history &&
      (wal_sync_us_ewma_ > options_.latency_slo_us ||
       queue_p99_us_ewma_ > options_.latency_slo_us ||
       snapshot.oldest_age_us > options_.latency_slo_us);
  if (latency_limited) {
    return EpochLimits{std::max<uint32_t>(1, options_.max_transactions / 2),
                       std::max<uint64_t>(1, options_.max_encoded_bytes / 2), 0};
  }

  if (snapshot.depth <= 1) {
    return EpochLimits{1, options_.max_encoded_bytes,
                       options_.maximum_collection_age_us};
  }

  limits.max_transactions = static_cast<uint32_t>(std::min<uint64_t>(
      options_.max_transactions, std::max<uint64_t>(2, snapshot.depth)));
  if (bytes_per_transaction_ewma_ != 0) {
    const uint64_t target_bytes = SaturatingMultiply(
        bytes_per_transaction_ewma_, limits.max_transactions);
    limits.max_encoded_bytes = std::min(options_.max_encoded_bytes,
                                        std::max<uint64_t>(1, target_bytes));
  }
  if (snapshot.depth >= limits.max_transactions) limits.max_age_us = 0;
  return limits;
}

}  // namespace cedar::internal
