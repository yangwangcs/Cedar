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
  options_.min_transactions_under_load = std::min<uint32_t>(
      options_.max_transactions,
      std::max<uint32_t>(1, options_.min_transactions_under_load));
  options_.deep_queue_threshold =
      std::max<uint32_t>(1, options_.deep_queue_threshold);
}

uint64_t AdaptiveEpochController::Ewma(uint64_t previous, uint64_t sample) {
  return previous == 0 ? sample : previous - previous / 8 + sample / 8;
}

void AdaptiveEpochController::Observe(const EpochObservation& observation) {
  if (observation.transactions != 0) {
    bytes_per_transaction_ewma_ = Ewma(
        bytes_per_transaction_ewma_, observation.encoded_bytes / observation.transactions);
  }
}

uint64_t AdaptiveEpochController::BytesForTarget(uint32_t target) const {
  return std::min(options_.max_encoded_bytes,
                  SaturatingMultiply(bytes_per_transaction_ewma_, target));
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

  if (snapshot.depth <= 1) {
    if (snapshot.oldest_age_us > options_.latency_slo_us) {
      return EpochLimits{1, options_.max_encoded_bytes, 0};
    }
    return EpochLimits{1, options_.max_encoded_bytes,
                       options_.maximum_collection_age_us};
  }

  const bool deep_queue = snapshot.depth >= options_.deep_queue_threshold;
  limits.max_transactions = deep_queue ? options_.max_transactions
      : static_cast<uint32_t>(std::min<uint64_t>(
            options_.max_transactions,
            std::max<uint64_t>(options_.min_transactions_under_load,
                               snapshot.depth)));
  if (bytes_per_transaction_ewma_ != 0) {
    limits.max_encoded_bytes =
        std::max<uint64_t>(1, BytesForTarget(limits.max_transactions));
  }
  if (snapshot.oldest_age_us > options_.latency_slo_us) {
    limits.max_transactions = std::max<uint32_t>(
        options_.min_transactions_under_load, limits.max_transactions / 2);
    if (bytes_per_transaction_ewma_ != 0) {
      limits.max_encoded_bytes =
          std::max<uint64_t>(1, BytesForTarget(limits.max_transactions));
    } else {
      limits.max_encoded_bytes =
          std::max<uint64_t>(1, options_.max_encoded_bytes / 2);
    }
    limits.max_age_us = 0;
    return limits;
  }
  if (deep_queue || snapshot.depth >= limits.max_transactions) {
    limits.max_age_us = 0;
  }
  return limits;
}

}  // namespace cedar::internal
