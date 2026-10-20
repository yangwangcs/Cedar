// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_KERNEL_ADAPTIVE_EPOCH_CONTROLLER_H_
#define CEDAR_KERNEL_ADAPTIVE_EPOCH_CONTROLLER_H_

#include <cstdint>

#include "cedar/runtime/pressure_controller.h"

namespace cedar::internal {

struct EpochObservation {
  uint64_t wal_sync_us = 0;
  uint64_t queue_p99_us = 0;
  uint64_t transactions = 0;
  uint64_t encoded_bytes = 0;
  bool wal_sync_cost_known = true;
};

struct EpochQueueSnapshot {
  uint64_t depth = 0;
  uint64_t oldest_age_us = 0;
  PressureState pressure_state = PressureState::kNormal;
};

struct EpochLimits {
  uint32_t max_transactions = 1;
  uint64_t max_encoded_bytes = 1;
  // Remaining collection time from the oldest request's enqueue time.
  uint64_t max_age_us = 0;
  // Extra collection time allowed when WAL sync dominates, to fill the batch.
  uint64_t settle_age_us = 0;
};

class AdaptiveEpochController {
 public:
  struct Options {
    uint32_t max_transactions = 128;
    uint64_t max_encoded_bytes = 2ULL * 1024ULL * 1024ULL;
    uint64_t latency_slo_us = 5'000;
    uint64_t maximum_collection_age_us = 200;
    uint32_t min_transactions_under_load = 2;
    uint32_t deep_queue_threshold = 16;
    // Group-commit settle: when observed WAL sync cost is at least the floor,
    // collection waits this much extra to fill the batch. 0 disables.
    uint64_t settle_sync_floor_us = 0;
    uint64_t settle_max_age_us = 8'000;
    uint32_t settle_sync_divisor = 8;
  };

  explicit AdaptiveEpochController(Options options);

  void Observe(const EpochObservation& observation);
  EpochLimits NextLimits(const EpochQueueSnapshot& snapshot) const;

 private:
  static uint64_t Ewma(uint64_t previous, uint64_t sample);
  uint64_t BytesForTarget(uint32_t target) const;

  Options options_;
  uint64_t bytes_per_transaction_ewma_ = 0;
  uint64_t wal_sync_us_ewma_ = 0;
  bool observed_wal_sync_cost_ = false;
};

}  // namespace cedar::internal

#endif  // CEDAR_KERNEL_ADAPTIVE_EPOCH_CONTROLLER_H_
