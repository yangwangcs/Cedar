// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_RUNTIME_IO_GOVERNOR_H_
#define CEDAR_RUNTIME_IO_GOVERNOR_H_

#include <cstdint>
#include <mutex>

#include "cedar/core/status.h"

namespace cedar {

struct IoTokenBudget {
  uint64_t capacity = 0;
  uint64_t replenish_per_second = 0;
};
struct IoGovernorLimits {
  IoTokenBudget sequential_read_bytes;
  IoTokenBudget random_read_ops;
  IoTokenBudget write_bytes;
  IoTokenBudget metadata_ops;
};
struct IoTokenRequest {
  uint64_t sequential_read_bytes = 0;
  uint64_t random_read_ops = 0;
  uint64_t write_bytes = 0;
  uint64_t metadata_ops = 0;
  bool commit_critical = false;
};
struct IoTokenSnapshot {
  uint64_t sequential_read_bytes = 0;
  uint64_t random_read_ops = 0;
  uint64_t write_bytes = 0;
  uint64_t metadata_ops = 0;
};

// Four independent device token buckets. Normal work cannot borrow the
// metadata reserve required to finish WAL, DecisionLog, and Manifest work.
class IoGovernor {
 public:
  IoGovernor(IoGovernorLimits limits, IoTokenSnapshot critical_reserve = {});
  Status TryAcquire(const IoTokenRequest& request, uint64_t monotonic_now_ns);
  void Replenish(uint64_t monotonic_now_ns);
  IoTokenSnapshot available(bool commit_critical = false) const;

 private:
  struct Bucket {
    IoTokenBudget budget;
    uint64_t tokens = 0;
  };
  static void ReplenishBucket(Bucket* bucket, uint64_t elapsed_ns);
  static bool Fits(uint64_t tokens, uint64_t requested, uint64_t reserve,
                   bool commit_critical);

  mutable std::mutex mutex_;
  Bucket sequential_read_;
  Bucket random_read_;
  Bucket write_;
  Bucket metadata_;
  IoTokenSnapshot critical_reserve_;
  uint64_t last_replenish_ns_ = 0;
};

}  // namespace cedar

#endif  // CEDAR_RUNTIME_IO_GOVERNOR_H_
