// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/runtime/io_governor.h"

#include <algorithm>
#include <limits>

namespace cedar {

IoGovernor::IoGovernor(IoGovernorLimits limits, IoTokenSnapshot critical_reserve)
    : sequential_read_{limits.sequential_read_bytes, limits.sequential_read_bytes.capacity},
      random_read_{limits.random_read_ops, limits.random_read_ops.capacity},
      write_{limits.write_bytes, limits.write_bytes.capacity},
      metadata_{limits.metadata_ops, limits.metadata_ops.capacity},
      critical_reserve_(critical_reserve) {}

void IoGovernor::ReplenishBucket(Bucket* bucket, uint64_t elapsed_ns) {
  if (bucket->budget.replenish_per_second == 0 || elapsed_ns == 0) return;
  const uint64_t rate = bucket->budget.replenish_per_second;
  const uint64_t refill = elapsed_ns > std::numeric_limits<uint64_t>::max() / rate
      ? bucket->budget.capacity
      : std::min<uint64_t>(bucket->budget.capacity,
                           (elapsed_ns * rate) / 1000000000ULL);
  bucket->tokens = std::min(bucket->budget.capacity,
                            bucket->tokens > std::numeric_limits<uint64_t>::max() - refill
                                ? bucket->budget.capacity : bucket->tokens + refill);
}

bool IoGovernor::Fits(uint64_t tokens, uint64_t requested, uint64_t reserve,
                      bool commit_critical) {
  const uint64_t protected_tokens = commit_critical ? 0 : reserve;
  return requested <= tokens && tokens - requested >= protected_tokens;
}

void IoGovernor::Replenish(uint64_t monotonic_now_ns) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (last_replenish_ns_ == 0) {
    last_replenish_ns_ = monotonic_now_ns;
    return;
  }
  if (monotonic_now_ns <= last_replenish_ns_) return;
  const uint64_t elapsed = monotonic_now_ns - last_replenish_ns_;
  ReplenishBucket(&sequential_read_, elapsed);
  ReplenishBucket(&random_read_, elapsed);
  ReplenishBucket(&write_, elapsed);
  ReplenishBucket(&metadata_, elapsed);
  last_replenish_ns_ = monotonic_now_ns;
}

Status IoGovernor::TryAcquire(const IoTokenRequest& request, uint64_t monotonic_now_ns) {
  Replenish(monotonic_now_ns);
  std::lock_guard<std::mutex> lock(mutex_);
  if (!Fits(sequential_read_.tokens, request.sequential_read_bytes,
            critical_reserve_.sequential_read_bytes, request.commit_critical) ||
      !Fits(random_read_.tokens, request.random_read_ops, critical_reserve_.random_read_ops,
            request.commit_critical) ||
      !Fits(write_.tokens, request.write_bytes, critical_reserve_.write_bytes,
            request.commit_critical) ||
      !Fits(metadata_.tokens, request.metadata_ops, critical_reserve_.metadata_ops,
            request.commit_critical)) {
    return Status::QueryMemoryLimit("I/O governor", "I/O token budget is exhausted");
  }
  sequential_read_.tokens -= request.sequential_read_bytes;
  random_read_.tokens -= request.random_read_ops;
  write_.tokens -= request.write_bytes;
  metadata_.tokens -= request.metadata_ops;
  return Status::OK();
}

IoTokenSnapshot IoGovernor::available(bool commit_critical) const {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto remaining = [commit_critical](uint64_t tokens, uint64_t reserve) {
    return commit_critical || tokens <= reserve ? (commit_critical ? tokens : 0) : tokens - reserve;
  };
  return IoTokenSnapshot{remaining(sequential_read_.tokens, critical_reserve_.sequential_read_bytes),
                         remaining(random_read_.tokens, critical_reserve_.random_read_ops),
                         remaining(write_.tokens, critical_reserve_.write_bytes),
                         remaining(metadata_.tokens, critical_reserve_.metadata_ops)};
}

}  // namespace cedar
