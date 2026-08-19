// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_KERNEL_EPOCH_COMPLETION_H_
#define CEDAR_KERNEL_EPOCH_COMPLETION_H_

#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <vector>

#include "cedar/transaction.h"

namespace cedar::internal {

// Shared terminal state for all requests selected into one ordered epoch.
// Result slots retain per-request outcomes, while durability and publication
// each use one broadcast barrier for the whole epoch.
class EpochCompletion {
 public:
  explicit EpochCompletion(size_t request_count);

  EpochCompletion(const EpochCompletion&) = delete;
  EpochCompletion& operator=(const EpochCompletion&) = delete;

  void MarkWalDurable();
  void Publish(std::vector<CommitResult> results);

  // Returns false when publication completed without a durable WAL callback.
  bool WaitForWalDurableOrPublication() const;
  StatusOr<CommitResult> WaitForResult(size_t ordinal) const;

  uint64_t publication_barrier_count() const;

 private:
  mutable std::mutex mutex_;
  mutable std::condition_variable completed_;
  const size_t request_count_;
  std::vector<CommitResult> results_;
  bool wal_durable_ = false;
  bool published_ = false;
  uint64_t publication_barrier_count_ = 0;
};

}  // namespace cedar::internal

#endif  // CEDAR_KERNEL_EPOCH_COMPLETION_H_
