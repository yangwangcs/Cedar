// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "kernel/epoch_completion.h"

#include <cassert>

namespace cedar::internal {

EpochCompletion::EpochCompletion(size_t request_count)
    : request_count_(request_count) {}

void EpochCompletion::MarkWalDurable() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    wal_durable_ = true;
  }
  completed_.notify_all();
}

void EpochCompletion::Publish(std::vector<CommitResult> results) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    assert(!published_);
    assert(results.size() == request_count_);
    results_ = std::move(results);
    published_ = true;
    ++publication_barrier_count_;
  }
  completed_.notify_all();
}

bool EpochCompletion::WaitForWalDurableOrPublication() const {
  std::unique_lock<std::mutex> lock(mutex_);
  completed_.wait(lock, [this] { return wal_durable_ || published_; });
  return wal_durable_;
}

StatusOr<CommitResult> EpochCompletion::WaitForResult(size_t ordinal) const {
  std::unique_lock<std::mutex> lock(mutex_);
  if (ordinal >= request_count_) {
    return Status::InvalidArgument("async commit", "epoch result ordinal is invalid");
  }
  completed_.wait(lock, [this] { return published_; });
  return results_[ordinal];
}

uint64_t EpochCompletion::publication_barrier_count() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return publication_barrier_count_;
}

}  // namespace cedar::internal
