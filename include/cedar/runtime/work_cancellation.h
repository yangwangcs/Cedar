// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_RUNTIME_WORK_CANCELLATION_H_
#define CEDAR_RUNTIME_WORK_CANCELLATION_H_

#include <atomic>
#include <cstdint>
#include <functional>
#include <string>
#include <utility>

#include "cedar/core/status.h"

namespace cedar {

// Task-scoped cooperative cancellation for optional maintenance. This is a
// runtime primitive rather than a query-layer dependency: production
// algorithms observe it only at durability-safe boundaries.
class WorkCancellation {
 public:
  using CheckpointObserverForTesting =
      std::function<void(WorkCancellation*, const std::string&, uint64_t)>;

  bool Cancel() {
    return !cancelled_.exchange(true, std::memory_order_acq_rel);
  }

  bool IsCancelled() const {
    return cancelled_.load(std::memory_order_acquire);
  }

  // Must be configured before the token is shared with a worker. Production
  // tokens leave this unset; tests use it to trigger cancellation at an exact
  // durability-safe boundary without timing-dependent sleeps.
  void SetCheckpointObserverForTesting(
      CheckpointObserverForTesting observer) {
    checkpoint_observer_for_testing_ = std::move(observer);
  }

  Status Checkpoint(const std::string& owner) {
    if (checkpoint_observer_for_testing_) {
      const uint64_t checkpoint =
          checkpoints_.fetch_add(1, std::memory_order_relaxed) + 1;
      checkpoint_observer_for_testing_(this, owner, checkpoint);
    }
    return IsCancelled()
        ? Status::QueryCancelled(owner, "optional maintenance cancelled")
        : Status::OK();
  }

 private:
  std::atomic<bool> cancelled_{false};
  std::atomic<uint64_t> checkpoints_{0};
  CheckpointObserverForTesting checkpoint_observer_for_testing_;
};

}  // namespace cedar

#endif  // CEDAR_RUNTIME_WORK_CANCELLATION_H_
