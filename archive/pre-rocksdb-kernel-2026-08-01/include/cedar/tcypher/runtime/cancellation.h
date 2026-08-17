// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TCYPHER_RUNTIME_CANCELLATION_H_
#define CEDAR_TCYPHER_RUNTIME_CANCELLATION_H_

#include <atomic>
#include <memory>

namespace cedar {

class QueryCancellation {
 public:
  QueryCancellation() = default;
  explicit QueryCancellation(std::shared_ptr<QueryCancellation> parent)
      : parent_(std::move(parent)) {}
  void Cancel() { cancelled_.store(true, std::memory_order_release); }
  bool IsCancelled() const {
    return cancelled_.load(std::memory_order_acquire) ||
           (parent_ && parent_->IsCancelled());
  }

 private:
  std::atomic<bool> cancelled_{false};
  std::shared_ptr<QueryCancellation> parent_;
};

}  // namespace cedar

#endif  // CEDAR_TCYPHER_RUNTIME_CANCELLATION_H_
