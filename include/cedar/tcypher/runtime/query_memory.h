// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TCYPHER_RUNTIME_QUERY_MEMORY_H_
#define CEDAR_TCYPHER_RUNTIME_QUERY_MEMORY_H_

#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <utility>

#include "cedar/core/status.h"

namespace cedar {

// Query-local root account. Operators must reserve before allocating or
// pinning buffers; supported blocking operators spill when soft pressure begins.
class QueryMemoryAccount {
 public:
  QueryMemoryAccount(uint64_t soft_limit_bytes, uint64_t hard_limit_bytes)
      : soft_limit_bytes_(soft_limit_bytes), hard_limit_bytes_(hard_limit_bytes) {}

  Status Reserve(uint64_t bytes) {
    uint64_t observed = used_bytes_.load(std::memory_order_relaxed);
    while (true) {
      if (bytes > hard_limit_bytes_ || observed > hard_limit_bytes_ - bytes) {
        return Status::QueryMemoryLimit("query memory", "hard memory limit exceeded");
      }
      if (used_bytes_.compare_exchange_weak(observed, observed + bytes,
                                            std::memory_order_acq_rel,
                                            std::memory_order_relaxed)) {
        const uint64_t current = observed + bytes;
        uint64_t peak = peak_bytes_.load(std::memory_order_relaxed);
        while (peak < current &&
               !peak_bytes_.compare_exchange_weak(
                   peak, current, std::memory_order_release,
                   std::memory_order_relaxed)) {
        }
        return Status::OK();
      }
    }
  }

  void Release(uint64_t bytes) {
    uint64_t observed = used_bytes_.load(std::memory_order_relaxed);
    while (true) {
      const uint64_t released = bytes > observed ? observed : bytes;
      if (used_bytes_.compare_exchange_weak(observed, observed - released,
                                            std::memory_order_acq_rel,
                                            std::memory_order_relaxed)) {
        return;
      }
    }
  }

  uint64_t used_bytes() const { return used_bytes_.load(std::memory_order_acquire); }
  uint64_t peak_bytes() const { return peak_bytes_.load(std::memory_order_acquire); }
  uint64_t soft_limit_bytes() const { return soft_limit_bytes_; }
  uint64_t hard_limit_bytes() const { return hard_limit_bytes_; }
  bool ShouldSpill() const { return used_bytes() >= soft_limit_bytes_; }

 private:
  uint64_t soft_limit_bytes_;
  uint64_t hard_limit_bytes_;
  std::atomic<uint64_t> used_bytes_{0};
  std::atomic<uint64_t> peak_bytes_{0};
};

// Transfers an existing reservation to an output vector or batch. The
// reservation remains charged until the last retained result reference dies.
class QueryMemoryLease {
 public:
  QueryMemoryLease(
      std::shared_ptr<QueryMemoryAccount> account, uint64_t reserved_bytes)
      : account_(std::move(account)), reserved_bytes_(reserved_bytes) {}
  ~QueryMemoryLease() {
    if (account_ && reserved_bytes_ != 0) account_->Release(reserved_bytes_);
  }

  QueryMemoryLease(const QueryMemoryLease&) = delete;
  QueryMemoryLease& operator=(const QueryMemoryLease&) = delete;

  Status ReserveAdditional(uint64_t bytes) {
    if (!account_ || bytes == 0) return Status::OK();
    if (bytes > std::numeric_limits<uint64_t>::max() - reserved_bytes_) {
      return Status::QueryMemoryLimit(
          "query memory", "lease reservation overflow");
    }
    const Status reserved = account_->Reserve(bytes);
    if (!reserved.ok()) return reserved;
    reserved_bytes_ += bytes;
    return Status::OK();
  }

 private:
  std::shared_ptr<QueryMemoryAccount> account_;
  uint64_t reserved_bytes_ = 0;
};

}  // namespace cedar

#endif  // CEDAR_TCYPHER_RUNTIME_QUERY_MEMORY_H_
