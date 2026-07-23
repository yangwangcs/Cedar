// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TRANSACTION_LOGICAL_ID_ALLOCATOR_H_
#define CEDAR_TRANSACTION_LOGICAL_ID_ALLOCATOR_H_

#include <cstdint>
#include <mutex>
#include <string>

#include "cedar/core/status.h"

namespace cedar {

// Persistent monotonic logical identity allocator. Gaps are permitted after a
// failed statement; reusing an identity after it has been returned is not.
class LogicalIdAllocator {
 public:
  explicit LogicalIdAllocator(std::string checkpoint_path)
      : checkpoint_path_(std::move(checkpoint_path)) {}

  Status Open();
  StatusOr<uint64_t> Allocate();

 private:
  Status PersistLocked(uint64_t next_id) const;

  std::string checkpoint_path_;
  mutable std::mutex mutex_;
  uint64_t next_id_ = 1;
  bool opened_ = false;
};

}  // namespace cedar

#endif  // CEDAR_TRANSACTION_LOGICAL_ID_ALLOCATOR_H_
