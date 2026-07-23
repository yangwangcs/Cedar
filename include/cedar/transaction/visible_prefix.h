// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#ifndef CEDAR_TRANSACTION_VISIBLE_PREFIX_H_
#define CEDAR_TRANSACTION_VISIBLE_PREFIX_H_

#include <cstdint>
#include <mutex>
#include <set>

namespace cedar {

// Advances only across a complete installed commit sequence. It is the single
// source for transaction and analytical snapshot cutoffs.
class VisiblePrefix {
 public:
  // Restores a Manifest-published prefix whose events are already represented
  // by checked SSTs.  It is used before replaying the retained DecisionLog.
  void RestorePersistedPrefix(uint64_t commit_seq);
  void MarkInstalled(uint64_t commit_seq);
  uint64_t visible_seq() const;

 private:
  mutable std::mutex mutex_;
  uint64_t visible_seq_ = 0;
  std::set<uint64_t> installed_ahead_;
};

}  // namespace cedar

#endif  // CEDAR_TRANSACTION_VISIBLE_PREFIX_H_
