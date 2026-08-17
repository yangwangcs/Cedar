// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#include "cedar/transaction/visible_prefix.h"

namespace cedar {

void VisiblePrefix::RestorePersistedPrefix(uint64_t commit_seq) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (commit_seq > visible_seq_) visible_seq_ = commit_seq;
  while (!installed_ahead_.empty() && *installed_ahead_.begin() <= visible_seq_) {
    installed_ahead_.erase(installed_ahead_.begin());
  }
  while (installed_ahead_.erase(visible_seq_ + 1) != 0) ++visible_seq_;
}

void VisiblePrefix::MarkInstalled(uint64_t commit_seq) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (commit_seq <= visible_seq_) return;
  installed_ahead_.insert(commit_seq);
  while (installed_ahead_.erase(visible_seq_ + 1) != 0) ++visible_seq_;
}

uint64_t VisiblePrefix::visible_seq() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return visible_seq_;
}

}  // namespace cedar
