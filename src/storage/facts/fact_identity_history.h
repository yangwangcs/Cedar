// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_STORAGE_FACTS_FACT_IDENTITY_HISTORY_H_
#define CEDAR_STORAGE_FACTS_FACT_IDENTITY_HISTORY_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "cedar/fact/fact.h"

namespace cedar::internal {

// Fixed-size Bloom filter over every fact identity ever written. It answers
// "has this identity ever existed" with no false negatives, which lets the
// write path skip canonical neighborhood reads for facts that were never
// written. False positives only fall back to the canonical read.
class FactIdentityHistory {
 public:
  explicit FactIdentityHistory(size_t max_bytes);

  void Publish(const FactRef& ref);
  bool MightContain(const FactRef& ref) const;

  size_t resident_bytes() const { return word_count_ * sizeof(uint64_t); }

 private:
  static uint64_t Mix(uint64_t value);
  static uint64_t IdentityHash(const FactRef& ref);

  std::unique_ptr<std::atomic<uint64_t>[]> words_;
  size_t word_count_ = 0;
};

}  // namespace cedar::internal

#endif  // CEDAR_STORAGE_FACTS_FACT_IDENTITY_HISTORY_H_
