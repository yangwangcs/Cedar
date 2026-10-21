// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "storage/facts/fact_identity_history.h"

#include <algorithm>

namespace cedar::internal {
namespace {

uint64_t MixHash(uint64_t value) {
  value ^= value >> 30;
  value *= 0xbf58476d1ce4e5b9ULL;
  value ^= value >> 27;
  value *= 0x94d049bb133111ebULL;
  return value ^ (value >> 31);
}

}  // namespace

size_t FactIdentityHistory::WordCountForBytes(size_t max_bytes) {
  size_t words = std::max<size_t>(1, max_bytes / sizeof(uint64_t));
  // Keep the mask a power-of-two friendly size without growing the budget.
  size_t power = 1;
  while (power <= words / 2) power <<= 1;
  return power;
}

FactIdentityHistory::FactIdentityHistory(size_t max_bytes)
    : word_count_(WordCountForBytes(max_bytes)) {
  words_ = std::make_unique<std::atomic<uint64_t>[]>(word_count_);
  for (size_t index = 0; index < word_count_; ++index) {
    words_[index].store(0, std::memory_order_relaxed);
  }
}

bool FactIdentityHistory::CanRepresent(size_t max_bytes,
                                       uint64_t estimated_identities) {
  const uint64_t bits =
      static_cast<uint64_t>(WordCountForBytes(max_bytes)) * 64;
  return estimated_identities <= bits / kUsefulBitsPerIdentity;
}

void FactIdentityHistory::Disable() {
  word_count_ = 0;
  words_.reset();
}

uint64_t FactIdentityHistory::Mix(uint64_t value) { return MixHash(value); }

uint64_t FactIdentityHistory::IdentityHash(const FactRef& ref) {
  uint64_t hash = static_cast<uint64_t>(ref.part_id().value) * 0x9E3779B97F4A7C15ULL;
  hash ^= static_cast<uint64_t>(ref.family()) << 56;
  hash ^= static_cast<uint64_t>(ref.property_id().value) << 40;
  hash ^= Mix(ref.entity_id());
  return Mix(hash);
}

void FactIdentityHistory::Publish(const FactRef& ref) {
  if (word_count_ == 0 || !ref.Validate().ok()) return;
  const uint64_t mask = word_count_ * 64 - 1;
  const uint64_t base = IdentityHash(ref);
  const uint64_t step = Mix(base ^ 0x9e3779b97f4a7c15ULL) | 1ULL;
  for (uint64_t probe = 0; probe != 3; ++probe) {
    const uint64_t bit = (base + probe * step) & mask;
    words_[bit >> 6].fetch_or(1ULL << (bit & 63), std::memory_order_relaxed);
  }
}

bool FactIdentityHistory::MightContain(const FactRef& ref) const {
  if (word_count_ == 0) return true;
  if (!ref.Validate().ok()) return true;
  const uint64_t mask = word_count_ * 64 - 1;
  const uint64_t base = IdentityHash(ref);
  const uint64_t step = Mix(base ^ 0x9e3779b97f4a7c15ULL) | 1ULL;
  for (uint64_t probe = 0; probe != 3; ++probe) {
    const uint64_t bit = (base + probe * step) & mask;
    if ((words_[bit >> 6].load(std::memory_order_relaxed) &
         (1ULL << (bit & 63))) == 0) {
      return false;
    }
  }
  return true;
}

}  // namespace cedar::internal
