// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "fact/version_validation_cache.h"

#include <algorithm>

namespace cedar::internal {

VersionValidationCache::VersionValidationCache(size_t max_bytes)
    : slots_(SlotCapacityForBytes(max_bytes)),
      reserved_bytes_(slots_.size() * sizeof(Slot)) {}

size_t VersionValidationCache::SlotCapacityForBytes(size_t max_bytes) {
  const size_t raw_slots = max_bytes / sizeof(Slot);
  if (raw_slots < kSetWays) return 0;
  size_t slots = kSetWays;
  while (slots <= raw_slots / 2) slots <<= 1;
  return slots;
}

VersionValidationCache::Key VersionValidationCache::Identity(const FactRef& ref) {
  return {ref.part_id().value, static_cast<uint8_t>(ref.family()),
          ref.property_id().value, ref.entity_id()};
}

bool VersionValidationCache::InsertBoundary(Chain* chain,
                                            ValidationBoundary boundary) {
  size_t position = 0;
  while (position < chain->size &&
         chain->boundaries[position].valid_from.value > boundary.valid_from.value) {
    ++position;
  }
  if (position < chain->size &&
      chain->boundaries[position].valid_from == boundary.valid_from) {
    chain->boundaries[position].commit_seq.value = std::max(
        chain->boundaries[position].commit_seq.value, boundary.commit_seq.value);
    return true;
  }
  if (chain->size == chain->boundaries.size()) return false;
  for (size_t index = chain->size; index > position; --index) {
    chain->boundaries[index] = chain->boundaries[index - 1];
  }
  chain->boundaries[position] = boundary;
  ++chain->size;
  return true;
}

size_t VersionValidationCache::SetOffset(const Key& key) const {
  const size_t sets = slots_.size() / kSetWays;
  return (FactIdentityHash{}(key) & (sets - 1)) * kSetWays;
}

VersionValidationCache::Slot* VersionValidationCache::FindLocked(const Key& key) {
  if (slots_.empty()) return nullptr;
  const size_t offset = SetOffset(key);
  for (size_t index = 0; index < kSetWays; ++index) {
    Slot& slot = slots_[offset + index];
    if (slot.occupied && slot.key == key) return &slot;
  }
  return nullptr;
}

const VersionValidationCache::Slot* VersionValidationCache::FindLocked(
    const Key& key) const {
  if (slots_.empty()) return nullptr;
  const size_t offset = SetOffset(key);
  for (size_t index = 0; index < kSetWays; ++index) {
    const Slot& slot = slots_[offset + index];
    if (slot.occupied && slot.key == key) return &slot;
  }
  return nullptr;
}

VersionValidationCache::Slot* VersionValidationCache::SelectForPrimeLocked(
    const Key& key) {
  const size_t offset = SetOffset(key);
  Slot* oldest = &slots_[offset];
  for (size_t index = 0; index < kSetWays; ++index) {
    Slot& slot = slots_[offset + index];
    if (!slot.occupied) return &slot;
    if (slot.last_touch < oldest->last_touch) oldest = &slot;
  }
  return oldest;
}

void VersionValidationCache::TouchLocked(Slot* slot) const {
  slot->last_touch = ++touch_clock_;
}

std::optional<std::vector<ValidationBoundary>>
VersionValidationCache::Lookup(const FactRef& ref) const {
  if (!ref.Validate().ok()) return std::nullopt;
  std::lock_guard<std::mutex> lock(mutex_);
  Slot* found =
      const_cast<VersionValidationCache*>(this)->FindLocked(Identity(ref));
  if (found == nullptr) {
    ++misses_;
    return std::nullopt;
  }
  ++hits_;
  TouchLocked(found);
  std::vector<ValidationBoundary> result;
  result.reserve(found->chain.size);
  for (size_t index = 0; index < found->chain.size; ++index) {
    result.push_back(found->chain.boundaries[index]);
  }
  return result;
}

void VersionValidationCache::Prime(
    const FactRef& ref, std::vector<ValidationBoundary> boundaries) {
  if (slots_.empty() || !ref.Validate().ok()) return;
  Chain chain;
  for (const ValidationBoundary& boundary : boundaries) {
    if (boundary.commit_seq.value == 0) continue;
    if (!InsertBoundary(&chain, boundary)) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (Slot* existing = FindLocked(Identity(ref)); existing != nullptr) {
        existing->occupied = false;
        --resident_chains_;
      }
      return;
    }
  }

  std::lock_guard<std::mutex> lock(mutex_);
  const Key key = Identity(ref);
  Slot* slot = FindLocked(key);
  if (slot == nullptr) {
    slot = SelectForPrimeLocked(key);
    if (!slot->occupied) {
      ++resident_chains_;
    }
  }
  slot->key = key;
  slot->chain = std::move(chain);
  slot->occupied = true;
  TouchLocked(slot);
}

void VersionValidationCache::Publish(const PendingFactMutation& mutation,
                                     CommitSeq commit_seq) {
  if (commit_seq.value == 0 || !mutation.ref.Validate().ok()) return;
  std::lock_guard<std::mutex> lock(mutex_);
  Slot* slot = FindLocked(Identity(mutation.ref));
  if (slot == nullptr) return;
  if (!InsertBoundary(&slot->chain,
                      ValidationBoundary{mutation.valid_from, commit_seq})) {
    slot->occupied = false;
    --resident_chains_;
    return;
  }
  TouchLocked(slot);
}

size_t VersionValidationCache::resident_chains() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return resident_chains_;
}

size_t VersionValidationCache::resident_bytes() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return resident_chains_ * sizeof(Slot);
}

VersionValidationCache::Metrics VersionValidationCache::metrics() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return Metrics{hits_, misses_};
}

}  // namespace cedar::internal
