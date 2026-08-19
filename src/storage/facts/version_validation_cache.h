// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_FACT_VERSION_VALIDATION_CACHE_H_
#define CEDAR_FACT_VERSION_VALIDATION_CACHE_H_

#include <array>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <vector>

#include "storage/facts/fact_store.h"
#include "storage/facts/group_commit_planner.h"

namespace cedar::internal {

struct ValidationBoundary {
  ValidTime valid_from;
  CommitSeq commit_seq;
};

// Bounded, derived validation state. A miss is never interpreted as absence:
// callers must use the canonical RocksDB path and may then Prime this cache.
class VersionValidationCache {
 public:
  // A resident chain is intentionally small. Longer histories keep their
  // canonical RocksDB representation and never enter this derived cache.
  static constexpr size_t kMaxBoundariesPerChain = 8;

  struct Metrics {
    uint64_t hits = 0;
    uint64_t misses = 0;
  };

  explicit VersionValidationCache(size_t max_bytes);

  std::optional<std::vector<ValidationBoundary>> Lookup(
      const FactRef& ref) const;
  void Prime(const FactRef& ref, std::vector<ValidationBoundary> boundaries);
  void Publish(const PendingFactMutation& mutation, CommitSeq commit_seq);

  size_t resident_chains() const;
  size_t resident_bytes() const;
  size_t slot_capacity() const { return slots_.size(); }
  size_t reserved_bytes() const { return reserved_bytes_; }
  Metrics metrics() const;

 private:
  struct Chain {
    // Descending valid-time order matches the facts key order. This packs an
    // entire hot chain into one allocation owned by its cache entry.
    std::array<ValidationBoundary, kMaxBoundariesPerChain> boundaries{};
    size_t size = 0;
  };

  using Key = FactIdentity;

  struct Slot {
    Key key{};
    Chain chain;
    uint64_t last_touch = 0;
    bool occupied = false;
  };

  static constexpr size_t kSetWays = 4;

  static size_t SlotCapacityForBytes(size_t max_bytes);
  static Key Identity(const FactRef& ref);
  static bool InsertBoundary(Chain* chain, ValidationBoundary boundary);
  size_t SetOffset(const Key& key) const;
  Slot* FindLocked(const Key& key);
  const Slot* FindLocked(const Key& key) const;
  Slot* SelectForPrimeLocked(const Key& key);
  void TouchLocked(Slot* slot) const;

  mutable std::mutex mutex_;
  mutable std::vector<Slot> slots_;
  const size_t reserved_bytes_;
  mutable uint64_t touch_clock_ = 0;
  mutable size_t resident_chains_ = 0;
  mutable uint64_t hits_ = 0;
  mutable uint64_t misses_ = 0;
};

}  // namespace cedar::internal

#endif  // CEDAR_FACT_VERSION_VALIDATION_CACHE_H_
