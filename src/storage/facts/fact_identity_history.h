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
  // Bits per identity below which a populated filter is not worth its 3-probe
  // false-positive rate; 8 bits/identity is the classic ~3% point, and the
  // filter budget is a fixed 3/8 of validation_cache_bytes, so a database with
  // many more identities than this capacity gains nothing from a rebuild.
  static constexpr uint64_t kUsefulBitsPerIdentity = 8;

  explicit FactIdentityHistory(size_t max_bytes);

  void Publish(const FactRef& ref);
  bool MightContain(const FactRef& ref) const;

  // True once the filter has been put into its always-true state. Such a filter
  // answers "maybe" for every probe and ignores Publish(); it is the safe
  // replacement for a filter that cannot usefully represent the data.
  bool disabled() const { return word_count_ == 0; }

  // Bits the filter actually owns. Zero after Disable().
  size_t bit_count() const { return word_count_ * 64; }

  // Resident bytes the filter holds; zero after Disable().
  size_t resident_bytes() const { return word_count_ * sizeof(uint64_t); }

  // Returns true when a filter built from `max_bytes` can usefully represent
  // `estimated_identities` distinct identities, i.e. it keeps at least
  // kUsefulBitsPerIdentity bits per identity. The estimate is a *key* estimate,
  // and one identity has one key per version, so it can only over-count
  // identities: an over-count can only make this return false (an unnecessary
  // skip), never true when the data would actually overflow the filter.
  static bool CanRepresent(size_t max_bytes, uint64_t estimated_identities);

  // Puts the filter into its always-true state (MightContain returns true for
  // every ref, Publish is a no-op) and releases its words. This is the only
  // safe way to stop using the filter: an empty (zeroed) filter would report
  // false negatives for identities that do exist, and the write path would then
  // skip the canonical read that detects their prior versions and point reads
  // would return no facts for them. Reopen calls this before the store is
  // published, so no reader or writer can race with it.
  void Disable();

 private:
  static uint64_t Mix(uint64_t value);
  static uint64_t IdentityHash(const FactRef& ref);
  // Word count the constructor allocates for `max_bytes`. Shared with
  // CanRepresent() so the capacity decision cannot drift from the allocation.
  static size_t WordCountForBytes(size_t max_bytes);

  std::unique_ptr<std::atomic<uint64_t>[]> words_;
  size_t word_count_ = 0;
};

}  // namespace cedar::internal

#endif  // CEDAR_STORAGE_FACTS_FACT_IDENTITY_HISTORY_H_
