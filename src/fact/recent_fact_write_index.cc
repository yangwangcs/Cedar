// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "fact/recent_fact_write_index.h"

#include <algorithm>
#include <limits>

namespace cedar::internal {

size_t RecentFactWriteIndex::WordsPerFilter(size_t max_bytes) {
  const size_t raw_words = max_bytes / kFilterCount / sizeof(uint64_t);
  if (raw_words == 0) return 0;
  size_t words = 1;
  while (words <= raw_words / 2) words <<= 1;
  return words;
}

RecentFactWriteIndex::RecentFactWriteIndex(size_t max_bytes,
                                           CommitSeq coverage_start_seq,
                                           uint64_t window_sequences)
    : window_sequences_(std::max<uint64_t>(1, window_sequences)),
      window_anchor_sequence_(coverage_start_seq.value ==
                                      std::numeric_limits<uint64_t>::max()
                                  ? coverage_start_seq.value
                                  : coverage_start_seq.value + 1),
      resident_bytes_(WordsPerFilter(max_bytes) * kFilterCount *
                      sizeof(uint64_t)),
      coverage_start_seq_(coverage_start_seq.value),
      latest_published_seq_(coverage_start_seq.value) {
  const size_t words = WordsPerFilter(max_bytes);
  if (words == 0) return;
  filters_.resize(kFilterCount);
  for (Filter& filter : filters_) filter.words.resize(words);
}

FactIdentity RecentFactWriteIndex::Identity(const FactRef& ref) {
  return {ref.part_id().value, static_cast<uint8_t>(ref.family()),
          ref.property_id().value, ref.entity_id()};
}

uint64_t RecentFactWriteIndex::Mix(uint64_t value) {
  value ^= value >> 30;
  value *= 0xbf58476d1ce4e5b9ULL;
  value ^= value >> 27;
  value *= 0x94d049bb133111ebULL;
  return value ^ (value >> 31);
}

uint64_t RecentFactWriteIndex::WindowStart(uint64_t sequence) const {
  if (sequence <= window_anchor_sequence_) return window_anchor_sequence_;
  const uint64_t offset = sequence - window_anchor_sequence_;
  const uint64_t quotient = offset / window_sequences_;
  if (quotient >
      (std::numeric_limits<uint64_t>::max() - window_anchor_sequence_) /
          window_sequences_) {
    return std::numeric_limits<uint64_t>::max();
  }
  return window_anchor_sequence_ + quotient * window_sequences_;
}

RecentFactWriteIndex::Filter* RecentFactWriteIndex::FindFilter(
    uint64_t sequence) {
  for (Filter& filter : filters_) {
    if (filter.active && sequence >= filter.first_sequence &&
        sequence <= filter.last_sequence) {
      return &filter;
    }
  }
  return nullptr;
}

const RecentFactWriteIndex::Filter* RecentFactWriteIndex::FindFilter(
    uint64_t sequence) const {
  for (const Filter& filter : filters_) {
    if (filter.active && sequence >= filter.first_sequence &&
        sequence <= filter.last_sequence) {
      return &filter;
    }
  }
  return nullptr;
}

bool RecentFactWriteIndex::HasActiveFilter() const {
  return std::any_of(filters_.begin(), filters_.end(),
                     [](const Filter& filter) { return filter.active; });
}

void RecentFactWriteIndex::Clear(Filter* filter, uint64_t first_sequence) {
  std::fill(filter->words.begin(), filter->words.end(), 0);
  filter->first_sequence = first_sequence;
  const uint64_t remaining =
      std::numeric_limits<uint64_t>::max() - first_sequence;
  filter->last_sequence =
      remaining < window_sequences_ - 1
          ? std::numeric_limits<uint64_t>::max()
          : first_sequence + window_sequences_ - 1;
  filter->published_through = first_sequence == 0 ? 0 : first_sequence - 1;
  filter->active = true;
}

RecentFactWriteIndex::Filter* RecentFactWriteIndex::ActivateFilter(
    uint64_t first_sequence) {
  for (Filter& filter : filters_) {
    if (!filter.active) {
      Clear(&filter, first_sequence);
      return &filter;
    }
  }
  auto oldest = std::min_element(
      filters_.begin(), filters_.end(), [](const Filter& left, const Filter& right) {
        return left.first_sequence < right.first_sequence;
      });
  coverage_start_seq_ = std::max(coverage_start_seq_, oldest->last_sequence);
  Clear(&*oldest, first_sequence);
  resets_.fetch_add(1, std::memory_order_relaxed);
  return &*oldest;
}

bool RecentFactWriteIndex::MightContain(
    const Filter& filter, const FactIdentity& identity) const {
  if (filter.words.empty()) return true;
  const uint64_t mask = filter.words.size() * 64 - 1;
  const uint64_t base = Mix(FactIdentityHash{}(identity));
  const uint64_t step = Mix(base ^ 0x9e3779b97f4a7c15ULL) | 1ULL;
  for (uint64_t probe = 0; probe != 3; ++probe) {
    const uint64_t bit = (base + probe * step) & mask;
    if ((filter.words[bit >> 6] & (1ULL << (bit & 63))) == 0) return false;
  }
  return true;
}

void RecentFactWriteIndex::Add(Filter* filter,
                               const FactIdentity& identity) {
  const uint64_t mask = filter->words.size() * 64 - 1;
  const uint64_t base = Mix(FactIdentityHash{}(identity));
  const uint64_t step = Mix(base ^ 0x9e3779b97f4a7c15ULL) | 1ULL;
  for (uint64_t probe = 0; probe != 3; ++probe) {
    const uint64_t bit = (base + probe * step) & mask;
    filter->words[bit >> 6] |= 1ULL << (bit & 63);
  }
}

bool RecentFactWriteIndex::CanProveUnchanged(const FactRef& ref,
                                              CommitSeq snapshot_seq) {
  if (filters_.empty() || !ref.Validate().ok() ||
      snapshot_seq.value < coverage_start_seq_) {
    misses_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }
  const FactIdentity identity = Identity(ref);
  for (const Filter& filter : filters_) {
    if (!filter.active || filter.published_through <= snapshot_seq.value) {
      continue;
    }
    if (MightContain(filter, identity)) {
      misses_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }
  }
  hits_.fetch_add(1, std::memory_order_relaxed);
  return true;
}

void RecentFactWriteIndex::Publish(const FactRef& ref, CommitSeq commit_seq) {
  if (filters_.empty() || commit_seq.value == 0 || !ref.Validate().ok()) {
    return;
  }
  if (commit_seq.value < coverage_start_seq_) {
    // Publication is expected to be monotonic. An unexpected older sequence
    // cannot be represented by a retained version window, so preserve safety
    // by dropping all historical proof.
    for (Filter& filter : filters_) {
      std::fill(filter.words.begin(), filter.words.end(), 0);
      filter.active = false;
    }
    coverage_start_seq_ = latest_published_seq_;
    resets_.fetch_add(1, std::memory_order_relaxed);
  }
  Filter* filter = FindFilter(commit_seq.value);
  if (filter == nullptr) {
    filter = ActivateFilter(WindowStart(commit_seq.value));
  }
  Add(filter, Identity(ref));
  filter->published_through =
      std::max(filter->published_through, commit_seq.value);
  latest_published_seq_ = std::max(latest_published_seq_, commit_seq.value);
}

RecentFactWriteIndexMetrics RecentFactWriteIndex::metrics() const {
  return RecentFactWriteIndexMetrics{
      hits_.load(std::memory_order_relaxed),
      misses_.load(std::memory_order_relaxed),
      resets_.load(std::memory_order_relaxed)};
}

}  // namespace cedar::internal
