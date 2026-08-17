// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/optimizer/runtime_feedback.h"

#include <algorithm>
#include <limits>

namespace cedar {
namespace {

uint64_t SaturatingAdd(uint64_t left, uint64_t right) {
  return left > std::numeric_limits<uint64_t>::max() - right
      ? std::numeric_limits<uint64_t>::max()
      : left + right;
}

uint64_t RoundedMean(uint64_t total, uint64_t count) {
  if (count == 0) return 0;
  uint64_t mean = total / count;
  const uint64_t remainder = total % count;
  if (remainder >= count / 2 + count % 2 &&
      mean != std::numeric_limits<uint64_t>::max()) {
    ++mean;
  }
  return mean;
}

uint64_t BlendRows(uint64_t feedback_rows, uint64_t static_rows,
                   uint32_t confidence_per_mille) {
  const unsigned __int128 weighted =
      static_cast<unsigned __int128>(feedback_rows) *
          confidence_per_mille +
      static_cast<unsigned __int128>(static_rows) *
          (1000U - confidence_per_mille);
  return static_cast<uint64_t>((weighted + 500U) / 1000U);
}

}  // namespace

SelectivityBucket ClassifySelectivity(uint64_t candidate_rows,
                                      uint64_t base_rows) {
  if (base_rows == 0) return SelectivityBucket::kNonSelective;
  if (candidate_rows <= base_rows / 100) {
    return SelectivityBucket::kVerySelective;
  }
  if (candidate_rows <= base_rows / 5) {
    return SelectivityBucket::kModerate;
  }
  return SelectivityBucket::kNonSelective;
}

RuntimeFeedbackStore::RuntimeFeedbackStore(
    size_t capacity, RuntimeFeedbackPolicy policy)
    : capacity_(capacity),
      policy_(RuntimeFeedbackPolicy{
          std::max<uint64_t>(1, policy.minimum_observations),
          std::max<uint64_t>(1, policy.decay_interval_epochs),
          std::max<uint64_t>(1, policy.expiry_epochs)}) {}

void RuntimeFeedbackStore::Observe(
    const RuntimeFeedbackKey& key,
    const RuntimeFeedbackObservation& observation) {
  std::lock_guard<std::mutex> lock(mutex_);
  const uint64_t epoch = AdvanceEpochLocked();
  PurgeExpiredLocked();
  if (capacity_ == 0) return;
  auto found = entries_.find(key);
  if (found == entries_.end()) {
    if (entries_.size() >= capacity_) EvictLeastRecentlyUsedLocked();
    found = entries_.emplace(key, Entry{}).first;
  }
  Entry& entry = found->second;
  entry.last_used = ++lru_clock_;
  entry.last_observed_epoch = epoch;
  RuntimeFeedbackAggregate& aggregate = entry.aggregate;
  aggregate.observations = SaturatingAdd(aggregate.observations, 1);
  aggregate.candidate_rows = SaturatingAdd(
      aggregate.candidate_rows, observation.candidate_rows);
  aggregate.survivor_rows = SaturatingAdd(
      aggregate.survivor_rows, observation.survivor_rows);
  aggregate.interval_splits = SaturatingAdd(
      aggregate.interval_splits, observation.interval_splits);
  aggregate.pages_read = SaturatingAdd(
      aggregate.pages_read, observation.pages_read);
  aggregate.blob_reads = SaturatingAdd(
      aggregate.blob_reads, observation.blob_reads);
}

std::optional<RuntimeFeedbackAggregate> RuntimeFeedbackStore::Lookup(
    const RuntimeFeedbackKey& key) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto found = entries_.find(key);
  if (found == entries_.end()) return std::nullopt;
  if (epoch_ - found->second.last_observed_epoch >= policy_.expiry_epochs) {
    entries_.erase(found);
    return std::nullopt;
  }
  found->second.last_used = ++lru_clock_;
  RuntimeFeedbackAggregate aggregate = found->second.aggregate;
  aggregate.confidence_per_mille = ConfidencePerMilleLocked(found->second);
  return aggregate;
}

ScanCostEstimate RuntimeFeedbackStore::ApplyToEstimate(
    const RuntimeFeedbackKey& key, const ScanCostEstimate& estimate) const {
  ScanCostEstimate corrected = estimate;
  std::lock_guard<std::mutex> lock(mutex_);
  AdvanceEpochLocked();
  PurgeExpiredLocked();
  auto found = entries_.find(key);
  if (found == entries_.end()) return corrected;
  found->second.last_used = ++lru_clock_;
  const uint32_t confidence = ConfidencePerMilleLocked(found->second);
  if (confidence == 0) return corrected;
  uint64_t feedback_rows = RoundedMean(
      found->second.aggregate.candidate_rows,
      found->second.aggregate.observations);
  if (corrected.base_rows != 0) {
    feedback_rows = std::min(feedback_rows, corrected.base_rows);
  }
  corrected.index_candidate_rows = BlendRows(
      feedback_rows, corrected.index_candidate_rows, confidence);
  if (corrected.base_rows != 0) {
    corrected.index_candidate_rows = std::min(
        corrected.index_candidate_rows, corrected.base_rows);
  }
  corrected.feedback_applied = true;
  return corrected;
}

size_t RuntimeFeedbackStore::size() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return entries_.size();
}

uint64_t RuntimeFeedbackStore::AdvanceEpochLocked() const {
  if (epoch_ != std::numeric_limits<uint64_t>::max()) ++epoch_;
  return epoch_;
}

uint32_t RuntimeFeedbackStore::ConfidencePerMilleLocked(
    const Entry& entry) const {
  if (entry.aggregate.observations < policy_.minimum_observations) return 0;
  const uint64_t age = epoch_ - entry.last_observed_epoch;
  if (age >= policy_.expiry_epochs) return 0;
  const uint64_t decay_periods = age / policy_.decay_interval_epochs;
  if (decay_periods >= 10) return 0;
  return static_cast<uint32_t>(1000U >> decay_periods);
}

void RuntimeFeedbackStore::PurgeExpiredLocked() const {
  for (auto current = entries_.begin(); current != entries_.end();) {
    if (epoch_ - current->second.last_observed_epoch >=
        policy_.expiry_epochs) {
      current = entries_.erase(current);
    } else {
      ++current;
    }
  }
}

void RuntimeFeedbackStore::EvictLeastRecentlyUsedLocked() {
  if (entries_.empty()) return;
  auto victim = entries_.begin();
  for (auto current = std::next(entries_.begin()); current != entries_.end();
       ++current) {
    if (current->second.last_used < victim->second.last_used) victim = current;
  }
  entries_.erase(victim);
}

}  // namespace cedar
