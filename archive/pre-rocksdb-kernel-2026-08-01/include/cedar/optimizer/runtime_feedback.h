// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_OPTIMIZER_RUNTIME_FEEDBACK_H_
#define CEDAR_OPTIMIZER_RUNTIME_FEEDBACK_H_

#include <cstddef>
#include <cstdint>
#include <map>
#include <mutex>
#include <optional>

#include "cedar/optimizer/cost_model.h"

namespace cedar {

enum class SelectivityBucket : uint8_t {
  kVerySelective,
  kModerate,
  kNonSelective,
};

SelectivityBucket ClassifySelectivity(uint64_t candidate_rows,
                                      uint64_t base_rows);

struct RuntimeFeedbackKey {
  uint64_t plan_shape_hash = 0;
  uint64_t schema_epoch_fingerprint = 0;
  uint64_t catalog_generation = 0;
  uint64_t statistics_snapshot_id = 0;
  SelectivityBucket selectivity_bucket = SelectivityBucket::kNonSelective;

  friend bool operator<(const RuntimeFeedbackKey& left,
                        const RuntimeFeedbackKey& right) {
    if (left.plan_shape_hash != right.plan_shape_hash) {
      return left.plan_shape_hash < right.plan_shape_hash;
    }
    if (left.schema_epoch_fingerprint != right.schema_epoch_fingerprint) {
      return left.schema_epoch_fingerprint < right.schema_epoch_fingerprint;
    }
    if (left.catalog_generation != right.catalog_generation) {
      return left.catalog_generation < right.catalog_generation;
    }
    if (left.statistics_snapshot_id != right.statistics_snapshot_id) {
      return left.statistics_snapshot_id < right.statistics_snapshot_id;
    }
    return left.selectivity_bucket < right.selectivity_bucket;
  }
};

struct RuntimeFeedbackObservation {
  uint64_t candidate_rows = 0;
  uint64_t survivor_rows = 0;
  uint64_t interval_splits = 0;
  uint64_t pages_read = 0;
  uint64_t blob_reads = 0;
};

struct RuntimeFeedbackPolicy {
  uint64_t minimum_observations = 2;
  uint64_t decay_interval_epochs = 64;
  uint64_t expiry_epochs = 256;
};

struct RuntimeFeedbackAggregate {
  uint64_t observations = 0;
  uint64_t candidate_rows = 0;
  uint64_t survivor_rows = 0;
  uint64_t interval_splits = 0;
  uint64_t pages_read = 0;
  uint64_t blob_reads = 0;
  uint32_t confidence_per_mille = 0;
};

class RuntimeFeedbackStore {
 public:
  explicit RuntimeFeedbackStore(
      size_t capacity, RuntimeFeedbackPolicy policy = {});

  void Observe(const RuntimeFeedbackKey& key,
               const RuntimeFeedbackObservation& observation);
  std::optional<RuntimeFeedbackAggregate> Lookup(
      const RuntimeFeedbackKey& key) const;
  ScanCostEstimate ApplyToEstimate(const RuntimeFeedbackKey& key,
                                   const ScanCostEstimate& estimate) const;
  size_t size() const;

 private:
  struct Entry {
    RuntimeFeedbackAggregate aggregate;
    uint64_t last_used = 0;
    uint64_t last_observed_epoch = 0;
  };

  uint64_t AdvanceEpochLocked() const;
  uint32_t ConfidencePerMilleLocked(const Entry& entry) const;
  void PurgeExpiredLocked() const;
  void EvictLeastRecentlyUsedLocked();

  const size_t capacity_;
  const RuntimeFeedbackPolicy policy_;
  mutable std::mutex mutex_;
  mutable uint64_t epoch_ = 0;
  mutable uint64_t lru_clock_ = 0;
  mutable std::map<RuntimeFeedbackKey, Entry> entries_;
};

}  // namespace cedar

#endif  // CEDAR_OPTIMIZER_RUNTIME_FEEDBACK_H_
