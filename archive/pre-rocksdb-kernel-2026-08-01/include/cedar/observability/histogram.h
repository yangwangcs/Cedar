// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_OBSERVABILITY_HISTOGRAM_H_
#define CEDAR_OBSERVABILITY_HISTOGRAM_H_

#include <algorithm>
#include <cstdint>
#include <limits>
#include <utility>
#include <vector>

#include "cedar/core/status.h"

namespace cedar {

// Bounded, mergeable histogram. Bounds describe the lower edge of each bucket
// after the first; values equal to a bound enter that bound's bucket.
class Histogram {
 public:
  Histogram() : Histogram(std::vector<uint64_t>{}) {}

  explicit Histogram(std::vector<uint64_t> bounds)
      : bounds_(std::move(bounds)), counts_(bounds_.size() + 1, 0) {
    std::sort(bounds_.begin(), bounds_.end());
    bounds_.erase(std::unique(bounds_.begin(), bounds_.end()), bounds_.end());
    counts_.assign(bounds_.size() + 1, 0);
  }

  void Observe(uint64_t value) {
    const size_t bucket = static_cast<size_t>(
        std::upper_bound(bounds_.begin(), bounds_.end(), value) - bounds_.begin());
    if (counts_[bucket] != UINT64_MAX) ++counts_[bucket];
    if (count_ != UINT64_MAX) ++count_;
    sum_ = value > UINT64_MAX - sum_ ? UINT64_MAX : sum_ + value;
    if (value < min_) min_ = value;
    if (value > max_) max_ = value;
  }

  Status Merge(const Histogram& other) {
    if (bounds_ != other.bounds_) return Status::InvalidArgument("histogram", "bucket bounds differ");
    for (size_t index = 0; index < counts_.size(); ++index) {
      counts_[index] = other.counts_[index] > UINT64_MAX - counts_[index]
          ? UINT64_MAX : counts_[index] + other.counts_[index];
    }
    count_ = other.count_ > UINT64_MAX - count_ ? UINT64_MAX : count_ + other.count_;
    sum_ = other.sum_ > UINT64_MAX - sum_ ? UINT64_MAX : sum_ + other.sum_;
    min_ = min_ < other.min_ ? min_ : other.min_;
    max_ = max_ > other.max_ ? max_ : other.max_;
    return Status::OK();
  }

  StatusOr<Histogram> DifferenceFrom(const Histogram& before) const {
    if (bounds_ != before.bounds_) {
      return Status::InvalidArgument("histogram", "bucket bounds differ");
    }
    if (count_ < before.count_ || sum_ < before.sum_) {
      return Status::Corruption("histogram", "cumulative value regressed");
    }
    Histogram result(bounds_);
    for (size_t index = 0; index < counts_.size(); ++index) {
      if (counts_[index] < before.counts_[index]) {
        return Status::Corruption("histogram", "bucket count regressed");
      }
      result.counts_[index] = counts_[index] - before.counts_[index];
    }
    result.count_ = count_ - before.count_;
    result.sum_ = sum_ - before.sum_;
    uint64_t bucket_total = 0;
    size_t first = result.counts_.size();
    size_t last = 0;
    for (size_t index = 0; index < result.counts_.size(); ++index) {
      bucket_total = result.counts_[index] > UINT64_MAX - bucket_total
          ? UINT64_MAX : bucket_total + result.counts_[index];
      if (result.counts_[index] != 0) {
        first = std::min(first, index);
        last = index;
      }
    }
    if (bucket_total != result.count_) {
      return Status::Corruption("histogram", "bucket total mismatches count");
    }
    if (result.count_ != 0) {
      result.min_ = first == 0 ? 0 : bounds_[first - 1];
      result.max_ = last >= bounds_.size() ? bounds_.back() : bounds_[last];
    }
    return result;
  }

  uint64_t count() const { return count_; }
  uint64_t sum() const { return sum_; }
  uint64_t min() const { return count_ == 0 ? 0 : min_; }
  uint64_t max() const { return count_ == 0 ? 0 : max_; }
  const std::vector<uint64_t>& bounds() const { return bounds_; }
  const std::vector<uint64_t>& bucket_counts() const { return counts_; }

  // Bucket upper approximation, sufficient for bounded operational telemetry.
  uint64_t Quantile(double quantile) const {
    if (count_ == 0) return 0;
    if (quantile <= 0) return min();
    if (quantile >= 1) return max();
    const uint64_t target = static_cast<uint64_t>(quantile * (count_ - 1));
    uint64_t cumulative = 0;
    for (size_t index = 0; index < counts_.size(); ++index) {
      cumulative += counts_[index];
      if (cumulative > target) return index == 0 ? bounds_.empty() ? max() : bounds_.front()
          : index == bounds_.size() ? max() : bounds_[index];
    }
    return max();
  }

 private:
  std::vector<uint64_t> bounds_;
  std::vector<uint64_t> counts_;
  uint64_t count_ = 0;
  uint64_t sum_ = 0;
  uint64_t min_ = std::numeric_limits<uint64_t>::max();
  uint64_t max_ = 0;
};

}  // namespace cedar

#endif  // CEDAR_OBSERVABILITY_HISTOGRAM_H_
