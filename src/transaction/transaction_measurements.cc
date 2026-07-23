// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/transaction/transaction_measurements.h"

#include <array>
#include <utility>

namespace cedar {
namespace {

constexpr std::array<uint64_t, 12> kLatencyBoundsNs = {
    1'000, 5'000, 10'000, 50'000, 100'000, 500'000,
    1'000'000, 5'000'000, 10'000'000, 50'000'000, 100'000'000,
    1'000'000'000};

StatusOr<uint64_t> Delta(uint64_t before, uint64_t after,
                         const char* field) {
  if (after < before) {
    return Status::Corruption("transaction measurements",
                              std::string("counter regressed: ") + field);
  }
  return after - before;
}

StatusOr<TransactionMeasurementDistribution> DistributionDelta(
    const Histogram& before, const Histogram& after) {
  const auto difference = after.DifferenceFrom(before);
  if (!difference.ok()) return difference.status();
  const Histogram& histogram = difference.ValueOrDie();
  TransactionMeasurementDistribution distribution;
  distribution.sample_count = histogram.count();
  if (distribution.sample_count == 0) return distribution;
  distribution.defined = true;
  distribution.min_ns = histogram.min();
  distribution.p50_ns = histogram.Quantile(0.50);
  distribution.p95_ns = histogram.Quantile(0.95);
  distribution.p99_ns = histogram.Quantile(0.99);
  distribution.p999_ns = histogram.Quantile(0.999);
  distribution.max_ns = histogram.max();
  distribution.sum_ns = histogram.sum();
  return distribution;
}

}  // namespace

const std::vector<uint64_t>& TransactionMeasurementHistogramBounds() {
  static const std::vector<uint64_t> bounds(kLatencyBoundsNs.begin(),
                                             kLatencyBoundsNs.end());
  return bounds;
}

TransactionMeasurementSnapshot::TransactionMeasurementSnapshot()
    : commit_latency(TransactionMeasurementHistogramBounds()),
      prepare_latency(TransactionMeasurementHistogramBounds()),
      decision_latency(TransactionMeasurementHistogramBounds()),
      decision_fsync_latency(TransactionMeasurementHistogramBounds()),
      visible_prefix_wait_success(TransactionMeasurementHistogramBounds()),
      visible_prefix_wait_failure(TransactionMeasurementHistogramBounds()) {}

StatusOr<TransactionMeasurementWindow> BuildTransactionMeasurementWindow(
    const TransactionMeasurementSnapshot& before,
    const TransactionMeasurementSnapshot& after) {
  TransactionMeasurementWindow window;
  if (!before.available || !after.available) {
    window.available = false;
    window.availability_reason = after.available
        ? before.availability_reason : after.availability_reason;
    if (window.availability_reason.empty()) {
      window.availability_reason = "transaction_measurements_unavailable";
    }
    return window;
  }
#define CEDAR_ASSIGN_DELTA(field)                                      \
  do {                                                                 \
    const auto delta = Delta(before.field, after.field, #field);      \
    if (!delta.ok()) return delta.status();                            \
    window.field = delta.ValueOrDie();                                 \
  } while (false)
  CEDAR_ASSIGN_DELTA(started);
  CEDAR_ASSIGN_DELTA(committed);
  CEDAR_ASSIGN_DELTA(aborted);
  CEDAR_ASSIGN_DELTA(indeterminate);
  CEDAR_ASSIGN_DELTA(conflicts);
  CEDAR_ASSIGN_DELTA(visible_prefix_nonzero_stalls);
#undef CEDAR_ASSIGN_DELTA
  const auto lag_samples = Delta(before.visible_prefix_lag_sample_count,
                                 after.visible_prefix_lag_sample_count,
                                 "visible_prefix_lag_sample_count");
  if (!lag_samples.ok()) return lag_samples.status();
  if (lag_samples.ValueOrDie() != 0 &&
      lag_samples.ValueOrDie() <= kTransactionMeasurementLagSampleCapacity) {
    window.visible_prefix_max_lag_defined = true;
    for (uint64_t index = before.visible_prefix_lag_sample_count;
         index < after.visible_prefix_lag_sample_count; ++index) {
      window.visible_prefix_max_lag_seq = std::max(
          window.visible_prefix_max_lag_seq,
          after.visible_prefix_lag_samples[
              index % kTransactionMeasurementLagSampleCapacity]);
    }
  }
  window.conflict_abort_rate.numerator = window.conflicts;
  window.conflict_abort_rate.denominator = window.aborted;
  window.conflict_abort_rate.defined = window.aborted != 0;

#define CEDAR_ASSIGN_DISTRIBUTION(field)                              \
  do {                                                                 \
    const auto distribution = DistributionDelta(before.field, after.field); \
    if (!distribution.ok()) return distribution.status();              \
    window.field = distribution.ValueOrDie();                           \
  } while (false)
  CEDAR_ASSIGN_DISTRIBUTION(commit_latency);
  CEDAR_ASSIGN_DISTRIBUTION(prepare_latency);
  CEDAR_ASSIGN_DISTRIBUTION(decision_latency);
  CEDAR_ASSIGN_DISTRIBUTION(decision_fsync_latency);
  CEDAR_ASSIGN_DISTRIBUTION(visible_prefix_wait_success);
  CEDAR_ASSIGN_DISTRIBUTION(visible_prefix_wait_failure);
#undef CEDAR_ASSIGN_DISTRIBUTION
  return window;
}

}  // namespace cedar
