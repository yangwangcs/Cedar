// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TRANSACTION_TRANSACTION_MEASUREMENTS_H_
#define CEDAR_TRANSACTION_TRANSACTION_MEASUREMENTS_H_

#include <array>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/observability/histogram.h"

namespace cedar {

enum class TransactionMeasurementMode : uint8_t { kSnapshot = 0, kStrict = 1 };
enum class TransactionMeasurementOutcome : uint8_t {
  kCommitted = 0,
  kAborted = 1,
  kIndeterminate = 2,
  kSucceeded = 3,
  kFailed = 4,
};
enum class TransactionMeasurementKind : uint8_t {
  kStarted = 0,
  kTerminal = 1,
  kPrepareLatency = 2,
  kDecisionLatency = 3,
  kDecisionFsyncLatency = 4,
  kVisiblePrefixWait = 5,
};

struct TransactionMeasurementEvent {
  TransactionMeasurementKind kind = TransactionMeasurementKind::kTerminal;
  TransactionMeasurementMode mode = TransactionMeasurementMode::kSnapshot;
  TransactionMeasurementOutcome outcome = TransactionMeasurementOutcome::kAborted;
  std::string reason;
  uint64_t duration_ns = 0;
  uint64_t lag_seq = 0;
  bool nonzero_stall = false;
};

using TransactionMeasurementSink =
    std::function<void(const TransactionMeasurementEvent&)>;

const std::vector<uint64_t>& TransactionMeasurementHistogramBounds();
constexpr size_t kTransactionMeasurementLagSampleCapacity = 1024;

struct TransactionMeasurementDistribution {
  bool defined = false;
  uint64_t sample_count = 0;
  uint64_t min_ns = 0;
  uint64_t p50_ns = 0;
  uint64_t p95_ns = 0;
  uint64_t p99_ns = 0;
  uint64_t p999_ns = 0;
  uint64_t max_ns = 0;
  uint64_t sum_ns = 0;
};

struct TransactionMeasurementSnapshot {
  TransactionMeasurementSnapshot();

  bool available = true;
  std::string availability_reason;
  uint64_t started = 0;
  uint64_t committed = 0;
  uint64_t aborted = 0;
  uint64_t indeterminate = 0;
  uint64_t conflicts = 0;
  uint64_t visible_prefix_nonzero_stalls = 0;
  uint64_t visible_prefix_lag_sample_count = 0;
  std::array<uint64_t, kTransactionMeasurementLagSampleCapacity>
      visible_prefix_lag_samples{};
  Histogram commit_latency;
  Histogram prepare_latency;
  Histogram decision_latency;
  Histogram decision_fsync_latency;
  Histogram visible_prefix_wait_success;
  Histogram visible_prefix_wait_failure;
};

struct TransactionMeasurementRatio {
  bool defined = false;
  uint64_t numerator = 0;
  uint64_t denominator = 0;
};

struct TransactionMeasurementWindow {
  bool available = true;
  std::string availability_reason;
  uint64_t started = 0;
  uint64_t committed = 0;
  uint64_t aborted = 0;
  uint64_t indeterminate = 0;
  uint64_t conflicts = 0;
  uint64_t visible_prefix_nonzero_stalls = 0;
  bool visible_prefix_max_lag_defined = false;
  uint64_t visible_prefix_max_lag_seq = 0;
  TransactionMeasurementRatio conflict_abort_rate;
  TransactionMeasurementDistribution commit_latency;
  TransactionMeasurementDistribution prepare_latency;
  TransactionMeasurementDistribution decision_latency;
  TransactionMeasurementDistribution decision_fsync_latency;
  TransactionMeasurementDistribution visible_prefix_wait_success;
  TransactionMeasurementDistribution visible_prefix_wait_failure;
};

StatusOr<TransactionMeasurementWindow> BuildTransactionMeasurementWindow(
    const TransactionMeasurementSnapshot& before,
    const TransactionMeasurementSnapshot& after);

}  // namespace cedar

#endif  // CEDAR_TRANSACTION_TRANSACTION_MEASUREMENTS_H_
