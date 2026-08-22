// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_OBSERVABILITY_QUERY_METRICS_H_
#define CEDAR_QUERY_OBSERVABILITY_QUERY_METRICS_H_

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/fact/fact.h"
#include "cedar/query/types.h"
#include "cedar/types/value.h"

namespace cedar {
namespace internal {
struct ProjectionManifest;

struct EntityRange {
  uint64_t min = 0;
  uint64_t max_exclusive = 0;
  bool operator==(const EntityRange&) const = default;
};

struct HllSketch {
  uint8_t precision = 14;
  std::vector<uint8_t> registers;
  bool operator==(const HllSketch&) const = default;
};

struct HistogramBucket {
  Value upper_bound = Value::Int64(0);
  uint64_t cumulative_count = 0;
  bool operator==(const HistogramBucket&) const = default;
};

struct TopValue {
  Value value = Value::Int64(0);
  uint64_t estimated_count = 0;
  bool operator==(const TopValue&) const = default;
};

struct QuantilePoint {
  double quantile = 0;
  uint64_t value = 0;
  bool operator==(const QuantilePoint&) const = default;
};
using QuantileSummary = std::vector<QuantilePoint>;

struct QueryColumnStatistics {
  uint64_t rows = 0;
  uint64_t pages = 0;
  uint64_t bytes = 0;
  uint64_t interval_count = 0;
  uint64_t edge_count = 0;
  std::optional<EntityRange> entity_range;
  std::optional<ValidTimeInterval> valid_time_range;
  HllSketch distinct;
  std::vector<HistogramBucket> histogram;
  std::vector<TopValue> top_values;
  QuantileSummary fanout;
  QuantileSummary interval_length;
  bool operator==(const QueryColumnStatistics&) const = default;
};

struct QueryStatisticsSnapshot {
  std::string database_identity;
  std::string schema_fingerprint;
  std::string coverage;
  uint64_t generation_id = 0;
  CommitSeq base_seq;
  std::vector<QueryColumnStatistics> columns;
  uint32_t checksum = 0;
  // Refresh currently produces structural/page estimates only. Such a file
  // is deliberately unavailable to the planner until a complete statistics
  // builder sets this bit.
  bool complete = false;
  bool operator==(const QueryStatisticsSnapshot&) const = default;
};

StatusOr<std::string> EncodeQueryStatistics(const QueryStatisticsSnapshot&);
StatusOr<QueryStatisticsSnapshot> DecodeQueryStatistics(const std::string&);

class QueryStatisticsStore {
 public:
  QueryStatisticsStore(std::string directory, std::string database_identity);
  Status Refresh(const ProjectionManifest& manifest,
                 const std::string& schema_fingerprint = {});
  StatusOr<QueryStatisticsSnapshot> Load(uint64_t generation_id,
                                         CommitSeq base_seq,
                                         const std::string& schema_fingerprint = {}) const;
  static std::string FileName(uint64_t generation_id);

 private:
  std::string directory_;
  std::string database_identity_;
};

enum class QueryMetricOperator : uint8_t {
  kScan = 0, kExpand, kJoin, kFilter, kProject, kAggregate, kSort,
  kCount,
};
enum class QueryMetricTerminal : uint8_t {
  kComplete = 0, kCancelled, kFailed, kCount,
};
enum class QueryMetricFallback : uint8_t {
  kNone = 0, kCanonical, kDelta, kUntrustedStatistics, kCount,
};

struct QueryMetricsSnapshot {
  std::array<uint64_t, static_cast<size_t>(QueryMetricOperator::kCount)> operator_rows{};
  std::array<uint64_t, static_cast<size_t>(QueryMetricTerminal::kCount)> terminal{};
  std::array<uint64_t, static_cast<size_t>(QueryMetricFallback::kCount)> fallback{};
  uint64_t batches = 0;
  uint64_t physical_bytes = 0;
  uint64_t decoded_bytes = 0;
  uint64_t interval_fragments = 0;
  uint64_t spill_bytes = 0;
};

class QueryMetrics {
 public:
  void AddBatch(QueryMetricOperator op, uint64_t rows, uint64_t physical_bytes,
                uint64_t decoded_bytes, uint64_t interval_fragments = 0);
  void AddTerminal(QueryMetricTerminal terminal);
  void AddFallback(QueryMetricFallback fallback);
  void AddSpillBytes(uint64_t bytes);
  QueryMetricsSnapshot Snapshot() const;
  // Labels are fixed Cedar enums. There is intentionally no string-label API:
  // query ids, text, properties, parameters, and user values cannot enter
  // the global metric cardinality.
  Status RegisterLabel(QueryMetricOperator);
  Status RegisterLabel(QueryMetricTerminal);
  Status RegisterLabel(QueryMetricFallback);

 private:
  std::array<std::atomic<uint64_t>, static_cast<size_t>(QueryMetricOperator::kCount)> operator_rows_{};
  std::array<std::atomic<uint64_t>, static_cast<size_t>(QueryMetricTerminal::kCount)> terminal_{};
  std::array<std::atomic<uint64_t>, static_cast<size_t>(QueryMetricFallback::kCount)> fallback_{};
  std::atomic<uint64_t> batches_{0};
  std::atomic<uint64_t> physical_bytes_{0};
  std::atomic<uint64_t> decoded_bytes_{0};
  std::atomic<uint64_t> interval_fragments_{0};
  std::atomic<uint64_t> spill_bytes_{0};
};
}  // namespace internal
}  // namespace cedar

#endif  // CEDAR_QUERY_OBSERVABILITY_QUERY_METRICS_H_
