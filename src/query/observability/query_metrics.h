// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_OBSERVABILITY_QUERY_METRICS_H_
#define CEDAR_QUERY_OBSERVABILITY_QUERY_METRICS_H_

#include <array>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
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
  // A snapshot is planner-usable only when every manifest-referenced segment
  // was decoded successfully. Partial/corrupt generations remain observable
  // but conservative.
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
  mutable std::mutex refresh_mutex_;
  uint64_t refresh_counter_ = 0;
  std::optional<uint64_t> latest_generation_id_;
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
enum class QueryMetricAdmission : uint8_t {
  kAdmitted = 0, kQueued, kRejected, kCount,
};
enum class QueryMetricProjection : uint8_t {
  kHit = 0, kFallback, kCount,
};
enum class QueryMetricProjectionHealth : uint8_t {
  kHealthy = 0, kHole, kCorrupt, kStale, kCount,
};
enum class QueryMetricAdjacencyPruning : uint8_t {
  kPruned = 0, kExpanded, kCount,
};
enum class QueryMetricLabelDominance : uint8_t {
  kBalanced = 0, kDominant, kCount,
};

constexpr size_t kQueryMetricHistogramBuckets = 16;

using QueryMetricHistogram = std::array<uint64_t, kQueryMetricHistogramBuckets>;

struct QueryMetricsSnapshot {
  std::array<uint64_t, static_cast<size_t>(QueryMetricOperator::kCount)> operator_rows{};
  std::array<uint64_t, static_cast<size_t>(QueryMetricTerminal::kCount)> terminal{};
  std::array<uint64_t, static_cast<size_t>(QueryMetricFallback::kCount)> fallback{};
  std::array<uint64_t, static_cast<size_t>(QueryMetricAdmission::kCount)> admission{};
  std::array<uint64_t, static_cast<size_t>(QueryMetricProjection::kCount)> projection{};
  std::array<uint64_t, static_cast<size_t>(QueryMetricProjectionHealth::kCount)> projection_health{};
  std::array<uint64_t, static_cast<size_t>(QueryMetricAdjacencyPruning::kCount)> adjacency_pruning{};
  std::array<uint64_t, static_cast<size_t>(QueryMetricLabelDominance::kCount)> label_dominance{};
  QueryMetricHistogram latency_us{};
  QueryMetricHistogram admission_wait_us{};
  QueryMetricHistogram worker_wait_us{};
  QueryMetricHistogram io_wait_us{};
  QueryMetricHistogram delta_lag{};
  uint64_t batches = 0;
  uint64_t physical_bytes = 0;
  uint64_t decoded_bytes = 0;
  uint64_t interval_fragments = 0;
  uint64_t spill_bytes = 0;
  uint64_t memory_bytes = 0;
  uint64_t scratch_bytes = 0;
};

class QueryMetrics {
 public:
  void AddBatch(QueryMetricOperator op, uint64_t rows, uint64_t physical_bytes,
                uint64_t decoded_bytes, uint64_t interval_fragments = 0);
  void AddTerminal(QueryMetricTerminal terminal);
  void AddFallback(QueryMetricFallback fallback);
  void AddSpillBytes(uint64_t bytes);
  void AddAdmission(QueryMetricAdmission admission);
  void AddProjection(QueryMetricProjection projection);
  void AddProjectionHealth(QueryMetricProjectionHealth health);
  void AddAdjacencyPruning(QueryMetricAdjacencyPruning pruning);
  void AddLabelDominance(QueryMetricLabelDominance dominance);
  void AddMemoryBytes(uint64_t bytes);
  void AddScratchBytes(uint64_t bytes);
  void ObserveLatencyUs(uint64_t microseconds);
  void ObserveAdmissionWaitUs(uint64_t microseconds);
  void ObserveWorkerWaitUs(uint64_t microseconds);
  void ObserveIoWaitUs(uint64_t microseconds);
  void ObserveDeltaLag(uint64_t commits);
  QueryMetricsSnapshot Snapshot() const;
  // Labels are fixed Cedar enums. There is intentionally no string-label API:
  // query ids, text, properties, parameters, and user values cannot enter
  // the global metric cardinality.
  Status RegisterLabel(QueryMetricOperator);
  Status RegisterLabel(QueryMetricTerminal);
  Status RegisterLabel(QueryMetricFallback);
  Status RegisterLabel(QueryMetricAdmission);
  Status RegisterLabel(QueryMetricProjection);
  Status RegisterLabel(QueryMetricProjectionHealth);
  Status RegisterLabel(QueryMetricAdjacencyPruning);
  Status RegisterLabel(QueryMetricLabelDominance);

 private:
  static size_t HistogramBucket(uint64_t value);
  std::array<std::atomic<uint64_t>, static_cast<size_t>(QueryMetricOperator::kCount)> operator_rows_{};
  std::array<std::atomic<uint64_t>, static_cast<size_t>(QueryMetricTerminal::kCount)> terminal_{};
  std::array<std::atomic<uint64_t>, static_cast<size_t>(QueryMetricFallback::kCount)> fallback_{};
  std::array<std::atomic<uint64_t>, static_cast<size_t>(QueryMetricAdmission::kCount)> admission_{};
  std::array<std::atomic<uint64_t>, static_cast<size_t>(QueryMetricProjection::kCount)> projection_{};
  std::array<std::atomic<uint64_t>, static_cast<size_t>(QueryMetricProjectionHealth::kCount)> projection_health_{};
  std::array<std::atomic<uint64_t>, static_cast<size_t>(QueryMetricAdjacencyPruning::kCount)> adjacency_pruning_{};
  std::array<std::atomic<uint64_t>, static_cast<size_t>(QueryMetricLabelDominance::kCount)> label_dominance_{};
  std::array<std::atomic<uint64_t>, kQueryMetricHistogramBuckets> latency_us_{};
  std::array<std::atomic<uint64_t>, kQueryMetricHistogramBuckets> admission_wait_us_{};
  std::array<std::atomic<uint64_t>, kQueryMetricHistogramBuckets> worker_wait_us_{};
  std::array<std::atomic<uint64_t>, kQueryMetricHistogramBuckets> io_wait_us_{};
  std::array<std::atomic<uint64_t>, kQueryMetricHistogramBuckets> delta_lag_{};
  std::atomic<uint64_t> batches_{0};
  std::atomic<uint64_t> physical_bytes_{0};
  std::atomic<uint64_t> decoded_bytes_{0};
  std::atomic<uint64_t> interval_fragments_{0};
  std::atomic<uint64_t> spill_bytes_{0};
  std::atomic<uint64_t> memory_bytes_{0};
  std::atomic<uint64_t> scratch_bytes_{0};
};
}  // namespace internal
}  // namespace cedar

#endif  // CEDAR_QUERY_OBSERVABILITY_QUERY_METRICS_H_
