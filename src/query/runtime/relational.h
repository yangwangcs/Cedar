// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_RUNTIME_RELATIONAL_H_
#define CEDAR_QUERY_RUNTIME_RELATIONAL_H_

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/query/types.h"

namespace cedar::internal {

enum class ResourceDimension : uint8_t {
  kMemory = 0,
  kScratch,
  kReadBytes,
  kPrefetchBytes,
  kDecodedRows,
  kOutputRows,
  kOutputBytes,
  kIntervalFragments,
  kGraphLabels,
  kVisitedVertices,
  kCpuMicros,
  kCount,
};

using RelationalScalar =
    std::variant<bool, int32_t, int64_t, float, double, Timestamp64,
                 std::string, Binary, VertexRef, EdgeRef, ValidTime,
                 ValidDuration, CommitSeq, ValidTimeInterval>;

struct RelationalCell {
  QueryType type = QueryType::kBool;
  bool present = false;
  RelationalScalar value = false;

  template <typename T>
  static RelationalCell Present(QueryType type, T value) {
    return {type, true, RelationalScalar(std::move(value))};
  }
  static RelationalCell Missing(QueryType type) { return {type, false, false}; }
  bool operator==(const RelationalCell&) const = default;
};

struct RelationalRow {
  std::vector<RelationalCell> cells;
  std::optional<ValidTimeInterval> effective;
  mutable std::optional<size_t> cached_hash;
  bool operator==(const RelationalRow& other) const {
    return cells == other.cells && effective == other.effective;
  }
};

class QueryReservationLease;
class QueryScratch;

using SpillPartitionObserver = std::function<void(bool rebuilt)>;
void SetSpillPartitionObserverForTesting(SpillPartitionObserver observer);

// Test seam for validating framed spill decoding without requiring a full
// physical join to manufacture a malformed run.
StatusOr<std::vector<RelationalRow>> DeserializeRowsForTesting(
    std::string_view payload, class QueryReservation* reservation);

struct BatchStream {
  BatchStream() = default;
  BatchStream(std::vector<RelationalRow> rows, bool order_specified = false)
      : rows(std::move(rows)), order_specified(order_specified) {}
  BatchStream(const BatchStream&) = delete;
  BatchStream& operator=(const BatchStream&) = delete;
  BatchStream(BatchStream&&) noexcept = default;
  BatchStream& operator=(BatchStream&&) noexcept = default;

  std::vector<RelationalRow> rows;
  bool order_specified = false;
  std::shared_ptr<QueryReservationLease> reservation_lease;
  // External operators may produce output a partition at a time. Keep each
  // partition lease alive while the rows are accumulated into this stream.
  std::vector<std::shared_ptr<QueryReservationLease>> reservation_leases;
  bool operator==(const BatchStream& other) const {
    return rows == other.rows && order_specified == other.order_specified;
  }
};

enum class SortDirection : uint8_t { kAscending, kDescending };
struct SortKey {
  size_t column = 0;
  SortDirection direction = SortDirection::kAscending;
};

StatusOr<BatchStream> UnionAll(
    const BatchStream& left, const BatchStream& right,
    class QueryReservation* reservation,
    size_t max_output_rows = std::numeric_limits<size_t>::max());
StatusOr<BatchStream> Distinct(
    const BatchStream& input, class QueryReservation* reservation,
    size_t max_output_rows = std::numeric_limits<size_t>::max());
StatusOr<BatchStream> Sort(const BatchStream& input,
                           const std::vector<SortKey>& keys,
                           class QueryReservation* reservation,
                           size_t max_output_rows =
                               std::numeric_limits<size_t>::max());
StatusOr<BatchStream> Limit(
    const BatchStream& input, size_t offset, size_t count,
    class QueryReservation* reservation,
    size_t max_output_rows = std::numeric_limits<size_t>::max());

enum class JoinKind : uint8_t { kInner, kSemi, kAnti };
enum class JoinAlgorithm : uint8_t {
  kIndexNestedLoop,
  kHash,
  kSortMerge,
  kIntervalMerge,
};

struct JoinInput {
  BatchStream left;
  BatchStream right;
  size_t left_key = 0;
  size_t right_key = 0;
  JoinKind kind = JoinKind::kInner;
};

struct TemporalJoinInput {
  BatchStream left;
  BatchStream right;
  size_t left_key = 0;
  size_t right_key = 0;
  JoinKind kind = JoinKind::kInner;
};

class QueryReservation {
 public:
  struct State;
  explicit QueryReservation(size_t limit_bytes);
  explicit QueryReservation(std::array<uint64_t, static_cast<size_t>(ResourceDimension::kCount)> limits);
  bool TryGrow(size_t bytes);
  Status ReserveMemory(uint64_t bytes);
  Status ReserveScratch(uint64_t bytes);
  Status ReserveReadBytes(uint64_t bytes);
  Status ReservePrefetchBytes(uint64_t bytes);
  Status ReserveDecodedRows(uint64_t rows);
  Status ReserveOutputRows(uint64_t rows);
  Status ReserveOutputBytes(uint64_t bytes);
  Status ReserveIntervalFragments(uint64_t count);
  Status ReserveGraphLabels(uint64_t count);
  Status ReserveVisitedVertices(uint64_t count);
  Status ReserveCpuMicros(uint64_t micros);
  void ReleaseMemory(uint64_t bytes);
  void ReleaseScratch(uint64_t bytes);
  void ReleaseReadBytes(uint64_t bytes);
  uint64_t used(ResourceDimension dimension) const;
  uint64_t limit(ResourceDimension dimension) const;
  void AttachPoolAdmission(
      std::array<std::shared_ptr<std::atomic<uint64_t>>,
                 static_cast<size_t>(ResourceDimension::kCount)> dimensions,
                           std::shared_ptr<std::atomic<uint32_t>> workers,
                           std::array<uint64_t,
                                      static_cast<size_t>(ResourceDimension::kCount)>
                               admitted_dimensions,
                           uint32_t admitted_workers);
  void Release(size_t bytes);
  std::shared_ptr<QueryReservationLease> TryRetain(size_t bytes);
  size_t limit_bytes() const;
  size_t used_bytes() const;
  size_t peak_bytes() const;

 private:
  std::shared_ptr<State> state_;

  friend class QueryReservationLease;
  friend class QueryResourcePool;
};

class QueryReservationLease {
 public:
  ~QueryReservationLease();

 private:
  QueryReservationLease(std::shared_ptr<QueryReservation::State> state,
                        size_t bytes);

  std::shared_ptr<QueryReservation::State> state_;
  size_t bytes_ = 0;

  friend class QueryReservation;
};

class FragmentBudget {
 public:
  explicit FragmentBudget(size_t limit) : limit_(limit) {}
  bool TryConsume(size_t fragments = 1);
  size_t limit_fragments() const { return limit_; }
  size_t used_fragments() const { return used_; }

 private:
  size_t limit_ = 0;
  size_t used_ = 0;
};

Status NeedsSpill();
bool IsNeedsSpill(const Status& status);

JoinAlgorithm ChooseJoinAlgorithm(size_t estimated_rows, bool sorted_keys,
                                  bool temporal);
StatusOr<BatchStream> IndexNestedLoopJoin(
    const JoinInput& input, QueryReservation* reservation,
    size_t max_output_rows = std::numeric_limits<size_t>::max());
StatusOr<BatchStream> HashJoin(const JoinInput& input,
                               QueryReservation* reservation,
                               size_t max_output_rows =
                                   std::numeric_limits<size_t>::max(),
                               QueryScratch* scratch = nullptr);
StatusOr<BatchStream> SortMergeJoin(const JoinInput& input,
                                    QueryReservation* reservation,
                                    size_t max_output_rows =
                                        std::numeric_limits<size_t>::max(),
                                    QueryScratch* scratch = nullptr);
StatusOr<BatchStream> IntervalMergeJoin(const TemporalJoinInput& input,
                                        FragmentBudget* budget,
                                        QueryReservation* reservation,
                                        size_t max_output_rows =
                                            std::numeric_limits<size_t>::max());

enum class AggregateKind : uint8_t { kCount, kSum, kMin, kMax };
struct AggregateSpec {
  AggregateKind kind = AggregateKind::kCount;
  size_t input_column = 0;
};
struct AggregateInput {
  BatchStream input;
  std::vector<size_t> group_by;
  std::vector<AggregateSpec> aggregates;
};
struct TemporalAggregateInput {
  BatchStream input;
  std::vector<size_t> group_by;
};

StatusOr<BatchStream> AggregateRows(
    const AggregateInput& input, QueryReservation* reservation,
    size_t max_output_rows = std::numeric_limits<size_t>::max());
StatusOr<BatchStream> TemporalAggregate(
    const TemporalAggregateInput& input, FragmentBudget* budget,
    QueryReservation* reservation,
    size_t max_output_rows = std::numeric_limits<size_t>::max());

}  // namespace cedar::internal

#endif  // CEDAR_QUERY_RUNTIME_RELATIONAL_H_
