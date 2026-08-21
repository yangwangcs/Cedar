// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_RUNTIME_RELATIONAL_H_
#define CEDAR_QUERY_RUNTIME_RELATIONAL_H_

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/query/types.h"

namespace cedar::internal {

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
  bool operator==(const RelationalRow&) const = default;
};

struct BatchStream {
  std::vector<RelationalRow> rows;
  bool order_specified = false;
  bool operator==(const BatchStream&) const = default;
};

enum class SortDirection : uint8_t { kAscending, kDescending };
struct SortKey {
  size_t column = 0;
  SortDirection direction = SortDirection::kAscending;
};

BatchStream UnionAll(BatchStream left, BatchStream right);
BatchStream Distinct(const BatchStream& input);
StatusOr<BatchStream> Sort(const BatchStream& input,
                           const std::vector<SortKey>& keys,
                           class QueryReservation* reservation = nullptr);
BatchStream Limit(const BatchStream& input, size_t offset, size_t count);

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
  explicit QueryReservation(size_t limit_bytes) : limit_bytes_(limit_bytes) {}
  bool TryGrow(size_t bytes);
  void Release(size_t bytes);
  size_t limit_bytes() const { return limit_bytes_; }
  size_t used_bytes() const { return used_bytes_; }
  size_t peak_bytes() const { return peak_bytes_; }

 private:
  size_t limit_bytes_ = 0;
  size_t used_bytes_ = 0;
  size_t peak_bytes_ = 0;
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
StatusOr<BatchStream> IndexNestedLoopJoin(const JoinInput& input);
StatusOr<BatchStream> HashJoin(const JoinInput& input,
                               QueryReservation* reservation);
StatusOr<BatchStream> SortMergeJoin(const JoinInput& input,
                                    QueryReservation* reservation);
StatusOr<BatchStream> IntervalMergeJoin(TemporalJoinInput input,
                                        FragmentBudget* budget);

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

StatusOr<BatchStream> AggregateRows(AggregateInput input);
StatusOr<BatchStream> TemporalAggregate(TemporalAggregateInput input,
                                        FragmentBudget* budget);

}  // namespace cedar::internal

#endif  // CEDAR_QUERY_RUNTIME_RELATIONAL_H_
