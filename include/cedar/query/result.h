// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_RESULT_H_
#define CEDAR_QUERY_RESULT_H_

#include <cstddef>
#include <cstdint>
#include <chrono>
#include <atomic>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <variant>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/query/types.h"

namespace cedar {
namespace internal { class QueryMetrics; }

class Database;
class Snapshot;
template <typename T, bool Optional>
class Slot;
template <typename T>
constexpr QueryType QueryTypeOf();

namespace internal {
class QueryRuntime;
}

// A path is materialized only for a selected shortest-path target.  Keeping
// the nested values in this small value type lets the public column use flat
// buffers instead of a vector-of-vectors representation.
struct PathValue {
  std::vector<VertexRef> vertices;
  std::vector<EdgeRef> edges;
  ValidTimeInterval common;

  bool operator==(const PathValue&) const = default;
};

struct PathColumn {
  // vertex_offsets[i..i+1] and edge_offsets[i..i+1] delimit row i.
  // intervals[i] is the common witness for row i. row_offsets is retained
  // as a compatibility alias for vertex_offsets and is never used to decode
  // edges.
  std::vector<uint32_t> row_offsets;
  std::vector<uint32_t> vertex_offsets;
  std::vector<uint32_t> edge_offsets;
  std::vector<VertexRef> vertices;
  std::vector<EdgeRef> edges;
  std::vector<ValidTimeInterval> intervals;

  static PathColumn FromValues(const std::vector<PathValue>& values) {
    PathColumn column;
    column.row_offsets.reserve(values.size() + 1);
    column.vertex_offsets.reserve(values.size() + 1);
    column.edge_offsets.reserve(values.size() + 1);
    column.row_offsets.push_back(0);
    column.vertex_offsets.push_back(0);
    column.edge_offsets.push_back(0);
    for (const PathValue& value : values) {
      column.vertices.insert(column.vertices.end(), value.vertices.begin(),
                             value.vertices.end());
      column.edges.insert(column.edges.end(), value.edges.begin(),
                          value.edges.end());
      column.intervals.push_back(value.common);
      column.vertex_offsets.push_back(
          static_cast<uint32_t>(column.vertices.size()));
      column.edge_offsets.push_back(static_cast<uint32_t>(column.edges.size()));
      column.row_offsets.push_back(column.vertex_offsets.back());
    }
    return column;
  }

  PathValue Value(size_t row) const {
    if (row + 1 >= vertex_offsets.size() || row + 1 >= edge_offsets.size() ||
        row >= intervals.size()) {
      throw std::out_of_range("path row");
    }
    PathValue value;
    value.vertices.assign(vertices.begin() + vertex_offsets[row],
                          vertices.begin() + vertex_offsets[row + 1]);
    value.edges.assign(edges.begin() + edge_offsets[row],
                       edges.begin() + edge_offsets[row + 1]);
    value.common = intervals[row];
    return value;
  }
};

struct JourneyValue {
  std::vector<VertexRef> vertices;
  std::vector<EdgeRef> edges;
  std::vector<ValidTime> departures;
  std::vector<ValidTime> arrivals;
  ValidTime initial_departure;
  ValidTime final_arrival;
  ValidTime departure;
  ValidTime arrival;
  ValidDuration duration;
  bool operator==(const JourneyValue&) const = default;
};

struct JourneyColumn {
  std::vector<uint32_t> vertex_offsets;
  std::vector<uint32_t> edge_offsets;
  std::vector<uint32_t> departure_offsets;
  std::vector<uint32_t> arrival_offsets;
  std::vector<VertexRef> vertices;
  std::vector<EdgeRef> edges;
  std::vector<ValidTime> departures;
  std::vector<ValidTime> arrivals;
  std::vector<ValidTime> initial_departures;
  std::vector<ValidTime> final_arrivals;
  std::vector<ValidDuration> durations;

  static JourneyColumn FromValues(const std::vector<JourneyValue>& values) {
    JourneyColumn column;
    column.vertex_offsets.push_back(0);
    column.edge_offsets.push_back(0);
    column.departure_offsets.push_back(0);
    column.arrival_offsets.push_back(0);
    for (const auto& value : values) {
      column.vertices.insert(column.vertices.end(), value.vertices.begin(), value.vertices.end());
      column.edges.insert(column.edges.end(), value.edges.begin(), value.edges.end());
      column.departures.insert(column.departures.end(), value.departures.begin(), value.departures.end());
      column.arrivals.insert(column.arrivals.end(), value.arrivals.begin(), value.arrivals.end());
      column.initial_departures.push_back(value.initial_departure);
      column.final_arrivals.push_back(value.final_arrival);
      column.durations.push_back(value.duration);
      column.vertex_offsets.push_back(static_cast<uint32_t>(column.vertices.size()));
      column.edge_offsets.push_back(static_cast<uint32_t>(column.edges.size()));
      column.departure_offsets.push_back(static_cast<uint32_t>(column.departures.size()));
      column.arrival_offsets.push_back(static_cast<uint32_t>(column.arrivals.size()));
    }
    return column;
  }

  JourneyValue Value(size_t row) const {
    if (row + 1 >= vertex_offsets.size() || row + 1 >= edge_offsets.size() ||
        row + 1 >= departure_offsets.size() || row + 1 >= arrival_offsets.size() ||
        row >= initial_departures.size() || row >= final_arrivals.size() ||
        row >= durations.size()) throw std::out_of_range("journey row");
    JourneyValue value;
    value.vertices.assign(vertices.begin() + vertex_offsets[row], vertices.begin() + vertex_offsets[row + 1]);
    value.edges.assign(edges.begin() + edge_offsets[row], edges.begin() + edge_offsets[row + 1]);
    value.departures.assign(departures.begin() + departure_offsets[row], departures.begin() + departure_offsets[row + 1]);
    value.arrivals.assign(arrivals.begin() + arrival_offsets[row], arrivals.begin() + arrival_offsets[row + 1]);
    value.initial_departure = initial_departures[row];
    value.final_arrival = final_arrivals[row];
    value.departure = value.initial_departure;
    value.arrival = value.final_arrival;
    value.duration = durations[row];
    return value;
  }
};

using QueryColumnVector = std::variant<
    std::vector<uint8_t>, std::vector<int32_t>, std::vector<int64_t>,
    std::vector<float>, std::vector<double>, std::vector<uint64_t>,
    std::vector<std::string>, std::vector<VertexRef>, std::vector<EdgeRef>,
    std::vector<ValidTime>, std::vector<ValidDuration>,
    std::vector<CommitSeq>, std::vector<ValidTimeInterval>>;

struct QueryColumn {
  SlotId slot;
  QueryType type;
  QueryColumnVector values;
  std::vector<uint8_t> present;
  std::shared_ptr<const PathColumn> path_values;
  std::shared_ptr<const JourneyColumn> journey_values;
};

enum class QueryCursorState : uint8_t {
  kRunning,
  kCleanEnd,
  kCancelled,
  kFailed,
};

struct QueryTerminalInfo {
  QueryCursorState state = QueryCursorState::kRunning;
  bool complete = false;
  Status status = Status::OK();
};

struct QueryOperatorProfile {
  uint32_t operator_id = 0;
  uint64_t rows = 0;
  uint64_t batches = 0;
  uint64_t cpu_us = 0;
  uint64_t wall_us = 0;
  uint64_t queue_us = 0;
  uint64_t first_result_us = 0;
  uint64_t physical_bytes = 0;
  uint64_t decoded_bytes = 0;
  uint64_t pages = 0;
  uint64_t delta_repairs = 0;
  uint64_t interval_fragments = 0;
  uint64_t spill_bytes = 0;
  uint64_t frontier_labels = 0;
};

struct QueryProfile {
  std::vector<QueryOperatorProfile> operators;
  QueryTerminalInfo terminal;
};

// Shared lifecycle state lets Database shutdown request cancellation without
// holding a query/runtime mutex or waiting for a blocked read callback.
class QueryExecutionState {
 public:
  QueryExecutionState();
  void RequestCancel();
  bool cancelled() const;
  Status FinishClean();
  Status FinishCancelled();
  Status FinishFailed(Status status);
  QueryTerminalInfo terminal_info() const;
  QueryProfile profile() const;
  Status Close();
  // QueryRuntime brackets every potentially blocking Next call with these
  // methods. Close waits for the last operation before releasing snapshots.
  void BeginOperation();
  void EndOperation();
  void WaitForOperations();
  void SetCancelCallback(std::function<void()> callback);
  void SetCloseCallback(std::function<void()> callback);
  void ClearCancelCallback();
  void SetSnapshotSeq(CommitSeq seq);
  std::optional<CommitSeq> snapshot_seq() const;
  void RecordBatch(uint64_t rows, uint64_t decoded_bytes,
                   bool capture_profile = true, uint64_t physical_bytes = 0,
                   uint64_t pages = 0, uint64_t interval_fragments = 0,
                   uint32_t operator_id = 0, uint8_t metric_operator = 0);
  void SetMetrics(internal::QueryMetrics* metrics) { metrics_ = metrics; }

 private:
  mutable std::mutex mutex_;
  std::condition_variable operations_cv_;
  uint32_t active_operations_ = 0;
  bool close_started_ = false;
  bool close_callback_called_ = false;
  std::atomic<bool> cancelled_{false};
  QueryTerminalInfo terminal_;
  std::optional<CommitSeq> snapshot_seq_;
  QueryProfile profile_;
  std::function<void()> cancel_callback_;
  std::function<void()> close_callback_;
  internal::QueryMetrics* metrics_ = nullptr;
  std::chrono::steady_clock::time_point profile_started_at_;
  std::chrono::steady_clock::time_point profile_last_at_;
  uint64_t profile_cpu_started_us_ = 0;
  uint64_t profile_cpu_last_us_ = 0;
  bool profile_first_result_recorded_ = false;
};

class QueryBatch {
 public:
  size_t row_count() const { return row_count_; }
  const std::vector<QueryColumn>& columns() const { return columns_; }

  template <typename T, bool Optional>
  T Get(const Slot<T, Optional>& slot, size_t row) const {
    if (row >= row_count_) throw std::out_of_range("query row");
    for (const QueryColumn& column : columns_) {
      if (column.slot != slot.id()) continue;
      if (column.type != QueryTypeOf<T>()) {
        throw std::invalid_argument("query slot type mismatch");
      }
      if (!column.present.empty() && column.present.at(row) == 0) {
        throw std::invalid_argument("query value is absent");
      }
      if constexpr (std::is_same_v<T, bool>) {
        return std::get<std::vector<uint8_t>>(column.values).at(row) != 0;
      } else if constexpr (std::is_same_v<T, Timestamp64>) {
        return Timestamp64{
            std::get<std::vector<uint64_t>>(column.values).at(row)};
      } else if constexpr (std::is_same_v<T, Binary>) {
        return Binary{std::get<std::vector<std::string>>(column.values).at(row)};
      } else if constexpr (std::is_same_v<T, PathValue>) {
        if (!column.path_values) throw std::out_of_range("path column");
        return column.path_values->Value(row);
      } else if constexpr (std::is_same_v<T, JourneyValue>) {
        if (!column.journey_values) throw std::out_of_range("journey column");
        return column.journey_values->Value(row);
      } else {
        return std::get<std::vector<T>>(column.values).at(row);
      }
    }
    throw std::out_of_range("query slot");
  }

 private:
  QueryBatch(size_t row_count, std::vector<QueryColumn> columns,
             std::shared_ptr<void> lifetime_guard = {})
      : row_count_(row_count), columns_(std::move(columns)),
        lifetime_guard_(std::move(lifetime_guard)) {}

  size_t row_count_ = 0;
  std::vector<QueryColumn> columns_;
  // Keeps Cedar-owned decoded-buffer accounting alive after cursor Close().
  std::shared_ptr<void> lifetime_guard_;

  friend class QueryCursor;
  friend class internal::QueryRuntime;
};

class QueryCursor {
 public:
  ~QueryCursor();
  QueryCursor(QueryCursor&&) noexcept;
  QueryCursor& operator=(QueryCursor&&) noexcept;

  QueryCursor(const QueryCursor&) = delete;
  QueryCursor& operator=(const QueryCursor&) = delete;

  StatusOr<std::optional<QueryBatch>> Next();
  Status Cancel();
  Status Close();
  QueryTerminalInfo terminal_info() const;
  QueryProfile profile() const;

 private:
  class State;
  explicit QueryCursor(std::unique_ptr<State> state);

  std::unique_ptr<State> state_;

  friend class internal::QueryRuntime;
};

class PreparedQuery {
 public:
  PreparedQuery(const PreparedQuery&) = default;
  PreparedQuery& operator=(const PreparedQuery&) = default;
  PreparedQuery(PreparedQuery&&) noexcept = default;
  PreparedQuery& operator=(PreparedQuery&&) noexcept = default;

  StatusOr<QueryCursor> Execute(Snapshot snapshot, const Bindings& bindings,
                                const QueryOptions& options) const;
  // Explain does not execute the query. Logical explanation is independent of
  // any snapshot; physical explanation binds a plan at the borrowed snapshot
  // cut and reports canonical fallback when no derived catalog is available.
  StatusOr<std::string> ExplainLogical() const;
  StatusOr<std::string> ExplainPhysical(const Snapshot& snapshot,
                                         const QueryOptions& options) const;

 private:
  class State;
  explicit PreparedQuery(std::shared_ptr<const State> state)
      : state_(std::move(state)) {}

  std::shared_ptr<const State> state_;

  friend class Database;
};

}  // namespace cedar

#endif  // CEDAR_QUERY_RESULT_H_
