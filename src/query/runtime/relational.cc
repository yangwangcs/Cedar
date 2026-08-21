// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/runtime/relational.h"

#include <algorithm>
#include <bit>
#include <cmath>
#include <functional>
#include <limits>
#include <numeric>
#include <utility>

#include "query/temporal/interval.h"

namespace cedar::internal {

Status NeedsSpill(size_t requested_bytes, size_t available_bytes);

struct QueryReservation::State {
  explicit State(size_t limit) : limit_bytes(limit) {}

  size_t limit_bytes = 0;
  size_t used_bytes = 0;
  size_t peak_bytes = 0;
};

namespace {

Status ValidateKey(const BatchStream& stream, size_t key) {
  for (const RelationalRow& row : stream.rows) {
    if (key >= row.cells.size()) {
      return Status::InvalidArgument("relational operator",
                                     "key column is out of range");
    }
  }
  return Status::OK();
}

Status ValidateJoinStreams(const BatchStream& left_stream,
                           const BatchStream& right_stream, size_t left_key,
                           size_t right_key) {
  if (Status status = ValidateKey(left_stream, left_key); !status.ok()) {
    return status;
  }
  if (Status status = ValidateKey(right_stream, right_key); !status.ok()) {
    return status;
  }
  for (const RelationalRow& left : left_stream.rows) {
    for (const RelationalRow& right : right_stream.rows) {
      if (left.cells[left_key].type != right.cells[right_key].type) {
        return Status::InvalidArgument("relational join",
                                       "join key types differ");
      }
    }
  }
  return Status::OK();
}

Status ValidateJoin(const JoinInput& input) {
  return ValidateJoinStreams(input.left, input.right, input.left_key,
                             input.right_key);
}

bool KeysEqual(const RelationalCell& left, const RelationalCell& right) {
  return left.present && right.present && left.type == right.type &&
         left.value == right.value;
}

template <typename T>
int CompareSimple(const T& left, const T& right) {
  if constexpr (std::is_floating_point_v<T>) {
    const bool left_nan = std::isnan(left);
    const bool right_nan = std::isnan(right);
    if (left_nan || right_nan) {
      if (left_nan && right_nan) return 0;
      return left_nan ? 1 : -1;
    }
  }
  if (left < right) return -1;
  if (right < left) return 1;
  return 0;
}

int CompareCell(const RelationalCell& left, const RelationalCell& right) {
  if (left.type != right.type) {
    return left.type < right.type ? -1 : 1;
  }
  if (left.present != right.present) return left.present ? 1 : -1;
  if (!left.present) return 0;
  return std::visit(
      [](const auto& left_value, const auto& right_value) -> int {
        using Left = std::decay_t<decltype(left_value)>;
        using Right = std::decay_t<decltype(right_value)>;
        if constexpr (!std::is_same_v<Left, Right>) {
          return 0;
        } else if constexpr (std::is_arithmetic_v<Left> ||
                             std::is_same_v<Left, std::string>) {
          return CompareSimple(left_value, right_value);
        } else if constexpr (std::is_same_v<Left, Binary>) {
          return CompareSimple(left_value.value, right_value.value);
        } else if constexpr (std::is_same_v<Left, VertexRef>) {
          if (int part = CompareSimple(left_value.part_id.value,
                                       right_value.part_id.value);
              part != 0) {
            return part;
          }
          return CompareSimple(left_value.vertex_id.value,
                               right_value.vertex_id.value);
        } else if constexpr (std::is_same_v<Left, EdgeRef>) {
          if (int part = CompareSimple(left_value.home_part_id.value,
                                       right_value.home_part_id.value);
              part != 0) {
            return part;
          }
          return CompareSimple(left_value.edge_id.value,
                               right_value.edge_id.value);
        } else if constexpr (std::is_same_v<Left, ValidTimeInterval>) {
          if (int from = CompareSimple(left_value.from.value,
                                       right_value.from.value);
              from != 0) {
            return from;
          }
          if (left_value.to.has_value() != right_value.to.has_value()) {
            return left_value.to.has_value() ? -1 : 1;
          }
          return left_value.to.has_value()
                     ? CompareSimple(left_value.to->value,
                                     right_value.to->value)
                     : 0;
        } else {
          return CompareSimple(left_value.value, right_value.value);
        }
      },
      left.value, right.value);
}

size_t HashCell(const RelationalCell& cell) {
  if (!cell.present) return 0;
  size_t hash = static_cast<size_t>(cell.type) + 0x9e3779b9U;
  std::visit(
      [&hash](const auto& value) {
        using T = std::decay_t<decltype(value)>;
        auto combine = [&hash](size_t part) {
          hash ^= part + 0x9e3779b97f4a7c15ULL + (hash << 6) + (hash >> 2);
        };
        if constexpr (std::is_same_v<T, bool> ||
                      std::is_same_v<T, int32_t> ||
                      std::is_same_v<T, int64_t>) {
          combine(std::hash<T>{}(value));
        } else if constexpr (std::is_same_v<T, float>) {
          const float normalized = value == 0.0F ? 0.0F : value;
          combine(std::hash<uint32_t>{}(std::bit_cast<uint32_t>(normalized)));
        } else if constexpr (std::is_same_v<T, double>) {
          const double normalized = value == 0.0 ? 0.0 : value;
          combine(std::hash<uint64_t>{}(std::bit_cast<uint64_t>(normalized)));
        } else if constexpr (std::is_same_v<T, std::string>) {
          combine(std::hash<std::string>{}(value));
        } else if constexpr (std::is_same_v<T, Binary>) {
          combine(std::hash<std::string>{}(value.value));
        } else if constexpr (std::is_same_v<T, VertexRef>) {
          combine(value.part_id.value);
          combine(value.vertex_id.value);
        } else if constexpr (std::is_same_v<T, EdgeRef>) {
          combine(value.home_part_id.value);
          combine(value.edge_id.value);
        } else if constexpr (std::is_same_v<T, ValidTimeInterval>) {
          combine(value.from.value);
          combine(value.to ? value.to->value : std::numeric_limits<uint64_t>::max());
          combine(value.to.has_value());
        } else {
          combine(value.value);
        }
      },
      cell.value);
  return hash;
}

bool AddBytes(size_t bytes, size_t* total) {
  if (bytes > std::numeric_limits<size_t>::max() - *total) return false;
  *total += bytes;
  return true;
}

size_t EstimateCellBytes(const RelationalCell& cell) {
  size_t bytes = sizeof(RelationalCell);
  if (!cell.present) return bytes;
  std::visit(
      [&bytes](const auto& value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, std::string>) {
          if (!AddBytes(value.size(), &bytes)) {
            bytes = std::numeric_limits<size_t>::max();
          }
        } else if constexpr (std::is_same_v<T, Binary>) {
          if (!AddBytes(value.value.size(), &bytes)) {
            bytes = std::numeric_limits<size_t>::max();
          }
        }
      },
      cell.value);
  return bytes;
}

size_t EstimateRowBytes(const RelationalRow& row) {
  size_t bytes = sizeof(RelationalRow);
  for (const RelationalCell& cell : row.cells) {
    if (!AddBytes(EstimateCellBytes(cell), &bytes)) {
      return std::numeric_limits<size_t>::max();
    }
  }
  return bytes;
}

size_t EstimateCombinedRowBytes(const RelationalRow& left,
                                const RelationalRow& right) {
  size_t bytes = sizeof(RelationalRow);
  for (const RelationalCell& cell : left.cells) {
    if (!AddBytes(EstimateCellBytes(cell), &bytes)) {
      return std::numeric_limits<size_t>::max();
    }
  }
  for (const RelationalCell& cell : right.cells) {
    if (!AddBytes(EstimateCellBytes(cell), &bytes)) {
      return std::numeric_limits<size_t>::max();
    }
  }
  return bytes;
}

size_t EstimateBytes(const BatchStream& stream) {
  size_t bytes = sizeof(BatchStream);
  for (const RelationalRow& row : stream.rows) {
    if (!AddBytes(EstimateRowBytes(row), &bytes)) {
      return std::numeric_limits<size_t>::max();
    }
  }
  return bytes;
}

size_t EstimateRowsBytes(const std::vector<RelationalRow>& first,
                         const std::vector<RelationalRow>* second = nullptr) {
  size_t bytes = sizeof(BatchStream);
  for (const RelationalRow& row : first) {
    if (!AddBytes(EstimateRowBytes(row), &bytes)) {
      return std::numeric_limits<size_t>::max();
    }
  }
  if (second != nullptr) {
    for (const RelationalRow& row : *second) {
      if (!AddBytes(EstimateRowBytes(row), &bytes)) {
        return std::numeric_limits<size_t>::max();
      }
    }
  }
  return bytes;
}

size_t AvailableBytes(const QueryReservation& reservation) {
  return reservation.limit_bytes() - reservation.used_bytes();
}

Status MemoryExhausted(size_t requested_bytes, size_t available_bytes) {
  return Status::ResourceExhausted(
      "query relational", "memory_bytes=" + std::to_string(requested_bytes) +
                              " available_bytes=" +
                              std::to_string(available_bytes));
}

Status OutputRowsExhausted() {
  return Status::ResourceExhausted("query", "output row budget exceeded");
}

Status TemporalBudgetExhausted(size_t memory_bytes, size_t output_rows,
                               size_t output_bytes, size_t interval_fragments,
                               const char* reason = "budget exceeded") {
  return Status::ResourceExhausted(
      "query", std::string(reason) + " memory_bytes=" +
                   std::to_string(memory_bytes) +
                   " output_rows=" + std::to_string(output_rows) +
                   " output_bytes=" + std::to_string(output_bytes) +
                   " interval_fragments=" +
                   std::to_string(interval_fragments));
}

StatusOr<std::shared_ptr<QueryReservationLease>> RetainOutput(
    QueryReservation* reservation, size_t bytes, bool spill_capable) {
  if (reservation == nullptr) {
    return spill_capable ? NeedsSpill()
                         : MemoryExhausted(bytes, size_t{0});
  }
  auto lease = reservation->TryRetain(bytes);
  if (!lease) {
    return spill_capable ? NeedsSpill(bytes, AvailableBytes(*reservation))
                         : MemoryExhausted(bytes, AvailableBytes(*reservation));
  }
  return lease;
}

struct JoinOutputEstimate {
  size_t rows = 0;
  size_t bytes = sizeof(BatchStream);
};

struct HashEntry {
  const RelationalRow* row = nullptr;
  size_t next = std::numeric_limits<size_t>::max();
};

constexpr size_t kNoHashEntry = std::numeric_limits<size_t>::max();

bool AddOutputEstimate(JoinOutputEstimate* estimate,
                       const RelationalRow& left,
                       const RelationalRow* right) {
  const size_t bytes = right == nullptr ? EstimateRowBytes(left)
                                        : EstimateCombinedRowBytes(left, *right);
  if (!AddBytes(bytes, &estimate->bytes) ||
      estimate->rows == std::numeric_limits<size_t>::max()) {
    return false;
  }
  ++estimate->rows;
  return true;
}

StatusOr<JoinOutputEstimate> EstimateHashOutput(
    const JoinInput& input, const std::vector<size_t>& buckets,
    const std::vector<HashEntry>& table) {
  JoinOutputEstimate estimate;
  for (const RelationalRow& left : input.left.rows) {
    bool matched = false;
    if (left.cells[input.left_key].present) {
      for (size_t entry = buckets[HashCell(left.cells[input.left_key]) &
                                   (buckets.size() - 1)];
           entry != kNoHashEntry; entry = table[entry].next) {
        const RelationalRow* right = table[entry].row;
        if (!KeysEqual(left.cells[input.left_key],
                       right->cells[input.right_key])) {
          continue;
        }
        matched = true;
        if (input.kind == JoinKind::kInner &&
            !AddOutputEstimate(&estimate, left, right)) {
          return Status::ResourceExhausted("query relational",
                                           "join output size overflows");
        }
        if (input.kind == JoinKind::kSemi) break;
      }
    }
    if ((input.kind == JoinKind::kSemi && matched) ||
        (input.kind == JoinKind::kAnti && !matched)) {
      if (!AddOutputEstimate(&estimate, left, nullptr)) {
        return Status::ResourceExhausted("query relational",
                                         "join output size overflows");
      }
    }
  }
  return estimate;
}

StatusOr<JoinOutputEstimate> EstimateSortedMergeOutput(
    const std::vector<RelationalRow>& left,
    const std::vector<RelationalRow>& right, const JoinInput& input) {
  JoinOutputEstimate estimate;
  size_t left_index = 0;
  size_t right_index = 0;
  while (left_index < left.size() && right_index < right.size()) {
    const int compared = CompareCell(left[left_index].cells[input.left_key],
                                     right[right_index].cells[input.right_key]);
    if (compared < 0) {
      if (input.kind == JoinKind::kAnti &&
          !AddOutputEstimate(&estimate, left[left_index], nullptr)) {
        return Status::ResourceExhausted("query relational",
                                         "join output size overflows");
      }
      ++left_index;
      continue;
    }
    if (compared > 0) {
      ++right_index;
      continue;
    }
    size_t left_end = left_index + 1;
    while (left_end < left.size() && CompareCell(left[left_index].cells[input.left_key],
                                                   left[left_end].cells[input.left_key]) == 0) {
      ++left_end;
    }
    size_t right_end = right_index + 1;
    while (right_end < right.size() && CompareCell(right[right_index].cells[input.right_key],
                                                     right[right_end].cells[input.right_key]) == 0) {
      ++right_end;
    }
    const bool equal = KeysEqual(left[left_index].cells[input.left_key],
                                 right[right_index].cells[input.right_key]);
    for (size_t left_row = left_index; left_row < left_end; ++left_row) {
      if (equal && input.kind == JoinKind::kInner) {
        for (size_t right_row = right_index; right_row < right_end; ++right_row) {
          if (!AddOutputEstimate(&estimate, left[left_row], &right[right_row])) {
            return Status::ResourceExhausted("query relational",
                                             "join output size overflows");
          }
        }
      } else if ((equal && input.kind == JoinKind::kSemi) ||
                 (!equal && input.kind == JoinKind::kAnti)) {
        if (!AddOutputEstimate(&estimate, left[left_row], nullptr)) {
          return Status::ResourceExhausted("query relational",
                                           "join output size overflows");
        }
      }
    }
    left_index = left_end;
    right_index = right_end;
  }
  if (input.kind == JoinKind::kAnti) {
    for (; left_index < left.size(); ++left_index) {
      if (!AddOutputEstimate(&estimate, left[left_index], nullptr)) {
        return Status::ResourceExhausted("query relational",
                                         "join output size overflows");
      }
    }
  }
  return estimate;
}

std::optional<size_t> HashBucketCount(size_t entries) {
  if (entries == 0) return size_t{1};
  if (entries > std::numeric_limits<size_t>::max() / 2) return std::nullopt;
  size_t buckets = 1;
  const size_t minimum = entries * 2;
  while (buckets < minimum) {
    if (buckets > std::numeric_limits<size_t>::max() / 2) {
      return std::nullopt;
    }
    buckets *= 2;
  }
  return buckets;
}

std::optional<JoinOutputEstimate> EstimateJoinOutput(const JoinInput& input) {
  JoinOutputEstimate estimate;
  for (const RelationalRow& left : input.left.rows) {
    bool matched = false;
    for (const RelationalRow& right : input.right.rows) {
      if (!KeysEqual(left.cells[input.left_key], right.cells[input.right_key])) {
        continue;
      }
      matched = true;
      if (input.kind != JoinKind::kInner) break;
      if (!AddBytes(EstimateCombinedRowBytes(left, right), &estimate.bytes) ||
          estimate.rows == std::numeric_limits<size_t>::max()) {
        return std::nullopt;
      }
      ++estimate.rows;
    }
    if ((input.kind == JoinKind::kSemi && matched) ||
        (input.kind == JoinKind::kAnti && !matched)) {
      if (!AddBytes(EstimateRowBytes(left), &estimate.bytes) ||
          estimate.rows == std::numeric_limits<size_t>::max()) {
        return std::nullopt;
      }
      ++estimate.rows;
    }
  }
  return estimate;
}

class ReservationGuard {
 public:
  ReservationGuard(QueryReservation* reservation, size_t bytes)
      : reservation_(reservation), bytes_(bytes), acquired_(reservation == nullptr) {
    if (reservation_ != nullptr) acquired_ = reservation_->TryGrow(bytes_);
  }
  ~ReservationGuard() {
    if (reservation_ != nullptr && acquired_) reservation_->Release(bytes_);
  }
  bool acquired() const { return acquired_; }

 private:
  QueryReservation* reservation_;
  size_t bytes_;
  bool acquired_;
};

RelationalRow Combine(const RelationalRow& left, const RelationalRow& right) {
  RelationalRow result = left;
  result.cells.insert(result.cells.end(), right.cells.begin(), right.cells.end());
  return result;
}

bool RowLess(const RelationalRow& left, const RelationalRow& right,
             const std::vector<SortKey>& keys) {
  for (const SortKey& key : keys) {
    const int compared = CompareCell(left.cells[key.column],
                                     right.cells[key.column]);
    if (compared == 0) continue;
    return key.direction == SortDirection::kAscending ? compared < 0
                                                      : compared > 0;
  }
  return false;
}

bool TemporalRowLess(const RelationalRow& left, const RelationalRow& right,
                     size_t key) {
  const int key_order = CompareCell(left.cells[key], right.cells[key]);
  if (key_order != 0) return key_order < 0;
  if (left.effective->from.value != right.effective->from.value) {
    return left.effective->from.value < right.effective->from.value;
  }
  if (left.effective->to.has_value() != right.effective->to.has_value()) {
    return left.effective->to.has_value();
  }
  if (left.effective->to.has_value() &&
      left.effective->to->value != right.effective->to->value) {
    return left.effective->to->value < right.effective->to->value;
  }
  const size_t common = std::min(left.cells.size(), right.cells.size());
  for (size_t column = 0; column < common; ++column) {
    const int compared = CompareCell(left.cells[column], right.cells[column]);
    if (compared != 0) return compared < 0;
  }
  return left.cells.size() < right.cells.size();
}

template <typename Less>
void StableInsertionSort(std::vector<RelationalRow>* rows, Less less) {
  for (size_t i = 1; i < rows->size(); ++i) {
    for (size_t j = i; j > 0 && less((*rows)[j], (*rows)[j - 1]); --j) {
      std::swap((*rows)[j], (*rows)[j - 1]);
    }
  }
}

Status ValidateSort(const BatchStream& input,
                    const std::vector<SortKey>& keys) {
  for (const RelationalRow& row : input.rows) {
    for (const SortKey& key : keys) {
      if (key.column >= row.cells.size()) {
        return Status::InvalidArgument("relational sort",
                                       "sort column is out of range");
      }
    }
  }
  for (const SortKey& key : keys) {
    std::optional<QueryType> type;
    for (const RelationalRow& row : input.rows) {
      if (!type.has_value()) type = row.cells[key.column].type;
      if (*type != row.cells[key.column].type) {
        return Status::InvalidArgument("relational sort",
                                       "sort column types differ");
      }
    }
  }
  return Status::OK();
}

bool Before(ValidTime left, ValidTime right) { return left.value < right.value; }

bool EndsAfter(const std::optional<ValidTime>& end, ValidTime point) {
  return !end.has_value() || Before(point, *end);
}

bool IntervalValid(const ValidTimeInterval& interval) {
  return !interval.to.has_value() || Before(interval.from, *interval.to);
}

Status Publish(RelationalRow row, FragmentBudget* budget,
               size_t max_output_rows, BatchStream* output) {
  if (!output->rows.empty() &&
      output->rows.back().cells == row.cells &&
      output->rows.back().effective.has_value() && row.effective.has_value() &&
      output->rows.back().effective->to.has_value() &&
      *output->rows.back().effective->to == row.effective->from) {
    output->rows.back().effective->to = row.effective->to;
    return Status::OK();
  }
  if (output->rows.size() >= max_output_rows) {
    return TemporalBudgetExhausted(0, output->rows.size() + 1, 0,
                                   budget == nullptr ? 0
                                                      : budget->used_fragments(),
                                   "output row budget exceeded");
  }
  if (budget == nullptr || !budget->TryConsume()) {
    return TemporalBudgetExhausted(0, output->rows.size(), 0,
                                   budget == nullptr ? 0
                                                      : budget->limit_fragments(),
                                   "interval fragment budget exceeded");
  }
  output->rows.push_back(std::move(row));
  return Status::OK();
}

void SortAndCoalesceTemporalRows(std::vector<RelationalRow>* rows,
                                 size_t key_column) {
  StableInsertionSort(rows, [key_column](const RelationalRow& left,
                                         const RelationalRow& right) {
    return TemporalRowLess(left, right, key_column);
  });
  size_t output = 0;
  for (size_t input = 0; input < rows->size(); ++input) {
    if (output != 0 && (*rows)[output - 1].cells == (*rows)[input].cells &&
        (*rows)[output - 1].effective->to.has_value() &&
        *(*rows)[output - 1].effective->to == (*rows)[input].effective->from) {
      (*rows)[output - 1].effective->to = (*rows)[input].effective->to;
      continue;
    }
    if (output != input) (*rows)[output] = std::move((*rows)[input]);
    ++output;
  }
  rows->resize(output);
}

std::vector<ValidTimeInterval> NormalizeIntervals(
    std::vector<ValidTimeInterval> intervals);

StatusOr<std::pair<size_t, size_t>> CountCoalescedCoverageRows(
    const TemporalJoinInput& input) {
  BatchStream candidates;
  for (const RelationalRow& left : input.left.rows) {
    std::vector<ValidTimeInterval> overlaps;
    for (const RelationalRow& right : input.right.rows) {
      if (!KeysEqual(left.cells[input.left_key],
                     right.cells[input.right_key])) {
        continue;
      }
      auto intersection = Intersect(*left.effective, *right.effective);
      if (intersection) overlaps.push_back(*intersection);
    }
    overlaps = NormalizeIntervals(std::move(overlaps));
    if (input.kind == JoinKind::kSemi) {
      for (const ValidTimeInterval& interval : overlaps) {
        RelationalRow row = left;
        row.effective = interval;
        candidates.rows.push_back(std::move(row));
      }
      continue;
    }
    ValidTime cursor = left.effective->from;
    bool exhausted = false;
    for (const ValidTimeInterval& overlap : overlaps) {
      if (Before(cursor, overlap.from)) {
        RelationalRow row = left;
        row.effective = ValidTimeInterval{cursor, overlap.from};
        candidates.rows.push_back(std::move(row));
      }
      if (!overlap.to.has_value()) {
        exhausted = true;
        break;
      }
      if (Before(cursor, *overlap.to)) cursor = *overlap.to;
    }
    if (!exhausted && EndsAfter(left.effective->to, cursor)) {
      RelationalRow row = left;
      row.effective = ValidTimeInterval{cursor, left.effective->to};
      candidates.rows.push_back(std::move(row));
    }
  }
  SortAndCoalesceTemporalRows(&candidates.rows, input.left_key);
  return std::make_pair(candidates.rows.size(),
                        EstimateRowsBytes(candidates.rows));
}

std::vector<ValidTimeInterval> NormalizeIntervals(
    std::vector<ValidTimeInterval> intervals) {
  std::sort(intervals.begin(), intervals.end(), [](const auto& left,
                                                   const auto& right) {
    if (left.from.value != right.from.value) return left.from.value < right.from.value;
    if (!left.to.has_value()) return false;
    if (!right.to.has_value()) return true;
    return left.to->value < right.to->value;
  });
  std::vector<ValidTimeInterval> normalized;
  for (const ValidTimeInterval& interval : intervals) {
    if (normalized.empty()) {
      normalized.push_back(interval);
      continue;
    }
    ValidTimeInterval& previous = normalized.back();
    if (!previous.to.has_value() || interval.from.value <= previous.to->value) {
      if (!previous.to.has_value() ||
          (interval.to.has_value() && interval.to->value <= previous.to->value)) {
        continue;
      }
      previous.to = interval.to;
      continue;
    }
    normalized.push_back(interval);
  }
  return normalized;
}

// Counts the normalized coverage fragments for one left interval without
// allocating an intermediate intersection vector. The cursor advances over
// covered regions and uncovered gaps, so adjacent/overlapping intersections
// are counted exactly as Publish will coalesce them.
StatusOr<size_t> CountCoverageFragments(const RelationalRow& left,
                                        const BatchStream& right,
                                        size_t left_key, size_t right_key,
                                        JoinKind kind) {
  size_t fragments = 0;
  ValidTime cursor = left.effective->from;
  while (true) {
    bool coverage_found = false;
    std::optional<ValidTime> coverage_end;
    std::optional<ValidTime> next_start;
    for (const RelationalRow& candidate : right.rows) {
      if (!KeysEqual(left.cells[left_key], candidate.cells[right_key])) {
        continue;
      }
      const auto intersection =
          Intersect(*left.effective, *candidate.effective);
      if (!intersection) continue;
      if (!Before(cursor, intersection->from) &&
          EndsAfter(intersection->to, cursor)) {
        coverage_found = true;
        if (!coverage_end.has_value() || !intersection->to.has_value() ||
            Before(*coverage_end, *intersection->to)) {
          coverage_end = intersection->to;
        }
      } else if (Before(cursor, intersection->from) &&
                 (!next_start.has_value() ||
                  Before(intersection->from, *next_start))) {
        next_start = intersection->from;
      }
    }
    if (!coverage_found && next_start.has_value()) {
      if (kind == JoinKind::kAnti && Before(cursor, *next_start)) {
        if (fragments == std::numeric_limits<size_t>::max()) {
          return Status::ResourceExhausted("query",
                                           "temporal output row overflow");
        }
        ++fragments;
      }
      cursor = *next_start;
      continue;
    }
    if (coverage_found) {
      // Extend through every interval that overlaps the current covered
      // region; this is the allocation-free equivalent of NormalizeIntervals.
      while (coverage_end.has_value()) {
        std::optional<ValidTime> extended = coverage_end;
        for (const RelationalRow& candidate : right.rows) {
          if (!KeysEqual(left.cells[left_key], candidate.cells[right_key])) {
            continue;
          }
          const auto intersection =
              Intersect(*left.effective, *candidate.effective);
          if (!intersection || Before(*extended, intersection->from) ||
              !EndsAfter(intersection->to, *extended)) {
            continue;
          }
          if (!intersection->to.has_value() ||
              !extended.has_value() || Before(*extended, *intersection->to)) {
            extended = intersection->to;
          }
        }
        if (extended == coverage_end) break;
        coverage_end = extended;
      }
      if (fragments == std::numeric_limits<size_t>::max()) {
        return Status::ResourceExhausted("query", "temporal output row overflow");
      }
      if (kind == JoinKind::kSemi) ++fragments;
      if (!coverage_end.has_value()) break;
      cursor = *coverage_end;
      if (!EndsAfter(left.effective->to, cursor)) break;
      continue;
    }
    if (!next_start.has_value()) {
      if (kind == JoinKind::kAnti && EndsAfter(left.effective->to, cursor)) {
        if (fragments == std::numeric_limits<size_t>::max()) {
          return Status::ResourceExhausted("query", "temporal output row overflow");
        }
        ++fragments;
      }
      break;
    }
    if (kind == JoinKind::kAnti && Before(cursor, *next_start)) {
      if (fragments == std::numeric_limits<size_t>::max()) {
        return Status::ResourceExhausted("query", "temporal output row overflow");
      }
      ++fragments;
    }
    cursor = *next_start;
  }
  return fragments;
}

std::vector<RelationalCell> GroupCells(const RelationalRow& row,
                                       const std::vector<size_t>& group_by) {
  std::vector<RelationalCell> cells;
  cells.reserve(group_by.size());
  for (size_t column : group_by) cells.push_back(row.cells[column]);
  return cells;
}

struct Group {
  std::vector<RelationalCell> key;
  std::vector<const RelationalRow*> rows;
};

struct TemporalEvent {
  ValidTime time;
  int delta;
};

Status ValidateGroupBy(const BatchStream& input,
                       const std::vector<size_t>& group_by) {
  for (const RelationalRow& row : input.rows) {
    for (size_t column : group_by) {
      if (column >= row.cells.size()) {
        return Status::InvalidArgument("relational aggregate",
                                       "group column is out of range");
      }
    }
  }
  return Status::OK();
}

size_t EstimateGroupingScratch(const BatchStream& input,
                               const std::vector<size_t>& group_by,
                               size_t extra_bytes_per_row = 0) {
  size_t bytes = sizeof(std::vector<Group>);
  for (const RelationalRow& row : input.rows) {
    if (!AddBytes(sizeof(Group) + 2 * sizeof(const RelationalRow*) +
                      extra_bytes_per_row,
                  &bytes)) {
      return std::numeric_limits<size_t>::max();
    }
    for (size_t column : group_by) {
      if (!AddBytes(EstimateCellBytes(row.cells[column]), &bytes)) {
        return std::numeric_limits<size_t>::max();
      }
    }
  }
  return bytes;
}

StatusOr<std::vector<Group>> BuildGroups(const BatchStream& input,
                                         const std::vector<size_t>& group_by) {
  if (Status status = ValidateGroupBy(input, group_by); !status.ok()) {
    return status;
  }
  std::vector<Group> groups;
  groups.reserve(std::max(size_t{1}, input.rows.size()));
  for (const RelationalRow& row : input.rows) {
    std::vector<RelationalCell> key = GroupCells(row, group_by);
    auto found = std::find_if(groups.begin(), groups.end(),
                              [&key](const Group& group) {
                                return group.key == key;
                              });
    if (found == groups.end()) {
      groups.push_back({std::move(key), {&row}});
    } else {
      found->rows.push_back(&row);
    }
  }
  if (input.rows.empty() && group_by.empty()) groups.push_back({{}, {}});
  return groups;
}

size_t EstimateAggregateOutputBytes(
    const std::vector<Group>& groups,
    const std::vector<AggregateSpec>& aggregates) {
  size_t bytes = sizeof(BatchStream);
  for (const Group& group : groups) {
    size_t row_bytes = sizeof(RelationalRow);
    for (const RelationalCell& cell : group.key) {
      if (!AddBytes(EstimateCellBytes(cell), &row_bytes)) {
        return std::numeric_limits<size_t>::max();
      }
    }
    for (const AggregateSpec& spec : aggregates) {
      size_t cell_bytes = sizeof(RelationalCell);
      if (spec.kind == AggregateKind::kMin ||
          spec.kind == AggregateKind::kMax) {
        for (const RelationalRow* row : group.rows) {
          if (spec.input_column >= row->cells.size()) continue;
          cell_bytes = std::max(
              cell_bytes, EstimateCellBytes(row->cells[spec.input_column]));
        }
      }
      if (!AddBytes(cell_bytes, &row_bytes)) {
        return std::numeric_limits<size_t>::max();
      }
    }
    if (!AddBytes(row_bytes, &bytes)) {
      return std::numeric_limits<size_t>::max();
    }
  }
  return bytes;
}

bool SameGroup(const RelationalRow& left, const RelationalRow& right,
               const std::vector<size_t>& group_by) {
  for (size_t column : group_by) {
    if (left.cells[column] != right.cells[column]) return false;
  }
  return true;
}

StatusOr<size_t> CountTemporalGroupFragments(
    const BatchStream& input, const std::vector<size_t>& group_by,
    size_t representative) {
  std::optional<ValidTime> cursor;
  for (const RelationalRow& row : input.rows) {
    if (!SameGroup(input.rows[representative], row, group_by)) continue;
    if (!cursor.has_value() || Before(row.effective->from, *cursor)) {
      cursor = row.effective->from;
    }
  }
  size_t fragments = 0;
  std::optional<int64_t> last_count;
  while (cursor.has_value()) {
    int64_t count = 0;
    std::optional<ValidTime> next;
    for (const RelationalRow& row : input.rows) {
      if (!SameGroup(input.rows[representative], row, group_by)) continue;
      if (!Before(*cursor, row.effective->from) &&
          (!row.effective->to.has_value() ||
           Before(*cursor, *row.effective->to))) {
        ++count;
      }
      if (Before(*cursor, row.effective->from) &&
          (!next.has_value() || Before(row.effective->from, *next))) {
        next = row.effective->from;
      }
      if (row.effective->to.has_value() &&
          Before(*cursor, *row.effective->to) &&
          (!next.has_value() || Before(*row.effective->to, *next))) {
        next = *row.effective->to;
      }
    }
    if (count > 0) {
      if (!last_count.has_value() || *last_count != count) {
        if (fragments == std::numeric_limits<size_t>::max()) {
          return Status::ResourceExhausted("query",
                                           "temporal output row overflow");
        }
        ++fragments;
      }
      last_count = count;
    } else {
      last_count.reset();
    }
    if (!next.has_value()) break;
    cursor = next;
  }
  return fragments;
}

StatusOr<size_t> EstimateTemporalAggregateOutputBytesExact(
    const BatchStream& input, const std::vector<size_t>& group_by,
    size_t* row_bound) {
  size_t bytes = sizeof(BatchStream);
  *row_bound = 0;
  for (size_t representative = 0; representative < input.rows.size();
       ++representative) {
    bool already_seen = false;
    for (size_t prior = 0; prior < representative; ++prior) {
      if (SameGroup(input.rows[representative], input.rows[prior], group_by)) {
        already_seen = true;
        break;
      }
    }
    if (already_seen) continue;
    for (const RelationalRow& row : input.rows) {
      if (!SameGroup(input.rows[representative], row, group_by) ||
          !row.effective || !IntervalValid(*row.effective)) {
        return Status::InvalidArgument("temporal aggregate",
                                       "input interval is absent or invalid");
      }
    }
    auto group_fragments =
        CountTemporalGroupFragments(input, group_by, representative);
    if (!group_fragments.ok()) return group_fragments.status();
    if (group_fragments.ValueOrDie() >
        std::numeric_limits<size_t>::max() - *row_bound) {
      return Status::ResourceExhausted("query", "temporal output row overflow");
    }
    *row_bound += group_fragments.ValueOrDie();
    size_t row_bytes = sizeof(RelationalRow) + sizeof(RelationalCell);
    for (size_t column : group_by) {
      const RelationalCell& cell = input.rows[representative].cells[column];
      if (!AddBytes(EstimateCellBytes(cell), &row_bytes)) {
        return std::numeric_limits<size_t>::max();
      }
    }
    if (group_fragments.ValueOrDie() != 0 &&
        row_bytes >
            (std::numeric_limits<size_t>::max() - bytes) /
                group_fragments.ValueOrDie()) {
      return Status::ResourceExhausted("query", "temporal output byte overflow");
    }
    bytes += row_bytes * group_fragments.ValueOrDie();
  }
  return bytes;
}

StatusOr<RelationalCell> AggregateOne(const Group& group,
                                      const AggregateSpec& spec) {
  if (spec.kind == AggregateKind::kCount) {
    int64_t count = 0;
    for (const RelationalRow* row : group.rows) {
      if (spec.input_column >= row->cells.size()) {
        return Status::InvalidArgument("row aggregate",
                                       "aggregate column is out of range");
      }
      if (row->cells[spec.input_column].present) ++count;
    }
    return RelationalCell::Present(QueryType::kInt64, count);
  }

  const RelationalCell* accumulated = nullptr;
  int64_t int64_sum = 0;
  double double_sum = 0;
  for (const RelationalRow* row : group.rows) {
    if (spec.input_column >= row->cells.size()) {
      return Status::InvalidArgument("row aggregate",
                                     "aggregate column is out of range");
    }
    const RelationalCell& cell = row->cells[spec.input_column];
    if (!cell.present) continue;
    if (accumulated != nullptr && accumulated->type != cell.type) {
      return Status::InvalidArgument("row aggregate",
                                     "aggregate input types differ");
    }
    if (spec.kind == AggregateKind::kSum) {
      if (cell.type == QueryType::kInt64) {
        const int64_t value = std::get<int64_t>(cell.value);
        if ((value > 0 && int64_sum > std::numeric_limits<int64_t>::max() - value) ||
            (value < 0 && int64_sum < std::numeric_limits<int64_t>::min() - value)) {
          return Status::NumericOverflow("row aggregate", "sum overflow");
        }
        int64_sum += value;
      } else if (cell.type == QueryType::kFloat64) {
        double_sum += std::get<double>(cell.value);
      } else {
        return Status::InvalidArgument("row aggregate",
                                       "sum requires exact int64 or float64 input");
      }
    } else if (accumulated == nullptr ||
               (spec.kind == AggregateKind::kMin &&
                CompareCell(cell, *accumulated) < 0) ||
               (spec.kind == AggregateKind::kMax &&
                CompareCell(cell, *accumulated) > 0)) {
      accumulated = &cell;
    }
    if (accumulated == nullptr) accumulated = &cell;
  }
  if (spec.kind == AggregateKind::kSum) {
    if (accumulated == nullptr) {
      return RelationalCell::Missing(QueryType::kInt64);
    }
    return accumulated->type == QueryType::kInt64
               ? RelationalCell::Present(QueryType::kInt64, int64_sum)
               : RelationalCell::Present(QueryType::kFloat64, double_sum);
  }
  return accumulated != nullptr ? *accumulated
                                : RelationalCell::Missing(QueryType::kInt64);
}

}  // namespace

QueryReservation::QueryReservation(size_t limit_bytes)
    : state_(std::make_shared<State>(limit_bytes)) {}

bool QueryReservation::TryGrow(size_t bytes) {
  if (bytes > state_->limit_bytes - state_->used_bytes) return false;
  state_->used_bytes += bytes;
  state_->peak_bytes = std::max(state_->peak_bytes, state_->used_bytes);
  return true;
}

void QueryReservation::Release(size_t bytes) {
  state_->used_bytes =
      bytes >= state_->used_bytes ? 0 : state_->used_bytes - bytes;
}

std::shared_ptr<QueryReservationLease> QueryReservation::TryRetain(
    size_t bytes) {
  if (!TryGrow(bytes)) return {};
  try {
    return std::shared_ptr<QueryReservationLease>(
        new QueryReservationLease(state_, bytes));
  } catch (...) {
    Release(bytes);
    throw;
  }
}

size_t QueryReservation::limit_bytes() const { return state_->limit_bytes; }
size_t QueryReservation::used_bytes() const { return state_->used_bytes; }
size_t QueryReservation::peak_bytes() const { return state_->peak_bytes; }

QueryReservationLease::QueryReservationLease(
    std::shared_ptr<QueryReservation::State> state, size_t bytes)
    : state_(std::move(state)), bytes_(bytes) {}

QueryReservationLease::~QueryReservationLease() {
  state_->used_bytes =
      bytes_ >= state_->used_bytes ? 0 : state_->used_bytes - bytes_;
}

bool FragmentBudget::TryConsume(size_t fragments) {
  if (fragments > limit_ - used_) return false;
  used_ += fragments;
  return true;
}

Status NeedsSpill() {
  return Status::ResourceExhausted("query relational",
                                   "NeedsSpill memory_bytes=unknown");
}

Status NeedsSpill(size_t requested_bytes, size_t available_bytes) {
  return Status::ResourceExhausted(
      "query relational", "NeedsSpill memory_bytes=" +
                              std::to_string(requested_bytes) +
                              " available_bytes=" +
                              std::to_string(available_bytes));
}

bool IsNeedsSpill(const Status& status) {
  return status.IsResourceExhausted() &&
         status.ToString().find("NeedsSpill") != std::string::npos;
}

StatusOr<BatchStream> UnionAll(const BatchStream& left,
                               const BatchStream& right,
                               QueryReservation* reservation,
                               size_t max_output_rows) {
  if (right.rows.size() >
      std::numeric_limits<size_t>::max() - left.rows.size()) {
    return MemoryExhausted(std::numeric_limits<size_t>::max(),
                           reservation == nullptr ? 0
                                                  : AvailableBytes(*reservation));
  }
  const size_t row_count = left.rows.size() + right.rows.size();
  if (row_count > max_output_rows) return OutputRowsExhausted();
  const size_t bytes = EstimateRowsBytes(left.rows, &right.rows);
  auto lease = RetainOutput(reservation, bytes, false);
  if (!lease.ok()) return lease.status();
  BatchStream result;
  result.reservation_lease = std::move(lease).ConsumeValueOrDie();
  result.rows.reserve(row_count);
  result.rows.insert(result.rows.end(), left.rows.begin(), left.rows.end());
  result.rows.insert(result.rows.end(), right.rows.begin(), right.rows.end());
  return result;
}

StatusOr<BatchStream> Distinct(const BatchStream& input,
                               QueryReservation* reservation,
                               size_t max_output_rows) {
  size_t row_count = 0;
  size_t bytes = sizeof(BatchStream);
  for (size_t row_index = 0; row_index < input.rows.size(); ++row_index) {
    const RelationalRow& row = input.rows[row_index];
    bool duplicate = false;
    for (size_t previous = 0; previous < row_index; ++previous) {
      if (input.rows[previous] == row) {
        duplicate = true;
        break;
      }
    }
    if (duplicate) continue;
    if (++row_count > max_output_rows) return OutputRowsExhausted();
    if (!AddBytes(EstimateRowBytes(row), &bytes)) {
      return MemoryExhausted(std::numeric_limits<size_t>::max(),
                             reservation == nullptr
                                 ? 0
                                 : AvailableBytes(*reservation));
    }
  }
  auto lease = RetainOutput(reservation, bytes, false);
  if (!lease.ok()) return lease.status();
  BatchStream result;
  result.reservation_lease = std::move(lease).ConsumeValueOrDie();
  result.rows.reserve(row_count);
  for (const RelationalRow& row : input.rows) {
    if (std::find(result.rows.begin(), result.rows.end(), row) ==
        result.rows.end()) {
      result.rows.push_back(row);
    }
  }
  return result;
}

StatusOr<BatchStream> Sort(const BatchStream& input,
                           const std::vector<SortKey>& keys,
                           QueryReservation* reservation,
                           size_t max_output_rows) {
  if (Status status = ValidateSort(input, keys); !status.ok()) return status;
  if (input.rows.size() > max_output_rows) return OutputRowsExhausted();
  auto lease = RetainOutput(reservation, EstimateBytes(input), true);
  if (!lease.ok()) return lease.status();
  BatchStream result;
  result.reservation_lease = std::move(lease).ConsumeValueOrDie();
  result.rows = input.rows;
  // Stable insertion sort only swaps adjacent moved rows, so it needs no
  // unreserved auxiliary buffer after the copied stream has been accounted.
  for (size_t i = 1; i < result.rows.size(); ++i) {
    for (size_t j = i; j > 0 && RowLess(result.rows[j], result.rows[j - 1], keys);
         --j) {
      std::swap(result.rows[j], result.rows[j - 1]);
    }
  }
  result.order_specified = true;
  return result;
}

StatusOr<BatchStream> Limit(const BatchStream& input, size_t offset,
                            size_t count, QueryReservation* reservation,
                            size_t max_output_rows) {
  const size_t available =
      offset >= input.rows.size() ? 0 : input.rows.size() - offset;
  const size_t emitted = std::min(count, available);
  if (emitted > max_output_rows) return OutputRowsExhausted();
  size_t bytes = sizeof(BatchStream);
  for (size_t row = offset; row < offset + emitted; ++row) {
    if (!AddBytes(EstimateRowBytes(input.rows[row]), &bytes)) {
      return MemoryExhausted(std::numeric_limits<size_t>::max(),
                             reservation == nullptr
                                 ? 0
                                 : AvailableBytes(*reservation));
    }
  }
  auto lease = RetainOutput(reservation, bytes, false);
  if (!lease.ok()) return lease.status();
  BatchStream result;
  result.order_specified = input.order_specified;
  result.reservation_lease = std::move(lease).ConsumeValueOrDie();
  result.rows.reserve(emitted);
  if (emitted != 0) {
    result.rows.insert(result.rows.end(), input.rows.begin() + offset,
                       input.rows.begin() + offset + emitted);
  }
  return result;
}

JoinAlgorithm ChooseJoinAlgorithm(size_t estimated_rows, bool sorted_keys,
                                  bool temporal) {
  if (temporal) return JoinAlgorithm::kIntervalMerge;
  if (estimated_rows < 4096) return JoinAlgorithm::kIndexNestedLoop;
  return sorted_keys ? JoinAlgorithm::kSortMerge : JoinAlgorithm::kHash;
}

StatusOr<BatchStream> IndexNestedLoopJoin(const JoinInput& input,
                                          QueryReservation* reservation,
                                          size_t max_output_rows) {
  if (Status status = ValidateJoin(input); !status.ok()) return status;
  const auto output = EstimateJoinOutput(input);
  if (!output.has_value()) {
    return Status::ResourceExhausted("relational join",
                                     "output size overflows");
  }
  if (output->rows > max_output_rows) return OutputRowsExhausted();
  auto lease = RetainOutput(reservation, output->bytes, false);
  if (!lease.ok()) return lease.status();
  BatchStream result;
  result.reservation_lease = std::move(lease).ConsumeValueOrDie();
  result.rows.reserve(output->rows);
  for (const RelationalRow& left : input.left.rows) {
    bool matched = false;
    for (const RelationalRow& right : input.right.rows) {
      if (!KeysEqual(left.cells[input.left_key],
                     right.cells[input.right_key])) {
        continue;
      }
      matched = true;
      if (input.kind == JoinKind::kInner) {
        result.rows.push_back(Combine(left, right));
      } else if (input.kind == JoinKind::kSemi) {
        result.rows.push_back(left);
        break;
      }
    }
    if (!matched && input.kind == JoinKind::kAnti) result.rows.push_back(left);
  }
  return result;
}

StatusOr<BatchStream> HashJoin(const JoinInput& input,
                               QueryReservation* reservation,
                               size_t max_output_rows) {
  if (Status status = ValidateJoin(input); !status.ok()) return status;
  if (reservation == nullptr) return NeedsSpill();
  const auto bucket_count = HashBucketCount(input.right.rows.size());
  if (!bucket_count.has_value() ||
      input.right.rows.size() >
          std::numeric_limits<size_t>::max() / sizeof(HashEntry)) {
    return NeedsSpill();
  }
  size_t index_bytes = input.right.rows.size() * sizeof(HashEntry);
  if (*bucket_count > std::numeric_limits<size_t>::max() / sizeof(size_t) ||
      !AddBytes(*bucket_count * sizeof(size_t), &index_bytes)) {
    return NeedsSpill();
  }
  ReservationGuard index_guard(reservation, index_bytes);
  if (!index_guard.acquired()) {
    return NeedsSpill(index_bytes, reservation->limit_bytes() -
                                      reservation->used_bytes());
  }

  std::vector<size_t> buckets(*bucket_count, kNoHashEntry);
  std::vector<HashEntry> table;
  table.reserve(input.right.rows.size());
  for (const RelationalRow& right : input.right.rows) {
    const RelationalCell& key = right.cells[input.right_key];
    if (!key.present) continue;
    const size_t bucket = HashCell(key) & (*bucket_count - 1);
    table.push_back({&right, buckets[bucket]});
    buckets[bucket] = table.size() - 1;
  }
  auto output = EstimateHashOutput(input, buckets, table);
  if (!output.ok()) return output.status();
  if (output.ValueOrDie().rows > max_output_rows) return OutputRowsExhausted();
  auto lease = RetainOutput(reservation, output.ValueOrDie().bytes, true);
  if (!lease.ok()) return lease.status();
  BatchStream result;
  result.reservation_lease = std::move(lease).ConsumeValueOrDie();
  result.rows.reserve(output.ValueOrDie().rows);
  for (const RelationalRow& left : input.left.rows) {
    bool matched = false;
    const RelationalCell& key = left.cells[input.left_key];
    if (key.present) {
      for (size_t entry = buckets[HashCell(key) & (*bucket_count - 1)];
           entry != kNoHashEntry; entry = table[entry].next) {
        const RelationalRow* right = table[entry].row;
        if (!KeysEqual(key, right->cells[input.right_key])) continue;
        matched = true;
        if (input.kind == JoinKind::kInner) {
          result.rows.push_back(Combine(left, *right));
        } else if (input.kind == JoinKind::kSemi) {
          result.rows.push_back(left);
          break;
        }
        if (input.kind == JoinKind::kSemi && matched) break;
      }
    }
    if (!matched && input.kind == JoinKind::kAnti) result.rows.push_back(left);
  }
  return result;
}

StatusOr<BatchStream> SortMergeJoin(const JoinInput& input,
                                    QueryReservation* reservation,
                                    size_t max_output_rows) {
  if (Status status = ValidateJoin(input); !status.ok()) return status;
  if (reservation == nullptr) return NeedsSpill();
  size_t input_bytes = EstimateBytes(input.left);
  const size_t right_bytes = EstimateBytes(input.right);
  if (!AddBytes(right_bytes, &input_bytes)) {
    return NeedsSpill();
  }
  ReservationGuard input_guard(reservation, input_bytes);
  if (!input_guard.acquired()) {
    return NeedsSpill(input_bytes, reservation->limit_bytes() -
                                        reservation->used_bytes());
  }
  std::vector<RelationalRow> left = input.left.rows;
  std::vector<RelationalRow> right = input.right.rows;
  const auto left_less = [&input](const RelationalRow& first,
                                  const RelationalRow& second) {
    return CompareCell(first.cells[input.left_key],
                       second.cells[input.left_key]) < 0;
  };
  const auto right_less = [&input](const RelationalRow& first,
                                   const RelationalRow& second) {
    return CompareCell(first.cells[input.right_key],
                       second.cells[input.right_key]) < 0;
  };
  std::sort(left.begin(), left.end(), left_less);
  std::sort(right.begin(), right.end(), right_less);
  auto output = EstimateSortedMergeOutput(left, right, input);
  if (!output.ok()) return output.status();
  if (output.ValueOrDie().rows > max_output_rows) return OutputRowsExhausted();
  auto lease = RetainOutput(reservation, output.ValueOrDie().bytes, true);
  if (!lease.ok()) return lease.status();

  BatchStream result;
  result.reservation_lease = std::move(lease).ConsumeValueOrDie();
  result.rows.reserve(output.ValueOrDie().rows);
  size_t left_index = 0;
  size_t right_index = 0;
  while (left_index < left.size() && right_index < right.size()) {
    const int compared = CompareCell(left[left_index].cells[input.left_key],
                                     right[right_index].cells[input.right_key]);
    if (compared < 0) {
      if (input.kind == JoinKind::kAnti) result.rows.push_back(left[left_index]);
      ++left_index;
      continue;
    }
    if (compared > 0) {
      ++right_index;
      continue;
    }
    size_t left_end = left_index + 1;
    while (left_end < left.size() &&
           CompareCell(left[left_index].cells[input.left_key],
                       left[left_end].cells[input.left_key]) == 0) {
      ++left_end;
    }
    size_t right_end = right_index + 1;
    while (right_end < right.size() &&
           CompareCell(right[right_index].cells[input.right_key],
                       right[right_end].cells[input.right_key]) == 0) {
      ++right_end;
    }
    if (KeysEqual(left[left_index].cells[input.left_key],
                  right[right_index].cells[input.right_key])) {
      for (size_t left_row = left_index; left_row < left_end; ++left_row) {
        if (input.kind == JoinKind::kInner) {
          for (size_t right_row = right_index; right_row < right_end; ++right_row) {
            result.rows.push_back(Combine(left[left_row], right[right_row]));
          }
        } else if (input.kind == JoinKind::kSemi) {
          result.rows.push_back(left[left_row]);
        }
      }
    } else if (input.kind == JoinKind::kAnti) {
      result.rows.insert(result.rows.end(), left.begin() + left_index,
                         left.begin() + left_end);
    }
    left_index = left_end;
    right_index = right_end;
  }
  if (input.kind == JoinKind::kAnti) {
    result.rows.insert(result.rows.end(), left.begin() + left_index, left.end());
  }
  result.order_specified = true;
  return result;
}

StatusOr<BatchStream> IntervalMergeJoin(const TemporalJoinInput& input,
                                        FragmentBudget* budget,
                                        QueryReservation* reservation,
                                        size_t max_output_rows) {
  if (Status status = ValidateJoinStreams(input.left, input.right,
                                          input.left_key, input.right_key);
      !status.ok()) {
    return status;
  }
  if (budget == nullptr) {
    return Status::InvalidArgument("temporal join",
                                   "fragment budget is required");
  }
  for (const RelationalRow& row : input.left.rows) {
    if (!row.effective || !IntervalValid(*row.effective)) {
      return Status::InvalidArgument("temporal join",
                                     "left interval is absent or invalid");
    }
  }
  for (const RelationalRow& row : input.right.rows) {
    if (!row.effective || !IntervalValid(*row.effective)) {
      return Status::InvalidArgument("temporal join",
                                     "right interval is absent or invalid");
    }
  }
  size_t output_rows = 0;
  size_t output_bytes = sizeof(BatchStream);
  std::unique_ptr<ReservationGuard> preflight_scratch_guard;
  size_t scratch_bytes = 0;
  for (const RelationalRow& left : input.left.rows) {
    if (input.kind == JoinKind::kInner) {
      for (const RelationalRow& right : input.right.rows) {
        if (!KeysEqual(left.cells[input.left_key],
                       right.cells[input.right_key]) ||
            !Intersect(*left.effective, *right.effective).has_value()) {
          continue;
        }
        if (!AddBytes(EstimateCombinedRowBytes(left, right), &output_bytes) ||
            output_rows == std::numeric_limits<size_t>::max()) {
          return MemoryExhausted(std::numeric_limits<size_t>::max(),
                                 reservation == nullptr
                                     ? 0
                                     : AvailableBytes(*reservation));
        }
        ++output_rows;
      }
    } else {
      auto fragments = CountCoverageFragments(
          left, input.right, input.left_key, input.right_key, input.kind);
      if (!fragments.ok()) return fragments.status();
      if (fragments.ValueOrDie() >
          std::numeric_limits<size_t>::max() - output_rows) {
        return MemoryExhausted(std::numeric_limits<size_t>::max(),
                               reservation == nullptr
                                   ? 0
                                   : AvailableBytes(*reservation));
      }
      if (fragments.ValueOrDie() != 0 &&
          EstimateRowBytes(left) >
              (std::numeric_limits<size_t>::max() - output_bytes) /
                  fragments.ValueOrDie()) {
        return MemoryExhausted(std::numeric_limits<size_t>::max(),
                               reservation == nullptr
                                   ? 0
                                   : AvailableBytes(*reservation));
      }
      output_rows += fragments.ValueOrDie();
      output_bytes += EstimateRowBytes(left) * fragments.ValueOrDie();
    }
  }
  if (input.kind != JoinKind::kInner && output_rows > max_output_rows) {
    scratch_bytes = EstimateBytes(input.left);
    if (!AddBytes(EstimateBytes(input.right), &scratch_bytes) ||
        !AddBytes(sizeof(std::vector<ValidTimeInterval>) * 2,
                  &scratch_bytes) ||
        input.right.rows.size() >
            std::numeric_limits<size_t>::max() /
                (2 * sizeof(ValidTimeInterval)) ||
        !AddBytes(input.right.rows.size() * 2 * sizeof(ValidTimeInterval),
                  &scratch_bytes)) {
      return MemoryExhausted(std::numeric_limits<size_t>::max(),
                             reservation == nullptr
                                 ? 0
                                 : AvailableBytes(*reservation));
    }
    if (reservation == nullptr) {
      return TemporalBudgetExhausted(scratch_bytes, output_rows, output_bytes,
                                     budget->limit_fragments(),
                                     "memory budget exceeded");
    }
    preflight_scratch_guard =
        std::make_unique<ReservationGuard>(reservation, scratch_bytes);
    if (!preflight_scratch_guard->acquired()) {
      return TemporalBudgetExhausted(scratch_bytes, output_rows, output_bytes,
                                     budget->limit_fragments(),
                                     "memory budget exceeded");
    }
    auto coalesced = CountCoalescedCoverageRows(input);
    if (!coalesced.ok()) return coalesced.status();
    output_rows = coalesced.ValueOrDie().first;
    output_bytes = coalesced.ValueOrDie().second;
  }
  if (output_rows > max_output_rows) {
    return TemporalBudgetExhausted(0, output_rows, output_bytes,
                                   budget->limit_fragments(),
                                   "output row budget exceeded");
  }
  if (budget->limit_fragments() == 0 && output_rows != 0) {
    return TemporalBudgetExhausted(0, output_rows, output_bytes, output_rows,
                                   "interval fragment budget exceeded");
  }
  if (output_rows == 0) {
    BatchStream empty;
    empty.order_specified = true;
    return empty;
  }

  if (preflight_scratch_guard == nullptr) {
    scratch_bytes = EstimateBytes(input.left);
    if (!AddBytes(EstimateBytes(input.right), &scratch_bytes) ||
        !AddBytes(sizeof(std::vector<ValidTimeInterval>) * 2,
                  &scratch_bytes) ||
        input.right.rows.size() >
            std::numeric_limits<size_t>::max() /
                (2 * sizeof(ValidTimeInterval)) ||
        !AddBytes(input.right.rows.size() * 2 * sizeof(ValidTimeInterval),
                  &scratch_bytes)) {
      return MemoryExhausted(std::numeric_limits<size_t>::max(),
                             reservation == nullptr
                                 ? 0
                                 : AvailableBytes(*reservation));
    }
    if (reservation == nullptr) {
      return TemporalBudgetExhausted(output_bytes, output_rows, output_bytes,
                                     budget->limit_fragments(),
                                     "memory budget exceeded");
    }
    preflight_scratch_guard =
        std::make_unique<ReservationGuard>(reservation, scratch_bytes);
    if (!preflight_scratch_guard->acquired()) {
      return TemporalBudgetExhausted(scratch_bytes, output_rows, output_bytes,
                                     budget->limit_fragments(),
                                     "memory budget exceeded");
    }
  }
  std::vector<RelationalRow> left_rows = input.left.rows;
  std::vector<RelationalRow> right_rows = input.right.rows;
  StableInsertionSort(&left_rows,
                      [&input](const RelationalRow& left,
                               const RelationalRow& right) {
                        return TemporalRowLess(left, right, input.left_key);
                      });
  StableInsertionSort(&right_rows,
                      [&input](const RelationalRow& left,
                               const RelationalRow& right) {
                        return TemporalRowLess(left, right, input.right_key);
                      });
  auto output_lease = RetainOutput(reservation, output_bytes, false);
  if (!output_lease.ok()) return output_lease.status();

  BatchStream result;
  result.order_specified = true;
  result.reservation_lease = std::move(output_lease).ConsumeValueOrDie();
  result.rows.reserve(output_rows);
  for (const RelationalRow& left : left_rows) {
    std::vector<ValidTimeInterval> overlaps;
    overlaps.reserve(input.right.rows.size());
    for (const RelationalRow& right : right_rows) {
      if (!KeysEqual(left.cells[input.left_key],
                     right.cells[input.right_key])) {
        continue;
      }
      auto intersection = Intersect(*left.effective, *right.effective);
      if (!intersection) continue;
      if (input.kind == JoinKind::kInner) {
        RelationalRow row = Combine(left, right);
        row.effective = *intersection;
        if (Status status =
                Publish(std::move(row), budget, max_output_rows, &result);
            !status.ok()) {
          return status;
        }
      } else {
        overlaps.push_back(*intersection);
      }
    }
    if (input.kind == JoinKind::kSemi) {
      overlaps = NormalizeIntervals(std::move(overlaps));
      for (const auto& overlap : overlaps) {
        RelationalRow row = left;
        row.effective = overlap;
        if (Status status =
                Publish(std::move(row), budget, max_output_rows, &result);
            !status.ok()) {
          return status;
        }
      }
    } else if (input.kind == JoinKind::kAnti) {
      overlaps = NormalizeIntervals(std::move(overlaps));
      ValidTime cursor = left.effective->from;
      bool exhausted = false;
      for (const ValidTimeInterval& overlap : overlaps) {
        if (Before(cursor, overlap.from)) {
          RelationalRow row = left;
          row.effective = ValidTimeInterval{cursor, overlap.from};
          if (Status status =
                  Publish(std::move(row), budget, max_output_rows, &result);
              !status.ok()) {
            return status;
          }
        }
        if (!overlap.to.has_value()) {
          exhausted = true;
          break;
        }
        if (Before(cursor, *overlap.to)) cursor = *overlap.to;
      }
      if (!exhausted && EndsAfter(left.effective->to, cursor)) {
        RelationalRow row = left;
        row.effective = ValidTimeInterval{cursor, left.effective->to};
        if (Status status =
                Publish(std::move(row), budget, max_output_rows, &result);
            !status.ok()) {
          return status;
        }
      }
    }
  }
  SortAndCoalesceTemporalRows(&result.rows, input.left_key);
  return result;
}

StatusOr<BatchStream> AggregateRows(const AggregateInput& input,
                                    QueryReservation* reservation,
                                    size_t max_output_rows) {
  if (Status status = ValidateGroupBy(input.input, input.group_by);
      !status.ok()) {
    return status;
  }
  const size_t scratch_bytes =
      EstimateGroupingScratch(input.input, input.group_by);
  if (reservation == nullptr) {
    return TemporalBudgetExhausted(scratch_bytes, 0, 0,
                                   0,
                                   "memory budget exceeded");
  }
  ReservationGuard scratch_guard(reservation, scratch_bytes);
  if (!scratch_guard.acquired()) {
    return TemporalBudgetExhausted(scratch_bytes, 0, 0,
                                   0,
                                   "memory budget exceeded");
  }
  auto groups = BuildGroups(input.input, input.group_by);
  if (!groups.ok()) return groups.status();
  if (groups.ValueOrDie().size() > max_output_rows) {
    return OutputRowsExhausted();
  }
  const size_t output_bytes =
      EstimateAggregateOutputBytes(groups.ValueOrDie(), input.aggregates);
  auto lease = RetainOutput(reservation, output_bytes, false);
  if (!lease.ok()) {
    return TemporalBudgetExhausted(output_bytes, groups.ValueOrDie().size(),
                                   output_bytes, 0,
                                   "memory budget exceeded");
  }
  BatchStream result;
  result.reservation_lease = std::move(lease).ConsumeValueOrDie();
  result.rows.reserve(groups.ValueOrDie().size());
  for (const Group& group : groups.ValueOrDie()) {
    RelationalRow output{group.key, std::nullopt};
    for (const AggregateSpec& spec : input.aggregates) {
      auto aggregate = AggregateOne(group, spec);
      if (!aggregate.ok()) return aggregate.status();
      output.cells.push_back(std::move(aggregate).ConsumeValueOrDie());
    }
    result.rows.push_back(std::move(output));
  }
  return result;
}

StatusOr<BatchStream> TemporalAggregate(const TemporalAggregateInput& input,
                                        FragmentBudget* budget,
                                        QueryReservation* reservation,
                                        size_t max_output_rows) {
  if (budget == nullptr) {
    return Status::InvalidArgument("temporal aggregate",
                                   "fragment budget is required");
  }
  if (Status status = ValidateGroupBy(input.input, input.group_by);
      !status.ok()) {
    return status;
  }
  const size_t scratch_bytes = EstimateGroupingScratch(
      input.input, input.group_by, 2 * sizeof(TemporalEvent));
  size_t output_rows = 0;
  auto output_estimate = EstimateTemporalAggregateOutputBytesExact(
      input.input, input.group_by, &output_rows);
  if (!output_estimate.ok()) return output_estimate.status();
  const size_t output_bytes = output_estimate.ValueOrDie();
  if (output_rows > max_output_rows) {
    return TemporalBudgetExhausted(0, output_rows, output_bytes,
                                   budget->limit_fragments(),
                                   "output row budget exceeded");
  }
  if (budget->limit_fragments() == 0 && output_rows != 0) {
    return TemporalBudgetExhausted(0, output_rows, output_bytes,
                                   output_rows,
                                   "interval fragment budget exceeded");
  }
  if (output_rows == 0) {
    BatchStream empty;
    empty.order_specified = true;
    return empty;
  }
  if (reservation == nullptr) {
    return TemporalBudgetExhausted(scratch_bytes, 0, 0,
                                   budget->limit_fragments(),
                                   "memory budget exceeded");
  }
  ReservationGuard scratch_guard(reservation, scratch_bytes);
  if (!scratch_guard.acquired()) {
    return TemporalBudgetExhausted(scratch_bytes, 0, 0,
                                   budget->limit_fragments(),
                                   "memory budget exceeded");
  }
  auto groups = BuildGroups(input.input, input.group_by);
  if (!groups.ok()) return groups.status();
  auto lease = RetainOutput(reservation, output_bytes, false);
  if (!lease.ok()) {
    return TemporalBudgetExhausted(output_bytes, output_rows, output_bytes,
                                   budget->limit_fragments(),
                                   "memory budget exceeded");
  }
  BatchStream result;
  result.order_specified = true;
  result.reservation_lease = std::move(lease).ConsumeValueOrDie();
  result.rows.reserve(output_rows);
  for (const Group& group : groups.ValueOrDie()) {
    std::vector<TemporalEvent> events;
    events.reserve(group.rows.size() * 2);
    int unbounded = 0;
    for (const RelationalRow* row : group.rows) {
      if (!row->effective || !IntervalValid(*row->effective)) {
        return Status::InvalidArgument("temporal aggregate",
                                       "input interval is absent or invalid");
      }
      events.push_back({row->effective->from, 1});
      if (row->effective->to) {
        events.push_back({*row->effective->to, -1});
      } else {
        ++unbounded;
      }
    }
    std::sort(events.begin(), events.end(), [](const TemporalEvent& left,
                                               const TemporalEvent& right) {
      if (left.time.value != right.time.value) {
        return left.time.value < right.time.value;
      }
      return left.delta < right.delta;
    });
    if (events.empty()) continue;
    int64_t count = 0;
    ValidTime previous = events.front().time;
    size_t index = 0;
    while (index < events.size()) {
      const ValidTime time = events[index].time;
      if (Before(previous, time) && count > 0) {
        RelationalRow row{
            group.key, ValidTimeInterval{previous, time}};
        row.cells.push_back(
            RelationalCell::Present(QueryType::kInt64, count));
        if (Status status =
                Publish(std::move(row), budget, max_output_rows, &result);
            !status.ok()) {
          return status;
        }
      }
      while (index < events.size() && events[index].time == time &&
             events[index].delta < 0) {
        count += events[index++].delta;
      }
      while (index < events.size() && events[index].time == time) {
        count += events[index++].delta;
      }
      previous = time;
    }
    if (count > 0 && unbounded > 0) {
      RelationalRow row{group.key,
                        ValidTimeInterval{previous, std::nullopt}};
      row.cells.push_back(RelationalCell::Present(QueryType::kInt64, count));
      if (Status status =
              Publish(std::move(row), budget, max_output_rows, &result);
          !status.ok()) {
        return status;
      }
    }
  }
  return result;
}

}  // namespace cedar::internal
