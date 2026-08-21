// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/runtime/relational.h"

#include <algorithm>
#include <bit>
#include <functional>
#include <limits>
#include <numeric>
#include <utility>

#include "query/temporal/interval.h"

namespace cedar::internal {
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

Status ValidateJoin(const JoinInput& input) {
  if (Status status = ValidateKey(input.left, input.left_key); !status.ok()) {
    return status;
  }
  if (Status status = ValidateKey(input.right, input.right_key); !status.ok()) {
    return status;
  }
  for (const RelationalRow& left : input.left.rows) {
    for (const RelationalRow& right : input.right.rows) {
      if (left.cells[input.left_key].type !=
          right.cells[input.right_key].type) {
        return Status::InvalidArgument("relational join",
                                       "join key types differ");
      }
    }
  }
  return Status::OK();
}

bool KeysEqual(const RelationalCell& left, const RelationalCell& right) {
  return left.present && right.present && left.type == right.type &&
         left.value == right.value;
}

template <typename T>
int CompareSimple(const T& left, const T& right) {
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
          combine(std::hash<uint32_t>{}(std::bit_cast<uint32_t>(value)));
        } else if constexpr (std::is_same_v<T, double>) {
          combine(std::hash<uint64_t>{}(std::bit_cast<uint64_t>(value)));
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

Status Publish(RelationalRow row, FragmentBudget* budget, BatchStream* output) {
  if (!output->rows.empty() &&
      output->rows.back().cells == row.cells &&
      output->rows.back().effective.has_value() && row.effective.has_value() &&
      output->rows.back().effective->to.has_value() &&
      *output->rows.back().effective->to == row.effective->from) {
    output->rows.back().effective->to = row.effective->to;
    return Status::OK();
  }
  if (budget == nullptr || !budget->TryConsume()) {
    return Status::ResourceExhausted("query",
                                     "interval fragment budget exceeded");
  }
  output->rows.push_back(std::move(row));
  return Status::OK();
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

StatusOr<std::vector<Group>> BuildGroups(const BatchStream& input,
                                         const std::vector<size_t>& group_by) {
  for (const RelationalRow& row : input.rows) {
    for (size_t column : group_by) {
      if (column >= row.cells.size()) {
        return Status::InvalidArgument("relational aggregate",
                                       "group column is out of range");
      }
    }
  }
  std::vector<Group> groups;
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

bool QueryReservation::TryGrow(size_t bytes) {
  if (bytes > limit_bytes_ - used_bytes_) return false;
  used_bytes_ += bytes;
  peak_bytes_ = std::max(peak_bytes_, used_bytes_);
  return true;
}

void QueryReservation::Release(size_t bytes) {
  used_bytes_ = bytes >= used_bytes_ ? 0 : used_bytes_ - bytes;
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

BatchStream UnionAll(BatchStream left, BatchStream right) {
  left.rows.reserve(left.rows.size() + right.rows.size());
  std::move(right.rows.begin(), right.rows.end(),
            std::back_inserter(left.rows));
  left.order_specified = false;
  return left;
}

BatchStream Distinct(const BatchStream& input) {
  BatchStream result;
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
                           QueryReservation* reservation) {
  if (Status status = ValidateSort(input, keys); !status.ok()) return status;
  if (reservation == nullptr) return NeedsSpill();
  ReservationGuard guard(reservation, EstimateBytes(input));
  if (!guard.acquired()) {
    return NeedsSpill(EstimateBytes(input), reservation->limit_bytes() -
                                                reservation->used_bytes());
  }
  BatchStream result = input;
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

BatchStream Limit(const BatchStream& input, size_t offset, size_t count) {
  BatchStream result;
  result.order_specified = input.order_specified;
  if (offset >= input.rows.size()) return result;
  const size_t available = input.rows.size() - offset;
  const size_t emitted = std::min(count, available);
  result.rows.insert(result.rows.end(), input.rows.begin() + offset,
                     input.rows.begin() + offset + emitted);
  return result;
}

JoinAlgorithm ChooseJoinAlgorithm(size_t estimated_rows, bool sorted_keys,
                                  bool temporal) {
  if (temporal) return JoinAlgorithm::kIntervalMerge;
  if (estimated_rows < 4096) return JoinAlgorithm::kIndexNestedLoop;
  return sorted_keys ? JoinAlgorithm::kSortMerge : JoinAlgorithm::kHash;
}

StatusOr<BatchStream> IndexNestedLoopJoin(const JoinInput& input) {
  if (Status status = ValidateJoin(input); !status.ok()) return status;
  const auto output = EstimateJoinOutput(input);
  if (!output.has_value()) {
    return Status::ResourceExhausted("relational join",
                                     "output size overflows");
  }
  BatchStream result;
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
                               QueryReservation* reservation) {
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
  ReservationGuard output_guard(reservation, output.ValueOrDie().bytes);
  if (!output_guard.acquired()) {
    return NeedsSpill(output.ValueOrDie().bytes,
                      reservation->limit_bytes() - reservation->used_bytes());
  }
  BatchStream result;
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
                                    QueryReservation* reservation) {
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
  ReservationGuard output_guard(reservation, output.ValueOrDie().bytes);
  if (!output_guard.acquired()) {
    return NeedsSpill(output.ValueOrDie().bytes,
                      reservation->limit_bytes() - reservation->used_bytes());
  }

  BatchStream result;
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

StatusOr<BatchStream> IntervalMergeJoin(TemporalJoinInput input,
                                        FragmentBudget* budget) {
  JoinInput validation{input.left, input.right, input.left_key,
                       input.right_key, input.kind};
  if (Status status = ValidateJoin(validation); !status.ok()) return status;
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

  BatchStream result;
  result.order_specified = true;
  for (const RelationalRow& left : input.left.rows) {
    std::vector<ValidTimeInterval> overlaps;
    for (const RelationalRow& right : input.right.rows) {
      if (!KeysEqual(left.cells[input.left_key],
                     right.cells[input.right_key])) {
        continue;
      }
      auto intersection = Intersect(*left.effective, *right.effective);
      if (!intersection) continue;
      if (input.kind == JoinKind::kInner) {
        RelationalRow row = Combine(left, right);
        row.effective = *intersection;
        if (Status status = Publish(std::move(row), budget, &result);
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
        if (Status status = Publish(std::move(row), budget, &result);
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
          if (Status status = Publish(std::move(row), budget, &result);
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
        if (Status status = Publish(std::move(row), budget, &result);
            !status.ok()) {
          return status;
        }
      }
    }
  }
  return result;
}

StatusOr<BatchStream> AggregateRows(AggregateInput input) {
  auto groups = BuildGroups(input.input, input.group_by);
  if (!groups.ok()) return groups.status();
  BatchStream result;
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

StatusOr<BatchStream> TemporalAggregate(TemporalAggregateInput input,
                                        FragmentBudget* budget) {
  if (budget == nullptr) {
    return Status::InvalidArgument("temporal aggregate",
                                   "fragment budget is required");
  }
  auto groups = BuildGroups(input.input, input.group_by);
  if (!groups.ok()) return groups.status();
  BatchStream result;
  result.order_specified = true;
  struct Event {
    ValidTime time;
    int delta;
  };
  for (const Group& group : groups.ValueOrDie()) {
    std::vector<Event> events;
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
    std::sort(events.begin(), events.end(), [](const Event& left,
                                               const Event& right) {
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
        if (Status status = Publish(std::move(row), budget, &result);
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
      if (Status status = Publish(std::move(row), budget, &result);
          !status.ok()) {
        return status;
      }
    }
  }
  return result;
}

}  // namespace cedar::internal
