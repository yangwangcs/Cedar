// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/runtime/vector_kernels.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <type_traits>

namespace cedar::internal {
namespace {

size_t ValueCount(const QueryColumn& column) {
  return std::visit([](const auto& values) { return values.size(); },
                    column.values);
}

bool IsPresent(const QueryColumn& column, size_t row) {
  return column.present.empty() || column.present[row] != 0;
}

Status ValidateColumn(const QueryColumn& column) {
  const size_t size = ValueCount(column);
  if (!column.present.empty() && column.present.size() != size) {
    return Status::InvalidArgument("vector kernel",
                                   "presence and value lane sizes differ");
  }
  bool representation_matches = false;
  switch (column.type) {
    case QueryType::kBool:
      representation_matches =
          std::holds_alternative<std::vector<uint8_t>>(column.values);
      break;
    case QueryType::kInt32:
      representation_matches =
          std::holds_alternative<std::vector<int32_t>>(column.values);
      break;
    case QueryType::kInt64:
      representation_matches =
          std::holds_alternative<std::vector<int64_t>>(column.values);
      break;
    case QueryType::kFloat32:
      representation_matches =
          std::holds_alternative<std::vector<float>>(column.values);
      break;
    case QueryType::kFloat64:
      representation_matches =
          std::holds_alternative<std::vector<double>>(column.values);
      break;
    case QueryType::kTimestamp64:
      representation_matches =
          std::holds_alternative<std::vector<uint64_t>>(column.values);
      break;
    case QueryType::kString:
    case QueryType::kBinary:
      representation_matches =
          std::holds_alternative<std::vector<std::string>>(column.values);
      break;
    case QueryType::kVertexRef:
      representation_matches =
          std::holds_alternative<std::vector<VertexRef>>(column.values);
      break;
    case QueryType::kEdgeRef:
      representation_matches =
          std::holds_alternative<std::vector<EdgeRef>>(column.values);
      break;
    case QueryType::kValidTime:
      representation_matches =
          std::holds_alternative<std::vector<ValidTime>>(column.values);
      break;
    case QueryType::kValidDuration:
      representation_matches =
          std::holds_alternative<std::vector<ValidDuration>>(column.values);
      break;
    case QueryType::kCommitSeq:
      representation_matches =
          std::holds_alternative<std::vector<CommitSeq>>(column.values);
      break;
    case QueryType::kValidTimeInterval:
      representation_matches =
          std::holds_alternative<std::vector<ValidTimeInterval>>(column.values);
      break;
    case QueryType::kPath:
    case QueryType::kJourney:
      break;
  }
  if (!representation_matches) {
    return Status::InvalidArgument("vector kernel",
                                   "column type and value lanes differ");
  }
  return Status::OK();
}

StatusOr<SelectionVector> ResolveSelection(
    size_t row_count, std::span<const size_t> input_selection) {
  SelectionVector rows;
  if (input_selection.empty()) {
    rows.resize(row_count);
    for (size_t row = 0; row < row_count; ++row) rows[row] = row;
    return rows;
  }
  rows.assign(input_selection.begin(), input_selection.end());
  if (std::any_of(rows.begin(), rows.end(),
                  [row_count](size_t row) { return row >= row_count; })) {
    return Status::InvalidArgument("vector kernel",
                                   "selection row is out of range");
  }
  return rows;
}

template <typename T>
bool Less(const T& left, const T& right) {
  if constexpr (std::is_arithmetic_v<T> || std::is_same_v<T, std::string>) {
    return left < right;
  } else if constexpr (std::is_same_v<T, VertexRef>) {
    return left.part_id.value < right.part_id.value ||
           (left.part_id == right.part_id &&
            left.vertex_id.value < right.vertex_id.value);
  } else if constexpr (std::is_same_v<T, EdgeRef>) {
    return left.home_part_id.value < right.home_part_id.value ||
           (left.home_part_id == right.home_part_id &&
            left.edge_id.value < right.edge_id.value);
  } else if constexpr (std::is_same_v<T, Binary>) {
    return left.value < right.value;
  } else if constexpr (std::is_same_v<T, Timestamp64> ||
                       std::is_same_v<T, ValidDuration> ||
                       std::is_same_v<T, ValidTime> ||
                       std::is_same_v<T, CommitSeq>) {
    return left.value < right.value;
  } else {
    if (left.from.value != right.from.value) {
      return left.from.value < right.from.value;
    }
    if (!left.to.has_value()) return false;
    if (!right.to.has_value()) return true;
    return left.to->value < right.to->value;
  }
}

template <typename T>
bool Compare(const T& left, const T& right, ComparisonKind kind) {
  switch (kind) {
    case ComparisonKind::kEqual:
      return left == right;
    case ComparisonKind::kNotEqual:
      return left != right;
    case ComparisonKind::kLessThan:
      return Less(left, right);
    case ComparisonKind::kLessThanOrEqual:
      return !Less(right, left);
    case ComparisonKind::kGreaterThan:
      return Less(right, left);
    case ComparisonKind::kGreaterThanOrEqual:
      return !Less(left, right);
  }
  return false;
}

template <typename T>
BoolVector CompareVectors(const QueryColumn& left, const QueryColumn& right,
                          const SelectionVector& rows, ComparisonKind kind) {
  const auto& left_values = std::get<std::vector<T>>(left.values);
  const auto& right_values = std::get<std::vector<T>>(right.values);
  BoolVector result;
  result.reserve(rows.size());
  for (size_t row : rows) {
    if (!IsPresent(left, row) || !IsPresent(right, row)) {
      result.push_back(false);
      continue;
    }
    result.push_back(Compare(left_values[row], right_values[row], kind));
  }
  return result;
}

template <typename Output>
StatusOr<QueryColumn> CastNumeric(const QueryColumn& input,
                                  const SelectionVector& rows,
                                  QueryType output_type) {
  std::vector<Output> output;
  std::vector<uint8_t> present;
  output.reserve(rows.size());
  present.reserve(rows.size());
  for (size_t row : rows) {
    const bool is_present = IsPresent(input, row);
    present.push_back(is_present ? uint8_t{1} : uint8_t{0});
    if (!is_present) {
      output.push_back(Output{});
      continue;
    }
    long double value = 0;
    switch (input.type) {
      case QueryType::kInt32:
        value = std::get<std::vector<int32_t>>(input.values)[row];
        break;
      case QueryType::kInt64:
        value = std::get<std::vector<int64_t>>(input.values)[row];
        break;
      case QueryType::kFloat32:
        value = std::get<std::vector<float>>(input.values)[row];
        break;
      case QueryType::kFloat64:
        value = std::get<std::vector<double>>(input.values)[row];
        break;
      default:
        return Status::InvalidArgument("vector cast",
                                       "explicit cast requires numeric types");
    }
    if (!std::isfinite(value) ||
        value < static_cast<long double>(std::numeric_limits<Output>::lowest()) ||
        value > static_cast<long double>(std::numeric_limits<Output>::max())) {
      return Status::NumericOverflow("vector cast",
                                     "numeric value is outside target range");
    }
    output.push_back(static_cast<Output>(value));
  }
  return QueryColumn{input.slot, output_type, std::move(output),
                     std::move(present)};
}

}  // namespace

StatusOr<BoolVector> EvaluateComparison(
    const QueryColumn& left, const QueryColumn& right, ComparisonKind kind,
    std::span<const size_t> input_selection) {
  if (Status status = ValidateColumn(left); !status.ok()) return status;
  if (Status status = ValidateColumn(right); !status.ok()) return status;
  if (left.type != right.type) {
    return Status::InvalidArgument("vector comparison",
                                   "operand types differ");
  }
  if (ValueCount(left) != ValueCount(right)) {
    return Status::InvalidArgument("vector comparison",
                                   "operand lane sizes differ");
  }
  auto rows = ResolveSelection(ValueCount(left), input_selection);
  if (!rows.ok()) return rows.status();
  switch (left.type) {
    case QueryType::kBool:
      return CompareVectors<uint8_t>(left, right, rows.ValueOrDie(), kind);
    case QueryType::kInt32:
      return CompareVectors<int32_t>(left, right, rows.ValueOrDie(), kind);
    case QueryType::kInt64:
      return CompareVectors<int64_t>(left, right, rows.ValueOrDie(), kind);
    case QueryType::kFloat32:
      return CompareVectors<float>(left, right, rows.ValueOrDie(), kind);
    case QueryType::kFloat64:
      return CompareVectors<double>(left, right, rows.ValueOrDie(), kind);
    case QueryType::kTimestamp64:
      return CompareVectors<uint64_t>(left, right, rows.ValueOrDie(), kind);
    case QueryType::kString:
    case QueryType::kBinary:
      return CompareVectors<std::string>(left, right, rows.ValueOrDie(), kind);
    case QueryType::kVertexRef:
      return CompareVectors<VertexRef>(left, right, rows.ValueOrDie(), kind);
    case QueryType::kEdgeRef:
      return CompareVectors<EdgeRef>(left, right, rows.ValueOrDie(), kind);
    case QueryType::kValidTime:
      return CompareVectors<ValidTime>(left, right, rows.ValueOrDie(), kind);
    case QueryType::kValidDuration:
      return CompareVectors<ValidDuration>(left, right, rows.ValueOrDie(), kind);
    case QueryType::kCommitSeq:
      return CompareVectors<CommitSeq>(left, right, rows.ValueOrDie(), kind);
    case QueryType::kValidTimeInterval:
      return CompareVectors<ValidTimeInterval>(left, right, rows.ValueOrDie(),
                                               kind);
    case QueryType::kPath:
    case QueryType::kJourney:
      return Status::NotSupported("vector comparison",
                                  "non-scalar columns are unsupported");
  }
  return Status::InvalidArgument("vector comparison", "unknown column type");
}

BoolVector EvaluateEqual(const QueryColumn& left, const QueryColumn& right) {
  auto result = EvaluateComparison(left, right, ComparisonKind::kEqual);
  return result.ok() ? std::move(result).ConsumeValueOrDie() : BoolVector{};
}

BoolVector EvaluateNotEqual(const QueryColumn& left,
                            const QueryColumn& right) {
  auto result = EvaluateComparison(left, right, ComparisonKind::kNotEqual);
  return result.ok() ? std::move(result).ConsumeValueOrDie() : BoolVector{};
}

BoolVector EvaluateEqual(const DictionaryStringColumn& left,
                         const DictionaryStringColumn& right) {
  if (!left.dictionary || !right.dictionary ||
      left.ids.size() != right.ids.size() ||
      (!left.present.empty() && left.present.size() != left.ids.size()) ||
      (!right.present.empty() && right.present.size() != right.ids.size())) {
    return {};
  }
  BoolVector result;
  result.reserve(left.ids.size());
  for (size_t row = 0; row < left.ids.size(); ++row) {
    const bool left_present = left.present.empty() || left.present[row] != 0;
    const bool right_present = right.present.empty() || right.present[row] != 0;
    if (!left_present || !right_present ||
        left.ids[row] >= left.dictionary->size() ||
        right.ids[row] >= right.dictionary->size()) {
      result.push_back(false);
    } else if (left.dictionary == right.dictionary) {
      result.push_back(left.ids[row] == right.ids[row]);
    } else {
      result.push_back((*left.dictionary)[left.ids[row]] ==
                       (*right.dictionary)[right.ids[row]]);
    }
  }
  return result;
}

StatusOr<QueryColumn> CastColumn(const QueryColumn& input, QueryType output_type,
                                 std::span<const size_t> input_selection) {
  if (Status status = ValidateColumn(input); !status.ok()) return status;
  auto rows = ResolveSelection(ValueCount(input), input_selection);
  if (!rows.ok()) return rows.status();
  if (input.type == output_type) {
    return ProjectColumn(input, rows.ValueOrDie());
  }
  switch (output_type) {
    case QueryType::kInt32:
      return CastNumeric<int32_t>(input, rows.ValueOrDie(), output_type);
    case QueryType::kInt64:
      return CastNumeric<int64_t>(input, rows.ValueOrDie(), output_type);
    case QueryType::kFloat32:
      return CastNumeric<float>(input, rows.ValueOrDie(), output_type);
    case QueryType::kFloat64:
      return CastNumeric<double>(input, rows.ValueOrDie(), output_type);
    default:
      return Status::InvalidArgument("vector cast",
                                     "target type is not numeric");
  }
}

StatusOr<SelectionVector> FilterSelection(const QueryColumn& predicate) {
  if (Status status = ValidateColumn(predicate); !status.ok()) return status;
  return FilterSelection(predicate, ResolveSelection(ValueCount(predicate), {})
                                        .ConsumeValueOrDie());
}

StatusOr<SelectionVector> FilterSelection(
    const QueryColumn& predicate, std::span<const size_t> input_selection) {
  if (Status status = ValidateColumn(predicate); !status.ok()) return status;
  if (predicate.type != QueryType::kBool) {
    return Status::InvalidArgument("vector filter",
                                   "predicate column is not bool");
  }
  auto rows = ResolveSelection(ValueCount(predicate), input_selection);
  if (!rows.ok()) return rows.status();
  const auto& values = std::get<std::vector<uint8_t>>(predicate.values);
  SelectionVector output;
  output.reserve(rows.ValueOrDie().size());
  for (size_t row : rows.ValueOrDie()) {
    if (IsPresent(predicate, row) && values[row] != 0) output.push_back(row);
  }
  return output;
}

StatusOr<QueryColumn> ProjectColumn(const QueryColumn& input) {
  if (Status status = ValidateColumn(input); !status.ok()) return status;
  return ProjectColumn(input,
                       ResolveSelection(ValueCount(input), {}).ConsumeValueOrDie());
}

StatusOr<QueryColumn> ProjectColumn(
    const QueryColumn& input, std::span<const size_t> input_selection) {
  if (Status status = ValidateColumn(input); !status.ok()) return status;
  auto rows = ResolveSelection(ValueCount(input), input_selection);
  if (!rows.ok()) return rows.status();
  QueryColumnVector output_values = std::visit(
      [](const auto& source) -> QueryColumnVector {
        using Vector = std::decay_t<decltype(source)>;
        return Vector{};
      },
      input.values);
  QueryColumn output{input.slot, input.type, std::move(output_values), {}};
  std::visit(
      [&](auto& destination) {
        using Vector = std::decay_t<decltype(destination)>;
        const auto& source = std::get<Vector>(input.values);
        destination.clear();
        destination.reserve(rows.ValueOrDie().size());
        for (size_t row : rows.ValueOrDie()) destination.push_back(source[row]);
      },
      output.values);
  if (!input.present.empty()) {
    output.present.reserve(rows.ValueOrDie().size());
    for (size_t row : rows.ValueOrDie()) {
      output.present.push_back(input.present[row]);
    }
  }
  return output;
}

}  // namespace cedar::internal
