// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_RUNTIME_VECTOR_KERNELS_H_
#define CEDAR_QUERY_RUNTIME_VECTOR_KERNELS_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "cedar/query/result.h"

namespace cedar::internal {

using BoolVector = std::vector<bool>;
using SelectionVector = std::vector<size_t>;

enum class ComparisonKind : uint8_t {
  kEqual,
  kNotEqual,
  kLessThan,
  kLessThanOrEqual,
  kGreaterThan,
  kGreaterThanOrEqual,
};

struct DictionaryStringColumn {
  std::shared_ptr<const std::vector<std::string>> dictionary;
  std::vector<uint32_t> ids;
  std::vector<uint8_t> present;
};

StatusOr<BoolVector> EvaluateComparison(
    const QueryColumn& left, const QueryColumn& right, ComparisonKind kind,
    std::span<const size_t> input_selection = {});
BoolVector EvaluateEqual(const QueryColumn& left, const QueryColumn& right);
BoolVector EvaluateNotEqual(const QueryColumn& left, const QueryColumn& right);
BoolVector EvaluateEqual(const DictionaryStringColumn& left,
                         const DictionaryStringColumn& right);

StatusOr<QueryColumn> CastColumn(const QueryColumn& input, QueryType output_type,
                                 std::span<const size_t> input_selection = {});

StatusOr<SelectionVector> FilterSelection(const QueryColumn& predicate);
StatusOr<SelectionVector> FilterSelection(
    const QueryColumn& predicate, std::span<const size_t> input_selection);
StatusOr<QueryColumn> ProjectColumn(const QueryColumn& input);
StatusOr<QueryColumn> ProjectColumn(
    const QueryColumn& input, std::span<const size_t> input_selection);

}  // namespace cedar::internal

#endif  // CEDAR_QUERY_RUNTIME_VECTOR_KERNELS_H_
