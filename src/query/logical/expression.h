// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_LOGICAL_EXPRESSION_H_
#define CEDAR_QUERY_LOGICAL_EXPRESSION_H_

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "cedar/query/expression.h"

namespace cedar::internal {

enum class ExpressionKind : uint8_t {
  kSlot,
  kParameter,
  kLiteral,
  kIsPresent,
  kEqual,
  kNotEqual,
  kGreaterThan,
  kGreaterThanOrEqual,
  kLessThan,
  kLessThanOrEqual,
  kAnd,
  kNot,
};

class ExpressionNode {
 public:
  ExpressionNode(ExpressionKind kind, QueryType type,
                 std::vector<std::shared_ptr<const ExpressionNode>> children,
                 SlotId slot = {},
                 bool optional = false,
                 ParameterId parameter = {},
                 std::optional<detail::ExpressionLiteral> literal = std::nullopt)
      : kind_(kind), type_(type), children_(std::move(children)), slot_(slot),
        optional_(optional), parameter_(parameter), literal_(std::move(literal)) {}
  ExpressionKind kind() const { return kind_; }
  QueryType type() const { return type_; }
  const std::vector<std::shared_ptr<const ExpressionNode>>& children() const { return children_; }
  SlotId slot() const { return slot_; }
  bool optional() const { return optional_; }
  ParameterId parameter() const { return parameter_; }
  const std::optional<detail::ExpressionLiteral>& literal() const {
    return literal_;
  }
 private:
  const ExpressionKind kind_;
  const QueryType type_;
  const std::vector<std::shared_ptr<const ExpressionNode>> children_;
  const SlotId slot_;
  const bool optional_;
  const ParameterId parameter_;
  const std::optional<detail::ExpressionLiteral> literal_;
};

}  // namespace cedar::internal

#endif  // CEDAR_QUERY_LOGICAL_EXPRESSION_H_
