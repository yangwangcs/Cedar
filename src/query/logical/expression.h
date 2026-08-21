// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_LOGICAL_EXPRESSION_H_
#define CEDAR_QUERY_LOGICAL_EXPRESSION_H_

#include <memory>
#include <utility>
#include <vector>

#include "cedar/query/expression.h"

namespace cedar::internal {

enum class ExpressionKind : uint8_t {
  kSlot,
  kLiteral,
  kIsPresent,
  kEqual,
  kNotEqual,
  kGreaterThan,
  kAnd,
  kNot,
};

class ExpressionNode {
 public:
  ExpressionNode(ExpressionKind kind, QueryType type,
                 std::vector<std::shared_ptr<const ExpressionNode>> children)
      : kind_(kind), type_(type), children_(std::move(children)) {}
  ExpressionKind kind() const { return kind_; }
  QueryType type() const { return type_; }
  const std::vector<std::shared_ptr<const ExpressionNode>>& children() const { return children_; }
 private:
  const ExpressionKind kind_;
  const QueryType type_;
  const std::vector<std::shared_ptr<const ExpressionNode>> children_;
};

class ExpressionInspector {
 public:
  template <typename T>
  static const ExpressionNode* Inspect(const Expr<T>& expression) {
    return expression.node_.get();
  }
  template <typename T>
  static std::shared_ptr<const ExpressionNode> Share(const Expr<T>& expression) {
    return expression.node_;
  }
};

}  // namespace cedar::internal

#endif  // CEDAR_QUERY_LOGICAL_EXPRESSION_H_
