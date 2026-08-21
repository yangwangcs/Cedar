// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/logical/expression.h"

namespace cedar {
namespace internal {

class ExpressionFactory {
 public:
  template <typename T>
  static Expr<T> Make(ExpressionKind kind,
                      std::vector<std::shared_ptr<const ExpressionNode>> children = {}) {
    return Expr<T>(std::make_shared<const ExpressionNode>(
        kind, QueryTypeOf<T>(), std::move(children)));
  }
};

}  // namespace internal
namespace {

template <typename T>
std::vector<std::shared_ptr<const internal::ExpressionNode>> Children(Expr<T> left, Expr<T> right) {
  return {internal::ExpressionInspector::Share(left),
          internal::ExpressionInspector::Share(right)};
}

}  // namespace

template <typename T, bool Optional>
Expr<T> ValueOf(const Slot<T, Optional>& slot) {
  return internal::ExpressionFactory::Make<T>(internal::ExpressionKind::kSlot);
}

template <typename T>
Expr<bool> IsPresent(const OptionalSlot<T>& slot) {
  return internal::ExpressionFactory::Make<bool>(internal::ExpressionKind::kIsPresent);
}

template <typename T>
Expr<T> Literal(T value) {
  return internal::ExpressionFactory::Make<T>(internal::ExpressionKind::kLiteral);
}

template <typename T>
Expr<bool> Equal(Expr<T> left, Expr<T> right) {
  return internal::ExpressionFactory::Make<bool>(internal::ExpressionKind::kEqual,
                                                  Children(left, right));
}

template <typename T>
Expr<bool> NotEqual(Expr<T> left, Expr<T> right) {
  return internal::ExpressionFactory::Make<bool>(internal::ExpressionKind::kNotEqual,
                                                  Children(left, right));
}

template <typename T>
  requires std::is_arithmetic_v<T>
Expr<bool> GreaterThan(Expr<T> left, Expr<T> right) {
  return internal::ExpressionFactory::Make<bool>(internal::ExpressionKind::kGreaterThan,
                                                  Children(left, right));
}

Expr<bool> operator&&(Expr<bool> left, Expr<bool> right) {
  return internal::ExpressionFactory::Make<bool>(internal::ExpressionKind::kAnd,
                                                  Children(left, right));
}

Expr<bool> Not(Expr<bool> expression) {
  return internal::ExpressionFactory::Make<bool>(internal::ExpressionKind::kNot,
      {internal::ExpressionInspector::Share(expression)});
}

template Expr<int64_t> ValueOf(const OptionalSlot<int64_t>&);
template Expr<bool> IsPresent(const OptionalSlot<int64_t>&);
template Expr<int64_t> Literal(int64_t);
template Expr<bool> Equal(Expr<int64_t>, Expr<int64_t>);
template Expr<bool> GreaterThan(Expr<int64_t>, Expr<int64_t>);

}  // namespace cedar
