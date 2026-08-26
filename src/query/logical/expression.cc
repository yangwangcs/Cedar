// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/logical/expression.h"

namespace cedar {
namespace detail {
namespace {

std::shared_ptr<const internal::ExpressionNode> MakeBinary(
    internal::ExpressionKind kind,
    std::shared_ptr<const internal::ExpressionNode> left,
    std::shared_ptr<const internal::ExpressionNode> right) {
  return std::make_shared<const internal::ExpressionNode>(
      kind, QueryType::kBool,
      std::vector<std::shared_ptr<const internal::ExpressionNode>>{
          std::move(left), std::move(right)});
}

}  // namespace

std::shared_ptr<const internal::ExpressionNode> MakeSlotExpression(
    QueryType type, SlotId slot, bool optional) {
  return std::make_shared<const internal::ExpressionNode>(
      internal::ExpressionKind::kSlot, type,
      std::vector<std::shared_ptr<const internal::ExpressionNode>>{}, slot,
      optional);
}

std::shared_ptr<const internal::ExpressionNode> MakeParameterExpression(
    QueryType type, ParameterId parameter) {
  return std::make_shared<const internal::ExpressionNode>(
      internal::ExpressionKind::kParameter, type,
      std::vector<std::shared_ptr<const internal::ExpressionNode>>{}, SlotId{},
      false, parameter);
}

std::shared_ptr<const internal::ExpressionNode> MakeLiteralExpression(
    QueryType type, ExpressionLiteral literal) {
  return std::make_shared<const internal::ExpressionNode>(
      internal::ExpressionKind::kLiteral, type,
      std::vector<std::shared_ptr<const internal::ExpressionNode>>{}, SlotId{},
      false, ParameterId{}, std::move(literal));
}

std::shared_ptr<const internal::ExpressionNode> MakeIsPresentExpression(
    std::shared_ptr<const internal::ExpressionNode> expression) {
  return std::make_shared<const internal::ExpressionNode>(
      internal::ExpressionKind::kIsPresent, QueryType::kBool,
      std::vector<std::shared_ptr<const internal::ExpressionNode>>{
          std::move(expression)});
}

std::shared_ptr<const internal::ExpressionNode> MakeEqualExpression(
    std::shared_ptr<const internal::ExpressionNode> left,
    std::shared_ptr<const internal::ExpressionNode> right) {
  return MakeBinary(internal::ExpressionKind::kEqual, std::move(left),
                    std::move(right));
}

std::shared_ptr<const internal::ExpressionNode> MakeNotEqualExpression(
    std::shared_ptr<const internal::ExpressionNode> left,
    std::shared_ptr<const internal::ExpressionNode> right) {
  return MakeBinary(internal::ExpressionKind::kNotEqual, std::move(left),
                    std::move(right));
}

std::shared_ptr<const internal::ExpressionNode> MakeGreaterThanExpression(
    std::shared_ptr<const internal::ExpressionNode> left,
    std::shared_ptr<const internal::ExpressionNode> right) {
  return MakeBinary(internal::ExpressionKind::kGreaterThan, std::move(left),
                    std::move(right));
}

std::shared_ptr<const internal::ExpressionNode> MakeGreaterThanOrEqualExpression(
    std::shared_ptr<const internal::ExpressionNode> left,
    std::shared_ptr<const internal::ExpressionNode> right) {
  return MakeBinary(internal::ExpressionKind::kGreaterThanOrEqual,
                    std::move(left), std::move(right));
}

std::shared_ptr<const internal::ExpressionNode> MakeLessThanExpression(
    std::shared_ptr<const internal::ExpressionNode> left,
    std::shared_ptr<const internal::ExpressionNode> right) {
  return MakeBinary(internal::ExpressionKind::kLessThan, std::move(left),
                    std::move(right));
}

std::shared_ptr<const internal::ExpressionNode> MakeLessThanOrEqualExpression(
    std::shared_ptr<const internal::ExpressionNode> left,
    std::shared_ptr<const internal::ExpressionNode> right) {
  return MakeBinary(internal::ExpressionKind::kLessThanOrEqual,
                    std::move(left), std::move(right));
}

std::shared_ptr<const internal::ExpressionNode> MakeAndExpression(
    std::shared_ptr<const internal::ExpressionNode> left,
    std::shared_ptr<const internal::ExpressionNode> right) {
  return MakeBinary(internal::ExpressionKind::kAnd, std::move(left),
                    std::move(right));
}

std::shared_ptr<const internal::ExpressionNode> MakeNotExpression(
    std::shared_ptr<const internal::ExpressionNode> expression) {
  return std::make_shared<const internal::ExpressionNode>(
      internal::ExpressionKind::kNot, QueryType::kBool,
      std::vector<std::shared_ptr<const internal::ExpressionNode>>{
          std::move(expression)});
}

}  // namespace detail

}  // namespace cedar
