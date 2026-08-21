// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_EXPRESSION_H_
#define CEDAR_QUERY_EXPRESSION_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>
#include <variant>

#include "cedar/query/types.h"

namespace cedar {
namespace internal {
class ExpressionFactory;
class ExpressionInspector;
class ExpressionNode;
}  // namespace internal

namespace detail {
inline std::atomic<uint32_t> next_slot_id{1};
inline std::atomic<uint32_t> next_parameter_id{1};
}  // namespace detail

template <typename T> constexpr QueryType QueryTypeOf();
template <> constexpr QueryType QueryTypeOf<bool>() { return QueryType::kBool; }
template <> constexpr QueryType QueryTypeOf<int32_t>() { return QueryType::kInt32; }
template <> constexpr QueryType QueryTypeOf<int64_t>() { return QueryType::kInt64; }
template <> constexpr QueryType QueryTypeOf<float>() { return QueryType::kFloat32; }
template <> constexpr QueryType QueryTypeOf<double>() { return QueryType::kFloat64; }
template <> constexpr QueryType QueryTypeOf<Timestamp64>() {
  return QueryType::kTimestamp64;
}
template <> constexpr QueryType QueryTypeOf<std::string>() { return QueryType::kString; }
template <> constexpr QueryType QueryTypeOf<Binary>() { return QueryType::kBinary; }
template <> constexpr QueryType QueryTypeOf<VertexRef>() { return QueryType::kVertexRef; }
template <> constexpr QueryType QueryTypeOf<EdgeRef>() { return QueryType::kEdgeRef; }
template <> constexpr QueryType QueryTypeOf<ValidTime>() { return QueryType::kValidTime; }
template <> constexpr QueryType QueryTypeOf<ValidDuration>() { return QueryType::kValidDuration; }
template <> constexpr QueryType QueryTypeOf<CommitSeq>() { return QueryType::kCommitSeq; }
template <> constexpr QueryType QueryTypeOf<ValidTimeInterval>() { return QueryType::kValidTimeInterval; }

template <typename T>
class Expr;
template <typename T>
class OptionalExpr;
template <typename T, bool Optional>
class Slot;
template <typename T>
class Parameter;

template <typename T>
Expr<T> ValueOf(const Slot<T, false>& slot);
template <typename T>
OptionalExpr<T> ValueOf(const Slot<T, true>& slot);
template <typename T>
Expr<T> ValueOf(const Parameter<T>& parameter);

namespace detail {

using ExpressionLiteral = std::variant<bool, int32_t, int64_t, float, double,
                                       Timestamp64, std::string, Binary,
                                       VertexRef, EdgeRef, ValidTime,
                                       ValidDuration, CommitSeq,
                                       ValidTimeInterval>;

std::shared_ptr<const internal::ExpressionNode> MakeSlotExpression(
    QueryType type, SlotId slot, bool optional);
std::shared_ptr<const internal::ExpressionNode> MakeParameterExpression(
    QueryType type, ParameterId parameter);
std::shared_ptr<const internal::ExpressionNode> MakeLiteralExpression(
    QueryType type, ExpressionLiteral literal);
std::shared_ptr<const internal::ExpressionNode> MakeIsPresentExpression(
    std::shared_ptr<const internal::ExpressionNode> expression);
std::shared_ptr<const internal::ExpressionNode> MakeEqualExpression(
    std::shared_ptr<const internal::ExpressionNode> left,
    std::shared_ptr<const internal::ExpressionNode> right);
std::shared_ptr<const internal::ExpressionNode> MakeNotEqualExpression(
    std::shared_ptr<const internal::ExpressionNode> left,
    std::shared_ptr<const internal::ExpressionNode> right);
std::shared_ptr<const internal::ExpressionNode> MakeGreaterThanExpression(
    std::shared_ptr<const internal::ExpressionNode> left,
    std::shared_ptr<const internal::ExpressionNode> right);
std::shared_ptr<const internal::ExpressionNode> MakeAndExpression(
    std::shared_ptr<const internal::ExpressionNode> left,
    std::shared_ptr<const internal::ExpressionNode> right);
std::shared_ptr<const internal::ExpressionNode> MakeNotExpression(
    std::shared_ptr<const internal::ExpressionNode> expression);

}  // namespace detail

template <typename T>
class Expr {
 public:
  Expr() = default;
  QueryType type() const { return QueryTypeOf<T>(); }
  bool valid() const { return static_cast<bool>(node_); }
 private:
  explicit Expr(std::shared_ptr<const internal::ExpressionNode> node) : node_(std::move(node)) {}
  std::shared_ptr<const internal::ExpressionNode> node_;
  friend class internal::ExpressionInspector;
};

template <typename T>
class OptionalExpr {
 public:
  OptionalExpr() = default;
  QueryType type() const { return QueryTypeOf<T>(); }
  bool valid() const { return static_cast<bool>(node_); }
 private:
  explicit OptionalExpr(std::shared_ptr<const internal::ExpressionNode> node) : node_(std::move(node)) {}
  std::shared_ptr<const internal::ExpressionNode> node_;
  friend class internal::ExpressionInspector;
};

template <typename T, bool Optional = false>
class Slot {
 public:
  static Slot Named(std::string name) {
    return Slot(SlotId{detail::next_slot_id.fetch_add(1, std::memory_order_relaxed)},
                std::move(name));
  }
  static Slot WithId(SlotId id, std::string name = {}) { return Slot(id, std::move(name)); }
  SlotId id() const { return id_; }
  const std::string& name() const { return name_; }
  QueryType type() const { return QueryTypeOf<T>(); }
  static constexpr bool optional() { return Optional; }
 private:
  Slot(SlotId id, std::string name) : id_(id), name_(std::move(name)) {}
  SlotId id_;
  std::string name_;
};
template <typename T> using OptionalSlot = Slot<T, true>;

template <typename T>
class Parameter {
 public:
  static Parameter Named(std::string name) {
    return Parameter(ParameterId{detail::next_parameter_id.fetch_add(
                         1, std::memory_order_relaxed)},
                     std::move(name));
  }
  static Parameter WithId(ParameterId id, std::string name = {}) { return Parameter(id, std::move(name)); }
  ParameterId id() const { return id_; }
  const std::string& name() const { return name_; }
  QueryType type() const { return QueryTypeOf<T>(); }
 private:
  Parameter(ParameterId id, std::string name) : id_(id), name_(std::move(name)) {}
  ParameterId id_;
  std::string name_;
};

namespace internal {

class ExpressionInspector {
 public:
  template <typename T>
  static const ExpressionNode* Inspect(const Expr<T>& expression) {
    return expression.node_.get();
  }
  template <typename T>
  static const ExpressionNode* Inspect(const OptionalExpr<T>& expression) {
    return expression.node_.get();
  }
  template <typename T>
  static std::shared_ptr<const ExpressionNode> Share(const Expr<T>& expression) {
    return expression.node_;
  }
  template <typename T>
  static std::shared_ptr<const ExpressionNode> Share(
      const OptionalExpr<T>& expression) {
    return expression.node_;
  }
  template <typename T>
  static Expr<T> Make(std::shared_ptr<const ExpressionNode> node) {
    return Expr<T>(std::move(node));
  }
  template <typename T>
  static OptionalExpr<T> MakeOptional(
      std::shared_ptr<const ExpressionNode> node) {
    return OptionalExpr<T>(std::move(node));
  }
};

}  // namespace internal

template <typename T>
Expr<T> ValueOf(const Slot<T, false>& slot) {
  return internal::ExpressionInspector::Make<T>(
      detail::MakeSlotExpression(slot.type(), slot.id(), false));
}

template <typename T>
OptionalExpr<T> ValueOf(const Slot<T, true>& slot) {
  return internal::ExpressionInspector::MakeOptional<T>(
      detail::MakeSlotExpression(slot.type(), slot.id(), true));
}

template <typename T>
Expr<T> ValueOf(const Parameter<T>& parameter) {
  return internal::ExpressionInspector::Make<T>(
      detail::MakeParameterExpression(parameter.type(), parameter.id()));
}

template <typename T>
Expr<bool> IsPresent(const OptionalSlot<T>& slot) {
  return IsPresent(ValueOf(slot));
}

template <typename T>
Expr<bool> IsPresent(OptionalExpr<T> expression) {
  return internal::ExpressionInspector::Make<bool>(
      detail::MakeIsPresentExpression(
          internal::ExpressionInspector::Share(expression)));
}

template <typename T>
Expr<bool> IsMissing(const OptionalSlot<T>& slot) {
  return IsMissing(ValueOf(slot));
}

template <typename T>
Expr<bool> IsMissing(OptionalExpr<T> expression) {
  return internal::ExpressionInspector::Make<bool>(detail::MakeNotExpression(
      detail::MakeIsPresentExpression(
          internal::ExpressionInspector::Share(expression))));
}

template <typename T>
Expr<T> Literal(T value) {
  return internal::ExpressionInspector::Make<T>(
      detail::MakeLiteralExpression(QueryTypeOf<T>(), std::move(value)));
}

template <typename T>
Expr<bool> Equal(Expr<T> left, Expr<T> right) {
  return internal::ExpressionInspector::Make<bool>(
      detail::MakeEqualExpression(internal::ExpressionInspector::Share(left),
                                  internal::ExpressionInspector::Share(right)));
}

template <typename T>
Expr<bool> Equal(OptionalExpr<T> left, Expr<T> right) {
  return internal::ExpressionInspector::Make<bool>(
      detail::MakeEqualExpression(internal::ExpressionInspector::Share(left),
                                  internal::ExpressionInspector::Share(right)));
}

template <typename T>
Expr<bool> Equal(Expr<T> left, OptionalExpr<T> right) {
  return Equal(std::move(right), std::move(left));
}

template <typename T>
Expr<bool> Equal(OptionalExpr<T> left, OptionalExpr<T> right) {
  return internal::ExpressionInspector::Make<bool>(
      detail::MakeEqualExpression(internal::ExpressionInspector::Share(left),
                                  internal::ExpressionInspector::Share(right)));
}

template <typename T>
Expr<bool> NotEqual(Expr<T> left, Expr<T> right) {
  return internal::ExpressionInspector::Make<bool>(
      detail::MakeNotEqualExpression(internal::ExpressionInspector::Share(left),
                                     internal::ExpressionInspector::Share(right)));
}

template <typename T>
Expr<bool> NotEqual(OptionalExpr<T> left, Expr<T> right) {
  return internal::ExpressionInspector::Make<bool>(
      detail::MakeNotEqualExpression(internal::ExpressionInspector::Share(left),
                                     internal::ExpressionInspector::Share(right)));
}

template <typename T>
Expr<bool> NotEqual(Expr<T> left, OptionalExpr<T> right) {
  return NotEqual(std::move(right), std::move(left));
}

template <typename T>
Expr<bool> NotEqual(OptionalExpr<T> left, OptionalExpr<T> right) {
  return internal::ExpressionInspector::Make<bool>(
      detail::MakeNotEqualExpression(internal::ExpressionInspector::Share(left),
                                     internal::ExpressionInspector::Share(right)));
}

template <typename T>
  requires std::is_arithmetic_v<T>
Expr<bool> GreaterThan(Expr<T> left, Expr<T> right) {
  return internal::ExpressionInspector::Make<bool>(
      detail::MakeGreaterThanExpression(
          internal::ExpressionInspector::Share(left),
          internal::ExpressionInspector::Share(right)));
}

template <typename T>
  requires std::is_arithmetic_v<T>
Expr<bool> GreaterThan(OptionalExpr<T> left, Expr<T> right) {
  return internal::ExpressionInspector::Make<bool>(
      detail::MakeGreaterThanExpression(
          internal::ExpressionInspector::Share(left),
          internal::ExpressionInspector::Share(right)));
}

template <typename T>
  requires std::is_arithmetic_v<T>
Expr<bool> GreaterThan(Expr<T> left, OptionalExpr<T> right) {
  return internal::ExpressionInspector::Make<bool>(
      detail::MakeGreaterThanExpression(
          internal::ExpressionInspector::Share(left),
          internal::ExpressionInspector::Share(right)));
}

template <typename T>
  requires std::is_arithmetic_v<T>
Expr<bool> GreaterThan(OptionalExpr<T> left, OptionalExpr<T> right) {
  return internal::ExpressionInspector::Make<bool>(
      detail::MakeGreaterThanExpression(
          internal::ExpressionInspector::Share(left),
          internal::ExpressionInspector::Share(right)));
}

inline Expr<bool> operator&&(Expr<bool> left, Expr<bool> right) {
  return internal::ExpressionInspector::Make<bool>(
      detail::MakeAndExpression(internal::ExpressionInspector::Share(left),
                                internal::ExpressionInspector::Share(right)));
}

inline Expr<bool> Not(Expr<bool> expression) {
  return internal::ExpressionInspector::Make<bool>(
      detail::MakeNotExpression(internal::ExpressionInspector::Share(expression)));
}

}  // namespace cedar

#endif  // CEDAR_QUERY_EXPRESSION_H_
