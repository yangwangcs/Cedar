// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_EXPRESSION_H_
#define CEDAR_QUERY_EXPRESSION_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "cedar/query/types.h"

namespace cedar {
namespace internal {
class ExpressionFactory;
class ExpressionInspector;
class ExpressionNode;
enum class ExpressionKind : uint8_t;
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
template <> constexpr QueryType QueryTypeOf<std::string>() { return QueryType::kString; }
template <> constexpr QueryType QueryTypeOf<VertexRef>() { return QueryType::kVertexRef; }
template <> constexpr QueryType QueryTypeOf<EdgeRef>() { return QueryType::kEdgeRef; }
template <> constexpr QueryType QueryTypeOf<ValidTime>() { return QueryType::kValidTime; }
template <> constexpr QueryType QueryTypeOf<ValidDuration>() { return QueryType::kValidDuration; }
template <> constexpr QueryType QueryTypeOf<CommitSeq>() { return QueryType::kCommitSeq; }
template <> constexpr QueryType QueryTypeOf<ValidTimeInterval>() { return QueryType::kValidTimeInterval; }

template <typename T>
class Expr {
 public:
  Expr() = default;
  QueryType type() const { return QueryTypeOf<T>(); }
  bool valid() const { return static_cast<bool>(node_); }
 private:
  explicit Expr(std::shared_ptr<const internal::ExpressionNode> node) : node_(std::move(node)) {}
  std::shared_ptr<const internal::ExpressionNode> node_;
  friend class internal::ExpressionFactory;
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
  friend class internal::ExpressionFactory;
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

template <typename T, bool Optional> Expr<T> ValueOf(const Slot<T, Optional>& slot);
template <typename T> Expr<bool> IsPresent(const OptionalSlot<T>& slot);
template <typename T> Expr<T> Literal(T value);
template <typename T> Expr<bool> Equal(Expr<T> left, Expr<T> right);
template <typename T> Expr<bool> NotEqual(Expr<T> left, Expr<T> right);
template <typename T> requires std::is_arithmetic_v<T> Expr<bool> GreaterThan(Expr<T> left, Expr<T> right);
Expr<bool> operator&&(Expr<bool> left, Expr<bool> right);
Expr<bool> Not(Expr<bool> expression);

}  // namespace cedar

#endif  // CEDAR_QUERY_EXPRESSION_H_
