// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/tcypher/runtime/vector_project.h"

#include <variant>

namespace cedar {

StatusOr<std::optional<Value>> VectorExpression::Evaluate(
    const ColumnBatch& input, uint32_t row, const VectorExpression& expression) {
  switch (expression.kind_) {
    case VectorExpressionKind::kInputColumn:
      if (expression.input_column_ >= input.column_count()) {
        return Status::InvalidArgument("vector project", "input column out of range");
      }
      return input.ValueAt(expression.input_column_, row);
    case VectorExpressionKind::kLiteral:
      return expression.literal_;
    case VectorExpressionKind::kAdd:
    case VectorExpressionKind::kEqual:
    case VectorExpressionKind::kOperationName:
      break;
  }

  const auto left = Evaluate(input, row, *expression.left_);
  if (!left.ok()) return left.status();
  if (expression.kind_ == VectorExpressionKind::kOperationName) {
    if (!left.ValueOrDie().has_value() ||
        left.ValueOrDie()->type() != PhysicalType::kInt32) {
      return Status::InvalidArgument("vector project", "operation requires an Int32 operation code");
    }
    const int32_t operation = std::get<int32_t>(left.ValueOrDie()->data());
    if (operation == 0) return std::optional<Value>(Value::String("PUT"));
    if (operation == 1) return std::optional<Value>(Value::String("DELETE"));
    return Status::Corruption("vector project", "unknown temporal operation code");
  }
  const auto right = Evaluate(input, row, *expression.right_);
  if (!right.ok()) return right.status();
  if (!left.ValueOrDie().has_value() || !right.ValueOrDie().has_value()) {
    return std::optional<Value>{};
  }
  const Value& left_value = *left.ValueOrDie();
  const Value& right_value = *right.ValueOrDie();

  if (expression.kind_ == VectorExpressionKind::kEqual) {
    if (left_value.type() != right_value.type()) {
      return Status::InvalidArgument("vector project", "equality type mismatch");
    }
    return std::optional<Value>(Value::Bool(left_value == right_value));
  }

  if (left_value.type() == PhysicalType::kInt32 && right_value.type() == PhysicalType::kInt32) {
    return std::optional<Value>(Value::Int32(std::get<int32_t>(left_value.data()) +
                                              std::get<int32_t>(right_value.data())));
  }
  if (left_value.type() == PhysicalType::kInt64 && right_value.type() == PhysicalType::kInt64) {
    return std::optional<Value>(Value::Int64(std::get<int64_t>(left_value.data()) +
                                              std::get<int64_t>(right_value.data())));
  }
  return Status::InvalidArgument("vector project", "ADD requires matching integer inputs");
}

VectorExpression VectorExpression::InputColumn(uint32_t column) {
  VectorExpression expression(VectorExpressionKind::kInputColumn);
  expression.input_column_ = column;
  return expression;
}

VectorExpression VectorExpression::Literal(std::optional<Value> value) {
  VectorExpression expression(VectorExpressionKind::kLiteral);
  expression.literal_ = std::move(value);
  return expression;
}

VectorExpression VectorExpression::Add(VectorExpression left, VectorExpression right) {
  VectorExpression expression(VectorExpressionKind::kAdd);
  expression.left_ = std::make_shared<VectorExpression>(std::move(left));
  expression.right_ = std::make_shared<VectorExpression>(std::move(right));
  return expression;
}

VectorExpression VectorExpression::Equal(VectorExpression left, VectorExpression right) {
  VectorExpression expression(VectorExpressionKind::kEqual);
  expression.left_ = std::make_shared<VectorExpression>(std::move(left));
  expression.right_ = std::make_shared<VectorExpression>(std::move(right));
  return expression;
}

VectorExpression VectorExpression::OperationName(VectorExpression operation_code) {
  VectorExpression expression(VectorExpressionKind::kOperationName);
  expression.left_ = std::make_shared<VectorExpression>(std::move(operation_code));
  return expression;
}

Status ProjectColumnBatch(const ColumnBatch& input,
                          const std::vector<VectorExpression>& expressions,
                          ColumnBatch* output) {
  if (output == nullptr || expressions.empty()) {
    return Status::InvalidArgument("vector project", "missing output or expressions");
  }
  ColumnBatch result(input.row_count());
  for (const VectorExpression& expression : expressions) {
    std::vector<Value> values;
    std::vector<bool> validity;
    values.reserve(input.row_count());
    validity.reserve(input.row_count());
    for (uint32_t row = 0; row < input.row_count(); ++row) {
      const auto value = VectorExpression::Evaluate(input, row, expression);
      if (!value.ok()) return value.status();
      validity.push_back(value.ValueOrDie().has_value());
      values.push_back(value.ValueOrDie().value_or(Value::Binary("")));
    }
    Status status = result.AddVector(std::make_shared<FlatVector>(
        std::move(values), std::move(validity)));
    if (!status.ok()) return status;
  }
  *output = std::move(result);
  return Status::OK();
}

}  // namespace cedar
