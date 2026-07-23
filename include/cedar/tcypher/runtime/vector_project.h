// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TCYPHER_RUNTIME_VECTOR_PROJECT_H_
#define CEDAR_TCYPHER_RUNTIME_VECTOR_PROJECT_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/tcypher/runtime/column_batch.h"

namespace cedar {

enum class VectorExpressionKind : uint8_t {
  kInputColumn,
  kLiteral,
  kAdd,
  kEqual,
  kOperationName,
};

// Expressions are immutable value objects owned by one physical project
// specification. The runtime only observes the selected input rows.
class VectorExpression {
 public:
  static VectorExpression InputColumn(uint32_t column);
  static VectorExpression Literal(std::optional<Value> value);
  static VectorExpression Add(VectorExpression left, VectorExpression right);
  static VectorExpression Equal(VectorExpression left, VectorExpression right);
  static VectorExpression OperationName(VectorExpression operation_code);

 private:
  explicit VectorExpression(VectorExpressionKind kind) : kind_(kind) {}

  VectorExpressionKind kind_;
  uint32_t input_column_ = 0;
  std::optional<Value> literal_;
  std::shared_ptr<const VectorExpression> left_;
  std::shared_ptr<const VectorExpression> right_;

  static StatusOr<std::optional<Value>> Evaluate(const ColumnBatch& input,
                                                 uint32_t row,
                                                 const VectorExpression& expression);

  friend Status ProjectColumnBatch(const ColumnBatch& input,
                                  const std::vector<VectorExpression>& expressions,
                                  ColumnBatch* output);
};

// Project materializes one vector per expression over surviving logical rows.
// Unlike filtering, projection intentionally compacts the input selection.
Status ProjectColumnBatch(const ColumnBatch& input,
                          const std::vector<VectorExpression>& expressions,
                          ColumnBatch* output);

}  // namespace cedar

#endif  // CEDAR_TCYPHER_RUNTIME_VECTOR_PROJECT_H_
