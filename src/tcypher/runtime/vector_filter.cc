// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/tcypher/runtime/vector_filter.h"

namespace cedar {

Status FilterColumnBatch(const ColumnBatch& input, const VectorPredicate& predicate,
                         ColumnBatch* output) {
  if (output == nullptr || !predicate) {
    return Status::InvalidArgument("vector filter", "missing output or predicate");
  }
  std::vector<uint32_t> selected_rows;
  selected_rows.reserve(input.row_count());
  for (uint32_t row = 0; row < input.row_count(); ++row) {
    if (predicate(input, row)) selected_rows.push_back(*input.SourceRowAt(row));
  }
  *output = input;
  return output->SetSelection(std::move(selected_rows));
}

}  // namespace cedar
