// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TCYPHER_RUNTIME_VECTOR_FILTER_H_
#define CEDAR_TCYPHER_RUNTIME_VECTOR_FILTER_H_

#include <functional>

#include "cedar/core/status.h"
#include "cedar/tcypher/runtime/column_batch.h"

namespace cedar {

using VectorPredicate = std::function<bool(const ColumnBatch&, uint32_t)>;

// Filters only the selection vector. Flat and dictionary vector buffers remain
// shared with the input batch until a later materializing operator needs them.
Status FilterColumnBatch(const ColumnBatch& input, const VectorPredicate& predicate,
                         ColumnBatch* output);

}  // namespace cedar

#endif  // CEDAR_TCYPHER_RUNTIME_VECTOR_FILTER_H_
