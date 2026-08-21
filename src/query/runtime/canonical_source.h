// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_RUNTIME_CANONICAL_SOURCE_H_
#define CEDAR_QUERY_RUNTIME_CANONICAL_SOURCE_H_

#include <vector>

#include "cedar/snapshot.h"

namespace cedar::internal {

class CanonicalSource {
 public:
  static StatusOr<std::vector<VertexRef>> ReadVerticesAt(
      Snapshot& snapshot, ValidTime valid_time);
};

}  // namespace cedar::internal

#endif  // CEDAR_QUERY_RUNTIME_CANONICAL_SOURCE_H_
