// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_RUNTIME_PROPERTY_BINDING_H_
#define CEDAR_QUERY_RUNTIME_PROPERTY_BINDING_H_

#include <optional>
#include <vector>

#include "cedar/schema.h"
#include "query/runtime/temporal_source.h"

namespace cedar::internal {

struct BoundPropertyRow {
  FactRef ref;
  ValidTimeInterval effective;
  std::optional<Value> value;

  bool operator==(const BoundPropertyRow&) const = default;
};

class PropertyBinder {
 public:
  static StatusOr<std::vector<BoundPropertyRow>> BindIntervals(
      Snapshot& snapshot, const std::vector<StateRow>& entities,
      const PropertyDefinition& definition);
  static StatusOr<std::vector<BoundPropertyRow>> BindAt(
      Snapshot& snapshot, const std::vector<StateRow>& entities,
      ValidTime valid_time, const PropertyDefinition& definition);
};

}  // namespace cedar::internal

#endif  // CEDAR_QUERY_RUNTIME_PROPERTY_BINDING_H_
