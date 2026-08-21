// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_TEMPORAL_INTERVAL_H_
#define CEDAR_QUERY_TEMPORAL_INTERVAL_H_

#include <optional>
#include <vector>

#include "cedar/query/types.h"

namespace cedar::internal {

struct StateInterval {
  ValidTimeInterval interval;
  std::optional<Value> value;

  bool operator==(const StateInterval&) const = default;
};

std::optional<ValidTimeInterval> Intersect(const ValidTimeInterval& left,
                                           const ValidTimeInterval& right);
std::optional<ValidTimeInterval> Clip(const ValidTimeInterval& interval,
                                      const ValidTimeInterval& bounds);
std::vector<StateInterval> Coalesce(std::vector<StateInterval> intervals);

}  // namespace cedar::internal

#endif  // CEDAR_QUERY_TEMPORAL_INTERVAL_H_
