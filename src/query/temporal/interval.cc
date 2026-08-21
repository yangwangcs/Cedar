// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/temporal/interval.h"

#include <algorithm>

namespace cedar::internal {
namespace {

bool Before(const ValidTime& left, const ValidTime& right) {
  return left.value < right.value;
}

std::optional<ValidTime> EarlierEnd(const std::optional<ValidTime>& left,
                                    const std::optional<ValidTime>& right) {
  if (!left.has_value()) return right;
  if (!right.has_value()) return left;
  return Before(*left, *right) ? left : right;
}

bool EndsAt(const StateInterval& left, const StateInterval& right) {
  return left.interval.to.has_value() &&
         *left.interval.to == right.interval.from;
}

}  // namespace

std::optional<ValidTimeInterval> Intersect(const ValidTimeInterval& left,
                                           const ValidTimeInterval& right) {
  const ValidTime from = Before(left.from, right.from) ? right.from : left.from;
  const std::optional<ValidTime> to = EarlierEnd(left.to, right.to);
  if (to.has_value() && !Before(from, *to)) return std::nullopt;
  return ValidTimeInterval{from, to};
}

std::optional<ValidTimeInterval> Clip(const ValidTimeInterval& interval,
                                      const ValidTimeInterval& bounds) {
  return Intersect(interval, bounds);
}

std::vector<StateInterval> Coalesce(std::vector<StateInterval> intervals) {
  if (intervals.empty()) return intervals;

  std::vector<StateInterval> result;
  result.reserve(intervals.size());
  for (StateInterval& interval : intervals) {
    if (!result.empty() && result.back().value == interval.value &&
        EndsAt(result.back(), interval)) {
      result.back().interval.to = interval.interval.to;
      continue;
    }
    result.push_back(std::move(interval));
  }
  return result;
}

}  // namespace cedar::internal
