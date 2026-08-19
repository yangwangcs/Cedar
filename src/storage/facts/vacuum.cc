// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "storage/facts/vacuum.h"

#include <utility>

#include "cedar/fact/fact_codec.h"

namespace cedar {
namespace {

constexpr size_t kFactValidTimePrefixBytes = 24;

}  // namespace

VacuumCleanupPlanner::VacuumCleanupPlanner(CommitSeq target, size_t max_keys)
    : target_(target), max_keys_(max_keys) {}

StatusOr<VacuumKeyDecision> VacuumCleanupPlanner::Consider(
    const std::string& encoded_fact_key) {
  const auto decoded = DecodeFactKey(encoded_fact_key);
  if (!decoded.ok()) return decoded.status();
  const std::string boundary =
      encoded_fact_key.substr(0, kFactValidTimePrefixBytes);
  if (processed_ >= max_keys_ && boundary != active_boundary_) {
    return VacuumKeyDecision{.stop_before_key = true};
  }
  if (boundary != active_boundary_) {
    active_boundary_ = boundary;
    retained_baseline_ = false;
  }
  VacuumKeyDecision decision;
  if (decoded.ValueOrDie().commit_seq.value <= target_.value) {
    decision.delete_key = retained_baseline_;
    retained_baseline_ = true;
  }
  last_key_ = encoded_fact_key;
  ++processed_;
  return decision;
}

}  // namespace cedar
