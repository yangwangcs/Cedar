// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_FACT_VACUUM_H_
#define CEDAR_FACT_VACUUM_H_

#include <cstddef>
#include <string>

#include "cedar/core/status.h"
#include "cedar/fact/fact.h"

namespace cedar {

struct VacuumKeyDecision {
  bool stop_before_key = false;
  bool delete_key = false;
};

// Plans one bounded cleanup batch without coupling the retention rule to a
// particular LSM implementation. Callers persist the resulting cursor with
// their physical delete batch.
class VacuumCleanupPlanner {
 public:
  static constexpr size_t kDefaultKeysPerBatch = 1024;

  explicit VacuumCleanupPlanner(CommitSeq target,
                                size_t max_keys = kDefaultKeysPerBatch);

  StatusOr<VacuumKeyDecision> Consider(const std::string& encoded_fact_key);
  const std::string& last_key() const { return last_key_; }

 private:
  CommitSeq target_;
  size_t max_keys_;
  size_t processed_ = 0;
  std::string active_boundary_;
  std::string last_key_;
  bool retained_baseline_ = false;
};

}  // namespace cedar

#endif  // CEDAR_FACT_VACUUM_H_
