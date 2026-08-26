// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_TEMPORAL_STATE_READER_H_
#define CEDAR_QUERY_TEMPORAL_STATE_READER_H_

#include <optional>
#include <vector>

#include "cedar/fact/read_spec.h"

namespace cedar::internal {

// Consumes identity-contiguous canonical events and emits only the visible
// state at one valid time. It owns at most one entity's history plus one
// output batch, so bounded reads do not materialize the full family.
class StateRowStream {
 public:
  StateRowStream(CanonicalStateReadSpec spec,
                 CanonicalStateBatchVisitor visitor);

  Status Consume(const FactEventBatch& batch);
  Status Finish();
  bool limit_reached() const { return limit_reached_; }

 private:
  Status FlushCurrent();
  Status FlushOutput();

  CanonicalStateReadSpec spec_;
  CanonicalStateBatchVisitor visitor_;
  std::vector<FactEvent> current_events_;
  std::optional<FactRef> current_ref_;
  std::vector<CanonicalStateRow> output_;
  bool limit_reached_ = false;
  bool finished_ = false;
};

}  // namespace cedar::internal

#endif  // CEDAR_QUERY_TEMPORAL_STATE_READER_H_
