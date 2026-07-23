// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/tcypher/runtime/transaction_sink.h"

#include "cedar/tcypher/session.h"

namespace cedar {

Status TransactionSink::Submit(const std::vector<TypedMutation>& mutations,
                               uint64_t* commit_seq) {
  if (coordinator_ == nullptr || commit_seq == nullptr || mutations.empty()) {
    return Status::InvalidArgument("transaction sink", "missing coordinator, output, or mutations");
  }
  if (cancellation_ && cancellation_->IsCancelled()) {
    return Status::QueryCancelled("transaction sink", "query cancelled before commit");
  }
  std::vector<PendingEvent> events;
  events.reserve(mutations.size());
  for (const TypedMutation& mutation : mutations) {
    if (cancellation_ && cancellation_->IsCancelled()) {
      return Status::QueryCancelled("transaction sink", "query cancelled before commit");
    }
    if (mutation.operation == TemporalOperation::kPut) {
      events.push_back(PendingEvent::Put(mutation.logical_key, mutation.valid_from,
                                         mutation.schema_epoch, mutation.value));
    } else if (mutation.operation == TemporalOperation::kDelete) {
      events.push_back(PendingEvent{mutation.logical_key, mutation.valid_from,
                                    mutation.schema_epoch, TemporalOperation::kDelete,
                                    Value::Binary("", 0), std::nullopt});
    } else {
      return Status::InvalidArgument("transaction sink", "unknown mutation operation");
    }
  }
  if (session_ != nullptr) {
    const Status staged = session_->Stage(std::move(events));
    if (!staged.ok()) return staged;
    *commit_seq = 0;
    return Status::OK();
  }
  return coordinator_->Commit(snapshot_seq_, events, commit_seq);
}

}  // namespace cedar
