// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/temporal/state_reader.h"

#include <algorithm>

#include "query/temporal/corrected_chain.h"

namespace cedar::internal {
namespace {

bool Contains(const ValidTimeInterval& interval, ValidTime time) {
  return interval.from.value <= time.value &&
         (!interval.to.has_value() || time.value < interval.to->value);
}

CommitSeq BoundaryCommit(const std::vector<CorrectedBoundary>& boundaries,
                         const ValidTime& from,
                         CommitSeq fallback) {
  for (const CorrectedBoundary& boundary : boundaries) {
    if (boundary.valid_from == from) return boundary.commit_seq;
  }
  return fallback;
}

}  // namespace

StateRowStream::StateRowStream(CanonicalStateReadSpec spec,
                               CanonicalStateBatchVisitor visitor)
    : spec_(std::move(spec)), visitor_(std::move(visitor)) {}

Status StateRowStream::Consume(const FactEventBatch& batch) {
  if (finished_) return Status::InvalidArgument("state row stream", "already finished");
  if (limit_reached_) return Status::QueryCancelled("canonical reader", "state max_rows reached");
  for (const FactEvent& event : batch.events) {
    if (!current_ref_.has_value()) current_ref_ = event.ref;
    if (event.ref != *current_ref_) {
      Status status = FlushCurrent();
      if (!status.ok()) return status;
      current_ref_ = event.ref;
    }
    current_events_.push_back(event);
  }
  return Status::OK();
}

Status StateRowStream::FlushOutput() {
  if (output_.empty()) return Status::OK();
  Status status = visitor_(output_);
  if (!status.ok()) return status;
  output_.clear();
  return Status::OK();
}

Status StateRowStream::FlushCurrent() {
  if (current_events_.empty()) return Status::OK();
  auto boundaries = ResolveCorrectedBoundaries(current_events_, spec_.snapshot_seq);
  if (!boundaries.ok()) return boundaries.status();
  const auto present = MaterializePresentState(boundaries.ValueOrDie());
  for (const StateInterval& state : present) {
    if (!Contains(state.interval, spec_.valid_time)) continue;
    if (spec_.max_rows.has_value() &&
        output_.size() >= *spec_.max_rows) {
      limit_reached_ = true;
      return Status::QueryCancelled("canonical reader", "state max_rows reached");
    }
    output_.push_back(CanonicalStateRow{
        *current_ref_, state.interval,
        BoundaryCommit(boundaries.ValueOrDie(), state.interval.from,
                       spec_.snapshot_seq),
        state.value});
    if (spec_.max_rows.has_value() &&
        output_.size() >= *spec_.max_rows) {
      Status status = FlushOutput();
      if (!status.ok()) return status;
      limit_reached_ = true;
      return Status::QueryCancelled("canonical reader", "state max_rows reached");
    }
    if (output_.size() >= spec_.facts.batch_row_limit) {
      Status status = FlushOutput();
      if (!status.ok()) return status;
    }
  }
  current_events_.clear();
  return Status::OK();
}

Status StateRowStream::Finish() {
  if (finished_) return Status::InvalidArgument("state row stream", "already finished");
  finished_ = true;
  if (limit_reached_) return Status::OK();
  Status status = FlushCurrent();
  if (status.IsQueryCancelled() && limit_reached_) return Status::OK();
  if (!status.ok()) return status;
  return FlushOutput();
}

}  // namespace cedar::internal
