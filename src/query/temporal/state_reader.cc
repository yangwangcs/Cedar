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
    : spec_(std::move(spec)), visitor_(std::move(visitor)),
      history_memory_(spec_.facts.read_memory.get()),
      history_payload_(spec_.facts.read_memory.get()),
      output_memory_(spec_.facts.read_memory.get()),
      output_payload_(spec_.facts.read_memory.get()) {}

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
    Status status = ReserveFactAppend(&current_events_, &history_memory_);
    if (!status.ok()) return status;
    status = history_payload_.Reserve(FactValueHeapBytes(event.value));
    if (!status.ok()) return status;
    current_events_.push_back(event);
  }
  return Status::OK();
}

Status StateRowStream::FlushOutput() {
  if (output_.empty()) return Status::OK();
  Status status = visitor_(output_);
  if (!status.ok()) return status;
  emitted_rows_ += output_.size();
  output_.clear();
  output_payload_.Reset();
  return Status::OK();
}

Status StateRowStream::FlushCurrent() {
  if (current_events_.empty()) return Status::OK();
  // Bound the existing correction/materialization helpers before they copy
  // history, grow boundary/state vectors, or coalesce variable-size values.
  FactReadMemoryScope workspace(spec_.facts.read_memory.get());
  Status reserved = workspace.ReserveElements(
      current_events_.size(), sizeof(FactEvent) + 2 * sizeof(CorrectedBoundary) +
                                  3 * sizeof(StateInterval));
  if (!reserved.ok()) return reserved;
  for (const FactEvent& event : current_events_) {
    reserved = workspace.ReserveElements(FactValueHeapBytes(event.value), 4);
    if (!reserved.ok()) return reserved;
  }
  auto boundaries = ResolveCorrectedBoundaries(current_events_, spec_.snapshot_seq);
  if (!boundaries.ok()) return boundaries.status();
  const auto present = MaterializePresentState(boundaries.ValueOrDie());
  for (const StateInterval& state : present) {
    if (!Contains(state.interval, spec_.valid_time)) continue;
    if (spec_.max_rows.has_value() &&
        (emitted_rows_ >= *spec_.max_rows ||
         output_.size() >= *spec_.max_rows - emitted_rows_)) {
      limit_reached_ = true;
      return Status::QueryCancelled("canonical reader", "state max_rows reached");
    }
    reserved = ReserveFactAppend(&output_, &output_memory_);
    if (!reserved.ok()) return reserved;
    reserved = output_payload_.Reserve(FactValueHeapBytes(state.value));
    if (!reserved.ok()) return reserved;
    output_.push_back(CanonicalStateRow{
        *current_ref_, state.interval,
        BoundaryCommit(boundaries.ValueOrDie(), state.interval.from,
                       spec_.snapshot_seq),
        state.value});
    if (spec_.max_rows.has_value() &&
        output_.size() >= *spec_.max_rows - emitted_rows_) {
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
  history_payload_.Reset();
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
