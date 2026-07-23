// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#include "cedar/storage/temporal_event.h"

namespace cedar {

TemporalEvent::TemporalEvent(LogicalKey key, uint64_t valid_from,
                             uint64_t commit_seq, uint32_t schema_epoch, TemporalOperation operation,
                             Value value, std::optional<BlobRef> blob_ref)
    : logical_key_(std::move(key)),
      valid_from_(valid_from),
      commit_seq_(commit_seq),
      schema_epoch_(schema_epoch),
      operation_(operation),
      value_(std::move(value)),
      blob_ref_(std::move(blob_ref)) {}

TemporalEvent TemporalEvent::Put(LogicalKey key, uint64_t valid_from,
                                 uint64_t commit_seq, uint32_t schema_epoch, Value value) {
  return TemporalEvent(std::move(key), valid_from, commit_seq, schema_epoch,
                       TemporalOperation::kPut, std::move(value), std::nullopt);
}

TemporalEvent TemporalEvent::Delete(LogicalKey key, uint64_t valid_from,
                                    uint64_t commit_seq, uint32_t schema_epoch) {
  return TemporalEvent(std::move(key), valid_from, commit_seq, schema_epoch,
                       TemporalOperation::kDelete, Value::Binary("", 0), std::nullopt);
}

TemporalEvent TemporalEvent::PutBlob(LogicalKey key, uint64_t valid_from,
                                     uint64_t commit_seq, uint32_t schema_epoch,
                                     BlobRef blob_ref) {
  return TemporalEvent(std::move(key), valid_from, commit_seq, schema_epoch,
                       TemporalOperation::kPut, Value::Binary("", 0),
                       std::move(blob_ref));
}

bool SameTemporalEventContent(const TemporalEvent& left,
                              const TemporalEvent& right) {
  const auto same_blob_reference = [&]() {
    if (left.blob_ref().has_value() != right.blob_ref().has_value()) return false;
    if (!left.blob_ref().has_value()) return true;
    return left.blob_ref()->content_hash == right.blob_ref()->content_hash &&
           left.blob_ref()->raw_length == right.blob_ref()->raw_length;
  };
  return left.logical_key() == right.logical_key() &&
         left.valid_from() == right.valid_from() &&
         left.commit_seq() == right.commit_seq() &&
         left.schema_epoch() == right.schema_epoch() &&
         left.operation() == right.operation() && left.value() == right.value() &&
         same_blob_reference();
}

std::optional<TemporalEvent> ResolveVisibleEvent(
    const std::vector<TemporalEvent>& events, const LogicalKey& key,
    uint64_t valid_time, uint64_t snapshot_seq) {
  std::optional<TemporalEvent> selected;
  for (const TemporalEvent& event : events) {
    if (event.logical_key() != key || event.valid_from() > valid_time ||
        event.commit_seq() > snapshot_seq) {
      continue;
    }
    if (!selected.has_value() ||
        event.valid_from() > selected->valid_from() ||
        (event.valid_from() == selected->valid_from() &&
         event.commit_seq() > selected->commit_seq())) {
      selected = event;
    }
  }
  return selected;
}

std::optional<Value> ResolveValue(const std::vector<TemporalEvent>& events,
                                  const LogicalKey& key, uint64_t valid_time,
                                  uint64_t snapshot_seq) {
  const auto event = ResolveVisibleEvent(events, key, valid_time, snapshot_seq);
  if (!event.has_value() || event->is_delete()) {
    return std::nullopt;
  }
  return event->value();
}

}  // namespace cedar
