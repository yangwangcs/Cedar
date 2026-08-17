// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#ifndef CEDAR_STORAGE_TEMPORAL_EVENT_H_
#define CEDAR_STORAGE_TEMPORAL_EVENT_H_

#include <cstdint>
#include <optional>
#include <vector>

#include "cedar/blob/blob_store.h"
#include "cedar/transaction/logical_key.h"
#include "cedar/types/value.h"

namespace cedar {

enum class TemporalOperation : uint8_t {
  kPut = 0,
  kDelete = 1,
};

// The payload is opaque bytes at the event layer. Columnar storage preserves
// the same event contract while encoding typed values in pages.
// Value storage while preserving the event identity and visibility rule.
class TemporalEvent {
 public:
  static TemporalEvent Put(LogicalKey key, uint64_t valid_from,
                           uint64_t commit_seq, uint32_t schema_epoch,
                           Value value);
  static TemporalEvent Delete(LogicalKey key, uint64_t valid_from,
                              uint64_t commit_seq, uint32_t schema_epoch);
  static TemporalEvent PutBlob(LogicalKey key, uint64_t valid_from,
                               uint64_t commit_seq, uint32_t schema_epoch,
                               BlobRef blob_ref);

  const LogicalKey& logical_key() const { return logical_key_; }
  uint64_t valid_from() const { return valid_from_; }
  uint64_t commit_seq() const { return commit_seq_; }
  uint32_t schema_epoch() const { return schema_epoch_; }
  TemporalOperation operation() const { return operation_; }
  bool is_delete() const { return operation_ == TemporalOperation::kDelete; }
  bool is_blob_reference() const { return blob_ref_.has_value(); }
  const std::optional<BlobRef>& blob_ref() const { return blob_ref_; }
  const Value& value() const { return value_; }

 private:
  TemporalEvent(LogicalKey key, uint64_t valid_from, uint64_t commit_seq, uint32_t schema_epoch,
                TemporalOperation operation, Value value, std::optional<BlobRef> blob_ref);

  LogicalKey logical_key_;
  uint64_t valid_from_;
  uint64_t commit_seq_;
  uint32_t schema_epoch_;
  TemporalOperation operation_;
  Value value_;
  std::optional<BlobRef> blob_ref_;
};

// Compares durable event content. Blob placement hints are intentionally
// excluded because relocation does not change the immutable event identity.
bool SameTemporalEventContent(const TemporalEvent& left,
                              const TemporalEvent& right);

std::optional<TemporalEvent> ResolveVisibleEvent(
    const std::vector<TemporalEvent>& events, const LogicalKey& key,
    uint64_t valid_time, uint64_t snapshot_seq);

std::optional<Value> ResolveValue(const std::vector<TemporalEvent>& events,
                                  const LogicalKey& key, uint64_t valid_time,
                                  uint64_t snapshot_seq);

}  // namespace cedar

#endif  // CEDAR_STORAGE_TEMPORAL_EVENT_H_
