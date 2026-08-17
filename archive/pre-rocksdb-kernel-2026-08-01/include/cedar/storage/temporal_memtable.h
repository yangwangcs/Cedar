// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_STORAGE_TEMPORAL_MEMTABLE_H_
#define CEDAR_STORAGE_TEMPORAL_MEMTABLE_H_

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/storage/temporal_event.h"

namespace cedar {

struct TemporalMemTableCursorImpl;

// All immutable versions of one logical fact. Physical order matches the
// SST order so freezing and flush preserve adjacent version chains.
class TemporalVersionChain {
 public:
  explicit TemporalVersionChain(LogicalKey logical_key)
      : logical_key_(std::move(logical_key)) {}

  Status Insert(const TemporalEvent& event);
  std::optional<TemporalEvent> Resolve(uint64_t valid_time,
                                       uint64_t snapshot_seq) const;
  const std::vector<TemporalEvent>& events() const { return events_; }
  uint64_t approximate_memory_bytes() const { return approximate_memory_bytes_; }

 private:
  LogicalKey logical_key_;
  std::vector<TemporalEvent> events_;
  uint64_t approximate_memory_bytes_ = 0;
};

class TemporalMemTable {
 public:
  Status Insert(const TemporalEvent& event);
  std::optional<TemporalEvent> GetEvent(const LogicalKey& key,
                                        uint64_t valid_time,
                                        uint64_t snapshot_seq) const;
  std::optional<Value> Get(const LogicalKey& key, uint64_t valid_time,
                           uint64_t snapshot_seq) const;
  std::vector<TemporalEvent> SnapshotEvents() const;
  Status VisitEvents(
      const std::function<Status(const TemporalEvent&)>& visitor) const;
  Status VisitKeyEvents(
      const LogicalKey& key,
      const std::function<Status(const TemporalEvent&)>& visitor) const;

  bool empty() const { return event_count_ == 0; }
  uint64_t event_count() const { return event_count_; }
  uint64_t chain_count() const { return chains_.size(); }
  uint64_t approximate_memory_bytes() const { return approximate_memory_bytes_; }
  uint64_t generation() const { return generation_; }
  bool has_blob_references() const { return has_blob_references_; }

 private:
  friend struct TemporalMemTableCursorImpl;
  std::map<LogicalKey, TemporalVersionChain> chains_;
  uint64_t event_count_ = 0;
  uint64_t approximate_memory_bytes_ = 0;
  uint64_t generation_ = 0;
  bool has_blob_references_ = false;
};

struct TemporalMemTableCursorOptions {
  std::optional<LogicalKey> exact_key;
  std::optional<LogicalKey> lower_inclusive;
  std::optional<LogicalKey> upper_exclusive;
};

class TemporalMemTableCursor {
 public:
  TemporalMemTableCursor() = default;
  TemporalMemTableCursor(TemporalMemTableCursor&&) noexcept;
  TemporalMemTableCursor& operator=(TemporalMemTableCursor&&) noexcept;
  ~TemporalMemTableCursor();

  TemporalMemTableCursor(const TemporalMemTableCursor&) = delete;
  TemporalMemTableCursor& operator=(const TemporalMemTableCursor&) = delete;

  bool valid() const;
  const TemporalEvent& current() const;
  Status Advance();
  const Status& terminal_status() const;

 private:
  explicit TemporalMemTableCursor(
      std::unique_ptr<TemporalMemTableCursorImpl> impl);
  std::unique_ptr<TemporalMemTableCursorImpl> impl_;
  friend StatusOr<TemporalMemTableCursor> OpenTemporalMemTableCursor(
      std::shared_ptr<const TemporalMemTable>, TemporalMemTableCursorOptions);
};

StatusOr<TemporalMemTableCursor> OpenTemporalMemTableCursor(
    std::shared_ptr<const TemporalMemTable> memtable,
    TemporalMemTableCursorOptions options = {});

}  // namespace cedar

#endif  // CEDAR_STORAGE_TEMPORAL_MEMTABLE_H_
