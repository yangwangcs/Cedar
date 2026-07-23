// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/storage/temporal_memtable.h"

#include <algorithm>

namespace cedar {
namespace {

uint64_t EstimateEventBytes(const TemporalEvent& event) {
  constexpr uint64_t kEventOverheadBytes = 96;
  const uint64_t value_bytes = event.is_blob_reference()
      ? 64 : static_cast<uint64_t>(event.value().Encode().size());
  return kEventOverheadBytes + value_bytes;
}

bool BeforeInVersionChain(const TemporalEvent& left, const TemporalEvent& right) {
  if (left.valid_from() != right.valid_from()) {
    return left.valid_from() > right.valid_from();
  }
  return left.commit_seq() > right.commit_seq();
}

}  // namespace

Status TemporalVersionChain::Insert(const TemporalEvent& event) {
  if (event.logical_key() != logical_key_ || event.commit_seq() == 0) {
    return Status::InvalidArgument("temporal version chain",
                                   "event identity does not belong to this chain");
  }
  const auto position = std::lower_bound(
      events_.begin(), events_.end(), event, BeforeInVersionChain);
  if (position != events_.end() && position->valid_from() == event.valid_from() &&
      position->commit_seq() == event.commit_seq()) {
    return SameTemporalEventContent(*position, event)
        ? Status::OK()
        : Status::Corruption("temporal version chain",
                             "contradictory content for one event identity");
  }
  approximate_memory_bytes_ += EstimateEventBytes(event);
  events_.insert(position, event);
  return Status::OK();
}

std::optional<TemporalEvent> TemporalVersionChain::Resolve(
    uint64_t valid_time, uint64_t snapshot_seq) const {
  for (const TemporalEvent& event : events_) {
    if (event.valid_from() <= valid_time && event.commit_seq() <= snapshot_seq) {
      return event;
    }
  }
  return std::nullopt;
}

Status TemporalMemTable::Insert(const TemporalEvent& event) {
  auto inserted = chains_.try_emplace(event.logical_key(), event.logical_key());
  TemporalVersionChain& chain = inserted.first->second;
  const uint64_t before_count = chain.events().size();
  const uint64_t before_bytes = chain.approximate_memory_bytes();
  const Status status = chain.Insert(event);
  if (!status.ok()) {
    if (inserted.second && chain.events().empty()) chains_.erase(inserted.first);
    return status;
  }
  event_count_ += chain.events().size() - before_count;
  approximate_memory_bytes_ += chain.approximate_memory_bytes() - before_bytes;
  generation_ = std::max(generation_, event.commit_seq());
  has_blob_references_ = has_blob_references_ || event.is_blob_reference();
  return Status::OK();
}

std::optional<TemporalEvent> TemporalMemTable::GetEvent(
    const LogicalKey& key, uint64_t valid_time, uint64_t snapshot_seq) const {
  const auto found = chains_.find(key);
  return found == chains_.end()
      ? std::nullopt : found->second.Resolve(valid_time, snapshot_seq);
}

std::optional<Value> TemporalMemTable::Get(const LogicalKey& key,
                                           uint64_t valid_time,
                                           uint64_t snapshot_seq) const {
  const auto event = GetEvent(key, valid_time, snapshot_seq);
  return !event.has_value() || event->is_delete()
      ? std::nullopt : std::optional<Value>(event->value());
}

std::vector<TemporalEvent> TemporalMemTable::SnapshotEvents() const {
  std::vector<TemporalEvent> events;
  events.reserve(event_count_);
  for (const auto& chain : chains_) {
    events.insert(events.end(), chain.second.events().begin(),
                  chain.second.events().end());
  }
  return events;
}

Status TemporalMemTable::VisitEvents(
    const std::function<Status(const TemporalEvent&)>& visitor) const {
  if (!visitor) {
    return Status::InvalidArgument("temporal memtable", "event visitor is required");
  }
  for (const auto& chain : chains_) {
    for (const TemporalEvent& event : chain.second.events()) {
      const Status status = visitor(event);
      if (!status.ok()) return status;
    }
  }
  return Status::OK();
}

Status TemporalMemTable::VisitKeyEvents(
    const LogicalKey& key,
    const std::function<Status(const TemporalEvent&)>& visitor) const {
  if (!visitor) {
    return Status::InvalidArgument("temporal memtable", "event visitor is required");
  }
  const auto found = chains_.find(key);
  if (found == chains_.end()) return Status::OK();
  for (const TemporalEvent& event : found->second.events()) {
    const Status status = visitor(event);
    if (!status.ok()) return status;
  }
  return Status::OK();
}

struct TemporalMemTableCursorImpl {
  using Iterator = std::map<LogicalKey, TemporalVersionChain>::const_iterator;

  explicit TemporalMemTableCursorImpl(
      std::shared_ptr<const TemporalMemTable> pinned,
      TemporalMemTableCursorOptions cursor_options)
      : memtable(std::move(pinned)), options(std::move(cursor_options)) {
    if (options.exact_key.has_value()) {
      chain = memtable->chains_.find(*options.exact_key);
    } else if (options.lower_inclusive.has_value()) {
      chain = memtable->chains_.lower_bound(*options.lower_inclusive);
    } else {
      chain = memtable->chains_.begin();
    }
  }

  bool InBounds() const {
    if (!memtable || chain == memtable->chains_.end()) return false;
    if (options.exact_key.has_value() && chain->first != *options.exact_key) return false;
    return !options.upper_exclusive.has_value() ||
           chain->first < *options.upper_exclusive;
  }

  std::shared_ptr<const TemporalMemTable> memtable;
  TemporalMemTableCursorOptions options;
  Iterator chain;
  size_t event = 0;
  Status terminal_status = Status::OK();
};

TemporalMemTableCursor::TemporalMemTableCursor(
    std::unique_ptr<TemporalMemTableCursorImpl> impl)
    : impl_(std::move(impl)) {}
TemporalMemTableCursor::TemporalMemTableCursor(TemporalMemTableCursor&&) noexcept = default;
TemporalMemTableCursor& TemporalMemTableCursor::operator=(
    TemporalMemTableCursor&&) noexcept = default;
TemporalMemTableCursor::~TemporalMemTableCursor() = default;

bool TemporalMemTableCursor::valid() const {
  return impl_ && impl_->terminal_status.ok() && impl_->InBounds() &&
         impl_->event < impl_->chain->second.events().size();
}

const TemporalEvent& TemporalMemTableCursor::current() const {
  return impl_->chain->second.events()[impl_->event];
}

Status TemporalMemTableCursor::Advance() {
  if (!impl_) {
    return Status::InvalidArgument("temporal memtable cursor", "cursor was moved from");
  }
  if (!impl_->terminal_status.ok()) return impl_->terminal_status;
  if (!valid()) return Status::NotFound("temporal memtable cursor", "end of input");
  ++impl_->event;
  if (impl_->event == impl_->chain->second.events().size()) {
    ++impl_->chain;
    impl_->event = 0;
  }
  return Status::OK();
}

const Status& TemporalMemTableCursor::terminal_status() const {
  return impl_->terminal_status;
}

StatusOr<TemporalMemTableCursor> OpenTemporalMemTableCursor(
    std::shared_ptr<const TemporalMemTable> memtable,
    TemporalMemTableCursorOptions options) {
  if (!memtable) {
    return Status::InvalidArgument("temporal memtable cursor", "missing pinned memtable");
  }
  if (options.exact_key.has_value() &&
      (options.lower_inclusive.has_value() || options.upper_exclusive.has_value())) {
    return Status::InvalidArgument("temporal memtable cursor",
                                   "exact key and key range are mutually exclusive");
  }
  if (options.lower_inclusive.has_value() && options.upper_exclusive.has_value() &&
      !(*options.lower_inclusive < *options.upper_exclusive)) {
    return Status::InvalidArgument("temporal memtable cursor", "invalid logical-key range");
  }
  auto impl = std::make_unique<TemporalMemTableCursorImpl>(
      std::move(memtable), std::move(options));
  return TemporalMemTableCursor(std::move(impl));
}

}  // namespace cedar
