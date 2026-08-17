// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#include "cedar/storage/storage_shard.h"

#include <algorithm>

namespace cedar {

namespace {

bool Overlap(uint64_t left_from, uint64_t left_to,
             uint64_t right_from, uint64_t right_to) {
  return left_from < right_to && right_from < left_to;
}

bool Contains(const StorageShard::WriteReservation& interval,
              uint64_t valid_time) {
  return interval.valid_from <= valid_time && valid_time < interval.valid_to;
}

}  // namespace

StorageShard::ValidationGuard StorageShard::LockValidation() {
  return ValidationGuard(&validation_mutex_);
}

Status StorageShard::ValidateReservationLocked(
    const ValidationGuard& guard, uint64_t txn_id,
    const std::vector<ReadReservation>& reads,
    const std::vector<WriteReservation>& writes) const {
  if (guard.mutex_ != &validation_mutex_ || !guard.lock_.owns_lock()) {
    return Status::InvalidArgument("storage shard", "validation guard is not held");
  }
  for (const PreparedReservation& prepared : reservations_) {
    if (prepared.txn_id == txn_id) continue;
    for (const WriteReservation& write : writes) {
      for (const ReadReservation& read : prepared.reads) {
        if (write.logical_key == read.logical_key && Contains(write, read.valid_time)) {
          return Status::Conflict(
              "storage shard", "write conflicts with prepared read reservation");
        }
      }
      for (const WriteReservation& prepared_write : prepared.writes) {
        if (write.logical_key == prepared_write.logical_key &&
            Overlap(write.valid_from, write.valid_to,
                    prepared_write.valid_from, prepared_write.valid_to)) {
          return Status::Conflict(
              "storage shard", "write conflicts with prepared write reservation");
        }
      }
    }
    for (const ReadReservation& read : reads) {
      for (const WriteReservation& prepared_write : prepared.writes) {
        if (read.logical_key == prepared_write.logical_key &&
            Contains(prepared_write, read.valid_time)) {
          return Status::Conflict(
              "storage shard", "read conflicts with prepared write reservation");
        }
      }
    }
  }
  return Status::OK();
}

void StorageShard::InstallReservationLocked(
    const ValidationGuard& guard, uint64_t txn_id,
    std::vector<ReadReservation> reads,
    std::vector<WriteReservation> writes) {
  if (guard.mutex_ != &validation_mutex_ || !guard.lock_.owns_lock()) return;
  reservations_.push_back(
      PreparedReservation{txn_id, std::move(reads), std::move(writes), false});
}

bool StorageShard::MarkReservationCommittedPendingLocked(
    const ValidationGuard& guard, uint64_t txn_id) {
  if (guard.mutex_ != &validation_mutex_ || !guard.lock_.owns_lock()) return false;
  const auto found = std::find_if(
      reservations_.begin(), reservations_.end(),
      [txn_id](const PreparedReservation& reservation) {
        return reservation.txn_id == txn_id;
      });
  if (found == reservations_.end()) return false;
  found->committed_pending = true;
  return true;
}

void StorageShard::ReleaseReservationLocked(const ValidationGuard& guard,
                                             uint64_t txn_id) {
  if (guard.mutex_ != &validation_mutex_ || !guard.lock_.owns_lock()) return;
  reservations_.erase(
      std::remove_if(reservations_.begin(), reservations_.end(),
                     [txn_id](const PreparedReservation& reservation) {
                       return reservation.txn_id == txn_id;
                     }),
      reservations_.end());
}

void StorageShard::ClearReservationsForRecovery() {
  std::lock_guard<std::mutex> lock(validation_mutex_);
  reservations_.clear();
}

Status StorageShard::InstallCommitted(uint64_t txn_id, uint64_t commit_seq,
                                      const std::vector<PendingEvent>& events) {
  if (commit_seq == 0) return Status::InvalidArgument("storage shard", "zero commit sequence");
  std::lock_guard<std::mutex> lock(mutex_);
  if (active_memtable_.use_count() != 1) {
    active_memtable_ = std::make_shared<TemporalMemTable>(*active_memtable_);
  }
  for (uint32_t index = 0; index < events.size(); ++index) {
    const PendingEvent& event = events[index];
    const TemporalEvent materialized = event.operation == TemporalOperation::kPut
        ? (event.blob_ref.has_value()
              ? TemporalEvent::PutBlob(event.logical_key, event.valid_from, commit_seq,
                                       event.schema_epoch, *event.blob_ref)
              : TemporalEvent::Put(event.logical_key, event.valid_from, commit_seq,
                                   event.schema_epoch, event.value))
        : TemporalEvent::Delete(event.logical_key, event.valid_from, commit_seq,
                                event.schema_epoch);
    const std::pair<uint64_t, uint32_t> install_slot{txn_id, index};
    const auto installed = installed_events_.find(install_slot);
    if (installed != installed_events_.end()) {
      if (!SameTemporalEventContent(installed->second, materialized)) {
        return Status::Corruption(
            "storage shard", "contradictory replay for installed event slot");
      }
      continue;
    }
    const Status inserted = active_memtable_->Insert(materialized);
    if (!inserted.ok()) return inserted;
    installed_events_.emplace(install_slot, materialized);
  }
  return Status::OK();
}

std::optional<Value> StorageShard::Get(const LogicalKey& key, uint64_t valid_time,
                                       uint64_t snapshot_seq) const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::optional<TemporalEvent> visible;
  const auto consider = [&](const std::shared_ptr<TemporalMemTable>& source) {
    if (!source) return;
    const auto candidate = source->GetEvent(key, valid_time, snapshot_seq);
    if (!candidate.has_value()) return;
    if (!visible.has_value() || candidate->valid_from() > visible->valid_from() ||
        (candidate->valid_from() == visible->valid_from() &&
         candidate->commit_seq() > visible->commit_seq())) {
      visible = candidate;
    }
  };
  consider(frozen_memtable_);
  consider(active_memtable_);
  return !visible.has_value() || visible->is_delete()
      ? std::nullopt : std::optional<Value>(visible->value());
}

size_t StorageShard::event_count() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return active_memtable_->event_count() +
         (frozen_memtable_ ? frozen_memtable_->event_count() : 0);
}

MemtableUsage StorageShard::memtable_usage() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return MemtableUsage{
      active_memtable_->approximate_memory_bytes(),
      frozen_memtable_ ? frozen_memtable_->approximate_memory_bytes() : 0,
      active_memtable_->event_count(),
      frozen_memtable_ ? frozen_memtable_->event_count() : 0};
}

std::vector<TemporalEvent> StorageShard::SnapshotEvents() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<TemporalEvent> events;
  if (frozen_memtable_) {
    const std::vector<TemporalEvent> frozen = frozen_memtable_->SnapshotEvents();
    events.insert(events.end(), frozen.begin(), frozen.end());
  }
  const std::vector<TemporalEvent> active = active_memtable_->SnapshotEvents();
  events.insert(events.end(), active.begin(), active.end());
  return events;
}

std::vector<TemporalEvent> StorageShard::SnapshotUnflushedEvents() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<TemporalEvent> events;
  if (frozen_memtable_) events = frozen_memtable_->SnapshotEvents();
  const std::vector<TemporalEvent> active = active_memtable_->SnapshotEvents();
  events.insert(events.end(), active.begin(), active.end());
  return events;
}

std::vector<std::shared_ptr<const TemporalMemTable>>
StorageShard::PinUnflushedMemtables() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<std::shared_ptr<const TemporalMemTable>> pinned;
  if (frozen_memtable_ && !frozen_memtable_->empty()) {
    pinned.push_back(frozen_memtable_);
  }
  if (!active_memtable_->empty()) pinned.push_back(active_memtable_);
  return pinned;
}

std::vector<TemporalEvent> StorageShard::FreezeForFlush() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!frozen_memtable_ && !active_memtable_->empty()) {
    frozen_memtable_ = std::move(active_memtable_);
    active_memtable_ = std::make_shared<TemporalMemTable>();
  }
  return frozen_memtable_
      ? frozen_memtable_->SnapshotEvents() : std::vector<TemporalEvent>{};
}

void StorageShard::CompleteFlushPublished() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!frozen_memtable_) return;
  frozen_memtable_.reset();
}

}  // namespace cedar
