// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#ifndef CEDAR_STORAGE_STORAGE_SHARD_H_
#define CEDAR_STORAGE_STORAGE_SHARD_H_

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "cedar/transaction/decision_log.h"
#include "cedar/storage/temporal_memtable.h"

namespace cedar {

struct MemtableUsage {
  uint64_t active_bytes = 0;
  uint64_t frozen_bytes = 0;
  uint64_t active_event_count = 0;
  uint64_t frozen_event_count = 0;

  uint64_t total_bytes() const { return active_bytes + frozen_bytes; }
};

// The committed in-memory family for one stable shard. Prepared records remain
// in ShardPrepareLog; only DecisionLog-authorized events enter this structure.
class StorageShard {
 public:
  struct ReadReservation {
    LogicalKey logical_key;
    uint64_t valid_time = 0;
  };
  struct WriteReservation {
    LogicalKey logical_key;
    uint64_t valid_from = 0;
    uint64_t valid_to = UINT64_MAX;
  };
  class ValidationGuard {
   public:
    ValidationGuard(ValidationGuard&&) noexcept = default;
    ValidationGuard& operator=(ValidationGuard&&) noexcept = default;
    ValidationGuard(const ValidationGuard&) = delete;
    ValidationGuard& operator=(const ValidationGuard&) = delete;

   private:
    friend class StorageShard;
    explicit ValidationGuard(std::mutex* mutex) : lock_(*mutex), mutex_(mutex) {}
    std::unique_lock<std::mutex> lock_;
    std::mutex* mutex_;
  };

  explicit StorageShard(uint32_t shard_id)
      : shard_id_(shard_id), active_memtable_(std::make_shared<TemporalMemTable>()) {}

  Status InstallCommitted(uint64_t txn_id, uint64_t commit_seq,
                          const std::vector<PendingEvent>& events);
  std::optional<Value> Get(const LogicalKey& key, uint64_t valid_time,
                           uint64_t snapshot_seq) const;
  uint32_t shard_id() const { return shard_id_; }
  size_t event_count() const;
  MemtableUsage memtable_usage() const;
  std::vector<TemporalEvent> SnapshotEvents() const;
  std::vector<TemporalEvent> SnapshotUnflushedEvents() const;
  std::vector<std::shared_ptr<const TemporalMemTable>>
      PinUnflushedMemtables() const;
  ValidationGuard LockValidation();
  Status ValidateReservationLocked(
      const ValidationGuard& guard, uint64_t txn_id,
      const std::vector<ReadReservation>& reads,
      const std::vector<WriteReservation>& writes) const;
  void InstallReservationLocked(
      const ValidationGuard& guard, uint64_t txn_id,
      std::vector<ReadReservation> reads,
      std::vector<WriteReservation> writes);
  bool MarkReservationCommittedPendingLocked(
      const ValidationGuard& guard, uint64_t txn_id);
  void ReleaseReservationLocked(const ValidationGuard& guard,
                                uint64_t txn_id);
  void ClearReservationsForRecovery();

  // Transfers the current active MemTable into a frozen flush generation.
  // A failed flush leaves that generation intact; only CompleteFlushPublished
  // releases it after the corresponding Manifest edit is durable.
  std::vector<TemporalEvent> FreezeForFlush();
  void CompleteFlushPublished();

 private:
  uint32_t shard_id_;
  mutable std::mutex mutex_;
  mutable std::mutex validation_mutex_;
  struct PreparedReservation {
    uint64_t txn_id = 0;
    std::vector<ReadReservation> reads;
    std::vector<WriteReservation> writes;
    bool committed_pending = false;
  };
  std::vector<PreparedReservation> reservations_;
  std::map<std::pair<uint64_t, uint32_t>, TemporalEvent> installed_events_;
  std::shared_ptr<TemporalMemTable> active_memtable_;
  std::shared_ptr<TemporalMemTable> frozen_memtable_;
};

}  // namespace cedar

#endif  // CEDAR_STORAGE_STORAGE_SHARD_H_
