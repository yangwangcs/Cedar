// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TCYPHER_SESSION_H_
#define CEDAR_TCYPHER_SESSION_H_

#include <cstdint>
#include <iterator>
#include <memory>
#include <utility>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/db/database_lifecycle.h"
#include "cedar/transaction/transaction_coordinator.h"

namespace cedar {

enum class TcypherSessionMode : uint8_t { kSnapshot, kStrict };

class TcypherSession {
 public:
  explicit TcypherSession(TransactionCoordinator* coordinator)
      : TcypherSession(coordinator, std::make_shared<DatabaseLifecycle>()) {}
  TcypherSession(TransactionCoordinator* coordinator,
                 std::shared_ptr<DatabaseLifecycle> lifecycle)
      : coordinator_(coordinator), lifecycle_(std::move(lifecycle)) {}

  Status Begin(TcypherSessionMode mode) {
    auto entered = Enter(DatabaseOperationClass::kQuery);
    if (!entered.ok()) return entered.status();
    if (coordinator_ == nullptr) return Status::InvalidArgument("T-Cypher session", "missing coordinator");
    if (active_) return Status::InvalidArgument("T-Cypher session", "transaction is already active");
    snapshot_seq_ = coordinator_->visible_seq();
    mode_ = mode;
    pending_events_.clear();
    strict_reads_.clear();
    active_ = true;
    return Status::OK();
  }

  Status Stage(std::vector<PendingEvent> events) {
    auto entered = Enter(DatabaseOperationClass::kQuery);
    if (!entered.ok()) return entered.status();
    if (!active_) return Status::InvalidArgument("T-Cypher session", "no active transaction");
    if (events.empty()) return Status::InvalidArgument("T-Cypher session", "no events to stage");
    pending_events_.insert(pending_events_.end(),
                           std::make_move_iterator(events.begin()),
                           std::make_move_iterator(events.end()));
    return Status::OK();
  }

  Status RecordRead(const TransactionCoordinator::StrictReadPoint& read) {
    auto entered = Enter(DatabaseOperationClass::kQuery);
    if (!entered.ok()) return entered.status();
    if (!active_ || mode_ != TcypherSessionMode::kStrict) return Status::OK();
    if (coordinator_ == nullptr) {
      return Status::InvalidArgument("T-Cypher session", "missing coordinator");
    }
    TransactionCoordinator::StrictReadPoint captured = read;
    if (!captured.observed_event.has_value() &&
        !captured.predecessor_fence.has_value() &&
        !captured.successor_fence.has_value()) {
      const auto snapshot = coordinator_->CaptureStrictReadPoint(
          read.logical_key, read.valid_time, snapshot_seq_);
      if (!snapshot.ok()) return snapshot.status();
      captured = snapshot.ValueOrDie();
    }
    strict_reads_.push_back(std::move(captured));
    return Status::OK();
  }

  Status Commit(uint64_t* commit_seq) {
    auto entered = Enter(DatabaseOperationClass::kCommit);
    if (!entered.ok()) return entered.status();
    if (!active_) return Status::InvalidArgument("T-Cypher session", "no active transaction");
    if (commit_seq == nullptr) return Status::InvalidArgument("T-Cypher session", "missing commit output");
    if (pending_events_.empty()) {
      *commit_seq = 0;
      active_ = false;
      snapshot_seq_ = 0;
      return Status::OK();
    }
    const Status status = mode_ == TcypherSessionMode::kStrict
        ? coordinator_->CommitStrict(snapshot_seq_, pending_events_, strict_reads_, commit_seq)
        : coordinator_->Commit(snapshot_seq_, pending_events_, commit_seq);
    pending_events_.clear();
    strict_reads_.clear();
    active_ = false;
    snapshot_seq_ = 0;
    return status;
  }

  Status Rollback() {
    if (!active_) return Status::InvalidArgument("T-Cypher session", "no active transaction");
    active_ = false;
    snapshot_seq_ = 0;
    pending_events_.clear();
    strict_reads_.clear();
    return Status::OK();
  }

  bool active() const { return active_; }
  uint64_t snapshot_seq() const { return snapshot_seq_; }
  TcypherSessionMode mode() const { return mode_; }
  const std::vector<PendingEvent>& pending_events() const { return pending_events_; }
  bool IsBoundTo(const TransactionCoordinator* coordinator,
                 const DatabaseLifecycle* lifecycle) const {
    return coordinator_ == coordinator && lifecycle_.get() == lifecycle;
  }

 private:
  StatusOr<DatabaseOperationLease> Enter(
      DatabaseOperationClass operation_class) const {
    if (!lifecycle_) {
      return Status::ShutdownInProgress("T-Cypher session",
                                        "database lifecycle is unavailable");
    }
    return lifecycle_->TryEnter(operation_class);
  }

  TransactionCoordinator* coordinator_;
  std::shared_ptr<DatabaseLifecycle> lifecycle_;
  TcypherSessionMode mode_ = TcypherSessionMode::kSnapshot;
  uint64_t snapshot_seq_ = 0;
  bool active_ = false;
  std::vector<PendingEvent> pending_events_;
  std::vector<TransactionCoordinator::StrictReadPoint> strict_reads_;
};

}  // namespace cedar

#endif  // CEDAR_TCYPHER_SESSION_H_
