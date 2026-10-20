// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TRANSACTION_H_
#define CEDAR_TRANSACTION_H_

#include <functional>
#include <condition_variable>
#include <mutex>
#include <memory>
#include <optional>

#include "cedar/core/status.h"
#include "cedar/fact/fact.h"

namespace cedar {

class Database;
class Snapshot;

using TransactionFactVisitor = std::function<Status(const FactEvent&)>;

enum class IsolationLevel : uint8_t { kSnapshot = 1, kStrict = 2 };

struct TransactionOptions {
  IsolationLevel isolation = IsolationLevel::kSnapshot;
  uint64_t commit_deadline_us = 0;
};

enum class CommitOutcome : uint8_t {
  kCommitted = 1,
  kAborted = 2,
  kIndeterminate = 3,
};

struct CommitResult {
  CommitOutcome outcome = CommitOutcome::kAborted;
  CommitSeq commit_seq;
  TxnId txn_id;
  Status status = Status::InvalidArgument("commit", "not attempted");
};

enum class CommitAcceptance : uint8_t {
  // The transaction crossed Cedar's durable acceptance boundary.
  kAccepted = 1,
  kIndeterminate = 2,
  // Opt-in pipelined acceptance only: the request is in the append pipeline,
  // not yet durable. Wait() must be called for the durable terminal result.
  kEnqueued = 3,
};

class CommitHandle {
 public:
  class State;

  CommitHandle() = default;
  CommitHandle(const CommitHandle&) = default;
  CommitHandle& operator=(const CommitHandle&) = default;

  TxnId txn_id() const;
  CommitAcceptance acceptance() const;
  StatusOr<CommitResult> Wait() const;

 private:
  explicit CommitHandle(std::shared_ptr<State> state);

  std::shared_ptr<State> state_;

  friend class Database;
  friend class Transaction;
};

class Transaction {
 public:
  ~Transaction();
  Transaction(Transaction&&) noexcept;
  Transaction& operator=(Transaction&&) noexcept;

  Transaction(const Transaction&) = delete;
  Transaction& operator=(const Transaction&) = delete;

  StatusOr<bool> Exists(EntityFact entity, ValidTime valid_time);
  StatusOr<std::optional<Value>> Get(PropertyFact property, ValidTime valid_time);
  Status Scan(FactFamily family, PropertyId property_id,
              const TransactionFactVisitor& visitor);
  StatusOr<Snapshot> BeginReadSnapshot(bool include_staged = true) const;
  Status Assert(EntityFact entity, ValidTime valid_time);
  Status Assert(EdgeIdentity identity, ValidTime valid_time);
  Status Retract(EntityFact entity, ValidTime valid_time);
  Status Set(PropertyFact property, ValidTime valid_time, Value value);
  Status Unset(PropertyFact property, ValidTime valid_time);
  StatusOr<CommitResult> Commit();
  // Durable asynchronous commit: returns only after the complete transaction
  // reaches Cedar's durable acceptance boundary.
  StatusOr<CommitHandle> CommitAsync();
  // Opt-in pipelined commit acceptance: returns as soon as the request is
  // admitted into the append pipeline (mailbox handoff and append queue
  // admission) without waiting for epoch selection, WAL durability or
  // publication. CommitHandle::Wait() remains the barrier for the durable
  // terminal result; callers must not treat the returned handle as durable.
  // A nonzero TransactionOptions::commit_deadline_us is rejected because the
  // queued-request cancellation contract belongs to CommitAsync().
  StatusOr<CommitHandle> CommitAsyncPipelined();
  Status Rollback();

 private:
  class State;
  explicit Transaction(std::unique_ptr<State> state);
  StatusOr<CommitHandle> CommitAsyncImpl(bool pipelined);

  std::unique_ptr<State> state_;

  friend class Database;
};

}  // namespace cedar

#endif  // CEDAR_TRANSACTION_H_
