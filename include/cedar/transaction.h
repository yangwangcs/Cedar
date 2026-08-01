// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TRANSACTION_H_
#define CEDAR_TRANSACTION_H_

#include <functional>
#include <memory>
#include <optional>

#include "cedar/core/status.h"
#include "cedar/fact/fact.h"

namespace cedar {

class Database;

using TransactionFactVisitor = std::function<Status(const FactEvent&)>;

enum class IsolationLevel : uint8_t { kSnapshot = 1, kStrict = 2 };

struct TransactionOptions {
  IsolationLevel isolation = IsolationLevel::kSnapshot;
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
  Status Assert(EntityFact entity, ValidTime valid_time);
  Status Retract(EntityFact entity, ValidTime valid_time);
  Status Set(PropertyFact property, ValidTime valid_time, Value value);
  Status Unset(PropertyFact property, ValidTime valid_time);
  StatusOr<CommitResult> Commit();
  Status Rollback();

 private:
  class State;
  explicit Transaction(std::unique_ptr<State> state);

  std::unique_ptr<State> state_;

  friend class Database;
};

}  // namespace cedar

#endif  // CEDAR_TRANSACTION_H_
