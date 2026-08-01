// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/transaction.h"

#include "cedar/database.h"
#include "kernel/database_impl.h"

namespace cedar {

class Transaction::State {
 public:
  explicit State(std::shared_ptr<Database::Impl> database,
                 TransactionOptions options)
      : database(std::move(database)), options(options) {}

  std::shared_ptr<Database::Impl> database;
  TransactionOptions options;
  bool terminal = false;

  Status CheckDatabaseOpen() const {
    std::lock_guard<std::mutex> lock(database->mutex);
    return database->closed
               ? Status::InvalidArgument("transaction", "database is closed")
               : Status::OK();
  }
};

Transaction::Transaction(std::unique_ptr<State> state) : state_(std::move(state)) {}
Transaction::~Transaction() = default;
Transaction::Transaction(Transaction&&) noexcept = default;
Transaction& Transaction::operator=(Transaction&&) noexcept = default;

StatusOr<bool> Transaction::Exists(EntityFact, ValidTime) {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status database_open = state_->CheckDatabaseOpen();
  if (!database_open.ok()) return database_open;
  return Status::NotSupported("transaction", "reads are not implemented yet");
}

StatusOr<std::optional<Value>> Transaction::Get(PropertyFact, ValidTime) {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status database_open = state_->CheckDatabaseOpen();
  if (!database_open.ok()) return database_open;
  return Status::NotSupported("transaction", "reads are not implemented yet");
}

Status Transaction::Assert(EntityFact, ValidTime) {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status database_open = state_->CheckDatabaseOpen();
  if (!database_open.ok()) return database_open;
  return Status::NotSupported("transaction", "mutations are not implemented yet");
}

Status Transaction::Retract(EntityFact, ValidTime) {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status database_open = state_->CheckDatabaseOpen();
  if (!database_open.ok()) return database_open;
  return Status::NotSupported("transaction", "mutations are not implemented yet");
}

Status Transaction::Set(PropertyFact, ValidTime, Value) {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status database_open = state_->CheckDatabaseOpen();
  if (!database_open.ok()) return database_open;
  return Status::NotSupported("transaction", "mutations are not implemented yet");
}

Status Transaction::Unset(PropertyFact, ValidTime) {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status database_open = state_->CheckDatabaseOpen();
  if (!database_open.ok()) return database_open;
  return Status::NotSupported("transaction", "mutations are not implemented yet");
}

StatusOr<CommitResult> Transaction::Commit() {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status database_open = state_->CheckDatabaseOpen();
  if (!database_open.ok()) return database_open;
  return Status::NotSupported("transaction", "commits are not implemented yet");
}

Status Transaction::Rollback() {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  state_->terminal = true;
  return Status::OK();
}

StatusOr<std::unique_ptr<Transaction>> Database::BeginTransaction(
    TransactionOptions options) {
  if (!impl_) return Status::InvalidArgument("database", "moved-from database");
  std::lock_guard<std::mutex> lock(impl_->mutex);
  if (impl_->closed) return Status::InvalidArgument("database", "database is closed");
  return std::unique_ptr<Transaction>(
      new Transaction(std::make_unique<Transaction::State>(impl_, options)));
}

}  // namespace cedar
