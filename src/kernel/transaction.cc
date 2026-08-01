// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/transaction.h"

#include <algorithm>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

#include "cedar/database.h"
#include "kernel/database_impl.h"
#include "kernel/temporal_validation.h"
#include "kernel/transaction_mutation.h"

namespace cedar {

class Transaction::State {
 public:
  explicit State(std::shared_ptr<Database::Impl> database,
                 TransactionOptions options, StoreSnapshot snapshot)
      : database(std::move(database)),
        options(options),
        snapshot(std::move(snapshot)) {}

  std::shared_ptr<Database::Impl> database;
  TransactionOptions options;
  std::optional<StoreSnapshot> snapshot;
  std::vector<PendingFactMutation> mutations;
  std::vector<SnapshotWriteDependency> snapshot_write_dependencies;
  std::vector<std::tuple<uint8_t, uint16_t, uint64_t, uint64_t>> mutation_keys;
  bool terminal = false;

  Status CheckDatabaseOpen() const {
    std::lock_guard<std::mutex> lock(database->mutex);
    return database->closed
               ? Status::InvalidArgument("transaction", "database is closed")
               : Status::OK();
  }

  StatusOr<PropertyDefinition> ResolveProperty(PropertyFact property) const {
    const auto schema = database->store.LookupProperty(
        *snapshot, property.ref().property_id());
    if (!schema.ok()) return schema.status();
    if (!schema.ValueOrDie().has_value()) {
      return Status::SchemaMismatch("transaction", "property is not registered");
    }
    const PropertyEntityKind expected_kind =
        property.ref().family() == FactFamily::kVertexProperty
            ? PropertyEntityKind::kVertex
            : PropertyEntityKind::kEdge;
    if (schema.ValueOrDie()->entity_kind != expected_kind) {
      return Status::SchemaMismatch("transaction", "property entity kind differs");
    }
    return *schema.ValueOrDie();
  }

  Status Stage(PendingFactMutation mutation) {
    const Status valid = mutation.Validate();
    if (!valid.ok()) return valid;
    const auto key = std::tuple{static_cast<uint8_t>(mutation.ref.family()),
                                mutation.ref.property_id().value,
                                mutation.ref.entity_id(),
                                mutation.valid_from.value};
    if (std::find(mutation_keys.begin(), mutation_keys.end(), key) !=
        mutation_keys.end()) {
      return Status::InvalidArgument("transaction", "duplicate fact mutation");
    }
    auto dependencies = DeriveSnapshotWriteDependencies(
        database->store, *snapshot, std::vector<PendingFactMutation>{mutation});
    if (!dependencies.ok()) return dependencies.status();
    mutation_keys.push_back(key);
    mutations.push_back(std::move(mutation));
    snapshot_write_dependencies.push_back(
        dependencies.ConsumeValueOrDie().front());
    std::sort(mutations.begin(), mutations.end(), CanonicalMutationLess);
    return Status::OK();
  }

  void Finish() {
    terminal = true;
    snapshot.reset();
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

Status Transaction::Assert(EntityFact entity, ValidTime valid_time) {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status database_open = state_->CheckDatabaseOpen();
  if (!database_open.ok()) return database_open;
  return state_->Stage(PendingFactMutation{entity.ref(), valid_time,
                                            FactOperation::kPut, 0,
                                            std::nullopt});
}

Status Transaction::Retract(EntityFact entity, ValidTime valid_time) {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status database_open = state_->CheckDatabaseOpen();
  if (!database_open.ok()) return database_open;
  return state_->Stage(PendingFactMutation{entity.ref(), valid_time,
                                            FactOperation::kDelete, 0,
                                            std::nullopt});
}

Status Transaction::Set(PropertyFact property, ValidTime valid_time, Value value) {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status database_open = state_->CheckDatabaseOpen();
  if (!database_open.ok()) return database_open;
  const auto schema = state_->ResolveProperty(property);
  if (!schema.ok()) return schema.status();
  if (schema.ValueOrDie().physical_type != value.type()) {
    return Status::SchemaMismatch("transaction", "property value type differs");
  }
  return state_->Stage(PendingFactMutation{property.ref(), valid_time,
                                            FactOperation::kPut,
                                            schema.ValueOrDie().schema_epoch,
                                            std::move(value)});
}

Status Transaction::Unset(PropertyFact property, ValidTime valid_time) {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status database_open = state_->CheckDatabaseOpen();
  if (!database_open.ok()) return database_open;
  const auto schema = state_->ResolveProperty(property);
  if (!schema.ok()) return schema.status();
  return state_->Stage(PendingFactMutation{property.ref(), valid_time,
                                            FactOperation::kDelete,
                                            schema.ValueOrDie().schema_epoch,
                                            std::nullopt});
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
  state_->Finish();
  return Status::OK();
}

StatusOr<std::unique_ptr<Transaction>> Database::BeginTransaction(
    TransactionOptions options) {
  if (!impl_) return Status::InvalidArgument("database", "moved-from database");
  std::lock_guard<std::mutex> lock(impl_->mutex);
  if (impl_->closed) return Status::InvalidArgument("database", "database is closed");
  auto snapshot = impl_->store.BeginSnapshot();
  if (!snapshot.ok()) return snapshot.status();
  return std::unique_ptr<Transaction>(
      new Transaction(std::make_unique<Transaction::State>(
          impl_, options, snapshot.ConsumeValueOrDie())));
}

}  // namespace cedar
