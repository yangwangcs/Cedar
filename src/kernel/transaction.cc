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

CommitHandle::CommitHandle(std::shared_ptr<State> state) : state_(std::move(state)) {}

TxnId CommitHandle::txn_id() const {
  return state_ == nullptr ? TxnId{} : state_->txn_id;
}

CommitAcceptance CommitHandle::acceptance() const {
  return state_ == nullptr ? CommitAcceptance::kIndeterminate : state_->acceptance;
}

StatusOr<CommitResult> CommitHandle::Wait() const {
  if (state_ == nullptr) return Status::InvalidArgument("async commit", "empty handle");
  std::unique_lock<std::mutex> lock(state_->mutex);
  state_->completed.wait(lock, [this] { return state_->result.has_value(); });
  return *state_->result;
}

class Transaction::State {
 public:
  explicit State(std::shared_ptr<Database::Impl> database,
                 TransactionOptions options, StoreSnapshot snapshot, TxnId txn_id)
      : database(std::move(database)),
        options(options),
        snapshot(std::move(snapshot)),
        txn_id(txn_id) {}

  std::shared_ptr<Database::Impl> database;
  TransactionOptions options;
  std::optional<StoreSnapshot> snapshot;
  TxnId txn_id;
  std::vector<PendingFactMutation> mutations;
  std::vector<EdgeIdentity> edge_identities;
  std::vector<SnapshotWriteDependency> snapshot_write_dependencies;
  std::vector<StrictReadDependency> strict_read_dependencies;
  std::vector<std::tuple<uint32_t, uint8_t, uint16_t, uint64_t, uint64_t>>
      mutation_keys;
  bool terminal = false;

  Status CheckDatabaseOpen() const {
    std::lock_guard<std::mutex> lock(database->mutex);
    if (database->closed) {
      return Status::InvalidArgument("transaction", "database is closed");
    }
    if (database->closing) {
      return Status::ShutdownInProgress("transaction", "database close is in progress");
    }
    return Status::OK();
  }

  Status BeginCommit() {
    std::lock_guard<std::mutex> lock(database->mutex);
    if (database->closed) {
      return Status::InvalidArgument("transaction", "database is closed");
    }
    if (database->closing) {
      return Status::ShutdownInProgress("transaction", "database close is in progress");
    }
    ++database->active_commit_calls;
    return Status::OK();
  }

  void EndCommit() {
    std::lock_guard<std::mutex> lock(database->mutex);
    --database->active_commit_calls;
    if (database->active_commit_calls == 0) {
      database->commits_drained.notify_all();
    }
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

  Status CaptureStrictRead(const FactRef& ref, ValidTime valid_time) {
    if (options.isolation != IsolationLevel::kStrict) return Status::OK();
    const auto duplicate = std::find_if(
        strict_read_dependencies.begin(), strict_read_dependencies.end(),
        [&ref, valid_time](const StrictReadDependency& dependency) {
          return dependency.ref == ref && dependency.valid_time == valid_time;
        });
    if (duplicate != strict_read_dependencies.end()) return Status::OK();
    auto dependency = CaptureStrictReadDependency(
        database->store, *snapshot, ref, valid_time);
    if (!dependency.ok()) return dependency.status();
    strict_read_dependencies.push_back(dependency.ConsumeValueOrDie());
    return Status::OK();
  }

  StatusOr<bool> EdgeVisible(EdgeRef edge, ValidTime valid_time) {
    const FactRef edge_ref = EntityFact::Edge(edge).ref();
    const Status edge_dependency = CaptureStrictRead(edge_ref, valid_time);
    if (!edge_dependency.ok()) return edge_dependency;
    const auto edge_event = database->store.Read(*snapshot, edge_ref, valid_time);
    if (!edge_event.ok()) return edge_event.status();
    if (!edge_event.ValueOrDie().has_value()) return false;
    const auto identity = database->store.LookupEdgeIdentity(*snapshot, edge);
    if (!identity.ok()) return identity.status();
    if (!identity.ValueOrDie().has_value()) return false;

    for (VertexRef endpoint : {identity.ValueOrDie()->source_ref(),
                               identity.ValueOrDie()->target_ref()}) {
      const FactRef endpoint_ref = EntityFact::Vertex(endpoint).ref();
      const Status endpoint_dependency = CaptureStrictRead(endpoint_ref, valid_time);
      if (!endpoint_dependency.ok()) return endpoint_dependency;
      const auto state = database->store.Read(*snapshot, endpoint_ref, valid_time);
      if (!state.ok()) return state.status();
      if (!state.ValueOrDie().has_value()) return false;
    }
    return true;
  }

  Status Stage(PendingFactMutation mutation) {
    const Status valid = mutation.Validate();
    if (!valid.ok()) return valid;
    const auto key = std::tuple{mutation.ref.part_id().value,
                                static_cast<uint8_t>(mutation.ref.family()),
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

  Status StageEdge(EdgeIdentity identity, ValidTime valid_time) {
    const Status valid = identity.Validate();
    if (!valid.ok()) return valid;
    const auto existing = std::find_if(
        edge_identities.begin(), edge_identities.end(),
        [&identity](const EdgeIdentity& candidate) {
          return candidate.edge_id == identity.edge_id;
        });
    if (existing != edge_identities.end() && *existing != identity) {
      return Status::IdentityConflict("transaction", "edge ID has a different identity");
    }
    const Status staged = Stage(PendingFactMutation{
        EntityFact::Edge(identity.edge_ref()).ref(), valid_time, FactOperation::kPut,
        0, std::nullopt});
    if (!staged.ok()) return staged;
    if (existing == edge_identities.end()) edge_identities.push_back(identity);
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

StatusOr<bool> Transaction::Exists(EntityFact entity, ValidTime valid_time) {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status database_open = state_->CheckDatabaseOpen();
  if (!database_open.ok()) return database_open;
  if (entity.ref().family() == FactFamily::kEdgeState) {
    return state_->EdgeVisible(
        EdgeRef{entity.ref().part_id(), EdgeId{entity.ref().entity_id()}}, valid_time);
  }
  const Status dependency = state_->CaptureStrictRead(entity.ref(), valid_time);
  if (!dependency.ok()) return dependency;
  const auto event = state_->database->store.Read(
      *state_->snapshot, entity.ref(), valid_time);
  if (!event.ok()) return event.status();
  return event.ValueOrDie().has_value();
}

StatusOr<std::optional<Value>> Transaction::Get(PropertyFact property,
                                                ValidTime valid_time) {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status database_open = state_->CheckDatabaseOpen();
  if (!database_open.ok()) return database_open;
  const Status dependency = state_->CaptureStrictRead(property.ref(), valid_time);
  if (!dependency.ok()) return dependency;
  const auto event = state_->database->store.Read(
      *state_->snapshot, property.ref(), valid_time);
  if (!event.ok()) return event.status();
  if (!event.ValueOrDie().has_value()) return std::optional<Value>{};
  if (property.ref().family() == FactFamily::kEdgeProperty) {
    const auto visible = state_->EdgeVisible(
        EdgeRef{property.ref().part_id(), EdgeId{property.ref().entity_id()}}, valid_time);
    if (!visible.ok()) return visible.status();
    if (!visible.ValueOrDie()) return std::optional<Value>{};
  }
  return event.ValueOrDie()->value;
}

Status Transaction::Scan(FactFamily family, PropertyId property_id,
                         const TransactionFactVisitor& visitor) {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status database_open = state_->CheckDatabaseOpen();
  if (!database_open.ok()) return database_open;
  if (state_->options.isolation == IsolationLevel::kStrict) {
    return Status::UnsupportedSerializablePredicate(
        "transaction", "strict transactions support only exact reads");
  }
  return state_->database->store.Scan(*state_->snapshot,
                                      FactPrefix::Family(PartId{}, family, property_id),
                                      visitor);
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

Status Transaction::Assert(EdgeIdentity identity, ValidTime valid_time) {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status database_open = state_->CheckDatabaseOpen();
  if (!database_open.ok()) return database_open;
  return state_->StageEdge(std::move(identity), valid_time);
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
  const Status begun = state_->BeginCommit();
  if (!begun.ok()) return begun;
  StoreCommitBatch batch{state_->txn_id,
                         state_->txn_id.value,
                         std::move(state_->mutations),
                         std::move(state_->edge_identities),
                         std::move(state_->snapshot_write_dependencies),
                         std::move(state_->strict_read_dependencies), true};
  const auto committed = state_->database->SubmitSyncCommit(
      batch, state_->options.commit_deadline_us);
  state_->Finish();
  state_->EndCommit();
  if (!committed.ok()) {
    const Status status = committed.status();
    return CommitResult{status.IsIndeterminate() || status.IsRecoveryRequired()
                            ? CommitOutcome::kIndeterminate
                            : CommitOutcome::kAborted,
                        CommitSeq{}, batch.txn_id, status};
  }
  return CommitResult{CommitOutcome::kCommitted, committed.ValueOrDie().commit_seq,
                      batch.txn_id, Status::OK()};
}

StatusOr<CommitHandle> Transaction::CommitAsync() {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status begun = state_->BeginCommit();
  if (!begun.ok()) return begun;
  StoreCommitBatch batch{state_->txn_id,
                         state_->txn_id.value,
                         std::move(state_->mutations),
                         std::move(state_->edge_identities),
                         std::move(state_->snapshot_write_dependencies),
                         std::move(state_->strict_read_dependencies), true};
  auto handle_state = std::make_shared<CommitHandle::State>(batch.txn_id);
  const Status enqueued = state_->database->SubmitAsyncCommit(
      batch, handle_state, state_->options.commit_deadline_us);
  state_->Finish();
  state_->EndCommit();
  if (!enqueued.ok()) return enqueued;
  return CommitHandle(std::move(handle_state));
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
  if (impl_->closing) {
    return Status::ShutdownInProgress("database", "database close is in progress");
  }
  auto txn_id = impl_->store.AllocateTransactionId();
  if (!txn_id.ok()) return txn_id.status();
  auto snapshot = impl_->store.BeginSnapshot();
  if (!snapshot.ok()) return snapshot.status();
  return std::unique_ptr<Transaction>(
      new Transaction(std::make_unique<Transaction::State>(
          impl_, options, snapshot.ConsumeValueOrDie(),
          txn_id.ConsumeValueOrDie())));
}

}  // namespace cedar
