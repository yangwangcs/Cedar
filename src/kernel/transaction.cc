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
  std::vector<StrictReadDependency> strict_read_dependencies;
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

  StatusOr<bool> EdgeVisible(EdgeId edge_id, ValidTime valid_time) {
    const FactRef edge_ref = EntityFact::Edge(edge_id).ref();
    const Status edge_dependency = CaptureStrictRead(edge_ref, valid_time);
    if (!edge_dependency.ok()) return edge_dependency;
    const auto edge = database->store.Read(*snapshot, edge_ref, valid_time);
    if (!edge.ok()) return edge.status();
    if (!edge.ValueOrDie().has_value()) return false;
    const auto identity = database->store.LookupEdgeIdentity(*snapshot, edge_id);
    if (!identity.ok()) return identity.status();
    if (!identity.ValueOrDie().has_value()) return false;

    for (VertexId endpoint : {identity.ValueOrDie()->source_vertex_id,
                              identity.ValueOrDie()->target_vertex_id}) {
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

StatusOr<bool> Transaction::Exists(EntityFact entity, ValidTime valid_time) {
  if (!state_) return Status::InvalidArgument("transaction", "moved-from transaction");
  if (state_->terminal) return Status::InvalidArgument("transaction", "terminal transaction");
  const Status database_open = state_->CheckDatabaseOpen();
  if (!database_open.ok()) return database_open;
  if (entity.ref().family() == FactFamily::kEdgeState) {
    return state_->EdgeVisible(EdgeId{entity.ref().entity_id()}, valid_time);
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
        EdgeId{property.ref().entity_id()}, valid_time);
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
                                      FactPrefix::Family(family, property_id),
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
