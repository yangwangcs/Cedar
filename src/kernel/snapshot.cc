// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/snapshot.h"

#include <utility>

#include "cedar/database.h"
#include "kernel/database_impl.h"

namespace cedar {

class Snapshot::State {
 public:
  State(std::shared_ptr<Database::Impl> database, StoreSnapshot snapshot)
      : database(std::move(database)), snapshot(std::move(snapshot)) {}

  std::shared_ptr<Database::Impl> database;
  StoreSnapshot snapshot;

  StatusOr<bool> EdgeVisible(EdgeId edge_id, ValidTime valid_time) const {
    const auto edge = database->store.Read(
        snapshot, EntityFact::Edge(edge_id).ref(), valid_time);
    if (!edge.ok()) return edge.status();
    if (!edge.ValueOrDie().has_value()) return false;
    const auto identity = database->store.LookupEdgeIdentity(snapshot, edge_id);
    if (!identity.ok()) return identity.status();
    if (!identity.ValueOrDie().has_value()) return false;
    const auto source = database->store.Read(
        snapshot,
        EntityFact::Vertex(identity.ValueOrDie()->source_vertex_id).ref(),
        valid_time);
    if (!source.ok()) return source.status();
    if (!source.ValueOrDie().has_value()) return false;
    const auto target = database->store.Read(
        snapshot,
        EntityFact::Vertex(identity.ValueOrDie()->target_vertex_id).ref(),
        valid_time);
    if (!target.ok()) return target.status();
    return target.ValueOrDie().has_value();
  }
};

Snapshot::Snapshot(std::unique_ptr<State> state) : state_(std::move(state)) {}
Snapshot::~Snapshot() = default;
Snapshot::Snapshot(Snapshot&&) noexcept = default;
Snapshot& Snapshot::operator=(Snapshot&&) noexcept = default;

CommitSeq Snapshot::commit_seq() const {
  return state_ == nullptr ? CommitSeq{} : state_->snapshot.commit_seq();
}

CommitSeq Snapshot::oldest_readable_seq() const {
  return state_ == nullptr ? CommitSeq{} : state_->snapshot.oldest_readable_seq();
}

StatusOr<bool> Snapshot::Exists(EntityFact entity, ValidTime valid_time) const {
  if (!state_) return Status::InvalidArgument("snapshot", "moved-from snapshot");
  if (entity.ref().family() == FactFamily::kEdgeState) {
    return state_->EdgeVisible(EdgeId{entity.ref().entity_id()}, valid_time);
  }
  const auto event = state_->database->store.Read(state_->snapshot, entity.ref(),
                                                   valid_time);
  if (!event.ok()) return event.status();
  return event.ValueOrDie().has_value();
}

StatusOr<std::optional<Value>> Snapshot::Get(PropertyFact property,
                                              ValidTime valid_time) const {
  if (!state_) return Status::InvalidArgument("snapshot", "moved-from snapshot");
  const auto event = state_->database->store.Read(state_->snapshot, property.ref(),
                                                   valid_time);
  if (!event.ok()) return event.status();
  if (!event.ValueOrDie().has_value()) return std::optional<Value>{};
  if (property.ref().family() == FactFamily::kEdgeProperty) {
    const auto visible = state_->EdgeVisible(EdgeId{property.ref().entity_id()},
                                             valid_time);
    if (!visible.ok()) return visible.status();
    if (!visible.ValueOrDie()) return std::optional<Value>{};
  }
  return event.ValueOrDie()->value;
}

Status Snapshot::Scan(FactFamily family, PropertyId property_id,
                      const SnapshotFactVisitor& visitor) const {
  if (!state_) return Status::InvalidArgument("snapshot", "moved-from snapshot");
  return state_->database->store.Scan(state_->snapshot,
                                      FactPrefix::Family(family, property_id),
                                      visitor);
}

StatusOr<Snapshot> Database::BeginSnapshot(SnapshotOptions options) const {
  if (!impl_) return Status::InvalidArgument("database", "moved-from database");
  std::lock_guard<std::mutex> lock(impl_->mutex);
  if (impl_->closed) return Status::InvalidArgument("database", "database is closed");
  auto snapshot = impl_->store.BeginSnapshot(options);
  if (!snapshot.ok()) return snapshot.status();
  return Snapshot(std::make_unique<Snapshot::State>(
      impl_, snapshot.ConsumeValueOrDie()));
}

}  // namespace cedar
