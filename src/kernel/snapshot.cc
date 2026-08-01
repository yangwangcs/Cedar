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

StatusOr<bool> Snapshot::Exists(EntityFact, ValidTime) const {
  if (!state_) return Status::InvalidArgument("snapshot", "moved-from snapshot");
  return Status::NotSupported("snapshot", "reads are not implemented yet");
}

StatusOr<std::optional<Value>> Snapshot::Get(PropertyFact, ValidTime) const {
  if (!state_) return Status::InvalidArgument("snapshot", "moved-from snapshot");
  return Status::NotSupported("snapshot", "reads are not implemented yet");
}

Status Snapshot::Scan(FactFamily, PropertyId, const SnapshotFactVisitor&) const {
  if (!state_) return Status::InvalidArgument("snapshot", "moved-from snapshot");
  return Status::NotSupported("snapshot", "scans are not implemented yet");
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
