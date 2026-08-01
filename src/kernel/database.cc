// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/database.h"

#include <utility>

#include "kernel/database_impl.h"

namespace cedar {

Database::Database(std::shared_ptr<Impl> impl) : impl_(std::move(impl)) {}
Database::~Database() { Close().IgnoreError(); }

StatusOr<std::unique_ptr<Database>> Database::Open(DatabaseOptions options) {
  if (options.path.empty()) return Status::InvalidArgument("database", "missing path");
  auto impl = std::make_shared<Impl>(std::move(options));
  const Status opened = impl->store.Open();
  if (!opened.ok()) return opened;
  return std::unique_ptr<Database>(new Database(std::move(impl)));
}

Status Database::Close() {
  if (!impl_) return Status::OK();
  std::lock_guard<std::mutex> lock(impl_->mutex);
  if (impl_->closed) return Status::OK();
  const Status closed = impl_->store.Close();
  if (!closed.ok()) return closed;
  impl_->closed = true;
  return Status::OK();
}

StatusOr<VertexId> Database::AllocateVertexId() {
  if (!impl_) return Status::InvalidArgument("database", "moved-from database");
  std::lock_guard<std::mutex> lock(impl_->mutex);
  if (impl_->closed) return Status::InvalidArgument("database", "database is closed");
  const auto lease = impl_->store.LeaseIds(IdKind::kVertex, 1);
  if (!lease.ok()) return lease.status();
  return VertexId{lease.ValueOrDie().first_id};
}

StatusOr<EdgeId> Database::AllocateEdgeId() {
  if (!impl_) return Status::InvalidArgument("database", "moved-from database");
  std::lock_guard<std::mutex> lock(impl_->mutex);
  if (impl_->closed) return Status::InvalidArgument("database", "database is closed");
  const auto lease = impl_->store.LeaseIds(IdKind::kEdge, 1);
  if (!lease.ok()) return lease.status();
  return EdgeId{lease.ValueOrDie().first_id};
}

StatusOr<PropertyDefinition> Database::RegisterProperty(
    PropertyDefinition definition) {
  if (!impl_) return Status::InvalidArgument("database", "moved-from database");
  std::lock_guard<std::mutex> lock(impl_->mutex);
  if (impl_->closed) return Status::InvalidArgument("database", "database is closed");
  return impl_->store.RegisterProperty(std::move(definition));
}

Status Database::Vacuum(CommitSeq) {
  if (!impl_) return Status::InvalidArgument("database", "moved-from database");
  std::lock_guard<std::mutex> lock(impl_->mutex);
  if (impl_->closed) return Status::InvalidArgument("database", "database is closed");
  return Status::NotSupported("database", "vacuum is not implemented yet");
}

StatusOr<std::optional<CommitResult>> Database::ResolveTransaction(
    TxnId txn_id) const {
  if (!impl_) return Status::InvalidArgument("database", "moved-from database");
  std::lock_guard<std::mutex> lock(impl_->mutex);
  if (impl_->closed) return Status::InvalidArgument("database", "database is closed");
  const auto resolved = impl_->store.ResolveTransaction(txn_id);
  if (!resolved.ok()) return resolved.status();
  if (!resolved.ValueOrDie().has_value()) {
    return std::optional<CommitResult>{};
  }
  const StoreCommitResult stored = *resolved.ValueOrDie();
  return std::optional<CommitResult>{CommitResult{
      CommitOutcome::kCommitted, stored.commit_seq, txn_id, Status::OK()}};
}

}  // namespace cedar
