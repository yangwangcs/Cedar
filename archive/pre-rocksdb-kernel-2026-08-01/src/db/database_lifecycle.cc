// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/db/database_lifecycle.h"

#include <utility>
#include <vector>

namespace cedar {

DatabaseOperationLease::~DatabaseOperationLease() { Reset(); }

DatabaseOperationLease::DatabaseOperationLease(
    DatabaseOperationLease&& other) noexcept
    : lifecycle_(std::move(other.lifecycle_)),
      operation_class_(other.operation_class_) {}

DatabaseOperationLease& DatabaseOperationLease::operator=(
    DatabaseOperationLease&& other) noexcept {
  if (this == &other) return *this;
  Reset();
  lifecycle_ = std::move(other.lifecycle_);
  operation_class_ = other.operation_class_;
  return *this;
}

void DatabaseOperationLease::Reset() {
  if (!lifecycle_) return;
  std::shared_ptr<DatabaseLifecycle> lifecycle = std::move(lifecycle_);
  lifecycle->Leave(operation_class_);
}

size_t DatabaseLifecycle::OperationIndex(
    DatabaseOperationClass operation_class) {
  return static_cast<size_t>(operation_class);
}

StatusOr<DatabaseOperationLease> DatabaseLifecycle::TryEnter(
    DatabaseOperationClass operation_class) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (phase_ != DatabasePhase::kRunning) {
    return Status::ShutdownInProgress("database lifecycle",
                                      "database is closing");
  }
  ++active_operations_[OperationIndex(operation_class)];
  return DatabaseOperationLease(shared_from_this(), operation_class);
}

Status DatabaseLifecycle::BeginOpen() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (phase_ != DatabasePhase::kRunning) {
    return Status::ShutdownInProgress("database open",
                                      "database is closing");
  }
  if (open_attempted_) {
    return Status::InvalidArgument("database open",
                                   "database object open was already attempted");
  }
  open_attempted_ = true;
  return Status::OK();
}

bool DatabaseLifecycle::BeginClose() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (phase_ != DatabasePhase::kRunning) return false;
  phase_ = DatabasePhase::kQuiescing;
  changed_.notify_all();
  return true;
}

void DatabaseLifecycle::SetPhase(DatabasePhase phase) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (phase_ == DatabasePhase::kClosed) return;
  if (static_cast<uint8_t>(phase) < static_cast<uint8_t>(phase_)) return;
  phase_ = phase;
  changed_.notify_all();
}

void DatabaseLifecycle::FinishClose(Status result) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (phase_ == DatabasePhase::kClosed) return;
  close_result_ = std::move(result);
  phase_ = DatabasePhase::kClosed;
  changed_.notify_all();
}

Status DatabaseLifecycle::WaitForClose() const {
  std::unique_lock<std::mutex> lock(mutex_);
  changed_.wait(lock, [this] { return phase_ == DatabasePhase::kClosed; });
  return close_result_;
}

void DatabaseLifecycle::WaitForNoOperations(
    DatabaseOperationClass operation_class) const {
  const size_t index = OperationIndex(operation_class);
  std::unique_lock<std::mutex> lock(mutex_);
  changed_.wait(lock, [this, index] { return active_operations_[index] == 0; });
}

uint64_t DatabaseLifecycle::active_operations(
    DatabaseOperationClass operation_class) const {
  std::lock_guard<std::mutex> lock(mutex_);
  return active_operations_[OperationIndex(operation_class)];
}

DatabasePhase DatabaseLifecycle::phase() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return phase_;
}

void DatabaseLifecycle::Leave(DatabaseOperationClass operation_class) {
  std::lock_guard<std::mutex> lock(mutex_);
  const size_t index = OperationIndex(operation_class);
  if (active_operations_[index] != 0) --active_operations_[index];
  changed_.notify_all();
}

StatusOr<std::shared_ptr<DatabaseQueryRegistration>>
DatabaseLifecycle::RegisterQuery(
    std::shared_ptr<QueryCancellation> cancellation) {
  if (!cancellation) {
    return Status::InvalidArgument("database lifecycle",
                                   "missing query cancellation token");
  }
  std::lock_guard<std::mutex> lock(mutex_);
  if (phase_ != DatabasePhase::kRunning) {
    return Status::ShutdownInProgress("database lifecycle",
                                      "database is closing");
  }
  const uint64_t query_id = next_query_id_++;
  queries_.emplace(
      query_id,
      QueryState{std::move(cancellation), std::function<void()>{}, 0, false});
  return std::shared_ptr<DatabaseQueryRegistration>(
      new DatabaseQueryRegistration(shared_from_this(), query_id));
}

uint64_t DatabaseLifecycle::CancelQueriesAndWaitForCalls() {
  std::unique_lock<std::mutex> lock(mutex_);
  cancel_queries_ = true;
  uint64_t cancelled = 0;
  for (auto& query : queries_) {
    if (!query.second.terminal) ++cancelled;
    query.second.cancellation->Cancel();
  }
  changed_.wait(lock, [this] {
    for (const auto& query : queries_) {
      if (query.second.active_calls != 0) return false;
    }
    return true;
  });
  std::vector<std::function<void()>> cleanups;
  cleanups.reserve(queries_.size());
  for (const auto& query : queries_) {
    if (query.second.cleanup) cleanups.push_back(query.second.cleanup);
  }
  lock.unlock();
  for (const auto& cleanup : cleanups) cleanup();
  return cancelled;
}

void DatabaseLifecycle::WaitForQueriesDrained() const {
  std::unique_lock<std::mutex> lock(mutex_);
  changed_.wait(lock, [this] {
    for (const auto& query : queries_) {
      if (!query.second.terminal) return false;
    }
    return true;
  });
}

uint64_t DatabaseLifecycle::active_query_count() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return queries_.size();
}

uint64_t DatabaseLifecycle::active_query_calls() const {
  std::lock_guard<std::mutex> lock(mutex_);
  uint64_t calls = 0;
  for (const auto& query : queries_) calls += query.second.active_calls;
  return calls;
}

Status DatabaseLifecycle::BeginQueryCall(uint64_t query_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto query = queries_.find(query_id);
  if (query == queries_.end() || cancel_queries_) {
    return Status::QueryCancelled("query shutdown",
                                  "database is closing");
  }
  ++query->second.active_calls;
  return Status::OK();
}

void DatabaseLifecycle::EndQueryCall(uint64_t query_id, bool terminal) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto query = queries_.find(query_id);
  if (query == queries_.end()) return;
  if (query->second.active_calls != 0) --query->second.active_calls;
  query->second.terminal = query->second.terminal || terminal;
  changed_.notify_all();
}

void DatabaseLifecycle::ReleaseQuery(uint64_t query_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  queries_.erase(query_id);
  changed_.notify_all();
}

DatabaseQueryRegistration::~DatabaseQueryRegistration() {
  if (lifecycle_) lifecycle_->ReleaseQuery(query_id_);
}

Status DatabaseQueryRegistration::BeginCall() {
  if (!lifecycle_) {
    return Status::QueryCancelled("query shutdown",
                                  "query registration was released");
  }
  return lifecycle_->BeginQueryCall(query_id_);
}

void DatabaseQueryRegistration::EndCall(bool terminal) {
  if (lifecycle_) lifecycle_->EndQueryCall(query_id_, terminal);
}

void DatabaseQueryRegistration::SetCleanup(std::function<void()> cleanup) {
  if (!lifecycle_) return;
  std::lock_guard<std::mutex> lock(lifecycle_->mutex_);
  const auto query = lifecycle_->queries_.find(query_id_);
  if (query != lifecycle_->queries_.end()) {
    query->second.cleanup = std::move(cleanup);
  }
}

}  // namespace cedar
