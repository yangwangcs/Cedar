// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_DB_DATABASE_LIFECYCLE_H_
#define CEDAR_DB_DATABASE_LIFECYCLE_H_

#include <array>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "cedar/core/status.h"
#include "cedar/tcypher/runtime/cancellation.h"

namespace cedar {

enum class DatabasePhase : uint8_t {
  kRunning = 0,
  kQuiescing = 1,
  kDrainingCommits = 2,
  kDrainingMaintenance = 3,
  kCheckpointing = 4,
  kClosed = 5,
};

enum class DatabaseOperationClass : uint8_t {
  kCommit = 0,
  kPointRead = 1,
  kQuery = 2,
  kMaintenance = 3,
};

class DatabaseLifecycle;
class DatabaseQueryRegistration;

class DatabaseOperationLease {
 public:
  DatabaseOperationLease() = default;
  ~DatabaseOperationLease();
  DatabaseOperationLease(DatabaseOperationLease&& other) noexcept;
  DatabaseOperationLease& operator=(DatabaseOperationLease&& other) noexcept;

  DatabaseOperationLease(const DatabaseOperationLease&) = delete;
  DatabaseOperationLease& operator=(const DatabaseOperationLease&) = delete;

 private:
  friend class DatabaseLifecycle;
  DatabaseOperationLease(std::shared_ptr<DatabaseLifecycle> lifecycle,
                         DatabaseOperationClass operation_class)
      : lifecycle_(std::move(lifecycle)), operation_class_(operation_class) {}
  void Reset();

  std::shared_ptr<DatabaseLifecycle> lifecycle_;
  DatabaseOperationClass operation_class_ = DatabaseOperationClass::kCommit;
};

class DatabaseLifecycle
    : public std::enable_shared_from_this<DatabaseLifecycle> {
 public:
  StatusOr<DatabaseOperationLease> TryEnter(
      DatabaseOperationClass operation_class);
  Status BeginOpen();
  bool BeginClose();
  void SetPhase(DatabasePhase phase);
  void FinishClose(Status result);
  Status WaitForClose() const;
  void WaitForNoOperations(DatabaseOperationClass operation_class) const;
  uint64_t active_operations(DatabaseOperationClass operation_class) const;
  StatusOr<std::shared_ptr<DatabaseQueryRegistration>> RegisterQuery(
      std::shared_ptr<QueryCancellation> cancellation);
  uint64_t CancelQueriesAndWaitForCalls();
  void WaitForQueriesDrained() const;
  uint64_t active_query_count() const;
  uint64_t active_query_calls() const;
  DatabasePhase phase() const;

 private:
  friend class DatabaseOperationLease;
  friend class DatabaseQueryRegistration;
  struct QueryState {
    std::shared_ptr<QueryCancellation> cancellation;
    std::function<void()> cleanup;
    uint64_t active_calls = 0;
    bool terminal = false;
  };
  static size_t OperationIndex(DatabaseOperationClass operation_class);
  void Leave(DatabaseOperationClass operation_class);
  Status BeginQueryCall(uint64_t query_id);
  void EndQueryCall(uint64_t query_id, bool terminal);
  void ReleaseQuery(uint64_t query_id);

  mutable std::mutex mutex_;
  mutable std::condition_variable changed_;
  std::array<uint64_t, 4> active_operations_{};
  std::unordered_map<uint64_t, QueryState> queries_;
  uint64_t next_query_id_ = 1;
  bool cancel_queries_ = false;
  bool open_attempted_ = false;
  DatabasePhase phase_ = DatabasePhase::kRunning;
  Status close_result_ = Status::OK();
};

class DatabaseQueryRegistration {
 public:
  ~DatabaseQueryRegistration();
  DatabaseQueryRegistration(const DatabaseQueryRegistration&) = delete;
  DatabaseQueryRegistration& operator=(const DatabaseQueryRegistration&) = delete;

  Status BeginCall();
  void EndCall(bool terminal);
  void SetCleanup(std::function<void()> cleanup);

 private:
  friend class DatabaseLifecycle;
  DatabaseQueryRegistration(std::shared_ptr<DatabaseLifecycle> lifecycle,
                            uint64_t query_id)
      : lifecycle_(std::move(lifecycle)), query_id_(query_id) {}

  std::shared_ptr<DatabaseLifecycle> lifecycle_;
  uint64_t query_id_ = 0;
};

}  // namespace cedar

#endif  // CEDAR_DB_DATABASE_LIFECYCLE_H_
