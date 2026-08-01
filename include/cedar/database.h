// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_DATABASE_H_
#define CEDAR_DATABASE_H_

#include <functional>
#include <cstdint>
#include <memory>
#include <string>

#include "cedar/core/status.h"
#include "cedar/fact/fact.h"
#include "cedar/schema.h"
#include "cedar/snapshot.h"
#include "cedar/transaction.h"

namespace cedar {

struct DatabaseOptions {
  std::string path;
  uint64_t write_buffer_bytes = 64ULL * 1024ULL * 1024ULL;
  uint64_t block_cache_bytes = 256ULL * 1024ULL * 1024ULL;
  uint64_t blob_threshold_bytes = 4096;
  std::function<Status()> commit_prewrite_fault_injector_for_testing;
  std::function<Status()> commit_fault_injector_for_testing;
};

class Database {
 public:
  static StatusOr<std::unique_ptr<Database>> Open(DatabaseOptions options);

  ~Database();
  Database(const Database&) = delete;
  Database& operator=(const Database&) = delete;

  Status Close();
  StatusOr<VertexId> AllocateVertexId();
  StatusOr<EdgeId> AllocateEdgeId();
  StatusOr<PropertyDefinition> RegisterProperty(PropertyDefinition definition);
  StatusOr<std::unique_ptr<Transaction>> BeginTransaction(
      TransactionOptions options = {});
  StatusOr<Snapshot> BeginSnapshot(SnapshotOptions options = {}) const;
  StatusOr<std::optional<CommitResult>> ResolveTransaction(TxnId txn_id) const;
  Status Vacuum(CommitSeq oldest_readable);

 private:
  class Impl;
  explicit Database(std::shared_ptr<Impl> impl);

  std::shared_ptr<Impl> impl_;

  friend class Snapshot;
  friend class Transaction;
};

}  // namespace cedar

#endif  // CEDAR_DATABASE_H_
