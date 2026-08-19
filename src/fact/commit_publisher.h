// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_FACT_COMMIT_PUBLISHER_H_
#define CEDAR_FACT_COMMIT_PUBLISHER_H_

#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/fact/fact_store.h"

namespace rocksdb {
class ColumnFamilyHandle;
class WriteBatch;
}  // namespace rocksdb

namespace cedar::internal {

struct CandidateCommit {
  const StoreCommitBatch* batch = nullptr;
  CommitSeq commit_seq;
  std::vector<std::string> fact_keys;
};

Status AppendCandidateToWriteBatch(const CandidateCommit& candidate,
                                   rocksdb::ColumnFamilyHandle* facts_cf,
                                   rocksdb::ColumnFamilyHandle* meta_cf,
                                   rocksdb::WriteBatch* write_batch);

}  // namespace cedar::internal

#endif  // CEDAR_FACT_COMMIT_PUBLISHER_H_
