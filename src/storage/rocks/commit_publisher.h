// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_FACT_COMMIT_PUBLISHER_H_
#define CEDAR_FACT_COMMIT_PUBLISHER_H_

#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "storage/facts/fact_store.h"

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

// Appends one candidate transaction to `write_batch`. The committed sequence
// record is always encoded with the only supported format (meta sequence v2,
// `kSequenceV2`); the transaction-outcome record, facts and routing metadata are
// unchanged.
Status AppendCandidateToWriteBatch(const CandidateCommit& candidate,
                                   rocksdb::ColumnFamilyHandle* facts_cf,
                                   rocksdb::ColumnFamilyHandle* meta_cf,
                                   rocksdb::WriteBatch* write_batch);

// Rejects a fully assembled group WriteBatch whose real encoded size exceeds
// the hard byte cap. Callers run it after assembly, so an estimate that is too
// small can never turn into an oversized durable write.
Status CheckCommittedBatchSize(const rocksdb::WriteBatch& batch,
                               uint64_t max_bytes);

}  // namespace cedar::internal

#endif  // CEDAR_FACT_COMMIT_PUBLISHER_H_
