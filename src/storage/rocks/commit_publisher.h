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

// The canonical fact set includes identity facts synthesized by the writer.
// Visit without retaining another copy of the transaction's value payloads.
template <typename Visitor>
Status VisitCommitFacts(const StoreCommitBatch& batch, Visitor visitor) {
  for (const auto& mutation : batch.mutations) {
    const Status status = visitor(mutation, nullptr);
    if (!status.ok()) return status;
  }
  for (const auto& identity : batch.edge_identities) {
    const PendingFactMutation mutation{
        FactRef(identity.home_part_id, FactFamily::kEdgeIdentity,
                PropertyId{}, identity.edge_id.value),
        ValidTime{0}, FactOperation::kPut, 0, std::nullopt};
    const Status status = visitor(mutation, &identity);
    if (!status.ok()) return status;
  }
  return Status::OK();
}

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
