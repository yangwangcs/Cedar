// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_FACT_GROUP_COMMIT_PLANNER_H_
#define CEDAR_FACT_GROUP_COMMIT_PLANNER_H_

#include <cstdint>
#include <set>
#include <tuple>
#include <unordered_set>

#include "cedar/fact/fact_store.h"

namespace cedar::internal {

using FactIdentity = std::tuple<uint32_t, uint8_t, uint16_t, uint64_t>;
using EdgeIdentityKey = std::pair<uint32_t, uint64_t>;

struct FactIdentityHash {
  size_t operator()(const FactIdentity& identity) const noexcept;
};

struct EdgeIdentityKeyHash {
  size_t operator()(const EdgeIdentityKey& identity) const noexcept;
};

struct CommitFootprint {
  std::set<uint64_t> txn_ids;
  std::set<FactIdentity> writes;
  std::set<FactIdentity> strict_reads;
  std::set<EdgeIdentityKey> edge_ids;
};

// Epoch-local index for FIFO group selection. Each candidate checks only its
// own footprint against the accumulated epoch state, making selection linear
// in the total number of footprint entries rather than quadratic in requests.
class CommitConflictIndex {
 public:
  bool CanInsert(const CommitFootprint& candidate) const;
  bool Insert(const CommitFootprint& candidate);
  size_t size() const { return txn_ids_.size(); }

 private:
  std::unordered_set<uint64_t> txn_ids_;
  std::unordered_set<FactIdentity, FactIdentityHash> writes_;
  std::unordered_set<FactIdentity, FactIdentityHash> strict_reads_;
  std::unordered_set<EdgeIdentityKey, EdgeIdentityKeyHash> edge_ids_;
};

CommitFootprint BuildCommitFootprint(const StoreCommitBatch& batch);
size_t EstimateCommitBatchBytes(const StoreCommitBatch& batch);
bool CanSharePhysicalWrite(const CommitFootprint& left,
                           const CommitFootprint& right);
bool CanUseAppendFastPath(const StoreCommitBatch& batch);

}  // namespace cedar::internal

#endif  // CEDAR_FACT_GROUP_COMMIT_PLANNER_H_
