// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_FACT_PENDING_VERSION_OVERLAY_H_
#define CEDAR_FACT_PENDING_VERSION_OVERLAY_H_

#include <cstddef>
#include <optional>
#include <unordered_set>
#include <vector>

#include "fact/group_commit_planner.h"

namespace cedar::internal {

// Immutable, compacted validation state for the epoch currently in the
// RocksDB write path. It deliberately records only conflict-relevant identity
// information; it neither owns mutation payloads nor exposes RocksDB types.
class PendingVersionOverlay {
 public:
  struct Entry {
    FactIdentity identity;
    ValidTime valid_from;
    FactOperation operation;
    uint32_t schema_epoch = 0;
    std::optional<PhysicalType> value_type;
  };

  static PendingVersionOverlay FromBatch(const StoreCommitBatch& batch);
  static PendingVersionOverlay FromBatches(
      const std::vector<const StoreCommitBatch*>& batches);

  bool Conflicts(const CommitFootprint& candidate) const;
  size_t size() const { return entries_.size() + edge_ids_.size(); }

 private:
  std::unordered_set<FactIdentity, FactIdentityHash> writes_;
  std::unordered_set<FactIdentity, FactIdentityHash> strict_reads_;
  std::unordered_set<EdgeIdentityKey, EdgeIdentityKeyHash> edge_ids_;
  std::unordered_set<uint64_t> txn_ids_;
  std::vector<Entry> entries_;
};

}  // namespace cedar::internal

#endif  // CEDAR_FACT_PENDING_VERSION_OVERLAY_H_
