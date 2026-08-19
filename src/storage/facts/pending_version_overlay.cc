// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "storage/facts/pending_version_overlay.h"

namespace cedar::internal {
namespace {

template <typename Accumulated, typename Candidate>
bool Intersects(const Accumulated& accumulated, const Candidate& candidate) {
  for (const auto& value : candidate) {
    if (accumulated.contains(value)) return true;
  }
  return false;
}

FactIdentity IdentityOf(const FactRef& ref) {
  return {ref.part_id().value, static_cast<uint8_t>(ref.family()),
          ref.property_id().value, ref.entity_id()};
}

}  // namespace

PendingVersionOverlay PendingVersionOverlay::FromBatch(
    const StoreCommitBatch& batch) {
  return FromBatches({&batch});
}

PendingVersionOverlay PendingVersionOverlay::FromBatches(
    const std::vector<const StoreCommitBatch*>& batches) {
  PendingVersionOverlay overlay;
  for (const StoreCommitBatch* batch : batches) {
    if (batch == nullptr) continue;
    const CommitFootprint footprint = BuildCommitFootprint(*batch);
    overlay.writes_.insert(footprint.writes.begin(), footprint.writes.end());
    overlay.strict_reads_.insert(footprint.strict_reads.begin(),
                                 footprint.strict_reads.end());
    overlay.edge_ids_.insert(footprint.edge_ids.begin(), footprint.edge_ids.end());
    overlay.txn_ids_.insert(footprint.txn_ids.begin(), footprint.txn_ids.end());
    for (const PendingFactMutation& mutation : batch->mutations) {
      overlay.entries_.push_back(Entry{
          IdentityOf(mutation.ref), mutation.valid_from, mutation.operation,
          mutation.schema_epoch,
          mutation.value.has_value()
              ? std::optional<PhysicalType>(mutation.value->type())
              : std::nullopt});
    }
  }
  return overlay;
}

bool PendingVersionOverlay::Conflicts(
    const CommitFootprint& candidate) const {
  return Intersects(txn_ids_, candidate.txn_ids) ||
         Intersects(writes_, candidate.writes) ||
         Intersects(writes_, candidate.strict_reads) ||
         Intersects(strict_reads_, candidate.writes) ||
         Intersects(edge_ids_, candidate.edge_ids);
}

}  // namespace cedar::internal
