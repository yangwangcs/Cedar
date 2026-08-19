// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "storage/facts/group_commit_planner.h"

#include <algorithm>
#include <limits>

namespace cedar::internal {
namespace {

void SaturatingAdd(size_t amount, size_t* total) {
  if (amount > std::numeric_limits<size_t>::max() - *total) {
    *total = std::numeric_limits<size_t>::max();
  } else {
    *total += amount;
  }
}

size_t ValuePayloadBytes(const Value& value) {
  switch (value.type()) {
    case PhysicalType::kBool:
      return 1;
    case PhysicalType::kInt32:
    case PhysicalType::kFloat32:
      return 4;
    case PhysicalType::kInt64:
    case PhysicalType::kFloat64:
    case PhysicalType::kTimestamp64:
      return 8;
    case PhysicalType::kString:
    case PhysicalType::kBinary:
      return std::get<std::string>(value.data()).size();
  }
  return 0;
}

void SaturatingMultiply(size_t left, size_t right, size_t* total) {
  if (left != 0 && right > std::numeric_limits<size_t>::max() / left) {
    *total = std::numeric_limits<size_t>::max();
    return;
  }
  SaturatingAdd(left * right, total);
}

void AddFactEventBytes(const FactEvent& event, size_t* total) {
  // Includes the fixed event fields plus the largest owned payload. The
  // surrounding dependency and vector framing are accounted for separately.
  SaturatingAdd(64, total);
  if (event.value.has_value()) {
    SaturatingAdd(ValuePayloadBytes(*event.value), total);
  }
}

FactIdentity IdentityOf(const FactRef& ref) {
  return {ref.part_id().value, static_cast<uint8_t>(ref.family()),
          ref.property_id().value, ref.entity_id()};
}

template <typename Value>
bool Disjoint(const std::set<Value>& left, const std::set<Value>& right) {
  auto left_it = left.begin();
  auto right_it = right.begin();
  while (left_it != left.end() && right_it != right.end()) {
    if (*left_it < *right_it) {
      ++left_it;
    } else if (*right_it < *left_it) {
      ++right_it;
    } else {
      return false;
    }
  }
  return true;
}

uint64_t Mix(uint64_t value) {
  value += 0x9e3779b97f4a7c15ULL;
  value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
  value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
  return value ^ (value >> 31);
}

template <typename Values, typename Index>
bool ContainsAny(const Values& values, const Index& index) {
  return std::any_of(values.begin(), values.end(), [&index](const auto& value) {
    return index.contains(value);
  });
}

}  // namespace

size_t FactIdentityHash::operator()(const FactIdentity& identity) const noexcept {
  const auto [part_id, family, property, entity] = identity;
  const uint64_t family_property =
      (static_cast<uint64_t>(part_id) << 32) |
      (static_cast<uint64_t>(family) << 16) | property;
  return static_cast<size_t>(Mix(entity) ^ Mix(family_property));
}

size_t EdgeIdentityKeyHash::operator()(const EdgeIdentityKey& identity) const noexcept {
  return static_cast<size_t>(Mix(identity.second) ^ Mix(identity.first));
}

CommitFootprint BuildCommitFootprint(const StoreCommitBatch& batch) {
  CommitFootprint footprint;
  footprint.txn_ids.insert(batch.txn_id.value);
  for (const PendingFactMutation& mutation : batch.mutations) {
    footprint.writes.insert(IdentityOf(mutation.ref));
  }
  for (const StrictReadDependency& dependency :
       batch.strict_read_dependencies) {
    footprint.strict_reads.insert(IdentityOf(dependency.ref));
  }
  for (const EdgeIdentity& identity : batch.edge_identities) {
    footprint.edge_ids.emplace(identity.home_part_id.value, identity.edge_id.value);
  }
  return footprint;
}

size_t EstimateCommitBatchBytes(const StoreCommitBatch& batch) {
  // This is deliberately conservative. It accounts for the encoded fact,
  // sequence, transaction, identity, and dependency records plus RocksDB
  // WriteBatch framing. A conservative estimate prevents a queued request
  // from making the hard batch limit fail only after assembly.
  size_t total = 256;
  for (const PendingFactMutation& mutation : batch.mutations) {
    SaturatingAdd(128, &total);
    if (mutation.value.has_value()) {
      SaturatingAdd(ValuePayloadBytes(*mutation.value), &total);
    }
  }
  SaturatingMultiply(batch.mutations.size(), 32, &total);
  SaturatingMultiply(batch.edge_identities.size(), 128, &total);
  SaturatingMultiply(batch.snapshot_write_dependencies.size(), 96, &total);
  for (const StrictReadDependency& dependency : batch.strict_read_dependencies) {
    SaturatingAdd(128, &total);
    if (dependency.observed_event.has_value()) {
      AddFactEventBytes(*dependency.observed_event, &total);
    }
  }
  // One queued request owns its staged batch and, while an epoch is active,
  // RocksDB assembly owns encoded representations and completion state. Keep
  // a conservative two-copy reservation rather than admitting an untracked
  // temporary allocation burst.
  SaturatingMultiply(total, 2, &total);
  SaturatingAdd(512, &total);
  return total;
}

bool CanSharePhysicalWrite(const CommitFootprint& left,
                           const CommitFootprint& right) {
  return Disjoint(left.txn_ids, right.txn_ids) &&
         Disjoint(left.writes, right.writes) &&
         Disjoint(left.writes, right.strict_reads) &&
         Disjoint(left.strict_reads, right.writes) &&
         Disjoint(left.edge_ids, right.edge_ids);
}

bool CommitConflictIndex::CanInsert(const CommitFootprint& candidate) const {
  return !ContainsAny(candidate.txn_ids, txn_ids_) &&
         !ContainsAny(candidate.writes, writes_) &&
         !ContainsAny(candidate.writes, strict_reads_) &&
         !ContainsAny(candidate.strict_reads, writes_) &&
         !ContainsAny(candidate.edge_ids, edge_ids_);
}

bool CommitConflictIndex::Insert(const CommitFootprint& candidate) {
  if (!CanInsert(candidate)) return false;
  txn_ids_.insert(candidate.txn_ids.begin(), candidate.txn_ids.end());
  writes_.insert(candidate.writes.begin(), candidate.writes.end());
  strict_reads_.insert(candidate.strict_reads.begin(),
                       candidate.strict_reads.end());
  edge_ids_.insert(candidate.edge_ids.begin(), candidate.edge_ids.end());
  return true;
}

bool CanUseAppendFastPath(const StoreCommitBatch& batch) {
  if (batch.mutations.empty() || !batch.edge_identities.empty() ||
      !batch.strict_read_dependencies.empty()) {
    return false;
  }
  for (const PendingFactMutation& mutation : batch.mutations) {
    if (mutation.operation != FactOperation::kPut) return false;
  }
  for (const SnapshotWriteDependency& dependency :
       batch.snapshot_write_dependencies) {
    if (dependency.predecessor.has_value() || dependency.successor.has_value()) {
      return false;
    }
  }
  return true;
}

}  // namespace cedar::internal
