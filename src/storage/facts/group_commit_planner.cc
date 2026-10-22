// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "storage/facts/group_commit_planner.h"

#include <algorithm>
#include <limits>

#include "cedar/fact/fact_codec.h"

namespace cedar::internal {
namespace {

// Encoded-record layout mirrored from fact_codec.cc / meta_codec.cc and the
// RocksDB WriteBatch record framing used by AppendCandidateToWriteBatch. The
// estimator must track the encoder instead of carrying a fixed 2x reservation:
// the group byte cap is enforced again on the assembled batch, so a tight
// upper bound is safe and stops cutting forecast groups far below the cap.
constexpr size_t kWriteBatchHeaderBytes = 12;
constexpr size_t kFactValueHeaderBytes = 11;
constexpr size_t kFactValueChecksumBytes = 4;
constexpr size_t kMetaRecordHeaderBytes = 6;
constexpr size_t kMetaRecordChecksumBytes = 4;
constexpr size_t kEdgeIdentityPayloadBytes = 44;
constexpr size_t kTransactionOutcomePayloadBytes = 17;
constexpr size_t kSequenceFixedPayloadBytes = 28;
// Mirrors meta_codec.cc's v2a sequence payload layout: payload_version(1) +
// fact_key_version(1) + txn_id(8) + system_hlc(8) + count(4), then one 23-byte
// [part(4), family(1), property(2), entity(8), ~valid_from(8)] descriptor per
// fact instead of the v1 32-byte duplicated canonical key.
constexpr size_t kSequenceV2FixedPayloadBytes = 1 + 1 + 8 + 8 + 4;  // 22
constexpr size_t kSequenceV2DescriptorBytes = 4 + 1 + 2 + 8 + 8;    // 23
constexpr size_t kVisibleWatermarkPayloadBytes = 8;
constexpr size_t kTransactionMetaKeyBytes = 4 + 8;         // "txn/"
constexpr size_t kSequenceMetaKeyBytes = 9 + 8;            // "sequence/"
constexpr size_t kEdgeIdentityMetaKeyBytes = 5 + 4 + 8;    // "edge/"
constexpr size_t kVisibleWatermarkKeyBytes = 10 + 7;       // "watermark/visible"

void SaturatingAdd(size_t amount, size_t* total) {
  if (amount > std::numeric_limits<size_t>::max() - *total) {
    *total = std::numeric_limits<size_t>::max();
  } else {
    *total += amount;
  }
}

void SaturatingAddProduct(size_t amount, size_t count, size_t* total) {
  if (amount != 0 && count > std::numeric_limits<size_t>::max() / amount) {
    *total = std::numeric_limits<size_t>::max();
    return;
  }
  SaturatingAdd(amount * count, total);
}

size_t Varint32Bytes(size_t value) {
  if (value < (size_t{1} << 7)) return 1;
  if (value < (size_t{1} << 14)) return 2;
  if (value < (size_t{1} << 21)) return 3;
  if (value < (size_t{1} << 28)) return 4;
  return 5;
}

// One WriteBatch put record: value tag, column-family varint, key length, key,
// value length and value. Facts and meta both live in column families < 128.
size_t WriteBatchPutBytes(size_t key_bytes, size_t value_bytes) {
  size_t total = 2;
  SaturatingAdd(Varint32Bytes(key_bytes), &total);
  SaturatingAdd(key_bytes, &total);
  SaturatingAdd(Varint32Bytes(value_bytes), &total);
  SaturatingAdd(value_bytes, &total);
  return total;
}

size_t EncodedFactValueBytes(size_t payload_bytes) {
  return kFactValueHeaderBytes + payload_bytes + kFactValueChecksumBytes;
}

size_t EncodedMetaRecordValueBytes(size_t payload_bytes) {
  return kMetaRecordHeaderBytes + payload_bytes + kMetaRecordChecksumBytes;
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

size_t EstimateSequenceRecordBytes(uint32_t sequence_format,
                                   size_t fact_key_count) {
  size_t value_bytes = kMetaRecordHeaderBytes;
  if (sequence_format == kMetaSequenceFormatV2) {
    SaturatingAdd(kSequenceV2FixedPayloadBytes, &value_bytes);
    SaturatingAddProduct(kSequenceV2DescriptorBytes, fact_key_count,
                         &value_bytes);
  } else {
    // v1 is the default and remains byte-identical to previous releases,
    // including the per-fact 32-byte canonical key reservation.
    SaturatingAdd(kSequenceFixedPayloadBytes, &value_bytes);
    SaturatingAddProduct(kEncodedFactKeyBytes, fact_key_count, &value_bytes);
  }
  SaturatingAdd(kMetaRecordChecksumBytes, &value_bytes);
  return value_bytes;
}

size_t EstimateCommitBatchBytes(const StoreCommitBatch& batch,
                                uint32_t sequence_format) {
  // Per committed batch (one transaction) the writer emits: the transaction
  // outcome record, the sequence record (with one duplicated canonical key per
  // fact and per edge identity), the visible watermark, one fact put per
  // mutation, and one authoritative fact plus routing record per edge identity.
  // The visible watermark is emitted once per group, so counting it per batch is
  // a small conservative overcount that costs a single record.
  const size_t fact_key_count =
      batch.mutations.size() + batch.edge_identities.size();
  size_t total = kWriteBatchHeaderBytes;
  SaturatingAdd(
      WriteBatchPutBytes(kTransactionMetaKeyBytes,
                         EncodedMetaRecordValueBytes(
                             kTransactionOutcomePayloadBytes)),
      &total);
  const size_t sequence_value_bytes =
      EstimateSequenceRecordBytes(sequence_format, fact_key_count);
  SaturatingAdd(
      WriteBatchPutBytes(kSequenceMetaKeyBytes, sequence_value_bytes), &total);
  SaturatingAdd(WriteBatchPutBytes(kVisibleWatermarkKeyBytes,
                                   EncodedMetaRecordValueBytes(
                                       kVisibleWatermarkPayloadBytes)),
                &total);
  for (const PendingFactMutation& mutation : batch.mutations) {
    const size_t payload_bytes =
        mutation.value.has_value() ? ValuePayloadBytes(*mutation.value) : 0;
    SaturatingAdd(
        WriteBatchPutBytes(kEncodedFactKeyBytes,
                           EncodedFactValueBytes(payload_bytes)),
        &total);
  }
  for ([[maybe_unused]] const EdgeIdentity& identity : batch.edge_identities) {
    // Authoritative identity fact (payload is the fixed 44-byte identity),
    // its duplicated sequence key (accounted for by fact_key_count above), and
    // the routing metadata record.
    SaturatingAdd(
        WriteBatchPutBytes(kEncodedFactKeyBytes,
                           EncodedFactValueBytes(kEdgeIdentityPayloadBytes)),
        &total);
    SaturatingAdd(
        WriteBatchPutBytes(kEdgeIdentityMetaKeyBytes,
                           EncodedMetaRecordValueBytes(
                               kEdgeIdentityPayloadBytes)),
        &total);
  }
  // Snapshot write dependencies are validated in memory and are not encoded.
  // Strict-read observed events are also not encoded, but the queued request
  // retains their payload for validation, so reserve that memory here (this is
  // a queue-admission bound, not a WAL-byte count).
  for (const StrictReadDependency& dependency : batch.strict_read_dependencies) {
    SaturatingAdd(128, &total);
    if (dependency.observed_event.has_value()) {
      AddFactEventBytes(*dependency.observed_event, &total);
    }
  }
  // Small absolute + 1/256 safety margin keeps the estimate above the real
  // encoder for every committed record without the former 2x reservation.
  size_t margin = total / 256;
  SaturatingAdd(256, &margin);
  SaturatingAdd(margin, &total);
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
    if (mutation.operation != FactOperation::kPut ||
        mutation.ref.family() == FactFamily::kEdgeState) {
      return false;
    }
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
