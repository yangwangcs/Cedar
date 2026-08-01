// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_FACT_META_CODEC_H_
#define CEDAR_FACT_META_CODEC_H_

#include <cstdint>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/fact/fact.h"
#include "cedar/schema.h"

namespace cedar {

enum class IdKind : uint8_t { kVertex = 1, kEdge = 2, kTransaction = 3 };

struct IdAllocatorState {
  IdKind kind = IdKind::kVertex;
  uint64_t next_id = 0;

  Status Validate() const;
  bool operator==(const IdAllocatorState&) const = default;
};

enum class TransactionOutcome : uint8_t { kCommitted = 1 };

struct TransactionOutcomeRecord {
  TxnId txn_id;
  CommitSeq commit_seq;
  TransactionOutcome outcome = TransactionOutcome::kCommitted;

  Status Validate() const;
  bool operator==(const TransactionOutcomeRecord&) const = default;
};

struct SequenceRecord {
  CommitSeq commit_seq;
  TxnId txn_id;
  uint64_t system_hlc = 0;
  std::vector<std::string> fact_keys;

  Status Validate() const;
  bool operator==(const SequenceRecord&) const = default;
};

std::string EncodeCurrentFormatKey();
StatusOr<std::string> EncodeSchemaMetaKey(PropertyId property_id,
                                          uint32_t schema_epoch);
StatusOr<std::string> EncodeEdgeIdentityMetaKey(EdgeId edge_id);
std::string EncodeAllocatorMetaKey(IdKind kind);
StatusOr<std::string> EncodeTransactionMetaKey(TxnId txn_id);
StatusOr<std::string> EncodeSequenceMetaKey(CommitSeq commit_seq);
std::string EncodeVisibleWatermarkKey();
std::string EncodeOldestReadableWatermarkKey();

StatusOr<std::string> EncodeFormatVersion(uint32_t format_version);
StatusOr<uint32_t> DecodeFormatVersion(const std::string& encoded);
StatusOr<std::string> EncodeWatermark(CommitSeq watermark);
StatusOr<CommitSeq> DecodeWatermark(const std::string& encoded);
StatusOr<std::string> EncodePropertyDefinition(
    const PropertyDefinition& definition);
StatusOr<PropertyDefinition> DecodePropertyDefinition(const std::string& encoded);
StatusOr<std::string> EncodeEdgeIdentity(const EdgeIdentity& identity);
StatusOr<EdgeIdentity> DecodeEdgeIdentity(const std::string& encoded);
StatusOr<std::string> EncodeIdAllocatorState(const IdAllocatorState& state);
StatusOr<IdAllocatorState> DecodeIdAllocatorState(const std::string& encoded);
StatusOr<std::string> EncodeTransactionOutcome(
    const TransactionOutcomeRecord& record);
StatusOr<TransactionOutcomeRecord> DecodeTransactionOutcome(
    const std::string& encoded);
StatusOr<std::string> EncodeSequenceRecord(const SequenceRecord& record);
StatusOr<SequenceRecord> DecodeSequenceRecord(const std::string& encoded);

}  // namespace cedar

#endif  // CEDAR_FACT_META_CODEC_H_
