// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_FACT_META_CODEC_H_
#define CEDAR_FACT_META_CODEC_H_

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/fact/fact.h"
#include "cedar/schema.h"

namespace cedar {

enum class IdKind : uint8_t { kVertex = 1, kEdge = 2, kTransaction = 3 };

// Values of the durable meta sequence-format marker (see
// EncodeSequenceFormatKey). 2 is the compact v2a `kSequenceV2` format and the
// only format this release writes or reads. 1 is the retired v1 `kSequence`
// format: it is recognized only so a legacy database can be rejected as
// unsupported; it is never decoded, and there is no migration or downgrade.
inline constexpr uint32_t kMetaSequenceFormatV1 = 1;
inline constexpr uint32_t kMetaSequenceFormatV2 = 2;
// The only supported writer format. Kept as a named constant so the public
// option default and the v2-only validation share one spelling.
inline constexpr uint32_t kMetaSequenceFormatDefault = kMetaSequenceFormatV2;

// The error for a database that still carries the retired v1 meta sequence
// format: a missing or 1-valued durable format marker, or a legacy `kSequence`
// (on-disk kind 7) record. `detail` names the artifact that was found. The
// message states that v1 is unsupported and that no migration or downgrade
// exists; the database must be deleted and recreated, or restored from a
// format-2 backup. This release never decodes v1 sequence records.
Status LegacyMetaSequenceFormatError(std::string_view detail);

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

// Zero-copy view of one committed sequence record. The only supported record
// is v2 (`kSequenceV2`): it stores a 23-byte descriptor per fact instead of the
// packed key, so the view decoder rebuilds the 32-byte keys once into
// `owned_identities` and `identities` points into that shared buffer; the view
// keeps the buffer alive through copies and moves.
struct SequenceRecordView {
  CommitSeq commit_seq;
  TxnId txn_id;
  uint64_t system_hlc = 0;
  uint32_t fact_count = 0;
  std::string_view identities;
  // Rebuilt fact keys owned by the view (non-null for every record this
  // release can decode). `identities` points at this buffer.
  std::shared_ptr<const std::string> owned_identities;

  Status Validate() const;
};

enum class VacuumPhase : uint8_t { kPrepared = 1, kRunning = 2 };

struct VacuumState {
  CommitSeq target;
  VacuumPhase phase = VacuumPhase::kPrepared;
  std::string cursor;

  Status Validate() const;
  bool operator==(const VacuumState&) const = default;
};

struct SystemIdentity {
  std::string system;
  uint32_t system_format = 0;
  std::string fact_key_format;
  std::string facts_table_format;
  std::string comparator_digest;

  Status Validate() const;
  bool operator==(const SystemIdentity&) const = default;
};

std::string EncodeCurrentFormatKey();
StatusOr<std::string> EncodeSchemaMetaKey(PropertyId property_id,
                                          uint32_t schema_epoch);
StatusOr<std::string> EncodeEdgeIdentityMetaKey(EdgeRef edge);
std::string EncodeAllocatorMetaKey(IdKind kind);
StatusOr<std::string> EncodeTransactionMetaKey(TxnId txn_id);
StatusOr<std::string> EncodeSequenceMetaKey(CommitSeq commit_seq);
std::string EncodeVisibleWatermarkKey();
std::string EncodeOldestReadableWatermarkKey();
// Durable sequence-retention boundary ("watermark/sequence-retention"). Value 0
// (or an absent record) means no `sequence/*` record has ever been pruned, so
// history from sequence 1 must still be contiguous. A value W >= 1 means
// records below W may be absent (they were pruned after Vacuum advanced the
// oldest-readable boundary to at least W); every record from W through the
// visible watermark must still exist. The record is written with Sync=true
// before the first pruning delete, which is what makes a mid-prune crash safe:
// reopen either sees the old contiguous history or a watermark that excuses the
// missing prefix.
std::string EncodeSequenceRetentionWatermarkKey();
std::string EncodeVacuumStateKey();
// Durable meta sequence-format marker key ("format/sequence-record-format").
// The marker is an ordinary framed meta record whose 4-byte big-endian payload
// is a kMetaSequenceFormat* value. It is written with Sync=true by an open that
// opts into format 2, before that store accepts any commit, so a crash can
// never leave a v2 record behind a database that would reopen as v1.
std::string EncodeSequenceFormatKey();
// Encodes the marker payload (the raw format value; open rejects unsupported
// values) with the standard meta framing and CRC32C.
StatusOr<std::string> EncodeSequenceFormat(uint32_t format);
StatusOr<uint32_t> DecodeSequenceFormat(const std::string& encoded);

StatusOr<std::string> EncodeSystemIdentity(const SystemIdentity& identity);
StatusOr<SystemIdentity> DecodeSystemIdentity(const std::string& encoded);
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
// Compact v2a sequence record (`kSequenceV2`, on-disk kind 9), the only
// sequence-record encoding this release writes. The payload is
//   payload_version(1) + fact_key_version(1) + txn_id(8) + system_hlc(8) +
//   count(4) + N x [part(4) + family(1) + property(2) + entity(8) +
//   ~valid_from(8)]   // 23 bytes per fact, big-endian
// commit_seq is not in the payload: it is the sequence/<seq> record key, and the
// reader rebuilds the 32-byte fact keys from it with EncodeFactKey.
StatusOr<std::string> EncodeSequenceRecordV2(const SequenceRecord& record);
// Decodes a v2 `kSequenceV2` record. `commit_seq` is the sequence number the
// record was stored under (`sequence/<seq>`); the payload omits it, so callers
// must pass it or the rebuilt fact keys would be wrong. A zero commit_seq is
// rejected with Corruption. A legacy `kSequence` (kind 7) record is rejected
// with LegacyMetaSequenceFormatError (v1 is unsupported; no migration or
// downgrade) and is never decoded; any other record kind is Corruption.
StatusOr<SequenceRecord> DecodeSequenceRecord(const std::string& encoded,
                                              CommitSeq commit_seq);
// View decoder with the same contract as DecodeSequenceRecord: only a v2 record
// is accepted, and `commit_seq` is required because the fact keys are rebuilt
// from the record key. The rebuilt keys are owned by the returned view.
StatusOr<SequenceRecordView> DecodeSequenceRecordView(
    std::string_view encoded, CommitSeq commit_seq);
StatusOr<std::string> EncodeVacuumState(const VacuumState& state);
StatusOr<VacuumState> DecodeVacuumState(const std::string& encoded);

}  // namespace cedar

#endif  // CEDAR_FACT_META_CODEC_H_
