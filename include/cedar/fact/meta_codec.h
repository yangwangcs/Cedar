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
// EncodeSequenceFormatKey). 1 keeps writing v1 `kSequence` records; 2 enables
// the compact v2a `kSequenceV2` writer. The marker is a one-way upgrade: an
// open that observes 2 keeps writing v2 even when the caller asks for 1. Any
// other marker value is rejected at open as Corruption.
inline constexpr uint32_t kMetaSequenceFormatV1 = 1;
inline constexpr uint32_t kMetaSequenceFormatV2 = 2;
// Default writer format (F19). Normal builds write v2 and durably record the
// marker on the first open, which is a one-way upgrade for older binaries; a
// caller opts out with meta_sequence_format = kMetaSequenceFormatV1 before the
// database is upgraded. The v1-only compatibility build (F17) simulates a
// pre-v2 binary and therefore keeps the historical v1 default.
#if defined(CEDAR_META_SEQUENCE_V1_ONLY)
inline constexpr uint32_t kMetaSequenceFormatDefault = kMetaSequenceFormatV1;
#else
inline constexpr uint32_t kMetaSequenceFormatDefault = kMetaSequenceFormatV2;
#endif

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

// Zero-copy view of one committed sequence record. For v1 records (`kSequence`)
// `identities` borrows the caller's encoded record bytes; each entry is
// kEncodedFactKeyBytes long and the view is valid only while those bytes stay
// alive. v2 records (`kSequenceV2`) store a 23-byte descriptor per fact instead
// of the packed key, so the key-aware view decoder rebuilds the keys once into
// `owned_identities` and `identities` points into that shared buffer; the view
// keeps the buffer alive through copies and moves.
struct SequenceRecordView {
  CommitSeq commit_seq;
  TxnId txn_id;
  uint64_t system_hlc = 0;
  uint32_t fact_count = 0;
  std::string_view identities;
  // Non-null only for v2 records (whose identities are rebuilt, not borrowed).
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
StatusOr<std::string> EncodeSequenceRecord(const SequenceRecord& record);
// Compact v2a sequence record (`kSequenceV2`). The payload is
//   payload_version(1) + fact_key_version(1) + txn_id(8) + system_hlc(8) +
//   count(4) + N x [part(4) + family(1) + property(2) + entity(8) +
//   ~valid_from(8)]   // 23 bytes per fact, big-endian
// commit_seq is not in the payload: it is the sequence/<seq> record key, and the
// reader rebuilds the 32-byte fact keys from it with EncodeFactKey. The v1
// writer keeps writing `kSequence`; this encoder exists for the opt-in writer
// and the migration tool.
StatusOr<std::string> EncodeSequenceRecordV2(const SequenceRecord& record);
// Decodes a v1 `kSequence` record. Returns Corruption for a v2 `kSequenceV2`
// record: the v2 payload does not carry commit_seq, so that decoder cannot
// rebuild the fact keys without the record key. Use the key-aware overload.
StatusOr<SequenceRecord> DecodeSequenceRecord(const std::string& encoded);
// Decodes either record kind. v1 records carry commit_seq themselves (the
// argument is unused); v2 records rebuild every fact key with the supplied
// commit_seq, so callers must pass the sequence number the record was stored
// under (`sequence/<seq>`). A zero commit_seq for a v2 record is rejected with
// Corruption.
StatusOr<SequenceRecord> DecodeSequenceRecord(const std::string& encoded,
                                              CommitSeq commit_seq);
// Decodes only the record framing and the packed identity region; fact-key and
// duplicate checks are Validate()'s job so callers that only need txn_id/count
// do not pay for them twice. This overload is v1-only (zero-copy) and returns
// Corruption for a v2 record for the same reason as DecodeSequenceRecord.
StatusOr<SequenceRecordView> DecodeSequenceRecordView(
    std::string_view encoded);
// Dual-kind view decoder. For a v1 record this is the same zero-copy decode as
// the overload above; for a v2 record it rebuilds the fact keys into
// `owned_identities` (23 bytes per fact in the payload) and points `identities`
// at that buffer. `commit_seq` is required for v2 records exactly like the key-
// aware DecodeSequenceRecord overload.
StatusOr<SequenceRecordView> DecodeSequenceRecordView(
    std::string_view encoded, CommitSeq commit_seq);
StatusOr<std::string> EncodeVacuumState(const VacuumState& state);
StatusOr<VacuumState> DecodeVacuumState(const std::string& encoded);

}  // namespace cedar

#endif  // CEDAR_FACT_META_CODEC_H_
