// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "storage/facts/prepared_commit_codec.h"

#include <limits>

#include "cedar/core/crc32c.h"
#include "cedar/fact/fact_codec.h"

namespace cedar::internal {
namespace {

constexpr uint8_t kRecordVersion = 1;
constexpr uint8_t kPreparedRecord = 1;
constexpr uint8_t kAbortedRecord = 2;
constexpr uint8_t kPreparedDecisionRecord = 3;
constexpr size_t kHeaderBytes = 6;
constexpr size_t kChecksumBytes = 4;
constexpr size_t kMaxPayloadBytes = 16U * 1024U * 1024U;

void AppendU16(std::string* output, uint16_t value) {
  output->push_back(static_cast<char>(value >> 8));
  output->push_back(static_cast<char>(value));
}

void AppendU32(std::string* output, uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    output->push_back(static_cast<char>(value >> shift));
  }
}

void AppendU64(std::string* output, uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    output->push_back(static_cast<char>(value >> shift));
  }
}

bool ReadU16(const std::string& input, size_t* offset, uint16_t* value) {
  if (input.size() - *offset < 2) return false;
  *value = (static_cast<uint16_t>(static_cast<uint8_t>(input[*offset])) << 8) |
           static_cast<uint8_t>(input[*offset + 1]);
  *offset += 2;
  return true;
}

bool ReadU32(const std::string& input, size_t* offset, uint32_t* value) {
  if (input.size() - *offset < 4) return false;
  *value = 0;
  for (int index = 0; index < 4; ++index) {
    *value = (*value << 8) | static_cast<uint8_t>(input[*offset + index]);
  }
  *offset += 4;
  return true;
}

bool ReadU64(const std::string& input, size_t* offset, uint64_t* value) {
  if (input.size() - *offset < 8) return false;
  *value = 0;
  for (int index = 0; index < 8; ++index) {
    *value = (*value << 8) | static_cast<uint8_t>(input[*offset + index]);
  }
  *offset += 8;
  return true;
}

StatusOr<std::string> EncodeRecord(uint8_t kind, const std::string& payload) {
  if (payload.size() > kMaxPayloadBytes) {
    return Status::InvalidArgument("async prepare", "payload exceeds size limit");
  }
  std::string encoded;
  encoded.reserve(kHeaderBytes + payload.size() + kChecksumBytes);
  encoded.push_back(static_cast<char>(kRecordVersion));
  encoded.push_back(static_cast<char>(kind));
  AppendU32(&encoded, static_cast<uint32_t>(payload.size()));
  encoded.append(payload);
  AppendU32(&encoded, crc32c::Value(encoded.data(), encoded.size()));
  return encoded;
}

StatusOr<std::string> DecodeRecord(uint8_t expected_kind,
                                   const std::string& encoded) {
  if (encoded.size() < kHeaderBytes + kChecksumBytes) {
    return Status::Corruption("async prepare", "truncated record");
  }
  size_t offset = 0;
  const uint8_t version = static_cast<uint8_t>(encoded[offset++]);
  const uint8_t kind = static_cast<uint8_t>(encoded[offset++]);
  uint32_t payload_size = 0;
  if (version != kRecordVersion || kind != expected_kind ||
      !ReadU32(encoded, &offset, &payload_size) ||
      payload_size > kMaxPayloadBytes ||
      encoded.size() - offset != payload_size + kChecksumBytes) {
    return Status::Corruption("async prepare", "invalid record header");
  }
  const size_t checksum_offset = encoded.size() - kChecksumBytes;
  size_t checksum_cursor = checksum_offset;
  uint32_t checksum = 0;
  if (!ReadU32(encoded, &checksum_cursor, &checksum) ||
      checksum != crc32c::Value(encoded.data(), checksum_offset)) {
    return Status::Corruption("async prepare", "CRC32C mismatch");
  }
  return encoded.substr(offset, payload_size);
}

Status AppendMutation(std::string* payload, const PendingFactMutation& mutation) {
  const std::string key = EncodeFactKey(mutation.ref, mutation.valid_from, CommitSeq{1});
  if (key.empty()) return Status::InvalidArgument("async prepare", "invalid mutation");
  const FactEvent event{mutation.ref, mutation.valid_from, CommitSeq{1},
                        mutation.operation, mutation.schema_epoch, mutation.value};
  const auto value = EncodeFactValue(event);
  if (!value.ok()) return value.status();
  payload->append(key);
  AppendU32(payload, static_cast<uint32_t>(value.ValueOrDie().size()));
  payload->append(value.ValueOrDie());
  return Status::OK();
}

StatusOr<PendingFactMutation> ReadMutation(const std::string& payload,
                                           size_t* offset) {
  if (payload.size() - *offset < kEncodedFactKeyBytes) {
    return Status::Corruption("async prepare", "truncated mutation key");
  }
  const auto key = DecodeFactKey(payload.substr(*offset, kEncodedFactKeyBytes));
  if (!key.ok()) return key.status();
  *offset += kEncodedFactKeyBytes;
  uint32_t value_size = 0;
  if (!ReadU32(payload, offset, &value_size) ||
      value_size > payload.size() - *offset) {
    return Status::Corruption("async prepare", "invalid mutation value length");
  }
  const auto event = DecodeFactValue(key.ValueOrDie().ref,
                                     key.ValueOrDie().valid_from, CommitSeq{1},
                                     payload.substr(*offset, value_size));
  if (!event.ok()) return event.status();
  *offset += value_size;
  return PendingFactMutation{event.ValueOrDie().ref, event.ValueOrDie().valid_from,
                             event.ValueOrDie().operation,
                             event.ValueOrDie().schema_epoch,
                             event.ValueOrDie().value};
}

void AppendOptionalTime(std::string* payload, const std::optional<ValidTime>& value) {
  payload->push_back(value.has_value() ? 1 : 0);
  if (value.has_value()) AppendU64(payload, value->value);
}

bool ReadOptionalTime(const std::string& payload, size_t* offset,
                      std::optional<ValidTime>* value) {
  if (*offset == payload.size()) return false;
  const uint8_t present = static_cast<uint8_t>(payload[(*offset)++]);
  if (present > 1) return false;
  if (present == 0) {
    *value = std::nullopt;
    return true;
  }
  uint64_t time = 0;
  if (!ReadU64(payload, offset, &time)) return false;
  *value = ValidTime{time};
  return true;
}

void AppendRef(std::string* payload, const FactRef& ref) {
  AppendU32(payload, ref.part_id().value);
  payload->push_back(static_cast<char>(ref.family()));
  AppendU16(payload, ref.property_id().value);
  AppendU64(payload, ref.entity_id());
}

bool ReadRef(const std::string& payload, size_t* offset, FactRef* ref) {
  if (payload.size() - *offset < 15) return false;
  uint32_t part_id = 0;
  if (!ReadU32(payload, offset, &part_id)) return false;
  const uint8_t family = static_cast<uint8_t>(payload[(*offset)++]);
  uint16_t property_id = 0;
  uint64_t entity_id = 0;
  if (!ReadU16(payload, offset, &property_id) || !ReadU64(payload, offset, &entity_id)) {
    return false;
  }
  *ref = FactRef(PartId{part_id}, static_cast<FactFamily>(family),
                 PropertyId{property_id}, entity_id);
  return ref->Validate().ok();
}

}  // namespace

std::string PreparedCommitPrefix() { return "async/prepare/"; }

StatusOr<std::string> EncodePreparedCommitKey(TxnId txn_id) {
  if (!txn_id.valid()) return Status::InvalidArgument("async prepare", "zero transaction ID");
  std::string key = PreparedCommitPrefix();
  AppendU64(&key, txn_id.value);
  return key;
}

StatusOr<std::string> EncodePreparedCommit(const StoreCommitBatch& batch) {
  const Status valid = batch.Validate();
  if (!valid.ok()) return valid;
  std::string payload;
  AppendU64(&payload, batch.txn_id.value);
  AppendU64(&payload, batch.system_hlc);
  AppendU32(&payload, static_cast<uint32_t>(batch.mutations.size()));
  for (const PendingFactMutation& mutation : batch.mutations) {
    const Status appended = AppendMutation(&payload, mutation);
    if (!appended.ok()) return appended;
  }
  AppendU32(&payload, static_cast<uint32_t>(batch.edge_identities.size()));
  for (const EdgeIdentity& identity : batch.edge_identities) {
    AppendU32(&payload, identity.home_part_id.value);
    AppendU64(&payload, identity.edge_id.value);
    AppendU32(&payload, identity.source_part_id.value);
    AppendU64(&payload, identity.source_vertex_id.value);
    AppendU32(&payload, identity.target_part_id.value);
    AppendU64(&payload, identity.target_vertex_id.value);
    AppendU64(&payload, identity.edge_type);
  }
  AppendU32(&payload, static_cast<uint32_t>(batch.snapshot_write_dependencies.size()));
  for (const SnapshotWriteDependency& dependency : batch.snapshot_write_dependencies) {
    AppendRef(&payload, dependency.ref);
    AppendU64(&payload, dependency.valid_from.value);
    AppendOptionalTime(&payload, dependency.predecessor);
    AppendOptionalTime(&payload, dependency.successor);
    AppendU64(&payload, dependency.snapshot_seq.value);
  }
  AppendU32(&payload, static_cast<uint32_t>(batch.strict_read_dependencies.size()));
  for (const StrictReadDependency& dependency : batch.strict_read_dependencies) {
    AppendRef(&payload, dependency.ref);
    AppendU64(&payload, dependency.valid_time.value);
    AppendU64(&payload, dependency.snapshot_seq.value);
    payload.push_back(dependency.observed_event.has_value() ? 1 : 0);
    if (dependency.observed_event.has_value()) {
      const FactEvent& event = *dependency.observed_event;
      const std::string key = EncodeFactKey(event.ref, event.valid_from, event.commit_seq);
      const auto value = EncodeFactValue(event);
      if (key.empty() || !value.ok()) return Status::InvalidArgument("async prepare", "invalid observed event");
      payload.append(key);
      AppendU32(&payload, static_cast<uint32_t>(value.ValueOrDie().size()));
      payload.append(value.ValueOrDie());
    }
    AppendOptionalTime(&payload, dependency.predecessor);
    AppendOptionalTime(&payload, dependency.successor);
  }
  return EncodeRecord(kPreparedRecord, payload);
}

StatusOr<StoreCommitBatch> DecodePreparedCommit(const std::string& encoded) {
  const auto decoded = DecodeRecord(kPreparedRecord, encoded);
  if (!decoded.ok()) return decoded.status();
  const std::string& payload = decoded.ValueOrDie();
  size_t offset = 0;
  uint64_t txn_id = 0;
  uint64_t system_hlc = 0;
  uint32_t count = 0;
  if (!ReadU64(payload, &offset, &txn_id) || !ReadU64(payload, &offset, &system_hlc) ||
      !ReadU32(payload, &offset, &count)) {
    return Status::Corruption("async prepare", "truncated batch header");
  }
  StoreCommitBatch batch{TxnId{txn_id}, system_hlc, {}, {}, {}, {}};
  batch.mutations.reserve(count);
  for (uint32_t index = 0; index < count; ++index) {
    const auto mutation = ReadMutation(payload, &offset);
    if (!mutation.ok()) return mutation.status();
    batch.mutations.push_back(mutation.ValueOrDie());
  }
  if (!ReadU32(payload, &offset, &count)) return Status::Corruption("async prepare", "truncated edge count");
  batch.edge_identities.reserve(count);
  for (uint32_t index = 0; index < count; ++index) {
    uint32_t home_part = 0, source_part = 0, target_part = 0;
    uint64_t edge_id = 0, source = 0, target = 0, edge_type = 0;
    if (!ReadU32(payload, &offset, &home_part) || !ReadU64(payload, &offset, &edge_id) ||
        !ReadU32(payload, &offset, &source_part) || !ReadU64(payload, &offset, &source) ||
        !ReadU32(payload, &offset, &target_part) || !ReadU64(payload, &offset, &target) ||
        !ReadU64(payload, &offset, &edge_type)) {
      return Status::Corruption("async prepare", "truncated edge identity");
    }
    batch.edge_identities.push_back(EdgeIdentity{
        EdgeRef{PartId{home_part}, EdgeId{edge_id}},
        VertexRef{PartId{source_part}, VertexId{source}},
        VertexRef{PartId{target_part}, VertexId{target}}, edge_type});
  }
  if (!ReadU32(payload, &offset, &count)) return Status::Corruption("async prepare", "truncated snapshot dependency count");
  batch.snapshot_write_dependencies.reserve(count);
  for (uint32_t index = 0; index < count; ++index) {
    FactRef ref(PartId{0}, FactFamily::kVertexState, PropertyId{}, 1);
    uint64_t valid_from = 0, snapshot_seq = 0;
    std::optional<ValidTime> predecessor, successor;
    if (!ReadRef(payload, &offset, &ref) || !ReadU64(payload, &offset, &valid_from) ||
        !ReadOptionalTime(payload, &offset, &predecessor) ||
        !ReadOptionalTime(payload, &offset, &successor) || !ReadU64(payload, &offset, &snapshot_seq)) {
      return Status::Corruption("async prepare", "invalid snapshot dependency");
    }
    batch.snapshot_write_dependencies.push_back(
        SnapshotWriteDependency{ref, ValidTime{valid_from}, predecessor, successor, CommitSeq{snapshot_seq}});
  }
  if (!ReadU32(payload, &offset, &count)) return Status::Corruption("async prepare", "truncated strict dependency count");
  batch.strict_read_dependencies.reserve(count);
  for (uint32_t index = 0; index < count; ++index) {
    FactRef ref(PartId{0}, FactFamily::kVertexState, PropertyId{}, 1);
    uint64_t valid_time = 0, snapshot_seq = 0;
    if (!ReadRef(payload, &offset, &ref) || !ReadU64(payload, &offset, &valid_time) ||
        !ReadU64(payload, &offset, &snapshot_seq) || offset == payload.size()) {
      return Status::Corruption("async prepare", "invalid strict dependency");
    }
    const uint8_t observed = static_cast<uint8_t>(payload[offset++]);
    std::optional<FactEvent> observed_event;
    if (observed > 1) return Status::Corruption("async prepare", "invalid observed marker");
    if (observed == 1) {
      if (payload.size() - offset < kEncodedFactKeyBytes) return Status::Corruption("async prepare", "truncated observed key");
      const auto key = DecodeFactKey(payload.substr(offset, kEncodedFactKeyBytes));
      if (!key.ok()) return key.status();
      offset += kEncodedFactKeyBytes;
      uint32_t value_size = 0;
      if (!ReadU32(payload, &offset, &value_size) || value_size > payload.size() - offset) {
        return Status::Corruption("async prepare", "invalid observed value");
      }
      const auto event = DecodeFactValue(key.ValueOrDie().ref, key.ValueOrDie().valid_from,
                                         key.ValueOrDie().commit_seq,
                                         payload.substr(offset, value_size));
      if (!event.ok()) return event.status();
      offset += value_size;
      observed_event = event.ValueOrDie();
    }
    std::optional<ValidTime> predecessor, successor;
    if (!ReadOptionalTime(payload, &offset, &predecessor) ||
        !ReadOptionalTime(payload, &offset, &successor)) {
      return Status::Corruption("async prepare", "invalid strict fences");
    }
    batch.strict_read_dependencies.push_back(
        StrictReadDependency{ref, ValidTime{valid_time}, CommitSeq{snapshot_seq},
                             observed_event, predecessor, successor});
  }
  if (offset != payload.size()) return Status::Corruption("async prepare", "trailing bytes");
  const Status valid = batch.Validate();
  if (!valid.ok()) return Status::Corruption("async prepare", valid.ToString());
  return batch;
}

std::string AsyncTerminalPrefix() { return "async/terminal/"; }

StatusOr<std::string> EncodeAsyncTerminalKey(TxnId txn_id) {
  if (!txn_id.valid()) return Status::InvalidArgument("async terminal", "zero transaction ID");
  std::string key = AsyncTerminalPrefix();
  AppendU64(&key, txn_id.value);
  return key;
}

StatusOr<std::string> EncodeAsyncAbortTerminal(TxnId txn_id) {
  if (!txn_id.valid()) return Status::InvalidArgument("async terminal", "zero transaction ID");
  std::string payload;
  AppendU64(&payload, txn_id.value);
  return EncodeRecord(kAbortedRecord, payload);
}

StatusOr<TxnId> DecodeAsyncAbortTerminal(const std::string& encoded) {
  const auto payload = DecodeRecord(kAbortedRecord, encoded);
  if (!payload.ok()) return payload.status();
  size_t offset = 0;
  uint64_t txn_id = 0;
  if (!ReadU64(payload.ValueOrDie(), &offset, &txn_id) ||
      offset != payload.ValueOrDie().size() || !TxnId{txn_id}.valid()) {
    return Status::Corruption("async terminal", "invalid aborted record");
  }
  return TxnId{txn_id};
}

StatusOr<std::string> EncodePreparedDecisionKey(TxnId txn_id) {
  if (!txn_id.valid()) {
    return Status::InvalidArgument("prepared decision", "zero transaction ID");
  }
  std::string key = "external/decision/";
  AppendU64(&key, txn_id.value);
  return key;
}

StatusOr<std::string> EncodePreparedDecision(
    const StorePreparedDecision& decision) {
  if (!decision.txn_id.valid() || decision.certificate.empty() ||
      decision.certificate.size() > kMaxPayloadBytes - 13 ||
      (decision.outcome != StorePreparedDecisionOutcome::kCommit &&
       decision.outcome != StorePreparedDecisionOutcome::kAbort)) {
    return Status::InvalidArgument("prepared decision", "invalid decision");
  }
  std::string payload;
  AppendU64(&payload, decision.txn_id.value);
  payload.push_back(static_cast<char>(decision.outcome));
  AppendU32(&payload, static_cast<uint32_t>(decision.certificate.size()));
  payload.append(decision.certificate);
  return EncodeRecord(kPreparedDecisionRecord, payload);
}

StatusOr<StorePreparedDecision> DecodePreparedDecision(
    const std::string& encoded) {
  const auto decoded = DecodeRecord(kPreparedDecisionRecord, encoded);
  if (!decoded.ok()) return decoded.status();
  const std::string& payload = decoded.ValueOrDie();
  size_t offset = 0;
  uint64_t txn_id = 0;
  uint32_t certificate_size = 0;
  if (!ReadU64(payload, &offset, &txn_id) || offset == payload.size()) {
    return Status::Corruption("prepared decision", "truncated decision");
  }
  const auto outcome = static_cast<StorePreparedDecisionOutcome>(
      static_cast<uint8_t>(payload[offset++]));
  if (!ReadU32(payload, &offset, &certificate_size) || certificate_size == 0 ||
      certificate_size != payload.size() - offset || !TxnId{txn_id}.valid() ||
      (outcome != StorePreparedDecisionOutcome::kCommit &&
       outcome != StorePreparedDecisionOutcome::kAbort)) {
    return Status::Corruption("prepared decision", "invalid decision");
  }
  return StorePreparedDecision{TxnId{txn_id}, outcome,
                               payload.substr(offset, certificate_size)};
}

}  // namespace cedar::internal
