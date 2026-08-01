// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/fact/meta_codec.h"

#include <limits>

#include "cedar/core/crc32c.h"
#include "cedar/fact/fact_codec.h"

namespace cedar {
namespace {

constexpr uint8_t kMetaVersion = 1;
constexpr size_t kMetaHeaderBytes = 6;
constexpr size_t kMetaChecksumBytes = 4;
constexpr size_t kMaxMetaPayloadBytes = 16U * 1024U * 1024U;
constexpr size_t kMaxPropertyNameBytes = 4096;

enum class MetaRecordKind : uint8_t {
  kFormatVersion = 1,
  kWatermark = 2,
  kPropertyDefinition = 3,
  kEdgeIdentity = 4,
  kIdAllocator = 5,
  kTransactionOutcome = 6,
  kSequence = 7,
};

void AppendU16(std::string* out, uint16_t value) {
  out->push_back(static_cast<char>(value >> 8));
  out->push_back(static_cast<char>(value));
}

void AppendU32(std::string* out, uint32_t value) {
  for (int shift = 24; shift >= 0; shift -= 8) {
    out->push_back(static_cast<char>(value >> shift));
  }
}

void AppendU64(std::string* out, uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    out->push_back(static_cast<char>(value >> shift));
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

bool IsPhysicalType(uint8_t value) {
  return value >= static_cast<uint8_t>(PhysicalType::kBool) &&
         value <= static_cast<uint8_t>(PhysicalType::kBinary);
}

StatusOr<std::string> EncodeRecord(MetaRecordKind kind,
                                   const std::string& payload) {
  if (payload.size() > kMaxMetaPayloadBytes) {
    return Status::InvalidArgument("metadata", "payload exceeds size limit");
  }
  std::string encoded;
  encoded.reserve(kMetaHeaderBytes + payload.size() + kMetaChecksumBytes);
  encoded.push_back(static_cast<char>(kMetaVersion));
  encoded.push_back(static_cast<char>(kind));
  AppendU32(&encoded, static_cast<uint32_t>(payload.size()));
  encoded.append(payload);
  AppendU32(&encoded, crc32c::Value(encoded.data(), encoded.size()));
  return encoded;
}

StatusOr<std::string> DecodeRecord(MetaRecordKind expected_kind,
                                   const std::string& encoded) {
  if (encoded.size() < kMetaHeaderBytes + kMetaChecksumBytes) {
    return Status::Corruption("metadata", "truncated record");
  }
  size_t offset = 0;
  const uint8_t version = static_cast<uint8_t>(encoded[offset++]);
  const uint8_t kind = static_cast<uint8_t>(encoded[offset++]);
  uint32_t payload_length = 0;
  if (version != kMetaVersion || kind != static_cast<uint8_t>(expected_kind) ||
      !ReadU32(encoded, &offset, &payload_length) ||
      payload_length > kMaxMetaPayloadBytes ||
      encoded.size() - offset != payload_length + kMetaChecksumBytes) {
    return Status::Corruption("metadata", "invalid record header");
  }
  const size_t checksum_offset = encoded.size() - kMetaChecksumBytes;
  size_t checksum_cursor = checksum_offset;
  uint32_t stored_checksum = 0;
  if (!ReadU32(encoded, &checksum_cursor, &stored_checksum) ||
      stored_checksum != crc32c::Value(encoded.data(), checksum_offset)) {
    return Status::Corruption("metadata", "CRC32C mismatch");
  }
  return encoded.substr(offset, payload_length);
}

}  // namespace

Status PropertyDefinition::Validate() const {
  if (!property_id.valid() || schema_epoch == 0 || name.empty() ||
      name.size() > kMaxPropertyNameBytes || blob_threshold_bytes == 0 ||
      (entity_kind != PropertyEntityKind::kVertex &&
       entity_kind != PropertyEntityKind::kEdge) ||
      !IsPhysicalType(static_cast<uint8_t>(physical_type))) {
    return Status::InvalidArgument("property definition", "invalid schema definition");
  }
  return Status::OK();
}

Status IdAllocatorState::Validate() const {
  if (next_id == 0 || (kind != IdKind::kVertex && kind != IdKind::kEdge)) {
    return Status::InvalidArgument("ID allocator", "invalid allocator state");
  }
  return Status::OK();
}

Status TransactionOutcomeRecord::Validate() const {
  if (!txn_id.valid() || commit_seq.value == 0 ||
      outcome != TransactionOutcome::kCommitted) {
    return Status::InvalidArgument("transaction outcome", "invalid outcome record");
  }
  return Status::OK();
}

Status SequenceRecord::Validate() const {
  if (commit_seq.value == 0 || !txn_id.valid() || fact_keys.empty() ||
      fact_keys.size() > std::numeric_limits<uint32_t>::max()) {
    return Status::InvalidArgument("sequence record", "invalid sequence metadata");
  }
  for (const std::string& key : fact_keys) {
    if (!DecodeFactKey(key).ok()) {
      return Status::InvalidArgument("sequence record", "invalid fact key");
    }
  }
  return Status::OK();
}

std::string EncodeCurrentFormatKey() { return "format/current"; }

StatusOr<std::string> EncodeSchemaMetaKey(PropertyId property_id,
                                          uint32_t schema_epoch) {
  if (!property_id.valid() || schema_epoch == 0) {
    return Status::InvalidArgument("schema key", "zero property ID or epoch");
  }
  std::string key = "schema/";
  AppendU16(&key, property_id.value);
  key.push_back('/');
  AppendU32(&key, schema_epoch);
  return key;
}

StatusOr<std::string> EncodeEdgeIdentityMetaKey(EdgeId edge_id) {
  if (!edge_id.valid()) return Status::InvalidArgument("edge key", "zero edge ID");
  std::string key = "edge/";
  AppendU64(&key, edge_id.value);
  return key;
}

std::string EncodeAllocatorMetaKey(IdKind kind) {
  return kind == IdKind::kVertex ? "allocator/vertex_next" :
                                  "allocator/edge_next";
}

StatusOr<std::string> EncodeTransactionMetaKey(TxnId txn_id) {
  if (!txn_id.valid()) return Status::InvalidArgument("transaction key", "zero transaction ID");
  std::string key = "txn/";
  AppendU64(&key, txn_id.value);
  return key;
}

StatusOr<std::string> EncodeSequenceMetaKey(CommitSeq commit_seq) {
  if (commit_seq.value == 0) return Status::InvalidArgument("sequence key", "zero commit sequence");
  std::string key = "sequence/";
  AppendU64(&key, commit_seq.value);
  return key;
}

std::string EncodeVisibleWatermarkKey() { return "watermark/visible"; }

std::string EncodeOldestReadableWatermarkKey() {
  return "watermark/oldest_readable";
}

StatusOr<std::string> EncodeFormatVersion(uint32_t format_version) {
  if (format_version == 0) return Status::InvalidArgument("format", "zero format version");
  std::string payload;
  AppendU32(&payload, format_version);
  return EncodeRecord(MetaRecordKind::kFormatVersion, payload);
}

StatusOr<uint32_t> DecodeFormatVersion(const std::string& encoded) {
  const auto payload = DecodeRecord(MetaRecordKind::kFormatVersion, encoded);
  if (!payload.ok()) return payload.status();
  size_t offset = 0;
  uint32_t version = 0;
  if (!ReadU32(payload.ValueOrDie(), &offset, &version) || offset != 4 ||
      version == 0) {
    return Status::Corruption("format", "invalid format version");
  }
  return version;
}

StatusOr<std::string> EncodeWatermark(CommitSeq watermark) {
  std::string payload;
  AppendU64(&payload, watermark.value);
  return EncodeRecord(MetaRecordKind::kWatermark, payload);
}

StatusOr<CommitSeq> DecodeWatermark(const std::string& encoded) {
  const auto payload = DecodeRecord(MetaRecordKind::kWatermark, encoded);
  if (!payload.ok()) return payload.status();
  size_t offset = 0;
  uint64_t watermark = 0;
  if (!ReadU64(payload.ValueOrDie(), &offset, &watermark) || offset != 8) {
    return Status::Corruption("watermark", "invalid watermark record");
  }
  return CommitSeq{watermark};
}

StatusOr<std::string> EncodePropertyDefinition(
    const PropertyDefinition& definition) {
  const Status valid = definition.Validate();
  if (!valid.ok()) return valid;
  std::string payload;
  payload.reserve(16 + definition.name.size());
  AppendU16(&payload, definition.property_id.value);
  AppendU32(&payload, definition.schema_epoch);
  payload.push_back(static_cast<char>(definition.entity_kind));
  payload.push_back(static_cast<char>(definition.physical_type));
  AppendU64(&payload, definition.blob_threshold_bytes);
  AppendU16(&payload, static_cast<uint16_t>(definition.name.size()));
  payload.append(definition.name);
  return EncodeRecord(MetaRecordKind::kPropertyDefinition, payload);
}

StatusOr<PropertyDefinition> DecodePropertyDefinition(const std::string& encoded) {
  const auto payload = DecodeRecord(MetaRecordKind::kPropertyDefinition, encoded);
  if (!payload.ok()) return payload.status();
  const std::string& bytes = payload.ValueOrDie();
  size_t offset = 0;
  uint16_t property_id = 0;
  uint32_t schema_epoch = 0;
  uint64_t blob_threshold = 0;
  uint16_t name_length = 0;
  if (!ReadU16(bytes, &offset, &property_id) ||
      !ReadU32(bytes, &offset, &schema_epoch) || offset + 2 > bytes.size()) {
    return Status::Corruption("property definition", "truncated schema record");
  }
  const uint8_t entity_kind = static_cast<uint8_t>(bytes[offset++]);
  const uint8_t physical_type = static_cast<uint8_t>(bytes[offset++]);
  if (!ReadU64(bytes, &offset, &blob_threshold) ||
      !ReadU16(bytes, &offset, &name_length) || name_length != bytes.size() - offset) {
    return Status::Corruption("property definition", "invalid schema record");
  }
  PropertyDefinition definition{PropertyId{property_id}, schema_epoch,
                                bytes.substr(offset),
                                static_cast<PropertyEntityKind>(entity_kind),
                                static_cast<PhysicalType>(physical_type),
                                blob_threshold};
  const Status valid = definition.Validate();
  if (!valid.ok()) return Status::Corruption("property definition", valid.ToString());
  return definition;
}

StatusOr<std::string> EncodeEdgeIdentity(const EdgeIdentity& identity) {
  const Status valid = identity.Validate();
  if (!valid.ok()) return valid;
  std::string payload;
  payload.reserve(32);
  AppendU64(&payload, identity.edge_id.value);
  AppendU64(&payload, identity.source_vertex_id.value);
  AppendU64(&payload, identity.target_vertex_id.value);
  AppendU64(&payload, identity.edge_type);
  return EncodeRecord(MetaRecordKind::kEdgeIdentity, payload);
}

StatusOr<EdgeIdentity> DecodeEdgeIdentity(const std::string& encoded) {
  const auto payload = DecodeRecord(MetaRecordKind::kEdgeIdentity, encoded);
  if (!payload.ok()) return payload.status();
  const std::string& bytes = payload.ValueOrDie();
  size_t offset = 0;
  uint64_t edge_id = 0;
  uint64_t source = 0;
  uint64_t target = 0;
  uint64_t edge_type = 0;
  if (!ReadU64(bytes, &offset, &edge_id) || !ReadU64(bytes, &offset, &source) ||
      !ReadU64(bytes, &offset, &target) || !ReadU64(bytes, &offset, &edge_type) ||
      offset != bytes.size()) {
    return Status::Corruption("edge identity", "invalid edge record");
  }
  EdgeIdentity identity{EdgeId{edge_id}, VertexId{source}, VertexId{target}, edge_type};
  const Status valid = identity.Validate();
  if (!valid.ok()) return Status::Corruption("edge identity", valid.ToString());
  return identity;
}

StatusOr<std::string> EncodeIdAllocatorState(const IdAllocatorState& state) {
  const Status valid = state.Validate();
  if (!valid.ok()) return valid;
  std::string payload(1, static_cast<char>(state.kind));
  AppendU64(&payload, state.next_id);
  return EncodeRecord(MetaRecordKind::kIdAllocator, payload);
}

StatusOr<IdAllocatorState> DecodeIdAllocatorState(const std::string& encoded) {
  const auto payload = DecodeRecord(MetaRecordKind::kIdAllocator, encoded);
  if (!payload.ok()) return payload.status();
  const std::string& bytes = payload.ValueOrDie();
  if (bytes.size() != 9) return Status::Corruption("ID allocator", "invalid allocator record");
  size_t offset = 1;
  uint64_t next_id = 0;
  if (!ReadU64(bytes, &offset, &next_id)) return Status::Corruption("ID allocator", "truncated allocator record");
  IdAllocatorState state{static_cast<IdKind>(static_cast<uint8_t>(bytes[0])), next_id};
  const Status valid = state.Validate();
  if (!valid.ok()) return Status::Corruption("ID allocator", valid.ToString());
  return state;
}

StatusOr<std::string> EncodeTransactionOutcome(
    const TransactionOutcomeRecord& record) {
  const Status valid = record.Validate();
  if (!valid.ok()) return valid;
  std::string payload;
  payload.reserve(17);
  AppendU64(&payload, record.txn_id.value);
  AppendU64(&payload, record.commit_seq.value);
  payload.push_back(static_cast<char>(record.outcome));
  return EncodeRecord(MetaRecordKind::kTransactionOutcome, payload);
}

StatusOr<TransactionOutcomeRecord> DecodeTransactionOutcome(
    const std::string& encoded) {
  const auto payload = DecodeRecord(MetaRecordKind::kTransactionOutcome, encoded);
  if (!payload.ok()) return payload.status();
  const std::string& bytes = payload.ValueOrDie();
  if (bytes.size() != 17) return Status::Corruption("transaction outcome", "invalid outcome record");
  size_t offset = 0;
  uint64_t txn_id = 0;
  uint64_t commit_seq = 0;
  if (!ReadU64(bytes, &offset, &txn_id) || !ReadU64(bytes, &offset, &commit_seq)) {
    return Status::Corruption("transaction outcome", "truncated outcome record");
  }
  TransactionOutcomeRecord record{TxnId{txn_id}, CommitSeq{commit_seq},
                                  static_cast<TransactionOutcome>(
                                      static_cast<uint8_t>(bytes[offset]))};
  const Status valid = record.Validate();
  if (!valid.ok()) return Status::Corruption("transaction outcome", valid.ToString());
  return record;
}

StatusOr<std::string> EncodeSequenceRecord(const SequenceRecord& record) {
  const Status valid = record.Validate();
  if (!valid.ok()) return valid;
  std::string payload;
  payload.reserve(28 + record.fact_keys.size() * kEncodedFactKeyBytes);
  AppendU64(&payload, record.commit_seq.value);
  AppendU64(&payload, record.txn_id.value);
  AppendU64(&payload, record.system_hlc);
  AppendU32(&payload, static_cast<uint32_t>(record.fact_keys.size()));
  for (const std::string& key : record.fact_keys) payload.append(key);
  return EncodeRecord(MetaRecordKind::kSequence, payload);
}

StatusOr<SequenceRecord> DecodeSequenceRecord(const std::string& encoded) {
  const auto payload = DecodeRecord(MetaRecordKind::kSequence, encoded);
  if (!payload.ok()) return payload.status();
  const std::string& bytes = payload.ValueOrDie();
  size_t offset = 0;
  uint64_t commit_seq = 0;
  uint64_t txn_id = 0;
  uint64_t system_hlc = 0;
  uint32_t count = 0;
  if (!ReadU64(bytes, &offset, &commit_seq) || !ReadU64(bytes, &offset, &txn_id) ||
      !ReadU64(bytes, &offset, &system_hlc) || !ReadU32(bytes, &offset, &count) ||
      count > (bytes.size() - offset) / kEncodedFactKeyBytes ||
      bytes.size() - offset != static_cast<size_t>(count) * kEncodedFactKeyBytes) {
    return Status::Corruption("sequence record", "invalid sequence record");
  }
  std::vector<std::string> keys;
  keys.reserve(count);
  for (uint32_t index = 0; index < count; ++index) {
    keys.push_back(bytes.substr(offset, kEncodedFactKeyBytes));
    offset += kEncodedFactKeyBytes;
  }
  SequenceRecord record{CommitSeq{commit_seq}, TxnId{txn_id}, system_hlc,
                        std::move(keys)};
  const Status valid = record.Validate();
  if (!valid.ok()) return Status::Corruption("sequence record", valid.ToString());
  return record;
}

}  // namespace cedar
