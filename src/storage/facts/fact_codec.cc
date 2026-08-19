// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/fact/fact_codec.h"

#include <cstring>
#include <limits>

#include "cedar/core/crc32c.h"

namespace cedar {
namespace {

constexpr uint8_t kFactKeyVersion = 2;
constexpr uint8_t kFactValueVersion = 1;
constexpr uint8_t kNoValueKind = 0;
constexpr uint8_t kEdgeIdentityValueKind = 9;
constexpr size_t kFactValueHeaderBytes = 11;
constexpr size_t kFactValueChecksumBytes = 4;

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

StatusOr<std::string> EncodeValuePayload(const Value& value) {
  std::string payload;
  switch (value.type()) {
    case PhysicalType::kBool:
      payload.push_back(std::get<bool>(value.data()) ? 1 : 0);
      break;
    case PhysicalType::kInt32:
      AppendU32(&payload,
                static_cast<uint32_t>(std::get<int32_t>(value.data())));
      break;
    case PhysicalType::kInt64:
      AppendU64(&payload,
                static_cast<uint64_t>(std::get<int64_t>(value.data())));
      break;
    case PhysicalType::kFloat32: {
      uint32_t bits = 0;
      const float number = std::get<float>(value.data());
      std::memcpy(&bits, &number, sizeof(bits));
      AppendU32(&payload, bits);
      break;
    }
    case PhysicalType::kFloat64: {
      uint64_t bits = 0;
      const double number = std::get<double>(value.data());
      std::memcpy(&bits, &number, sizeof(bits));
      AppendU64(&payload, bits);
      break;
    }
    case PhysicalType::kTimestamp64:
      AppendU64(&payload, std::get<uint64_t>(value.data()));
      break;
    case PhysicalType::kString:
    case PhysicalType::kBinary:
      payload = std::get<std::string>(value.data());
      break;
  }
  if (payload.size() > kMaxFactValuePayloadBytes) {
    return Status::InvalidArgument("fact value", "payload exceeds size limit");
  }
  return payload;
}

StatusOr<Value> DecodeValuePayload(PhysicalType type,
                                   const std::string& payload) {
  size_t offset = 0;
  uint32_t bits32 = 0;
  uint64_t bits64 = 0;
  switch (type) {
    case PhysicalType::kBool:
      if (payload.size() != 1 || (payload[0] != 0 && payload[0] != 1)) break;
      return Value::Bool(payload[0] == 1);
    case PhysicalType::kInt32:
      if (ReadU32(payload, &offset, &bits32) && offset == payload.size()) {
        return Value::Int32(static_cast<int32_t>(bits32));
      }
      break;
    case PhysicalType::kInt64:
      if (ReadU64(payload, &offset, &bits64) && offset == payload.size()) {
        return Value::Int64(static_cast<int64_t>(bits64));
      }
      break;
    case PhysicalType::kFloat32: {
      if (!ReadU32(payload, &offset, &bits32) || offset != payload.size()) break;
      float number = 0;
      std::memcpy(&number, &bits32, sizeof(number));
      return Value::Float32(number);
    }
    case PhysicalType::kFloat64: {
      if (!ReadU64(payload, &offset, &bits64) || offset != payload.size()) break;
      double number = 0;
      std::memcpy(&number, &bits64, sizeof(number));
      return Value::Float64(number);
    }
    case PhysicalType::kTimestamp64:
      if (ReadU64(payload, &offset, &bits64) && offset == payload.size()) {
        return Value::Timestamp(bits64);
      }
      break;
    case PhysicalType::kString:
      return Value::String(payload);
    case PhysicalType::kBinary:
      return Value::Binary(payload);
  }
  return Status::Corruption("fact value", "invalid physical value payload");
}

StatusOr<std::string> EncodeEdgeIdentityPayload(const EdgeIdentity& identity) {
  const Status valid = identity.Validate();
  if (!valid.ok()) return valid;
  std::string payload;
  payload.reserve(44);
  AppendU32(&payload, identity.home_part_id.value);
  AppendU64(&payload, identity.edge_id.value);
  AppendU32(&payload, identity.source_part_id.value);
  AppendU64(&payload, identity.source_vertex_id.value);
  AppendU32(&payload, identity.target_part_id.value);
  AppendU64(&payload, identity.target_vertex_id.value);
  AppendU64(&payload, identity.edge_type);
  return payload;
}

StatusOr<EdgeIdentity> DecodeEdgeIdentityPayload(const std::string& payload) {
  if (payload.size() != 44) {
    return Status::Corruption("edge identity fact", "invalid identity payload length");
  }
  size_t offset = 0;
  uint32_t home_part = 0;
  uint64_t edge_id = 0;
  uint32_t source_part = 0;
  uint64_t source = 0;
  uint32_t target_part = 0;
  uint64_t target = 0;
  uint64_t edge_type = 0;
  if (!ReadU32(payload, &offset, &home_part) ||
      !ReadU64(payload, &offset, &edge_id) ||
      !ReadU32(payload, &offset, &source_part) ||
      !ReadU64(payload, &offset, &source) ||
      !ReadU32(payload, &offset, &target_part) ||
      !ReadU64(payload, &offset, &target) ||
      !ReadU64(payload, &offset, &edge_type) || offset != payload.size()) {
    return Status::Corruption("edge identity fact", "invalid identity payload");
  }
  EdgeIdentity identity{EdgeRef{PartId{home_part}, EdgeId{edge_id}},
                        VertexRef{PartId{source_part}, VertexId{source}},
                        VertexRef{PartId{target_part}, VertexId{target}},
                        edge_type};
  const Status valid = identity.Validate();
  if (!valid.ok()) return Status::Corruption("edge identity fact", valid.ToString());
  return identity;
}

}  // namespace

std::string EncodeFactKey(const FactRef& ref, ValidTime valid_from,
                          CommitSeq commit_seq) {
  if (!ref.Validate().ok() || commit_seq.value == 0) return {};
  std::string encoded;
  encoded.reserve(kEncodedFactKeyBytes);
  encoded.push_back(static_cast<char>(kFactKeyVersion));
  AppendU32(&encoded, ref.part_id().value);
  encoded.push_back(static_cast<char>(ref.family()));
  AppendU16(&encoded, ref.property_id().value);
  AppendU64(&encoded, ref.entity_id());
  AppendU64(&encoded, ~valid_from.value);
  AppendU64(&encoded, ~commit_seq.value);
  return encoded;
}

StatusOr<DecodedFactKey> DecodeFactKey(const std::string& encoded) {
  if (encoded.size() != kEncodedFactKeyBytes) {
    return Status::Corruption("fact key", "incorrect fixed key length");
  }
  size_t offset = 0;
  const uint8_t version = static_cast<uint8_t>(encoded[offset++]);
  uint32_t part_id = 0;
  uint8_t family = 0;
  uint16_t property_id = 0;
  uint64_t entity_id = 0;
  uint64_t valid_from_desc = 0;
  uint64_t commit_seq_desc = 0;
  if (version != kFactKeyVersion || !ReadU32(encoded, &offset, &part_id) ||
      offset >= encoded.size()) {
    return Status::Corruption("fact key", "invalid key encoding");
  }
  family = static_cast<uint8_t>(encoded[offset++]);
  if (!ReadU16(encoded, &offset, &property_id) ||
      !ReadU64(encoded, &offset, &entity_id) ||
      !ReadU64(encoded, &offset, &valid_from_desc) ||
      !ReadU64(encoded, &offset, &commit_seq_desc)) {
    return Status::Corruption("fact key", "invalid key encoding");
  }
  const FactRef ref(PartId{part_id}, static_cast<FactFamily>(family),
                    PropertyId{property_id}, entity_id);
  const CommitSeq commit_seq{~commit_seq_desc};
  if (!ref.Validate().ok() || commit_seq.value == 0) {
    return Status::Corruption("fact key", "invalid fact identity");
  }
  return DecodedFactKey{ref, ValidTime{~valid_from_desc}, commit_seq};
}

StatusOr<std::string> EncodeFactValue(const FactEvent& event) {
  const Status valid = event.Validate();
  if (!valid.ok()) return valid;
  std::string payload;
  uint8_t kind = kNoValueKind;
  if (event.edge_identity.has_value()) {
    kind = kEdgeIdentityValueKind;
    auto encoded_payload = EncodeEdgeIdentityPayload(*event.edge_identity);
    if (!encoded_payload.ok()) return encoded_payload.status();
    payload = encoded_payload.ConsumeValueOrDie();
  } else if (event.value.has_value()) {
    kind = static_cast<uint8_t>(event.value->type());
    auto encoded_payload = EncodeValuePayload(*event.value);
    if (!encoded_payload.ok()) return encoded_payload.status();
    payload = encoded_payload.ConsumeValueOrDie();
  }
  std::string encoded;
  encoded.reserve(kFactValueHeaderBytes + payload.size() +
                  kFactValueChecksumBytes);
  encoded.push_back(static_cast<char>(kFactValueVersion));
  encoded.push_back(static_cast<char>(event.operation));
  AppendU32(&encoded, event.schema_epoch);
  encoded.push_back(static_cast<char>(kind));
  AppendU32(&encoded, static_cast<uint32_t>(payload.size()));
  encoded.append(payload);
  AppendU32(&encoded, crc32c::Value(encoded.data(), encoded.size()));
  return encoded;
}

StatusOr<FactEvent> DecodeFactValue(const FactRef& ref, ValidTime valid_from,
                                    CommitSeq commit_seq,
                                    const std::string& encoded) {
  if (encoded.size() < kFactValueHeaderBytes + kFactValueChecksumBytes) {
    return Status::Corruption("fact value", "truncated value record");
  }
  size_t offset = 0;
  const uint8_t version = static_cast<uint8_t>(encoded[offset++]);
  const uint8_t operation = static_cast<uint8_t>(encoded[offset++]);
  uint32_t schema_epoch = 0;
  uint32_t payload_length = 0;
  if (version != kFactValueVersion || !ReadU32(encoded, &offset, &schema_epoch)) {
    return Status::Corruption("fact value", "unsupported value format");
  }
  const uint8_t kind = static_cast<uint8_t>(encoded[offset++]);
  if (!ReadU32(encoded, &offset, &payload_length) ||
      payload_length > kMaxFactValuePayloadBytes ||
      encoded.size() - offset != payload_length + kFactValueChecksumBytes) {
    return Status::Corruption("fact value", "invalid payload length");
  }
  const size_t checksum_offset = encoded.size() - kFactValueChecksumBytes;
  size_t checksum_cursor = checksum_offset;
  uint32_t stored_checksum = 0;
  if (!ReadU32(encoded, &checksum_cursor, &stored_checksum) ||
      stored_checksum != crc32c::Value(encoded.data(), checksum_offset)) {
    return Status::Corruption("fact value", "CRC32C mismatch");
  }
  std::optional<Value> value;
  if (kind == kEdgeIdentityValueKind) {
    const auto identity = DecodeEdgeIdentityPayload(
        encoded.substr(offset, payload_length));
    if (!identity.ok()) return identity.status();
    value = std::nullopt;
    FactEvent event{ref, valid_from, commit_seq,
                    static_cast<FactOperation>(operation), schema_epoch, value,
                    identity.ValueOrDie()};
    const Status valid = event.Validate();
    if (!valid.ok()) return Status::Corruption("fact value", valid.ToString());
    return event;
  }
  if (kind == kNoValueKind) {
    if (payload_length != 0) {
      return Status::Corruption("fact value", "missing value kind");
    }
  } else {
    if (!IsPhysicalType(kind)) {
      return Status::Corruption("fact value", "unknown physical value kind");
    }
    const auto decoded = DecodeValuePayload(
        static_cast<PhysicalType>(kind), encoded.substr(offset, payload_length));
    if (!decoded.ok()) return decoded.status();
    value = decoded.ValueOrDie();
  }
  FactEvent event{ref, valid_from, commit_seq,
                  static_cast<FactOperation>(operation), schema_epoch, value};
  const Status valid = event.Validate();
  if (!valid.ok()) {
    return Status::Corruption("fact value", valid.ToString());
  }
  return event;
}

}  // namespace cedar
