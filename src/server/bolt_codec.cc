#include "cedar/server/bolt_codec.h"

#include <algorithm>
#include <cstdint>

namespace cedar::server {

StatusOr<std::string> EncodeBoltChunk(const std::string& payload,
                                      uint32_t max_chunk_bytes) {
  if (payload.size() > max_chunk_bytes || payload.size() > 0xffffU) {
    return Status::ResourceExhausted("bolt", "chunk exceeds configured limit");
  }
  std::string frame;
  frame.reserve(payload.size() + 2);
  frame.push_back(static_cast<char>((payload.size() >> 8) & 0xff));
  frame.push_back(static_cast<char>(payload.size() & 0xff));
  frame.append(payload);
  return frame;
}

StatusOr<std::string> DecodeBoltChunk(const std::string& frame,
                                      uint32_t max_chunk_bytes) {
  if (frame.size() < 2) return Status::ParseError("bolt", "truncated chunk header");
  const uint32_t length = (static_cast<uint32_t>(static_cast<uint8_t>(frame[0])) << 8) |
                          static_cast<uint8_t>(frame[1]);
  if (length > max_chunk_bytes) return Status::ResourceExhausted("bolt", "chunk exceeds configured limit");
  if (frame.size() != static_cast<size_t>(length) + 2) {
    return Status::ParseError("bolt", "chunk length does not match frame");
  }
  return frame.substr(2);
}

StatusOr<std::string> NegotiateBoltHandshake(const std::string& handshake) {
  if (handshake.size() != 20 || handshake.compare(0, 4, "\x60\x60\xB0\x17", 4) != 0) {
    return Status::ParseError("bolt", "invalid handshake preamble");
  }
  // Cedar accepts only the bounded v5.4 session shape; an unsupported
  // version receives the standard zero-version response.
  for (size_t offset = 4; offset < handshake.size(); offset += 4) {
    const uint32_t version =
        (static_cast<uint32_t>(static_cast<uint8_t>(handshake[offset])) << 24) |
        (static_cast<uint32_t>(static_cast<uint8_t>(handshake[offset + 1])) << 16) |
        (static_cast<uint32_t>(static_cast<uint8_t>(handshake[offset + 2])) << 8) |
        static_cast<uint8_t>(handshake[offset + 3]);
    if (version == 0x00000405U || version == 0x00020405U) {
      return std::string("\x00\x00\x04\x05", 4);
    }
  }
  return std::string(4, '\0');
}

Status AuthenticateBoltHello(const std::string& payload,
                              std::string_view expected_token) {
  if (expected_token.empty()) return Status::OK();
  if (payload.size() < 3 || static_cast<uint8_t>(payload[0]) < 0xB0 ||
      static_cast<uint8_t>(payload[0]) > 0xBF ||
      static_cast<uint8_t>(payload[1]) != 0x01) {
    return Status::InvalidArgument("bolt", "authentication requires HELLO credentials");
  }
  size_t offset = 2;
  const uint8_t map_marker = static_cast<uint8_t>(payload[offset++]);
  if (map_marker < 0xA0 || map_marker > 0xAF) {
    return Status::InvalidArgument("bolt", "authentication requires HELLO credentials");
  }
  const size_t entries = map_marker - 0xA0;
  std::string credentials;
  for (size_t entry = 0; entry < entries; ++entry) {
    if (offset >= payload.size()) {
      return Status::InvalidArgument("bolt", "malformed HELLO credentials");
    }
    const uint8_t key_marker = static_cast<uint8_t>(payload[offset++]);
    if (key_marker < 0x80 || key_marker > 0x8F ||
        offset > payload.size() - (key_marker - 0x80)) {
      return Status::InvalidArgument("bolt", "malformed HELLO credentials");
    }
    const std::string key = payload.substr(offset, key_marker - 0x80);
    offset += key_marker - 0x80;
    if (offset >= payload.size()) {
      return Status::InvalidArgument("bolt", "malformed HELLO credentials");
    }
    const uint8_t value_marker = static_cast<uint8_t>(payload[offset++]);
    if (value_marker < 0x80 || value_marker > 0x8F ||
        offset > payload.size() - (value_marker - 0x80)) {
      return Status::InvalidArgument("bolt", "malformed HELLO credentials");
    }
    const std::string value = payload.substr(offset, value_marker - 0x80);
    offset += value_marker - 0x80;
    if (key == "credentials") credentials = value;
  }
  if (credentials.size() != expected_token.size()) {
    return Status::InvalidArgument("bolt", "invalid credentials");
  }
  unsigned char difference = 0;
  for (size_t index = 0; index < credentials.size(); ++index) {
    difference |= static_cast<unsigned char>(credentials[index]) ^
                  static_cast<unsigned char>(expected_token[index]);
  }
  if (difference != 0) return Status::InvalidArgument("bolt", "invalid credentials");
  return Status::OK();
}

StatusOr<BoltMessageKind> DecodeBoltMessageKind(const std::string& payload) {
  if (payload.size() < 2 || static_cast<uint8_t>(payload[0]) < 0xB0 ||
      static_cast<uint8_t>(payload[0]) > 0xBF) {
    return Status::ParseError("bolt", "expected tiny struct message");
  }
  switch (static_cast<uint8_t>(payload[1])) {
    case 0x01: return BoltMessageKind::kHello;
    case 0x10: return BoltMessageKind::kRun;
    case 0x3F: return BoltMessageKind::kPull;
    case 0x11: return BoltMessageKind::kBegin;
    case 0x12: return BoltMessageKind::kCommit;
    case 0x13: return BoltMessageKind::kRollback;
    case 0x0F: return BoltMessageKind::kReset;
    case 0x02: return BoltMessageKind::kGoodbye;
    default: return Status::ParseError("bolt", "unsupported message signature");
  }
}

StatusOr<std::string> EncodeBoltSuccess(uint32_t max_chunk_bytes) {
  return EncodeBoltChunk(std::string("\xB1\x70\xA0", 3), max_chunk_bytes);
}

StatusOr<std::string> EncodeBoltIgnored(uint32_t max_chunk_bytes) {
  return EncodeBoltChunk(std::string("\xB1\x7E\xA0", 3), max_chunk_bytes);
}

StatusOr<std::string> EncodeBoltFailure(const Status& status,
                                        uint32_t max_chunk_bytes) {
  const std::string message = status.ToString();
  if (message.size() > 255) {
    return Status::ResourceExhausted("bolt", "failure message exceeds limit");
  }
  auto append_string = [](std::string* output, const std::string& value) {
    if (value.size() <= 15) {
      output->push_back(static_cast<char>(0x80 + value.size()));
    } else {
      output->push_back(static_cast<char>(0xD0));
      output->push_back(static_cast<char>(value.size()));
    }
    output->append(value);
  };
  std::string payload{static_cast<char>(0xB1), static_cast<char>(0x7F),
                      static_cast<char>(0xA2)};
  append_string(&payload, "code");
  append_string(&payload, "Cedar.ClientError");
  append_string(&payload, "message");
  append_string(&payload, message);
  return EncodeBoltChunk(payload, max_chunk_bytes);
}

}  // namespace cedar::server
