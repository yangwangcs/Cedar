// Copyright (c) 2025 The Cedar Authors. All rights reserved.

#include "cedar/compression/delta_encoder.h"
#include <cstring>

namespace cedar {

std::vector<uint8_t> DeltaEncoder::Encode(const std::vector<int64_t>& values) {
  if (values.empty()) return {};
  
  std::vector<uint8_t> result;
  result.resize(8);
  memcpy(result.data(), &values[0], sizeof(int64_t));
  
  for (size_t i = 1; i < values.size(); i++) {
    int64_t delta = values[i] - values[i - 1];
    auto encoded = EncodeVarint(delta);
    result.insert(result.end(), encoded.begin(), encoded.end());
  }
  
  return result;
}

std::vector<int64_t> DeltaEncoder::Decode(const std::vector<uint8_t>& encoded) {
  if (encoded.size() < 8) return {};
  
  std::vector<int64_t> result;
  int64_t value;
  memcpy(&value, encoded.data(), sizeof(int64_t));
  result.push_back(value);
  
  size_t pos = 8;
  while (pos < encoded.size()) {
    auto [delta, bytes_read] = DecodeVarint(encoded.data() + pos, encoded.size() - pos);
    if (bytes_read == 0) break;
    value += delta;
    result.push_back(value);
    pos += bytes_read;
  }
  
  return result;
}

std::vector<uint8_t> DeltaEncoder::EncodeVarint(int64_t value) {
  std::vector<uint8_t> result;
  uint64_t encoded = (value << 1) ^ (value >> 63);
  
  while (encoded >= 0x80) {
    result.push_back(static_cast<uint8_t>(encoded | 0x80));
    encoded >>= 7;
  }
  result.push_back(static_cast<uint8_t>(encoded));
  
  return result;
}

std::pair<int64_t, size_t> DeltaEncoder::DecodeVarint(const uint8_t* data, size_t len) {
  if (len == 0) return {0, 0};
  
  uint64_t result = 0;
  size_t shift = 0;
  size_t pos = 0;
  
  while (pos < len) {
    uint8_t byte = data[pos++];
    result |= static_cast<uint64_t>(byte & 0x7F) << shift;
    
    if ((byte & 0x80) == 0) {
      int64_t value = static_cast<int64_t>((result >> 1) ^ (-(result & 1)));
      return {value, pos};
    }
    
    shift += 7;
    if (shift >= 64) break;
  }
  
  return {0, 0};
}

std::vector<uint8_t> DeltaOfDeltaEncoder::Encode(const std::vector<uint64_t>& timestamps) {
  if (timestamps.size() < 2) {
    std::vector<uint8_t> result(timestamps.size() * sizeof(uint64_t));
    memcpy(result.data(), timestamps.data(), timestamps.size() * sizeof(uint64_t));
    return result;
  }
  
  std::vector<uint8_t> result;
  result.resize(16);
  memcpy(result.data(), &timestamps[0], sizeof(uint64_t));
  memcpy(result.data() + 8, &timestamps[1], sizeof(uint64_t));
  
  int64_t last_delta = static_cast<int64_t>(timestamps[1]) - static_cast<int64_t>(timestamps[0]);
  
  for (size_t i = 2; i < timestamps.size(); i++) {
    int64_t delta = static_cast<int64_t>(timestamps[i]) - static_cast<int64_t>(timestamps[i - 1]);
    int64_t delta_of_delta = delta - last_delta;
    last_delta = delta;
    
    auto encoded = DeltaEncoder::EncodeVarint(delta_of_delta);
    result.insert(result.end(), encoded.begin(), encoded.end());
  }
  
  return result;
}

std::vector<uint64_t> DeltaOfDeltaEncoder::Decode(const std::vector<uint8_t>& encoded) {
  if (encoded.size() < 16) {
    size_t count = encoded.size() / sizeof(uint64_t);
    std::vector<uint64_t> result(count);
    memcpy(result.data(), encoded.data(), count * sizeof(uint64_t));
    return result;
  }
  
  std::vector<uint64_t> result;
  uint64_t ts0, ts1;
  memcpy(&ts0, encoded.data(), sizeof(uint64_t));
  memcpy(&ts1, encoded.data() + 8, sizeof(uint64_t));
  result.push_back(ts0);
  result.push_back(ts1);
  
  int64_t last_delta = static_cast<int64_t>(ts1) - static_cast<int64_t>(ts0);
  uint64_t last_ts = ts1;
  
  size_t pos = 16;
  while (pos < encoded.size()) {
    auto [dod, bytes_read] = DeltaEncoder::DecodeVarint(encoded.data() + pos, encoded.size() - pos);
    if (bytes_read == 0) break;
    
    last_delta += dod;
    uint64_t ts = static_cast<uint64_t>(static_cast<int64_t>(last_ts) + last_delta);
    result.push_back(ts);
    last_ts = ts;
    pos += bytes_read;
  }
  
  return result;
}

}  // namespace cedar
