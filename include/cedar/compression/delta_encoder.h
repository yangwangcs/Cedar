// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Delta encoding for time-series data

#ifndef FERN_DELTA_ENCODER_H_
#define FERN_DELTA_ENCODER_H_

#include <cstdint>
#include <vector>
#include <optional>

namespace cedar {

// Delta encoding for integer time-series data
class DeltaEncoder {
 public:
  static std::vector<uint8_t> Encode(const std::vector<int64_t>& values);
  static std::vector<int64_t> Decode(const std::vector<uint8_t>& encoded);
  static std::vector<uint8_t> EncodeVarint(int64_t value);
  static std::pair<int64_t, size_t> DecodeVarint(const uint8_t* data, size_t len);
};

// Delta-of-delta encoding for timestamps
class DeltaOfDeltaEncoder {
 public:
  static std::vector<uint8_t> Encode(const std::vector<uint64_t>& timestamps);
  static std::vector<uint64_t> Decode(const std::vector<uint8_t>& encoded);
};

}  // namespace cedar

#endif  // FERN_DELTA_ENCODER_H_
