// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE::cedar_parquet {

// Cedar uses the Parquet split-block Bloom algorithm and its exact XXHASH
// serialization, while the logical values are the 32-byte v2 user keys.
class CedarSplitBlockBloomFilter {
 public:
  CedarSplitBlockBloomFilter() = default;

  static Status Build(const std::string* const* values, size_t count,
                      std::string* encoded);
  static Status Decode(std::string_view encoded,
                       CedarSplitBlockBloomFilter* filter);

  bool MayContain(std::string_view value) const;
  uint32_t bitset_bytes() const { return bitset_bytes_; }

 private:
  static constexpr uint32_t kBytesPerBlock = 32;
  static constexpr uint32_t kMaxBytes = 128U << 20;
  static constexpr uint32_t kS[8] = {
      0x47b6137bU, 0x44974d91U, 0x8824ad5bU, 0xa2b7289dU,
      0x705495c7U, 0x2df1424bU, 0x9efc4947U, 0x5c6bfb31U};

  static uint64_t Hash(std::string_view value);
  void InsertHash(uint64_t hash);
  bool FindHash(uint64_t hash) const;

  uint32_t bitset_bytes_ = 0;
  std::string bitset_;
};

}  // namespace ROCKSDB_NAMESPACE::cedar_parquet
