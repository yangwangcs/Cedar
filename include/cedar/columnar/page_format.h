// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#ifndef CEDAR_COLUMNAR_PAGE_FORMAT_H_
#define CEDAR_COLUMNAR_PAGE_FORMAT_H_

#include <array>
#include <cstdint>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/types/value.h"

namespace cedar {

enum class PageType : uint8_t { kEntityId = 1, kTargetId = 2, kValidFrom = 3,
  kCommitSeq = 4, kOperation = 5, kValueClass = 6, kTypedValue = 7,
  kBlobRef = 8, kEdgeId = 9, kInlinePresence = 10, kBlobPresence = 11 };
enum class EncodingId : uint8_t {
  kPlain = 1,
  kRle = 2,
  kDelta = 3,
  kDictionary = 4,
  kFrameOfReference = 5,
  kBitmap = 6,
  kBitPacking = 7,
  kDeltaOfDelta = 8,
  kXor = 9,
};
enum class CompressionId : uint8_t { kNone = 1, kLz4 = 2, kZstd = 3 };

struct PageCodecCapability {
  std::string name;
  std::string version;
  bool compiled = false;
};

struct PageCodecCapabilities {
  PageCodecCapability lz4;
  PageCodecCapability zstd;
};

struct PageHeader {
  PageType page_type;
  PhysicalType physical_type;
  EncodingId encoding_id;
  CompressionId compression_id;
  uint64_t first_row;
  uint32_t row_count;
  uint32_t value_count;
  uint32_t required_flags;
  // `uncompressed_size` is the final decoded payload size. `encoded_size` is
  // the size after page encoding and before optional compression.
  uint64_t uncompressed_size = 0;
  uint64_t encoded_size = 0;
  uint64_t compressed_size = 0;
  uint32_t payload_crc32c = 0;
};

struct Page { PageHeader header; std::string payload; };

struct PageDirectoryEntry {
  PageType page_type;
  uint32_t ordinal;
  uint64_t offset;
  uint64_t length;
  std::array<uint8_t, 32> content_hash{};
};

constexpr uint32_t kPageFormatVersion = 3;
constexpr uint16_t kPageFormatHeaderSize = 60;
constexpr uint32_t kPageSupportedRequiredFlags = 0;
constexpr uint64_t kHardMaxPageBytes = 256 * 1024;

// Checked production boundary. The codec validates shape and all bounded
// allocations before encoding or compression.
StatusOr<std::string> EncodePageChecked(PageHeader header,
                                         const std::string& payload);
PageCodecCapabilities GetPageCodecCapabilities();
Status VerifyPageCodecCapabilities();
StatusOr<PageHeader> DecodePageHeader(const std::string& encoded_header);
StatusOr<Page> DecodePage(const std::string& encoded);
std::string EncodePageDirectory(const std::vector<PageDirectoryEntry>& entries);
StatusOr<std::vector<PageDirectoryEntry>> DecodePageDirectory(const std::string& encoded);

}  // namespace cedar

#endif  // CEDAR_COLUMNAR_PAGE_FORMAT_H_
