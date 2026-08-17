// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_COLUMNAR_GRANULE_BLOCK_H_
#define CEDAR_COLUMNAR_GRANULE_BLOCK_H_

#include <array>
#include <optional>
#include <string>
#include <vector>

#include "cedar/columnar/page_format.h"
#include "cedar/storage/temporal_event.h"

namespace cedar {

constexpr size_t kPageTypeMetricSlots = 12;
// Identifies Cedar's persisted logical-type name registry. The concrete
// column/schema identity remains (entity_type, column_id, schema_epoch).
constexpr uint16_t kCedarLogicalTypeRegistryId = 1;

struct PageCompressionStats {
  std::array<uint64_t, kPageTypeMetricSlots> uncompressed_bytes{};
  std::array<uint64_t, kPageTypeMetricSlots> stored_bytes{};
};

struct BlockPartition {
  EntityType entity_type;
  uint16_t column_id;
  uint32_t schema_epoch;
  PhysicalType physical_type;
  uint16_t edge_type = 0;
  CompressionId compression_id = CompressionId::kNone;
  LogicalKeyKind key_kind = LogicalKeyKind::kProperty;
  uint32_t storage_shard_id = 0;
  uint16_t logical_type_id = kCedarLogicalTypeRegistryId;
};

struct GranuleBlock {
  std::string bytes;
  uint32_t row_count;
  PageCompressionStats compression;
  BlobHash identity;
};

StatusOr<GranuleBlock> BuildGranuleBlock(const BlockPartition& partition,
                                         const std::vector<TemporalEvent>& events);
Status AppendGranuleInlineValue(const Value& value, std::string* output);
bool DecodeGranuleInlineValue(const std::string& input, size_t* offset,
                              PhysicalType physical_type,
                              std::optional<Value>* value);
StatusOr<std::vector<TemporalEvent>> DecodeGranuleBlock(
    const std::string& bytes, const BlockPartition& partition);
BlobHash ComputeGranuleBlockIdentity(const std::string& encoded_header,
                                     const std::string& encoded_directory);
Status VerifyGranuleBlockIdentity(const std::string& bytes,
                                  const BlobHash& expected_identity);

}  // namespace cedar

#endif  // CEDAR_COLUMNAR_GRANULE_BLOCK_H_
