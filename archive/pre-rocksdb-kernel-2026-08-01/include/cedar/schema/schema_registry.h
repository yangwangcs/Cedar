// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#ifndef CEDAR_SCHEMA_SCHEMA_REGISTRY_H_
#define CEDAR_SCHEMA_SCHEMA_REGISTRY_H_

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/types/entity_type.h"
#include "cedar/types/value.h"

namespace cedar {

enum class EncodingPolicy : uint8_t { kAdaptive = 1, kPlain = 2 };
enum class CompressionPolicy : uint8_t { kNone = 1, kLz4 = 2, kZstd = 3 };

struct ColumnSchema {
  EntityType entity_type;
  uint16_t column_id;
  uint32_t schema_epoch;
  std::string logical_type;
  PhysicalType physical_type;
  uint64_t blob_threshold;
  EncodingPolicy encoding_policy;
  CompressionPolicy compression_policy;
};

constexpr size_t kMaxSchemaLogicalTypeBytes = 1024;

Status ValidateColumnSchema(const ColumnSchema& schema,
                            bool require_persisted_epoch);
bool SameColumnSchemaDefinition(const ColumnSchema& left,
                                const ColumnSchema& right);

struct SchemaSnapshot {
  using ColumnId = std::pair<uint8_t, uint16_t>;
  std::map<ColumnId, std::vector<ColumnSchema>> schemas;

  std::optional<ColumnSchema> Lookup(EntityType entity_type, uint16_t column_id,
                                     uint32_t epoch = 0) const;
};

class SchemaRegistry {
 public:
  SchemaRegistry() = default;
  Status Install(const std::vector<ColumnSchema>& schemas);
  Status Propose(const ColumnSchema& requested, ColumnSchema* proposed) const;
  std::shared_ptr<const SchemaSnapshot> Snapshot() const;
  std::optional<ColumnSchema> Lookup(EntityType entity_type, uint16_t column_id,
                                     uint32_t epoch = 0) const;
  Status Validate(const ColumnSchema& schema, const Value& value) const;

 private:
  using ColumnId = SchemaSnapshot::ColumnId;
  mutable std::mutex mutex_;
  std::map<ColumnId, std::vector<ColumnSchema>> schemas_;
};

}  // namespace cedar

#endif  // CEDAR_SCHEMA_SCHEMA_REGISTRY_H_
