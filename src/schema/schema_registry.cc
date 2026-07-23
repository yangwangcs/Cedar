// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#include "cedar/schema/schema_registry.h"

#include <limits>

#ifndef CEDAR_HAS_ZSTD
#define CEDAR_HAS_ZSTD 0
#endif

namespace cedar {
namespace {

bool ValidEntityType(EntityType entity_type) {
  const uint8_t value = static_cast<uint8_t>(entity_type);
  return value <= static_cast<uint8_t>(EntityType::EdgeIn);
}

bool ValidPhysicalType(PhysicalType physical_type) {
  const uint8_t value = static_cast<uint8_t>(physical_type);
  return value >= static_cast<uint8_t>(PhysicalType::kBool) &&
         value <= static_cast<uint8_t>(PhysicalType::kBinary);
}

bool ValidEncodingPolicy(EncodingPolicy policy) {
  return policy == EncodingPolicy::kAdaptive || policy == EncodingPolicy::kPlain;
}

bool ValidCompressionPolicy(CompressionPolicy policy) {
  return policy == CompressionPolicy::kNone || policy == CompressionPolicy::kLz4 ||
         policy == CompressionPolicy::kZstd;
}

}  // namespace

Status ValidateColumnSchema(const ColumnSchema& schema,
                            bool require_persisted_epoch) {
  if (!ValidEntityType(schema.entity_type) ||
      !ValidPhysicalType(schema.physical_type) ||
      !ValidEncodingPolicy(schema.encoding_policy) ||
      !ValidCompressionPolicy(schema.compression_policy)) {
    return Status::InvalidArgument("schema", "invalid schema enum value");
  }
  if ((require_persisted_epoch && schema.schema_epoch == 0) ||
      (!require_persisted_epoch && schema.schema_epoch != 0)) {
    return Status::InvalidArgument("schema", "invalid schema epoch");
  }
  if (schema.logical_type.empty() ||
      schema.logical_type.size() > kMaxSchemaLogicalTypeBytes) {
    return Status::InvalidArgument("schema", "logical type length is out of bounds");
  }
  if (schema.compression_policy == CompressionPolicy::kZstd && !CEDAR_HAS_ZSTD) {
    return Status::NotSupported("schema",
                                "Zstd compression is unavailable in this build");
  }
  return Status::OK();
}

bool SameColumnSchemaDefinition(const ColumnSchema& left,
                                const ColumnSchema& right) {
  return left.entity_type == right.entity_type &&
         left.column_id == right.column_id &&
         left.logical_type == right.logical_type &&
         left.physical_type == right.physical_type &&
         left.blob_threshold == right.blob_threshold &&
         left.encoding_policy == right.encoding_policy &&
         left.compression_policy == right.compression_policy;
}

Status SchemaRegistry::Install(const std::vector<ColumnSchema>& schemas) {
  std::map<ColumnId, std::vector<ColumnSchema>> installed;
  for (const ColumnSchema& schema : schemas) {
    const Status valid = ValidateColumnSchema(schema, true);
    if (!valid.ok()) return valid;
    auto& epochs = installed[{static_cast<uint8_t>(schema.entity_type),
                              schema.column_id}];
    if (epochs.size() == std::numeric_limits<uint32_t>::max() ||
        schema.schema_epoch != epochs.size() + 1) {
      return Status::Corruption("schema", "non-contiguous or duplicate schema epoch");
    }
    epochs.push_back(schema);
  }
  std::lock_guard<std::mutex> lock(mutex_);
  schemas_ = std::move(installed);
  return Status::OK();
}

Status SchemaRegistry::Propose(const ColumnSchema& requested,
                               ColumnSchema* proposed) const {
  if (proposed == nullptr) {
    return Status::InvalidArgument("schema", "missing proposal output");
  }
  const Status valid = ValidateColumnSchema(requested, false);
  if (!valid.ok()) return valid;
  std::lock_guard<std::mutex> lock(mutex_);
  const auto found = schemas_.find(
      {static_cast<uint8_t>(requested.entity_type), requested.column_id});
  if (found != schemas_.end() && !found->second.empty()) {
    const ColumnSchema& latest = found->second.back();
    if (SameColumnSchemaDefinition(latest, requested)) {
      *proposed = latest;
      return Status::OK();
    }
    if (found->second.size() == std::numeric_limits<uint32_t>::max()) {
      return Status::ResourceExhausted("schema", "schema epoch space exhausted");
    }
  }
  *proposed = requested;
  proposed->schema_epoch = found == schemas_.end()
      ? 1
      : static_cast<uint32_t>(found->second.size() + 1);
  return Status::OK();
}

std::optional<ColumnSchema> SchemaRegistry::Lookup(
    EntityType entity_type, uint16_t column_id, uint32_t epoch) const {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto it = schemas_.find({static_cast<uint8_t>(entity_type), column_id});
  if (it == schemas_.end() || it->second.empty()) return std::nullopt;
  if (epoch == 0) return it->second.back();
  if (epoch > it->second.size()) return std::nullopt;
  return it->second[epoch - 1];
}

std::optional<ColumnSchema> SchemaSnapshot::Lookup(
    EntityType entity_type, uint16_t column_id, uint32_t epoch) const {
  const auto it = schemas.find({static_cast<uint8_t>(entity_type), column_id});
  if (it == schemas.end() || it->second.empty()) return std::nullopt;
  if (epoch == 0) return it->second.back();
  if (epoch > it->second.size()) return std::nullopt;
  return it->second[epoch - 1];
}

std::shared_ptr<const SchemaSnapshot> SchemaRegistry::Snapshot() const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto snapshot = std::make_shared<SchemaSnapshot>();
  snapshot->schemas = schemas_;
  return snapshot;
}

Status SchemaRegistry::Validate(const ColumnSchema& schema,
                                const Value& value) const {
  if (schema.physical_type != value.type()) {
    return Status::SchemaMismatch(
        schema.logical_type,
        "value physical type differs from registered schema");
  }
  return Status::OK();
}

}  // namespace cedar
