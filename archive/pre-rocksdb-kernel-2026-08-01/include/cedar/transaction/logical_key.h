// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#ifndef CEDAR_TRANSACTION_LOGICAL_KEY_H_
#define CEDAR_TRANSACTION_LOGICAL_KEY_H_

#include <cstdint>

#include "cedar/types/entity_type.h"

namespace cedar {

enum class LogicalKeyKind : uint8_t {
  kExistence = 0,
  kProperty = 1,
};

// LogicalKey identifies one immutable fact timeline. It deliberately excludes
// valid time and transaction sequence, which identify versions of that fact.
class LogicalKey {
 public:
  static LogicalKey VertexExistence(uint64_t vertex_id);
  static LogicalKey VertexProperty(uint64_t vertex_id, uint16_t column_id);
  static LogicalKey EdgeExistence(uint64_t entity_id, uint64_t target_id,
                                  uint16_t edge_type, uint64_t edge_id,
                                  EntityType direction);
  static LogicalKey EdgeProperty(uint64_t entity_id, uint64_t target_id,
                                uint16_t edge_type, uint64_t edge_id,
                                uint16_t column_id, EntityType direction);

  EntityType entity_type() const { return entity_type_; }
  LogicalKeyKind kind() const { return kind_; }
  uint64_t entity_id() const { return entity_id_; }
  uint64_t target_id() const { return target_id_; }
  uint16_t column_id() const { return column_id_; }
  uint16_t edge_type() const { return edge_type_; }
  uint64_t edge_id() const { return edge_id_; }
  bool IsExistence() const { return kind_ == LogicalKeyKind::kExistence; }
  // Schema and SST partitions label edge existence by its relationship type;
  // property facts use their property column. This is not part of identity.
  uint16_t schema_column_id() const {
    return entity_type_ == EntityType::Vertex || kind_ == LogicalKeyKind::kProperty
        ? column_id_ : edge_type_;
  }

  uint64_t StableHash(uint64_t seed = 0) const;

  bool operator==(const LogicalKey& other) const;
  bool operator!=(const LogicalKey& other) const { return !(*this == other); }
  bool operator<(const LogicalKey& other) const;

 private:
  LogicalKey(EntityType entity_type, LogicalKeyKind kind, uint64_t entity_id,
             uint64_t target_id, uint16_t column_id, uint16_t edge_type,
             uint64_t edge_id);

  EntityType entity_type_;
  LogicalKeyKind kind_;
  uint64_t entity_id_;
  uint64_t target_id_;
  uint16_t column_id_;
  uint16_t edge_type_;
  uint64_t edge_id_;
};

}  // namespace cedar

#endif  // CEDAR_TRANSACTION_LOGICAL_KEY_H_
