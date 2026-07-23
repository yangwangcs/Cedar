// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#include "cedar/transaction/logical_key.h"

#include <tuple>

namespace cedar {
namespace {

uint64_t Mix(uint64_t hash, uint64_t value) {
  hash ^= value;
  hash *= 1099511628211ULL;
  return hash;
}

uint8_t LogicalIdentityType(EntityType type) {
  return type == EntityType::Vertex
      ? static_cast<uint8_t>(EntityType::Vertex)
      : static_cast<uint8_t>(EntityType::EdgeOut);
}

}  // namespace

LogicalKey::LogicalKey(EntityType entity_type, LogicalKeyKind kind,
                       uint64_t entity_id, uint64_t target_id,
                       uint16_t column_id, uint16_t edge_type,
                       uint64_t edge_id)
    : entity_type_(entity_type),
      kind_(kind),
      entity_id_(entity_id),
      target_id_(target_id),
      column_id_(column_id),
      edge_type_(edge_type),
      edge_id_(edge_id) {}

LogicalKey LogicalKey::VertexExistence(uint64_t vertex_id) {
  return LogicalKey(EntityType::Vertex, LogicalKeyKind::kExistence, vertex_id,
                    0, 0, 0, 0);
}

LogicalKey LogicalKey::VertexProperty(uint64_t vertex_id, uint16_t column_id) {
  return LogicalKey(EntityType::Vertex, LogicalKeyKind::kProperty, vertex_id,
                    0, column_id, 0, 0);
}

LogicalKey LogicalKey::EdgeExistence(uint64_t entity_id, uint64_t target_id,
                                      uint16_t edge_type, uint64_t edge_id,
                                      EntityType direction) {
  return LogicalKey(direction, LogicalKeyKind::kExistence, entity_id, target_id,
                    0, edge_type, edge_id);
}

LogicalKey LogicalKey::EdgeProperty(uint64_t entity_id, uint64_t target_id,
                                    uint16_t edge_type, uint64_t edge_id,
                                    uint16_t column_id, EntityType direction) {
  return LogicalKey(direction, LogicalKeyKind::kProperty, entity_id, target_id,
                    column_id, edge_type, edge_id);
}

uint64_t LogicalKey::StableHash(uint64_t seed) const {
  uint64_t hash = 1469598103934665603ULL ^ seed;
  hash = Mix(hash, LogicalIdentityType(entity_type_));
  hash = Mix(hash, static_cast<uint8_t>(kind_));
  hash = Mix(hash, entity_id_);
  hash = Mix(hash, target_id_);
  hash = Mix(hash, column_id_);
  hash = Mix(hash, edge_type_);
  return Mix(hash, edge_id_);
}

bool LogicalKey::operator==(const LogicalKey& other) const {
  return std::make_tuple(LogicalIdentityType(entity_type_), kind_, entity_id_,
                         target_id_, column_id_, edge_type_, edge_id_) ==
         std::make_tuple(LogicalIdentityType(other.entity_type_), other.kind_,
                         other.entity_id_, other.target_id_, other.column_id_,
                         other.edge_type_, other.edge_id_);
}

bool LogicalKey::operator<(const LogicalKey& other) const {
  return std::make_tuple(LogicalIdentityType(entity_type_), kind_, entity_id_,
                         target_id_, column_id_, edge_type_, edge_id_) <
         std::make_tuple(LogicalIdentityType(other.entity_type_), other.kind_,
                         other.entity_id_, other.target_id_, other.column_id_,
                         other.edge_type_, other.edge_id_);
}

}  // namespace cedar
