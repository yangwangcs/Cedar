// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_FACT_FACT_H_
#define CEDAR_FACT_FACT_H_

#include <cstdint>
#include <optional>

#include "cedar/core/status.h"
#include "cedar/types/value.h"

namespace cedar {

struct VertexId {
  uint64_t value = 0;
  constexpr bool valid() const { return value != 0; }
  constexpr bool operator==(const VertexId&) const = default;
};

struct PartId {
  uint32_t value = 0;
  constexpr bool operator==(const PartId&) const = default;
};

struct EdgeId {
  uint64_t value = 0;
  constexpr bool valid() const { return value != 0; }
  constexpr bool operator==(const EdgeId&) const = default;
};

struct VertexRef {
  PartId part_id;
  VertexId vertex_id;

  constexpr bool valid() const { return vertex_id.valid(); }
  constexpr bool operator==(const VertexRef&) const = default;
};

struct EdgeRef {
  PartId home_part_id;
  EdgeId edge_id;

  constexpr bool valid() const { return edge_id.valid(); }
  constexpr bool operator==(const EdgeRef&) const = default;
};

struct PropertyId {
  uint16_t value = 0;
  constexpr bool valid() const { return value != 0; }
  constexpr bool operator==(const PropertyId&) const = default;
};

struct CommitSeq {
  uint64_t value = 0;
  constexpr bool operator==(const CommitSeq&) const = default;
};

struct TxnId {
  uint64_t value = 0;
  constexpr bool valid() const { return value != 0; }
  constexpr bool operator==(const TxnId&) const = default;
};

struct ValidTime {
  uint64_t value = 0;
  constexpr bool operator==(const ValidTime&) const = default;
};

struct SnapshotOptions {
  std::optional<CommitSeq> as_of;
};

enum class VacuumFaultPoint : uint8_t {
  kBeforeBoundaryWrite = 1,
  kAfterBoundaryWrite = 2,
  kAfterCleanupBatch = 3,
  kBeforeCompletion = 4,
};

enum class FactFamily : uint8_t {
  kVertexState = 1,
  kVertexProperty = 2,
  kEdgeIdentity = 3,
  kEdgeState = 4,
  kEdgeProperty = 5,
};

enum class FactOperation : uint8_t { kPut = 1, kDelete = 2 };

class FactRef {
 public:
  constexpr FactRef(PartId part_id, FactFamily family, PropertyId property_id,
                    uint64_t entity_id)
      : part_id_(part_id),
        family_(family),
        property_id_(property_id),
        entity_id_(entity_id) {}

  constexpr PartId part_id() const { return part_id_; }
  constexpr FactFamily family() const { return family_; }
  constexpr PropertyId property_id() const { return property_id_; }
  constexpr uint64_t entity_id() const { return entity_id_; }
  constexpr bool operator==(const FactRef&) const = default;

  bool IsProperty() const;
  Status Validate() const;

 private:
  PartId part_id_;
  FactFamily family_;
  PropertyId property_id_;
  uint64_t entity_id_;
};

class EntityFact {
 public:
  static EntityFact Vertex(VertexRef vertex);
  static EntityFact Edge(EdgeRef edge);

  constexpr const FactRef& ref() const { return ref_; }

 private:
  explicit constexpr EntityFact(FactRef ref) : ref_(ref) {}

  FactRef ref_;
};

class PropertyFact {
 public:
  static PropertyFact Vertex(VertexRef vertex, PropertyId property_id);
  static PropertyFact Edge(EdgeRef edge, PropertyId property_id);

  constexpr const FactRef& ref() const { return ref_; }

 private:
  explicit constexpr PropertyFact(FactRef ref) : ref_(ref) {}

  FactRef ref_;
};

struct EdgeIdentity {
  EdgeId edge_id;
  VertexId source_vertex_id;
  VertexId target_vertex_id;
  uint64_t edge_type = 0;
  PartId home_part_id;
  PartId source_part_id;
  PartId target_part_id;

  constexpr EdgeIdentity() = default;
  constexpr EdgeIdentity(EdgeId edge, VertexId source, VertexId target,
                         uint64_t type)
      : edge_id(edge),
        source_vertex_id(source),
        target_vertex_id(target),
        edge_type(type) {}
  constexpr EdgeIdentity(EdgeRef edge, VertexRef source, VertexRef target,
                         uint64_t type)
      : edge_id(edge.edge_id),
        source_vertex_id(source.vertex_id),
        target_vertex_id(target.vertex_id),
        edge_type(type),
        home_part_id(edge.home_part_id),
        source_part_id(source.part_id),
        target_part_id(target.part_id) {}

  constexpr EdgeRef edge_ref() const { return {home_part_id, edge_id}; }
  constexpr VertexRef source_ref() const {
    return {source_part_id, source_vertex_id};
  }
  constexpr VertexRef target_ref() const {
    return {target_part_id, target_vertex_id};
  }

  Status Validate() const;
  constexpr bool operator==(const EdgeIdentity&) const = default;
};

struct FactEvent {
  FactRef ref;
  ValidTime valid_from;
  CommitSeq commit_seq;
  FactOperation operation = FactOperation::kPut;
  uint32_t schema_epoch = 0;
  std::optional<Value> value;
  std::optional<EdgeIdentity> edge_identity;

  Status Validate() const;
};

struct PendingFactMutation {
  FactRef ref;
  ValidTime valid_from;
  FactOperation operation = FactOperation::kPut;
  uint32_t schema_epoch = 0;
  std::optional<Value> value;

  Status Validate() const;
};

}  // namespace cedar

#endif  // CEDAR_FACT_FACT_H_
