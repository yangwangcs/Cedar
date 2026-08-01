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

struct EdgeId {
  uint64_t value = 0;
  constexpr bool valid() const { return value != 0; }
  constexpr bool operator==(const EdgeId&) const = default;
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

enum class FactFamily : uint8_t {
  kVertexState = 1,
  kVertexProperty = 2,
  kEdgeState = 3,
  kEdgeProperty = 4,
};

enum class FactOperation : uint8_t { kPut = 1, kDelete = 2 };

class FactRef {
 public:
  constexpr FactRef(FactFamily family, PropertyId property_id,
                    uint64_t entity_id)
      : family_(family), property_id_(property_id), entity_id_(entity_id) {}

  constexpr FactFamily family() const { return family_; }
  constexpr PropertyId property_id() const { return property_id_; }
  constexpr uint64_t entity_id() const { return entity_id_; }
  constexpr bool operator==(const FactRef&) const = default;

  bool IsProperty() const;
  Status Validate() const;

 private:
  FactFamily family_;
  PropertyId property_id_;
  uint64_t entity_id_;
};

class EntityFact {
 public:
  static EntityFact Vertex(VertexId vertex_id);
  static EntityFact Edge(EdgeId edge_id);

  constexpr const FactRef& ref() const { return ref_; }

 private:
  explicit constexpr EntityFact(FactRef ref) : ref_(ref) {}

  FactRef ref_;
};

class PropertyFact {
 public:
  static PropertyFact Vertex(VertexId vertex_id, PropertyId property_id);
  static PropertyFact Edge(EdgeId edge_id, PropertyId property_id);

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
