// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TCYPHER_LOGICAL_PLAN_H_
#define CEDAR_TCYPHER_LOGICAL_PLAN_H_

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/tcypher/binder.h"

namespace cedar {

enum class LogicalOperatorKind : uint8_t {
  kTemporalScan,
  kEventScan,
  kChangeScan,
  kIntervalDerive,
  kIntervalAlign,
  kTemporalCoalesce,
  kPropertyGather,
  kFilter,
  kDistinct,
  kAggregate,
  kSort,
  kExpand,
  kVariableExpand,
  kJoin,
  kProduceResult,
};

struct FactDemandSet {
  struct VariableDemand {
    std::string variable;
    BoundGraphKind kind = BoundGraphKind::kNode;
    std::optional<RelationshipDirection> direction;
    EntityType entity_type = EntityType::Vertex;
    std::optional<ColumnSchema> entity_schema;
    bool non_nullable = true;
    bool existence = false;
    bool complete_entity = false;
    bool grouping_identity = false;
    bool join_identity = false;
    std::vector<ColumnSchema> predicate_properties;
    std::vector<ColumnSchema> projection_properties;
    std::vector<ColumnSchema> grouping_properties;
    std::vector<ColumnSchema> ordering_properties;
    std::vector<ColumnSchema> join_properties;
    std::vector<ProvenanceField> provenance;
    std::vector<ProvenanceField> grouping_provenance;
    BindingId binding_id;
  };

  std::vector<VariableDemand> variables;
  bool existence_fact = false;
  std::vector<std::string> property_names;
};

struct LogicalPlanNode {
  LogicalOperatorKind kind;
  std::string variable;
  std::vector<std::string> properties;
  std::vector<BoundTemporalScope> temporal_scopes;
};

struct LogicalJoinEndpoint {
  BindingId binding_id;
  bool identity = false;
  std::optional<BoundPropertyId> property_id;
  PhysicalType type = PhysicalType::kBinary;
  bool nullable = true;
};

struct LogicalJoinEdge {
  LogicalJoinEndpoint left;
  LogicalJoinEndpoint right;
};

struct LogicalPlan {
  FactDemandSet demand;
  std::vector<LogicalPlanNode> nodes;
  std::vector<LogicalJoinEdge> join_edges;
};

StatusOr<LogicalPlan> LowerTcypher(const BoundTcypherStatement& statement);

}  // namespace cedar

#endif  // CEDAR_TCYPHER_LOGICAL_PLAN_H_
