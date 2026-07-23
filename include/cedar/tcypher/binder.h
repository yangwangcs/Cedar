// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TCYPHER_BINDER_H_
#define CEDAR_TCYPHER_BINDER_H_

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/schema/schema_registry.h"
#include "cedar/tcypher/syntax/parser.h"
#include "cedar/transaction/commit_timeline.h"

namespace cedar {

struct BindingId {
  uint32_t value = 0;
  friend bool operator==(BindingId left, BindingId right) {
    return left.value == right.value;
  }
  friend bool operator!=(BindingId left, BindingId right) { return !(left == right); }
  friend bool operator<(BindingId left, BindingId right) {
    return left.value < right.value;
  }
};

struct BoundPropertyId {
  uint32_t value = 0;
  friend bool operator==(BoundPropertyId left, BoundPropertyId right) {
    return left.value == right.value;
  }
  friend bool operator!=(BoundPropertyId left, BoundPropertyId right) {
    return !(left == right);
  }
  friend bool operator<(BoundPropertyId left, BoundPropertyId right) {
    return left.value < right.value;
  }
};

struct TcypherBindingContext {
  const CommitTimeline& commit_timeline;
  uint64_t visible_seq_ceiling;
  uint64_t statement_start_valid_time;
  std::map<std::string, uint64_t> timestamp_parameters;
  std::shared_ptr<const SchemaSnapshot> schema_snapshot;
};

enum class BoundGraphKind : uint8_t { kNode, kRelationship };

enum class ProvenanceField : uint8_t {
  kValidFrom,
  kValidTo,
  kCommitSeq,
  kOperation,
  kSystemTime,
};

struct BoundVariable {
  std::string variable;
  BoundGraphKind kind = BoundGraphKind::kNode;
  std::optional<RelationshipDirection> direction;
  EntityType entity_type = EntityType::Vertex;
  bool non_nullable = true;
  bool complete_entity = false;
  bool grouping_identity = false;
  bool join_identity = false;
  std::optional<ColumnSchema> entity_schema;
  std::vector<ProvenanceField> provenance;
  std::vector<ProvenanceField> grouping_provenance;
  BindingId binding_id;
};

struct BoundPropertyReference {
  std::string variable;
  ColumnSchema column;
  bool nullable = true;
  bool predicate = false;
  bool projection = false;
  bool grouping = false;
  bool ordering = false;
  bool join = false;
  BoundPropertyId property_id;
  BindingId binding_id;
};

struct BoundPredicateExpression {
  BoundPropertyId property_id;
  BindingId binding_id;
  ColumnSchema column;
  bool nullable = true;
  StringPredicateKind kind = StringPredicateKind::kEquality;
  std::vector<Value> values;
  std::optional<Value> lower_bound;
  std::optional<Value> upper_bound;
  bool lower_inclusive = true;
  bool upper_inclusive = true;
};

struct BoundProjectionExpression {
  ReturnExpressionKind kind = ReturnExpressionKind::kBinding;
  BindingId binding_id;
  std::optional<BoundPropertyId> property_id;
  PhysicalType type = PhysicalType::kBinary;
  bool nullable = true;
  std::string output_name;
  bool relationship_identity = false;
};

struct BoundJoinInput {
  std::string variable;
  bool identity = false;
  std::optional<ColumnSchema> column;
};

struct BoundJoinEquality {
  BoundJoinInput left;
  BoundJoinInput right;
};

struct BoundTemporalScope {
  TemporalAxis axis;
  TemporalScopeMode mode;
  uint64_t valid_time_start;
  uint64_t valid_time_end;
  uint64_t snapshot_seq;
  uint64_t system_time_start;
  uint64_t system_time_end;
};

struct BoundTcypherStatement {
  TcypherStatement syntax;
  std::vector<BoundTemporalScope> temporal_scopes;
  std::vector<BoundTemporalScope> primary_match_scopes;
  std::vector<std::vector<BoundTemporalScope>> additional_match_scopes;
  std::optional<uint64_t> mutation_valid_from;
  std::optional<uint64_t> mutation_target_entity_id;
  std::vector<BoundVariable> variables;
  std::vector<BoundPropertyReference> properties;
  std::vector<BoundJoinEquality> joins;
  std::vector<BoundPredicateExpression> predicates;
  std::vector<BoundProjectionExpression> projections;
  bool root_point_candidate = false;
  bool root_temporal_candidate = false;
  // The graph-runtime integration is deliberately narrow. The physical
  // planner owns the bound relationship identity; the executor does not
  // rediscover it from parser text.
  bool fixed_expand_point_candidate = false;
  // The first range graph candidate deliberately covers one fixed hop without
  // property demand. Its physical path aligns raw endpoint/edge intervals.
  bool fixed_expand_range_candidate = false;
  // Relationship change scans preserve the complete edge identity and raw
  // event provenance without materializing committed history.
  bool fixed_expand_change_candidate = false;
  std::optional<uint64_t> root_exact_entity_id;
  std::string root_exact_entity_parameter;
};

StatusOr<BoundTcypherStatement> BindTcypher(
    const TcypherStatement& statement, const TcypherBindingContext& context);

}  // namespace cedar

#endif  // CEDAR_TCYPHER_BINDER_H_
