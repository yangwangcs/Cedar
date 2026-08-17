// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/tcypher/logical_plan.h"

#include <algorithm>

namespace cedar {
namespace {

void SortAndDeduplicateColumns(std::vector<ColumnSchema>* columns) {
  std::sort(columns->begin(), columns->end(),
            [](const ColumnSchema& left, const ColumnSchema& right) {
              if (left.entity_type != right.entity_type) {
                return static_cast<uint8_t>(left.entity_type) <
                       static_cast<uint8_t>(right.entity_type);
              }
              if (left.column_id != right.column_id) return left.column_id < right.column_id;
              return left.schema_epoch < right.schema_epoch;
            });
  columns->erase(
      std::unique(columns->begin(), columns->end(),
                  [](const ColumnSchema& left, const ColumnSchema& right) {
                    return left.entity_type == right.entity_type &&
                           left.column_id == right.column_id &&
                           left.schema_epoch == right.schema_epoch;
                  }),
      columns->end());
}

FactDemandSet BuildFactDemand(const BoundTcypherStatement& statement) {
  FactDemandSet demand;
  for (const BoundVariable& variable : statement.variables) {
    FactDemandSet::VariableDemand variable_demand;
    variable_demand.variable = variable.variable;
    variable_demand.kind = variable.kind;
    variable_demand.direction = variable.direction;
    variable_demand.entity_type = variable.entity_type;
    variable_demand.entity_schema = variable.entity_schema;
    variable_demand.non_nullable = variable.non_nullable;
    variable_demand.existence = true;
    variable_demand.complete_entity = variable.complete_entity;
    variable_demand.grouping_identity = variable.grouping_identity;
    variable_demand.join_identity = variable.join_identity;
    variable_demand.provenance = variable.provenance;
    variable_demand.grouping_provenance = variable.grouping_provenance;
    variable_demand.binding_id = variable.binding_id;
    demand.variables.push_back(std::move(variable_demand));
  }
  demand.existence_fact = !demand.variables.empty();
  for (const BoundPropertyReference& property : statement.properties) {
    const auto variable = std::find_if(
        demand.variables.begin(), demand.variables.end(),
        [&](const FactDemandSet::VariableDemand& candidate) {
          return candidate.variable == property.variable;
        });
    if (variable == demand.variables.end()) continue;
    if (property.predicate) variable->predicate_properties.push_back(property.column);
    if (property.projection) variable->projection_properties.push_back(property.column);
    if (property.grouping) variable->grouping_properties.push_back(property.column);
    if (property.ordering) variable->ordering_properties.push_back(property.column);
    if (property.join) variable->join_properties.push_back(property.column);
    demand.property_names.push_back(property.column.logical_type);
  }
  for (FactDemandSet::VariableDemand& variable : demand.variables) {
    SortAndDeduplicateColumns(&variable.predicate_properties);
    SortAndDeduplicateColumns(&variable.projection_properties);
    SortAndDeduplicateColumns(&variable.grouping_properties);
    SortAndDeduplicateColumns(&variable.ordering_properties);
    SortAndDeduplicateColumns(&variable.join_properties);
    std::sort(variable.provenance.begin(), variable.provenance.end());
    variable.provenance.erase(
        std::unique(variable.provenance.begin(), variable.provenance.end()),
        variable.provenance.end());
    std::sort(variable.grouping_provenance.begin(),
              variable.grouping_provenance.end());
    variable.grouping_provenance.erase(
        std::unique(variable.grouping_provenance.begin(),
                    variable.grouping_provenance.end()),
        variable.grouping_provenance.end());
  }
  std::sort(demand.variables.begin(), demand.variables.end(),
            [](const auto& left, const auto& right) {
              return left.variable < right.variable;
            });
  std::sort(demand.property_names.begin(), demand.property_names.end());
  demand.property_names.erase(
      std::unique(demand.property_names.begin(), demand.property_names.end()),
      demand.property_names.end());
  return demand;
}

bool SameColumn(const ColumnSchema& left, const ColumnSchema& right) {
  return left.entity_type == right.entity_type && left.column_id == right.column_id &&
         left.schema_epoch == right.schema_epoch;
}

StatusOr<LogicalJoinEndpoint> LowerJoinEndpoint(
    const BoundTcypherStatement& statement, const BoundJoinInput& input) {
  const auto variable = std::find_if(
      statement.variables.begin(), statement.variables.end(),
      [&input](const BoundVariable& candidate) { return candidate.variable == input.variable; });
  if (variable == statement.variables.end()) {
    return Status::BindError("logical plan", "join references an unknown binding");
  }

  LogicalJoinEndpoint endpoint;
  endpoint.binding_id = variable->binding_id;
  endpoint.identity = input.identity;
  if (input.identity) {
    endpoint.type = PhysicalType::kInt64;
    endpoint.nullable = !variable->non_nullable;
    return endpoint;
  }

  if (!input.column.has_value()) {
    return Status::BindError("logical plan", "property join has no bound column");
  }
  const auto property = std::find_if(
      statement.properties.begin(), statement.properties.end(),
      [&input](const BoundPropertyReference& candidate) {
        return candidate.variable == input.variable &&
               SameColumn(candidate.column, *input.column);
      });
  if (property == statement.properties.end()) {
    return Status::BindError("logical plan", "join property has no bound property id");
  }
  endpoint.property_id = property->property_id;
  endpoint.type = property->column.physical_type;
  endpoint.nullable = property->nullable;
  return endpoint;
}

bool LogicalJoinEndpointLess(const LogicalJoinEndpoint& left,
                             const LogicalJoinEndpoint& right) {
  if (left.binding_id != right.binding_id) {
    return left.binding_id < right.binding_id;
  }
  if (left.identity != right.identity) return left.identity < right.identity;
  if (left.property_id != right.property_id) return left.property_id < right.property_id;
  if (left.type != right.type) {
    return static_cast<uint8_t>(left.type) < static_cast<uint8_t>(right.type);
  }
  return left.nullable < right.nullable;
}

bool JoinEdgeLess(const LogicalJoinEdge& left, const LogicalJoinEdge& right) {
  if (LogicalJoinEndpointLess(left.left, right.left)) return true;
  if (LogicalJoinEndpointLess(right.left, left.left)) return false;
  return LogicalJoinEndpointLess(left.right, right.right);
}

bool SameJoinEdge(const LogicalJoinEdge& left, const LogicalJoinEdge& right) {
  return !JoinEdgeLess(left, right) && !JoinEdgeLess(right, left);
}

bool IsDistinctBindingJoinPredicate(const StringEqualityPredicate& predicate) {
  return predicate.entity_id_variable.has_value() &&
         predicate.variable != *predicate.entity_id_variable;
}

Status LowerJoinGraph(const BoundTcypherStatement& statement, LogicalPlan* plan) {
  for (const BoundJoinEquality& equality : statement.joins) {
    auto left = LowerJoinEndpoint(statement, equality.left);
    if (!left.ok()) return left.status();
    auto right = LowerJoinEndpoint(statement, equality.right);
    if (!right.ok()) return right.status();
    LogicalJoinEdge edge{std::move(left).ConsumeValueOrDie(),
                         std::move(right).ConsumeValueOrDie()};
    if (edge.left.binding_id == edge.right.binding_id) continue;
    if (LogicalJoinEndpointLess(edge.right, edge.left)) std::swap(edge.left, edge.right);
    plan->join_edges.push_back(std::move(edge));
  }
  std::sort(plan->join_edges.begin(), plan->join_edges.end(), JoinEdgeLess);
  plan->join_edges.erase(
      std::unique(plan->join_edges.begin(), plan->join_edges.end(), SameJoinEdge),
      plan->join_edges.end());
  return Status::OK();
}

}  // namespace

StatusOr<LogicalPlan> LowerTcypher(const BoundTcypherStatement& statement) {
  if (statement.syntax.match.variable.empty() || statement.syntax.returns.empty()) {
    return Status::BindError("logical plan", "bound statement has no match or return binding");
  }
  LogicalPlan plan;
  plan.demand = BuildFactDemand(statement);
  const std::vector<BoundTemporalScope>& primary_scopes =
      statement.primary_match_scopes.empty() ? statement.temporal_scopes
                                             : statement.primary_match_scopes;
  LogicalOperatorKind scan_kind = LogicalOperatorKind::kTemporalScan;
  bool state_range = false;
  bool change_scan = false;
  for (const BoundTemporalScope& scope : primary_scopes) {
    switch (scope.mode) {
      case TemporalScopeMode::kStateAsOf:
        break;
      case TemporalScopeMode::kStateBetween:
        state_range = true;
        break;
      case TemporalScopeMode::kChangesBetween:
        change_scan = true;
        break;
    }
  }
  if (change_scan) {
    scan_kind = LogicalOperatorKind::kChangeScan;
  } else if (state_range) {
    scan_kind = LogicalOperatorKind::kEventScan;
  }
  plan.nodes.push_back(LogicalPlanNode{scan_kind, statement.syntax.match.variable, {},
                                      primary_scopes});
  for (const MatchRelationshipPattern& relationship : statement.syntax.relationships) {
    plan.nodes.push_back(LogicalPlanNode{relationship.variable_length
                                            ? LogicalOperatorKind::kVariableExpand
                                            : LogicalOperatorKind::kExpand,
                                        relationship.variable, {}, primary_scopes});
  }
  for (size_t clause_index = 0; clause_index < statement.syntax.additional_matches.size(); ++clause_index) {
    const MatchClause& clause = statement.syntax.additional_matches[clause_index];
    const std::vector<BoundTemporalScope>& clause_scopes =
        clause_index < statement.additional_match_scopes.size() &&
                !statement.additional_match_scopes[clause_index].empty()
            ? statement.additional_match_scopes[clause_index]
            : primary_scopes;
    plan.nodes.push_back(LogicalPlanNode{scan_kind, clause.match.variable, {}, clause_scopes});
    for (const MatchRelationshipPattern& relationship : clause.relationships) {
      plan.nodes.push_back(LogicalPlanNode{relationship.variable_length
                                              ? LogicalOperatorKind::kVariableExpand
                                              : LogicalOperatorKind::kExpand,
                                          relationship.variable, {}, clause_scopes});
    }
  }
  Status join_graph_status = LowerJoinGraph(statement, &plan);
  if (!join_graph_status.ok()) return join_graph_status;
  for (const LogicalJoinEdge& edge : plan.join_edges) {
    plan.nodes.push_back(LogicalPlanNode{LogicalOperatorKind::kJoin,
                                        std::to_string(edge.left.binding_id.value), {},
                                        primary_scopes});
  }
  if (state_range && !change_scan) {
    plan.nodes.push_back(LogicalPlanNode{LogicalOperatorKind::kIntervalDerive,
                                        statement.syntax.match.variable, {},
                                        primary_scopes});
    plan.nodes.push_back(LogicalPlanNode{LogicalOperatorKind::kIntervalAlign,
                                        statement.syntax.match.variable,
                                        plan.demand.property_names, primary_scopes});
    plan.nodes.push_back(LogicalPlanNode{LogicalOperatorKind::kTemporalCoalesce,
                                        statement.syntax.match.variable,
                                        plan.demand.property_names, primary_scopes});
  }
  if (!plan.demand.property_names.empty()) {
    plan.nodes.push_back(LogicalPlanNode{LogicalOperatorKind::kPropertyGather,
                                        statement.syntax.match.variable,
                                        plan.demand.property_names, primary_scopes});
  }
  if (statement.syntax.where.has_value() &&
      !IsDistinctBindingJoinPredicate(*statement.syntax.where)) {
    plan.nodes.push_back(LogicalPlanNode{LogicalOperatorKind::kFilter,
                                        statement.syntax.where->variable,
                                        {statement.syntax.where->property_name},
                                        primary_scopes});
  }
  for (const StringEqualityPredicate& predicate : statement.syntax.and_predicates) {
    if (IsDistinctBindingJoinPredicate(predicate)) continue;
    plan.nodes.push_back(LogicalPlanNode{LogicalOperatorKind::kFilter,
                                        predicate.variable, {predicate.property_name},
                                        primary_scopes});
  }
  if (statement.syntax.distinct) {
    plan.nodes.push_back(LogicalPlanNode{LogicalOperatorKind::kDistinct,
                                        statement.syntax.match.variable,
                                        plan.demand.property_names, primary_scopes});
  }
  if (std::any_of(statement.syntax.returns.begin(), statement.syntax.returns.end(),
                  [](const ReturnExpression& expression) {
                    return expression.kind == ReturnExpressionKind::kCount ||
                           expression.kind == ReturnExpressionKind::kSum ||
                           expression.kind == ReturnExpressionKind::kAvg ||
                           expression.kind == ReturnExpressionKind::kMin ||
                           expression.kind == ReturnExpressionKind::kMax ||
                           expression.kind == ReturnExpressionKind::kCollect;
                  })) {
    plan.nodes.push_back(LogicalPlanNode{LogicalOperatorKind::kAggregate,
                                        statement.syntax.match.variable, {},
                                        primary_scopes});
  }
  if (statement.syntax.order_by.has_value()) {
    plan.nodes.push_back(LogicalPlanNode{LogicalOperatorKind::kSort,
                                        statement.syntax.order_by->variable,
                                        {statement.syntax.order_by->property_name},
                                        primary_scopes});
  }
  plan.nodes.push_back(LogicalPlanNode{LogicalOperatorKind::kProduceResult,
                                      statement.syntax.match.variable,
                                      plan.demand.property_names, primary_scopes});
  return plan;
}

}  // namespace cedar
