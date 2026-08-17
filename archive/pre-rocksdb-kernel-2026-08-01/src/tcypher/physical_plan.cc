// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/tcypher/physical_plan.h"

#include <algorithm>
#include <functional>
#include <limits>
#include <map>
#include <set>
#include <sstream>
#include <tuple>
#include <utility>

namespace cedar {

const char* PhysicalOperatorKindName(PhysicalOperatorKind kind) {
  switch (kind) {
    case PhysicalOperatorKind::kTemporalPointScan: return "TemporalPointScan";
    case PhysicalOperatorKind::kTemporalRangeScan: return "TemporalRangeScan";
    case PhysicalOperatorKind::kChangeScan: return "ChangeScan";
    case PhysicalOperatorKind::kIntervalDerive: return "IntervalDerive";
    case PhysicalOperatorKind::kIntervalAlign: return "IntervalAlign";
    case PhysicalOperatorKind::kTemporalCoalesce: return "TemporalCoalesce";
    case PhysicalOperatorKind::kExpand: return "VectorExpand";
    case PhysicalOperatorKind::kHashJoin: return "HashJoin";
    case PhysicalOperatorKind::kCrossJoin: return "CrossJoin";
    case PhysicalOperatorKind::kDistinct: return "DistinctSink";
    case PhysicalOperatorKind::kAggregate: return "AggregateSink";
    case PhysicalOperatorKind::kSort: return "SortSink";
    case PhysicalOperatorKind::kPropertyGather: return "PropertyGather";
    case PhysicalOperatorKind::kFilter: return "VectorFilter";
    case PhysicalOperatorKind::kMetadataProject: return "MetadataProject";
    case PhysicalOperatorKind::kProject: return "VectorProject";
    case PhysicalOperatorKind::kResultSink: return "ResultSink";
  }
  return "Unknown";
}

namespace {

uint64_t PlanFingerprint(
    BindingId binding, const std::vector<SlotDescriptor>& slots,
    const std::vector<PhysicalOperatorSpec>& operators,
    const std::vector<PipelineDescriptor>& pipelines,
    const std::vector<PhysicalPredicate>& predicates,
    const std::vector<PhysicalExpression>& projections,
    const std::vector<PhysicalPropertySlot>& predicate_properties,
    const std::vector<PhysicalPropertySlot>& projection_properties,
    std::optional<uint64_t> exact_entity_id,
    const std::string& exact_entity_parameter, bool include_valid_to,
    bool include_system_time, TemporalContextId temporal_context,
    PhysicalTemporalMode temporal_mode,
    std::optional<std::pair<uint64_t, uint64_t>> valid_time_range,
    std::optional<std::pair<uint64_t, uint64_t>> system_time_range,
    std::optional<uint64_t> valid_time_as_of,
    const std::optional<PhysicalExpandSpec>& expand,
    const std::vector<PhysicalExpandSpec>& expand_steps = {}) {
  uint64_t hash = 1469598103934665603ULL;
  auto mix = [&](uint64_t value) {
    for (uint32_t index = 0; index < 8; ++index) {
      hash ^= static_cast<uint8_t>(value >> (index * 8));
      hash *= 1099511628211ULL;
    }
  };
  const auto mix_string = [&](const std::string& value) {
    mix(value.size());
    for (unsigned char byte : value) {
      hash ^= byte;
      hash *= 1099511628211ULL;
    }
  };
  const auto mix_value = [&](const Value& value) {
    mix_string(value.Encode());
  };
  const auto mix_column = [&](const ColumnSchema& column) {
    mix(static_cast<uint8_t>(column.entity_type));
    mix(column.column_id);
    mix(column.schema_epoch);
    mix_string(column.logical_type);
    mix(static_cast<uint8_t>(column.physical_type));
    mix(column.blob_threshold);
    mix(static_cast<uint8_t>(column.encoding_policy));
    mix(static_cast<uint8_t>(column.compression_policy));
  };

  mix(binding.value);
  mix(slots.size());
  for (const SlotDescriptor& slot : slots) {
    mix(slot.id.value);
    mix(slot.binding.value);
    mix(static_cast<uint8_t>(slot.type));
    mix(slot.nullable);
  }
  mix(operators.size());
  for (const PhysicalOperatorSpec& op : operators) {
    mix(op.id.value);
    mix(static_cast<uint8_t>(op.kind));
    mix(op.required_slots.size());
    for (SlotId slot : op.required_slots) mix(slot.value);
    mix(op.produced_slots.size());
    for (SlotId slot : op.produced_slots) mix(slot.value);
  }
  mix(pipelines.size());
  for (const PipelineDescriptor& pipeline : pipelines) {
    mix(pipeline.id.value);
    mix(pipeline.operators.size());
    for (OperatorId op : pipeline.operators) mix(op.value);
    mix(pipeline.dependencies.size());
    for (PipelineId dependency : pipeline.dependencies) mix(dependency.value);
  }
  mix(predicates.size());
  for (const PhysicalPredicate& predicate : predicates) {
    mix(predicate.slot.value);
    mix_column(predicate.column);
    mix(static_cast<uint8_t>(predicate.type));
    mix(predicate.nullable);
    mix(static_cast<uint8_t>(predicate.kind));
    mix(predicate.values.size());
    for (const Value& value : predicate.values) mix_value(value);
    mix(predicate.lower_bound.has_value());
    if (predicate.lower_bound.has_value()) mix_value(*predicate.lower_bound);
    mix(predicate.upper_bound.has_value());
    if (predicate.upper_bound.has_value()) mix_value(*predicate.upper_bound);
    mix(predicate.lower_inclusive);
    mix(predicate.upper_inclusive);
  }
  mix(projections.size());
  for (const PhysicalExpression& projection : projections) {
    mix(static_cast<uint8_t>(projection.kind));
    mix(projection.referenced_slot.value);
    mix(projection.output_slot.value);
    mix(static_cast<uint8_t>(projection.type));
    mix(projection.nullable);
    mix_string(projection.output_name);
    mix(projection.relationship_slots.size());
    for (SlotId slot : projection.relationship_slots) mix(slot.value);
    mix(projection.binding_id.value);
    mix(static_cast<uint8_t>(projection.result_kind));
    mix(projection.property_id.has_value());
    if (projection.property_id.has_value()) mix(projection.property_id->value);
    mix(projection.relationship_identity);
  }
  const auto mix_properties = [&](const std::vector<PhysicalPropertySlot>& properties) {
    mix(properties.size());
    for (const PhysicalPropertySlot& property : properties) {
      mix(property.slot.value);
      mix_column(property.column);
    }
  };
  mix_properties(predicate_properties);
  mix_properties(projection_properties);
  mix(exact_entity_id.has_value());
  if (exact_entity_id.has_value()) mix(*exact_entity_id);
  mix_string(exact_entity_parameter);
  mix(include_valid_to);
  mix(include_system_time);
  mix(temporal_context.value);
  mix(static_cast<uint8_t>(temporal_mode));
  mix(valid_time_range.has_value());
  if (valid_time_range.has_value()) {
    mix(valid_time_range->first);
    mix(valid_time_range->second);
  }
  mix(system_time_range.has_value());
  if (system_time_range.has_value()) {
    mix(system_time_range->first);
    mix(system_time_range->second);
  }
  mix(valid_time_as_of.has_value());
  if (valid_time_as_of.has_value()) mix(*valid_time_as_of);
  const auto mix_expand = [&](const PhysicalExpandSpec& spec) {
    mix(spec.source_binding.value);
    mix(spec.relationship_binding.value);
    mix(spec.target_binding.value);
    mix(spec.source_slot.value);
    mix(spec.target_slot.value);
    mix(spec.edge_type_slot.value);
    mix(spec.edge_id_slot.value);
    mix(spec.valid_from_slot.value);
    mix(spec.commit_seq_slot.value);
    mix(spec.operation_slot.value);
    mix(spec.system_time_slot.value);
    mix(spec.valid_to_slot.value);
    mix(spec.path_slot.value);
    mix(static_cast<uint8_t>(spec.direction));
    mix(spec.edge_type.has_value());
    if (spec.edge_type.has_value()) mix(*spec.edge_type);
    mix(spec.min_hops);
    mix(spec.max_hops);
  };
  mix(expand.has_value());
  if (expand.has_value()) mix_expand(*expand);
  mix(expand_steps.size());
  for (const PhysicalExpandSpec& step : expand_steps) mix_expand(step);
  return hash == 0 ? 1 : hash;
}

std::optional<PhysicalAggregateSinkSpec> BuildPhysicalAggregateSink(
    const TcypherStatement& statement, OperatorId operator_id) {
  const auto aggregate_kind = [](ReturnExpressionKind kind)
      -> std::optional<PhysicalAggregateKind> {
    switch (kind) {
      case ReturnExpressionKind::kCount: return PhysicalAggregateKind::kCount;
      case ReturnExpressionKind::kSum: return PhysicalAggregateKind::kSum;
      case ReturnExpressionKind::kAvg: return PhysicalAggregateKind::kAvg;
      case ReturnExpressionKind::kMin: return PhysicalAggregateKind::kMin;
      case ReturnExpressionKind::kMax: return PhysicalAggregateKind::kMax;
      case ReturnExpressionKind::kCollect: return PhysicalAggregateKind::kCollect;
      default: return std::nullopt;
    }
  };
  const auto aggregate_name = [](const ReturnExpression& expression) {
    if (expression.kind == ReturnExpressionKind::kCount) {
      return "count(" + expression.variable + ")";
    }
    if (expression.kind == ReturnExpressionKind::kCollect) {
      return "collect(" + expression.variable +
          (expression.property_name.empty()
               ? std::string{} : "." + expression.property_name) + ")";
    }
    const char* name = expression.kind == ReturnExpressionKind::kSum ? "sum" :
        expression.kind == ReturnExpressionKind::kAvg ? "avg" :
        expression.kind == ReturnExpressionKind::kMin ? "min" : "max";
    return std::string(name) + "(" + expression.variable + "." +
        expression.property_name + ")";
  };

  PhysicalAggregateSinkSpec sink;
  sink.op = PhysicalOperatorSpec{
      operator_id, PhysicalOperatorKind::kAggregate, {}, {}};
  uint32_t group_index = 0;
  uint32_t aggregate_index = 0;
  const size_t collect_count = std::count_if(
      statement.returns.begin(), statement.returns.end(),
      [](const ReturnExpression& expression) {
        return expression.kind == ReturnExpressionKind::kCollect;
      });
  const size_t standard_aggregate_count = std::count_if(
      statement.returns.begin(), statement.returns.end(),
      [](const ReturnExpression& expression) {
        return expression.kind == ReturnExpressionKind::kCount ||
            expression.kind == ReturnExpressionKind::kSum ||
            expression.kind == ReturnExpressionKind::kAvg ||
            expression.kind == ReturnExpressionKind::kMin ||
            expression.kind == ReturnExpressionKind::kMax;
      });
  if (collect_count > 1 || (collect_count != 0 && standard_aggregate_count != 0)) {
    return std::nullopt;
  }
  for (uint32_t column = 0; column < statement.returns.size(); ++column) {
    const ReturnExpression& expression = statement.returns[column];
    const auto kind = aggregate_kind(expression.kind);
    if (kind.has_value()) {
      PhysicalAggregateValueKind input_kind =
          PhysicalAggregateValueKind::kScalar;
      if (expression.kind == ReturnExpressionKind::kCollect &&
          expression.property_name.empty()) {
        const auto relationship = std::find_if(
            statement.relationships.begin(), statement.relationships.end(),
            [&expression](const MatchRelationshipPattern& candidate) {
              return candidate.variable == expression.variable;
            });
        if (relationship != statement.relationships.end()) {
          input_kind = relationship->variable_length
              ? PhysicalAggregateValueKind::kList
              : PhysicalAggregateValueKind::kStruct;
        }
      }
      sink.aggregates.push_back(PhysicalAggregateExpression{
          *kind, column, aggregate_name(expression), input_kind});
      sink.outputs.push_back(PhysicalAggregateOutput{true, aggregate_index++});
    } else {
      sink.group_columns.push_back(column);
      sink.outputs.push_back(PhysicalAggregateOutput{false, group_index++});
    }
  }
  if (sink.aggregates.empty()) return std::nullopt;
  return sink;
}

std::optional<PhysicalSortSinkSpec> BuildPhysicalSortSink(
    const TcypherStatement& statement, OperatorId operator_id) {
  if (!statement.order_by.has_value()) return std::nullopt;
  const OrderByTerm& order = *statement.order_by;
  for (uint32_t column = 0; column < statement.returns.size(); ++column) {
    const ReturnExpression& expression = statement.returns[column];
    if (expression.variable == order.variable &&
        expression.property_name == order.property_name) {
      return PhysicalSortSinkSpec{
          PhysicalOperatorSpec{
              operator_id, PhysicalOperatorKind::kSort, {}, {}},
          column, order.descending};
    }
  }
  return std::nullopt;
}

void MixResultOperatorFingerprint(
    uint64_t* fingerprint,
    const std::optional<PhysicalAggregateSinkSpec>& aggregate_sink,
    bool distinct, const std::optional<PhysicalSortSinkSpec>& sort_sink) {
  const auto mix = [fingerprint](uint64_t value) {
    for (uint32_t index = 0; index < 8; ++index) {
      *fingerprint ^= static_cast<uint8_t>(value >> (index * 8));
      *fingerprint *= 1099511628211ULL;
    }
  };
  const auto mix_string = [&](const std::string& value) {
    mix(value.size());
    for (unsigned char byte : value) {
      *fingerprint ^= byte;
      *fingerprint *= 1099511628211ULL;
    }
  };
  mix(aggregate_sink.has_value());
  if (aggregate_sink.has_value()) {
    mix(aggregate_sink->group_columns.size());
    for (uint32_t column : aggregate_sink->group_columns) mix(column);
    mix(aggregate_sink->aggregates.size());
    for (const PhysicalAggregateExpression& aggregate : aggregate_sink->aggregates) {
      mix(static_cast<uint8_t>(aggregate.kind));
      mix(aggregate.input_column);
      mix_string(aggregate.output_name);
      mix(static_cast<uint8_t>(aggregate.input_kind));
    }
    mix(aggregate_sink->outputs.size());
    for (const PhysicalAggregateOutput& output : aggregate_sink->outputs) {
      mix(output.aggregate);
      mix(output.index);
    }
  }
  mix(distinct);
  mix(sort_sink.has_value());
  if (sort_sink.has_value()) {
    mix(sort_sink->input_column);
    mix(sort_sink->descending);
  }
  if (*fingerprint == 0) *fingerprint = 1;
}

Status ValidateResultOperators(
    const std::vector<PhysicalOperatorSpec>& operators,
    const std::optional<PhysicalAggregateSinkSpec>& aggregate_sink,
    const std::optional<PhysicalSortSinkSpec>& sort_sink,
    uint32_t output_count, std::set<OperatorId>* operator_ids) {
  if (operator_ids == nullptr) {
    return Status::InvalidArgument(
        "physical plan", "missing operator identity set");
  }
  size_t aggregate_count = 0;
  size_t distinct_count = 0;
  size_t sort_count = 0;
  uint8_t previous_rank = 0;
  bool first = true;
  for (const PhysicalOperatorSpec& op : operators) {
    if (op.id.value == 0 || !operator_ids->insert(op.id).second ||
        !op.required_slots.empty() || !op.produced_slots.empty()) {
      return Status::InvalidArgument(
          "physical plan", "post-result operator identity or slots are invalid");
    }
    uint8_t rank = 0;
    switch (op.kind) {
      case PhysicalOperatorKind::kAggregate:
        ++aggregate_count;
        rank = 1;
        break;
      case PhysicalOperatorKind::kDistinct:
        ++distinct_count;
        rank = 2;
        break;
      case PhysicalOperatorKind::kSort:
        ++sort_count;
        rank = 3;
        break;
      default:
        return Status::InvalidArgument(
            "physical plan", "unsupported post-result operator kind");
    }
    if (!first && rank <= previous_rank) {
      return Status::InvalidArgument(
          "physical plan", "post-result operator order is invalid");
    }
    first = false;
    previous_rank = rank;
  }
  if (aggregate_count != (aggregate_sink.has_value() ? 1U : 0U) ||
      distinct_count > 1 || sort_count != (sort_sink.has_value() ? 1U : 0U)) {
    return Status::InvalidArgument(
        "physical plan", "post-result sink specification is incomplete");
  }
  if (aggregate_sink.has_value()) {
    if (aggregate_sink->op.kind != PhysicalOperatorKind::kAggregate ||
        std::find(operators.begin(), operators.end(), aggregate_sink->op) ==
            operators.end() ||
        aggregate_sink->aggregates.empty() ||
        aggregate_sink->outputs.size() != output_count) {
      return Status::InvalidArgument(
          "physical plan", "aggregate sink shape is invalid");
    }
    for (uint32_t column : aggregate_sink->group_columns) {
      if (column >= output_count) {
        return Status::InvalidArgument(
            "physical plan", "aggregate grouping column is invalid");
      }
    }
    for (const PhysicalAggregateExpression& aggregate :
         aggregate_sink->aggregates) {
      if (aggregate.input_column >= output_count || aggregate.output_name.empty() ||
          static_cast<uint8_t>(aggregate.input_kind) >
              static_cast<uint8_t>(PhysicalAggregateValueKind::kList)) {
        return Status::InvalidArgument(
            "physical plan", "aggregate input column is invalid");
      }
    }
    for (const PhysicalAggregateOutput& output : aggregate_sink->outputs) {
      const size_t size = output.aggregate ? aggregate_sink->aggregates.size()
                                           : aggregate_sink->group_columns.size();
      if (output.index >= size) {
        return Status::InvalidArgument(
            "physical plan", "aggregate output mapping is invalid");
      }
    }
  }
  if (sort_sink.has_value() &&
      (sort_sink->op.kind != PhysicalOperatorKind::kSort ||
       std::find(operators.begin(), operators.end(), sort_sink->op) ==
           operators.end() ||
       sort_sink->input_column >= output_count)) {
    return Status::InvalidArgument(
        "physical plan", "sort sink shape is invalid");
  }
  return Status::OK();
}

}  // namespace

bool CanPlanPhysicalRootPoint(const BoundTcypherStatement& statement) {
  return statement.root_point_candidate || statement.root_temporal_candidate ||
      statement.fixed_expand_point_candidate ||
      statement.fixed_expand_range_candidate ||
      statement.fixed_expand_change_candidate;
}

bool CanPlanPhysicalHashJoin(const BoundTcypherStatement& statement) {
  if (statement.joins.empty()) return false;
  const std::string& left_variable = statement.joins.front().left.variable;
  const std::string& right_variable = statement.joins.front().right.variable;
  if (left_variable == right_variable) return false;
  const auto input_type = [](const BoundJoinInput& input)
      -> std::optional<PhysicalType> {
    if (input.identity) return PhysicalType::kInt64;
    return input.column.has_value()
        ? std::optional<PhysicalType>(input.column->physical_type)
        : std::nullopt;
  };
  const bool valid_equalities = std::all_of(
      statement.joins.begin(), statement.joins.end(),
      [&](const BoundJoinEquality& equality) {
        const BoundJoinInput* left = nullptr;
        const BoundJoinInput* right = nullptr;
        if (equality.left.variable == left_variable &&
            equality.right.variable == right_variable) {
          left = &equality.left;
          right = &equality.right;
        } else if (equality.left.variable == right_variable &&
                   equality.right.variable == left_variable) {
          left = &equality.right;
          right = &equality.left;
        } else {
          return false;
        }
        const auto left_type = input_type(*left);
        const auto right_type = input_type(*right);
        return left_type.has_value() && right_type.has_value() &&
            *left_type == *right_type;
      });
  return statement.syntax.relationships.empty() &&
      statement.syntax.additional_matches.size() == 1 &&
      statement.syntax.additional_matches.front().relationships.empty() &&
      statement.variables.size() == 2 && valid_equalities &&
      statement.predicates.empty() &&
      !statement.syntax.order_by.has_value() &&
      statement.additional_match_scopes.size() == 1 &&
      statement.additional_match_scopes.front().empty() &&
      !statement.projections.empty() &&
      std::all_of(statement.projections.begin(), statement.projections.end(),
                  [](const BoundProjectionExpression& expression) {
                    return expression.kind == ReturnExpressionKind::kBinding ||
                        expression.kind == ReturnExpressionKind::kProperty ||
                        expression.kind == ReturnExpressionKind::kValidFrom ||
                        expression.kind == ReturnExpressionKind::kValidTo ||
                        expression.kind == ReturnExpressionKind::kCommitSeq ||
                        expression.kind == ReturnExpressionKind::kOperation ||
                        expression.kind == ReturnExpressionKind::kSystemTime;
                  });
}

bool CanPlanPhysicalMultiHashJoin(const BoundTcypherStatement& statement) {
  const size_t input_count = statement.syntax.additional_matches.size() + 1;
  if (input_count < 2 || statement.variables.size() < input_count ||
      statement.additional_match_scopes.size() !=
          statement.syntax.additional_matches.size() ||
      statement.projections.empty()) {
    return false;
  }
  const bool has_expand_input = !statement.syntax.relationships.empty() ||
      std::any_of(
          statement.syntax.additional_matches.begin(),
          statement.syntax.additional_matches.end(),
          [](const MatchClause& clause) {
            return !clause.relationships.empty();
          });
  if (input_count == 2 && !has_expand_input && !statement.joins.empty() &&
      statement.predicates.empty()) {
    return false;
  }
  const auto point_scopes = [](const std::vector<BoundTemporalScope>& scopes) {
    return std::all_of(
        scopes.begin(), scopes.end(), [](const BoundTemporalScope& scope) {
          return scope.mode == TemporalScopeMode::kStateAsOf;
        });
  };
  const auto apply_scope_overrides = [](
      std::vector<BoundTemporalScope> inherited,
      const std::vector<BoundTemporalScope>& overrides) {
    for (const BoundTemporalScope& scope : overrides) {
      inherited.erase(
          std::remove_if(
              inherited.begin(), inherited.end(),
              [&](const BoundTemporalScope& candidate) {
                return candidate.axis == scope.axis;
              }),
          inherited.end());
      inherited.push_back(scope);
    }
    return inherited;
  };
  const std::vector<BoundTemporalScope> primary_scopes =
      apply_scope_overrides(statement.temporal_scopes,
                            statement.primary_match_scopes);
  if (!point_scopes(primary_scopes)) return false;
  for (const std::vector<BoundTemporalScope>& match_scopes :
       statement.additional_match_scopes) {
    if (!point_scopes(match_scopes.empty() ? primary_scopes : match_scopes)) {
      return false;
    }
  }
  const auto supported_clause = [](const MatchClause& clause) {
    return clause.relationships.size() == clause.expanded_nodes.size() &&
        std::all_of(
            clause.relationships.begin(), clause.relationships.end(),
            [](const MatchRelationshipPattern& relationship) {
              return !relationship.variable.empty() &&
                  !relationship.variable_length;
            });
  };
  const MatchClause primary_clause{
      statement.syntax.match, statement.syntax.relationships,
      statement.syntax.expanded_nodes, statement.syntax.match_temporal_scopes};
  if (!supported_clause(primary_clause) ||
      std::any_of(statement.syntax.additional_matches.begin(),
                  statement.syntax.additional_matches.end(),
                  [&](const MatchClause& clause) {
                    return !supported_clause(clause);
                  })) {
    return false;
  }
  if (!std::all_of(
          statement.projections.begin(), statement.projections.end(),
          [](const BoundProjectionExpression& expression) {
            return expression.kind == ReturnExpressionKind::kBinding ||
                expression.kind == ReturnExpressionKind::kProperty ||
                expression.kind == ReturnExpressionKind::kValidFrom ||
                expression.kind == ReturnExpressionKind::kValidTo ||
                expression.kind == ReturnExpressionKind::kCommitSeq ||
                expression.kind == ReturnExpressionKind::kOperation ||
                expression.kind == ReturnExpressionKind::kSystemTime;
          })) {
    return false;
  }

  std::map<std::string, const BoundVariable*> variables;
  for (const BoundVariable& variable : statement.variables) {
    if (variable.binding_id.value == 0 ||
        !variables.emplace(variable.variable, &variable).second) {
      return false;
    }
  }
  std::map<BindingId, uint32_t> input_by_binding;
  const auto assign_clause = [&](const MatchClause& clause, uint32_t input) {
    std::vector<std::string> names{clause.match.variable};
    for (const MatchRelationshipPattern& relationship : clause.relationships) {
      names.push_back(relationship.variable);
    }
    for (const MatchNodePattern& node : clause.expanded_nodes) {
      names.push_back(node.variable);
    }
    for (const std::string& name : names) {
      const auto variable = variables.find(name);
      if (variable == variables.end() ||
          !input_by_binding.emplace(variable->second->binding_id, input).second) {
        return false;
      }
    }
    return true;
  };
  if (!assign_clause(primary_clause, 0)) return false;
  for (uint32_t index = 0;
       index < statement.syntax.additional_matches.size(); ++index) {
    if (!assign_clause(statement.syntax.additional_matches[index], index + 1)) {
      return false;
    }
  }
  if (input_by_binding.size() != statement.variables.size()) return false;
  for (const BoundPredicateExpression& predicate : statement.predicates) {
    if (input_by_binding.count(predicate.binding_id) == 0) return false;
    const auto property = std::find_if(
        statement.properties.begin(), statement.properties.end(),
        [&](const BoundPropertyReference& candidate) {
          return candidate.property_id == predicate.property_id &&
              candidate.binding_id == predicate.binding_id &&
              candidate.predicate &&
              candidate.column.physical_type ==
                  predicate.column.physical_type;
        });
    if (property == statement.properties.end()) return false;
  }
  std::vector<std::set<uint32_t>> adjacency(input_count);
  const auto input_type = [](const BoundJoinInput& input)
      -> std::optional<PhysicalType> {
    if (input.identity) return PhysicalType::kInt64;
    return input.column.has_value()
        ? std::optional<PhysicalType>(input.column->physical_type)
        : std::nullopt;
  };
  for (const BoundJoinEquality& equality : statement.joins) {
    const auto left = variables.find(equality.left.variable);
    const auto right = variables.find(equality.right.variable);
    const auto left_type = input_type(equality.left);
    const auto right_type = input_type(equality.right);
    if (left == variables.end() || right == variables.end() ||
        left->second->binding_id == right->second->binding_id ||
        !left_type.has_value() || !right_type.has_value() ||
        *left_type != *right_type) {
      return false;
    }
    const uint32_t left_input = input_by_binding.at(left->second->binding_id);
    const uint32_t right_input = input_by_binding.at(right->second->binding_id);
    if (left_input == right_input) return false;
    adjacency[left_input].insert(right_input);
    adjacency[right_input].insert(left_input);
  }

  return true;
}

StatusOr<std::shared_ptr<const PhysicalPlan>> PlanPhysicalRootPoint(
    const BoundTcypherStatement& statement, const LogicalPlan& logical_plan,
    const TcypherStatement* result_statement) {
  if (!CanPlanPhysicalRootPoint(statement)) {
    return Status::InvalidArgument("physical planner", "statement is not a supported root temporal query");
  }
  const auto annotate_projections = [&](std::vector<PhysicalExpression>* projections) {
    if (projections == nullptr ||
        projections->size() != statement.projections.size()) {
      return Status::Corruption(
          "physical planner", "physical projection metadata is incomplete");
    }
    for (size_t index = 0; index < projections->size(); ++index) {
      PhysicalExpression& physical = (*projections)[index];
      const BoundProjectionExpression& bound = statement.projections[index];
      physical.binding_id = bound.binding_id;
      physical.result_kind = bound.kind;
      physical.property_id = bound.property_id;
      physical.relationship_identity = bound.relationship_identity;
    }
    return Status::OK();
  };
  if ((statement.fixed_expand_point_candidate || statement.fixed_expand_range_candidate) &&
      statement.syntax.relationships.size() > 1) {
    const bool range_expand = statement.fixed_expand_range_candidate;
    const auto find_variable = [&statement](const std::string& name)
        -> const BoundVariable* {
      const auto found = std::find_if(statement.variables.begin(), statement.variables.end(),
          [&name](const BoundVariable& variable) { return variable.variable == name; });
      return found == statement.variables.end() ? nullptr : &*found;
    };
    std::vector<const BoundVariable*> nodes;
    std::vector<const BoundVariable*> relationships;
    nodes.reserve(statement.syntax.expanded_nodes.size() + 1);
    relationships.reserve(statement.syntax.relationships.size());
    const BoundVariable* source = find_variable(statement.syntax.match.variable);
    if (source == nullptr || source->kind != BoundGraphKind::kNode) {
      return Status::InvalidArgument("physical planner", "multi-hop source binding is invalid");
    }
    nodes.push_back(source);
    for (size_t index = 0; index < statement.syntax.relationships.size(); ++index) {
      const BoundVariable* relationship =
          find_variable(statement.syntax.relationships[index].variable);
      const BoundVariable* target =
          find_variable(statement.syntax.expanded_nodes[index].variable);
      if (relationship == nullptr || target == nullptr ||
          relationship->kind != BoundGraphKind::kRelationship ||
          target->kind != BoundGraphKind::kNode ||
          relationship->binding_id.value == 0 || target->binding_id.value == 0) {
        return Status::InvalidArgument("physical planner", "multi-hop binding is invalid");
      }
      relationships.push_back(relationship);
      nodes.push_back(target);
    }
    uint32_t next_slot = 1;
    std::vector<SlotDescriptor> slots;
    const auto add_slot = [&](BindingId binding, PhysicalType type, bool nullable) {
      const SlotId id{next_slot++};
      slots.push_back(SlotDescriptor{id, binding, type, nullable});
      return id;
    };
    std::map<BindingId, SlotId> node_slots;
    for (const BoundVariable* node : nodes) {
      node_slots.emplace(node->binding_id,
                         add_slot(node->binding_id, PhysicalType::kInt64, false));
    }
    std::vector<PhysicalExpandSpec> expand_steps;
    std::map<BindingId, size_t> relationship_steps;
    expand_steps.reserve(relationships.size());
    for (size_t index = 0; index < relationships.size(); ++index) {
      const MatchRelationshipPattern& syntax_relationship =
          statement.syntax.relationships[index];
      const EntityType direction = syntax_relationship.direction == RelationshipDirection::kOutgoing
          ? EntityType::EdgeOut : EntityType::EdgeIn;
      if (relationships[index]->entity_type != direction) {
        return Status::InvalidArgument("physical planner", "multi-hop direction differs from binding");
      }
      std::optional<uint16_t> edge_type;
      if (!syntax_relationship.type.empty()) {
        if (!relationships[index]->entity_schema.has_value()) {
          return Status::InvalidArgument("physical planner", "multi-hop relationship schema is missing");
        }
        edge_type = relationships[index]->entity_schema->column_id;
      }
      const SlotId path_slot = syntax_relationship.variable_length
          ? add_slot(relationships[index]->binding_id, PhysicalType::kBinary,
                     false)
          : SlotId{};
      const PhysicalExpandSpec step{
          nodes[index]->binding_id, relationships[index]->binding_id,
          nodes[index + 1]->binding_id, node_slots.at(nodes[index]->binding_id),
          node_slots.at(nodes[index + 1]->binding_id),
          add_slot(relationships[index]->binding_id, PhysicalType::kInt32, false),
          add_slot(relationships[index]->binding_id, PhysicalType::kInt64, false),
          add_slot(relationships[index]->binding_id, PhysicalType::kTimestamp64, false),
          add_slot(relationships[index]->binding_id, PhysicalType::kInt64, false),
          add_slot(relationships[index]->binding_id, PhysicalType::kInt32, false),
          add_slot(relationships[index]->binding_id, PhysicalType::kTimestamp64, false),
          add_slot(relationships[index]->binding_id, PhysicalType::kTimestamp64, false),
          direction, edge_type, syntax_relationship.min_hops,
          syntax_relationship.max_hops, path_slot};
      relationship_steps.emplace(relationships[index]->binding_id, expand_steps.size());
      expand_steps.push_back(step);
    }
    std::map<BoundPropertyId, SlotId> predicate_property_slots;
    std::map<BoundPropertyId, SlotId> projection_property_slots;
    std::vector<PhysicalPropertySlot> predicate_properties;
    std::vector<PhysicalPropertySlot> projection_properties;
    for (const BoundPropertyReference& property : statement.properties) {
      const bool known_node = node_slots.count(property.binding_id) != 0;
      const bool known_relationship = relationship_steps.count(property.binding_id) != 0;
      if ((!property.projection && !property.predicate) || property.grouping ||
          property.ordering || property.join ||
          (!known_node && !known_relationship)) {
        return Status::InvalidArgument(
            "physical planner", "multi-hop property demand is unsupported");
      }
      if (property.predicate) {
        const SlotId slot = add_slot(
            property.binding_id, property.column.physical_type,
            property.nullable);
        predicate_property_slots.emplace(property.property_id, slot);
        predicate_properties.push_back(
            PhysicalPropertySlot{slot, property.column, property.binding_id});
      }
      if (property.projection) {
        const SlotId slot = add_slot(
            property.binding_id, property.column.physical_type,
            property.nullable);
        projection_property_slots.emplace(property.property_id, slot);
        projection_properties.push_back(
            PhysicalPropertySlot{slot, property.column, property.binding_id});
      }
    }
    std::vector<PhysicalPredicate> predicates;
    for (const BoundPredicateExpression& predicate : statement.predicates) {
      const auto slot = predicate_property_slots.find(predicate.property_id);
      if (slot == predicate_property_slots.end()) {
        return Status::InvalidArgument("physical planner", "multi-hop predicate slot is missing");
      }
      predicates.push_back(PhysicalPredicate{
          slot->second, predicate.column, predicate.column.physical_type, predicate.nullable,
          static_cast<PhysicalPredicateKind>(predicate.kind), predicate.values,
          predicate.lower_bound, predicate.upper_bound, predicate.lower_inclusive,
          predicate.upper_inclusive});
    }
    std::vector<PhysicalExpression> projections;
    for (const BoundProjectionExpression& expression : statement.projections) {
      if (expression.property_id.has_value()) {
        const auto property =
            projection_property_slots.find(*expression.property_id);
        if (expression.kind != ReturnExpressionKind::kProperty ||
            property == projection_property_slots.end()) {
          return Status::InvalidArgument(
              "physical planner", "multi-hop property projection is invalid");
        }
        projections.push_back(PhysicalExpression{
            PhysicalExpressionKind::kSlot, property->second,
            add_slot(expression.binding_id, expression.type, expression.nullable),
            expression.type, expression.nullable, expression.output_name, {}});
        continue;
      }
      const auto node = node_slots.find(expression.binding_id);
      if (node != node_slots.end()) {
        if (expression.kind != ReturnExpressionKind::kBinding) {
          return Status::InvalidArgument("physical planner", "multi-hop node projection is unsupported");
        }
        projections.push_back(PhysicalExpression{PhysicalExpressionKind::kSlot, node->second,
                                                  add_slot(expression.binding_id, expression.type,
                                                           expression.nullable),
                                                  expression.type, expression.nullable,
                                                  expression.output_name, {}});
        continue;
      }
      const auto relationship = relationship_steps.find(expression.binding_id);
      if (relationship == relationship_steps.end()) {
        return Status::InvalidArgument("physical planner", "multi-hop relationship projection is unsupported");
      }
      const PhysicalExpandSpec& step = expand_steps[relationship->second];
      if (expression.kind == ReturnExpressionKind::kValidFrom ||
          expression.kind == ReturnExpressionKind::kValidTo ||
          expression.kind == ReturnExpressionKind::kCommitSeq ||
          expression.kind == ReturnExpressionKind::kOperation ||
          expression.kind == ReturnExpressionKind::kSystemTime) {
        const SlotId slot = expression.kind == ReturnExpressionKind::kValidFrom
            ? step.valid_from_slot : expression.kind == ReturnExpressionKind::kValidTo
                ? step.valid_to_slot : expression.kind == ReturnExpressionKind::kCommitSeq
                    ? step.commit_seq_slot : expression.kind == ReturnExpressionKind::kOperation
                        ? step.operation_slot : step.system_time_slot;
        projections.push_back(PhysicalExpression{
            expression.kind == ReturnExpressionKind::kOperation
                ? PhysicalExpressionKind::kOperationName : PhysicalExpressionKind::kSlot,
            slot, add_slot(expression.binding_id, expression.type, expression.nullable),
            expression.type, expression.nullable, expression.output_name, {}});
        continue;
      }
      if (expression.kind != ReturnExpressionKind::kBinding) {
        return Status::InvalidArgument("physical planner", "multi-hop relationship projection is unsupported");
      }
      if (expression.relationship_identity) {
        projections.push_back(PhysicalExpression{
            PhysicalExpressionKind::kSlot, step.edge_id_slot,
            add_slot(expression.binding_id, PhysicalType::kInt64, false),
            PhysicalType::kInt64, false, expression.output_name, {}});
        continue;
      }
      if (step.path_slot.value != 0) {
        projections.push_back(PhysicalExpression{
            PhysicalExpressionKind::kPathBinding, step.path_slot,
            add_slot(expression.binding_id, PhysicalType::kBinary, false),
            PhysicalType::kBinary, false, expression.output_name, {}});
        continue;
      }
      projections.push_back(PhysicalExpression{
          PhysicalExpressionKind::kRelationshipBinding, step.edge_id_slot,
          add_slot(expression.binding_id, PhysicalType::kBinary, false),
          PhysicalType::kBinary, false, expression.output_name,
          {step.source_slot, step.target_slot, step.edge_type_slot, step.edge_id_slot,
           step.valid_from_slot, step.commit_seq_slot}});
    }
    uint32_t next_operator = 1;
    std::vector<PhysicalOperatorSpec> operators;
    operators.push_back(PhysicalOperatorSpec{OperatorId{next_operator++},
        range_expand ? PhysicalOperatorKind::kTemporalRangeScan
                     : PhysicalOperatorKind::kTemporalPointScan,
        {}, {node_slots.at(source->binding_id)}});
    if (range_expand) {
      operators.push_back(PhysicalOperatorSpec{OperatorId{next_operator++},
          PhysicalOperatorKind::kIntervalDerive, {node_slots.at(source->binding_id)}, {}});
      operators.push_back(PhysicalOperatorSpec{OperatorId{next_operator++},
          PhysicalOperatorKind::kIntervalAlign, {node_slots.at(source->binding_id)}, {}});
      operators.push_back(PhysicalOperatorSpec{OperatorId{next_operator++},
          PhysicalOperatorKind::kTemporalCoalesce, {node_slots.at(source->binding_id)}, {}});
    }
    for (const PhysicalExpandSpec& step : expand_steps) {
      std::vector<SlotId> produced{
          step.target_slot, step.edge_type_slot, step.edge_id_slot,
          step.valid_from_slot, step.commit_seq_slot, step.operation_slot,
          step.system_time_slot, step.valid_to_slot};
      if (step.path_slot.value != 0) produced.push_back(step.path_slot);
      operators.push_back(PhysicalOperatorSpec{OperatorId{next_operator++},
          PhysicalOperatorKind::kExpand, {step.source_slot},
          std::move(produced)});
    }
    if (!predicate_properties.empty() || !projection_properties.empty()) {
      std::vector<SlotId> produced;
      produced.reserve(predicate_properties.size() + projection_properties.size());
      for (const PhysicalPropertySlot& property : predicate_properties) {
        produced.push_back(property.slot);
      }
      for (const PhysicalPropertySlot& property : projection_properties) {
        produced.push_back(property.slot);
      }
      operators.push_back(PhysicalOperatorSpec{OperatorId{next_operator++},
          PhysicalOperatorKind::kPropertyGather, {node_slots.at(source->binding_id)},
          produced});
      if (!predicate_properties.empty()) {
        std::vector<SlotId> required;
        required.reserve(predicate_properties.size());
        for (const PhysicalPropertySlot& property : predicate_properties) {
          required.push_back(property.slot);
        }
        operators.push_back(PhysicalOperatorSpec{OperatorId{next_operator++},
            PhysicalOperatorKind::kFilter, std::move(required), {}});
      }
    }
    std::vector<SlotId> projection_inputs;
    std::vector<SlotId> projection_outputs;
    for (const PhysicalExpression& projection : projections) {
      projection_inputs.push_back(projection.referenced_slot);
      projection_outputs.push_back(projection.output_slot);
    }
    std::sort(projection_inputs.begin(), projection_inputs.end());
    projection_inputs.erase(std::unique(projection_inputs.begin(), projection_inputs.end()),
                            projection_inputs.end());
    operators.push_back(PhysicalOperatorSpec{OperatorId{next_operator++},
        PhysicalOperatorKind::kProject, projection_inputs, projection_outputs});
    operators.push_back(PhysicalOperatorSpec{OperatorId{next_operator++},
        PhysicalOperatorKind::kResultSink, projection_outputs, {}});
    PipelineDescriptor pipeline{PipelineId{1}, {}, {}};
    for (const PhysicalOperatorSpec& op : operators) pipeline.operators.push_back(op.id);
    const bool include_system_time = std::any_of(
        statement.projections.begin(), statement.projections.end(),
        [](const BoundProjectionExpression& expression) {
          return expression.kind == ReturnExpressionKind::kSystemTime;
        });
    const bool include_valid_to = std::any_of(
        statement.projections.begin(), statement.projections.end(),
        [](const BoundProjectionExpression& expression) {
          return expression.kind == ReturnExpressionKind::kValidTo;
        });
    std::optional<std::pair<uint64_t, uint64_t>> range;
    if (range_expand) {
      const std::vector<BoundTemporalScope>& scopes =
          statement.primary_match_scopes.empty() ? statement.temporal_scopes
                                                 : statement.primary_match_scopes;
      if (scopes.empty() || scopes.front().axis != TemporalAxis::kValidTime ||
          scopes.front().mode != TemporalScopeMode::kStateBetween) {
        return Status::InvalidArgument("physical planner", "multi-hop range scope is invalid");
      }
      range = std::make_pair(scopes.front().valid_time_start, scopes.front().valid_time_end);
    }
    const PhysicalTemporalMode temporal_mode = range_expand
        ? PhysicalTemporalMode::kValidTimeRange : PhysicalTemporalMode::kPoint;
    const Status annotated = annotate_projections(&projections);
    if (!annotated.ok()) return annotated;
    uint64_t fingerprint = PlanFingerprint(
        source->binding_id, slots, operators, {pipeline}, predicates, projections,
        predicate_properties,
        projection_properties,
        std::nullopt, {}, include_valid_to, include_system_time,
        TemporalContextId{1}, temporal_mode,
        range, std::nullopt, std::nullopt, expand_steps.front(), expand_steps);
    const TcypherStatement& result =
        result_statement == nullptr ? statement.syntax : *result_statement;
    std::optional<PhysicalAggregateSinkSpec> aggregate_sink =
        BuildPhysicalAggregateSink(result, OperatorId{next_operator});
    std::optional<PhysicalSortSinkSpec> sort_sink;
    std::vector<PhysicalOperatorSpec> post_result_operators;
    if (aggregate_sink.has_value()) {
      post_result_operators.push_back(aggregate_sink->op);
      ++next_operator;
    }
    if (result.distinct) {
      post_result_operators.push_back(PhysicalOperatorSpec{
          OperatorId{next_operator++}, PhysicalOperatorKind::kDistinct, {}, {}});
    }
    sort_sink = BuildPhysicalSortSink(result, OperatorId{next_operator});
    if (sort_sink.has_value()) {
      post_result_operators.push_back(sort_sink->op);
    }
    MixResultOperatorFingerprint(
        &fingerprint, aggregate_sink, result.distinct, sort_sink);
    auto plan = std::make_shared<const PhysicalPlan>(
        fingerprint, source->binding_id, std::move(slots), std::move(operators),
        std::vector<PipelineDescriptor>{std::move(pipeline)}, std::move(predicates),
        std::move(projections), std::move(predicate_properties),
        std::move(projection_properties), std::nullopt, std::string{}, include_valid_to,
        include_system_time,
        TemporalContextId{1}, temporal_mode, range, std::nullopt,
        std::nullopt, expand_steps.front(), std::move(expand_steps),
        std::move(aggregate_sink), std::move(sort_sink),
        std::move(post_result_operators));
    const Status valid = ValidatePhysicalPlan(*plan);
    if (!valid.ok()) return valid;
    return plan;
  }
  if (statement.fixed_expand_point_candidate ||
      statement.fixed_expand_range_candidate ||
      statement.fixed_expand_change_candidate) {
    const bool range_expand = statement.fixed_expand_range_candidate;
    const bool change_expand = statement.fixed_expand_change_candidate;
    if (logical_plan.demand.variables.size() != 3 ||
        statement.syntax.relationships.size() != 1 ||
        statement.syntax.expanded_nodes.size() != 1) {
      return Status::InvalidArgument("physical planner", "fixed expand demand is incomplete");
    }
    const auto find_variable = [&statement](const std::string& name)
        -> const BoundVariable* {
      const auto found = std::find_if(statement.variables.begin(), statement.variables.end(),
          [&name](const BoundVariable& variable) { return variable.variable == name; });
      return found == statement.variables.end() ? nullptr : &*found;
    };
    const BoundVariable* source = find_variable(statement.syntax.match.variable);
    const BoundVariable* relationship =
        find_variable(statement.syntax.relationships.front().variable);
    const BoundVariable* target =
        find_variable(statement.syntax.expanded_nodes.front().variable);
    if (source == nullptr || relationship == nullptr || target == nullptr ||
        source->kind != BoundGraphKind::kNode ||
        relationship->kind != BoundGraphKind::kRelationship ||
        target->kind != BoundGraphKind::kNode || source->binding_id.value == 0 ||
        relationship->binding_id.value == 0 || target->binding_id.value == 0) {
      return Status::InvalidArgument("physical planner", "fixed expand bindings are invalid");
    }
    const MatchRelationshipPattern& syntax_relationship =
        statement.syntax.relationships.front();
    const EntityType direction = syntax_relationship.direction == RelationshipDirection::kOutgoing
        ? EntityType::EdgeOut : EntityType::EdgeIn;
    if (relationship->entity_type != direction) {
      return Status::InvalidArgument("physical planner", "fixed expand direction differs from binding");
    }
    std::optional<uint16_t> edge_type;
    if (!syntax_relationship.type.empty()) {
      if (!relationship->entity_schema.has_value()) {
        return Status::InvalidArgument("physical planner", "fixed expand relationship schema is missing");
      }
      edge_type = relationship->entity_schema->column_id;
    }
    uint32_t next_slot = 1;
    std::vector<SlotDescriptor> slots;
    const auto add_slot = [&](BindingId binding, PhysicalType type, bool nullable) {
      const SlotId id{next_slot++};
      slots.push_back(SlotDescriptor{id, binding, type, nullable});
      return id;
    };
    const SlotId source_slot = add_slot(source->binding_id, PhysicalType::kInt64, false);
    const SlotId target_slot = add_slot(target->binding_id, PhysicalType::kInt64, false);
    const SlotId edge_type_slot =
        add_slot(relationship->binding_id, PhysicalType::kInt32, false);
    const SlotId edge_id_slot =
        add_slot(relationship->binding_id, PhysicalType::kInt64, false);
    const SlotId valid_from_slot =
        add_slot(relationship->binding_id, PhysicalType::kTimestamp64, false);
    const SlotId commit_seq_slot =
        add_slot(relationship->binding_id, PhysicalType::kInt64, false);
    const SlotId operation_slot =
        add_slot(relationship->binding_id, PhysicalType::kInt32, false);
    const SlotId system_time_slot =
        add_slot(relationship->binding_id, PhysicalType::kTimestamp64, false);
    const SlotId valid_to_slot =
        add_slot(relationship->binding_id, PhysicalType::kTimestamp64, false);
    const SlotId path_slot = syntax_relationship.variable_length
        ? add_slot(relationship->binding_id, PhysicalType::kBinary, false)
        : SlotId{};
    std::map<BindingId, SlotId> node_slots{{source->binding_id, source_slot},
                                            {target->binding_id, target_slot}};
    std::map<BoundPropertyId, SlotId> predicate_property_slots;
    std::map<BoundPropertyId, SlotId> projection_property_slots;
    std::vector<PhysicalPropertySlot> predicate_properties;
    std::vector<PhysicalPropertySlot> projection_properties;
    for (const BoundPropertyReference& property : statement.properties) {
      if (property.binding_id != relationship->binding_id &&
          property.binding_id != target->binding_id &&
          property.binding_id != source->binding_id) {
        continue;
      }
      if ((!property.projection && !property.predicate) || property.grouping ||
          property.ordering || property.join) {
        return Status::InvalidArgument(
            "physical planner", "fixed expand property demand is unsupported");
      }
      if (property.predicate) {
        const SlotId slot = add_slot(
            property.binding_id, property.column.physical_type,
            property.nullable);
        predicate_property_slots.emplace(property.property_id, slot);
        predicate_properties.push_back(
            PhysicalPropertySlot{slot, property.column, property.binding_id});
      }
      if (property.projection) {
        const SlotId slot = add_slot(
            property.binding_id, property.column.physical_type,
            property.nullable);
        projection_property_slots.emplace(property.property_id, slot);
        projection_properties.push_back(
            PhysicalPropertySlot{slot, property.column, property.binding_id});
      }
    }
    std::vector<PhysicalPredicate> predicates;
    for (const BoundPredicateExpression& predicate : statement.predicates) {
      const auto slot = predicate_property_slots.find(predicate.property_id);
      if (slot == predicate_property_slots.end()) {
        return Status::InvalidArgument("physical planner", "fixed expand predicate slot is missing");
      }
      predicates.push_back(PhysicalPredicate{
          slot->second, predicate.column, predicate.column.physical_type, predicate.nullable,
          static_cast<PhysicalPredicateKind>(predicate.kind), predicate.values,
          predicate.lower_bound, predicate.upper_bound,
          predicate.lower_inclusive, predicate.upper_inclusive});
    }
    std::vector<PhysicalExpression> projections;
    for (const BoundProjectionExpression& expression : statement.projections) {
      const auto node = node_slots.find(expression.binding_id);
      if (expression.property_id.has_value()) {
        const auto property =
            projection_property_slots.find(*expression.property_id);
        if (expression.kind != ReturnExpressionKind::kProperty ||
            property == projection_property_slots.end()) {
          return Status::InvalidArgument(
              "physical planner", "fixed expand property projection is invalid");
        }
        projections.push_back(PhysicalExpression{PhysicalExpressionKind::kSlot, property->second,
                                                  add_slot(expression.binding_id, expression.type,
                                                           expression.nullable),
                                                  expression.type, expression.nullable,
                                                  expression.output_name, {}});
        continue;
      }
      if (node != node_slots.end()) {
        if (change_expand &&
            (expression.kind == ReturnExpressionKind::kValidFrom ||
             expression.kind == ReturnExpressionKind::kCommitSeq ||
             expression.kind == ReturnExpressionKind::kOperation ||
             expression.kind == ReturnExpressionKind::kSystemTime)) {
          const SlotId temporal_slot =
              expression.kind == ReturnExpressionKind::kValidFrom
                  ? valid_from_slot
                  : expression.kind == ReturnExpressionKind::kCommitSeq
                      ? commit_seq_slot
                      : expression.kind == ReturnExpressionKind::kOperation
                          ? operation_slot
                          : system_time_slot;
          projections.push_back(PhysicalExpression{
              expression.kind == ReturnExpressionKind::kOperation
                  ? PhysicalExpressionKind::kOperationName
                  : PhysicalExpressionKind::kSlot,
              temporal_slot,
              add_slot(expression.binding_id, expression.type,
                       expression.nullable),
              expression.type, expression.nullable,
              expression.output_name, {}});
          continue;
        }
        if (range_expand && (expression.kind == ReturnExpressionKind::kValidFrom ||
                             expression.kind == ReturnExpressionKind::kValidTo)) {
          const SlotId temporal_slot = expression.kind == ReturnExpressionKind::kValidFrom
              ? valid_from_slot : valid_to_slot;
          projections.push_back(PhysicalExpression{PhysicalExpressionKind::kSlot, temporal_slot,
                                                    add_slot(expression.binding_id, expression.type,
                                                             expression.nullable),
                                                    expression.type, expression.nullable,
                                                    expression.output_name, {}});
          continue;
        }
        if (expression.kind != ReturnExpressionKind::kBinding) {
          return Status::InvalidArgument(
              "physical planner", "fixed expand node projection is unsupported");
        }
        projections.push_back(PhysicalExpression{PhysicalExpressionKind::kSlot, node->second,
                                                  add_slot(expression.binding_id, expression.type,
                                                           expression.nullable),
                                                  expression.type, expression.nullable,
                                                  expression.output_name, {}});
        continue;
      }
      if (expression.binding_id != relationship->binding_id) {
        return Status::InvalidArgument("physical planner", "fixed expand projection binding is unknown");
      }
      if (expression.kind == ReturnExpressionKind::kValidFrom ||
          expression.kind == ReturnExpressionKind::kValidTo ||
          expression.kind == ReturnExpressionKind::kCommitSeq ||
          expression.kind == ReturnExpressionKind::kOperation ||
          expression.kind == ReturnExpressionKind::kSystemTime) {
        const SlotId slot = expression.kind == ReturnExpressionKind::kValidFrom
            ? valid_from_slot : expression.kind == ReturnExpressionKind::kValidTo
                ? valid_to_slot : expression.kind == ReturnExpressionKind::kCommitSeq
                ? commit_seq_slot : expression.kind == ReturnExpressionKind::kOperation
                    ? operation_slot : system_time_slot;
        const PhysicalExpressionKind kind = expression.kind == ReturnExpressionKind::kOperation
            ? PhysicalExpressionKind::kOperationName : PhysicalExpressionKind::kSlot;
        projections.push_back(PhysicalExpression{kind, slot,
                                                  add_slot(expression.binding_id, expression.type,
                                                           expression.nullable),
                                                  expression.type, expression.nullable,
                                                  expression.output_name, {}});
        continue;
      }
      if (expression.kind != ReturnExpressionKind::kBinding) {
        return Status::InvalidArgument(
            "physical planner", "fixed expand relationship projection is unsupported");
      }
      if (expression.relationship_identity) {
        projections.push_back(PhysicalExpression{
            PhysicalExpressionKind::kSlot, edge_id_slot,
            add_slot(expression.binding_id, PhysicalType::kInt64, false),
            PhysicalType::kInt64, false, expression.output_name, {}});
        continue;
      }
      if (syntax_relationship.variable_length) {
        projections.push_back(PhysicalExpression{
            PhysicalExpressionKind::kPathBinding, path_slot,
            add_slot(expression.binding_id, PhysicalType::kBinary, false),
            PhysicalType::kBinary, false, expression.output_name, {}});
        continue;
      }
      const std::vector<SlotId> relationship_slots{
          source_slot, target_slot, edge_type_slot, edge_id_slot, valid_from_slot, commit_seq_slot};
      projections.push_back(PhysicalExpression{
          PhysicalExpressionKind::kRelationshipBinding, edge_id_slot,
          add_slot(expression.binding_id, PhysicalType::kBinary, false),
          PhysicalType::kBinary, false, expression.output_name, relationship_slots});
    }
    uint32_t next_operator = 1;
    std::vector<PhysicalOperatorSpec> operators;
    std::vector<PhysicalPropertySlot> relationship_projection_properties;
    std::vector<PhysicalPropertySlot> source_projection_properties;
    std::vector<PhysicalPropertySlot> target_projection_properties;
    std::vector<PhysicalPropertySlot> source_predicate_properties;
    std::vector<PhysicalPropertySlot> relationship_predicate_properties;
    std::vector<PhysicalPropertySlot> target_predicate_properties;
    for (const PhysicalPropertySlot& property : projection_properties) {
      if (property.binding == relationship->binding_id) {
        relationship_projection_properties.push_back(property);
      } else if (property.binding == source->binding_id) {
        source_projection_properties.push_back(property);
      } else if (property.binding == target->binding_id) {
        target_projection_properties.push_back(property);
      } else {
        return Status::InvalidArgument(
            "physical planner", "fixed expand property binding is unknown");
      }
    }
    for (const PhysicalPropertySlot& property : predicate_properties) {
      if (property.binding == source->binding_id) {
        source_predicate_properties.push_back(property);
      } else if (property.binding == relationship->binding_id) {
        relationship_predicate_properties.push_back(property);
      } else if (property.binding == target->binding_id) {
        target_predicate_properties.push_back(property);
      } else {
        return Status::InvalidArgument(
            "physical planner", "fixed expand predicate binding is unknown");
      }
    }
    operators.push_back(PhysicalOperatorSpec{OperatorId{next_operator++},
        change_expand ? PhysicalOperatorKind::kChangeScan
            : range_expand ? PhysicalOperatorKind::kTemporalRangeScan
                           : PhysicalOperatorKind::kTemporalPointScan,
        {}, {source_slot}});
    if (range_expand) {
      operators.push_back(PhysicalOperatorSpec{OperatorId{next_operator++},
          PhysicalOperatorKind::kIntervalDerive, {source_slot}, {}});
      operators.push_back(PhysicalOperatorSpec{OperatorId{next_operator++},
          PhysicalOperatorKind::kIntervalAlign, {source_slot}, {}});
      operators.push_back(PhysicalOperatorSpec{OperatorId{next_operator++},
          PhysicalOperatorKind::kTemporalCoalesce, {source_slot}, {}});
    }
    if (!source_predicate_properties.empty()) {
      std::vector<SlotId> produced;
      produced.reserve(source_predicate_properties.size());
      for (const PhysicalPropertySlot& property : source_predicate_properties) {
        produced.push_back(property.slot);
      }
      operators.push_back(PhysicalOperatorSpec{OperatorId{next_operator++},
          PhysicalOperatorKind::kPropertyGather, {source_slot}, std::move(produced)});
      std::vector<SlotId> required;
      required.reserve(source_predicate_properties.size());
      for (const PhysicalPropertySlot& property : source_predicate_properties) {
        required.push_back(property.slot);
      }
      operators.push_back(PhysicalOperatorSpec{OperatorId{next_operator++},
          PhysicalOperatorKind::kFilter, std::move(required), {}});
    }
    std::vector<SlotId> expand_outputs{
        target_slot, edge_type_slot, edge_id_slot, valid_from_slot, commit_seq_slot,
        operation_slot, system_time_slot, valid_to_slot};
    if (path_slot.value != 0) expand_outputs.push_back(path_slot);
    operators.push_back(PhysicalOperatorSpec{OperatorId{next_operator++},
        PhysicalOperatorKind::kExpand, {source_slot}, std::move(expand_outputs)});
    if (!relationship_predicate_properties.empty()) {
      std::vector<SlotId> produced;
      produced.reserve(relationship_predicate_properties.size());
      for (const PhysicalPropertySlot& property : relationship_predicate_properties) {
        produced.push_back(property.slot);
      }
      operators.push_back(PhysicalOperatorSpec{OperatorId{next_operator++},
          PhysicalOperatorKind::kPropertyGather,
          {source_slot, target_slot, edge_type_slot, edge_id_slot, valid_from_slot},
          std::move(produced)});
      std::vector<SlotId> required;
      required.reserve(relationship_predicate_properties.size());
      for (const PhysicalPropertySlot& property : relationship_predicate_properties) {
        required.push_back(property.slot);
      }
      operators.push_back(PhysicalOperatorSpec{OperatorId{next_operator++},
          PhysicalOperatorKind::kFilter, std::move(required), {}});
    }
    if (!target_predicate_properties.empty()) {
      std::vector<SlotId> produced;
      produced.reserve(target_predicate_properties.size());
      for (const PhysicalPropertySlot& property : target_predicate_properties) {
        produced.push_back(property.slot);
      }
      operators.push_back(PhysicalOperatorSpec{OperatorId{next_operator++},
          PhysicalOperatorKind::kPropertyGather, {target_slot}, std::move(produced)});
      std::vector<SlotId> required;
      required.reserve(target_predicate_properties.size());
      for (const PhysicalPropertySlot& property : target_predicate_properties) {
        required.push_back(property.slot);
      }
      operators.push_back(PhysicalOperatorSpec{OperatorId{next_operator++},
          PhysicalOperatorKind::kFilter, std::move(required), {}});
    }
    if (!relationship_projection_properties.empty()) {
      std::vector<SlotId> produced;
      produced.reserve(relationship_projection_properties.size());
      for (const PhysicalPropertySlot& property : relationship_projection_properties) {
        produced.push_back(property.slot);
      }
      operators.push_back(PhysicalOperatorSpec{OperatorId{next_operator++},
          PhysicalOperatorKind::kPropertyGather,
          {source_slot, target_slot, edge_type_slot, edge_id_slot, valid_from_slot},
          std::move(produced)});
    }
    if (!source_projection_properties.empty()) {
      std::vector<SlotId> produced;
      produced.reserve(source_projection_properties.size());
      for (const PhysicalPropertySlot& property : source_projection_properties) {
        produced.push_back(property.slot);
      }
      operators.push_back(PhysicalOperatorSpec{OperatorId{next_operator++},
          PhysicalOperatorKind::kPropertyGather, {source_slot}, std::move(produced)});
    }
    if (!target_projection_properties.empty()) {
      std::vector<SlotId> produced;
      produced.reserve(target_projection_properties.size());
      for (const PhysicalPropertySlot& property : target_projection_properties) {
        produced.push_back(property.slot);
      }
      operators.push_back(PhysicalOperatorSpec{OperatorId{next_operator++},
          PhysicalOperatorKind::kPropertyGather, {target_slot}, std::move(produced)});
    }
    std::vector<SlotId> projection_inputs;
    std::vector<SlotId> projection_outputs;
    for (const PhysicalExpression& projection : projections) {
      projection_inputs.push_back(projection.referenced_slot);
      projection_outputs.push_back(projection.output_slot);
    }
    std::sort(projection_inputs.begin(), projection_inputs.end());
    projection_inputs.erase(std::unique(projection_inputs.begin(), projection_inputs.end()),
                            projection_inputs.end());
    operators.push_back(PhysicalOperatorSpec{OperatorId{next_operator++},
        PhysicalOperatorKind::kProject, projection_inputs, projection_outputs});
    operators.push_back(PhysicalOperatorSpec{OperatorId{next_operator++},
        PhysicalOperatorKind::kResultSink, projection_outputs, {}});
    PipelineDescriptor pipeline{PipelineId{1}, {}, {}};
    for (const PhysicalOperatorSpec& op : operators) pipeline.operators.push_back(op.id);
    const bool include_system_time = std::any_of(
        statement.projections.begin(), statement.projections.end(),
        [](const BoundProjectionExpression& expression) {
          return expression.kind == ReturnExpressionKind::kSystemTime;
        });
    const bool include_valid_to = std::any_of(
        statement.projections.begin(), statement.projections.end(),
        [](const BoundProjectionExpression& expression) {
          return expression.kind == ReturnExpressionKind::kValidTo;
        });
    const PhysicalExpandSpec expand{source->binding_id, relationship->binding_id,
                                    target->binding_id, source_slot, target_slot,
                                    edge_type_slot, edge_id_slot, valid_from_slot,
                                    commit_seq_slot, operation_slot, system_time_slot,
                                    valid_to_slot, direction, edge_type,
                                    syntax_relationship.min_hops,
                                    syntax_relationship.max_hops, path_slot};
    std::optional<std::pair<uint64_t, uint64_t>> valid_time_range;
    std::optional<std::pair<uint64_t, uint64_t>> system_time_range;
    std::optional<uint64_t> valid_time_as_of;
    PhysicalTemporalMode expand_mode = range_expand
        ? PhysicalTemporalMode::kValidTimeRange : PhysicalTemporalMode::kPoint;
    const std::vector<BoundTemporalScope>& scopes =
        statement.primary_match_scopes.empty() ? statement.temporal_scopes
                                               : statement.primary_match_scopes;
    for (const BoundTemporalScope& scope : scopes) {
      if (scope.axis == TemporalAxis::kValidTime &&
          scope.mode == TemporalScopeMode::kStateAsOf) {
        valid_time_as_of = scope.valid_time_start;
      } else if (scope.axis == TemporalAxis::kValidTime &&
                 scope.mode == TemporalScopeMode::kStateBetween) {
        valid_time_range =
            std::make_pair(scope.valid_time_start, scope.valid_time_end);
      } else if (scope.axis == TemporalAxis::kValidTime &&
                 scope.mode == TemporalScopeMode::kChangesBetween) {
        valid_time_range =
            std::make_pair(scope.valid_time_start, scope.valid_time_end);
        expand_mode = PhysicalTemporalMode::kValidTimeChanges;
      } else if (scope.axis == TemporalAxis::kSystemTime &&
                 scope.mode == TemporalScopeMode::kChangesBetween) {
        system_time_range =
            std::make_pair(scope.system_time_start, scope.system_time_end);
        expand_mode = PhysicalTemporalMode::kSystemTimeChanges;
      }
    }
    if (range_expand && !valid_time_range.has_value()) {
      return Status::InvalidArgument(
          "physical planner", "range expand scope is invalid");
    }
    if (change_expand &&
        expand_mode != PhysicalTemporalMode::kValidTimeChanges &&
        expand_mode != PhysicalTemporalMode::kSystemTimeChanges) {
      return Status::InvalidArgument(
          "physical planner", "change expand scope is invalid");
    }
    const Status annotated = annotate_projections(&projections);
    if (!annotated.ok()) return annotated;
    uint64_t fingerprint = PlanFingerprint(
        source->binding_id, slots, operators, {pipeline}, predicates, projections,
        predicate_properties, projection_properties,
        std::nullopt, {}, include_valid_to, include_system_time, TemporalContextId{1}, expand_mode,
        valid_time_range, system_time_range, valid_time_as_of, expand);
    const TcypherStatement& result =
        result_statement == nullptr ? statement.syntax : *result_statement;
    std::optional<PhysicalAggregateSinkSpec> aggregate_sink =
        BuildPhysicalAggregateSink(result, OperatorId{next_operator});
    std::optional<PhysicalSortSinkSpec> sort_sink;
    std::vector<PhysicalOperatorSpec> post_result_operators;
    if (aggregate_sink.has_value()) {
      post_result_operators.push_back(aggregate_sink->op);
      ++next_operator;
    }
    if (result.distinct) {
      post_result_operators.push_back(PhysicalOperatorSpec{
          OperatorId{next_operator++}, PhysicalOperatorKind::kDistinct, {}, {}});
    }
    sort_sink = BuildPhysicalSortSink(result, OperatorId{next_operator});
    if (sort_sink.has_value()) {
      post_result_operators.push_back(sort_sink->op);
    }
    MixResultOperatorFingerprint(
        &fingerprint, aggregate_sink, result.distinct, sort_sink);
    auto plan = std::make_shared<const PhysicalPlan>(
        fingerprint, source->binding_id, std::move(slots), std::move(operators),
        std::vector<PipelineDescriptor>{std::move(pipeline)},
        std::move(predicates), std::move(projections),
        std::move(predicate_properties), std::move(projection_properties),
        std::nullopt, std::string{}, include_valid_to, include_system_time, TemporalContextId{1},
        expand_mode, valid_time_range, system_time_range, valid_time_as_of,
        expand,
        std::vector<PhysicalExpandSpec>{}, std::move(aggregate_sink),
        std::move(sort_sink), std::move(post_result_operators));
    const Status valid = ValidatePhysicalPlan(*plan);
    if (!valid.ok()) return valid;
    return plan;
  }
  if (logical_plan.demand.variables.size() != 1) {
    return Status::InvalidArgument("physical planner", "root point demand is incomplete");
  }
  const BindingId binding = logical_plan.demand.variables.front().binding_id;
  if (binding.value == 0 || statement.variables.front().binding_id != binding) {
    return Status::InvalidArgument("physical planner", "bound/logical binding id mismatch");
  }
  PhysicalTemporalMode temporal_mode = PhysicalTemporalMode::kPoint;
  std::optional<std::pair<uint64_t, uint64_t>> valid_time_range;
  std::optional<std::pair<uint64_t, uint64_t>> system_time_range;
  std::optional<uint64_t> valid_time_as_of;
  const std::vector<BoundTemporalScope>& scopes =
      statement.primary_match_scopes.empty() ? statement.temporal_scopes
                                              : statement.primary_match_scopes;
  for (const BoundTemporalScope& scope : scopes) {
    if (scope.mode == TemporalScopeMode::kStateAsOf &&
        scope.axis == TemporalAxis::kValidTime) {
      valid_time_as_of = scope.valid_time_start;
    } else if (scope.mode == TemporalScopeMode::kStateBetween &&
        scope.axis == TemporalAxis::kValidTime) {
      valid_time_range = std::make_pair(scope.valid_time_start, scope.valid_time_end);
      if (temporal_mode != PhysicalTemporalMode::kSystemTimeChanges) {
        temporal_mode = PhysicalTemporalMode::kValidTimeRange;
      }
    } else if (scope.mode == TemporalScopeMode::kChangesBetween) {
      if (scope.axis == TemporalAxis::kValidTime) {
        valid_time_range = std::make_pair(scope.valid_time_start, scope.valid_time_end);
        if (temporal_mode != PhysicalTemporalMode::kSystemTimeChanges) {
          temporal_mode = PhysicalTemporalMode::kValidTimeChanges;
        }
      } else {
        system_time_range = std::make_pair(scope.system_time_start, scope.system_time_end);
        temporal_mode = PhysicalTemporalMode::kSystemTimeChanges;
      }
    }
  }
  uint32_t next_slot = 1;
  std::vector<SlotDescriptor> slots;
  auto add_slot = [&](PhysicalType type, bool nullable) {
    const SlotId id{next_slot++};
    slots.push_back(SlotDescriptor{id, binding, type, nullable});
    return id;
  };
  const SlotId entity = add_slot(PhysicalType::kInt64, false);
  const SlotId valid_from = add_slot(PhysicalType::kTimestamp64, false);
  const SlotId commit_seq = add_slot(PhysicalType::kInt64, false);
  const SlotId operation = add_slot(PhysicalType::kInt32, false);

  const bool include_valid_to = std::find(
      statement.variables.front().provenance.begin(),
      statement.variables.front().provenance.end(),
      ProvenanceField::kValidTo) != statement.variables.front().provenance.end();
  const bool include_system_time = std::find(
      statement.variables.front().provenance.begin(),
      statement.variables.front().provenance.end(),
      ProvenanceField::kSystemTime) != statement.variables.front().provenance.end();
  const SlotId valid_to = include_valid_to
      ? add_slot(PhysicalType::kTimestamp64, false) : SlotId{};
  const SlotId system_time = include_system_time
      ? add_slot(PhysicalType::kTimestamp64, false) : SlotId{};

  std::map<BoundPropertyId, SlotId> predicate_property_slots;
  std::map<BoundPropertyId, SlotId> projection_property_slots;
  std::vector<PhysicalPropertySlot> predicate_properties;
  std::vector<PhysicalPropertySlot> projection_properties;
  for (const BoundPropertyReference& property : statement.properties) {
    if (property.binding_id != binding) continue;
    if (property.predicate) {
      const SlotId slot = add_slot(
          property.column.physical_type, property.nullable);
      predicate_property_slots.emplace(property.property_id, slot);
      predicate_properties.push_back(PhysicalPropertySlot{slot, property.column});
    }
    if (property.projection || property.grouping || property.ordering) {
      const SlotId slot = add_slot(
          property.column.physical_type, property.nullable);
      projection_property_slots.emplace(property.property_id, slot);
      projection_properties.push_back(PhysicalPropertySlot{slot, property.column});
    }
  }

  std::vector<PhysicalPredicate> predicates;
  for (const BoundPredicateExpression& expression : statement.predicates) {
    const auto slot = predicate_property_slots.find(expression.property_id);
    if (slot == predicate_property_slots.end()) {
      return Status::InvalidArgument("physical planner", "bound predicate slot is missing");
    }
    predicates.push_back(PhysicalPredicate{
        slot->second, expression.column, expression.column.physical_type,
        expression.nullable,
        static_cast<PhysicalPredicateKind>(expression.kind), expression.values,
        expression.lower_bound, expression.upper_bound,
        expression.lower_inclusive, expression.upper_inclusive});
  }

  std::vector<PhysicalExpression> projections;
  for (const BoundProjectionExpression& expression : statement.projections) {
    PhysicalExpression output;
    output.nullable = expression.nullable;
    output.type = expression.type;
    output.output_name = expression.output_name;
    if (expression.kind == ReturnExpressionKind::kBinding ||
        expression.kind == ReturnExpressionKind::kCount ||
        (expression.kind == ReturnExpressionKind::kCollect &&
         !expression.property_id.has_value())) {
      output.referenced_slot = entity;
    } else if (expression.kind == ReturnExpressionKind::kValidFrom) {
      output.referenced_slot = valid_from;
    } else if (expression.kind == ReturnExpressionKind::kValidTo) {
      output.referenced_slot = valid_to;
    } else if (expression.kind == ReturnExpressionKind::kCommitSeq) {
      output.referenced_slot = commit_seq;
    } else if (expression.kind == ReturnExpressionKind::kOperation) {
      output.kind = PhysicalExpressionKind::kOperationName;
      output.referenced_slot = operation;
    } else if (expression.kind == ReturnExpressionKind::kSystemTime) {
      output.referenced_slot = system_time;
    } else if (expression.property_id.has_value()) {
      const auto property =
          projection_property_slots.find(*expression.property_id);
      if (property == projection_property_slots.end()) {
        return Status::InvalidArgument("physical planner", "bound projection slot is missing");
      }
      output.referenced_slot = property->second;
    } else {
      return Status::InvalidArgument("physical planner", "unsupported root point projection");
    }
    output.output_slot = add_slot(output.type, output.nullable);
    projections.push_back(std::move(output));
  }

  uint32_t next_operator = 1;
  std::vector<PhysicalOperatorSpec> operators;
  const PhysicalOperatorKind scan_kind =
      temporal_mode == PhysicalTemporalMode::kPoint
          ? PhysicalOperatorKind::kTemporalPointScan
          : temporal_mode == PhysicalTemporalMode::kValidTimeRange
              ? PhysicalOperatorKind::kTemporalRangeScan
              : PhysicalOperatorKind::kChangeScan;
  std::vector<SlotId> scan_produced{entity, valid_from, commit_seq, operation};
  if (include_valid_to &&
      (temporal_mode == PhysicalTemporalMode::kValidTimeChanges ||
       temporal_mode == PhysicalTemporalMode::kSystemTimeChanges)) {
    scan_produced.push_back(valid_to);
  }
  operators.push_back(PhysicalOperatorSpec{
      OperatorId{next_operator++}, scan_kind, {}, std::move(scan_produced)});
  if (temporal_mode == PhysicalTemporalMode::kValidTimeRange) {
    std::vector<SlotId> produced;
    if (include_valid_to) produced.push_back(valid_to);
    operators.push_back(PhysicalOperatorSpec{
        OperatorId{next_operator++}, PhysicalOperatorKind::kIntervalDerive,
        {entity, valid_from, commit_seq, operation}, std::move(produced)});
    operators.push_back(PhysicalOperatorSpec{
        OperatorId{next_operator++}, PhysicalOperatorKind::kIntervalAlign,
        {entity, valid_from, commit_seq}, {}});
    operators.push_back(PhysicalOperatorSpec{
        OperatorId{next_operator++}, PhysicalOperatorKind::kTemporalCoalesce,
        {entity, valid_from, commit_seq}, {}});
    if (include_system_time) {
      operators.push_back(PhysicalOperatorSpec{
          OperatorId{next_operator++}, PhysicalOperatorKind::kMetadataProject,
          {entity, valid_from, commit_seq}, {system_time}});
    }
  } else if (include_valid_to || include_system_time) {
    std::vector<SlotId> produced;
    if (include_valid_to && temporal_mode == PhysicalTemporalMode::kPoint) {
      produced.push_back(valid_to);
    }
    if (include_system_time) produced.push_back(system_time);
    operators.push_back(PhysicalOperatorSpec{
        OperatorId{next_operator++}, PhysicalOperatorKind::kMetadataProject,
        {entity, valid_from, commit_seq}, std::move(produced)});
  }
  if (!predicate_properties.empty()) {
    std::vector<SlotId> produced;
    for (const auto& property : predicate_properties) produced.push_back(property.slot);
    operators.push_back(PhysicalOperatorSpec{
        OperatorId{next_operator++}, PhysicalOperatorKind::kPropertyGather,
        {entity}, produced});
    operators.push_back(PhysicalOperatorSpec{
        OperatorId{next_operator++}, PhysicalOperatorKind::kFilter,
        produced, {}});
  }
  if (!projection_properties.empty()) {
    std::vector<SlotId> produced;
    for (const auto& property : projection_properties) produced.push_back(property.slot);
    operators.push_back(PhysicalOperatorSpec{
        OperatorId{next_operator++}, PhysicalOperatorKind::kPropertyGather,
        {entity}, std::move(produced)});
  }
  std::vector<SlotId> projection_inputs;
  std::vector<SlotId> projection_outputs;
  for (const PhysicalExpression& expression : projections) {
    projection_inputs.push_back(expression.referenced_slot);
    projection_outputs.push_back(expression.output_slot);
  }
  std::sort(projection_inputs.begin(), projection_inputs.end());
  projection_inputs.erase(std::unique(projection_inputs.begin(), projection_inputs.end()),
                          projection_inputs.end());
  operators.push_back(PhysicalOperatorSpec{
      OperatorId{next_operator++}, PhysicalOperatorKind::kProject,
      projection_inputs, projection_outputs});
  operators.push_back(PhysicalOperatorSpec{
      OperatorId{next_operator++}, PhysicalOperatorKind::kResultSink,
      projection_outputs, {}});
  PipelineDescriptor pipeline{PipelineId{1}, {}, {}};
  for (const PhysicalOperatorSpec& op : operators) pipeline.operators.push_back(op.id);

  std::optional<uint64_t> exact_entity_id;
  std::string exact_entity_parameter;
  exact_entity_id = statement.root_exact_entity_id;
  exact_entity_parameter = statement.root_exact_entity_parameter;
  std::vector<PipelineDescriptor> pipelines{std::move(pipeline)};
  const TemporalContextId temporal_context{1};
  const Status annotated = annotate_projections(&projections);
  if (!annotated.ok()) return annotated;
  uint64_t fingerprint = PlanFingerprint(
      binding, slots, operators, pipelines, predicates, projections,
      predicate_properties, projection_properties, exact_entity_id,
      exact_entity_parameter, include_valid_to, include_system_time,
      temporal_context, temporal_mode, valid_time_range, system_time_range,
      valid_time_as_of, std::nullopt);
  const TcypherStatement& result =
      result_statement == nullptr ? statement.syntax : *result_statement;
  std::optional<PhysicalAggregateSinkSpec> aggregate_sink =
      BuildPhysicalAggregateSink(result, OperatorId{next_operator});
  std::optional<PhysicalSortSinkSpec> sort_sink;
  std::vector<PhysicalOperatorSpec> post_result_operators;
  if (aggregate_sink.has_value()) {
    post_result_operators.push_back(aggregate_sink->op);
    ++next_operator;
  }
  if (result.distinct) {
    post_result_operators.push_back(PhysicalOperatorSpec{
        OperatorId{next_operator++}, PhysicalOperatorKind::kDistinct, {}, {}});
  }
  sort_sink = BuildPhysicalSortSink(result, OperatorId{next_operator});
  if (sort_sink.has_value()) {
    post_result_operators.push_back(sort_sink->op);
  }
  MixResultOperatorFingerprint(
      &fingerprint, aggregate_sink, result.distinct, sort_sink);
  auto plan = std::make_shared<const PhysicalPlan>(
      fingerprint, binding, std::move(slots), std::move(operators),
      std::move(pipelines), std::move(predicates),
      std::move(projections), std::move(predicate_properties),
      std::move(projection_properties), exact_entity_id,
      std::move(exact_entity_parameter), include_valid_to, include_system_time,
      temporal_context, temporal_mode, valid_time_range, system_time_range,
      valid_time_as_of, std::nullopt, std::vector<PhysicalExpandSpec>{},
      std::move(aggregate_sink), std::move(sort_sink),
      std::move(post_result_operators));
  const Status valid = ValidatePhysicalPlan(*plan);
  if (!valid.ok()) return valid;
  return plan;
}

StatusOr<std::shared_ptr<const PhysicalHashJoinPlan>> PlanPhysicalHashJoin(
    const BoundTcypherStatement& statement, const LogicalPlan& logical_plan,
    const TcypherStatement* result_statement,
    PhysicalHashJoinPlanningStats planning_stats) {
  if (!CanPlanPhysicalHashJoin(statement) ||
      logical_plan.demand.variables.size() != 2) {
    return Status::InvalidArgument(
        "physical planner", "statement is not a supported two-root hash join");
  }

  const std::string& left_variable_name = statement.joins.front().left.variable;
  const std::string& right_variable_name = statement.joins.front().right.variable;
  std::vector<const BoundJoinInput*> left_inputs;
  std::vector<const BoundJoinInput*> right_inputs;
  left_inputs.reserve(statement.joins.size());
  right_inputs.reserve(statement.joins.size());
  for (const BoundJoinEquality& equality : statement.joins) {
    if (equality.left.variable == left_variable_name &&
        equality.right.variable == right_variable_name) {
      left_inputs.push_back(&equality.left);
      right_inputs.push_back(&equality.right);
    } else if (equality.left.variable == right_variable_name &&
               equality.right.variable == left_variable_name) {
      left_inputs.push_back(&equality.right);
      right_inputs.push_back(&equality.left);
    } else {
      return Status::InvalidArgument(
          "physical planner", "hash join equalities connect different bindings");
    }
  }
  const auto find_variable = [&](const std::string& name) -> const BoundVariable* {
    const auto found = std::find_if(
        statement.variables.begin(), statement.variables.end(),
        [&](const BoundVariable& variable) { return variable.variable == name; });
    return found == statement.variables.end() ? nullptr : &*found;
  };
  const auto find_property = [&](const BoundJoinInput& input)
      -> const BoundPropertyReference* {
    if (!input.column.has_value()) return nullptr;
    const auto found = std::find_if(
        statement.properties.begin(), statement.properties.end(),
        [&](const BoundPropertyReference& property) {
          return property.variable == input.variable &&
              property.column.entity_type == input.column->entity_type &&
              property.column.column_id == input.column->column_id &&
              property.column.schema_epoch == input.column->schema_epoch;
        });
    return found == statement.properties.end() ? nullptr : &*found;
  };
  const BoundVariable* left_variable = find_variable(left_variable_name);
  const BoundVariable* right_variable = find_variable(right_variable_name);
  std::vector<const BoundPropertyReference*> left_properties;
  std::vector<const BoundPropertyReference*> right_properties;
  left_properties.reserve(left_inputs.size());
  right_properties.reserve(right_inputs.size());
  for (size_t index = 0; index < left_inputs.size(); ++index) {
    left_properties.push_back(find_property(*left_inputs[index]));
    right_properties.push_back(find_property(*right_inputs[index]));
  }
  if (left_variable == nullptr || right_variable == nullptr ||
      left_variable->kind != BoundGraphKind::kNode ||
      right_variable->kind != BoundGraphKind::kNode) {
    return Status::InvalidArgument(
        "physical planner", "hash join bindings or key properties are incomplete");
  }
  for (size_t index = 0; index < left_inputs.size(); ++index) {
    if ((!left_inputs[index]->identity && left_properties[index] == nullptr) ||
        (!right_inputs[index]->identity && right_properties[index] == nullptr)) {
      return Status::InvalidArgument(
          "physical planner", "hash join key property is missing");
    }
  }
  std::vector<size_t> left_projection_indexes;
  std::vector<size_t> right_projection_indexes;
  for (size_t index = 0; index < statement.projections.size(); ++index) {
    if (statement.projections[index].binding_id == left_variable->binding_id) {
      left_projection_indexes.push_back(index);
    } else if (statement.projections[index].binding_id == right_variable->binding_id) {
      right_projection_indexes.push_back(index);
    } else {
      return Status::InvalidArgument(
          "physical planner", "hash join projection binding is unknown");
    }
  }

  const MatchNodePattern* left_pattern = nullptr;
  const MatchNodePattern* right_pattern = nullptr;
  const MatchNodePattern& primary = statement.syntax.match;
  const MatchNodePattern& additional = statement.syntax.additional_matches.front().match;
  for (const auto& pair : {std::make_pair(&primary, &left_pattern),
                           std::make_pair(&additional, &left_pattern)}) {
    if (pair.first->variable == left_variable_name) *pair.second = pair.first;
  }
  for (const auto& pair : {std::make_pair(&primary, &right_pattern),
                           std::make_pair(&additional, &right_pattern)}) {
    if (pair.first->variable == right_variable_name) *pair.second = pair.first;
  }
  if (left_pattern == nullptr || right_pattern == nullptr) {
    return Status::InvalidArgument("physical planner", "hash join MATCH patterns are missing");
  }

  const auto build_child = [&](const BoundVariable& variable,
                               const std::vector<const BoundJoinInput*>& join_inputs,
                               const std::vector<const BoundPropertyReference*>& join_properties,
                               const MatchNodePattern& pattern,
                               bool primary_input,
                               const std::vector<size_t>& projection_indexes)
      -> StatusOr<std::shared_ptr<const PhysicalPlan>> {
    BoundTcypherStatement child;
    child.syntax = statement.syntax;
    child.syntax.match = pattern;
    child.syntax.relationships.clear();
    child.syntax.expanded_nodes.clear();
    child.syntax.additional_matches.clear();
    child.syntax.where.reset();
    child.syntax.and_predicates.clear();
    child.syntax.distinct = false;
    child.syntax.order_by.reset();
    child.syntax.skip.reset();
    child.syntax.limit.reset();
    child.syntax.returns.clear();
    for (size_t index : projection_indexes) {
      const BoundProjectionExpression& projection = statement.projections[index];
      child.syntax.returns.push_back(ReturnExpression{
          projection.kind, variable.variable, {}, {}});
    }
    for (size_t index = 0; index < join_inputs.size(); ++index) {
      child.syntax.returns.push_back(ReturnExpression{
          join_inputs[index]->identity ? ReturnExpressionKind::kBinding
                                       : ReturnExpressionKind::kProperty,
          variable.variable,
          join_properties[index] == nullptr ? std::string{} :
              join_properties[index]->column.logical_type,
          {}});
    }
    child.temporal_scopes = statement.temporal_scopes;
    child.primary_match_scopes = primary_input ||
            statement.additional_match_scopes.empty() ||
            statement.additional_match_scopes.front().empty()
        ? statement.primary_match_scopes
        : statement.additional_match_scopes.front();
    child.variables = {variable};
    child.variables.front().join_identity = false;
    for (const BoundPropertyReference& property : statement.properties) {
      const bool join_key = std::any_of(
          join_properties.begin(), join_properties.end(),
          [&](const BoundPropertyReference* candidate) {
            return candidate != nullptr &&
                candidate->property_id == property.property_id;
          });
      if (property.binding_id != variable.binding_id ||
          (!property.projection && !join_key)) {
        continue;
      }
      BoundPropertyReference projected = property;
      projected.predicate = false;
      projected.projection = true;
      projected.grouping = false;
      projected.ordering = false;
      projected.join = false;
      child.properties.push_back(std::move(projected));
    }
    for (size_t index : projection_indexes) {
      child.projections.push_back(statement.projections[index]);
    }
    for (size_t index = 0; index < join_inputs.size(); ++index) {
      const BoundJoinInput& join_input = *join_inputs[index];
      const BoundPropertyReference* join_property = join_properties[index];
      child.projections.push_back(BoundProjectionExpression{
          join_input.identity ? ReturnExpressionKind::kBinding
                              : ReturnExpressionKind::kProperty,
          variable.binding_id,
          join_property == nullptr
              ? std::optional<BoundPropertyId>{}
              : std::optional<BoundPropertyId>{join_property->property_id},
          join_input.identity ? PhysicalType::kInt64
                              : join_property->column.physical_type,
          join_input.identity ? false : join_property->nullable,
          variable.variable + (join_input.identity
              ? ".id" : "." + join_property->column.logical_type)});
    }
    child.root_point_candidate = true;
    if (pattern.entity_id.has_value()) {
      child.root_exact_entity_id = pattern.entity_id->integer_literal;
      child.root_exact_entity_parameter = pattern.entity_id->parameter_name;
    }
    LogicalPlan child_logical;
    FactDemandSet::VariableDemand demand;
    demand.variable = variable.variable;
    demand.kind = variable.kind;
    demand.entity_type = variable.entity_type;
    demand.entity_schema = variable.entity_schema;
    demand.non_nullable = variable.non_nullable;
    demand.existence = true;
    for (const BoundPropertyReference& property : child.properties) {
      demand.projection_properties.push_back(property.column);
    }
    demand.binding_id = variable.binding_id;
    child_logical.demand.variables = {std::move(demand)};
    child_logical.demand.existence_fact = true;
    for (const BoundPropertyReference& property : child.properties) {
      child_logical.demand.property_names.push_back(property.column.logical_type);
    }
    return PlanPhysicalRootPoint(child, child_logical);
  };

  const auto left = build_child(
      *left_variable, left_inputs, left_properties, *left_pattern,
      left_pattern == &primary, left_projection_indexes);
  if (!left.ok()) return left.status();
  const auto right = build_child(
      *right_variable, right_inputs, right_properties, *right_pattern,
      right_pattern == &primary, right_projection_indexes);
  if (!right.ok()) return right.status();

  uint64_t fingerprint = left.ValueOrDie()->plan_id() ^
      (right.ValueOrDie()->plan_id() + 0x9e3779b97f4a7c15ULL +
       (left.ValueOrDie()->plan_id() << 6) + (left.ValueOrDie()->plan_id() >> 2));
  std::vector<std::string> output_names;
  std::vector<PhysicalHashJoinPlan::Output> outputs;
  for (size_t index = 0; index < statement.projections.size(); ++index) {
    const BoundProjectionExpression& projection = statement.projections[index];
    output_names.push_back(projection.output_name);
    const bool from_left = projection.binding_id == left_variable->binding_id;
    const std::vector<size_t>& indexes =
        from_left ? left_projection_indexes : right_projection_indexes;
    const auto found = std::find(indexes.begin(), indexes.end(), index);
    if (found == indexes.end()) {
      return Status::Corruption(
          "physical planner", "hash join output column mapping is missing");
    }
    outputs.push_back(PhysicalHashJoinPlan::Output{
        from_left, static_cast<uint32_t>(found - indexes.begin()),
        projection.type, projection.nullable, projection.output_name});
    for (unsigned char byte : projection.output_name) {
      fingerprint ^= byte;
      fingerprint *= 1099511628211ULL;
    }
  }
  const PhysicalHashJoinBuildSide build_side =
      planning_stats.left.rows < planning_stats.right.rows
          ? PhysicalHashJoinBuildSide::kLeft
          : PhysicalHashJoinBuildSide::kRight;
  const auto mix_plan_value = [&](uint64_t value) {
    for (uint32_t byte = 0; byte < 8; ++byte) {
      fingerprint ^= static_cast<uint8_t>(value >> (byte * 8));
      fingerprint *= 1099511628211ULL;
    }
  };
  mix_plan_value(static_cast<uint8_t>(build_side));
  mix_plan_value(planning_stats.left.rows);
  mix_plan_value(planning_stats.left.confidence_per_mille);
  mix_plan_value(planning_stats.left.conservative ? 1 : 0);
  mix_plan_value(planning_stats.right.rows);
  mix_plan_value(planning_stats.right.confidence_per_mille);
  mix_plan_value(planning_stats.right.conservative ? 1 : 0);
  mix_plan_value(planning_stats.statistics_snapshot_id);
  if (fingerprint == 0) fingerprint = 1;
  PhysicalOperatorSpec join{OperatorId{1}, PhysicalOperatorKind::kHashJoin, {}, {}};
  std::vector<PhysicalOperatorSpec> post_join_operators;
  const TcypherStatement& result =
      result_statement == nullptr ? statement.syntax : *result_statement;
  std::optional<PhysicalAggregateSinkSpec> aggregate_sink =
      BuildPhysicalAggregateSink(result, OperatorId{2});
  std::optional<PhysicalSortSinkSpec> sort_sink;
  uint32_t next_post_operator = 2;
  if (aggregate_sink.has_value()) {
    post_join_operators.push_back(aggregate_sink->op);
    ++next_post_operator;
  }
  if (result.distinct) {
    post_join_operators.push_back(
        PhysicalOperatorSpec{OperatorId{next_post_operator},
                             PhysicalOperatorKind::kDistinct, {}, {}});
    ++next_post_operator;
  }
  sort_sink = BuildPhysicalSortSink(
      result, OperatorId{next_post_operator});
  if (sort_sink.has_value()) {
    post_join_operators.push_back(sort_sink->op);
  }
  MixResultOperatorFingerprint(
      &fingerprint, aggregate_sink, result.distinct, sort_sink);
  std::vector<OperatorId> result_operators{join.id};
  for (const PhysicalOperatorSpec& op : post_join_operators) {
    result_operators.push_back(op.id);
  }
  std::vector<PipelineDescriptor> pipelines{
      PipelineDescriptor{PipelineId{1}, {}, {}},
      PipelineDescriptor{PipelineId{2}, {}, {}},
      PipelineDescriptor{PipelineId{3}, std::move(result_operators),
                         {PipelineId{1}, PipelineId{2}}}};
  std::vector<uint32_t> left_key_columns;
  std::vector<uint32_t> right_key_columns;
  for (uint32_t index = 0; index < left_inputs.size(); ++index) {
    left_key_columns.push_back(
        static_cast<uint32_t>(left_projection_indexes.size()) + index);
    right_key_columns.push_back(
        static_cast<uint32_t>(right_projection_indexes.size()) + index);
  }
  auto plan = std::make_shared<const PhysicalHashJoinPlan>(PhysicalHashJoinPlan{
      fingerprint, left.ValueOrDie(), right.ValueOrDie(), join,
      0, left_key_columns.front(), 0, right_key_columns.front(),
      std::move(left_key_columns), std::move(right_key_columns),
      std::move(output_names), std::move(outputs),
      std::move(aggregate_sink),
      std::move(sort_sink),
      std::move(post_join_operators), std::move(pipelines), build_side,
      planning_stats.left, planning_stats.right,
      planning_stats.statistics_snapshot_id});
  const Status valid = ValidatePhysicalHashJoinPlan(*plan);
  if (!valid.ok()) return valid;
  return plan;
}

StatusOr<std::shared_ptr<const PhysicalMultiHashJoinPlan>>
PlanPhysicalMultiHashJoin(
    const BoundTcypherStatement& statement, const LogicalPlan& logical_plan,
    const TcypherStatement* result_statement,
    const std::map<BindingId, PhysicalCardinalityEstimate>& estimates,
    uint64_t statistics_snapshot_id, uint64_t visible_seq_ceiling) {
  if (!CanPlanPhysicalMultiHashJoin(statement) ||
      logical_plan.demand.variables.size() != statement.variables.size()) {
    return Status::InvalidArgument(
        "physical planner", "statement is not a supported multi-root join");
  }

  struct RootColumn {
    PhysicalMultiJoinColumn layout;
    BoundProjectionExpression projection;
    ReturnExpression syntax;
    bool final_output = false;
  };
  struct RootInput {
    const BoundVariable* variable = nullptr;
    MatchClause clause;
    std::vector<const BoundVariable*> variables;
    std::vector<BoundTemporalScope> scopes;
    std::vector<RootColumn> columns;
    std::vector<BoundPredicateExpression> predicates;
    std::shared_ptr<const PhysicalPlan> plan;
    PhysicalCardinalityEstimate estimate;
    uint64_t snapshot_seq = 0;
  };
  struct ActiveColumn {
    PhysicalMultiJoinColumn layout;
    ReturnExpressionKind kind = ReturnExpressionKind::kBinding;
    bool final_output = false;
  };

  std::map<std::string, const BoundVariable*> variable_by_name;
  for (const BoundVariable& variable : statement.variables) {
    if (!variable_by_name.emplace(variable.variable, &variable).second) {
      return Status::InvalidArgument(
          "physical planner", "multi-root binding name is duplicated");
    }
  }
  std::vector<RootInput> roots;
  roots.reserve(statement.syntax.additional_matches.size() + 1);
  roots.push_back(RootInput{});
  roots.back().clause = MatchClause{
      statement.syntax.match, statement.syntax.relationships,
      statement.syntax.expanded_nodes, statement.syntax.match_temporal_scopes};
  for (const MatchClause& clause : statement.syntax.additional_matches) {
    roots.push_back(RootInput{});
    roots.back().clause = clause;
  }
  const auto apply_scope_overrides = [](
      std::vector<BoundTemporalScope> inherited,
      const std::vector<BoundTemporalScope>& overrides) {
    for (const BoundTemporalScope& scope : overrides) {
      inherited.erase(
          std::remove_if(
              inherited.begin(), inherited.end(),
              [&](const BoundTemporalScope& candidate) {
                return candidate.axis == scope.axis;
              }),
          inherited.end());
      inherited.push_back(scope);
    }
    return inherited;
  };
  const std::vector<BoundTemporalScope> primary_scopes =
      apply_scope_overrides(statement.temporal_scopes,
                            statement.primary_match_scopes);
  for (uint32_t index = 0; index < roots.size(); ++index) {
    RootInput& root = roots[index];
    const auto root_variable = variable_by_name.find(root.clause.match.variable);
    if (root_variable == variable_by_name.end() ||
        root_variable->second->kind != BoundGraphKind::kNode) {
      return Status::InvalidArgument(
          "physical planner", "multi-root MATCH root binding is invalid");
    }
    root.variable = root_variable->second;
    root.scopes = index == 0
        ? primary_scopes
        : apply_scope_overrides(
              primary_scopes, statement.additional_match_scopes[index - 1]);
    root.snapshot_seq = visible_seq_ceiling;
    for (const BoundTemporalScope& scope : root.scopes) {
      if (scope.axis == TemporalAxis::kSystemTime) {
        root.snapshot_seq = scope.snapshot_seq;
      }
    }
    if (visible_seq_ceiling != 0 && root.snapshot_seq > visible_seq_ceiling) {
      return Status::InvalidArgument(
          "physical planner", "multi-root temporal cutoff exceeds visible prefix");
    }
    root.variables.push_back(root.variable);
    for (const MatchRelationshipPattern& relationship :
         root.clause.relationships) {
      const auto variable = variable_by_name.find(relationship.variable);
      if (variable == variable_by_name.end() ||
          variable->second->kind != BoundGraphKind::kRelationship) {
        return Status::InvalidArgument(
            "physical planner", "multi-root relationship binding is invalid");
      }
      root.variables.push_back(variable->second);
    }
    for (const MatchNodePattern& node : root.clause.expanded_nodes) {
      const auto variable = variable_by_name.find(node.variable);
      if (variable == variable_by_name.end() ||
          variable->second->kind != BoundGraphKind::kNode) {
        return Status::InvalidArgument(
            "physical planner", "multi-root expanded binding is invalid");
      }
      root.variables.push_back(variable->second);
    }
    root.estimate = estimates.count(root.variable->binding_id) != 0
        ? estimates.at(root.variable->binding_id)
        : PhysicalCardinalityEstimate{};
    if (root.estimate.confidence_per_mille > 1000) {
      return Status::InvalidArgument(
          "physical planner", "multi-root cardinality confidence is invalid");
    }
  }
  std::sort(roots.begin(), roots.end(),
            [](const RootInput& left, const RootInput& right) {
              return left.variable->binding_id < right.variable->binding_id;
            });
  std::map<BindingId, uint32_t> input_by_binding;
  std::map<std::string, BindingId> binding_by_name;
  for (uint32_t index = 0; index < roots.size(); ++index) {
    for (const BoundVariable* variable : roots[index].variables) {
      if (!input_by_binding.emplace(variable->binding_id, index).second ||
          !binding_by_name.emplace(variable->variable,
                                   variable->binding_id).second) {
        return Status::InvalidArgument(
            "physical planner", "multi-root binding identity is duplicated");
      }
    }
  }

  std::map<BoundPropertyId, const BoundPropertyReference*> property_by_id;
  for (const BoundPropertyReference& property : statement.properties) {
    property_by_id.emplace(property.property_id, &property);
  }
  const auto property_for = [&](BindingId binding, BoundPropertyId property_id)
      -> const BoundPropertyReference* {
    const auto found = property_by_id.find(property_id);
    return found != property_by_id.end() &&
            found->second->binding_id == binding
        ? found->second : nullptr;
  };

  if (input_by_binding.size() != statement.variables.size()) {
    return Status::InvalidArgument(
        "physical planner", "multi-root binding ownership is incomplete");
  }

  for (const BoundPredicateExpression& predicate : statement.predicates) {
    const auto root = input_by_binding.find(predicate.binding_id);
    const BoundPropertyReference* property =
        property_for(predicate.binding_id, predicate.property_id);
    if (root == input_by_binding.end() || property == nullptr ||
        !property->predicate ||
        property->column.physical_type != predicate.column.physical_type) {
      return Status::InvalidArgument(
          "physical planner", "multi-root local predicate is incomplete");
    }
    roots[root->second].predicates.push_back(predicate);
  }

  const auto same_column = [](const RootColumn& column,
                              const BoundProjectionExpression& projection) {
    return column.projection.binding_id == projection.binding_id &&
        column.projection.kind == projection.kind &&
        column.projection.property_id == projection.property_id &&
        column.projection.relationship_identity ==
            projection.relationship_identity;
  };
  const auto append_column = [&](RootInput* root,
                                 BoundProjectionExpression projection,
                                 ReturnExpression syntax,
                                 bool final_output) {
    const auto found = std::find_if(
        root->columns.begin(), root->columns.end(),
        [&](const RootColumn& column) {
          return same_column(column, projection);
        });
    if (found != root->columns.end()) {
      if (final_output) found->final_output = true;
      return;
    }
    root->columns.push_back(RootColumn{
        PhysicalMultiJoinColumn{projection.binding_id, projection.kind,
                                projection.property_id, projection.type,
                                projection.nullable, projection.output_name,
                                projection.relationship_identity},
        std::move(projection), std::move(syntax), final_output});
  };
  for (size_t projection_index = 0;
       projection_index < statement.projections.size(); ++projection_index) {
    const BoundProjectionExpression& projection =
        statement.projections[projection_index];
    const auto root = input_by_binding.find(projection.binding_id);
    if (root == input_by_binding.end() ||
        projection_index >= statement.syntax.returns.size()) {
      return Status::InvalidArgument(
          "physical planner", "multi-root projection binding is unknown");
    }
    append_column(&roots[root->second], projection,
                  statement.syntax.returns[projection_index], true);
  }
  const auto append_endpoint = [&](const LogicalJoinEndpoint& endpoint)
      -> Status {
    const auto root = input_by_binding.find(endpoint.binding_id);
    if (root == input_by_binding.end() ||
        endpoint.identity == endpoint.property_id.has_value()) {
      return Status::InvalidArgument(
          "physical planner", "multi-root join endpoint is invalid");
    }
    BoundProjectionExpression projection;
    projection.kind = endpoint.identity ? ReturnExpressionKind::kBinding
                                        : ReturnExpressionKind::kProperty;
    projection.binding_id = endpoint.binding_id;
    projection.property_id = endpoint.property_id;
    projection.type = endpoint.type;
    projection.nullable = endpoint.nullable;
    const auto endpoint_variable = std::find_if(
        statement.variables.begin(), statement.variables.end(),
        [&](const BoundVariable& variable) {
          return variable.binding_id == endpoint.binding_id;
        });
    if (endpoint_variable == statement.variables.end()) {
      return Status::InvalidArgument(
          "physical planner", "multi-root join endpoint owner is missing");
    }
    projection.relationship_identity = endpoint.identity &&
        endpoint_variable->kind == BoundGraphKind::kRelationship;
    ReturnExpression syntax;
    syntax.kind = projection.kind;
    syntax.variable = roots[root->second].variable->variable;
    if (endpoint.identity) {
      projection.output_name = syntax.variable + ".id";
    } else {
      const BoundPropertyReference* property =
          property_for(endpoint.binding_id, *endpoint.property_id);
      if (property == nullptr ||
          property->column.physical_type != endpoint.type ||
          property->nullable != endpoint.nullable) {
        return Status::InvalidArgument(
            "physical planner", "multi-root join property is incomplete");
      }
      syntax.property_name = property->column.logical_type;
      projection.output_name = syntax.variable + "." + syntax.property_name;
    }
    append_column(&roots[root->second], std::move(projection),
                  std::move(syntax), false);
    return Status::OK();
  };
  std::vector<std::vector<bool>> adjacency(
      roots.size(), std::vector<bool>(roots.size(), false));
  for (const LogicalJoinEdge& edge : logical_plan.join_edges) {
    const auto left = input_by_binding.find(edge.left.binding_id);
    const auto right = input_by_binding.find(edge.right.binding_id);
    if (left == input_by_binding.end() || right == input_by_binding.end() ||
        left->second == right->second || edge.left.type != edge.right.type) {
      return Status::InvalidArgument(
          "physical planner", "multi-root logical edge is invalid");
    }
    const Status left_status = append_endpoint(edge.left);
    if (!left_status.ok()) return left_status;
    const Status right_status = append_endpoint(edge.right);
    if (!right_status.ok()) return right_status;
    adjacency[left->second][right->second] = true;
    adjacency[right->second][left->second] = true;
  }

  for (RootInput& root : roots) {
    BoundTcypherStatement child;
    child.syntax = statement.syntax;
    child.syntax.match = root.clause.match;
    child.syntax.relationships = root.clause.relationships;
    child.syntax.expanded_nodes = root.clause.expanded_nodes;
    child.syntax.additional_matches.clear();
    child.syntax.where.reset();
    child.syntax.and_predicates.clear();
    child.syntax.distinct = false;
    child.syntax.order_by.reset();
    child.syntax.skip.reset();
    child.syntax.limit.reset();
    child.syntax.returns.clear();
    child.temporal_scopes = statement.temporal_scopes;
    child.primary_match_scopes = root.scopes;
    child.variables.reserve(root.variables.size());
    for (const BoundVariable* variable : root.variables) {
      BoundVariable copied = *variable;
      copied.join_identity = false;
      child.variables.push_back(std::move(copied));
    }
    for (const RootColumn& column : root.columns) {
      child.syntax.returns.push_back(column.syntax);
      child.projections.push_back(column.projection);
      if (!column.projection.property_id.has_value()) continue;
      const BoundPropertyReference* source = property_for(
          column.projection.binding_id, *column.projection.property_id);
      if (source == nullptr) {
        return Status::InvalidArgument(
            "physical planner", "multi-root projected property is missing");
      }
      if (std::any_of(child.properties.begin(), child.properties.end(),
                      [&](const BoundPropertyReference& property) {
                        return property.property_id == source->property_id;
                      })) {
        continue;
      }
      BoundPropertyReference projected = *source;
      projected.predicate = false;
      projected.projection = true;
      projected.grouping = false;
      projected.ordering = false;
      projected.join = false;
      child.properties.push_back(std::move(projected));
    }
    child.predicates = root.predicates;
    for (const BoundPredicateExpression& predicate : root.predicates) {
      const BoundPropertyReference* source = property_for(
          predicate.binding_id, predicate.property_id);
      if (source == nullptr) {
        return Status::InvalidArgument(
            "physical planner", "multi-root predicate property is missing");
      }
      const auto existing = std::find_if(
          child.properties.begin(), child.properties.end(),
          [&](const BoundPropertyReference& property) {
            return property.property_id == source->property_id;
          });
      if (existing != child.properties.end()) {
        existing->predicate = true;
        continue;
      }
      BoundPropertyReference filtered = *source;
      filtered.predicate = true;
      filtered.projection = false;
      filtered.grouping = false;
      filtered.ordering = false;
      filtered.join = false;
      child.properties.push_back(std::move(filtered));
    }
    if (root.clause.relationships.empty()) {
      child.root_point_candidate = true;
    } else {
      child.fixed_expand_point_candidate = true;
    }
    if (root.clause.match.entity_id.has_value()) {
      child.root_exact_entity_id = root.clause.match.entity_id->integer_literal;
      child.root_exact_entity_parameter = root.clause.match.entity_id->parameter_name;
    }
    LogicalPlan child_logical;
    for (const BoundVariable* variable : root.variables) {
      FactDemandSet::VariableDemand demand;
      demand.variable = variable->variable;
      demand.kind = variable->kind;
      demand.direction = variable->direction;
      demand.entity_type = variable->entity_type;
      demand.entity_schema = variable->entity_schema;
      demand.non_nullable = variable->non_nullable;
      demand.existence = true;
      demand.binding_id = variable->binding_id;
      child_logical.demand.variables.push_back(std::move(demand));
    }
    for (const BoundPropertyReference& property : child.properties) {
      const auto demand = std::find_if(
          child_logical.demand.variables.begin(),
          child_logical.demand.variables.end(),
          [&](const FactDemandSet::VariableDemand& candidate) {
            return candidate.binding_id == property.binding_id;
          });
      if (demand == child_logical.demand.variables.end()) {
        return Status::Corruption(
            "physical planner", "child property demand has no owner");
      }
      if (property.predicate) {
        demand->predicate_properties.push_back(property.column);
      }
      if (property.projection || property.grouping || property.ordering) {
        demand->projection_properties.push_back(property.column);
      }
      child_logical.demand.property_names.push_back(
          property.column.logical_type);
    }
    child_logical.demand.existence_fact = true;
    auto child_plan = PlanPhysicalRootPoint(child, child_logical);
    if (!child_plan.ok()) return child_plan.status();
    root.plan = std::move(child_plan).ConsumeValueOrDie();
    if (root.plan->projections().size() != root.columns.size()) {
      return Status::Corruption(
          "physical planner", "multi-root child layout is incomplete");
    }
    for (size_t column = 0; column < root.columns.size(); ++column) {
      const PhysicalExpression& physical = root.plan->projections()[column];
      root.columns[column].layout.type = physical.type;
      root.columns[column].layout.nullable = physical.nullable;
      root.columns[column].layout.name = physical.output_name;
    }
  }

  const auto saturated_product = [](uint64_t left, uint64_t right) {
    if (left == 0 || right == 0) return uint64_t{0};
    if (left > std::numeric_limits<uint64_t>::max() / right) {
      return std::numeric_limits<uint64_t>::max();
    }
    return left * right;
  };
  const auto saturated_add = [](uint64_t left, uint64_t right) {
    if (left > std::numeric_limits<uint64_t>::max() - right) {
      return std::numeric_limits<uint64_t>::max();
    }
    return left + right;
  };
  std::vector<GraphOrderDecision> graph_order_decisions;
  graph_order_decisions.reserve(roots.size());
  for (const RootInput& root : roots) {
    GraphOrderEstimate graph_estimate;
    graph_estimate.index_available =
        root.estimate.index_available && !root.predicates.empty();
    graph_estimate.adjacency_available = !root.clause.relationships.empty() ||
        roots.size() > 1;
    graph_estimate.index_start_rows = root.estimate.rows == 0
        ? 1 : std::max<uint64_t>(1, root.estimate.rows / 4);
    graph_estimate.index_validation_rows = graph_estimate.index_start_rows;
    graph_estimate.adjacency_start_rows = root.estimate.rows;
    graph_estimate.adjacency_degree = root.clause.relationships.empty() ? 1 : 8;
    graph_order_decisions.push_back(ChooseGraphOrderDecision(
        graph_estimate, OptimizerBudget{4, false}));
  }

  std::vector<uint32_t> join_order;
  if (roots.size() <= 6) {
    struct DpEntry {
      bool populated = false;
      uint64_t cumulative_cost = 0;
      uint64_t accumulated_rows = 0;
      uint32_t cross_steps = 0;
      std::vector<uint32_t> order;
    };
    const uint64_t state_count = uint64_t{1} << roots.size();
    std::vector<DpEntry> dp(state_count);
    for (uint32_t root = 0; root < roots.size(); ++root) {
      const uint64_t graph_start_cost = graph_order_decisions[root].score;
      dp[uint64_t{1} << root] =
          DpEntry{true, saturated_add(roots[root].estimate.rows,
                                      graph_start_cost),
                  roots[root].estimate.rows, 0, {root}};
    }
    for (uint64_t mask = 1; mask < state_count; ++mask) {
      if (!dp[mask].populated) continue;
      for (uint32_t candidate = 0; candidate < roots.size(); ++candidate) {
        if ((mask & (uint64_t{1} << candidate)) != 0) continue;
        bool connected = false;
        for (uint32_t attached = 0; attached < roots.size(); ++attached) {
          connected = connected ||
              ((mask & (uint64_t{1} << attached)) != 0 &&
               adjacency[attached][candidate]);
        }
        const uint64_t product = saturated_product(
            dp[mask].accumulated_rows, roots[candidate].estimate.rows);
        const uint64_t cost = saturated_add(dp[mask].cumulative_cost, product);
        const uint32_t cross_steps =
            dp[mask].cross_steps + (connected ? 0U : 1U);
        std::vector<uint32_t> order = dp[mask].order;
        order.push_back(candidate);
        DpEntry& next = dp[mask | (uint64_t{1} << candidate)];
        if (!next.populated || cost < next.cumulative_cost ||
            (cost == next.cumulative_cost &&
             (cross_steps < next.cross_steps ||
              (cross_steps == next.cross_steps && order < next.order)))) {
          next = DpEntry{true, cost, product, cross_steps, std::move(order)};
        }
      }
    }
    const DpEntry& complete = dp.back();
    if (!complete.populated) {
      return Status::InvalidArgument(
          "physical planner", "multi-root join order is unavailable");
    }
    join_order = complete.order;
  } else {
    const auto startup_cost = [&](uint32_t input) {
      const uint64_t graph_cost = graph_order_decisions[input].score;
      return saturated_add(roots[input].estimate.rows, graph_cost);
    };
    const auto less_candidate = [&](uint32_t left, uint32_t right) {
      if (startup_cost(left) != startup_cost(right)) {
        return startup_cost(left) < startup_cost(right);
      }
      if (roots[left].estimate.rows != roots[right].estimate.rows) {
        return roots[left].estimate.rows < roots[right].estimate.rows;
      }
      return roots[left].variable->binding_id < roots[right].variable->binding_id;
    };
    uint32_t first = 0;
    for (uint32_t index = 1; index < roots.size(); ++index) {
      if (less_candidate(index, first)) first = index;
    }
    join_order.push_back(first);
    std::vector<bool> attached(roots.size(), false);
    attached[first] = true;
    while (join_order.size() != roots.size()) {
      std::optional<uint32_t> best_connected;
      std::optional<uint32_t> best_disconnected;
      for (uint32_t candidate = 0; candidate < roots.size(); ++candidate) {
        if (attached[candidate]) continue;
        bool connected = false;
        for (uint32_t input = 0; input < roots.size(); ++input) {
          connected = connected ||
              (attached[input] && adjacency[input][candidate]);
        }
        std::optional<uint32_t>& best =
            connected ? best_connected : best_disconnected;
        if (!best.has_value() || less_candidate(candidate, *best)) {
          best = candidate;
        }
      }
      const std::optional<uint32_t> best =
          best_connected.has_value() ? best_connected : best_disconnected;
      if (!best.has_value()) {
        return Status::InvalidArgument(
            "physical planner", "multi-root join order is incomplete");
      }
      attached[*best] = true;
      join_order.push_back(*best);
    }
  }

  const auto active_from_root = [](const RootInput& root) {
    std::vector<ActiveColumn> columns;
    columns.reserve(root.columns.size());
    for (const RootColumn& column : root.columns) {
      columns.push_back(ActiveColumn{
          column.layout, column.projection.kind, column.final_output});
    }
    return columns;
  };
  const auto endpoint_matches = [&](const ActiveColumn& column,
                                    const LogicalJoinEndpoint& endpoint) {
    if (column.layout.binding_id != endpoint.binding_id) return false;
    if (!endpoint.identity) {
      return !column.layout.relationship_identity &&
          column.kind == ReturnExpressionKind::kProperty &&
          column.layout.property_id == endpoint.property_id;
    }
    const auto variable = std::find_if(
        statement.variables.begin(), statement.variables.end(),
        [&](const BoundVariable& candidate) {
          return candidate.binding_id == endpoint.binding_id;
        });
    const bool relationship_identity = variable != statement.variables.end() &&
        variable->kind == BoundGraphKind::kRelationship;
    return column.kind == ReturnExpressionKind::kBinding &&
        !column.layout.property_id.has_value() &&
        column.layout.relationship_identity == relationship_identity;
  };
  const auto find_endpoint = [&](const std::vector<ActiveColumn>& columns,
                                 const LogicalJoinEndpoint& endpoint)
      -> std::optional<uint32_t> {
    for (uint32_t index = 0; index < columns.size(); ++index) {
      if (endpoint_matches(columns[index], endpoint)) return index;
    }
    return std::nullopt;
  };
  const auto needed_for_future = [&](const ActiveColumn& column,
                                     const std::vector<bool>& attached) {
    if (column.final_output) return true;
    for (const LogicalJoinEdge& edge : logical_plan.join_edges) {
      const LogicalJoinEndpoint* own = nullptr;
      const LogicalJoinEndpoint* other = nullptr;
      if (endpoint_matches(column, edge.left)) {
        own = &edge.left;
        other = &edge.right;
      } else if (endpoint_matches(column, edge.right)) {
        own = &edge.right;
        other = &edge.left;
      }
      if (own == nullptr) continue;
      const auto other_input = input_by_binding.find(other->binding_id);
      if (other_input != input_by_binding.end() &&
          !attached[other_input->second]) {
        return true;
      }
    }
    return false;
  };

  std::vector<PhysicalMultiHashJoinStep> steps;
  std::vector<ActiveColumn> active = active_from_root(roots[join_order.front()]);
  std::vector<bool> attached(roots.size(), false);
  attached[join_order.front()] = true;
  PhysicalCardinalityEstimate accumulated_estimate =
      roots[join_order.front()].estimate;
  for (size_t order_index = 1; order_index < join_order.size(); ++order_index) {
    const uint32_t input_index = join_order[order_index];
    const std::vector<ActiveColumn> input_columns =
        active_from_root(roots[input_index]);
    PhysicalMultiHashJoinStep step;
    step.join = PhysicalOperatorSpec{
        OperatorId{static_cast<uint32_t>(order_index)},
        PhysicalOperatorKind::kCrossJoin, {}, {}};
    step.input_index = input_index;
    step.accumulated_estimate = accumulated_estimate;
    step.input_estimate = roots[input_index].estimate;
    step.build_side = accumulated_estimate.rows < step.input_estimate.rows
        ? PhysicalHashJoinBuildSide::kLeft
        : PhysicalHashJoinBuildSide::kRight;
    for (const LogicalJoinEdge& edge : logical_plan.join_edges) {
      const LogicalJoinEndpoint* accumulated_endpoint = nullptr;
      const LogicalJoinEndpoint* input_endpoint = nullptr;
      if (input_by_binding.count(edge.left.binding_id) != 0 &&
          input_by_binding.at(edge.left.binding_id) == input_index &&
          input_by_binding.count(edge.right.binding_id) != 0 &&
          attached[input_by_binding.at(edge.right.binding_id)]) {
        accumulated_endpoint = &edge.right;
        input_endpoint = &edge.left;
      } else if (input_by_binding.count(edge.right.binding_id) != 0 &&
                 input_by_binding.at(edge.right.binding_id) == input_index &&
                 input_by_binding.count(edge.left.binding_id) != 0 &&
                 attached[input_by_binding.at(edge.left.binding_id)]) {
        accumulated_endpoint = &edge.left;
        input_endpoint = &edge.right;
      }
      if (accumulated_endpoint == nullptr) continue;
      const auto accumulated_column =
          find_endpoint(active, *accumulated_endpoint);
      const auto input_column = find_endpoint(input_columns, *input_endpoint);
      if (!accumulated_column.has_value() || !input_column.has_value()) {
        return Status::Corruption(
            "physical planner", "multi-root future join key was dropped");
      }
      step.accumulated_key_columns.push_back(*accumulated_column);
      step.input_key_columns.push_back(*input_column);
    }
    step.join.kind = step.accumulated_key_columns.empty()
        ? PhysicalOperatorKind::kCrossJoin
        : PhysicalOperatorKind::kHashJoin;

    attached[input_index] = true;
    std::vector<ActiveColumn> next_active;
    for (uint32_t column = 0; column < active.size(); ++column) {
      if (!needed_for_future(active[column], attached)) continue;
      step.outputs.push_back(PhysicalHashJoinPlan::Output{
          true, column, active[column].layout.type,
          active[column].layout.nullable, active[column].layout.name});
      step.output_layout.push_back(active[column].layout);
      next_active.push_back(active[column]);
    }
    for (uint32_t column = 0; column < input_columns.size(); ++column) {
      if (!needed_for_future(input_columns[column], attached)) continue;
      step.outputs.push_back(PhysicalHashJoinPlan::Output{
          false, column, input_columns[column].layout.type,
          input_columns[column].layout.nullable,
          input_columns[column].layout.name});
      step.output_layout.push_back(input_columns[column].layout);
      next_active.push_back(input_columns[column]);
    }
    accumulated_estimate.rows = saturated_product(
        accumulated_estimate.rows, step.input_estimate.rows);
    accumulated_estimate.confidence_per_mille = std::min(
        accumulated_estimate.confidence_per_mille,
        step.input_estimate.confidence_per_mille);
    accumulated_estimate.conservative = accumulated_estimate.conservative ||
        step.input_estimate.conservative;
    active = std::move(next_active);
    steps.push_back(std::move(step));
  }

  std::vector<std::string> output_names;
  std::vector<uint32_t> final_output_columns;
  for (const BoundProjectionExpression& projection : statement.projections) {
    output_names.push_back(projection.output_name);
    const auto found = std::find_if(
        active.begin(), active.end(), [&](const ActiveColumn& column) {
          return column.layout.binding_id == projection.binding_id &&
              column.kind == projection.kind &&
              column.layout.property_id == projection.property_id &&
              column.layout.relationship_identity ==
                  projection.relationship_identity;
        });
    if (found == active.end()) {
      return Status::Corruption(
          "physical planner", "multi-root final projection was dropped");
    }
    final_output_columns.push_back(
        static_cast<uint32_t>(found - active.begin()));
  }

  const TcypherStatement& result =
      result_statement == nullptr ? statement.syntax : *result_statement;
  uint32_t next_operator = static_cast<uint32_t>(steps.size()) + 1;
  std::optional<PhysicalAggregateSinkSpec> aggregate_sink =
      BuildPhysicalAggregateSink(result, OperatorId{next_operator});
  std::optional<PhysicalSortSinkSpec> sort_sink;
  std::vector<PhysicalOperatorSpec> post_join_operators;
  if (aggregate_sink.has_value()) {
    post_join_operators.push_back(aggregate_sink->op);
    ++next_operator;
  }
  if (result.distinct) {
    post_join_operators.push_back(PhysicalOperatorSpec{
        OperatorId{next_operator++}, PhysicalOperatorKind::kDistinct, {}, {}});
  }
  sort_sink = BuildPhysicalSortSink(result, OperatorId{next_operator});
  if (sort_sink.has_value()) post_join_operators.push_back(sort_sink->op);

  std::vector<PipelineDescriptor> pipelines;
  pipelines.reserve(roots.size() + steps.size());
  for (uint32_t input = 0; input < roots.size(); ++input) {
    pipelines.push_back(PipelineDescriptor{PipelineId{input + 1}, {}, {}});
  }
  PipelineId previous_pipeline{};
  for (uint32_t step_index = 0; step_index < steps.size(); ++step_index) {
    const PipelineId pipeline_id{
        static_cast<uint32_t>(roots.size()) + step_index + 1};
    std::vector<OperatorId> operators{steps[step_index].join.id};
    if (step_index + 1 == steps.size()) {
      for (const PhysicalOperatorSpec& op : post_join_operators) {
        operators.push_back(op.id);
      }
    }
    const PipelineId accumulated_dependency = step_index == 0
        ? PipelineId{join_order.front() + 1} : previous_pipeline;
    pipelines.push_back(PipelineDescriptor{
        pipeline_id, std::move(operators),
        {accumulated_dependency, PipelineId{steps[step_index].input_index + 1}}});
    previous_pipeline = pipeline_id;
  }

  const GraphOrder selected_graph_order = graph_order_decisions.empty()
      ? GraphOrder::kAdjacencyFirst
      : graph_order_decisions[join_order.front()].order;
  uint64_t fingerprint = 1469598103934665603ULL;
  const auto mix = [&](uint64_t value) {
    for (uint32_t byte = 0; byte < 8; ++byte) {
      fingerprint ^= static_cast<uint8_t>(value >> (byte * 8));
      fingerprint *= 1099511628211ULL;
    }
  };
  const auto mix_string = [&](const std::string& value) {
    mix(value.size());
    for (unsigned char byte : value) {
      fingerprint ^= byte;
      fingerprint *= 1099511628211ULL;
    }
  };
  mix(statistics_snapshot_id);
  for (const RootInput& root : roots) {
    mix(root.plan->plan_id());
    mix(root.snapshot_seq);
    for (const RootColumn& root_column : root.columns) {
      const PhysicalMultiJoinColumn& column = root_column.layout;
      mix(column.binding_id.value);
      mix(static_cast<uint8_t>(column.kind));
      mix(column.property_id.has_value());
      if (column.property_id.has_value()) mix(column.property_id->value);
      mix(static_cast<uint8_t>(column.type));
      mix(column.nullable);
      mix_string(column.name);
      mix(column.relationship_identity);
    }
  }
  for (uint32_t input : join_order) mix(input);
  mix(static_cast<uint8_t>(selected_graph_order));
  for (const PhysicalMultiHashJoinStep& step : steps) {
    mix(step.join.id.value);
    mix(static_cast<uint8_t>(step.join.kind));
    mix(step.input_index);
    mix(static_cast<uint8_t>(step.build_side));
    mix(step.accumulated_estimate.rows);
    mix(step.accumulated_estimate.confidence_per_mille);
    mix(step.accumulated_estimate.conservative);
    mix(step.input_estimate.rows);
    mix(step.input_estimate.confidence_per_mille);
    mix(step.input_estimate.conservative);
    for (uint32_t column : step.accumulated_key_columns) mix(column);
    for (uint32_t column : step.input_key_columns) mix(column);
    for (const PhysicalHashJoinPlan::Output& output : step.outputs) {
      mix(output.from_left);
      mix(output.column);
      mix(static_cast<uint8_t>(output.type));
      mix(output.nullable);
      mix_string(output.name);
    }
    for (const PhysicalMultiJoinColumn& column : step.output_layout) {
      mix(column.binding_id.value);
      mix(static_cast<uint8_t>(column.kind));
      mix(column.property_id.has_value());
      if (column.property_id.has_value()) mix(column.property_id->value);
      mix(static_cast<uint8_t>(column.type));
      mix(column.nullable);
      mix_string(column.name);
      mix(column.relationship_identity);
    }
  }
  for (const std::string& name : output_names) mix_string(name);
  for (uint32_t column : final_output_columns) mix(column);
  MixResultOperatorFingerprint(
      &fingerprint, aggregate_sink, result.distinct, sort_sink);
  if (fingerprint == 0) fingerprint = 1;

  std::vector<std::shared_ptr<const PhysicalPlan>> inputs;
  std::vector<std::vector<PhysicalMultiJoinColumn>> input_layouts;
  std::vector<uint64_t> input_snapshot_seqs;
  inputs.reserve(roots.size());
  input_layouts.reserve(roots.size());
  input_snapshot_seqs.reserve(roots.size());
  for (RootInput& root : roots) {
    inputs.push_back(std::move(root.plan));
    input_snapshot_seqs.push_back(root.snapshot_seq);
    std::vector<PhysicalMultiJoinColumn> layout;
    layout.reserve(root.columns.size());
    for (RootColumn& column : root.columns) {
      layout.push_back(std::move(column.layout));
    }
    input_layouts.push_back(std::move(layout));
  }
  auto plan = std::make_shared<const PhysicalMultiHashJoinPlan>(
      PhysicalMultiHashJoinPlan{
          fingerprint, statistics_snapshot_id, selected_graph_order,
          std::move(inputs),
          std::move(input_layouts), std::move(input_snapshot_seqs),
          std::move(join_order), std::move(steps),
          std::move(output_names), std::move(final_output_columns),
          std::move(aggregate_sink), std::move(sort_sink),
          std::move(post_join_operators), std::move(pipelines)});
  const Status valid = ValidatePhysicalMultiHashJoinPlan(*plan);
  if (!valid.ok()) return valid;
  return plan;
}

Status ValidatePhysicalPlan(const PhysicalPlan& plan) {
  if (plan.plan_id() == 0 || plan.binding_id().value == 0 ||
      plan.temporal_context().value == 0 || plan.pipelines().empty() ||
      plan.operators().empty()) {
    return Status::InvalidArgument("physical plan", "plan identity or pipeline is missing");
  }
  const auto valid_range = [](const auto& range) {
    return range.has_value() && range->first < range->second;
  };
  PhysicalOperatorKind expected_scan = PhysicalOperatorKind::kTemporalPointScan;
  switch (plan.temporal_mode()) {
    case PhysicalTemporalMode::kPoint:
      if (plan.valid_time_range().has_value() ||
          plan.system_time_range().has_value()) {
        return Status::InvalidArgument(
            "physical plan", "point mode carries temporal range bounds");
      }
      break;
    case PhysicalTemporalMode::kValidTimeRange:
      expected_scan = PhysicalOperatorKind::kTemporalRangeScan;
      if (!valid_range(plan.valid_time_range()) ||
          plan.system_time_range().has_value()) {
        return Status::InvalidArgument(
            "physical plan", "valid-time range mode has invalid bounds");
      }
      break;
    case PhysicalTemporalMode::kValidTimeChanges:
      expected_scan = PhysicalOperatorKind::kChangeScan;
      if (!valid_range(plan.valid_time_range()) ||
          plan.system_time_range().has_value()) {
        return Status::InvalidArgument(
            "physical plan", "valid-time change mode has invalid bounds");
      }
      break;
    case PhysicalTemporalMode::kSystemTimeChanges:
      expected_scan = PhysicalOperatorKind::kChangeScan;
      if (!valid_range(plan.system_time_range()) ||
          (plan.valid_time_range().has_value() &&
           !valid_range(plan.valid_time_range()))) {
        return Status::InvalidArgument(
            "physical plan", "system-time change mode has invalid bounds");
      }
      break;
  }
  if (plan.operators().front().kind != expected_scan) {
    return Status::InvalidArgument(
        "physical plan", "temporal mode and source operator disagree");
  }
  if (plan.temporal_mode() == PhysicalTemporalMode::kValidTimeRange) {
    if (plan.operators().size() < 4 ||
        plan.operators()[1].kind != PhysicalOperatorKind::kIntervalDerive ||
        plan.operators()[2].kind != PhysicalOperatorKind::kIntervalAlign ||
        plan.operators()[3].kind != PhysicalOperatorKind::kTemporalCoalesce) {
      return Status::InvalidArgument(
          "physical plan", "valid-time range operator order is invalid");
    }
  }
  std::set<SlotId> declared;
  std::map<SlotId, const SlotDescriptor*> descriptors;
  for (const SlotDescriptor& slot : plan.slots()) {
    const bool binding_is_valid = !plan.expand().has_value()
        ? slot.binding == plan.binding_id()
        : std::any_of(plan.expand_steps().begin(), plan.expand_steps().end(),
                      [&slot](const PhysicalExpandSpec& step) {
                        return slot.binding == step.source_binding ||
                            slot.binding == step.relationship_binding ||
                            slot.binding == step.target_binding;
                      });
    if (slot.id.value == 0 || !binding_is_valid || !declared.insert(slot.id).second) {
      return Status::InvalidArgument("physical plan", "duplicate or zero slot declaration");
    }
    descriptors.emplace(slot.id, &slot);
  }
  if (plan.expand().has_value()) {
    const PhysicalExpandSpec& expand = *plan.expand();
    if ((plan.temporal_mode() != PhysicalTemporalMode::kPoint &&
         plan.temporal_mode() != PhysicalTemporalMode::kValidTimeRange &&
         plan.temporal_mode() != PhysicalTemporalMode::kValidTimeChanges &&
         plan.temporal_mode() != PhysicalTemporalMode::kSystemTimeChanges) ||
        expand.source_binding.value == 0 || expand.relationship_binding.value == 0 ||
        expand.target_binding.value == 0 || expand.source_binding != plan.binding_id() ||
        expand.source_binding == expand.target_binding ||
        expand.min_hops == 0 || expand.min_hops > expand.max_hops ||
        (expand.direction != EntityType::EdgeOut && expand.direction != EntityType::EdgeIn)) {
      return Status::InvalidArgument("physical plan", "expand identity or temporal mode is invalid");
    }
    const auto source = descriptors.find(expand.source_slot);
    const auto target = descriptors.find(expand.target_slot);
    const auto edge_type = descriptors.find(expand.edge_type_slot);
    const auto edge_id = descriptors.find(expand.edge_id_slot);
    const auto valid_from = descriptors.find(expand.valid_from_slot);
    const auto commit_seq = descriptors.find(expand.commit_seq_slot);
    const auto operation = descriptors.find(expand.operation_slot);
    const auto system_time = descriptors.find(expand.system_time_slot);
    const auto valid_to = descriptors.find(expand.valid_to_slot);
    const auto path = expand.path_slot.value == 0
        ? descriptors.end() : descriptors.find(expand.path_slot);
    if (source == descriptors.end() || target == descriptors.end() ||
        edge_type == descriptors.end() || edge_id == descriptors.end() ||
        valid_from == descriptors.end() || commit_seq == descriptors.end() ||
        operation == descriptors.end() || system_time == descriptors.end() ||
        valid_to == descriptors.end() ||
        source->second->binding != expand.source_binding ||
        target->second->binding != expand.target_binding ||
        edge_type->second->binding != expand.relationship_binding ||
        edge_id->second->binding != expand.relationship_binding ||
        valid_from->second->binding != expand.relationship_binding ||
        commit_seq->second->binding != expand.relationship_binding ||
        operation->second->binding != expand.relationship_binding ||
        system_time->second->binding != expand.relationship_binding ||
        valid_to->second->binding != expand.relationship_binding ||
        source->second->type != PhysicalType::kInt64 ||
        target->second->type != PhysicalType::kInt64 ||
        edge_type->second->type != PhysicalType::kInt32 ||
        edge_id->second->type != PhysicalType::kInt64 ||
        valid_from->second->type != PhysicalType::kTimestamp64 ||
        commit_seq->second->type != PhysicalType::kInt64 ||
        operation->second->type != PhysicalType::kInt32 || source->second->nullable ||
        system_time->second->type != PhysicalType::kTimestamp64 ||
        valid_to->second->type != PhysicalType::kTimestamp64 ||
        target->second->nullable || edge_type->second->nullable || edge_id->second->nullable ||
        valid_from->second->nullable || commit_seq->second->nullable ||
        operation->second->nullable || system_time->second->nullable ||
        valid_to->second->nullable ||
        (expand.path_slot.value != 0 &&
         (path == descriptors.end() ||
          path->second->binding != expand.relationship_binding ||
          path->second->type != PhysicalType::kBinary ||
          path->second->nullable))) {
      return Status::InvalidArgument("physical plan", "expand endpoint slots are invalid");
    }
    const auto operator_found = std::find_if(
        plan.operators().begin(), plan.operators().end(), [&expand](const PhysicalOperatorSpec& op) {
          return op.kind == PhysicalOperatorKind::kExpand;
        });
    std::vector<SlotId> expected_outputs{
        expand.target_slot, expand.edge_type_slot, expand.edge_id_slot,
        expand.valid_from_slot, expand.commit_seq_slot, expand.operation_slot,
        expand.system_time_slot, expand.valid_to_slot};
    if (expand.path_slot.value != 0) expected_outputs.push_back(expand.path_slot);
    if (operator_found == plan.operators().end() ||
        operator_found->required_slots != std::vector<SlotId>{expand.source_slot} ||
        operator_found->produced_slots != expected_outputs) {
      return Status::InvalidArgument("physical plan", "expand operator topology is invalid");
    }
    if (plan.expand_steps().empty()) {
      return Status::InvalidArgument(
          "physical plan", "expand plan has no ordered steps");
    }
    size_t expand_operator_count = 0;
    for (const PhysicalOperatorSpec& op : plan.operators()) {
      if (op.kind == PhysicalOperatorKind::kExpand) ++expand_operator_count;
    }
    if (expand_operator_count != plan.expand_steps().size()) {
      return Status::InvalidArgument(
          "physical plan", "expand step and operator counts differ");
    }
    for (size_t index = 0; index < plan.expand_steps().size(); ++index) {
      const PhysicalExpandSpec& step = plan.expand_steps()[index];
      if (step.source_binding.value == 0 ||
          step.relationship_binding.value == 0 ||
          step.target_binding.value == 0 ||
          step.source_binding == step.target_binding || step.min_hops == 0 ||
          step.min_hops > step.max_hops ||
          (step.direction != EntityType::EdgeOut &&
           step.direction != EntityType::EdgeIn) ||
          ((step.min_hops != 1 || step.max_hops != 1) &&
           step.path_slot.value == 0) ||
          (index == 0 && step.source_binding != plan.binding_id()) ||
          (index > 0 &&
           (step.source_binding != plan.expand_steps()[index - 1].target_binding ||
            step.source_slot != plan.expand_steps()[index - 1].target_slot))) {
        return Status::InvalidArgument(
            "physical plan", "ordered expand step identity is invalid");
      }
      const auto source_slot = descriptors.find(step.source_slot);
      const auto target_slot = descriptors.find(step.target_slot);
      const auto edge_type_slot = descriptors.find(step.edge_type_slot);
      const auto edge_id_slot = descriptors.find(step.edge_id_slot);
      const auto valid_from_slot = descriptors.find(step.valid_from_slot);
      const auto commit_seq_slot = descriptors.find(step.commit_seq_slot);
      const auto operation_slot = descriptors.find(step.operation_slot);
      const auto system_time_slot = descriptors.find(step.system_time_slot);
      const auto valid_to_slot = descriptors.find(step.valid_to_slot);
      const auto path_slot = step.path_slot.value == 0
          ? descriptors.end()
          : descriptors.find(step.path_slot);
      if (source_slot == descriptors.end() || target_slot == descriptors.end() ||
          edge_type_slot == descriptors.end() ||
          edge_id_slot == descriptors.end() ||
          valid_from_slot == descriptors.end() ||
          commit_seq_slot == descriptors.end() ||
          operation_slot == descriptors.end() ||
          system_time_slot == descriptors.end() ||
          valid_to_slot == descriptors.end() ||
          source_slot->second->binding != step.source_binding ||
          source_slot->second->type != PhysicalType::kInt64 ||
          source_slot->second->nullable ||
          target_slot->second->binding != step.target_binding ||
          target_slot->second->type != PhysicalType::kInt64 ||
          target_slot->second->nullable ||
          edge_type_slot->second->binding != step.relationship_binding ||
          edge_type_slot->second->type != PhysicalType::kInt32 ||
          edge_type_slot->second->nullable ||
          edge_id_slot->second->binding != step.relationship_binding ||
          edge_id_slot->second->type != PhysicalType::kInt64 ||
          edge_id_slot->second->nullable ||
          valid_from_slot->second->binding != step.relationship_binding ||
          valid_from_slot->second->type != PhysicalType::kTimestamp64 ||
          valid_from_slot->second->nullable ||
          commit_seq_slot->second->binding != step.relationship_binding ||
          commit_seq_slot->second->type != PhysicalType::kInt64 ||
          commit_seq_slot->second->nullable ||
          operation_slot->second->binding != step.relationship_binding ||
          operation_slot->second->type != PhysicalType::kInt32 ||
          operation_slot->second->nullable ||
          system_time_slot->second->binding != step.relationship_binding ||
          system_time_slot->second->type != PhysicalType::kTimestamp64 ||
          system_time_slot->second->nullable ||
          valid_to_slot->second->binding != step.relationship_binding ||
          valid_to_slot->second->type != PhysicalType::kTimestamp64 ||
          valid_to_slot->second->nullable ||
          (step.path_slot.value != 0 &&
           (path_slot == descriptors.end() ||
            path_slot->second->binding != step.relationship_binding ||
            path_slot->second->type != PhysicalType::kBinary ||
            path_slot->second->nullable))) {
        return Status::InvalidArgument(
            "physical plan", "ordered expand step slots are invalid");
      }
      std::vector<SlotId> produced{
          step.target_slot, step.edge_type_slot, step.edge_id_slot,
          step.valid_from_slot, step.commit_seq_slot, step.operation_slot,
          step.system_time_slot, step.valid_to_slot};
      if (step.path_slot.value != 0) produced.push_back(step.path_slot);
      const auto matching_operator = std::find_if(
          plan.operators().begin(), plan.operators().end(),
          [&step, &produced](const PhysicalOperatorSpec& op) {
            return op.kind == PhysicalOperatorKind::kExpand &&
                op.required_slots == std::vector<SlotId>{step.source_slot} &&
                op.produced_slots == produced;
          });
      if (matching_operator == plan.operators().end()) {
        return Status::InvalidArgument(
            "physical plan", "ordered expand operator topology is invalid");
      }
    }
  }
  std::map<SlotId, OperatorId> producers;
  std::set<OperatorId> operator_ids;
  for (const PhysicalOperatorSpec& op : plan.operators()) {
    if (op.id.value == 0 || !operator_ids.insert(op.id).second) {
      return Status::InvalidArgument("physical plan", "duplicate or zero operator id");
    }
    for (SlotId required : op.required_slots) {
      if (declared.count(required) == 0) {
        return Status::InvalidArgument("physical plan", "operator requires an unknown slot");
      }
    }
    for (SlotId produced : op.produced_slots) {
      if (declared.count(produced) == 0 || producers.count(produced) != 0) {
        return Status::InvalidArgument("physical plan", "duplicate or unknown slot producer");
      }
      producers.emplace(produced, op.id);
    }
  }
  for (SlotId slot : declared) {
    if (producers.count(slot) == 0) {
      return Status::InvalidArgument("physical plan", "slot has no producer");
    }
  }
  std::set<PipelineId> pipeline_ids;
  std::map<PipelineId, const PipelineDescriptor*> pipeline_by_id;
  std::map<OperatorId, std::pair<PipelineId, size_t>> operator_locations;
  std::set<OperatorId> scheduled;
  for (const PipelineDescriptor& pipeline : plan.pipelines()) {
    if (pipeline.id.value == 0 || !pipeline_ids.insert(pipeline.id).second) {
      return Status::InvalidArgument("physical plan", "duplicate or zero pipeline id");
    }
    pipeline_by_id.emplace(pipeline.id, &pipeline);
    for (size_t position = 0; position < pipeline.operators.size(); ++position) {
      const OperatorId op = pipeline.operators[position];
      if (operator_ids.count(op) == 0 || !scheduled.insert(op).second) {
        return Status::InvalidArgument("physical plan", "pipeline operator is invalid");
      }
      operator_locations.emplace(op, std::make_pair(pipeline.id, position));
    }
  }
  if (scheduled != operator_ids) {
    return Status::InvalidArgument("physical plan", "operator is absent from pipelines");
  }
  for (const PipelineDescriptor& pipeline : plan.pipelines()) {
    std::set<PipelineId> dependencies;
    for (PipelineId dependency : pipeline.dependencies) {
      if (dependency == pipeline.id || pipeline_ids.count(dependency) == 0 ||
          !dependencies.insert(dependency).second) {
        return Status::InvalidArgument("physical plan", "pipeline dependency is invalid");
      }
    }
  }
  std::map<PipelineId, uint8_t> visit_state;
  std::function<bool(PipelineId)> has_dependency_cycle = [&](PipelineId pipeline_id) {
    uint8_t& state = visit_state[pipeline_id];
    if (state == 1) return true;
    if (state == 2) return false;
    state = 1;
    for (PipelineId dependency : pipeline_by_id.at(pipeline_id)->dependencies) {
      if (has_dependency_cycle(dependency)) return true;
    }
    state = 2;
    return false;
  };
  for (PipelineId pipeline_id : pipeline_ids) {
    if (has_dependency_cycle(pipeline_id)) {
      return Status::InvalidArgument("physical plan", "pipeline dependencies are cyclic");
    }
  }
  const auto has_dependency_path = [&](PipelineId pipeline_id,
                                       PipelineId producer_pipeline) {
    std::set<PipelineId> visited;
    std::vector<PipelineId> pending{pipeline_id};
    while (!pending.empty()) {
      const PipelineId current = pending.back();
      pending.pop_back();
      if (!visited.insert(current).second) continue;
      for (PipelineId dependency : pipeline_by_id.at(current)->dependencies) {
        if (dependency == producer_pipeline) return true;
        pending.push_back(dependency);
      }
    }
    return false;
  };
  for (const PhysicalOperatorSpec& op : plan.operators()) {
    const auto consumer = operator_locations.at(op.id);
    for (SlotId required : op.required_slots) {
      const auto producer = operator_locations.at(producers.at(required));
      if (producer.first == consumer.first) {
        if (producer.second >= consumer.second) {
          return Status::InvalidArgument("physical plan", "slot producer does not precede consumer");
        }
      } else if (!has_dependency_path(consumer.first, producer.first)) {
        return Status::InvalidArgument("physical plan", "slot producer pipeline is not a dependency");
      }
    }
  }
  for (const PhysicalPredicate& predicate : plan.predicates()) {
    const auto slot = descriptors.find(predicate.slot);
    if (slot == descriptors.end() || slot->second->type != predicate.type ||
        predicate.column.physical_type != predicate.type ||
        slot->second->nullable != predicate.nullable) {
      return Status::InvalidArgument("physical plan", "predicate slot type mismatch");
    }
    const auto has_type = [&](const Value& value) {
      return value.type() == predicate.type;
    };
    if (!std::all_of(predicate.values.begin(), predicate.values.end(), has_type) ||
        (predicate.lower_bound.has_value() && !has_type(*predicate.lower_bound)) ||
        (predicate.upper_bound.has_value() && !has_type(*predicate.upper_bound))) {
      return Status::InvalidArgument("physical plan", "predicate literal type mismatch");
    }
    const bool has_bounds = predicate.lower_bound.has_value() ||
        predicate.upper_bound.has_value();
    switch (predicate.kind) {
      case PhysicalPredicateKind::kEquality:
        if (predicate.values.size() != 1 || has_bounds) {
          return Status::InvalidArgument("physical plan", "equality predicate shape is invalid");
        }
        break;
      case PhysicalPredicateKind::kIn:
        if (predicate.values.empty() || has_bounds) {
          return Status::InvalidArgument("physical plan", "IN predicate shape is invalid");
        }
        break;
      case PhysicalPredicateKind::kRange:
        if (!predicate.values.empty() || !has_bounds) {
          return Status::InvalidArgument("physical plan", "range predicate shape is invalid");
        }
        break;
      case PhysicalPredicateKind::kPrefix:
        if (predicate.type != PhysicalType::kString ||
            predicate.values.size() != 1 || has_bounds) {
          return Status::InvalidArgument("physical plan", "prefix predicate shape is invalid");
        }
        break;
    }
  }
  const auto validate_property = [&](const PhysicalPropertySlot& property) {
    const auto slot = descriptors.find(property.slot);
    return slot != descriptors.end() &&
        slot->second->type == property.column.physical_type;
  };
  for (const PhysicalPropertySlot& property : plan.predicate_properties()) {
    if (!validate_property(property)) {
      return Status::InvalidArgument("physical plan", "predicate gather slot type mismatch");
    }
  }
  for (const PhysicalPropertySlot& property : plan.projection_properties()) {
    if (!validate_property(property)) {
      return Status::InvalidArgument("physical plan", "projection gather slot type mismatch");
    }
  }
  for (const PhysicalExpression& expression : plan.projections()) {
    const auto slot = descriptors.find(expression.referenced_slot);
    const auto output = descriptors.find(expression.output_slot);
    if (slot == descriptors.end() || output == descriptors.end() ||
        output->second->type != expression.type ||
        output->second->nullable != expression.nullable) {
      return Status::InvalidArgument("physical plan", "projection references an unknown slot");
    }
    if (expression.kind == PhysicalExpressionKind::kOperationName) {
      if (slot->second->type != PhysicalType::kInt32 ||
          expression.type != PhysicalType::kString || expression.nullable) {
        return Status::InvalidArgument("physical plan", "operation projection type mismatch");
      }
    } else if (expression.kind == PhysicalExpressionKind::kRelationshipBinding) {
      const bool matches_expand_step = std::any_of(
          plan.expand_steps().begin(), plan.expand_steps().end(),
          [&expression](const PhysicalExpandSpec& step) {
            return expression.relationship_slots == std::vector<SlotId>{
                step.source_slot, step.target_slot, step.edge_type_slot, step.edge_id_slot,
                step.valid_from_slot, step.commit_seq_slot};
          });
      if (!matches_expand_step || expression.type != PhysicalType::kBinary ||
          expression.nullable) {
        return Status::InvalidArgument("physical plan", "relationship projection shape is invalid");
      }
    } else if (expression.kind == PhysicalExpressionKind::kPathBinding) {
      const bool matches_path = std::any_of(
          plan.expand_steps().begin(), plan.expand_steps().end(),
          [&expression](const PhysicalExpandSpec& step) {
            return step.path_slot.value != 0 &&
                expression.referenced_slot == step.path_slot;
          });
      if (!matches_path || expression.type != PhysicalType::kBinary ||
          expression.nullable) {
        return Status::InvalidArgument(
            "physical plan", "path projection shape is invalid");
      }
    } else if (slot->second->type != expression.type ||
               slot->second->nullable != expression.nullable) {
      return Status::InvalidArgument("physical plan", "projection slot type mismatch");
    }
  }
  const Status result_operators = ValidateResultOperators(
      plan.post_result_operators(), plan.aggregate_sink(), plan.sort_sink(),
      static_cast<uint32_t>(plan.projections().size()), &operator_ids);
  if (!result_operators.ok()) return result_operators;
  return Status::OK();
}

Status ValidatePhysicalHashJoinPlan(const PhysicalHashJoinPlan& plan) {
  if (plan.plan_id == 0 || !plan.left || !plan.right ||
      plan.join.id.value == 0 ||
      plan.join.kind != PhysicalOperatorKind::kHashJoin ||
      plan.output_names.empty() || plan.outputs.size() != plan.output_names.size() ||
      plan.left_key_columns.empty() ||
      plan.left_key_columns.size() != plan.right_key_columns.size() ||
      plan.pipelines.size() != 3 ||
      (plan.build_side != PhysicalHashJoinBuildSide::kLeft &&
       plan.build_side != PhysicalHashJoinBuildSide::kRight) ||
      plan.left_estimate.confidence_per_mille > 1000 ||
      plan.right_estimate.confidence_per_mille > 1000) {
    return Status::InvalidArgument("physical hash join", "join plan shape is invalid");
  }
  const Status left = ValidatePhysicalPlan(*plan.left);
  if (!left.ok()) return left;
  const Status right = ValidatePhysicalPlan(*plan.right);
  if (!right.ok()) return right;
  for (size_t index = 0; index < plan.left_key_columns.size(); ++index) {
    if (plan.left_key_columns[index] >= plan.left->projections().size() ||
        plan.right_key_columns[index] >= plan.right->projections().size()) {
      return Status::InvalidArgument(
          "physical hash join", "join key column is invalid");
    }
  }
  for (const PhysicalHashJoinPlan::Output& output : plan.outputs) {
    const size_t child_columns = output.from_left
        ? plan.left->projections().size() : plan.right->projections().size();
    if (output.column >= child_columns || output.name.empty()) {
      return Status::InvalidArgument(
          "physical hash join", "join output mapping is invalid");
    }
  }
  std::set<OperatorId> operator_ids{plan.join.id};
  const Status result_operators = ValidateResultOperators(
      plan.post_join_operators, plan.aggregate_sink, plan.sort_sink,
      static_cast<uint32_t>(plan.outputs.size()), &operator_ids);
  if (!result_operators.ok()) return result_operators;
  const PipelineDescriptor& result = plan.pipelines.back();
  std::vector<OperatorId> expected{plan.join.id};
  for (const PhysicalOperatorSpec& op : plan.post_join_operators) {
    expected.push_back(op.id);
  }
  if (result.operators != expected || result.dependencies.size() != 2 ||
      result.dependencies[0] == result.dependencies[1]) {
    return Status::InvalidArgument(
        "physical hash join", "join result pipeline is invalid");
  }
  return Status::OK();
}

Status ValidatePhysicalMultiHashJoinPlan(const PhysicalMultiHashJoinPlan& plan) {
  const auto valid_type = [](PhysicalType type) {
    const uint8_t value = static_cast<uint8_t>(type);
    return value >= static_cast<uint8_t>(PhysicalType::kBool) &&
        value <= static_cast<uint8_t>(PhysicalType::kBinary);
  };
  const auto valid_kind = [](ReturnExpressionKind kind) {
    switch (kind) {
      case ReturnExpressionKind::kBinding:
      case ReturnExpressionKind::kProperty:
      case ReturnExpressionKind::kValidFrom:
      case ReturnExpressionKind::kValidTo:
      case ReturnExpressionKind::kCommitSeq:
      case ReturnExpressionKind::kOperation:
      case ReturnExpressionKind::kSystemTime:
        return true;
      case ReturnExpressionKind::kCount:
      case ReturnExpressionKind::kSum:
      case ReturnExpressionKind::kAvg:
      case ReturnExpressionKind::kMin:
      case ReturnExpressionKind::kMax:
      case ReturnExpressionKind::kCollect:
        return false;
    }
    return false;
  };
  const auto valid_column = [&](const PhysicalMultiJoinColumn& column) {
    return column.binding_id.value != 0 && valid_kind(column.kind) &&
        valid_type(column.type) && !column.name.empty() &&
        (column.kind == ReturnExpressionKind::kProperty) ==
            column.property_id.has_value() &&
        (!column.property_id.has_value() || column.property_id->value != 0) &&
        (!column.relationship_identity ||
         (column.kind == ReturnExpressionKind::kBinding &&
          !column.property_id.has_value() &&
          column.type == PhysicalType::kInt64 && !column.nullable));
  };
  if (plan.plan_id == 0 || plan.inputs.size() < 2 ||
      (plan.graph_order != GraphOrder::kIndexFirst &&
       plan.graph_order != GraphOrder::kAdjacencyFirst) ||
      plan.input_layouts.size() != plan.inputs.size() ||
      plan.input_snapshot_seqs.size() != plan.inputs.size() ||
      plan.join_order.size() != plan.inputs.size() ||
      plan.steps.size() + 1 != plan.inputs.size() ||
      plan.output_names.empty() ||
      plan.final_output_columns.size() != plan.output_names.size() ||
      plan.pipelines.size() != plan.inputs.size() + plan.steps.size()) {
    return Status::InvalidArgument(
        "physical multi hash join", "multi-join plan shape is invalid");
  }

  std::set<BindingId> input_bindings;
  std::vector<std::set<BindingId>> owned_bindings(plan.inputs.size());
  for (size_t input_index = 0; input_index < plan.inputs.size(); ++input_index) {
    const std::shared_ptr<const PhysicalPlan>& input = plan.inputs[input_index];
    if (!input || !ValidatePhysicalPlan(*input).ok() ||
        input->projections().empty() ||
        input->projections().size() != plan.input_layouts[input_index].size() ||
        input->aggregate_sink().has_value() || input->sort_sink().has_value() ||
        !input->post_result_operators().empty()) {
      return Status::InvalidArgument(
          "physical multi hash join", "multi-join input plan is invalid");
    }
    for (const SlotDescriptor& slot : input->slots()) {
      owned_bindings[input_index].insert(slot.binding);
    }
    if (owned_bindings[input_index].empty() ||
        owned_bindings[input_index].count(input->binding_id()) == 0) {
      return Status::InvalidArgument(
          "physical multi hash join", "multi-join input ownership is invalid");
    }
    for (BindingId binding : owned_bindings[input_index]) {
      if (!input_bindings.insert(binding).second) {
        return Status::InvalidArgument(
            "physical multi hash join", "multi-join binding has multiple owners");
      }
    }
    if (input_index != 0 &&
        !(plan.inputs[input_index - 1]->binding_id() < input->binding_id())) {
      return Status::InvalidArgument(
          "physical multi hash join", "multi-join inputs are not binding sorted");
    }
    std::set<std::tuple<uint32_t, uint8_t, uint32_t, bool>> columns;
    for (size_t column_index = 0;
         column_index < plan.input_layouts[input_index].size(); ++column_index) {
      const PhysicalMultiJoinColumn& column =
          plan.input_layouts[input_index][column_index];
      const PhysicalExpression& projection = input->projections()[column_index];
      const uint32_t property = column.property_id.has_value()
          ? column.property_id->value : 0;
      if (!valid_column(column)) {
        return Status::InvalidArgument(
            "physical multi hash join", "multi-join input column is invalid");
      }
      if (owned_bindings[input_index].count(column.binding_id) == 0) {
        return Status::InvalidArgument(
            "physical multi hash join", "multi-join input column has no owner");
      }
      if (column.type != projection.type ||
          column.nullable != projection.nullable ||
          column.name != projection.output_name ||
          column.binding_id != projection.binding_id ||
          column.kind != projection.result_kind ||
          column.property_id != projection.property_id ||
          column.relationship_identity != projection.relationship_identity) {
        return Status::InvalidArgument(
            "physical multi hash join",
            "multi-join input projection differs from layout: " +
                column.name + "/" + projection.output_name + " type=" +
                std::to_string(static_cast<uint8_t>(column.type)) + "/" +
                std::to_string(static_cast<uint8_t>(projection.type)) +
                " nullable=" + std::to_string(column.nullable) + "/" +
                std::to_string(projection.nullable));
      }
      if (!columns.emplace(column.binding_id.value,
                           static_cast<uint8_t>(column.kind), property,
                           column.relationship_identity).second) {
        return Status::InvalidArgument(
            "physical multi hash join", "multi-join input layout duplicates a column");
      }
    }
  }

  std::vector<bool> seen_inputs(plan.inputs.size(), false);
  for (uint32_t input : plan.join_order) {
    if (input >= plan.inputs.size() || seen_inputs[input]) {
      return Status::InvalidArgument(
          "physical multi hash join", "multi-join order is invalid");
    }
    seen_inputs[input] = true;
  }
  std::vector<PhysicalMultiJoinColumn> accumulated =
      plan.input_layouts[plan.join_order.front()];
  PhysicalCardinalityEstimate propagated_estimate;
  bool have_propagated_estimate = false;
  std::fill(seen_inputs.begin(), seen_inputs.end(), false);
  seen_inputs[plan.join_order.front()] = true;
  std::set<OperatorId> operator_ids;
  for (size_t step_index = 0; step_index < plan.steps.size(); ++step_index) {
    const PhysicalMultiHashJoinStep& step = plan.steps[step_index];
    const bool hash_step =
        step.join.kind == PhysicalOperatorKind::kHashJoin;
    const bool cross_step =
        step.join.kind == PhysicalOperatorKind::kCrossJoin;
    const bool keys_match = step.accumulated_key_columns.size() ==
        step.input_key_columns.size();
    const bool keys_valid = hash_step
        ? keys_match && !step.accumulated_key_columns.empty()
        : cross_step && keys_match && step.accumulated_key_columns.empty();
    const PhysicalHashJoinBuildSide expected_build_side =
        step.accumulated_estimate.rows < step.input_estimate.rows
        ? PhysicalHashJoinBuildSide::kLeft
        : PhysicalHashJoinBuildSide::kRight;
    if (step.input_index != plan.join_order[step_index + 1] ||
        step.input_index >= plan.inputs.size() || seen_inputs[step.input_index] ||
        step.join.id.value == 0 ||
        (!hash_step && !cross_step) ||
        !step.join.required_slots.empty() || !step.join.produced_slots.empty() ||
        !operator_ids.insert(step.join.id).second ||
        !keys_valid ||
        step.outputs.size() != step.output_layout.size() ||
        (step.build_side != PhysicalHashJoinBuildSide::kLeft &&
         step.build_side != PhysicalHashJoinBuildSide::kRight) ||
        step.build_side != expected_build_side ||
        step.accumulated_estimate.confidence_per_mille > 1000 ||
        step.input_estimate.confidence_per_mille > 1000 ||
        (have_propagated_estimate &&
         (step.accumulated_estimate.rows != propagated_estimate.rows ||
          step.accumulated_estimate.confidence_per_mille !=
              propagated_estimate.confidence_per_mille ||
          step.accumulated_estimate.conservative !=
              propagated_estimate.conservative))) {
      return Status::InvalidArgument(
          "physical multi hash join", "multi-join step shape is invalid");
    }
    const std::vector<PhysicalMultiJoinColumn>& input_layout =
        plan.input_layouts[step.input_index];
    std::set<std::pair<uint32_t, uint32_t>> key_pairs;
    for (size_t key = 0; key < step.accumulated_key_columns.size(); ++key) {
      const uint32_t accumulated_column = step.accumulated_key_columns[key];
      const uint32_t input_column = step.input_key_columns[key];
      if (accumulated_column >= accumulated.size() ||
          input_column >= input_layout.size() ||
          !key_pairs.emplace(accumulated_column, input_column).second ||
          owned_bindings[step.input_index].count(
              accumulated[accumulated_column].binding_id) != 0 ||
          owned_bindings[step.input_index].count(
              input_layout[input_column].binding_id) == 0 ||
          accumulated[accumulated_column].type != input_layout[input_column].type) {
        return Status::InvalidArgument(
            "physical multi hash join", "multi-join key mapping is invalid");
      }
    }
    std::set<std::tuple<uint32_t, uint8_t, uint32_t, bool>> output_columns;
    for (size_t output_index = 0; output_index < step.outputs.size(); ++output_index) {
      const PhysicalHashJoinPlan::Output& output = step.outputs[output_index];
      const std::vector<PhysicalMultiJoinColumn>& source =
          output.from_left ? accumulated : input_layout;
      const PhysicalMultiJoinColumn& layout = step.output_layout[output_index];
      if (output.column >= source.size()) {
        return Status::InvalidArgument(
            "physical multi hash join", "multi-join output index is invalid");
      }
      const PhysicalMultiJoinColumn& source_column = source[output.column];
      const uint32_t property = layout.property_id.has_value()
          ? layout.property_id->value : 0;
      if (!valid_column(layout) ||
          layout.binding_id != source_column.binding_id ||
          layout.kind != source_column.kind ||
          layout.property_id != source_column.property_id ||
          layout.relationship_identity != source_column.relationship_identity ||
          layout.type != source_column.type ||
          layout.nullable != source_column.nullable ||
          layout.name != source_column.name ||
          output.type != layout.type || output.nullable != layout.nullable ||
          output.name != layout.name ||
          !output_columns.emplace(layout.binding_id.value,
                                  static_cast<uint8_t>(layout.kind), property,
                                  layout.relationship_identity).second) {
        return Status::InvalidArgument(
            "physical multi hash join", "multi-join output layout is invalid");
      }
    }
    seen_inputs[step.input_index] = true;
    accumulated = step.output_layout;
    propagated_estimate.rows =
        step.accumulated_estimate.rows == 0 || step.input_estimate.rows == 0
        ? 0
        : step.accumulated_estimate.rows >
                std::numeric_limits<uint64_t>::max() /
                    step.input_estimate.rows
            ? std::numeric_limits<uint64_t>::max()
            : step.accumulated_estimate.rows * step.input_estimate.rows;
    propagated_estimate.confidence_per_mille = std::min(
        step.accumulated_estimate.confidence_per_mille,
        step.input_estimate.confidence_per_mille);
    propagated_estimate.conservative =
        step.accumulated_estimate.conservative ||
        step.input_estimate.conservative;
    have_propagated_estimate = true;
  }
  for (size_t output = 0; output < plan.final_output_columns.size(); ++output) {
    const uint32_t column = plan.final_output_columns[output];
    if (column >= accumulated.size() || plan.output_names[output].empty() ||
        accumulated[column].name != plan.output_names[output]) {
      return Status::InvalidArgument(
          "physical multi hash join", "multi-join final projection is invalid");
    }
  }

  const Status result_operators = ValidateResultOperators(
      plan.post_join_operators, plan.aggregate_sink, plan.sort_sink,
      static_cast<uint32_t>(plan.output_names.size()), &operator_ids);
  if (!result_operators.ok()) return result_operators;

  for (size_t input = 0; input < plan.inputs.size(); ++input) {
    const PipelineDescriptor& pipeline = plan.pipelines[input];
    if (pipeline.id != PipelineId{static_cast<uint32_t>(input + 1)} ||
        !pipeline.operators.empty() || !pipeline.dependencies.empty()) {
      return Status::InvalidArgument(
          "physical multi hash join", "multi-join input pipeline is invalid");
    }
  }
  for (size_t step_index = 0; step_index < plan.steps.size(); ++step_index) {
    const PhysicalMultiHashJoinStep& step = plan.steps[step_index];
    const PipelineDescriptor& pipeline =
        plan.pipelines[plan.inputs.size() + step_index];
    const PipelineId expected_id{
        static_cast<uint32_t>(plan.inputs.size() + step_index + 1)};
    const PipelineId accumulated_dependency = step_index == 0
        ? PipelineId{plan.join_order.front() + 1}
        : PipelineId{static_cast<uint32_t>(plan.inputs.size() + step_index)};
    const std::vector<PipelineId> dependencies{
        accumulated_dependency, PipelineId{step.input_index + 1}};
    std::vector<OperatorId> operators{step.join.id};
    if (step_index + 1 == plan.steps.size()) {
      for (const PhysicalOperatorSpec& op : plan.post_join_operators) {
        operators.push_back(op.id);
      }
    }
    if (pipeline.id != expected_id || pipeline.dependencies != dependencies ||
        pipeline.operators != operators) {
      return Status::InvalidArgument(
          "physical multi hash join", "multi-join pipeline is invalid");
    }
  }
  return Status::OK();
}

std::string FormatPhysicalPlan(const PhysicalPlan& plan) {
  std::ostringstream output;
  const char* temporal_mode = "point";
  switch (plan.temporal_mode()) {
    case PhysicalTemporalMode::kPoint: temporal_mode = "point"; break;
    case PhysicalTemporalMode::kValidTimeRange: temporal_mode = "valid-range"; break;
    case PhysicalTemporalMode::kValidTimeChanges: temporal_mode = "valid-changes"; break;
    case PhysicalTemporalMode::kSystemTimeChanges: temporal_mode = "system-changes"; break;
  }
  output << "PhysicalPlan[id=" << plan.plan_id() << ",pipeline="
         << plan.pipelines().front().id.value << ",temporal=" << temporal_mode << "] ";
  for (size_t index = 0; index < plan.operators().size(); ++index) {
    if (index != 0) output << " -> ";
    const PhysicalOperatorSpec& op = plan.operators()[index];
    output << PhysicalOperatorKindName(op.kind) << "#" << op.id.value;
  }
  output << " slots=";
  for (size_t index = 0; index < plan.slots().size(); ++index) {
    if (index != 0) output << ',';
    output << plan.slots()[index].id.value;
  }
  if (plan.expand().has_value()) {
    const PhysicalExpandSpec& expand = *plan.expand();
    output << " expand=" << (expand.direction == EntityType::EdgeOut ? "out" : "in")
           << ':' << expand.source_binding.value << ':' << expand.target_binding.value
           << ":type=";
    if (expand.edge_type.has_value()) output << *expand.edge_type;
    else output << '*';
  }
  for (const PhysicalOperatorSpec& op : plan.post_result_operators()) {
    output << " -> " << PhysicalOperatorKindName(op.kind) << "#" << op.id.value;
  }
  return output.str();
}

std::string FormatPhysicalHashJoinPlan(const PhysicalHashJoinPlan& plan) {
  std::ostringstream output;
  output << "PhysicalHashJoinPlan[id=" << plan.plan_id
         << ",build="
         << (plan.build_side == PhysicalHashJoinBuildSide::kLeft ? "left" : "right")
         << ",left_rows=" << plan.left_estimate.rows
         << ",right_rows=" << plan.right_estimate.rows
         << ",left_confidence=" << plan.left_estimate.confidence_per_mille
         << ",right_confidence=" << plan.right_estimate.confidence_per_mille
         << ",left_conservative=" << (plan.left_estimate.conservative ? 1 : 0)
         << ",right_conservative=" << (plan.right_estimate.conservative ? 1 : 0)
         << ",statistics=" << plan.statistics_snapshot_id << "] {"
         << FormatPhysicalPlan(*plan.left) << "} + {"
         << FormatPhysicalPlan(*plan.right) << "} -> "
         << PhysicalOperatorKindName(plan.join.kind) << "#" << plan.join.id.value;
  for (const PhysicalOperatorSpec& op : plan.post_join_operators) {
    output << " -> " << PhysicalOperatorKindName(op.kind) << "#" << op.id.value;
  }
  return output.str();
}

std::string FormatPhysicalMultiHashJoinPlan(
    const PhysicalMultiHashJoinPlan& plan) {
  std::ostringstream output;
  output << "PhysicalMultiHashJoinPlan[id=" << plan.plan_id
         << ",statistics=" << plan.statistics_snapshot_id
         << ",graph_order="
         << (plan.graph_order == GraphOrder::kIndexFirst
                 ? "index-first" : "adjacency-first")
         << ",order=";
  for (size_t order = 0; order < plan.join_order.size(); ++order) {
    if (order != 0) output << ',';
    const uint32_t input = plan.join_order[order];
    if (input < plan.inputs.size() && plan.inputs[input]) {
      output << plan.inputs[input]->binding_id().value;
    } else {
      output << '?';
    }
  }
  output << ",snapshots=";
  for (size_t input = 0; input < plan.input_snapshot_seqs.size(); ++input) {
    if (input != 0) output << ',';
    output << plan.input_snapshot_seqs[input];
  }
  output << "]";
  for (size_t step_index = 0; step_index < plan.steps.size(); ++step_index) {
    const PhysicalMultiHashJoinStep& step = plan.steps[step_index];
    output << " -> step" << step_index << '{'
           << PhysicalOperatorKindName(step.join.kind) << '#' << step.join.id.value
           << ",input=" << step.input_index
           << ",build="
           << (step.build_side == PhysicalHashJoinBuildSide::kLeft
                   ? "left" : "right")
           << ",accumulated_rows=" << step.accumulated_estimate.rows
           << ",input_rows=" << step.input_estimate.rows
           << ",accumulated_confidence="
           << step.accumulated_estimate.confidence_per_mille
           << ",input_confidence=" << step.input_estimate.confidence_per_mille
           << ",accumulated_conservative="
           << (step.accumulated_estimate.conservative ? 1 : 0)
           << ",input_conservative="
           << (step.input_estimate.conservative ? 1 : 0)
           << ",keys=";
    for (size_t key = 0; key < step.accumulated_key_columns.size(); ++key) {
      if (key != 0) output << ',';
      output << step.accumulated_key_columns[key] << '='
             << step.input_key_columns[key];
    }
    output << ",layout=";
    for (size_t column = 0; column < step.output_layout.size(); ++column) {
      if (column != 0) output << ',';
      output << step.output_layout[column].name;
    }
    output << '}';
  }
  for (const PhysicalOperatorSpec& op : plan.post_join_operators) {
    output << " -> " << PhysicalOperatorKindName(op.kind) << '#' << op.id.value;
  }
  return output.str();
}

}  // namespace cedar
