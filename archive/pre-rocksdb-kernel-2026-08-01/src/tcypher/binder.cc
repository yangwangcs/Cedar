// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/tcypher/binder.h"

#include <algorithm>
#include <limits>
#include <map>
#include <utility>

namespace cedar {
namespace {

bool ParseDigits(const std::string& text, size_t offset, size_t length, uint32_t* value) {
  if (offset + length > text.size()) return false;
  uint32_t parsed = 0;
  for (size_t index = offset; index < offset + length; ++index) {
    const char character = text[index];
    if (character < '0' || character > '9') return false;
    parsed = parsed * 10 + static_cast<uint32_t>(character - '0');
  }
  *value = parsed;
  return true;
}

bool IsLeapYear(uint32_t year) {
  return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
}

uint32_t DaysInMonth(uint32_t year, uint32_t month) {
  static constexpr uint32_t kDays[] = {31, 28, 31, 30, 31, 30,
                                        31, 31, 30, 31, 30, 31};
  if (month == 2 && IsLeapYear(year)) return 29;
  return kDays[month - 1];
}

uint64_t DaysSinceUnixEpoch(uint32_t year, uint32_t month, uint32_t day) {
  int64_t adjusted_year = static_cast<int64_t>(year) - (month <= 2 ? 1 : 0);
  const int64_t era = adjusted_year / 400;
  const uint32_t year_of_era = static_cast<uint32_t>(adjusted_year - era * 400);
  const uint32_t day_of_year = (153 * (month + (month > 2 ? -3 : 9)) + 2) / 5 + day - 1;
  const uint32_t day_of_era = year_of_era * 365 + year_of_era / 4 -
                              year_of_era / 100 + day_of_year;
  return static_cast<uint64_t>(era * 146097 + static_cast<int64_t>(day_of_era) - 719468);
}

StatusOr<uint64_t> ParseUtcTimestampMicros(const std::string& timestamp) {
  if (timestamp.size() < 20 || timestamp[4] != '-' || timestamp[7] != '-' ||
      timestamp[10] != 'T' || timestamp[13] != ':' || timestamp[16] != ':' ||
      timestamp.back() != 'Z') {
    return Status::BindError("T-Cypher", "timestamp must use UTC ISO-8601 form");
  }
  uint32_t year = 0;
  uint32_t month = 0;
  uint32_t day = 0;
  uint32_t hour = 0;
  uint32_t minute = 0;
  uint32_t second = 0;
  if (!ParseDigits(timestamp, 0, 4, &year) || !ParseDigits(timestamp, 5, 2, &month) ||
      !ParseDigits(timestamp, 8, 2, &day) || !ParseDigits(timestamp, 11, 2, &hour) ||
      !ParseDigits(timestamp, 14, 2, &minute) || !ParseDigits(timestamp, 17, 2, &second) ||
      year < 1970 || month == 0 || month > 12 || day == 0 || day > DaysInMonth(year, month) ||
      hour > 23 || minute > 59 || second > 59) {
    return Status::BindError("T-Cypher", "timestamp calendar value is invalid");
  }
  uint32_t micros = 0;
  if (timestamp.size() > 20) {
    if (timestamp[19] != '.' || timestamp.size() > 27) {
      return Status::BindError("T-Cypher", "timestamp fraction is invalid");
    }
    const size_t digits = timestamp.size() - 21;
    if (digits == 0 || !ParseDigits(timestamp, 20, digits, &micros)) {
      return Status::BindError("T-Cypher", "timestamp fraction is invalid");
    }
    for (size_t index = digits; index < 6; ++index) micros *= 10;
  }
  const uint64_t seconds = DaysSinceUnixEpoch(year, month, day) * 86400 +
                           static_cast<uint64_t>(hour) * 3600 + minute * 60 + second;
  if (seconds > (std::numeric_limits<uint64_t>::max() - micros) / 1000000) {
    return Status::BindError("T-Cypher", "timestamp exceeds supported range");
  }
  return seconds * 1000000 + micros;
}

StatusOr<uint64_t> ResolveTemporalValue(const TemporalValue& value,
                                        const TcypherBindingContext& context) {
  if (!value.parameter_name.empty()) {
    const auto parameter = context.timestamp_parameters.find(value.parameter_name);
    if (parameter == context.timestamp_parameters.end()) {
      return Status::BindError("T-Cypher", "missing temporal parameter");
    }
    return parameter->second;
  }
  if (value.integer_literal.has_value()) return *value.integer_literal;
  return ParseUtcTimestampMicros(value.timestamp_literal);
}

StatusOr<ColumnSchema> ResolveLatestColumnByName(
    const SchemaSnapshot& snapshot, EntityType entity_type,
    const std::string& name, bool include_existence = false) {
  std::optional<ColumnSchema> resolved;
  for (const auto& entry : snapshot.schemas) {
    if (entry.first.first != static_cast<uint8_t>(entity_type) ||
        (!include_existence && entry.first.second == 0) || entry.second.empty()) {
      continue;
    }
    const ColumnSchema& latest = entry.second.back();
    if (latest.logical_type != name) continue;
    if (resolved.has_value()) {
      return Status::BindError("T-Cypher", "schema name is ambiguous for its entity type");
    }
    resolved = latest;
  }
  if (!resolved.has_value()) {
    return Status::BindError("T-Cypher", "schema name is not registered for its entity type");
  }
  return *resolved;
}

bool IsAggregate(ReturnExpressionKind kind) {
  return kind == ReturnExpressionKind::kCount ||
         kind == ReturnExpressionKind::kSum ||
         kind == ReturnExpressionKind::kAvg ||
         kind == ReturnExpressionKind::kMin ||
         kind == ReturnExpressionKind::kMax ||
         kind == ReturnExpressionKind::kCollect;
}

enum class PropertyUse : uint8_t {
  kPredicate,
  kProjection,
  kGrouping,
  kOrdering,
  kJoin,
  kMutation,
};

class SchemaBindingState {
 public:
  SchemaBindingState(const TcypherBindingContext& context,
                     BoundTcypherStatement* bound)
      : context_(context), bound_(bound) {}

  Status AddNode(const MatchNodePattern& node) {
    if (node.variable.empty()) {
      return Status::BindError("T-Cypher", "MATCH node variable is empty");
    }
    BoundVariable variable;
    variable.variable = node.variable;
    variable.kind = BoundGraphKind::kNode;
    variable.entity_type = EntityType::Vertex;
    if (context_.schema_snapshot) {
      variable.entity_schema = context_.schema_snapshot->Lookup(EntityType::Vertex, 0);
    }
    if (!node.label.empty()) {
      if (!variable.entity_schema.has_value() ||
          variable.entity_schema->logical_type != node.label) {
        return Status::BindError("T-Cypher", "vertex label is not registered");
      }
    }
    return AddVariable(std::move(variable));
  }

  Status AddRelationship(const MatchRelationshipPattern& relationship) {
    const EntityType entity_type = relationship.direction == RelationshipDirection::kOutgoing
                                       ? EntityType::EdgeOut
                                       : EntityType::EdgeIn;
    std::optional<ColumnSchema> entity_schema;
    if (!relationship.type.empty()) {
      if (!context_.schema_snapshot) {
        return Status::BindError("T-Cypher", "relationship type requires a schema snapshot");
      }
      const auto schema = ResolveLatestColumnByName(
          *context_.schema_snapshot, entity_type, relationship.type, true);
      if (!schema.ok()) return schema.status();
      entity_schema = schema.ValueOrDie();
    }
    if (relationship.variable.empty()) return Status::OK();
    BoundVariable variable;
    variable.variable = relationship.variable;
    variable.kind = BoundGraphKind::kRelationship;
    variable.direction = relationship.direction;
    variable.entity_type = entity_type;
    variable.entity_schema = std::move(entity_schema);
    return AddVariable(std::move(variable));
  }

  Status DemandProperty(const std::string& variable_name,
                        const std::string& property_name, PropertyUse use,
                        ColumnSchema* resolved = nullptr) {
    const auto variable = variables_.find(variable_name);
    if (variable == variables_.end()) {
      return Status::BindError("T-Cypher", "property references an unknown MATCH variable");
    }
    if (!context_.schema_snapshot) {
      return Status::BindError("T-Cypher", "property requires a schema snapshot");
    }
    const EntityType entity_type = bound_->variables[variable->second].entity_type;
    const auto column = ResolveLatestColumnByName(
        *context_.schema_snapshot, entity_type, property_name);
    if (!column.ok()) return column.status();
    if (resolved != nullptr) *resolved = column.ValueOrDie();
    auto demand = std::find_if(
        bound_->properties.begin(), bound_->properties.end(),
        [&](const BoundPropertyReference& property) {
          return property.variable == variable_name &&
                 property.column.entity_type == column.ValueOrDie().entity_type &&
                 property.column.column_id == column.ValueOrDie().column_id &&
                 property.column.schema_epoch == column.ValueOrDie().schema_epoch;
        });
    if (demand == bound_->properties.end()) {
      bound_->properties.push_back(BoundPropertyReference{
          variable_name, column.ValueOrDie(), true, false, false, false, false, false});
      demand = std::prev(bound_->properties.end());
    }
    switch (use) {
      case PropertyUse::kPredicate:
        demand->predicate = true;
        break;
      case PropertyUse::kProjection:
        demand->projection = true;
        break;
      case PropertyUse::kGrouping:
        demand->grouping = true;
        break;
      case PropertyUse::kOrdering:
        demand->ordering = true;
        break;
      case PropertyUse::kJoin:
        demand->join = true;
        break;
      case PropertyUse::kMutation:
        break;
    }
    return Status::OK();
  }

  Status MarkCompleteEntity(const std::string& variable_name) {
    const auto variable = variables_.find(variable_name);
    if (variable == variables_.end()) {
      return Status::BindError("T-Cypher", "return references an unknown MATCH variable");
    }
    bound_->variables[variable->second].complete_entity = true;
    return Status::OK();
  }

  Status MarkGroupingIdentity(const std::string& variable_name) {
    const auto variable = variables_.find(variable_name);
    if (variable == variables_.end()) {
      return Status::BindError("T-Cypher", "grouping references an unknown MATCH variable");
    }
    bound_->variables[variable->second].grouping_identity = true;
    return Status::OK();
  }

  Status MarkJoinIdentity(const std::string& variable_name) {
    const auto variable = variables_.find(variable_name);
    if (variable == variables_.end()) {
      return Status::BindError("T-Cypher", "join references an unknown MATCH variable");
    }
    bound_->variables[variable->second].join_identity = true;
    return Status::OK();
  }

  Status DemandProvenance(const std::string& variable_name,
                          ProvenanceField field, bool grouping = false) {
    const auto variable = variables_.find(variable_name);
    if (variable == variables_.end()) {
      return Status::BindError("T-Cypher", "provenance references an unknown MATCH variable");
    }
    BoundVariable& demand = bound_->variables[variable->second];
    (grouping ? demand.grouping_provenance : demand.provenance).push_back(field);
    return Status::OK();
  }

  bool HasVariable(const std::string& variable_name) const {
    return variables_.count(variable_name) != 0;
  }

  void Finish() {
    for (BoundVariable& variable : bound_->variables) {
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
    std::sort(bound_->variables.begin(), bound_->variables.end(),
              [](const BoundVariable& left, const BoundVariable& right) {
                return left.variable < right.variable;
              });
    std::sort(bound_->properties.begin(), bound_->properties.end(),
              [](const BoundPropertyReference& left,
                 const BoundPropertyReference& right) {
                if (left.variable != right.variable) return left.variable < right.variable;
                if (left.column.entity_type != right.column.entity_type) {
                  return static_cast<uint8_t>(left.column.entity_type) <
                         static_cast<uint8_t>(right.column.entity_type);
                }
                return left.column.column_id < right.column.column_id;
              });
    for (size_t index = 0; index < bound_->variables.size(); ++index) {
      bound_->variables[index].binding_id =
          BindingId{static_cast<uint32_t>(index + 1)};
    }
    for (size_t index = 0; index < bound_->properties.size(); ++index) {
      BoundPropertyReference& property = bound_->properties[index];
      property.property_id = BoundPropertyId{static_cast<uint32_t>(index + 1)};
      const auto variable = std::find_if(
          bound_->variables.begin(), bound_->variables.end(),
          [&property](const BoundVariable& candidate) {
            return candidate.variable == property.variable;
          });
      if (variable != bound_->variables.end()) {
        property.binding_id = variable->binding_id;
      }
    }
  }

 private:
  Status AddVariable(BoundVariable variable) {
    if (!variables_.emplace(variable.variable, bound_->variables.size()).second) {
      return Status::BindError("T-Cypher", "MATCH variable is declared more than once");
    }
    bound_->variables.push_back(std::move(variable));
    return Status::OK();
  }

  const TcypherBindingContext& context_;
  BoundTcypherStatement* bound_;
  std::map<std::string, size_t> variables_;
};

Status ValidateLiteralPredicate(const ColumnSchema& column,
                                const StringEqualityPredicate& predicate) {
  const bool integer = predicate.integer_value.has_value() ||
                       !predicate.in_integer_values.empty() ||
                       predicate.lower_integer_bound.has_value() ||
                       predicate.upper_integer_bound.has_value();
  if (!integer) {
    if (column.physical_type != PhysicalType::kString) {
      return Status::BindError("T-Cypher", "string predicate requires a string property");
    }
    return Status::OK();
  }
  if (column.physical_type != PhysicalType::kInt32 &&
      column.physical_type != PhysicalType::kInt64 &&
      column.physical_type != PhysicalType::kTimestamp64) {
    return Status::BindError("T-Cypher", "integer predicate requires an integer property");
  }
  std::vector<int64_t> values;
  if (predicate.integer_value.has_value()) values.push_back(*predicate.integer_value);
  values.insert(values.end(), predicate.in_integer_values.begin(),
                predicate.in_integer_values.end());
  if (predicate.lower_integer_bound.has_value()) {
    values.push_back(*predicate.lower_integer_bound);
  }
  if (predicate.upper_integer_bound.has_value()) {
    values.push_back(*predicate.upper_integer_bound);
  }
  for (int64_t value : values) {
    if (column.physical_type == PhysicalType::kInt32 &&
        (value < std::numeric_limits<int32_t>::min() ||
         value > std::numeric_limits<int32_t>::max())) {
      return Status::BindError("T-Cypher", "integer predicate exceeds Int32 range");
    }
    if (column.physical_type == PhysicalType::kTimestamp64 && value < 0) {
      return Status::BindError("T-Cypher", "timestamp predicate cannot be negative");
    }
  }
  return Status::OK();
}

const BoundPropertyReference* FindBoundPropertyUse(
    const BoundTcypherStatement& bound, const std::string& variable,
    const std::string& property_name) {
  const auto property = std::find_if(
      bound.properties.begin(), bound.properties.end(),
      [&](const BoundPropertyReference& candidate) {
        return candidate.variable == variable &&
            candidate.column.logical_type == property_name;
      });
  return property == bound.properties.end() ? nullptr : &*property;
}

const BoundVariable* FindBoundVariableUse(
    const BoundTcypherStatement& bound, const std::string& variable) {
  const auto found = std::find_if(
      bound.variables.begin(), bound.variables.end(),
      [&](const BoundVariable& candidate) {
        return candidate.variable == variable;
      });
  return found == bound.variables.end() ? nullptr : &*found;
}

StatusOr<std::optional<Value>> TypedBoundInteger(PhysicalType type, int64_t value) {
  if (type == PhysicalType::kInt32) {
    return std::optional<Value>(Value::Int32(static_cast<int32_t>(value)));
  }
  if (type == PhysicalType::kInt64) {
    return std::optional<Value>(Value::Int64(value));
  }
  if (type == PhysicalType::kTimestamp64) {
    return std::optional<Value>(Value::Timestamp(static_cast<uint64_t>(value)));
  }
  return Status::BindError("T-Cypher", "integer literal has non-integer schema");
}

Status BuildBoundUseSites(const TcypherStatement& statement,
                          BoundTcypherStatement* bound) {
  bound->predicates.clear();
  bound->projections.clear();
  const auto add_predicate = [&](const StringEqualityPredicate& syntax) -> Status {
    if (syntax.entity_id_variable.has_value()) return Status::OK();
    const BoundPropertyReference* property = FindBoundPropertyUse(
        *bound, syntax.variable, syntax.property_name);
    if (property == nullptr || !property->predicate) {
      return Status::BindError("T-Cypher", "predicate property use is not bound");
    }
    BoundPredicateExpression expression;
    expression.property_id = property->property_id;
    expression.binding_id = property->binding_id;
    expression.column = property->column;
    expression.nullable = property->nullable;
    expression.kind = syntax.kind;
    expression.lower_inclusive = syntax.lower_inclusive;
    expression.upper_inclusive = syntax.upper_inclusive;
    const bool integer = syntax.integer_value.has_value() ||
        !syntax.in_integer_values.empty() ||
        syntax.lower_integer_bound.has_value() ||
        syntax.upper_integer_bound.has_value();
    if (!integer) {
      if (syntax.kind == StringPredicateKind::kEquality ||
          syntax.kind == StringPredicateKind::kPrefix) {
        expression.values.push_back(Value::String(syntax.string_value));
      } else if (syntax.kind == StringPredicateKind::kIn) {
        for (const std::string& value : syntax.in_values) {
          expression.values.push_back(Value::String(value));
        }
      }
      if (syntax.lower_bound.has_value()) {
        expression.lower_bound = Value::String(*syntax.lower_bound);
      }
      if (syntax.upper_bound.has_value()) {
        expression.upper_bound = Value::String(*syntax.upper_bound);
      }
    } else {
      const auto append = [&](int64_t value) -> Status {
        auto typed = TypedBoundInteger(property->column.physical_type, value);
        if (!typed.ok()) return typed.status();
        expression.values.push_back(*std::move(typed).ConsumeValueOrDie());
        return Status::OK();
      };
      if (syntax.integer_value.has_value()) {
        const Status status = append(*syntax.integer_value);
        if (!status.ok()) return status;
      }
      for (int64_t value : syntax.in_integer_values) {
        const Status status = append(value);
        if (!status.ok()) return status;
      }
      if (syntax.lower_integer_bound.has_value()) {
        auto typed = TypedBoundInteger(
            property->column.physical_type, *syntax.lower_integer_bound);
        if (!typed.ok()) return typed.status();
        expression.lower_bound = *std::move(typed).ConsumeValueOrDie();
      }
      if (syntax.upper_integer_bound.has_value()) {
        auto typed = TypedBoundInteger(
            property->column.physical_type, *syntax.upper_integer_bound);
        if (!typed.ok()) return typed.status();
        expression.upper_bound = *std::move(typed).ConsumeValueOrDie();
      }
    }
    bound->predicates.push_back(std::move(expression));
    return Status::OK();
  };
  if (statement.where.has_value()) {
    const Status status = add_predicate(*statement.where);
    if (!status.ok()) return status;
  }
  for (const StringEqualityPredicate& predicate : statement.and_predicates) {
    const Status status = add_predicate(predicate);
    if (!status.ok()) return status;
  }

  for (const ReturnExpression& syntax : statement.returns) {
    const BoundVariable* variable = FindBoundVariableUse(*bound, syntax.variable);
    if (variable == nullptr) {
      return Status::BindError("T-Cypher", "projection binding is not bound");
    }
    BoundProjectionExpression expression;
    expression.kind = syntax.kind;
    expression.binding_id = variable->binding_id;
    expression.nullable = false;
    if (!syntax.property_name.empty()) {
      const BoundPropertyReference* property = FindBoundPropertyUse(
          *bound, syntax.variable, syntax.property_name);
      if (property == nullptr) {
        return Status::BindError("T-Cypher", "projection property use is not bound");
      }
      expression.property_id = property->property_id;
      expression.type = property->column.physical_type;
      expression.nullable = property->nullable;
      const std::string input = syntax.variable + "." + property->column.logical_type;
      if (syntax.kind == ReturnExpressionKind::kSum) expression.output_name = "sum(" + input + ")";
      else if (syntax.kind == ReturnExpressionKind::kAvg) expression.output_name = "avg(" + input + ")";
      else if (syntax.kind == ReturnExpressionKind::kMin) expression.output_name = "min(" + input + ")";
      else if (syntax.kind == ReturnExpressionKind::kMax) expression.output_name = "max(" + input + ")";
      else if (syntax.kind == ReturnExpressionKind::kCollect) expression.output_name = "collect(" + input + ")";
      else expression.output_name = input;
    } else if (syntax.kind == ReturnExpressionKind::kValidFrom ||
               syntax.kind == ReturnExpressionKind::kValidTo ||
               syntax.kind == ReturnExpressionKind::kSystemTime) {
      expression.type = PhysicalType::kTimestamp64;
      const char* name = syntax.kind == ReturnExpressionKind::kValidFrom
          ? "valid_from" : syntax.kind == ReturnExpressionKind::kValidTo
              ? "valid_to" : "system_time";
      expression.output_name = std::string(name) + "(" + syntax.variable + ")";
    } else if (syntax.kind == ReturnExpressionKind::kCommitSeq) {
      expression.type = PhysicalType::kInt64;
      expression.output_name = "commit_seq(" + syntax.variable + ")";
    } else if (syntax.kind == ReturnExpressionKind::kOperation) {
      expression.type = PhysicalType::kString;
      expression.output_name = "operation(" + syntax.variable + ")";
    } else {
      expression.type = PhysicalType::kInt64;
      expression.output_name = syntax.kind == ReturnExpressionKind::kCollect
          ? "collect(" + syntax.variable + ")" : syntax.variable;
    }
    bound->projections.push_back(std::move(expression));
  }
  return Status::OK();
}

Status BindSchemaMetadata(const TcypherStatement& statement,
                          const TcypherBindingContext& context,
                          BoundTcypherStatement* bound) {
  SchemaBindingState state(context, bound);
  if (statement.kind == TcypherStatementKind::kCreate) {
    if (!statement.mutation.has_value()) {
      return Status::BindError("T-Cypher", "mutation has no payload");
    }
    MatchNodePattern node;
    node.variable = statement.mutation->variable;
    node.label = statement.mutation->label;
    Status status = state.AddNode(node);
    if (!status.ok()) return status;
    ColumnSchema column;
    status = state.DemandProperty(node.variable, statement.mutation->property_name,
                                  PropertyUse::kMutation, &column);
    if (!status.ok()) return status;
    if (column.physical_type != PhysicalType::kString) {
      return Status::BindError("T-Cypher", "string mutation requires a string property");
    }
    state.Finish();
    return Status::OK();
  }
  if (statement.kind == TcypherStatementKind::kSet ||
      statement.kind == TcypherStatementKind::kDelete) {
    Status status = state.AddNode(statement.match);
    if (!status.ok()) return status;
    if (statement.kind == TcypherStatementKind::kSet && statement.mutation.has_value()) {
      ColumnSchema column;
      status = state.DemandProperty(statement.mutation->variable,
                                    statement.mutation->property_name,
                                    PropertyUse::kMutation, &column);
      if (!status.ok()) return status;
      if (column.physical_type != PhysicalType::kString) {
        return Status::BindError("T-Cypher", "string mutation requires a string property");
      }
    }
    state.Finish();
    return Status::OK();
  }
  if (statement.kind != TcypherStatementKind::kQuery) return Status::OK();

  Status status = state.AddNode(statement.match);
  if (!status.ok()) return status;
  for (size_t index = 0; index < statement.relationships.size(); ++index) {
    status = state.AddRelationship(statement.relationships[index]);
    if (!status.ok()) return status;
    if (index >= statement.expanded_nodes.size()) {
      return Status::BindError("T-Cypher", "relationship endpoint is missing");
    }
    status = state.AddNode(statement.expanded_nodes[index]);
    if (!status.ok()) return status;
  }
  for (const MatchClause& clause : statement.additional_matches) {
    status = state.AddNode(clause.match);
    if (!status.ok()) return status;
    for (size_t index = 0; index < clause.relationships.size(); ++index) {
      status = state.AddRelationship(clause.relationships[index]);
      if (!status.ok()) return status;
      if (index >= clause.expanded_nodes.size()) {
        return Status::BindError("T-Cypher", "relationship endpoint is missing");
      }
      status = state.AddNode(clause.expanded_nodes[index]);
      if (!status.ok()) return status;
    }
  }

  const bool grouped = std::any_of(statement.returns.begin(), statement.returns.end(),
                                   [](const ReturnExpression& expression) {
                                     return IsAggregate(expression.kind);
                                   });
  for (const ReturnExpression& expression : statement.returns) {
    if (!state.HasVariable(expression.variable)) {
      return Status::BindError("T-Cypher", "return references an unknown MATCH variable");
    }
    if (expression.kind == ReturnExpressionKind::kBinding ||
        (expression.kind == ReturnExpressionKind::kCollect &&
         expression.property_name.empty())) {
      status = state.MarkCompleteEntity(expression.variable);
      if (!status.ok()) return status;
      if (grouped && !IsAggregate(expression.kind)) {
        status = state.MarkGroupingIdentity(expression.variable);
        if (!status.ok()) return status;
      }
    }
    if (!expression.property_name.empty()) {
      status = state.DemandProperty(expression.variable, expression.property_name,
                                    PropertyUse::kProjection);
      if (!status.ok()) return status;
      if (grouped && !IsAggregate(expression.kind)) {
        status = state.DemandProperty(expression.variable, expression.property_name,
                                      PropertyUse::kGrouping);
        if (!status.ok()) return status;
      }
    }
    std::optional<ProvenanceField> provenance;
    if (expression.kind == ReturnExpressionKind::kValidFrom) {
      provenance = ProvenanceField::kValidFrom;
    } else if (expression.kind == ReturnExpressionKind::kValidTo) {
      provenance = ProvenanceField::kValidTo;
    } else if (expression.kind == ReturnExpressionKind::kCommitSeq) {
      provenance = ProvenanceField::kCommitSeq;
    } else if (expression.kind == ReturnExpressionKind::kOperation) {
      provenance = ProvenanceField::kOperation;
    } else if (expression.kind == ReturnExpressionKind::kSystemTime) {
      provenance = ProvenanceField::kSystemTime;
    }
    if (provenance.has_value()) {
      status = state.DemandProvenance(expression.variable, *provenance);
      if (!status.ok()) return status;
      if (grouped && !IsAggregate(expression.kind)) {
        status = state.DemandProvenance(expression.variable, *provenance, true);
        if (!status.ok()) return status;
      }
    }
  }

  if (statement.order_by.has_value()) {
    const OrderByTerm& order = *statement.order_by;
    if (!state.HasVariable(order.variable)) {
      return Status::BindError("T-Cypher", "ORDER BY references an unknown MATCH variable");
    }
    const bool projected = std::any_of(
        statement.returns.begin(), statement.returns.end(),
        [&order](const ReturnExpression& expression) {
          return expression.kind == ReturnExpressionKind::kProperty &&
                 expression.variable == order.variable &&
                 expression.property_name == order.property_name;
        });
    if (!projected) {
      return Status::BindError("T-Cypher", "ORDER BY property must be projected");
    }
    status = state.DemandProperty(order.variable, order.property_name,
                                  PropertyUse::kOrdering);
    if (!status.ok()) return status;
  }

  const auto bind_predicate = [&](const StringEqualityPredicate& predicate) -> Status {
    if (!state.HasVariable(predicate.variable)) {
      return Status::BindError("T-Cypher", "WHERE references an unknown MATCH variable");
    }
    if (!predicate.entity_id_variable.has_value()) {
      ColumnSchema column;
      Status demanded = state.DemandProperty(predicate.variable, predicate.property_name,
                                             PropertyUse::kPredicate, &column);
      if (!demanded.ok()) return demanded;
      return ValidateLiteralPredicate(column, predicate);
    }
    if (!state.HasVariable(*predicate.entity_id_variable)) {
      return Status::BindError("T-Cypher", "join references an unknown MATCH variable");
    }
    const std::string right_name = predicate.rhs_property_name.value_or("id");
    BoundJoinEquality equality;
    equality.left.variable = predicate.variable;
    equality.left.identity = predicate.property_name == "id";
    equality.right.variable = *predicate.entity_id_variable;
    equality.right.identity = right_name == "id";
    if (!equality.left.identity) {
      ColumnSchema left;
      Status demanded = state.DemandProperty(predicate.variable, predicate.property_name,
                                             PropertyUse::kJoin, &left);
      if (!demanded.ok()) return demanded;
      equality.left.column = left;
    }
    if (!equality.right.identity) {
      ColumnSchema right;
      Status demanded = state.DemandProperty(*predicate.entity_id_variable, right_name,
                                             PropertyUse::kJoin, &right);
      if (!demanded.ok()) return demanded;
      equality.right.column = right;
    }
    const PhysicalType left_type = equality.left.identity
                                       ? PhysicalType::kInt64
                                       : equality.left.column->physical_type;
    const PhysicalType right_type = equality.right.identity
                                        ? PhysicalType::kInt64
                                        : equality.right.column->physical_type;
    if (left_type != right_type) {
      return Status::BindError("T-Cypher", "join property types are incompatible");
    }
    if (equality.left.identity) {
      Status demanded = state.MarkJoinIdentity(equality.left.variable);
      if (!demanded.ok()) return demanded;
    }
    if (equality.right.identity) {
      Status demanded = state.MarkJoinIdentity(equality.right.variable);
      if (!demanded.ok()) return demanded;
    }
    bound->joins.push_back(std::move(equality));
    return Status::OK();
  };
  if (statement.where.has_value()) {
    status = bind_predicate(*statement.where);
    if (!status.ok()) return status;
  }
  for (const StringEqualityPredicate& predicate : statement.and_predicates) {
    status = bind_predicate(predicate);
    if (!status.ok()) return status;
  }
  state.Finish();
  return BuildBoundUseSites(statement, bound);
}

}  // namespace

StatusOr<BoundTcypherStatement> BindTcypher(
    const TcypherStatement& statement, const TcypherBindingContext& context) {
  BoundTcypherStatement bound;
  bound.syntax = statement;
  const Status schema_status = BindSchemaMetadata(statement, context, &bound);
  if (!schema_status.ok()) return schema_status;
  if (statement.kind == TcypherStatementKind::kCreate ||
      statement.kind == TcypherStatementKind::kSet ||
      statement.kind == TcypherStatementKind::kDelete) {
    if (!statement.mutation.has_value()) {
      return Status::BindError("T-Cypher", "mutation has no payload");
    }
    const auto valid_from = ResolveTemporalValue(statement.mutation->valid_from, context);
    if (!valid_from.ok()) return valid_from.status();
    bound.mutation_valid_from = valid_from.ValueOrDie();
    if (statement.kind != TcypherStatementKind::kCreate) {
      if (!statement.match.entity_id.has_value()) {
        return Status::BindError("T-Cypher", "mutation requires an exact id MATCH");
      }
      const auto entity_id = ResolveTemporalValue(*statement.match.entity_id, context);
      if (!entity_id.ok()) return entity_id.status();
      bound.mutation_target_entity_id = entity_id.ValueOrDie();
    }
    return bound;
  }
  for (const TemporalScope& scope : statement.temporal_scopes) {
    const auto start = ResolveTemporalValue(scope.start, context);
    if (!start.ok()) return start.status();
    BoundTemporalScope resolved{scope.axis, scope.mode, context.statement_start_valid_time,
                                0, context.visible_seq_ceiling, 0, 0};
    if (scope.mode == TemporalScopeMode::kStateAsOf &&
        scope.axis == TemporalAxis::kSystemTime) {
      const auto sequence = context.commit_timeline.ResolveAsOf(
          start.ValueOrDie(), context.visible_seq_ceiling);
      if (!sequence.ok()) return sequence.status();
      resolved.snapshot_seq = sequence.ValueOrDie();
    } else if (scope.axis == TemporalAxis::kValidTime) {
      resolved.valid_time_start = start.ValueOrDie();
    } else {
      resolved.system_time_start = start.ValueOrDie();
    }
    if (scope.mode != TemporalScopeMode::kStateAsOf) {
      if (!scope.end.has_value()) {
        return Status::BindError("T-Cypher", "range scope has no end value");
      }
      const auto end = ResolveTemporalValue(*scope.end, context);
      if (!end.ok()) return end.status();
      if (start.ValueOrDie() >= end.ValueOrDie()) {
        return Status::BindError("T-Cypher", "temporal range is empty or reversed");
      }
      if (scope.axis == TemporalAxis::kValidTime) {
        resolved.valid_time_end = end.ValueOrDie();
      } else {
        resolved.system_time_end = end.ValueOrDie();
      }
    }
    bound.temporal_scopes.push_back(resolved);
  }
  for (const TemporalScope& scope : statement.match_temporal_scopes) {
    const auto start = ResolveTemporalValue(scope.start, context);
    if (!start.ok()) return start.status();
    BoundTemporalScope resolved{scope.axis, scope.mode, context.statement_start_valid_time,
                                0, context.visible_seq_ceiling, 0, 0};
    if (scope.mode == TemporalScopeMode::kStateAsOf &&
        scope.axis == TemporalAxis::kSystemTime) {
      const auto sequence = context.commit_timeline.ResolveAsOf(
          start.ValueOrDie(), context.visible_seq_ceiling);
      if (!sequence.ok()) return sequence.status();
      resolved.snapshot_seq = sequence.ValueOrDie();
    } else if (scope.axis == TemporalAxis::kValidTime) {
      resolved.valid_time_start = start.ValueOrDie();
    } else {
      return Status::BindError("T-Cypher", "MATCH SYSTEM_TIME supports only AS OF");
    }
    if (scope.mode != TemporalScopeMode::kStateAsOf) {
      if (!scope.end.has_value()) return Status::BindError("T-Cypher", "MATCH range has no end");
      const auto end = ResolveTemporalValue(*scope.end, context);
      if (!end.ok()) return end.status();
      if (start.ValueOrDie() >= end.ValueOrDie()) {
        return Status::BindError("T-Cypher", "MATCH temporal range is empty or reversed");
      }
      resolved.valid_time_end = end.ValueOrDie();
    }
    bound.primary_match_scopes.push_back(resolved);
  }
  for (const MatchClause& clause : statement.additional_matches) {
    std::vector<BoundTemporalScope> resolved_scopes;
    resolved_scopes.reserve(clause.temporal_scopes.size());
    for (const TemporalScope& scope : clause.temporal_scopes) {
      const auto start = ResolveTemporalValue(scope.start, context);
      if (!start.ok()) return start.status();
      BoundTemporalScope resolved{scope.axis, scope.mode, context.statement_start_valid_time,
                                  0, context.visible_seq_ceiling, 0, 0};
      if (scope.mode == TemporalScopeMode::kStateAsOf &&
          scope.axis == TemporalAxis::kSystemTime) {
        const auto sequence = context.commit_timeline.ResolveAsOf(
            start.ValueOrDie(), context.visible_seq_ceiling);
        if (!sequence.ok()) return sequence.status();
        resolved.snapshot_seq = sequence.ValueOrDie();
      } else if (scope.axis == TemporalAxis::kValidTime) {
        resolved.valid_time_start = start.ValueOrDie();
      } else {
        return Status::BindError("T-Cypher", "MATCH SYSTEM_TIME supports only AS OF");
      }
      if (scope.mode != TemporalScopeMode::kStateAsOf) {
        if (!scope.end.has_value()) return Status::BindError("T-Cypher", "MATCH range has no end");
        const auto end = ResolveTemporalValue(*scope.end, context);
        if (!end.ok()) return end.status();
        if (start.ValueOrDie() >= end.ValueOrDie()) {
          return Status::BindError("T-Cypher", "MATCH temporal range is empty or reversed");
        }
        resolved.valid_time_end = end.ValueOrDie();
      }
      resolved_scopes.push_back(resolved);
    }
    bound.additional_match_scopes.push_back(std::move(resolved_scopes));
  }
  if (statement.match.entity_id.has_value()) {
    bound.root_exact_entity_id = statement.match.entity_id->integer_literal;
    bound.root_exact_entity_parameter = statement.match.entity_id->parameter_name;
  }
  const auto point_scope = [](const BoundTemporalScope& scope) {
    return scope.mode == TemporalScopeMode::kStateAsOf;
  };
  bound.root_point_candidate = statement.kind == TcypherStatementKind::kQuery &&
      statement.relationships.empty() && statement.additional_matches.empty() &&
      bound.variables.size() == 1 &&
      bound.variables.front().kind == BoundGraphKind::kNode && bound.joins.empty() &&
      std::all_of(bound.temporal_scopes.begin(), bound.temporal_scopes.end(),
                  point_scope) &&
      std::all_of(bound.primary_match_scopes.begin(),
                  bound.primary_match_scopes.end(), point_scope) &&
      bound.additional_match_scopes.empty();
  const auto range_or_change_scope = [](const BoundTemporalScope& scope) {
    return scope.mode == TemporalScopeMode::kStateBetween ||
        scope.mode == TemporalScopeMode::kChangesBetween;
  };
  const bool has_range_or_change = std::any_of(
      bound.temporal_scopes.begin(), bound.temporal_scopes.end(),
      range_or_change_scope) ||
      std::any_of(bound.primary_match_scopes.begin(),
                  bound.primary_match_scopes.end(), range_or_change_scope);
  bound.root_temporal_candidate =
      statement.kind == TcypherStatementKind::kQuery && has_range_or_change &&
      statement.relationships.empty() && statement.additional_matches.empty() &&
      bound.variables.size() == 1 &&
      bound.variables.front().kind == BoundGraphKind::kNode &&
      bound.joins.empty();
  const bool has_supported_expand_bindings = std::all_of(
      statement.returns.begin(), statement.returns.end(),
      [&statement](const ReturnExpression& expression) {
        const bool node_binding = expression.kind == ReturnExpressionKind::kBinding &&
            (expression.variable == statement.match.variable ||
             (!statement.expanded_nodes.empty() &&
              expression.variable == statement.expanded_nodes.front().variable));
        const bool target_property = expression.kind == ReturnExpressionKind::kProperty &&
            !statement.expanded_nodes.empty() &&
            expression.variable == statement.expanded_nodes.front().variable;
        const bool source_property = expression.kind == ReturnExpressionKind::kProperty &&
            expression.variable == statement.match.variable;
        const bool relationship_projection = !statement.relationships.empty() &&
            expression.variable == statement.relationships.front().variable &&
            (expression.kind == ReturnExpressionKind::kBinding ||
             expression.kind == ReturnExpressionKind::kValidFrom ||
             expression.kind == ReturnExpressionKind::kValidTo ||
             expression.kind == ReturnExpressionKind::kCommitSeq ||
             expression.kind == ReturnExpressionKind::kOperation ||
             expression.kind == ReturnExpressionKind::kSystemTime ||
             expression.kind == ReturnExpressionKind::kProperty);
        return node_binding || source_property || target_property || relationship_projection;
      });
  const bool has_supported_expand_predicates = std::all_of(
      bound.predicates.begin(), bound.predicates.end(), [&bound, &statement](
          const BoundPredicateExpression& predicate) {
        const auto property = std::find_if(
            bound.properties.begin(), bound.properties.end(), [&predicate](
                const BoundPropertyReference& candidate) {
              return candidate.property_id == predicate.property_id;
            });
        return property != bound.properties.end() && !statement.relationships.empty() &&
            (property->variable == statement.match.variable ||
             property->variable == statement.relationships.front().variable ||
             (!statement.expanded_nodes.empty() &&
              property->variable == statement.expanded_nodes.front().variable));
      });
  const auto finite_relationship_segment = [](
      const MatchRelationshipPattern& relationship) {
    return !relationship.variable.empty() && relationship.min_hops > 0 &&
        relationship.max_hops >= relationship.min_hops;
  };
  const auto relationship_pattern = [&statement](const std::string& variable)
      -> const MatchRelationshipPattern* {
    const auto found = std::find_if(
        statement.relationships.begin(), statement.relationships.end(),
        [&variable](const MatchRelationshipPattern& relationship) {
          return relationship.variable == variable;
        });
    return found == statement.relationships.end() ? nullptr : &*found;
  };
  const bool supports_multi_hop_binding_expand =
      statement.relationships.size() > 1 &&
      statement.relationships.size() == statement.expanded_nodes.size() &&
      std::all_of(statement.relationships.begin(), statement.relationships.end(),
                  finite_relationship_segment) &&
      std::all_of(bound.properties.begin(), bound.properties.end(),
                  [&relationship_pattern](const BoundPropertyReference& property) {
                    const MatchRelationshipPattern* relationship =
                        relationship_pattern(property.variable);
                    return relationship == nullptr || !relationship->variable_length;
                  }) &&
      std::all_of(statement.returns.begin(), statement.returns.end(),
                  [&statement, &relationship_pattern](
                      const ReturnExpression& expression) {
                    if (expression.variable == statement.match.variable) {
                      return expression.kind == ReturnExpressionKind::kBinding ||
                          expression.kind == ReturnExpressionKind::kProperty;
                    }
                    const bool node = std::any_of(statement.expanded_nodes.begin(),
                                       statement.expanded_nodes.end(),
                                       [&expression](const MatchNodePattern& node) {
                                         return expression.variable == node.variable;
                                       });
                    if (node) {
                      return expression.kind == ReturnExpressionKind::kBinding ||
                          expression.kind == ReturnExpressionKind::kProperty;
                    }
                    const MatchRelationshipPattern* relationship =
                        relationship_pattern(expression.variable);
                    if (relationship == nullptr) return false;
                    if (relationship->variable_length) {
                      return expression.kind == ReturnExpressionKind::kBinding;
                    }
                    return expression.kind == ReturnExpressionKind::kBinding ||
                         expression.kind == ReturnExpressionKind::kValidFrom ||
                         expression.kind == ReturnExpressionKind::kValidTo ||
                         expression.kind == ReturnExpressionKind::kCommitSeq ||
                         expression.kind == ReturnExpressionKind::kOperation ||
                         expression.kind == ReturnExpressionKind::kSystemTime ||
                         expression.kind == ReturnExpressionKind::kProperty;
                  });
  const bool supports_variable_point_expand =
      statement.relationships.size() == 1 &&
      statement.expanded_nodes.size() == 1 &&
      statement.relationships.front().variable_length &&
      std::all_of(bound.properties.begin(), bound.properties.end(),
                  [&statement](const BoundPropertyReference& property) {
                    return (property.variable == statement.match.variable ||
                            property.variable ==
                                statement.expanded_nodes.front().variable) &&
                        (property.projection || property.predicate) &&
                        !property.grouping && !property.ordering &&
                        !property.join;
                  }) &&
      std::all_of(statement.returns.begin(), statement.returns.end(),
                  [&statement](const ReturnExpression& expression) {
                    const bool endpoint =
                        expression.variable == statement.match.variable ||
                        expression.variable ==
                            statement.expanded_nodes.front().variable;
                    const bool path_binding =
                        expression.variable ==
                            statement.relationships.front().variable &&
                        expression.kind == ReturnExpressionKind::kBinding;
                    return (endpoint &&
                            (expression.kind == ReturnExpressionKind::kBinding ||
                             expression.kind == ReturnExpressionKind::kProperty)) ||
                        path_binding;
                  });
  bound.fixed_expand_point_candidate =
      statement.kind == TcypherStatementKind::kQuery &&
      ((statement.relationships.size() == 1 && statement.expanded_nodes.size() == 1 &&
        !statement.relationships.front().variable_length &&
        has_supported_expand_predicates && has_supported_expand_bindings) ||
       supports_variable_point_expand ||
       supports_multi_hop_binding_expand) &&
      statement.additional_matches.empty() &&
      bound.variables.size() == 1 + statement.relationships.size() +
          statement.expanded_nodes.size() &&
      bound.joins.empty() && !statement.distinct && !statement.order_by.has_value() &&
      std::all_of(bound.temporal_scopes.begin(), bound.temporal_scopes.end(), point_scope) &&
      std::all_of(bound.primary_match_scopes.begin(), bound.primary_match_scopes.end(), point_scope) &&
      (statement.relationships.size() == 1 ||
       bound.variables.size() == 1 + statement.relationships.size() +
           statement.expanded_nodes.size());
  const auto range_scope = [](const BoundTemporalScope& scope) {
    return scope.axis == TemporalAxis::kValidTime &&
        scope.mode == TemporalScopeMode::kStateBetween;
  };
  const bool has_supported_range_expand_bindings = std::all_of(
      statement.returns.begin(), statement.returns.end(), [&statement](
          const ReturnExpression& expression) {
        if (expression.variable == statement.match.variable ||
            (!statement.expanded_nodes.empty() &&
             expression.variable == statement.expanded_nodes.front().variable)) {
          return expression.kind == ReturnExpressionKind::kBinding ||
              expression.kind == ReturnExpressionKind::kProperty;
        }
        return !statement.relationships.empty() &&
            expression.variable == statement.relationships.front().variable &&
            (expression.kind == ReturnExpressionKind::kBinding ||
             expression.kind == ReturnExpressionKind::kProperty ||
             expression.kind == ReturnExpressionKind::kValidFrom ||
             expression.kind == ReturnExpressionKind::kValidTo ||
             expression.kind == ReturnExpressionKind::kCommitSeq ||
             expression.kind == ReturnExpressionKind::kOperation ||
             expression.kind == ReturnExpressionKind::kSystemTime);
      });
  const bool supports_multi_hop_range_expand =
      statement.relationships.size() > 1 &&
      statement.relationships.size() == statement.expanded_nodes.size() &&
      std::all_of(statement.relationships.begin(), statement.relationships.end(),
                  finite_relationship_segment) &&
      std::all_of(bound.properties.begin(), bound.properties.end(),
                  [&statement, &relationship_pattern](
                      const BoundPropertyReference& property) {
                    const bool node = property.variable == statement.match.variable ||
                        std::any_of(statement.expanded_nodes.begin(),
                                    statement.expanded_nodes.end(),
                                    [&property](const MatchNodePattern& node) {
                                      return property.variable == node.variable;
                                    });
                    const MatchRelationshipPattern* relationship =
                        relationship_pattern(property.variable);
                    return (node || (relationship != nullptr &&
                                     !relationship->variable_length)) &&
                        (property.projection || property.predicate) && !property.grouping &&
                        !property.ordering && !property.join;
                  }) &&
      std::all_of(statement.returns.begin(), statement.returns.end(),
                  [&statement, &relationship_pattern](
                      const ReturnExpression& expression) {
                    const bool node = expression.variable == statement.match.variable ||
                        std::any_of(statement.expanded_nodes.begin(),
                                    statement.expanded_nodes.end(),
                                    [&expression](const MatchNodePattern& node) {
                                      return expression.variable == node.variable;
                                    });
                    if (node) {
                      return expression.kind == ReturnExpressionKind::kBinding ||
                          expression.kind == ReturnExpressionKind::kProperty;
                    }
                    const MatchRelationshipPattern* relationship =
                        relationship_pattern(expression.variable);
                    if (relationship == nullptr) return false;
                    if (relationship->variable_length) {
                      return expression.kind == ReturnExpressionKind::kBinding ||
                          expression.kind == ReturnExpressionKind::kValidFrom ||
                          expression.kind == ReturnExpressionKind::kValidTo;
                    }
                    return expression.kind == ReturnExpressionKind::kBinding ||
                         expression.kind == ReturnExpressionKind::kValidFrom ||
                         expression.kind == ReturnExpressionKind::kValidTo ||
                         expression.kind == ReturnExpressionKind::kCommitSeq ||
                         expression.kind == ReturnExpressionKind::kOperation ||
                         expression.kind == ReturnExpressionKind::kSystemTime ||
                         expression.kind == ReturnExpressionKind::kProperty;
                  });
  const bool supports_variable_range_expand =
      statement.relationships.size() == 1 && statement.expanded_nodes.size() == 1 &&
      statement.relationships.front().variable_length &&
      std::all_of(bound.properties.begin(), bound.properties.end(), [&statement](
          const BoundPropertyReference& property) {
        return (property.variable == statement.match.variable ||
                property.variable == statement.expanded_nodes.front().variable) &&
            (property.projection || property.predicate) && !property.grouping &&
            !property.ordering && !property.join;
      }) &&
      std::all_of(statement.returns.begin(), statement.returns.end(), [&statement](
          const ReturnExpression& expression) {
        const bool endpoint = expression.variable == statement.match.variable ||
            expression.variable == statement.expanded_nodes.front().variable;
        const bool path_temporal =
            expression.variable == statement.relationships.front().variable &&
            (expression.kind == ReturnExpressionKind::kValidFrom ||
             expression.kind == ReturnExpressionKind::kValidTo);
        return (endpoint && (expression.kind == ReturnExpressionKind::kBinding ||
                             expression.kind == ReturnExpressionKind::kProperty ||
                             expression.kind == ReturnExpressionKind::kValidFrom ||
                             expression.kind == ReturnExpressionKind::kValidTo)) ||
            path_temporal;
      });
  bound.fixed_expand_range_candidate =
      statement.kind == TcypherStatementKind::kQuery &&
      ((statement.relationships.size() == 1 && statement.expanded_nodes.size() == 1 &&
        !statement.relationships.front().variable_length &&
        has_supported_range_expand_bindings) || supports_multi_hop_range_expand ||
       supports_variable_range_expand) &&
      statement.additional_matches.empty() &&
      bound.variables.size() == 1 + statement.relationships.size() +
          statement.expanded_nodes.size() &&
      bound.joins.empty() && !statement.distinct && !statement.order_by.has_value() &&
      !(bound.primary_match_scopes.empty() && bound.temporal_scopes.empty()) &&
      std::all_of((bound.primary_match_scopes.empty() ? bound.temporal_scopes
                                                       : bound.primary_match_scopes).begin(),
                  (bound.primary_match_scopes.empty() ? bound.temporal_scopes
                                                       : bound.primary_match_scopes).end(),
                  range_scope);
  const bool supports_fixed_change_expand =
      statement.relationships.size() == 1 &&
      statement.expanded_nodes.size() == 1 &&
      !statement.relationships.front().variable_length &&
      bound.predicates.empty() &&
      std::all_of(bound.properties.begin(), bound.properties.end(),
                  [](const BoundPropertyReference& property) {
                    return property.projection && !property.predicate &&
                        !property.grouping && !property.ordering &&
                        !property.join;
                  }) &&
      std::all_of(statement.returns.begin(), statement.returns.end(),
                  [&statement](const ReturnExpression& expression) {
                    const bool owned =
                        expression.variable == statement.match.variable ||
                        expression.variable ==
                            statement.relationships.front().variable ||
                        expression.variable ==
                            statement.expanded_nodes.front().variable;
                    return owned &&
                        (expression.kind == ReturnExpressionKind::kBinding ||
                         expression.kind == ReturnExpressionKind::kProperty ||
                         expression.kind == ReturnExpressionKind::kValidFrom ||
                         expression.kind == ReturnExpressionKind::kCommitSeq ||
                         expression.kind == ReturnExpressionKind::kOperation ||
                         expression.kind == ReturnExpressionKind::kSystemTime);
                  });
  const std::vector<BoundTemporalScope>& effective_scopes =
      bound.primary_match_scopes.empty() ? bound.temporal_scopes
                                         : bound.primary_match_scopes;
  const size_t change_scope_count = static_cast<size_t>(std::count_if(
      effective_scopes.begin(), effective_scopes.end(),
      [](const BoundTemporalScope& scope) {
        return scope.mode == TemporalScopeMode::kChangesBetween;
      }));
  bound.fixed_expand_change_candidate =
      statement.kind == TcypherStatementKind::kQuery &&
      supports_fixed_change_expand && statement.additional_matches.empty() &&
      bound.variables.size() == 3 && bound.joins.empty() &&
      !statement.distinct && !statement.order_by.has_value() &&
      change_scope_count == 1;
  return bound;
}

}  // namespace cedar
