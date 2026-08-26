#include "cedar/cypher/binder.h"

#include <algorithm>
#include <iterator>
#include <sstream>
#include <unordered_set>

#include "cedar/cypher/fingerprint.h"
#include "cedar/cypher/relationship_type.h"
#include "query/read/read_catalog.h"

namespace cedar::cypher {
namespace {

void RebuildReadCatalog(const std::vector<PropertyDefinition>& properties,
                        std::shared_ptr<const internal::ReadCatalog>* target) {
  internal::ReadCatalogBuilder builder;
  for (const PropertyDefinition& property : properties) {
    if (!builder.AddSchema(property).ok()) return;
  }
  auto catalog = std::move(builder).Finish();
  if (catalog.ok()) *target = std::move(catalog).ConsumeValueOrDie();
}

Status BindError(const char* message) {
  return Status::BindError("cypher binder", message);
}

std::string ScopeFingerprint(const std::optional<TimeScope>& scope) {
  if (!scope.has_value()) return "none";
  std::ostringstream out;
  if (scope->as_of.has_value()) return "asof:" + std::to_string(*scope->as_of);
  out << scope->from << ':' << (scope->to.has_value() ? std::to_string(*scope->to) : "-");
  return out.str();
}

}  // namespace

Status SchemaCatalog::Add(PropertyDefinition definition) {
  const Status valid = definition.Validate();
  if (!valid.ok()) return valid;
  const NameKindKey name_key{definition.name, definition.entity_kind};
  const auto by_name = by_name_kind_.find(name_key);
  if (by_name != by_name_kind_.end()) {
    if (by_name->second == definition) return Status::OK();
    return Status::SchemaMismatch("cypher schema", "property name already has a different definition");
  }
  const auto by_id = by_id_.find(definition.property_id.value);
  if (by_id != by_id_.end() && by_id->second != definition) {
    return Status::SchemaMismatch("cypher schema", "property ID already has a different definition");
  }
  by_id_[definition.property_id.value] = definition;
  by_name_kind_[name_key] = definition;
  properties_.push_back(std::move(definition));
  std::sort(properties_.begin(), properties_.end(),
            [](const PropertyDefinition& left, const PropertyDefinition& right) {
              if (left.name != right.name) return left.name < right.name;
              return static_cast<uint8_t>(left.entity_kind) <
                     static_cast<uint8_t>(right.entity_kind);
            });
  RebuildReadCatalog(properties_, &read_catalog_);
  return Status::OK();
}

StatusOr<PropertyDefinition> SchemaCatalog::Find(
    const std::string& name, std::optional<PropertyEntityKind> kind) const {
  if (kind.has_value()) {
    if (read_catalog_) {
      auto found = read_catalog_->LookupByName(name, kind);
      if (found.ok() && found.ValueOrDie().has_value()) return *found.ValueOrDie();
    } else {
      const auto found = by_name_kind_.find(NameKindKey{name, *kind});
      if (found != by_name_kind_.end()) return found->second;
    }
  } else {
    if (read_catalog_) {
      auto found = read_catalog_->LookupByName(name);
      if (found.ok() && found.ValueOrDie().has_value()) return *found.ValueOrDie();
    } else {
      const auto vertex = by_name_kind_.find(
          NameKindKey{name, PropertyEntityKind::kVertex});
      if (vertex != by_name_kind_.end()) return vertex->second;
      const auto edge = by_name_kind_.find(
          NameKindKey{name, PropertyEntityKind::kEdge});
      if (edge != by_name_kind_.end()) return edge->second;
    }
  }
  return Status::BindError("cypher schema", "unknown property");
}

StatusOr<std::optional<PropertyDefinition>> SchemaCatalog::Lookup(
    PropertyId property_id) const {
  if (read_catalog_) return read_catalog_->Lookup(property_id);
  const auto found = by_id_.find(property_id.value);
  if (found != by_id_.end()) return std::optional<PropertyDefinition>{found->second};
  return std::optional<PropertyDefinition>{};
}

StatusOr<std::optional<PropertyDefinition>> SchemaCatalog::LookupByName(
    std::string_view name, std::optional<PropertyEntityKind> kind) const {
  if (read_catalog_) return read_catalog_->LookupByName(name, kind);
  auto found = Find(std::string(name), kind);
  if (!found.ok()) {
    if (found.status().IsBindError()) return std::optional<PropertyDefinition>{};
    return found.status();
  }
  return std::optional<PropertyDefinition>{found.ValueOrDie()};
}

std::string SchemaCatalog::Fingerprint() const {
  uint64_t hash = 1469598103934665603ULL;
  auto mix = [&hash](std::string_view value) {
    for (unsigned char byte : value) {
      hash ^= byte;
      hash *= 1099511628211ULL;
    }
  };
  for (const auto& property : properties_) {
    mix(property.name);
    mix(std::to_string(property.property_id.value));
    mix(std::to_string(property.schema_epoch));
    mix(std::to_string(static_cast<uint8_t>(property.entity_kind)));
    mix(std::to_string(static_cast<uint8_t>(property.physical_type)));
  }
  return std::to_string(hash);
}

StatusOr<PropertyDefinition> LookupRequired(const SchemaProvider& provider,
                                            const std::string& name,
                                            PropertyEntityKind kind) {
  auto found = provider.LookupByName(name, kind);
  if (!found.ok()) return found.status();
  if (!found.ValueOrDie().has_value()) {
    return Status::BindError("cypher schema", "unknown property");
  }
  return *found.ValueOrDie();
}

StatusOr<BoundStatement> Bind(const Statement& statement,
                              const SchemaProvider& catalog,
                              const BinderOptions& options) {
  if (statement.graph.has_value() && !options.graph.empty() &&
      *statement.graph != options.graph) {
    return BindError("query graph differs from configured graph");
  }
  BoundStatement bound;
  bound.kind = statement.kind;
  bound.graph = statement.graph.value_or(options.graph);
  bound.part_id = options.part_id;
  bound.part_scope = (options.require_explicit_part_id || options.part_id.value != 0)
                         ? PartScope::Exact(options.part_id)
                         : PartScope::All();
  bound.valid_time = statement.valid_time;
  bound.system_time = statement.system_time;
  bound.changes = statement.changes;
  bound.patterns = statement.patterns;
  bound.projections = statement.projections;
  bound.limit_count = statement.limit_count;
  bound.deletions = statement.deletions;
  bound.demand.needs_event_history = statement.changes;
  bound.demand.needs_system_metadata = statement.system_time.has_value();
  if (statement.changes) {
    if (!statement.valid_time.has_value() || !statement.valid_time->to.has_value() ||
        statement.valid_time->as_of.has_value()) {
      return Status::InvalidArgument("cypher binder",
                                     "CHANGES requires a bounded valid-time range");
    }
  }
  if (statement.head == StatementHead::kMatch && statement.kind == StatementKind::kWrite) {
    return Status::InvalidArgument("cypher binder", "MATCH writes are not supported");
  }
  std::string canonical = "v2|graph=" + bound.graph + "|part=" + std::to_string(bound.part_id.value);
  canonical += "|kind=" + std::to_string(static_cast<uint8_t>(statement.kind));
  canonical += "|valid=" + ScopeFingerprint(bound.valid_time) + "|system=" + ScopeFingerprint(bound.system_time);
  canonical += "|changes=" + std::to_string(bound.changes ? 1 : 0);
  std::unordered_set<std::string> variables;
  for (size_t index = 0; index < statement.patterns.size(); ++index) {
    const auto& pattern = statement.patterns[index];
    if (pattern.source_part_id.has_value() != pattern.source_vertex_id.has_value()) {
      return BindError("vertex reference requires both part_id and id");
    }
    if (pattern.max_hops > options.max_hops || pattern.max_hops == 0) {
      return BindError("path hop bound exceeds configured limit");
    }
    if (!pattern.source_label.empty() || !pattern.destination_label.empty()) {
      return Status::NotSupported("cypher binder", "labels require a Cedar label fact family");
    }
    if (statement.kind == StatementKind::kWrite &&
        (pattern.min_hops != 1 || pattern.max_hops != 1)) {
      return Status::NotSupported("cypher binder", "variable-length CREATE is not supported");
    }
    auto add_variable = [&](const std::string& name) -> Status {
      if (name.empty()) return Status::OK();
      if (!variables.insert(name).second) {
        return Status::InvalidArgument("cypher binder", "graph variables must be unique");
      }
      return Status::OK();
    };
    const bool connected_source =
        index > 0 && statement.patterns[index - 1].destination == pattern.source;
    Status variable_status = connected_source ? Status::OK()
                                              : add_variable(pattern.source);
    if (!variable_status.ok()) return variable_status;
    if (!pattern.edge.empty() || !pattern.relationship.empty()) {
      variable_status = add_variable(pattern.edge);
      if (!variable_status.ok()) return variable_status;
      variable_status = add_variable(pattern.destination);
      if (!variable_status.ok()) return variable_status;
    }
    std::optional<uint64_t> relationship_type;
    if (!pattern.relationship.empty()) {
      const auto resolved = ResolveRelationshipType(pattern.relationship);
      if (!resolved.ok()) return resolved.status();
      relationship_type = resolved.ValueOrDie();
      canonical += "|edge_type=" + std::to_string(*relationship_type);
    }
    bound.relationship_types.push_back(relationship_type);
    ++bound.demand.vertex_families;
    if (!pattern.edge.empty() || !pattern.relationship.empty() || pattern.max_hops != 1) {
      ++bound.demand.edge_families;
    }
    canonical += "|path=" + pattern.source_label + ':' + pattern.relationship + ':' +
                 std::to_string(pattern.min_hops) + ':' + std::to_string(pattern.max_hops) +
                 ':' + std::to_string(pattern.trail ? 1 : 0);
  }
  for (const auto& predicate : statement.predicates) {
    if (variables.find(predicate.variable) == variables.end()) {
      return BindError("predicate references an unknown graph variable");
    }
    auto property = LookupRequired(catalog, predicate.property,
                                   PropertyEntityKind::kVertex);
    if (!property.ok()) return property.status();
    if (property.ValueOrDie().physical_type != PhysicalType::kString &&
        property.ValueOrDie().physical_type != PhysicalType::kInt32 &&
        property.ValueOrDie().physical_type != PhysicalType::kInt64 &&
        property.ValueOrDie().physical_type != PhysicalType::kFloat32 &&
        property.ValueOrDie().physical_type != PhysicalType::kFloat64) {
      return Status::NotSupported("cypher binder", "predicate type is not indexable");
    }
    BoundPredicate bound_predicate;
    bound_predicate.variable = predicate.variable;
    bound_predicate.property_id = property.ValueOrDie().property_id;
    bound_predicate.physical_type = property.ValueOrDie().physical_type;
    bound_predicate.op = predicate.op;
    bound_predicate.literal = predicate.literal;
    if (predicate.parameter.has_value()) {
      auto parameter = std::find_if(
          bound.parameters.begin(), bound.parameters.end(),
          [&](const BoundParameter& existing) {
            return existing.name == *predicate.parameter;
          });
      if (parameter == bound.parameters.end()) {
        bound.parameters.push_back(
            {*predicate.parameter,
             ParameterId{static_cast<uint32_t>(bound.parameters.size() + 1)}});
        parameter = std::prev(bound.parameters.end());
      }
      bound_predicate.parameter = *parameter;
    }
    bound.predicates.push_back(std::move(bound_predicate));
    bound.demand.properties.push_back(property.ValueOrDie().property_id);
    canonical += "|where=" + predicate.variable + "." + predicate.property;
  }
  for (const auto& assignment : statement.assignments) {
    auto property = LookupRequired(catalog, assignment.property,
                                   PropertyEntityKind::kVertex);
    if (!property.ok()) property = LookupRequired(catalog, assignment.property,
                                                  PropertyEntityKind::kEdge);
    if (!property.ok()) return property.status();
    auto parameter = std::find_if(
        bound.parameters.begin(), bound.parameters.end(),
        [&](const BoundParameter& existing) {
          return existing.name == assignment.parameter;
        });
    if (parameter == bound.parameters.end()) {
      bound.parameters.push_back(
          {assignment.parameter,
           ParameterId{static_cast<uint32_t>(bound.parameters.size())}});
      parameter = std::prev(bound.parameters.end());
    }
    BoundAssignment bound_assignment{assignment.target,
                                     property.ValueOrDie().property_id,
                                     parameter->id};
    bound.assignments.push_back(std::move(bound_assignment));
    bound.demand.properties.push_back(property.ValueOrDie().property_id);
    canonical += "|set=" + assignment.target + ":" +
                 std::to_string(property.ValueOrDie().property_id.value);
  }
  for (const auto& parameter : statement.parameters) {
    if (std::none_of(bound.parameters.begin(), bound.parameters.end(),
                     [&](const BoundParameter& existing) { return existing.name == parameter; })) {
      bound.parameters.push_back({parameter, ParameterId{static_cast<uint32_t>(bound.parameters.size())}});
    }
  }
  std::sort(bound.demand.properties.begin(), bound.demand.properties.end(),
            [](PropertyId left, PropertyId right) { return left.value < right.value; });
  for (PropertyId property : bound.demand.properties) canonical += "|prop=" + std::to_string(property.value);
  for (const auto& projection : statement.projections) {
    canonical += "|return=" + projection.expression + ":" + projection.function;
  }
  canonical += "|security=" + options.security_digest;
  bound.fingerprint = StableFingerprint(canonical);
  return bound;
}

StatusOr<BoundStatement> Bind(const Statement& statement,
                              const SchemaCatalog& catalog,
                              const BinderOptions& options) {
  return Bind(statement, static_cast<const SchemaProvider&>(catalog), options);
}

}  // namespace cedar::cypher
