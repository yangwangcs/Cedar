#include "cedar/cypher/binder.h"

#include <algorithm>
#include <iterator>
#include <sstream>

#include "cedar/cypher/fingerprint.h"

namespace cedar::cypher {
namespace {

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
  for (const auto& existing : properties_) {
    if (existing.name == definition.name &&
        existing.entity_kind == definition.entity_kind) {
      if (existing == definition) return Status::OK();
      return Status::SchemaMismatch("cypher schema", "property name already has a different definition");
    }
    if (existing.property_id == definition.property_id && existing != definition) {
      return Status::SchemaMismatch("cypher schema", "property ID already has a different definition");
    }
  }
  properties_.push_back(std::move(definition));
  return Status::OK();
}

StatusOr<PropertyDefinition> SchemaCatalog::Find(
    const std::string& name, std::optional<PropertyEntityKind> kind) const {
  for (const auto& property : properties_) {
    if (property.name == name && (!kind.has_value() || property.entity_kind == *kind)) {
      return property;
    }
  }
  return Status::BindError("cypher schema", "unknown property");
}

StatusOr<BoundStatement> Bind(const Statement& statement,
                              const SchemaCatalog& catalog,
                              const BinderOptions& options) {
  if (statement.graph.has_value() && !options.graph.empty() &&
      *statement.graph != options.graph) {
    return BindError("query graph differs from configured graph");
  }
  if (options.require_explicit_part_id && options.part_id.value == 0) {
    return BindError("distributed identity requires a PartID");
  }
  BoundStatement bound;
  bound.kind = statement.kind;
  bound.graph = statement.graph.value_or(options.graph);
  bound.part_id = options.part_id;
  bound.valid_time = statement.valid_time;
  bound.system_time = statement.system_time;
  bound.changes = statement.changes;
  bound.patterns = statement.patterns;
  bound.projections = statement.projections;
  bound.deletions = statement.deletions;
  bound.demand.needs_event_history = statement.changes;
  bound.demand.needs_system_metadata = statement.system_time.has_value();
  std::string canonical = "v2|graph=" + bound.graph + "|part=" + std::to_string(bound.part_id.value);
  canonical += "|valid=" + ScopeFingerprint(bound.valid_time) + "|system=" + ScopeFingerprint(bound.system_time);
  canonical += "|changes=" + std::to_string(bound.changes ? 1 : 0);
  for (const auto& pattern : statement.patterns) {
    if (pattern.max_hops > options.max_hops || pattern.max_hops == 0) {
      return BindError("path hop bound exceeds configured limit");
    }
    ++bound.demand.vertex_families;
    if (!pattern.edge.empty() || !pattern.relationship.empty() || pattern.max_hops != 1) {
      ++bound.demand.edge_families;
    }
    canonical += "|path=" + pattern.source_label + ':' + pattern.relationship + ':' +
                 std::to_string(pattern.min_hops) + ':' + std::to_string(pattern.max_hops) +
                 ':' + std::to_string(pattern.trail ? 1 : 0);
  }
  for (const auto& assignment : statement.assignments) {
    auto property = catalog.Find(assignment.property, PropertyEntityKind::kVertex);
    if (!property.ok()) property = catalog.Find(assignment.property, PropertyEntityKind::kEdge);
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
    canonical += "|set=" + std::to_string(property.ValueOrDie().property_id.value);
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
  canonical += "|security=" + options.security_digest;
  bound.fingerprint = StableFingerprint(canonical);
  return bound;
}

}  // namespace cedar::cypher
