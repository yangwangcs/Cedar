#include "cypher/semantic_key.h"

#include <utility>

namespace cedar::cypher {
namespace {

class SemanticKeyWriter {
 public:
  void Integer(uint64_t value) {
    key_.push_back('i');
    for (unsigned shift = 0; shift < 64; shift += 8) {
      key_.push_back(static_cast<char>(value >> shift));
    }
  }

  void Text(std::string_view value) {
    key_.push_back('s');
    Integer(value.size());
    key_.append(value);
  }

  template <typename T, typename Emit>
  void Optional(const std::optional<T>& value, Emit emit) {
    key_.push_back(value ? '1' : '0');
    if (value) emit(*value);
  }

  template <typename Range, typename Emit>
  void Sequence(const Range& values, Emit emit) {
    key_.push_back('v');
    Integer(values.size());
    for (const auto& value : values) emit(value);
  }

  void Scope(const std::optional<TimeScope>& scope) {
    Optional(scope, [&](const TimeScope& value) {
      Integer(value.from);
      Optional(value.to, [&](uint64_t number) { Integer(number); });
      Optional(value.as_of, [&](uint64_t number) { Integer(number); });
    });
  }

  void Parameter(const BoundParameter& parameter) {
    Text(parameter.name);
    Integer(parameter.id.value);
  }

  std::string Finish() && { return std::move(key_); }

 private:
  std::string key_;
};

}  // namespace

std::string PreparedSemanticKey(const BoundStatement& statement,
                                std::string_view schema_fingerprint,
                                const BinderOptions& options) {
  SemanticKeyWriter out;
  out.Text("cypher-bound-v1");
  out.Text(schema_fingerprint);
  out.Text(options.graph);
  out.Integer(options.part_id.value);
  out.Integer(options.require_explicit_part_id);
  out.Integer(options.max_hops);
  out.Text(options.security_digest);
  out.Integer(static_cast<uint8_t>(statement.kind));
  out.Text(statement.graph);
  out.Integer(statement.part_id.value);
  out.Integer(static_cast<uint8_t>(statement.part_scope.kind));
  out.Sequence(statement.part_scope.parts,
               [&](PartId part) { out.Integer(part.value); });
  out.Scope(statement.valid_time);
  out.Scope(statement.system_time);
  out.Integer(statement.changes);
  out.Sequence(statement.patterns, [&](const PathPattern& pattern) {
    out.Text(pattern.source);
    out.Text(pattern.source_label);
    out.Text(pattern.edge);
    out.Text(pattern.relationship);
    out.Text(pattern.destination);
    out.Text(pattern.destination_label);
    out.Integer(pattern.min_hops);
    out.Integer(pattern.max_hops);
    out.Integer(pattern.trail);
    out.Optional(pattern.source_part_id,
                 [&](uint32_t id) { out.Integer(id); });
    out.Optional(pattern.source_vertex_id,
                 [&](uint64_t id) { out.Integer(id); });
    out.Optional(pattern.destination_part_id,
                 [&](uint32_t id) { out.Integer(id); });
    out.Optional(pattern.destination_vertex_id,
                 [&](uint64_t id) { out.Integer(id); });
  });
  out.Sequence(statement.predicates, [&](const BoundPredicate& predicate) {
    out.Text(predicate.variable);
    out.Integer(predicate.property_id.value);
    out.Integer(static_cast<uint8_t>(predicate.physical_type));
    out.Integer(static_cast<uint8_t>(predicate.op));
    out.Optional(predicate.literal,
                 [&](const std::string& value) { out.Text(value); });
    out.Optional(predicate.parameter,
                 [&](const BoundParameter& value) { out.Parameter(value); });
  });
  out.Sequence(statement.relationship_types,
               [&](const std::optional<uint64_t>& type) {
    out.Optional(type, [&](uint64_t value) { out.Integer(value); });
  });
  out.Sequence(statement.projections, [&](const ProjectionItem& projection) {
    out.Text(projection.expression);
    out.Text(projection.function);
  });
  out.Optional(statement.limit_count,
               [&](size_t count) { out.Integer(count); });
  out.Sequence(statement.parameters,
               [&](const BoundParameter& value) { out.Parameter(value); });
  out.Sequence(statement.assignments, [&](const BoundAssignment& assignment) {
    out.Text(assignment.target);
    out.Integer(assignment.property_id.value);
    out.Integer(assignment.parameter.value);
  });
  out.Sequence(statement.deletions,
               [&](const std::string& target) { out.Text(target); });
  out.Integer(statement.demand.vertex_families);
  out.Integer(statement.demand.edge_families);
  out.Sequence(statement.demand.properties,
               [&](PropertyId property) { out.Integer(property.value); });
  out.Integer(statement.demand.needs_event_history);
  out.Integer(statement.demand.needs_system_metadata);
  return std::move(out).Finish();
}

}  // namespace cedar::cypher
