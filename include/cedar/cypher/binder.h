#ifndef CEDAR_CYPHER_BINDER_H_
#define CEDAR_CYPHER_BINDER_H_

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/cypher/ast.h"
#include "cedar/fact/fact.h"
#include "cedar/fact/read_spec.h"
#include "cedar/query/types.h"
#include "cedar/schema.h"
#include "cedar/schema_provider.h"

namespace cedar::internal {
class ReadCatalog;
}

namespace cedar::cypher {

class SchemaCatalog final : public SchemaProvider {
 public:
  Status Add(PropertyDefinition definition);
  StatusOr<PropertyDefinition> Find(const std::string& name,
                                    std::optional<PropertyEntityKind> kind = {}) const;
  StatusOr<std::optional<PropertyDefinition>> Lookup(
      PropertyId property_id) const override;
  StatusOr<std::optional<PropertyDefinition>> LookupByName(
      std::string_view name,
      std::optional<PropertyEntityKind> kind = {}) const override;
 std::string Fingerprint() const override;

 private:
  struct NameKindKey {
    std::string name;
    PropertyEntityKind kind = PropertyEntityKind::kVertex;
    bool operator==(const NameKindKey&) const = default;
  };
  struct NameKindHash {
    size_t operator()(const NameKindKey& key) const noexcept {
      const size_t name_hash = std::hash<std::string>{}(key.name);
      return name_hash ^ (static_cast<size_t>(key.kind) +
                          (name_hash << 6) + (name_hash >> 2));
    }
  };
  std::vector<PropertyDefinition> properties_;
  // The immutable read catalog is the production lookup backend. The maps
  // remain construction-time compatibility state and are never consulted by
  // prepared query lookups once the catalog is published.
  std::unordered_map<uint16_t, PropertyDefinition> by_id_;
  std::unordered_map<NameKindKey, PropertyDefinition, NameKindHash> by_name_kind_;
  std::shared_ptr<const internal::ReadCatalog> read_catalog_;
};

struct BinderOptions {
  std::string graph;
  PartId part_id{0};
  bool require_explicit_part_id = false;
  uint32_t max_hops = 64;
  std::string security_digest;
};

struct BoundParameter {
  std::string name;
  ParameterId id;
};

struct FactDemandSet {
  uint32_t vertex_families = 0;
  uint32_t edge_families = 0;
  std::vector<PropertyId> properties;
  bool needs_event_history = false;
  bool needs_system_metadata = false;
};

struct BoundAssignment {
  std::string target;
  PropertyId property_id;
  ParameterId parameter;
};

struct BoundPredicate {
  std::string variable;
  PropertyId property_id;
  PhysicalType physical_type = PhysicalType::kBinary;
  PredicateOperator op = PredicateOperator::kEqual;
  std::optional<std::string> literal;
  std::optional<BoundParameter> parameter;
};

struct BoundStatement {
  StatementKind kind = StatementKind::kRead;
  std::string graph;
  PartId part_id{0};
  // Explicit routing is carried independently from the numeric value so
  // PartId{0} remains a valid partition and never doubles as wildcard.
  PartScope part_scope = PartScope::All();
  std::optional<TimeScope> valid_time;
  std::optional<TimeScope> system_time;
  bool changes = false;
  std::vector<PathPattern> patterns;
  std::vector<BoundPredicate> predicates;
  std::vector<std::optional<uint64_t>> relationship_types;
  std::vector<ProjectionItem> projections;
  std::optional<size_t> limit_count;
  std::vector<BoundParameter> parameters;
  std::vector<BoundAssignment> assignments;
  std::vector<std::string> deletions;
  FactDemandSet demand;
  uint64_t fingerprint = 0;
};

StatusOr<BoundStatement> Bind(const Statement& statement,
                              const SchemaCatalog& catalog,
                              const BinderOptions& options = {});
StatusOr<BoundStatement> Bind(const Statement& statement,
                              const SchemaProvider& provider,
                              const BinderOptions& options = {});

}  // namespace cedar::cypher

#endif  // CEDAR_CYPHER_BINDER_H_
