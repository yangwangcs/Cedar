#ifndef CEDAR_CYPHER_BINDER_H_
#define CEDAR_CYPHER_BINDER_H_

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/cypher/ast.h"
#include "cedar/fact/fact.h"
#include "cedar/query/types.h"
#include "cedar/schema.h"

namespace cedar::cypher {

class SchemaCatalog {
 public:
  Status Add(PropertyDefinition definition);
  StatusOr<PropertyDefinition> Find(const std::string& name,
                                    std::optional<PropertyEntityKind> kind = {}) const;

 private:
  std::vector<PropertyDefinition> properties_;
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

struct BoundStatement {
  StatementKind kind = StatementKind::kRead;
  std::string graph;
  PartId part_id{0};
  std::optional<TimeScope> valid_time;
  std::optional<TimeScope> system_time;
  bool changes = false;
  std::vector<PathPattern> patterns;
  std::vector<ProjectionItem> projections;
  std::vector<BoundParameter> parameters;
  std::vector<BoundAssignment> assignments;
  std::vector<std::string> deletions;
  FactDemandSet demand;
  uint64_t fingerprint = 0;
};

StatusOr<BoundStatement> Bind(const Statement& statement,
                              const SchemaCatalog& catalog,
                              const BinderOptions& options = {});

}  // namespace cedar::cypher

#endif  // CEDAR_CYPHER_BINDER_H_
