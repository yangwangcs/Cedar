#include "cedar/cypher/write.h"

#include <unordered_map>
#include <utility>
#include <variant>

namespace cedar::cypher {
namespace {

struct WriteEntity {
  std::optional<VertexRef> vertex;
  std::optional<EdgeRef> edge;
};

uint64_t RelationshipType(const std::string& value) {
  uint64_t hash = 1469598103934665603ULL;
  for (unsigned char byte : value) {
    hash ^= byte;
    hash *= 1099511628211ULL;
  }
  return hash == 0 ? 1 : hash;
}

Status WriteError(const char* message) {
  return Status::InvalidArgument("cypher write", message);
}

StatusOr<Value> LookupValue(const Bindings& bindings, ParameterId parameter) {
  constexpr QueryType types[] = {
      QueryType::kBool, QueryType::kInt32, QueryType::kInt64,
      QueryType::kFloat32, QueryType::kFloat64, QueryType::kTimestamp64,
      QueryType::kString, QueryType::kBinary};
  for (QueryType type : types) {
    const auto value = bindings.Lookup(parameter, type);
    if (!value.ok()) continue;
    if (const auto* scalar = std::get_if<Value>(&value.ValueOrDie())) {
      return *scalar;
    }
  }
  return WriteError("missing or non-scalar SET parameter");
}

}  // namespace

Status StageWrite(Database& database, Transaction& tx,
                  const BoundStatement& statement, const Bindings& bindings,
                  ValidTime valid_time) {
  if (statement.kind != StatementKind::kWrite) return WriteError("statement is not a write");
  if (statement.system_time.has_value()) return WriteError("historical system-time writes are forbidden");
  if (statement.valid_time.has_value() && statement.valid_time->to.has_value()) {
    return WriteError("range writes are forbidden; supply one valid time");
  }
  if (statement.valid_time.has_value() && statement.valid_time->as_of.has_value() &&
      *statement.valid_time->as_of != valid_time.value) {
    return WriteError("write valid time disagrees with statement scope");
  }
  std::unordered_map<std::string, WriteEntity> entities;
  for (const PathPattern& pattern : statement.patterns) {
    auto source = entities.find(pattern.source);
    if (source == entities.end()) {
      auto id = database.AllocateVertexId();
      if (!id.ok()) return id.status();
      source = entities.emplace(pattern.source,
                                WriteEntity{VertexRef{PartId{statement.part_id.value}, id.ValueOrDie()}, std::nullopt})
                   .first;
      if (const Status status = tx.Assert(EntityFact::Vertex(*source->second.vertex), valid_time);
          !status.ok()) return status;
    }
    if (pattern.edge.empty() && pattern.relationship.empty()) continue;
    auto destination = entities.find(pattern.destination);
    if (destination == entities.end()) {
      auto id = database.AllocateVertexId();
      if (!id.ok()) return id.status();
      destination = entities.emplace(pattern.destination,
                                     WriteEntity{VertexRef{PartId{statement.part_id.value}, id.ValueOrDie()}, std::nullopt})
                        .first;
      if (const Status status = tx.Assert(EntityFact::Vertex(*destination->second.vertex), valid_time);
          !status.ok()) return status;
    }
    const std::string edge_name = pattern.edge.empty() ? "edge" : pattern.edge;
    auto edge = entities.find(edge_name);
    if (edge == entities.end()) {
      auto id = database.AllocateEdgeId();
      if (!id.ok()) return id.status();
      const EdgeRef edge_ref{PartId{statement.part_id.value}, id.ValueOrDie()};
      const EdgeIdentity identity{edge_ref, *source->second.vertex,
                                  *destination->second.vertex,
                                  RelationshipType(pattern.relationship)};
      edge = entities.emplace(edge_name, WriteEntity{std::nullopt, edge_ref}).first;
      if (const Status status = tx.Assert(identity, valid_time); !status.ok()) return status;
    }
  }
  for (const BoundAssignment& assignment : statement.assignments) {
    const auto entity = entities.find(assignment.target);
    if (entity == entities.end()) return WriteError("SET target is not created in this statement");
    const auto value = LookupValue(bindings, assignment.parameter);
    if (!value.ok()) return value.status();
    Status status;
    if (entity->second.vertex.has_value()) {
      status = tx.Set(PropertyFact::Vertex(*entity->second.vertex, assignment.property_id),
                       valid_time, value.ValueOrDie());
    } else if (entity->second.edge.has_value()) {
      status = tx.Set(PropertyFact::Edge(*entity->second.edge, assignment.property_id),
                       valid_time, value.ValueOrDie());
    } else {
      return WriteError("SET target has no identity");
    }
    if (!status.ok()) return status;
  }
  for (const std::string& name : statement.deletions) {
    const auto entity = entities.find(name);
    if (entity == entities.end()) return WriteError("DELETE target is not created in this statement");
    Status status;
    if (entity->second.vertex.has_value()) {
      status = tx.Retract(EntityFact::Vertex(*entity->second.vertex), valid_time);
    } else if (entity->second.edge.has_value()) {
      status = tx.Retract(EntityFact::Edge(*entity->second.edge), valid_time);
    } else {
      return WriteError("DELETE target has no identity");
    }
    if (!status.ok()) return status;
  }
  return Status::OK();
}

StatusOr<CommitResult> ExecuteWrite(Database& database,
                                    const BoundStatement& statement,
                                    const Bindings& bindings,
                                    ValidTime valid_time) {
  auto transaction = database.BeginTransaction();
  if (!transaction.ok()) return transaction.status();
  std::unique_ptr<Transaction> tx = std::move(transaction).ConsumeValueOrDie();
  const Status staged = StageWrite(database, *tx, statement, bindings, valid_time);
  if (!staged.ok()) {
    tx->Rollback().IgnoreError();
    return staged;
  }
  return tx->Commit();
}

}  // namespace cedar::cypher
