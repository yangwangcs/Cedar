// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/database.h"

#include <memory>
#include <utility>
#include <vector>

#include "cedar/query/query.h"
#include "cedar/query/result.h"
#include "kernel/database_impl.h"
#include "query/runtime/query_runtime.h"
#include "query/planner/query_planner.h"
#include "query/logical/logical_plan.h"

namespace cedar {
namespace {

std::optional<PhysicalType> PhysicalTypeOf(QueryType type) {
  switch (type) {
    case QueryType::kBool:
      return PhysicalType::kBool;
    case QueryType::kInt32:
      return PhysicalType::kInt32;
    case QueryType::kInt64:
      return PhysicalType::kInt64;
    case QueryType::kFloat32:
      return PhysicalType::kFloat32;
    case QueryType::kFloat64:
      return PhysicalType::kFloat64;
    case QueryType::kTimestamp64:
      return PhysicalType::kTimestamp64;
    case QueryType::kString:
      return PhysicalType::kString;
    case QueryType::kBinary:
      return PhysicalType::kBinary;
    default:
      return std::nullopt;
  }
}

}  // namespace

class PreparedQuery::State {
 public:
  State(std::weak_ptr<Database::Impl> database,
        internal::PreparedQueryPlan plan,
        std::vector<PropertyDefinition> schema_fingerprint,
        std::shared_ptr<const internal::LogicalPlanNode> logical_root)
      : database(std::move(database)),
        plan(std::move(plan)),
        schema_fingerprint(std::move(schema_fingerprint)),
        logical_root(std::move(logical_root)) {}

  std::weak_ptr<Database::Impl> database;
  internal::PreparedQueryPlan plan;
  std::vector<PropertyDefinition> schema_fingerprint;
  std::shared_ptr<const internal::LogicalPlanNode> logical_root;
};

StatusOr<PreparedQuery> Database::PrepareQuery(const Query& query) const {
  if (!impl_) return Status::InvalidArgument("database", "moved-from database");
  auto plan = internal::AnalyzeQuery(query);
  if (!plan.ok()) return plan.status();

  std::vector<PropertyDefinition> fingerprint;
  {
    std::lock_guard<std::mutex> lock(impl_->mutex);
    if (impl_->closed) {
      return Status::InvalidArgument("database", "database is closed");
    }
    if (impl_->closing) {
      return Status::ShutdownInProgress("database",
                                        "database close is in progress");
    }
    fingerprint.reserve(plan.ValueOrDie().referenced_properties.size());
    for (PropertyId property : plan.ValueOrDie().referenced_properties) {
      const auto definition = impl_->store.LookupProperty(property);
      if (!definition.ok()) return definition.status();
      if (!definition.ValueOrDie().has_value()) {
        return Status::SchemaMismatch(
            "query", "referenced property is not registered");
      }
      const PropertyDefinition resolved = *definition.ValueOrDie();
      for (internal::PreparedPropertyBinding& binding :
           plan.ValueOrDie().property_bindings) {
        if (binding.property != property) continue;
        const std::optional<PhysicalType> output_type =
            PhysicalTypeOf(binding.output.type);
        if (resolved.entity_kind != binding.entity_kind) {
          return Status::SchemaMismatch(
              "query", "property entity kind differs from binding");
        }
        if (!output_type.has_value() ||
            resolved.physical_type != *output_type) {
          return Status::SchemaMismatch(
              "query", "property physical type differs from output slot");
        }
        binding.definition = resolved;
      }
      fingerprint.push_back(resolved);
    }
  }
  return PreparedQuery(std::make_shared<const PreparedQuery::State>(
      impl_, std::move(plan).ConsumeValueOrDie(), std::move(fingerprint),
      internal::LogicalPlanInspector::InspectShared(query)));
}

StatusOr<QueryCursor> PreparedQuery::Execute(
    Snapshot snapshot, const Bindings& bindings,
    const QueryOptions& options) const {
  if (!state_) {
    return Status::InvalidArgument("prepared query", "moved-from query");
  }
  const std::shared_ptr<Database::Impl> database = state_->database.lock();
  if (!database) {
    return Status::ShutdownInProgress("query", "database no longer exists");
  }
  const Status valid = database->ValidatePreparedQuery(
      snapshot.commit_seq(), state_->schema_fingerprint);
  if (!valid.ok()) return valid;
  if (!snapshot.BelongsToDatabase(database.get())) {
    return Status::InvalidArgument(
        "query", "snapshot belongs to a different database");
  }
  if (!state_->plan.canonical_temporal) {
    return Status::NotSupported(
        "query", "query is outside canonical temporal execution");
  }
  return internal::QueryRuntime::Execute(
      state_->plan, std::move(snapshot), bindings, options);
}

StatusOr<std::string> PreparedQuery::ExplainLogical() const {
  if (!state_ || !state_->logical_root) {
    return Status::InvalidArgument("prepared query", "moved-from query");
  }
  return internal::QueryPlanner::ExplainLogical(*state_->logical_root);
}

StatusOr<std::string> PreparedQuery::ExplainPhysical(
    const Snapshot& snapshot, const QueryOptions& options) const {
  if (!state_ || !state_->logical_root) {
    return Status::InvalidArgument("prepared query", "moved-from query");
  }
  // Database-owned projection and delta views are intentionally not exposed
  // through the public API yet. The explain path therefore binds an explicit
  // empty derived catalog, making canonical fallback visible and truthful.
  internal::ProjectionCatalogView catalog;
  internal::QueryDeltaView delta{CommitSeq{0}, snapshot.commit_seq(), {}, {}, {}};
  internal::QueryStatisticsView statistics;
  const internal::PlanningContext context{snapshot.commit_seq(), catalog, delta,
                                          statistics, options};
  auto plan = internal::QueryPlanner::Bind(*state_->logical_root, context);
  if (!plan.ok()) return plan.status();
  return internal::QueryPlanner::ExplainPhysical(plan.ValueOrDie());
}

}  // namespace cedar
