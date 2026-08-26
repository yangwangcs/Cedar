// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/database.h"

#include <memory>
#include <utility>
#include <vector>

#include "cedar/query/query.h"
#include "cedar/query/result.h"
#include "cedar/transaction.h"
#include "kernel/database_impl.h"
#include "query/execution_context.h"
#include "query/plan_outcome.h"
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
      const bool is_journey_duration =
          plan.ValueOrDie().graph_duration_property.has_value() &&
          *plan.ValueOrDie().graph_duration_property == property;
      if (is_journey_duration) {
        if (resolved.entity_kind != PropertyEntityKind::kEdge) {
          return Status::SchemaMismatch(
              "query", "journey duration property must belong to edges");
        }
        if (resolved.physical_type != PhysicalType::kInt32 &&
            resolved.physical_type != PhysicalType::kInt64 &&
            resolved.physical_type != PhysicalType::kTimestamp64) {
          return Status::SchemaMismatch(
              "query", "journey duration property must be a non-negative integer type");
        }
      }
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
  const bool bounded_system_range =
      state_->plan.execution_scope.system_time_range.has_value();
  if (snapshot.commit_seq().value != 0 && !bounded_system_range) {
    auto binding = internal::QueryExecutionContextFactory::Bind(
        database, state_->logical_root, snapshot.commit_seq(), options,
        state_->plan.execution_scope.part_scope);
    // The canonical runtime retains the established handling for unbounded
    // History and synthetic zero-sequence test snapshots. A planner refusal
    // for those shapes must not change their existing execution contract.
    if (!binding.ok() && internal::ClassifyPlanStatus(binding.status()) !=
                           internal::PlanOutcomeKind::kUnsupported &&
        internal::ClassifyPlanStatus(binding.status()) !=
            internal::PlanOutcomeKind::kInvalidRequest) {
      return binding.status();
    }
    if (binding.ok()) {
      internal::PreparedQueryPlan execution_plan = state_->plan;
      const auto& bound = binding.ValueOrDie();
      if (bound.read_binding) {
        const auto& capsule = *bound.read_binding;
        execution_plan.physical_plan = capsule.plan_template().physical;
        if (capsule.plan_template().physical) {
          execution_plan.safe_read_limit =
              capsule.plan_template().physical->safe_read_limit;
        }
        execution_plan.bound_delta_view = capsule.delta_view();
        execution_plan.bound_delta_lease = capsule.delta_lease();
        execution_plan.projection_generation = capsule.projection_generation();
        execution_plan.projection_stats = capsule.projection_stats();
        execution_plan.projection_reader = capsule.projection_reader();
        execution_plan.property_index_reader = capsule.property_index_reader();
        execution_plan.delta_reader = capsule.delta_reader();
      } else {
        execution_plan.physical_plan = std::make_shared<const internal::PhysicalPlan>(
            bound.physical);
        execution_plan.safe_read_limit = bound.physical.safe_read_limit;
        execution_plan.bound_delta_view = bound.delta_view;
        execution_plan.bound_delta_lease = bound.delta_lease;
        execution_plan.projection_generation = bound.projection_generation;
        execution_plan.projection_stats = bound.projection_stats;
        execution_plan.projection_reader = bound.projection_reader;
        execution_plan.property_index_reader = bound.property_index_reader;
        execution_plan.delta_reader = bound.delta_reader;
      }
      return internal::QueryRuntime::Execute(
          execution_plan, std::move(snapshot), bindings, options,
          database->query_resource_pool.get(),
          [database](const std::shared_ptr<QueryExecutionState>& state) -> Status {
            return database->RegisterQueryState(state);
          },
          [database](const std::shared_ptr<QueryExecutionState>& state) {
            database->UnregisterQueryState(state);
          },
          database->query_crash_fault_injector_for_testing,
          &database->query_metrics);
    }
  }
  if (!state_->plan.canonical_temporal) {
    return Status::NotSupported(
        "query", "query is outside canonical temporal execution");
  }
  return internal::QueryRuntime::Execute(
      state_->plan, std::move(snapshot), bindings, options,
      database->query_resource_pool.get(),
      [database](const std::shared_ptr<QueryExecutionState>& state) -> Status {
        return database->RegisterQueryState(state);
      },
      [database](const std::shared_ptr<QueryExecutionState>& state) {
        database->UnregisterQueryState(state);
      },
      database->query_crash_fault_injector_for_testing,
      &database->query_metrics);
}

StatusOr<QueryCursor> PreparedQuery::Execute(
    Transaction& transaction, const Bindings& bindings,
    const QueryOptions& options) const {
  if (!state_) return Status::InvalidArgument("prepared query", "moved-from query");
  const bool historical = state_->plan.execution_scope.system_time_as_of.has_value() ||
                          state_->plan.execution_scope.system_time_range.has_value();
  auto snapshot = transaction.BeginReadSnapshot(!historical);
  if (!snapshot.ok()) return snapshot.status();
  return Execute(std::move(snapshot).ConsumeValueOrDie(), bindings, options);
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
  const auto database = state_->database.lock();
  if (!database) return Status::ShutdownInProgress("query", "database no longer exists");
  auto binding = internal::QueryExecutionContextFactory::Bind(
      database, state_->logical_root, snapshot.commit_seq(), options,
      state_->plan.execution_scope.part_scope);
  if (!binding.ok()) return binding.status();
  return internal::QueryPlanner::ExplainPhysical(binding.ValueOrDie().physical);
}

StatusOr<QueryPhysicalSummary> PreparedQuery::ExplainPhysicalSummary(
    const Snapshot& snapshot, const QueryOptions& options) const {
  if (!state_ || !state_->logical_root) {
    return Status::InvalidArgument("prepared query", "moved-from query");
  }
  if (snapshot.commit_seq().value == 0) return QueryPhysicalSummary{};
  const auto database = state_->database.lock();
  if (!database) {
    return Status::ShutdownInProgress("query", "database no longer exists");
  }
  auto binding = internal::QueryExecutionContextFactory::Bind(
      database, state_->logical_root, snapshot.commit_seq(), options,
      state_->plan.execution_scope.part_scope);
  if (!binding.ok()) return binding.status();
  bool projection = false;
  bool delta = false;
  bool canonical = false;
  QueryPhysicalSummary summary;
  for (const auto& slice : binding.ValueOrDie().physical.coverage_slices) {
    projection |= slice.source == internal::CoverageSource::kProjection;
    delta |= slice.source == internal::CoverageSource::kDeltaMerge;
    canonical |= slice.source == internal::CoverageSource::kCanonical;
    if (!summary.projection_generation && slice.projection_generation) {
      summary.projection_generation = slice.projection_generation;
      summary.projection_base = slice.projection_base;
    }
  }
  const int sources = static_cast<int>(projection) + static_cast<int>(delta) +
                      static_cast<int>(canonical);
  if (sources > 1) {
    summary.source = QueryAccessSource::kMixed;
  } else if (projection) {
    summary.source = QueryAccessSource::kProjection;
  } else if (delta) {
    summary.source = QueryAccessSource::kDeltaMerge;
  }
  return summary;
}

}  // namespace cedar
