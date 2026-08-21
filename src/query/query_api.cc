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

template <typename ImplT>
StatusOr<internal::PhysicalPlan> BindPhysicalForSnapshot(
    const std::shared_ptr<ImplT>& database,
    const std::shared_ptr<const internal::LogicalPlanNode>& root,
    CommitSeq snapshot_seq, const QueryOptions& options,
    std::shared_ptr<const internal::QueryDeltaView>* bound_delta,
    std::optional<internal::ProjectionGeneration>* pinned_generation) {
  internal::ProjectionCatalogView catalog;
  if (database->projection_store) {
    const auto manifest = database->projection_store->current_manifest();
    if (manifest) catalog = internal::ProjectionCatalogView(*manifest);
  }
  internal::QueryDeltaView empty_delta{catalog.base_seq, snapshot_seq, {}, {}, {}, {}};
  internal::QueryDeltaView delta = empty_delta;
  bool delta_usable = false;
  if (database->query_delta) {
    auto acquired = database->query_delta->AcquireThrough(snapshot_seq);
    if (acquired.ok()) {
      delta = std::move(acquired).ConsumeValueOrDie();
      delta_usable = delta.base_seq == catalog.base_seq &&
                     delta.through.value >= snapshot_seq.value &&
                     delta.first_missing.value == 0;
      if (delta_usable && bound_delta) {
        *bound_delta = std::make_shared<const internal::QueryDeltaView>(delta);
      }
    } else {
      delta.first_missing = snapshot_seq;
    }
  }
  internal::QueryStatisticsView statistics;
  internal::PlanningContext context{snapshot_seq, catalog, delta, statistics,
                                    options};
  context.database_identity = catalog.database_identity;
  context.schema_epoch = 0;
  context.allow_delta_merge = delta_usable;
  auto physical = internal::QueryPlanner::Bind(*root, context);
  if (!physical.ok() || !pinned_generation || !database->projection_store) {
    return physical;
  }
  for (const auto& slice : physical.ValueOrDie().coverage_slices) {
    if (slice.source == internal::CoverageSource::kCanonical ||
        !slice.projection_generation) {
      continue;
    }
    internal::CoverageRequest request;
    request.kind = slice.kind;
    request.part_id = slice.part_id;
    request.property_id = slice.property_id;
    request.schema_epoch = slice.schema_epoch;
    request.entity_min = slice.entity_min;
    request.entity_max_exclusive = slice.entity_max_exclusive;
    request.valid_time = slice.interval;
    request.snapshot_seq = snapshot_seq;
    request.generation_id = slice.projection_generation;
    request.expected_base_seq = slice.projection_base;
    request.database_identity = slice.database_identity;
    auto acquired = database->projection_store->Acquire(request);
    if (!acquired.has_value()) {
      return Status::Conflict("query", "projection generation rolled over during planning");
    }
    if (pinned_generation->has_value() &&
        ((*pinned_generation)->generation_id() != acquired->generation_id() ||
         !(*pinned_generation)->manifest() || !acquired->manifest() ||
         (*pinned_generation)->manifest()->base_seq != acquired->manifest()->base_seq)) {
      return Status::Conflict("query", "physical plan spans multiple projection generations");
    }
    if (!pinned_generation->has_value()) {
      *pinned_generation = std::move(acquired);
    }
  }
  return physical;
}

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
  if (snapshot.commit_seq().value != 0) {
    std::shared_ptr<const internal::QueryDeltaView> bound_delta;
    std::optional<internal::ProjectionGeneration> pinned_generation;
    auto physical = BindPhysicalForSnapshot(database, state_->logical_root,
                                            snapshot.commit_seq(), options,
                                            &bound_delta, &pinned_generation);
    // The canonical runtime retains the established handling for unbounded
    // History and synthetic zero-sequence test snapshots. A planner refusal
    // for those shapes must not change their existing execution contract.
    if (!physical.ok() && !physical.status().IsNotSupportedError() &&
        !physical.status().IsInvalidArgument()) {
      return physical.status();
    }
    if (physical.ok()) {
      internal::PreparedQueryPlan execution_plan = state_->plan;
      execution_plan.physical_plan = std::make_shared<const internal::PhysicalPlan>(
          physical.ValueOrDie());
      execution_plan.bound_delta_view = bound_delta;
      execution_plan.projection_generation = std::move(pinned_generation);
      const FactFamily family = execution_plan.entity_family;
      const std::weak_ptr<Database::Impl> weak_database = database;
      execution_plan.projection_reader =
          [weak_database, family, snapshot_seq = snapshot.commit_seq(),
           pinned_generation = execution_plan.projection_generation](
              const internal::CoverageSlice& slice)
          -> StatusOr<std::vector<internal::ProjectionChain>> {
        const auto db = weak_database.lock();
        if (!db || !db->projection_store) {
          return Status::NotFound("query", "projection store is unavailable");
        }
        if (!pinned_generation || !pinned_generation->exists()) {
          return Status::NotFound("query", "pinned projection generation is unavailable");
        }
        internal::CoverageRequest request;
        request.kind = slice.kind;
        request.part_id = slice.part_id;
        request.property_id = slice.property_id;
        request.schema_epoch = slice.schema_epoch;
        request.entity_min = slice.entity_min;
        request.entity_max_exclusive = slice.entity_max_exclusive;
        request.valid_time = slice.interval;
        request.snapshot_seq = snapshot_seq;
        request.generation_id = slice.projection_generation;
        request.expected_base_seq = slice.projection_base;
        request.database_identity = slice.database_identity;
        return db->projection_store->ReadChains(request, *pinned_generation);
      };
      execution_plan.delta_reader =
          [weak_database, snapshot_seq = snapshot.commit_seq()]()
          -> StatusOr<internal::QueryDeltaView> {
        const auto db = weak_database.lock();
        if (!db || !db->query_delta) {
          return Status::NotFound("query", "query delta is unavailable");
        }
        return db->query_delta->AcquireThrough(snapshot_seq);
      };
      return internal::QueryRuntime::Execute(
          execution_plan, std::move(snapshot), bindings, options,
          database->query_resource_pool.get());
    }
  }
  if (!state_->plan.canonical_temporal) {
    return Status::NotSupported(
        "query", "query is outside canonical temporal execution");
  }
  return internal::QueryRuntime::Execute(
      state_->plan, std::move(snapshot), bindings, options,
      database->query_resource_pool.get());
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
  auto plan = BindPhysicalForSnapshot(database, state_->logical_root,
                                      snapshot.commit_seq(), options, nullptr,
                                      nullptr);
  if (!plan.ok()) return plan.status();
  return internal::QueryPlanner::ExplainPhysical(plan.ValueOrDie());
}

}  // namespace cedar
