#include "query/execution_context.h"

#include <memory>
#include <mutex>
#include <utility>

#include "kernel/database_impl.h"

namespace cedar::internal {

StatusOr<QueryExecutionBinding> QueryExecutionContextFactory::Bind(
    const std::shared_ptr<Database::Impl>& database,
    const std::shared_ptr<const LogicalPlanNode>& root, CommitSeq snapshot_seq,
    const QueryOptions& options, const PartScope& part_scope) {
  // Database::Close detaches the derived query state under the same lifecycle
  // mutex, so take references to everything this binding needs in one critical
  // section. Bind never holds the kernel mutex while planning: the references
  // keep the objects alive independently of the members that Close detaches,
  // and the closed/closing check cannot race the teardown.
  std::shared_ptr<internal::QueryProjectionStore> projection_store;
  std::shared_ptr<internal::QueryDelta> query_delta;
  std::shared_ptr<internal::QueryStatisticsStore> query_statistics;
  {
    std::lock_guard<std::mutex> lock(database->mutex);
    if (database->closed || database->closing) {
      return Status::ShutdownInProgress("query", "database is closing or closed");
    }
    projection_store = database->projection_store;
    query_delta = database->query_delta;
    query_statistics = database->query_statistics;
    // Test-only window fired *while the lifecycle mutex is held*, immediately
    // after the synchronized reference copy above. It pins the B1 invariant that
    // the closed/closing check and the capture form one critical section: a
    // concurrent Close must block on this mutex here, so it cannot detach the
    // owners between the check and the capture. The observer must not call back
    // into the database.
    if (database->query_open_stage_observer_for_testing) {
      database->query_open_stage_observer_for_testing(
          "query_bind_state_captured");
    }
  }
  if (database->query_open_stage_observer_for_testing) {
    database->query_open_stage_observer_for_testing("query_bind_state_acquired");
  }
  ProjectionCatalogView catalog;
  if (projection_store) {
    const auto manifest = projection_store->current_manifest();
    if (manifest) catalog = ProjectionCatalogView(*manifest);
  }
  QueryDeltaView delta{catalog.base_seq, snapshot_seq, {}, {}, {}, {}};
  bool delta_usable = false;
  if (query_delta) {
    auto acquired = query_delta->AcquireThrough(snapshot_seq);
    if (acquired.ok()) {
      delta = std::move(acquired).ConsumeValueOrDie();
      delta_usable = delta.base_seq == catalog.base_seq &&
                     delta.through.value >= snapshot_seq.value &&
                     delta.first_missing.value == 0;
    } else {
      delta.first_missing = snapshot_seq;
    }
  }
  QueryStatisticsView statistics;
  if (query_statistics && catalog.generation_id != 0) {
    const auto schema = database->store.SchemaFingerprint();
    const auto loaded = schema.ok()
        ? query_statistics->Load(catalog.generation_id, catalog.base_seq,
                                 schema.ValueOrDie())
        : StatusOr<QueryStatisticsSnapshot>(schema.status());
    if (loaded.ok() && loaded.ValueOrDie().complete &&
        !loaded.ValueOrDie().schema_fingerprint.empty()) {
      statistics.known = true;
      for (const auto& column : loaded.ValueOrDie().columns) {
        statistics.candidate_rows += column.rows;
        statistics.pages += column.pages;
        statistics.physical_bytes += column.bytes;
        statistics.interval_fragments += column.interval_count;
        statistics.fanout += column.edge_count;
      }
    }
  }
  PlanningContext context{snapshot_seq, catalog, delta, statistics, options};
  context.database_identity = catalog.database_identity;
  context.allow_delta_merge = delta_usable;
  context.part_scope = part_scope;
  const auto schema = database->store.SchemaFingerprint();
  if (!schema.ok()) return schema.status();
  auto static_preparation = QueryPlanner::PrepareStatic(*root, schema.ValueOrDie());
  if (!static_preparation.ok()) return static_preparation.status();
  // Bound coverage and routing depend on this query's temporal/options state.
  auto physical = QueryPlanner::Bind(*root, context);
  if (!physical.ok()) return physical.status();
  auto physical_plan = std::make_shared<const PhysicalPlan>(
      std::move(physical).ConsumeValueOrDie());

  QueryExecutionBinding binding;
  binding.physical = *physical_plan;
  binding.projection_stats = std::make_shared<ProjectionReadStats>();
  if (delta_usable) {
    binding.delta_view = std::make_shared<const QueryDeltaView>(delta);
    if (query_delta) {
      auto lease = query_delta->AcquireLeaseThrough(snapshot_seq);
      if (!lease.ok()) return lease.status();
      binding.delta_lease = std::move(lease).ConsumeValueOrDie();
    }
  }
  if (projection_store) {
    for (const auto& slice : physical_plan->coverage_slices) {
      if (slice.source == CoverageSource::kCanonical ||
          !slice.projection_generation) continue;
      CoverageRequest request;
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
      request.stats = binding.projection_stats.get();
      auto acquired = projection_store->Acquire(request);
      if (!acquired.has_value()) {
        return Status::Conflict("query", "projection generation rolled over during planning");
      }
      if (binding.projection_generation.has_value() &&
          (binding.projection_generation->generation_id() != acquired->generation_id() ||
           !binding.projection_generation->manifest() || !acquired->manifest() ||
           binding.projection_generation->manifest()->base_seq !=
               acquired->manifest()->base_seq)) {
        return Status::Conflict("query", "physical plan spans multiple projection generations");
      }
      if (!binding.projection_generation.has_value()) {
        binding.projection_generation = std::move(acquired);
      }
    }
    // Property-index regions are derived read capabilities rather than state
    // slices. Pin the same immutable generation so candidate reads cannot
    // observe a rollover midway through execution.
    for (const auto& region : catalog.regions) {
      if (region.kind != ProjectionKind::kPropertyIndex ||
          !region.property_id.has_value() || region.segments.empty()) {
        continue;
      }
      CoverageRequest request;
      request.kind = ProjectionKind::kPropertyIndex;
      request.part_id = region.part_id;
      request.property_id = region.property_id;
      request.schema_epoch = region.schema_epoch;
      request.entity_min = region.entity_min;
      request.entity_max_exclusive = region.entity_max_exclusive;
      request.valid_time = region.valid_time;
      request.snapshot_seq = snapshot_seq;
      request.generation_id = catalog.generation_id;
      request.expected_base_seq = catalog.base_seq;
      request.database_identity = catalog.database_identity;
      auto acquired = projection_store->Acquire(request);
      if (!acquired.has_value()) continue;
      if (binding.projection_generation.has_value() &&
          binding.projection_generation->generation_id() != acquired->generation_id()) {
        return Status::Conflict("query", "physical plan spans multiple projection generations");
      }
      if (!binding.projection_generation.has_value()) {
        binding.projection_generation = std::move(acquired);
      }
      break;
    }
  }
  // Execution outlives planning. The readers capture their own references to
  // the derived objects instead of re-reading Database::Impl members that Close
  // detaches, and they deliberately do not retain the database itself.
  const auto pinned_generation = binding.projection_generation;
  const auto projection_stats = binding.projection_stats;
  binding.projection_visitor =
      [projection_store, snapshot_seq, pinned_generation, projection_stats](
          const CoverageSlice& slice, QueryReservation& reservation,
          const ProjectionChainVisitor& visitor) -> Status {
    if (!projection_store || !pinned_generation ||
        !pinned_generation->exists()) {
      return Status::NotFound("query", "pinned projection generation is unavailable");
    }
    CoverageRequest request;
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
    request.stats = projection_stats.get();
    request.admit_pages = slice.entity_max_exclusive > slice.entity_min &&
        slice.entity_max_exclusive - slice.entity_min == 1;
    return projection_store->VisitChains(request, *pinned_generation,
                                         reservation, visitor);
  };
  binding.delta_reader = [query_delta, snapshot_seq]() -> StatusOr<QueryDeltaView> {
    if (!query_delta) {
      return Status::NotFound("query", "query delta is unavailable");
    }
    return query_delta->AcquireThrough(snapshot_seq);
  };
  const auto pinned_property_generation = binding.projection_generation;
  binding.property_index_reader =
      [projection_store, snapshot_seq, pinned_property_generation](
          const CoverageRequest& coverage, QueryReservation& reservation)
          -> StatusOr<PropertyIndexReadResult> {
    if (!projection_store || !pinned_property_generation ||
        !pinned_property_generation->exists()) {
      return Status::NotFound("query", "property index generation is unavailable");
    }
    CoverageRequest request = coverage;
    request.kind = ProjectionKind::kPropertyIndex;
    request.snapshot_seq = snapshot_seq;
    request.generation_id = pinned_property_generation->generation_id();
    request.expected_base_seq = pinned_property_generation->manifest()
                                    ? std::optional<CommitSeq>(
                                          pinned_property_generation->manifest()->base_seq)
                                    : std::nullopt;
    request.database_identity = pinned_property_generation->manifest()
                                    ? pinned_property_generation->manifest()->database_identity
                                    : std::string{};
    return projection_store->ReadBudgetedPropertyIndex(
        request, *pinned_property_generation, reservation);
  };
  // The capsule is the ownership boundary for all snapshot-specific state.
  // Keep the value-shaped fields above for explain/test compatibility, but
  // hand execution one immutable holder so leases and readers are transferred
  // together and the prepared physical plan is shared.
  PreparedPlanTemplate prepared{physical_plan,
                                static_preparation.ValueOrDie().fingerprint};
  binding.read_binding = std::make_shared<const ReadBindingCapsule>(
      std::move(prepared), std::move(binding.delta_view),
      std::move(binding.delta_lease), std::move(binding.projection_generation),
      std::move(binding.projection_stats),
      std::move(binding.property_index_reader),
      std::move(binding.delta_reader), std::move(binding.projection_visitor));
  return binding;
}

}  // namespace cedar::internal
