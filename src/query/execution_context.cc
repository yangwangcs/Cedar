#include "query/execution_context.h"

#include <memory>
#include <list>
#include <mutex>
#include <sstream>
#include <unordered_map>
#include <utility>

#include "kernel/database_impl.h"

namespace cedar::internal {

namespace {

class BoundedPlanTemplateCache {
 public:
  std::shared_ptr<const PhysicalPlan> Lookup(const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = positions_.find(key);
    if (found == positions_.end()) return nullptr;
    entries_.splice(entries_.begin(), entries_, found->second);
    return found->second->plan;
  }

  void Insert(std::string key, std::shared_ptr<const PhysicalPlan> plan) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = positions_.find(key);
    if (found != positions_.end()) {
      found->second->plan = std::move(plan);
      entries_.splice(entries_.begin(), entries_, found->second);
      return;
    }
    entries_.push_front(Entry{std::move(key), std::move(plan)});
    positions_[entries_.front().key] = entries_.begin();
    while (entries_.size() > kCapacity) {
      positions_.erase(entries_.back().key);
      entries_.pop_back();
    }
  }

 private:
  struct Entry {
    std::string key;
    std::shared_ptr<const PhysicalPlan> plan;
  };
  static constexpr size_t kCapacity = 64;
  std::mutex mutex_;
  std::list<Entry> entries_;
  std::unordered_map<std::string, std::list<Entry>::iterator> positions_;
};

BoundedPlanTemplateCache& PlanTemplateCache() {
  static BoundedPlanTemplateCache cache;
  return cache;
}

std::string PlanCacheKey(const LogicalPlanNode& root, const PlanningContext& context,
                         const std::string& schema_fingerprint) {
  std::ostringstream out;
  out << QueryPlanner::ExplainLogical(root) << '|' << schema_fingerprint << '|'
      << context.database_identity << '|' << context.snapshot_seq.value << '|'
      << static_cast<unsigned>(context.part_scope.kind);
  for (PartId part : context.part_scope.parts) out << ':' << part.value;
  out << '|' << context.projections.generation_id << '|'
      << context.projections.base_seq.value << '|' << context.statistics.candidate_rows
      << ':' << context.statistics.pages << ':' << context.statistics.physical_bytes << ':'
      << context.statistics.decoded_bytes << ':' << context.statistics.known;
  return out.str();
}

}  // namespace

StatusOr<QueryExecutionBinding> QueryExecutionContextFactory::Bind(
    const std::shared_ptr<Database::Impl>& database,
    const std::shared_ptr<const LogicalPlanNode>& root, CommitSeq snapshot_seq,
    const QueryOptions& options, const PartScope& part_scope) {
  ProjectionCatalogView catalog;
  if (database->projection_store) {
    const auto manifest = database->projection_store->current_manifest();
    if (manifest) catalog = ProjectionCatalogView(*manifest);
  }
  QueryDeltaView delta{catalog.base_seq, snapshot_seq, {}, {}, {}, {}};
  bool delta_usable = false;
  if (database->query_delta) {
    auto acquired = database->query_delta->AcquireThrough(snapshot_seq);
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
  if (database->query_statistics && catalog.generation_id != 0) {
    const auto schema = database->store.SchemaFingerprint();
    const auto loaded = schema.ok()
        ? database->query_statistics->Load(catalog.generation_id, catalog.base_seq,
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
  const std::string plan_cache_key = PlanCacheKey(*root, context, schema.ValueOrDie());
  std::shared_ptr<const PhysicalPlan> physical_plan =
      PlanTemplateCache().Lookup(plan_cache_key);
  if (!physical_plan) {
    auto physical = QueryPlanner::Bind(*root, context);
    if (!physical.ok()) return physical.status();
    physical_plan = std::make_shared<const PhysicalPlan>(
        std::move(physical).ConsumeValueOrDie());
    PlanTemplateCache().Insert(plan_cache_key, physical_plan);
  }

  QueryExecutionBinding binding;
  binding.physical = *physical_plan;
  binding.projection_stats = std::make_shared<ProjectionReadStats>();
  if (delta_usable) {
    binding.delta_view = std::make_shared<const QueryDeltaView>(delta);
    if (database->query_delta) {
      auto lease = database->query_delta->AcquireLeaseThrough(snapshot_seq);
      if (!lease.ok()) return lease.status();
      binding.delta_lease = std::move(lease).ConsumeValueOrDie();
    }
  }
  if (database->projection_store) {
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
      auto acquired = database->projection_store->Acquire(request);
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
  }
  const std::weak_ptr<Database::Impl> weak_database = database;
  const auto pinned_generation = binding.projection_generation;
  const auto projection_stats = binding.projection_stats;
  binding.projection_reader =
      [weak_database, snapshot_seq, pinned_generation, projection_stats](const CoverageSlice& slice)
      -> StatusOr<std::vector<ProjectionChain>> {
    const auto db = weak_database.lock();
    if (!db || !db->projection_store || !pinned_generation ||
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
    return db->projection_store->ReadChains(request, *pinned_generation);
  };
  binding.delta_reader = [weak_database, snapshot_seq]()
      -> StatusOr<QueryDeltaView> {
    const auto db = weak_database.lock();
    if (!db || !db->query_delta) {
      return Status::NotFound("query", "query delta is unavailable");
    }
    return db->query_delta->AcquireThrough(snapshot_seq);
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
      std::move(binding.projection_stats), std::move(binding.projection_reader),
      std::move(binding.delta_reader));
  return binding;
}

}  // namespace cedar::internal
