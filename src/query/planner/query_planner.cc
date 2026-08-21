#include "query/planner/query_planner.h"

#include <algorithm>
#include <limits>
#include <sstream>
#include <type_traits>

namespace cedar::internal {
namespace {

bool IsUnbounded(const ValidTimeInterval& interval) {
  return !interval.to.has_value();
}

bool ValidRange(const ValidTimeInterval& interval) {
  return !interval.to || interval.from.value < interval.to->value;
}

bool Overlap(const ValidTimeInterval& a, const ValidTimeInterval& b) {
  const uint64_t a_to = a.to.value_or(ValidTime{std::numeric_limits<uint64_t>::max()}).value;
  const uint64_t b_to = b.to.value_or(ValidTime{std::numeric_limits<uint64_t>::max()}).value;
  return a.from.value < b_to && b.from.value < a_to;
}

bool EntityOverlap(const CoverageRegion& a, const CoverageRegion& b) {
  return a.entity_min < b.entity_max_exclusive &&
         b.entity_min < a.entity_max_exclusive;
}

std::optional<ValidTimeInterval> ScopeInterval(const TemporalScope& scope) {
  return std::visit([](const auto& value) -> std::optional<ValidTimeInterval> {
    using T = std::decay_t<decltype(value)>;
    if constexpr (std::is_same_v<T, At>) {
      if (value.time.value == std::numeric_limits<uint64_t>::max()) return std::nullopt;
      return ValidTimeInterval{value.time, ValidTime{value.time.value + 1}};
    } else if constexpr (requires { value.interval; }) {
      return value.interval;
    } else {
      return value.interval;
    }
  }, scope);
}

const char* SourceName(CoverageSource source) {
  switch (source) {
    case CoverageSource::kProjection: return "projection";
    case CoverageSource::kDeltaMerge: return "delta-merge";
    case CoverageSource::kCanonical: return "canonical";
  }
  return "unknown";
}
const char* PhysicalName(PhysicalOpKind kind) {
  switch (kind) {
    case PhysicalOpKind::kCanonicalScan: return "canonical-scan";
    case PhysicalOpKind::kProjectionScan: return "projection-scan";
    case PhysicalOpKind::kDeltaMerge: return "delta-merge";
    case PhysicalOpKind::kCanonicalFallback: return "canonical-fallback";
    case PhysicalOpKind::kAdjacencySeek: return "adjacency-seek";
    case PhysicalOpKind::kLaneExchange: return "lane-exchange";
    case PhysicalOpKind::kFilter: return "filter";
    case PhysicalOpKind::kProject: return "project";
    case PhysicalOpKind::kAggregate: return "aggregate";
    case PhysicalOpKind::kSort: return "sort";
  }
  return "unknown";
}

void CollectPushdowns(const LogicalPlanNode& node, PhysicalPlan* plan) {
  if (node.predicate()) plan->pushdowns.push_back("predicate");
  if (node.property_binding()) plan->pushdowns.push_back("property-presence");
  if (node.expand_spec()) {
    plan->pushdowns.push_back("entity/type/time");
    if (node.expand_spec()->direction != ExpandDirection::kBoth) {
      plan->operations.push_back(PhysicalOpKind::kAdjacencySeek);
    }
  }
  for (const auto& child : node.inputs()) CollectPushdowns(*child, plan);
}

std::optional<CoverageRegion> MatchingRegion(const CoverageRegion& region,
                                              const LogicalPlanNode& logical) {
  const LogicalPlanNode* scoped = &logical;
  while (!scoped->scope() && !scoped->inputs().empty()) {
    scoped = scoped->inputs().front().get();
  }
  const auto scope = scoped->scope();
  if (!scope) return std::nullopt;
  if (!ValidRange(region.valid_time)) return std::nullopt;
  const LogicalPlanNode* scan = &logical;
  while (!scan->inputs().empty()) scan = scan->inputs().front().get();
  if (scan->kind() == LogicalOpKind::kVertexScan &&
      region.kind != ProjectionKind::kState) return std::nullopt;
  if (scan->kind() == LogicalOpKind::kEdgeScan &&
      region.kind != ProjectionKind::kAdjacency &&
      region.kind != ProjectionKind::kState) return std::nullopt;
  return region;
}

const LogicalPlanNode* FindScopedNode(const LogicalPlanNode& logical) {
  const LogicalPlanNode* node = &logical;
  while (!node->scope() && !node->inputs().empty()) node = node->inputs().front().get();
  return node->scope() ? node : nullptr;
}

bool ContainsKind(const LogicalPlanNode& node, LogicalOpKind kind) {
  if (node.kind() == kind) return true;
  return std::any_of(node.inputs().begin(), node.inputs().end(),
                     [kind](const auto& child) { return ContainsKind(*child, kind); });
}

}  // namespace

StatusOr<PhysicalPlan> QueryPlanner::Bind(const LogicalPlanNode& logical,
                                          const PlanningContext& context) {
  const LogicalPlanNode* scoped_node = FindScopedNode(logical);
  if (!scoped_node) return Status::InvalidArgument("query planner", "plan has no temporal scope");
  const auto scope = scoped_node->scope();
  const auto requested = ScopeInterval(*scope);
  if (!requested || !ValidRange(*requested)) {
    if (std::holds_alternative<History>(*scope) &&
        !std::get<History>(*scope).interval.has_value() &&
        context.options.mode != QueryExecutionMode::kAnalytical) {
      return Status::InvalidArgument("query planner", "unbounded History requires analytical mode");
    }
    return Status::NotSupported("query planner", "unbounded temporal scope requires analytical execution");
  }
  if (context.snapshot_seq.value == 0) {
    return Status::InvalidArgument("query planner", "snapshot sequence is zero");
  }
  if (!context.database_identity.empty() &&
      context.projections.database_identity != context.database_identity) {
    return Status::IdentityConflict("query planner", "projection database identity differs");
  }

  std::vector<CoverageRegion> regions;
  for (const auto& region : context.projections.regions) {
    if (MatchingRegion(region, logical)) regions.push_back(region);
  }
  std::sort(regions.begin(), regions.end(), [](const auto& a, const auto& b) {
    return a.valid_time.from.value < b.valid_time.from.value;
  });
  bool entity_partition = false;
  for (size_t i = 1; i < regions.size(); ++i) {
    const auto& left = regions[i - 1];
    const auto& right = regions[i];
    const bool same_key = left.kind == right.kind && left.part_id == right.part_id &&
                          left.property_id == right.property_id &&
                          left.schema_epoch == right.schema_epoch;
    if (same_key && EntityOverlap(left, right) &&
        Overlap(left.valid_time, right.valid_time)) {
      return Status::Corruption("query planner", "overlapping projection coverage");
    }
    if (same_key && !EntityOverlap(left, right) &&
        Overlap(left.valid_time, right.valid_time)) {
      entity_partition = true;
    }
  }

  PhysicalPlan plan;
  const uint64_t estimate_rows = context.statistics.known
                                     ? context.statistics.candidate_rows
                                     : 4096;
  plan.estimate.rows = estimate_rows;
  plan.estimate.pages = context.statistics.pages;
  plan.estimate.physical_bytes = context.statistics.physical_bytes;
  plan.estimate.decoded_bytes = context.statistics.decoded_bytes;
  plan.estimate.dirty_chains = context.statistics.dirty_chains;
  plan.estimate.interval_fragments = context.statistics.interval_fragments;
  plan.estimate.fanout = context.statistics.fanout;
  plan.estimate.uncertain = !context.statistics.known;
  plan.conservative = !context.statistics.known;

  const bool broad = estimate_rows > 4096 || ContainsKind(logical, LogicalOpKind::kSort) ||
                     ContainsKind(logical, LogicalOpKind::kAggregateRows) ||
                     ContainsKind(logical, LogicalOpKind::kTemporalAggregate);
  plan.lane = context.options.mode == QueryExecutionMode::kAuto
                  ? (broad ? QueryExecutionMode::kAnalytical : QueryExecutionMode::kInteractive)
                  : context.options.mode;
  plan.spill_allowed = plan.lane == QueryExecutionMode::kAnalytical;

  uint64_t cursor = requested->from.value;
  const uint64_t requested_to = requested->to ? requested->to->value
                                             : std::numeric_limits<uint64_t>::max();
  if (entity_partition) {
    // CoverageSlice is intentionally one-dimensional in valid time. When
    // regions partition the entity key space, claiming a complete temporal
    // slice would omit entities; retain canonical correctness until an
    // executor with multidimensional slices is available.
    plan.coverage_slices.push_back(Canonical(*requested));
  }
  for (const auto& region : regions) {
    if (entity_partition) break;
    const uint64_t region_from = std::max(cursor, region.valid_time.from.value);
    const uint64_t region_end = region.valid_time.to
                                    ? region.valid_time.to->value
                                    : std::numeric_limits<uint64_t>::max();
    const uint64_t region_to = std::min(requested_to, region_end);
    if (region_from >= region_to) continue;
    if (cursor < region_from) {
      plan.coverage_slices.push_back(Canonical(ValidTimeInterval{ValidTime{cursor}, ValidTime{region_from}}));
    }
    CoverageSlice slice{CoverageSource::kProjection,
                        ValidTimeInterval{ValidTime{region_from},
                                          region_to == std::numeric_limits<uint64_t>::max()
                                              ? std::nullopt
                                              : std::optional<ValidTime>(ValidTime{region_to})},
                        region.segments.empty() ? std::nullopt : std::optional<uint64_t>(context.projections.generation_id),
                        region.segments.empty() ? std::nullopt : std::optional<CommitSeq>(context.projections.base_seq)};
    slice.kind = region.kind;
    slice.part_id = region.part_id;
    slice.property_id = region.property_id;
    slice.schema_epoch = region.schema_epoch;
    slice.entity_min = region.entity_min;
    slice.entity_max_exclusive = region.entity_max_exclusive;
    slice.database_identity = context.projections.database_identity;
    if (context.projections.base_seq.value > context.snapshot_seq.value) {
      return Status::Corruption("query planner", "projection base is newer than snapshot");
    }
    const bool delta_complete =
        context.projections.base_seq.value <= context.snapshot_seq.value &&
        context.delta.base_seq.value <= context.projections.base_seq.value &&
        context.delta.through.value >= context.snapshot_seq.value &&
        context.delta.first_missing.value == 0;
    if (context.projections.base_seq.value < context.snapshot_seq.value &&
        delta_complete && context.allow_delta_merge) {
      slice.source = CoverageSource::kDeltaMerge;
      plan.operations.push_back(PhysicalOpKind::kDeltaMerge);
    } else if (context.projections.base_seq.value < context.snapshot_seq.value) {
      // A projection without a contiguous (base,S] tail is not complete.
      // Keep the slice canonical rather than claiming a partial merge.
      slice.source = CoverageSource::kCanonical;
      slice.projection_generation.reset();
      slice.projection_base.reset();
      plan.pushdowns.push_back("delta-fallback");
    }
    plan.coverage_slices.push_back(std::move(slice));
    cursor = region_to;
    if (cursor >= requested_to) break;
  }
  if (!entity_partition && cursor < requested_to) {
    plan.coverage_slices.push_back(Canonical(ValidTimeInterval{
        ValidTime{cursor}, requested->to}));
  }
  if (plan.coverage_slices.empty()) plan.coverage_slices.push_back(Canonical(*requested));
  for (const auto& slice : plan.coverage_slices) {
    plan.operations.push_back(slice.source == CoverageSource::kProjection
                                  ? PhysicalOpKind::kProjectionScan
                                  : slice.source == CoverageSource::kDeltaMerge
                                        ? PhysicalOpKind::kDeltaMerge
                                        : PhysicalOpKind::kCanonicalFallback);
  }
  CollectPushdowns(logical, &plan);
  if (plan.lane == QueryExecutionMode::kInteractive && plan.coverage_slices.size() > 1) {
    plan.has_lane_exchange = true;
    plan.operations.push_back(PhysicalOpKind::kLaneExchange);
  }
  plan.explain.logical = logical.kind();
  plan.explain.physical = plan.operations.empty() ? PhysicalOpKind::kCanonicalScan : plan.operations.back();
  plan.explain.lane = plan.lane;
  plan.explain.estimate = plan.estimate;
  for (const auto& slice : plan.coverage_slices) {
    plan.explain.coverage.push_back({slice.source, slice.interval.from.value,
                                    slice.interval.to ? std::optional<uint64_t>(slice.interval.to->value) : std::nullopt,
                                    slice.projection_generation, slice.projection_base});
  }
  return plan;
}

std::string QueryPlanner::ExplainLogical(const LogicalPlanNode& logical) {
  std::ostringstream out;
  out << "logical=" << static_cast<int>(logical.kind())
      << " columns=" << logical.schema().columns().size();
  if (logical.scope()) out << " temporal=bound";
  for (const auto& child : logical.inputs()) out << " [" << ExplainLogical(*child) << "]";
  return out.str();
}

std::string QueryPlanner::ExplainPhysical(const PhysicalPlan& plan) {
  std::ostringstream out;
  out << "lane=" << (plan.lane == QueryExecutionMode::kAnalytical ? "analytical" : "interactive")
      << " rows=" << plan.estimate.rows << " uncertain=" << (plan.estimate.uncertain ? "true" : "false")
      << " spill=" << (plan.spill_allowed ? "true" : "false") << " ops=";
  for (size_t i = 0; i < plan.operations.size(); ++i) {
    if (i) out << ',';
    out << PhysicalName(plan.operations[i]);
  }
  out << " coverage=";
  for (size_t i = 0; i < plan.coverage_slices.size(); ++i) {
    if (i) out << ';';
    const auto& slice = plan.coverage_slices[i];
    out << SourceName(slice.source) << '[' << slice.interval.from.value << ',';
    if (slice.interval.to) out << slice.interval.to->value;
    else out << "inf";
    out << ')';
  }
  return out.str();
}

}  // namespace cedar::internal
