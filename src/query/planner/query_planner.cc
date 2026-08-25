#include "query/planner/query_planner.h"

#include <algorithm>
#include <functional>
#include <limits>
#include <map>
#include <queue>
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

bool PartAllowed(const PartScope& scope, PartId part) {
  if (scope.kind == PartScopeKind::kAll) return true;
  return std::binary_search(scope.parts.begin(), scope.parts.end(), part,
                            [](PartId left, PartId right) {
                              return left.value < right.value;
                            });
}

bool SafeCanonicalLimit(const LogicalPlanNode& logical) {
  if (logical.kind() != LogicalOpKind::kLimit ||
      !logical.limit_offset().has_value() || !logical.limit_count().has_value() ||
      *logical.limit_offset() != 0 || logical.inputs().size() != 1) {
    return false;
  }
  const LogicalPlanNode* project = logical.inputs().front().get();
  if (project->kind() != LogicalOpKind::kProject ||
      project->inputs().size() != 1 || project->schema().columns().size() != 1) {
    return false;
  }
  const LogicalPlanNode* temporal = project->inputs().front().get();
  if (temporal->inputs().size() != 1 || temporal->predicate() ||
      temporal->property_binding().has_value() ||
      temporal->metadata_binding().has_value()) {
    return false;
  }
  switch (temporal->kind()) {
    case LogicalOpKind::kStateAt:
      break;
    default:
      return false;
  }
  const LogicalPlanNode* scan = temporal->inputs().front().get();
  return scan->inputs().empty() && scan->schema().columns().size() == 1 &&
         (scan->kind() == LogicalOpKind::kVertexScan ||
          scan->kind() == LogicalOpKind::kEdgeScan);
}

struct CoverageKey {
  ProjectionKind kind;
  PartId part;
  std::optional<PropertyId> property;
  uint32_t epoch;
  bool operator<(const CoverageKey& other) const {
    if (kind != other.kind) return kind < other.kind;
    if (part.value != other.part.value) return part.value < other.part.value;
    if (property.has_value() != other.property.has_value()) {
      return property.has_value() < other.property.has_value();
    }
    if (property && property->value != other.property->value) {
      return property->value < other.property->value;
    }
    return epoch < other.epoch;
  }
};

class ActiveValidIntervals {
 public:
  ~ActiveValidIntervals() { Clear(root_); }
  bool Overlaps(uint64_t from, uint64_t to) const {
    return Overlaps(root_, from, to);
  }

  void Insert(uint64_t from, uint64_t to, uint64_t id) {
    root_ = Insert(root_, new Node{{from, id}, to, Priority(from, id)});
  }

  void Erase(uint64_t from, uint64_t id) {
    root_ = Erase(root_, Key{from, id});
  }

 private:
  struct Key {
    uint64_t from;
    uint64_t id;
    bool operator<(const Key& other) const {
      return from < other.from || (from == other.from && id < other.id);
    }
  };
  struct Node {
    Key key;
    uint64_t to;
    uint64_t priority;
    uint64_t max_to;
    Node* left = nullptr;
    Node* right = nullptr;
    Node(Key k, uint64_t end, uint64_t p)
        : key(k), to(end), priority(p), max_to(end) {}
  };

  static uint64_t Priority(uint64_t from, uint64_t id) {
    uint64_t x = from ^ (id + 0x9e3779b97f4a7c15ULL + (from << 6) + (from >> 2));
    x ^= x >> 30;
    x *= 0xbf58476d1ce4e5b9ULL;
    x ^= x >> 27;
    return x ^ (x >> 31);
  }
  static uint64_t MaxTo(const Node* node) { return node ? node->max_to : 0; }
  static void Pull(Node* node) {
    if (node) node->max_to = std::max(node->to, std::max(MaxTo(node->left), MaxTo(node->right)));
  }
  static Node* RotateRight(Node* node) {
    Node* next = node->left;
    node->left = next->right;
    next->right = node;
    Pull(node); Pull(next); return next;
  }
  static Node* RotateLeft(Node* node) {
    Node* next = node->right;
    node->right = next->left;
    next->left = node;
    Pull(node); Pull(next); return next;
  }
  static Node* Insert(Node* root, Node* node) {
    if (!root) return node;
    if (node->key < root->key) {
      root->left = Insert(root->left, node);
      if (root->left->priority > root->priority) root = RotateRight(root);
    } else {
      root->right = Insert(root->right, node);
      if (root->right->priority > root->priority) root = RotateLeft(root);
    }
    Pull(root); return root;
  }
  static Node* Merge(Node* left, Node* right) {
    if (!left) return right;
    if (!right) return left;
    if (left->priority > right->priority) {
      left->right = Merge(left->right, right); Pull(left); return left;
    }
    right->left = Merge(left, right->left); Pull(right); return right;
  }
  static Node* Erase(Node* root, Key key) {
    if (!root) return nullptr;
    if (root->key.from == key.from && root->key.id == key.id) {
      Node* merged = Merge(root->left, root->right);
      delete root; return merged;
    }
    if (key < root->key) root->left = Erase(root->left, key);
    else root->right = Erase(root->right, key);
    Pull(root); return root;
  }
  static bool Overlaps(const Node* root, uint64_t from, uint64_t to) {
    if (!root || root->max_to <= from) return false;
    if (root->left && Overlaps(root->left, from, to)) return true;
    if (root->key.from < to && root->to > from) return true;
    return root->key.from < to && Overlaps(root->right, from, to);
  }
  static void Clear(Node* node) {
    if (!node) return;
    Clear(node->left); Clear(node->right); delete node;
  }
  Node* root_ = nullptr;
};

Status ValidateCoverageRegions(const std::vector<CoverageRegion>& regions) {
  std::map<CoverageKey, std::vector<const CoverageRegion*>> grouped;
  for (const auto& region : regions) {
    grouped[{region.kind, region.part_id, region.property_id, region.schema_epoch}]
        .push_back(&region);
  }
  for (auto& [key, values] : grouped) {
    (void)key;
    std::sort(values.begin(), values.end(), [](const auto* left, const auto* right) {
      if (left->entity_min != right->entity_min) return left->entity_min < right->entity_min;
      if (left->entity_max_exclusive != right->entity_max_exclusive)
        return left->entity_max_exclusive < right->entity_max_exclusive;
      return left->valid_time.from.value < right->valid_time.from.value;
    });
    struct Expiration {
      uint64_t end;
      uint64_t start;
      uint64_t id;
      bool operator>(const Expiration& o) const { return end > o.end; }
    };
    std::priority_queue<Expiration, std::vector<Expiration>, std::greater<Expiration>> expirations;
    ActiveValidIntervals active;
    for (uint64_t id = 0; id < values.size(); ++id) {
      const auto& region = *values[id];
      while (!expirations.empty() && expirations.top().end <= region.entity_min) {
        active.Erase(expirations.top().start, expirations.top().id);
        expirations.pop();
      }
      const uint64_t valid_to = region.valid_time.to ? region.valid_time.to->value : UINT64_MAX;
      if (active.Overlaps(region.valid_time.from.value, valid_to)) {
        return Status::Corruption("query planner", "overlapping projection coverage");
      }
      active.Insert(region.valid_time.from.value, valid_to, id);
      expirations.push({region.entity_max_exclusive, region.valid_time.from.value, id});
    }
  }
  return Status::OK();
}

bool HasDifferentKeyTimeOverlap(const std::vector<CoverageRegion>& regions) {
  struct Item {
    const CoverageRegion* region;
    CoverageKey key;
  };
  std::vector<Item> items;
  items.reserve(regions.size());
  for (const auto& region : regions) {
    items.push_back({&region, {region.kind, region.part_id, region.property_id,
                               region.schema_epoch}});
  }
  std::sort(items.begin(), items.end(), [](const Item& left, const Item& right) {
    return left.region->valid_time.from.value < right.region->valid_time.from.value;
  });
  struct Expiration {
    uint64_t end;
    CoverageKey key;
    bool operator>(const Expiration& other) const { return end > other.end; }
  };
  std::priority_queue<Expiration, std::vector<Expiration>, std::greater<Expiration>> active;
  std::map<CoverageKey, size_t> counts;
  for (const Item& item : items) {
    const uint64_t from = item.region->valid_time.from.value;
    while (!active.empty() && active.top().end <= from) {
      auto it = counts.find(active.top().key);
      if (it != counts.end() && --it->second == 0) counts.erase(it);
      active.pop();
    }
    if (!counts.empty() && counts.find(item.key) == counts.end()) return true;
    const uint64_t end = item.region->valid_time.to ? item.region->valid_time.to->value : UINT64_MAX;
    active.push({end, item.key});
    ++counts[item.key];
  }
  return false;
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

StatusOr<StaticPlanPreparation> QueryPlanner::PrepareStatic(
    const LogicalPlanNode& logical, std::string_view schema_fingerprint) {
  StaticPlanPreparation prepared;
  std::function<void(const LogicalPlanNode&)> collect =
      [&prepared, &collect](const LogicalPlanNode& node) {
        switch (node.kind()) {
          case LogicalOpKind::kVertexScan:
          case LogicalOpKind::kEdgeScan:
          case LogicalOpKind::kStateAt:
          case LogicalOpKind::kEventsBetween:
          case LogicalOpKind::kChangesBetween:
          case LogicalOpKind::kHistory:
          case LogicalOpKind::kStateOverlaps:
          case LogicalOpKind::kStateThroughout:
            prepared.operations.push_back(PhysicalOpKind::kCanonicalScan);
            break;
          case LogicalOpKind::kExpandOut:
          case LogicalOpKind::kExpandIn:
          case LogicalOpKind::kExpandBoth:
          case LogicalOpKind::kKHopExpand:
          case LogicalOpKind::kCoexistingShortestPath:
          case LogicalOpKind::kEarliestArrival:
          case LogicalOpKind::kLatestDeparture:
          case LogicalOpKind::kFastestDuration:
            prepared.operations.push_back(PhysicalOpKind::kAdjacencySeek);
            break;
          case LogicalOpKind::kFilter:
            prepared.operations.push_back(PhysicalOpKind::kFilter);
            prepared.pushdowns.push_back("predicate");
            break;
          case LogicalOpKind::kProject:
          case LogicalOpKind::kBindProperty:
          case LogicalOpKind::kMetadataProject:
            prepared.operations.push_back(PhysicalOpKind::kProject);
            break;
          case LogicalOpKind::kSort:
            prepared.operations.push_back(PhysicalOpKind::kSort);
            break;
          case LogicalOpKind::kAggregateRows:
          case LogicalOpKind::kTemporalAggregate:
            prepared.operations.push_back(PhysicalOpKind::kAggregate);
            break;
          default:
            prepared.operations.push_back(PhysicalOpKind::kCanonicalScan);
            break;
        }
        for (const auto& input : node.inputs()) {
          if (input) collect(*input);
        }
      };
  collect(logical);
  const std::string canonical = ExplainLogical(logical) + "|" +
                                std::string(schema_fingerprint);
  uint64_t hash = 1469598103934665603ULL;
  for (unsigned char byte : canonical) {
    hash ^= byte;
    hash *= 1099511628211ULL;
  }
  prepared.fingerprint = hash == 0 ? 1 : hash;
  return prepared;
}

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
  if (context.projections.base_seq.value > context.snapshot_seq.value) {
    return Status::Corruption("query planner",
                              "projection base is newer than snapshot");
  }

  std::vector<CoverageRegion> regions;
  for (const auto& region : context.projections.regions) {
    if (PartAllowed(context.part_scope, region.part_id) &&
        MatchingRegion(region, logical)) {
      regions.push_back(region);
    }
  }
  std::sort(regions.begin(), regions.end(), [](const auto& a, const auto& b) {
    return a.valid_time.from.value < b.valid_time.from.value;
  });
  bool entity_partition = false;
  bool partial_entity_coverage = false;
  bool key_overlap_fallback = false;
  for (const auto& region : regions) {
    if (region.entity_min != 0 || region.entity_max_exclusive != UINT64_MAX) {
      partial_entity_coverage = true;
      break;
    }
  }
  const Status coverage_status = ValidateCoverageRegions(regions);
  if (!coverage_status.ok()) return coverage_status;
  key_overlap_fallback = context.part_scope.kind == PartScopeKind::kAll &&
                         HasDifferentKeyTimeOverlap(regions);

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
  if (SafeCanonicalLimit(logical)) {
    plan.safe_read_limit = *logical.limit_count();
  }

  uint64_t cursor = requested->from.value;
  const uint64_t requested_to = requested->to ? requested->to->value
                                             : std::numeric_limits<uint64_t>::max();
  const bool finite_part_scope = context.part_scope.kind != PartScopeKind::kAll;
  bool multidimensional_complete = false;
  if (finite_part_scope && !regions.empty()) {
    // For an exact partition, adjacent entity ranges form a complete slice
    // when every range covers the requested valid-time interval. Sorting once
    // gives O(R log R) planning and avoids the previous global fallback.
    std::sort(regions.begin(), regions.end(), [](const auto& a, const auto& b) {
      if (a.entity_min != b.entity_min)
        return a.entity_min < b.entity_min;
      return a.entity_max_exclusive < b.entity_max_exclusive;
    });
    std::map<uint32_t, uint64_t> entity_cursors;
    if (context.part_scope.kind == PartScopeKind::kExact) {
      entity_cursors[context.part_scope.parts.front().value] = 0;
    } else {
      for (PartId part : context.part_scope.parts) entity_cursors[part.value] = 0;
    }
    multidimensional_complete = true;
    for (const auto& region : regions) {
      const uint64_t region_end = region.entity_max_exclusive;
      const uint64_t valid_end = region.valid_time.to
                                     ? region.valid_time.to->value
                                     : std::numeric_limits<uint64_t>::max();
      auto cursor_it = entity_cursors.find(region.part_id.value);
      if (cursor_it == entity_cursors.end() ||
          region.entity_min != cursor_it->second ||
          region.valid_time.from.value > requested->from.value ||
          valid_end < requested_to) {
        multidimensional_complete = false;
        break;
      }
      cursor_it->second = region_end;
    }
    for (const auto& [part, cursor_value] : entity_cursors) {
      (void)part;
      multidimensional_complete = multidimensional_complete &&
                                  cursor_value == std::numeric_limits<uint64_t>::max();
    }
  }
  if (finite_part_scope && multidimensional_complete) {
    for (const auto& region : regions) {
      const uint64_t from = std::max(cursor, requested->from.value);
      const uint64_t to = std::min(
          requested_to,
          region.valid_time.to
              ? region.valid_time.to->value
              : std::numeric_limits<uint64_t>::max());
      if (from >= to) continue;
      CoverageSlice slice{CoverageSource::kProjection,
                          ValidTimeInterval{ValidTime{from},
                                            to == std::numeric_limits<uint64_t>::max()
                                                ? std::nullopt
                                                : std::optional<ValidTime>(ValidTime{to})},
                          region.segments.empty()
                              ? std::nullopt
                              : std::optional<uint64_t>(context.projections.generation_id),
                          region.segments.empty()
                              ? std::nullopt
                              : std::optional<CommitSeq>(context.projections.base_seq)};
      slice.kind = region.kind;
      slice.part_id = region.part_id;
      slice.part_bound = true;
      slice.property_id = region.property_id;
      slice.schema_epoch = region.schema_epoch;
      slice.entity_min = region.entity_min;
      slice.entity_max_exclusive = region.entity_max_exclusive;
      slice.database_identity = context.projections.database_identity;
      if (region.segments.empty()) {
        slice.source = CoverageSource::kCanonical;
        slice.projection_generation.reset();
        slice.projection_base.reset();
      }
      const bool delta_complete =
          context.projections.base_seq.value <= context.snapshot_seq.value &&
          context.delta.base_seq == context.projections.base_seq &&
          context.delta.through.value >= context.snapshot_seq.value &&
          context.delta.first_missing.value == 0;
      if (slice.source != CoverageSource::kCanonical &&
          context.projections.base_seq.value < context.snapshot_seq.value &&
          delta_complete && context.allow_delta_merge) {
        slice.source = CoverageSource::kDeltaMerge;
        plan.operations.push_back(PhysicalOpKind::kDeltaMerge);
      } else if (slice.source != CoverageSource::kCanonical &&
                 context.projections.base_seq.value < context.snapshot_seq.value) {
        slice.source = CoverageSource::kCanonical;
        slice.projection_generation.reset();
        slice.projection_base.reset();
      }
      plan.coverage_slices.push_back(std::move(slice));
    }
    cursor = requested_to;
  }
  if ((!finite_part_scope || !multidimensional_complete) &&
      (key_overlap_fallback || entity_partition || partial_entity_coverage)) {
    // CoverageSlice is intentionally one-dimensional in valid time. When
    // regions partition the entity key space, claiming a complete temporal
    // slice would omit entities; retain canonical correctness until an
    // executor with multidimensional slices is available.
    plan.coverage_slices.push_back(Canonical(*requested));
    plan.pushdowns.push_back("canonical-fallback");
  }
  for (const auto& region : regions) {
    if (finite_part_scope && multidimensional_complete) break;
    if (key_overlap_fallback || entity_partition || partial_entity_coverage) break;
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
    slice.part_bound = true;
    slice.property_id = region.property_id;
    slice.schema_epoch = region.schema_epoch;
    slice.entity_min = region.entity_min;
    slice.entity_max_exclusive = region.entity_max_exclusive;
    slice.database_identity = context.projections.database_identity;
    if (region.segments.empty()) {
      slice.source = CoverageSource::kCanonical;
      slice.projection_generation.reset();
      slice.projection_base.reset();
      plan.pushdowns.push_back("canonical-fallback");
    }
    const bool delta_complete =
        context.projections.base_seq.value <= context.snapshot_seq.value &&
        context.delta.base_seq == context.projections.base_seq &&
        context.delta.through.value >= context.snapshot_seq.value &&
        context.delta.first_missing.value == 0;
    if (slice.source != CoverageSource::kCanonical &&
        context.projections.base_seq.value < context.snapshot_seq.value &&
        delta_complete && context.allow_delta_merge) {
      slice.source = CoverageSource::kDeltaMerge;
      plan.operations.push_back(PhysicalOpKind::kDeltaMerge);
    } else if (slice.source != CoverageSource::kCanonical &&
               context.projections.base_seq.value < context.snapshot_seq.value) {
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
  if (!key_overlap_fallback && !entity_partition && !partial_entity_coverage &&
      cursor < requested_to) {
    plan.coverage_slices.push_back(Canonical(ValidTimeInterval{
        ValidTime{cursor}, requested->to}));
  }
  if (plan.coverage_slices.empty()) {
    plan.coverage_slices.push_back(Canonical(*requested));
    plan.pushdowns.push_back("canonical-fallback");
  }
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
    if (slice.projection_generation) {
      plan.explain.projection_generation = slice.projection_generation;
      plan.explain.projection_base = slice.projection_base;
      break;
    }
  }
  for (const auto& slice : plan.coverage_slices) {
    plan.explain.coverage.push_back({slice.source, slice.interval.from.value,
                                    slice.interval.to ? std::optional<uint64_t>(slice.interval.to->value) : std::nullopt,
                                    slice.projection_generation, slice.projection_base});
  }
  for (const auto& child : logical.inputs()) {
    QueryPlanNodeDescription child_description;
    child_description.logical = child->kind();
    child_description.physical = PhysicalOpKind::kCanonicalScan;
    child_description.lane = plan.lane;
    child_description.estimate = plan.estimate;
    plan.explain.children.push_back(std::move(child_description));
  }
  return plan;
}

std::string QueryPlanner::ExplainLogical(const LogicalPlanNode& logical) {
  std::ostringstream out;
  out << "logical=" << static_cast<int>(logical.kind())
      << " columns=" << logical.schema().columns().size();
  if (logical.scope()) out << " temporal=bound";
  if (logical.limit_offset() && logical.limit_count()) {
    out << " limit=" << *logical.limit_offset() << ":" << *logical.limit_count();
  }
  for (const auto& child : logical.inputs()) out << " [" << ExplainLogical(*child) << "]";
  return out.str();
}

std::string QueryPlanner::ExplainPhysical(const PhysicalPlan& plan) {
  std::ostringstream out;
  out << "lane=" << (plan.lane == QueryExecutionMode::kAnalytical ? "analytical" : "interactive")
      << " rows=" << plan.estimate.rows << " uncertain=" << (plan.estimate.uncertain ? "true" : "false")
      << " spill=" << (plan.spill_allowed ? "true" : "false")
      << " confidence=" << (plan.conservative ? "conservative" : "known")
      << " generation=";
  if (plan.explain.projection_generation) out << *plan.explain.projection_generation;
  else out << "none";
  out << " base=";
  if (plan.explain.projection_base) out << plan.explain.projection_base->value;
  else out << "none";
  out << " pushdowns=";
  for (size_t i = 0; i < plan.pushdowns.size(); ++i) {
    if (i) out << ',';
    out << plan.pushdowns[i];
  }
  out << " ops=";
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
    if (slice.projection_generation) out << "@generation=" << *slice.projection_generation;
    if (slice.projection_base) out << "@base=" << slice.projection_base->value;
  }
  return out.str();
}

}  // namespace cedar::internal
