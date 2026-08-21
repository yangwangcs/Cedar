// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/runtime/graph_frontier.h"

#include <algorithm>
#include <limits>
#include <map>
#include <set>

#include "query/temporal/corrected_chain.h"
#include "query/temporal/interval.h"

namespace cedar::internal {
namespace {

struct VertexKey {
  VertexRef ref;
  bool operator<(const VertexKey& other) const {
    if (ref.part_id.value != other.ref.part_id.value)
      return ref.part_id.value < other.ref.part_id.value;
    return ref.vertex_id.value < other.ref.vertex_id.value;
  }
};

struct EdgeKey {
  EdgeRef ref;
  bool operator<(const EdgeKey& other) const {
    if (ref.home_part_id.value != other.ref.home_part_id.value)
      return ref.home_part_id.value < other.ref.home_part_id.value;
    return ref.edge_id.value < other.ref.edge_id.value;
  }
};

struct IdentityKey {
  EdgeRef edge;
  VertexRef source;
  VertexRef target;
  uint64_t type;
  bool operator<(const IdentityKey& other) const {
    if (EdgeKey{edge} < EdgeKey{other.edge}) return true;
    if (EdgeKey{other.edge} < EdgeKey{edge}) return false;
    if (VertexKey{source} < VertexKey{other.source}) return true;
    if (VertexKey{other.source} < VertexKey{source}) return false;
    if (VertexKey{target} < VertexKey{other.target}) return true;
    if (VertexKey{other.target} < VertexKey{target}) return false;
    return type < other.type;
  }
};

std::optional<ValidTimeInterval> RequestInterval(const ValidTimeInterval& value) {
  if (!value.Validate().ok()) return std::nullopt;
  return value;
}

StatusOr<std::vector<FactEvent>> ReadEvents(Snapshot& snapshot,
                                             const FactRef& ref,
                                             const QueryDeltaView* delta) {
  std::vector<FactEvent> events;
  FactScanSpec spec;
  spec.part_id = ref.part_id();
  spec.family = ref.family();
  spec.property_id = ref.property_id();
  spec.entity_id_min = ref.entity_id();
  spec.entity_id_max = ref.entity_id();
  Status status = snapshot.EventScan(spec, [&events](const FactEventBatch& batch) {
    events.insert(events.end(), batch.events.begin(), batch.events.end());
    return Status::OK();
  });
  if (!status.ok()) return status;
  if (delta != nullptr) {
    auto tail = delta->EventsFor(ref);
    events.insert(events.end(), tail.begin(), tail.end());
  }
  return events;
}

StatusOr<std::vector<StateInterval>> VisibleIntervals(
    Snapshot& snapshot, const FactRef& ref, const QueryDeltaView* delta) {
  auto events = ReadEvents(snapshot, ref, delta);
  if (!events.ok()) return events.status();
  if (events.ValueOrDie().empty()) return std::vector<StateInterval>{};
  auto corrected = ResolveCorrectedBoundaries(events.ValueOrDie(),
                                               snapshot.commit_seq());
  if (!corrected.ok()) return corrected.status();
  return MaterializePresentState(corrected.ValueOrDie());
}

StatusOr<std::vector<EdgeIdentity>> Identities(Snapshot& snapshot,
                                                const QueryDeltaView* delta) {
  std::map<IdentityKey, EdgeIdentity> unique;
  Status status = snapshot.ScanFamily(FactFamily::kEdgeIdentity,
                                      [&unique](const FactEvent& event) {
    if (!event.edge_identity.has_value()) return Status::OK();
    const EdgeIdentity& identity = *event.edge_identity;
    const EdgeIdentity copy = identity;
    unique.emplace(IdentityKey{copy.edge_ref(), copy.source_ref(),
                                copy.target_ref(), copy.edge_type}, copy);
    return Status::OK();
  });
  if (!status.ok()) return status;
  if (delta != nullptr) {
    for (const EdgeIdentity& identity : delta->EdgeIdentitiesThrough(
             snapshot.commit_seq())) {
      unique.emplace(IdentityKey{identity.edge_ref(), identity.source_ref(),
                                 identity.target_ref(), identity.edge_type},
                     identity);
    }
  }
  std::vector<EdgeIdentity> result;
  result.reserve(unique.size());
  for (const auto& entry : unique) result.push_back(entry.second);
  return result;
}

bool Contains(const VertexRef& ref, const std::vector<VertexRef>& frontier) {
  return std::find(frontier.begin(), frontier.end(), ref) != frontier.end();
}

Status ChargeTraversal(const GraphFrontierOptions& options) {
  if (options.reservation == nullptr) return Status::OK();
  if (Status status = options.reservation->ReserveGraphLabels(1); !status.ok())
    return status;
  return options.reservation->ReserveIntervalFragments(1);
}

}  // namespace

StatusOr<std::vector<TemporalTraversal>> ExpandTemporal(
    Snapshot& snapshot, const GraphExpansionRequest& request,
    const GraphFrontierOptions& options) {
  const auto interval = RequestInterval(request.interval);
  if (!interval.has_value()) {
    return Status::InvalidArgument("graph expansion", "invalid request interval");
  }
  StatusOr<std::vector<EdgeIdentity>> identities =
      options.adjacency_seek
          ? options.adjacency_seek(request.frontier, request.direction,
                                   request.edge_type)
          : Identities(snapshot, options.delta);
  if (!identities.ok()) return identities.status();
  if (options.candidates_examined != nullptr) {
    *options.candidates_examined += identities.ValueOrDie().size();
  }
  std::vector<TemporalTraversal> output;
  for (const EdgeIdentity& identity : identities.ValueOrDie()) {
    if (request.edge_type.has_value() &&
        identity.edge_type != *request.edge_type) continue;
    const bool out = Contains(identity.source_ref(), request.frontier);
    const bool in = Contains(identity.target_ref(), request.frontier);
    if ((request.direction == ExpandDirection::kOut && !out) ||
        (request.direction == ExpandDirection::kIn && !in) ||
        (request.direction == ExpandDirection::kBoth && !out && !in)) {
      continue;
    }
    auto edge_intervals = VisibleIntervals(
        snapshot, EntityFact::Edge(identity.edge_ref()).ref(), options.delta);
    if (!edge_intervals.ok()) return edge_intervals.status();
    auto source_intervals = VisibleIntervals(
        snapshot, EntityFact::Vertex(identity.source_ref()).ref(), options.delta);
    if (!source_intervals.ok()) return source_intervals.status();
    auto target_intervals = VisibleIntervals(
        snapshot, EntityFact::Vertex(identity.target_ref()).ref(), options.delta);
    if (!target_intervals.ok()) return target_intervals.status();
    for (const StateInterval& edge : edge_intervals.ValueOrDie()) {
      for (const StateInterval& source : source_intervals.ValueOrDie()) {
        auto effective = Intersect(*interval, edge.interval);
        if (!effective) continue;
        effective = Intersect(*effective, source.interval);
        if (!effective) continue;
        for (const StateInterval& target : target_intervals.ValueOrDie()) {
          auto clipped = Intersect(*effective, target.interval);
          if (!clipped) continue;
          if (Status status = ChargeTraversal(options); !status.ok()) return status;
          output.push_back({identity.source_ref(), identity.edge_ref(),
                            identity.target_ref(), identity.edge_type, *clipped});
        }
      }
    }
  }
  std::sort(output.begin(), output.end(), [](const auto& a, const auto& b) {
    if (a.effective.from.value != b.effective.from.value)
      return a.effective.from.value < b.effective.from.value;
    if (a.source.part_id.value != b.source.part_id.value)
      return a.source.part_id.value < b.source.part_id.value;
    if (a.source.vertex_id.value != b.source.vertex_id.value)
      return a.source.vertex_id.value < b.source.vertex_id.value;
    return a.edge.edge_id.value < b.edge.edge_id.value;
  });
  return output;
}

StatusOr<KHopResult> KHopExpand(Snapshot& snapshot,
                                const GraphExpansionRequest& request,
                                const GraphFrontierOptions& options) {
  if (options.max_hops == 0) return KHopResult{};
  KHopResult result;
  std::map<VertexKey, std::pair<uint32_t, std::vector<ValidTimeInterval>>> visited;
  std::vector<VertexRef> frontier = request.frontier;
  for (const VertexRef& vertex : frontier) {
    visited.emplace(VertexKey{vertex}, std::make_pair(0, std::vector<ValidTimeInterval>{}));
  }
  for (uint32_t depth = 1; depth <= options.max_hops && !frontier.empty(); ++depth) {
    GraphExpansionRequest layer = request;
    layer.frontier = frontier;
    auto expanded = ExpandTemporal(snapshot, layer, options);
    if (!expanded.ok()) return expanded.status();
    std::vector<VertexRef> next;
    for (const TemporalTraversal& traversal : expanded.ValueOrDie()) {
      result.traversals.push_back(traversal);
      const VertexRef destination =
          request.direction == ExpandDirection::kIn ? traversal.source
                                                     : traversal.target;
      auto found = visited.find(VertexKey{destination});
      if (found != visited.end()) {
        if (found->second.first < depth) continue;
        bool covered = false;
        for (const auto& prior : found->second.second) {
          const uint64_t prior_to = prior.to.value_or(
              ValidTime{std::numeric_limits<uint64_t>::max()}).value;
          const uint64_t current_to = traversal.effective.to.value_or(
              ValidTime{std::numeric_limits<uint64_t>::max()}).value;
          if (prior.from.value <= traversal.effective.from.value &&
              current_to <= prior_to) {
            covered = true;
            break;
          }
        }
        if (covered) continue;
        found->second.second.push_back(traversal.effective);
      }
      if (options.reservation != nullptr) {
        if (found == visited.end()) {
          if (Status status = options.reservation->ReserveVisitedVertices(1);
              !status.ok()) return status;
        }
      }
      if (found == visited.end()) {
        visited.emplace(VertexKey{destination},
                        std::make_pair(depth,
                                       std::vector<ValidTimeInterval>{traversal.effective}));
      }
      next.push_back(destination);
      result.labels.push_back(GraphLabel{destination, depth,
                                         std::optional<VertexRef>{traversal.source},
                                         traversal.effective});
    }
    frontier = std::move(next);
  }
  return result;
}

}  // namespace cedar::internal
