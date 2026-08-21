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
                                                const QueryDeltaView* delta,
                                                uint64_t max_candidates = 0) {
  std::map<IdentityKey, EdgeIdentity> unique;
  Status status = snapshot.ScanFamily(FactFamily::kEdgeIdentity,
                                      [&unique, max_candidates](const FactEvent& event) {
    if (!event.edge_identity.has_value()) return Status::OK();
    const EdgeIdentity& identity = *event.edge_identity;
    const EdgeIdentity copy = identity;
    unique.emplace(IdentityKey{copy.edge_ref(), copy.source_ref(),
                                copy.target_ref(), copy.edge_type}, copy);
    if (max_candidates != 0 && unique.size() > max_candidates) {
      return Status::ResourceExhausted("adjacency", "bounded fallback candidate limit exceeded");
    }
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

bool AdjacencyIndex::Key::operator<(const Key& other) const {
  if (VertexKey{vertex} < VertexKey{other.vertex}) return true;
  if (VertexKey{other.vertex} < VertexKey{vertex}) return false;
  if (direction != other.direction)
    return static_cast<uint8_t>(direction) < static_cast<uint8_t>(other.direction);
  if (edge_type.has_value() != other.edge_type.has_value())
    return !edge_type.has_value();
  return edge_type.has_value() && *edge_type < *other.edge_type;
}

void AdjacencyIndex::Add(const Entry& entry) {
  const EdgeIdentity& identity = entry.identity;
  const Key keys[] = {
      {identity.source_ref(), ExpandDirection::kOut, std::nullopt},
      {identity.source_ref(), ExpandDirection::kOut, identity.edge_type},
      {identity.target_ref(), ExpandDirection::kIn, std::nullopt},
      {identity.target_ref(), ExpandDirection::kIn, identity.edge_type},
  };
  for (const Key& key : keys) {
    auto& posting = postings_[key];
    const auto duplicate = std::find_if(
        posting.begin(), posting.end(), [&entry](const Entry& prior) {
          return prior.identity.edge_ref() == entry.identity.edge_ref();
        });
    if (duplicate == posting.end()) posting.push_back(entry);
  }
}

Status AdjacencyIndex::Build(Snapshot& snapshot, uint64_t generation) {
  postings_.clear();
  generation_ = generation;
  Status status = snapshot.ScanFamily(
      FactFamily::kEdgeIdentity, [this, generation](const FactEvent& event) {
        if (!event.edge_identity.has_value()) return Status::OK();
        Add(Entry{*event.edge_identity, event.commit_seq, generation});
        return Status::OK();
      });
  if (!status.ok()) return status;
  built_through_ = snapshot.commit_seq();
  return Status::OK();
}

Status AdjacencyIndex::Build(const std::vector<FactEvent>& events,
                             CommitSeq snapshot_seq, uint64_t generation) {
  postings_.clear();
  generation_ = generation;
  for (const FactEvent& event : events) {
    if (!event.edge_identity.has_value()) continue;
    Add(Entry{*event.edge_identity, event.commit_seq, generation});
  }
  built_through_ = snapshot_seq;
  return Status::OK();
}

Status AdjacencyIndex::ApplyDelta(const QueryDeltaView& delta,
                                  uint64_t generation) {
  generation_ = generation;
  if (delta.edge_identity_records.empty()) {
    for (const EdgeIdentity& identity : delta.edge_identities) {
      Add(Entry{identity, delta.through, generation});
    }
  } else {
    for (const auto& record : delta.edge_identity_records) {
      if (record.first.value > delta.through.value) continue;
      Add(Entry{record.second, record.first, generation});
    }
  }
  if (delta.through.value > built_through_.value) built_through_ = delta.through;
  return Status::OK();
}

StatusOr<std::vector<EdgeIdentity>> AdjacencyIndex::Seek(
    const std::vector<VertexRef>& frontier, ExpandDirection direction,
    std::optional<uint64_t> edge_type, CommitSeq snapshot_seq,
    std::optional<uint64_t> generation, const QueryDeltaView* delta) const {
  const bool delta_has_identity =
      delta != nullptr && (!delta->edge_identities.empty() ||
                           !delta->edge_identity_records.empty());
  const bool delta_covers = delta_has_identity &&
                            delta->through.value >= snapshot_seq.value;
  if (!covers(snapshot_seq) && !delta_covers) {
    return Status::NotFound("adjacency index", "posting coverage is missing");
  }
  std::map<IdentityKey, EdgeIdentity> unique;
  auto collect = [&](const VertexRef& vertex, ExpandDirection posting_direction) {
    const Key key{vertex, posting_direction, edge_type};
    const auto found = postings_.find(key);
    if (found == postings_.end()) return;
    for (const Entry& entry : found->second) {
      if (entry.commit_seq.value > snapshot_seq.value) continue;
      if (generation.has_value() && entry.generation != *generation) continue;
      const EdgeIdentity& identity = entry.identity;
      unique.emplace(IdentityKey{identity.edge_ref(), identity.source_ref(),
                                 identity.target_ref(), identity.edge_type},
                     identity);
    }
  };
  for (const VertexRef& vertex : frontier) {
    if (direction == ExpandDirection::kIn || direction == ExpandDirection::kBoth)
      collect(vertex, ExpandDirection::kIn);
    if (direction == ExpandDirection::kOut || direction == ExpandDirection::kBoth)
      collect(vertex, ExpandDirection::kOut);
  }
  if (delta != nullptr) {
    auto add_delta = [&](CommitSeq seq, const EdgeIdentity& identity) {
      if (seq.value > snapshot_seq.value) return;
      const bool out = Contains(identity.source_ref(), frontier);
      const bool in = Contains(identity.target_ref(), frontier);
      if ((direction == ExpandDirection::kOut && !out) ||
          (direction == ExpandDirection::kIn && !in) ||
          (direction == ExpandDirection::kBoth && !out && !in)) return;
      if (edge_type.has_value() && identity.edge_type != *edge_type) return;
      unique.emplace(IdentityKey{identity.edge_ref(), identity.source_ref(),
                                 identity.target_ref(), identity.edge_type},
                     identity);
    };
    if (delta->edge_identity_records.empty()) {
      for (const EdgeIdentity& identity : delta->edge_identities)
        add_delta(delta->through, identity);
    } else {
      for (const auto& record : delta->edge_identity_records)
        add_delta(record.first, record.second);
    }
  }
  std::vector<EdgeIdentity> result;
  result.reserve(unique.size());
  for (const auto& item : unique) result.push_back(item.second);
  return result;
}

StatusOr<std::vector<TemporalTraversal>> ExpandTemporal(
    Snapshot& snapshot, const GraphExpansionRequest& request,
    const GraphFrontierOptions& options) {
  const auto interval = RequestInterval(request.interval);
  if (!interval.has_value()) {
    return Status::InvalidArgument("graph expansion", "invalid request interval");
  }
  StatusOr<std::vector<EdgeIdentity>> identities =
      options.adjacency_index
          ? options.adjacency_index->Seek(request.frontier, request.direction,
                                         request.edge_type, snapshot.commit_seq(),
                                         std::nullopt, options.delta)
          : options.adjacency_seek
          ? options.adjacency_seek(request.frontier, request.direction,
                                   request.edge_type)
      : Identities(snapshot, options.delta, options.fallback_candidate_limit);
  if (!identities.ok() && identities.status().IsNotFound() &&
      options.adjacency_seek) {
    identities = options.adjacency_seek(request.frontier, request.direction,
                                        request.edge_type);
  } else if (!identities.ok() && identities.status().IsNotFound() &&
             options.adjacency_index) {
    // Cache misses are explicit: the canonical lane is only used when the
    // caller supplied a finite fallback bound (or left it unlimited for an
    // analytical query).
    identities = Identities(snapshot, options.delta,
                            options.fallback_candidate_limit);
  }
  if (!identities.ok()) return identities.status();
  if (options.fallback_candidate_limit != 0 &&
      identities.ValueOrDie().size() > options.fallback_candidate_limit) {
    return Status::ResourceExhausted("adjacency", "bounded fallback candidate limit exceeded");
  }
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
