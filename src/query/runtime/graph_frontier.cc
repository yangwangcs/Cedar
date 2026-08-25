// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/runtime/graph_frontier.h"

#include <algorithm>
#include <deque>
#include <limits>
#include <map>
#include <set>
#include <tuple>
#include <unordered_set>

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

struct VertexHash {
  size_t operator()(const VertexRef& ref) const noexcept {
    return std::hash<uint64_t>{}(ref.part_id.value) * 1315423911ULL ^
           std::hash<uint64_t>{}(ref.vertex_id.value);
  }
};

std::optional<ValidTimeInterval> RequestInterval(const ValidTimeInterval& value) {
  if (!value.Validate().ok()) return std::nullopt;
  return value;
}

StatusOr<std::vector<FactEvent>> ReadEvents(Snapshot& snapshot,
                                             const FactRef& ref,
                                             const QueryDeltaView* delta,
                                             const std::function<Status()>& check_abort = {}) {
  std::vector<FactEvent> events;
  FactReadSpec read_spec;
  read_spec.part_scope = PartScope::Exact(ref.part_id());
  read_spec.family = ref.family();
  read_spec.property_id = ref.property_id();
  read_spec.entity_range = EntityRange{ref.entity_id(), ref.entity_id() + 1};
  Status status = snapshot.canonical_reader().ReadEvents(
      read_spec, [&events, &check_abort](const FactEventBatch& batch) {
    if (check_abort) {
      if (Status status = check_abort(); !status.ok()) return status;
    }
    events.insert(events.end(), batch.events.begin(), batch.events.end());
    return Status::OK();
      });
  if (!status.ok()) return status;
  if (delta != nullptr) {
    auto tail = delta->EventsFor(ref);
    if (check_abort) {
      if (Status status = check_abort(); !status.ok()) return status;
    }
    events.insert(events.end(), tail.begin(), tail.end());
  }
  return events;
}

StatusOr<std::vector<StateInterval>> VisibleIntervals(
    Snapshot& snapshot, const FactRef& ref, const QueryDeltaView* delta,
    const std::function<Status()>& check_abort = {}) {
  auto events = ReadEvents(snapshot, ref, delta, check_abort);
  if (!events.ok()) return events.status();
  if (events.ValueOrDie().empty()) return std::vector<StateInterval>{};
  auto corrected = ResolveCorrectedBoundaries(events.ValueOrDie(),
                                               snapshot.commit_seq());
  if (!corrected.ok()) return corrected.status();
  return MaterializePresentState(corrected.ValueOrDie());
}

StatusOr<std::vector<EdgeIdentity>> Identities(Snapshot& snapshot,
                                             const QueryDeltaView* delta,
                                             uint64_t max_candidates = 0,
                                             const std::function<Status()>& check_abort = {},
                                             const PartScope& part_scope = PartScope::All()) {
  std::map<IdentityKey, EdgeIdentity> unique;
  FactReadSpec spec;
  spec.part_scope = part_scope;
  spec.family = FactFamily::kEdgeIdentity;
  spec.commit_seq_max = snapshot.commit_seq();
  Status status = snapshot.canonical_reader().ReadEvents(
      spec, [&unique, max_candidates, &check_abort](const FactEventBatch& batch) {
    for (const FactEvent& event : batch.events) {
    if (check_abort) {
      if (Status status = check_abort(); !status.ok()) return status;
    }
    if (!event.edge_identity.has_value()) return Status::OK();
    const EdgeIdentity& identity = *event.edge_identity;
    const EdgeIdentity copy = identity;
    unique.emplace(IdentityKey{copy.edge_ref(), copy.source_ref(),
                                copy.target_ref(), copy.edge_type}, copy);
    if (max_candidates != 0 && unique.size() > max_candidates) {
      return Status::ResourceExhausted("adjacency", "bounded fallback candidate limit exceeded");
    }
    }
    return Status::OK();
  });
  if (!status.ok()) return status;
  if (delta != nullptr) {
    for (const EdgeIdentity& identity : delta->EdgeIdentitiesThrough(
             snapshot.commit_seq())) {
      if (check_abort) {
        if (Status status = check_abort(); !status.ok()) return status;
      }
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

bool Contains(const VertexRef& ref,
              const std::unordered_set<VertexRef, VertexHash>& frontier) {
  return frontier.find(ref) != frontier.end();
}

Status ChargeTraversal(const GraphFrontierOptions& options) {
  if (options.check_abort) {
    if (Status status = options.check_abort(); !status.ok()) return status;
  }
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
  ReadCatalogBuilder catalog_builder;
  generation_ = generation;
  generation_complete_ = true;
  Status status = snapshot.ScanFamily(
      FactFamily::kEdgeIdentity, [this, generation, &catalog_builder](const FactEvent& event) {
        if (!event.edge_identity.has_value()) return Status::OK();
        const EdgeIdentity& identity = *event.edge_identity;
        Add(Entry{*event.edge_identity, event.commit_seq, generation});
        for (const auto& descriptor : {
                 std::tuple<VertexRef, ExpandDirection, std::optional<uint64_t>>{
                     identity.source_ref(), ExpandDirection::kOut, std::nullopt},
                 {identity.source_ref(), ExpandDirection::kOut, identity.edge_type},
                 {identity.target_ref(), ExpandDirection::kIn, std::nullopt},
                 {identity.target_ref(), ExpandDirection::kIn, identity.edge_type}}) {
          if (Status catalog_status = catalog_builder.AddAdjacency(
                  std::get<0>(descriptor), identity, std::get<1>(descriptor),
                  std::get<2>(descriptor));
              !catalog_status.ok()) return catalog_status;
        }
        return Status::OK();
      });
  if (!status.ok()) return status;
  auto catalog = std::move(catalog_builder).Finish();
  if (!catalog.ok()) return catalog.status();
  read_catalog_ = std::move(catalog).ConsumeValueOrDie();
  built_through_ = snapshot.commit_seq();
  return Status::OK();
}

Status AdjacencyIndex::Build(const std::vector<FactEvent>& events,
                             CommitSeq snapshot_seq, uint64_t generation) {
  postings_.clear();
  ReadCatalogBuilder catalog_builder;
  generation_ = generation;
  generation_complete_ = true;
  for (const FactEvent& event : events) {
    if (!event.edge_identity.has_value()) continue;
    const EdgeIdentity& identity = *event.edge_identity;
    Add(Entry{identity, event.commit_seq, generation});
    for (const auto& descriptor : {
             std::tuple<VertexRef, ExpandDirection, std::optional<uint64_t>>{
                 identity.source_ref(), ExpandDirection::kOut, std::nullopt},
             {identity.source_ref(), ExpandDirection::kOut, identity.edge_type},
             {identity.target_ref(), ExpandDirection::kIn, std::nullopt},
             {identity.target_ref(), ExpandDirection::kIn, identity.edge_type}}) {
      if (Status status = catalog_builder.AddAdjacency(
              std::get<0>(descriptor), identity, std::get<1>(descriptor),
              std::get<2>(descriptor));
          !status.ok()) return status;
    }
  }
  auto catalog = std::move(catalog_builder).Finish();
  if (!catalog.ok()) return catalog.status();
  read_catalog_ = std::move(catalog).ConsumeValueOrDie();
  built_through_ = snapshot_seq;
  return Status::OK();
}

Status AdjacencyIndex::ApplyDelta(const QueryDeltaView& delta,
                                  uint64_t generation) {
  if (!postings_.empty() && generation != generation_) {
    // A delta from a new projection generation cannot relabel the existing
    // snapshot postings. Keep the data for diagnostics, but force callers to
    // use canonical fallback until a complete rebuild publishes the rollover.
    generation_complete_ = false;
  }
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
    std::optional<uint64_t> generation, const QueryDeltaView* delta,
    const std::function<Status()>& check_abort) const {
  const bool delta_has_identity =
      delta != nullptr && (!delta->edge_identities.empty() ||
                           !delta->edge_identity_records.empty());
  const bool delta_covers = delta_has_identity &&
                            delta->through.value >= snapshot_seq.value;
  if (!covers(snapshot_seq) && !delta_covers) {
    return Status::NotFound("adjacency index", "posting coverage is missing");
  }
  // A snapshot index is materialized for one projection generation.  Treat a
  // pinned rollover as a cache miss so the caller can use authoritative
  // identity fallback; filtering every posting to an incompatible generation
  // would otherwise look like a valid empty result.
  if (!generation_complete_ ||
      (generation.has_value() && generation_ != *generation)) {
    return Status::NotFound("adjacency index", "posting generation is unavailable");
  }
  std::map<IdentityKey, EdgeIdentity> unique;
  auto collect = [&](const VertexRef& vertex, ExpandDirection posting_direction) -> Status {
    const Key key{vertex, posting_direction, edge_type};
    if (delta == nullptr && read_catalog_ != nullptr &&
        snapshot_seq.value == built_through_.value &&
        (!generation.has_value() || generation_ == *generation)) {
      const auto* catalog_posting =
          read_catalog_->SeekAdjacency(vertex, posting_direction, edge_type);
      if (catalog_posting != nullptr) {
        for (const EdgeIdentity& identity : *catalog_posting) {
          if (check_abort) {
            if (Status status = check_abort(); !status.ok()) return status;
          }
          unique.emplace(IdentityKey{identity.edge_ref(), identity.source_ref(),
                                     identity.target_ref(), identity.edge_type},
                        identity);
        }
        return Status::OK();
      }
    }
    const auto found = postings_.find(key);
    if (found == postings_.end()) return Status::OK();
    for (const Entry& entry : found->second) {
      if (check_abort) {
        if (Status status = check_abort(); !status.ok()) return status;
      }
      if (entry.commit_seq.value > snapshot_seq.value) continue;
      if (generation.has_value() && entry.generation != *generation) continue;
      const EdgeIdentity& identity = entry.identity;
      unique.emplace(IdentityKey{identity.edge_ref(), identity.source_ref(),
                                 identity.target_ref(), identity.edge_type},
                     identity);
    }
    return Status::OK();
  };
  for (const VertexRef& vertex : frontier) {
    if (check_abort) {
      if (Status status = check_abort(); !status.ok()) return status;
    }
    if (direction == ExpandDirection::kIn || direction == ExpandDirection::kBoth) {
      if (Status status = collect(vertex, ExpandDirection::kIn); !status.ok()) return status;
    }
    if (direction == ExpandDirection::kOut || direction == ExpandDirection::kBoth) {
      if (Status status = collect(vertex, ExpandDirection::kOut); !status.ok()) return status;
    }
  }
  if (delta != nullptr) {
    std::unordered_set<VertexRef, VertexHash> frontier_set(
        frontier.begin(), frontier.end());
    auto add_delta = [&](CommitSeq seq, const EdgeIdentity& identity) {
      if (seq.value > snapshot_seq.value) return;
      const bool out = Contains(identity.source_ref(), frontier_set);
      const bool in = Contains(identity.target_ref(), frontier_set);
      if ((direction == ExpandDirection::kOut && !out) ||
          (direction == ExpandDirection::kIn && !in) ||
          (direction == ExpandDirection::kBoth && !out && !in)) return;
      if (edge_type.has_value() && identity.edge_type != *edge_type) return;
      unique.emplace(IdentityKey{identity.edge_ref(), identity.source_ref(),
                                 identity.target_ref(), identity.edge_type},
                     identity);
    };
    if (delta->edge_identity_records.empty()) {
      for (const EdgeIdentity& identity : delta->edge_identities) {
        if (check_abort) {
          if (Status status = check_abort(); !status.ok()) return status;
        }
        add_delta(delta->through, identity);
      }
    } else {
      for (const auto& record : delta->edge_identity_records) {
        if (check_abort) {
          if (Status status = check_abort(); !status.ok()) return status;
        }
        add_delta(record.first, record.second);
      }
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
                                         options.projection_generation,
                                         options.delta,
                                         options.check_abort)
          : options.adjacency_seek
          ? options.adjacency_seek(request.frontier, request.direction,
                                   request.edge_type)
      : Identities(snapshot, options.delta, options.fallback_candidate_limit,
                   options.check_abort, options.part_scope);
  if (!identities.ok() && identities.status().IsNotFound()) {
    // Cache misses are explicit: the canonical lane is only used when the
    // caller supplied a finite fallback bound (or left it unlimited for an
    // analytical query).
    identities = Identities(snapshot, options.delta,
                            options.fallback_candidate_limit,
                            options.check_abort, options.part_scope);
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
  const std::unordered_set<VertexRef, VertexHash> frontier_set(
      request.frontier.begin(), request.frontier.end());
  for (const EdgeIdentity& identity : identities.ValueOrDie()) {
    if (options.check_abort) {
      if (Status status = options.check_abort(); !status.ok()) return status;
    }
    if (request.edge_type.has_value() &&
        identity.edge_type != *request.edge_type) continue;
    const bool out = Contains(identity.source_ref(), frontier_set);
    const bool in = Contains(identity.target_ref(), frontier_set);
    if ((request.direction == ExpandDirection::kOut && !out) ||
        (request.direction == ExpandDirection::kIn && !in) ||
        (request.direction == ExpandDirection::kBoth && !out && !in)) {
      continue;
    }
    auto edge_intervals = VisibleIntervals(
        snapshot, EntityFact::Edge(identity.edge_ref()).ref(), options.delta,
        options.check_abort);
    if (!edge_intervals.ok()) return edge_intervals.status();
    auto source_intervals = VisibleIntervals(
        snapshot, EntityFact::Vertex(identity.source_ref()).ref(), options.delta,
        options.check_abort);
    if (!source_intervals.ok()) return source_intervals.status();
    auto target_intervals = VisibleIntervals(
        snapshot, EntityFact::Vertex(identity.target_ref()).ref(), options.delta,
        options.check_abort);
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
  std::vector<GraphLabel> frontier;
  frontier.reserve(request.frontier.size());
  for (const VertexRef& vertex : request.frontier) {
    visited.emplace(VertexKey{vertex}, std::make_pair(0, std::vector<ValidTimeInterval>{}));
    frontier.push_back(GraphLabel{vertex, 0, std::nullopt, request.interval});
  }
  for (uint32_t depth = 1; depth <= options.max_hops && !frontier.empty(); ++depth) {
    auto interval_end = [](const ValidTimeInterval& interval) {
      return interval.to.value_or(ValidTime{std::numeric_limits<uint64_t>::max()}).value;
    };
    auto subtract_covered = [&](const ValidTimeInterval& current,
                                const std::vector<ValidTimeInterval>& prior) {
      std::vector<ValidTimeInterval> pieces{current};
      for (const auto& cover : prior) {
        std::vector<ValidTimeInterval> remaining;
        for (const auto& piece : pieces) {
          const auto overlap = Intersect(piece, cover);
          if (!overlap) {
            remaining.push_back(piece);
            continue;
          }
          if (piece.from.value < overlap->from.value)
            remaining.push_back({piece.from, overlap->from});
          const auto overlap_to = interval_end(*overlap);
          const auto piece_to = interval_end(piece);
          if (overlap_to < piece_to) {
            remaining.push_back({ValidTime{overlap_to}, piece.to});
          }
        }
        pieces = std::move(remaining);
        if (pieces.empty()) break;
      }
      return pieces;
    };
    auto merge_interval = [&](std::vector<ValidTimeInterval>* intervals,
                              const ValidTimeInterval& added) {
      intervals->push_back(added);
      std::sort(intervals->begin(), intervals->end(),
                [](const auto& a, const auto& b) { return a.from.value < b.from.value; });
      std::vector<ValidTimeInterval> merged;
      for (const auto& interval : *intervals) {
        if (merged.empty() || interval.from.value > interval_end(merged.back())) {
          merged.push_back(interval);
          continue;
        }
        auto& last = merged.back();
        if (!last.to.has_value() || !interval.to.has_value() ||
            interval_end(interval) > interval_end(last)) {
          last.to = interval.to;
        }
      }
      *intervals = std::move(merged);
    };
    std::vector<GraphLabel> next;
    for (const GraphLabel& prior : frontier) {
      if (options.check_abort) {
        if (Status status = options.check_abort(); !status.ok()) return status;
      }
      GraphExpansionRequest layer = request;
      layer.frontier = {prior.vertex};
      // Every hop is constrained by the common interval carried by its
      // predecessor label.  Reusing request.interval here would admit time
      // periods that were not present on the preceding path.
      if (prior.effective.has_value()) layer.interval = *prior.effective;
      auto expanded = ExpandTemporal(snapshot, layer, options);
      if (!expanded.ok()) return expanded.status();
      for (const TemporalTraversal& traversal : expanded.ValueOrDie()) {
        if (options.check_abort) {
          if (Status status = options.check_abort(); !status.ok()) return status;
        }
        if (options.trail &&
            std::find(prior.trail_edges.begin(), prior.trail_edges.end(),
                      traversal.edge) != prior.trail_edges.end()) {
          continue;
        }
        std::vector<std::pair<VertexRef, VertexRef>> moves;
        const bool out = traversal.source == prior.vertex;
        const bool in = traversal.target == prior.vertex;
        if ((request.direction == ExpandDirection::kOut && out) ||
            (request.direction == ExpandDirection::kBoth && out))
          moves.emplace_back(traversal.target, traversal.source);
        if ((request.direction == ExpandDirection::kIn && in) ||
            (request.direction == ExpandDirection::kBoth && in)) {
          if (moves.empty() || moves.front().first != traversal.source)
            moves.emplace_back(traversal.source, traversal.target);
        }
        for (const auto& move : moves) {
          const VertexRef destination = move.first;
          const VertexRef predecessor = move.second;
          auto found = visited.find(VertexKey{destination});
          if (found != visited.end() && found->second.first < depth) continue;
          std::vector<ValidTimeInterval> pieces;
          if (found == visited.end()) {
            found = visited.emplace(
                VertexKey{destination},
                std::make_pair(depth, std::vector<ValidTimeInterval>{}))
                        .first;
            if (options.reservation != nullptr) {
              if (Status status = options.reservation->ReserveVisitedVertices(1);
                  !status.ok())
                return status;
            }
            pieces.push_back(traversal.effective);
          } else if (found->second.first == depth) {
            pieces = subtract_covered(traversal.effective,
                                       found->second.second);
          }
          if (pieces.empty()) continue;
          merge_interval(&found->second.second, traversal.effective);
          for (const auto& piece : pieces) {
            TemporalTraversal emitted = traversal;
            emitted.effective = piece;
            result.traversals.push_back(emitted);
            result.labels.push_back(GraphLabel{
                destination, depth, std::optional<VertexRef>{predecessor}, piece,
                [&prior, &traversal] {
                  std::vector<EdgeRef> edges = prior.trail_edges;
                  edges.push_back(traversal.edge);
                  return edges;
                }()});
            next.push_back(GraphLabel{
                destination, depth, std::optional<VertexRef>{predecessor}, piece,
                [&prior, &traversal] {
                  std::vector<EdgeRef> edges = prior.trail_edges;
                  edges.push_back(traversal.edge);
                  return edges;
                }()});
          }
        }
      }
    }
    frontier = std::move(next);
  }
  return result;
}

namespace {

bool IntervalContains(const ValidTimeInterval& outer,
                      const ValidTimeInterval& inner) {
  if (outer.from.value > inner.from.value) return false;
  if (!outer.to.has_value()) return true;
  if (!inner.to.has_value()) return false;
  return outer.to->value >= inner.to->value;
}

bool EdgeRefLess(const EdgeRef& left, const EdgeRef& right) {
  if (left.home_part_id.value != right.home_part_id.value)
    return left.home_part_id.value < right.home_part_id.value;
  return left.edge_id.value < right.edge_id.value;
}

std::vector<EdgeRef> LabelEdges(const std::vector<CoexistingLabel>& labels,
                                uint64_t id) {
  std::vector<EdgeRef> edges;
  while (id != std::numeric_limits<uint64_t>::max() && id < labels.size()) {
    const CoexistingLabel& label = labels[id];
    if (label.depth != 0) edges.push_back(label.incoming_edge);
    id = label.predecessor_label;
  }
  std::reverse(edges.begin(), edges.end());
  return edges;
}

bool LabelPathLess(const std::vector<CoexistingLabel>& labels, uint64_t left,
                  uint64_t right) {
  const std::vector<EdgeRef> a = LabelEdges(labels, left);
  const std::vector<EdgeRef> b = LabelEdges(labels, right);
  return std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end(),
                                       EdgeRefLess);
}

bool IntervalOverlaps(const ValidTimeInterval& left,
                      const ValidTimeInterval& right) {
  const uint64_t left_to = left.to ? left.to->value
                                   : std::numeric_limits<uint64_t>::max();
  const uint64_t right_to = right.to ? right.to->value
                                     : std::numeric_limits<uint64_t>::max();
  return left.from.value < right_to && right.from.value < left_to;
}

uint64_t IntervalLength(const ValidTimeInterval& interval) {
  if (!interval.to) return std::numeric_limits<uint64_t>::max();
  return interval.to->value - interval.from.value;
}

PathValue ReconstructPath(const std::vector<CoexistingLabel>& labels,
                          uint64_t id) {
  PathValue path;
  if (id >= labels.size()) return path;
  path.common = labels[id].common;
  std::vector<uint64_t> chain;
  for (uint64_t current = id; current != std::numeric_limits<uint64_t>::max() &&
                               current < labels.size();
       current = labels[current].predecessor_label) {
    chain.push_back(current);
  }
  std::reverse(chain.begin(), chain.end());
  path.vertices.reserve(chain.size());
  for (uint64_t label_id : chain) {
    path.vertices.push_back(labels[label_id].vertex);
    if (labels[label_id].depth != 0)
      path.edges.push_back(labels[label_id].incoming_edge);
  }
  return path;
}

Status ChargeCoexistingLabel(const GraphFrontierOptions& options) {
  if (options.check_abort) {
    if (Status status = options.check_abort(); !status.ok()) return status;
  }
  if (options.reservation == nullptr) return Status::OK();
  // Preflight both dimensions so a failed second reservation cannot leave a
  // partially charged label.
  const auto labels = ResourceDimension::kGraphLabels;
  const auto fragments = ResourceDimension::kIntervalFragments;
  if (options.reservation->used(labels) >= options.reservation->limit(labels) ||
      options.reservation->used(fragments) >= options.reservation->limit(fragments)) {
    return Status::ResourceExhausted("query", "coexisting label budget exhausted");
  }
  if (Status status = options.reservation->ReserveGraphLabels(1);
      !status.ok())
    return status;
  return options.reservation->ReserveIntervalFragments(1);
}

}  // namespace

StatusOr<CoexistingPathResult> CoexistingShortestPath(
    Snapshot& snapshot, const GraphExpansionRequest& request,
    const VertexRef& target, const GraphFrontierOptions& options) {
  const auto interval = RequestInterval(request.interval);
  if (!interval.has_value()) {
    return Status::InvalidArgument("coexisting shortest path",
                                   "invalid request interval");
  }
  CoexistingPathResult result;
  if (options.max_hops == 0 || request.frontier.empty()) return result;

  // Keep labels grouped by vertex.  Entries are ids into result.labels, so
  // dominance never invalidates predecessor references.
  std::map<VertexKey, std::vector<uint64_t>> by_vertex;
  std::vector<uint64_t> frontier;
  std::vector<bool> active;
  for (const VertexRef& source : request.frontier) {
    const uint64_t id = result.labels.size();
    if (Status status = ChargeCoexistingLabel(options); !status.ok())
      return status;
    result.labels.push_back(
        CoexistingLabel{source, *interval, 0,
                        std::numeric_limits<uint64_t>::max(), EdgeRef{}});
    active.push_back(true);
    by_vertex[VertexKey{source}].push_back(id);
    frontier.push_back(id);
  }

  std::optional<uint32_t> target_depth;
  std::vector<uint64_t> target_labels;
  GraphFrontierOptions expansion_options = options;
  // ExpandTemporal charges raw traversals. Coexisting execution charges only
  // labels that survive interval dominance, as required by QueryReservation.
  expansion_options.reservation = nullptr;
  for (uint32_t depth = 1;
       depth <= options.max_hops && !frontier.empty(); ++depth) {
    if (target_depth.has_value() && depth > *target_depth) break;
    std::vector<uint64_t> next;
    for (uint64_t prior_id : frontier) {
      if (prior_id >= active.size() || !active[prior_id]) continue;
      if (options.check_abort) {
        if (Status status = options.check_abort(); !status.ok()) return status;
      }
      // Copy the small label: accepted candidates append to result.labels and
      // may reallocate the backing vector while this expansion is in flight.
      const CoexistingLabel prior = result.labels[prior_id];
      GraphExpansionRequest layer = request;
      layer.frontier = {prior.vertex};
      layer.interval = prior.common;
      auto expanded = ExpandTemporal(snapshot, layer, expansion_options);
      if (!expanded.ok()) return expanded.status();
      for (const TemporalTraversal& traversal : expanded.ValueOrDie()) {
        if (options.check_abort) {
          if (Status status = options.check_abort(); !status.ok()) return status;
        }
        VertexRef destination;
        if (request.direction == ExpandDirection::kOut) {
          if (traversal.source != prior.vertex) continue;
          destination = traversal.target;
        } else if (request.direction == ExpandDirection::kIn) {
          if (traversal.target != prior.vertex) continue;
          destination = traversal.source;
        } else {
          if (traversal.source == prior.vertex) destination = traversal.target;
          else if (traversal.target == prior.vertex) destination = traversal.source;
          else continue;
        }
        const ValidTimeInterval common = traversal.effective;
        auto& existing = by_vertex[VertexKey{destination}];
        const bool target_candidate = destination == target;
        if (!target_candidate) {
          bool dominated = false;
          for (uint64_t existing_id : existing) {
            const CoexistingLabel& label = result.labels[existing_id];
            if (label.depth <= depth && IntervalContains(label.common, common)) {
              dominated = true;
              break;
            }
          }
          if (dominated) continue;
          existing.erase(std::remove_if(existing.begin(), existing.end(),
                                        [&](uint64_t existing_id) {
            const CoexistingLabel& label = result.labels[existing_id];
            const bool remove = depth <= label.depth &&
                                IntervalContains(common, label.common);
            if (remove && existing_id < active.size()) active[existing_id] = false;
            return remove;
          }), existing.end());
        }
        if (Status status = ChargeCoexistingLabel(options); !status.ok())
          return status;
        const uint64_t id = result.labels.size();
        result.labels.push_back(CoexistingLabel{destination, common, depth,
                                                prior_id, traversal.edge});
        active.push_back(true);
        existing.push_back(id);
        if (destination == target) {
          if (!target_depth.has_value()) target_depth = depth;
          if (depth == *target_depth) {
            target_labels.push_back(id);
          }
        }
        if (!target_depth.has_value() || depth < *target_depth)
          next.push_back(id);
      }
    }
    frontier = std::move(next);
  }

  // Select winners globally. A candidate rejected by an earlier winner may
  // become independent after that winner is replaced, so rejected candidates
  // are re-evaluated whenever the winner set changes. This avoids making the
  // result depend on the order of a chain of overlapping intervals.
  std::sort(target_labels.begin(), target_labels.end(), [&](uint64_t left, uint64_t right) {
    if (result.labels[left].common.from.value != result.labels[right].common.from.value)
      return result.labels[left].common.from.value < result.labels[right].common.from.value;
    return left < right;
  });
  auto better = [&](uint64_t left, uint64_t right) {
    const uint64_t left_length = IntervalLength(result.labels[left].common);
    const uint64_t right_length = IntervalLength(result.labels[right].common);
    if (left_length != right_length) return left_length > right_length;
    if (result.labels[left].depth != result.labels[right].depth)
      return result.labels[left].depth < result.labels[right].depth;
    if (LabelPathLess(result.labels, left, right)) return true;
    if (LabelPathLess(result.labels, right, left)) return false;
    return left < right;
  };
  std::deque<uint64_t> pending(target_labels.begin(), target_labels.end());
  std::set<uint64_t> rejected;
  std::vector<uint64_t> winners;
  while (!pending.empty()) {
    const uint64_t candidate = pending.front();
    pending.pop_front();
    if (std::find(winners.begin(), winners.end(), candidate) != winners.end()) continue;
    std::vector<uint64_t> overlapping;
    for (uint64_t winner : winners) {
      if (IntervalOverlaps(result.labels[winner].common,
                           result.labels[candidate].common)) {
        overlapping.push_back(winner);
      }
    }
    bool beats_all = true;
    for (uint64_t winner : overlapping) {
      if (!better(candidate, winner)) {
        beats_all = false;
        break;
      }
    }
    if (!beats_all) {
      rejected.insert(candidate);
      continue;
    }
    for (uint64_t winner : overlapping) {
      winners.erase(std::remove(winners.begin(), winners.end(), winner), winners.end());
      pending.push_back(winner);
    }
    winners.push_back(candidate);
    // Any rejected candidate may have been blocked only by a winner removed
    // above. Requeue all of them after every winner-set change.
    for (uint64_t id : rejected) pending.push_back(id);
    rejected.clear();
  }
  std::sort(winners.begin(), winners.end(), [&](uint64_t left, uint64_t right) {
    if (result.labels[left].common.from.value != result.labels[right].common.from.value)
      return result.labels[left].common.from.value < result.labels[right].common.from.value;
    return left < right;
  });
  for (uint64_t id : winners) result.paths.push_back(ReconstructPath(result.labels, id));
  return result;
}

}  // namespace cedar::internal
