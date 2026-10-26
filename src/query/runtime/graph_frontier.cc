// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/runtime/graph_frontier.h"

#include <algorithm>
#include <array>
#include <limits>
#include <map>
#include <tuple>

#include "query/temporal/corrected_chain.h"
#include "query/temporal/interval.h"
#include "query/runtime/fact_history.h"
#include "query/resource/query_read_memory.h"

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

Status InsertIdentity(std::map<IdentityKey, EdgeIdentity>* unique,
                      const EdgeIdentity& identity, FactReadMemoryScope* memory) {
  const IdentityKey key{identity.edge_ref(), identity.source_ref(),
                         identity.target_ref(), identity.edge_type};
  if (unique->find(key) != unique->end()) return Status::OK();
  const Status reserved = memory->Reserve(
      sizeof(std::pair<const IdentityKey, EdgeIdentity>) + 4 * sizeof(void*));
  if (!reserved.ok()) return reserved;
  unique->emplace(key, identity);
  return Status::OK();
}

StatusOr<BudgetedRows<EdgeIdentity>> Identities(Snapshot& snapshot,
                                             const QueryDeltaView* delta,
                                             uint64_t max_candidates,
                                             const std::function<Status()>& check_abort,
                                             const PartScope& part_scope,
                                             std::shared_ptr<FactReadMemory> memory) {
  FactReadMemoryScope nodes(memory.get());
  FactReadMemoryScope output(memory.get());
  std::map<IdentityKey, EdgeIdentity> unique;
  FactReadSpec spec;
  spec.part_scope = part_scope;
  spec.family = FactFamily::kEdgeIdentity;
  spec.commit_seq_max = snapshot.commit_seq();
  spec.read_memory = memory;
  Status status = snapshot.canonical_reader().ReadEvents(
      spec, [&unique, &nodes, max_candidates, &check_abort](const FactEventBatch& batch) {
    for (const FactEvent& event : batch.events) {
      if (check_abort) {
        if (Status status = check_abort(); !status.ok()) return status;
      }
      if (!event.edge_identity.has_value()) continue;
      const EdgeIdentity& identity = *event.edge_identity;
      if (Status status = InsertIdentity(&unique, identity, &nodes); !status.ok())
        return status;
      if (max_candidates != 0 && unique.size() > max_candidates) {
        return Status::ResourceExhausted("adjacency", "bounded fallback candidate limit exceeded");
      }
    }
    return Status::OK();
  });
  if (!status.ok()) return status;
  if (delta != nullptr) {
    const auto append = [&](CommitSeq seq, const EdgeIdentity& identity) -> Status {
      if (seq.value > snapshot.commit_seq().value) return Status::OK();
      if (check_abort) {
        if (Status status = check_abort(); !status.ok()) return status;
      }
      if (Status status = InsertIdentity(&unique, identity, &nodes); !status.ok())
        return status;
      if (max_candidates && unique.size() > max_candidates)
        return Status::ResourceExhausted("adjacency", "bounded fallback candidate limit exceeded");
      return Status::OK();
    };
    if (delta->edge_identity_records.empty()) {
      for (const auto& identity : delta->edge_identities) {
        if (Status status = append(delta->through, identity); !status.ok()) return status;
      }
    } else {
      for (const auto& [seq, identity] : delta->edge_identity_records) {
        if (Status status = append(seq, identity); !status.ok()) return status;
      }
    }
  }
  std::vector<EdgeIdentity> result;
  if (Status status = output.ReserveElements(unique.size(), sizeof(EdgeIdentity));
      !status.ok()) return status;
  result.reserve(unique.size());
  for (const auto& entry : unique) result.push_back(entry.second);
  return RetainRows(std::move(result), &output, std::move(memory));
}

StatusOr<BudgetedRows<VertexRef>> SortedFrontier(
    const std::vector<VertexRef>& frontier, std::shared_ptr<FactReadMemory> memory) {
  FactReadMemoryScope working(memory.get());
  if (Status status = working.ReserveElements(frontier.size(), sizeof(VertexRef));
      !status.ok()) return status;
  std::vector<VertexRef> sorted;
  sorted.reserve(frontier.size());
  sorted.assign(frontier.begin(), frontier.end());
  std::sort(sorted.begin(), sorted.end(), [](const VertexRef& a, const VertexRef& b) {
    return VertexKey{a} < VertexKey{b};
  });
  return RetainRows(std::move(sorted), &working, std::move(memory));
}

bool Contains(const VertexRef& ref, const BudgetedRows<VertexRef>& frontier) {
  return std::binary_search(frontier.begin(), frontier.end(), ref,
      [](const VertexRef& a, const VertexRef& b) { return VertexKey{a} < VertexKey{b}; });
}

Status ChargeTraversal(const GraphFrontierOptions& options) {
  if (options.check_abort) {
    if (Status status = options.check_abort(); !status.ok()) return status;
  }
  if (options.reservation == nullptr || !options.count_expansion_resources)
    return Status::OK();
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

StatusOr<BudgetedRows<EdgeIdentity>> AdjacencyIndex::Seek(
    const std::vector<VertexRef>& frontier, ExpandDirection direction,
    std::optional<uint64_t> edge_type, CommitSeq snapshot_seq,
    std::optional<uint64_t> generation, const QueryDeltaView* delta,
    const std::function<Status()>& check_abort,
    std::shared_ptr<FactReadMemory> memory) const {
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
  FactReadMemoryScope nodes(memory.get());
  FactReadMemoryScope output(memory.get());
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
          if (Status status = InsertIdentity(&unique, identity, &nodes); !status.ok())
            return status;
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
      if (Status status = InsertIdentity(&unique, identity, &nodes); !status.ok())
        return status;
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
    auto sorted = SortedFrontier(frontier, memory);
    if (!sorted.ok()) return sorted.status();
    auto add_delta = [&](CommitSeq seq, const EdgeIdentity& identity) -> Status {
      if (seq.value > snapshot_seq.value) return Status::OK();
      const bool out = Contains(identity.source_ref(), sorted.ValueOrDie());
      const bool in = Contains(identity.target_ref(), sorted.ValueOrDie());
      if ((direction == ExpandDirection::kOut && !out) ||
          (direction == ExpandDirection::kIn && !in) ||
          (direction == ExpandDirection::kBoth && !out && !in)) return Status::OK();
      if (edge_type.has_value() && identity.edge_type != *edge_type) return Status::OK();
      return InsertIdentity(&unique, identity, &nodes);
    };
    if (delta->edge_identity_records.empty()) {
      for (const EdgeIdentity& identity : delta->edge_identities) {
        if (check_abort) {
          if (Status status = check_abort(); !status.ok()) return status;
        }
        if (Status status = add_delta(delta->through, identity); !status.ok()) return status;
      }
    } else {
      for (const auto& record : delta->edge_identity_records) {
        if (check_abort) {
          if (Status status = check_abort(); !status.ok()) return status;
        }
        if (Status status = add_delta(record.first, record.second); !status.ok()) return status;
      }
    }
  }
  std::vector<EdgeIdentity> result;
  if (Status status = output.ReserveElements(unique.size(), sizeof(EdgeIdentity));
      !status.ok()) return status;
  result.reserve(unique.size());
  for (const auto& item : unique) result.push_back(item.second);
  return RetainRows(std::move(result), &output, std::move(memory));
}

StatusOr<BudgetedRows<TemporalTraversal>> ExpandTemporal(
    Snapshot& snapshot, const GraphExpansionRequest& request,
    const GraphFrontierOptions& options) {
  const auto interval = RequestInterval(request.interval);
  if (!interval.has_value()) {
    return Status::InvalidArgument("graph expansion", "invalid request interval");
  }
  auto memory = options.reservation
      ? std::make_shared<QueryReadMemory>(*options.reservation) : nullptr;
  FactReadMemoryScope output_memory(memory.get());
  StatusOr<BudgetedRows<EdgeIdentity>> identities =
      options.adjacency_index
          ? options.adjacency_index->Seek(request.frontier, request.direction,
                                         request.edge_type, snapshot.commit_seq(),
                                         options.projection_generation,
                                         options.delta,
                                         options.check_abort, memory)
          : options.adjacency_seek
          ? [&]() -> StatusOr<BudgetedRows<EdgeIdentity>> {
              FactReadMemoryScope candidates(memory.get());
              std::vector<EdgeIdentity> rows;
              const Status status = options.adjacency_seek(
                  request.frontier, request.direction, request.edge_type,
                  [&](const EdgeIdentity& identity) {
                    if (options.check_abort) {
                      if (Status status = options.check_abort(); !status.ok()) return status;
                    }
                    if (options.fallback_candidate_limit &&
                        rows.size() >= options.fallback_candidate_limit)
                      return Status::ResourceExhausted("adjacency", "bounded fallback candidate limit exceeded");
                    const Status reserved = ReserveFactAppend(&rows, &candidates);
                    if (!reserved.ok()) return reserved;
                    rows.push_back(identity);
                    return Status::OK();
                  });
              if (!status.ok()) return status;
              return RetainRows(std::move(rows), &candidates, memory);
            }()
      : Identities(snapshot, options.delta, options.fallback_candidate_limit,
                   options.check_abort, options.part_scope, memory);
  if (!identities.ok() && identities.status().IsNotFound()) {
    // Cache misses are explicit: the canonical lane is only used when the
    // caller supplied a finite fallback bound (or left it unlimited for an
    // analytical query).
    identities = Identities(snapshot, options.delta,
                            options.fallback_candidate_limit,
                            options.check_abort, options.part_scope, memory);
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
  auto sorted = SortedFrontier(request.frontier, memory);
  if (!sorted.ok()) return sorted.status();
  for (const EdgeIdentity& identity : identities.ValueOrDie()) {
    if (options.check_abort) {
      if (Status status = options.check_abort(); !status.ok()) return status;
    }
    if (request.edge_type.has_value() &&
        identity.edge_type != *request.edge_type) continue;
    const bool out = Contains(identity.source_ref(), sorted.ValueOrDie());
    const bool in = Contains(identity.target_ref(), sorted.ValueOrDie());
    if ((request.direction == ExpandDirection::kOut && !out) ||
        (request.direction == ExpandDirection::kIn && !in) ||
        (request.direction == ExpandDirection::kBoth && !out && !in)) {
      continue;
    }
    auto edge_intervals = ReadPresentIntervals(
        snapshot, EntityFact::Edge(identity.edge_ref()).ref(), options.delta,
        options.check_abort, options.reservation);
    if (!edge_intervals.ok()) return edge_intervals.status();
    auto source_intervals = ReadPresentIntervals(
        snapshot, EntityFact::Vertex(identity.source_ref()).ref(), options.delta,
        options.check_abort, options.reservation);
    if (!source_intervals.ok()) return source_intervals.status();
    auto target_intervals = ReadPresentIntervals(
        snapshot, EntityFact::Vertex(identity.target_ref()).ref(), options.delta,
        options.check_abort, options.reservation);
    if (!target_intervals.ok()) return target_intervals.status();
    for (const StateInterval& edge : *edge_intervals.ValueOrDie()) {
      for (const StateInterval& source : *source_intervals.ValueOrDie()) {
        auto effective = Intersect(*interval, edge.interval);
        if (!effective) continue;
        effective = Intersect(*effective, source.interval);
        if (!effective) continue;
        for (const StateInterval& target : *target_intervals.ValueOrDie()) {
          auto clipped = Intersect(*effective, target.interval);
          if (!clipped) continue;
          if (Status status = ChargeTraversal(options); !status.ok()) return status;
          if (Status status = ReserveFactAppend(&output, &output_memory); !status.ok())
            return status;
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
  return RetainRows(std::move(output), &output_memory, std::move(memory));
}

StatusOr<KHopResult> KHopExpand(Snapshot& snapshot,
                                const GraphExpansionRequest& request,
                                const GraphFrontierOptions& options) {
  if (options.max_hops == 0) return KHopResult{};
  auto memory = options.reservation
      ? std::make_shared<QueryReadMemory>(*options.reservation) : nullptr;
  FactReadMemoryScope output_memory(memory.get());
  FactReadMemoryScope visited_memory(memory.get());
  FactReadMemoryScope initial_memory(memory.get());
  KHopResult result;
  std::map<VertexKey, std::pair<uint32_t, std::vector<ValidTimeInterval>>> visited;
  std::shared_ptr<const void> frontier_owner;
  std::vector<GraphLabel> frontier;
  if (Status status = initial_memory.ReserveElements(request.frontier.size(), sizeof(GraphLabel));
      !status.ok()) return status;
  frontier.reserve(request.frontier.size());
  for (const VertexRef& vertex : request.frontier) {
    if (visited.find(VertexKey{vertex}) == visited.end()) {
      if (Status status = visited_memory.Reserve(sizeof(decltype(visited)::value_type) +
                                                  4 * sizeof(void*)); !status.ok()) return status;
    }
    visited.emplace(VertexKey{vertex}, std::make_pair(0, std::vector<ValidTimeInterval>{}));
    frontier.push_back(GraphLabel{vertex, 0, std::nullopt, request.interval});
  }
  auto retained_frontier = initial_memory.Retain(memory);
  if (!retained_frontier.ok()) return retained_frontier.status();
  frontier_owner = retained_frontier.ConsumeValueOrDie();
  const auto append_label = [](std::vector<GraphLabel>* labels, FactReadMemoryScope* scope,
                                const GraphLabel& prior, const TemporalTraversal& traversal,
                                VertexRef destination, VertexRef predecessor,
                                uint32_t depth, ValidTimeInterval effective) -> Status {
    if (Status status = ReserveFactAppend(labels, scope); !status.ok()) return status;
    if (prior.trail_edges.size() == SIZE_MAX)
      return Status::ResourceExhausted("graph", "trail capacity overflow");
    if (Status status = scope->ReserveElements(prior.trail_edges.size() + 1, sizeof(EdgeRef));
        !status.ok()) return status;
    std::vector<EdgeRef> edges;
    edges.reserve(prior.trail_edges.size() + 1);
    edges.insert(edges.end(), prior.trail_edges.begin(), prior.trail_edges.end());
    edges.push_back(traversal.edge);
    labels->push_back(GraphLabel{destination, depth, predecessor, effective, std::move(edges)});
    return Status::OK();
  };
  for (uint64_t depth = 1; depth <= options.max_hops && !frontier.empty(); ++depth) {
    auto interval_end = [](const ValidTimeInterval& interval) {
      return interval.to.value_or(ValidTime{std::numeric_limits<uint64_t>::max()}).value;
    };
    auto subtract_covered = [&](const ValidTimeInterval& current,
                                const std::vector<ValidTimeInterval>& prior,
                                FactReadMemoryScope* scratch)
        -> StatusOr<std::vector<ValidTimeInterval>> {
      std::vector<ValidTimeInterval> pieces;
      if (Status status = ReserveFactAppend(&pieces, scratch); !status.ok()) return status;
      pieces.push_back(current);
      for (const auto& cover : prior) {
        std::vector<ValidTimeInterval> remaining;
        for (const auto& piece : pieces) {
          const auto overlap = Intersect(piece, cover);
          if (!overlap) {
            if (Status status = ReserveFactAppend(&remaining, scratch); !status.ok()) return status;
            remaining.push_back(piece);
            continue;
          }
          if (piece.from.value < overlap->from.value) {
            if (Status status = ReserveFactAppend(&remaining, scratch); !status.ok()) return status;
            remaining.push_back({piece.from, overlap->from});
          }
          const auto overlap_to = interval_end(*overlap);
          const auto piece_to = interval_end(piece);
          if (overlap_to < piece_to) {
            if (Status status = ReserveFactAppend(&remaining, scratch); !status.ok()) return status;
            remaining.push_back({ValidTime{overlap_to}, piece.to});
          }
        }
        const uint64_t released = pieces.capacity() * sizeof(ValidTimeInterval);
        pieces = std::move(remaining);
        scratch->Release(released);
        if (pieces.empty()) break;
      }
      return pieces;
    };
    auto merge_interval = [&](std::vector<ValidTimeInterval>* intervals,
                              const ValidTimeInterval& added) -> Status {
      if (Status status = ReserveFactAppend(intervals, &visited_memory); !status.ok()) return status;
      intervals->push_back(added);
      std::sort(intervals->begin(), intervals->end(),
                [](const auto& a, const auto& b) { return a.from.value < b.from.value; });
      size_t kept = 0;
      for (const auto& interval : *intervals) {
        if (kept == 0 || interval.from.value > interval_end((*intervals)[kept - 1])) {
          (*intervals)[kept++] = interval;
          continue;
        }
        auto& last = (*intervals)[kept - 1];
        if (!last.to.has_value() || !interval.to.has_value() ||
            interval_end(interval) > interval_end(last)) {
          last.to = interval.to;
        }
      }
      intervals->resize(kept);
      return Status::OK();
    };
    FactReadMemoryScope next_memory(memory.get());
    std::vector<GraphLabel> next;
    for (const GraphLabel& prior : frontier) {
      if (options.check_abort) {
        if (Status status = options.check_abort(); !status.ok()) return status;
      }
      FactReadMemoryScope layer_memory(memory.get());
      if (Status status = layer_memory.Reserve(sizeof(VertexRef)); !status.ok()) return status;
      GraphExpansionRequest layer{{prior.vertex}, request.interval,
                                   request.direction, request.edge_type};
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
        std::array<std::pair<VertexRef, VertexRef>, 2> moves;
        size_t move_count = 0;
        const bool out = traversal.source == prior.vertex;
        const bool in = traversal.target == prior.vertex;
        if ((request.direction == ExpandDirection::kOut && out) ||
            (request.direction == ExpandDirection::kBoth && out))
          moves[move_count++] = {traversal.target, traversal.source};
        if ((request.direction == ExpandDirection::kIn && in) ||
            (request.direction == ExpandDirection::kBoth && in)) {
          if (move_count == 0 || moves.front().first != traversal.source)
            moves[move_count++] = {traversal.source, traversal.target};
        }
        for (size_t move_index = 0; move_index < move_count; ++move_index) {
          const auto& move = moves[move_index];
          const VertexRef destination = move.first;
          const VertexRef predecessor = move.second;
          auto found = visited.find(VertexKey{destination});
          if (found != visited.end() && found->second.first < depth) continue;
          FactReadMemoryScope piece_memory(memory.get());
          std::vector<ValidTimeInterval> pieces;
          if (found == visited.end()) {
            if (Status status = visited_memory.Reserve(sizeof(decltype(visited)::value_type) +
                                                        4 * sizeof(void*)); !status.ok()) return status;
            found = visited.emplace(
                VertexKey{destination},
                std::make_pair(depth, std::vector<ValidTimeInterval>{}))
                        .first;
            if (options.reservation != nullptr && options.count_expansion_resources) {
              if (Status status = options.reservation->ReserveVisitedVertices(1);
                  !status.ok())
                return status;
            }
            if (Status status = ReserveFactAppend(&pieces, &piece_memory); !status.ok()) return status;
            pieces.push_back(traversal.effective);
          } else if (found->second.first == depth) {
            auto uncovered = subtract_covered(traversal.effective, found->second.second,
                                               &piece_memory);
            if (!uncovered.ok()) return uncovered.status();
            pieces = uncovered.ConsumeValueOrDie();
          }
          if (pieces.empty()) continue;
          if (Status status = merge_interval(&found->second.second, traversal.effective);
              !status.ok()) return status;
          for (const auto& piece : pieces) {
            TemporalTraversal emitted = traversal;
            emitted.effective = piece;
            if (Status status = ReserveFactAppend(&result.traversals, &output_memory);
                !status.ok()) return status;
            result.traversals.push_back(emitted);
            if (Status status = append_label(&result.labels, &output_memory, prior, traversal,
                                               destination, predecessor, depth, piece);
                !status.ok()) return status;
            if (Status status = append_label(&next, &next_memory, prior, traversal,
                                               destination, predecessor, depth, piece);
                !status.ok()) return status;
          }
        }
      }
    }
    auto next_owner = next_memory.Retain(memory);
    if (!next_owner.ok()) return next_owner.status();
    frontier = std::move(next);
    frontier_owner = next_owner.ConsumeValueOrDie();
  }
  auto output_owner = output_memory.Retain(memory);
  if (!output_owner.ok()) return output_owner.status();
  result.memory = output_owner.ConsumeValueOrDie();
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

void LabelEdges(const std::vector<CoexistingLabel>& labels, uint64_t id,
                std::vector<EdgeRef>* edges) {
  edges->clear();
  while (id != std::numeric_limits<uint64_t>::max() && id < labels.size()) {
    const CoexistingLabel& label = labels[id];
    if (label.depth != 0) edges->push_back(label.incoming_edge);
    id = label.predecessor_label;
  }
  std::reverse(edges->begin(), edges->end());
}

bool LabelPathLess(const std::vector<CoexistingLabel>& labels, uint64_t left,
                  uint64_t right, std::vector<EdgeRef>* a, std::vector<EdgeRef>* b) {
  LabelEdges(labels, left, a);
  LabelEdges(labels, right, b);
  return std::lexicographical_compare(a->begin(), a->end(), b->begin(), b->end(),
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

StatusOr<PathValue> ReconstructPath(const std::vector<CoexistingLabel>& labels,
                                    uint64_t id, FactReadMemoryScope* memory) {
  PathValue path;
  if (id >= labels.size()) return path;
  path.common = labels[id].common;
  const uint64_t depth = labels[id].depth;
  if (Status status = memory->ReserveElements(depth + 1, sizeof(VertexRef));
      !status.ok()) return status;
  if (Status status = memory->ReserveElements(depth, sizeof(EdgeRef));
      !status.ok()) return status;
  path.vertices.resize(depth + 1);
  path.edges.resize(depth);
  for (uint64_t position = depth;; --position) {
    path.vertices[position] = labels[id].vertex;
    if (position == 0) break;
    path.edges[position - 1] = labels[id].incoming_edge;
    id = labels[id].predecessor_label;
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
  auto memory = options.reservation
      ? std::make_shared<QueryReadMemory>(*options.reservation) : nullptr;
  FactReadMemoryScope output_memory(memory.get());
  FactReadMemoryScope working_memory(memory.get());
  CoexistingPathResult result;
  if (options.max_hops == 0 || request.frontier.empty()) return result;

  // Keep labels grouped by vertex.  Entries are ids into result.labels, so
  // dominance never invalidates predecessor references.
  std::map<VertexKey, std::vector<uint64_t>> by_vertex;
  std::shared_ptr<const void> frontier_owner;
  FactReadMemoryScope initial_memory(memory.get());
  std::vector<uint64_t> frontier;
  std::vector<uint8_t> active;
  const auto vertex_labels = [&](VertexRef vertex)
      -> StatusOr<std::vector<uint64_t>*> {
    const VertexKey key{vertex};
    auto found = by_vertex.find(key);
    if (found == by_vertex.end()) {
      if (Status status = working_memory.Reserve(
              sizeof(decltype(by_vertex)::value_type) + 4 * sizeof(void*));
          !status.ok()) return status;
      found = by_vertex.emplace(key, std::vector<uint64_t>{}).first;
    }
    return &found->second;
  };
  for (const VertexRef& source : request.frontier) {
    const uint64_t id = result.labels.size();
    if (Status status = ChargeCoexistingLabel(options); !status.ok())
      return status;
    auto existing = vertex_labels(source);
    if (!existing.ok()) return existing.status();
    if (Status status = ReserveFactAppend(&result.labels, &output_memory); !status.ok()) return status;
    if (Status status = ReserveFactAppend(&active, &working_memory); !status.ok()) return status;
    if (Status status = ReserveFactAppend(existing.ValueOrDie(), &working_memory); !status.ok()) return status;
    if (Status status = ReserveFactAppend(&frontier, &initial_memory); !status.ok()) return status;
    result.labels.push_back(
        CoexistingLabel{source, *interval, 0,
                        std::numeric_limits<uint64_t>::max(), EdgeRef{}});
    active.push_back(true);
    existing.ValueOrDie()->push_back(id);
    frontier.push_back(id);
  }
  auto initial_owner = initial_memory.Retain(memory);
  if (!initial_owner.ok()) return initial_owner.status();
  frontier_owner = initial_owner.ConsumeValueOrDie();

  std::optional<uint32_t> target_depth;
  std::vector<uint64_t> target_labels;
  GraphFrontierOptions expansion_options = options;
  // ExpandTemporal charges raw traversals. Coexisting execution charges only
  // labels that survive interval dominance, as required by QueryReservation.
  expansion_options.count_expansion_resources = false;
  for (uint64_t depth = 1;
       depth <= options.max_hops && !frontier.empty(); ++depth) {
    if (target_depth.has_value() && depth > *target_depth) break;
    FactReadMemoryScope next_memory(memory.get());
    std::vector<uint64_t> next;
    for (uint64_t prior_id : frontier) {
      if (prior_id >= active.size() || !active[prior_id]) continue;
      if (options.check_abort) {
        if (Status status = options.check_abort(); !status.ok()) return status;
      }
      // Copy the small label: accepted candidates append to result.labels and
      // may reallocate the backing vector while this expansion is in flight.
      const CoexistingLabel prior = result.labels[prior_id];
      FactReadMemoryScope layer_memory(memory.get());
      if (Status status = layer_memory.Reserve(sizeof(VertexRef)); !status.ok()) return status;
      GraphExpansionRequest layer{{prior.vertex}, prior.common,
                                   request.direction, request.edge_type};
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
        auto existing_labels = vertex_labels(destination);
        if (!existing_labels.ok()) return existing_labels.status();
        auto& existing = *existing_labels.ValueOrDie();
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
        if (Status status = ReserveFactAppend(&result.labels, &output_memory); !status.ok()) return status;
        if (Status status = ReserveFactAppend(&active, &working_memory); !status.ok()) return status;
        if (Status status = ReserveFactAppend(&existing, &working_memory); !status.ok()) return status;
        result.labels.push_back(CoexistingLabel{destination, common, static_cast<uint32_t>(depth),
                                                prior_id, traversal.edge});
        active.push_back(true);
        existing.push_back(id);
        if (destination == target) {
          if (!target_depth.has_value()) target_depth = depth;
          if (depth == *target_depth) {
            if (Status status = ReserveFactAppend(&target_labels, &working_memory); !status.ok()) return status;
            target_labels.push_back(id);
          }
        }
        if (!target_depth.has_value() || depth < *target_depth) {
          if (Status status = ReserveFactAppend(&next, &next_memory); !status.ok()) return status;
          next.push_back(id);
        }
      }
    }
    auto next_owner = next_memory.Retain(memory);
    if (!next_owner.ok()) return next_owner.status();
    frontier = std::move(next);
    frontier_owner = next_owner.ConsumeValueOrDie();
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
  uint64_t longest_path = 0;
  for (uint64_t id : target_labels)
    longest_path = std::max<uint64_t>(longest_path, result.labels[id].depth);
  if (Status status = working_memory.ReserveElements(longest_path, 2 * sizeof(EdgeRef));
      !status.ok()) return status;
  std::vector<EdgeRef> left_edges;
  std::vector<EdgeRef> right_edges;
  left_edges.reserve(longest_path);
  right_edges.reserve(longest_path);
  auto better = [&](uint64_t left, uint64_t right) {
    const uint64_t left_length = IntervalLength(result.labels[left].common);
    const uint64_t right_length = IntervalLength(result.labels[right].common);
    if (left_length != right_length) return left_length > right_length;
    if (result.labels[left].depth != result.labels[right].depth)
      return result.labels[left].depth < result.labels[right].depth;
    if (LabelPathLess(result.labels, left, right, &left_edges, &right_edges)) return true;
    if (LabelPathLess(result.labels, right, left, &left_edges, &right_edges)) return false;
    return left < right;
  };
  if (Status status = working_memory.ReserveElements(target_labels.size(), sizeof(uint64_t));
      !status.ok()) return status;
  std::vector<uint64_t> pending;
  pending.reserve(target_labels.size());
  pending.assign(target_labels.begin(), target_labels.end());
  size_t pending_head = 0;
  const auto enqueue = [&](uint64_t id) -> Status {
    if (pending_head > pending.size() / 2) {
      pending.erase(pending.begin(), pending.begin() + pending_head);
      pending_head = 0;
    }
    if (Status status = ReserveFactAppend(&pending, &working_memory); !status.ok()) return status;
    pending.push_back(id);
    return Status::OK();
  };
  std::vector<uint64_t> rejected;
  std::vector<uint64_t> winners;
  std::vector<uint64_t> overlapping;
  while (pending_head < pending.size()) {
    if (options.check_abort) {
      if (Status status = options.check_abort(); !status.ok()) return status;
    }
    const uint64_t candidate = pending[pending_head++];
    if (std::find(winners.begin(), winners.end(), candidate) != winners.end()) continue;
    overlapping.clear();
    for (uint64_t winner : winners) {
      if (IntervalOverlaps(result.labels[winner].common,
                           result.labels[candidate].common)) {
        if (Status status = ReserveFactAppend(&overlapping, &working_memory); !status.ok()) return status;
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
      const auto position = std::lower_bound(rejected.begin(), rejected.end(), candidate);
      if (position == rejected.end() || *position != candidate) {
        const size_t index = position - rejected.begin();
        if (Status status = ReserveFactAppend(&rejected, &working_memory); !status.ok()) return status;
        rejected.insert(rejected.begin() + index, candidate);
      }
      continue;
    }
    for (uint64_t winner : overlapping) {
      winners.erase(std::remove(winners.begin(), winners.end(), winner), winners.end());
      if (Status status = enqueue(winner); !status.ok()) return status;
    }
    if (Status status = ReserveFactAppend(&winners, &working_memory); !status.ok()) return status;
    winners.push_back(candidate);
    // Any rejected candidate may have been blocked only by a winner removed
    // above. Requeue all of them after every winner-set change.
    for (uint64_t id : rejected) {
      if (Status status = enqueue(id); !status.ok()) return status;
    }
    rejected.clear();
  }
  std::sort(winners.begin(), winners.end(), [&](uint64_t left, uint64_t right) {
    if (result.labels[left].common.from.value != result.labels[right].common.from.value)
      return result.labels[left].common.from.value < result.labels[right].common.from.value;
    return left < right;
  });
  for (uint64_t id : winners) {
    if (options.check_abort) {
      if (Status status = options.check_abort(); !status.ok()) return status;
    }
    if (Status status = ReserveFactAppend(&result.paths, &output_memory); !status.ok()) return status;
    auto path = ReconstructPath(result.labels, id, &output_memory);
    if (!path.ok()) return path.status();
    result.paths.push_back(path.ConsumeValueOrDie());
  }
  auto output_owner = output_memory.Retain(memory);
  if (!output_owner.ok()) return output_owner.status();
  result.memory = output_owner.ConsumeValueOrDie();
  return result;
}

}  // namespace cedar::internal
