// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_RUNTIME_GRAPH_FRONTIER_H_
#define CEDAR_QUERY_RUNTIME_GRAPH_FRONTIER_H_

#include <cstdint>
#include <limits>
#include <optional>
#include <vector>
#include <functional>
#include <memory>
#include <map>
#include <unordered_map>

#include "cedar/query/query.h"
#include "cedar/query/result.h"
#include "cedar/query/types.h"
#include "cedar/snapshot.h"
#include "query/projection/query_delta.h"
#include "query/read/read_catalog.h"
#include "query/resource/query_resource_pool.h"

namespace cedar::internal {

// A graph row is deliberately independent of the storage identity fact.  The
// endpoint and edge references are materialized from Cedar facts at one
// borrowed Snapshot and the effective interval is the intersection of every
// participating state interval.
struct TemporalTraversal {
  VertexRef source;
  EdgeRef edge;
  VertexRef target;
  uint64_t edge_type = 0;
  ValidTimeInterval effective;

  bool operator==(const TemporalTraversal&) const = default;
};

struct GraphExpansionRequest {
  std::vector<VertexRef> frontier;
  ValidTimeInterval interval;
  ExpandDirection direction = ExpandDirection::kOut;
  std::optional<uint64_t> edge_type;
};

// Cedar-owned, immutable-at-read adjacency postings.  The index contains no
// RocksDB handles: it is rebuilt from authoritative EdgeIdentity facts and
// can then be advanced with QueryDelta records.
class AdjacencyIndex {
 public:
  struct Entry {
    EdgeIdentity identity;
    CommitSeq commit_seq;
    uint64_t generation = 0;
  };

  Status Build(Snapshot& snapshot, uint64_t generation = 0);
  Status Build(const std::vector<FactEvent>& events, CommitSeq snapshot_seq,
               uint64_t generation = 0);
  Status ApplyDelta(const QueryDeltaView& delta, uint64_t generation = 0);
  StatusOr<std::vector<EdgeIdentity>> Seek(
      const std::vector<VertexRef>& frontier, ExpandDirection direction,
      std::optional<uint64_t> edge_type, CommitSeq snapshot_seq,
      std::optional<uint64_t> generation = std::nullopt,
      const QueryDeltaView* delta = nullptr,
      const std::function<Status()>& check_abort = {}) const;
  bool covers(CommitSeq snapshot_seq) const { return built_through_.value >= snapshot_seq.value; }
  CommitSeq built_through() const { return built_through_; }
  std::shared_ptr<const ReadCatalog> read_catalog() const { return read_catalog_; }

 private:
  struct Key {
    VertexRef vertex;
    ExpandDirection direction = ExpandDirection::kOut;
    std::optional<uint64_t> edge_type;
    bool operator==(const Key&) const = default;
    bool operator<(const Key& other) const;
  };
  struct KeyHash {
    size_t operator()(const Key& key) const noexcept {
      size_t hash = static_cast<size_t>(key.vertex.part_id.value) * 1315423911u;
      hash ^= std::hash<uint64_t>{}(key.vertex.vertex_id.value) +
              (hash << 6) + (hash >> 2);
      hash ^= static_cast<size_t>(key.direction) + (hash << 6) + (hash >> 2);
      if (key.edge_type.has_value()) {
        hash ^= std::hash<uint64_t>{}(*key.edge_type) +
                (hash << 6) + (hash >> 2);
      }
      return hash;
    }
  };
  std::unordered_map<Key, std::vector<Entry>, KeyHash> postings_;
  CommitSeq built_through_;
  uint64_t generation_ = 0;
  bool generation_complete_ = true;
  std::shared_ptr<const ReadCatalog> read_catalog_;
  void Add(const Entry& entry);
};

struct GraphFrontierOptions {
  QueryReservation* reservation = nullptr;
  const QueryDeltaView* delta = nullptr;
  uint32_t max_hops = 1;
  bool trail = false;
  // Projection-backed adjacency seek. When present, canonical identity
  // scanning is bypassed and the callback is responsible for returning only
  // postings for the requested frontier.
  std::function<StatusOr<std::vector<EdgeIdentity>>(
      const std::vector<VertexRef>&, ExpandDirection,
      std::optional<uint64_t>)> adjacency_seek;
  std::shared_ptr<const AdjacencyIndex> adjacency_index;
  std::optional<uint64_t> projection_generation;
  PartScope part_scope = PartScope::All();
  std::function<Status()> check_abort;
  // Zero means no fallback bound. A non-zero value makes a cache miss fail
  // explicitly once the authoritative fallback would exceed this count.
  uint64_t fallback_candidate_limit = 0;
  uint64_t* candidates_examined = nullptr;
};

struct GraphLabel {
  VertexRef vertex;
  uint32_t depth = 0;
  std::optional<VertexRef> predecessor;
  std::optional<ValidTimeInterval> effective;
  std::vector<EdgeRef> trail_edges;

  bool operator==(const GraphLabel&) const = default;
};

struct KHopResult {
  std::vector<GraphLabel> labels;
  std::vector<TemporalTraversal> traversals;
};

// A coexisting label keeps one common valid-time witness and a predecessor
// id.  Path vectors are deliberately absent from frontier state and are
// reconstructed only for selected targets.
struct CoexistingLabel {
  VertexRef vertex;
  ValidTimeInterval common;
  uint32_t depth = 0;
  uint64_t predecessor_label = std::numeric_limits<uint64_t>::max();
  EdgeRef incoming_edge;
};

struct CoexistingPathResult {
  std::vector<CoexistingLabel> labels;
  std::vector<PathValue> paths;
};

StatusOr<CoexistingPathResult> CoexistingShortestPath(
    Snapshot& snapshot, const GraphExpansionRequest& request,
    const VertexRef& target, const GraphFrontierOptions& options = {});

StatusOr<std::vector<TemporalTraversal>> ExpandTemporal(
    Snapshot& snapshot, const GraphExpansionRequest& request,
    const GraphFrontierOptions& options = {});

StatusOr<KHopResult> KHopExpand(
    Snapshot& snapshot, const GraphExpansionRequest& request,
    const GraphFrontierOptions& options = {});

}  // namespace cedar::internal

#endif  // CEDAR_QUERY_RUNTIME_GRAPH_FRONTIER_H_
