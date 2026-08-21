// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_QUERY_RUNTIME_GRAPH_FRONTIER_H_
#define CEDAR_QUERY_RUNTIME_GRAPH_FRONTIER_H_

#include <cstdint>
#include <optional>
#include <vector>
#include <functional>

#include "cedar/query/query.h"
#include "cedar/query/types.h"
#include "cedar/snapshot.h"
#include "query/projection/query_delta.h"
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

struct GraphFrontierOptions {
  QueryReservation* reservation = nullptr;
  const QueryDeltaView* delta = nullptr;
  uint32_t max_hops = 1;
  // Projection-backed adjacency seek. When present, canonical identity
  // scanning is bypassed and the callback is responsible for returning only
  // postings for the requested frontier.
  std::function<StatusOr<std::vector<EdgeIdentity>>(
      const std::vector<VertexRef>&, ExpandDirection,
      std::optional<uint64_t>)> adjacency_seek;
  uint64_t* candidates_examined = nullptr;
};

struct GraphLabel {
  VertexRef vertex;
  uint32_t depth = 0;
  std::optional<VertexRef> predecessor;
  std::optional<ValidTimeInterval> effective;

  bool operator==(const GraphLabel&) const = default;
};

struct KHopResult {
  std::vector<GraphLabel> labels;
  std::vector<TemporalTraversal> traversals;
};

StatusOr<std::vector<TemporalTraversal>> ExpandTemporal(
    Snapshot& snapshot, const GraphExpansionRequest& request,
    const GraphFrontierOptions& options = {});

StatusOr<KHopResult> KHopExpand(
    Snapshot& snapshot, const GraphExpansionRequest& request,
    const GraphFrontierOptions& options = {});

}  // namespace cedar::internal

#endif  // CEDAR_QUERY_RUNTIME_GRAPH_FRONTIER_H_
