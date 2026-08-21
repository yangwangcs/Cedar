#ifndef CEDAR_QUERY_RUNTIME_JOURNEY_H_
#define CEDAR_QUERY_RUNTIME_JOURNEY_H_

#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include "cedar/query/result.h"
#include "cedar/query/types.h"
#include "cedar/snapshot.h"
#include "query/runtime/graph_frontier.h"
#include "query/resource/query_resource_pool.h"

namespace cedar::internal {

enum class JourneyObjective : uint8_t { kEarliestArrival, kLatestDeparture,
                                        kFastestDuration };

struct JourneyRequest {
  VertexRef source;
  VertexRef target;
  ValidTimeInterval interval;
  JourneyObjective objective = JourneyObjective::kEarliestArrival;
  std::optional<PropertyId> duration_property;
  std::function<StatusOr<std::optional<ValidDuration>>(EdgeRef, ValidTime)>
      duration_at;
  std::optional<ValidTime> arrival_deadline;
  uint32_t max_hops = 0;
  ExpandDirection direction = ExpandDirection::kOut;
  std::optional<uint64_t> edge_type;
};

struct JourneyOptions {
  QueryReservation* reservation = nullptr;
  const QueryDeltaView* delta = nullptr;
  std::shared_ptr<const AdjacencyIndex> adjacency_index;
  std::optional<uint64_t> projection_generation;
  std::function<Status()> check_abort;
  uint64_t max_labels = 0;
};

struct JourneyTraversal {
  TemporalTraversal traversal;
  ValidTime departure;
  ValidTime arrival;
  ValidDuration duration;
};

StatusOr<ValidTime> AddDuration(ValidTime departure, ValidDuration duration);
bool TraversalFits(const ValidTimeInterval& effective, ValidTime departure,
                   ValidDuration duration);

StatusOr<JourneyValue> EarliestArrival(Snapshot& snapshot,
                                       const JourneyRequest& request,
                                       const JourneyOptions& options = {});
StatusOr<JourneyValue> LatestDeparture(Snapshot& snapshot,
                                        const JourneyRequest& request,
                                        const JourneyOptions& options = {});
StatusOr<JourneyValue> FastestDuration(Snapshot& snapshot,
                                       const JourneyRequest& request,
                                       const JourneyOptions& options = {});

StatusOr<JourneyValue> FindJourney(Snapshot& snapshot,
                                   const JourneyRequest& request,
                                   const JourneyOptions& options = {});

}  // namespace cedar::internal

#endif
