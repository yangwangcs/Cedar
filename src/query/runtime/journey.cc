#include "query/runtime/journey.h"

#include <algorithm>
#include <limits>
#include <map>
#include <queue>
#include <set>
#include <tuple>

#include "query/temporal/corrected_chain.h"

namespace cedar::internal {
namespace {

struct VertexLess {
  bool operator()(const VertexRef& a, const VertexRef& b) const {
    return std::tie(a.part_id.value, a.vertex_id.value) <
           std::tie(b.part_id.value, b.vertex_id.value);
  }
};
Status Check(const JourneyOptions& options) {
  if (options.check_abort) return options.check_abort();
  return Status::OK();
}

Status Charge(const JourneyOptions& options) {
  if (Status status = Check(options); !status.ok()) return status;
  if (!options.reservation) return Status::OK();
  return options.reservation->ReserveGraphLabels(1);
}

Status ValidateCallbackFifo(const JourneyRequest& request,
                            const TemporalTraversal& traversal) {
  if (!request.duration_at) return Status::OK();
  const uint64_t first = traversal.effective.from.value;
  uint64_t second = first;
  if (traversal.effective.to.has_value()) {
    if (traversal.effective.to->value <= first + 1) return Status::OK();
    second = first + 1;
  } else {
    return Status::NotSupported("journey", "FIFO cannot be proven for an unbounded duration callback");
  }
  auto left = request.duration_at(traversal.edge, ValidTime{first});
  if (!left.ok()) return left.status();
  auto right = request.duration_at(traversal.edge, ValidTime{second});
  if (!right.ok()) return right.status();
  if (!left.ValueOrDie() || !right.ValueOrDie()) return Status::OK();
  auto left_arrival = AddDuration(ValidTime{first}, *left.ValueOrDie());
  if (!left_arrival.ok()) return left_arrival.status();
  auto right_arrival = AddDuration(ValidTime{second}, *right.ValueOrDie());
  if (!right_arrival.ok()) return right_arrival.status();
  if (right_arrival.ValueOrDie().value < left_arrival.ValueOrDie().value)
    return Status::NotSupported("journey", "duration callback is non-FIFO");
  return Status::OK();
}

StatusOr<std::optional<ValidDuration>> PropertyDuration(
    Snapshot& snapshot, EdgeRef edge, ValidTime time,
    const JourneyRequest& request, const QueryDeltaView* delta) {
  if (request.duration_at) return request.duration_at(edge, time);
  if (!request.duration_property) return std::optional<ValidDuration>{};
  const FactRef ref = PropertyFact::Edge(edge, *request.duration_property).ref();
  std::vector<FactEvent> events;
  FactScanSpec spec;
  spec.part_id = ref.part_id();
  spec.family = ref.family();
  spec.property_id = ref.property_id();
  spec.entity_id_min = ref.entity_id();
  spec.entity_id_max = ref.entity_id();
  Status scanned = snapshot.EventScan(spec, [&events](const FactEventBatch& batch) {
    events.insert(events.end(), batch.events.begin(), batch.events.end());
    return Status::OK();
  });
  if (!scanned.ok()) return scanned;
  if (delta != nullptr) {
    auto tail = delta->EventsFor(ref);
    events.insert(events.end(), tail.begin(), tail.end());
  }
  auto corrected = ResolveCorrectedBoundaries(events, snapshot.commit_seq());
  if (!corrected.ok()) return corrected.status();
  auto intervals = MaterializePresentState(corrected.ValueOrDie());
  std::optional<Value> value;
  for (const auto& interval : intervals) {
    if (time.value < interval.interval.from.value) continue;
    if (interval.interval.to && time.value >= interval.interval.to->value) continue;
    value = interval.value;
    break;
  }
  if (!value) return std::optional<ValidDuration>{};
  const Value& raw = *value;
  uint64_t duration = 0;
  switch (raw.type()) {
    case PhysicalType::kInt32: {
      const int32_t v = std::get<int32_t>(raw.data());
      if (v < 0) return Status::InvalidArgument("journey", "duration is negative");
      duration = static_cast<uint64_t>(v);
      break;
    }
    case PhysicalType::kInt64: {
      const int64_t v = std::get<int64_t>(raw.data());
      if (v < 0) return Status::InvalidArgument("journey", "duration is negative");
      duration = static_cast<uint64_t>(v);
      break;
    }
    case PhysicalType::kTimestamp64:
      duration = std::get<uint64_t>(raw.data());
      break;
    default:
      return Status::SchemaMismatch("journey", "duration must be numeric");
  }
  return std::optional<ValidDuration>{ValidDuration{duration}};
}

StatusOr<std::vector<JourneyTraversal>> ExpandAt(
    Snapshot& snapshot, const JourneyRequest& request, VertexRef vertex,
    ValidTime arrival, const JourneyOptions& options) {
  GraphExpansionRequest graph{{vertex}, request.interval, request.direction,
                              request.edge_type};
  GraphFrontierOptions graph_options;
  graph_options.reservation = nullptr;
  graph_options.delta = options.delta;
  graph_options.adjacency_index = options.adjacency_index
                                      ? options.adjacency_index
                                      : snapshot.adjacency_index();
  graph_options.projection_generation = options.projection_generation;
  graph_options.check_abort = options.check_abort;
  auto expanded = ExpandTemporal(snapshot, graph, graph_options);
  if (!expanded.ok()) return expanded.status();
  std::vector<JourneyTraversal> result;
  for (const TemporalTraversal& traversal : expanded.ValueOrDie()) {
    if (Status fifo = ValidateCallbackFifo(request, traversal); !fifo.ok())
      return fifo;
    TemporalTraversal oriented = traversal;
    if (request.direction == ExpandDirection::kOut) {
      if (traversal.source != vertex) continue;
    } else if (request.direction == ExpandDirection::kIn) {
      if (traversal.target != vertex) continue;
      oriented.source = traversal.target;
      oriented.target = traversal.source;
    } else if (traversal.source == vertex) {
      // Already oriented for an outbound move.
    } else if (traversal.target == vertex) {
      oriented.source = traversal.target;
      oriented.target = traversal.source;
    } else {
      continue;
    }
    ValidTime departure = arrival;
    if (departure.value < oriented.effective.from.value)
      departure = oriented.effective.from;
    auto duration = PropertyDuration(snapshot, oriented.edge, departure, request,
                                     options.delta);
    if (!duration.ok()) return duration.status();
    if (!duration.ValueOrDie()) continue;
    auto end = AddDuration(departure, *duration.ValueOrDie());
    if (!end.ok()) return end.status();
    if (!TraversalFits(oriented.effective, departure, *duration.ValueOrDie()))
      continue;
    if (request.interval.to && end.ValueOrDie().value >= request.interval.to->value)
      continue;
    result.push_back({oriented, departure, end.ValueOrDie(), *duration.ValueOrDie()});
  }
  return result;
}

JourneyValue BuildJourney(const std::vector<JourneyTraversal>& edges,
                          ValidTime initial) {
  JourneyValue result;
  result.initial_departure = initial;
  result.departure = initial;
  result.final_arrival = initial;
  result.arrival = initial;
  result.duration = ValidDuration{0};
  if (edges.empty()) return result;
  result.vertices.push_back(edges.front().traversal.source);
  for (const auto& edge : edges) {
    result.edges.push_back(edge.traversal.edge);
    result.vertices.push_back(edge.traversal.target);
    result.departures.push_back(edge.departure);
    result.arrivals.push_back(edge.arrival);
    result.final_arrival = edge.arrival;
  }
  result.departure = result.initial_departure;
  result.arrival = result.final_arrival;
  result.duration = ValidDuration{result.final_arrival.value - result.initial_departure.value};
  return result;
}

}  // namespace

StatusOr<ValidTime> AddDuration(ValidTime departure, ValidDuration duration) {
  if (duration.value > std::numeric_limits<uint64_t>::max() - departure.value)
    return Status::NumericOverflow("journey", "arrival time overflows");
  return ValidTime{departure.value + duration.value};
}

bool TraversalFits(const ValidTimeInterval& effective, ValidTime departure,
                   ValidDuration duration) {
  if (departure.value < effective.from.value) return false;
  if (effective.to && departure.value >= effective.to->value) return false;
  if (duration.value == 0) return true;
  if (!effective.to) return true;
  return duration.value < effective.to->value - departure.value;
}

StatusOr<JourneyValue> EarliestArrival(Snapshot& snapshot,
                                       const JourneyRequest& request,
                                       const JourneyOptions& options) {
  if (!request.interval.Validate().ok())
    return Status::InvalidArgument("journey", "invalid interval");
  struct Label { VertexRef vertex; ValidTime arrival; uint32_t depth; uint64_t predecessor; JourneyTraversal edge; };
  std::vector<Label> labels;
  std::map<VertexRef, ValidTime, VertexLess> best;
  using HeapEntry = std::pair<uint64_t, uint64_t>;
  std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<>> heap;
  labels.push_back({request.source, request.interval.from, 0, std::numeric_limits<uint64_t>::max(), {}});
  best[request.source] = request.interval.from;
  heap.push({request.interval.from.value, 0});
  while (!heap.empty()) {
    if (Status status = Check(options); !status.ok()) return status;
    const auto [time, id] = heap.top(); heap.pop();
    const Label current = labels[id];
    if (best[current.vertex].value != time) continue;
    if (current.vertex == request.target)
      return BuildJourney([&] { std::vector<JourneyTraversal> path; for (uint64_t i = id; i != 0 && labels[i].predecessor != std::numeric_limits<uint64_t>::max(); i = labels[i].predecessor) path.push_back(labels[i].edge); std::reverse(path.begin(), path.end()); return path; }(), request.interval.from);
    auto next = ExpandAt(snapshot, request, current.vertex, current.arrival, options);
    if (!next.ok()) return next.status();
    for (auto& edge : next.ValueOrDie()) {
      if (request.max_hops != 0 && current.depth >= request.max_hops) continue;
      auto found = best.find(edge.traversal.target);
      if (found != best.end() && found->second.value <= edge.arrival.value) continue;
      if (options.max_labels && labels.size() >= options.max_labels)
        return Status::ResourceExhausted("journey", "label budget exceeded");
      if (Status status = Charge(options); !status.ok()) return status;
      best[edge.traversal.target] = edge.arrival;
      labels.push_back({edge.traversal.target, edge.arrival, current.depth + 1, id, edge});
      heap.push({edge.arrival.value, labels.size() - 1});
    }
  }
  return Status::NotFound("journey", "target is unreachable");
}

StatusOr<JourneyValue> LatestDeparture(Snapshot& snapshot,
                                        const JourneyRequest& request,
                                        const JourneyOptions& options) {
  if (!request.interval.to && !request.arrival_deadline)
    return Status::NotSupported("journey", "latest departure requires a finite time bound");
  JourneyRequest copy = request;
  copy.objective = JourneyObjective::kEarliestArrival;
  const ValidTime deadline = request.arrival_deadline.value_or(
      request.interval.to.value_or(ValidTime{std::numeric_limits<uint64_t>::max()}));
  if (deadline.value <= request.interval.from.value)
    return Status::NotFound("journey", "target is unreachable");
  if (deadline.value - request.interval.from.value > 1'000'000)
    return Status::ResourceExhausted("journey", "latest departure candidate range is too wide");
  // Reverse feasibility is equivalent to enumerating candidate departure
  // boundaries and selecting the latest one; the edge searches remain
  // authoritative and preserve the same half-open checks.
  std::optional<JourneyValue> best;
  for (uint64_t t = request.interval.from.value;
       t < deadline.value && t != std::numeric_limits<uint64_t>::max(); ++t) {
    copy.interval.from = ValidTime{t};
    auto candidate = EarliestArrival(snapshot, copy, options);
    if (!candidate.ok()) {
      if (candidate.status().IsNotFound()) continue;
      return candidate.status();
    }
    if (candidate.ValueOrDie().final_arrival.value <= deadline.value &&
        (!best || candidate.ValueOrDie().initial_departure.value > best->initial_departure.value))
      best = candidate.ValueOrDie();
  }
  if (!best) return Status::NotFound("journey", "target is unreachable");
  return *best;
}

StatusOr<JourneyValue> FastestDuration(Snapshot& snapshot,
                                       const JourneyRequest& request,
                                       const JourneyOptions& options) {
  if (!request.interval.to)
    return Status::NotSupported("journey", "fastest duration requires a finite time bound");
  if (request.interval.to->value <= request.interval.from.value)
    return Status::NotFound("journey", "target is unreachable");
  if (request.interval.to->value - request.interval.from.value > 1'000'000)
    return Status::ResourceExhausted("journey", "fastest duration candidate range is too wide");
  // Keep non-dominated departure/arrival labels per vertex.  This bounded
  // search intentionally shares traversal feasibility with earliest arrival.
  std::vector<JourneyValue> candidates;
  for (uint64_t t = request.interval.from.value;
       (!request.interval.to || t < request.interval.to->value) &&
       t != std::numeric_limits<uint64_t>::max(); ++t) {
    JourneyRequest point = request;
    point.interval.from = ValidTime{t};
    auto result = EarliestArrival(snapshot, point, options);
    if (!result.ok()) {
      if (result.status().IsNotFound()) continue;
      return result.status();
    }
    candidates.push_back(std::move(result).ConsumeValueOrDie());
    if (options.max_labels && candidates.size() > options.max_labels)
      return Status::ResourceExhausted("journey", "Pareto label budget exceeded");
  }
  if (candidates.empty()) return Status::NotFound("journey", "target is unreachable");
  return *std::min_element(candidates.begin(), candidates.end(), [](const auto& a, const auto& b) {
    if (a.duration.value != b.duration.value) return a.duration.value < b.duration.value;
    return a.initial_departure.value < b.initial_departure.value;
  });
}

StatusOr<JourneyValue> FindJourney(Snapshot& snapshot,
                                   const JourneyRequest& request,
                                   const JourneyOptions& options) {
  switch (request.objective) {
    case JourneyObjective::kEarliestArrival: return EarliestArrival(snapshot, request, options);
    case JourneyObjective::kLatestDeparture: return LatestDeparture(snapshot, request, options);
    case JourneyObjective::kFastestDuration: return FastestDuration(snapshot, request, options);
  }
  return Status::InvalidArgument("journey", "unknown objective");
}

}  // namespace cedar::internal
