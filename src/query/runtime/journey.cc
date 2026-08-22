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

StatusOr<std::vector<StateInterval>> VertexIntervals(
    Snapshot& snapshot, VertexRef vertex, const QueryDeltaView* delta) {
  const FactRef ref = EntityFact::Vertex(vertex).ref();
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
  return MaterializePresentState(corrected.ValueOrDie());
}

bool CoversWaitingInterval(const std::vector<StateInterval>& intervals,
                           ValidTime arrival, ValidTime departure) {
  if (arrival.value >= departure.value) return true;
  for (const StateInterval& interval : intervals) {
    if (interval.interval.from.value > arrival.value) continue;
    if (!interval.interval.to || departure.value <= interval.interval.to->value)
      return true;
  }
  return false;
}

Status ValidateCallbackFifo(const JourneyRequest& request,
                            const TemporalTraversal& traversal) {
  if (!request.duration_at) return Status::OK();
  const uint64_t first = traversal.effective.from.value;
  if (!traversal.effective.to.has_value()) {
    return Status::NotSupported(
        "journey", "FIFO cannot be proven for an unbounded duration callback");
  }
  const uint64_t end = traversal.effective.to->value;
  if (end <= first) return Status::OK();
  constexpr uint64_t kMaxFifoValidationPoints = 1'000'000;
  if (end - first > kMaxFifoValidationPoints) {
    return Status::NotSupported(
        "journey", "duration callback interval is too wide to prove FIFO");
  }

  // ValidTime is an integer domain.  Checking every point in a bounded
  // interval is a complete proof for callbacks over that interval; sparse
  // sampling would allow a hidden non-FIFO transition between samples.
  std::optional<ValidTime> previous_arrival;
  for (uint64_t time = first; time < end; ++time) {
    auto value = request.duration_at(traversal.edge, ValidTime{time});
    if (!value.ok()) return value.status();
    if (!value.ValueOrDie()) continue;
    auto current_arrival = AddDuration(ValidTime{time}, *value.ValueOrDie());
    if (!current_arrival.ok()) return current_arrival.status();
    if (previous_arrival &&
        current_arrival.ValueOrDie().value < previous_arrival->value) {
      return Status::NotSupported("journey", "duration callback is non-FIFO");
    }
    previous_arrival = current_arrival.ValueOrDie();
  }
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
    if (departure.value > arrival.value) {
      auto source_intervals = VertexIntervals(snapshot, oriented.source,
                                              options.delta);
      if (!source_intervals.ok()) return source_intervals.status();
      if (!CoversWaitingInterval(source_intervals.ValueOrDie(), arrival,
                                 departure)) {
        continue;
      }
    }
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

bool TraversalLess(const JourneyTraversal& a, const JourneyTraversal& b) {
  return std::tie(a.traversal.source.part_id.value,
                  a.traversal.source.vertex_id.value,
                  a.traversal.target.part_id.value,
                  a.traversal.target.vertex_id.value,
                  a.traversal.edge.home_part_id.value,
                  a.traversal.edge.edge_id.value,
                  a.departure.value,
                  a.arrival.value) <
         std::tie(b.traversal.source.part_id.value,
                  b.traversal.source.vertex_id.value,
                  b.traversal.target.part_id.value,
                  b.traversal.target.vertex_id.value,
                  b.traversal.edge.home_part_id.value,
                  b.traversal.edge.edge_id.value,
                  b.departure.value,
                  b.arrival.value);
}

bool PathLess(const std::vector<JourneyTraversal>& a,
              const std::vector<JourneyTraversal>& b) {
  const size_t common = std::min(a.size(), b.size());
  for (size_t i = 0; i < common; ++i) {
    if (TraversalLess(a[i], b[i])) return true;
    if (TraversalLess(b[i], a[i])) return false;
  }
  return a.size() < b.size();
}

Status CheckFragmentBudget(const JourneyOptions& options, uint64_t count) {
  if (options.max_interval_fragments != 0 &&
      count > options.max_interval_fragments) {
    return Status::ResourceExhausted("journey", "interval fragment budget exceeded");
  }
  return Status::OK();
}

StatusOr<std::optional<JourneyTraversal>> LatestIncoming(
    Snapshot& snapshot, const JourneyRequest& request,
    const JourneyOptions& options, VertexRef target, ValidTime deadline) {
  GraphExpansionRequest graph{{target}, request.interval, ExpandDirection::kIn,
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
  if (Status budget = CheckFragmentBudget(options, expanded.ValueOrDie().size());
      !budget.ok())
    return budget;

  std::optional<JourneyTraversal> best;
  for (const TemporalTraversal& raw : expanded.ValueOrDie()) {
    if (Status status = Check(options); !status.ok()) return status;
    if (raw.target != target) continue;
    if (Status fifo = ValidateCallbackFifo(request, raw); !fifo.ok())
      return fifo;
    TemporalTraversal oriented = raw;
    const uint64_t lower = oriented.effective.from.value;
    if (deadline.value < lower) continue;
    uint64_t upper = deadline.value;
    if (oriented.effective.to) {
      if (*oriented.effective.to == ValidTime{0}) continue;
      upper = std::min(upper, oriented.effective.to->value - 1);
    }
    if (request.interval.to) {
      if (*request.interval.to == ValidTime{0}) continue;
      upper = std::min(upper, request.interval.to->value - 1);
    }
    if (upper < lower) continue;
    auto feasible = [&](uint64_t point) -> StatusOr<bool> {
      auto duration = PropertyDuration(snapshot, oriented.edge, ValidTime{point},
                                       request, options.delta);
      if (!duration.ok()) return duration.status();
      if (!duration.ValueOrDie()) return false;
      auto arrival = AddDuration(ValidTime{point}, *duration.ValueOrDie());
      if (!arrival.ok()) return arrival.status();
      if (arrival.ValueOrDie().value > deadline.value) return false;
      if (request.interval.to &&
          arrival.ValueOrDie().value >= request.interval.to->value)
        return false;
      return TraversalFits(oriented.effective, ValidTime{point},
                           *duration.ValueOrDie());
    };
    auto at_lower = feasible(lower);
    if (!at_lower.ok()) return at_lower.status();
    if (!at_lower.ValueOrDie()) continue;
    uint64_t lo = lower;
    uint64_t hi = upper;
    while (lo < hi) {
      const uint64_t mid = lo + (hi - lo + 1) / 2;
      auto ok = feasible(mid);
      if (!ok.ok()) return ok.status();
      if (ok.ValueOrDie()) lo = mid;
      else hi = mid - 1;
    }
    auto duration = PropertyDuration(snapshot, oriented.edge, ValidTime{lo},
                                     request, options.delta);
    if (!duration.ok()) return duration.status();
    if (!duration.ValueOrDie()) continue;
    auto arrival = AddDuration(ValidTime{lo}, *duration.ValueOrDie());
    if (!arrival.ok()) return arrival.status();
    JourneyTraversal candidate{oriented, ValidTime{lo}, arrival.ValueOrDie(),
                               *duration.ValueOrDie()};
    if (!best || candidate.departure.value > best->departure.value ||
        (candidate.departure == best->departure && TraversalLess(candidate, *best)))
      best = candidate;
  }
  return best;
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
    if (Status budget = CheckFragmentBudget(options, next.ValueOrDie().size());
        !budget.ok())
      return budget;
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
  const ValidTime deadline = request.arrival_deadline.value_or(
      request.interval.to.value_or(ValidTime{std::numeric_limits<uint64_t>::max()}));
  if (deadline.value <= request.interval.from.value)
    return Status::NotFound("journey", "target is unreachable");
  struct Label {
    VertexRef vertex;
    ValidTime time;
    uint32_t depth;
    uint64_t successor;
    JourneyTraversal edge;
  };
  std::vector<Label> labels;
  std::map<VertexRef, uint64_t, VertexLess> best;
  using HeapEntry = std::pair<uint64_t, uint64_t>;
  std::priority_queue<HeapEntry> heap;
  labels.push_back({request.target, deadline, 0,
                    std::numeric_limits<uint64_t>::max(), {}});
  best[request.target] = 0;
  heap.push({deadline.value, 0});
  while (!heap.empty()) {
    if (Status status = Check(options); !status.ok()) return status;
    const auto [time, id] = heap.top();
    heap.pop();
    const Label current = labels[id];
    auto found = best.find(current.vertex);
    if (found == best.end() || found->second != id || current.time.value != time)
      continue;
    if (current.vertex == request.source) {
      std::vector<JourneyTraversal> path;
      uint64_t cursor = id;
      while (labels[cursor].successor != std::numeric_limits<uint64_t>::max()) {
        path.push_back(labels[cursor].edge);
        cursor = labels[cursor].successor;
      }
      return BuildJourney(path, current.time);
    }
    if (request.max_hops != 0 && current.depth >= request.max_hops) continue;
    auto edge = LatestIncoming(snapshot, request, options, current.vertex,
                               current.time);
    if (!edge.ok()) return edge.status();
    if (!edge.ValueOrDie()) continue;
    const JourneyTraversal candidate = *edge.ValueOrDie();
    const VertexRef predecessor = candidate.traversal.source;
    auto existing = best.find(predecessor);
    if (existing != best.end()) {
      const Label& prior = labels[existing->second];
      if (prior.time.value > candidate.departure.value ||
          (prior.time == candidate.departure &&
           !TraversalLess(candidate, prior.edge)))
        continue;
    }
    if (options.max_labels && labels.size() >= options.max_labels)
      return Status::ResourceExhausted("journey", "label budget exceeded");
    if (Status status = Charge(options); !status.ok()) return status;
    const uint64_t next = labels.size();
    labels.push_back({predecessor, candidate.departure, current.depth + 1,
                      id, candidate});
    best[predecessor] = next;
    heap.push({candidate.departure.value, next});
  }
  return Status::NotFound("journey", "target is unreachable");
}

StatusOr<JourneyValue> FastestDuration(Snapshot& snapshot,
                                       const JourneyRequest& request,
                                       const JourneyOptions& options) {
  if (!request.interval.to)
    return Status::NotSupported("journey", "fastest duration requires a finite time bound");
  if (request.interval.to->value <= request.interval.from.value)
    return Status::NotFound("journey", "target is unreachable");
  struct Label {
    VertexRef vertex;
    ValidTime departure;
    ValidTime arrival;
    uint32_t depth;
    uint64_t predecessor;
    JourneyTraversal edge;
  };
  std::vector<Label> labels;
  std::map<VertexRef, std::vector<uint64_t>, VertexLess> frontier;
  auto dominates = [](const Label& a, const Label& b) {
    return a.departure.value >= b.departure.value &&
           a.arrival.value <= b.arrival.value &&
           (a.departure.value > b.departure.value ||
            a.arrival.value < b.arrival.value);
  };
  auto path_for = [&](uint64_t id) {
    std::vector<JourneyTraversal> path;
    for (uint64_t cursor = id;
         cursor != std::numeric_limits<uint64_t>::max() && labels[cursor].predecessor != std::numeric_limits<uint64_t>::max();
         cursor = labels[cursor].predecessor)
      path.push_back(labels[cursor].edge);
    std::reverse(path.begin(), path.end());
    return path;
  };
  labels.push_back({request.source, request.interval.from, request.interval.from,
                    0, std::numeric_limits<uint64_t>::max(), {}});
  frontier[request.source].push_back(0);
  std::queue<uint64_t> pending;
  pending.push(0);
  std::optional<uint64_t> answer;
  while (!pending.empty()) {
    if (Status status = Check(options); !status.ok()) return status;
    const uint64_t id = pending.front();
    pending.pop();
    const Label current = labels[id];
    if (current.vertex == request.target && current.depth != 0) {
      const uint64_t duration = current.arrival.value - current.departure.value;
      bool better = !answer;
      if (answer) {
        const Label& prior = labels[*answer];
        const uint64_t prior_duration = prior.arrival.value - prior.departure.value;
        better = duration < prior_duration ||
                 (duration == prior_duration &&
                  (current.departure.value < prior.departure.value ||
                   (current.departure == prior.departure &&
                    PathLess(path_for(id), path_for(*answer)))));
      }
      if (better)
        answer = id;
    }
    if (request.max_hops != 0 && current.depth >= request.max_hops) continue;
    auto next = ExpandAt(snapshot, request, current.vertex, current.arrival, options);
    if (!next.ok()) return next.status();
    std::sort(next.ValueOrDie().begin(), next.ValueOrDie().end(), TraversalLess);
    if (Status budget = CheckFragmentBudget(options, next.ValueOrDie().size());
        !budget.ok())
      return budget;
    for (const JourneyTraversal& edge : next.ValueOrDie()) {
      Label candidate{edge.traversal.target,
                      current.depth == 0 ? edge.departure : current.departure,
                      edge.arrival, current.depth + 1, id, edge};
      auto& labels_at_vertex = frontier[candidate.vertex];
      bool rejected = false;
      for (uint64_t prior_id : labels_at_vertex) {
        if (dominates(labels[prior_id], candidate) ||
            (labels[prior_id].departure == candidate.departure &&
             labels[prior_id].arrival == candidate.arrival)) {
          rejected = true;
          break;
        }
      }
      if (rejected) continue;
      labels_at_vertex.erase(
          std::remove_if(labels_at_vertex.begin(), labels_at_vertex.end(),
                         [&](uint64_t prior_id) { return dominates(candidate, labels[prior_id]); }),
          labels_at_vertex.end());
      if (options.max_labels && labels.size() >= options.max_labels)
        return Status::ResourceExhausted("journey", "Pareto label budget exceeded");
      if (Status status = Charge(options); !status.ok()) return status;
      const uint64_t next_id = labels.size();
      labels.push_back(std::move(candidate));
      labels_at_vertex.push_back(next_id);
      pending.push(next_id);
    }
  }
  if (!answer) return Status::NotFound("journey", "target is unreachable");
  return BuildJourney(path_for(*answer), labels[*answer].departure);
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
