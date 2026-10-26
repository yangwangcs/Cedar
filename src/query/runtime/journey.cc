#include "query/runtime/journey.h"

#include <algorithm>
#include <limits>
#include <map>
#include <tuple>

#include "query/runtime/fact_history.h"
#include "query/resource/query_read_memory.h"

namespace cedar::internal {
namespace {

struct VertexLess {
  bool operator()(const VertexRef& a, const VertexRef& b) const {
    return std::tie(a.part_id.value, a.vertex_id.value) <
           std::tie(b.part_id.value, b.vertex_id.value);
  }
};

struct JourneyStateKey {
  VertexRef vertex;
  uint32_t depth = 0;
};

struct JourneyStateLess {
  bool operator()(const JourneyStateKey& a, const JourneyStateKey& b) const {
    VertexLess less;
    if (less(a.vertex, b.vertex)) return true;
    if (less(b.vertex, a.vertex)) return false;
    return a.depth < b.depth;
  }
};

struct JourneyTimedStateKey {
  VertexRef vertex;
  uint32_t depth = 0;
  uint64_t time = 0;
};

struct JourneyTimedStateLess {
  bool operator()(const JourneyTimedStateKey& a,
                  const JourneyTimedStateKey& b) const {
    VertexLess less;
    if (less(a.vertex, b.vertex)) return true;
    if (less(b.vertex, a.vertex)) return false;
    if (a.depth != b.depth) return a.depth < b.depth;
    return a.time < b.time;
  }
};

// With no hop bound, FIFO/non-negative durations make a single state per
// vertex sufficient and avoid admitting infinitely many zero-cost cycles.
JourneyStateKey StateKey(VertexRef vertex, uint32_t depth,
                         uint32_t max_hops) {
  return {vertex, max_hops == 0 ? 0U : depth};
}
Status Check(const JourneyOptions& options) {
  if (options.check_abort) return options.check_abort();
  return Status::OK();
}

Status Charge(const JourneyOptions& options) {
  if (Status status = Check(options); !status.ok()) return status;
  if (!options.reservation) return Status::OK();
  return options.reservation->ReserveGraphLabels(1);
}

StatusOr<std::shared_ptr<const std::vector<StateInterval>>> VertexIntervals(
    Snapshot& snapshot, VertexRef vertex, const JourneyOptions& options) {
  return ReadPresentIntervals(snapshot, EntityFact::Vertex(vertex).ref(),
                               options.delta, options.check_abort, options.reservation);
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

bool VisibleAt(const std::vector<StateInterval>& intervals, ValidTime time) {
  for (const StateInterval& interval : intervals) {
    if (time.value < interval.interval.from.value) continue;
    if (!interval.interval.to || time.value < interval.interval.to->value)
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

StatusOr<std::shared_ptr<const std::vector<StateInterval>>> DurationIntervals(
    Snapshot& snapshot, EdgeRef edge, PropertyId property,
    const JourneyOptions& options) {
  return ReadPresentIntervals(snapshot, PropertyFact::Edge(edge, property).ref(),
                               options.delta, options.check_abort, options.reservation);
}

StatusOr<ValidDuration> DurationValue(const Value& raw) {
  switch (raw.type()) {
    case PhysicalType::kInt32: {
      const int32_t value = std::get<int32_t>(raw.data());
      if (value < 0) return Status::InvalidArgument("journey", "duration is negative");
      return ValidDuration{static_cast<uint64_t>(value)};
    }
    case PhysicalType::kInt64: {
      const int64_t value = std::get<int64_t>(raw.data());
      if (value < 0) return Status::InvalidArgument("journey", "duration is negative");
      return ValidDuration{static_cast<uint64_t>(value)};
    }
    case PhysicalType::kTimestamp64:
      return ValidDuration{std::get<uint64_t>(raw.data())};
    default:
      return Status::SchemaMismatch("journey", "duration must be numeric");
  }
}

// Registered durations are piecewise constant over valid-time intervals.  A
// time-dependent edge is FIFO exactly when the arrival times at each segment
// boundary never decrease.  Checking the first and last departure of each
// segment is complete because t + duration is monotone inside a segment.
Status ValidatePropertyFifo(Snapshot& snapshot, EdgeRef edge,
                            const ValidTimeInterval& effective,
                            PropertyId property, const JourneyOptions& options) {
  auto intervals = DurationIntervals(snapshot, edge, property, options);
  if (!intervals.ok()) return intervals.status();
  std::optional<uint64_t> previous_arrival;
  for (const StateInterval& state : *intervals.ValueOrDie()) {
    if (Status status = Check(options); !status.ok()) return status;
    auto clipped = Intersect(state.interval, effective);
    if (!clipped || !state.value) continue;
    auto duration = DurationValue(*state.value);
    if (!duration.ok()) return duration.status();
    const uint64_t first = clipped->from.value;
    auto first_arrival = AddDuration(ValidTime{first}, duration.ValueOrDie());
    if (!first_arrival.ok()) return first_arrival.status();
    if (previous_arrival && first_arrival.ValueOrDie().value < *previous_arrival) {
      return Status::NotSupported("journey", "registered duration is non-FIFO");
    }
    if (!clipped->to) {
      if (duration.ValueOrDie().value != 0) {
        auto terminal = AddDuration(
            ValidTime{std::numeric_limits<uint64_t>::max()},
            duration.ValueOrDie());
        if (!terminal.ok()) return terminal.status();
      }
      previous_arrival = first_arrival.ValueOrDie().value;
      continue;
    }
    if (clipped->to->value <= first) continue;
    const uint64_t last = clipped->to->value - 1;
    auto last_arrival = AddDuration(ValidTime{last}, duration.ValueOrDie());
    if (!last_arrival.ok()) return last_arrival.status();
    previous_arrival = last_arrival.ValueOrDie().value;
  }
  return Status::OK();
}

StatusOr<std::optional<ValidDuration>> PropertyDuration(
    Snapshot& snapshot, EdgeRef edge, ValidTime time,
    const JourneyRequest& request, const JourneyOptions& options) {
  if (request.duration_at) return request.duration_at(edge, time);
  if (!request.duration_property) return std::optional<ValidDuration>{};
  auto intervals = DurationIntervals(snapshot, edge, *request.duration_property, options);
  if (!intervals.ok()) return intervals.status();
  const Value* value = nullptr;
  for (const auto& interval : *intervals.ValueOrDie()) {
    if (Status status = Check(options); !status.ok()) return status;
    if (time.value < interval.interval.from.value) continue;
    if (interval.interval.to && time.value >= interval.interval.to->value) continue;
    value = interval.value ? &*interval.value : nullptr;
    break;
  }
  if (!value) return std::optional<ValidDuration>{};
  auto duration = DurationValue(*value);
  if (!duration.ok()) return duration.status();
  return std::optional<ValidDuration>{duration.ValueOrDie()};
}

StatusOr<BudgetedRows<JourneyTraversal>> ExpandAt(
    Snapshot& snapshot, const JourneyRequest& request, VertexRef vertex,
    ValidTime arrival, const JourneyOptions& options) {
  auto memory = options.reservation
      ? std::make_shared<QueryReadMemory>(*options.reservation) : nullptr;
  FactReadMemoryScope graph_memory(memory.get());
  FactReadMemoryScope output_memory(memory.get());
  if (Status status = graph_memory.Reserve(sizeof(VertexRef)); !status.ok()) return status;
  GraphExpansionRequest graph{{vertex}, request.interval, request.direction,
                              request.edge_type};
  GraphFrontierOptions graph_options;
  graph_options.reservation = options.reservation;
  graph_options.count_expansion_resources = false;
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
    if (request.duration_property) {
      const auto effective = Intersect(oriented.effective, request.interval);
      if (effective.has_value()) {
        Status fifo = ValidatePropertyFifo(snapshot, oriented.edge, *effective,
                                           *request.duration_property,
                                           options);
        if (!fifo.ok()) return fifo;
      }
    }
    ValidTime departure = arrival;
    if (departure.value < oriented.effective.from.value)
      departure = oriented.effective.from;
    if (departure.value > arrival.value) {
      auto source_intervals = VertexIntervals(snapshot, oriented.source,
                                              options);
      if (!source_intervals.ok()) return source_intervals.status();
      if (!CoversWaitingInterval(*source_intervals.ValueOrDie(), arrival,
                                 departure)) {
        continue;
      }
    }
    auto duration = PropertyDuration(snapshot, oriented.edge, departure, request,
                                     options);
    if (!duration.ok()) return duration.status();
    if (!duration.ValueOrDie()) continue;
    auto end = AddDuration(departure, *duration.ValueOrDie());
    if (!end.ok()) return end.status();
    if (!TraversalFits(oriented.effective, departure, *duration.ValueOrDie()))
      continue;
    if (request.interval.to && end.ValueOrDie().value >= request.interval.to->value)
      continue;
    if (Status status = ReserveFactAppend(&result, &output_memory); !status.ok()) return status;
    result.push_back({oriented, departure, end.ValueOrDie(), *duration.ValueOrDie()});
  }
  return RetainRows(std::move(result), &output_memory, std::move(memory));
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
  if (!options.interval_fragments_used) return Status::OK();
  uint64_t& used = *options.interval_fragments_used;
  if (count > std::numeric_limits<uint64_t>::max() - used) {
    return Status::ResourceExhausted("journey", "interval fragment budget overflow");
  }
  const uint64_t total = used + count;
  if (options.max_interval_fragments != 0 &&
      total > options.max_interval_fragments) {
    return Status::ResourceExhausted("journey", "interval fragment budget exceeded");
  }
  used = total;
  return Status::OK();
}

StatusOr<BudgetedRows<JourneyTraversal>> LatestIncoming(
    Snapshot& snapshot, const JourneyRequest& request,
    const JourneyOptions& options, VertexRef target, ValidTime deadline,
    bool final_target) {
  const ExpandDirection reverse_direction =
      request.direction == ExpandDirection::kOut ? ExpandDirection::kIn
      : request.direction == ExpandDirection::kIn ? ExpandDirection::kOut
                                                  : ExpandDirection::kBoth;
  auto memory = options.reservation
      ? std::make_shared<QueryReadMemory>(*options.reservation) : nullptr;
  FactReadMemoryScope graph_memory(memory.get());
  FactReadMemoryScope output_memory(memory.get());
  if (Status status = graph_memory.Reserve(sizeof(VertexRef)); !status.ok()) return status;
  GraphExpansionRequest graph{{target}, request.interval, reverse_direction,
                              request.edge_type};
  GraphFrontierOptions graph_options;
  graph_options.reservation = options.reservation;
  graph_options.count_expansion_resources = false;
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

  std::vector<JourneyTraversal> candidates;
  for (const TemporalTraversal& raw : expanded.ValueOrDie()) {
    if (Status status = Check(options); !status.ok()) return status;
    TemporalTraversal oriented = raw;
    if (request.direction == ExpandDirection::kOut) {
      if (raw.target != target) continue;
    } else if (request.direction == ExpandDirection::kIn) {
      if (raw.source != target) continue;
      std::swap(oriented.source, oriented.target);
    } else if (raw.target == target) {
      // Already oriented from predecessor into current target.
    } else if (raw.source == target) {
      std::swap(oriented.source, oriented.target);
    } else {
      continue;
    }
    if (Status fifo = ValidateCallbackFifo(request, oriented); !fifo.ok())
      return fifo;
    if (request.duration_property) {
      const auto effective = Intersect(oriented.effective, request.interval);
      if (effective.has_value()) {
        Status fifo = ValidatePropertyFifo(snapshot, oriented.edge, *effective,
                                           *request.duration_property,
                                           options);
        if (!fifo.ok()) return fifo;
      }
    }
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
                                       request, options);
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
    // Feasibility is not generally monotone when a callback can be missing
    // at the beginning of an edge's effective interval.  Search from the
    // deadline backwards and choose the first valid departure instead of
    // assuming lower is feasible and binary-searching a false predicate.
    std::optional<uint64_t> latest;
    for (uint64_t point = upper;; --point) {
      if (Status status = Check(options); !status.ok()) return status;
      auto ok = feasible(point);
      if (!ok.ok()) return ok.status();
      if (ok.ValueOrDie()) {
        latest = point;
        break;
      }
      if (point == lower) break;
    }
    if (!latest) continue;
    const uint64_t departure = *latest;
    auto duration = PropertyDuration(snapshot, oriented.edge, ValidTime{departure},
                                     request, options);
    if (!duration.ok()) return duration.status();
    if (!duration.ValueOrDie()) continue;
    auto arrival = AddDuration(ValidTime{departure}, *duration.ValueOrDie());
    if (!arrival.ok()) return arrival.status();
    auto target_intervals = VertexIntervals(snapshot, target, options);
    if (!target_intervals.ok()) return target_intervals.status();
    // The requested destination only needs to be visible at the instant the
    // journey arrives.  A reverse search's deadline is a bound on the
    // predecessor departure and must not force the destination to remain
    // visible after arrival. Intermediate vertices, however, may need to
    // wait until the next reverse expansion and therefore remain continuously
    // visible over [arrival, deadline).
    if (final_target) {
      if (!VisibleAt(*target_intervals.ValueOrDie(), arrival.ValueOrDie()))
        continue;
    } else if (!CoversWaitingInterval(*target_intervals.ValueOrDie(),
                                      arrival.ValueOrDie(), deadline)) {
      continue;
    }
    JourneyTraversal candidate{oriented, ValidTime{departure}, arrival.ValueOrDie(),
                               *duration.ValueOrDie()};
    if (Status status = ReserveFactAppend(&candidates, &output_memory); !status.ok()) return status;
    candidates.push_back(std::move(candidate));
  }
  std::sort(candidates.begin(), candidates.end(),
            [](const JourneyTraversal& a, const JourneyTraversal& b) {
              if (a.departure.value != b.departure.value)
                return a.departure.value > b.departure.value;
              return TraversalLess(a, b);
            });
  return RetainRows(std::move(candidates), &output_memory, std::move(memory));
}

StatusOr<JourneyResult> BuildJourney(const std::vector<JourneyTraversal>& edges,
                                     ValidTime initial,
                                     std::shared_ptr<FactReadMemory> memory) {
  FactReadMemoryScope output_memory(memory.get());
  JourneyResult result;
  result.initial_departure = initial;
  result.departure = initial;
  result.final_arrival = initial;
  result.arrival = initial;
  result.duration = ValidDuration{0};
  if (edges.empty()) return result;
  if (edges.size() == SIZE_MAX)
    return Status::ResourceExhausted("journey", "path size overflow");
  if (Status status = output_memory.ReserveElements(edges.size() + 1, sizeof(VertexRef));
      !status.ok()) return status;
  if (Status status = output_memory.ReserveElements(edges.size(), sizeof(EdgeRef) + 2 * sizeof(ValidTime));
      !status.ok()) return status;
  result.vertices.reserve(edges.size() + 1);
  result.edges.reserve(edges.size());
  result.departures.reserve(edges.size());
  result.arrivals.reserve(edges.size());
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
  auto owner = output_memory.Retain(std::move(memory));
  if (!owner.ok()) return owner.status();
  result.memory = owner.ConsumeValueOrDie();
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

StatusOr<JourneyResult> EarliestArrival(Snapshot& snapshot,
                                       const JourneyRequest& request,
                                       const JourneyOptions& options) {
  if (!request.interval.Validate().ok())
    return Status::InvalidArgument("journey", "invalid interval");
  auto memory = options.reservation
      ? std::make_shared<QueryReadMemory>(*options.reservation) : nullptr;
  FactReadMemoryScope working_memory(memory.get());
  JourneyOptions scoped = options;
  scoped.interval_fragments_used = std::make_shared<uint64_t>(0);
  struct Label { VertexRef vertex; ValidTime arrival; uint32_t depth; uint64_t predecessor; JourneyTraversal edge; };
  std::vector<Label> labels;
  // Arrival time is part of the state.  An earlier label is not universally
  // dominant: waiting from that time to a later departure may cross a
  // visibility gap at the intermediate vertex.  Exact timed states retain
  // correctness while still collapsing duplicate zero-duration states.
  std::map<JourneyTimedStateKey, uint64_t, JourneyTimedStateLess> best;
  using HeapEntry = std::pair<uint64_t, uint64_t>;
  std::vector<HeapEntry> heap;
  const auto enqueue = [&](HeapEntry entry) -> Status {
    if (Status status = ReserveFactAppend(&heap, &working_memory); !status.ok()) return status;
    heap.push_back(entry);
    std::push_heap(heap.begin(), heap.end(), std::greater<>{});
    return Status::OK();
  };
  if (Status status = ReserveFactAppend(&labels, &working_memory); !status.ok()) return status;
  if (Status status = working_memory.Reserve(sizeof(decltype(best)::value_type) + 4 * sizeof(void*));
      !status.ok()) return status;
  labels.push_back({request.source, request.interval.from, 0, std::numeric_limits<uint64_t>::max(), {}});
  best[{request.source, 0U, request.interval.from.value}] = 0;
  if (Status status = enqueue({request.interval.from.value, 0}); !status.ok()) return status;
  while (!heap.empty()) {
    if (Status status = Check(options); !status.ok()) return status;
    std::pop_heap(heap.begin(), heap.end(), std::greater<>{});
    const auto [time, id] = heap.back();
    heap.pop_back();
    const Label current = labels[id];
    const auto current_key = JourneyTimedStateKey{
        current.vertex, request.max_hops == 0 ? 0U : current.depth,
        current.arrival.value};
    auto best_current = best.find(current_key);
    if (best_current == best.end() || best_current->second != id)
      continue;
    if (current.vertex == request.target) {
      FactReadMemoryScope path_memory(memory.get());
      std::vector<JourneyTraversal> path;
      for (uint64_t i = id; labels[i].predecessor != std::numeric_limits<uint64_t>::max();
           i = labels[i].predecessor) {
        if (Status status = Check(options); !status.ok()) return status;
        if (Status status = ReserveFactAppend(&path, &path_memory); !status.ok()) return status;
        path.push_back(labels[i].edge);
      }
      std::reverse(path.begin(), path.end());
      return BuildJourney(path, request.interval.from, memory);
    }
    if (request.max_hops != 0 && current.depth >= request.max_hops) continue;
    if (current.depth == UINT32_MAX)
      return Status::ResourceExhausted("journey", "path depth overflow");
    auto next = ExpandAt(snapshot, request, current.vertex, current.arrival, scoped);
    if (!next.ok()) return next.status();
    if (Status budget = CheckFragmentBudget(scoped, next.ValueOrDie().size());
        !budget.ok())
      return budget;
    for (auto& edge : next.ValueOrDie()) {
      if (request.max_hops != 0 && current.depth >= request.max_hops) continue;
      const uint32_t depth = current.depth + 1;
      const JourneyTimedStateKey state_key{
          edge.traversal.target, request.max_hops == 0 ? 0U : depth,
          edge.arrival.value};
      if (best.find(state_key) != best.end()) continue;
      if (options.max_labels && labels.size() >= options.max_labels)
        return Status::ResourceExhausted("journey", "label budget exceeded");
      if (Status status = Charge(scoped); !status.ok()) return status;
      const uint64_t next_id = labels.size();
      if (Status status = working_memory.Reserve(sizeof(decltype(best)::value_type) + 4 * sizeof(void*));
          !status.ok()) return status;
      if (Status status = ReserveFactAppend(&labels, &working_memory); !status.ok()) return status;
      best[state_key] = next_id;
      labels.push_back({edge.traversal.target, edge.arrival, depth, id, edge});
      if (Status status = enqueue({edge.arrival.value, next_id}); !status.ok()) return status;
    }
  }
  return Status::NotFound("journey", "target is unreachable");
}

StatusOr<JourneyResult> LatestDeparture(Snapshot& snapshot,
                                        const JourneyRequest& request,
                                        const JourneyOptions& options) {
  if (!request.interval.to && !request.arrival_deadline)
    return Status::NotSupported("journey", "latest departure requires a finite time bound");
  const ValidTime deadline = request.arrival_deadline.value_or(
      request.interval.to.value_or(ValidTime{std::numeric_limits<uint64_t>::max()}));
  if (deadline.value <= request.interval.from.value)
    return Status::NotFound("journey", "target is unreachable");
  auto memory = options.reservation
      ? std::make_shared<QueryReadMemory>(*options.reservation) : nullptr;
  FactReadMemoryScope working_memory(memory.get());
  JourneyOptions scoped = options;
  scoped.interval_fragments_used = std::make_shared<uint64_t>(0);
  struct Label {
    VertexRef vertex;
    ValidTime time;
    uint32_t depth;
    uint64_t successor;
    JourneyTraversal edge;
  };
  std::vector<Label> labels;
  // A later reverse departure is not generally dominant over an earlier one:
  // the intermediate vertex may disappear during the waiting interval. Keep
  // one label per exact (vertex, depth, departure) state, while still
  // collapsing exact duplicates (including zero-duration cycles when hops
  // are unbounded).
  std::map<JourneyTimedStateKey, uint64_t, JourneyTimedStateLess> best;
  using HeapEntry = std::pair<uint64_t, uint64_t>;
  std::vector<HeapEntry> heap;
  const auto enqueue = [&](HeapEntry entry) -> Status {
    if (Status status = ReserveFactAppend(&heap, &working_memory); !status.ok()) return status;
    heap.push_back(entry);
    std::push_heap(heap.begin(), heap.end());
    return Status::OK();
  };
  if (Status status = ReserveFactAppend(&labels, &working_memory); !status.ok()) return status;
  if (Status status = working_memory.Reserve(sizeof(decltype(best)::value_type) + 4 * sizeof(void*));
      !status.ok()) return status;
  labels.push_back({request.target, deadline, 0,
                    std::numeric_limits<uint64_t>::max(), {}});
  best[{request.target, 0, deadline.value}] = 0;
  if (Status status = enqueue({deadline.value, 0}); !status.ok()) return status;
  while (!heap.empty()) {
    if (Status status = Check(options); !status.ok()) return status;
    std::pop_heap(heap.begin(), heap.end());
    const auto [time, id] = heap.back();
    heap.pop_back();
    const Label current = labels[id];
    auto found = best.find({current.vertex,
                            request.max_hops == 0 ? 0U : current.depth,
                            current.time.value});
    if (found == best.end() || found->second != id || current.time.value != time)
      continue;
    if (current.vertex == request.source) {
      FactReadMemoryScope path_memory(memory.get());
      std::vector<JourneyTraversal> path;
      uint64_t cursor = id;
      while (labels[cursor].successor != std::numeric_limits<uint64_t>::max()) {
        if (Status status = Check(options); !status.ok()) return status;
        if (Status status = ReserveFactAppend(&path, &path_memory); !status.ok()) return status;
        path.push_back(labels[cursor].edge);
        cursor = labels[cursor].successor;
      }
      return BuildJourney(path, current.time, memory);
    }
    if (request.max_hops != 0 && current.depth >= request.max_hops) continue;
    if (current.depth == UINT32_MAX)
      return Status::ResourceExhausted("journey", "path depth overflow");
    auto incoming = LatestIncoming(snapshot, request, scoped, current.vertex,
                                   current.time, current.depth == 0);
    if (!incoming.ok()) return incoming.status();
    for (const JourneyTraversal& candidate : incoming.ValueOrDie()) {
      const VertexRef predecessor = candidate.traversal.source;
      const uint32_t depth = current.depth + 1;
      const uint32_t state_depth = request.max_hops == 0 ? 0U : depth;
      auto existing = best.find(
          {predecessor, state_depth, candidate.departure.value});
      if (existing != best.end()) {
        const Label& prior = labels[existing->second];
        // Same time/state has the same future search space. Keep the first
        // deterministic edge selected by LatestIncoming's ordering.
        if (prior.time == candidate.departure)
          continue;
      }
      if (options.max_labels && labels.size() >= options.max_labels)
        return Status::ResourceExhausted("journey", "label budget exceeded");
      if (Status status = Charge(scoped); !status.ok()) return status;
      const uint64_t next = labels.size();
      if (Status status = ReserveFactAppend(&labels, &working_memory); !status.ok()) return status;
      if (existing == best.end()) {
        if (Status status = working_memory.Reserve(sizeof(decltype(best)::value_type) + 4 * sizeof(void*));
            !status.ok()) return status;
      }
      labels.push_back({predecessor, candidate.departure, depth,
                        id, candidate});
      best[{predecessor, state_depth, candidate.departure.value}] = next;
      if (Status status = enqueue({candidate.departure.value, next}); !status.ok()) return status;
    }
  }
  return Status::NotFound("journey", "target is unreachable");
}

StatusOr<JourneyResult> FastestDuration(Snapshot& snapshot,
                                       const JourneyRequest& request,
                                       const JourneyOptions& options) {
  if (!request.interval.to)
    return Status::NotSupported("journey", "fastest duration requires a finite time bound");
  if (request.interval.to->value <= request.interval.from.value)
    return Status::NotFound("journey", "target is unreachable");
  auto memory = options.reservation
      ? std::make_shared<QueryReadMemory>(*options.reservation) : nullptr;
  FactReadMemoryScope working_memory(memory.get());
  JourneyOptions scoped = options;
  scoped.interval_fragments_used = std::make_shared<uint64_t>(0);
  struct Label {
    VertexRef vertex;
    ValidTime departure;
    ValidTime arrival;
    uint32_t depth;
    uint64_t predecessor;
    JourneyTraversal edge;
  };
  std::vector<Label> labels;
  // Keep exact timed labels.  Pareto dominance based only on departure and
  // arrival is unsound when the vertex disappears between those times: the
  // earlier-arriving label may be unable to wait for the next edge, while a
  // later-arriving label remains feasible.  Exact duplicate states are still
  // collapsed below, and max_labels/graph reservations bound resource use.
  std::map<JourneyStateKey, std::vector<uint64_t>, JourneyStateLess> frontier;
  auto path_for = [&](uint64_t id) -> StatusOr<BudgetedRows<JourneyTraversal>> {
    FactReadMemoryScope path_memory(memory.get());
    std::vector<JourneyTraversal> path;
    for (uint64_t cursor = id;
         cursor != std::numeric_limits<uint64_t>::max() && labels[cursor].predecessor != std::numeric_limits<uint64_t>::max();
         cursor = labels[cursor].predecessor) {
      if (Status status = Check(options); !status.ok()) return status;
      if (Status status = ReserveFactAppend(&path, &path_memory); !status.ok()) return status;
      path.push_back(labels[cursor].edge);
    }
    std::reverse(path.begin(), path.end());
    return RetainRows(std::move(path), &path_memory, memory);
  };
  const auto labels_for = [&](JourneyStateKey key) -> StatusOr<std::vector<uint64_t>*> {
    auto found = frontier.find(key);
    if (found == frontier.end()) {
      if (Status status = working_memory.Reserve(sizeof(decltype(frontier)::value_type) + 4 * sizeof(void*));
          !status.ok()) return status;
      found = frontier.emplace(key, std::vector<uint64_t>{}).first;
    }
    return &found->second;
  };
  if (Status status = ReserveFactAppend(&labels, &working_memory); !status.ok()) return status;
  labels.push_back({request.source, request.interval.from, request.interval.from,
                    0, std::numeric_limits<uint64_t>::max(), {}});
  auto source_labels = labels_for(StateKey(request.source, 0, request.max_hops));
  if (!source_labels.ok()) return source_labels.status();
  if (Status status = ReserveFactAppend(source_labels.ValueOrDie(), &working_memory); !status.ok()) return status;
  source_labels.ValueOrDie()->push_back(0);
  std::vector<uint64_t> pending;
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
  if (Status status = enqueue(0); !status.ok()) return status;
  std::optional<uint64_t> answer;
  while (pending_head < pending.size()) {
    if (Status status = Check(options); !status.ok()) return status;
    const uint64_t id = pending[pending_head++];
    const Label current = labels[id];
    if (current.vertex == request.target && current.depth != 0) {
      const uint64_t duration = current.arrival.value - current.departure.value;
      bool better = !answer;
      if (answer) {
        const Label& prior = labels[*answer];
        const uint64_t prior_duration = prior.arrival.value - prior.departure.value;
        better = duration < prior_duration ||
                 (duration == prior_duration && current.departure.value < prior.departure.value);
        if (duration == prior_duration && current.departure == prior.departure) {
          auto candidate_path = path_for(id);
          if (!candidate_path.ok()) return candidate_path.status();
          auto prior_path = path_for(*answer);
          if (!prior_path.ok()) return prior_path.status();
          better = PathLess(candidate_path.ValueOrDie(), prior_path.ValueOrDie());
        }
      }
      if (better)
        answer = id;
    }
    if (request.max_hops != 0 && current.depth >= request.max_hops) continue;
    if (current.depth == UINT32_MAX)
      return Status::ResourceExhausted("journey", "path depth overflow");
    auto next = ExpandAt(snapshot, request, current.vertex, current.arrival, scoped);
    if (!next.ok()) return next.status();
    std::sort(next.ValueOrDie().begin(), next.ValueOrDie().end(), TraversalLess);
    if (Status budget = CheckFragmentBudget(scoped, next.ValueOrDie().size());
        !budget.ok())
      return budget;
    for (const JourneyTraversal& edge : next.ValueOrDie()) {
      Label candidate{edge.traversal.target,
                      current.depth == 0 ? edge.departure : current.departure,
                      edge.arrival, current.depth + 1, id, edge};
      auto at_vertex = labels_for(StateKey(candidate.vertex, candidate.depth, request.max_hops));
      if (!at_vertex.ok()) return at_vertex.status();
      auto& labels_at_vertex = *at_vertex.ValueOrDie();
      bool rejected = false;
      for (uint64_t prior_id : labels_at_vertex) {
        if (labels[prior_id].departure == candidate.departure &&
            labels[prior_id].arrival == candidate.arrival) {
          rejected = true;
          break;
        }
      }
      if (rejected) continue;
      if (options.max_labels && labels.size() >= options.max_labels)
        return Status::ResourceExhausted("journey", "Pareto label budget exceeded");
      if (Status status = Charge(scoped); !status.ok()) return status;
      const uint64_t next_id = labels.size();
      if (Status status = ReserveFactAppend(&labels, &working_memory); !status.ok()) return status;
      if (Status status = ReserveFactAppend(&labels_at_vertex, &working_memory); !status.ok()) return status;
      labels.push_back(std::move(candidate));
      labels_at_vertex.push_back(next_id);
      if (Status status = enqueue(next_id); !status.ok()) return status;
    }
  }
  if (!answer) return Status::NotFound("journey", "target is unreachable");
  auto path = path_for(*answer);
  if (!path.ok()) return path.status();
  return BuildJourney(path.ValueOrDie(), labels[*answer].departure, memory);
}

StatusOr<JourneyResult> FindJourney(Snapshot& snapshot,
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
