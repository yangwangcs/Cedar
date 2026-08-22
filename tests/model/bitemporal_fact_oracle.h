// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TESTS_MODEL_BITEMPORAL_FACT_ORACLE_H_
#define CEDAR_TESTS_MODEL_BITEMPORAL_FACT_ORACLE_H_

#include <algorithm>
#include <limits>
#include <map>
#include <functional>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "cedar/fact/fact.h"
#include "cedar/query/query.h"
#include "cedar/query/result.h"
#include "cedar/query/types.h"

namespace cedar::test {

struct OracleStateInterval {
  ValidTimeInterval interval;
  std::optional<Value> value;

  bool operator==(const OracleStateInterval&) const = default;
};

struct OracleChange {
  ValidTime valid_from;
  std::optional<Value> before;
  std::optional<Value> after;

  bool operator==(const OracleChange&) const = default;
};

struct OracleRow {
  FactRef ref;
  ValidTimeInterval interval;
  std::optional<Value> value;
  std::optional<EdgeIdentity> edge_identity;
  bool operator==(const OracleRow&) const = default;
};
using OracleRows = std::vector<OracleRow>;

struct OracleLogicalQuery {
  std::vector<FactRef> refs;
  std::optional<ValidTimeInterval> interval;
};

struct OracleExpandSpec {
  VertexRef source;
  ValidTimeInterval interval{{ValidTime{0}}, std::nullopt};
  ExpandDirection direction = ExpandDirection::kOut;
  std::optional<uint64_t> edge_type;
  uint32_t max_hops = 1;
};

struct OraclePathSpec {
  VertexRef source;
  VertexRef target;
  ValidTimeInterval interval{{ValidTime{0}}, std::nullopt};
  ExpandDirection direction = ExpandDirection::kOut;
  std::optional<uint64_t> edge_type;
  uint32_t max_hops = 4;
};

struct OracleJourneySpec : OraclePathSpec {
  std::optional<PropertyId> duration_property;
};
using OraclePath = PathValue;
using OracleJourney = JourneyValue;

inline bool EdgeRefLess(const EdgeRef& a, const EdgeRef& b) {
  if (a.home_part_id.value != b.home_part_id.value) {
    return a.home_part_id.value < b.home_part_id.value;
  }
  return a.edge_id.value < b.edge_id.value;
}

inline bool IntervalLess(const ValidTimeInterval& a,
                         const ValidTimeInterval& b) {
  if (a.from != b.from) return a.from.value < b.from.value;
  const uint64_t ae = a.to ? a.to->value : UINT64_MAX;
  const uint64_t be = b.to ? b.to->value : UINT64_MAX;
  return ae < be;
}

class BitemporalFactOracle {
 public:
  void Add(FactEvent event) { events_.push_back(std::move(event)); }

  // Stable, lossless replay text used by randomized failures.  It deliberately
  // serializes the event log rather than any production query/planner state.
  std::string Serialize() const {
    std::vector<FactEvent> ordered = events_;
    std::sort(ordered.begin(), ordered.end(), [](const FactEvent& a, const FactEvent& b) {
      if (a.commit_seq != b.commit_seq) return a.commit_seq.value < b.commit_seq.value;
      if (a.valid_from != b.valid_from) return a.valid_from.value < b.valid_from.value;
      if (a.ref.part_id().value != b.ref.part_id().value) return a.ref.part_id().value < b.ref.part_id().value;
      return a.ref.entity_id() < b.ref.entity_id();
    });
    std::string out;
    for (const FactEvent& event : ordered) {
      out += std::to_string(event.ref.part_id().value) + ":" +
             std::to_string(static_cast<uint8_t>(event.ref.family())) + ":" +
             std::to_string(event.ref.property_id().value) + ":" +
             std::to_string(event.ref.entity_id()) + ":" +
             std::to_string(event.valid_from.value) + ":" +
             std::to_string(event.commit_seq.value) + ":" +
             std::to_string(static_cast<uint8_t>(event.operation)) + ":" +
             std::to_string(event.schema_epoch) + ":" +
             (event.value ? event.value->Encode() : "-");
      if (event.edge_identity) {
        const auto& e = *event.edge_identity;
        out += ":" + std::to_string(e.home_part_id.value) + ":" +
               std::to_string(e.edge_id.value) + ":" +
               std::to_string(e.source_part_id.value) + ":" +
               std::to_string(e.source_vertex_id.value) + ":" +
               std::to_string(e.target_part_id.value) + ":" +
               std::to_string(e.target_vertex_id.value) + ":" +
               std::to_string(e.edge_type);
      }
      out.push_back('\n');
    }
    return out;
  }

  std::optional<FactEvent> Read(const FactRef& ref, ValidTime valid_time,
                                CommitSeq snapshot_seq) const {
    std::optional<FactEvent> selected;
    for (const FactEvent& event : events_) {
      if (event.ref != ref || event.valid_from.value > valid_time.value ||
          event.commit_seq.value > snapshot_seq.value) {
        continue;
      }
      if (!selected.has_value() ||
          event.valid_from.value > selected->valid_from.value ||
          (event.valid_from == selected->valid_from &&
           event.commit_seq.value > selected->commit_seq.value)) {
        selected = event;
      }
    }
    if (selected.has_value() && selected->operation == FactOperation::kDelete) {
      return std::nullopt;
    }
    return selected;
  }

  std::vector<FactEvent> CorrectedEvents(const FactRef& ref,
                                          CommitSeq snapshot_seq) const {
    std::vector<FactEvent> visible;
    for (const FactEvent& event : events_) {
      if (event.ref == ref && event.commit_seq.value <= snapshot_seq.value) {
        visible.push_back(event);
      }
    }
    std::sort(visible.begin(), visible.end(), [](const FactEvent& left,
                                                  const FactEvent& right) {
      if (left.valid_from != right.valid_from) {
        return left.valid_from.value < right.valid_from.value;
      }
      return left.commit_seq.value < right.commit_seq.value;
    });

    std::vector<FactEvent> corrected;
    for (size_t index = 0; index < visible.size();) {
      size_t next = index + 1;
      while (next < visible.size() &&
             visible[next].valid_from == visible[index].valid_from) {
        ++next;
      }
      corrected.push_back(visible[next - 1]);
      index = next;
    }
    return corrected;
  }

  std::vector<OracleStateInterval> History(const FactRef& ref,
                                            CommitSeq snapshot_seq) const {
    const std::vector<FactEvent> corrected =
        CorrectedEvents(ref, snapshot_seq);
    bool is_present = false;
    std::optional<Value> value;
    std::vector<OracleStateInterval> history;
    for (size_t index = 0; index < corrected.size(); ++index) {
      is_present = corrected[index].operation == FactOperation::kPut;
      if (is_present) {
        value = corrected[index].value;
      } else {
        value.reset();
      }
      const std::optional<ValidTime> to =
          index + 1 == corrected.size()
              ? std::nullopt
              : std::optional<ValidTime>(corrected[index + 1].valid_from);
      if (!is_present) continue;
      if (!history.empty() && history.back().value == value &&
          history.back().interval.to.has_value() &&
          *history.back().interval.to == corrected[index].valid_from) {
        history.back().interval.to = to;
      } else {
        history.push_back({{corrected[index].valid_from, to}, value});
      }
    }
    return history;
  }

  std::vector<OracleChange> Changes(const FactRef& ref,
                                    CommitSeq snapshot_seq) const {
    const std::vector<FactEvent> corrected =
        CorrectedEvents(ref, snapshot_seq);
    std::optional<Value> current;
    std::vector<OracleChange> changes;
    for (const FactEvent& event : corrected) {
      std::optional<Value> next =
          event.operation == FactOperation::kPut ? event.value : std::nullopt;
      if (next != current) {
        changes.push_back({event.valid_from, current, next});
      }
      current = std::move(next);
    }
    return changes;
  }

  OracleRows Evaluate(const OracleLogicalQuery& query,
                      CommitSeq snapshot_seq) const {
    OracleRows rows;
    for (const FactRef& ref : query.refs) {
      for (const OracleStateInterval& state : History(ref, snapshot_seq)) {
        if (query.interval.has_value()) {
          const auto& a = *query.interval;
          const uint64_t end = state.interval.to.value_or(ValidTime{UINT64_MAX}).value;
          const uint64_t clip_end = a.to.value_or(ValidTime{UINT64_MAX}).value;
          const uint64_t from = std::max(state.interval.from.value, a.from.value);
          if (from >= std::min(end, clip_end)) continue;
          rows.push_back({ref, {ValidTime{from},
                                (state.interval.to && a.to)
                                    ? std::optional<ValidTime>(ValidTime{std::min(end, clip_end)})
                                    : (state.interval.to ? state.interval.to : a.to)},
                          state.value, std::nullopt});
        } else {
          rows.push_back({ref, state.interval, state.value, std::nullopt});
        }
      }
    }
    std::sort(rows.begin(), rows.end(), [](const OracleRow& a, const OracleRow& b) {
      if (a.interval.from != b.interval.from) return a.interval.from.value < b.interval.from.value;
      if (a.ref.part_id() != b.ref.part_id()) return a.ref.part_id().value < b.ref.part_id().value;
      return a.ref.entity_id() < b.ref.entity_id();
    });
    return rows;
  }

  OracleRows Expand(const OracleExpandSpec& spec,
                    CommitSeq snapshot_seq) const {
    OracleRows rows;
    if (spec.interval.to && spec.interval.from.value >= spec.interval.to->value) {
      return rows;
    }
    for (const auto& edge_state : Edges(snapshot_seq)) {
      const EdgeIdentity& edge = edge_state.identity;
      const VertexRef source = edge.source_ref();
      const VertexRef target = edge.target_ref();
      const bool matches =
          (spec.direction == ExpandDirection::kOut && source == spec.source) ||
          (spec.direction == ExpandDirection::kIn && target == spec.source) ||
          (spec.direction == ExpandDirection::kBoth &&
           (source == spec.source || target == spec.source));
      if (!matches || (spec.edge_type && *spec.edge_type != edge.edge_type)) continue;
      const auto interval = Intersect(spec.interval, edge_state.interval);
      if (!interval) continue;
      rows.push_back({FactRef(edge.home_part_id, FactFamily::kEdgeIdentity,
                              PropertyId{}, edge.edge_id.value), *interval,
                      std::nullopt, edge});
    }
    std::sort(rows.begin(), rows.end(), [](const OracleRow& a, const OracleRow& b) {
      const auto& x = *a.edge_identity;
      const auto& y = *b.edge_identity;
      return EdgeRefLess(x.edge_ref(), y.edge_ref());
    });
    return rows;
  }

  OraclePath CoexistingShortestPath(const OraclePathSpec& spec,
                                    CommitSeq snapshot_seq) const {
    OraclePath result;
    if (spec.source == spec.target) {
      result.vertices.push_back(spec.source);
      result.common = spec.interval;
      return result;
    }
    std::vector<PathCandidate> candidates;
    EnumeratePaths(spec, snapshot_seq, &candidates);
    if (candidates.empty()) return {};
    std::sort(candidates.begin(), candidates.end(), [](const PathCandidate& a,
                                                       const PathCandidate& b) {
      if (a.edges.size() != b.edges.size()) return a.edges.size() < b.edges.size();
      if (a.edges != b.edges) {
        return std::lexicographical_compare(a.edges.begin(), a.edges.end(),
                                            b.edges.begin(), b.edges.end(),
                                            EdgeRefLess);
      }
      return IntervalLess(a.interval, b.interval);
    });
    result.vertices = candidates.front().vertices;
    result.edges = candidates.front().edges;
    result.common = candidates.front().interval;
    return result;
  }

  OracleJourney EarliestArrival(const OracleJourneySpec& spec,
                                 CommitSeq snapshot_seq) const {
    return Journey(spec, snapshot_seq, 0);
  }
  OracleJourney LatestDeparture(const OracleJourneySpec& spec,
                                CommitSeq snapshot_seq) const {
    return Journey(spec, snapshot_seq, 1);
  }
  OracleJourney FastestDuration(const OracleJourneySpec& spec,
                                CommitSeq snapshot_seq) const {
    return Journey(spec, snapshot_seq, 2);
  }

 private:
  struct EdgeState {
    EdgeIdentity identity;
    ValidTimeInterval interval;
  };
  struct PathCandidate {
    std::vector<VertexRef> vertices;
    std::vector<EdgeRef> edges;
    ValidTimeInterval interval;
  };

  static std::optional<ValidTimeInterval> Intersect(
      const ValidTimeInterval& a, const ValidTimeInterval& b) {
    const uint64_t a_end = a.to ? a.to->value : UINT64_MAX;
    const uint64_t b_end = b.to ? b.to->value : UINT64_MAX;
    const uint64_t from = std::max(a.from.value, b.from.value);
    const uint64_t end = std::min(a_end, b_end);
    if (from >= end) return std::nullopt;
    return ValidTimeInterval{ValidTime{from},
                             end == UINT64_MAX ? std::nullopt
                                               : std::optional<ValidTime>(ValidTime{end})};
  }

  std::vector<EdgeState> Edges(CommitSeq snapshot_seq) const {
    std::vector<EdgeIdentity> identities;
    std::set<EdgeRef, decltype(&EdgeRefLess)> seen(&EdgeRefLess);
    for (const FactEvent& event : events_) {
      if (event.commit_seq.value > snapshot_seq.value ||
          event.ref.family() != FactFamily::kEdgeIdentity ||
          !event.edge_identity.has_value()) continue;
      const EdgeRef edge_ref = event.edge_identity->edge_ref();
      if (seen.contains(edge_ref)) continue;
      const auto current = Read(event.ref, ValidTime{UINT64_MAX}, snapshot_seq);
      if (!current || current->operation == FactOperation::kDelete ||
          !current->edge_identity) continue;
      identities.push_back(*current->edge_identity);
      seen.insert(edge_ref);
    }
    std::vector<EdgeState> result;
    for (const EdgeIdentity& identity : identities) {
      const FactRef state_ref = EntityFact::Edge(identity.edge_ref()).ref();
      for (const OracleStateInterval& state : History(state_ref, snapshot_seq)) {
        result.push_back({identity, state.interval});
      }
    }
    std::sort(result.begin(), result.end(), [](const EdgeState& a, const EdgeState& b) {
      return EdgeRefLess(a.identity.edge_ref(), b.identity.edge_ref());
    });
    return result;
  }

  void EnumeratePaths(const OraclePathSpec& spec, CommitSeq snapshot_seq,
                      std::vector<PathCandidate>* output) const {
    if (spec.interval.to && spec.interval.from.value >= spec.interval.to->value) return;
    std::vector<VertexRef> vertices{spec.source};
    std::vector<EdgeRef> edges;
    std::vector<VertexRef> seen{spec.source};
    std::function<void(VertexRef, ValidTimeInterval)> visit =
        [&](VertexRef current, ValidTimeInterval interval) {
          if (edges.size() >= spec.max_hops) return;
          for (const OracleRow& row : Expand(
                   {current, interval, spec.direction, spec.edge_type, 1},
                   snapshot_seq)) {
            const EdgeIdentity& identity = *row.edge_identity;
            VertexRef next = identity.target_ref();
            if (spec.direction == ExpandDirection::kIn) next = identity.source_ref();
            if (spec.direction == ExpandDirection::kBoth && current == identity.target_ref()) {
              next = identity.source_ref();
            }
            if (std::find(seen.begin(), seen.end(), next) != seen.end()) continue;
            const auto common = Intersect(interval, row.interval);
            if (!common) continue;
            seen.push_back(next); vertices.push_back(next); edges.push_back(identity.edge_ref());
            if (next == spec.target) output->push_back({vertices, edges, *common});
            else visit(next, *common);
            edges.pop_back(); vertices.pop_back(); seen.pop_back();
          }
        };
    visit(spec.source, spec.interval);
  }

  OracleJourney Journey(const OracleJourneySpec& spec, CommitSeq snapshot_seq,
                        int objective) const {
    OracleJourney result;
    std::vector<PathCandidate> candidates;
    EnumeratePaths(spec, snapshot_seq, &candidates);
    std::vector<OracleJourney> journeys;
    for (const PathCandidate& path : candidates) {
      OracleJourney candidate;
      candidate.vertices = path.vertices; candidate.edges = path.edges;
      candidate.departures.resize(path.edges.size()); candidate.arrivals.resize(path.edges.size());
      uint64_t departure = spec.interval.from.value;
      if (objective == 1 && spec.interval.to) departure = spec.interval.to->value;
      uint64_t total = 0;
      bool valid = true;
      for (size_t i = 0; i < path.edges.size(); ++i) {
        const size_t edge_index = objective == 1 ? path.edges.size() - 1 - i : i;
        uint64_t duration = DurationAt(path.edges[edge_index], ValidTime{departure}, spec, snapshot_seq);
        if (objective == 1) {
          if (duration > departure) { valid = false; break; }
          departure -= duration;
          candidate.departures[path.edges.size() - 1 - i] = ValidTime{departure};
          candidate.arrivals[path.edges.size() - 1 - i] = ValidTime{departure + duration};
        } else {
          candidate.departures[i] = ValidTime{departure};
          if (duration > UINT64_MAX - departure) { valid = false; break; }
          departure += duration; total += duration;
          candidate.arrivals[i] = ValidTime{departure};
        }
      }
      if (!valid || candidate.edges.empty()) continue;
      candidate.initial_departure = objective == 1 ? ValidTime{departure} : spec.interval.from;
      candidate.final_arrival = objective == 1 ? spec.interval.to.value_or(ValidTime{departure}) : ValidTime{departure};
      candidate.departure = candidate.initial_departure; candidate.arrival = candidate.final_arrival;
      candidate.duration = ValidDuration{objective == 1 ? candidate.final_arrival.value - candidate.initial_departure.value : total};
      journeys.push_back(std::move(candidate));
    }
    if (journeys.empty()) return {};
    std::sort(journeys.begin(), journeys.end(), [objective](const OracleJourney& a, const OracleJourney& b) {
      if (objective == 0 && a.final_arrival != b.final_arrival) return a.final_arrival.value < b.final_arrival.value;
      if (objective == 1 && a.initial_departure != b.initial_departure) return a.initial_departure.value > b.initial_departure.value;
      if (objective == 2 && a.duration != b.duration) return a.duration.value < b.duration.value;
      if (a.edges.size() != b.edges.size()) return a.edges.size() < b.edges.size();
      return std::lexicographical_compare(a.edges.begin(), a.edges.end(), b.edges.begin(), b.edges.end(), EdgeRefLess);
    });
    return journeys.front();
  }

  uint64_t DurationAt(const EdgeRef& edge, ValidTime at,
                      const OracleJourneySpec& spec, CommitSeq snapshot_seq) const {
    if (!spec.duration_property) return 1;
    auto value = Read(PropertyFact::Edge(edge, *spec.duration_property).ref(), at, snapshot_seq);
    if (!value || !value->value || value->value->type() != PhysicalType::kInt64) return 1;
    const int64_t raw = std::get<int64_t>(value->value->data());
    return raw >= 0 ? static_cast<uint64_t>(raw) : 1;
  }

  std::vector<FactEvent> events_;
};

}  // namespace cedar::test

#endif  // CEDAR_TESTS_MODEL_BITEMPORAL_FACT_ORACLE_H_
