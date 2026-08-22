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

class BitemporalFactOracle {
 public:
  void Add(FactEvent event) { events_.push_back(std::move(event)); }

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
    for (const EdgeIdentity& edge : Edges(snapshot_seq)) {
      const VertexRef source = edge.source_ref();
      const VertexRef target = edge.target_ref();
      const bool matches =
          (spec.direction == ExpandDirection::kOut && source == spec.source) ||
          (spec.direction == ExpandDirection::kIn && target == spec.source) ||
          (spec.direction == ExpandDirection::kBoth &&
           (source == spec.source || target == spec.source));
      if (!matches || (spec.edge_type && *spec.edge_type != edge.edge_type)) continue;
      rows.push_back({FactRef(edge.home_part_id, FactFamily::kEdgeIdentity,
                              PropertyId{}, edge.edge_id.value),
                      spec.interval, std::nullopt, edge});
    }
    std::sort(rows.begin(), rows.end(), [](const OracleRow& a, const OracleRow& b) {
      const auto& x = *a.edge_identity;
      const auto& y = *b.edge_identity;
      if (x.source_ref() != y.source_ref()) return x.source_ref().vertex_id.value < y.source_ref().vertex_id.value;
      return x.edge_id.value < y.edge_id.value;
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
    std::vector<VertexRef> vertices{spec.source};
    std::vector<EdgeRef> edges;
    std::vector<VertexRef> seen{spec.source};
    OraclePath best;
    std::function<void(VertexRef)> visit = [&](VertexRef current) {
      if (edges.size() >= spec.max_hops) return;
      OracleExpandSpec expand{current, spec.interval, spec.direction,
                              spec.edge_type, 1};
      for (const OracleRow& row : Expand(expand, snapshot_seq)) {
        const EdgeIdentity& identity = *row.edge_identity;
        VertexRef next = identity.target_ref();
        if (spec.direction == ExpandDirection::kIn) next = identity.source_ref();
        if (spec.direction == ExpandDirection::kBoth && current == identity.target_ref()) {
          next = identity.source_ref();
        }
        if (std::find(seen.begin(), seen.end(), next) != seen.end()) continue;
        seen.push_back(next);
        vertices.push_back(next);
        edges.push_back(identity.edge_ref());
        if (next == spec.target && (best.vertices.empty() || edges.size() < best.edges.size())) {
          best.vertices = vertices;
          best.edges = edges;
          best.common = spec.interval;
        } else {
          visit(next);
        }
        edges.pop_back();
        vertices.pop_back();
        seen.pop_back();
      }
    };
    visit(spec.source);
    return best;
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
  std::vector<EdgeIdentity> Edges(CommitSeq snapshot_seq) const {
    std::vector<std::pair<CommitSeq, EdgeIdentity>> latest;
    for (const FactEvent& event : events_) {
      if (event.commit_seq.value > snapshot_seq.value ||
          event.ref.family() != FactFamily::kEdgeIdentity ||
          !event.edge_identity.has_value()) continue;
      const EdgeRef ref = event.edge_identity->edge_ref();
      auto it = std::find_if(latest.begin(), latest.end(), [&](const auto& value) {
        return value.second.edge_ref() == ref;
      });
      if (it == latest.end() || it->first.value < event.commit_seq.value) {
        if (event.operation == FactOperation::kDelete) {
          if (it != latest.end()) latest.erase(it);
        } else if (it == latest.end()) {
          latest.push_back({event.commit_seq, *event.edge_identity});
        } else {
          *it = {event.commit_seq, *event.edge_identity};
        }
      }
    }
    std::vector<EdgeIdentity> result;
    for (const auto& entry : latest) result.push_back(entry.second);
    return result;
  }

  OracleJourney Journey(const OracleJourneySpec& spec, CommitSeq snapshot_seq,
                        int objective) const {
    OracleJourney result;
    OraclePath path = CoexistingShortestPath(spec, snapshot_seq);
    if (path.vertices.empty()) return result;
    result.vertices = path.vertices;
    result.edges = path.edges;
    result.departures.reserve(path.edges.size());
    result.arrivals.reserve(path.edges.size());
    uint64_t departure = spec.interval.from.value;
    uint64_t total = 0;
    for (const EdgeRef& edge : path.edges) {
      uint64_t duration = 1;
      if (spec.duration_property) {
        auto value = Read(PropertyFact::Edge(edge, *spec.duration_property).ref(),
                          ValidTime{departure}, snapshot_seq);
        if (value && value->value.has_value() &&
            value->value->type() == PhysicalType::kInt64) {
          const int64_t raw = std::get<int64_t>(value->value->data());
          if (raw >= 0) duration = static_cast<uint64_t>(raw);
        }
      }
      result.departures.push_back(ValidTime{departure});
      if (duration > UINT64_MAX - departure) return OracleJourney{};
      departure += duration;
      total += duration;
      result.arrivals.push_back(ValidTime{departure});
    }
    if (result.arrivals.empty()) return OracleJourney{};
    result.initial_departure = spec.interval.from;
    result.final_arrival = ValidTime{departure};
    result.departure = result.initial_departure;
    result.arrival = result.final_arrival;
    result.duration = ValidDuration{total};
    (void)objective;
    return result;
  }

  std::vector<FactEvent> events_;
};

}  // namespace cedar::test

#endif  // CEDAR_TESTS_MODEL_BITEMPORAL_FACT_ORACLE_H_
