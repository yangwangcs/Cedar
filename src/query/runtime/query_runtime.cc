// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/runtime/query_runtime.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <map>
#include <optional>
#include <set>
#include <tuple>
#include <type_traits>
#include <utility>
#include <time.h>

#include "query/logical/logical_plan.h"
#include "query/runtime/property_binding.h"
#include "query/runtime/temporal_source.h"
#include "query/runtime/graph_frontier.h"
#include "query/runtime/journey.h"
#include "query/resource/query_scratch.h"

namespace cedar {
namespace {

std::atomic<uint64_t> g_next_query_id{1};

uint64_t ThreadCpuMicros() {
  struct timespec ts;
  if (::clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) != 0) return 0;
  return static_cast<uint64_t>(ts.tv_sec) * 1000000ULL +
         static_cast<uint64_t>(ts.tv_nsec) / 1000ULL;
}

std::pair<uint32_t, internal::QueryMetricOperator> ProfileOperatorFor(
    const internal::PreparedQueryPlan& plan) {
  internal::PhysicalOpKind physical = internal::PhysicalOpKind::kCanonicalScan;
  if (plan.physical_plan && !plan.physical_plan->operations.empty()) {
    physical = plan.physical_plan->operations.back();
  }
  const uint32_t id = static_cast<uint32_t>(physical) + 1;
  switch (physical) {
    case internal::PhysicalOpKind::kFilter: return {id, internal::QueryMetricOperator::kFilter};
    case internal::PhysicalOpKind::kProject: return {id, internal::QueryMetricOperator::kProject};
    case internal::PhysicalOpKind::kAggregate: return {id, internal::QueryMetricOperator::kAggregate};
    case internal::PhysicalOpKind::kSort: return {id, internal::QueryMetricOperator::kSort};
    case internal::PhysicalOpKind::kAdjacencySeek: return {id, internal::QueryMetricOperator::kExpand};
    default: return {id, internal::QueryMetricOperator::kScan};
  }
}

struct RuntimeRow {
  FactRef ref;
  std::optional<ValidTimeInterval> effective;
  std::optional<ValidTime> point;
  std::optional<Value> property_value;
  std::optional<VertexRef> graph_source;
  std::optional<EdgeRef> graph_edge;
  std::optional<VertexRef> graph_destination;
  uint64_t graph_edge_type = 0;
  std::map<uint32_t, std::optional<Value>> property_values;
  std::optional<PathValue> path;
  std::optional<JourneyValue> journey;
};

StatusOr<uint64_t> PropertyPayloadBytes(const RuntimeRow& row) {
  uint64_t bytes = 0;
  auto add = [&bytes](const std::optional<Value>& value) -> Status {
    if (!value.has_value() ||
        (value->type() != PhysicalType::kString &&
         value->type() != PhysicalType::kBinary)) {
      return Status::OK();
    }
    const uint64_t size = std::get<std::string>(value->data()).size();
    if (size > std::numeric_limits<uint64_t>::max() - bytes) {
      return Status::ResourceExhausted("query", "property payload accounting overflow");
    }
    bytes += size;
    return Status::OK();
  };
  // Canonical bindings are keyed by output slot. The legacy field aliases the
  // first binding, so count it only when no canonical map is present.
  if (!row.property_values.empty()) {
    for (const auto& [slot, value] : row.property_values) {
      (void)slot;
      if (Status status = add(value); !status.ok()) return status;
    }
  } else if (Status status = add(row.property_value); !status.ok()) {
    return status;
  }
  return bytes;
}

using EvaluatedLiteral = detail::ExpressionLiteral;

struct EvaluatedValue {
  QueryType type;
  bool present;
  std::optional<EvaluatedLiteral> value;
};

StatusOr<EvaluatedValue> ValueAsLiteral(const Value& value) {
  switch (value.type()) {
    case PhysicalType::kBool:
      return EvaluatedValue{QueryType::kBool, true,
                            EvaluatedLiteral{std::get<bool>(value.data())}};
    case PhysicalType::kInt32:
      return EvaluatedValue{QueryType::kInt32, true,
                            EvaluatedLiteral{std::get<int32_t>(value.data())}};
    case PhysicalType::kInt64:
      return EvaluatedValue{QueryType::kInt64, true,
                            EvaluatedLiteral{std::get<int64_t>(value.data())}};
    case PhysicalType::kFloat32:
      return EvaluatedValue{QueryType::kFloat32, true,
                            EvaluatedLiteral{std::get<float>(value.data())}};
    case PhysicalType::kFloat64:
      return EvaluatedValue{QueryType::kFloat64, true,
                            EvaluatedLiteral{std::get<double>(value.data())}};
    case PhysicalType::kTimestamp64:
      return EvaluatedValue{
          QueryType::kTimestamp64, true,
          EvaluatedLiteral{Timestamp64{std::get<uint64_t>(value.data())}}};
    case PhysicalType::kString:
      return EvaluatedValue{
          QueryType::kString, true,
          EvaluatedLiteral{std::get<std::string>(value.data())}};
    case PhysicalType::kBinary:
      return EvaluatedValue{
          QueryType::kBinary, true,
          EvaluatedLiteral{Binary{std::get<std::string>(value.data())}}};
  }
  return Status::Corruption("query", "unknown property physical type");
}

StatusOr<EvaluatedValue> EvaluateExpression(
    const internal::ExpressionNode& expression, const RuntimeRow& row,
    const internal::PreparedQueryPlan& plan, const Bindings& bindings) {
  using internal::ExpressionKind;
  if (expression.kind() == ExpressionKind::kSlot) {
    if (expression.slot() == plan.entity_slot) {
      if (plan.entity_family == FactFamily::kVertexState) {
        return EvaluatedValue{
            QueryType::kVertexRef, true,
            EvaluatedLiteral{VertexRef{row.ref.part_id(),
                                       VertexId{row.ref.entity_id()}}}};
      }
      return EvaluatedValue{
          QueryType::kEdgeRef, true,
          EvaluatedLiteral{
              EdgeRef{row.ref.part_id(), EdgeId{row.ref.entity_id()}}}};
    }
    const auto binding = std::find_if(
        plan.property_bindings.begin(), plan.property_bindings.end(),
        [&expression](const internal::PreparedPropertyBinding& candidate) {
          return candidate.output.slot == expression.slot();
        });
    if (binding == plan.property_bindings.end()) {
      return Status::InvalidArgument("query", "predicate slot is unavailable");
    }
    std::optional<Value> property_value = row.property_value;
    if (const auto found = row.property_values.find(binding->output.slot.value);
        found != row.property_values.end()) {
      property_value = found->second;
    }
    if (!property_value.has_value()) {
      return EvaluatedValue{binding->output.type, false, std::nullopt};
    }
    return ValueAsLiteral(*property_value);
  }
  if (expression.kind() == ExpressionKind::kLiteral) {
    if (!expression.literal().has_value()) {
      return Status::InvalidArgument("query", "literal expression has no value");
    }
    return EvaluatedValue{expression.type(), true, expression.literal()};
  }
  if (expression.kind() == ExpressionKind::kParameter) {
    auto bound = bindings.Lookup(expression.parameter(), expression.type());
    if (!bound.ok()) return bound.status();
    return std::visit(
        [](const auto& value) -> StatusOr<EvaluatedValue> {
          using T = std::decay_t<decltype(value)>;
          if constexpr (std::is_same_v<T, Value>) {
            return ValueAsLiteral(value);
          } else if constexpr (std::is_same_v<T, VertexRef>) {
            return EvaluatedValue{QueryType::kVertexRef, true,
                                  EvaluatedLiteral{value}};
          } else if constexpr (std::is_same_v<T, EdgeRef>) {
            return EvaluatedValue{QueryType::kEdgeRef, true,
                                  EvaluatedLiteral{value}};
          } else if constexpr (std::is_same_v<T, ValidTime>) {
            return EvaluatedValue{QueryType::kValidTime, true,
                                  EvaluatedLiteral{value}};
          } else if constexpr (std::is_same_v<T, ValidDuration>) {
            return EvaluatedValue{QueryType::kValidDuration, true,
                                  EvaluatedLiteral{value}};
          } else if constexpr (std::is_same_v<T, CommitSeq>) {
            return EvaluatedValue{QueryType::kCommitSeq, true,
                                  EvaluatedLiteral{value}};
          } else {
            return EvaluatedValue{QueryType::kValidTimeInterval, true,
                                  EvaluatedLiteral{value}};
          }
        },
        bound.ValueOrDie());
  }
  if (expression.kind() == ExpressionKind::kIsPresent) {
    auto child = EvaluateExpression(*expression.children().front(), row, plan,
                                    bindings);
    if (!child.ok()) return child.status();
    return EvaluatedValue{QueryType::kBool, true,
                          EvaluatedLiteral{child.ValueOrDie().present}};
  }
  if (expression.kind() == ExpressionKind::kNot) {
    auto child = EvaluateExpression(*expression.children().front(), row, plan,
                                    bindings);
    if (!child.ok()) return child.status();
    const bool value = child.ValueOrDie().present &&
                       std::get<bool>(*child.ValueOrDie().value);
    return EvaluatedValue{QueryType::kBool, true, EvaluatedLiteral{!value}};
  }

  auto left = EvaluateExpression(*expression.children()[0], row, plan, bindings);
  if (!left.ok()) return left.status();
  auto right = EvaluateExpression(*expression.children()[1], row, plan, bindings);
  if (!right.ok()) return right.status();
  if (expression.kind() == ExpressionKind::kAnd) {
    const bool value = left.ValueOrDie().present &&
                       right.ValueOrDie().present &&
                       std::get<bool>(*left.ValueOrDie().value) &&
                       std::get<bool>(*right.ValueOrDie().value);
    return EvaluatedValue{QueryType::kBool, true, EvaluatedLiteral{value}};
  }
  if (!left.ValueOrDie().present || !right.ValueOrDie().present) {
    return EvaluatedValue{QueryType::kBool, true, EvaluatedLiteral{false}};
  }
  if (left.ValueOrDie().type != right.ValueOrDie().type) {
    return Status::InvalidArgument("query", "comparison operand types differ");
  }

  bool value = false;
  if (expression.kind() == ExpressionKind::kEqual) {
    value = *left.ValueOrDie().value == *right.ValueOrDie().value;
  } else if (expression.kind() == ExpressionKind::kNotEqual) {
    value = *left.ValueOrDie().value != *right.ValueOrDie().value;
  } else if (expression.kind() == ExpressionKind::kGreaterThan) {
    switch (left.ValueOrDie().type) {
      case QueryType::kInt32:
        value = std::get<int32_t>(*left.ValueOrDie().value) >
                std::get<int32_t>(*right.ValueOrDie().value);
        break;
      case QueryType::kInt64:
        value = std::get<int64_t>(*left.ValueOrDie().value) >
                std::get<int64_t>(*right.ValueOrDie().value);
        break;
      case QueryType::kFloat32:
        value = std::get<float>(*left.ValueOrDie().value) >
                std::get<float>(*right.ValueOrDie().value);
        break;
      case QueryType::kFloat64:
        value = std::get<double>(*left.ValueOrDie().value) >
                std::get<double>(*right.ValueOrDie().value);
        break;
      default:
        return Status::InvalidArgument(
            "query", "greater-than requires an arithmetic operand");
    }
  } else {
    return Status::NotSupported("query", "unsupported canonical expression");
  }
  return EvaluatedValue{QueryType::kBool, true, EvaluatedLiteral{value}};
}

StatusOr<std::vector<RuntimeRow>> ReadSourceRows(
    Snapshot& snapshot, const internal::PreparedQueryPlan& plan,
    internal::QueryReservation* reservation = nullptr) {
  std::vector<RuntimeRow> result;
  auto append = [&result, reservation](RuntimeRow row) -> Status {
    if (reservation != nullptr) {
      Status status = reservation->ReserveMemory(sizeof(RuntimeRow));
      if (!status.ok()) return status;
    }
    result.push_back(std::move(row));
    return Status::OK();
  };
  return std::visit(
      [&](const auto& scope) -> StatusOr<std::vector<RuntimeRow>> {
        using T = std::decay_t<decltype(scope)>;
        if constexpr (std::is_same_v<T, At>) {
          auto rows = internal::TemporalSource::ReadAt(
              snapshot, plan.entity_family, PropertyId{}, scope.time);
          if (!rows.ok()) return rows.status();
          for (internal::StateRow& row : rows.ValueOrDie()) {
            if (Status status = append({row.ref, row.effective, scope.time, std::nullopt}); !status.ok()) return status;
          }
        } else if constexpr (std::is_same_v<T, Events>) {
          auto rows = internal::TemporalSource::ReadEvents(
              snapshot, plan.entity_family, PropertyId{}, scope.interval);
          if (!rows.ok()) return rows.status();
          for (const internal::EventRow& row : rows.ValueOrDie()) {
            if (Status status = append({row.ref, std::nullopt, row.valid_from, std::nullopt}); !status.ok()) return status;
          }
        } else if constexpr (std::is_same_v<T, Changes>) {
          auto rows = internal::TemporalSource::ReadChanges(
              snapshot, plan.entity_family, PropertyId{}, scope.interval);
          if (!rows.ok()) return rows.status();
          for (const internal::ChangeRow& row : rows.ValueOrDie()) {
            if (Status status = append({row.ref, std::nullopt, row.valid_from, std::nullopt}); !status.ok()) return status;
          }
        } else if constexpr (std::is_same_v<T, Overlaps>) {
          auto rows = internal::TemporalSource::ReadOverlaps(
              snapshot, plan.entity_family, PropertyId{}, scope.interval);
          if (!rows.ok()) return rows.status();
          for (internal::StateRow& row : rows.ValueOrDie()) {
            if (Status status = append({row.ref, row.effective, std::nullopt, std::nullopt}); !status.ok()) return status;
          }
        } else if constexpr (std::is_same_v<T, Throughout>) {
          auto rows = internal::TemporalSource::ReadThroughout(
              snapshot, plan.entity_family, PropertyId{}, scope.interval);
          if (!rows.ok()) return rows.status();
          for (internal::StateRow& row : rows.ValueOrDie()) {
            if (Status status = append({row.ref, row.effective, std::nullopt, std::nullopt}); !status.ok()) return status;
          }
        } else {
          auto rows = internal::TemporalSource::ReadHistory(
              snapshot, plan.entity_family, PropertyId{}, scope.interval);
          if (!rows.ok()) return rows.status();
          for (internal::StateRow& row : rows.ValueOrDie()) {
            if (Status status = append({row.ref, row.effective, std::nullopt, std::nullopt}); !status.ok()) return status;
          }
        }
        return result;
      },
      plan.scope);
}

StatusOr<std::vector<RuntimeRow>> ReadProjectionRows(
    const internal::PreparedQueryPlan& plan) {
  if (!plan.physical_plan || !plan.projection_reader) {
    return Status::NotFound("query runtime", "projection reader is unavailable");
  }
  std::vector<RuntimeRow> rows;
  bool found = false;
  for (const auto& slice : plan.physical_plan->coverage_slices) {
    if (slice.source == internal::CoverageSource::kCanonical) {
      continue;
    }
    auto chains = plan.projection_reader(slice);
    if (!chains.ok()) return chains.status();
    found = true;
    std::optional<internal::QueryDeltaView> delta;
    if (slice.source == internal::CoverageSource::kDeltaMerge) {
      if (!plan.delta_reader) return Status::NotFound("query runtime", "delta reader unavailable");
      if (plan.bound_delta_view) {
        delta = *plan.bound_delta_view;
      } else {
        auto acquired = plan.delta_reader();
        if (!acquired.ok()) return acquired.status();
        delta = std::move(acquired).ConsumeValueOrDie();
      }
    }
    if (slice.source == internal::CoverageSource::kDeltaMerge) {
      // A projection may be split across pages/chains. Merge all boundaries
      // for one logical fact before materializing state; merging per chain
      // would reset state at every page and duplicate/truncate intervals.
      struct FactKey {
        PartId part;
        uint64_t entity = 0;
        PropertyId property;
        bool operator<(const FactKey& other) const {
          if (part.value != other.part.value) return part.value < other.part.value;
          if (entity != other.entity) return entity < other.entity;
          return property.value < other.property.value;
        }
      };
      std::map<FactKey, std::vector<internal::CorrectedBoundary>> base_by_fact;
      std::map<FactKey, FactRef> refs;
      for (const internal::ProjectionChain& chain : chains.ValueOrDie()) {
        if (chain.header.base_seq != delta->base_seq ||
            (slice.projection_base.has_value() &&
             chain.header.base_seq != *slice.projection_base)) {
          return Status::Conflict(
              "query runtime",
              "projection and delta base sequences must match exactly");
        }
        const PropertyId property = slice.property_id.value_or(chain.header.property_id);
        for (const auto& interval : chain.intervals) {
          const FactKey key{chain.header.part_id, interval.entity_id, property};
          auto& base = base_by_fact[key];
          base.push_back({interval.effective.from, chain.header.base_seq,
                          FactOperation::kPut, 0, interval.value, std::nullopt});
          if (interval.effective.to) {
            base.push_back({*interval.effective.to, chain.header.base_seq,
                            FactOperation::kDelete, 0, std::nullopt, std::nullopt});
          }
          refs.emplace(key, FactRef{chain.header.part_id, plan.entity_family,
                                    property, interval.entity_id});
        }
      }
      for (auto& [key, base] : base_by_fact) {
        std::sort(base.begin(), base.end(), [](const auto& a, const auto& b) {
          if (a.valid_from != b.valid_from) {
            return a.valid_from.value < b.valid_from.value;
          }
          return a.commit_seq.value < b.commit_seq.value;
        });
        const FactRef& ref = refs.at(key);
        auto merged = internal::QueryDelta::MergeBoundaries(
            base, delta->EventsFor(ref), delta->through);
        if (!merged.ok()) return merged.status();
        for (const auto& state : internal::MaterializePresentState(
                 merged.ValueOrDie())) {
          rows.push_back({ref, state.interval, std::nullopt, state.value});
        }
      }
      continue;
    }
    for (const internal::ProjectionChain& chain : chains.ValueOrDie()) {
      for (const internal::ProjectionInterval& interval : chain.intervals) {
        const FactFamily family = plan.entity_family;
        rows.push_back({FactRef{chain.header.part_id, family, PropertyId{},
                                interval.entity_id},
                        interval.effective, std::nullopt, interval.value});
      }
    }
  }
  if (!found) return Status::NotFound("query runtime", "projection slice is unavailable");
  return rows;
}

bool RowInInterval(const RuntimeRow& row, const ValidTimeInterval& interval) {
  const uint64_t from = row.point ? row.point->value
                                  : row.effective ? row.effective->from.value : 0;
  const uint64_t to = row.point ? from + 1
                                : row.effective && row.effective->to
                                      ? row.effective->to->value
                                      : std::numeric_limits<uint64_t>::max();
  const uint64_t end = interval.to ? interval.to->value
                                   : std::numeric_limits<uint64_t>::max();
  return from < end && interval.from.value < to;
}

std::optional<RuntimeRow> ClipRowToInterval(
    const RuntimeRow& row, const ValidTimeInterval& interval) {
  if (row.point.has_value()) {
    return RowInInterval(row, interval) ? std::optional<RuntimeRow>(row)
                                        : std::nullopt;
  }
  if (!row.effective.has_value()) return std::nullopt;
  const uint64_t from = std::max(row.effective->from.value,
                                 interval.from.value);
  const std::optional<uint64_t> row_to =
      row.effective->to ? std::optional<uint64_t>(row.effective->to->value)
                        : std::nullopt;
  std::optional<uint64_t> to = interval.to
                                   ? std::optional<uint64_t>(interval.to->value)
                                   : row_to;
  if (row_to && (!to || *row_to < *to)) to = row_to;
  if (to && from >= *to) return std::nullopt;
  RuntimeRow clipped = row;
  clipped.effective = ValidTimeInterval{
      ValidTime{from}, to ? std::optional<ValidTime>(ValidTime{*to})
                          : std::nullopt};
  return clipped;
}

StatusOr<std::vector<RuntimeRow>> BindPropertyRows(
    Snapshot& snapshot, std::vector<RuntimeRow> rows,
    const std::vector<internal::PreparedPropertyBinding>& bindings) {
  // Each binding is a temporal join against the same entity row. Applying
  // them in plan order intersects effective intervals while carrying values
  // already materialized for earlier output slots.
  for (size_t binding_index = 0; binding_index < bindings.size();
       ++binding_index) {
    const internal::PreparedPropertyBinding& binding = bindings[binding_index];
    if (!binding.definition.has_value()) {
      return Status::SchemaMismatch("query", "property binding was not prepared");
    }
    // Bind the complete entity batch in one temporal source pass. Calling
    // BindIntervals once per row turns a property predicate into an N+1 scan
    // over the full property family and can dominate query execution.
    std::vector<internal::StateRow> entities;
    entities.reserve(rows.size());
    for (const RuntimeRow& row : rows) {
      const ValidTimeInterval entity_interval = row.effective.value_or(
          ValidTimeInterval{row.point.value_or(ValidTime{0}), std::nullopt});
      entities.emplace_back(row.ref, entity_interval, std::nullopt);
    }
    auto bound = internal::PropertyBinder::BindIntervals(
        snapshot, entities, *binding.definition);
    if (!bound.ok()) return bound.status();
    using RefKey = std::tuple<uint32_t, uint8_t, uint16_t, uint64_t>;
    auto key_for = [](const FactRef& ref) {
      return RefKey{ref.part_id().value, static_cast<uint8_t>(ref.family()),
                    ref.property_id().value, ref.entity_id()};
    };
    std::map<RefKey, std::vector<const internal::BoundPropertyRow*>> by_ref;
    for (const auto& property : bound.ValueOrDie()) {
      by_ref[key_for(property.ref)].push_back(&property);
    }
    std::vector<RuntimeRow> result;
    for (const RuntimeRow& row : rows) {
      const ValidTimeInterval entity_interval = row.effective.value_or(
          ValidTimeInterval{row.point.value_or(ValidTime{0}), std::nullopt});
      const auto found = by_ref.find(key_for(row.ref));
      if (found == by_ref.end()) continue;
      for (const internal::BoundPropertyRow* property : found->second) {
        if (row.point.has_value() &&
            !(property->effective.from.value <= row.point->value &&
              (!property->effective.to.has_value() ||
               row.point->value < property->effective.to->value))) {
          continue;
        }
        const auto effective = internal::Intersect(entity_interval,
                                                    property->effective);
        if (!effective.has_value()) continue;
        RuntimeRow bound_row = row;
        bound_row.effective = *effective;
        bound_row.property_values[binding.output.slot.value] = property->value;
        // Keep the old single-value field for one-binding internal callers;
        // canonical consumers resolve values by output slot from the map.
        if (binding_index == 0) bound_row.property_value = property->value;
        result.push_back(std::move(bound_row));
      }
    }
    rows = std::move(result);
  }
  return rows;
}

StatusOr<std::vector<RuntimeRow>> BindGraphPropertyRows(
    Snapshot& snapshot, std::vector<RuntimeRow> rows,
    const internal::PreparedQueryPlan& plan) {
  for (const internal::PreparedPropertyBinding& binding : plan.property_bindings) {
    if (!binding.definition.has_value()) {
      return Status::SchemaMismatch("query", "property binding was not prepared");
    }
    struct GraphEntity {
      const RuntimeRow* row;
      FactRef ref;
      ValidTimeInterval interval;
    };
    std::vector<GraphEntity> entities;
    entities.reserve(rows.size());
    std::vector<internal::StateRow> state_rows;
    state_rows.reserve(rows.size());
    std::vector<RuntimeRow> bound_rows;
    for (const RuntimeRow& row : rows) {
      std::optional<FactRef> entity_ref;
      if (plan.graph_source_slot && binding.source == *plan.graph_source_slot &&
          row.graph_source.has_value()) {
        entity_ref = FactRef(row.graph_source->part_id, FactFamily::kVertexState,
                             PropertyId{}, row.graph_source->vertex_id.value);
      } else if (plan.graph_edge_slot && binding.source == *plan.graph_edge_slot &&
                 row.graph_edge.has_value()) {
        entity_ref = FactRef(row.graph_edge->home_part_id, FactFamily::kEdgeState,
                             PropertyId{}, row.graph_edge->edge_id.value);
      } else if (plan.graph_destination_slot &&
                 binding.source == *plan.graph_destination_slot &&
                 row.graph_destination.has_value()) {
        entity_ref = FactRef(row.graph_destination->part_id, FactFamily::kVertexState,
                             PropertyId{}, row.graph_destination->vertex_id.value);
      } else {
        return Status::NotSupported(
            "query", "graph property binding source is not an expansion endpoint");
      }
      const ValidTimeInterval entity_interval = row.effective.value_or(
          ValidTimeInterval{row.point.value_or(ValidTime{0}), std::nullopt});
      entities.push_back({&row, *entity_ref, entity_interval});
      state_rows.emplace_back(*entity_ref, entity_interval, std::nullopt);
    }
    auto properties = internal::PropertyBinder::BindIntervals(
        snapshot, state_rows, *binding.definition);
    if (!properties.ok()) return properties.status();
    using RefKey = std::tuple<uint32_t, uint8_t, uint16_t, uint64_t>;
    auto key_for = [](const FactRef& ref) {
      return RefKey{ref.part_id().value, static_cast<uint8_t>(ref.family()),
                    ref.property_id().value, ref.entity_id()};
    };
    std::map<RefKey, std::vector<const internal::BoundPropertyRow*>> by_ref;
    for (const auto& property : properties.ValueOrDie()) {
      by_ref[key_for(property.ref)].push_back(&property);
    }
    for (const GraphEntity& entity : entities) {
      const auto found = by_ref.find(key_for(entity.ref));
      if (found == by_ref.end()) continue;
      for (const internal::BoundPropertyRow* property : found->second) {
        auto effective = internal::Intersect(entity.interval, property->effective);
        if (!effective.has_value()) continue;
        RuntimeRow bound = *entity.row;
        bound.effective = *effective;
        bound.property_values[binding.output.slot.value] = property->value;
        bound_rows.push_back(std::move(bound));
      }
    }
    rows = std::move(bound_rows);
  }
  return rows;
}

StatusOr<std::vector<RuntimeRow>> MaterializeRows(
    Snapshot& snapshot, const internal::PreparedQueryPlan& plan,
    const Bindings& bindings,
    internal::QueryReservation* reservation = nullptr) {
  auto reserve_row = [reservation](const RuntimeRow& row) -> Status {
    if (reservation == nullptr) return Status::OK();
    size_t bytes = sizeof(RuntimeRow);
    auto payload = PropertyPayloadBytes(row);
    if (!payload.ok()) return payload.status();
    if (payload.ValueOrDie() > std::numeric_limits<size_t>::max() - bytes) {
      return Status::ResourceExhausted("query", "memory_bytes estimate overflow");
    }
    bytes += static_cast<size_t>(payload.ValueOrDie());
    return reservation->ReserveMemory(bytes);
  };
  StatusOr<std::vector<RuntimeRow>> rows =
      Status::NotFound("query runtime", "canonical source is not required");
  if (plan.physical_plan) {
    StatusOr<std::vector<RuntimeRow>> derived =
        Status::NotFound("query runtime", "projection reader is unavailable");
    if (plan.projection_reader) derived = ReadProjectionRows(plan);
    if (!derived.ok() && !derived.status().IsNotFound()) return derived.status();
    const bool have_derived = derived.ok();
    const bool has_canonical_slice = std::any_of(
        plan.physical_plan->coverage_slices.begin(),
        plan.physical_plan->coverage_slices.end(), [](const auto& slice) {
          return slice.source == internal::CoverageSource::kCanonical;
        });
    // A fully covered plan must stay on its derived source. Canonical rows are
    // read only for explicit fallback slices or when the derived source is
    // unavailable, preserving mixed-slice behavior without an extra scan.
    if (has_canonical_slice || !have_derived) {
      rows = ReadSourceRows(snapshot, plan, reservation);
      if (!rows.ok()) return rows.status();
    }
    std::vector<RuntimeRow> combined;
    for (const auto& slice : plan.physical_plan->coverage_slices) {
      const auto& input = slice.source != internal::CoverageSource::kCanonical &&
                                  have_derived
                              ? derived.ValueOrDie()
                              : rows.ValueOrDie();
      for (const auto& row : input) {
        auto clipped = ClipRowToInterval(row, slice.interval);
        if (clipped) {
          if (Status status = reserve_row(*clipped); !status.ok()) return status;
          combined.push_back(std::move(*clipped));
        }
      }
    }
    rows = std::move(combined);
  } else {
    rows = ReadSourceRows(snapshot, plan, reservation);
    if (!rows.ok()) return rows.status();
  }
  if (!rows.ok()) return rows.status();
  if (!plan.property_bindings.empty()) {
    rows = BindPropertyRows(snapshot, std::move(rows).ConsumeValueOrDie(),
                            plan.property_bindings);
    if (!rows.ok()) return rows.status();
    // PropertyBinder materializes string/binary values from the canonical
    // history. Charge the actual payload before any subsequent filtering or
    // column copy can retain it.
    if (reservation != nullptr) {
      for (const RuntimeRow& row : rows.ValueOrDie()) {
        if (Status status = reserve_row(row); !status.ok()) return status;
      }
    }
  }
  if (plan.predicate) {
    std::vector<RuntimeRow> filtered;
    for (RuntimeRow& row : rows.ValueOrDie()) {
      auto selected = EvaluateExpression(*plan.predicate, row, plan, bindings);
      if (!selected.ok()) return selected.status();
      if (selected.ValueOrDie().present &&
          std::get<bool>(*selected.ValueOrDie().value)) {
        if (Status status = reserve_row(row); !status.ok()) return status;
        filtered.push_back(std::move(row));
      }
    }
    return filtered;
  }
  return std::move(rows).ConsumeValueOrDie();
}

StatusOr<ValidTimeInterval> ScopeAsInterval(const TemporalScope& scope) {
  return std::visit([](const auto& value) -> StatusOr<ValidTimeInterval> {
    using T = std::decay_t<decltype(value)>;
    if constexpr (std::is_same_v<T, At>) {
      if (value.time.value == std::numeric_limits<uint64_t>::max())
        return Status::InvalidArgument("graph expansion", "point time overflows interval");
      return ValidTimeInterval{value.time, ValidTime{value.time.value + 1}};
    } else if constexpr (std::is_same_v<T, History>) {
      if (!value.interval) return Status::NotSupported("graph expansion", "unbounded history requires interval");
      return *value.interval;
    } else {
      return value.interval;
    }
  }, scope);
}

bool ExpressionUsesSlot(const internal::ExpressionNode& expression,
                        SlotId slot) {
  if (expression.kind() == internal::ExpressionKind::kSlot &&
      expression.slot() == slot) {
    return true;
  }
  for (const auto& child : expression.children()) {
    if (child && ExpressionUsesSlot(*child, slot)) return true;
  }
  return false;
}

StatusOr<std::vector<RuntimeRow>> MaterializeGraphRows(
    Snapshot& snapshot, const internal::PreparedQueryPlan& plan,
    internal::QueryReservation* reservation,
    QueryExecutionMode mode,
    const Bindings& bindings,
    const std::function<Status()>& check_abort = {},
    uint64_t max_journey_labels = 0,
    uint64_t max_journey_interval_fragments = 0) {
  if (!plan.graph_expand) {
    return Status::InvalidArgument("graph expansion", "missing graph specification");
  }
  auto interval = ScopeAsInterval(plan.scope);
  if (!interval.ok()) return interval.status();
  auto seeds = ReadSourceRows(snapshot, plan, reservation);
  if (!seeds.ok()) return seeds.status();
  // Graph operators consume the source rows directly, so apply bindings and
  // predicates here before frontier expansion. Otherwise a WHERE clause on a
  // source property is ignored and the journey/path operator searches from
  // every canonical entity (an accidental multiplicative scan).
  std::vector<internal::PreparedPropertyBinding> seed_bindings;
  if (plan.graph_source_slot && plan.predicate) {
    for (const auto& binding : plan.property_bindings) {
      if (binding.source == *plan.graph_source_slot &&
          ExpressionUsesSlot(*plan.predicate, binding.output.slot)) {
        seed_bindings.push_back(binding);
      }
    }
  }
  if (!seed_bindings.empty()) {
    seeds = BindPropertyRows(snapshot, std::move(seeds).ConsumeValueOrDie(),
                             seed_bindings);
    if (!seeds.ok()) return seeds.status();
  }
  if (plan.predicate) {
    std::vector<RuntimeRow> filtered;
    for (RuntimeRow& row : seeds.ValueOrDie()) {
      auto selected = EvaluateExpression(*plan.predicate, row, plan, bindings);
      if (!selected.ok()) return selected.status();
      if (selected.ValueOrDie().present &&
          std::get<bool>(*selected.ValueOrDie().value)) {
        filtered.push_back(std::move(row));
      }
    }
    seeds = std::move(filtered);
  }
  const internal::QueryDeltaView* delta = plan.bound_delta_view ? plan.bound_delta_view.get() : nullptr;
  internal::GraphFrontierOptions options{reservation, delta, plan.graph_k_hops};
  options.check_abort = check_abort;
  options.adjacency_index = snapshot.adjacency_index();
  if (plan.projection_generation.has_value()) {
    options.projection_generation = plan.projection_generation->generation_id();
  }
  // A missing/lagging cache must never silently turn an interactive expansion
  // into an unbounded family scan. The canonical path remains available for
  // plans that do not provide a budget (for example analytical callers).
  options.fallback_candidate_limit =
      mode == QueryExecutionMode::kAnalytical ? 0 : 4096;
  std::vector<RuntimeRow> result;
  for (const RuntimeRow& seed : seeds.ValueOrDie()) {
    VertexRef vertex{seed.ref.part_id(), VertexId{seed.ref.entity_id()}};
    ValidTimeInterval expansion_interval = interval.ValueOrDie();
    if (seed.effective.has_value()) {
      auto clipped = internal::Intersect(expansion_interval, *seed.effective);
      if (!clipped.has_value()) continue;
      expansion_interval = *clipped;
    }
    internal::GraphExpansionRequest request{{vertex}, expansion_interval,
                                            plan.graph_expand->direction,
                                            plan.graph_expand->edge_type};
    if (plan.graph_journey != 0) {
      internal::JourneyRequest journey_request;
      journey_request.source = vertex;
      journey_request.interval = expansion_interval;
      journey_request.duration_property = plan.graph_duration_property;
      journey_request.max_hops = plan.graph_k_hops;
      journey_request.direction = plan.graph_expand->direction;
      journey_request.edge_type = plan.graph_expand->edge_type;
      journey_request.objective = static_cast<internal::JourneyObjective>(plan.graph_journey - 1);
      internal::GraphFrontierOptions discovery_options = options;
      discovery_options.reservation = nullptr;
      auto discovered = internal::KHopExpand(
          snapshot,
          internal::GraphExpansionRequest{{vertex}, expansion_interval,
                                           plan.graph_expand->direction,
                                           plan.graph_expand->edge_type},
          discovery_options);
      if (!discovered.ok()) return discovered.status();
      std::set<VertexRef, std::function<bool(const VertexRef&, const VertexRef&)>> targets(
          [](const VertexRef& a, const VertexRef& b) { return std::tie(a.part_id.value, a.vertex_id.value) < std::tie(b.part_id.value, b.vertex_id.value); });
      for (const auto& label : discovered.ValueOrDie().labels) {
        if (label.depth != 0) targets.insert(label.vertex);
      }
      for (const VertexRef& target : targets) {
        journey_request.target = target;
        auto journey = internal::FindJourney(snapshot, journey_request,
                                             internal::JourneyOptions{reservation, delta,
                                                                     snapshot.adjacency_index(),
                                                                     options.projection_generation,
                                                                     check_abort,
                                                                     max_journey_labels,
                                                                     max_journey_interval_fragments});
        if (!journey.ok()) {
          if (journey.status().IsNotFound()) continue;
          return journey.status();
        }
        RuntimeRow row{FactRef{vertex.part_id, FactFamily::kVertexState, PropertyId{}, vertex.vertex_id.value},
                       std::nullopt, std::nullopt, std::nullopt, std::nullopt,
                       std::nullopt, std::nullopt, 0, {}, std::nullopt, journey.ValueOrDie()};
        row.graph_source = vertex;
        row.graph_destination = target;
        if (!journey.ValueOrDie().edges.empty())
          row.graph_edge = journey.ValueOrDie().edges.back();
        row.journey = journey.ValueOrDie();
        result.push_back(std::move(row));
      }
    } else if (plan.graph_coexisting) {
      // Candidate discovery must not consume the live label/fragment
      // reservation. CoexistingShortestPath charges only its surviving
      // labels, so charging raw traversals here would count each edge twice.
      internal::GraphFrontierOptions discovery_options = options;
      discovery_options.reservation = nullptr;
      std::vector<VertexRef> targets;
      auto add_target = [&](const VertexRef& candidate) {
        if (std::find(targets.begin(), targets.end(), candidate) == targets.end())
          targets.push_back(candidate);
      };
      if (plan.graph_k_hops > 1) {
        auto hops = KHopExpand(snapshot, request, discovery_options);
        if (!hops.ok()) return hops.status();
        for (const auto& label : hops.ValueOrDie().labels) {
          // KHopExpand includes the depth-zero seed label; it is not a
          // reachable target for a path expansion.
          if (label.depth != 0) add_target(label.vertex);
        }
      } else {
        auto expanded = ExpandTemporal(snapshot, request, discovery_options);
        if (!expanded.ok()) return expanded.status();
        for (const auto& traversal : expanded.ValueOrDie()) {
          VertexRef candidate;
          if (plan.graph_expand->direction == ExpandDirection::kOut) {
            if (traversal.source != vertex) continue;
            candidate = traversal.target;
          } else if (plan.graph_expand->direction == ExpandDirection::kIn) {
            if (traversal.target != vertex) continue;
            candidate = traversal.source;
          } else if (traversal.source == vertex) {
            candidate = traversal.target;
          } else if (traversal.target == vertex) {
            candidate = traversal.source;
          } else {
            continue;
          }
          add_target(candidate);
        }
      }
      for (const VertexRef& candidate : targets) {
        auto paths = CoexistingShortestPath(snapshot, request, candidate, options);
        if (!paths.ok()) return paths.status();
        for (const PathValue& path : paths.ValueOrDie().paths) {
          RuntimeRow row{FactRef{PartId{}, FactFamily::kVertexState, PropertyId{}, 0},
                         std::nullopt, std::nullopt, std::nullopt, std::nullopt,
                         std::nullopt, std::nullopt, 0, {}, std::nullopt};
          row.ref = FactRef{path.edges.empty() ? path.vertices.front().part_id
                                               : path.edges.back().home_part_id,
                            FactFamily::kEdgeState, PropertyId{},
                            path.edges.empty() ? path.vertices.front().vertex_id.value
                                               : path.edges.back().edge_id.value};
          row.effective = path.common;
          row.graph_source = path.vertices.front();
          row.graph_destination = path.vertices.back();
          if (!path.edges.empty()) row.graph_edge = path.edges.back();
          row.path = path;
          result.push_back(std::move(row));
        }
      }
    } else if (plan.graph_k_hops > 1) {
      auto hops = KHopExpand(snapshot, request, options);
      if (!hops.ok()) return hops.status();
      for (const auto& traversal : hops.ValueOrDie().traversals) {
        result.push_back({FactRef(traversal.edge.home_part_id, FactFamily::kEdgeState,
                                  PropertyId{}, traversal.edge.edge_id.value),
                          traversal.effective, std::nullopt, std::nullopt,
                          traversal.source, traversal.edge, traversal.target,
                          traversal.edge_type});
      }
    } else {
      auto expanded = ExpandTemporal(snapshot, request, options);
      if (!expanded.ok()) return expanded.status();
      for (const auto& traversal : expanded.ValueOrDie()) {
        result.push_back({FactRef(traversal.edge.home_part_id, FactFamily::kEdgeState,
                                  PropertyId{}, traversal.edge.edge_id.value),
                          traversal.effective, std::nullopt, std::nullopt,
                          traversal.source, traversal.edge, traversal.target,
                          traversal.edge_type});
      }
    }
  }
  if (!plan.property_bindings.empty()) {
    auto bound = BindGraphPropertyRows(snapshot, std::move(result), plan);
    if (!bound.ok()) return bound.status();
    return std::move(bound).ConsumeValueOrDie();
  }
  return result;
}

QueryColumnVector EmptyColumn(QueryType type) {
  switch (type) {
    case QueryType::kBool:
      return std::vector<uint8_t>{};
    case QueryType::kInt32:
      return std::vector<int32_t>{};
    case QueryType::kInt64:
      return std::vector<int64_t>{};
    case QueryType::kFloat32:
      return std::vector<float>{};
    case QueryType::kFloat64:
      return std::vector<double>{};
    case QueryType::kTimestamp64:
      return std::vector<uint64_t>{};
    case QueryType::kString:
    case QueryType::kBinary:
      return std::vector<std::string>{};
    case QueryType::kVertexRef:
      return std::vector<VertexRef>{};
    case QueryType::kEdgeRef:
      return std::vector<EdgeRef>{};
    case QueryType::kValidTime:
      return std::vector<ValidTime>{};
    case QueryType::kValidDuration:
      return std::vector<ValidDuration>{};
    case QueryType::kCommitSeq:
      return std::vector<CommitSeq>{};
    case QueryType::kValidTimeInterval:
      return std::vector<ValidTimeInterval>{};
    case QueryType::kPath:
      return std::vector<uint8_t>{};
    default:
      return std::vector<uint8_t>{};
  }
}

void ReserveColumn(QueryColumn* column, size_t rows) {
  std::visit([rows](auto& values) { values.reserve(rows); }, column->values);
  column->present.reserve(rows);
}

template <typename T>
void Append(QueryColumn* column, T value, bool present) {
  std::get<std::vector<T>>(column->values).push_back(std::move(value));
  column->present.push_back(present ? uint8_t{1} : uint8_t{0});
}

Status AppendProperty(QueryColumn* column, const std::optional<Value>& value) {
  const bool present = value.has_value();
  switch (column->type) {
    case QueryType::kBool:
      Append(column, static_cast<uint8_t>(present &&
                                          std::get<bool>(value->data())),
             present);
      return Status::OK();
    case QueryType::kInt32:
      Append(column, present ? std::get<int32_t>(value->data()) : int32_t{},
             present);
      return Status::OK();
    case QueryType::kInt64:
      Append(column, present ? std::get<int64_t>(value->data()) : int64_t{},
             present);
      return Status::OK();
    case QueryType::kFloat32:
      Append(column, present ? std::get<float>(value->data()) : float{}, present);
      return Status::OK();
    case QueryType::kFloat64:
      Append(column, present ? std::get<double>(value->data()) : double{},
             present);
      return Status::OK();
    case QueryType::kTimestamp64:
      Append(column,
             present ? std::get<uint64_t>(value->data()) : uint64_t{}, present);
      return Status::OK();
    case QueryType::kString:
    case QueryType::kBinary:
      Append(column,
             present ? std::get<std::string>(value->data()) : std::string{},
             present);
      return Status::OK();
    default:
      return Status::Corruption("query", "property output type is invalid");
  }
}

StatusOr<std::vector<QueryColumn>> BuildColumns(
    const std::vector<RuntimeRow>& rows, size_t offset, size_t count,
    const internal::PreparedQueryPlan& plan,
    internal::QueryReservation* reservation = nullptr) {
  std::vector<QueryColumn> columns;
  columns.reserve(plan.output_columns.size());
  for (const RowColumn& output : plan.output_columns) {
    columns.push_back(
        QueryColumn{output.slot, output.type, EmptyColumn(output.type), {}});
    ReserveColumn(&columns.back(), count);
  }
  std::vector<PathValue> path_values;
  if (plan.graph_path_slot.has_value()) path_values.reserve(count);
  for (size_t index = offset; index < offset + count; ++index) {
    const RuntimeRow& row = rows[index];
    for (size_t column_index = 0; column_index < columns.size(); ++column_index) {
      QueryColumn* column = &columns[column_index];
      const RowColumn& output = plan.output_columns[column_index];
      if (plan.graph_path_slot && output.slot == *plan.graph_path_slot) {
        if (!row.path) return Status::Corruption("query", "graph path is unavailable");
        path_values.push_back(*row.path);
        continue;
      }
      if (plan.graph_journey_slot && output.slot == *plan.graph_journey_slot) {
        if (!row.journey) return Status::Corruption("query", "graph journey is unavailable");
        continue;
      }
      if (plan.graph_source_slot && output.slot == *plan.graph_source_slot) {
        if (!row.graph_source) return Status::Corruption("query", "graph source is unavailable");
        Append(column, *row.graph_source, true);
        continue;
      }
      if (plan.graph_edge_slot && output.slot == *plan.graph_edge_slot) {
        if (!row.graph_edge) return Status::Corruption("query", "graph edge is unavailable");
        Append(column, *row.graph_edge, true);
        continue;
      }
      if (plan.graph_destination_slot && output.slot == *plan.graph_destination_slot) {
        if (!row.graph_destination) return Status::Corruption("query", "graph destination is unavailable");
        Append(column, *row.graph_destination, true);
        continue;
      }
      if (output.slot == plan.entity_slot) {
        if (plan.entity_family == FactFamily::kVertexState) {
          Append(column,
                 VertexRef{row.ref.part_id(), VertexId{row.ref.entity_id()}},
                 true);
        } else {
          Append(column, EdgeRef{row.ref.part_id(), EdgeId{row.ref.entity_id()}},
                 true);
        }
        continue;
      }
      const auto binding = std::find_if(
          plan.property_bindings.begin(), plan.property_bindings.end(),
          [&output](const internal::PreparedPropertyBinding& candidate) {
            return candidate.output.slot == output.slot;
          });
      if (binding == plan.property_bindings.end()) {
        return Status::Corruption("query", "projected slot is unavailable");
      }
      std::optional<Value> property_value = row.property_value;
      if (const auto found = row.property_values.find(output.slot.value);
          found != row.property_values.end()) {
        property_value = found->second;
      }
      if (reservation != nullptr && property_value.has_value() &&
          (property_value->type() == PhysicalType::kString ||
           property_value->type() == PhysicalType::kBinary)) {
        const auto& value = std::get<std::string>(property_value->data());
        if (Status status = reservation->ReserveMemory(value.size());
            !status.ok()) return status;
      }
      const Status appended = AppendProperty(column, property_value);
      if (!appended.ok()) return appended;
    }
  }
  if (plan.graph_path_slot) {
    auto it = std::find_if(columns.begin(), columns.end(), [&](const QueryColumn& column) {
      return column.slot == *plan.graph_path_slot;
    });
    if (it != columns.end()) {
      it->path_values = std::make_shared<const PathColumn>(
          PathColumn::FromValues(path_values));
    }
  }
  if (plan.graph_journey_slot) {
    auto it = std::find_if(columns.begin(), columns.end(), [&](const QueryColumn& column) {
      return column.slot == *plan.graph_journey_slot;
    });
    if (it != columns.end()) {
      std::vector<JourneyValue> journeys;
      journeys.reserve(count);
      for (size_t index = offset; index < offset + count; ++index) {
        if (!rows[index].journey) return Status::Corruption("query", "graph journey is unavailable");
        journeys.push_back(*rows[index].journey);
      }
      it->journey_values = std::make_shared<const JourneyColumn>(JourneyColumn::FromValues(journeys));
    }
  }
  return columns;
}

Status AppendRelationalCell(QueryColumn* column,
                            const internal::RelationalCell& cell) {
  if (column->type != cell.type) {
    return Status::InvalidArgument("query runtime",
                                   "relational output types differ");
  }
  const bool present = cell.present;
  switch (column->type) {
    case QueryType::kBool:
      Append(column, static_cast<uint8_t>(present && std::get<bool>(cell.value)),
             present);
      return Status::OK();
    case QueryType::kInt32:
      Append(column, present ? std::get<int32_t>(cell.value) : int32_t{}, present);
      return Status::OK();
    case QueryType::kInt64:
      Append(column, present ? std::get<int64_t>(cell.value) : int64_t{}, present);
      return Status::OK();
    case QueryType::kFloat32:
      Append(column, present ? std::get<float>(cell.value) : float{}, present);
      return Status::OK();
    case QueryType::kFloat64:
      Append(column, present ? std::get<double>(cell.value) : double{}, present);
      return Status::OK();
    case QueryType::kTimestamp64:
      Append(column,
             present ? std::get<Timestamp64>(cell.value).value : uint64_t{},
             present);
      return Status::OK();
    case QueryType::kString:
      Append(column, present ? std::get<std::string>(cell.value) : std::string{},
             present);
      return Status::OK();
    case QueryType::kBinary:
      Append(column,
             present ? std::get<Binary>(cell.value).value : std::string{}, present);
      return Status::OK();
    case QueryType::kVertexRef:
      Append(column, present ? std::get<VertexRef>(cell.value) : VertexRef{}, present);
      return Status::OK();
    case QueryType::kEdgeRef:
      Append(column, present ? std::get<EdgeRef>(cell.value) : EdgeRef{}, present);
      return Status::OK();
    case QueryType::kValidTime:
      Append(column, present ? std::get<ValidTime>(cell.value) : ValidTime{}, present);
      return Status::OK();
    case QueryType::kValidDuration:
      Append(column,
             present ? std::get<ValidDuration>(cell.value) : ValidDuration{}, present);
      return Status::OK();
    case QueryType::kCommitSeq:
      Append(column, present ? std::get<CommitSeq>(cell.value) : CommitSeq{}, present);
      return Status::OK();
    case QueryType::kValidTimeInterval:
      Append(column,
             present ? std::get<ValidTimeInterval>(cell.value)
                     : ValidTimeInterval{},
             present);
      return Status::OK();
    default:
      return Status::NotSupported("query runtime",
                                  "relational output type is unsupported");
  }
}

std::optional<size_t> EffectiveOutputColumn(
    const internal::RelationalRow& row,
    const std::vector<RowColumn>& output_columns,
    std::optional<SlotId> effective_output_slot) {
  if (!row.effective.has_value() || !effective_output_slot.has_value() ||
      row.cells.size() + 1 != output_columns.size()) {
    return std::nullopt;
  }
  for (size_t column = 0; column < output_columns.size(); ++column) {
    if (output_columns[column].slot != *effective_output_slot) continue;
    return output_columns[column].type == QueryType::kValidTimeInterval
               ? std::optional<size_t>{column}
               : std::nullopt;
  }
  return std::nullopt;
}

size_t RelationalValueBytes(const internal::RelationalCell& cell) {
  if (!cell.present) return 1;
  return std::visit(
      [](const auto& value) -> size_t {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, std::string>) {
          return sizeof(std::string) + value.size() + 1;
        } else if constexpr (std::is_same_v<T, Binary>) {
          return sizeof(std::string) + value.value.size() + 1;
        } else {
          return sizeof(T) + 1;
        }
      },
      cell.value);
}

StatusOr<size_t> EstimateRelationalBatchBytes(
    const internal::BatchStream& stream,
    const std::vector<RowColumn>& output_columns,
    std::optional<SlotId> effective_output_slot) {
  constexpr size_t kBatchRows = 1024;
  const size_t batch_count =
      stream.rows.empty() ? 0 : (stream.rows.size() - 1) / kBatchRows + 1;
  if (batch_count >
      std::numeric_limits<size_t>::max() / sizeof(QueryBatch)) {
    return Status::ResourceExhausted("query runtime",
                                     "memory_bytes estimate overflow");
  }
  size_t bytes = batch_count * sizeof(QueryBatch);
  if (output_columns.size() >
          std::numeric_limits<size_t>::max() / sizeof(QueryColumn) ||
      (output_columns.size() != 0 &&
       batch_count > (std::numeric_limits<size_t>::max() - bytes) /
                         (output_columns.size() * sizeof(QueryColumn)))) {
    return Status::ResourceExhausted("query runtime",
                                     "memory_bytes estimate overflow");
  }
  bytes += batch_count * output_columns.size() * sizeof(QueryColumn);
  for (const internal::RelationalRow& row : stream.rows) {
    const std::optional<size_t> effective_column =
        EffectiveOutputColumn(row, output_columns, effective_output_slot);
    if (row.cells.size() != output_columns.size() &&
        !effective_column.has_value()) {
      return Status::InvalidArgument("query runtime",
                                     "relational output schema differs");
    }
    size_t cell_index = 0;
    for (size_t column = 0; column < output_columns.size(); ++column) {
      size_t value_bytes = 0;
      if (effective_column == column) {
        value_bytes = sizeof(ValidTimeInterval) + 1;
      } else {
        value_bytes = RelationalValueBytes(row.cells[cell_index++]);
      }
      if (value_bytes > std::numeric_limits<size_t>::max() - bytes) {
        return Status::ResourceExhausted("query runtime",
                                         "memory_bytes estimate overflow");
      }
      bytes += value_bytes;
    }
  }
  return bytes;
}

StatusOr<std::vector<QueryColumn>> BuildRelationalColumns(
    const internal::BatchStream& stream, size_t offset, size_t count,
    const std::vector<RowColumn>& output_columns,
    std::optional<SlotId> effective_output_slot) {
  std::vector<QueryColumn> columns;
  columns.reserve(output_columns.size());
  for (const RowColumn& output : output_columns) {
    columns.push_back(
        QueryColumn{output.slot, output.type, EmptyColumn(output.type), {}});
    ReserveColumn(&columns.back(), count);
  }
  for (size_t row_index = offset; row_index < offset + count; ++row_index) {
    const internal::RelationalRow& row = stream.rows[row_index];
    const std::optional<size_t> effective_column =
        EffectiveOutputColumn(row, output_columns, effective_output_slot);
    if (row.cells.size() != columns.size() && !effective_column.has_value()) {
      return Status::InvalidArgument("query runtime",
                                     "relational output schema differs");
    }
    size_t cell_index = 0;
    for (size_t column_index = 0; column_index < columns.size(); ++column_index) {
      if (effective_column == column_index) {
        const internal::RelationalCell effective =
            internal::RelationalCell::Present(
                QueryType::kValidTimeInterval, *row.effective);
        if (Status status =
                AppendRelationalCell(&columns[column_index], effective);
            !status.ok()) {
          return status;
        }
      } else {
        if (Status status = AppendRelationalCell(
                &columns[column_index], row.cells[cell_index++]);
            !status.ok()) {
          return status;
        }
      }
    }
  }
  return columns;
}

}  // namespace

void QueryExecutionState::RequestCancel() {
  cancelled_.store(true, std::memory_order_release);
  std::function<void()> callback;
  std::function<Status(const char*)> crash_fault_injector;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (terminal_.state == QueryCursorState::kRunning) {
      terminal_.state = QueryCursorState::kCancelled;
      terminal_.complete = false;
      terminal_.status = Status::QueryCancelled("query", "query cancelled");
      if (metrics_) metrics_->AddTerminal(internal::QueryMetricTerminal::kCancelled);
    }
    callback = cancel_callback_;
    crash_fault_injector = crash_fault_injector_;
  }
  if (crash_fault_injector) crash_fault_injector("cursor_cancel").IgnoreError();
  if (callback) callback();
}

bool QueryExecutionState::cancelled() const {
  return cancelled_.load(std::memory_order_acquire);
}

Status QueryExecutionState::FinishClean() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (terminal_.state != QueryCursorState::kRunning) return terminal_.status;
  terminal_.state = QueryCursorState::kCleanEnd;
  terminal_.complete = true;
  terminal_.status = Status::OK();
  if (metrics_) metrics_->AddTerminal(internal::QueryMetricTerminal::kComplete);
  return Status::OK();
}

Status QueryExecutionState::FinishCancelled() {
  RequestCancel();
  return terminal_info().status;
}

Status QueryExecutionState::FinishFailed(Status status) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (terminal_.state != QueryCursorState::kRunning) return terminal_.status;
  terminal_.state = QueryCursorState::kFailed;
  terminal_.complete = false;
  terminal_.status = std::move(status);
  if (metrics_) metrics_->AddTerminal(internal::QueryMetricTerminal::kFailed);
  return terminal_.status;
}

QueryTerminalInfo QueryExecutionState::terminal_info() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return terminal_;
}

void QueryExecutionState::BeginOperation() {
  std::lock_guard<std::mutex> lock(mutex_);
  ++active_operations_;
}

void QueryExecutionState::EndOperation() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (active_operations_ != 0) --active_operations_;
  if (active_operations_ == 0) operations_cv_.notify_all();
}

void QueryExecutionState::WaitForOperations() {
  std::unique_lock<std::mutex> lock(mutex_);
  operations_cv_.wait(lock, [this] { return active_operations_ == 0; });
}

Status QueryExecutionState::Close() {
  std::function<Status(const char*)> crash_fault_injector;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    crash_fault_injector = crash_fault_injector_;
  }
  if (crash_fault_injector) crash_fault_injector("cursor_close_before").IgnoreError();
  RequestCancel();
  std::function<void()> close_callback;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    close_started_ = true;
    operations_cv_.wait(lock, [this] { return active_operations_ == 0; });
    if (!close_callback_called_) {
      close_callback_called_ = true;
      close_callback = close_callback_;
    }
  }
  if (close_callback) close_callback();
  if (crash_fault_injector) crash_fault_injector("cursor_close_after").IgnoreError();
  return Status::OK();
}

void QueryExecutionState::SetCancelCallback(std::function<void()> callback) {
  std::lock_guard<std::mutex> lock(mutex_);
  cancel_callback_ = std::move(callback);
}

void QueryExecutionState::SetCrashFaultInjector(
    std::function<Status(const char*)> callback) {
  std::lock_guard<std::mutex> lock(mutex_);
  crash_fault_injector_ = std::move(callback);
}

void QueryExecutionState::SetCloseCallback(std::function<void()> callback) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!close_callback_) {
    close_callback_ = std::move(callback);
  } else if (callback) {
    auto prior = std::move(close_callback_);
    close_callback_ = [prior = std::move(prior), callback = std::move(callback)] {
      prior();
      callback();
    };
  }
}

void QueryExecutionState::ClearCancelCallback() {
  std::lock_guard<std::mutex> lock(mutex_);
  cancel_callback_ = {};
}

void QueryExecutionState::SetSnapshotSeq(CommitSeq seq) {
  std::lock_guard<std::mutex> lock(mutex_);
  snapshot_seq_ = seq;
}

std::optional<CommitSeq> QueryExecutionState::snapshot_seq() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return snapshot_seq_;
}

QueryProfile QueryExecutionState::profile() const {
  std::lock_guard<std::mutex> lock(mutex_);
  QueryProfile copy = profile_;
  copy.terminal = terminal_;
  return copy;
}

QueryExecutionState::QueryExecutionState()
    : profile_started_at_(std::chrono::steady_clock::now()),
      profile_last_at_(profile_started_at_),
      profile_cpu_started_us_(ThreadCpuMicros()),
      profile_cpu_last_us_(profile_cpu_started_us_) {}

void QueryExecutionState::RecordBatch(uint64_t rows, uint64_t decoded_bytes,
                                      bool capture_profile, uint64_t physical_bytes,
                                      uint64_t pages, uint64_t interval_fragments,
                                      uint32_t operator_id, uint8_t metric_operator) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (capture_profile) {
    auto it = std::find_if(profile_.operators.begin(), profile_.operators.end(),
                           [operator_id](const QueryOperatorProfile& value) {
                             return value.operator_id == operator_id;
                           });
    if (it == profile_.operators.end()) {
      profile_.operators.push_back(QueryOperatorProfile{});
      it = std::prev(profile_.operators.end());
      it->operator_id = operator_id;
    }
    const auto now = std::chrono::steady_clock::now();
    const uint64_t cpu = ThreadCpuMicros();
    const uint64_t wall_us = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(now - profile_last_at_).count());
    const uint64_t cpu_us = cpu >= profile_cpu_last_us_ ? cpu - profile_cpu_last_us_ : 0;
    auto& op = *it;
    op.rows += rows;
    op.batches += 1;
    op.physical_bytes += physical_bytes;
    op.decoded_bytes += decoded_bytes;
    op.pages += pages;
    op.interval_fragments += interval_fragments;
    op.wall_us += wall_us;
    op.cpu_us += cpu_us;
    if (!profile_first_result_recorded_) {
      op.first_result_us = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(now - profile_started_at_).count());
      profile_first_result_recorded_ = true;
    }
    profile_last_at_ = now;
    profile_cpu_last_us_ = cpu;
  }
  if (metrics_) {
    const auto bounded = static_cast<internal::QueryMetricOperator>(metric_operator);
    metrics_->AddBatch(bounded, rows, physical_bytes, decoded_bytes, interval_fragments);
  }
}

class QueryCursor::State {
 public:
  struct TerminalError {
    State* owner = nullptr;
    std::optional<Status> value;
    bool has_value() const { return value.has_value(); }
    Status& operator*() { return *value; }
    const Status& operator*() const { return *value; }
    Status* operator->() { return &*value; }
    const Status* operator->() const { return &*value; }
    TerminalError& operator=(Status status) {
      value = std::move(status);
      if (owner && owner->execution) {
        if (value->IsQueryCancelled()) {
          owner->execution->FinishCancelled();
        } else {
          owner->execution->FinishFailed(*value);
        }
      }
      return *this;
    }
  };

  State(internal::PreparedQueryPlan plan, Snapshot snapshot,
        Bindings bindings, QueryOptions options,
        internal::QueryReservation reservation,
        std::unique_ptr<internal::QueryScratch> scratch)
      : plan(std::move(plan)), snapshot(std::move(snapshot)),
        bindings(std::move(bindings)), options(std::move(options)),
        reservation(std::move(reservation)),
        fragments(this->options.budget.interval_fragments),
        started_at(std::chrono::steady_clock::now()), scratch(std::move(scratch)),
        execution(std::make_shared<QueryExecutionState>()) {
    execution->SetSnapshotSeq(this->snapshot->commit_seq());
    execution->SetCancelCallback([this] {
      cancelled.store(true, std::memory_order_release);
    });
    execution->SetCloseCallback([this] {
      // This callback runs only after all in-flight Next operations have
      // acknowledged cancellation. Releasing the Snapshot here is therefore
      // ordered after every engine read and before Database closes RocksDB.
      std::lock_guard<std::mutex> lock(call_mutex);
      this->snapshot.reset();
      // A cursor close releases the physical projection-generation lease as
      // part of the same terminal transition. Returned QueryBatch values own
      // their decoded storage independently and remain readable.
      this->plan.projection_generation.reset();
      this->materialized_output_lease.reset();
      this->batches.clear();
      CleanupScratch();
    });
    if (this->scratch) this->scratch->SetReservation(&this->reservation);
    if (this->scratch) {
      this->scratch->SetAbortCheck([this] {
        if (cancelled.load(std::memory_order_acquire)) {
          return Status::QueryCancelled("query", "query cancelled");
        }
        if (this->options.budget.deadline_us != 0 &&
            static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - started_at).count()) >=
                this->options.budget.deadline_us) {
          return Status::DeadlineExceeded("query", "deadline_us budget exhausted");
        }
        return Status::OK();
      });
    }
  }

  void CleanupScratch() {
    if (scratch) scratch->Cleanup().IgnoreError();
  }

  void SetTerminalError(Status status) {
    terminal_error = std::move(status);
    if (terminal_error->IsQueryCancelled()) {
      execution->FinishCancelled();
    } else {
      execution->FinishFailed(*terminal_error);
    }
  }

  internal::PreparedQueryPlan plan;
  std::optional<Snapshot> snapshot;
  Bindings bindings;
  QueryOptions options;
  internal::QueryReservation reservation;
  internal::FragmentBudget fragments;
  std::shared_ptr<internal::QueryReservationLease> materialized_output_lease;
  std::vector<QueryBatch> batches;
  size_t next_batch = 0;
  bool initialized = false;
  bool clean_terminal = false;
  std::shared_ptr<std::atomic<uint32_t>> retained_batches =
      std::make_shared<std::atomic<uint32_t>>(0);
  std::atomic<bool> cancelled{false};
  std::chrono::steady_clock::time_point started_at;
  std::unique_ptr<internal::QueryScratch> scratch;
  std::shared_ptr<QueryExecutionState> execution;
  TerminalError terminal_error{this};
  // Serializes callers of Next/Close while Cancel remains non-blocking.
  mutable std::mutex call_mutex;
};

struct BatchLease {
  std::shared_ptr<QueryBatch> backing;
  std::shared_ptr<std::atomic<uint32_t>> retained;
  ~BatchLease() {
    retained->fetch_sub(1, std::memory_order_acq_rel);
  }
};

QueryCursor::QueryCursor(std::unique_ptr<State> state)
    : state_(std::move(state)) {}
QueryCursor::~QueryCursor() {
  if (state_ && state_->execution) {
    state_->execution->Close();
    state_->execution->ClearCancelCallback();
  }
}
QueryCursor::QueryCursor(QueryCursor&&) noexcept = default;
QueryCursor& QueryCursor::operator=(QueryCursor&&) noexcept = default;

QueryTerminalInfo QueryCursor::terminal_info() const {
  if (!state_) {
    return QueryTerminalInfo{QueryCursorState::kFailed, false,
                             Status::InvalidArgument("query cursor",
                                                     "moved-from cursor")};
  }
  return state_->execution->terminal_info();
}

QueryProfile QueryCursor::profile() const {
  if (!state_) {
    QueryProfile profile;
    profile.terminal = QueryTerminalInfo{QueryCursorState::kFailed, false,
                                         Status::InvalidArgument("query cursor",
                                                                 "moved-from cursor")};
    return profile;
  }
  return state_->execution->profile();
}

StatusOr<std::optional<QueryBatch>> QueryCursor::Next() {
  if (!state_) {
    return Status::InvalidArgument("query cursor", "moved-from cursor");
  }
  std::unique_lock<std::mutex> call_lock(state_->call_mutex);
  state_->execution->BeginOperation();
  struct OperationGuard {
    QueryCursor::State* state;
    std::shared_ptr<QueryExecutionState> execution;
    ~OperationGuard() {
      // Keep the shared execution state authoritative even for legacy error
      // exits that assign terminal_error directly in the materializer.
      if (state->terminal_error.has_value()) {
        if (state->terminal_error->IsQueryCancelled()) {
          execution->FinishCancelled();
        } else {
          execution->FinishFailed(*state->terminal_error);
        }
      }
      execution->EndOperation();
    }
  } operation_guard{state_.get(), state_->execution};
  // Any terminal error produced below must release analytical scratch before
  // returning from this first failing call. The guard is a no-op for batches
  // and clean completion, and makes cleanup independent of a later Next/Close.
  struct TerminalCleanup {
    State* state;
    ~TerminalCleanup() {
      if (state->terminal_error.has_value()) state->CleanupScratch();
    }
  } terminal_cleanup{state_.get()};
  if (state_->terminal_error.has_value()) {
    state_->CleanupScratch();
    return *state_->terminal_error;
  }
  if (state_->clean_terminal) return std::optional<QueryBatch>{};
  if (state_->cancelled.load(std::memory_order_acquire)) {
    state_->terminal_error = Status::QueryCancelled("query", "query cancelled");
    state_->CleanupScratch();
    return *state_->terminal_error;
  }
  if (state_->options.budget.deadline_us != 0 &&
      static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - state_->started_at).count()) >=
          state_->options.budget.deadline_us) {
    state_->terminal_error = Status::DeadlineExceeded("query", "deadline_us budget exhausted");
    state_->CleanupScratch();
    return *state_->terminal_error;
  }

  if (!state_->initialized) {
    state_->initialized = true;
    if (state_->plan.relational_kind.has_value()) {
      if (!state_->plan.relational_input.has_value()) {
        state_->terminal_error = Status::InvalidArgument(
            "query runtime", "relational logical node has no input");
        state_->snapshot.reset();
        return *state_->terminal_error;
      }
      auto relational = internal::ExecuteRelationalPlanNode(
          *state_->plan.relational_kind,
          std::move(*state_->plan.relational_input),
          &state_->reservation, &state_->fragments,
          state_->options.budget.output_rows, state_->scratch.get());
      if (!relational.ok()) {
        state_->terminal_error = relational.status();
        state_->snapshot.reset();
        return *state_->terminal_error;
      }
      const internal::BatchStream& stream = relational.ValueOrDie().stream;
      if (stream.rows.size() > state_->options.budget.output_rows) {
        state_->terminal_error = Status::ResourceExhausted(
            "query", "output row budget exceeded");
        state_->snapshot.reset();
        return *state_->terminal_error;
      }
      if (Status status = state_->reservation.ReserveOutputRows(stream.rows.size());
          !status.ok()) {
        state_->terminal_error = status;
        state_->snapshot.reset();
        return *state_->terminal_error;
      }
      auto materialized_bytes =
          EstimateRelationalBatchBytes(stream, state_->plan.output_columns,
                                       state_->plan.effective_output_slot);
      if (!materialized_bytes.ok()) {
        state_->terminal_error = materialized_bytes.status();
        state_->snapshot.reset();
        return *state_->terminal_error;
      }
      if (materialized_bytes.ValueOrDie() >
          state_->options.budget.output_bytes) {
        state_->terminal_error = Status::ResourceExhausted(
            "query", "output byte budget exceeded");
        state_->snapshot.reset();
        return *state_->terminal_error;
      }
      if (Status status = state_->reservation.ReserveOutputBytes(materialized_bytes.ValueOrDie());
          !status.ok()) {
        state_->terminal_error = status;
        state_->snapshot.reset();
        return *state_->terminal_error;
      }
      state_->materialized_output_lease = state_->reservation.TryRetain(
          materialized_bytes.ValueOrDie());
      if (!state_->materialized_output_lease) {
        state_->terminal_error = Status::ResourceExhausted(
            "query runtime",
            "memory_bytes=" +
                std::to_string(materialized_bytes.ValueOrDie()) +
                " available_bytes=" +
                std::to_string(state_->reservation.limit_bytes() -
                               state_->reservation.used_bytes()));
        state_->snapshot.reset();
        return *state_->terminal_error;
      }
      const size_t kBatchRows = state_->options.mode == QueryExecutionMode::kInteractive
                                    ? 256
                                    : (state_->options.mode == QueryExecutionMode::kAnalytical ? 4096 : 1024);
      const size_t batch_count = stream.rows.empty()
                                     ? 0
                                     : (stream.rows.size() - 1) / kBatchRows + 1;
      state_->batches.reserve(batch_count);
      for (size_t offset = 0; offset < stream.rows.size(); offset += kBatchRows) {
        const size_t count = std::min(kBatchRows, stream.rows.size() - offset);
        auto columns = BuildRelationalColumns(
            stream, offset, count, state_->plan.output_columns,
            state_->plan.effective_output_slot);
        if (!columns.ok()) {
          state_->terminal_error = columns.status();
          state_->snapshot.reset();
          return *state_->terminal_error;
        }
        state_->batches.emplace_back(
            QueryBatch(count, std::move(columns).ConsumeValueOrDie(),
                       state_->materialized_output_lease));
      }
    } else {
    const size_t pre_materialize = std::min<size_t>(
        state_->options.budget.memory_bytes, 1ULL << 20);
    auto pre_materialize_lease = state_->reservation.TryRetain(pre_materialize);
    if (!pre_materialize_lease && pre_materialize != 0) {
      state_->terminal_error = Status::ResourceExhausted(
          "query", "memory_bytes pre-materialization reservation exhausted");
      state_->snapshot.reset();
      return *state_->terminal_error;
    }
    auto rows = state_->plan.graph_expand.has_value()
                    ? MaterializeGraphRows(*state_->snapshot, state_->plan,
                                           &state_->reservation,
                                           state_->options.mode,
                                           state_->bindings,
                                           [state = state_.get()]() -> Status {
                                             if (state->cancelled.load(std::memory_order_acquire))
                                               return Status::QueryCancelled("query", "query cancelled");
                                             if (state->options.budget.deadline_us != 0 &&
                                                 static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                                     std::chrono::steady_clock::now() - state->started_at).count()) >=
                                                     state->options.budget.deadline_us)
                                               return Status::DeadlineExceeded("query", "deadline_us budget exhausted");
                                             return Status::OK();
                                           },
                                           state_->options.budget.graph_labels,
                                           state_->options.budget.interval_fragments)
                    : MaterializeRows(*state_->snapshot, state_->plan,
                                      state_->bindings,
                                      &state_->reservation);
    // The lease owns the pre-materialization reservation. Reset it now that
    // MaterializeRows has completed so the reservation is released exactly
    // once, including on an error path.
    pre_materialize_lease.reset();
    if (!rows.ok()) {
      state_->terminal_error = rows.status();
      state_->snapshot.reset();
      return *state_->terminal_error;
    }
    const size_t row_count = rows.ValueOrDie().size();
    if (row_count > state_->options.budget.output_rows) {
      state_->terminal_error = Status::ResourceExhausted(
          "query", "output row budget exceeded");
      state_->snapshot.reset();
      return *state_->terminal_error;
    }
    if (Status status = state_->reservation.ReserveDecodedRows(row_count);
        !status.ok()) {
      state_->terminal_error = status;
      state_->snapshot.reset();
      return *state_->terminal_error;
    }
    uint64_t estimated_read = 0;
    uint64_t estimated_cpu = row_count;
    for (const auto& row : rows.ValueOrDie()) {
      auto payload = PropertyPayloadBytes(row);
      if (!payload.ok() ||
          payload.ValueOrDie() > std::numeric_limits<uint64_t>::max() - estimated_read) {
        state_->terminal_error = payload.ok()
                                     ? Status::ResourceExhausted(
                                           "query", "read byte accounting overflow")
                                     : payload.status();
        state_->snapshot.reset();
        return *state_->terminal_error;
      }
      estimated_read += payload.ValueOrDie();
    }
    if (Status status = state_->reservation.ReserveReadBytes(estimated_read);
        !status.ok()) {
      state_->terminal_error = status;
      state_->snapshot.reset();
      return *state_->terminal_error;
    }
    if (state_->options.budget.cpu_us != 0) {
      if (Status status = state_->reservation.ReserveCpuMicros(estimated_cpu);
          !status.ok()) {
        state_->terminal_error = status;
        state_->snapshot.reset();
        return *state_->terminal_error;
      }
    }
    if (Status status = state_->reservation.ReserveOutputRows(row_count);
        !status.ok()) {
      state_->terminal_error = status;
      state_->snapshot.reset();
      return *state_->terminal_error;
    }
    if (row_count > state_->options.budget.interval_fragments) {
      state_->terminal_error = Status::ResourceExhausted(
          "query", "interval fragment budget exceeded");
      state_->snapshot.reset();
      return *state_->terminal_error;
    }
    uint64_t output_payload = 0;
    for (const RuntimeRow& row : rows.ValueOrDie()) {
      auto payload = PropertyPayloadBytes(row);
      if (!payload.ok() ||
          payload.ValueOrDie() > std::numeric_limits<uint64_t>::max() - output_payload) {
        state_->terminal_error = payload.ok()
                                     ? Status::ResourceExhausted(
                                           "query", "output byte accounting overflow")
                                     : payload.status();
        state_->snapshot.reset();
        return *state_->terminal_error;
      }
      output_payload += payload.ValueOrDie();
    }
    const size_t fixed_materialization =
        row_count > (std::numeric_limits<size_t>::max() - sizeof(QueryBatch)) /
                         std::max<size_t>(1, state_->plan.output_columns.size() * sizeof(QueryColumn))
            ? std::numeric_limits<size_t>::max()
            : sizeof(QueryBatch) + row_count *
                  std::max<size_t>(1, state_->plan.output_columns.size() * sizeof(QueryColumn));
    const size_t estimated_materialization =
        fixed_materialization == std::numeric_limits<size_t>::max() ||
                output_payload > std::numeric_limits<size_t>::max() - fixed_materialization
            ? std::numeric_limits<size_t>::max()
            : fixed_materialization + static_cast<size_t>(output_payload);
    if (estimated_materialization == std::numeric_limits<size_t>::max()) {
      state_->terminal_error = Status::ResourceExhausted("query", "memory_bytes estimate overflow");
      state_->snapshot.reset();
      return *state_->terminal_error;
    }
    if (Status status = state_->reservation.ReserveMemory(estimated_materialization);
        !status.ok()) {
      state_->terminal_error = status;
      state_->snapshot.reset();
      return *state_->terminal_error;
    }
    if (Status status = state_->reservation.ReserveOutputBytes(estimated_materialization);
        !status.ok()) {
      state_->terminal_error = status;
      state_->snapshot.reset();
      return *state_->terminal_error;
    }

    const size_t kBatchRows = state_->options.mode == QueryExecutionMode::kInteractive
                                  ? 256
                                  : (state_->options.mode == QueryExecutionMode::kAnalytical ? 4096 : 1024);
    for (size_t offset = 0; offset < row_count; offset += kBatchRows) {
      if (state_->cancelled.load(std::memory_order_acquire)) {
        state_->terminal_error = Status::QueryCancelled("query", "query cancelled");
        state_->snapshot.reset();
        return *state_->terminal_error;
      }
      const size_t count = std::min(kBatchRows, row_count - offset);
      auto columns =
          BuildColumns(rows.ValueOrDie(), offset, count, state_->plan,
                       &state_->reservation);
      if (!columns.ok()) {
        state_->terminal_error = columns.status();
        state_->snapshot.reset();
        return *state_->terminal_error;
      }
      state_->batches.emplace_back(
          QueryBatch(count, std::move(columns).ConsumeValueOrDie()));
    }
    }
  }

  if (state_->next_batch < state_->batches.size()) {
    const uint32_t retained = state_->retained_batches->load(std::memory_order_acquire);
    if (retained >= state_->options.budget.retained_output_batches) {
      return Status::ResourceExhausted("query", "retained_output_batches budget exhausted");
    }
    state_->retained_batches->fetch_add(1, std::memory_order_acq_rel);
    QueryBatch& stored = state_->batches[state_->next_batch++];
    auto lease = std::make_shared<BatchLease>();
    lease->backing = std::make_shared<QueryBatch>(std::move(stored));
    lease->retained = state_->retained_batches;
    uint64_t decoded_bytes = 0;
    for (const auto& column : lease->backing->columns()) {
      const uint64_t column_bytes = std::visit(
          [](const auto& values) -> uint64_t {
            using T = typename std::decay_t<decltype(values)>::value_type;
            if constexpr (std::is_same_v<T, std::string>) {
              uint64_t bytes = 0;
              for (const auto& value : values) {
                if (value.size() > UINT64_MAX - bytes) return UINT64_MAX;
                bytes += value.size();
              }
              return bytes;
            } else {
              if (values.size() > UINT64_MAX / sizeof(T)) return UINT64_MAX;
              return static_cast<uint64_t>(values.size()) * sizeof(T);
            }
          }, column.values);
      if (column_bytes > UINT64_MAX - decoded_bytes) {
        decoded_bytes = UINT64_MAX;
      } else {
        decoded_bytes += column_bytes;
      }
    }
    // Global metrics are always batch-level and do not require profile clocks
    // or per-row instrumentation when capture_profile is disabled.
    const auto profile_operator = ProfileOperatorFor(state_->plan);
    // The output batch owns decoded-byte accounting. Physical file bytes,
    // page counts, and interval fragments require decoder-level counters that
    // are not retained by this materialized batch; leave them unavailable
    // rather than relabeling planner estimates as execution actuals.
    const uint64_t physical_bytes = 0;
    const uint64_t pages = 0;
    const uint64_t fragments = 0;
    state_->execution->RecordBatch(lease->backing->row_count(), decoded_bytes,
                                   state_->options.capture_profile, physical_bytes,
                                   pages, fragments, profile_operator.first,
                                   static_cast<uint8_t>(profile_operator.second));
    return std::optional<QueryBatch>{
        QueryBatch(lease->backing->row_count(), lease->backing->columns(), lease)};
  }
  state_->batches.clear();
  state_->materialized_output_lease.reset();
  state_->snapshot.reset();
  state_->CleanupScratch();
  state_->clean_terminal = true;
  state_->execution->FinishClean();
  return std::optional<QueryBatch>{};
}

Status QueryCursor::Close() {
  if (!state_) {
    return Status::InvalidArgument("query cursor", "moved-from cursor");
  }
  const Status closed = state_->execution->Close();
  std::lock_guard<std::mutex> call_lock(state_->call_mutex);
  state_->batches.clear();
  state_->materialized_output_lease.reset();
  state_->snapshot.reset();
  state_->CleanupScratch();
  return closed.ok() ? Status::OK() : closed;
}

Status QueryCursor::Cancel() {
  if (!state_) return Status::InvalidArgument("query cursor", "moved-from cursor");
  state_->execution->RequestCancel();
  state_->cancelled.store(true, std::memory_order_release);
  return Status::OK();
}

namespace internal {
namespace {

void CollectMetadata(const LogicalPlanNode& node, PreparedQueryPlan* plan) {
  if (node.property_binding().has_value()) {
    const PropertyBinding& binding = *node.property_binding();
    if (std::none_of(plan->referenced_properties.begin(),
                     plan->referenced_properties.end(),
                     [binding](PropertyId property) {
                       return property == binding.property;
                     })) {
      plan->referenced_properties.push_back(binding.property);
    }
    PropertyEntityKind kind = PropertyEntityKind::kVertex;
    if (!node.inputs().empty()) {
      const auto source = std::find_if(
          node.inputs().front()->schema().columns().begin(),
          node.inputs().front()->schema().columns().end(),
          [binding](const RowColumn& column) {
            return column.slot == binding.source;
          });
      if (source != node.inputs().front()->schema().columns().end() &&
          source->type == QueryType::kEdgeRef) {
        kind = PropertyEntityKind::kEdge;
      }
    }
    plan->property_bindings.push_back(
        {binding.source, binding.property, binding.output, kind, std::nullopt});
  }
  for (const auto& input : node.inputs()) CollectMetadata(*input, plan);
}

bool IsProjectedCanonicalColumn(const RowColumn& column,
                                const PreparedQueryPlan& plan) {
  if (column.slot == plan.entity_slot) {
    const QueryType expected = plan.entity_family == FactFamily::kVertexState
                                   ? QueryType::kVertexRef
                                   : QueryType::kEdgeRef;
    return column.type == expected && !column.optional;
  }
  return std::any_of(
      plan.property_bindings.begin(), plan.property_bindings.end(),
      [&column](const PreparedPropertyBinding& binding) {
        return binding.output == column;
      });
}

}  // namespace

StatusOr<PreparedQueryPlan> AnalyzeQuery(const Query& query) {
  const LogicalPlanNode* root = LogicalPlanInspector::Inspect(query);
  if (root == nullptr) {
    return Status::InvalidArgument("query", "missing logical plan");
  }

  PreparedQueryPlan plan;
  plan.output_columns = root->schema().columns();
  CollectMetadata(*root, &plan);

  // Graph operators retain the source temporal scope from their vertex scan;
  // unlike the simple canonical scan shape they are executed by the frontier
  // runtime and therefore need their three graph slots carried explicitly.
  const LogicalPlanNode* graph = nullptr;
  std::function<void(const LogicalPlanNode*)> find_graph =
      [&](const LogicalPlanNode* node) {
        if (graph != nullptr) return;
        if (node->kind() == LogicalOpKind::kExpandOut ||
            node->kind() == LogicalOpKind::kExpandIn ||
            node->kind() == LogicalOpKind::kExpandBoth ||
            node->kind() == LogicalOpKind::kKHopExpand ||
            node->kind() == LogicalOpKind::kCoexistingShortestPath ||
            node->kind() == LogicalOpKind::kEarliestArrival ||
            node->kind() == LogicalOpKind::kLatestDeparture ||
            node->kind() == LogicalOpKind::kFastestDuration) {
          graph = node;
          return;
        }
        for (const auto& child : node->inputs()) find_graph(child.get());
      };
  find_graph(root);
  if (graph != nullptr && graph->expand_spec().has_value() &&
      !graph->inputs().empty()) {
    const ExpandSpec& spec = *graph->expand_spec();
    plan.graph_expand = spec;
    plan.graph_source_slot = spec.source.id();
    plan.graph_edge_slot = spec.edge.id();
    plan.graph_destination_slot = spec.destination.id();
    plan.graph_k_hops = graph->max_hops();
    plan.graph_coexisting = graph->kind() == LogicalOpKind::kCoexistingShortestPath;
    if (graph->kind() == LogicalOpKind::kEarliestArrival ||
        graph->kind() == LogicalOpKind::kLatestDeparture ||
        graph->kind() == LogicalOpKind::kFastestDuration) {
      plan.graph_journey = graph->journey_objective() + 1;
      plan.graph_journey_slot = graph->journey_slot();
      plan.graph_duration_property = graph->journey_duration_property();
      if (plan.graph_duration_property.has_value()) {
        if (std::find(plan.referenced_properties.begin(),
                      plan.referenced_properties.end(),
                      *plan.graph_duration_property) ==
            plan.referenced_properties.end()) {
          plan.referenced_properties.push_back(*plan.graph_duration_property);
        }
      }
    }
    if (plan.graph_coexisting) {
      for (const RowColumn& column : plan.output_columns) {
        if (column.type == QueryType::kPath) {
          plan.graph_path_slot = column.slot;
          break;
        }
      }
    }
    const LogicalPlanNode* scoped = graph->inputs().front().get();
    // Graph plans can have a filter/property-binding chain below the graph
    // operator. Preserve that predicate for source-seed pruning; the graph
    // runtime otherwise starts from every entity regardless of WHERE.
    const LogicalPlanNode* metadata_node = graph->inputs().front().get();
    while (metadata_node != nullptr && !metadata_node->inputs().empty()) {
      if (metadata_node->kind() == LogicalOpKind::kFilter &&
          metadata_node->predicate()) {
        plan.predicate = metadata_node->predicate();
        break;
      }
      metadata_node = metadata_node->inputs().front().get();
    }
    while (scoped != nullptr && !scoped->scope().has_value() &&
           !scoped->inputs().empty()) {
      scoped = scoped->inputs().front().get();
    }
    if (scoped != nullptr && scoped->scope().has_value()) {
      plan.scope = *scoped->scope();
      plan.entity_family = FactFamily::kVertexState;
      plan.entity_slot = spec.source.id();
    }
    return plan;
  }
  if (root->kind() != LogicalOpKind::kProject || root->inputs().size() != 1) {
    return plan;
  }
  const LogicalPlanNode* node = root->inputs().front().get();
  if (node->kind() == LogicalOpKind::kFilter) {
    if (node->inputs().size() != 1 || !node->predicate()) return plan;
    plan.predicate = node->predicate();
    node = node->inputs().front().get();
  }
  // A canonical query may chain any number of property bindings. Metadata is
  // collected from the full logical tree above; walk the complete binding
  // chain before validating the underlying temporal scan.
  while (node->kind() == LogicalOpKind::kBindProperty) {
    if (node->inputs().size() != 1) return plan;
    node = node->inputs().front().get();
  }
  if (node->inputs().size() != 1 || !node->scope().has_value()) return plan;
  const LogicalPlanNode& scan = *node->inputs().front();
  if (!scan.inputs().empty() || scan.schema().columns().size() != 1) return plan;
  if (scan.kind() == LogicalOpKind::kVertexScan) {
    plan.entity_family = FactFamily::kVertexState;
  } else if (scan.kind() == LogicalOpKind::kEdgeScan) {
    plan.entity_family = FactFamily::kEdgeState;
  } else {
    return plan;
  }
  const RowColumn& entity = scan.schema().columns().front();
  const QueryType expected = plan.entity_family == FactFamily::kVertexState
                                 ? QueryType::kVertexRef
                                 : QueryType::kEdgeRef;
  if (entity.type != expected || entity.optional) return plan;
  plan.entity_slot = entity.slot;
  plan.scope = *node->scope();
  if (!std::all_of(plan.output_columns.begin(), plan.output_columns.end(),
                   [&plan](const RowColumn& column) {
                     return IsProjectedCanonicalColumn(column, plan);
                   })) {
    return plan;
  }
  plan.canonical_temporal = true;
  return plan;
}

StatusOr<RuntimeRelationalResult> ExecuteRelationalPlanNode(
    LogicalOpKind kind, RuntimeRelationalInput input,
    QueryReservation* reservation, FragmentBudget* fragment_budget,
    size_t max_output_rows, QueryScratch* scratch) {
  if (kind == LogicalOpKind::kUnionAll) {
    auto result = UnionAll(input.left, input.right, reservation, max_output_rows);
    if (!result.ok()) return result.status();
    return RuntimeRelationalResult{std::move(result).ConsumeValueOrDie(),
                                   std::nullopt};
  }
  if (kind == LogicalOpKind::kDistinct) {
    auto result = Distinct(input.left, reservation, max_output_rows);
    if (!result.ok()) return result.status();
    return RuntimeRelationalResult{std::move(result).ConsumeValueOrDie(),
                                   std::nullopt};
  }
  if (kind == LogicalOpKind::kSort) {
    auto result =
        Sort(input.left, input.sort_keys, reservation, max_output_rows);
    if (!result.ok()) return result.status();
    return RuntimeRelationalResult{std::move(result).ConsumeValueOrDie(),
                                   std::nullopt};
  }
  if (kind == LogicalOpKind::kLimit) {
    auto result = Limit(input.left, input.offset, input.count, reservation,
                        max_output_rows);
    if (!result.ok()) return result.status();
    return RuntimeRelationalResult{std::move(result).ConsumeValueOrDie(),
                                   std::nullopt};
  }
  if (kind == LogicalOpKind::kAggregateRows) {
    auto result = AggregateRows(
        {std::move(input.left), std::move(input.group_by),
         std::move(input.aggregates)},
        reservation, max_output_rows);
    if (!result.ok()) return result.status();
    return RuntimeRelationalResult{std::move(result).ConsumeValueOrDie(),
                                   std::nullopt};
  }
  if (kind == LogicalOpKind::kTemporalAggregate) {
    auto result = TemporalAggregate(
        {std::move(input.left), std::move(input.group_by)}, fragment_budget,
        reservation, max_output_rows);
    if (!result.ok()) return result.status();
    return RuntimeRelationalResult{std::move(result).ConsumeValueOrDie(),
                                   std::nullopt};
  }

  JoinKind join_kind;
  switch (kind) {
    case LogicalOpKind::kInnerJoin:
      join_kind = JoinKind::kInner;
      break;
    case LogicalOpKind::kSemiJoin:
      join_kind = JoinKind::kSemi;
      break;
    case LogicalOpKind::kAntiJoin:
      join_kind = JoinKind::kAnti;
      break;
    default:
      return Status::NotSupported("query runtime",
                                  "logical node is not relational");
  }

  const JoinAlgorithm algorithm = ChooseJoinAlgorithm(
      input.estimated_rows, input.sorted_keys, input.temporal);
  StatusOr<BatchStream> result = Status::NotSupported(
      "query runtime", "relational join algorithm is unavailable");
  if (algorithm == JoinAlgorithm::kIntervalMerge) {
    result = IntervalMergeJoin(
        {std::move(input.left), std::move(input.right), input.left_key,
         input.right_key, join_kind},
        fragment_budget, reservation, max_output_rows);
  } else {
    JoinInput join{std::move(input.left), std::move(input.right),
                   input.left_key, input.right_key, join_kind};
    if (algorithm == JoinAlgorithm::kIndexNestedLoop) {
      result = IndexNestedLoopJoin(join, reservation, max_output_rows);
    } else if (algorithm == JoinAlgorithm::kHash) {
      result = HashJoin(join, reservation, max_output_rows, scratch);
    } else {
      result = SortMergeJoin(join, reservation, max_output_rows, scratch);
    }
  }
  if (!result.ok()) return result.status();
  return RuntimeRelationalResult{std::move(result).ConsumeValueOrDie(),
                                 algorithm};
}

StatusOr<QueryCursor> QueryRuntime::Execute(const PreparedQueryPlan& plan,
                                            Snapshot snapshot,
                                            const Bindings& bindings,
                                            const QueryOptions& options,
                                            QueryResourcePool* resource_pool,
                                            std::function<Status(const std::shared_ptr<QueryExecutionState>&)>
                                                register_query_state,
                                            std::function<void(const std::shared_ptr<QueryExecutionState>&)>
                                                unregister_query_state,
                                            std::function<Status(const char*)>
                                                crash_fault_injector,
                                            QueryMetrics* metrics) {
  QueryOptions resolved_options = options;
  const auto lifecycle_fault_injector = crash_fault_injector;
  if (resolved_options.mode == QueryExecutionMode::kAuto &&
      plan.physical_plan != nullptr) {
    resolved_options.mode = plan.physical_plan->lane;
  }
  if (const auto* history = std::get_if<History>(&plan.scope);
      history != nullptr && !history->interval.has_value() &&
      resolved_options.mode != QueryExecutionMode::kAnalytical) {
    return Status::InvalidArgument(
        "query", "unbounded History requires an analytical budget");
  }
  StatusOr<QueryReservation> admitted = resource_pool != nullptr
      ? resource_pool->Admit(resolved_options.budget, resolved_options.mode)
      : StatusOr<QueryReservation>(QueryReservation(static_cast<size_t>(resolved_options.budget.memory_bytes)));
  if (!admitted.ok()) return admitted.status();
  std::unique_ptr<QueryScratch> scratch;
  if (resolved_options.mode == QueryExecutionMode::kAnalytical) {
    const std::string query_id =
        "query-" + std::to_string(g_next_query_id.fetch_add(1, std::memory_order_relaxed));
    std::filesystem::path scratch_root = std::filesystem::temp_directory_path() / "cedar-query-runtime";
    std::string scratch_instance = "runtime";
    uint64_t scratch_free_reserve = 0;
    if (resource_pool != nullptr && !resource_pool->options().scratch_root.empty()) {
      scratch_root = resource_pool->options().scratch_root;
      scratch_instance = resource_pool->options().scratch_instance;
      scratch_free_reserve = resource_pool->options().scratch_free_space_reserve_bytes;
    }
    scratch = std::make_unique<QueryScratch>(
        scratch_root, scratch_instance, query_id, options.budget.scratch_bytes,
        scratch_free_reserve);
    const uint64_t read_rate = resource_pool == nullptr
                                   ? 0
                                   : resource_pool->options().read_bytes_per_second;
    const uint64_t scratch_rate = resource_pool == nullptr
                                      ? 0
                                      : resource_pool->options().scratch_bytes_per_second;
    scratch->SetRateLimits(read_rate, scratch_rate);
    scratch->SetCrashFaultInjector(std::move(crash_fault_injector));
    if (resource_pool != nullptr) {
      scratch->SetIoAdmission([resource_pool](uint64_t bytes)
                                  -> StatusOr<std::shared_ptr<IoPermit>> {
        auto permit = resource_pool->AcquireIo(QueryWorkClass::kAnalytical, bytes);
        if (!permit.ok()) return permit.status();
        return std::make_shared<IoPermit>(std::move(permit).ConsumeValueOrDie());
      });
    }
  }
  auto state = std::make_unique<QueryCursor::State>(
      plan, std::move(snapshot), bindings, std::move(resolved_options),
      std::move(admitted).ConsumeValueOrDie(), std::move(scratch));
  // Fallback counters describe the source selected for this query, rather
  // than individual batches. Keep the labels fixed and charge each selected
  // fallback at most once even when a plan has multiple coverage slices.
  if (metrics != nullptr) {
    bool canonical = plan.physical_plan == nullptr;
    bool delta = false;
    bool untrusted_statistics = false;
    if (plan.physical_plan != nullptr) {
      untrusted_statistics = plan.physical_plan->conservative ||
                             plan.physical_plan->estimate.uncertain;
      for (const auto& slice : plan.physical_plan->coverage_slices) {
        canonical = canonical ||
                    slice.source == internal::CoverageSource::kCanonical;
        delta = delta ||
                slice.source == internal::CoverageSource::kDeltaMerge;
      }
    }
    if (canonical) metrics->AddFallback(internal::QueryMetricFallback::kCanonical);
    if (delta) metrics->AddFallback(internal::QueryMetricFallback::kDelta);
    if (untrusted_statistics) {
      metrics->AddFallback(internal::QueryMetricFallback::kUntrustedStatistics);
    }
  }
  state->execution->SetMetrics(metrics);
  state->execution->SetCrashFaultInjector(lifecycle_fault_injector);
  // Every cursor owns an engine Snapshot and therefore participates in the
  // ordered shutdown/pin registry. The mode only changes budgeting, never
  // snapshot lifetime.
  if (register_query_state) {
    if (unregister_query_state) {
      std::weak_ptr<QueryExecutionState> weak_state = state->execution;
      // Registration is intentionally retained until Close. The callback is
      // attached after registration and invoked by the shared terminal
      // state, including database-initiated shutdown.
      state->execution->SetCloseCallback(
          [unregister_query_state, weak_state] {
            if (auto locked = weak_state.lock()) unregister_query_state(locked);
          });
    }
    const Status registered = register_query_state(state->execution);
    if (!registered.ok()) {
      // Registration is the atomic lifecycle admission point. Close the
      // just-created state before returning so its snapshot, scratch files,
      // output leases, and any other Cedar-owned resources are released while
      // the caller still owns the cursor state.
      state->execution->Close().IgnoreError();
      return registered;
    }
  }
  return QueryCursor(std::move(state));
}

}  // namespace internal
}  // namespace cedar
