// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/runtime/property_binding.h"

#include <algorithm>
#include <map>
#include <utility>

#include "query/temporal/interval.h"
#include "query/runtime/read_context.h"

namespace cedar::internal {
namespace {

FactFamily PropertyFamily(PropertyEntityKind kind) {
  return kind == PropertyEntityKind::kVertex
             ? FactFamily::kVertexProperty
             : FactFamily::kEdgeProperty;
}

bool Before(ValidTime left, ValidTime right) {
  return left.value < right.value;
}

bool Contains(const ValidTimeInterval& interval, ValidTime time) {
  return interval.from.value <= time.value &&
         (!interval.to.has_value() || time.value < interval.to->value);
}

Status ValidateEvents(const std::vector<EventRow>& events,
                      const PropertyDefinition& definition) {
  for (const EventRow& event : events) {
    if (event.schema_epoch != definition.schema_epoch) {
      return Status::SchemaMismatch(
          "query", "property event schema epoch differs from prepared schema");
    }
    if (event.operation == FactOperation::kPut &&
        (!event.value.has_value() ||
         event.value->type() != definition.physical_type)) {
      return Status::SchemaMismatch(
          "query", "property event physical type differs from prepared schema");
    }
  }
  return Status::OK();
}

}  // namespace

StatusOr<BudgetedRows<BoundPropertyRow>> PropertyBinder::BindIntervals(
    Snapshot& snapshot, const std::vector<StateRow>& entities,
    const PropertyDefinition& definition, const PartScope& part_scope) {
  return BindIntervals(QueryReadContext{snapshot.canonical_reader(),
                                        snapshot.commit_seq(), part_scope, {}, {}},
                       entities, definition);
}

StatusOr<BudgetedRows<BoundPropertyRow>> PropertyBinder::BindIntervals(
    const QueryReadContext& context, const std::vector<StateRow>& entities,
    const PropertyDefinition& definition) {
  const Status valid = definition.Validate();
  if (!valid.ok()) return valid;
  const FactFamily family = PropertyFamily(definition.entity_kind);
  auto events = TemporalSource::ReadEvents(
      context, family, definition.property_id,
      ValidTimeInterval{ValidTime{0}, std::nullopt});
  if (!events.ok()) return events.status();
  const Status event_schema = ValidateEvents(events.ValueOrDie(), definition);
  if (!event_schema.ok()) return event_schema;

  auto property_states = TemporalSource::ReadHistory(
      context, family, definition.property_id, std::nullopt);
  if (!property_states.ok()) return property_states.status();
  using PropertyKey = std::pair<uint32_t, uint64_t>;
  FactReadMemoryScope index_memory(context.runtime_memory.get());
  std::map<PropertyKey, std::vector<const StateRow*>> states_by_entity;
  for (const StateRow& state : property_states.ValueOrDie()) {
    const PropertyKey key{state.ref.part_id().value, state.ref.entity_id()};
    auto found = states_by_entity.find(key);
    if (found == states_by_entity.end()) {
      const Status status = index_memory.Reserve(
          sizeof(decltype(states_by_entity)::value_type) + 4 * sizeof(void*));
      if (!status.ok()) return status;
      found = states_by_entity.emplace(key, std::vector<const StateRow*>{}).first;
    }
    const Status status = ReserveFactAppend(&found->second, &index_memory);
    if (!status.ok()) return status;
    found->second.push_back(&state);
  }

  FactReadMemoryScope output(context.runtime_memory.get());
  std::vector<BoundPropertyRow> result;
  auto append = [&](const FactRef& ref, ValidTimeInterval interval,
                    const std::optional<Value>& value) {
    Status status = ReserveFactAppend(&result, &output);
    if (!status.ok()) return status;
    status = output.Reserve(FactValueHeapBytes(value));
    if (!status.ok()) return status;
    result.push_back({ref, interval, value});
    return Status::OK();
  };
  for (const StateRow& entity : entities) {
    const PropertyEntityKind entity_kind =
        entity.ref.family() == FactFamily::kVertexState
            ? PropertyEntityKind::kVertex
            : PropertyEntityKind::kEdge;
    if (entity_kind != definition.entity_kind) {
      return Status::SchemaMismatch(
          "query", "property entity kind differs from bound entity");
    }

    ValidTime cursor = entity.effective.from;
    const auto found = states_by_entity.find(
        {entity.ref.part_id().value, entity.ref.entity_id()});
    if (found != states_by_entity.end()) {
      for (const StateRow* property_ptr : found->second) {
        const StateRow& property = *property_ptr;
        const auto clipped = Intersect(entity.effective, property.effective);
        if (!clipped.has_value()) continue;
        if (Before(cursor, clipped->from)) {
          const Status status = append(entity.ref, {cursor, clipped->from}, std::nullopt);
          if (!status.ok()) return status;
        }
        const Status status = append(entity.ref, *clipped, property.value);
        if (!status.ok()) return status;
        if (!clipped->to.has_value()) {
          cursor = clipped->from;
          break;
        }
        cursor = *clipped->to;
      }
    }
    if (!entity.effective.to.has_value() ||
        Before(cursor, *entity.effective.to)) {
      const bool already_unbounded = !result.empty() &&
                                     result.back().ref == entity.ref &&
                                     !result.back().effective.to.has_value();
      if (!already_unbounded) {
        const Status status = append(entity.ref, {cursor, entity.effective.to}, std::nullopt);
        if (!status.ok()) return status;
      }
    }
  }
  return RetainRows(std::move(result), &output, context.runtime_memory);
}

StatusOr<BudgetedRows<BoundPropertyRow>> PropertyBinder::BindAt(
    Snapshot& snapshot, const std::vector<StateRow>& entities,
    ValidTime valid_time, const PropertyDefinition& definition,
    const PartScope& part_scope) {
  return BindAt(QueryReadContext{snapshot.canonical_reader(), snapshot.commit_seq(),
                                 part_scope, {}, {}}, entities, valid_time,
                definition);
}

StatusOr<BudgetedRows<BoundPropertyRow>> PropertyBinder::BindAt(
    const QueryReadContext& context, const std::vector<StateRow>& entities,
    ValidTime valid_time, const PropertyDefinition& definition) {
  auto rows = BindIntervals(context, entities, definition);
  if (!rows.ok()) return rows.status();
  rows.ValueOrDie().EraseIf([&](const BoundPropertyRow& row) {
    return !Contains(row.effective, valid_time);
  });
  return rows.ConsumeValueOrDie();
}

}  // namespace cedar::internal
