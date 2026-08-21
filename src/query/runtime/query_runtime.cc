// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/runtime/query_runtime.h"

#include <algorithm>
#include <map>
#include <optional>
#include <type_traits>
#include <utility>

#include "query/logical/logical_plan.h"
#include "query/runtime/property_binding.h"
#include "query/runtime/temporal_source.h"

namespace cedar {
namespace {

struct RuntimeRow {
  FactRef ref;
  std::optional<ValidTimeInterval> effective;
  std::optional<ValidTime> point;
  std::optional<Value> property_value;
};

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
    const internal::PreparedQueryPlan& plan) {
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
    if (!row.property_value.has_value()) {
      return EvaluatedValue{binding->output.type, false, std::nullopt};
    }
    return ValueAsLiteral(*row.property_value);
  }
  if (expression.kind() == ExpressionKind::kLiteral) {
    if (!expression.literal().has_value()) {
      return Status::InvalidArgument("query", "literal expression has no value");
    }
    return EvaluatedValue{expression.type(), true, expression.literal()};
  }
  if (expression.kind() == ExpressionKind::kParameter) {
    return Status::NotSupported("query", "canonical predicates do not bind parameters");
  }
  if (expression.kind() == ExpressionKind::kIsPresent) {
    auto child = EvaluateExpression(*expression.children().front(), row, plan);
    if (!child.ok()) return child.status();
    return EvaluatedValue{QueryType::kBool, true,
                          EvaluatedLiteral{child.ValueOrDie().present}};
  }
  if (expression.kind() == ExpressionKind::kNot) {
    auto child = EvaluateExpression(*expression.children().front(), row, plan);
    if (!child.ok()) return child.status();
    const bool value = child.ValueOrDie().present &&
                       std::get<bool>(*child.ValueOrDie().value);
    return EvaluatedValue{QueryType::kBool, true, EvaluatedLiteral{!value}};
  }

  auto left = EvaluateExpression(*expression.children()[0], row, plan);
  if (!left.ok()) return left.status();
  auto right = EvaluateExpression(*expression.children()[1], row, plan);
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
    Snapshot& snapshot, const internal::PreparedQueryPlan& plan) {
  std::vector<RuntimeRow> result;
  return std::visit(
      [&](const auto& scope) -> StatusOr<std::vector<RuntimeRow>> {
        using T = std::decay_t<decltype(scope)>;
        if constexpr (std::is_same_v<T, At>) {
          auto rows = internal::TemporalSource::ReadAt(
              snapshot, plan.entity_family, PropertyId{}, scope.time);
          if (!rows.ok()) return rows.status();
          for (internal::StateRow& row : rows.ValueOrDie()) {
            result.push_back(
                {row.ref, row.effective, scope.time, std::nullopt});
          }
        } else if constexpr (std::is_same_v<T, Events>) {
          auto rows = internal::TemporalSource::ReadEvents(
              snapshot, plan.entity_family, PropertyId{}, scope.interval);
          if (!rows.ok()) return rows.status();
          for (const internal::EventRow& row : rows.ValueOrDie()) {
            result.push_back(
                {row.ref, std::nullopt, row.valid_from, std::nullopt});
          }
        } else if constexpr (std::is_same_v<T, Changes>) {
          auto rows = internal::TemporalSource::ReadChanges(
              snapshot, plan.entity_family, PropertyId{}, scope.interval);
          if (!rows.ok()) return rows.status();
          for (const internal::ChangeRow& row : rows.ValueOrDie()) {
            result.push_back(
                {row.ref, std::nullopt, row.valid_from, std::nullopt});
          }
        } else if constexpr (std::is_same_v<T, Overlaps>) {
          auto rows = internal::TemporalSource::ReadOverlaps(
              snapshot, plan.entity_family, PropertyId{}, scope.interval);
          if (!rows.ok()) return rows.status();
          for (internal::StateRow& row : rows.ValueOrDie()) {
            result.push_back(
                {row.ref, row.effective, std::nullopt, std::nullopt});
          }
        } else if constexpr (std::is_same_v<T, Throughout>) {
          auto rows = internal::TemporalSource::ReadThroughout(
              snapshot, plan.entity_family, PropertyId{}, scope.interval);
          if (!rows.ok()) return rows.status();
          for (internal::StateRow& row : rows.ValueOrDie()) {
            result.push_back(
                {row.ref, row.effective, std::nullopt, std::nullopt});
          }
        } else {
          auto rows = internal::TemporalSource::ReadHistory(
              snapshot, plan.entity_family, PropertyId{}, scope.interval);
          if (!rows.ok()) return rows.status();
          for (internal::StateRow& row : rows.ValueOrDie()) {
            result.push_back(
                {row.ref, row.effective, std::nullopt, std::nullopt});
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
    for (const internal::ProjectionChain& chain : chains.ValueOrDie()) {
      if (slice.source == internal::CoverageSource::kDeltaMerge) {
        std::map<uint64_t, std::vector<const internal::ProjectionInterval*>> by_entity;
        for (const auto& interval : chain.intervals) by_entity[interval.entity_id].push_back(&interval);
        for (const auto& [entity_id, intervals] : by_entity) {
          std::vector<internal::CorrectedBoundary> base;
          for (const auto* interval : intervals) {
            base.push_back({interval->effective.from, chain.header.base_seq,
                            FactOperation::kPut, 0, interval->value, std::nullopt});
            if (interval->effective.to) base.push_back({*interval->effective.to,
                                                         chain.header.base_seq,
                                                         FactOperation::kDelete, 0,
                                                         std::nullopt, std::nullopt});
          }
          std::sort(base.begin(), base.end(), [](const auto& a, const auto& b) {
            return a.valid_from.value < b.valid_from.value;
          });
          const FactRef ref{chain.header.part_id, plan.entity_family,
                            slice.property_id.value_or(PropertyId{}), entity_id};
          auto merged = internal::QueryDelta::MergeBoundaries(base, delta->EventsFor(ref), delta->through);
          if (!merged.ok()) return merged.status();
          for (const auto& state : internal::MaterializePresentState(merged.ValueOrDie())) {
            rows.push_back({ref, state.interval, std::nullopt, state.value});
          }
        }
        continue;
      }
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

StatusOr<std::vector<RuntimeRow>> BindPropertyRows(
    Snapshot& snapshot, std::vector<RuntimeRow> rows,
    const internal::PreparedPropertyBinding& binding) {
  if (!binding.definition.has_value()) {
    return Status::SchemaMismatch("query", "property binding was not prepared");
  }
  std::vector<RuntimeRow> result;
  for (RuntimeRow& row : rows) {
    internal::StateRow entity{row.ref,
                              row.effective.value_or(ValidTimeInterval{
                                  row.point.value_or(ValidTime{0}), std::nullopt}),
                              std::nullopt};
    StatusOr<std::vector<internal::BoundPropertyRow>> bound =
        row.point.has_value()
            ? internal::PropertyBinder::BindAt(
                  snapshot, std::vector<internal::StateRow>{entity}, *row.point,
                  *binding.definition)
            : internal::PropertyBinder::BindIntervals(
                  snapshot, std::vector<internal::StateRow>{entity},
                  *binding.definition);
    if (!bound.ok()) return bound.status();
    for (internal::BoundPropertyRow& property : bound.ValueOrDie()) {
      result.push_back({property.ref, property.effective, row.point,
                        std::move(property.value)});
    }
  }
  return result;
}

StatusOr<std::vector<RuntimeRow>> MaterializeRows(
    Snapshot& snapshot, const internal::PreparedQueryPlan& plan) {
  StatusOr<std::vector<RuntimeRow>> rows = ReadSourceRows(snapshot, plan);
  if (!rows.ok()) return rows.status();
  if (plan.physical_plan && plan.projection_reader) {
    auto derived = ReadProjectionRows(plan);
    if (!derived.ok() && !derived.status().IsNotFound()) return derived.status();
    if (derived.ok()) {
      std::vector<RuntimeRow> combined;
      for (const auto& slice : plan.physical_plan->coverage_slices) {
        const auto& source = slice.source;
        const auto& input = source == internal::CoverageSource::kProjection
                                ? derived.ValueOrDie()
                                : rows.ValueOrDie();
        for (const auto& row : input) {
          if (RowInInterval(row, slice.interval)) combined.push_back(row);
        }
      }
      rows = std::move(combined);
    }
  }
  if (!rows.ok()) return rows.status();
  if (!plan.property_bindings.empty()) {
    rows = BindPropertyRows(snapshot, std::move(rows).ConsumeValueOrDie(),
                            plan.property_bindings.front());
    if (!rows.ok()) return rows.status();
  }
  if (plan.predicate) {
    std::vector<RuntimeRow> filtered;
    for (RuntimeRow& row : rows.ValueOrDie()) {
      auto selected = EvaluateExpression(*plan.predicate, row, plan);
      if (!selected.ok()) return selected.status();
      if (selected.ValueOrDie().present &&
          std::get<bool>(*selected.ValueOrDie().value)) {
        filtered.push_back(std::move(row));
      }
    }
    return filtered;
  }
  return std::move(rows).ConsumeValueOrDie();
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
    const internal::PreparedQueryPlan& plan) {
  std::vector<QueryColumn> columns;
  columns.reserve(plan.output_columns.size());
  for (const RowColumn& output : plan.output_columns) {
    columns.push_back(
        QueryColumn{output.slot, output.type, EmptyColumn(output.type), {}});
    ReserveColumn(&columns.back(), count);
  }
  for (size_t index = offset; index < offset + count; ++index) {
    const RuntimeRow& row = rows[index];
    for (size_t column_index = 0; column_index < columns.size(); ++column_index) {
      QueryColumn* column = &columns[column_index];
      const RowColumn& output = plan.output_columns[column_index];
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
      const Status appended = AppendProperty(column, row.property_value);
      if (!appended.ok()) return appended;
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

class QueryCursor::State {
 public:
  State(internal::PreparedQueryPlan plan, Snapshot snapshot,
        QueryOptions options)
      : plan(std::move(plan)), snapshot(std::move(snapshot)),
        options(std::move(options)),
        reservation(this->options.budget.memory_bytes),
        fragments(this->options.budget.interval_fragments) {}

  internal::PreparedQueryPlan plan;
  std::optional<Snapshot> snapshot;
  QueryOptions options;
  internal::QueryReservation reservation;
  internal::FragmentBudget fragments;
  std::shared_ptr<internal::QueryReservationLease> materialized_output_lease;
  std::vector<QueryBatch> batches;
  size_t next_batch = 0;
  bool initialized = false;
  bool clean_terminal = false;
  std::optional<Status> terminal_error;
};

QueryCursor::QueryCursor(std::unique_ptr<State> state)
    : state_(std::move(state)) {}
QueryCursor::~QueryCursor() = default;
QueryCursor::QueryCursor(QueryCursor&&) noexcept = default;
QueryCursor& QueryCursor::operator=(QueryCursor&&) noexcept = default;

StatusOr<std::optional<QueryBatch>> QueryCursor::Next() {
  if (!state_) {
    return Status::InvalidArgument("query cursor", "moved-from cursor");
  }
  if (state_->terminal_error.has_value()) return *state_->terminal_error;
  if (state_->clean_terminal) return std::optional<QueryBatch>{};

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
          state_->options.budget.output_rows);
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
      constexpr size_t kBatchRows = 1024;
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
    auto rows = MaterializeRows(*state_->snapshot, state_->plan);
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
    if (row_count > state_->options.budget.interval_fragments) {
      state_->terminal_error = Status::ResourceExhausted(
          "query", "interval fragment budget exceeded");
      state_->snapshot.reset();
      return *state_->terminal_error;
    }

    constexpr size_t kBatchRows = 1024;
    for (size_t offset = 0; offset < row_count; offset += kBatchRows) {
      const size_t count = std::min(kBatchRows, row_count - offset);
      auto columns =
          BuildColumns(rows.ValueOrDie(), offset, count, state_->plan);
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
    return std::optional<QueryBatch>{
        std::move(state_->batches[state_->next_batch++])};
  }
  state_->batches.clear();
  state_->materialized_output_lease.reset();
  state_->snapshot.reset();
  state_->clean_terminal = true;
  return std::optional<QueryBatch>{};
}

Status QueryCursor::Close() {
  if (!state_) {
    return Status::InvalidArgument("query cursor", "moved-from cursor");
  }
  state_->batches.clear();
  state_->materialized_output_lease.reset();
  state_->snapshot.reset();
  state_->terminal_error.reset();
  state_->clean_terminal = true;
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
  if (root->kind() != LogicalOpKind::kProject || root->inputs().size() != 1) {
    return plan;
  }
  const LogicalPlanNode* node = root->inputs().front().get();
  if (node->kind() == LogicalOpKind::kFilter) {
    if (node->inputs().size() != 1 || !node->predicate()) return plan;
    plan.predicate = node->predicate();
    node = node->inputs().front().get();
  }
  if (node->kind() == LogicalOpKind::kBindProperty) {
    if (node->inputs().size() != 1 || plan.property_bindings.size() != 1) {
      return plan;
    }
    node = node->inputs().front().get();
  } else if (!plan.property_bindings.empty()) {
    return plan;
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
    size_t max_output_rows) {
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
      result = HashJoin(join, reservation, max_output_rows);
    } else {
      result = SortMergeJoin(join, reservation, max_output_rows);
    }
  }
  if (!result.ok()) return result.status();
  return RuntimeRelationalResult{std::move(result).ConsumeValueOrDie(),
                                 algorithm};
}

StatusOr<QueryCursor> QueryRuntime::Execute(const PreparedQueryPlan& plan,
                                            Snapshot snapshot,
                                            const Bindings&,
                                            const QueryOptions& options) {
  if (const auto* history = std::get_if<History>(&plan.scope);
      history != nullptr && !history->interval.has_value() &&
      options.mode != QueryExecutionMode::kAnalytical) {
    return Status::InvalidArgument(
        "query", "unbounded History requires an analytical budget");
  }
  return QueryCursor(std::make_unique<QueryCursor::State>(
      plan, std::move(snapshot), options));
}

}  // namespace internal
}  // namespace cedar
