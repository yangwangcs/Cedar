// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/runtime/temporal_source.h"

#include <map>
#include <string>
#include <utility>

#include "query/temporal/corrected_chain.h"
#include "query/temporal/interval.h"

namespace cedar::internal {
namespace {

struct FactEntityKey {
  PartId part_id;
  uint64_t entity_id = 0;

  bool operator<(const FactEntityKey& other) const {
    if (part_id.value != other.part_id.value) {
      return part_id.value < other.part_id.value;
    }
    return entity_id < other.entity_id;
  }
};

using FactChains = std::map<FactEntityKey, std::vector<FactEvent>>;

const FactColumn* FindColumn(const FactColumnarBatch& batch, FactColumnId id) {
  for (const FactColumn& column : batch.columns) {
    if (column.id == id) return &column;
  }
  return nullptr;
}

template <typename T>
StatusOr<T> ColumnValue(const FactColumnarBatch& batch, FactColumnId id,
                        size_t row) {
  const FactColumn* column = FindColumn(batch, id);
  if (column == nullptr || row >= column->present.size() ||
      column->present[row] == 0) {
    return Status::Corruption("query temporal source", "missing projected fact column");
  }
  const auto* values = std::get_if<std::vector<T>>(&column->values);
  if (values == nullptr || row >= values->size()) {
    return Status::Corruption("query temporal source", "projected fact column type mismatch");
  }
  return (*values)[row];
}

StatusOr<std::optional<Value>> DecodeProjectedValue(
    const FactColumnarBatch& batch, size_t row, PhysicalType type,
    FactOperation operation) {
  if (operation == FactOperation::kDelete || type == PhysicalType{})
    return std::optional<Value>{};
  switch (type) {
    case PhysicalType::kBool: {
      auto value = ColumnValue<uint8_t>(batch, FactColumnId::kBoolValue, row);
      if (!value.ok()) return value.status();
      return std::optional<Value>{Value::Bool(value.ValueOrDie() != 0)};
    }
    case PhysicalType::kInt32: {
      auto value = ColumnValue<int32_t>(batch, FactColumnId::kInt32Value, row);
      if (!value.ok()) return value.status();
      return std::optional<Value>{Value::Int32(value.ValueOrDie())};
    }
    case PhysicalType::kInt64: {
      auto value = ColumnValue<int64_t>(batch, FactColumnId::kInt64Value, row);
      if (!value.ok()) return value.status();
      return std::optional<Value>{Value::Int64(value.ValueOrDie())};
    }
    case PhysicalType::kFloat32: {
      auto value = ColumnValue<float>(batch, FactColumnId::kFloat32Value, row);
      if (!value.ok()) return value.status();
      return std::optional<Value>{Value::Float32(value.ValueOrDie())};
    }
    case PhysicalType::kFloat64: {
      auto value = ColumnValue<double>(batch, FactColumnId::kFloat64Value, row);
      if (!value.ok()) return value.status();
      return std::optional<Value>{Value::Float64(value.ValueOrDie())};
    }
    case PhysicalType::kTimestamp64: {
      auto value = ColumnValue<uint64_t>(batch, FactColumnId::kTimestamp64Value, row);
      if (!value.ok()) return value.status();
      return std::optional<Value>{Value::Timestamp(value.ValueOrDie())};
    }
    case PhysicalType::kString:
    case PhysicalType::kBinary: {
      auto value = ColumnValue<std::string>(batch, FactColumnId::kBytesValue, row);
      if (!value.ok()) return value.status();
      return std::optional<Value>{type == PhysicalType::kString
                                      ? Value::String(value.ValueOrDie())
                                      : Value::Binary(value.ValueOrDie())};
    }
  }
  return Status::Corruption("query temporal source", "unknown projected physical type");
}

StatusOr<FactChains> ReadChains(Snapshot& snapshot, FactFamily family,
                                PropertyId property) {
  FactChains chains;
  const std::vector<FactColumnId> projection = {
      FactColumnId::kPartId,       FactColumnId::kEntityId,
      FactColumnId::kValidFrom,    FactColumnId::kCedarCommitSeq,
      FactColumnId::kOperation,    FactColumnId::kSchemaEpoch,
      FactColumnId::kPhysicalType, FactColumnId::kBoolValue,
      FactColumnId::kInt32Value,   FactColumnId::kInt64Value,
      FactColumnId::kFloat32Value, FactColumnId::kFloat64Value,
      FactColumnId::kTimestamp64Value, FactColumnId::kBytesValue};
  const Status scanned = snapshot.EventColumnarScanFamily(
      family, property, projection,
      [&chains, family, property](const FactColumnarBatch& batch) {
        const size_t rows = batch.row_count();
        for (size_t row = 0; row < rows; ++row) {
          auto part = ColumnValue<uint32_t>(batch, FactColumnId::kPartId, row);
          auto entity = ColumnValue<uint64_t>(batch, FactColumnId::kEntityId, row);
          auto valid_from = ColumnValue<uint64_t>(batch, FactColumnId::kValidFrom, row);
          auto commit_seq = ColumnValue<uint64_t>(batch, FactColumnId::kCedarCommitSeq, row);
          auto operation = ColumnValue<uint32_t>(batch, FactColumnId::kOperation, row);
          auto schema_epoch = ColumnValue<uint32_t>(batch, FactColumnId::kSchemaEpoch, row);
          auto physical = ColumnValue<uint32_t>(batch, FactColumnId::kPhysicalType, row);
          if (!part.ok()) return part.status();
          if (!entity.ok()) return entity.status();
          if (!valid_from.ok()) return valid_from.status();
          if (!commit_seq.ok()) return commit_seq.status();
          if (!operation.ok()) return operation.status();
          if (!schema_epoch.ok()) return schema_epoch.status();
          if (!physical.ok()) return physical.status();
          const auto family_value = family;
          const auto property_value = property;
          auto value = DecodeProjectedValue(
              batch, row, static_cast<PhysicalType>(physical.ValueOrDie()),
              static_cast<FactOperation>(operation.ValueOrDie()));
          if (!value.ok()) return value.status();
          FactEvent event{FactRef{PartId{part.ValueOrDie()}, family_value,
                                  property_value, entity.ValueOrDie()},
                          ValidTime{valid_from.ValueOrDie()},
                          CommitSeq{commit_seq.ValueOrDie()},
                          static_cast<FactOperation>(operation.ValueOrDie()),
                          schema_epoch.ValueOrDie(), value.ValueOrDie(),
                          std::nullopt};
          chains[{event.ref.part_id(), event.ref.entity_id()}].push_back(
              std::move(event));
        }
        return Status::OK();
      });
  if (!scanned.ok()) return scanned;
  return chains;
}

bool Contains(const ValidTimeInterval& interval, ValidTime time) {
  return interval.from.value <= time.value &&
         (!interval.to.has_value() || time.value < interval.to->value);
}

StatusOr<std::vector<StateRow>> Materialize(
    Snapshot& snapshot, FactFamily family, PropertyId property,
    std::optional<ValidTimeInterval> bounds) {
  if (bounds.has_value()) {
    const Status valid = bounds->Validate();
    if (!valid.ok()) return valid;
  }
  auto chains = ReadChains(snapshot, family, property);
  if (!chains.ok()) return chains.status();

  std::vector<StateRow> rows;
  for (const auto& [key, events] : chains.ValueOrDie()) {
    auto corrected = ResolveCorrectedBoundaries(events, snapshot.commit_seq());
    if (!corrected.ok()) return corrected.status();
    for (const StateInterval& state :
         MaterializePresentState(corrected.ValueOrDie())) {
      std::optional<ValidTimeInterval> effective = state.interval;
      if (bounds.has_value()) effective = Clip(state.interval, *bounds);
      if (!effective.has_value()) continue;
      rows.push_back({FactRef{key.part_id, family, property, key.entity_id}, *effective,
                      state.value});
    }
  }
  return rows;
}

}  // namespace

StatusOr<std::vector<EventRow>> TemporalSource::ReadEvents(
    Snapshot& snapshot, FactFamily family, PropertyId property,
    const ValidTimeInterval& interval) {
  const Status valid = interval.Validate();
  if (!valid.ok()) return valid;
  auto chains = ReadChains(snapshot, family, property);
  if (!chains.ok()) return chains.status();

  std::vector<EventRow> rows;
  for (const auto& [key, events] : chains.ValueOrDie()) {
    auto corrected = ResolveCorrectedBoundaries(events, snapshot.commit_seq());
    if (!corrected.ok()) return corrected.status();
    for (const CorrectedBoundary& boundary : corrected.ValueOrDie()) {
      if (!Contains(interval, boundary.valid_from)) continue;
      rows.push_back({FactRef{key.part_id, family, property, key.entity_id},
                      boundary.valid_from, boundary.commit_seq,
                      boundary.operation, boundary.value,
                      boundary.schema_epoch});
    }
  }
  return rows;
}

StatusOr<std::vector<ChangeRow>> TemporalSource::ReadChanges(
    Snapshot& snapshot, FactFamily family, PropertyId property,
    const ValidTimeInterval& interval) {
  const Status valid = interval.Validate();
  if (!valid.ok()) return valid;
  auto chains = ReadChains(snapshot, family, property);
  if (!chains.ok()) return chains.status();

  std::vector<ChangeRow> rows;
  for (const auto& [key, events] : chains.ValueOrDie()) {
    auto corrected = ResolveCorrectedBoundaries(events, snapshot.commit_seq());
    if (!corrected.ok()) return corrected.status();
    bool present = false;
    std::optional<Value> value;
    for (const CorrectedBoundary& boundary : corrected.ValueOrDie()) {
      const bool after_present = boundary.operation == FactOperation::kPut;
      const std::optional<Value> after = after_present ? boundary.value
                                                       : std::nullopt;
      const bool changed = present != after_present ||
                           (present && value != after);
      if (changed && Contains(interval, boundary.valid_from)) {
        rows.push_back({FactRef{key.part_id, family, property, key.entity_id},
                        boundary.valid_from, present ? value : std::nullopt,
                        after_present ? after : std::nullopt});
      }
      present = after_present;
      value = after;
    }
  }
  return rows;
}

StatusOr<std::vector<StateRow>> TemporalSource::ReadAt(
    Snapshot& snapshot, FactFamily family, PropertyId property,
    ValidTime valid_time) {
  auto rows = Materialize(snapshot, family, property, std::nullopt);
  if (!rows.ok()) return rows.status();
  std::vector<StateRow> result;
  for (StateRow& row : rows.ValueOrDie()) {
    if (Contains(row.effective, valid_time)) result.push_back(std::move(row));
  }
  return result;
}

StatusOr<std::vector<StateRow>> TemporalSource::ReadHistory(
    Snapshot& snapshot, FactFamily family, PropertyId property,
    std::optional<ValidTimeInterval> interval) {
  return Materialize(snapshot, family, property, std::move(interval));
}

StatusOr<std::vector<StateRow>> TemporalSource::ReadOverlaps(
    Snapshot& snapshot, FactFamily family, PropertyId property,
    const ValidTimeInterval& interval) {
  return Materialize(snapshot, family, property, interval);
}

StatusOr<std::vector<StateRow>> TemporalSource::ReadThroughout(
    Snapshot& snapshot, FactFamily family, PropertyId property,
    const ValidTimeInterval& interval) {
  const Status valid = interval.Validate();
  if (!valid.ok()) return valid;
  auto rows = Materialize(snapshot, family, property, std::nullopt);
  if (!rows.ok()) return rows.status();
  std::vector<StateRow> result;
  for (const StateRow& row : rows.ValueOrDie()) {
    const bool starts_before = row.effective.from.value <= interval.from.value;
    const bool ends_after = !row.effective.to.has_value() ||
                            (interval.to.has_value() &&
                             interval.to->value <= row.effective.to->value);
    if (starts_before && ends_after) {
      result.push_back({row.ref, interval, row.value});
    }
  }
  return result;
}

}  // namespace cedar::internal
