// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/runtime/temporal_source.h"

#include <algorithm>
#include <map>
#include <utility>

#include "query/temporal/corrected_chain.h"
#include "query/temporal/interval.h"

namespace cedar::internal {
namespace {

using FactChains = std::map<uint64_t, std::vector<FactEvent>>;

const FactColumn* FindColumn(const FactColumnarBatch& batch, FactColumnId id) {
  const auto found = std::find_if(
      batch.columns.begin(), batch.columns.end(),
      [id](const FactColumn& column) { return column.id == id; });
  return found == batch.columns.end() ? nullptr : &*found;
}

template <typename T>
StatusOr<T> RequiredValue(const FactColumnarBatch& batch, FactColumnId id,
                          size_t row) {
  const FactColumn* column = FindColumn(batch, id);
  if (column == nullptr || row >= column->present.size() ||
      column->present[row] == 0) {
    return Status::Corruption("temporal source", "required projected value is absent");
  }
  const auto* values = std::get_if<std::vector<T>>(&column->values);
  if (values == nullptr || row >= values->size()) {
    return Status::Corruption("temporal source", "projected vector type mismatch");
  }
  return (*values)[row];
}

template <typename T>
StatusOr<std::optional<T>> OptionalValue(const FactColumnarBatch& batch,
                                         FactColumnId id, size_t row) {
  const FactColumn* column = FindColumn(batch, id);
  if (column == nullptr || row >= column->present.size()) {
    return Status::Corruption("temporal source", "projected value is missing");
  }
  const auto* values = std::get_if<std::vector<T>>(&column->values);
  if (values == nullptr || row >= values->size()) {
    return Status::Corruption("temporal source", "projected vector type mismatch");
  }
  if (column->present[row] == 0) return std::optional<T>{};
  return std::optional<T>{(*values)[row]};
}

StatusOr<std::optional<Value>> DecodeValue(const FactColumnarBatch& batch,
                                           size_t row) {
  auto physical = RequiredValue<uint32_t>(batch, FactColumnId::kPhysicalType,
                                          row);
  if (!physical.ok()) return physical.status();
  switch (static_cast<PhysicalType>(physical.ValueOrDie())) {
    case PhysicalType::kBool: {
      auto value = OptionalValue<uint8_t>(batch, FactColumnId::kBoolValue, row);
      if (!value.ok()) return value.status();
      return value.ValueOrDie().has_value()
                 ? std::optional<Value>{Value::Bool(*value.ValueOrDie() != 0)}
                 : std::optional<Value>{};
    }
    case PhysicalType::kInt32: {
      auto value = OptionalValue<int32_t>(batch, FactColumnId::kInt32Value, row);
      if (!value.ok()) return value.status();
      return value.ValueOrDie().has_value()
                 ? std::optional<Value>{Value::Int32(*value.ValueOrDie())}
                 : std::optional<Value>{};
    }
    case PhysicalType::kInt64: {
      auto value = OptionalValue<int64_t>(batch, FactColumnId::kInt64Value, row);
      if (!value.ok()) return value.status();
      return value.ValueOrDie().has_value()
                 ? std::optional<Value>{Value::Int64(*value.ValueOrDie())}
                 : std::optional<Value>{};
    }
    case PhysicalType::kFloat32: {
      auto value = OptionalValue<float>(batch, FactColumnId::kFloat32Value, row);
      if (!value.ok()) return value.status();
      return value.ValueOrDie().has_value()
                 ? std::optional<Value>{Value::Float32(*value.ValueOrDie())}
                 : std::optional<Value>{};
    }
    case PhysicalType::kFloat64: {
      auto value = OptionalValue<double>(batch, FactColumnId::kFloat64Value, row);
      if (!value.ok()) return value.status();
      return value.ValueOrDie().has_value()
                 ? std::optional<Value>{Value::Float64(*value.ValueOrDie())}
                 : std::optional<Value>{};
    }
    case PhysicalType::kTimestamp64: {
      auto value =
          OptionalValue<uint64_t>(batch, FactColumnId::kTimestamp64Value, row);
      if (!value.ok()) return value.status();
      return value.ValueOrDie().has_value()
                 ? std::optional<Value>{Value::Timestamp(*value.ValueOrDie())}
                 : std::optional<Value>{};
    }
    case PhysicalType::kString:
    case PhysicalType::kBinary: {
      auto value =
          OptionalValue<std::string>(batch, FactColumnId::kBytesValue, row);
      if (!value.ok()) return value.status();
      if (!value.ValueOrDie().has_value()) return std::optional<Value>{};
      return static_cast<PhysicalType>(physical.ValueOrDie()) ==
                     PhysicalType::kString
                 ? std::optional<Value>{Value::String(*value.ValueOrDie())}
                 : std::optional<Value>{Value::Binary(*value.ValueOrDie())};
    }
    default:
      return std::optional<Value>{};
  }
}

StatusOr<FactChains> ReadChains(Snapshot& snapshot, FactFamily family,
                                PropertyId property) {
  FactScanSpec scan;
  scan.part_id = PartId{0};
  scan.family = family;
  scan.property_id = property;
  const std::vector<FactColumnId> projection = {
      FactColumnId::kEntityId,       FactColumnId::kValidFrom,
      FactColumnId::kCedarCommitSeq, FactColumnId::kOperation,
      FactColumnId::kSchemaEpoch,    FactColumnId::kPhysicalType,
      FactColumnId::kBoolValue,      FactColumnId::kInt32Value,
      FactColumnId::kInt64Value,     FactColumnId::kFloat32Value,
      FactColumnId::kFloat64Value,   FactColumnId::kTimestamp64Value,
      FactColumnId::kBytesValue};

  FactChains chains;
  const Status scanned = snapshot.EventColumnarScan(
      scan, projection,
      [&chains, family, property](const FactColumnarBatch& batch) -> Status {
        for (size_t row = 0; row < batch.row_count(); ++row) {
          auto entity =
              RequiredValue<uint64_t>(batch, FactColumnId::kEntityId, row);
          auto valid_from =
              RequiredValue<uint64_t>(batch, FactColumnId::kValidFrom, row);
          auto commit = RequiredValue<uint64_t>(
              batch, FactColumnId::kCedarCommitSeq, row);
          auto operation =
              RequiredValue<uint32_t>(batch, FactColumnId::kOperation, row);
          auto schema_epoch =
              RequiredValue<uint32_t>(batch, FactColumnId::kSchemaEpoch, row);
          if (!entity.ok()) return entity.status();
          if (!valid_from.ok()) return valid_from.status();
          if (!commit.ok()) return commit.status();
          if (!operation.ok()) return operation.status();
          if (!schema_epoch.ok()) return schema_epoch.status();

          std::optional<Value> value;
          if (static_cast<FactOperation>(operation.ValueOrDie()) ==
              FactOperation::kPut) {
            auto decoded = DecodeValue(batch, row);
            if (!decoded.ok()) return decoded.status();
            value = std::move(decoded).ConsumeValueOrDie();
          }
          FactEvent event{FactRef{PartId{0}, family, property,
                                  entity.ValueOrDie()},
                          ValidTime{valid_from.ValueOrDie()},
                          CommitSeq{commit.ValueOrDie()},
                          static_cast<FactOperation>(operation.ValueOrDie()),
                          schema_epoch.ValueOrDie(), std::move(value),
                          std::nullopt};
          const Status valid = event.Validate();
          if (!valid.ok()) return valid;
          chains[entity.ValueOrDie()].push_back(std::move(event));
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
  for (const auto& [entity, events] : chains.ValueOrDie()) {
    auto corrected = ResolveCorrectedBoundaries(events, snapshot.commit_seq());
    if (!corrected.ok()) return corrected.status();
    for (const StateInterval& state :
         MaterializePresentState(corrected.ValueOrDie())) {
      std::optional<ValidTimeInterval> effective = state.interval;
      if (bounds.has_value()) effective = Clip(state.interval, *bounds);
      if (!effective.has_value()) continue;
      rows.push_back({FactRef{PartId{0}, family, property, entity}, *effective,
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
  for (const auto& [entity, events] : chains.ValueOrDie()) {
    auto corrected = ResolveCorrectedBoundaries(events, snapshot.commit_seq());
    if (!corrected.ok()) return corrected.status();
    for (const CorrectedBoundary& boundary : corrected.ValueOrDie()) {
      if (!Contains(interval, boundary.valid_from)) continue;
      rows.push_back({FactRef{PartId{0}, family, property, entity},
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
  for (const auto& [entity, events] : chains.ValueOrDie()) {
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
        rows.push_back({FactRef{PartId{0}, family, property, entity},
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
