// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "query/runtime/temporal_source.h"

#include <mutex>
#include <sstream>
#include <string>
#include <utility>

#include "query/temporal/corrected_chain.h"
#include "query/temporal/interval.h"
#include "query/runtime/read_context.h"
#include "query/runtime/fact_chain_cursor.h"

namespace cedar::internal {
namespace {

using FactChains = std::vector<FactChainView>;

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

StatusOr<FactChains> ReadChains(const CanonicalFactReader& reader,
                                CommitSeq snapshot_seq, FactFamily family,
                                PropertyId property,
                                const PartScope& part_scope,
                                const std::shared_ptr<TemporalChainCache>& cache = nullptr,
                                std::optional<CommitSeqRange> system_range = std::nullopt) {
  FactChains chains;
  std::ostringstream cache_key;
  cache_key << static_cast<unsigned>(family) << ':' << property.value << ':'
            << static_cast<unsigned>(part_scope.kind);
  for (const PartId part : part_scope.parts) cache_key << ':' << part.value;
  cache_key << ':' << snapshot_seq.value;
  if (system_range.has_value()) {
    cache_key << ":range:" << system_range->from.value << ':'
              << system_range->to.value;
  }
  if (cache != nullptr) {
    std::lock_guard<std::mutex> lock(cache->mutex);
    const auto found = cache->chains.find(cache_key.str());
    if (found != cache->chains.end()) {
      return *found->second;
    }
  }
  const std::vector<FactColumnId> projection = {
      FactColumnId::kPartId,       FactColumnId::kEntityId,
      FactColumnId::kValidFrom,    FactColumnId::kCedarCommitSeq,
      FactColumnId::kOperation,    FactColumnId::kSchemaEpoch,
      FactColumnId::kPhysicalType, FactColumnId::kBoolValue,
      FactColumnId::kInt32Value,   FactColumnId::kInt64Value,
      FactColumnId::kFloat32Value, FactColumnId::kFloat64Value,
      FactColumnId::kTimestamp64Value, FactColumnId::kBytesValue};
  FactChainCursor cursor(FactBatchOrder::kIdentityValidDescCommitDesc,
                         snapshot_seq, system_range);
  auto consume = [&cursor, family, property](const FactColumnarBatch& batch) {
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
          Status consumed = cursor.Consume(event);
          if (!consumed.ok()) return consumed;
        }
        return Status::OK();
      };
  FactReadSpec spec;
  spec.part_scope = part_scope;
  spec.family = family;
  spec.property_id = property;
  spec.projection = projection;
  spec.batch_row_limit = 1024;
  spec.commit_seq_max = snapshot_seq.value == 0
                            ? std::nullopt
                            : std::optional<CommitSeq>{snapshot_seq};
  spec.preserve_predecessor_context = system_range.has_value();
  const Status scanned = reader.ReadColumnar(spec, consume);
  if (!scanned.ok()) return scanned;
  const Status finished = cursor.Finish(snapshot_seq);
  if (!finished.ok()) return finished;
  chains = cursor.chains();
  if (cache != nullptr) {
    auto reduced = std::make_shared<const std::vector<FactChainView>>(chains);
    std::lock_guard<std::mutex> lock(cache->mutex);
    cache->chains.emplace(cache_key.str(), std::move(reduced));
  }
  return chains;
}

bool Contains(const ValidTimeInterval& interval, ValidTime time) {
  return interval.from.value <= time.value &&
         (!interval.to.has_value() || time.value < interval.to->value);
}

StatusOr<std::vector<StateRow>> Materialize(
    const CanonicalFactReader& reader, CommitSeq snapshot_seq,
    FactFamily family, PropertyId property,
    std::optional<ValidTimeInterval> bounds, const PartScope& part_scope,
    const std::shared_ptr<TemporalChainCache>& cache = nullptr,
    std::optional<CommitSeqRange> system_range = std::nullopt) {
  if (bounds.has_value()) {
    const Status valid = bounds->Validate();
    if (!valid.ok()) return valid;
  }
  auto chains = ReadChains(reader, snapshot_seq, family, property, part_scope, cache,
                           system_range);
  if (!chains.ok()) return chains.status();

  std::vector<StateRow> rows;
  for (const auto& view : chains.ValueOrDie()) {
    for (const StateInterval& state : view.present) {
      std::optional<ValidTimeInterval> effective = state.interval;
      if (bounds.has_value()) effective = Clip(state.interval, *bounds);
      if (!effective.has_value()) continue;
      rows.push_back({view.ref, *effective, state.value});
    }
  }
  return rows;
}

}  // namespace

StatusOr<std::vector<EventRow>> TemporalSource::ReadEvents(
    Snapshot& snapshot, FactFamily family, PropertyId property,
    const ValidTimeInterval& interval, const PartScope& part_scope) {
  return ReadEvents(QueryReadContext{snapshot.canonical_reader(),
                                     snapshot.commit_seq(), part_scope, {}, {}},
                    family, property, interval);
}

StatusOr<std::vector<EventRow>> TemporalSource::ReadEvents(
    const QueryReadContext& context, FactFamily family, PropertyId property,
    const ValidTimeInterval& interval) {
  const Status valid = interval.Validate();
  if (!valid.ok()) return valid;
  auto chains = ReadChains(context.facts, context.snapshot_seq, family, property,
                           context.part_scope, context.chain_cache,
                           context.system_time_range);
  if (!chains.ok()) return chains.status();

  std::vector<EventRow> rows;
  for (const auto& view : chains.ValueOrDie()) {
    for (const CorrectedBoundary& boundary : view.boundaries) {
      if (!Contains(interval, boundary.valid_from)) continue;
      if (context.system_time_range.has_value() &&
          !context.system_time_range->Contains(boundary.commit_seq)) continue;
      rows.push_back({view.ref, boundary.valid_from, boundary.commit_seq,
                      boundary.operation, boundary.value,
                      boundary.schema_epoch});
    }
  }
  return rows;
}

StatusOr<std::vector<ChangeRow>> TemporalSource::ReadChanges(
    Snapshot& snapshot, FactFamily family, PropertyId property,
    const ValidTimeInterval& interval, const PartScope& part_scope) {
  return ReadChanges(QueryReadContext{snapshot.canonical_reader(),
                                      snapshot.commit_seq(), part_scope, {}, {}},
                     family, property, interval);
}

StatusOr<std::vector<ChangeRow>> TemporalSource::ReadChanges(
    const QueryReadContext& context, FactFamily family, PropertyId property,
    const ValidTimeInterval& interval) {
  const Status valid = interval.Validate();
  if (!valid.ok()) return valid;
  auto chains = ReadChains(context.facts, context.snapshot_seq, family, property,
                           context.part_scope, context.chain_cache,
                           context.system_time_range);
  if (!chains.ok()) return chains.status();

  std::vector<ChangeRow> rows;
  for (const auto& view : chains.ValueOrDie()) {
    bool present = false;
    std::optional<Value> value;
    for (const CorrectedBoundary& boundary : view.boundaries) {
      const bool after_present = boundary.operation == FactOperation::kPut;
      const std::optional<Value> after = after_present ? boundary.value
                                                       : std::nullopt;
      const bool changed = present != after_present ||
                           (present && value != after);
      if (changed && Contains(interval, boundary.valid_from) &&
          (!context.system_time_range.has_value() ||
           context.system_time_range->Contains(boundary.commit_seq))) {
        rows.push_back({view.ref, boundary.valid_from,
                        present ? value : std::nullopt,
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
    ValidTime valid_time, const PartScope& part_scope) {
  return ReadAt(QueryReadContext{snapshot.canonical_reader(), snapshot.commit_seq(),
                                 part_scope, {}, {}}, family, property, valid_time);
}

StatusOr<std::vector<StateRow>> TemporalSource::ReadAt(
    const QueryReadContext& context, FactFamily family, PropertyId property,
    ValidTime valid_time) {
  // A state-row cap is only safe for the planner-proven direct canonical
  // shape. System-time overlays require predecessor context and stay on the
  // existing complete chain path.
  if (context.max_rows.has_value() && !context.system_time_range.has_value()) {
    CanonicalStateReadSpec spec;
    spec.facts.part_scope = context.part_scope;
    spec.facts.family = family;
    spec.facts.property_id = property;
    spec.facts.batch_row_limit = 1024;
    spec.snapshot_seq = context.snapshot_seq;
    spec.valid_time = valid_time;
    spec.max_rows = context.max_rows;
    std::vector<StateRow> bounded;
    const Status status = context.facts.ReadStateRows(
        spec, [&bounded](const std::vector<CanonicalStateRow>& batch) {
          bounded.reserve(bounded.size() + batch.size());
          for (const CanonicalStateRow& row : batch) {
            bounded.push_back({row.ref, row.effective, row.value});
          }
          return Status::OK();
        });
    if (!status.ok()) return status;
    if (context.on_limit_early_stop &&
        context.max_rows.has_value() &&
        bounded.size() >= *context.max_rows && *context.max_rows != 0) {
      context.on_limit_early_stop();
    }
    return bounded;
  }
  auto rows = Materialize(context.facts, context.snapshot_seq, family, property,
                          std::nullopt, context.part_scope, context.chain_cache,
                          context.system_time_range);
  if (!rows.ok()) return rows.status();
  std::vector<StateRow> result;
  for (StateRow& row : rows.ValueOrDie()) {
    if (Contains(row.effective, valid_time)) result.push_back(std::move(row));
  }
  if (context.max_rows.has_value() &&
      result.size() > *context.max_rows) {
    result.erase(result.begin() + static_cast<size_t>(*context.max_rows),
                 result.end());
  }
  return result;
}

StatusOr<std::vector<StateRow>> TemporalSource::ReadHistory(
    Snapshot& snapshot, FactFamily family, PropertyId property,
    std::optional<ValidTimeInterval> interval, const PartScope& part_scope) {
  return ReadHistory(QueryReadContext{snapshot.canonical_reader(), snapshot.commit_seq(),
                                      part_scope, {}, {}}, family, property,
                     std::move(interval));
}

StatusOr<std::vector<StateRow>> TemporalSource::ReadHistory(
    const QueryReadContext& context, FactFamily family, PropertyId property,
    std::optional<ValidTimeInterval> interval) {
  return Materialize(context.facts, context.snapshot_seq, family, property,
                     std::move(interval), context.part_scope, context.chain_cache,
                     context.system_time_range);
}

StatusOr<std::vector<StateRow>> TemporalSource::ReadOverlaps(
    Snapshot& snapshot, FactFamily family, PropertyId property,
    const ValidTimeInterval& interval, const PartScope& part_scope) {
  return ReadOverlaps(QueryReadContext{snapshot.canonical_reader(), snapshot.commit_seq(),
                                       part_scope, {}, {}}, family, property, interval);
}

StatusOr<std::vector<StateRow>> TemporalSource::ReadOverlaps(
    const QueryReadContext& context, FactFamily family, PropertyId property,
    const ValidTimeInterval& interval) {
  return Materialize(context.facts, context.snapshot_seq, family, property,
                     interval, context.part_scope, context.chain_cache,
                     context.system_time_range);
}

StatusOr<std::vector<StateRow>> TemporalSource::ReadThroughout(
    Snapshot& snapshot, FactFamily family, PropertyId property,
    const ValidTimeInterval& interval, const PartScope& part_scope) {
  return ReadThroughout(QueryReadContext{snapshot.canonical_reader(), snapshot.commit_seq(),
                                         part_scope, {}, {}}, family, property, interval);
}

StatusOr<std::vector<StateRow>> TemporalSource::ReadThroughout(
    const QueryReadContext& context, FactFamily family, PropertyId property,
    const ValidTimeInterval& interval) {
  const Status valid = interval.Validate();
  if (!valid.ok()) return valid;
  auto rows = Materialize(context.facts, context.snapshot_seq, family, property,
                          std::nullopt, context.part_scope, context.chain_cache,
                          context.system_time_range);
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
