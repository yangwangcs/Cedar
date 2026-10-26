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
    FactOperation operation, FactReadMemoryScope* workspace) {
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
      const FactColumn* column = FindColumn(batch, FactColumnId::kBytesValue);
      const auto* values = column ? std::get_if<std::vector<std::string>>(&column->values) : nullptr;
      if (!values || row >= values->size()) {
        return Status::Corruption("query temporal source", "missing projected string");
      }
      const Status reserved = workspace->Reserve((*values)[row].capacity() + 1);
      if (!reserved.ok()) return reserved;
      auto value = ColumnValue<std::string>(batch, FactColumnId::kBytesValue, row);
      if (!value.ok()) return value.status();
      return std::optional<Value>{type == PhysicalType::kString
                                      ? Value::String(value.ConsumeValueOrDie())
                                      : Value::Binary(value.ConsumeValueOrDie())};
    }
  }
  return Status::Corruption("query temporal source", "unknown projected physical type");
}

StatusOr<std::shared_ptr<const FactChains>> ReadChains(const CanonicalFactReader& reader,
                                CommitSeq snapshot_seq, FactFamily family,
                                PropertyId property,
                                const PartScope& part_scope,
                                const std::shared_ptr<TemporalChainCache>& cache = nullptr,
                                std::optional<CommitSeqRange> system_range = std::nullopt,
                                std::optional<EntityRange> entity_range = std::nullopt,
                                std::shared_ptr<FactReadMemory> runtime_memory = {}) {
  std::ostringstream cache_key;
  cache_key << static_cast<unsigned>(family) << ':' << property.value << ':'
            << static_cast<unsigned>(part_scope.kind);
  for (const PartId part : part_scope.parts) cache_key << ':' << part.value;
  cache_key << ':' << snapshot_seq.value;
  if (system_range.has_value()) {
    cache_key << ":range:" << system_range->from.value << ':'
              << system_range->to.value;
  }
  if (entity_range.has_value()) {
    cache_key << ":entity:";
    if (entity_range->min) cache_key << *entity_range->min;
    cache_key << ':';
    if (entity_range->max_exclusive) cache_key << *entity_range->max_exclusive;
  }
  if (cache != nullptr) {
    std::lock_guard<std::mutex> lock(cache->mutex);
    const auto found = cache->chains.find(cache_key.str());
    if (found != cache->chains.end()) {
      return found->second;
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
                         snapshot_seq, system_range, runtime_memory);
  auto consume = [&cursor, family, property, &runtime_memory](const FactColumnarBatch& batch) {
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
          FactReadMemoryScope conversion(runtime_memory.get());
          auto value = DecodeProjectedValue(
              batch, row, static_cast<PhysicalType>(physical.ValueOrDie()),
              static_cast<FactOperation>(operation.ValueOrDie()), &conversion);
          if (!value.ok()) return value.status();
          FactEvent event{FactRef{PartId{part.ValueOrDie()}, family_value,
                                  property_value, entity.ValueOrDie()},
                          ValidTime{valid_from.ValueOrDie()},
                          CommitSeq{commit_seq.ValueOrDie()},
                          static_cast<FactOperation>(operation.ValueOrDie()),
                          schema_epoch.ValueOrDie(), value.ConsumeValueOrDie(),
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
  if (entity_range.has_value()) spec.entity_range = *entity_range;
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
  // Charge the map node/key with the chain owner before inserting it. Sharing
  // this immutable owner avoids deep copies on both insertion and lookup.
  const std::string key = cache_key.str();
  const uint64_t cache_bytes = cache ? sizeof(decltype(cache->chains)::value_type) +
                                         4 * sizeof(void*) + key.capacity() + 1 : 0;
  auto chains = cursor.TakeChains(cache_bytes);
  if (!chains.ok()) return chains.status();
  if (cache != nullptr) {
    std::lock_guard<std::mutex> lock(cache->mutex);
    cache->chains.emplace(key, chains.ValueOrDie());
  }
  return chains.ConsumeValueOrDie();
}

bool Contains(const ValidTimeInterval& interval, ValidTime time) {
  return interval.from.value <= time.value &&
         (!interval.to.has_value() || time.value < interval.to->value);
}

StatusOr<BudgetedRows<StateRow>> Materialize(
    const CanonicalFactReader& reader, CommitSeq snapshot_seq,
    FactFamily family, PropertyId property,
    std::optional<ValidTimeInterval> bounds, const PartScope& part_scope,
    const std::shared_ptr<TemporalChainCache>& cache = nullptr,
    std::optional<CommitSeqRange> system_range = std::nullopt,
    std::optional<cedar::EntityRange> entity_range = std::nullopt,
    std::shared_ptr<FactReadMemory> runtime_memory = {}) {
  if (bounds.has_value()) {
    const Status valid = bounds->Validate();
    if (!valid.ok()) return valid;
  }
  auto chains = ReadChains(reader, snapshot_seq, family, property, part_scope, cache,
                           system_range, entity_range, runtime_memory);
  if (!chains.ok()) return chains.status();

  FactReadMemoryScope output(runtime_memory.get());
  std::vector<StateRow> rows;
  for (const auto& view : *chains.ValueOrDie()) {
    for (const StateInterval& state : view.present) {
      std::optional<ValidTimeInterval> effective = state.interval;
      if (bounds.has_value()) effective = Clip(state.interval, *bounds);
      if (!effective.has_value()) continue;
      Status status = ReserveFactAppend(&rows, &output);
      if (!status.ok()) return status;
      status = output.Reserve(FactValueHeapBytes(state.value));
      if (!status.ok()) return status;
      rows.push_back({view.ref, *effective, state.value});
    }
  }
  return RetainRows(std::move(rows), &output, std::move(runtime_memory));
}

}  // namespace

StatusOr<BudgetedRows<EventRow>> TemporalSource::ReadEvents(
    Snapshot& snapshot, FactFamily family, PropertyId property,
    const ValidTimeInterval& interval, const PartScope& part_scope) {
  return ReadEvents(QueryReadContext{snapshot.canonical_reader(),
                                     snapshot.commit_seq(), part_scope, {}, {}},
                    family, property, interval);
}

StatusOr<BudgetedRows<EventRow>> TemporalSource::ReadEvents(
    const QueryReadContext& context, FactFamily family, PropertyId property,
    const ValidTimeInterval& interval) {
  const Status valid = interval.Validate();
  if (!valid.ok()) return valid;
  auto chains = ReadChains(context.facts, context.snapshot_seq, family, property,
                           context.part_scope, context.chain_cache,
                           context.system_time_range, context.entity_range,
                           context.runtime_memory);
  if (!chains.ok()) return chains.status();

  FactReadMemoryScope output(context.runtime_memory.get());
  std::vector<EventRow> rows;
  for (const auto& view : *chains.ValueOrDie()) {
    for (const CorrectedBoundary& boundary : view.boundaries) {
      if (!Contains(interval, boundary.valid_from)) continue;
      if (context.system_time_range.has_value() &&
          !context.system_time_range->Contains(boundary.commit_seq)) continue;
      Status status = ReserveFactAppend(&rows, &output);
      if (!status.ok()) return status;
      status = output.Reserve(FactValueHeapBytes(boundary.value));
      if (!status.ok()) return status;
      rows.push_back({view.ref, boundary.valid_from, boundary.commit_seq,
                      boundary.operation, boundary.value,
                      boundary.schema_epoch});
    }
  }
  return RetainRows(std::move(rows), &output, context.runtime_memory);
}

StatusOr<BudgetedRows<ChangeRow>> TemporalSource::ReadChanges(
    Snapshot& snapshot, FactFamily family, PropertyId property,
    const ValidTimeInterval& interval, const PartScope& part_scope) {
  return ReadChanges(QueryReadContext{snapshot.canonical_reader(),
                                      snapshot.commit_seq(), part_scope, {}, {}},
                     family, property, interval);
}

StatusOr<BudgetedRows<ChangeRow>> TemporalSource::ReadChanges(
    const QueryReadContext& context, FactFamily family, PropertyId property,
    const ValidTimeInterval& interval) {
  const Status valid = interval.Validate();
  if (!valid.ok()) return valid;
  auto chains = ReadChains(context.facts, context.snapshot_seq, family, property,
                           context.part_scope, context.chain_cache,
                           context.system_time_range, context.entity_range,
                           context.runtime_memory);
  if (!chains.ok()) return chains.status();

  FactReadMemoryScope output(context.runtime_memory.get());
  std::vector<ChangeRow> rows;
  for (const auto& view : *chains.ValueOrDie()) {
    bool present = false;
    const std::optional<Value> empty;
    const std::optional<Value>* value = &empty;
    for (const CorrectedBoundary& boundary : view.boundaries) {
      const bool after_present = boundary.operation == FactOperation::kPut;
      const auto& after = after_present ? boundary.value : empty;
      const bool changed = present != after_present ||
                           (present && *value != after);
      if (changed && Contains(interval, boundary.valid_from) &&
          (!context.system_time_range.has_value() ||
           context.system_time_range->Contains(boundary.commit_seq))) {
        Status status = ReserveFactAppend(&rows, &output);
        if (!status.ok()) return status;
        status = output.Reserve(FactValueHeapBytes(*value));
        if (!status.ok()) return status;
        status = output.Reserve(FactValueHeapBytes(after));
        if (!status.ok()) return status;
        rows.push_back({view.ref, boundary.valid_from,
                        present ? *value : std::nullopt,
                        after_present ? after : std::nullopt});
      }
      present = after_present;
      value = &after;
    }
  }
  return RetainRows(std::move(rows), &output, context.runtime_memory);
}

StatusOr<BudgetedRows<StateRow>> TemporalSource::ReadAt(
    Snapshot& snapshot, FactFamily family, PropertyId property,
    ValidTime valid_time, const PartScope& part_scope) {
  return ReadAt(QueryReadContext{snapshot.canonical_reader(), snapshot.commit_seq(),
                                 part_scope, {}, {}}, family, property, valid_time);
}

StatusOr<BudgetedRows<StateRow>> TemporalSource::ReadAt(
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
    FactReadMemoryScope output(context.runtime_memory.get());
    std::vector<StateRow> bounded;
    const Status status = context.facts.ReadStateRows(
        spec, [&bounded, &output](const std::vector<CanonicalStateRow>& batch) {
          for (const CanonicalStateRow& row : batch) {
            Status status = ReserveFactAppend(&bounded, &output);
            if (!status.ok()) return status;
            status = output.Reserve(FactValueHeapBytes(row.value));
            if (!status.ok()) return status;
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
    return RetainRows(std::move(bounded), &output, context.runtime_memory);
  }
  auto rows = Materialize(context.facts, context.snapshot_seq, family, property,
                          std::nullopt, context.part_scope, context.chain_cache,
                          context.system_time_range, context.entity_range,
                          context.runtime_memory);
  if (!rows.ok()) return rows.status();
  uint64_t kept = 0;
  rows.ValueOrDie().EraseIf([&](const StateRow& row) {
    if (!Contains(row.effective, valid_time)) return true;
    return context.max_rows.has_value() && kept++ >= *context.max_rows;
  });
  return rows.ConsumeValueOrDie();
}

StatusOr<BudgetedRows<StateRow>> TemporalSource::ReadHistory(
    Snapshot& snapshot, FactFamily family, PropertyId property,
    std::optional<ValidTimeInterval> interval, const PartScope& part_scope) {
  return ReadHistory(QueryReadContext{snapshot.canonical_reader(), snapshot.commit_seq(),
                                      part_scope, {}, {}}, family, property,
                     std::move(interval));
}

StatusOr<BudgetedRows<StateRow>> TemporalSource::ReadHistory(
    const QueryReadContext& context, FactFamily family, PropertyId property,
    std::optional<ValidTimeInterval> interval) {
  return Materialize(context.facts, context.snapshot_seq, family, property,
                     std::move(interval), context.part_scope, context.chain_cache,
                     context.system_time_range, context.entity_range,
                     context.runtime_memory);
}

StatusOr<BudgetedRows<StateRow>> TemporalSource::ReadOverlaps(
    Snapshot& snapshot, FactFamily family, PropertyId property,
    const ValidTimeInterval& interval, const PartScope& part_scope) {
  return ReadOverlaps(QueryReadContext{snapshot.canonical_reader(), snapshot.commit_seq(),
                                       part_scope, {}, {}}, family, property, interval);
}

StatusOr<BudgetedRows<StateRow>> TemporalSource::ReadOverlaps(
    const QueryReadContext& context, FactFamily family, PropertyId property,
    const ValidTimeInterval& interval) {
  return Materialize(context.facts, context.snapshot_seq, family, property,
                     interval, context.part_scope, context.chain_cache,
                     context.system_time_range, context.entity_range,
                     context.runtime_memory);
}

StatusOr<BudgetedRows<StateRow>> TemporalSource::ReadThroughout(
    Snapshot& snapshot, FactFamily family, PropertyId property,
    const ValidTimeInterval& interval, const PartScope& part_scope) {
  return ReadThroughout(QueryReadContext{snapshot.canonical_reader(), snapshot.commit_seq(),
                                         part_scope, {}, {}}, family, property, interval);
}

StatusOr<BudgetedRows<StateRow>> TemporalSource::ReadThroughout(
    const QueryReadContext& context, FactFamily family, PropertyId property,
    const ValidTimeInterval& interval) {
  const Status valid = interval.Validate();
  if (!valid.ok()) return valid;
  auto rows = Materialize(context.facts, context.snapshot_seq, family, property,
                          std::nullopt, context.part_scope, context.chain_cache,
                          context.system_time_range, context.entity_range,
                          context.runtime_memory);
  if (!rows.ok()) return rows.status();
  rows.ValueOrDie().EraseIf([&](const StateRow& row) {
    const bool starts_before = row.effective.from.value <= interval.from.value;
    const bool ends_after = !row.effective.to.has_value() ||
                            (interval.to.has_value() &&
                             interval.to->value <= row.effective.to->value);
    return !starts_before || !ends_after;
  });
  for (auto& row : rows.ValueOrDie()) row.effective = interval;
  return rows.ConsumeValueOrDie();
}

}  // namespace cedar::internal
