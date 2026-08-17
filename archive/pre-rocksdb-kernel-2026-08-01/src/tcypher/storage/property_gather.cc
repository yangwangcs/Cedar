// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/tcypher/storage/property_gather.h"

#include <limits>
#include <map>

#include "cedar/tcypher/storage/temporal_scan.h"

namespace cedar {
namespace {

class GatherMemoryLease {
 public:
  GatherMemoryLease(std::shared_ptr<QueryMemoryAccount> account, uint64_t bytes)
      : account_(std::move(account)), bytes_(bytes) {}
  ~GatherMemoryLease() {
    if (account_ && bytes_ != 0) account_->Release(bytes_);
  }
  Status Reserve(uint64_t bytes) {
    if (!account_ || bytes == 0) return Status::OK();
    const Status reserved = account_->Reserve(bytes);
    if (!reserved.ok()) return reserved;
    bytes_ += bytes;
    return Status::OK();
  }

 private:
  std::shared_ptr<QueryMemoryAccount> account_;
  uint64_t bytes_ = 0;
};

Status AppendCompactedColumn(const ColumnBatch& input, uint32_t column,
                             ColumnBatch* output,
                             std::shared_ptr<void> retention = nullptr) {
  std::vector<Value> values;
  std::vector<bool> validity;
  values.reserve(input.row_count());
  validity.reserve(input.row_count());
  for (uint32_t row = 0; row < input.row_count(); ++row) {
    const Value* value = input.ValueRefAt(column, row);
    validity.push_back(value != nullptr);
    values.push_back(value != nullptr ? *value : Value::Bool(false));
  }
  return output->AddVector(std::make_shared<FlatVector>(
      std::move(values), std::move(validity), std::move(retention)));
}

}  // namespace

struct PinnedPropertyGatherCursor::Impl {
  ColumnBatch entities;
  PinnedTemporalScanSources sources;
  TemporalScanSpec scan_spec;
  PropertyGatherSpec spec;
  std::shared_ptr<GatherMemoryLease> lease;
  std::vector<std::vector<Value>> values;
  std::vector<std::vector<bool>> validity;
  size_t property_index = 0;
  uint32_t row = 0;
  bool finished = false;
};

PinnedPropertyGatherCursor::PinnedPropertyGatherCursor(
    std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}
PinnedPropertyGatherCursor::PinnedPropertyGatherCursor(
    PinnedPropertyGatherCursor&&) noexcept = default;
PinnedPropertyGatherCursor& PinnedPropertyGatherCursor::operator=(
    PinnedPropertyGatherCursor&&) noexcept = default;
PinnedPropertyGatherCursor::~PinnedPropertyGatherCursor() = default;

StatusOr<PinnedPropertyGatherCursor> OpenPinnedPropertyGather(
    ColumnBatch entities, PinnedTemporalScanSources property_sources,
    TemporalScanSpec scan_spec, PropertyGatherSpec spec) {
  if (spec.column_ids.empty() ||
      (spec.snapshot_seq == 0 && !property_sources.session_overlay) ||
      entities.column_count() <= kEdgeType ||
      spec.schema_epochs.size() != spec.column_ids.size() ||
      (!spec.blob_predicate_probes.empty() &&
       spec.blob_predicate_probes.size() != spec.column_ids.size())) {
    return Status::InvalidArgument("property gather", "invalid pinned gather request");
  }
  const uint64_t vector_count = static_cast<uint64_t>(entities.column_count()) +
      static_cast<uint64_t>(spec.column_ids.size());
  const uint64_t row_count = entities.row_count();
  const uint64_t per_vector = sizeof(FlatVector) +
      row_count * (sizeof(Value) + sizeof(bool));
  if (vector_count > (std::numeric_limits<uint64_t>::max() -
                      sizeof(GatherMemoryLease)) / per_vector) {
    return Status::QueryMemoryLimit("property gather", "gather charge overflow");
  }
  uint64_t fixed_charge = sizeof(GatherMemoryLease) + vector_count * per_vector;
  for (const auto& probes : spec.blob_predicate_probes) {
    if (!probes) continue;
    for (const BlobPredicateProbe& probe : *probes) {
      if (probe.memory_lease) continue;
      if (fixed_charge > std::numeric_limits<uint64_t>::max() -
                             sizeof(BlobPredicateProbe)) {
        return Status::QueryMemoryLimit(
            "property gather", "Blob predicate probe charge overflow");
      }
      fixed_charge += sizeof(BlobPredicateProbe);
      const uint64_t payload = probe.literal.Encode().size();
      if (fixed_charge > std::numeric_limits<uint64_t>::max() - payload) {
        return Status::QueryMemoryLimit(
            "property gather", "Blob predicate literal charge overflow");
      }
      fixed_charge += payload;
    }
  }
  for (uint32_t column = 0; column < entities.column_count(); ++column) {
    for (uint32_t row = 0; row < entities.row_count(); ++row) {
      const uint64_t payload = entities.RetainedPayloadBytesAt(column, row);
      if (fixed_charge > std::numeric_limits<uint64_t>::max() - payload) {
        return Status::QueryMemoryLimit("property gather", "gather payload charge overflow");
      }
      fixed_charge += payload;
    }
  }
  if (scan_spec.memory_account) {
    const Status reserved = scan_spec.memory_account->Reserve(fixed_charge);
    if (!reserved.ok()) return reserved;
  }
  auto impl = std::make_unique<PinnedPropertyGatherCursor::Impl>();
  impl->lease = std::make_shared<GatherMemoryLease>(
      scan_spec.memory_account, fixed_charge);
  impl->entities = std::move(entities);
  impl->sources = std::move(property_sources);
  impl->scan_spec = std::move(scan_spec);
  impl->scan_spec.valid_time = spec.valid_time;
  impl->scan_spec.snapshot_seq = spec.snapshot_seq;
  impl->scan_spec.batch_capacity = 1;
  impl->scan_spec.key_kind = LogicalKeyKind::kProperty;
  impl->spec = std::move(spec);
  impl->values.resize(impl->spec.column_ids.size());
  impl->validity.resize(impl->spec.column_ids.size());
  for (size_t property = 0; property < impl->spec.column_ids.size(); ++property) {
    impl->values[property].assign(impl->entities.row_count(), Value::Bool(false));
    impl->validity[property].assign(impl->entities.row_count(), false);
  }
  return PinnedPropertyGatherCursor(std::move(impl));
}

Status PinnedPropertyGatherCursor::Advance(
    uint32_t max_lookups, uint32_t* completed_lookups) {
  if (!impl_ || completed_lookups == nullptr || max_lookups == 0) {
    return Status::InvalidArgument("property gather", "invalid gather quantum");
  }
  *completed_lookups = 0;
  while (!done() && *completed_lookups < max_lookups) {
    if (impl_->scan_spec.cancellation &&
        impl_->scan_spec.cancellation->IsCancelled()) {
      return Status::QueryCancelled("property gather", "query cancelled before property I/O");
    }
    const uint32_t property_id = impl_->spec.column_ids[impl_->property_index];
    if (property_id > std::numeric_limits<uint16_t>::max()) {
      return Status::InvalidArgument("property gather", "property id exceeds UInt16");
    }
    const auto entity_type = impl_->entities.ValueAt(kEntityType, impl_->row);
    const auto entity_id = impl_->entities.ValueAt(kEntityId, impl_->row);
    if (!entity_type.has_value() || !entity_id.has_value() ||
        entity_type->type() != PhysicalType::kInt32 ||
        entity_id->type() != PhysicalType::kInt64 ||
        std::get<int64_t>(entity_id->data()) < 0) {
      return Status::InvalidArgument("property gather", "entity identity is invalid");
    }
    const EntityType type = static_cast<EntityType>(
        std::get<int32_t>(entity_type->data()));
    const uint64_t id = static_cast<uint64_t>(
        std::get<int64_t>(entity_id->data()));
    std::optional<LogicalKey> key;
    if (type == EntityType::Vertex) {
      key = LogicalKey::VertexProperty(id, static_cast<uint16_t>(property_id));
    } else if (type == EntityType::EdgeOut || type == EntityType::EdgeIn) {
      const auto target_id = impl_->entities.ValueAt(kTargetId, impl_->row);
      const auto edge_id = impl_->entities.ValueAt(kEdgeId, impl_->row);
      const auto edge_type = impl_->entities.ValueAt(kEdgeType, impl_->row);
      if (!target_id.has_value() || !edge_id.has_value() ||
          !edge_type.has_value() || target_id->type() != PhysicalType::kInt64 ||
          edge_id->type() != PhysicalType::kInt64 ||
          edge_type->type() != PhysicalType::kInt32 ||
          std::get<int64_t>(target_id->data()) < 0 ||
          std::get<int64_t>(edge_id->data()) < 0 ||
          std::get<int32_t>(edge_type->data()) < 0 ||
          std::get<int32_t>(edge_type->data()) >
              std::numeric_limits<uint16_t>::max()) {
        return Status::InvalidArgument("property gather", "edge identity is invalid");
      }
      key = LogicalKey::EdgeProperty(
          id, static_cast<uint64_t>(std::get<int64_t>(target_id->data())),
          static_cast<uint16_t>(std::get<int32_t>(edge_type->data())),
          static_cast<uint64_t>(std::get<int64_t>(edge_id->data())),
          static_cast<uint16_t>(property_id), type);
    } else {
      return Status::InvalidArgument("property gather", "entity type is invalid");
    }
    impl_->scan_spec.entity_type = type;
    impl_->scan_spec.column_id = static_cast<uint16_t>(property_id);
    impl_->scan_spec.schema_epoch =
        impl_->spec.schema_epochs[impl_->property_index];
    impl_->scan_spec.blob_predicate_probes =
        impl_->spec.blob_predicate_probes.empty()
            ? nullptr
            : impl_->spec.blob_predicate_probes[impl_->property_index];
    impl_->scan_spec.exact_key = *key;
    if (impl_->spec.valid_time_column.has_value()) {
      const Value* valid = impl_->entities.ValueRefAt(
          *impl_->spec.valid_time_column, impl_->row);
      if (valid == nullptr || valid->type() != PhysicalType::kTimestamp64) {
        return Status::InvalidArgument(
            "property gather", "row valid-time column is invalid");
      }
      impl_->scan_spec.valid_time = std::get<uint64_t>(valid->data());
    }
    auto opened = OpenPinnedTemporalScan(impl_->sources, impl_->scan_spec);
    if (!opened.ok()) return opened.status();
    ColumnBatch property;
    const Status scanned = opened.ValueOrDie().NextMorsel(&property);
    if (!scanned.IsNotFound() && !scanned.ok()) return scanned;
    if (scanned.ok()) {
      const Value* value = property.ValueRefAt(kValue, 0);
      const uint64_t retained_payload =
          property.RetainedPayloadBytesAt(kValue, 0);
      const Status payload_reserved = impl_->lease->Reserve(retained_payload);
      if (!payload_reserved.ok()) return payload_reserved;
      impl_->validity[impl_->property_index][impl_->row] = value != nullptr;
      impl_->values[impl_->property_index][impl_->row] =
          value != nullptr ? *value : Value::Bool(false);
      const uint64_t copied_payload = property.ValuePayloadBytesAt(kValue, 0);
      if (impl_->spec.payload_copy_observer && copied_payload != 0) {
        impl_->spec.payload_copy_observer(copied_payload);
      }
    }
    ++*completed_lookups;
    ++impl_->row;
    if (impl_->row == impl_->entities.row_count()) {
      impl_->row = 0;
      ++impl_->property_index;
    }
  }
  return Status::OK();
}

bool PinnedPropertyGatherCursor::done() const {
  return impl_ && (impl_->entities.row_count() == 0 ||
                   impl_->property_index == impl_->spec.column_ids.size());
}

Status PinnedPropertyGatherCursor::Finish(ColumnBatch* gathered) {
  if (!impl_ || gathered == nullptr || !done() || impl_->finished) {
    return Status::InvalidArgument("property gather", "gather is not ready to finish");
  }
  ColumnBatch result(impl_->entities.row_count());
  for (uint32_t column = 0; column < impl_->entities.column_count(); ++column) {
    const Status appended = AppendCompactedColumn(
        impl_->entities, column, &result, impl_->lease);
    if (!appended.ok()) return appended;
  }
  for (size_t property = 0; property < impl_->values.size(); ++property) {
    const Status appended = result.AddVector(std::make_shared<FlatVector>(
        std::move(impl_->values[property]),
        std::move(impl_->validity[property]), impl_->lease));
    if (!appended.ok()) return appended;
  }
  impl_->finished = true;
  *gathered = std::move(result);
  return Status::OK();
}

Status BatchGatherProperties(const ColumnBatch& entities,
                             const std::vector<TemporalEvent>& property_candidates,
                             const PropertyGatherSpec& spec, ColumnBatch* gathered) {
  if (gathered == nullptr || spec.column_ids.empty() || spec.snapshot_seq == 0 ||
      entities.column_count() <= kEdgeType ||
      (!spec.schema_epochs.empty() &&
       spec.schema_epochs.size() != spec.column_ids.size())) {
    return Status::InvalidArgument("property gather", "invalid property gather request");
  }
  std::map<LogicalKey, std::vector<TemporalEvent>> by_key;
  for (const TemporalEvent& event : property_candidates) {
    by_key[event.logical_key()].push_back(event);
  }

  ColumnBatch result(entities.row_count());
  for (uint32_t column = 0; column < entities.column_count(); ++column) {
    const Status appended = AppendCompactedColumn(entities, column, &result);
    if (!appended.ok()) return appended;
  }
  for (uint32_t property_id : spec.column_ids) {
    if (property_id > std::numeric_limits<uint16_t>::max()) {
      return Status::InvalidArgument("property gather", "property id exceeds UInt16");
    }
    std::vector<Value> values;
    std::vector<bool> validity;
    values.reserve(entities.row_count());
    validity.reserve(entities.row_count());
    for (uint32_t row = 0; row < entities.row_count(); ++row) {
      const auto entity_type = entities.ValueAt(kEntityType, row);
      const auto entity_id = entities.ValueAt(kEntityId, row);
      if (!entity_type.has_value() || !entity_id.has_value() ||
          entity_type->type() != PhysicalType::kInt32 || entity_id->type() != PhysicalType::kInt64 ||
          std::get<int64_t>(entity_id->data()) < 0) {
        return Status::InvalidArgument("property gather", "entity identity is invalid");
      }
      const auto type = static_cast<EntityType>(std::get<int32_t>(entity_type->data()));
      const uint64_t id = static_cast<uint64_t>(std::get<int64_t>(entity_id->data()));
      std::optional<LogicalKey> key;
      if (type == EntityType::Vertex) {
        key = LogicalKey::VertexProperty(id, static_cast<uint16_t>(property_id));
      } else if (type == EntityType::EdgeOut || type == EntityType::EdgeIn) {
        const auto target_id = entities.ValueAt(kTargetId, row);
        const auto edge_id = entities.ValueAt(kEdgeId, row);
        const auto edge_type = entities.ValueAt(kEdgeType, row);
        if (!target_id.has_value() || !edge_id.has_value() || !edge_type.has_value() ||
            target_id->type() != PhysicalType::kInt64 ||
            edge_id->type() != PhysicalType::kInt64 ||
            edge_type->type() != PhysicalType::kInt32 ||
            std::get<int64_t>(target_id->data()) < 0 ||
            std::get<int64_t>(edge_id->data()) < 0 ||
            std::get<int32_t>(edge_type->data()) < 0 ||
            std::get<int32_t>(edge_type->data()) > std::numeric_limits<uint16_t>::max()) {
          return Status::InvalidArgument("property gather", "edge identity is invalid");
        }
        key = LogicalKey::EdgeProperty(
            id, static_cast<uint64_t>(std::get<int64_t>(target_id->data())),
            static_cast<uint16_t>(std::get<int32_t>(edge_type->data())),
            static_cast<uint64_t>(std::get<int64_t>(edge_id->data())),
            static_cast<uint16_t>(property_id), type);
      } else {
        return Status::InvalidArgument("property gather", "entity type is invalid");
      }
      const auto candidates = by_key.find(*key);
      const auto value = candidates == by_key.end()
          ? std::optional<Value>{}
          : ResolveValue(candidates->second, *key, spec.valid_time, spec.snapshot_seq);
      validity.push_back(value.has_value());
      values.push_back(value.value_or(Value::Bool(false)));
    }
    const Status appended = result.AddVector(std::make_shared<FlatVector>(
        std::move(values), std::move(validity)));
    if (!appended.ok()) return appended;
  }
  *gathered = std::move(result);
  return Status::OK();
}

Status BatchGatherProperties(
    const ColumnBatch& entities,
    const PinnedTemporalScanSources& property_sources,
    TemporalScanSpec scan_spec, const PropertyGatherSpec& spec,
    ColumnBatch* gathered) {
  if (gathered == nullptr) {
    return Status::InvalidArgument("property gather", "missing gather output");
  }
  auto cursor = OpenPinnedPropertyGather(
      entities, property_sources, std::move(scan_spec), spec);
  if (!cursor.ok()) return cursor.status();
  while (!cursor.ValueOrDie().done()) {
    uint32_t completed = 0;
    const Status advanced = cursor.ValueOrDie().Advance(
        std::numeric_limits<uint32_t>::max(), &completed);
    if (!advanced.ok()) return advanced;
  }
  return cursor.ValueOrDie().Finish(gathered);
}

}  // namespace cedar
