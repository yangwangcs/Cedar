// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/snapshot.h"

#include <tuple>
#include <utility>

#include "cedar/database.h"
#include "kernel/database_impl.h"

namespace cedar {
namespace {

Status ValidateFactScanSpec(const FactScanSpec& spec,
                            const FactEventBatchVisitor& visitor) {
  if (!visitor) return Status::InvalidArgument("fact scan", "missing visitor");
  if (spec.batch_row_limit == 0) {
    return Status::InvalidArgument("fact scan", "zero batch row limit");
  }
  if ((spec.entity_id_min.has_value() && spec.entity_id_max.has_value() &&
       *spec.entity_id_min > *spec.entity_id_max) ||
      (spec.event_valid_from_min.has_value() &&
       spec.event_valid_from_max.has_value() &&
       spec.event_valid_from_min->value > spec.event_valid_from_max->value) ||
      (spec.event_commit_seq_min.has_value() &&
       spec.event_commit_seq_max.has_value() &&
       spec.event_commit_seq_min->value > spec.event_commit_seq_max->value)) {
    return Status::InvalidArgument("fact scan", "invalid inclusive range");
  }
  return FactPrefix::Family(spec.part_id, spec.family, spec.property_id).Validate();
}

Status ValidateFactColumnarScanSpec(const FactScanSpec& spec,
                                    const FactColumnarBatchVisitor& visitor) {
  if (!visitor) return Status::InvalidArgument("fact scan", "missing visitor");
  if (spec.batch_row_limit == 0) {
    return Status::InvalidArgument("fact scan", "zero batch row limit");
  }
  if ((spec.entity_id_min.has_value() && spec.entity_id_max.has_value() &&
       *spec.entity_id_min > *spec.entity_id_max) ||
      (spec.event_valid_from_min.has_value() &&
       spec.event_valid_from_max.has_value() &&
       spec.event_valid_from_min->value > spec.event_valid_from_max->value) ||
      (spec.event_commit_seq_min.has_value() &&
       spec.event_commit_seq_max.has_value() &&
       spec.event_commit_seq_min->value > spec.event_commit_seq_max->value)) {
    return Status::InvalidArgument("fact scan", "invalid inclusive range");
  }
  return FactPrefix::Family(spec.part_id, spec.family, spec.property_id).Validate();
}

bool MatchesFactScanSpec(const FactScanSpec& spec, const FactEvent& event) {
  if ((spec.entity_id_min.has_value() &&
       event.ref.entity_id() < *spec.entity_id_min) ||
      (spec.entity_id_max.has_value() &&
       event.ref.entity_id() > *spec.entity_id_max) ||
      (spec.event_valid_from_min.has_value() &&
       event.valid_from.value < spec.event_valid_from_min->value) ||
      (spec.event_valid_from_max.has_value() &&
       event.valid_from.value > spec.event_valid_from_max->value) ||
      (spec.event_commit_seq_min.has_value() &&
       event.commit_seq.value < spec.event_commit_seq_min->value) ||
      (spec.event_commit_seq_max.has_value() &&
       event.commit_seq.value > spec.event_commit_seq_max->value)) {
    return false;
  }
  return true;
}

Status FlushFactEventBatch(std::vector<FactEvent>* events,
                           const FactEventBatchVisitor& visitor) {
  if (events->empty()) return Status::OK();
  FactEventBatch batch{std::move(*events)};
  events->clear();
  const Status status = visitor(batch);
  if (!status.ok()) return status;
  return Status::OK();
}

std::tuple<uint32_t, uint8_t, uint16_t, uint64_t> FactIdentityOf(
    const FactRef& reference) {
  return {reference.part_id().value, static_cast<uint8_t>(reference.family()),
          reference.property_id().value, reference.entity_id()};
}

Status MakeFactColumn(FactColumnId id, FactColumn* column) {
  column->id = id;
  switch (id) {
    case FactColumnId::kPartId:
    case FactColumnId::kFactFamily:
    case FactColumnId::kPropertyId:
    case FactColumnId::kOperation:
    case FactColumnId::kSchemaEpoch:
    case FactColumnId::kPhysicalType:
    case FactColumnId::kSourcePartId:
    case FactColumnId::kTargetPartId:
      column->values = std::vector<uint32_t>{};
      return Status::OK();
    case FactColumnId::kEntityId:
    case FactColumnId::kValidFrom:
    case FactColumnId::kCedarCommitSeq:
    case FactColumnId::kRocksdbSequence:
    case FactColumnId::kTimestamp64Value:
    case FactColumnId::kSourceVertexId:
    case FactColumnId::kTargetVertexId:
    case FactColumnId::kEdgeType:
      column->values = std::vector<uint64_t>{};
      return Status::OK();
    case FactColumnId::kBoolValue:
      column->values = std::vector<uint8_t>{};
      return Status::OK();
    case FactColumnId::kInt32Value:
      column->values = std::vector<int32_t>{};
      return Status::OK();
    case FactColumnId::kInt64Value:
      column->values = std::vector<int64_t>{};
      return Status::OK();
    case FactColumnId::kFloat32Value:
      column->values = std::vector<float>{};
      return Status::OK();
    case FactColumnId::kFloat64Value:
      column->values = std::vector<double>{};
      return Status::OK();
    case FactColumnId::kBytesValue:
      column->values = std::vector<std::string>{};
      return Status::OK();
  }
  return Status::InvalidArgument("fact scan", "unknown projected Cedar column");
}

template <typename T>
Status AppendFactColumnValue(FactColumn* column, const std::optional<T>& value) {
  auto* values = std::get_if<std::vector<T>>(&column->values);
  if (values == nullptr) {
    return Status::Corruption("fact scan", "projected vector type mismatch");
  }
  values->push_back(value.value_or(T{}));
  column->present.push_back(value.has_value() ? 1 : 0);
  return Status::OK();
}

template <typename T>
Status AppendFactColumnValue(FactColumn* column, T value) {
  return AppendFactColumnValue(column, std::optional<T>{value});
}

Status AppendEventColumn(FactColumn* column, const FactEvent& event) {
  const std::optional<Value>& value = event.value;
  const std::optional<EdgeIdentity>& identity = event.edge_identity;
  switch (column->id) {
    case FactColumnId::kPartId:
      return AppendFactColumnValue(column, event.ref.part_id().value);
    case FactColumnId::kFactFamily:
      return AppendFactColumnValue(column, static_cast<uint32_t>(event.ref.family()));
    case FactColumnId::kPropertyId:
      return AppendFactColumnValue(column, static_cast<uint32_t>(event.ref.property_id().value));
    case FactColumnId::kEntityId:
      return AppendFactColumnValue(column, event.ref.entity_id());
    case FactColumnId::kValidFrom:
      return AppendFactColumnValue(column, event.valid_from.value);
    case FactColumnId::kCedarCommitSeq:
      return AppendFactColumnValue(column, event.commit_seq.value);
    case FactColumnId::kRocksdbSequence:
      return Status::NotSupported("fact scan",
                                  "StateColumnarScan has no storage sequence lane");
    case FactColumnId::kOperation:
      return AppendFactColumnValue(column, static_cast<uint32_t>(event.operation));
    case FactColumnId::kSchemaEpoch:
      return AppendFactColumnValue(column, event.schema_epoch);
    case FactColumnId::kPhysicalType:
      return AppendFactColumnValue(
          column, static_cast<uint32_t>(value.has_value() ? value->type()
                                                           : PhysicalType{}));
    case FactColumnId::kBoolValue:
      return AppendFactColumnValue<uint8_t>(
          column, value.has_value() && value->type() == PhysicalType::kBool
                      ? std::optional<uint8_t>{static_cast<uint8_t>(std::get<bool>(value->data()))}
                      : std::nullopt);
    case FactColumnId::kInt32Value:
      return AppendFactColumnValue<int32_t>(
          column, value.has_value() && value->type() == PhysicalType::kInt32
                      ? std::optional<int32_t>{std::get<int32_t>(value->data())}
                      : std::nullopt);
    case FactColumnId::kInt64Value:
      return AppendFactColumnValue<int64_t>(
          column, value.has_value() && value->type() == PhysicalType::kInt64
                      ? std::optional<int64_t>{std::get<int64_t>(value->data())}
                      : std::nullopt);
    case FactColumnId::kFloat32Value:
      return AppendFactColumnValue<float>(
          column, value.has_value() && value->type() == PhysicalType::kFloat32
                      ? std::optional<float>{std::get<float>(value->data())}
                      : std::nullopt);
    case FactColumnId::kFloat64Value:
      return AppendFactColumnValue<double>(
          column, value.has_value() && value->type() == PhysicalType::kFloat64
                      ? std::optional<double>{std::get<double>(value->data())}
                      : std::nullopt);
    case FactColumnId::kTimestamp64Value:
      return AppendFactColumnValue<uint64_t>(
          column, value.has_value() && value->type() == PhysicalType::kTimestamp64
                      ? std::optional<uint64_t>{std::get<uint64_t>(value->data())}
                      : std::nullopt);
    case FactColumnId::kBytesValue:
      return AppendFactColumnValue<std::string>(
          column, value.has_value() &&
                          (value->type() == PhysicalType::kString ||
                           value->type() == PhysicalType::kBinary)
                      ? std::optional<std::string>{std::get<std::string>(value->data())}
                      : std::nullopt);
    case FactColumnId::kSourcePartId:
      return AppendFactColumnValue<uint32_t>(
          column, identity.has_value() ? std::optional<uint32_t>{identity->source_part_id.value}
                                       : std::nullopt);
    case FactColumnId::kSourceVertexId:
      return AppendFactColumnValue<uint64_t>(
          column, identity.has_value() ? std::optional<uint64_t>{identity->source_vertex_id.value}
                                       : std::nullopt);
    case FactColumnId::kTargetPartId:
      return AppendFactColumnValue<uint32_t>(
          column, identity.has_value() ? std::optional<uint32_t>{identity->target_part_id.value}
                                       : std::nullopt);
    case FactColumnId::kTargetVertexId:
      return AppendFactColumnValue<uint64_t>(
          column, identity.has_value() ? std::optional<uint64_t>{identity->target_vertex_id.value}
                                       : std::nullopt);
    case FactColumnId::kEdgeType:
      return AppendFactColumnValue<uint64_t>(
          column, identity.has_value() ? std::optional<uint64_t>{identity->edge_type}
                                       : std::nullopt);
  }
  return Status::InvalidArgument("fact scan", "unknown projected Cedar column");
}

}  // namespace

class Snapshot::State {
 public:
  State(std::shared_ptr<Database::Impl> database, StoreSnapshot snapshot)
      : database(std::move(database)), snapshot(std::move(snapshot)) {}

  std::shared_ptr<Database::Impl> database;
  StoreSnapshot snapshot;

  StatusOr<bool> EdgeVisible(EdgeRef edge_ref, ValidTime valid_time) const {
    const auto edge = database->store.Read(
        snapshot, EntityFact::Edge(edge_ref).ref(), valid_time);
    if (!edge.ok()) return edge.status();
    if (!edge.ValueOrDie().has_value()) return false;
    const auto identity = database->store.LookupEdgeIdentity(snapshot, edge_ref);
    if (!identity.ok()) return identity.status();
    if (!identity.ValueOrDie().has_value()) return false;
    const auto source = database->store.Read(
        snapshot,
        EntityFact::Vertex(identity.ValueOrDie()->source_ref()).ref(),
        valid_time);
    if (!source.ok()) return source.status();
    if (!source.ValueOrDie().has_value()) return false;
    const auto target = database->store.Read(
        snapshot,
        EntityFact::Vertex(identity.ValueOrDie()->target_ref()).ref(),
        valid_time);
    if (!target.ok()) return target.status();
    return target.ValueOrDie().has_value();
  }
};

Snapshot::Snapshot(std::unique_ptr<State> state) : state_(std::move(state)) {}
Snapshot::~Snapshot() = default;
Snapshot::Snapshot(Snapshot&&) noexcept = default;
Snapshot& Snapshot::operator=(Snapshot&&) noexcept = default;

CommitSeq Snapshot::commit_seq() const {
  return state_ == nullptr ? CommitSeq{} : state_->snapshot.commit_seq();
}

CommitSeq Snapshot::oldest_readable_seq() const {
  return state_ == nullptr ? CommitSeq{} : state_->snapshot.oldest_readable_seq();
}

StatusOr<bool> Snapshot::Exists(EntityFact entity, ValidTime valid_time) const {
  if (!state_) return Status::InvalidArgument("snapshot", "moved-from snapshot");
  if (entity.ref().family() == FactFamily::kEdgeState) {
    state_->database->store.RecordPointRead();
    return state_->EdgeVisible(
        EdgeRef{entity.ref().part_id(), EdgeId{entity.ref().entity_id()}}, valid_time);
  }
  state_->database->store.RecordPointRead();
  const auto event = state_->database->store.Read(state_->snapshot, entity.ref(),
                                                   valid_time);
  if (!event.ok()) return event.status();
  return event.ValueOrDie().has_value();
}

StatusOr<std::vector<bool>> Snapshot::MultiExists(
    const std::vector<EntityFact>& entities, ValidTime valid_time) const {
  if (!state_) return Status::InvalidArgument("snapshot", "moved-from snapshot");
  std::vector<bool> results;
  results.reserve(entities.size());
  for (const EntityFact& entity : entities) {
    auto found = Exists(entity, valid_time);
    if (!found.ok()) return found.status();
    results.push_back(found.ValueOrDie());
  }
  state_->database->store.RecordMultiGet(entities.size());
  return results;
}

StatusOr<std::optional<Value>> Snapshot::Get(PropertyFact property,
                                              ValidTime valid_time) const {
  if (!state_) return Status::InvalidArgument("snapshot", "moved-from snapshot");
  state_->database->store.RecordPointRead();
  const auto event = state_->database->store.Read(state_->snapshot, property.ref(),
                                                   valid_time);
  if (!event.ok()) return event.status();
  if (!event.ValueOrDie().has_value()) return std::optional<Value>{};
  if (property.ref().family() == FactFamily::kEdgeProperty) {
    const auto visible = state_->EdgeVisible(
        EdgeRef{property.ref().part_id(), EdgeId{property.ref().entity_id()}},
        valid_time);
    if (!visible.ok()) return visible.status();
    if (!visible.ValueOrDie()) return std::optional<Value>{};
  }
  return event.ValueOrDie()->value;
}

Status Snapshot::Scan(FactFamily family, PropertyId property_id,
                      const SnapshotFactVisitor& visitor) const {
  if (!state_) return Status::InvalidArgument("snapshot", "moved-from snapshot");
  return state_->database->store.Scan(state_->snapshot,
                                      FactPrefix::Family(PartId{}, family, property_id),
                                      visitor);
}

Status Snapshot::EventScan(const FactScanSpec& spec,
                           const FactEventBatchVisitor& visitor) const {
  if (!state_) return Status::InvalidArgument("snapshot", "moved-from snapshot");
  const Status valid = ValidateFactScanSpec(spec, visitor);
  if (!valid.ok()) return valid;
  std::vector<FactEvent> events;
  events.reserve(spec.batch_row_limit);
  const Status scanned = state_->database->store.Scan(
      state_->snapshot,
      FactPrefix::Family(spec.part_id, spec.family, spec.property_id),
      FactScanBounds{spec.entity_id_min, spec.entity_id_max},
      [&events, &spec, &visitor](const FactEvent& event) {
        if (!MatchesFactScanSpec(spec, event)) return Status::OK();
        events.push_back(event);
        if (events.size() < spec.batch_row_limit) return Status::OK();
        return FlushFactEventBatch(&events, visitor);
      });
  if (!scanned.ok()) return scanned;
  return FlushFactEventBatch(&events, visitor);
}

Status Snapshot::EventColumnarScan(
    const FactScanSpec& spec, const std::vector<FactColumnId>& projection,
    const FactColumnarBatchVisitor& visitor) const {
  if (!state_) return Status::InvalidArgument("snapshot", "moved-from snapshot");
  const Status valid = ValidateFactColumnarScanSpec(spec, visitor);
  if (!valid.ok()) return valid;
  FactColumnarScanOptions options;
  options.event_valid_from_min = spec.event_valid_from_min;
  options.event_valid_from_max = spec.event_valid_from_max;
  options.event_commit_seq_min = spec.event_commit_seq_min;
  options.event_commit_seq_max = spec.event_commit_seq_max;
  options.projection = projection;
  options.batch_row_limit = spec.batch_row_limit;
  return state_->database->store.ScanColumnar(
      state_->snapshot,
      FactPrefix::Family(spec.part_id, spec.family, spec.property_id),
      FactScanBounds{spec.entity_id_min, spec.entity_id_max}, options, visitor);
}

Status Snapshot::StateScan(const FactScanSpec& spec,
                           const FactEventBatchVisitor& visitor) const {
  if (!state_) return Status::InvalidArgument("snapshot", "moved-from snapshot");
  const Status valid = ValidateFactScanSpec(spec, visitor);
  if (!valid.ok()) return valid;
  std::optional<std::tuple<uint32_t, uint8_t, uint16_t, uint64_t>> last_identity;
  std::vector<FactEvent> events;
  events.reserve(spec.batch_row_limit);
  const Status scanned = state_->database->store.Scan(
      state_->snapshot,
      FactPrefix::Family(spec.part_id, spec.family, spec.property_id),
      FactScanBounds{spec.entity_id_min, spec.entity_id_max},
      [this, &spec, &visitor, &last_identity, &events](const FactEvent& scanned) {
        if ((spec.entity_id_min.has_value() &&
             scanned.ref.entity_id() < *spec.entity_id_min) ||
            (spec.entity_id_max.has_value() &&
             scanned.ref.entity_id() > *spec.entity_id_max)) {
          return Status::OK();
        }
        const auto identity = FactIdentityOf(scanned.ref);
        if (last_identity.has_value() && *last_identity == identity) return Status::OK();
        last_identity = identity;
        const auto event = state_->database->store.Read(state_->snapshot, scanned.ref,
                                                        spec.valid_time);
        if (!event.ok()) return event.status();
        if (!event.ValueOrDie().has_value()) return Status::OK();
        if (!MatchesFactScanSpec(spec, *event.ValueOrDie())) return Status::OK();
        events.push_back(*event.ValueOrDie());
        if (events.size() < spec.batch_row_limit) return Status::OK();
        return FlushFactEventBatch(&events, visitor);
      });
  if (!scanned.ok()) return scanned;
  return FlushFactEventBatch(&events, visitor);
}

Status Snapshot::StateColumnarScan(
    const FactScanSpec& spec, const std::vector<FactColumnId>& projection,
    const FactColumnarBatchVisitor& visitor) const {
  if (!state_) return Status::InvalidArgument("snapshot", "moved-from snapshot");
  const Status valid = ValidateFactColumnarScanSpec(spec, visitor);
  if (!valid.ok()) return valid;
  if (projection.empty()) {
    return Status::InvalidArgument("fact scan", "missing projection");
  }
  FactColumnarBatch schema;
  schema.columns.reserve(projection.size());
  for (FactColumnId id : projection) {
    if (id == FactColumnId::kRocksdbSequence) {
      return Status::NotSupported("fact scan",
                                  "StateColumnarScan has no storage sequence lane");
    }
    for (const FactColumn& existing : schema.columns) {
      if (existing.id == id) {
        return Status::InvalidArgument("fact scan", "duplicate projection column");
      }
    }
    FactColumn column;
    const Status column_status = MakeFactColumn(id, &column);
    if (!column_status.ok()) return column_status;
    schema.columns.push_back(std::move(column));
  }
  return StateScan(spec, [&schema, &visitor](const FactEventBatch& events) {
    FactColumnarBatch output = schema;
    for (const FactEvent& event : events.events) {
      for (FactColumn& column : output.columns) {
        const Status appended = AppendEventColumn(&column, event);
        if (!appended.ok()) return appended;
      }
    }
    return visitor(output);
  });
}

StatusOr<Snapshot> Database::BeginSnapshot(SnapshotOptions options) const {
  if (!impl_) return Status::InvalidArgument("database", "moved-from database");
  std::lock_guard<std::mutex> lock(impl_->mutex);
  if (impl_->closed) return Status::InvalidArgument("database", "database is closed");
  if (impl_->closing) {
    return Status::ShutdownInProgress("database", "database close is in progress");
  }
  auto snapshot = impl_->store.BeginSnapshot(options);
  if (!snapshot.ok()) return snapshot.status();
  return Snapshot(std::make_unique<Snapshot::State>(
      impl_, snapshot.ConsumeValueOrDie()));
}

}  // namespace cedar
