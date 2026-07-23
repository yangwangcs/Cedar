// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/tcypher/runtime/vector_expand.h"

#include <limits>
#include <map>

#include "cedar/tcypher/storage/temporal_scan.h"

namespace cedar {
namespace {

StatusOr<int64_t> ToInt64(uint64_t value, const char* field) {
  if (value > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
    return Status::InvalidArgument("vector expand", field);
  }
  return static_cast<int64_t>(value);
}

bool IsVisibleVertex(const std::map<LogicalKey, std::vector<TemporalEvent>>& by_key,
                     uint64_t vertex_id, const VectorExpandSpec& spec) {
  const LogicalKey key = LogicalKey::VertexExistence(vertex_id);
  const auto found = by_key.find(key);
  if (found == by_key.end()) return false;
  const auto visible = ResolveVisibleEvent(found->second, key, spec.valid_time, spec.snapshot_seq);
  return visible.has_value() && !visible->is_delete();
}

Status AddColumn(ColumnBatch* batch, std::vector<Value> values) {
  return batch->AddVector(std::make_shared<FlatVector>(
      std::move(values), std::vector<bool>{}));
}

}  // namespace

Status ExpandAsOfBatch(const ColumnBatch& sources,
                       const std::vector<TemporalEvent>& candidates,
                       const VectorExpandSpec& spec, ColumnBatch* expanded) {
  if (expanded == nullptr || spec.snapshot_seq == 0 || spec.max_output_rows == 0 ||
      spec.max_output_rows > kTcypherStandardBatchCapacity ||
      (spec.direction != EntityType::EdgeOut && spec.direction != EntityType::EdgeIn) ||
      sources.column_count() <= kEntityId) {
    return Status::InvalidArgument("vector expand", "invalid expand request");
  }
  std::map<LogicalKey, std::vector<TemporalEvent>> by_key;
  for (const TemporalEvent& event : candidates) by_key[event.logical_key()].push_back(event);

  std::vector<Value> source_ids;
  std::vector<Value> target_ids;
  std::vector<Value> edge_ids;
  std::vector<Value> edge_types;
  std::vector<Value> valid_froms;
  std::vector<Value> commit_seqs;
  for (uint32_t row = 0; row < sources.row_count(); ++row) {
    const auto source = sources.ValueAt(kEntityId, row);
    if (!source.has_value() || source->type() != PhysicalType::kInt64 ||
        std::get<int64_t>(source->data()) < 0) {
      return Status::InvalidArgument("vector expand", "source entity id is invalid");
    }
    const uint64_t source_id = static_cast<uint64_t>(std::get<int64_t>(source->data()));
    for (const auto& entry : by_key) {
      const LogicalKey& key = entry.first;
      if (key.kind() != LogicalKeyKind::kExistence || key.entity_type() != spec.direction ||
          key.entity_id() != source_id) {
        continue;
      }
      const auto edge = ResolveVisibleEvent(entry.second, key, spec.valid_time, spec.snapshot_seq);
      if (!edge.has_value() || edge->is_delete() ||
          !IsVisibleVertex(by_key, key.entity_id(), spec) ||
          !IsVisibleVertex(by_key, key.target_id(), spec)) {
        continue;
      }
      if (source_ids.size() == spec.max_output_rows) {
        return Status::InvalidArgument("vector expand", "morsel output exceeds declared capacity");
      }
      const auto source_value = ToInt64(key.entity_id(), "source id exceeds Int64");
      const auto target_value = ToInt64(key.target_id(), "target id exceeds Int64");
      const auto edge_value = ToInt64(key.edge_id(), "edge id exceeds Int64");
      const auto commit_value = ToInt64(edge->commit_seq(), "commit sequence exceeds Int64");
      if (!source_value.ok()) return source_value.status();
      if (!target_value.ok()) return target_value.status();
      if (!edge_value.ok()) return edge_value.status();
      if (!commit_value.ok()) return commit_value.status();
      source_ids.push_back(Value::Int64(source_value.ValueOrDie()));
      target_ids.push_back(Value::Int64(target_value.ValueOrDie()));
      edge_ids.push_back(Value::Int64(edge_value.ValueOrDie()));
      edge_types.push_back(Value::Int32(static_cast<int32_t>(key.edge_type())));
      valid_froms.push_back(Value::Timestamp(edge->valid_from()));
      commit_seqs.push_back(Value::Int64(commit_value.ValueOrDie()));
    }
  }
  ColumnBatch result(spec.max_output_rows);
  for (std::vector<Value>* column : {&source_ids, &target_ids, &edge_ids,
                                     &edge_types, &valid_froms, &commit_seqs}) {
    const Status added = AddColumn(&result, std::move(*column));
    if (!added.ok()) return added;
  }
  *expanded = std::move(result);
  return Status::OK();
}

}  // namespace cedar
