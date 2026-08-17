// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/benchmark/ldbc_adapter.h"

#include <algorithm>
#include <charconv>
#include <cctype>
#include <limits>
#include <sstream>
#include <unordered_set>
#include <vector>

namespace cedar {
namespace {

std::string Trim(std::string value) {
  while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front()))) {
    value.erase(value.begin());
  }
  while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back()))) {
    value.pop_back();
  }
  return value;
}

std::string Upper(std::string value) {
  for (char& byte : value) byte = static_cast<char>(
      std::toupper(static_cast<unsigned char>(byte)));
  return value;
}

StatusOr<uint64_t> ParseU64(const std::string& field, const char* name) {
  const std::string value = Trim(field);
  uint64_t parsed = 0;
  const auto result = std::from_chars(value.data(), value.data() + value.size(), parsed);
  if (result.ec != std::errc() || result.ptr != value.data() + value.size()) {
    return Status::ParseError("LDBC adapter", std::string("invalid ") + name);
  }
  return parsed;
}

std::vector<std::string> SplitCsvLine(const std::string& line) {
  std::vector<std::string> fields;
  std::string field;
  bool quoted = false;
  for (size_t index = 0; index < line.size(); ++index) {
    const char byte = line[index];
    if (byte == '"') {
      if (quoted && index + 1 < line.size() && line[index + 1] == '"') {
        field.push_back('"');
        ++index;
      } else {
        quoted = !quoted;
      }
    } else if (byte == ',' && !quoted) {
      fields.push_back(field);
      field.clear();
    } else {
      field.push_back(byte);
    }
  }
  if (quoted) return {};
  fields.push_back(field);
  return fields;
}

template <typename Callback>
Status ForEachCsvRecord(const std::string& csv, size_t expected_fields,
                        const std::string& expected_header, Callback callback) {
  std::istringstream input(csv);
  std::string line;
  if (!std::getline(input, line)) {
    return Status::ParseError("LDBC adapter", "CSV is missing a header");
  }
  if (Trim(line) != expected_header) {
    return Status::ParseError("LDBC adapter", "unexpected CSV header");
  }
  uint64_t line_number = 1;
  while (std::getline(input, line)) {
    ++line_number;
    if (Trim(line).empty()) continue;
    const auto fields = SplitCsvLine(line);
    if (fields.size() != expected_fields) {
      return Status::ParseError(
          "LDBC adapter", "wrong field count at CSV line " +
              std::to_string(line_number));
    }
    const Status status = callback(fields, line_number);
    if (!status.ok()) return status;
  }
  return Status::OK();
}

struct ParsedEvent {
  TemporalEvent event;
};

bool EventLess(const TemporalEvent& left, const TemporalEvent& right) {
  if (left.logical_key() != right.logical_key()) {
    return left.logical_key() < right.logical_key();
  }
  if (left.valid_from() != right.valid_from()) {
    return left.valid_from() < right.valid_from();
  }
  if (left.commit_seq() != right.commit_seq()) {
    return left.commit_seq() < right.commit_seq();
  }
  return static_cast<uint8_t>(left.operation()) <
      static_cast<uint8_t>(right.operation());
}

Status MakeEvent(const LogicalKey& key, uint64_t valid_from,
                 uint64_t commit_seq, uint32_t schema_epoch,
                 const std::string& operation, Value value,
                 TemporalEvent* output) {
  if (output == nullptr) {
    return Status::InvalidArgument("LDBC adapter", "event output is absent");
  }
  const std::string normalized = Upper(Trim(operation));
  if (normalized == "PUT") {
    *output = TemporalEvent::Put(key, valid_from, commit_seq, schema_epoch,
                                 std::move(value));
    return Status::OK();
  }
  if (normalized == "DELETE") {
    *output = TemporalEvent::Delete(key, valid_from, commit_seq, schema_epoch);
    return Status::OK();
  }
  return Status::ParseError("LDBC adapter", "operation must be PUT or DELETE");
}

}  // namespace

StatusOr<LdbcTemporalDataset> AdaptLdbcCsv(
    const std::string& nodes_csv, const std::string& edges_csv,
    const LdbcAdapterConfig& config) {
  if (config.vertex_property_column == 0 || config.edge_type == 0 ||
      config.schema_epoch == 0) {
    return Status::InvalidArgument("LDBC adapter", "schema identifiers must be positive");
  }
  CedarTgDataset dataset;
  dataset.config.seed = 0;
  dataset.config.property_events_per_vertex = 1;
  std::unordered_set<uint64_t> seen_vertices;
  Status status = ForEachCsvRecord(
      nodes_csv, 5, "id,valid_from,commit_seq,name,operation",
      [&](const std::vector<std::string>& fields, uint64_t) -> Status {
        const auto id = ParseU64(fields[0], "node id");
        const auto valid_from = ParseU64(fields[1], "node valid_from");
        const auto commit_seq = ParseU64(fields[2], "node commit_seq");
        if (!id.ok()) return id.status();
        if (!valid_from.ok()) return valid_from.status();
        if (!commit_seq.ok()) return commit_seq.status();
        if (id.ValueOrDie() == 0) {
          return Status::ParseError("LDBC adapter", "node id must be positive");
        }
        if (seen_vertices.insert(id.ValueOrDie()).second) {
          TemporalEvent existence = TemporalEvent::Put(
              LogicalKey::VertexExistence(id.ValueOrDie()), valid_from.ValueOrDie(),
              commit_seq.ValueOrDie(), config.schema_epoch, Value::Binary(""));
          const Status existence_status = MakeEvent(
              LogicalKey::VertexExistence(id.ValueOrDie()), valid_from.ValueOrDie(),
              commit_seq.ValueOrDie(), config.schema_epoch, fields[4],
              Value::Binary(""), &existence);
          if (!existence_status.ok()) return existence_status;
          dataset.events.push_back(std::move(existence));
          ++dataset.vertex_events;
        }
        TemporalEvent property = TemporalEvent::Put(
            LogicalKey::VertexProperty(id.ValueOrDie(), config.vertex_property_column),
            valid_from.ValueOrDie(), commit_seq.ValueOrDie(), config.schema_epoch,
            Value::String(Trim(fields[3])));
        const Status property_status = MakeEvent(
            LogicalKey::VertexProperty(id.ValueOrDie(), config.vertex_property_column),
            valid_from.ValueOrDie(), commit_seq.ValueOrDie(), config.schema_epoch,
            fields[4], Value::String(Trim(fields[3])), &property);
        if (!property_status.ok()) return property_status;
        dataset.events.push_back(std::move(property));
        ++dataset.property_events;
        return Status::OK();
      });
  if (!status.ok()) return status;
  status = ForEachCsvRecord(
      edges_csv, 7,
      "source_id,target_id,edge_id,edge_type,valid_from,commit_seq,operation",
      [&](const std::vector<std::string>& fields, uint64_t) -> Status {
        const auto source = ParseU64(fields[0], "edge source_id");
        const auto target = ParseU64(fields[1], "edge target_id");
        const auto edge_id = ParseU64(fields[2], "edge edge_id");
        const auto edge_type = ParseU64(fields[3], "edge edge_type");
        const auto valid_from = ParseU64(fields[4], "edge valid_from");
        const auto commit_seq = ParseU64(fields[5], "edge commit_seq");
        if (!source.ok()) return source.status();
        if (!target.ok()) return target.status();
        if (!edge_id.ok()) return edge_id.status();
        if (!edge_type.ok()) return edge_type.status();
        if (!valid_from.ok()) return valid_from.status();
        if (!commit_seq.ok()) return commit_seq.status();
        if (source.ValueOrDie() == 0 || target.ValueOrDie() == 0 ||
            edge_id.ValueOrDie() == 0 || edge_type.ValueOrDie() == 0 ||
            edge_type.ValueOrDie() > std::numeric_limits<uint16_t>::max()) {
          return Status::ParseError("LDBC adapter", "edge identity is invalid");
        }
        TemporalEvent event = TemporalEvent::Put(
            LogicalKey::EdgeExistence(
                source.ValueOrDie(), target.ValueOrDie(),
                static_cast<uint16_t>(edge_type.ValueOrDie()), edge_id.ValueOrDie(),
                EntityType::EdgeOut),
            valid_from.ValueOrDie(), commit_seq.ValueOrDie(), config.schema_epoch,
            Value::Binary(""));
        const Status event_status = MakeEvent(
            LogicalKey::EdgeExistence(
                source.ValueOrDie(), target.ValueOrDie(),
                static_cast<uint16_t>(edge_type.ValueOrDie()), edge_id.ValueOrDie(),
                EntityType::EdgeOut),
            valid_from.ValueOrDie(), commit_seq.ValueOrDie(), config.schema_epoch,
            fields[6], Value::Binary(""), &event);
        if (!event_status.ok()) return event_status;
        dataset.events.push_back(std::move(event));
        ++dataset.edge_events;
        return Status::OK();
      });
  if (!status.ok()) return status;
  std::sort(dataset.events.begin(), dataset.events.end(), EventLess);
  dataset.config.vertex_count = seen_vertices.size();
  dataset.config.edge_count = dataset.edge_events;
  dataset.dataset_hash = HashCedarTgDataset(dataset);
  return LdbcTemporalDataset{std::move(dataset), config.source_name,
                             config.source_license, config.transform_policy};
}

}  // namespace cedar
