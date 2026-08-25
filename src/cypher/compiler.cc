#include "cedar/cypher/compiler.h"

#include <algorithm>
#include <cctype>

namespace cedar::cypher {
namespace {

Status CompileError(const char* message) {
  return Status::NotSupported("cypher compiler", message);
}

std::string Lower(std::string value) {
  for (char& byte : value) byte = static_cast<char>(std::tolower(static_cast<unsigned char>(byte)));
  return value;
}

StatusOr<TemporalScope> MakeScope(const BoundStatement& statement) {
  if (!statement.valid_time.has_value()) return TemporalScope{At{ValidTime{0}}};
  const TimeScope& scope = *statement.valid_time;
  if (scope.as_of.has_value()) return TemporalScope{At{ValidTime{*scope.as_of}}};
  if (!scope.to.has_value()) return CompileError("open temporal range is not bounded");
  const ValidTimeInterval interval{ValidTime{scope.from}, ValidTime{*scope.to}};
  if (statement.changes) return TemporalScope{Changes{interval}};
  return TemporalScope{History{interval}};
}

ExecutionScope MakeExecutionScope(const BoundStatement& statement) {
  ExecutionScope scope;
  scope.part_scope = statement.part_scope;
  scope.graph = statement.graph;
  if (statement.system_time.has_value() &&
      statement.system_time->as_of.has_value()) {
    scope.system_time_as_of = CommitSeq{*statement.system_time->as_of};
  } else if (statement.system_time.has_value() &&
             statement.system_time->to.has_value()) {
    scope.system_time_range = CommitSeqRange{
        CommitSeq{statement.system_time->from},
        CommitSeq{*statement.system_time->to}};
  }
  if (statement.valid_time.has_value() &&
      statement.valid_time->to.has_value() &&
      !statement.valid_time->as_of.has_value()) {
    scope.valid_time = ValidTimeInterval{
        ValidTime{statement.valid_time->from},
        ValidTime{*statement.valid_time->to}};
  }
  return scope;
}

}  // namespace

StatusOr<Query> Compile(const BoundStatement& statement) {
  if (statement.patterns.empty()) return Status::InvalidArgument("cypher compiler", "missing pattern");
  // A comma-separated pattern is a relational product. Cedar's public Query
  // algebra has no product node yet; rejecting it is safer than compiling only
  // the first pattern and silently dropping predicates/results.
  if (statement.patterns.empty()) {
    return CompileError("multiple graph patterns require a product operator");
  }
  const auto scope = MakeScope(statement);
  if (!scope.ok()) return scope.status();
  if (statement.patterns.size() > 1) {
    for (size_t index = 1; index < statement.patterns.size(); ++index) {
      if (statement.patterns[index - 1].destination !=
          statement.patterns[index].source) {
        return CompileError("disconnected graph patterns require a product operator");
      }
    }
  }
  const PathPattern& first = statement.patterns.front();
  const Slot<VertexRef> source = Slot<VertexRef>::WithId(SlotId{1}, first.source);
  auto query = Query::Vertices(source, scope.ValueOrDie());
  if (!query.ok()) return query.status();
  std::vector<std::string> vertex_names{first.source};
  std::vector<Slot<VertexRef>> vertices{source};
  std::vector<std::string> edge_names;
  std::vector<Slot<EdgeRef>> edges;
  for (size_t index = 0; index < statement.patterns.size(); ++index) {
    const PathPattern& pattern = statement.patterns[index];
    const bool standalone_vertex =
        index == 0 && pattern.edge.empty() && pattern.relationship.empty() &&
        pattern.destination == pattern.source;
    if (standalone_vertex) continue;
    const Slot<VertexRef> current = vertices.back();
    const SlotId edge_id{static_cast<uint32_t>(2 + index * 2)};
    const SlotId destination_id{static_cast<uint32_t>(3 + index * 2)};
    const Slot<EdgeRef> edge = Slot<EdgeRef>::WithId(
        edge_id, pattern.edge.empty() ? "edge" + std::to_string(index) : pattern.edge);
    const Slot<VertexRef> destination = Slot<VertexRef>::WithId(
        destination_id, pattern.destination);
    ExpandSpec spec{current, edge, destination, ExpandDirection::kOut,
                    statement.relationship_types[index], pattern.trail};
    if (pattern.max_hops == 1) {
      query = query.ValueOrDie().Expand(spec);
    } else {
      query = query.ValueOrDie().KHopExpand(spec, pattern.max_hops);
    }
    if (!query.ok()) return query.status();
    edges.push_back(edge);
    vertices.push_back(destination);
    edge_names.push_back(edge.name());
    vertex_names.push_back(destination.name());
  }
  if (!statement.projections.empty()) {
    std::vector<Projection> projections;
    uint32_t metadata_index = 0;
    for (const auto& item : statement.projections) {
      if (!item.function.empty()) {
        const std::string function = Lower(item.function);
        const size_t open = item.expression.find('(');
        const size_t close = item.expression.rfind(')');
        const std::string argument =
            open != std::string::npos && close > open
                ? item.expression.substr(open + 1, close - open - 1)
                : std::string{};
        SlotId metadata_source;
        bool found_source = false;
        for (size_t index = 0; index < vertex_names.size(); ++index) {
          if (vertex_names[index] == argument) {
            metadata_source = vertices[index].id();
            found_source = true;
            break;
          }
        }
        for (size_t index = 0; !found_source && index < edge_names.size(); ++index) {
          if (edge_names[index] == argument) {
            metadata_source = edges[index].id();
            found_source = true;
            break;
          }
        }
        if (!found_source) {
          return Status::BindError("cypher compiler", "metadata variable is not bound");
        }
        const SlotId output_id{1000U + metadata_index++};
        if (function == "valid_from") {
          const auto metadata_slot = Slot<ValidTime>::WithId(output_id, item.expression);
          auto projected = query.ValueOrDie().ProjectMetadata(
              metadata_source, MetadataKind::kValidFrom, Project(metadata_slot));
          if (!projected.ok()) return projected.status();
          query = std::move(projected);
          projections.push_back(Project(metadata_slot));
        } else if (function == "commit_seq") {
          const auto metadata_slot = Slot<CommitSeq>::WithId(output_id, item.expression);
          auto projected = query.ValueOrDie().ProjectMetadata(
              metadata_source, MetadataKind::kCommitSeq, Project(metadata_slot));
          if (!projected.ok()) return projected.status();
          query = std::move(projected);
          projections.push_back(Project(metadata_slot));
        } else {
          return CompileError("unsupported metadata function");
        }
        continue;
      }
      bool projected = false;
      for (const auto& vertex : vertices) {
        if (vertex.name() == item.expression) {
          projections.push_back(Project(vertex));
          projected = true;
          break;
        }
      }
      if (!projected) for (const auto& edge : edges) {
        if (edge.name() == item.expression) {
          projections.push_back(Project(edge));
          projected = true;
          break;
        }
      }
      if (!projected) {
        return Status::BindError("cypher compiler", "projection variable is not bound");
      }
    }
    query = query.ValueOrDie().Select(std::move(projections));
  }
  return query.ValueOrDie().WithExecutionScope(MakeExecutionScope(statement));
}

}  // namespace cedar::cypher
