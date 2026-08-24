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

}  // namespace

StatusOr<Query> Compile(const BoundStatement& statement) {
  if (statement.patterns.empty()) return Status::InvalidArgument("cypher compiler", "missing pattern");
  // A comma-separated pattern is a relational product. Cedar's public Query
  // algebra has no product node yet; rejecting it is safer than compiling only
  // the first pattern and silently dropping predicates/results.
  if (statement.patterns.size() != 1) {
    return CompileError("multiple graph patterns require a product operator");
  }
  const auto scope = MakeScope(statement);
  if (!scope.ok()) return scope.status();
  const PathPattern& pattern = statement.patterns.front();
  const Slot<VertexRef> source = Slot<VertexRef>::WithId(SlotId{1}, pattern.source);
  auto query = Query::Vertices(source, scope.ValueOrDie());
  if (!query.ok()) return query.status();
  if (!pattern.edge.empty() || !pattern.relationship.empty() || pattern.max_hops != 1) {
    const Slot<EdgeRef> edge = Slot<EdgeRef>::WithId(SlotId{2}, pattern.edge.empty() ? "edge" : pattern.edge);
    const Slot<VertexRef> destination = Slot<VertexRef>::WithId(SlotId{3}, pattern.destination);
    ExpandSpec spec{source, edge, destination, ExpandDirection::kOut,
                    std::nullopt, pattern.trail};
    if (pattern.max_hops == 1) {
      query = query.ValueOrDie().Expand(spec);
    } else {
      query = query.ValueOrDie().KHopExpand(spec, pattern.max_hops);
    }
    if (!query.ok()) return query.status();
  }
  if (!statement.projections.empty()) {
    std::vector<Projection> projections;
    uint32_t metadata_index = 0;
    for (const auto& item : statement.projections) {
      if (!item.function.empty()) {
        const std::string function = Lower(item.function);
        SlotId metadata_source;
        if (item.expression == item.function + "(" + pattern.source + ")") {
          metadata_source = source.id();
        } else if (!pattern.edge.empty() &&
                   item.expression == item.function + "(" + pattern.edge + ")") {
          metadata_source = SlotId{2};
        } else if (!pattern.destination.empty() &&
                   item.expression == item.function + "(" + pattern.destination + ")") {
          metadata_source = SlotId{3};
        } else {
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
      if (item.expression == pattern.source) {
        projections.push_back(Project(source));
      } else if (item.expression == pattern.edge && !pattern.edge.empty()) {
        projections.push_back(Project(Slot<EdgeRef>::WithId(SlotId{2}, pattern.edge)));
      } else if (item.expression == pattern.destination && !pattern.destination.empty()) {
        projections.push_back(Project(Slot<VertexRef>::WithId(SlotId{3}, pattern.destination)));
      } else {
        return Status::BindError("cypher compiler", "projection variable is not bound");
      }
    }
    query = query.ValueOrDie().Select(std::move(projections));
  }
  return query;
}

}  // namespace cedar::cypher
