#ifndef CEDAR_CYPHER_AST_H_
#define CEDAR_CYPHER_AST_H_

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace cedar::cypher {

struct SourceSpan {
  uint32_t offset = 0;
  uint32_t length = 0;
};

struct TimeScope {
  uint64_t from = 0;
  std::optional<uint64_t> to;
  std::optional<uint64_t> as_of;
  SourceSpan span;
};

struct PathPattern {
  std::string source;
  std::string source_label;
  std::string edge;
  std::string relationship;
  std::string destination;
  std::string destination_label;
  uint32_t min_hops = 1;
  uint32_t max_hops = 1;
  bool trail = false;
  SourceSpan span;
};

struct ProjectionItem {
  std::string expression;
  std::string function;
  SourceSpan span;
};

struct Assignment {
  std::string target;
  std::string property;
  std::string parameter;
  SourceSpan span;
};

enum class StatementKind : uint8_t { kRead, kWrite };

struct Statement {
  StatementKind kind = StatementKind::kRead;
  std::optional<std::string> graph;
  std::optional<TimeScope> valid_time;
  std::optional<TimeScope> system_time;
  bool changes = false;
  std::vector<PathPattern> patterns;
  std::vector<ProjectionItem> projections;
  std::vector<std::string> parameters;
  std::vector<Assignment> assignments;
  std::vector<std::string> deletions;
  SourceSpan span;
};

}  // namespace cedar::cypher

#endif  // CEDAR_CYPHER_AST_H_
