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
  std::optional<uint32_t> source_part_id;
  std::optional<uint64_t> source_vertex_id;
  std::optional<uint32_t> destination_part_id;
  std::optional<uint64_t> destination_vertex_id;
  SourceSpan span;
};

struct ProjectionItem {
  std::string expression;
  std::string function;
  SourceSpan span;
};

enum class PredicateOperator : uint8_t { kEqual, kLess, kLessEqual,
                                         kGreater, kGreaterEqual };

struct Predicate {
  std::string variable;
  std::string property;
  PredicateOperator op = PredicateOperator::kEqual;
  std::optional<std::string> literal;
  std::optional<std::string> parameter;
  SourceSpan span;
};

struct Assignment {
  std::string target;
  std::string property;
  std::string parameter;
  SourceSpan span;
};

enum class StatementKind : uint8_t { kRead, kWrite };
enum class StatementHead : uint8_t { kMatch, kCreate };

struct Statement {
  StatementKind kind = StatementKind::kRead;
  StatementHead head = StatementHead::kMatch;
  std::optional<std::string> graph;
  std::optional<TimeScope> valid_time;
  std::optional<TimeScope> system_time;
  bool changes = false;
  std::vector<PathPattern> patterns;
  std::vector<Predicate> predicates;
  std::vector<ProjectionItem> projections;
  std::optional<size_t> limit_count;
  std::vector<std::string> parameters;
  std::vector<Assignment> assignments;
  std::vector<std::string> deletions;
  SourceSpan span;
};

}  // namespace cedar::cypher

#endif  // CEDAR_CYPHER_AST_H_
