#include "cedar/cypher/parser.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string_view>

namespace cedar::cypher {
namespace {

std::string Upper(std::string_view value) {
  std::string result(value);
  for (char& c : result) {
    if (c >= 'a' && c <= 'z') c = static_cast<char>(c - ('a' - 'A'));
  }
  return result;
}

class Parser {
 public:
  explicit Parser(std::vector<Token> tokens) : tokens_(std::move(tokens)) {}

  StatusOr<Statement> Run() {
    Statement statement;
    statement.span = tokens_.empty() ? SourceSpan{} : tokens_.front().span;
    if (Match("AT") || Match("DIFF")) {
      return Error("legacy temporal syntax is not supported");
    }
    if (Match("USE")) {
      auto graph = Identifier("graph name");
      if (!graph.ok()) return graph.status();
      statement.graph = graph.ValueOrDie();
    }
    const bool changes_prefix = Match("CHANGES");
    while (PeekKeyword("FOR")) {
      Advance();
      auto scope = ParseScope();
      if (!scope.ok()) return scope.status();
      if (scope.ValueOrDie().first) {
        if (statement.valid_time.has_value()) return Error("duplicate valid-time scope");
        statement.valid_time = std::move(scope.ValueOrDie().second);
      } else {
        if (statement.system_time.has_value()) return Error("duplicate system-time scope");
        statement.system_time = std::move(scope.ValueOrDie().second);
      }
    }
    statement.changes = changes_prefix || Match("CHANGES");
    if (Match("MATCH")) {
      statement.kind = StatementKind::kRead;
      auto status = ParsePatterns(&statement);
      if (!status.ok()) return status;
      if (Match("TRAIL")) {
        for (auto& pattern : statement.patterns) pattern.trail = true;
      }
      auto projections = ParseReturn(&statement);
      if (!projections.ok()) return projections.status();
    } else if (Match("CREATE")) {
      statement.kind = StatementKind::kWrite;
      auto status = ParsePatterns(&statement);
      if (!status.ok()) return status;
    } else {
      return Error("expected MATCH, CREATE, or CHANGES");
    }
    while (!AtEnd()) {
      if (Match("SET")) {
        auto assignment = ParseAssignment();
        if (!assignment.ok()) return assignment.status();
        statement.parameters.push_back(assignment.ValueOrDie().parameter);
        statement.assignments.push_back(std::move(assignment.ValueOrDie()));
      } else if (Match("DELETE")) {
        auto name = Identifier("delete target");
        if (!name.ok()) return name.status();
        statement.deletions.push_back(std::move(name.ValueOrDie()));
      } else {
        return Error("unexpected trailing input");
      }
    }
    if (!statement.assignments.empty() || !statement.deletions.empty()) {
      statement.kind = StatementKind::kWrite;
    }
    statement.span.length = static_cast<uint32_t>(tokens_.back().span.offset - statement.span.offset);
    return statement;
  }

 private:
  bool AtEnd() const { return Peek().kind == TokenKind::kEnd; }
  const Token& Peek() const { return tokens_[index_]; }
  bool PeekKeyword(std::string_view keyword) const {
    return Peek().kind == TokenKind::kIdentifier && Upper(Peek().text) == keyword;
  }
  const Token& Advance() { return tokens_[index_++]; }
  bool Match(std::string_view keyword) {
    if (!PeekKeyword(keyword)) return false;
    Advance();
    return true;
  }
  Status Error(const char* message) const {
    return Status::ParseError("cypher parser", message);
  }
  StatusOr<std::string> Identifier(const char* what) {
    if (Peek().kind != TokenKind::kIdentifier) return Error(what);
    return Advance().text;
  }
  StatusOr<uint64_t> Integer(const char* what) {
    if (Peek().kind != TokenKind::kInteger) return Error(what);
    try {
      const unsigned long long value = std::stoull(Advance().text);
      return static_cast<uint64_t>(value);
    } catch (...) {
      return Error(what);
    }
  }
  bool Symbol(std::string_view symbol) {
    if (Peek().kind != TokenKind::kSymbol || Peek().text != symbol) return false;
    Advance();
    return true;
  }

  StatusOr<std::pair<bool, TimeScope>> ParseScope() {
    const bool valid = Match("VALID_TIME");
    if (!valid && !Match("SYSTEM_TIME")) return Error("expected VALID_TIME or SYSTEM_TIME");
    TimeScope scope;
    if (Match("AS")) {
      if (!Match("OF")) return Error("expected OF");
      auto value = Integer("expected temporal sequence");
      if (!value.ok()) return value.status();
      scope.as_of = value.ValueOrDie();
    } else {
      if (!Match("BETWEEN")) return Error("expected BETWEEN or AS OF");
      auto from = Integer("expected temporal start");
      if (!from.ok()) return from.status();
      if (!Match("AND")) return Error("expected AND");
      auto to = Integer("expected temporal end");
      if (!to.ok()) return to.status();
      if (to.ValueOrDie() <= from.ValueOrDie()) return Error("temporal range must be increasing");
      scope.from = from.ValueOrDie();
      scope.to = to.ValueOrDie();
    }
    return std::make_pair(valid, scope);
  }

  Status ParsePatterns(Statement* statement) {
    while (!AtEnd() && !PeekKeyword("RETURN") && !PeekKeyword("TRAIL") &&
           !PeekKeyword("SET") && !PeekKeyword("DELETE")) {
      auto pattern = ParsePattern();
      if (!pattern.ok()) return pattern.status();
      statement->patterns.push_back(std::move(pattern.ValueOrDie()));
      if (Peek().kind == TokenKind::kSymbol && Peek().text == "-") {
        return Status::NotSupported("cypher parser",
                                    "mixed path sequence requires a bounded sequence operator");
      }
      if (!Symbol(",")) break;
    }
    if (statement->patterns.empty()) return Error("expected graph pattern");
    return Status::OK();
  }

  StatusOr<PathPattern> ParsePattern() {
    PathPattern pattern;
    const uint32_t start = Peek().span.offset;
    if (!Symbol("(")) return Error("expected (");
    auto source = Identifier("expected source variable");
    if (!source.ok()) return source.status();
    pattern.source = source.ValueOrDie();
    if (Symbol(":")) {
      auto label = Identifier("expected source label");
      if (!label.ok()) return label.status();
      pattern.source_label = label.ValueOrDie();
    }
    if (!Symbol(")")) return Error("expected )");
    if (!Symbol("-")) {
      pattern.destination = pattern.source;
      pattern.span = {start, static_cast<uint32_t>(Peek().span.offset - start)};
      return pattern;
    }
    if (!Symbol("[")) return Error("expected edge pattern");
    if (!AtEnd() && Peek().kind == TokenKind::kIdentifier) {
      pattern.edge = Advance().text;
    }
    if (Symbol(":")) {
      auto relationship = Identifier("expected relationship type");
      if (!relationship.ok()) return relationship.status();
      pattern.relationship = relationship.ValueOrDie();
    }
    if (Symbol("*")) {
      auto min = Integer("expected minimum hop");
      if (!min.ok()) return min.status();
      pattern.min_hops = static_cast<uint32_t>(min.ValueOrDie());
      if (Symbol("..")) {
        auto max = Integer("expected maximum hop");
        if (!max.ok()) return max.status();
        pattern.max_hops = static_cast<uint32_t>(max.ValueOrDie());
      } else {
        pattern.max_hops = pattern.min_hops;
      }
    }
    if (pattern.max_hops < pattern.min_hops || pattern.max_hops > 64) {
      return Error("path hop bound exceeds limit");
    }
    if (!Symbol("]") || !Symbol("-") || !Symbol(">") || !Symbol("(")) {
      return Error("expected path destination");
    }
    auto destination = Identifier("expected destination variable");
    if (!destination.ok()) return destination.status();
    pattern.destination = destination.ValueOrDie();
    if (Symbol(":")) {
      auto label = Identifier("expected destination label");
      if (!label.ok()) return label.status();
      pattern.destination_label = label.ValueOrDie();
    }
    if (!Symbol(")")) return Error("expected )");
    pattern.span = {start, static_cast<uint32_t>(Peek().span.offset - start)};
    return pattern;
  }

  StatusOr<bool> ParseReturn(Statement* statement) {
    if (!Match("RETURN")) return Error("expected RETURN");
    while (!AtEnd() && !PeekKeyword("SET") && !PeekKeyword("DELETE")) {
      const Token start = Peek();
      auto expression = Identifier("expected return expression");
      if (!expression.ok()) return expression.status();
      ProjectionItem item;
      item.expression = expression.ValueOrDie();
      item.span = start.span;
      if (Symbol("(")) {
        item.function = item.expression;
        auto argument = Identifier("expected function argument");
        if (!argument.ok() || !Symbol(")")) return Error("invalid function projection");
        item.expression = item.function + "(" + argument.ValueOrDie() + ")";
      }
      statement->projections.push_back(std::move(item));
      if (!Symbol(",")) break;
    }
    if (statement->projections.empty()) return Error("expected return projection");
    return true;
  }

  StatusOr<Assignment> ParseAssignment() {
    auto target = Identifier("expected SET target");
    if (!target.ok() || !Symbol(".")) return Error("expected SET property");
    auto property = Identifier("expected SET property");
    if (!property.ok() || !Symbol("=")) return Error("expected SET value");
    if (Peek().kind != TokenKind::kParameter) return Error("writes require named parameters");
    Assignment assignment{target.ValueOrDie(), property.ValueOrDie(), Advance().text,
                          {target.ValueOrDie().empty() ? 0U : Peek().span.offset, 0}};
    return assignment;
  }

  std::vector<Token> tokens_;
  size_t index_ = 0;
};

}  // namespace

StatusOr<Statement> Parse(const std::string& source, LexerOptions options) {
  const auto tokens = Lex(source, options);
  if (!tokens.ok()) return tokens.status();
  Parser parser(tokens.ValueOrDie());
  return parser.Run();
}

}  // namespace cedar::cypher
