// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/tcypher/syntax/parser.h"

#include <algorithm>
#include <charconv>

namespace cedar {
namespace {

class Parser {
 public:
  explicit Parser(const std::vector<TcypherToken>& tokens) : tokens_(tokens) {}

  StatusOr<TcypherStatement> ParseStatement() {
    TcypherStatement statement;
    statement.explain = ConsumeKeyword("EXPLAIN");
    statement.explain_analyze = statement.explain && ConsumeKeyword("ANALYZE");
    if (ConsumeKeyword("BEGIN")) {
      if (ConsumeKeyword("SNAPSHOT")) {
        statement.kind = TcypherStatementKind::kBeginSnapshot;
      } else if (ConsumeKeyword("STRICT")) {
        statement.kind = TcypherStatementKind::kBeginStrict;
      } else {
        return Error("BEGIN requires SNAPSHOT or STRICT");
      }
      return FinishStatement(std::move(statement));
    }
    if (ConsumeKeyword("COMMIT")) {
      statement.kind = TcypherStatementKind::kCommit;
      return FinishStatement(std::move(statement));
    }
    if (ConsumeKeyword("ROLLBACK")) {
      statement.kind = TcypherStatementKind::kRollback;
      return FinishStatement(std::move(statement));
    }
    if (ConsumeKeyword("CREATE")) {
      statement.kind = TcypherStatementKind::kCreate;
      TemporalMutation mutation;
      const Status parsed = ParseCreateMutation(&mutation);
      if (!parsed.ok()) return parsed;
      statement.mutation = std::move(mutation);
      return FinishStatement(std::move(statement));
    }
    while (IsKeyword("CHANGES") || IsKeyword("FOR")) {
      const bool changes = ConsumeKeyword("CHANGES");
      const TcypherSourceLocation location = Current().location;
      if (!ConsumeKeyword("FOR")) return Error("expected FOR after CHANGES");
      TemporalScope scope;
      scope.location = location;
      if (IsKeyword("VALID_TIME")) {
        scope.axis = TemporalAxis::kValidTime;
      } else if (IsKeyword("SYSTEM_TIME")) {
        scope.axis = TemporalAxis::kSystemTime;
      } else {
        return Error("expected VALID_TIME or SYSTEM_TIME");
      }
      Advance();
      if (ConsumeKeyword("AS")) {
        if (changes || !ConsumeKeyword("OF")) return Error("expected AS OF for state scope");
        scope.mode = TemporalScopeMode::kStateAsOf;
        const auto value = ParseTemporalValue();
        if (!value.ok()) return value.status();
        scope.start = value.ValueOrDie();
      } else if (ConsumeKeyword("BETWEEN")) {
        if (!changes && scope.axis == TemporalAxis::kSystemTime) {
          return Error("SYSTEM_TIME BETWEEN requires CHANGES");
        }
        scope.mode = changes ? TemporalScopeMode::kChangesBetween
                             : TemporalScopeMode::kStateBetween;
        const auto start = ParseTemporalValue();
        if (!start.ok()) return start.status();
        if (!ConsumeKeyword("AND")) return Error("expected AND in temporal range");
        const auto end = ParseTemporalValue();
        if (!end.ok()) return end.status();
        scope.start = start.ValueOrDie();
        scope.end = end.ValueOrDie();
      } else {
        return Error("expected AS OF or BETWEEN temporal scope");
      }
      for (const TemporalScope& existing : statement.temporal_scopes) {
        if (existing.axis == scope.axis) return Error("temporal axis is already scoped");
        if (existing.mode == TemporalScopeMode::kChangesBetween &&
            scope.mode == TemporalScopeMode::kChangesBetween) {
          return Error("only one temporal change axis is permitted");
        }
      }
      statement.temporal_scopes.push_back(std::move(scope));
    }
    if (!ConsumeKeyword("MATCH")) return Error("expected MATCH");
    const Status root = ParseNodePattern(&statement.match);
    if (!root.ok()) return root;
    if (ConsumeKeyword("SET")) {
      statement.kind = TcypherStatementKind::kSet;
      TemporalMutation mutation;
      const Status parsed = ParseSetMutation(statement.match.variable, &mutation);
      if (!parsed.ok()) return parsed;
      statement.mutation = std::move(mutation);
      return FinishStatement(std::move(statement));
    }
    if (ConsumeKeyword("DELETE")) {
      statement.kind = TcypherStatementKind::kDelete;
      TemporalMutation mutation;
      const Status parsed = ParseDeleteMutation(statement.match.variable, &mutation);
      if (!parsed.ok()) return parsed;
      statement.mutation = std::move(mutation);
      return FinishStatement(std::move(statement));
    }
    while (Current().kind == TcypherTokenKind::kMinus ||
           Current().kind == TcypherTokenKind::kLess) {
      MatchRelationshipPattern relationship;
      relationship.location = Current().location;
      if (Consume(TcypherTokenKind::kMinus)) {
        relationship.direction = RelationshipDirection::kOutgoing;
        const Status parsed = ParseRelationshipPattern(&relationship);
        if (!parsed.ok()) return parsed;
        if (!Consume(TcypherTokenKind::kMinus) || !Consume(TcypherTokenKind::kGreater)) {
          return Error("expected -> after relationship pattern");
        }
      } else {
        relationship.direction = RelationshipDirection::kIncoming;
        Advance();
        if (!Consume(TcypherTokenKind::kMinus)) return Error("expected <- relationship prefix");
        const Status parsed = ParseRelationshipPattern(&relationship);
        if (!parsed.ok()) return parsed;
        if (!Consume(TcypherTokenKind::kMinus)) return Error("expected - after relationship pattern");
      }
      MatchNodePattern target;
      const Status parsed_target = ParseNodePattern(&target);
      if (!parsed_target.ok()) return parsed_target;
      statement.relationships.push_back(std::move(relationship));
      statement.expanded_nodes.push_back(std::move(target));
    }
    while (ConsumeKeyword("FOR")) {
      const auto scope = ParseMatchTemporalScope();
      if (!scope.ok()) return scope.status();
      if (std::any_of(statement.match_temporal_scopes.begin(),
                      statement.match_temporal_scopes.end(),
                      [&scope](const TemporalScope& existing) {
                        return existing.axis == scope.ValueOrDie().axis;
                      })) {
        return Error("MATCH temporal axis is already scoped");
      }
      statement.match_temporal_scopes.push_back(scope.ValueOrDie());
    }
    while (ConsumeKeyword("MATCH")) {
      MatchClause clause;
      const Status clause_root = ParseNodePattern(&clause.match);
      if (!clause_root.ok()) return clause_root;
      while (Current().kind == TcypherTokenKind::kMinus ||
             Current().kind == TcypherTokenKind::kLess) {
        MatchRelationshipPattern relationship;
        relationship.location = Current().location;
        if (Consume(TcypherTokenKind::kMinus)) {
          relationship.direction = RelationshipDirection::kOutgoing;
          const Status parsed = ParseRelationshipPattern(&relationship);
          if (!parsed.ok()) return parsed;
          if (!Consume(TcypherTokenKind::kMinus) || !Consume(TcypherTokenKind::kGreater)) {
            return Error("expected -> after relationship pattern");
          }
        } else {
          relationship.direction = RelationshipDirection::kIncoming;
          Advance();
          if (!Consume(TcypherTokenKind::kMinus)) return Error("expected <- relationship prefix");
          const Status parsed = ParseRelationshipPattern(&relationship);
          if (!parsed.ok()) return parsed;
          if (!Consume(TcypherTokenKind::kMinus)) return Error("expected - after relationship pattern");
        }
        MatchNodePattern target;
        const Status parsed_target = ParseNodePattern(&target);
        if (!parsed_target.ok()) return parsed_target;
        clause.relationships.push_back(std::move(relationship));
        clause.expanded_nodes.push_back(std::move(target));
      }
      while (ConsumeKeyword("FOR")) {
        const auto scope = ParseMatchTemporalScope();
        if (!scope.ok()) return scope.status();
        if (std::any_of(clause.temporal_scopes.begin(), clause.temporal_scopes.end(),
                        [&scope](const TemporalScope& existing) {
                          return existing.axis == scope.ValueOrDie().axis;
                        })) {
          return Error("MATCH temporal axis is already scoped");
        }
        clause.temporal_scopes.push_back(scope.ValueOrDie());
      }
      statement.additional_matches.push_back(std::move(clause));
    }
    if (ConsumeKeyword("WHERE")) {
      StringEqualityPredicate predicate;
      const Status parsed = ParseWherePredicate(&predicate);
      if (!parsed.ok()) return parsed;
      statement.where = std::move(predicate);
      while (ConsumeKeyword("AND")) {
        StringEqualityPredicate conjunct;
        const Status parsed_conjunct = ParseWherePredicate(&conjunct);
        if (!parsed_conjunct.ok()) return parsed_conjunct;
        statement.and_predicates.push_back(std::move(conjunct));
      }
    }
    if (!ConsumeKeyword("RETURN")) return Error("expected RETURN");
    statement.distinct = ConsumeKeyword("DISTINCT");
    do {
      if (Current().kind != TcypherTokenKind::kIdentifier && !IsKeyword("SYSTEM_TIME") &&
          !IsKeyword("COUNT") && !IsKeyword("SUM") && !IsKeyword("AVG") &&
          !IsKeyword("MIN") && !IsKeyword("MAX") && !IsKeyword("COLLECT")) {
        return Error("return variable expected");
      }
      ReturnExpression expression;
      expression.location = Current().location;
      const std::string identifier = Current().text;
      Advance();
      if (identifier == "COUNT" && Consume(TcypherTokenKind::kLParen)) {
        if (Current().kind != TcypherTokenKind::kIdentifier) {
          return Error("COUNT binding expected");
        }
        expression.kind = ReturnExpressionKind::kCount;
        expression.variable = Current().text;
        Advance();
        if (!Consume(TcypherTokenKind::kRParen)) return Error("expected ')' after COUNT binding");
      } else if (identifier == "SUM" && Consume(TcypherTokenKind::kLParen)) {
        if (Current().kind != TcypherTokenKind::kIdentifier) {
          return Error("SUM variable expected");
        }
        expression.kind = ReturnExpressionKind::kSum;
        expression.variable = Current().text;
        Advance();
        if (!Consume(TcypherTokenKind::kDot) || Current().kind != TcypherTokenKind::kIdentifier) {
          return Error("SUM property expected");
        }
        expression.property_name = Current().text;
        Advance();
        if (!Consume(TcypherTokenKind::kRParen)) return Error("expected ')' after SUM property");
      } else if (identifier == "AVG" && Consume(TcypherTokenKind::kLParen)) {
        if (Current().kind != TcypherTokenKind::kIdentifier) {
          return Error("AVG variable expected");
        }
        expression.kind = ReturnExpressionKind::kAvg;
        expression.variable = Current().text;
        Advance();
        if (!Consume(TcypherTokenKind::kDot) || Current().kind != TcypherTokenKind::kIdentifier) {
          return Error("AVG property expected");
        }
        expression.property_name = Current().text;
        Advance();
        if (!Consume(TcypherTokenKind::kRParen)) return Error("expected ')' after AVG property");
      } else if ((identifier == "MIN" || identifier == "MAX") &&
                 Consume(TcypherTokenKind::kLParen)) {
        if (Current().kind != TcypherTokenKind::kIdentifier) {
          return Error("extrema variable expected");
        }
        expression.kind = identifier == "MIN" ? ReturnExpressionKind::kMin
                                                : ReturnExpressionKind::kMax;
        expression.variable = Current().text;
        Advance();
        if (!Consume(TcypherTokenKind::kDot) || Current().kind != TcypherTokenKind::kIdentifier) {
          return Error("extrema property expected");
        }
        expression.property_name = Current().text;
        Advance();
        if (!Consume(TcypherTokenKind::kRParen)) return Error("expected ')' after extrema property");
      } else if (identifier == "COLLECT" && Consume(TcypherTokenKind::kLParen)) {
        if (Current().kind != TcypherTokenKind::kIdentifier) {
          return Error("COLLECT variable expected");
        }
        expression.kind = ReturnExpressionKind::kCollect;
        expression.variable = Current().text;
        Advance();
        if (Consume(TcypherTokenKind::kDot)) {
          if (Current().kind != TcypherTokenKind::kIdentifier) {
            return Error("COLLECT property expected");
          }
          expression.property_name = Current().text;
          Advance();
        }
        if (!Consume(TcypherTokenKind::kRParen)) return Error("expected ')' after COLLECT input");
      } else if ((identifier == "valid_from" || identifier == "valid_to" ||
           identifier == "commit_seq" || identifier == "operation") &&
          Consume(TcypherTokenKind::kLParen)) {
        if (Current().kind != TcypherTokenKind::kIdentifier) {
          return Error("temporal fact variable expected");
        }
        if (identifier == "valid_from") {
          expression.kind = ReturnExpressionKind::kValidFrom;
        } else if (identifier == "valid_to") {
          expression.kind = ReturnExpressionKind::kValidTo;
        } else if (identifier == "commit_seq") {
          expression.kind = ReturnExpressionKind::kCommitSeq;
        } else {
          expression.kind = ReturnExpressionKind::kOperation;
        }
        expression.variable = Current().text;
        Advance();
        if (!Consume(TcypherTokenKind::kRParen)) return Error("expected ')' after temporal fact");
      } else if (identifier == "SYSTEM_TIME" && Consume(TcypherTokenKind::kLParen)) {
        if (Current().kind != TcypherTokenKind::kIdentifier) {
          return Error("temporal fact variable expected");
        }
        expression.kind = ReturnExpressionKind::kSystemTime;
        expression.variable = Current().text;
        Advance();
        if (!Consume(TcypherTokenKind::kRParen)) return Error("expected ')' after temporal fact");
      } else {
        expression.variable = identifier;
        if (Consume(TcypherTokenKind::kDot)) {
          if (Current().kind != TcypherTokenKind::kIdentifier) {
            return Error("return property expected");
          }
          expression.kind = ReturnExpressionKind::kProperty;
          expression.property_name = Current().text;
          Advance();
        }
      }
      statement.returns.push_back(std::move(expression));
    } while (Consume(TcypherTokenKind::kComma));
    if (ConsumeKeyword("ORDER")) {
      if (!ConsumeKeyword("BY") || Current().kind != TcypherTokenKind::kIdentifier) {
        return Error("ORDER BY property expected");
      }
      OrderByTerm term;
      term.variable = Current().text;
      Advance();
      if (!Consume(TcypherTokenKind::kDot) || Current().kind != TcypherTokenKind::kIdentifier) {
        return Error("ORDER BY property expected");
      }
      term.property_name = Current().text;
      Advance();
      term.descending = ConsumeKeyword("DESC");
      if (IsKeyword("ASC")) Advance();
      statement.order_by = std::move(term);
    }
    if (ConsumeKeyword("SKIP")) {
      if (Current().kind != TcypherTokenKind::kInteger) return Error("SKIP requires an integer");
      uint64_t skip = 0;
      const auto parsed = std::from_chars(Current().text.data(),
                                          Current().text.data() + Current().text.size(), skip);
      if (parsed.ec != std::errc() || parsed.ptr != Current().text.data() + Current().text.size()) {
        return Error("invalid SKIP value");
      }
      statement.skip = skip;
      Advance();
    }
    if (ConsumeKeyword("LIMIT")) {
      if (Current().kind != TcypherTokenKind::kInteger) return Error("LIMIT requires an integer");
      uint64_t limit = 0;
      const auto parsed = std::from_chars(Current().text.data(),
                                          Current().text.data() + Current().text.size(), limit);
      if (parsed.ec != std::errc() || parsed.ptr != Current().text.data() + Current().text.size()) {
        return Error("invalid LIMIT value");
      }
      statement.limit = limit;
      Advance();
    }
    return FinishStatement(std::move(statement));
  }

 private:
  const TcypherToken& Current() const { return tokens_[position_]; }
  void Advance() { if (Current().kind != TcypherTokenKind::kEnd) ++position_; }
  bool IsKeyword(const char* keyword) const {
    return Current().kind == TcypherTokenKind::kKeyword && Current().text == keyword;
  }
  bool ConsumeKeyword(const char* keyword) {
    if (!IsKeyword(keyword)) return false;
    Advance();
    return true;
  }
  bool Consume(TcypherTokenKind kind) {
    if (Current().kind != kind) return false;
    Advance();
    return true;
  }
  StatusOr<TcypherStatement> FinishStatement(TcypherStatement statement) {
    Consume(TcypherTokenKind::kSemicolon);
    if (Current().kind != TcypherTokenKind::kEnd) return Error("unexpected trailing token");
    return statement;
  }
  Status ParseWherePredicate(StringEqualityPredicate* predicate) {
    if (predicate == nullptr) return Status::InvalidArgument("T-Cypher", "missing WHERE predicate");
    predicate->location = Current().location;
    if (Current().kind != TcypherTokenKind::kIdentifier) return Error("WHERE variable expected");
    predicate->variable = Current().text;
    Advance();
    if (!Consume(TcypherTokenKind::kDot) || Current().kind != TcypherTokenKind::kIdentifier) {
      return Error("WHERE property expected");
    }
    predicate->property_name = Current().text;
    Advance();
    if (Consume(TcypherTokenKind::kEquals)) {
      if (Current().kind == TcypherTokenKind::kIdentifier) {
        predicate->entity_id_variable = Current().text;
        Advance();
        if (!Consume(TcypherTokenKind::kDot) || Current().kind != TcypherTokenKind::kIdentifier ||
            Current().text.empty()) {
          return Error("binding equality requires a right-hand property");
        }
        predicate->rhs_property_name = Current().text;
        Advance();
        return Status::OK();
      }
      if (Current().kind == TcypherTokenKind::kString) {
        predicate->string_value = Current().text;
      } else if (Current().kind == TcypherTokenKind::kInteger) {
        const auto value = ParsePredicateInteger();
        if (!value.ok()) return value.status();
        predicate->integer_value = value.ValueOrDie();
      } else {
        return Error("WHERE requires a string or integer value");
      }
      Advance();
      return Status::OK();
    }
    if (ConsumeKeyword("IN")) {
      predicate->kind = StringPredicateKind::kIn;
      if (!Consume(TcypherTokenKind::kLBracket)) return Error("IN requires '['");
      bool integer_values = false;
      bool first = true;
      do {
        if (Current().kind == TcypherTokenKind::kString && (first || !integer_values)) {
          predicate->in_values.push_back(Current().text);
        } else if (Current().kind == TcypherTokenKind::kInteger && (first || integer_values)) {
          const auto value = ParsePredicateInteger();
          if (!value.ok()) return value.status();
          predicate->in_integer_values.push_back(value.ValueOrDie());
          integer_values = true;
        } else {
          return Error("IN requires values of one scalar type");
        }
        first = false;
        Advance();
      } while (Consume(TcypherTokenKind::kComma));
      return (predicate->in_values.empty() && predicate->in_integer_values.empty()) ||
              !Consume(TcypherTokenKind::kRBracket)
          ? Error("IN requires a non-empty closing list") : Status::OK();
    }
    if (ConsumeKeyword("STARTS")) {
      if (!ConsumeKeyword("WITH") || Current().kind != TcypherTokenKind::kString) {
        return Error("STARTS WITH requires a string value");
      }
      predicate->kind = StringPredicateKind::kPrefix;
      predicate->string_value = Current().text;
      Advance();
      return Status::OK();
    }
    const TcypherTokenKind comparison = Current().kind;
    if (comparison != TcypherTokenKind::kLess && comparison != TcypherTokenKind::kLessEqual &&
        comparison != TcypherTokenKind::kGreater && comparison != TcypherTokenKind::kGreaterEqual) {
      return Error("WHERE requires =, IN, STARTS WITH, or a range comparison");
    }
    Advance();
    const bool integer = Current().kind == TcypherTokenKind::kInteger;
    if (Current().kind != TcypherTokenKind::kString && !integer) {
      return Error("range comparison requires a string or integer value");
    }
    predicate->kind = StringPredicateKind::kRange;
    std::optional<int64_t> integer_value;
    if (integer) {
      const auto value = ParsePredicateInteger();
      if (!value.ok()) return value.status();
      integer_value = value.ValueOrDie();
    }
    if (comparison == TcypherTokenKind::kLess || comparison == TcypherTokenKind::kLessEqual) {
      if (integer_value.has_value()) predicate->upper_integer_bound = *integer_value;
      else predicate->upper_bound = Current().text;
      predicate->upper_inclusive = comparison == TcypherTokenKind::kLessEqual;
    } else {
      if (integer_value.has_value()) predicate->lower_integer_bound = *integer_value;
      else predicate->lower_bound = Current().text;
      predicate->lower_inclusive = comparison == TcypherTokenKind::kGreaterEqual;
    }
    Advance();
    return Status::OK();
  }
  StatusOr<int64_t> ParsePredicateInteger() const {
    int64_t value = 0;
    const std::string& text = Current().text;
    const auto parsed = std::from_chars(text.data(), text.data() + text.size(), value);
    if (parsed.ec != std::errc() || parsed.ptr != text.data() + text.size()) {
      return Status::ParseError("T-Cypher", "integer predicate literal is invalid");
    }
    return value;
  }
  StatusOr<TemporalScope> ParseMatchTemporalScope() {
    TemporalScope scope;
    scope.location = Current().location;
    if (IsKeyword("VALID_TIME")) {
      scope.axis = TemporalAxis::kValidTime;
    } else if (IsKeyword("SYSTEM_TIME")) {
      scope.axis = TemporalAxis::kSystemTime;
    } else {
      return Error("expected VALID_TIME or SYSTEM_TIME after MATCH FOR");
    }
    Advance();
    if (ConsumeKeyword("AS")) {
      if (!ConsumeKeyword("OF")) return Error("MATCH temporal scope requires AS OF");
      scope.mode = TemporalScopeMode::kStateAsOf;
      const auto value = ParseTemporalValue();
      if (!value.ok()) return value.status();
      scope.start = value.ValueOrDie();
      return scope;
    }
    if (!ConsumeKeyword("BETWEEN") || scope.axis == TemporalAxis::kSystemTime) {
      return Error("MATCH temporal scope requires VALID_TIME AS OF or BETWEEN");
    }
    scope.mode = TemporalScopeMode::kStateBetween;
    const auto start = ParseTemporalValue();
    if (!start.ok()) return start.status();
    if (!ConsumeKeyword("AND")) return Error("expected AND in MATCH temporal range");
    const auto end = ParseTemporalValue();
    if (!end.ok()) return end.status();
    scope.start = start.ValueOrDie();
    scope.end = end.ValueOrDie();
    return scope;
  }
  Status ParseNodePattern(MatchNodePattern* pattern) {
    if (pattern == nullptr) return Status::InvalidArgument("T-Cypher", "missing node pattern");
    pattern->location = Current().location;
    if (!Consume(TcypherTokenKind::kLParen)) return Error("expected '('");
    if (Current().kind != TcypherTokenKind::kIdentifier) return Error("node variable expected");
    pattern->variable = Current().text;
    Advance();
    if (Consume(TcypherTokenKind::kColon)) {
      if (Current().kind != TcypherTokenKind::kIdentifier) return Error("node label expected");
      pattern->label = Current().text;
      Advance();
    }
    if (Consume(TcypherTokenKind::kLBrace)) {
      if (Current().kind != TcypherTokenKind::kIdentifier || Current().text != "id") {
        return Error("exact-key MATCH requires id property");
      }
      Advance();
      if (!Consume(TcypherTokenKind::kColon)) return Error("expected ':' after id");
      const auto entity_id = ParseTemporalValue();
      if (!entity_id.ok()) return entity_id.status();
      pattern->entity_id = entity_id.ValueOrDie();
      if (!Consume(TcypherTokenKind::kRBrace)) return Error("expected '}' after id");
    }
    if (!Consume(TcypherTokenKind::kRParen)) return Error("expected ')'");
    return Status::OK();
  }
  Status ParseRelationshipPattern(MatchRelationshipPattern* relationship) {
    if (relationship == nullptr) {
      return Status::InvalidArgument("T-Cypher", "missing relationship pattern");
    }
    if (!Consume(TcypherTokenKind::kLBracket)) return Error("expected '[' for relationship");
    if (Current().kind == TcypherTokenKind::kIdentifier) {
      relationship->variable = Current().text;
      Advance();
    }
    if (Consume(TcypherTokenKind::kColon)) {
      if (Current().kind != TcypherTokenKind::kIdentifier) return Error("relationship type expected");
      relationship->type = Current().text;
      Advance();
    }
    if (Consume(TcypherTokenKind::kStar)) {
      relationship->variable_length = true;
      if (Current().kind != TcypherTokenKind::kInteger) {
        return Error("variable-length relationship minimum hop count expected");
      }
      if (!ParseHopCount(&relationship->min_hops)) return Error("invalid minimum hop count");
      if (!Consume(TcypherTokenKind::kDot) || !Consume(TcypherTokenKind::kDot)) {
        return Error("finite variable-length relationship requires '..'");
      }
      if (Current().kind != TcypherTokenKind::kInteger) {
        return Error("variable-length relationship maximum hop count expected");
      }
      if (!ParseHopCount(&relationship->max_hops) || relationship->min_hops == 0 ||
          relationship->min_hops > relationship->max_hops) {
        return Error("invalid variable-length relationship hop range");
      }
    }
    if (relationship->variable.empty() && relationship->type.empty()) {
      return Error("relationship variable or type expected");
    }
    if (!Consume(TcypherTokenKind::kRBracket)) return Error("expected ']' for relationship");
    return Status::OK();
  }
  Status ParseCreateMutation(TemporalMutation* mutation) {
    if (mutation == nullptr) return Status::InvalidArgument("T-Cypher", "missing mutation");
    if (!Consume(TcypherTokenKind::kLParen)) return Error("expected '(' after CREATE");
    if (Current().kind != TcypherTokenKind::kIdentifier) return Error("created node variable expected");
    mutation->variable = Current().text;
    Advance();
    if (!Consume(TcypherTokenKind::kColon) || Current().kind != TcypherTokenKind::kIdentifier) {
      return Error("created node label expected");
    }
    mutation->label = Current().text;
    Advance();
    if (!Consume(TcypherTokenKind::kLBrace) || Current().kind != TcypherTokenKind::kIdentifier) {
      return Error("created property name expected");
    }
    mutation->property_name = Current().text;
    Advance();
    if (!Consume(TcypherTokenKind::kColon) || Current().kind != TcypherTokenKind::kString) {
      return Error("created property string value expected");
    }
    mutation->string_value = Current().text;
    Advance();
    if (!Consume(TcypherTokenKind::kRBrace) || !Consume(TcypherTokenKind::kRParen)) {
      return Error("expected end of created node");
    }
    if (!ConsumeKeyword("VALID") || !ConsumeKeyword("FROM")) {
      return Error("CREATE requires VALID FROM");
    }
    const auto valid_from = ParseTemporalValue();
    if (!valid_from.ok()) return valid_from.status();
    mutation->valid_from = valid_from.ValueOrDie();
    if (IsKeyword("VALID")) return Error("CREATE does not permit VALID TO");
    return Status::OK();
  }
  Status ParseSetMutation(const std::string& match_variable, TemporalMutation* mutation) {
    if (Current().kind != TcypherTokenKind::kIdentifier || Current().text != match_variable) {
      return Error("SET variable must match exact-key MATCH");
    }
    mutation->variable = Current().text;
    Advance();
    if (!Consume(TcypherTokenKind::kDot) || Current().kind != TcypherTokenKind::kIdentifier) {
      return Error("SET property expected");
    }
    mutation->property_name = Current().text;
    Advance();
    if (!Consume(TcypherTokenKind::kEquals) || Current().kind != TcypherTokenKind::kString) {
      return Error("SET string value expected");
    }
    mutation->string_value = Current().text;
    Advance();
    return ParseMutationValidFrom(mutation);
  }
  Status ParseDeleteMutation(const std::string& match_variable, TemporalMutation* mutation) {
    if (Current().kind != TcypherTokenKind::kIdentifier || Current().text != match_variable) {
      return Error("DELETE variable must match exact-key MATCH");
    }
    mutation->variable = Current().text;
    Advance();
    return ParseMutationValidFrom(mutation);
  }
  Status ParseMutationValidFrom(TemporalMutation* mutation) {
    if (!ConsumeKeyword("VALID") || !ConsumeKeyword("FROM")) {
      return Error("mutation requires VALID FROM");
    }
    const auto valid_from = ParseTemporalValue();
    if (!valid_from.ok()) return valid_from.status();
    mutation->valid_from = valid_from.ValueOrDie();
    if (IsKeyword("VALID") || IsKeyword("TO")) {
      return Error("mutation does not permit VALID TO");
    }
    return Status::OK();
  }
  bool ParseHopCount(uint32_t* result) {
    uint32_t value = 0;
    const std::string& text = Current().text;
    const auto converted = std::from_chars(text.data(), text.data() + text.size(), value);
    if (converted.ec != std::errc() || converted.ptr != text.data() + text.size()) return false;
    *result = value;
    Advance();
    return true;
  }
  StatusOr<TemporalValue> ParseTemporalValue() {
    TemporalValue value;
    value.location = Current().location;
    if (ConsumeKeyword("TIMESTAMP")) {
      if (Current().kind != TcypherTokenKind::kString) return Error("timestamp string expected");
      value.timestamp_literal = Current().text;
      Advance();
    } else if (Current().kind == TcypherTokenKind::kParameter) {
      value.parameter_name = Current().text;
      Advance();
    } else if (Current().kind == TcypherTokenKind::kInteger) {
      uint64_t integer = 0;
      const std::string& text = Current().text;
      const auto converted = std::from_chars(text.data(), text.data() + text.size(), integer);
      if (converted.ec != std::errc()) return Error("invalid integer timestamp");
      value.integer_literal = integer;
      Advance();
    } else {
      return Error("temporal value expected");
    }
    return value;
  }
  Status Error(const char* detail) const {
    return Status::ParseError("T-Cypher", detail);
  }

  const std::vector<TcypherToken>& tokens_;
  size_t position_ = 0;
};

}  // namespace

StatusOr<TcypherStatement> ParseTcypher(const std::string& query) {
  const auto tokens = TokenizeTcypher(query);
  if (!tokens.ok()) return tokens.status();
  return Parser(tokens.ValueOrDie()).ParseStatement();
}

}  // namespace cedar
