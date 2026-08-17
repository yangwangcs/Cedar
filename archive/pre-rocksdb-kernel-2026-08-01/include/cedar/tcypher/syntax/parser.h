// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TCYPHER_SYNTAX_PARSER_H_
#define CEDAR_TCYPHER_SYNTAX_PARSER_H_

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/tcypher/syntax/tokenizer.h"

namespace cedar {

enum class TemporalAxis : uint8_t { kValidTime, kSystemTime };
enum class TemporalScopeMode : uint8_t {
  kStateAsOf,
  kStateBetween,
  kChangesBetween,
};

struct TemporalValue {
  std::string parameter_name;
  std::string timestamp_literal;
  std::optional<uint64_t> integer_literal;
  TcypherSourceLocation location;
};

struct TemporalScope {
  TemporalAxis axis;
  TemporalScopeMode mode;
  TemporalValue start;
  std::optional<TemporalValue> end;
  TcypherSourceLocation location;
};

struct MatchNodePattern {
  std::string variable;
  std::string label;
  std::optional<TemporalValue> entity_id;
  TcypherSourceLocation location;
};

enum class RelationshipDirection : uint8_t { kOutgoing, kIncoming };

struct MatchRelationshipPattern {
  std::string variable;
  std::string type;
  RelationshipDirection direction;
  bool variable_length = false;
  uint32_t min_hops = 1;
  uint32_t max_hops = 1;
  TcypherSourceLocation location;
};

struct MatchClause {
  MatchNodePattern match;
  std::vector<MatchRelationshipPattern> relationships;
  std::vector<MatchNodePattern> expanded_nodes;
  std::vector<TemporalScope> temporal_scopes;
};

enum class ReturnExpressionKind : uint8_t {
  kBinding,
  kProperty,
  kValidFrom,
  kValidTo,
  kCommitSeq,
  kOperation,
  kSystemTime,
  kCount,
  kSum,
  kAvg,
  kMin,
  kMax,
  kCollect,
};

struct ReturnExpression {
  ReturnExpressionKind kind = ReturnExpressionKind::kBinding;
  std::string variable;
  std::string property_name;
  TcypherSourceLocation location;
};

enum class TcypherStatementKind : uint8_t {
  kQuery,
  kCreate,
  kSet,
  kDelete,
  kBeginSnapshot,
  kBeginStrict,
  kCommit,
  kRollback,
};

struct TemporalMutation {
  std::string variable;
  std::string label;
  std::string property_name;
  std::string string_value;
  TemporalValue valid_from;
};

enum class StringPredicateKind : uint8_t { kEquality, kIn, kRange, kPrefix };

struct StringEqualityPredicate {
  std::string variable;
  std::string property_name;
  std::string string_value;
  std::optional<int64_t> integer_value;
  // Present for a cross-binding equality such as `a.id = b.id` or
  // `a.email = b.email`. The right-side property defaults to `id` for the
  // original identity form.
  std::optional<std::string> entity_id_variable;
  std::optional<std::string> rhs_property_name;
  StringPredicateKind kind = StringPredicateKind::kEquality;
  std::vector<std::string> in_values;
  std::vector<int64_t> in_integer_values;
  std::optional<std::string> lower_bound;
  std::optional<int64_t> lower_integer_bound;
  bool lower_inclusive = false;
  std::optional<std::string> upper_bound;
  std::optional<int64_t> upper_integer_bound;
  bool upper_inclusive = false;
  TcypherSourceLocation location;
};

struct OrderByTerm {
  std::string variable;
  std::string property_name;
  bool descending = false;
};

struct TcypherStatement {
  TcypherStatementKind kind = TcypherStatementKind::kQuery;
  bool explain = false;
  bool explain_analyze = false;
  std::optional<TemporalMutation> mutation;
  std::vector<TemporalScope> temporal_scopes;
  MatchNodePattern match;
  std::vector<MatchRelationshipPattern> relationships;
  std::vector<MatchNodePattern> expanded_nodes;
  std::vector<TemporalScope> match_temporal_scopes;
  std::vector<MatchClause> additional_matches;
  std::optional<StringEqualityPredicate> where;
  std::vector<StringEqualityPredicate> and_predicates;
  std::vector<ReturnExpression> returns;
  bool distinct = false;
  std::optional<OrderByTerm> order_by;
  std::optional<uint64_t> skip;
  std::optional<uint64_t> limit;
};

StatusOr<TcypherStatement> ParseTcypher(const std::string& query);

}  // namespace cedar

#endif  // CEDAR_TCYPHER_SYNTAX_PARSER_H_
