// Copyright (c) 2024 CedarGraph Project
// Licensed under the MIT License.

#pragma once

#include <cctype>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "cedar/types/cedar_types.h"
#include "cedar/cypher/ast.h"
#include "cedar/cypher/temporal_dialect.h"
#include "cedar/cypher/value.h"

namespace cedar {
namespace cypher {

/**
 * @brief Cedar Cypher Parser - Hand-written recursive descent parser
 * 
 * Supports:
 * - Standard Cypher: MATCH, CREATE, RETURN, WHERE, SET, DELETE
 * - Temporal extensions: AS OF, BETWEEN, ALL VERSIONS, etc.
 * 
 * Grammar:
 *   Query ::= TemporalClause? CypherQuery
 *   CypherQuery ::= MatchClause ReturnClause
 *                | CreateClause ReturnClause
 *                | MatchClause SetClause ReturnClause
 *                | MatchClause DeleteClause ReturnClause
 * 
 * Design decisions:
 * 1. Hand-written for better error messages and temporal extension support
 * 2. Single-pass parsing with AST construction
 * 3. Case-insensitive keyword matching
 */
class CypherParser {
 public:
  explicit CypherParser(const std::string& input);

  /**
   * @brief Parse a complete Cypher query
   * @return Root AST node, or nullptr on error
   */
  std::shared_ptr<QueryStatement> ParseStatement();

  /**
   * @brief Check if the query is syntactically valid
   */
  bool IsValid();

  /**
   * @brief Get the last error message
   */
  const std::string& GetError() const { return error_; }

  /**
   * @brief Check if query has temporal modifier
   */
  bool HasTemporalModifier() const;

  /**
   * @brief Get parsed temporal clause
   */
  std::shared_ptr<TemporalClause> GetTemporalClause() const;

 private:
  // ==========================================================================
  // Token handling
  // ==========================================================================
  
  void SkipWhitespace();
  void SkipWhitespaceAndComments();
  bool MatchKeyword(const std::string& keyword);
  bool ExpectKeyword(const std::string& keyword);
  bool MatchSymbol(char symbol);
  bool ExpectSymbol(char symbol);
  
  std::string ParseIdentifier();
  std::string ParseStringLiteral();
  int64_t ParseInteger();
  double ParseDouble();
  Value ParseLiteral();
  
  bool IsKeyword(const std::string& word) const;
  bool IsAtEnd() const;
  char Peek() const;
  char Advance();
  
  // ==========================================================================
  // Clause parsers
  // ==========================================================================
  
  std::shared_ptr<TemporalClause> ParseTemporalClause();
  std::shared_ptr<TemporalClause> ParseAsOfClause();
  std::shared_ptr<TemporalClause> ParseBetweenClause();
  std::shared_ptr<TemporalClause> ParseFromToClause();
  std::shared_ptr<TemporalClause> ParseContainedInClause();
  std::shared_ptr<TemporalClause> ParseAllVersionsClause();
  std::shared_ptr<TemporalClause> ParseVersionClause();
  std::shared_ptr<TemporalClause> ParseAtTimeClause();
  std::shared_ptr<TemporalClause> ParseDuringClause();
  
  TimestampExpression ParseTimestampExpression();
  TimestampExpression ParseTemporalFunction();
  
  std::shared_ptr<MatchClause> ParseMatchClause();
  std::shared_ptr<CreateClause> ParseCreateClause();
  std::shared_ptr<ReturnClause> ParseReturnClause();
  std::shared_ptr<WhereClause> ParseWhereClause();
  std::shared_ptr<SetClause> ParseSetClause();
  std::shared_ptr<DeleteClause> ParseDeleteClause();
  std::shared_ptr<OrderByClause> ParseOrderByClause();
  
  // ==========================================================================
  // Pattern parsers
  // ==========================================================================
  
  PathPattern ParsePattern();
  std::shared_ptr<NodePattern> ParseNodePattern();
  std::shared_ptr<RelationshipPattern> ParseRelationshipPattern();
  // std::vector<std::shared_ptr<Property>> ParseProperties();
  // std::shared_ptr<Property> ParseProperty();
  std::shared_ptr<Expression> ParseExpression();
  std::shared_ptr<Expression> ParseOrExpression();
  std::shared_ptr<Expression> ParseAndExpression();
  std::shared_ptr<Expression> ParseComparisonExpression();
  std::shared_ptr<Expression> ParseAdditiveExpression();
  std::shared_ptr<Expression> ParseMultiplicativeExpression();
  std::shared_ptr<Expression> ParseUnaryExpression();
  std::shared_ptr<Expression> ParsePrimaryExpression();
  
  // ==========================================================================
  // Temporal expression parsers
  // ==========================================================================
  
  std::shared_ptr<Expression> ParseTemporalExpression();
  std::shared_ptr<Expression> ParseAllenPredicateExpression();
  
  // ==========================================================================
  // Member variables
  // ==========================================================================
  
  std::string input_;
  size_t pos_;
  std::string error_;
  
  std::shared_ptr<TemporalClause> temporal_clause_;
  
  // Keywords set for fast lookup
  const std::unordered_map<std::string, bool> keywords_ = {
    // Standard Cypher
    {"match", true},
    {"create", true},
    {"return", true},
    {"where", true},
    {"set", true},
    {"delete", true},
    {"order", true},
    {"by", true},
    {"asc", true},
    {"desc", true},
    {"limit", true},
    {"skip", true},
    {"distinct", true},
    {"as", true},
    {"and", true},
    {"or", true},
    {"not", true},
    {"in", true},
    {"is", true},
    {"null", true},
    {"true", true},
    {"false", true},
    // Temporal extensions
    {"of", true},
    {"between", true},
    {"from", true},
    {"to", true},
    {"contained", true},
    {"all", true},
    {"versions", true},
    {"version", true},
    {"at", true},
    {"time", true},
    {"during", true},
    {"period", true},
    {"system", true},
    {"valid", true},
    {"for", true},
    {"portion", true},
    {"first", true},
    {"last", true},
    {"prev", true},
    {"next", true},
    {"overlaps", true},
    {"contains", true},
    {"precedes", true},
    {"succeeds", true},
    {"immediately", true},
    {"meets", true},
    {"equals", true},
    {"before", true},
    {"after", true},
    {"continuous", true},
    {"snapshot", true},
    {"sequential", true},
  };
};

/**
 * @brief Factory function to create parser
 */
inline std::shared_ptr<CypherParser> CreateParser(const std::string& query) {
  return std::make_shared<CypherParser>(query);
}

}  // namespace cypher
}  // namespace cedar
