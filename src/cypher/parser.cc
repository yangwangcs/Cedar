// Copyright (c) 2025 The Cedar Authors. All rights reserved.

#include "cedar/cypher/parser.h"
#include <cctype>
#include <algorithm>

namespace cedar {
namespace cypher {

CypherParser::CypherParser(const std::string& input) 
    : input_(input), pos_(0) {}

std::shared_ptr<QueryStatement> CypherParser::ParseStatement() {
  SkipWhitespaceAndComments();
  
  // Parse temporal clause first
  temporal_clause_ = ParseTemporalClause();
  
  SkipWhitespaceAndComments();
  
  auto stmt = std::make_shared<QueryStatement>();
  
  // Parse main query clauses
  while (!IsAtEnd()) {
    SkipWhitespaceAndComments();
    
    if (MatchKeyword("match")) {
      auto match = ParseMatchClause();
      if (match) {
        stmt->clauses.push_back(match);
      }
    } else if (MatchKeyword("return")) {
      auto ret = ParseReturnClause();
      if (ret) {
        stmt->clauses.push_back(ret);
      }
    } else if (MatchKeyword("create")) {
      auto create = ParseCreateClause();
      if (create) {
        stmt->clauses.push_back(create);
      }
    } else if (MatchKeyword("where")) {
      auto where = ParseWhereClause();
      if (where) {
        stmt->clauses.push_back(where);
      }
    } else {
      // Unknown clause, skip
      break;
    }
    
    SkipWhitespaceAndComments();
  }
  
  // Store temporal modifier in statement
  if (temporal_clause_) {
    stmt->temporal_modifier.type = temporal_clause_->modifier;
    stmt->temporal_modifier.timestamp = nullptr;  // TODO: Convert expression
    stmt->temporal_modifier.version_number = temporal_clause_->version_number.value_or(0);
  }
  
  return stmt;
}

bool CypherParser::IsValid() {
  size_t save_pos = pos_;
  auto stmt = ParseStatement();
  pos_ = save_pos;
  return stmt != nullptr && error_.empty();
}

// ============================================================================
// Token handling
// ============================================================================

void CypherParser::SkipWhitespace() {
  while (!IsAtEnd() && std::isspace(Peek())) {
    Advance();
  }
}

void CypherParser::SkipWhitespaceAndComments() {
  SkipWhitespace();
  // TODO: Handle comments
}

bool CypherParser::MatchKeyword(const std::string& keyword) {
  SkipWhitespaceAndComments();
  
  size_t save_pos = pos_;
  std::string word;
  
  while (!IsAtEnd() && std::isalpha(Peek())) {
    word += std::tolower(Advance());
  }
  
  if (word == keyword) {
    return true;
  }
  
  pos_ = save_pos;
  return false;
}

bool CypherParser::ExpectKeyword(const std::string& keyword) {
  if (!MatchKeyword(keyword)) {
    error_ = "Expected keyword: " + keyword;
    return false;
  }
  return true;
}

bool CypherParser::MatchSymbol(char symbol) {
  SkipWhitespaceAndComments();
  if (!IsAtEnd() && Peek() == symbol) {
    Advance();
    return true;
  }
  return false;
}

bool CypherParser::ExpectSymbol(char symbol) {
  if (!MatchSymbol(symbol)) {
    error_ = std::string("Expected symbol: ") + symbol;
    return false;
  }
  return true;
}

std::string CypherParser::ParseIdentifier() {
  SkipWhitespaceAndComments();
  
  std::string ident;
  if (!IsAtEnd() && (std::isalpha(Peek()) || Peek() == '_')) {
    ident += Advance();
    while (!IsAtEnd() && (std::isalnum(Peek()) || Peek() == '_')) {
      ident += Advance();
    }
  }
  
  return ident;
}

std::string CypherParser::ParseStringLiteral() {
  SkipWhitespaceAndComments();
  
  char quote = Peek();
  if (quote != '\'' && quote != '\"') {
    return "";
  }
  Advance();  // Consume opening quote
  
  std::string value;
  while (!IsAtEnd() && Peek() != quote) {
    if (Peek() == '\\') {
      Advance();  // Skip escape char
      if (!IsAtEnd()) {
        char escaped = Advance();
        switch (escaped) {
          case 'n': value += '\n'; break;
          case 't': value += '\t'; break;
          case 'r': value += '\r'; break;
          case '\\': value += '\\'; break;
          case '\'': value += '\''; break;
          case '\"': value += '\"'; break;
          default: value += escaped; break;
        }
      }
    } else {
      value += Advance();
    }
  }
  
  if (!IsAtEnd() && Peek() == quote) {
    Advance();  // Consume closing quote
  }
  
  return value;
}

int64_t CypherParser::ParseInteger() {
  SkipWhitespaceAndComments();
  
  int64_t value = 0;
  bool negative = false;
  
  if (!IsAtEnd() && Peek() == '-') {
    negative = true;
    Advance();
  }
  
  while (!IsAtEnd() && std::isdigit(Peek())) {
    value = value * 10 + (Advance() - '0');
  }
  
  return negative ? -value : value;
}

char CypherParser::Peek() const {
  if (pos_ < input_.size()) {
    return input_[pos_];
  }
  return '\0';
}

char CypherParser::Advance() {
  if (pos_ < input_.size()) {
    return input_[pos_++];
  }
  return '\0';
}

bool CypherParser::IsAtEnd() const {
  return pos_ >= input_.size();
}

// ============================================================================
// Clause parsers
// ============================================================================

std::shared_ptr<MatchClause> CypherParser::ParseMatchClause() {
  SkipWhitespaceAndComments();
  
  auto match = std::make_shared<MatchClause>();
  
  // Parse patterns
  while (!IsAtEnd()) {
    auto pattern = ParsePattern();
    if (!pattern.elements.empty()) {
      match->patterns.push_back(pattern);
    }
    
    SkipWhitespaceAndComments();
    if (!MatchSymbol(',')) {
      break;
    }
  }
  
  return match;
}

std::shared_ptr<CreateClause> CypherParser::ParseCreateClause() {
  SkipWhitespaceAndComments();
  
  auto create = std::make_shared<CreateClause>();
  
  // Parse patterns
  while (!IsAtEnd()) {
    auto pattern = ParsePattern();
    if (!pattern.elements.empty()) {
      create->patterns.push_back(pattern);
    }
    
    SkipWhitespaceAndComments();
    if (!MatchSymbol(',')) {
      break;
    }
  }
  
  return create;
}

std::shared_ptr<ReturnClause> CypherParser::ParseReturnClause() {
  SkipWhitespaceAndComments();
  
  auto ret = std::make_shared<ReturnClause>();
  
  // Check for DISTINCT
  if (MatchKeyword("distinct")) {
    ret->distinct = true;
  }
  
  // Parse return items
  while (!IsAtEnd()) {
    ReturnItem item;
    
    // Parse expression (simplified - just identifier for now)
    std::string ident = ParseIdentifier();
    if (!ident.empty()) {
      item.alias = ident;
    }
    
    // Check for AS alias
    SkipWhitespaceAndComments();
    if (MatchKeyword("as")) {
      item.alias = ParseIdentifier();
    }
    
    ret->items.push_back(item);
    
    SkipWhitespaceAndComments();
    if (!MatchSymbol(',')) {
      break;
    }
  }
  
  return ret;
}

std::shared_ptr<WhereClause> CypherParser::ParseWhereClause() {
  SkipWhitespaceAndComments();
  
  auto where = std::make_shared<WhereClause>();
  // TODO: Parse condition expression
  
  return where;
}

// ============================================================================
// Pattern parsers
// ============================================================================

PathPattern CypherParser::ParsePattern() {
  SkipWhitespaceAndComments();
  
  PathPattern pattern;
  
  // Parse alternating nodes and relationships
  while (!IsAtEnd()) {
    // Expect node pattern
    if (MatchSymbol('(')) {
      NodePattern node;
      
      // Variable
      SkipWhitespaceAndComments();
      std::string var = ParseIdentifier();
      if (!var.empty()) {
        node.variable = var;
      }
      
      // Labels
      SkipWhitespaceAndComments();
      while (MatchSymbol(':')) {
        std::string label = ParseIdentifier();
        if (!label.empty()) {
          node.labels.push_back(label);
        }
      }
      
      // Properties (simplified)
      SkipWhitespaceAndComments();
      if (MatchSymbol('{')) {
        // Skip properties for now
        while (!IsAtEnd() && Peek() != '}') {
          Advance();
        }
        ExpectSymbol('}');
      }
      
      ExpectSymbol(')');
      pattern.elements.push_back(node);
    }
    
    SkipWhitespaceAndComments();
    
    // Optional relationship pattern
    if (Peek() == '-' || Peek() == '<') {
      RelationshipPattern rel;
      
      if (MatchSymbol('<')) {
        rel.direction = Direction::INCOMING;
        ExpectSymbol('-');
      } else {
        rel.direction = Direction::OUTGOING;
        ExpectSymbol('-');
      }
      
      // Relationship details in brackets
      if (MatchSymbol('[')) {
        SkipWhitespaceAndComments();
        std::string rel_var = ParseIdentifier();
        if (!rel_var.empty()) {
          rel.variable = rel_var;
        }
        
        // Type
        SkipWhitespaceAndComments();
        if (MatchSymbol(':')) {
          std::string type = ParseIdentifier();
          if (!type.empty()) {
            rel.types.push_back(type);
          }
        }
        
        SkipWhitespaceAndComments();
        ExpectSymbol(']');
      }
      
      // Arrow end
      if (rel.direction == Direction::OUTGOING) {
        if (MatchSymbol('-')) {
          if (MatchSymbol('>')) {
            // Already outgoing
          }
        }
      } else {
        ExpectSymbol('-');
      }
      
      pattern.elements.push_back(rel);
    } else {
      break;
    }
    
    SkipWhitespaceAndComments();
  }
  
  return pattern;
}

}  // namespace cypher
}  // namespace cedar
