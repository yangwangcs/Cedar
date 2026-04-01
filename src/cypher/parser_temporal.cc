// Copyright (c) 2024 CedarGraph Project
// Licensed under the MIT License.

// Temporal query parsing implementation

#include "cedar/cypher/parser.h"
#include <cctype>
#include <chrono>

namespace cedar {
namespace cypher {

// ============================================================================
// Temporal Clause Parsers
// ============================================================================

std::shared_ptr<TemporalClause> CypherParser::ParseTemporalClause() {
  SkipWhitespaceAndComments();
  
  if (IsAtEnd()) {
    return nullptr;
  }
  
  // Try each temporal modifier in order
  if (auto clause = ParseAsOfClause()) return clause;
  if (auto clause = ParseAtTimeClause()) return clause;
  if (auto clause = ParseBetweenClause()) return clause;
  if (auto clause = ParseFromToClause()) return clause;
  if (auto clause = ParseContainedInClause()) return clause;
  if (auto clause = ParseDuringClause()) return clause;
  if (auto clause = ParseAllVersionsClause()) return clause;
  if (auto clause = ParseVersionClause()) return clause;
  
  // Temporal navigation keywords (FIRST, LAST, PREV, NEXT)
  if (MatchKeyword("first")) {
    auto clause = std::make_shared<TemporalClause>();
    clause->modifier = TemporalModifierType::AS_OF;  // Map to AS OF first version
    // TODO: Set to first version timestamp
    return clause;
  }
  
  if (MatchKeyword("last")) {
    auto clause = std::make_shared<TemporalClause>();
    clause->modifier = TemporalModifierType::AS_OF;  // Map to AS OF last version
    clause->timestamp = TimestampExpression::Function(TemporalFunction::kNow);
    return clause;
  }
  
  if (MatchKeyword("prev")) {
    auto clause = std::make_shared<TemporalClause>();
    clause->modifier = TemporalModifierType::VERSION_K;
    // TODO: Calculate previous version
    return clause;
  }
  
  if (MatchKeyword("next")) {
    auto clause = std::make_shared<TemporalClause>();
    clause->modifier = TemporalModifierType::VERSION_K;
    // TODO: Calculate next version
    return clause;
  }
  
  // Bi-temporal: SYSTEM TIME / VALID TIME
  if (MatchKeyword("system")) {
    SkipWhitespaceAndComments();
    if (MatchKeyword("time") || MatchKeyword("as")) {
      if (MatchKeyword("of")) {
        auto clause = std::make_shared<TemporalClause>();
        clause->dimension = TimeDimension::kTransactionTime;
        clause->modifier = TemporalModifierType::AS_OF;
        clause->timestamp = ParseTimestampExpression();
        return clause;
      }
    }
    // SYSTEM BETWEEN ... AND ...
    if (MatchKeyword("between")) {
      auto clause = std::make_shared<TemporalClause>();
      clause->dimension = TimeDimension::kTransactionTime;
      clause->modifier = TemporalModifierType::BETWEEN;
      clause->start_time = ParseTimestampExpression();
      ExpectKeyword("and");
      clause->end_time = ParseTimestampExpression();
      return clause;
    }
  }
  
  if (MatchKeyword("valid")) {
    SkipWhitespaceAndComments();
    if (MatchKeyword("time") || MatchKeyword("as")) {
      if (MatchKeyword("of")) {
        auto clause = std::make_shared<TemporalClause>();
        clause->dimension = TimeDimension::kValidTime;
        clause->modifier = TemporalModifierType::AS_OF;
        clause->timestamp = ParseTimestampExpression();
        return clause;
      }
    }
    if (MatchKeyword("between")) {
      auto clause = std::make_shared<TemporalClause>();
      clause->dimension = TimeDimension::kValidTime;
      clause->modifier = TemporalModifierType::BETWEEN;
      clause->start_time = ParseTimestampExpression();
      ExpectKeyword("and");
      clause->end_time = ParseTimestampExpression();
      return clause;
    }
  }
  
  return nullptr;
}

std::shared_ptr<TemporalClause> CypherParser::ParseAsOfClause() {
  size_t save_pos = pos_;
  
  if (!MatchKeyword("as")) {
    return nullptr;
  }
  
  SkipWhitespaceAndComments();
  
  if (!MatchKeyword("of")) {
    pos_ = save_pos;
    return nullptr;
  }
  
  SkipWhitespaceAndComments();
  
  auto clause = std::make_shared<TemporalClause>();
  clause->modifier = TemporalModifierType::AS_OF;
  clause->timestamp = ParseTimestampExpression();
  
  return clause;
}

std::shared_ptr<TemporalClause> CypherParser::ParseAtTimeClause() {
  size_t save_pos = pos_;
  
  if (!MatchKeyword("at")) {
    return nullptr;
  }
  
  SkipWhitespaceAndComments();
  
  if (!MatchKeyword("time")) {
    pos_ = save_pos;
    return nullptr;
  }
  
  SkipWhitespaceAndComments();
  
  auto clause = std::make_shared<TemporalClause>();
  clause->modifier = TemporalModifierType::AT_TIME;
  clause->timestamp = ParseTimestampExpression();
  
  return clause;
}

std::shared_ptr<TemporalClause> CypherParser::ParseBetweenClause() {
  size_t save_pos = pos_;
  
  if (!MatchKeyword("between")) {
    return nullptr;
  }
  
  SkipWhitespaceAndComments();
  
  auto clause = std::make_shared<TemporalClause>();
  clause->modifier = TemporalModifierType::BETWEEN;
  clause->start_time = ParseTimestampExpression();
  
  SkipWhitespaceAndComments();
  
  if (!MatchKeyword("and")) {
    error_ = "Expected AND after BETWEEN start time";
    return nullptr;
  }
  
  SkipWhitespaceAndComments();
  
  clause->end_time = ParseTimestampExpression();
  
  return clause;
}

std::shared_ptr<TemporalClause> CypherParser::ParseFromToClause() {
  size_t save_pos = pos_;
  
  if (!MatchKeyword("from")) {
    return nullptr;
  }
  
  SkipWhitespaceAndComments();
  
  auto clause = std::make_shared<TemporalClause>();
  clause->modifier = TemporalModifierType::FROM_TO;
  clause->start_time = ParseTimestampExpression();
  
  SkipWhitespaceAndComments();
  
  if (!MatchKeyword("to")) {
    error_ = "Expected TO after FROM start time";
    return nullptr;
  }
  
  SkipWhitespaceAndComments();
  
  clause->end_time = ParseTimestampExpression();
  
  return clause;
}

std::shared_ptr<TemporalClause> CypherParser::ParseContainedInClause() {
  size_t save_pos = pos_;
  
  if (!MatchKeyword("contained")) {
    return nullptr;
  }
  
  SkipWhitespaceAndComments();
  
  if (!MatchKeyword("in")) {
    pos_ = save_pos;
    return nullptr;
  }
  
  SkipWhitespaceAndComments();
  
  if (!MatchSymbol('(')) {
    error_ = "Expected ( after CONTAINED IN";
    return nullptr;
  }
  
  auto clause = std::make_shared<TemporalClause>();
  clause->modifier = TemporalModifierType::OVERLAPS;
  clause->start_time = ParseTimestampExpression();
  
  SkipWhitespaceAndComments();
  
  if (!MatchSymbol(',')) {
    error_ = "Expected , between CONTAINED IN times";
    return nullptr;
  }
  
  SkipWhitespaceAndComments();
  
  clause->end_time = ParseTimestampExpression();
  
  SkipWhitespaceAndComments();
  
  if (!MatchSymbol(')')) {
    error_ = "Expected ) after CONTAINED IN times";
    return nullptr;
  }
  
  return clause;
}

std::shared_ptr<TemporalClause> CypherParser::ParseDuringClause() {
  size_t save_pos = pos_;
  
  if (!MatchKeyword("during")) {
    return nullptr;
  }
  
  SkipWhitespaceAndComments();
  
  if (!MatchKeyword("period")) {
    // Just DURING without PERIOD - allow single timestamp
    pos_ = save_pos;
    return nullptr;
  }
  
  SkipWhitespaceAndComments();
  
  if (!MatchSymbol('(')) {
    error_ = "Expected ( after DURING PERIOD";
    return nullptr;
  }
  
  auto clause = std::make_shared<TemporalClause>();
  clause->modifier = TemporalModifierType::DURING;
  clause->start_time = ParseTimestampExpression();
  
  SkipWhitespaceAndComments();
  
  if (!MatchSymbol(',')) {
    error_ = "Expected , between DURING PERIOD times";
    return nullptr;
  }
  
  SkipWhitespaceAndComments();
  
  clause->end_time = ParseTimestampExpression();
  
  SkipWhitespaceAndComments();
  
  if (!MatchSymbol(')')) {
    error_ = "Expected ) after DURING PERIOD times";
    return nullptr;
  }
  
  return clause;
}

std::shared_ptr<TemporalClause> CypherParser::ParseAllVersionsClause() {
  size_t save_pos = pos_;
  
  if (!MatchKeyword("all")) {
    return nullptr;
  }
  
  SkipWhitespaceAndComments();
  
  if (!MatchKeyword("versions")) {
    pos_ = save_pos;
    return nullptr;
  }
  
  auto clause = std::make_shared<TemporalClause>();
  clause->modifier = TemporalModifierType::ALL_VERSIONS;
  
  return clause;
}

std::shared_ptr<TemporalClause> CypherParser::ParseVersionClause() {
  if (!MatchKeyword("version")) {
    return nullptr;
  }
  
  SkipWhitespaceAndComments();
  
  auto clause = std::make_shared<TemporalClause>();
  clause->modifier = TemporalModifierType::VERSION_K;
  
  // Parse version number
  if (std::isdigit(Peek())) {
    clause->version_number = ParseInteger();
  } else {
    error_ = "Expected version number after VERSION";
    return nullptr;
  }
  
  return clause;
}

// ============================================================================
// Timestamp Expression Parsing
// ============================================================================

TimestampExpression CypherParser::ParseTimestampExpression() {
  SkipWhitespaceAndComments();
  
  // Check for function call
  if (std::isalpha(Peek())) {
    std::string ident = ParseIdentifier();
    std::string lower_ident = ident;
    std::transform(lower_ident.begin(), lower_ident.end(), lower_ident.begin(), ::tolower);
    
    // Check for temporal functions
    if (lower_ident == "now" || lower_ident == "current_timestamp") {
      SkipWhitespaceAndComments();
      if (MatchSymbol('(')) {
        // now() with possible arguments
        SkipWhitespaceAndComments();
        ExpectSymbol(')');
      }
      return TimestampExpression::Function(TemporalFunction::kNow);
    }
    
    if (lower_ident == "transaction_time") {
      SkipWhitespaceAndComments();
      if (MatchSymbol('(')) {
        ExpectSymbol(')');
      }
      return TimestampExpression::Function(TemporalFunction::kTransactionTime);
    }
    
    if (lower_ident == "valid_time") {
      SkipWhitespaceAndComments();
      if (MatchSymbol('(')) {
        ExpectSymbol(')');
      }
      return TimestampExpression::Function(TemporalFunction::kValidTime);
    }
    
    if (lower_ident == "timestamp") {
      SkipWhitespaceAndComments();
      ExpectSymbol('(');
      SkipWhitespaceAndComments();
      
      // Parse argument to timestamp function
      if (Peek() == '\'' || Peek() == '\"') {
        std::string str = ParseStringLiteral();
        ExpectSymbol(')');
        return TimestampExpression::String(str);
      } else if (std::isdigit(Peek())) {
        int64_t val = ParseInteger();
        ExpectSymbol(')');
        return TimestampExpression::Literal(val);
      } else {
        // Could be a variable or expression
        std::string var = ParseIdentifier();
        ExpectSymbol(')');
        return TimestampExpression::Variable(var);
      }
    }
    
    // Date/time parsing functions
    if (lower_ident == "date") {
      SkipWhitespaceAndComments();
      ExpectSymbol('(');
      std::string str = ParseStringLiteral();
      ExpectSymbol(')');
      return TimestampExpression::String(str);  // Store as string, convert later
    }
    
    if (lower_ident == "datetime") {
      SkipWhitespaceAndComments();
      ExpectSymbol('(');
      std::string str = ParseStringLiteral();
      ExpectSymbol(')');
      return TimestampExpression::String(str);
    }
    
    // Arithmetic: identifier - duration
    SkipWhitespaceAndComments();
    if (Peek() == '-' || Peek() == '+') {
      char op = Advance();
      SkipWhitespaceAndComments();
      
      // Parse duration
      if (std::isdigit(Peek())) {
        int64_t val = ParseInteger();
        SkipWhitespaceAndComments();
        
        // Check for duration unit
        std::string unit = ParseIdentifier();
        std::transform(unit.begin(), unit.end(), unit.begin(), ::tolower);
        
        int64_t duration_us = val;
        if (unit == "d" || unit == "day" || unit == "days") {
          duration_us *= 24 * 3600 * 1000000LL;
        } else if (unit == "h" || unit == "hour" || unit == "hours") {
          duration_us *= 3600 * 1000000LL;
        } else if (unit == "m" || unit == "min" || unit == "minute" || unit == "minutes") {
          duration_us *= 60 * 1000000LL;
        } else if (unit == "s" || unit == "sec" || unit == "second" || unit == "seconds") {
          duration_us *= 1000000LL;
        } else if (unit == "ms" || unit == "millisecond" || unit == "milliseconds") {
          duration_us *= 1000;
        }
        
        TimestampExpression expr;
        expr.type = TimestampExprType::kArithmetic;
        expr.left = std::make_shared<TimestampExpression>(TimestampExpression::Variable(ident));
        expr.op = std::string(1, op);
        expr.duration_us = duration_us;
        return expr;
      }
    }
    
    // Just a variable reference
    return TimestampExpression::Variable(ident);
  }
  
  // String literal (ISO 8601 date)
  if (Peek() == '\'' || Peek() == '\"') {
    return TimestampExpression::String(ParseStringLiteral());
  }
  
  // Numeric literal (timestamp in microseconds)
  if (std::isdigit(Peek()) || Peek() == '-' || Peek() == '+') {
    int64_t val = ParseInteger();
    return TimestampExpression::Literal(val);
  }
  
  // Default: return null expression
  return TimestampExpression();
}

// ============================================================================
// Helper Methods
// ============================================================================

bool CypherParser::HasTemporalModifier() const {
  return temporal_clause_ != nullptr && temporal_clause_->HasTemporalConstraint();
}

std::shared_ptr<TemporalClause> CypherParser::GetTemporalClause() const {
  return temporal_clause_;
}

// ============================================================================
// TimestampExpression Implementation
// ============================================================================

Timestamp TimestampExpression::Evaluate(QueryContext* context) const {
  switch (type) {
    case TimestampExprType::kLiteral:
      return std::get<Timestamp>(value);
      
    case TimestampExprType::kString: {
      // Parse ISO 8601 string
      const std::string& str = std::get<std::string>(value);
      return ParseISO8601(str);
    }
      
    case TimestampExprType::kFunction: {
      TemporalFunction func = std::get<TemporalFunction>(value);
      switch (func) {
        case TemporalFunction::kNow:
        case TemporalFunction::kCurrentTimestamp: {
          auto now = std::chrono::system_clock::now();
          return std::chrono::duration_cast<std::chrono::microseconds>(
              now.time_since_epoch()).count();
        }
        case TemporalFunction::kTransactionTime:
          // Return current transaction timestamp
          return 0;  // TODO: Get from context
        case TemporalFunction::kValidTime:
          return 0;  // TODO: Get from context
        default:
          return 0;
      }
    }
      
    case TimestampExprType::kVariable: {
      // Look up variable in context
      if (context) {
        // TODO: Get variable value from context using variable_name
      }
      return 0;
    }
      
    case TimestampExprType::kArithmetic: {
      if (left && duration_us.has_value()) {
        Timestamp base = left->Evaluate(context);
        if (op == "-") {
          return Timestamp(base.value() - *duration_us);
        } else if (op == "+") {
          return Timestamp(base.value() + *duration_us);
        }
      }
      return 0;
    }
      
    default:
      return 0;
  }
}

std::string TimestampExpression::ToString() const {
  switch (type) {
    case TimestampExprType::kLiteral:
      return std::to_string(std::get<Timestamp>(value).value());
    case TimestampExprType::kString:
      return "'" + std::get<std::string>(value) + "'";
    case TimestampExprType::kFunction: {
      TemporalFunction func = std::get<TemporalFunction>(value);
      switch (func) {
        case TemporalFunction::kNow: return "now()";
        case TemporalFunction::kCurrentTimestamp: return "current_timestamp()";
        case TemporalFunction::kTransactionTime: return "transaction_time()";
        case TemporalFunction::kValidTime: return "valid_time()";
        default: return "unknown()";
      }
    }
    case TimestampExprType::kVariable:
      return variable_name;
    case TimestampExprType::kArithmetic:
      if (left && duration_us.has_value()) {
        return left->ToString() + " " + op + " " + std::to_string(*duration_us) + "us";
      }
      return "invalid";
    default:
      return "null";
  }
}

// ============================================================================
// TemporalClause Implementation
// ============================================================================

std::optional<std::pair<Timestamp, Timestamp>> TemporalClause::GetTimeRange(
    QueryContext* context) const {
  switch (modifier) {
    case TemporalModifierType::AS_OF:
    case TemporalModifierType::AT_TIME: {
      if (timestamp) {
        Timestamp ts = timestamp->Evaluate(context);
        return {{ts, ts}};
      }
      return std::nullopt;
    }
      
    case TemporalModifierType::BETWEEN: {
      if (start_time && end_time) {
        Timestamp start = start_time->Evaluate(context);
        Timestamp end = end_time->Evaluate(context);
        return {{start, end}};
      }
      return std::nullopt;
    }
      
    case TemporalModifierType::FROM_TO: {
      if (start_time && end_time) {
        Timestamp start = start_time->Evaluate(context);
        Timestamp end = end_time->Evaluate(context);
        return {{start, end}};  // Half-open: [start, end)
      }
      return std::nullopt;
    }
      
    case TemporalModifierType::OVERLAPS: {
      if (start_time && end_time) {
        Timestamp start = start_time->Evaluate(context);
        Timestamp end = end_time->Evaluate(context);
        return {{start, end}};  // Must be fully contained
      }
      return std::nullopt;
    }
      
    case TemporalModifierType::DURING: {
      if (start_time && end_time) {
        Timestamp start = start_time->Evaluate(context);
        Timestamp end = end_time->Evaluate(context);
        return {{start, end}};
      }
      return std::nullopt;
    }
      
    case TemporalModifierType::ALL_VERSIONS:
      return {{0, Timestamp::Max()}};
      
    case TemporalModifierType::VERSION_K:
      // Version queries need special handling
      return std::nullopt;
      
    default:
      return std::nullopt;
  }
}

std::string TemporalClause::ToString() const {
  std::ostringstream oss;
  
  // Time dimension prefix
  if (dimension == TimeDimension::kTransactionTime) {
    oss << "SYSTEM ";
  } else if (dimension == TimeDimension::kValidTime) {
    oss << "VALID ";
  }
  
  switch (modifier) {
    case TemporalModifierType::AS_OF:
      oss << "AS OF " << (timestamp ? timestamp->ToString() : "null");
      break;
    case TemporalModifierType::AT_TIME:
      oss << "AT TIME " << (timestamp ? timestamp->ToString() : "null");
      break;
    case TemporalModifierType::BETWEEN:
      oss << "BETWEEN " << (start_time ? start_time->ToString() : "null")
          << " AND " << (end_time ? end_time->ToString() : "null");
      break;
    case TemporalModifierType::FROM_TO:
      oss << "FROM " << (start_time ? start_time->ToString() : "null")
          << " TO " << (end_time ? end_time->ToString() : "null");
      break;
    case TemporalModifierType::OVERLAPS:
      oss << "CONTAINED IN (" << (start_time ? start_time->ToString() : "null")
          << ", " << (end_time ? end_time->ToString() : "null") << ")";
      break;
    case TemporalModifierType::DURING:
      oss << "DURING PERIOD(" << (start_time ? start_time->ToString() : "null")
          << ", " << (end_time ? end_time->ToString() : "null") << ")";
      break;
    case TemporalModifierType::ALL_VERSIONS:
      oss << "ALL VERSIONS";
      break;
    case TemporalModifierType::VERSION_K:
      oss << "VERSION " << (version_number ? std::to_string(*version_number) : "null");
      break;
    default:
      oss << "(no temporal modifier)";
  }
  
  return oss.str();
}

}  // namespace cypher
}  // namespace cedar
