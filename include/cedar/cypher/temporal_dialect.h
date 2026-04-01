// Copyright (c) 2024 CedarGraph Project
// Licensed under the MIT License.

// Cedar Temporal Query Dialect
// Based on research of SQL:2011, T-GQL, T-Cypher, and other temporal graph languages

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "cedar/cypher/ast.h"
#include "cedar/types/cedar_types.h"

namespace cedar {
namespace cypher {

// ============================================================================
// Temporal Types and Utilities
// ============================================================================

/**
 * @brief Time dimension selectors for bi-temporal queries
 * 
 * Cedar supports both Valid Time (application time) and 
 * Transaction Time (system time) per SQL:2011 standard.
 */
enum class TimeDimension {
  kDefault = 0,     ///< Use system default (usually valid time)
  kValidTime,       ///< Valid time - when fact was true in reality
  kTransactionTime, ///< Transaction time - when fact was recorded
  kBoth             ///< Both dimensions (intersection)
};

/**
 * @brief Temporal path semantics for graph traversals
 * 
 * Different path types based on temporal constraints:
 * - Snapshot: All elements valid at same point in time
 * - Continuous: All elements valid during common interval  
 * - Pairwise: Adjacent elements have temporal overlap
 * - Sequential: Just ordered by time
 */
enum class TemporalPathSemantics {
  kSnapshot,        ///< All elements valid at query timestamp
  kContinuous,      ///< All elements share common validity interval
  kPairwise,        ///< Adjacent elements overlap in time
  kSequential       ///< Just temporally ordered
};

// ============================================================================
// Timestamp Expression - Flexible time specification
// ============================================================================

/**
 * @brief Types of timestamp expressions supported
 */
enum class TimestampExprType {
  kLiteral,         ///< Direct timestamp value
  kString,          ///< ISO 8601 string (e.g., '2024-01-01T00:00:00Z')
  kFunction,        ///< Temporal function call
  kVariable,        ///< Variable bound to timestamp
  kArithmetic,      ///< Timestamp arithmetic (e.g., now() - interval('1d'))
};

/**
 * @brief Built-in temporal functions
 */
enum class TemporalFunction {
  kNow,                     ///< Current timestamp
  kCurrentTimestamp,        ///< Alias for now()
  kTransactionTime,         ///< Current transaction timestamp
  kValidTime,               ///< Current valid time context
  kTimestamp,               ///< Parse string to timestamp
  kDate,                    ///< Date component
  kDuration,                ///< Create duration
};

/**
 * @brief Timestamp expression - represents any way to specify a time point
 * 
 * This is used throughout the temporal system for flexible time specification:
 * - '2024-01-01' (string literal)
 * - 1704067200000000 (microseconds since epoch)
 * - now() (function call)
 * - $timestamp (variable)
 */
struct TimestampExpression {
  TimestampExprType type;
  
  // One of these will be populated based on type
  std::variant<
    Timestamp,                    // kLiteral - index 0
    std::string,                  // kString - index 1
    TemporalFunction,             // kFunction - index 2
    std::nullptr_t                // kArithmetic (handled separately) - index 3
  > value;
  
  // Variable name stored separately to avoid variant ambiguity
  std::string variable_name;
  
  // For arithmetic expressions
  std::shared_ptr<TimestampExpression> left;
  std::shared_ptr<TimestampExpression> right;
  std::string op;  // "+", "-"
  
  // Duration for arithmetic
  std::optional<int64_t> duration_us;
  
  TimestampExpression() : type(TimestampExprType::kLiteral), value(nullptr) {}
  
  // Factory methods for convenient construction
  static TimestampExpression Literal(Timestamp ts) {
    TimestampExpression expr;
    expr.type = TimestampExprType::kLiteral;
    expr.value = ts;
    return expr;
  }
  
  static TimestampExpression String(const std::string& iso_string) {
    TimestampExpression expr;
    expr.type = TimestampExprType::kString;
    expr.value = iso_string;
    return expr;
  }
  
  static TimestampExpression Function(TemporalFunction func) {
    TimestampExpression expr;
    expr.type = TimestampExprType::kFunction;
    expr.value = func;
    return expr;
  }
  
  static TimestampExpression Variable(const std::string& name) {
    TimestampExpression expr;
    expr.type = TimestampExprType::kVariable;
    expr.variable_name = name;
    return expr;
  }
  
  /**
   * @brief Evaluate expression to concrete timestamp
   * @param context Current query context for variable resolution
   * @return Resolved timestamp
   */
  Timestamp Evaluate(class QueryContext* context = nullptr) const;
  
  /**
   * @brief Convert to human-readable string for debugging
   */
  std::string ToString() const;
};

// ============================================================================
// Extended Temporal Clause
// ============================================================================

/**
 * @brief Extended temporal clause with additional metadata
 * 
 * Builds on ast.h TemporalModifier with additional execution metadata.
 */
struct TemporalClause {
  TemporalModifierType modifier = TemporalModifierType::NONE;
  TimeDimension dimension = TimeDimension::kDefault;
  
  // For point-in-time queries (AS_OF, AT_TIME)
  std::optional<TimestampExpression> timestamp;
  
  // For range queries (BETWEEN, FROM TO, DURING)
  std::optional<TimestampExpression> start_time;
  std::optional<TimestampExpression> end_time;
  
  // For VERSION k queries
  std::optional<uint64_t> version_number;
  
  // For path queries
  std::optional<TemporalPathSemantics> path_semantics;
  
  // Window specification for time-series queries
  std::optional<std::string> window_interval;
  bool window_cumulative = false;
  
  TemporalClause() = default;
  
  // Convenience constructors
  explicit TemporalClause(TemporalModifierType mod) : modifier(mod) {}
  
  static TemporalClause AsOf(TimestampExpression ts, 
                             TimeDimension dim = TimeDimension::kDefault) {
    TemporalClause clause;
    clause.modifier = TemporalModifierType::AS_OF;
    clause.timestamp = ts;
    clause.dimension = dim;
    return clause;
  }
  
  static TemporalClause Between(TimestampExpression start,
                                TimestampExpression end,
                                TimeDimension dim = TimeDimension::kDefault) {
    TemporalClause clause;
    clause.modifier = TemporalModifierType::BETWEEN;
    clause.start_time = start;
    clause.end_time = end;
    clause.dimension = dim;
    return clause;
  }
  
  static TemporalClause AllVersions(TimeDimension dim = TimeDimension::kDefault) {
    TemporalClause clause;
    clause.modifier = TemporalModifierType::ALL_VERSIONS;
    clause.dimension = dim;
    return clause;
  }
  
  static TemporalClause Version(uint64_t version) {
    TemporalClause clause;
    clause.modifier = TemporalModifierType::VERSION_K;
    clause.version_number = version;
    return clause;
  }
  
  /**
   * @brief Check if this clause has any temporal constraints
   */
  bool HasTemporalConstraint() const {
    return modifier != TemporalModifierType::NONE;
  }
  
  /**
   * @brief Get the time range represented by this clause
   * @return Pair of (start, end) timestamps, or nullopt for unbounded
   */
  std::optional<std::pair<Timestamp, Timestamp>> GetTimeRange(
      class QueryContext* context = nullptr) const;
  
  /**
   * @brief Convert to string representation for EXPLAIN output
   */
  std::string ToString() const;
};

// ============================================================================
// Temporal Predicates - For WHERE clause filtering
// ============================================================================

/**
 * @brief Allen's interval algebra predicates
 * 
 * Complete set of 13 temporal relations per Allen (1983).
 * These can be used in WHERE clauses for fine-grained temporal filtering.
 */
enum class AllenPredicate {
  kBefore,          ///< [a1,a2) before [b1,b2): a2 < b1
  kAfter,           ///< [a1,a2) after [b1,b2): a1 > b2
  kMeets,           ///< a2 == b1
  kMetBy,           ///< a1 == b2
  kOverlaps,        ///< a1 < b1 < a2 < b2
  kOverlappedBy,    ///< b1 < a1 < b2 < a2
  kContains,        ///< b1 >= a1 && b2 <= a2
  kDuring,          ///< a1 > b1 && a2 < b2
  kStarts,          ///< a1 == b1 && a2 < b2
  kStartedBy,       ///< a1 == b1 && a2 > b2
  kFinishes,        ///< a2 == b2 && a1 > b1
  kFinishedBy,      ///< a2 == b2 && a1 < b1
  kEquals           ///< a1 == b1 && a2 == b2
};

/**
 * @brief Temporal predicate expression
 * 
 * Used in WHERE clauses like:
 *   WHERE n@T OVERLAPS [t1, t2]
 *   WHERE r.valid_from BEFORE m.valid_from
 */
struct TemporalPredicate {
  AllenPredicate predicate;
  
  // Left operand (usually entity's validity interval)
  std::string left_entity;
  std::optional<std::string> left_property;  // nullopt means use default interval
  
  // Right operand
  std::variant<
    TimestampExpression,                    // Compare to point
    std::pair<TimestampExpression, TimestampExpression>,  // Compare to interval
    std::string                             // Compare to another entity
  > right;
  
  /**
   * @brief Check if two intervals satisfy the predicate
   */
  bool Evaluate(Timestamp a1, Timestamp a2, Timestamp b1, Timestamp b2) const;
};

// ============================================================================
// Temporal DML - Data modification with time
// ============================================================================

/**
 * @brief Valid time specification for CREATE/UPDATE
 * 
 * CREATE (n:Node) VALID FROM '2024-01-01' TO '2024-12-31'
 */
struct ValidTimeSpecification {
  TimestampExpression from;
  std::optional<TimestampExpression> to;  // nullopt = until changed
  
  ValidTimeSpecification() = default;
  ValidTimeSpecification(TimestampExpression f, std::optional<TimestampExpression> t = std::nullopt)
      : from(f), to(t) {}
};

/**
 * @brief Portion update specification
 * 
 * UPDATE n SET ... FOR PORTION OF VALID_TIME FROM t1 TO t2
 */
struct PortionSpecification {
  TimeDimension dimension;
  TimestampExpression from;
  TimestampExpression to;
};

// ============================================================================
// Utility Functions
// ============================================================================

/**
 * @brief Parse ISO 8601 string to timestamp
 * @param iso_string ISO 8601 formatted date/time string
 * @return Timestamp in microseconds since epoch
 */
Timestamp ParseISO8601(const std::string& iso_string);

/**
 * @brief Format timestamp as ISO 8601 string
 */
std::string FormatISO8601(Timestamp ts);

/**
 * @brief Parse duration string (e.g., "1d", "2h30m", "P1Y2M3D")
 * @param duration_str Duration specification
 * @return Duration in microseconds
 */
int64_t ParseDuration(const std::string& duration_str);

/**
 * @brief Get string representation of temporal modifier type
 */
const char* TemporalModifierTypeToString(TemporalModifierType modifier);

/**
 * @brief Get string representation of time dimension
 */
const char* TimeDimensionToString(TimeDimension dimension);

/**
 * @brief Get string representation of Allen predicate
 */
const char* AllenPredicateToString(AllenPredicate pred);

}  // namespace cypher
}  // namespace cedar
