// Copyright (c) 2024 CedarGraph Project
// Licensed under the MIT License.

#include "cedar/cypher/temporal_dialect.h"
#include <chrono>
#include <iomanip>
#include <sstream>

namespace cedar {
namespace cypher {

// ============================================================================
// Parse ISO 8601 timestamp
// ============================================================================

Timestamp ParseISO8601(const std::string& iso_string) {
  // Simple ISO 8601 parser for common formats
  // Format: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS or YYYY-MM-DDTHH:MM:SS.sss
  
  std::tm tm = {};
  std::istringstream ss(iso_string);
  
  // Try to parse date part
  ss >> std::get_time(&tm, "%Y-%m-%d");
  if (ss.fail()) {
    return 0;
  }
  
  // Check if there's time part
  if (ss.peek() == 'T' || ss.peek() == ' ') {
    ss.get();  // Consume 'T' or space
    ss >> std::get_time(&tm, "%H:%M:%S");
  }
  
  // Convert to timestamp (microseconds since epoch)
  auto time = std::mktime(&tm);
  if (time == -1) {
    return 0;
  }
  
  return Timestamp(static_cast<uint64_t>(time) * 1000000LL);
}

std::string FormatISO8601(Timestamp ts) {
  auto seconds = ts.value() / 1000000;
  auto microseconds = ts.value() % 1000000;
  
  std::time_t time = static_cast<std::time_t>(seconds);
  std::tm* tm = std::gmtime(&time);
  
  std::ostringstream oss;
  oss << std::put_time(tm, "%Y-%m-%dT%H:%M:%S");
  if (microseconds > 0) {
    oss << "." << std::setw(6) << std::setfill('0') << microseconds;
  }
  oss << "Z";
  
  return oss.str();
}

// ============================================================================
// Parse duration string
// ============================================================================

int64_t ParseDuration(const std::string& duration_str) {
  // Parse duration like "1d", "2h30m", "P1Y2M3D"
  int64_t microseconds = 0;
  
  // Check for ISO 8601 duration format (P...)
  if (duration_str.empty() || duration_str[0] == 'P') {
    // TODO: Implement full ISO 8601 duration parsing
    return 0;
  }
  
  // Parse simple format like "1d2h30m10s"
  size_t i = 0;
  int64_t value = 0;
  
  while (i < duration_str.size()) {
    char c = duration_str[i];
    
    if (std::isdigit(c)) {
      value = value * 10 + (c - '0');
    } else {
      switch (c) {
        case 'd':
        case 'D':
          microseconds += value * 24 * 3600 * 1000000LL;
          value = 0;
          break;
        case 'h':
        case 'H':
          microseconds += value * 3600 * 1000000LL;
          value = 0;
          break;
        case 'm':
        case 'M':
          microseconds += value * 60 * 1000000LL;
          value = 0;
          break;
        case 's':
        case 'S':
          microseconds += value * 1000000LL;
          value = 0;
          break;
        case 'l':  // milliseconds
        case 'L':
          microseconds += value * 1000;
          value = 0;
          break;
      }
    }
    i++;
  }
  
  return microseconds;
}

// ============================================================================
// String conversion functions
// ============================================================================

const char* TemporalModifierTypeToString(TemporalModifierType modifier) {
  switch (modifier) {
    case TemporalModifierType::NONE: return "NONE";
    case TemporalModifierType::AS_OF: return "AS_OF";
    case TemporalModifierType::AT_TIME: return "AT_TIME";
    case TemporalModifierType::BETWEEN: return "BETWEEN";
    case TemporalModifierType::FROM_TO: return "FROM_TO";
    case TemporalModifierType::CONTAINED_IN: return "CONTAINED_IN";
    case TemporalModifierType::DURING: return "DURING";
    case TemporalModifierType::OVERLAPS: return "OVERLAPS";
    case TemporalModifierType::FIRST: return "FIRST";
    case TemporalModifierType::LAST: return "LAST";
    case TemporalModifierType::PREV: return "PREV";
    case TemporalModifierType::NEXT: return "NEXT";
    case TemporalModifierType::ALL_VERSIONS: return "ALL_VERSIONS";
    case TemporalModifierType::VERSION_K: return "VERSION_K";
    default: return "UNKNOWN";
  }
}

const char* TimeDimensionToString(TimeDimension dimension) {
  switch (dimension) {
    case TimeDimension::kDefault: return "DEFAULT";
    case TimeDimension::kValidTime: return "VALID_TIME";
    case TimeDimension::kTransactionTime: return "TRANSACTION_TIME";
    case TimeDimension::kBoth: return "BOTH";
    default: return "UNKNOWN";
  }
}

const char* AllenPredicateToString(AllenPredicate pred) {
  switch (pred) {
    case AllenPredicate::kBefore: return "BEFORE";
    case AllenPredicate::kAfter: return "AFTER";
    case AllenPredicate::kMeets: return "MEETS";
    case AllenPredicate::kMetBy: return "MET_BY";
    case AllenPredicate::kOverlaps: return "OVERLAPS";
    case AllenPredicate::kOverlappedBy: return "OVERLAPPED_BY";
    case AllenPredicate::kContains: return "CONTAINS";
    case AllenPredicate::kDuring: return "DURING";
    case AllenPredicate::kStarts: return "STARTS";
    case AllenPredicate::kStartedBy: return "STARTED_BY";
    case AllenPredicate::kFinishes: return "FINISHES";
    case AllenPredicate::kFinishedBy: return "FINISHED_BY";
    case AllenPredicate::kEquals: return "EQUALS";
    default: return "UNKNOWN";
  }
}

}  // namespace cypher
}  // namespace cedar
