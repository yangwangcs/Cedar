// Copyright (c) 2024 CedarGraph Project
// Licensed under the MIT License.

#pragma once

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <variant>
#include <vector>

#include "cedar/types/cedar_types.h"

namespace cedar {
namespace cypher {

// Forward declarations
class Value;
struct Node;
struct Relationship;
struct Path;

// ============================================================================
// Value Types Enumeration
// ============================================================================

enum class ValueType {
  kNull = 0,
  kBool,
  kInt,
  kFloat,
  kString,
  kTimestamp,       // Temporal: point in time
  kDate,            // Temporal: date only
  kTime,            // Temporal: time only
  kDateTime,        // Temporal: date + time
  kDuration,        // Temporal: time interval
  kNode,
  kRelationship,
  kPath,
  kList,
  kMap,
  kBytes,
};

// ============================================================================
// Temporal Value Types
// ============================================================================

/**
 * @brief Duration representation for temporal arithmetic
 */
struct Duration {
  int64_t microseconds = 0;
  
  // Component accessors
  int64_t Days() const { return microseconds / (24 * 3600 * 1000000LL); }
  int64_t Hours() const { return (microseconds % (24 * 3600 * 1000000LL)) / (3600 * 1000000LL); }
  int64_t Minutes() const { return (microseconds % (3600 * 1000000LL)) / (60 * 1000000LL); }
  int64_t Seconds() const { return (microseconds % (60 * 1000000LL)) / 1000000LL; }
  int64_t Milliseconds() const { return (microseconds % 1000000LL) / 1000; }
  int64_t Microseconds() const { return microseconds % 1000; }
  
  Duration() = default;
  explicit Duration(int64_t us) : microseconds(us) {}
  
  static Duration Days(int64_t d) { return Duration(d * 24 * 3600 * 1000000LL); }
  static Duration Hours(int64_t h) { return Duration(h * 3600 * 1000000LL); }
  static Duration Minutes(int64_t m) { return Duration(m * 60 * 1000000LL); }
  static Duration Seconds(int64_t s) { return Duration(s * 1000000LL); }
  static Duration Milliseconds(int64_t ms) { return Duration(ms * 1000); }
  
  Duration operator+(const Duration& other) const {
    return Duration(microseconds + other.microseconds);
  }
  
  Duration operator-(const Duration& other) const {
    return Duration(microseconds - other.microseconds);
  }
  
  bool operator==(const Duration& other) const {
    return microseconds == other.microseconds;
  }
  
  std::string ToString() const;
};

/**
 * @brief Date representation (days since epoch)
 */
struct Date {
  int32_t days_since_epoch = 0;  // Days since 1970-01-01
  
  Date() = default;
  explicit Date(int32_t days) : days_since_epoch(days) {}
  
  static Date FromYMD(int year, int month, int day);
  void ToYMD(int& year, int& month, int& day) const;
  
  std::string ToString() const;
};

/**
 * @brief Time representation (nanoseconds since midnight)
 */
struct Time {
  int64_t nanos_since_midnight = 0;
  int32_t timezone_offset_minutes = 0;  // UTC offset in minutes
  
  Time() = default;
  Time(int64_t nanos, int32_t tz_offset = 0) 
      : nanos_since_midnight(nanos), timezone_offset_minutes(tz_offset) {}
  
  static Time FromHMS(int hour, int minute, int second, int nanosecond = 0);
  
  std::string ToString() const;
};

/**
 * @brief DateTime representation (timestamp with timezone)
 */
struct DateTime {
  Timestamp timestamp = Timestamp(0);  // Microseconds since epoch
  int32_t timezone_offset_minutes = 0;
  
  DateTime() = default;
  DateTime(Timestamp ts, int32_t tz_offset = 0) 
      : timestamp(ts), timezone_offset_minutes(tz_offset) {}
  
  static DateTime Now();
  static DateTime ParseISO8601(const std::string& str);
  
  std::string ToString() const;
};

// ============================================================================
// Graph Types
// ============================================================================

/**
 * @brief Node representation
 */
struct Node {
  uint64_t id = 0;
  std::vector<std::string> labels;
  std::map<std::string, Value> properties;
  
  // Temporal metadata
  Timestamp valid_from = Timestamp(0);
  Timestamp valid_to = Timestamp::Max();
  Timestamp txn_from = Timestamp(0);
  Timestamp txn_to = Timestamp::Max();
  uint64_t version = 0;
  
  Node() = default;
  Node(uint64_t id_, std::vector<std::string> labels_, 
       std::map<std::string, Value> props)
      : id(id_), labels(std::move(labels_)), properties(std::move(props)) {}
  
  bool HasLabel(const std::string& label) const;
  std::optional<Value> GetProperty(const std::string& key) const;
  
  std::string ToString() const;
};

/**
 * @brief Relationship representation
 */
struct Relationship {
  uint64_t id = 0;
  uint64_t start_id = 0;
  uint64_t end_id = 0;
  std::string type;
  std::map<std::string, Value> properties;
  
  // Temporal metadata
  Timestamp valid_from = Timestamp(0);
  Timestamp valid_to = Timestamp::Max();
  Timestamp txn_from = Timestamp(0);
  Timestamp txn_to = Timestamp::Max();
  uint64_t version = 0;
  
  Relationship() = default;
  Relationship(uint64_t id_, uint64_t start, uint64_t end,
               std::string type_, std::map<std::string, Value> props)
      : id(id_), start_id(start), end_id(end), 
        type(std::move(type_)), properties(std::move(props)) {}
  
  std::optional<Value> GetProperty(const std::string& key) const;
  
  std::string ToString() const;
};

/**
 * @brief Path representation
 */
struct Path {
  std::vector<std::variant<Node, Relationship>> elements;
  
  Path() = default;
  explicit Path(std::vector<std::variant<Node, Relationship>> elems)
      : elements(std::move(elems)) {}
  
  size_t Length() const { return elements.size() / 2; }  // Number of relationships
  const Node& GetNode(size_t index) const;
  const Relationship& GetRelationship(size_t index) const;
  
  std::string ToString() const;
};

// ============================================================================
// Main Value Class
// ============================================================================

/**
 * @brief Cypher Value type - supports all Cypher data types including temporal
 * 
 * This is the core data type used throughout the Cypher query engine.
 * It uses std::variant for type-safe storage and efficient memory usage.
 */
class Value {
 public:
  // Null constructor
  Value() : type_(ValueType::kNull) {}
  
  // Scalar constructors
  explicit Value(bool val) : type_(ValueType::kBool), value_(val) {}
  explicit Value(int64_t val) : type_(ValueType::kInt), value_(val) {}
  explicit Value(int val) : type_(ValueType::kInt), value_(static_cast<int64_t>(val)) {}
  explicit Value(double val) : type_(ValueType::kFloat), value_(val) {}
  explicit Value(const std::string& val) : type_(ValueType::kString), value_(val) {}
  explicit Value(const char* val) : type_(ValueType::kString), value_(std::string(val)) {}
  
  // Temporal constructors
  explicit Value(Timestamp ts) : type_(ValueType::kTimestamp), value_(ts) {}
  explicit Value(Date d) : type_(ValueType::kDate), value_(d) {}
  explicit Value(Time t) : type_(ValueType::kTime), value_(t) {}
  explicit Value(DateTime dt) : type_(ValueType::kDateTime), value_(dt) {}
  explicit Value(Duration dur) : type_(ValueType::kDuration), value_(dur) {}
  
  // Graph constructors
  explicit Value(Node node) : type_(ValueType::kNode), value_(std::move(node)) {}
  explicit Value(Relationship rel) : type_(ValueType::kRelationship), value_(std::move(rel)) {}
  explicit Value(Path path) : type_(ValueType::kPath), value_(std::move(path)) {}
  
  // Collection constructors
  explicit Value(std::vector<Value> list) : type_(ValueType::kList), value_(std::move(list)) {}
  explicit Value(std::map<std::string, Value> map) : type_(ValueType::kMap), value_(std::move(map)) {}
  
  // Static factory methods
  static Value Null() { return Value(); }
  static Value MakeNode(uint64_t id, const std::vector<std::string>& labels,
                        const std::map<std::string, Value>& props);
  static Value MakeRelationship(uint64_t id, uint64_t start, uint64_t end,
                                const std::string& type,
                                const std::map<std::string, Value>& props);
  static Value TimestampValue(Timestamp ts) { return Value(ts); }
  static Value DateValue(int year, int month, int day);
  static Value TimeValue(int hour, int minute, int second);
  static Value DateTimeValue(int year, int month, int day, 
                             int hour, int minute, int second);
  static Value DurationValue(int64_t microseconds);
  
  // Type accessors
  ValueType Type() const { return type_; }
  bool IsNull() const { return type_ == ValueType::kNull; }
  bool IsBool() const { return type_ == ValueType::kBool; }
  bool IsInt() const { return type_ == ValueType::kInt; }
  bool IsFloat() const { return type_ == ValueType::kFloat; }
  bool IsString() const { return type_ == ValueType::kString; }
  bool IsTimestamp() const { return type_ == ValueType::kTimestamp; }
  bool IsDate() const { return type_ == ValueType::kDate; }
  bool IsTime() const { return type_ == ValueType::kTime; }
  bool IsDateTime() const { return type_ == ValueType::kDateTime; }
  bool IsDuration() const { return type_ == ValueType::kDuration; }
  bool IsNode() const { return type_ == ValueType::kNode; }
  bool IsRelationship() const { return type_ == ValueType::kRelationship; }
  bool IsPath() const { return type_ == ValueType::kPath; }
  bool IsList() const { return type_ == ValueType::kList; }
  bool IsMap() const { return type_ == ValueType::kMap; }
  bool IsNumeric() const { return IsInt() || IsFloat() || IsTimestamp(); }
  bool IsTemporal() const { 
    return IsTimestamp() || IsDate() || IsTime() || IsDateTime() || IsDuration(); 
  }
  bool IsGraph() const { return IsNode() || IsRelationship() || IsPath(); }
  
  // Value accessors (with type checking)
  bool GetBool() const;
  int64_t GetInt() const;
  double GetFloat() const;
  const std::string& GetString() const;
  Timestamp GetTimestamp() const;
  Date GetDate() const;
  Time GetTime() const;
  DateTime GetDateTime() const;
  Duration GetDuration() const;
  Node& GetNode() { return std::get<Node>(value_); }
  const Node& GetNode() const { return std::get<Node>(value_); }
  Relationship& GetRelationship() { return std::get<Relationship>(value_); }
  const Relationship& GetRelationship() const { return std::get<Relationship>(value_); }
  Path& GetPath() { return std::get<Path>(value_); }
  const Path& GetPath() const { return std::get<Path>(value_); }
  std::vector<Value>& GetList() { return std::get<std::vector<Value>>(value_); }
  const std::vector<Value>& GetList() const { return std::get<std::vector<Value>>(value_); }
  std::map<std::string, Value>& GetMap() { return std::get<std::map<std::string, Value>>(value_); }
  const std::map<std::string, Value>& GetMap() const { return std::get<std::map<std::string, Value>>(value_); }
  
  // Comparison operators
  bool operator==(const Value& other) const;
  bool operator!=(const Value& other) const { return !(*this == other); }
  bool operator<(const Value& other) const;
  bool operator<=(const Value& other) const;
  bool operator>(const Value& other) const;
  bool operator>=(const Value& other) const;
  
  // Arithmetic operators
  Value operator+(const Value& other) const;
  Value operator-(const Value& other) const;
  Value operator*(const Value& other) const;
  Value operator/(const Value& other) const;
  Value operator%(const Value& other) const;
  
  Value& operator+=(const Value& other);
  Value& operator-=(const Value& other);
  
  Value operator-() const;  // Unary minus
  
  // Temporal operations
  Value AddDuration(const Duration& dur) const;  // timestamp + duration
  Value SubDuration(const Duration& dur) const;  // timestamp - duration
  Duration DiffTimestamp(const Value& other) const;  // timestamp - timestamp
  
  // String operations
  Value operator+(const std::string& str) const;  // Concatenation
  Value operator+(const char* str) const;
  
  // Collection operations
  std::optional<Value> GetProperty(const std::string& key) const;  // For nodes/maps
  void SetProperty(const std::string& key, const Value& val);
  std::optional<Value> GetElement(size_t index) const;  // For lists
  size_t Size() const;  // For lists/maps/strings
  
  // Conversion
  std::string ToString() const {
    switch (type_) {
      case ValueType::kNull: return "NULL";
      case ValueType::kBool: return std::get<bool>(value_) ? "true" : "false";
      case ValueType::kInt: return std::to_string(std::get<int64_t>(value_));
      case ValueType::kFloat: return std::to_string(std::get<double>(value_));
      case ValueType::kString: return std::get<std::string>(value_);
      case ValueType::kTimestamp: return std::to_string(std::get<Timestamp>(value_).value());
      case ValueType::kNode: return "Node(" + std::to_string(GetNode().id) + ")";
      case ValueType::kRelationship: return "Relationship(" + std::to_string(GetRelationship().id) + ")";
      case ValueType::kList: return "List[" + std::to_string(GetList().size()) + "]";
      case ValueType::kMap: return "Map{" + std::to_string(GetMap().size()) + "}";
      default: return "Unknown";
    }
  }
  double ToDouble() const;  // Numeric conversion
  int64_t ToInt() const;    // Numeric conversion
  
  // Hash support
  size_t Hash() const;
  
 private:
  ValueType type_;
  std::variant<
    std::monostate,           // kNull
    bool,                     // kBool
    int64_t,                  // kInt
    double,                   // kFloat
    std::string,              // kString
    Timestamp,                // kTimestamp
    Date,                     // kDate
    Time,                     // kTime
    DateTime,                 // kDateTime
    Duration,                 // kDuration
    Node,                     // kNode
    Relationship,             // kRelationship
    Path,                     // kPath
    std::vector<Value>,       // kList
    std::map<std::string, Value>  // kMap
  > value_;
};

// ============================================================================
// Record and ResultSet
// ============================================================================

/**
 * @brief Single record in query result
 */
struct Record {
  std::map<std::string, Value> values;
  
  Record() = default;
  explicit Record(std::map<std::string, Value> vals) 
      : values(std::move(vals)) {}
  
  bool HasKey(const std::string& key) const { return values.find(key) != values.end(); }
  std::optional<Value> Get(const std::string& key) const;
  void Set(const std::string& key, const Value& val) { values[key] = val; }
  
  std::string ToString() const;
};

/**
 * @brief Complete query result
 */
struct ResultSet {
  std::vector<std::string> columns;
  std::vector<Record> records;
  std::optional<std::string> error;
  
  // Statistics
  size_t rows_affected = 0;
  size_t rows_returned = 0;
  int64_t execution_time_us = 0;
  
  ResultSet() = default;
  explicit ResultSet(std::vector<std::string> cols) 
      : columns(std::move(cols)) {}
  
  bool HasError() const { return error.has_value(); }
  void SetError(const std::string& msg) { error = msg; }
  
  bool IsEmpty() const { return records.empty(); }
  size_t Size() const { return records.size(); }
  
  const Record* GetFirst() const { return records.empty() ? nullptr : &records[0]; }
  std::optional<Value> GetFirstValue(const std::string& column) const;
};

// ============================================================================
// Helper functions
// ============================================================================

inline ValueType StringToValueType(const std::string& str) {
  static const std::map<std::string, ValueType> type_map = {
    {"null", ValueType::kNull},
    {"bool", ValueType::kBool},
    {"int", ValueType::kInt},
    {"float", ValueType::kFloat},
    {"string", ValueType::kString},
    {"timestamp", ValueType::kTimestamp},
    {"date", ValueType::kDate},
    {"time", ValueType::kTime},
    {"datetime", ValueType::kDateTime},
    {"duration", ValueType::kDuration},
    {"node", ValueType::kNode},
    {"relationship", ValueType::kRelationship},
    {"path", ValueType::kPath},
    {"list", ValueType::kList},
    {"map", ValueType::kMap},
  };
  
  auto it = type_map.find(str);
  return it != type_map.end() ? it->second : ValueType::kNull;
}

inline const char* ValueTypeToString(ValueType type) {
  switch (type) {
    case ValueType::kNull: return "NULL";
    case ValueType::kBool: return "BOOL";
    case ValueType::kInt: return "INT";
    case ValueType::kFloat: return "FLOAT";
    case ValueType::kString: return "STRING";
    case ValueType::kTimestamp: return "TIMESTAMP";
    case ValueType::kDate: return "DATE";
    case ValueType::kTime: return "TIME";
    case ValueType::kDateTime: return "DATETIME";
    case ValueType::kDuration: return "DURATION";
    case ValueType::kNode: return "NODE";
    case ValueType::kRelationship: return "RELATIONSHIP";
    case ValueType::kPath: return "PATH";
    case ValueType::kList: return "LIST";
    case ValueType::kMap: return "MAP";
    default: return "UNKNOWN";
  }
}

}  // namespace cypher
}  // namespace cedar

// Hash specialization for Value
namespace std {
template<>
struct hash<cedar::cypher::Value> {
  size_t operator()(const cedar::cypher::Value& v) const {
    return v.Hash();
  }
};
}  // namespace std
