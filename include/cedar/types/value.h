// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#ifndef CEDAR_TYPES_VALUE_H_
#define CEDAR_TYPES_VALUE_H_

#include <cstdint>
#include <optional>
#include <string>
#include <variant>

namespace cedar {

enum class PhysicalType : uint8_t {
  kBool = 1,
  kInt32 = 2,
  kInt64 = 3,
  kFloat32 = 4,
  kFloat64 = 5,
  kTimestamp64 = 6,
  kString = 7,
  kBinary = 8,
};

// Schema-bound value used by every new storage and query contract. Strings and
// binary values share an in-memory representation but never a type tag.
class Value {
 public:
  static Value Bool(bool value);
  static Value Int32(int32_t value);
  static Value Int64(int64_t value);
  static Value Float32(float value);
  static Value Float64(double value);
  static Value Timestamp(uint64_t value);
  static Value String(std::string value);
  static Value Binary(const char* value, size_t size);
  static Value Binary(std::string value);

  PhysicalType type() const { return type_; }
  const std::variant<bool, int32_t, int64_t, float, double, uint64_t, std::string>&
  data() const { return data_; }
  std::string Encode() const;
  static std::optional<Value> Decode(const std::string& encoded);

  bool operator==(const Value& other) const;
  bool operator!=(const Value& other) const { return !(*this == other); }

 private:
  Value(PhysicalType type,
        std::variant<bool, int32_t, int64_t, float, double, uint64_t, std::string> data)
      : type_(type), data_(std::move(data)) {}

  PhysicalType type_;
  std::variant<bool, int32_t, int64_t, float, double, uint64_t, std::string> data_;
};

}  // namespace cedar

#endif  // CEDAR_TYPES_VALUE_H_
