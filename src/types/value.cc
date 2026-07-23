// Copyright 2026 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#include "cedar/types/value.h"

#include <cstring>

namespace cedar {
namespace {

template <typename T>
void PutLittleEndian(std::string* out, T value) {
  using U = typename std::make_unsigned<T>::type;
  U bits = static_cast<U>(value);
  for (size_t i = 0; i < sizeof(T); ++i) out->push_back(static_cast<char>(bits >> (i * 8)));
}

template <typename T>
bool GetLittleEndian(const std::string& input, size_t* offset, T* value) {
  if (input.size() - *offset < sizeof(T)) return false;
  using U = typename std::make_unsigned<T>::type;
  U bits = 0;
  for (size_t i = 0; i < sizeof(T); ++i) bits |= static_cast<U>(static_cast<uint8_t>(input[*offset + i])) << (i * 8);
  *offset += sizeof(T);
  *value = static_cast<T>(bits);
  return true;
}

void PutLength(std::string* out, uint32_t length) { PutLittleEndian(out, length); }
bool GetLength(const std::string& input, size_t* offset, uint32_t* length) {
  return GetLittleEndian(input, offset, length);
}

}  // namespace

Value Value::Bool(bool value) { return Value(PhysicalType::kBool, value); }
Value Value::Int32(int32_t value) { return Value(PhysicalType::kInt32, value); }
Value Value::Int64(int64_t value) { return Value(PhysicalType::kInt64, value); }
Value Value::Float32(float value) { return Value(PhysicalType::kFloat32, value); }
Value Value::Float64(double value) { return Value(PhysicalType::kFloat64, value); }
Value Value::Timestamp(uint64_t value) { return Value(PhysicalType::kTimestamp64, value); }
Value Value::String(std::string value) { return Value(PhysicalType::kString, std::move(value)); }
Value Value::Binary(const char* value, size_t size) { return Binary(std::string(value, size)); }
Value Value::Binary(std::string value) { return Value(PhysicalType::kBinary, std::move(value)); }

std::string Value::Encode() const {
  std::string out(1, static_cast<char>(type_));
  switch (type_) {
    case PhysicalType::kBool: out.push_back(std::get<bool>(data_) ? 1 : 0); break;
    case PhysicalType::kInt32: PutLittleEndian(&out, std::get<int32_t>(data_)); break;
    case PhysicalType::kInt64: PutLittleEndian(&out, std::get<int64_t>(data_)); break;
    case PhysicalType::kFloat32: {
      uint32_t bits; std::memcpy(&bits, &std::get<float>(data_), sizeof(bits)); PutLittleEndian(&out, bits); break;
    }
    case PhysicalType::kFloat64: {
      uint64_t bits; std::memcpy(&bits, &std::get<double>(data_), sizeof(bits)); PutLittleEndian(&out, bits); break;
    }
    case PhysicalType::kTimestamp64: PutLittleEndian(&out, std::get<uint64_t>(data_)); break;
    case PhysicalType::kString:
    case PhysicalType::kBinary: {
      const std::string& bytes = std::get<std::string>(data_);
      PutLength(&out, static_cast<uint32_t>(bytes.size())); out.append(bytes); break;
    }
  }
  return out;
}

std::optional<Value> Value::Decode(const std::string& encoded) {
  if (encoded.empty()) return std::nullopt;
  const auto type = static_cast<PhysicalType>(static_cast<uint8_t>(encoded[0]));
  size_t offset = 1;
  switch (type) {
    case PhysicalType::kBool:
      if (encoded.size() != 2 || (encoded[1] != 0 && encoded[1] != 1)) return std::nullopt;
      return Bool(encoded[1] == 1);
    case PhysicalType::kInt32: { int32_t value; return GetLittleEndian(encoded, &offset, &value) && offset == encoded.size() ? std::optional<Value>(Int32(value)) : std::nullopt; }
    case PhysicalType::kInt64: { int64_t value; return GetLittleEndian(encoded, &offset, &value) && offset == encoded.size() ? std::optional<Value>(Int64(value)) : std::nullopt; }
    case PhysicalType::kFloat32: { uint32_t bits; if (!GetLittleEndian(encoded, &offset, &bits) || offset != encoded.size()) return std::nullopt; float value; std::memcpy(&value, &bits, sizeof(value)); return Float32(value); }
    case PhysicalType::kFloat64: { uint64_t bits; if (!GetLittleEndian(encoded, &offset, &bits) || offset != encoded.size()) return std::nullopt; double value; std::memcpy(&value, &bits, sizeof(value)); return Float64(value); }
    case PhysicalType::kTimestamp64: { uint64_t value; return GetLittleEndian(encoded, &offset, &value) && offset == encoded.size() ? std::optional<Value>(Timestamp(value)) : std::nullopt; }
    case PhysicalType::kString:
    case PhysicalType::kBinary: {
      uint32_t length;
      if (!GetLength(encoded, &offset, &length) || length != encoded.size() - offset) return std::nullopt;
      std::string bytes = encoded.substr(offset);
      return type == PhysicalType::kString ? String(std::move(bytes)) : Binary(std::move(bytes));
    }
  }
  return std::nullopt;
}

bool Value::operator==(const Value& other) const {
  return type_ == other.type_ && data_ == other.data_;
}

}  // namespace cedar
