// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE::cedar_parquet {

class CompactWriter {
 public:
  void WriteStructBegin();
  void WriteStructEnd();
  void WriteFieldStop();
  void WriteI32Field(int16_t field_id, int32_t value);
  void WriteI64Field(int16_t field_id, int64_t value);
  void WriteBinaryField(int16_t field_id, const std::string& value);
  void WriteListFieldBegin(int16_t field_id, uint32_t count, uint8_t element_type);
  void WriteStructFieldBegin(int16_t field_id);
  void WriteListEnd();

  void WriteI32(int32_t value);
  void WriteI64(int64_t value);
  void WriteBool(bool value);
  void WriteBinary(const std::string& value);
  void WriteListBegin(uint32_t count, uint8_t element_type);

  const std::string& data() const { return data_; }

 private:
  void WriteFieldHeader(int16_t field_id, uint8_t type);
  void WriteUnsignedVarint(uint64_t value);
  std::string data_;
  int16_t last_field_id_ = 0;
  std::vector<int16_t> field_id_stack_;
};

class CompactReader {
 public:
  explicit CompactReader(const std::string& data, size_t max_bytes = 64U << 20);

  Status ReadFieldBegin(int16_t* field_id, uint8_t* type);
  void ReadStructBegin();
  void ReadStructEnd();
  Status ReadI32(int32_t* value);
  Status ReadI64(int64_t* value);
  Status ReadBool(bool* value);
  Status ReadBinary(std::string* value);
  Status ReadListBegin(uint32_t* count, uint8_t* element_type);
  Status Skip(uint8_t type, uint32_t depth = 0);
  bool empty() const { return position_ == data_.size(); }
  size_t position() const { return position_; }

 private:
  Status ReadUnsignedVarint(uint64_t* value);
  Status ReadByte(uint8_t* value);

  const std::string& data_;
  size_t position_ = 0;
  size_t max_bytes_;
  int16_t last_field_id_ = 0;
  std::vector<int16_t> field_id_stack_;
};

constexpr uint8_t kCompactStop = 0;
constexpr uint8_t kCompactTrue = 1;
constexpr uint8_t kCompactFalse = 2;
constexpr uint8_t kCompactI32 = 5;
constexpr uint8_t kCompactI64 = 6;
constexpr uint8_t kCompactBinary = 8;
constexpr uint8_t kCompactList = 9;
constexpr uint8_t kCompactStruct = 12;

}  // namespace ROCKSDB_NAMESPACE::cedar_parquet
