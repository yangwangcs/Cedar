// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "table/cedar_parquet/compact_protocol.h"

#include <limits>

namespace ROCKSDB_NAMESPACE::cedar_parquet {
namespace {

uint64_t ZigZagEncode(int64_t value) {
  return (static_cast<uint64_t>(value) << 1U) ^
         static_cast<uint64_t>(value >> 63U);
}

int64_t ZigZagDecode(uint64_t value) {
  return static_cast<int64_t>((value >> 1U) ^
                              (~(value & 1U) + 1U));
}

}  // namespace

void CompactWriter::WriteStructBegin() {
  field_id_stack_.push_back(last_field_id_);
  last_field_id_ = 0;
}

void CompactWriter::WriteStructEnd() {
  last_field_id_ = field_id_stack_.back();
  field_id_stack_.pop_back();
}

void CompactWriter::WriteFieldStop() { data_.push_back(static_cast<char>(kCompactStop)); }

void CompactWriter::WriteFieldHeader(int16_t field_id, uint8_t type) {
  const int16_t delta = field_id - last_field_id_;
  if (delta > 0 && delta <= 15) {
    data_.push_back(static_cast<char>((delta << 4U) | type));
  } else {
    data_.push_back(static_cast<char>(type));
    WriteI32(field_id);
  }
  last_field_id_ = field_id;
}

void CompactWriter::WriteUnsignedVarint(uint64_t value) {
  while (value >= 0x80U) {
    data_.push_back(static_cast<char>((value & 0x7fU) | 0x80U));
    value >>= 7U;
  }
  data_.push_back(static_cast<char>(value));
}

void CompactWriter::WriteI32(int32_t value) { WriteUnsignedVarint(ZigZagEncode(value)); }

void CompactWriter::WriteI64(int64_t value) { WriteUnsignedVarint(ZigZagEncode(value)); }

void CompactWriter::WriteBool(bool value) {
  data_.push_back(static_cast<char>(value ? kCompactTrue : kCompactFalse));
}

void CompactWriter::WriteBinary(const std::string& value) {
  WriteUnsignedVarint(value.size());
  data_.append(value);
}

void CompactWriter::WriteListBegin(uint32_t count, uint8_t element_type) {
  if (count <= 14) {
    data_.push_back(static_cast<char>((count << 4U) | element_type));
  } else {
    data_.push_back(static_cast<char>(0xf0U | element_type));
    WriteUnsignedVarint(count);
  }
}

void CompactWriter::WriteI32Field(int16_t field_id, int32_t value) {
  WriteFieldHeader(field_id, kCompactI32);
  WriteI32(value);
}

void CompactWriter::WriteI64Field(int16_t field_id, int64_t value) {
  WriteFieldHeader(field_id, kCompactI64);
  WriteI64(value);
}

void CompactWriter::WriteBinaryField(int16_t field_id, const std::string& value) {
  WriteFieldHeader(field_id, kCompactBinary);
  WriteBinary(value);
}

void CompactWriter::WriteListFieldBegin(int16_t field_id, uint32_t count,
                                         uint8_t element_type) {
  WriteFieldHeader(field_id, kCompactList);
  WriteListBegin(count, element_type);
}

void CompactWriter::WriteStructFieldBegin(int16_t field_id) {
  WriteFieldHeader(field_id, kCompactStruct);
}

void CompactWriter::WriteListEnd() {}

CompactReader::CompactReader(const std::string& data, size_t max_bytes)
    : data_(data), max_bytes_(max_bytes) {}

void CompactReader::ReadStructBegin() {
  field_id_stack_.push_back(last_field_id_);
  last_field_id_ = 0;
}

void CompactReader::ReadStructEnd() {
  last_field_id_ = field_id_stack_.back();
  field_id_stack_.pop_back();
}

Status CompactReader::ReadByte(uint8_t* value) {
  if (position_ >= data_.size()) return Status::Corruption("truncated compact protocol");
  *value = static_cast<uint8_t>(data_[position_++]);
  return Status::OK();
}

Status CompactReader::ReadUnsignedVarint(uint64_t* value) {
  *value = 0;
  for (uint32_t shift = 0; shift < 64; shift += 7) {
    uint8_t byte = 0;
    Status status = ReadByte(&byte);
    if (!status.ok()) return status;
    *value |= static_cast<uint64_t>(byte & 0x7fU) << shift;
    if ((byte & 0x80U) == 0) return Status::OK();
  }
  return Status::Corruption("compact varint exceeds 64 bits");
}

Status CompactReader::ReadFieldBegin(int16_t* field_id, uint8_t* type) {
  uint8_t header = 0;
  Status status = ReadByte(&header);
  if (!status.ok()) return status;
  *type = header & 0x0fU;
  if (*type == kCompactStop) return Status::OK();
  const uint8_t delta = header >> 4U;
  if (delta != 0) {
    *field_id = static_cast<int16_t>(last_field_id_ + delta);
  } else {
    int32_t explicit_id = 0;
    status = ReadI32(&explicit_id);
    if (!status.ok()) return status;
    if (explicit_id <= 0 || explicit_id > std::numeric_limits<int16_t>::max()) {
      return Status::Corruption("invalid compact field id");
    }
    *field_id = static_cast<int16_t>(explicit_id);
  }
  last_field_id_ = *field_id;
  return Status::OK();
}

Status CompactReader::ReadI32(int32_t* value) {
  uint64_t encoded = 0;
  Status status = ReadUnsignedVarint(&encoded);
  if (!status.ok()) return status;
  const int64_t decoded = ZigZagDecode(encoded);
  if (decoded < std::numeric_limits<int32_t>::min() ||
      decoded > std::numeric_limits<int32_t>::max()) {
    return Status::Corruption("compact i32 out of range");
  }
  *value = static_cast<int32_t>(decoded);
  return Status::OK();
}

Status CompactReader::ReadI64(int64_t* value) {
  uint64_t encoded = 0;
  Status status = ReadUnsignedVarint(&encoded);
  if (!status.ok()) return status;
  *value = ZigZagDecode(encoded);
  return Status::OK();
}

Status CompactReader::ReadBool(bool* value) {
  uint8_t encoded = 0;
  Status status = ReadByte(&encoded);
  if (!status.ok()) return status;
  if (encoded != kCompactTrue && encoded != kCompactFalse) {
    return Status::Corruption("invalid compact bool");
  }
  *value = encoded == kCompactTrue;
  return Status::OK();
}

Status CompactReader::ReadBinary(std::string* value) {
  uint64_t size = 0;
  Status status = ReadUnsignedVarint(&size);
  if (!status.ok()) return status;
  if (size > max_bytes_ || size > data_.size() - position_) {
    return Status::Corruption("invalid compact binary size");
  }
  value->assign(data_.data() + position_, static_cast<size_t>(size));
  position_ += static_cast<size_t>(size);
  return Status::OK();
}

Status CompactReader::ReadListBegin(uint32_t* count, uint8_t* element_type) {
  uint8_t header = 0;
  Status status = ReadByte(&header);
  if (!status.ok()) return status;
  *element_type = header & 0x0fU;
  uint64_t decoded_count = header >> 4U;
  if (decoded_count == 15) {
    status = ReadUnsignedVarint(&decoded_count);
    if (!status.ok()) return status;
  }
  if (decoded_count > max_bytes_ || decoded_count > std::numeric_limits<uint32_t>::max()) {
    return Status::MemoryLimit("compact list exceeds bound");
  }
  *count = static_cast<uint32_t>(decoded_count);
  return Status::OK();
}

Status CompactReader::Skip(uint8_t type, uint32_t depth) {
  if (depth > 32) return Status::Corruption("compact nesting exceeds bound");
  switch (type) {
    case kCompactI32: {
      int32_t value = 0;
      return ReadI32(&value);
    }
    case kCompactI64: {
      int64_t value = 0;
      return ReadI64(&value);
    }
    case kCompactBinary: {
      std::string value;
      return ReadBinary(&value);
    }
    case kCompactList: {
      uint32_t count = 0;
      uint8_t element_type = 0;
      Status status = ReadListBegin(&count, &element_type);
      if (!status.ok()) return status;
      for (uint32_t index = 0; index < count; ++index) {
        status = Skip(element_type, depth + 1);
        if (!status.ok()) return status;
      }
      return Status::OK();
    }
    case kCompactStruct: {
      ReadStructBegin();
      while (true) {
        int16_t field_id = 0;
        uint8_t field_type = 0;
        Status status = ReadFieldBegin(&field_id, &field_type);
        if (!status.ok()) return status;
        if (field_type == kCompactStop) {
          ReadStructEnd();
          return Status::OK();
        }
        status = Skip(field_type, depth + 1);
        if (!status.ok()) return status;
      }
    }
    default:
      return Status::NotSupported("unsupported compact field type");
  }
}

}  // namespace ROCKSDB_NAMESPACE::cedar_parquet
