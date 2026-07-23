// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/tcypher/runtime/query_spill.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <limits>
#include <utility>
#include <vector>

#include "cedar/core/crc32c.h"

namespace cedar {
namespace {

constexpr uint32_t kSpillMagic = 0x31505343U;  // CSP1
constexpr uint32_t kSpillVersion = 2;
constexpr uint64_t kMaxSpillRecordBytes = 64ULL << 20;
constexpr uint64_t kMaxSpillStringBytes = 16ULL << 20;
constexpr uint32_t kMaxCompositeElements = 1U << 20;
constexpr uint32_t kMaxSpillColumns = 1U << 16;
constexpr size_t kFileHeaderBytes = 8;
constexpr size_t kRecordHeaderBytes = 8;

void PutU8(std::string* output, uint8_t value) { output->push_back(static_cast<char>(value)); }
void PutU32(std::string* output, uint32_t value) {
  for (uint32_t shift = 0; shift < 32; shift += 8) {
    output->push_back(static_cast<char>(value >> shift));
  }
}
void PutU64(std::string* output, uint64_t value) {
  for (uint32_t shift = 0; shift < 64; shift += 8) {
    output->push_back(static_cast<char>(value >> shift));
  }
}
bool GetU8(const std::string& input, size_t* offset, uint8_t* value) {
  if (*offset == input.size()) return false;
  *value = static_cast<uint8_t>(input[(*offset)++]);
  return true;
}
bool GetU32(const std::string& input, size_t* offset, uint32_t* value) {
  if (input.size() - *offset < 4) return false;
  *value = 0;
  for (uint32_t shift = 0; shift < 32; shift += 8) {
    *value |= static_cast<uint32_t>(static_cast<uint8_t>(input[(*offset)++])) << shift;
  }
  return true;
}
bool GetU64(const std::string& input, size_t* offset, uint64_t* value) {
  if (input.size() - *offset < 8) return false;
  *value = 0;
  for (uint32_t shift = 0; shift < 64; shift += 8) {
    *value |= static_cast<uint64_t>(static_cast<uint8_t>(input[(*offset)++])) << shift;
  }
  return true;
}

Status WriteAll(int fd, const std::string& bytes, const std::string& path) {
  const char* cursor = bytes.data();
  size_t remaining = bytes.size();
  while (remaining != 0) {
    const ssize_t written = ::write(fd, cursor, remaining);
    if (written < 0) {
      if (errno == EINTR) continue;
      return Status::IOError(path, std::strerror(errno));
    }
    cursor += written;
    remaining -= static_cast<size_t>(written);
  }
  return Status::OK();
}

Status ReadExact(int fd, size_t bytes, std::string* output, const std::string& path,
                 bool* eof) {
  std::string().swap(*output);
  output->resize(bytes);
  size_t offset = 0;
  while (offset < bytes) {
    const ssize_t read_bytes = ::read(fd, output->data() + offset, bytes - offset);
    if (read_bytes == 0) {
      if (offset == 0) {
        *eof = true;
        output->clear();
        return Status::OK();
      }
      return Status::Corruption(path, "truncated spill record");
    }
    if (read_bytes < 0) {
      if (errno == EINTR) continue;
      return Status::IOError(path, std::strerror(errno));
    }
    offset += static_cast<size_t>(read_bytes);
  }
  *eof = false;
  return Status::OK();
}

bool AddMeasuredBytes(uint64_t bytes, uint64_t* total) {
  if (total == nullptr || bytes > std::numeric_limits<uint64_t>::max() - *total) {
    return false;
  }
  *total += bytes;
  return true;
}

StatusOr<uint64_t> EncodedValueBytes(const Value& value) {
  uint64_t bytes = 1;
  switch (value.type()) {
    case PhysicalType::kBool: return bytes + 1;
    case PhysicalType::kInt32:
    case PhysicalType::kFloat32: return bytes + 4;
    case PhysicalType::kInt64:
    case PhysicalType::kFloat64:
    case PhysicalType::kTimestamp64: return bytes + 8;
    case PhysicalType::kString:
    case PhysicalType::kBinary: {
      const size_t payload = std::get<std::string>(value.data()).size();
      if (payload > kMaxSpillStringBytes) {
        return Status::InvalidArgument(
            "query spill", "result value exceeds spill bound");
      }
      return bytes + 4 + static_cast<uint64_t>(payload);
    }
  }
  return Status::Corruption("query spill", "unknown result value type");
}

Status MeasureOptionalValue(const std::optional<Value>& value,
                            uint64_t* bytes) {
  if (!AddMeasuredBytes(1, bytes)) {
    return Status::QueryMemoryLimit("query spill", "encoded batch size overflow");
  }
  if (!value.has_value()) return Status::OK();
  const auto encoded = EncodedValueBytes(*value);
  if (!encoded.ok()) return encoded.status();
  if (!AddMeasuredBytes(4 + encoded.ValueOrDie(), bytes)) {
    return Status::QueryMemoryLimit("query spill", "encoded batch size overflow");
  }
  return Status::OK();
}

Status MeasureOptionalValue(const Value* value, uint64_t* bytes) {
  if (!AddMeasuredBytes(1, bytes)) {
    return Status::QueryMemoryLimit("query spill", "encoded batch size overflow");
  }
  if (value == nullptr) return Status::OK();
  const auto encoded = EncodedValueBytes(*value);
  if (!encoded.ok()) return encoded.status();
  if (!AddMeasuredBytes(4 + encoded.ValueOrDie(), bytes)) {
    return Status::QueryMemoryLimit("query spill", "encoded batch size overflow");
  }
  return Status::OK();
}

StatusOr<uint64_t> MeasureEncodedBatch(const ResultBatch& batch) {
  const Status valid = batch.Validate();
  if (!valid.ok()) return valid;
  const ColumnBatch& source = batch.batch();
  uint64_t bytes = 4 + 4 + 8 + 1 + 1;
  for (const std::string& name : batch.column_names()) {
    if (name.size() > kMaxSpillStringBytes ||
        !AddMeasuredBytes(4 + name.size(), &bytes)) {
      return Status::QueryMemoryLimit(
          "query spill", "encoded batch size exceeds bound");
    }
  }
  for (uint32_t column = 0; column < source.column_count(); ++column) {
    if (!AddMeasuredBytes(1, &bytes)) {
      return Status::QueryMemoryLimit(
          "query spill", "encoded batch size overflow");
    }
    for (uint32_t row = 0; row < source.row_count(); ++row) {
      if (!source.IsStructured(column) && !source.IsList(column)) {
        const Status measured =
            MeasureOptionalValue(source.ValueRefAt(column, row), &bytes);
        if (!measured.ok()) return measured;
        continue;
      }
      if (source.IsStructured(column)) {
        const StructValue* value = source.StructRefAt(column, row);
        if (!AddMeasuredBytes(1, &bytes)) {
          return Status::QueryMemoryLimit(
              "query spill", "encoded batch size overflow");
        }
        if (value == nullptr) continue;
        if (value->fields.size() > kMaxCompositeElements ||
            !AddMeasuredBytes(4, &bytes)) {
          return Status::InvalidArgument(
              "query spill", "result struct exceeds field bound");
        }
        for (const StructField& field : value->fields) {
          if (field.name.size() > kMaxSpillStringBytes ||
              !AddMeasuredBytes(4 + field.name.size(), &bytes)) {
            return Status::InvalidArgument(
                "query spill", "result struct field name exceeds spill bound");
          }
          const Status measured = MeasureOptionalValue(field.value, &bytes);
          if (!measured.ok()) return measured;
        }
        continue;
      }
      const ListValue* value = source.ListRefAt(column, row);
      if (!AddMeasuredBytes(1, &bytes)) {
        return Status::QueryMemoryLimit(
            "query spill", "encoded batch size overflow");
      }
      if (value == nullptr) continue;
      if (!value->IsConsistent()) {
        return Status::InvalidArgument(
            "query spill", "result list element kind is inconsistent");
      }
      const bool structured =
          value->element_kind == ListElementKind::kStruct;
      const size_t element_count = structured
          ? value->structured_elements.size() : value->elements.size();
      if (element_count > kMaxCompositeElements ||
          !AddMeasuredBytes(5, &bytes)) {
        return Status::InvalidArgument(
            "query spill", "result list exceeds element bound");
      }
      if (!structured) {
        for (const auto& element : value->elements) {
          const Status measured = MeasureOptionalValue(element, &bytes);
          if (!measured.ok()) return measured;
        }
      } else {
        for (const StructValue& element : value->structured_elements) {
          if (element.fields.size() > kMaxCompositeElements ||
              !AddMeasuredBytes(4, &bytes)) {
            return Status::InvalidArgument(
                "query spill", "result list struct exceeds field bound");
          }
          for (const StructField& field : element.fields) {
            if (field.name.size() > kMaxSpillStringBytes ||
                !AddMeasuredBytes(4 + field.name.size(), &bytes)) {
              return Status::InvalidArgument(
                  "query spill", "result list struct field name exceeds spill bound");
            }
            const Status measured = MeasureOptionalValue(field.value, &bytes);
            if (!measured.ok()) return measured;
          }
        }
      }
    }
  }
  if (bytes > kMaxSpillRecordBytes) {
    return Status::QueryMemoryLimit(
        "query spill", "encoded result batch exceeds spill record bound");
  }
  return bytes;
}

Status EncodeValue(std::string* output, const Value& value) {
  PutU8(output, static_cast<uint8_t>(value.type()));
  switch (value.type()) {
    case PhysicalType::kBool:
      PutU8(output, std::get<bool>(value.data()) ? 1 : 0);
      return Status::OK();
    case PhysicalType::kInt32:
      PutU32(output, static_cast<uint32_t>(std::get<int32_t>(value.data())));
      return Status::OK();
    case PhysicalType::kInt64:
      PutU64(output, static_cast<uint64_t>(std::get<int64_t>(value.data())));
      return Status::OK();
    case PhysicalType::kFloat32: {
      uint32_t bits = 0;
      const float scalar = std::get<float>(value.data());
      std::memcpy(&bits, &scalar, sizeof(bits));
      PutU32(output, bits);
      return Status::OK();
    }
    case PhysicalType::kFloat64: {
      uint64_t bits = 0;
      const double scalar = std::get<double>(value.data());
      std::memcpy(&bits, &scalar, sizeof(bits));
      PutU64(output, bits);
      return Status::OK();
    }
    case PhysicalType::kTimestamp64:
      PutU64(output, std::get<uint64_t>(value.data()));
      return Status::OK();
    case PhysicalType::kString:
    case PhysicalType::kBinary: {
      const std::string& bytes = std::get<std::string>(value.data());
      if (bytes.size() > kMaxSpillStringBytes) {
        return Status::InvalidArgument(
            "query spill", "result value exceeds spill bound");
      }
      PutU32(output, static_cast<uint32_t>(bytes.size()));
      output->append(bytes);
      return Status::OK();
    }
  }
  return Status::Corruption("query spill", "unknown result value type");
}

Status EncodeOptionalValue(std::string* output, const Value* value) {
  PutU8(output, value != nullptr ? 1 : 0);
  if (value == nullptr) return Status::OK();
  const auto encoded_bytes = EncodedValueBytes(*value);
  if (!encoded_bytes.ok()) return encoded_bytes.status();
  PutU32(output, static_cast<uint32_t>(encoded_bytes.ValueOrDie()));
  return EncodeValue(output, *value);
}

Status EncodeOptionalValue(std::string* output, const std::optional<Value>& value) {
  PutU8(output, value.has_value() ? 1 : 0);
  if (!value.has_value()) return Status::OK();
  const auto encoded_bytes = EncodedValueBytes(*value);
  if (!encoded_bytes.ok()) return encoded_bytes.status();
  PutU32(output, static_cast<uint32_t>(encoded_bytes.ValueOrDie()));
  return EncodeValue(output, *value);
}

Status DecodeOptionalValue(const std::string& input, size_t* offset,
                           std::optional<Value>* value) {
  uint8_t present = 0;
  if (!GetU8(input, offset, &present) || present > 1) {
    return Status::Corruption("query spill", "invalid result value validity");
  }
  if (present == 0) {
    value->reset();
    return Status::OK();
  }
  uint32_t length = 0;
  if (!GetU32(input, offset, &length) || length > kMaxSpillStringBytes ||
      length > input.size() - *offset) {
    return Status::Corruption("query spill", "invalid result value length");
  }
  const auto decoded = Value::Decode(input.substr(*offset, length));
  *offset += length;
  if (!decoded.has_value()) return Status::Corruption("query spill", "invalid encoded result value");
  *value = *decoded;
  return Status::OK();
}

Status EncodeStructValue(std::string* output, const StructValue& value) {
  if (value.fields.size() > kMaxCompositeElements) {
    return Status::InvalidArgument("query spill", "result struct exceeds field bound");
  }
  PutU32(output, static_cast<uint32_t>(value.fields.size()));
  for (const StructField& field : value.fields) {
    if (field.name.size() > kMaxSpillStringBytes) {
      return Status::InvalidArgument("query spill", "result struct field name exceeds spill bound");
    }
    PutU32(output, static_cast<uint32_t>(field.name.size()));
    output->append(field.name);
    const Status encoded = EncodeOptionalValue(output, field.value);
    if (!encoded.ok()) return encoded;
  }
  return Status::OK();
}

Status DecodeStructValue(const std::string& input, size_t* offset, StructValue* value) {
  uint32_t fields = 0;
  if (!GetU32(input, offset, &fields) || fields > kMaxCompositeElements) {
    return Status::Corruption("query spill", "invalid result struct field count");
  }
  value->fields.clear();
  value->fields.reserve(fields);
  for (uint32_t field = 0; field < fields; ++field) {
    uint32_t name_length = 0;
    if (!GetU32(input, offset, &name_length) || name_length > kMaxSpillStringBytes ||
        name_length > input.size() - *offset) {
      return Status::Corruption("query spill", "invalid result struct field name");
    }
    StructField decoded;
    decoded.name = input.substr(*offset, name_length);
    *offset += name_length;
    const Status value_status = DecodeOptionalValue(input, offset, &decoded.value);
    if (!value_status.ok()) return value_status;
    value->fields.push_back(std::move(decoded));
  }
  return Status::OK();
}

Status EncodeListValue(std::string* output, const ListValue& value) {
  if (!value.IsConsistent()) {
    return Status::InvalidArgument(
        "query spill", "result list element kind is inconsistent");
  }
  const bool structured = value.element_kind == ListElementKind::kStruct;
  const size_t element_count = structured
      ? value.structured_elements.size() : value.elements.size();
  if (element_count > kMaxCompositeElements) {
    return Status::InvalidArgument("query spill", "result list exceeds element bound");
  }
  PutU8(output, structured ? 1 : 0);
  PutU32(output, static_cast<uint32_t>(element_count));
  if (!structured) {
    for (const auto& element : value.elements) {
      const Status encoded = EncodeOptionalValue(output, element);
      if (!encoded.ok()) return encoded;
    }
  } else {
    for (const StructValue& element : value.structured_elements) {
      const Status encoded = EncodeStructValue(output, element);
      if (!encoded.ok()) return encoded;
    }
  }
  return Status::OK();
}

Status DecodeListValue(const std::string& input, size_t* offset, ListValue* value) {
  uint8_t structured = 0;
  uint32_t elements = 0;
  if (!GetU8(input, offset, &structured) || structured > 1 ||
      !GetU32(input, offset, &elements) || elements > kMaxCompositeElements) {
    return Status::Corruption("query spill", "invalid result list element count");
  }
  value->elements.clear();
  value->structured_elements.clear();
  value->element_kind = structured == 0
      ? ListElementKind::kScalar : ListElementKind::kStruct;
  if (structured == 0) {
    value->elements.reserve(elements);
    for (uint32_t element = 0; element < elements; ++element) {
      std::optional<Value> decoded;
      const Status status = DecodeOptionalValue(input, offset, &decoded);
      if (!status.ok()) return status;
      value->elements.push_back(std::move(decoded));
    }
  } else {
    value->structured_elements.reserve(elements);
    for (uint32_t element = 0; element < elements; ++element) {
      StructValue decoded;
      const Status status = DecodeStructValue(input, offset, &decoded);
      if (!status.ok()) return status;
      value->structured_elements.push_back(std::move(decoded));
    }
  }
  return Status::OK();
}

StatusOr<std::string> EncodeBatch(const ResultBatch& batch,
                                  uint64_t encoded_bytes) {
  const Status valid = batch.Validate();
  if (!valid.ok()) return valid;
  const ColumnBatch& source = batch.batch();
  std::string output;
  output.reserve(static_cast<size_t>(encoded_bytes));
  PutU32(&output, source.column_count());
  PutU32(&output, source.row_count());
  PutU64(&output, batch.temporal_metadata().snapshot_seq);
  PutU8(&output, batch.temporal_metadata().includes_valid_time ? 1 : 0);
  PutU8(&output, batch.temporal_metadata().includes_system_time ? 1 : 0);
  for (const std::string& name : batch.column_names()) {
    if (name.size() > kMaxSpillStringBytes) {
      return Status::InvalidArgument("query spill", "result column name exceeds spill bound");
    }
    PutU32(&output, static_cast<uint32_t>(name.size()));
    output.append(name);
  }
  for (uint32_t column = 0; column < source.column_count(); ++column) {
    const uint8_t kind = source.IsList(column) ? 2 : source.IsStructured(column) ? 1 : 0;
    PutU8(&output, kind);
    for (uint32_t row = 0; row < source.row_count(); ++row) {
      if (kind == 0) {
        const Status encoded = EncodeOptionalValue(
            &output, source.ValueRefAt(column, row));
        if (!encoded.ok()) return encoded;
      } else if (kind == 1) {
        const StructValue* value = source.StructRefAt(column, row);
        PutU8(&output, value != nullptr ? 1 : 0);
        if (value == nullptr) continue;
        const Status encoded = EncodeStructValue(&output, *value);
        if (!encoded.ok()) return encoded;
      } else {
        const ListValue* value = source.ListRefAt(column, row);
        PutU8(&output, value != nullptr ? 1 : 0);
        if (value == nullptr) continue;
        const Status encoded = EncodeListValue(&output, *value);
        if (!encoded.ok()) return encoded;
      }
    }
  }
  if (output.size() > kMaxSpillRecordBytes) {
    return Status::QueryMemoryLimit("query spill", "encoded result batch exceeds spill record bound");
  }
  if (output.size() != encoded_bytes) {
    return Status::Corruption("query spill", "encoded batch size mismatch");
  }
  return output;
}

Status MeasureDecodedOptionalValue(const std::string& input, size_t* offset,
                                   uint64_t* retained_bytes,
                                   uint64_t* decode_temporary_bytes) {
  uint8_t present = 0;
  if (!GetU8(input, offset, &present) || present > 1) {
    return Status::Corruption(
        "query spill", "invalid result value validity");
  }
  if (present == 0) return Status::OK();
  uint32_t length = 0;
  if (!GetU32(input, offset, &length) || length > kMaxSpillStringBytes ||
      length > input.size() - *offset) {
    return Status::Corruption("query spill", "invalid result value length");
  }
  if (!AddMeasuredBytes(length, retained_bytes)) {
    return Status::QueryMemoryLimit(
        "query spill", "decoded batch size overflow");
  }
  uint64_t value_temporary_bytes = 0;
  if (!AddMeasuredBytes(length, &value_temporary_bytes) ||
      !AddMeasuredBytes(length, &value_temporary_bytes)) {
    return Status::QueryMemoryLimit(
        "query spill", "decoded batch size overflow");
  }
  *decode_temporary_bytes = std::max(
      *decode_temporary_bytes, value_temporary_bytes);
  *offset += length;
  return Status::OK();
}

StatusOr<uint64_t> MeasureDecodedBatch(
    const std::string& input, uint64_t* decode_temporary_bytes) {
  if (decode_temporary_bytes == nullptr) {
    return Status::InvalidArgument(
        "query spill", "missing decoded temporary measurement");
  }
  *decode_temporary_bytes = 0;
  size_t offset = 0;
  uint32_t columns = 0;
  uint32_t rows = 0;
  uint64_t snapshot_seq = 0;
  uint8_t includes_valid_time = 0;
  uint8_t includes_system_time = 0;
  if (!GetU32(input, &offset, &columns) || !GetU32(input, &offset, &rows) ||
      !GetU64(input, &offset, &snapshot_seq) ||
      !GetU8(input, &offset, &includes_valid_time) ||
      !GetU8(input, &offset, &includes_system_time) || columns == 0 ||
      columns > kMaxSpillColumns || rows > kTcypherStandardBatchCapacity ||
      includes_valid_time > 1 || includes_system_time > 1) {
    return Status::Corruption("query spill", "invalid result batch header");
  }
  (void)snapshot_seq;
  uint64_t bytes = sizeof(ResultBatch) + sizeof(ColumnBatch) + 128;
  if (!AddMeasuredBytes(
          static_cast<uint64_t>(columns) *
              (sizeof(std::string) + sizeof(std::shared_ptr<const Vector>) + 64),
          &bytes)) {
    return Status::QueryMemoryLimit(
        "query spill", "decoded batch size overflow");
  }
  for (uint32_t column = 0; column < columns; ++column) {
    uint32_t length = 0;
    if (!GetU32(input, &offset, &length) || length > kMaxSpillStringBytes ||
        length > input.size() - offset) {
      return Status::Corruption("query spill", "invalid result column name");
    }
    if (!AddMeasuredBytes(length, &bytes)) {
      return Status::QueryMemoryLimit(
          "query spill", "decoded batch size overflow");
    }
    offset += length;
  }
  for (uint32_t column = 0; column < columns; ++column) {
    uint8_t kind = 0;
    if (!GetU8(input, &offset, &kind) || kind > 2) {
      return Status::Corruption("query spill", "invalid result column kind");
    }
    const uint64_t row_overhead = kind == 0
        ? sizeof(Value) + sizeof(bool)
        : kind == 1 ? sizeof(StructValue) + sizeof(bool)
                    : sizeof(ListValue) + sizeof(bool);
    if (!AddMeasuredBytes(
            64 + static_cast<uint64_t>(rows) * row_overhead, &bytes)) {
      return Status::QueryMemoryLimit(
          "query spill", "decoded batch size overflow");
    }
    for (uint32_t row = 0; row < rows; ++row) {
      if (kind == 0) {
        const Status measured = MeasureDecodedOptionalValue(
            input, &offset, &bytes, decode_temporary_bytes);
        if (!measured.ok()) return measured;
        continue;
      }
      uint8_t present = 0;
      if (!GetU8(input, &offset, &present) || present > 1) {
        return Status::Corruption(
            "query spill", "invalid composite result validity");
      }
      if (present == 0) continue;
      if (kind == 1) {
        uint32_t elements = 0;
        if (!GetU32(input, &offset, &elements) ||
            elements > kMaxCompositeElements) {
          return Status::Corruption(
              "query spill", "invalid composite result element count");
        }
        if (!AddMeasuredBytes(
                static_cast<uint64_t>(elements) * sizeof(StructField),
                &bytes)) {
          return Status::QueryMemoryLimit(
              "query spill", "decoded batch size overflow");
        }
        for (uint32_t field = 0; field < elements; ++field) {
          uint32_t name_length = 0;
          if (!GetU32(input, &offset, &name_length) ||
              name_length > kMaxSpillStringBytes ||
              name_length > input.size() - offset) {
            return Status::Corruption(
                "query spill", "invalid result struct field name");
          }
          if (!AddMeasuredBytes(name_length, &bytes)) {
            return Status::QueryMemoryLimit(
                "query spill", "decoded batch size overflow");
          }
          offset += name_length;
          const Status measured = MeasureDecodedOptionalValue(
              input, &offset, &bytes, decode_temporary_bytes);
          if (!measured.ok()) return measured;
        }
      } else {
        uint8_t structured = 0;
        uint32_t elements = 0;
        if (!GetU8(input, &offset, &structured) || structured > 1 ||
            !GetU32(input, &offset, &elements) ||
            elements > kMaxCompositeElements) {
          return Status::Corruption(
              "query spill", "invalid list result element count");
        }
        const uint64_t element_bytes = structured == 0
            ? sizeof(std::optional<Value>) : sizeof(StructValue);
        if (!AddMeasuredBytes(
                static_cast<uint64_t>(elements) * element_bytes, &bytes)) {
          return Status::QueryMemoryLimit(
              "query spill", "decoded batch size overflow");
        }
        for (uint32_t element = 0; element < elements; ++element) {
          if (structured == 0) {
            const Status measured = MeasureDecodedOptionalValue(
                input, &offset, &bytes, decode_temporary_bytes);
            if (!measured.ok()) return measured;
            continue;
          }
          uint32_t fields = 0;
          if (!GetU32(input, &offset, &fields) ||
              fields > kMaxCompositeElements ||
              !AddMeasuredBytes(
                  static_cast<uint64_t>(fields) * sizeof(StructField),
                  &bytes)) {
            return Status::Corruption(
                "query spill", "invalid structured list field count");
          }
          for (uint32_t field = 0; field < fields; ++field) {
            uint32_t name_length = 0;
            if (!GetU32(input, &offset, &name_length) ||
                name_length > kMaxSpillStringBytes ||
                name_length > input.size() - offset ||
                !AddMeasuredBytes(name_length, &bytes)) {
              return Status::Corruption(
                  "query spill", "invalid structured list field name");
            }
            offset += name_length;
            const Status measured = MeasureDecodedOptionalValue(
                input, &offset, &bytes, decode_temporary_bytes);
            if (!measured.ok()) return measured;
          }
        }
      }
    }
  }
  if (offset != input.size()) {
    return Status::Corruption("query spill", "trailing batch bytes");
  }
  return bytes;
}

StatusOr<ResultBatch> DecodeBatch(
    const std::string& input, const std::shared_ptr<void>& retention) {
  size_t offset = 0;
  uint32_t columns = 0;
  uint32_t rows = 0;
  uint64_t snapshot_seq = 0;
  uint8_t includes_valid_time = 0;
  uint8_t includes_system_time = 0;
  if (!GetU32(input, &offset, &columns) || !GetU32(input, &offset, &rows) ||
      !GetU64(input, &offset, &snapshot_seq) ||
      !GetU8(input, &offset, &includes_valid_time) ||
      !GetU8(input, &offset, &includes_system_time) ||
      columns == 0 || columns > kMaxSpillColumns ||
      rows > kTcypherStandardBatchCapacity ||
      (includes_valid_time > 1 || includes_system_time > 1)) {
    return Status::Corruption("query spill", "invalid result batch header");
  }
  std::vector<std::string> names;
  names.reserve(columns);
  for (uint32_t column = 0; column < columns; ++column) {
    uint32_t length = 0;
    if (!GetU32(input, &offset, &length) || length > kMaxSpillStringBytes ||
        length > input.size() - offset) {
      return Status::Corruption("query spill", "invalid result column name");
    }
    names.push_back(input.substr(offset, length));
    offset += length;
  }
  ColumnBatch batch(rows);
  for (uint32_t column = 0; column < columns; ++column) {
    uint8_t kind = 0;
    if (!GetU8(input, &offset, &kind) || kind > 2) {
      return Status::Corruption("query spill", "invalid result column kind");
    }
    if (kind == 0) {
      std::vector<Value> values;
      std::vector<bool> validity;
      values.reserve(rows);
      validity.reserve(rows);
      for (uint32_t row = 0; row < rows; ++row) {
        std::optional<Value> value;
        const Status decoded = DecodeOptionalValue(input, &offset, &value);
        if (!decoded.ok()) return decoded;
        validity.push_back(value.has_value());
        values.push_back(value.value_or(Value::Binary("")));
      }
      const Status added = batch.AddVector(
          std::make_shared<FlatVector>(
              std::move(values), std::move(validity), retention));
      if (!added.ok()) return added;
    } else if (kind == 1) {
      std::vector<StructValue> values;
      std::vector<bool> validity;
      values.reserve(rows);
      validity.reserve(rows);
      for (uint32_t row = 0; row < rows; ++row) {
        uint8_t present = 0;
        if (!GetU8(input, &offset, &present) || present > 1) {
          return Status::Corruption("query spill", "invalid result struct validity");
        }
        validity.push_back(present == 1);
        StructValue value;
        if (present == 1) {
          const Status decoded = DecodeStructValue(input, &offset, &value);
          if (!decoded.ok()) return decoded;
        }
        values.push_back(std::move(value));
      }
      const Status added = batch.AddVector(
          std::make_shared<StructVector>(
              std::move(values), std::move(validity), retention));
      if (!added.ok()) return added;
    } else {
      std::vector<ListValue> values;
      std::vector<bool> validity;
      values.reserve(rows);
      validity.reserve(rows);
      for (uint32_t row = 0; row < rows; ++row) {
        uint8_t present = 0;
        if (!GetU8(input, &offset, &present) || present > 1) {
          return Status::Corruption("query spill", "invalid result list validity");
        }
        validity.push_back(present == 1);
        ListValue value;
        if (present == 1) {
          const Status decoded = DecodeListValue(input, &offset, &value);
          if (!decoded.ok()) return decoded;
        }
        values.push_back(std::move(value));
      }
      const Status added = batch.AddVector(
          std::make_shared<ListVector>(
              std::move(values), std::move(validity), retention));
      if (!added.ok()) return added;
    }
  }
  if (offset != input.size()) return Status::Corruption("query spill", "trailing batch bytes");
  ResultBatch result(std::move(names), std::move(batch),
                     ResultTemporalMetadata{snapshot_seq, includes_valid_time == 1,
                                            includes_system_time == 1});
  const Status valid = result.Validate();
  if (!valid.ok()) return valid;
  return result;
}

}  // namespace

QuerySpillFile::QuerySpillFile(std::string directory,
                               std::shared_ptr<QueryCancellation> cancellation,
                               std::shared_ptr<ResourceGovernorExtension> resources,
                               std::shared_ptr<QueryMemoryAccount> memory_account,
                               std::function<void(uint64_t)> write_observer)
    : directory_(std::move(directory)), cancellation_(std::move(cancellation)),
      resources_(std::move(resources)),
      memory_account_(std::move(memory_account)),
      write_observer_(std::move(write_observer)) {}

QuerySpillFile::~QuerySpillFile() { Close().IgnoreError(); }

Status QuerySpillFile::CheckCancelled() {
  if (!cancellation_ || !cancellation_->IsCancelled()) return Status::OK();
  Close().IgnoreError();
  return Status::QueryCancelled("query spill", "query cancelled");
}

void QuerySpillFile::ReleaseReadBuffer() {
  if (memory_account_ && read_buffer_reserved_bytes_ != 0) {
    memory_account_->Release(read_buffer_reserved_bytes_);
  }
  read_buffer_reserved_bytes_ = 0;
}

Status QuerySpillFile::Open() {
  if (opened_) return Status::InvalidArgument("query spill", "spill file is already open");
  const Status cancelled = CheckCancelled();
  if (!cancelled.ok()) return cancelled;
  if (directory_.empty()) return Status::InvalidArgument("query spill", "missing spill directory");
  if (resources_ != nullptr) {
    auto acquired = resources_->Acquire(ResourceProfile{0, 0, 1});
    if (!acquired.ok()) return acquired.status();
    descriptor_lease_ = std::move(acquired).ConsumeValueOrDie();
  }
  std::error_code error;
  std::filesystem::create_directories(directory_, error);
  if (error) {
    descriptor_lease_.Release();
    return Status::IOError(directory_, error.message());
  }
  std::string pattern = (std::filesystem::path(directory_) / "cedar-query-spill-XXXXXX").string();
  std::vector<char> writable(pattern.begin(), pattern.end());
  writable.push_back('\0');
  fd_ = ::mkstemp(writable.data());
  if (fd_ < 0) {
    descriptor_lease_.Release();
    return Status::IOError(directory_, std::strerror(errno));
  }
  path_.assign(writable.data());
  if (resources_ != nullptr) {
    auto acquired = resources_->Acquire(ResourceProfile{0, 0, 0, kFileHeaderBytes});
    if (!acquired.ok()) {
      const Status status = acquired.status();
      Close().IgnoreError();
      return status;
    }
    temporary_lease_ = std::move(acquired).ConsumeValueOrDie();
  }
  std::string header;
  PutU32(&header, kSpillMagic);
  PutU32(&header, kSpillVersion);
  const Status written = WriteAll(fd_, header, path_);
  if (!written.ok()) {
    Close().IgnoreError();
    return written;
  }
  bytes_written_ = header.size();
  if (write_observer_) write_observer_(header.size());
  opened_ = true;
  return Status::OK();
}

Status QuerySpillFile::AppendRecord(const std::string& payload) {
  if (!opened_ || fd_ < 0 || reading_) {
    return Status::InvalidArgument(
        "query spill", "spill file is not writable");
  }
  const Status cancelled = CheckCancelled();
  if (!cancelled.ok()) return cancelled;
  if (payload.size() > kMaxSpillRecordBytes) {
    return Status::QueryMemoryLimit(
        "query spill", "operator spill record exceeds spill bound");
  }
  if (payload.size() > std::numeric_limits<uint64_t>::max() - kRecordHeaderBytes) {
    return Status::QueryMemoryLimit("query spill", "spill record size overflow");
  }
  const uint64_t record_bytes = kRecordHeaderBytes + payload.size();
  if (resources_ != nullptr) {
    const Status extended = temporary_lease_.Extend(
        ResourceProfile{0, 0, 0, record_bytes});
    if (!extended.ok()) return extended;
  }
  std::string header;
  PutU32(&header, static_cast<uint32_t>(payload.size()));
  PutU32(&header, crc32c::Value(payload.data(), payload.size()));
  Status written = WriteAll(fd_, header, path_);
  if (written.ok()) written = WriteAll(fd_, payload, path_);
  if (!written.ok()) return written;
  bytes_written_ += record_bytes;
  if (write_observer_) write_observer_(record_bytes);
  return Status::OK();
}

Status QuerySpillFile::Append(const ResultBatch& batch) {
  const auto measured = MeasureEncodedBatch(batch);
  if (!measured.ok()) return measured.status();
  const uint64_t reservation_bytes = measured.ValueOrDie() + 64;
  std::shared_ptr<void> reservation;
  if (memory_account_) {
    const Status reserved = memory_account_->Reserve(reservation_bytes);
    if (!reserved.ok()) return reserved;
    reservation = std::make_shared<QueryMemoryLease>(
        memory_account_, reservation_bytes);
  }
  const auto encoded = EncodeBatch(batch, measured.ValueOrDie());
  if (!encoded.ok()) return encoded.status();
  return AppendRecord(encoded.ValueOrDie());
}

Status QuerySpillFile::Seal() {
  if (!opened_) {
    return Status::InvalidArgument("query spill", "spill file is not open");
  }
  ReleaseReadBuffer();
  if (fd_ < 0) return Status::OK();
  Status status = Status::OK();
  if (::close(fd_) != 0) {
    status = Status::IOError(path_, std::strerror(errno));
  }
  fd_ = -1;
  reading_ = false;
  descriptor_lease_.Release();
  return status;
}

Status QuerySpillFile::Rewind() {
  if (!opened_) return Status::InvalidArgument("query spill", "spill file is not open");
  const Status cancelled = CheckCancelled();
  if (!cancelled.ok()) return cancelled;
  ReleaseReadBuffer();
  if (fd_ < 0) {
    ResourceLease descriptor;
    if (resources_ != nullptr) {
      auto acquired = resources_->Acquire(ResourceProfile{0, 0, 1});
      if (!acquired.ok()) return acquired.status();
      descriptor = std::move(acquired).ConsumeValueOrDie();
    }
    const int reopened = ::open(path_.c_str(), O_RDONLY);
    if (reopened < 0) {
      return Status::IOError(path_, std::strerror(errno));
    }
    fd_ = reopened;
    descriptor_lease_ = std::move(descriptor);
  }
  if (::lseek(fd_, 0, SEEK_SET) < 0) return Status::IOError(path_, std::strerror(errno));
  std::string header;
  bool eof = false;
  const Status read_header = ReadExact(fd_, kFileHeaderBytes, &header, path_, &eof);
  if (!read_header.ok()) return read_header;
  size_t offset = 0;
  uint32_t magic = 0;
  uint32_t version = 0;
  if (eof || !GetU32(header, &offset, &magic) || !GetU32(header, &offset, &version) ||
      magic != kSpillMagic || version != kSpillVersion) {
    return Status::Corruption(path_, "invalid spill file header");
  }
  reading_ = true;
  return Status::OK();
}

Status QuerySpillFile::NextRecord(std::string* record) {
  if (record == nullptr) return Status::InvalidArgument("query spill", "missing spill record output");
  if (!opened_ || !reading_) return Status::InvalidArgument("query spill", "spill file is not readable");
  const Status cancelled = CheckCancelled();
  if (!cancelled.ok()) return cancelled;
  ReleaseReadBuffer();
  std::string header;
  bool eof = false;
  const Status read_header = ReadExact(fd_, kRecordHeaderBytes, &header, path_, &eof);
  if (!read_header.ok()) return read_header;
  if (eof) return Status::NotFound("query spill", "end of spill file");
  size_t offset = 0;
  uint32_t length = 0;
  uint32_t checksum = 0;
  if (!GetU32(header, &offset, &length) || !GetU32(header, &offset, &checksum) ||
      length > kMaxSpillRecordBytes) {
    return Status::Corruption(path_, "invalid spill record header");
  }
  if (memory_account_ && length != 0) {
    const Status reserved = memory_account_->Reserve(length);
    if (!reserved.ok()) return reserved;
    read_buffer_reserved_bytes_ = length;
  }
  const Status read_payload = ReadExact(fd_, length, record, path_, &eof);
  if (!read_payload.ok()) {
    ReleaseReadBuffer();
    return read_payload;
  }
  if (eof || crc32c::Value(record->data(), record->size()) != checksum) {
    ReleaseReadBuffer();
    return Status::Corruption(path_, "spill record checksum mismatch");
  }
  return Status::OK();
}

Status QuerySpillFile::Next(ResultBatch* batch) {
  if (batch == nullptr) return Status::InvalidArgument("query spill", "missing result batch output");
  std::string payload;
  const Status next = NextRecord(&payload);
  if (!next.ok()) return next;
  uint64_t decode_temporary_bytes = 0;
  const auto measured =
      MeasureDecodedBatch(payload, &decode_temporary_bytes);
  if (!measured.ok()) {
    ReleaseReadBuffer();
    return measured.status();
  }
  std::shared_ptr<void> retention;
  std::shared_ptr<QueryMemoryLease> decode_temporary_retention;
  if (memory_account_) {
    uint64_t reserved_bytes = measured.ValueOrDie();
    if (!AddMeasuredBytes(decode_temporary_bytes, &reserved_bytes)) {
      ReleaseReadBuffer();
      return Status::QueryMemoryLimit(
          "query spill", "decoded batch reservation overflow");
    }
    const Status reserved = memory_account_->Reserve(reserved_bytes);
    if (!reserved.ok()) {
      ReleaseReadBuffer();
      return reserved;
    }
    retention = std::make_shared<QueryMemoryLease>(
        memory_account_, measured.ValueOrDie());
    decode_temporary_retention = std::make_shared<QueryMemoryLease>(
        memory_account_, decode_temporary_bytes);
  }
  const auto decoded = DecodeBatch(payload, retention);
  decode_temporary_retention.reset();
  ReleaseReadBuffer();
  if (!decoded.ok()) return decoded.status();
  *batch = decoded.ValueOrDie();
  return Status::OK();
}

Status QuerySpillFile::Close() {
  Status status = Status::OK();
  if (fd_ >= 0 && ::close(fd_) != 0) status = Status::IOError(path_, std::strerror(errno));
  fd_ = -1;
  opened_ = false;
  reading_ = false;
  if (!path_.empty() && ::unlink(path_.c_str()) != 0 && errno != ENOENT && status.ok()) {
    status = Status::IOError(path_, std::strerror(errno));
  }
  path_.clear();
  ReleaseReadBuffer();
  descriptor_lease_.Release();
  temporary_lease_.Release();
  return status;
}

Status SpillResultStream::Next(ResultBatch* batch) {
  if (batch == nullptr) return Status::InvalidArgument("query spill", "missing result batch output");
  if (!spill_) return Status::NotFound("query spill", "end of spill file");
  if (!rewound_) {
    const Status rewind = spill_->Rewind();
    if (!rewind.ok()) {
      terminal_status_ = rewind;
      return rewind;
    }
    rewound_ = true;
  }
  const Status next = spill_->Next(batch);
  if (!next.ok() && !next.IsNotFound()) terminal_status_ = next;
  return next;
}

PartitionedSpillSet::~PartitionedSpillSet() { Close().IgnoreError(); }

Status PartitionedSpillSet::CheckCancelled() {
  if (!cancellation_ || !cancellation_->IsCancelled()) return Status::OK();
  Close().IgnoreError();
  return Status::QueryCancelled("partitioned spill", "query cancelled");
}

Status PartitionedSpillSet::Open() {
  if (opened_) {
    return Status::InvalidArgument(
        "partitioned spill", "spill set is already open");
  }
  if (directory_.empty() || partition_count_ == 0 || partition_count_ > 256) {
    return Status::InvalidArgument(
        "partitioned spill", "invalid directory or partition count");
  }
  const Status cancelled = CheckCancelled();
  if (!cancelled.ok()) return cancelled;
  partitions_.resize(partition_count_);
  has_data_.assign(partition_count_, false);
  opened_ = true;
  return Status::OK();
}

Status PartitionedSpillSet::ValidatePartition(uint32_t partition) const {
  if (!opened_) {
    return Status::InvalidArgument(
        "partitioned spill", "spill set is not open");
  }
  if (partition >= partition_count_) {
    return Status::InvalidArgument(
        "partitioned spill", "partition is out of range");
  }
  return Status::OK();
}

Status PartitionedSpillSet::AppendRecord(
    uint32_t partition, const std::string& record) {
  const Status valid = ValidatePartition(partition);
  if (!valid.ok()) return valid;
  const Status cancelled = CheckCancelled();
  if (!cancelled.ok()) return cancelled;
  if (!partitions_[partition]) {
    partitions_[partition] =
        std::make_unique<QuerySpillFile>(
            directory_, cancellation_, resources_, memory_account_,
            write_observer_);
    const Status opened = partitions_[partition]->Open();
    if (!opened.ok()) {
      partitions_[partition].reset();
      return opened;
    }
  }
  const Status appended = partitions_[partition]->AppendRecord(record);
  if (appended.ok()) has_data_[partition] = true;
  return appended;
}

Status PartitionedSpillSet::Append(
    uint32_t partition, const ResultBatch& batch) {
  const Status valid = ValidatePartition(partition);
  if (!valid.ok()) return valid;
  const Status cancelled = CheckCancelled();
  if (!cancelled.ok()) return cancelled;
  if (!partitions_[partition]) {
    partitions_[partition] =
        std::make_unique<QuerySpillFile>(
            directory_, cancellation_, resources_, memory_account_,
            write_observer_);
    const Status opened = partitions_[partition]->Open();
    if (!opened.ok()) {
      partitions_[partition].reset();
      return opened;
    }
  }
  const Status appended = partitions_[partition]->Append(batch);
  if (appended.ok()) has_data_[partition] = true;
  return appended;
}

Status PartitionedSpillSet::Seal() {
  if (!opened_) {
    return Status::InvalidArgument(
        "partitioned spill", "spill set is not open");
  }
  Status result = Status::OK();
  for (auto& partition : partitions_) {
    if (!partition) continue;
    const Status sealed = partition->Seal();
    if (!sealed.ok() && result.ok()) result = sealed;
  }
  return result;
}

Status PartitionedSpillSet::Seal(uint32_t partition) {
  const Status valid = ValidatePartition(partition);
  if (!valid.ok()) return valid;
  if (!partitions_[partition]) return Status::OK();
  return partitions_[partition]->Seal();
}

Status PartitionedSpillSet::Rewind(uint32_t partition) {
  const Status valid = ValidatePartition(partition);
  if (!valid.ok()) return valid;
  const Status cancelled = CheckCancelled();
  if (!cancelled.ok()) return cancelled;
  if (!has_data_[partition] || !partitions_[partition]) {
    return Status::NotFound("partitioned spill", "partition is empty");
  }
  return partitions_[partition]->Rewind();
}

Status PartitionedSpillSet::NextRecord(
    uint32_t partition, std::string* record) {
  const Status valid = ValidatePartition(partition);
  if (!valid.ok()) return valid;
  const Status cancelled = CheckCancelled();
  if (!cancelled.ok()) return cancelled;
  if (!has_data_[partition] || !partitions_[partition]) {
    return Status::NotFound("partitioned spill", "partition is empty");
  }
  return partitions_[partition]->NextRecord(record);
}

Status PartitionedSpillSet::Next(
    uint32_t partition, ResultBatch* batch) {
  const Status valid = ValidatePartition(partition);
  if (!valid.ok()) return valid;
  const Status cancelled = CheckCancelled();
  if (!cancelled.ok()) return cancelled;
  if (!has_data_[partition] || !partitions_[partition]) {
    return Status::NotFound("partitioned spill", "partition is empty");
  }
  return partitions_[partition]->Next(batch);
}

bool PartitionedSpillSet::HasData(uint32_t partition) const {
  return opened_ && partition < has_data_.size() && has_data_[partition];
}

uint64_t PartitionedSpillSet::bytes_written() const {
  uint64_t total = 0;
  for (const auto& partition : partitions_) {
    if (partition) total += partition->bytes_written();
  }
  return total;
}

Status PartitionedSpillSet::Close() {
  Status result = Status::OK();
  for (auto& partition : partitions_) {
    if (!partition) continue;
    const Status closed = partition->Close();
    if (!closed.ok() && result.ok()) result = closed;
  }
  partitions_.clear();
  has_data_.clear();
  opened_ = false;
  return result;
}

}  // namespace cedar
