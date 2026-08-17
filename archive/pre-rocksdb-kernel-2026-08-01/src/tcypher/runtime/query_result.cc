// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/tcypher/runtime/query_result.h"

#include <algorithm>
#include <cstring>
#include <iterator>
#include <limits>
#include <string_view>

#include "cedar/tcypher/runtime/query_spill.h"

namespace cedar {

struct LifecycleTrackedResultStream::State {
  explicit State(std::unique_ptr<QueryResultStream> stream)
      : input(std::move(stream)) {}

  void Reset() {
    std::lock_guard<std::mutex> lock(mutex);
    input.reset();
  }

  mutable std::mutex mutex;
  std::unique_ptr<QueryResultStream> input;
};

LifecycleTrackedResultStream::LifecycleTrackedResultStream(
    std::unique_ptr<QueryResultStream> input,
    std::shared_ptr<DatabaseQueryRegistration> registration)
    : state_(std::make_shared<State>(std::move(input))),
      registration_(std::move(registration)) {
  if (registration_) {
    std::weak_ptr<State> weak_state = state_;
    registration_->SetCleanup([weak_state] {
      if (const auto state = weak_state.lock()) state->Reset();
    });
  }
}

LifecycleTrackedResultStream::~LifecycleTrackedResultStream() {
  if (state_) state_->Reset();
  registration_.reset();
  state_.reset();
}

Status LifecycleTrackedResultStream::Next(ResultBatch* batch) {
  if (!registration_) {
    terminal_status_ = Status::QueryCancelled(
        "query shutdown", "query registration was released");
    return terminal_status_;
  }
  const Status entered = registration_->BeginCall();
  if (!entered.ok()) {
    terminal_status_ = entered;
    return terminal_status_;
  }
  Status status = Status::InvalidArgument("query result",
                                          "missing result stream");
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->input) status = state_->input->Next(batch);
  }
  registration_->EndCall(!status.ok());
  if (!status.ok() && !status.IsNotFound()) terminal_status_ = status;
  return status;
}

Status LifecycleTrackedResultStream::terminal_status() const {
  if (!terminal_status_.ok()) return terminal_status_;
  if (!registration_) {
    return Status::QueryCancelled("query shutdown",
                                  "query registration was released");
  }
  const Status entered = registration_->BeginCall();
  if (!entered.ok()) return entered;
  Status status = Status::InvalidArgument("query result",
                                          "missing result stream");
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->input) status = state_->input->terminal_status();
  }
  registration_->EndCall(!status.ok());
  return status;
}

QueryOperatorResourceStats
LifecycleTrackedResultStream::operator_resource_stats() const {
  if (!registration_) return {};
  const Status entered = registration_->BeginCall();
  if (!entered.ok()) return {};
  QueryOperatorResourceStats stats;
  {
    std::lock_guard<std::mutex> lock(state_->mutex);
    if (state_->input) stats = state_->input->operator_resource_stats();
  }
  registration_->EndCall(false);
  return stats;
}
namespace {

constexpr uint32_t kGroupedAggregateSpillMagic = 0x31534147U;  // GAS1
constexpr uint32_t kGroupedCollectSpillMagic = 0x32534347U;  // GCS2
constexpr uint32_t kGroupedAggregateSpillPartitions = 16;
constexpr uint32_t kMaxGroupedAggregateSpillValues = 1U << 16;
constexpr uint64_t kMaxGroupedCollectSpillRecordBytes = 64ULL << 20;
constexpr uint64_t kMaxGroupedCollectSpillStringBytes = 16ULL << 20;

bool ResultCellPresent(const ResultValueCell& cell);

void AppendU8(std::string* output, uint8_t value) {
  output->push_back(static_cast<char>(value));
}

void AppendU32(std::string* output, uint32_t value) {
  for (uint32_t shift = 0; shift < 32; shift += 8) {
    output->push_back(static_cast<char>(value >> shift));
  }
}

void AppendU64(std::string* output, uint64_t value) {
  for (uint32_t shift = 0; shift < 64; shift += 8) {
    output->push_back(static_cast<char>(value >> shift));
  }
}

bool ReadU8(const std::string& input, size_t* offset, uint8_t* value) {
  if (*offset >= input.size()) return false;
  *value = static_cast<uint8_t>(input[(*offset)++]);
  return true;
}

bool ReadU32(const std::string& input, size_t* offset, uint32_t* value) {
  if (input.size() - *offset < sizeof(uint32_t)) return false;
  *value = 0;
  for (uint32_t shift = 0; shift < 32; shift += 8) {
    *value |= static_cast<uint32_t>(
        static_cast<uint8_t>(input[(*offset)++])) << shift;
  }
  return true;
}

bool ReadU64(const std::string& input, size_t* offset, uint64_t* value) {
  if (input.size() - *offset < sizeof(uint64_t)) return false;
  *value = 0;
  for (uint32_t shift = 0; shift < 64; shift += 8) {
    *value |= static_cast<uint64_t>(
        static_cast<uint8_t>(input[(*offset)++])) << shift;
  }
  return true;
}

void AppendOptionalScalar(
    std::string* output, const std::optional<Value>& value) {
  AppendU8(output, value.has_value() ? 1 : 0);
  if (!value.has_value()) return;
  const std::string encoded = value->Encode();
  AppendU32(output, static_cast<uint32_t>(encoded.size()));
  output->append(encoded);
}

bool ReadOptionalScalar(
    const std::string& input, size_t* offset, std::optional<Value>* value) {
  uint8_t present = 0;
  if (!ReadU8(input, offset, &present) || present > 1) return false;
  if (present == 0) {
    value->reset();
    return true;
  }
  uint32_t length = 0;
  if (!ReadU32(input, offset, &length) ||
      length > kMaxGroupedCollectSpillStringBytes ||
      length > input.size() - *offset) {
    return false;
  }
  const auto decoded = Value::Decode(input.substr(*offset, length));
  *offset += length;
  if (!decoded.has_value()) return false;
  *value = std::move(*decoded);
  return true;
}

uint64_t AddRetainedBytes(uint64_t left, uint64_t right) {
  return right > std::numeric_limits<uint64_t>::max() - left
      ? std::numeric_limits<uint64_t>::max() : left + right;
}

uint64_t ResultValuePayloadBytes(const Value& value) {
  if (value.type() != PhysicalType::kString &&
      value.type() != PhysicalType::kBinary) {
    return 0;
  }
  return std::get<std::string>(value.data()).capacity();
}

uint64_t StructPayloadBytes(const StructValue& value) {
  uint64_t bytes = static_cast<uint64_t>(value.fields.capacity()) *
      sizeof(StructField);
  for (const StructField& field : value.fields) {
    bytes = AddRetainedBytes(bytes, field.name.capacity());
    if (field.value.has_value()) {
      bytes = AddRetainedBytes(
          bytes, ResultValuePayloadBytes(*field.value));
    }
  }
  return bytes;
}

uint64_t StructRetainedBytes(const StructValue& value) {
  return AddRetainedBytes(sizeof(StructValue), StructPayloadBytes(value));
}

uint64_t ListRetainedBytes(const ListValue& value) {
  uint64_t bytes = sizeof(ListValue);
  if (value.element_kind == ListElementKind::kStruct) {
    bytes = AddRetainedBytes(
        bytes, static_cast<uint64_t>(value.structured_elements.capacity()) *
            sizeof(StructValue));
    for (const StructValue& element : value.structured_elements) {
      bytes = AddRetainedBytes(bytes, StructPayloadBytes(element));
    }
    return bytes;
  }
  bytes = AddRetainedBytes(
      bytes, static_cast<uint64_t>(value.elements.capacity()) *
          sizeof(std::optional<Value>));
  for (const auto& element : value.elements) {
    if (element.has_value()) {
      bytes = AddRetainedBytes(
          bytes, ResultValuePayloadBytes(*element));
    }
  }
  return bytes;
}

uint64_t BatchResultCellRetainedBytes(
    const ColumnBatch& batch, uint32_t column, uint32_t row) {
  if (batch.IsStructured(column)) {
    const StructValue* value = batch.StructRefAt(column, row);
    return value == nullptr ? 0 : StructRetainedBytes(*value);
  }
  if (batch.IsList(column)) {
    const ListValue* value = batch.ListRefAt(column, row);
    return value == nullptr ? 0 : ListRetainedBytes(*value);
  }
  const Value* value = batch.ValueRefAt(column, row);
  return value == nullptr ? 0 : ResultValuePayloadBytes(*value);
}

uint64_t CollectedElementsBytes(
    const ListValue& collected, ResultValueKind kind) {
  uint64_t bytes = 0;
  if (kind == ResultValueKind::kStruct) {
    bytes = AddRetainedBytes(
        bytes,
        static_cast<uint64_t>(collected.structured_elements.capacity()) *
            sizeof(StructValue));
    for (const StructValue& value : collected.structured_elements) {
      bytes = AddRetainedBytes(bytes, StructPayloadBytes(value));
    }
    return bytes;
  }
  bytes = AddRetainedBytes(
      bytes, static_cast<uint64_t>(collected.elements.capacity()) *
          sizeof(std::optional<Value>));
  for (const auto& value : collected.elements) {
    if (value.has_value()) {
      bytes = AddRetainedBytes(bytes, ResultValuePayloadBytes(*value));
    }
  }
  return bytes;
}

uint64_t CollectedPayloadBytes(
    const ListValue& collected, ResultValueKind kind) {
  uint64_t bytes = 0;
  if (kind == ResultValueKind::kStruct) {
    for (const StructValue& value : collected.structured_elements) {
      bytes = AddRetainedBytes(bytes, StructPayloadBytes(value));
    }
    return bytes;
  }
  for (const auto& value : collected.elements) {
    if (value.has_value()) {
      bytes = AddRetainedBytes(bytes, ResultValuePayloadBytes(*value));
    }
  }
  return bytes;
}

uint64_t ResultCellPayloadBytes(const ResultValueCell& cell) {
  if (!ResultCellPresent(cell)) return 0;
  if (cell.kind == ResultValueKind::kScalar) {
    return ResultValuePayloadBytes(*cell.scalar);
  }
  if (cell.kind == ResultValueKind::kStruct) {
    return StructPayloadBytes(*cell.structure);
  }
  const uint64_t retained = ListRetainedBytes(*cell.list);
  return retained < sizeof(ListValue) ? 0 : retained - sizeof(ListValue);
}

uint64_t ResultCellCopyBytes(const ResultValueCell& cell) {
  return AddRetainedBytes(sizeof(ResultValueCell),
                          ResultCellPayloadBytes(cell));
}

uint64_t ResultCellOutputBytes(const ResultValueCell& cell) {
  if (cell.kind == ResultValueKind::kScalar) {
    return AddRetainedBytes(
        sizeof(Value), cell.scalar.has_value()
            ? ResultValuePayloadBytes(*cell.scalar) : 0);
  }
  if (cell.kind == ResultValueKind::kStruct) {
    return cell.structure.has_value()
        ? StructRetainedBytes(*cell.structure) : sizeof(StructValue);
  }
  return cell.list.has_value()
      ? ListRetainedBytes(*cell.list) : sizeof(ListValue);
}

size_t AppendCapacity(size_t size, size_t capacity) {
  if (size < capacity) return capacity;
  if (capacity == 0) return 1;
  if (capacity > std::numeric_limits<size_t>::max() / 2) return size + 1;
  return std::max(size + 1, capacity * 2);
}

template <typename T>
Status ReserveVectorRelocation(
    std::vector<T>* values, size_t requested_capacity,
    const std::shared_ptr<QueryMemoryAccount>& memory_account,
    uint64_t* retained_bytes, uint64_t* memory_peak_bytes,
    const char* component) {
  if (values == nullptr || retained_bytes == nullptr ||
      requested_capacity <= values->capacity()) {
    return Status::OK();
  }
  if (requested_capacity >
      std::numeric_limits<uint64_t>::max() / sizeof(T)) {
    return Status::QueryMemoryLimit(component, "vector capacity overflow");
  }
  const uint64_t old_capacity_bytes =
      static_cast<uint64_t>(values->capacity()) * sizeof(T);
  const uint64_t requested_bytes =
      static_cast<uint64_t>(requested_capacity) * sizeof(T);
  if (memory_account) {
    const Status reserved = memory_account->Reserve(requested_bytes);
    if (!reserved.ok()) return reserved;
    *retained_bytes = AddRetainedBytes(*retained_bytes, requested_bytes);
    if (memory_peak_bytes != nullptr) {
      *memory_peak_bytes = std::max(
          *memory_peak_bytes, memory_account->used_bytes());
    }
  }
  values->reserve(requested_capacity);
  if (values->capacity() >
      std::numeric_limits<uint64_t>::max() / sizeof(T)) {
    return Status::QueryMemoryLimit(component, "vector capacity overflow");
  }
  const uint64_t observed_bytes =
      static_cast<uint64_t>(values->capacity()) * sizeof(T);
  if (memory_account && observed_bytes > requested_bytes) {
    const uint64_t additional = observed_bytes - requested_bytes;
    const Status reserved = memory_account->Reserve(additional);
    if (!reserved.ok()) return reserved;
    *retained_bytes = AddRetainedBytes(*retained_bytes, additional);
    if (memory_peak_bytes != nullptr) {
      *memory_peak_bytes = std::max(
          *memory_peak_bytes, memory_account->used_bytes());
    }
  }
  if (memory_account && old_capacity_bytes != 0) {
    memory_account->Release(old_capacity_bytes);
    *retained_bytes = old_capacity_bytes > *retained_bytes
        ? 0 : *retained_bytes - old_capacity_bytes;
  }
  return Status::OK();
}

Status AddGroupedCollectSpillBytes(uint64_t additional, uint64_t* bytes) {
  if (bytes == nullptr || *bytes > kMaxGroupedCollectSpillRecordBytes ||
      additional > kMaxGroupedCollectSpillRecordBytes - *bytes) {
    return Status::QueryMemoryLimit(
        "grouped collect spill", "spill record exceeds size bound");
  }
  *bytes += additional;
  return Status::OK();
}

Status MeasureValueForGroupedCollectSpill(
    const std::optional<Value>& value, uint64_t* bytes,
    uint64_t* max_encode_temporary_bytes = nullptr) {
  Status status = AddGroupedCollectSpillBytes(1, bytes);
  if (!status.ok() || !value.has_value()) return status;
  uint64_t encoded = 1;
  switch (value->type()) {
    case PhysicalType::kBool: encoded += 1; break;
    case PhysicalType::kInt32:
    case PhysicalType::kFloat32: encoded += 4; break;
    case PhysicalType::kInt64:
    case PhysicalType::kFloat64:
    case PhysicalType::kTimestamp64: encoded += 8; break;
    case PhysicalType::kString:
    case PhysicalType::kBinary: {
      const uint64_t payload = std::get<std::string>(value->data()).size();
      if (payload > kMaxGroupedCollectSpillStringBytes) {
        return Status::InvalidArgument(
            "grouped collect spill", "value exceeds string bound");
      }
      encoded += sizeof(uint32_t) + payload;
      break;
    }
  }
  if (max_encode_temporary_bytes != nullptr) {
    *max_encode_temporary_bytes = std::max(
        *max_encode_temporary_bytes, encoded);
  }
  return AddGroupedCollectSpillBytes(sizeof(uint32_t) + encoded, bytes);
}

Status MeasureStructForGroupedCollectSpill(
    const StructValue& value, uint64_t* bytes,
    uint64_t* max_encode_temporary_bytes = nullptr) {
  if (value.fields.size() > kMaxGroupedAggregateSpillValues) {
    return Status::InvalidArgument(
        "grouped collect spill", "struct exceeds field bound");
  }
  Status status = AddGroupedCollectSpillBytes(sizeof(uint32_t), bytes);
  if (!status.ok()) return status;
  for (const StructField& field : value.fields) {
    if (field.name.size() > kMaxGroupedCollectSpillStringBytes) {
      return Status::InvalidArgument(
          "grouped collect spill", "field name exceeds string bound");
    }
    status = AddGroupedCollectSpillBytes(
        sizeof(uint32_t) + field.name.size(), bytes);
    if (!status.ok()) return status;
    status = MeasureValueForGroupedCollectSpill(
        field.value, bytes, max_encode_temporary_bytes);
    if (!status.ok()) return status;
  }
  return Status::OK();
}

Status MeasureListForGroupedCollectSpill(
    const ListValue& value, uint64_t* bytes,
    uint64_t* max_encode_temporary_bytes = nullptr) {
  if (!value.IsConsistent()) {
    return Status::InvalidArgument(
        "grouped collect spill", "list element kind is inconsistent");
  }
  const bool structured = value.element_kind == ListElementKind::kStruct;
  const size_t count = structured
      ? value.structured_elements.size() : value.elements.size();
  if (count > kMaxGroupedAggregateSpillValues) {
    return Status::InvalidArgument(
        "grouped collect spill", "list exceeds element bound");
  }
  Status status = AddGroupedCollectSpillBytes(
      1 + sizeof(uint32_t), bytes);
  if (!status.ok()) return status;
  if (structured) {
    for (const StructValue& element : value.structured_elements) {
      status = MeasureStructForGroupedCollectSpill(
          element, bytes, max_encode_temporary_bytes);
      if (!status.ok()) return status;
    }
    return Status::OK();
  }
  for (const auto& element : value.elements) {
    status = MeasureValueForGroupedCollectSpill(
        element, bytes, max_encode_temporary_bytes);
    if (!status.ok()) return status;
  }
  return Status::OK();
}

Status MeasureCellForGroupedCollectSpill(
    const ResultValueCell& cell, uint64_t* bytes,
    uint64_t* max_encode_temporary_bytes = nullptr) {
  Status status = AddGroupedCollectSpillBytes(2, bytes);
  if (!status.ok() || !ResultCellPresent(cell)) return status;
  if (cell.kind == ResultValueKind::kScalar) {
    return MeasureValueForGroupedCollectSpill(
        cell.scalar, bytes, max_encode_temporary_bytes);
  }
  if (cell.kind == ResultValueKind::kStruct) {
    return MeasureStructForGroupedCollectSpill(
        *cell.structure, bytes, max_encode_temporary_bytes);
  }
  return MeasureListForGroupedCollectSpill(
      *cell.list, bytes, max_encode_temporary_bytes);
}

bool MeasureEncodedOptionalValue(
    const std::string& input, size_t* offset, uint64_t* bytes,
    uint64_t* max_decode_temporary_bytes = nullptr) {
  uint8_t present = 0;
  if (!ReadU8(input, offset, &present) || present > 1) return false;
  *bytes = AddRetainedBytes(*bytes, 1);
  if (present == 0) return true;
  uint32_t length = 0;
  if (!ReadU32(input, offset, &length) ||
      length > kMaxGroupedCollectSpillStringBytes ||
      length > input.size() - *offset) {
    return false;
  }
  *bytes = AddRetainedBytes(*bytes, sizeof(uint32_t) + length);
  if (max_decode_temporary_bytes != nullptr) {
    *max_decode_temporary_bytes = std::max<uint64_t>(
        *max_decode_temporary_bytes, length);
  }
  *offset += length;
  return true;
}

bool MeasureEncodedResultCell(
    const std::string& input, size_t* offset, uint64_t* bytes,
    ResultValueKind* measured_kind = nullptr, bool* measured_present = nullptr,
    uint64_t* max_decode_temporary_bytes = nullptr) {
  uint8_t kind = 0;
  uint8_t present = 0;
  if (!ReadU8(input, offset, &kind) ||
      kind > static_cast<uint8_t>(ResultValueKind::kList) ||
      !ReadU8(input, offset, &present) || present > 1) {
    return false;
  }
  const ResultValueKind result_kind = static_cast<ResultValueKind>(kind);
  if (measured_kind != nullptr) *measured_kind = result_kind;
  if (measured_present != nullptr) *measured_present = present == 1;
  *bytes = AddRetainedBytes(*bytes, 2);
  if (present == 0) return true;
  if (result_kind == ResultValueKind::kScalar) {
    return MeasureEncodedOptionalValue(
        input, offset, bytes, max_decode_temporary_bytes);
  }
  if (result_kind == ResultValueKind::kStruct) {
    uint32_t fields = 0;
    if (!ReadU32(input, offset, &fields) ||
        fields > kMaxGroupedAggregateSpillValues) {
      return false;
    }
    *bytes = AddRetainedBytes(
        *bytes, sizeof(StructValue) +
            static_cast<uint64_t>(fields) * sizeof(StructField));
    for (uint32_t field = 0; field < fields; ++field) {
      uint32_t name_length = 0;
      if (!ReadU32(input, offset, &name_length) ||
          name_length > kMaxGroupedCollectSpillStringBytes ||
          name_length > input.size() - *offset) {
        return false;
      }
      *bytes = AddRetainedBytes(*bytes, name_length);
      *offset += name_length;
      if (!MeasureEncodedOptionalValue(
              input, offset, bytes, max_decode_temporary_bytes)) return false;
    }
    return true;
  }
  uint8_t structured = 0;
  uint32_t count = 0;
  if (!ReadU8(input, offset, &structured) || structured > 1 ||
      !ReadU32(input, offset, &count) ||
      count > kMaxGroupedAggregateSpillValues) {
    return false;
  }
  *bytes = AddRetainedBytes(*bytes, sizeof(ListValue));
  if (structured == 0) {
    *bytes = AddRetainedBytes(
        *bytes, static_cast<uint64_t>(count) * sizeof(std::optional<Value>));
    for (uint32_t element = 0; element < count; ++element) {
      if (!MeasureEncodedOptionalValue(
              input, offset, bytes, max_decode_temporary_bytes)) return false;
    }
    return true;
  }
  *bytes = AddRetainedBytes(
      *bytes, static_cast<uint64_t>(count) * sizeof(StructValue));
  for (uint32_t element = 0; element < count; ++element) {
    uint32_t fields = 0;
    if (!ReadU32(input, offset, &fields) ||
        fields > kMaxGroupedAggregateSpillValues) {
      return false;
    }
    *bytes = AddRetainedBytes(
        *bytes, static_cast<uint64_t>(fields) * sizeof(StructField));
    for (uint32_t field = 0; field < fields; ++field) {
      uint32_t name_length = 0;
      if (!ReadU32(input, offset, &name_length) ||
          name_length > kMaxGroupedCollectSpillStringBytes ||
          name_length > input.size() - *offset) {
        return false;
      }
      *bytes = AddRetainedBytes(*bytes, name_length);
      *offset += name_length;
      if (!MeasureEncodedOptionalValue(
              input, offset, bytes, max_decode_temporary_bytes)) return false;
    }
  }
  return true;
}

uint64_t StableResultKeyHash(std::string_view key) {
  uint64_t hash = 1469598103934665603ULL;
  for (unsigned char byte : key) {
    hash ^= byte;
    hash *= 1099511628211ULL;
  }
  return hash;
}

Status ResultStreamTerminalAtEnd(const QueryResultStream* input) {
  return input == nullptr
      ? Status::InvalidArgument("result stream", "missing input stream")
      : input->terminal_status();
}

Status SliceResultBatch(const ResultBatch& source, uint32_t start, uint32_t rows,
                        ResultBatch* output) {
  if (output == nullptr || start > source.batch().row_count() ||
      rows > source.batch().row_count() - start) {
    return Status::InvalidArgument("result stream", "invalid result batch slice");
  }
  ColumnBatch sliced(rows);
  for (uint32_t column = 0; column < source.batch().column_count(); ++column) {
    const auto parent = source.batch().VectorAt(column);
    const Status added = sliced.AddVector(
        std::make_shared<SliceVector>(parent, start, rows));
    if (!added.ok()) return added;
  }
  *output = ResultBatch(source.column_names(), std::move(sliced), source.temporal_metadata());
  return output->Validate();
}

ResultValueKind ResultColumnKind(const ColumnBatch& batch, uint32_t column);
ResultValueCell ResultCellAt(
    const ColumnBatch& batch, uint32_t column, uint32_t row);
uint64_t ResultRowsBytes(
    const std::vector<std::vector<ResultValueCell>>& rows,
    size_t start, size_t end);
uint64_t ResultRowBytes(const std::vector<ResultValueCell>& row);

Status SelectResultRows(const ResultBatch& source,
                        const std::vector<uint32_t>& rows,
                        ResultBatch* output,
                        const std::shared_ptr<QueryMemoryAccount>& memory_account = nullptr) {
  if (output == nullptr) return Status::InvalidArgument("result stream", "missing batch output");
  std::vector<std::vector<ResultValueCell>> selected_rows;
  selected_rows.reserve(rows.size());
  std::vector<ResultValueKind> kinds;
  kinds.reserve(source.batch().column_count());
  for (uint32_t column = 0; column < source.batch().column_count(); ++column) {
    kinds.push_back(ResultColumnKind(source.batch(), column));
  }
  for (uint32_t row : rows) {
    if (row >= source.batch().row_count()) {
      return Status::InvalidArgument("result stream", "selected row is absent");
    }
    std::vector<ResultValueCell> values;
    values.reserve(source.batch().column_count());
    for (uint32_t column = 0; column < source.batch().column_count(); ++column) {
      values.push_back(ResultCellAt(source.batch(), column, row));
    }
    selected_rows.push_back(std::move(values));
  }
  auto lease = std::make_shared<QueryMemoryLease>(memory_account, 0);
  Status status = lease->ReserveAdditional(
      ResultRowsBytes(selected_rows, 0, selected_rows.size()));
  if (!status.ok()) return status;
  ColumnBatch selected(static_cast<uint32_t>(rows.size()));
  for (uint32_t column = 0; column < source.batch().column_count(); ++column) {
    if (source.batch().IsList(column)) {
      std::vector<ListValue> values;
      std::vector<bool> validity;
      values.reserve(rows.size());
      validity.reserve(rows.size());
      for (auto& row : selected_rows) {
        auto& value = row[column].list;
        validity.push_back(value.has_value());
        values.push_back(value.has_value() ? std::move(*value) : ListValue{});
      }
      const Status added = selected.AddVector(
          std::make_shared<ListVector>(
              std::move(values), std::move(validity), lease));
      if (!added.ok()) return added;
      continue;
    }
    if (source.batch().IsStructured(column)) {
      std::vector<StructValue> values;
      std::vector<bool> validity;
      values.reserve(rows.size());
      validity.reserve(rows.size());
      for (auto& row : selected_rows) {
        auto& value = row[column].structure;
        validity.push_back(value.has_value());
        values.push_back(value.has_value() ? std::move(*value) : StructValue{});
      }
      const Status added = selected.AddVector(
          std::make_shared<StructVector>(
              std::move(values), std::move(validity), lease));
      if (!added.ok()) return added;
      continue;
    }
    std::vector<Value> values;
    std::vector<bool> validity;
    values.reserve(rows.size());
    validity.reserve(rows.size());
    for (auto& row : selected_rows) {
      auto& value = row[column].scalar;
      validity.push_back(value.has_value());
      values.push_back(value.has_value() ? std::move(*value) : Value::Binary(""));
    }
    const Status added = selected.AddVector(
        std::make_shared<FlatVector>(
            std::move(values), std::move(validity), lease));
    if (!added.ok()) return added;
  }
  *output = ResultBatch(source.column_names(), std::move(selected), source.temporal_metadata());
  return output->Validate();
}

uint32_t Float32TotalOrderKey(float value) {
  static_assert(std::numeric_limits<float>::is_iec559,
                "Cedar requires IEEE 754 Float32");
  uint32_t bits = 0;
  std::memcpy(&bits, &value, sizeof(bits));
  constexpr uint32_t kSign = uint32_t{1} << 31U;
  return (bits & kSign) != 0 ? ~bits : bits | kSign;
}

uint64_t Float64TotalOrderKey(double value) {
  static_assert(std::numeric_limits<double>::is_iec559,
                "Cedar requires IEEE 754 Float64");
  uint64_t bits = 0;
  std::memcpy(&bits, &value, sizeof(bits));
  constexpr uint64_t kSign = uint64_t{1} << 63U;
  return (bits & kSign) != 0 ? ~bits : bits | kSign;
}

int CompareResultValues(const Value& left, const Value& right) {
  if (left.type() != right.type()) {
    return static_cast<uint8_t>(left.type()) < static_cast<uint8_t>(right.type()) ? -1 : 1;
  }
  switch (left.type()) {
    case PhysicalType::kBool:
      return std::get<bool>(left.data()) == std::get<bool>(right.data()) ? 0
             : std::get<bool>(left.data()) ? 1 : -1;
    case PhysicalType::kInt32:
      return std::get<int32_t>(left.data()) == std::get<int32_t>(right.data()) ? 0
             : std::get<int32_t>(left.data()) < std::get<int32_t>(right.data()) ? -1 : 1;
    case PhysicalType::kInt64:
      return std::get<int64_t>(left.data()) == std::get<int64_t>(right.data()) ? 0
             : std::get<int64_t>(left.data()) < std::get<int64_t>(right.data()) ? -1 : 1;
    case PhysicalType::kFloat32: {
      const uint32_t left_key =
          Float32TotalOrderKey(std::get<float>(left.data()));
      const uint32_t right_key =
          Float32TotalOrderKey(std::get<float>(right.data()));
      return left_key == right_key ? 0 : left_key < right_key ? -1 : 1;
    }
    case PhysicalType::kFloat64: {
      const uint64_t left_key =
          Float64TotalOrderKey(std::get<double>(left.data()));
      const uint64_t right_key =
          Float64TotalOrderKey(std::get<double>(right.data()));
      return left_key == right_key ? 0 : left_key < right_key ? -1 : 1;
    }
    case PhysicalType::kTimestamp64:
      return std::get<uint64_t>(left.data()) == std::get<uint64_t>(right.data()) ? 0
             : std::get<uint64_t>(left.data()) < std::get<uint64_t>(right.data()) ? -1 : 1;
    case PhysicalType::kString:
    case PhysicalType::kBinary:
      return std::get<std::string>(left.data()).compare(std::get<std::string>(right.data()));
  }
  return 0;
}

void AppendResultU32(std::string* output, uint32_t value) {
  for (uint32_t shift = 0; shift < 32; shift += 8) {
    output->push_back(static_cast<char>(value >> shift));
  }
}

void AppendResultValue(std::string* output, const std::optional<Value>& value) {
  output->push_back(value.has_value() ? '\x01' : '\x00');
  if (!value.has_value()) return;
  const std::string encoded = value->Encode();
  AppendResultU32(output, static_cast<uint32_t>(encoded.size()));
  output->append(encoded);
}

bool ReadResultValue(const std::string& input, size_t* offset,
                     std::optional<Value>* value) {
  uint8_t present = 0;
  if (!ReadU8(input, offset, &present) || present > 1) return false;
  if (present == 0) {
    value->reset();
    return true;
  }
  uint32_t length = 0;
  if (!ReadU32(input, offset, &length) ||
      length > kMaxGroupedCollectSpillStringBytes ||
      length > input.size() - *offset) {
    return false;
  }
  const auto decoded = Value::Decode(input.substr(*offset, length));
  *offset += length;
  if (!decoded.has_value()) return false;
  *value = std::move(*decoded);
  return true;
}

ResultValueKind ResultColumnKind(const ColumnBatch& batch, uint32_t column) {
  if (batch.IsList(column)) return ResultValueKind::kList;
  if (batch.IsStructured(column)) return ResultValueKind::kStruct;
  return ResultValueKind::kScalar;
}

ResultValueCell ResultCellAt(const ColumnBatch& batch, uint32_t column, uint32_t row) {
  ResultValueCell cell;
  cell.kind = ResultColumnKind(batch, column);
  switch (cell.kind) {
    case ResultValueKind::kScalar: cell.scalar = batch.ValueAt(column, row); break;
    case ResultValueKind::kStruct: cell.structure = batch.StructAt(column, row); break;
    case ResultValueKind::kList: cell.list = batch.ListAt(column, row); break;
  }
  return cell;
}

bool ResultCellPresent(const ResultValueCell& cell) {
  switch (cell.kind) {
    case ResultValueKind::kScalar: return cell.scalar.has_value();
    case ResultValueKind::kStruct: return cell.structure.has_value();
    case ResultValueKind::kList: return cell.list.has_value();
  }
  return false;
}

void AppendResultListValue(std::string* output, const ListValue& value) {
  const bool structured = value.element_kind == ListElementKind::kStruct;
  output->push_back(structured ? '\x01' : '\x00');
  AppendResultU32(output, static_cast<uint32_t>(
      structured ? value.structured_elements.size() : value.elements.size()));
  if (!structured) {
    for (const auto& element : value.elements) AppendResultValue(output, element);
    return;
  }
  for (const StructValue& element : value.structured_elements) {
    AppendResultU32(output, static_cast<uint32_t>(element.fields.size()));
    for (const StructField& field : element.fields) {
      AppendResultU32(output, static_cast<uint32_t>(field.name.size()));
      output->append(field.name);
      AppendResultValue(output, field.value);
    }
  }
}

void AppendResultCell(std::string* output, const ResultValueCell& cell) {
  output->push_back(static_cast<char>(cell.kind));
  output->push_back(ResultCellPresent(cell) ? '\x01' : '\x00');
  if (!ResultCellPresent(cell)) return;
  if (cell.kind == ResultValueKind::kScalar) {
    AppendResultValue(output, cell.scalar);
    return;
  }
  if (cell.kind == ResultValueKind::kStruct) {
    const StructValue& value = *cell.structure;
    AppendResultU32(output, static_cast<uint32_t>(value.fields.size()));
    for (const StructField& field : value.fields) {
      AppendResultU32(output, static_cast<uint32_t>(field.name.size()));
      output->append(field.name);
      AppendResultValue(output, field.value);
    }
    return;
  }
  AppendResultListValue(output, *cell.list);
}

bool ReadResultCell(const std::string& input, size_t* offset,
                    ResultValueCell* cell) {
  uint8_t kind = 0;
  uint8_t present = 0;
  if (!ReadU8(input, offset, &kind) ||
      kind > static_cast<uint8_t>(ResultValueKind::kList) ||
      !ReadU8(input, offset, &present) || present > 1) {
    return false;
  }
  *cell = ResultValueCell{};
  cell->kind = static_cast<ResultValueKind>(kind);
  if (present == 0) return true;
  if (cell->kind == ResultValueKind::kScalar) {
    return ReadResultValue(input, offset, &cell->scalar) &&
        cell->scalar.has_value();
  }
  if (cell->kind == ResultValueKind::kStruct) {
    uint32_t count = 0;
    if (!ReadU32(input, offset, &count) ||
        count > kMaxGroupedAggregateSpillValues) {
      return false;
    }
    StructValue value;
    value.fields.reserve(count);
    for (uint32_t index = 0; index < count; ++index) {
      uint32_t name_length = 0;
      if (!ReadU32(input, offset, &name_length) ||
          name_length > kMaxGroupedCollectSpillStringBytes ||
          name_length > input.size() - *offset) {
        return false;
      }
      StructField field;
      field.name = input.substr(*offset, name_length);
      *offset += name_length;
      if (!ReadResultValue(input, offset, &field.value)) return false;
      value.fields.push_back(std::move(field));
    }
    cell->structure = std::move(value);
    return true;
  }
  uint8_t structured = 0;
  uint32_t count = 0;
  if (!ReadU8(input, offset, &structured) || structured > 1 ||
      !ReadU32(input, offset, &count) ||
      count > kMaxGroupedAggregateSpillValues) {
    return false;
  }
  ListValue value;
  value.element_kind = structured == 0
      ? ListElementKind::kScalar : ListElementKind::kStruct;
  if (structured == 0) {
    value.elements.reserve(count);
    for (uint32_t index = 0; index < count; ++index) {
      std::optional<Value> element;
      if (!ReadResultValue(input, offset, &element)) return false;
      value.elements.push_back(std::move(element));
    }
  } else {
    value.structured_elements.reserve(count);
    for (uint32_t index = 0; index < count; ++index) {
      uint32_t fields = 0;
      if (!ReadU32(input, offset, &fields) ||
          fields > kMaxGroupedAggregateSpillValues) return false;
      StructValue element;
      element.fields.reserve(fields);
      for (uint32_t field_index = 0; field_index < fields; ++field_index) {
        uint32_t name_length = 0;
        if (!ReadU32(input, offset, &name_length) ||
            name_length > kMaxGroupedCollectSpillStringBytes ||
            name_length > input.size() - *offset) return false;
        StructField field;
        field.name = input.substr(*offset, name_length);
        *offset += name_length;
        if (!ReadResultValue(input, offset, &field.value)) return false;
        element.fields.push_back(std::move(field));
      }
      value.structured_elements.push_back(std::move(element));
    }
  }
  cell->list = std::move(value);
  return true;
}

std::string ResultRowKey(const std::vector<ResultValueCell>& row) {
  std::string key;
  for (const ResultValueCell& cell : row) AppendResultCell(&key, cell);
  return key;
}

int CompareOptionalResultValues(const std::optional<Value>& left,
                                const std::optional<Value>& right) {
  if (!left.has_value() || !right.has_value()) {
    if (!left.has_value() && !right.has_value()) return 0;
    return left.has_value() ? 1 : -1;
  }
  return CompareResultValues(*left, *right);
}

int CompareStructValues(const StructValue& left, const StructValue& right) {
  const size_t count = std::min(left.fields.size(), right.fields.size());
  for (size_t index = 0; index < count; ++index) {
    const int name = left.fields[index].name.compare(right.fields[index].name);
    if (name != 0) return name;
    const int value = CompareOptionalResultValues(
        left.fields[index].value, right.fields[index].value);
    if (value != 0) return value;
  }
  return left.fields.size() == right.fields.size() ? 0
      : left.fields.size() < right.fields.size() ? -1 : 1;
}

int CompareResultCells(const ResultValueCell& left, const ResultValueCell& right) {
  if (left.kind != right.kind) {
    return static_cast<uint8_t>(left.kind) < static_cast<uint8_t>(right.kind) ? -1 : 1;
  }
  const bool left_present = ResultCellPresent(left);
  const bool right_present = ResultCellPresent(right);
  if (!left_present || !right_present) {
    if (left_present == right_present) return 0;
    return left_present ? 1 : -1;
  }
  if (left.kind == ResultValueKind::kScalar) {
    return CompareResultValues(*left.scalar, *right.scalar);
  }
  if (left.kind == ResultValueKind::kStruct) {
    return CompareStructValues(*left.structure, *right.structure);
  }
  if (left.list->element_kind != right.list->element_kind) {
    return left.list->element_kind == ListElementKind::kScalar ? -1 : 1;
  }
  const bool left_structured =
      left.list->element_kind == ListElementKind::kStruct;
  if (left_structured) {
    const auto& left_elements = left.list->structured_elements;
    const auto& right_elements = right.list->structured_elements;
    const size_t count = std::min(left_elements.size(), right_elements.size());
    for (size_t index = 0; index < count; ++index) {
      const int value = CompareStructValues(left_elements[index], right_elements[index]);
      if (value != 0) return value;
    }
    return left_elements.size() == right_elements.size() ? 0
        : left_elements.size() < right_elements.size() ? -1 : 1;
  }
  const auto& left_elements = left.list->elements;
  const auto& right_elements = right.list->elements;
  const size_t count = std::min(left_elements.size(), right_elements.size());
  for (size_t index = 0; index < count; ++index) {
    const int value = CompareOptionalResultValues(left_elements[index], right_elements[index]);
    if (value != 0) return value;
  }
  return left_elements.size() == right_elements.size() ? 0
      : left_elements.size() < right_elements.size() ? -1 : 1;
}

uint64_t ResultCellBytes(const ResultValueCell& cell) {
  std::string encoded;
  AppendResultCell(&encoded, cell);
  if (cell.kind == ResultValueKind::kStruct && cell.structure.has_value()) {
    return sizeof(StructValue) + encoded.size();
  }
  if (cell.kind == ResultValueKind::kList && cell.list.has_value()) {
    return sizeof(ListValue) + encoded.size();
  }
  return encoded.size();
}

uint64_t ResultRowBytes(const std::vector<ResultValueCell>& row) {
  uint64_t bytes = sizeof(std::vector<ResultValueCell>) +
      static_cast<uint64_t>(row.size()) * sizeof(ResultValueCell);
  for (const ResultValueCell& cell : row) bytes += ResultCellBytes(cell);
  return bytes;
}

uint64_t ResultRowsBytes(
    const std::vector<std::vector<ResultValueCell>>& rows,
    size_t start, size_t end) {
  uint64_t bytes = 0;
  for (size_t row = start; row < end; ++row) {
    const uint64_t row_bytes = ResultRowBytes(rows[row]);
    if (row_bytes > std::numeric_limits<uint64_t>::max() - bytes) {
      return std::numeric_limits<uint64_t>::max();
    }
    bytes += row_bytes;
  }
  return bytes;
}

Status BuildResultBatch(const std::vector<std::string>& names,
                        const std::vector<ResultValueKind>& kinds,
                        const std::vector<std::vector<ResultValueCell>>& rows,
                        size_t start, size_t end,
                        const ResultTemporalMetadata& metadata,
                        ResultBatch* output,
                        const std::shared_ptr<void>& retention = nullptr) {
  ColumnBatch batch(static_cast<uint32_t>(end - start));
  for (size_t column = 0; column < names.size(); ++column) {
    if (kinds[column] == ResultValueKind::kScalar) {
      std::vector<Value> values;
      std::vector<bool> validity;
      for (size_t row = start; row < end; ++row) {
        validity.push_back(rows[row][column].scalar.has_value());
        values.push_back(rows[row][column].scalar.value_or(Value::Binary("")));
      }
      const Status added = batch.AddVector(
          std::make_shared<FlatVector>(
              std::move(values), std::move(validity), retention));
      if (!added.ok()) return added;
    } else if (kinds[column] == ResultValueKind::kStruct) {
      std::vector<StructValue> values;
      std::vector<bool> validity;
      for (size_t row = start; row < end; ++row) {
        validity.push_back(rows[row][column].structure.has_value());
        values.push_back(rows[row][column].structure.value_or(StructValue{}));
      }
      const Status added = batch.AddVector(
          std::make_shared<StructVector>(
              std::move(values), std::move(validity), retention));
      if (!added.ok()) return added;
    } else {
      std::vector<ListValue> values;
      std::vector<bool> validity;
      for (size_t row = start; row < end; ++row) {
        validity.push_back(rows[row][column].list.has_value());
        values.push_back(rows[row][column].list.value_or(ListValue{}));
      }
      const Status added = batch.AddVector(
          std::make_shared<ListVector>(
              std::move(values), std::move(validity), retention));
      if (!added.ok()) return added;
    }
  }
  *output = ResultBatch(names, std::move(batch), metadata);
  return output->Validate();
}

Status BuildResultBatchMoving(
    const std::vector<std::string>& names,
    const std::vector<ResultValueKind>& kinds,
    std::vector<std::vector<ResultValueCell>>* rows,
    const ResultTemporalMetadata& metadata, ResultBatch* output,
    const std::shared_ptr<void>& retention) {
  if (rows == nullptr) {
    return Status::InvalidArgument("result stream", "missing result rows");
  }
  ColumnBatch batch(static_cast<uint32_t>(rows->size()));
  for (size_t column = 0; column < names.size(); ++column) {
    if (kinds[column] == ResultValueKind::kScalar) {
      std::vector<Value> values;
      std::vector<bool> validity;
      values.reserve(rows->size());
      validity.reserve(rows->size());
      for (auto& row : *rows) {
        validity.push_back(row[column].scalar.has_value());
        values.push_back(row[column].scalar.has_value()
            ? std::move(*row[column].scalar) : Value::Binary(""));
      }
      const Status added = batch.AddVector(std::make_shared<FlatVector>(
          std::move(values), std::move(validity), retention));
      if (!added.ok()) return added;
    } else if (kinds[column] == ResultValueKind::kStruct) {
      std::vector<StructValue> values;
      std::vector<bool> validity;
      values.reserve(rows->size());
      validity.reserve(rows->size());
      for (auto& row : *rows) {
        validity.push_back(row[column].structure.has_value());
        values.push_back(row[column].structure.has_value()
            ? std::move(*row[column].structure) : StructValue{});
      }
      const Status added = batch.AddVector(std::make_shared<StructVector>(
          std::move(values), std::move(validity), retention));
      if (!added.ok()) return added;
    } else {
      std::vector<ListValue> values;
      std::vector<bool> validity;
      values.reserve(rows->size());
      validity.reserve(rows->size());
      for (auto& row : *rows) {
        validity.push_back(row[column].list.has_value());
        values.push_back(row[column].list.has_value()
            ? std::move(*row[column].list) : ListValue{});
      }
      const Status added = batch.AddVector(std::make_shared<ListVector>(
          std::move(values), std::move(validity), retention));
      if (!added.ok()) return added;
    }
  }
  *output = ResultBatch(names, std::move(batch), metadata);
  return output->Validate();
}

bool GroupValuesLess(const std::vector<std::optional<Value>>& left,
                     const std::vector<std::optional<Value>>& right) {
  for (size_t index = 0; index < left.size(); ++index) {
    if (!left[index].has_value() || !right[index].has_value()) {
      if (!left[index].has_value() && !right[index].has_value()) continue;
      return !left[index].has_value();
    }
    const int comparison = CompareResultValues(*left[index], *right[index]);
    if (comparison != 0) return comparison < 0;
  }
  return false;
}

bool GroupCellsLess(const std::vector<ResultValueCell>& left,
                    const std::vector<ResultValueCell>& right) {
  const size_t count = std::min(left.size(), right.size());
  for (size_t index = 0; index < count; ++index) {
    const int comparison = CompareResultCells(left[index], right[index]);
    if (comparison != 0) return comparison < 0;
  }
  return left.size() < right.size();
}

bool GroupCellsEqual(const std::vector<ResultValueCell>& left,
                     const std::vector<ResultValueCell>& right) {
  if (left.size() != right.size()) return false;
  for (size_t index = 0; index < left.size(); ++index) {
    if (CompareResultCells(left[index], right[index]) != 0) return false;
  }
  return true;
}

Status AppendGroupedColumns(const std::vector<uint32_t>& group_columns,
                            const std::vector<std::string>& group_names,
                            const std::vector<std::vector<std::optional<Value>>>& values,
                            size_t start, size_t end, ColumnBatch* grouped) {
  for (size_t column = 0; column < group_columns.size(); ++column) {
    std::vector<Value> output;
    std::vector<bool> validity;
    output.reserve(end - start);
    validity.reserve(end - start);
    for (size_t row = start; row < end; ++row) {
      validity.push_back(values[row][column].has_value());
      output.push_back(values[row][column].value_or(Value::Binary("")));
    }
    const Status added = grouped->AddVector(
        std::make_shared<FlatVector>(std::move(output), std::move(validity)));
    if (!added.ok()) return added;
  }
  return Status::OK();
}

}  // namespace

Status ResultBatch::Validate() const {
  if (column_names_.size() != batch_.column_count()) {
    return Status::InvalidArgument("result batch", "column name and vector count mismatch");
  }
  for (const std::string& name : column_names_) {
    if (name.empty()) return Status::InvalidArgument("result batch", "empty column name");
  }
  return Status::OK();
}

Status CancellableResultStream::Next(ResultBatch* batch) {
  if (batch == nullptr) return Status::InvalidArgument("result stream", "missing batch output");
  if (cancellation_ && cancellation_->IsCancelled()) {
    terminal_status_ = Status::QueryCancelled("result stream", "query cancelled");
    return terminal_status_;
  }
  if (!input_) return Status::NotFound("result stream", "end of stream");
  const Status next = input_->Next(batch);
  if (!next.ok() && !next.IsNotFound()) terminal_status_ = next;
  return next;
}

MemoryAccountedResultStream::~MemoryAccountedResultStream() {
  if (account_ && reserved_bytes_ != 0) account_->Release(reserved_bytes_);
}

Status MemoryAccountedResultStream::Next(ResultBatch* batch) {
  if (batch == nullptr) return Status::InvalidArgument("result stream", "missing batch output");
  if (!input_) return Status::NotFound("result stream", "end of stream");
  return input_->Next(batch);
}

Status InMemoryResultStream::Next(ResultBatch* batch) {
  if (batch == nullptr) return Status::InvalidArgument("result stream", "missing batch output");
  if (next_batch_ == batches_.size()) return Status::NotFound("result stream", "end of stream");
  *batch = batches_[next_batch_++];
  return batch->Validate();
}

Status ProjectColumnsResultStream::Next(ResultBatch* batch) {
  if (batch == nullptr) {
    return terminal_status_ =
        Status::InvalidArgument("result projection", "missing batch output");
  }
  if (!terminal_status_.ok()) return terminal_status_;
  if (!input_ || columns_.empty() || columns_.size() != names_.size() ||
      std::any_of(names_.begin(), names_.end(),
                  [](const std::string& name) { return name.empty(); })) {
    return terminal_status_ = Status::InvalidArgument(
        "result projection", "invalid projection specification");
  }
  ResultBatch source;
  const Status next = input_->Next(&source);
  if (!next.ok()) {
    if (!next.IsNotFound()) terminal_status_ = next;
    return next;
  }
  const Status valid_source = source.Validate();
  if (!valid_source.ok()) return terminal_status_ = valid_source;
  for (uint32_t column : columns_) {
    if (column >= source.batch().column_count()) {
      return terminal_status_ = Status::InvalidArgument(
          "result projection", "projected column is absent");
    }
  }

  const uint32_t rows = source.batch().row_count();
  std::vector<uint32_t> selection;
  selection.reserve(rows);
  bool has_selection = rows != source.batch().source_row_count();
  for (uint32_t row = 0; row < rows; ++row) {
    const auto source_row = source.batch().SourceRowAt(row);
    if (!source_row.has_value()) {
      return terminal_status_ = Status::Corruption(
          "result projection", "invalid input selection");
    }
    selection.push_back(*source_row);
    has_selection = has_selection || *source_row != row;
  }
  ColumnBatch projected(source.batch().capacity());
  for (uint32_t column : columns_) {
    const Status added = projected.AddVector(source.batch().VectorAt(column));
    if (!added.ok()) return terminal_status_ = added;
  }
  if (has_selection) {
    const Status selected = projected.SetSelection(std::move(selection));
    if (!selected.ok()) return terminal_status_ = selected;
  }
  *batch = ResultBatch(names_, std::move(projected),
                       source.temporal_metadata());
  const Status valid_output = batch->Validate();
  if (!valid_output.ok()) return terminal_status_ = valid_output;
  return Status::OK();
}

Status CollectResultStream::Next(ResultBatch* batch) {
  if (batch == nullptr) return Status::InvalidArgument("result stream", "missing batch output");
  if (!terminal_status_.ok()) return terminal_status_;
  if (emitted_) return Status::NotFound("result stream", "end of stream");
  if (!input_) return Status::InvalidArgument("result stream", "missing collect input");
  ListValue collected;
  std::optional<ResultValueKind> collect_kind = expected_kind_;
  if (collect_kind == ResultValueKind::kList) {
    return terminal_status_ = Status::NotSupported(
        "result stream", "COLLECT over list values is not available");
  }
  if (collect_kind == ResultValueKind::kStruct) {
    collected.element_kind = ListElementKind::kStruct;
  }
  if (memory_account_) {
    const Status reserved = memory_account_->Reserve(sizeof(ListValue));
    if (!reserved.ok()) return terminal_status_ = reserved;
    reserved_bytes_ = sizeof(ListValue);
    memory_peak_bytes_ = reserved_bytes_;
  }
  for (;;) {
    if (cancellation_ && cancellation_->IsCancelled()) {
      return terminal_status_ = Status::QueryCancelled(
          "result stream", "query cancelled during COLLECT");
    }
    ResultBatch source;
    const Status next = input_->Next(&source);
    if (next.IsNotFound()) {
      const Status terminal = ResultStreamTerminalAtEnd(input_.get());
      if (!terminal.ok()) return terminal_status_ = terminal;
      break;
    }
    if (!next.ok()) return terminal_status_ = next;
    if (source.batch().column_count() != 1) {
      return terminal_status_ = Status::InvalidArgument("result stream", "COLLECT requires one input column");
    }
    const ResultValueKind source_kind = ResultColumnKind(source.batch(), 0);
    if (source_kind == ResultValueKind::kList) {
      return terminal_status_ = Status::NotSupported(
          "result stream", "COLLECT over list values is not available");
    }
    if (collect_kind.has_value() && *collect_kind != source_kind) {
      return terminal_status_ = Status::SchemaMismatch(
          "result stream", "COLLECT input kind changed between batches");
    }
    collect_kind = source_kind;
    collected.element_kind = source_kind == ResultValueKind::kStruct
        ? ListElementKind::kStruct : ListElementKind::kScalar;
    for (uint32_t row = 0; row < source.batch().row_count(); ++row) {
      if (source_kind == ResultValueKind::kStruct) {
        const StructValue* value = source.batch().StructRefAt(0, row);
        if (value == nullptr) continue;
        const size_t requested_capacity = AppendCapacity(
            collected.structured_elements.size(),
            collected.structured_elements.capacity());
        const Status grown = ReserveVectorRelocation(
            &collected.structured_elements, requested_capacity,
            memory_account_, &reserved_bytes_, &memory_peak_bytes_,
            "result stream");
        if (!grown.ok()) return terminal_status_ = grown;
        const uint64_t bytes = StructPayloadBytes(*value);
        if (memory_account_) {
          const Status reserved = memory_account_->Reserve(bytes);
          if (!reserved.ok()) return terminal_status_ = reserved;
          reserved_bytes_ = AddRetainedBytes(reserved_bytes_, bytes);
          memory_peak_bytes_ = std::max(memory_peak_bytes_, reserved_bytes_);
        }
        collected.structured_elements.push_back(*value);
        continue;
      }
      const Value* value = source.batch().ValueRefAt(0, row);
      if (value == nullptr) continue;
      const size_t requested_capacity = AppendCapacity(
          collected.elements.size(), collected.elements.capacity());
      const Status grown = ReserveVectorRelocation(
          &collected.elements, requested_capacity, memory_account_,
          &reserved_bytes_, &memory_peak_bytes_, "result stream");
      if (!grown.ok()) return terminal_status_ = grown;
      const uint64_t bytes = ResultValuePayloadBytes(*value);
      if (memory_account_) {
        const Status reserved = memory_account_->Reserve(bytes);
        if (!reserved.ok()) return terminal_status_ = reserved;
        reserved_bytes_ = AddRetainedBytes(reserved_bytes_, bytes);
        memory_peak_bytes_ = std::max(memory_peak_bytes_, reserved_bytes_);
      }
      collected.elements.push_back(*value);
    }
  }
  ColumnBatch values(1);
  std::vector<ListValue> output_values;
  const Status output_reserved = ReserveVectorRelocation(
      &output_values, 1, memory_account_, &reserved_bytes_,
      &memory_peak_bytes_, "result stream");
  if (!output_reserved.ok()) return terminal_status_ = output_reserved;
  output_values.push_back(std::move(collected));
  if (memory_account_) {
    memory_account_->Release(sizeof(ListValue));
    reserved_bytes_ = sizeof(ListValue) > reserved_bytes_
        ? 0 : reserved_bytes_ - sizeof(ListValue);
  }
  std::shared_ptr<void> retention;
  if (memory_account_ && reserved_bytes_ != 0) {
    retention = std::make_shared<QueryMemoryLease>(
        memory_account_, reserved_bytes_);
    reserved_bytes_ = 0;
  }
  const Status added = values.AddVector(std::make_shared<ListVector>(
      std::move(output_values), std::vector<bool>{true},
      std::move(retention)));
  if (!added.ok()) return terminal_status_ = added;
  *batch = ResultBatch({column_name_}, std::move(values));
  const Status valid = batch->Validate();
  if (!valid.ok()) return terminal_status_ = valid;
  emitted_ = true;
  return Status::OK();
}

CollectResultStream::~CollectResultStream() {
  if (memory_account_ && reserved_bytes_ != 0) {
    memory_account_->Release(reserved_bytes_);
  }
}

GroupedCollectResultStream::GroupedCollectResultStream(
    std::unique_ptr<QueryResultStream> input,
    std::vector<uint32_t> group_columns, uint32_t collect_column,
    std::string collect_name, std::vector<ResultOutputSlot> output_slots,
    uint32_t batch_capacity,
    std::shared_ptr<QueryCancellation> cancellation,
    std::shared_ptr<QueryMemoryAccount> memory_account,
    std::string spill_directory,
    std::shared_ptr<ResourceGovernorExtension> spill_resources,
    std::optional<ResultValueKind> expected_collect_kind)
    : input_(std::move(input)), group_columns_(std::move(group_columns)),
      collect_column_(collect_column), collect_name_(std::move(collect_name)),
      output_slots_(std::move(output_slots)), batch_capacity_(batch_capacity),
      cancellation_(std::move(cancellation)),
      memory_account_(std::move(memory_account)),
      spill_directory_(std::move(spill_directory)),
      spill_resources_(std::move(spill_resources)),
      collect_kind_(expected_collect_kind) {}

StatusOr<uint64_t> GroupedCollectResultStream::MeasureGroup(
    const std::string& record, uint64_t* decode_temporary_bytes) const {
  if (decode_temporary_bytes == nullptr) {
    return Status::InvalidArgument(
        "grouped collect spill", "missing decode temporary size output");
  }
  *decode_temporary_bytes = 0;
  if (record.size() > kMaxGroupedCollectSpillRecordBytes) {
    return Status::Corruption(
        "grouped collect spill", "group record exceeds size bound");
  }
  size_t offset = 0;
  uint32_t magic = 0;
  uint32_t value_count = 0;
  if (!ReadU32(record, &offset, &magic) ||
      magic != kGroupedCollectSpillMagic ||
      !ReadU32(record, &offset, &value_count) ||
      value_count != group_columns_.size() ||
      value_count > kMaxGroupedAggregateSpillValues) {
    return Status::Corruption(
        "grouped collect spill", "invalid group record header");
  }
  uint64_t bytes =
      static_cast<uint64_t>(value_count) * sizeof(ResultValueCell);
  for (uint32_t value = 0; value < value_count; ++value) {
    if (!MeasureEncodedResultCell(
            record, &offset, &bytes, nullptr, nullptr,
            decode_temporary_bytes)) {
      return Status::Corruption(
          "grouped collect spill", "invalid group value");
    }
  }
  uint8_t collect_kind = 0;
  if (!ReadU8(record, &offset, &collect_kind) ||
      collect_kind > static_cast<uint8_t>(ResultValueKind::kStruct) ||
      (collect_kind_.has_value() &&
       collect_kind != static_cast<uint8_t>(*collect_kind_))) {
    return Status::Corruption(
        "grouped collect spill", "invalid collected value kind");
  }
  ResultValueKind encoded_kind = ResultValueKind::kScalar;
  bool present = false;
  if (!MeasureEncodedResultCell(
          record, &offset, &bytes, &encoded_kind, &present,
          decode_temporary_bytes) ||
      encoded_kind != ResultValueKind::kList || !present ||
      offset != record.size()) {
    return Status::Corruption(
        "grouped collect spill", "invalid collected list value");
  }
  if (bytes == std::numeric_limits<uint64_t>::max()) {
    return Status::QueryMemoryLimit(
        "grouped collect spill", "decoded group size overflow");
  }
  return bytes;
}

Status GroupedCollectResultStream::MeasureEncodedGroup(
    const Group& group, uint64_t* group_key_bytes,
    uint64_t* encoded_bytes, uint64_t* encode_temporary_bytes) const {
  if (group_key_bytes == nullptr || encoded_bytes == nullptr) {
    return Status::InvalidArgument(
        "grouped collect spill", "missing encoded size output");
  }
  if (encode_temporary_bytes == nullptr) {
    return Status::InvalidArgument(
        "grouped collect spill", "missing encode temporary size output");
  }
  *encode_temporary_bytes = 0;
  if (!collect_kind_.has_value() ||
      *collect_kind_ == ResultValueKind::kList ||
      group.values.size() > kMaxGroupedAggregateSpillValues ||
      group.collected.elements.size() > kMaxGroupedAggregateSpillValues ||
      group.collected.structured_elements.size() >
          kMaxGroupedAggregateSpillValues ||
      !group.collected.IsConsistent() ||
      group.collected.element_kind !=
          (*collect_kind_ == ResultValueKind::kStruct
               ? ListElementKind::kStruct : ListElementKind::kScalar)) {
    return Status::InvalidArgument(
        "grouped collect spill", "group state exceeds spill bounds");
  }
  *group_key_bytes = 0;
  for (const ResultValueCell& value : group.values) {
    const Status measured =
        MeasureCellForGroupedCollectSpill(
            value, group_key_bytes, encode_temporary_bytes);
    if (!measured.ok()) return measured;
  }
  *encoded_bytes = 2 * sizeof(uint32_t);
  Status measured = AddGroupedCollectSpillBytes(
      *group_key_bytes, encoded_bytes);
  if (!measured.ok()) return measured;
  measured = AddGroupedCollectSpillBytes(sizeof(uint8_t) + 2, encoded_bytes);
  if (!measured.ok()) return measured;
  measured = MeasureListForGroupedCollectSpill(
      group.collected, encoded_bytes, encode_temporary_bytes);
  if (!measured.ok()) return measured;
  return Status::OK();
}

StatusOr<std::string> GroupedCollectResultStream::EncodeGroup(
    const Group& group, uint64_t encoded_bytes) const {
  if (encoded_bytes > kMaxGroupedCollectSpillRecordBytes) {
    return Status::QueryMemoryLimit(
        "grouped collect spill", "spill record exceeds size bound");
  }
  std::string record;
  record.reserve(static_cast<size_t>(encoded_bytes));
  AppendU32(&record, kGroupedCollectSpillMagic);
  AppendU32(&record, static_cast<uint32_t>(group.values.size()));
  for (const ResultValueCell& value : group.values) AppendResultCell(&record, value);
  AppendU8(&record, static_cast<uint8_t>(*collect_kind_));
  AppendU8(&record, static_cast<uint8_t>(ResultValueKind::kList));
  AppendU8(&record, 1);
  AppendResultListValue(&record, group.collected);
  if (record.size() != encoded_bytes) {
    return Status::Corruption(
        "grouped collect spill", "encoded spill size mismatch");
  }
  return record;
}

StatusOr<GroupedCollectResultStream::Group>
GroupedCollectResultStream::DecodeGroup(const std::string& record) const {
  size_t offset = 0;
  uint32_t magic = 0;
  uint32_t value_count = 0;
  if (!ReadU32(record, &offset, &magic) || magic != kGroupedCollectSpillMagic ||
      !ReadU32(record, &offset, &value_count) ||
      value_count != group_columns_.size() ||
      value_count > kMaxGroupedAggregateSpillValues) {
    return Status::Corruption(
        "grouped collect spill", "invalid group record header");
  }
  Group group;
  group.values.resize(value_count);
  for (ResultValueCell& value : group.values) {
    if (!ReadResultCell(record, &offset, &value)) {
      return Status::Corruption(
          "grouped collect spill", "invalid group value");
    }
  }
  uint8_t collect_kind = 0;
  if (!ReadU8(record, &offset, &collect_kind) ||
      collect_kind > static_cast<uint8_t>(ResultValueKind::kStruct) ||
      (collect_kind_.has_value() &&
       collect_kind != static_cast<uint8_t>(*collect_kind_))) {
    return Status::Corruption(
        "grouped collect spill", "invalid collected value kind");
  }
  ResultValueCell collected;
  if (!ReadResultCell(record, &offset, &collected) ||
      collected.kind != ResultValueKind::kList ||
      !collected.list.has_value()) {
    return Status::Corruption(
        "grouped collect spill", "invalid collected list value");
  }
  group.collected = std::move(*collected.list);
  const ResultValueKind decoded_kind =
      static_cast<ResultValueKind>(collect_kind);
  if (!group.collected.IsConsistent() ||
      group.collected.element_kind !=
          (decoded_kind == ResultValueKind::kStruct
               ? ListElementKind::kStruct : ListElementKind::kScalar)) {
    return Status::Corruption(
        "grouped collect spill", "collected list kind mismatch");
  }
  if (offset != record.size()) {
    return Status::Corruption(
        "grouped collect spill", "trailing collect spill bytes");
  }
  return group;
}

Status GroupedCollectResultStream::AppendSpillGroup(const Group& group) {
  if (!spill_) {
    return Status::InvalidArgument(
        "grouped collect spill", "spill set is not open");
  }
  uint64_t group_key_bytes = 0;
  uint64_t encoded_bytes = 0;
  uint64_t encode_temporary_bytes = 0;
  const Status measured =
      MeasureEncodedGroup(
          group, &group_key_bytes, &encoded_bytes,
          &encode_temporary_bytes);
  if (!measured.ok()) return measured;
  std::shared_ptr<void> encoding_retention;
  std::shared_ptr<void> encode_temporary_retention;
  const uint64_t encode_peak_bytes = AddRetainedBytes(
      encoded_bytes, encode_temporary_bytes);
  if (memory_account_ && encode_peak_bytes != 0) {
    if (encode_peak_bytes == std::numeric_limits<uint64_t>::max()) {
      return Status::QueryMemoryLimit(
          "grouped collect spill", "encode reservation overflow");
    }
    const Status reserved = memory_account_->Reserve(encode_peak_bytes);
    if (!reserved.ok()) return reserved;
    memory_peak_bytes_ = std::max(
        memory_peak_bytes_, memory_account_->used_bytes());
    encoding_retention = std::make_shared<QueryMemoryLease>(
        memory_account_, encoded_bytes);
    encode_temporary_retention = std::make_shared<QueryMemoryLease>(
        memory_account_, encode_temporary_bytes);
  }
  const auto encoded = EncodeGroup(group, encoded_bytes);
  if (!encoded.ok()) return encoded.status();
  encode_temporary_retention.reset();
  if (group_key_bytes > encoded.ValueOrDie().size() - 2 * sizeof(uint32_t)) {
    return Status::Corruption(
        "grouped collect spill", "invalid encoded group-key size");
  }
  const std::string_view group_key(
      encoded.ValueOrDie().data() + 2 * sizeof(uint32_t),
      static_cast<size_t>(group_key_bytes));
  const uint32_t partition = static_cast<uint32_t>(
      StableResultKeyHash(group_key) % spill_->partition_count());
  return spill_->AppendRecord(partition, encoded.ValueOrDie());
}

void GroupedCollectResultStream::ReleaseGroups() {
  const uint64_t released_bytes = reserved_bytes_;
  reserved_bytes_ = 0;
  std::vector<Group>().swap(groups_);
  if (memory_account_ && released_bytes != 0) {
    memory_account_->Release(released_bytes);
  }
  next_group_ = 0;
}

Status GroupedCollectResultStream::SpillGroups() {
  if (!spill_) {
    const std::string directory =
        spill_directory_.empty() ? "/tmp" : spill_directory_;
    spill_ = std::make_unique<PartitionedSpillSet>(
        directory, kGroupedAggregateSpillPartitions, cancellation_,
        spill_resources_, memory_account_,
        [this](uint64_t bytes) { spill_bytes_ += bytes; });
    const Status opened = spill_->Open();
    if (!opened.ok()) return opened;
  }
  for (const Group& group : groups_) {
    const Status appended = AppendSpillGroup(group);
    if (!appended.ok()) return appended;
  }
  ReleaseGroups();
  spilling_ = true;
  return Status::OK();
}

StatusOr<bool> GroupedCollectResultStream::LoadNextSpillPartition() {
  ReleaseGroups();
  while (spill_ && next_spill_partition_ < spill_->partition_count()) {
    if (cancellation_ && cancellation_->IsCancelled()) {
      return Status::QueryCancelled(
          "result stream", "query cancelled during COLLECT spill replay");
    }
    const uint32_t partition = next_spill_partition_++;
    if (!spill_->HasData(partition)) continue;
    const Status rewound = spill_->Rewind(partition);
    if (!rewound.ok()) return rewound;
    for (;;) {
      std::string record;
      const Status next = spill_->NextRecord(partition, &record);
      if (next.IsNotFound()) break;
      if (!next.ok()) return next;
      uint64_t decode_temporary_bytes = 0;
      const auto measured = MeasureGroup(record, &decode_temporary_bytes);
      if (!measured.ok()) return measured.status();
      const uint64_t decoded_bytes = measured.ValueOrDie();
      const uint64_t decode_peak_bytes = AddRetainedBytes(
          decoded_bytes, decode_temporary_bytes);
      if (decode_peak_bytes == std::numeric_limits<uint64_t>::max()) {
        return Status::QueryMemoryLimit(
            "grouped collect spill", "decode reservation overflow");
      }
      if (memory_account_) {
        const Status reserved = memory_account_->Reserve(decode_peak_bytes);
        if (!reserved.ok()) return reserved;
        memory_peak_bytes_ = std::max(
            memory_peak_bytes_, memory_account_->used_bytes());
      }
      auto decoded = DecodeGroup(record);
      if (!decoded.ok()) {
        if (memory_account_) memory_account_->Release(decode_peak_bytes);
        return decoded.status();
      }
      if (memory_account_ && decode_temporary_bytes != 0) {
        memory_account_->Release(decode_temporary_bytes);
      }
      Group partial = std::move(decoded).ConsumeValueOrDie();
      auto group = std::find_if(
          groups_.begin(), groups_.end(), [&partial](const Group& candidate) {
            return GroupCellsEqual(candidate.values, partial.values);
          });
      if (group == groups_.end()) {
        if (groups_.size() == std::numeric_limits<size_t>::max()) {
          if (memory_account_) memory_account_->Release(decoded_bytes);
          return Status::QueryMemoryLimit(
              "grouped collect spill", "group capacity overflow");
        }
        const Status grown = ReserveVectorRelocation(
            &groups_, groups_.size() + 1, memory_account_,
            &reserved_bytes_, &memory_peak_bytes_,
            "grouped collect spill");
        if (!grown.ok()) {
          if (memory_account_) memory_account_->Release(decoded_bytes);
          return grown;
        }
        if (memory_account_) {
          reserved_bytes_ = AddRetainedBytes(
              reserved_bytes_, decoded_bytes);
        }
        groups_.push_back(std::move(partial));
        continue;
      }
      const ResultValueKind collected_kind =
          collect_kind_.value_or(ResultValueKind::kScalar);
      const uint64_t transferred_payload_bytes = CollectedPayloadBytes(
          partial.collected, collected_kind);
      if (transferred_payload_bytes > decoded_bytes) {
        if (memory_account_) memory_account_->Release(decoded_bytes);
        return Status::Corruption(
            "grouped collect spill", "decoded group charge is too small");
      }
      if (collected_kind == ResultValueKind::kStruct) {
        if (partial.collected.structured_elements.size() >
            std::numeric_limits<size_t>::max() -
                group->collected.structured_elements.size()) {
          if (memory_account_) memory_account_->Release(decoded_bytes);
          return Status::QueryMemoryLimit(
              "grouped collect spill", "merge capacity overflow");
        }
        const size_t required = group->collected.structured_elements.size() +
            partial.collected.structured_elements.size();
        const Status grown = ReserveVectorRelocation(
            &group->collected.structured_elements, required,
            memory_account_, &reserved_bytes_, &memory_peak_bytes_,
            "grouped collect spill");
        if (!grown.ok()) {
          if (memory_account_) memory_account_->Release(decoded_bytes);
          return grown;
        }
        group->collected.structured_elements.insert(
            group->collected.structured_elements.end(),
            std::make_move_iterator(
                partial.collected.structured_elements.begin()),
            std::make_move_iterator(
                partial.collected.structured_elements.end()));
      } else {
        if (partial.collected.elements.size() >
            std::numeric_limits<size_t>::max() -
                group->collected.elements.size()) {
          if (memory_account_) memory_account_->Release(decoded_bytes);
          return Status::QueryMemoryLimit(
              "grouped collect spill", "merge capacity overflow");
        }
        const size_t required = group->collected.elements.size() +
            partial.collected.elements.size();
        const Status grown = ReserveVectorRelocation(
            &group->collected.elements, required, memory_account_,
            &reserved_bytes_, &memory_peak_bytes_,
            "grouped collect spill");
        if (!grown.ok()) {
          if (memory_account_) memory_account_->Release(decoded_bytes);
          return grown;
        }
        group->collected.elements.insert(
            group->collected.elements.end(),
            std::make_move_iterator(partial.collected.elements.begin()),
            std::make_move_iterator(partial.collected.elements.end()));
      }
      partial = Group{};
      if (memory_account_) {
        memory_account_->Release(
            decoded_bytes - transferred_payload_bytes);
        reserved_bytes_ = AddRetainedBytes(
            reserved_bytes_, transferred_payload_bytes);
      }
    }
    const Status sealed = spill_->Seal(partition);
    if (!sealed.ok()) return sealed;
    std::stable_sort(
        groups_.begin(), groups_.end(), [](const Group& left, const Group& right) {
          return GroupCellsLess(left.values, right.values);
        });
    if (!groups_.empty()) return true;
  }
  return false;
}

GroupedCollectResultStream::~GroupedCollectResultStream() {
  ReleaseGroups();
}

Status GroupedCollectResultStream::Initialize() {
  if (initialized_) return terminal_status_;
  initialized_ = true;
  if (!input_ || output_slots_.empty() || batch_capacity_ == 0) {
    return terminal_status_ = Status::InvalidArgument("result stream", "invalid grouped COLLECT input");
  }
  for (;;) {
    if (cancellation_ && cancellation_->IsCancelled()) {
      return terminal_status_ = Status::QueryCancelled("result stream",
                                                        "query cancelled during COLLECT");
    }
    ResultBatch source;
    const Status next = input_->Next(&source);
    if (next.IsNotFound()) {
      const Status terminal = ResultStreamTerminalAtEnd(input_.get());
      if (!terminal.ok()) return terminal_status_ = terminal;
      break;
    }
    if (!next.ok()) return terminal_status_ = next;
    if (collect_column_ >= source.batch().column_count()) {
      return terminal_status_ = Status::InvalidArgument("result stream", "COLLECT input column is absent");
    }
    const ResultValueKind source_collect_kind =
        ResultColumnKind(source.batch(), collect_column_);
    if (source_collect_kind == ResultValueKind::kList) {
      return terminal_status_ = Status::NotSupported(
          "result stream", "COLLECT over list values is not available");
    }
    if (collect_kind_.has_value() &&
        *collect_kind_ != source_collect_kind) {
      return terminal_status_ = Status::SchemaMismatch(
          "result stream", "COLLECT input kind changed between batches");
    }
    collect_kind_ = source_collect_kind;
    for (uint32_t column : group_columns_) {
      if (column >= source.batch().column_count()) {
        return terminal_status_ = Status::InvalidArgument(
            "result stream", "COLLECT grouping column is absent");
      }
    }
    if (group_names_.empty()) {
      for (uint32_t column : group_columns_) {
        group_names_.push_back(source.column_names()[column]);
        group_kinds_.push_back(ResultColumnKind(source.batch(), column));
      }
      temporal_metadata_ = source.temporal_metadata();
    } else {
      for (size_t index = 0; index < group_columns_.size(); ++index) {
        if (ResultColumnKind(source.batch(), group_columns_[index]) !=
            group_kinds_[index]) {
          return terminal_status_ = Status::SchemaMismatch(
              "result stream",
              "COLLECT grouping kind changed between batches");
        }
      }
    }
    for (uint32_t row = 0; row < source.batch().row_count(); ++row) {
      const Value* scalar_collected =
          source_collect_kind == ResultValueKind::kScalar
              ? source.batch().ValueRefAt(collect_column_, row) : nullptr;
      const StructValue* structured_collected =
          source_collect_kind == ResultValueKind::kStruct
              ? source.batch().StructRefAt(collect_column_, row) : nullptr;
      uint64_t provisional_group_bytes =
          static_cast<uint64_t>(group_columns_.size()) *
          sizeof(ResultValueCell);
      for (uint32_t column : group_columns_) {
        provisional_group_bytes = AddRetainedBytes(
            provisional_group_bytes,
            BatchResultCellRetainedBytes(source.batch(), column, row));
      }
      if (spilling_ && structured_collected != nullptr) {
        provisional_group_bytes = AddRetainedBytes(
            provisional_group_bytes,
            AddRetainedBytes(sizeof(StructValue),
                             StructPayloadBytes(*structured_collected)));
      } else if (spilling_ && scalar_collected != nullptr) {
        provisional_group_bytes = AddRetainedBytes(
            provisional_group_bytes,
            sizeof(std::optional<Value>) +
                ResultValuePayloadBytes(*scalar_collected));
      }
      if (memory_account_) {
        const Status reserved =
            memory_account_->Reserve(provisional_group_bytes);
        if (!reserved.ok()) return terminal_status_ = reserved;
        memory_peak_bytes_ = std::max(
            memory_peak_bytes_, memory_account_->used_bytes());
      }
      std::vector<ResultValueCell> values;
      values.reserve(group_columns_.size());
      for (uint32_t column : group_columns_) {
        values.push_back(ResultCellAt(source.batch(), column, row));
      }
      if (spilling_) {
        Status appended = Status::OK();
        {
          Group partial{std::move(values), {}};
          partial.collected.element_kind =
              source_collect_kind == ResultValueKind::kStruct
                  ? ListElementKind::kStruct : ListElementKind::kScalar;
          if (structured_collected != nullptr) {
            partial.collected.structured_elements.reserve(1);
            partial.collected.structured_elements.push_back(
                *structured_collected);
          } else if (scalar_collected != nullptr) {
            partial.collected.elements.reserve(1);
            partial.collected.elements.push_back(*scalar_collected);
          }
          appended = AppendSpillGroup(partial);
        }
        if (memory_account_) {
          memory_account_->Release(provisional_group_bytes);
        }
        if (!appended.ok()) return terminal_status_ = appended;
        continue;
      }
      auto group = std::find_if(groups_.begin(), groups_.end(), [&values](const Group& candidate) {
        return GroupCellsEqual(candidate.values, values);
      });
      const bool existing_group = group != groups_.end();
      if (group == groups_.end()) {
        if (groups_.size() == std::numeric_limits<size_t>::max()) {
          std::vector<ResultValueCell>().swap(values);
          if (memory_account_) {
            memory_account_->Release(provisional_group_bytes);
          }
          return terminal_status_ = Status::QueryMemoryLimit(
              "result stream", "group capacity overflow");
        }
        const Status grown = ReserveVectorRelocation(
            &groups_, groups_.size() + 1, memory_account_,
            &reserved_bytes_, &memory_peak_bytes_, "result stream");
        if (!grown.ok()) {
          std::vector<ResultValueCell>().swap(values);
          if (memory_account_) {
            memory_account_->Release(provisional_group_bytes);
          }
          return terminal_status_ = grown;
        }
        Group inserted{std::move(values), {}};
        inserted.collected.element_kind =
            source_collect_kind == ResultValueKind::kStruct
                ? ListElementKind::kStruct : ListElementKind::kScalar;
        groups_.push_back(std::move(inserted));
        group = std::prev(groups_.end());
        if (memory_account_) {
          reserved_bytes_ = AddRetainedBytes(
              reserved_bytes_, provisional_group_bytes);
        }
      }
      if (structured_collected != nullptr) {
        const size_t requested_capacity = AppendCapacity(
            group->collected.structured_elements.size(),
            group->collected.structured_elements.capacity());
        const Status grown = ReserveVectorRelocation(
            &group->collected.structured_elements, requested_capacity,
            memory_account_, &reserved_bytes_, &memory_peak_bytes_,
            "result stream");
        if (!grown.ok()) {
          if (existing_group) {
            std::vector<ResultValueCell>().swap(values);
            if (memory_account_) {
              memory_account_->Release(provisional_group_bytes);
            }
          }
          return terminal_status_ = grown;
        }
        const uint64_t bytes = StructPayloadBytes(*structured_collected);
        if (memory_account_) {
          const Status reserved = memory_account_->Reserve(bytes);
          if (!reserved.ok()) {
            if (existing_group) {
              std::vector<ResultValueCell>().swap(values);
              memory_account_->Release(provisional_group_bytes);
            }
            return terminal_status_ = reserved;
          }
          reserved_bytes_ = AddRetainedBytes(reserved_bytes_, bytes);
          memory_peak_bytes_ = std::max(
              memory_peak_bytes_, memory_account_->used_bytes());
        }
        group->collected.structured_elements.push_back(
            *structured_collected);
      } else if (scalar_collected != nullptr) {
        const size_t requested_capacity = AppendCapacity(
            group->collected.elements.size(),
            group->collected.elements.capacity());
        const Status grown = ReserveVectorRelocation(
            &group->collected.elements, requested_capacity,
            memory_account_, &reserved_bytes_, &memory_peak_bytes_,
            "result stream");
        if (!grown.ok()) {
          if (existing_group) {
            std::vector<ResultValueCell>().swap(values);
            if (memory_account_) {
              memory_account_->Release(provisional_group_bytes);
            }
          }
          return terminal_status_ = grown;
        }
        const uint64_t bytes = ResultValuePayloadBytes(*scalar_collected);
        if (memory_account_) {
          const Status reserved = memory_account_->Reserve(bytes);
          if (!reserved.ok()) {
            if (existing_group) {
              std::vector<ResultValueCell>().swap(values);
              memory_account_->Release(provisional_group_bytes);
            }
            return terminal_status_ = reserved;
          }
          reserved_bytes_ = AddRetainedBytes(reserved_bytes_, bytes);
          memory_peak_bytes_ = std::max(
              memory_peak_bytes_, memory_account_->used_bytes());
        }
        group->collected.elements.push_back(*scalar_collected);
      }
      if (existing_group) {
        std::vector<ResultValueCell>().swap(values);
        if (memory_account_) {
          memory_account_->Release(provisional_group_bytes);
        }
      }
      if (memory_account_ && memory_account_->ShouldSpill()) {
        const Status spilled = SpillGroups();
        if (!spilled.ok()) return terminal_status_ = spilled;
      }
    }
  }
  if (!spilling_) {
    std::stable_sort(groups_.begin(), groups_.end(), [](const Group& left, const Group& right) {
      return GroupCellsLess(left.values, right.values);
    });
  } else {
    const Status sealed = spill_->Seal();
    if (!sealed.ok()) return terminal_status_ = sealed;
  }
  return Status::OK();
}

Status GroupedCollectResultStream::Next(ResultBatch* batch) {
  if (batch == nullptr) return Status::InvalidArgument("result stream", "missing batch output");
  const Status initialized = Initialize();
  if (!initialized.ok()) return initialized;
  while (next_group_ == groups_.size()) {
    if (!spilling_) return Status::NotFound("result stream", "end of stream");
    const auto loaded = LoadNextSpillPartition();
    if (!loaded.ok()) return terminal_status_ = loaded.status();
    if (!loaded.ValueOrDie()) {
      return Status::NotFound("result stream", "end of stream");
    }
  }
  const size_t end = std::min(groups_.size(), next_group_ + batch_capacity_);
  ColumnBatch grouped(static_cast<uint32_t>(end - next_group_));
  std::vector<std::string> names;
  names.reserve(output_slots_.size());
  for (const ResultOutputSlot& slot : output_slots_) {
    if (slot.aggregate) {
      if (slot.index != 0) return Status::Corruption("result stream", "invalid COLLECT output slot");
      uint64_t output_bytes =
          static_cast<uint64_t>(end - next_group_) * sizeof(ListValue);
      for (size_t row = next_group_; row < end; ++row) {
        output_bytes = AddRetainedBytes(
            output_bytes,
            CollectedElementsBytes(
                groups_[row].collected,
                collect_kind_.value_or(ResultValueKind::kScalar)));
      }
      std::shared_ptr<void> retention;
      if (memory_account_ && output_bytes != 0) {
        const Status reserved = memory_account_->Reserve(output_bytes);
        if (!reserved.ok()) return terminal_status_ = reserved;
        memory_peak_bytes_ = std::max(
            memory_peak_bytes_, memory_account_->used_bytes());
        retention = std::make_shared<QueryMemoryLease>(
            memory_account_, output_bytes);
      }
      std::vector<ListValue> collected;
      collected.reserve(end - next_group_);
      for (size_t row = next_group_; row < end; ++row) collected.push_back(groups_[row].collected);
      const Status added = grouped.AddVector(
          std::make_shared<ListVector>(
              std::move(collected), std::vector<bool>{}, std::move(retention)));
      if (!added.ok()) return added;
      names.push_back(collect_name_);
      continue;
    }
    if (slot.index >= group_columns_.size()) {
      return Status::Corruption("result stream", "invalid COLLECT group output slot");
    }
    uint64_t output_bytes = 0;
    uint64_t temporary_bytes =
        static_cast<uint64_t>(end - next_group_) *
        sizeof(std::vector<ResultValueCell>);
    for (size_t row = next_group_; row < end; ++row) {
      const ResultValueCell& value = groups_[row].values[slot.index];
      output_bytes = AddRetainedBytes(
          output_bytes, ResultCellOutputBytes(value));
      temporary_bytes = AddRetainedBytes(
          temporary_bytes, ResultCellCopyBytes(value));
    }
    std::shared_ptr<void> output_retention;
    if (memory_account_ && output_bytes != 0) {
      const Status reserved = memory_account_->Reserve(output_bytes);
      if (!reserved.ok()) return terminal_status_ = reserved;
      memory_peak_bytes_ = std::max(
          memory_peak_bytes_, memory_account_->used_bytes());
      output_retention = std::make_shared<QueryMemoryLease>(
          memory_account_, output_bytes);
    }
    std::shared_ptr<void> temporary_retention;
    if (memory_account_ && temporary_bytes != 0) {
      const Status reserved = memory_account_->Reserve(temporary_bytes);
      if (!reserved.ok()) return terminal_status_ = reserved;
      memory_peak_bytes_ = std::max(
          memory_peak_bytes_, memory_account_->used_bytes());
      temporary_retention = std::make_shared<QueryMemoryLease>(
          memory_account_, temporary_bytes);
    }
    const ResultValueKind kind = group_kinds_[slot.index];
    std::vector<std::vector<ResultValueCell>> rows;
    rows.reserve(end - next_group_);
    for (size_t row = next_group_; row < end; ++row) {
      std::vector<ResultValueCell> values;
      values.reserve(1);
      values.push_back(groups_[row].values[slot.index]);
      rows.push_back(std::move(values));
    }
    ResultBatch typed;
    const Status built = BuildResultBatchMoving(
        {group_names_[slot.index]}, {kind}, &rows, temporal_metadata_,
        &typed, output_retention);
    if (!built.ok()) return built;
    const Status added = grouped.AddVector(typed.batch().VectorAt(0));
    if (!added.ok()) return added;
    names.push_back(group_names_[slot.index]);
  }
  next_group_ = end;
  *batch = ResultBatch(std::move(names), std::move(grouped), temporal_metadata_);
  return batch->Validate();
}

Status LimitedResultStream::Next(ResultBatch* batch) {
  if (batch == nullptr) return Status::InvalidArgument("result stream", "missing batch output");
  if (!input_ || remaining_ == 0) return Status::NotFound("result stream", "limit reached");
  ResultBatch source;
  const Status next = input_->Next(&source);
  if (!next.ok()) return next;
  const uint32_t rows = static_cast<uint32_t>(std::min<uint64_t>(remaining_, source.batch().row_count()));
  remaining_ -= rows;
  return SliceResultBatch(source, 0, rows, batch);
}

Status SkipResultStream::Next(ResultBatch* batch) {
  if (batch == nullptr) return Status::InvalidArgument("result stream", "missing batch output");
  if (!input_) return Status::NotFound("result stream", "end of stream");
  while (true) {
    ResultBatch source;
    const Status next = input_->Next(&source);
    if (!next.ok()) return next;
    const uint32_t rows = source.batch().row_count();
    if (remaining_ >= rows) {
      remaining_ -= rows;
      continue;
    }
    const uint32_t start = static_cast<uint32_t>(remaining_);
    remaining_ = 0;
    return SliceResultBatch(source, start, rows - start, batch);
  }
}

DistinctResultStream::DistinctResultStream(
    std::unique_ptr<QueryResultStream> input,
    std::shared_ptr<QueryCancellation> cancellation,
    std::shared_ptr<QueryMemoryAccount> memory_account,
    std::string spill_directory,
    std::shared_ptr<ResourceGovernorExtension> spill_resources)
    : input_(std::move(input)), cancellation_(std::move(cancellation)),
      memory_account_(std::move(memory_account)),
      spill_directory_(std::move(spill_directory)),
      spill_resources_(std::move(spill_resources)) {}

void DistinctResultStream::ReleaseSeenRows() {
  if (memory_account_ && reserved_bytes_ != 0) {
    memory_account_->Release(reserved_bytes_);
  }
  reserved_bytes_ = 0;
  seen_rows_.clear();
}

Status DistinctResultStream::BeginSpill() {
  if (spilling_) return Status::OK();
  const std::string directory =
      spill_directory_.empty() ? "/tmp" : spill_directory_;
  emitted_keys_spill_ = std::make_unique<PartitionedSpillSet>(
      directory, kGroupedAggregateSpillPartitions, cancellation_,
      spill_resources_, memory_account_,
      [this](uint64_t bytes) { spill_bytes_ += bytes; });
  rows_spill_ = std::make_unique<PartitionedSpillSet>(
      directory, kGroupedAggregateSpillPartitions, cancellation_,
      spill_resources_, memory_account_,
      [this](uint64_t bytes) { spill_bytes_ += bytes; });
  Status status = emitted_keys_spill_->Open();
  if (!status.ok()) return status;
  status = rows_spill_->Open();
  if (!status.ok()) return status;
  for (const std::string& key : seen_rows_) {
    const uint32_t partition = static_cast<uint32_t>(
        StableResultKeyHash(key) % emitted_keys_spill_->partition_count());
    status = emitted_keys_spill_->AppendRecord(partition, key);
    if (!status.ok()) return status;
  }
  status = emitted_keys_spill_->Seal();
  if (!status.ok()) return status;
  ReleaseSeenRows();
  spilling_ = true;
  return Status::OK();
}

Status DistinctResultStream::DrainInputToSpill() {
  if (!spilling_ || !rows_spill_) {
    return Status::InvalidArgument("result stream", "DISTINCT spill is not open");
  }
  while (true) {
    if (cancellation_ && cancellation_->IsCancelled()) {
      return Status::QueryCancelled(
          "result stream", "query cancelled during DISTINCT spill");
    }
    if (!pending_batch_.has_value()) {
      ResultBatch source;
      const Status next = input_->Next(&source);
      if (next.IsNotFound()) {
        const Status terminal = ResultStreamTerminalAtEnd(input_.get());
        if (!terminal.ok()) return terminal;
        const Status sealed = rows_spill_->Seal();
        if (!sealed.ok()) return sealed;
        input_drained_ = true;
        return Status::OK();
      }
      if (!next.ok()) return next;
      pending_batch_ = std::move(source);
      pending_row_ = 0;
    }
    const ResultBatch& source = *pending_batch_;
    while (pending_row_ < source.batch().row_count()) {
      std::vector<ResultValueCell> candidate;
      candidate.reserve(source.batch().column_count());
      for (uint32_t column = 0; column < source.batch().column_count(); ++column) {
        candidate.push_back(ResultCellAt(source.batch(), column, pending_row_));
      }
      const std::string key = ResultRowKey(candidate);
      const uint32_t partition = static_cast<uint32_t>(
          StableResultKeyHash(key) % rows_spill_->partition_count());
      ResultBatch one_row;
      const Status selected =
          SelectResultRows(source, {pending_row_}, &one_row);
      if (!selected.ok()) return selected;
      const Status appended = rows_spill_->Append(partition, one_row);
      if (!appended.ok()) return appended;
      ++pending_row_;
    }
    pending_batch_.reset();
    pending_row_ = 0;
  }
}

StatusOr<bool> DistinctResultStream::OpenNextSpillPartition() {
  while (rows_spill_ && next_spill_partition_ < rows_spill_->partition_count()) {
    ReleaseSeenRows();
    const uint32_t partition = next_spill_partition_++;
    if (emitted_keys_spill_->HasData(partition)) {
      const Status rewound = emitted_keys_spill_->Rewind(partition);
      if (!rewound.ok()) return rewound;
      for (;;) {
        std::string key;
        const Status next = emitted_keys_spill_->NextRecord(partition, &key);
        if (next.IsNotFound()) break;
        if (!next.ok()) return next;
        if (memory_account_) {
          const uint64_t bytes = sizeof(std::string) + key.size();
          const Status reserved = memory_account_->Reserve(bytes);
          if (!reserved.ok()) return reserved;
          reserved_bytes_ += bytes;
          memory_peak_bytes_ = std::max(memory_peak_bytes_, reserved_bytes_);
        }
        seen_rows_.push_back(std::move(key));
      }
      const Status sealed = emitted_keys_spill_->Seal(partition);
      if (!sealed.ok()) return sealed;
    }
    if (!rows_spill_->HasData(partition)) continue;
    const Status rewound = rows_spill_->Rewind(partition);
    if (!rewound.ok()) return rewound;
    current_spill_partition_ = partition;
    spill_partition_open_ = true;
    return true;
  }
  ReleaseSeenRows();
  return false;
}

Status DistinctResultStream::NextSpilled(ResultBatch* batch) {
  if (!input_drained_) {
    const Status drained = DrainInputToSpill();
    if (!drained.ok()) return terminal_status_ = drained;
  }
  while (true) {
    if (cancellation_ && cancellation_->IsCancelled()) {
      return terminal_status_ = Status::QueryCancelled(
          "result stream", "query cancelled during DISTINCT spill replay");
    }
    if (!spill_partition_open_) {
      const auto opened = OpenNextSpillPartition();
      if (!opened.ok()) return terminal_status_ = opened.status();
      if (!opened.ValueOrDie()) {
        return Status::NotFound("result stream", "end of DISTINCT spill");
      }
    }
    ResultBatch source;
    const Status next = rows_spill_->Next(current_spill_partition_, &source);
    if (next.IsNotFound()) {
      const Status sealed = rows_spill_->Seal(current_spill_partition_);
      if (!sealed.ok()) return terminal_status_ = sealed;
      spill_partition_open_ = false;
      continue;
    }
    if (!next.ok()) return terminal_status_ = next;
    if (source.batch().row_count() != 1) {
      return terminal_status_ = Status::Corruption(
          "result stream", "DISTINCT spill row has invalid cardinality");
    }
    std::vector<ResultValueCell> candidate;
    candidate.reserve(source.batch().column_count());
    for (uint32_t column = 0; column < source.batch().column_count(); ++column) {
      candidate.push_back(ResultCellAt(source.batch(), column, 0));
    }
    const std::string key = ResultRowKey(candidate);
    if (std::find(seen_rows_.begin(), seen_rows_.end(), key) != seen_rows_.end()) {
      continue;
    }
    if (memory_account_) {
      const uint64_t bytes = sizeof(std::string) + key.size();
      const Status reserved = memory_account_->Reserve(bytes);
      if (!reserved.ok()) return terminal_status_ = reserved;
      reserved_bytes_ += bytes;
      memory_peak_bytes_ = std::max(memory_peak_bytes_, reserved_bytes_);
    }
    seen_rows_.push_back(key);
    *batch = std::move(source);
    return Status::OK();
  }
}

Status DistinctResultStream::Next(ResultBatch* batch) {
  if (batch == nullptr) return Status::InvalidArgument("result stream", "missing batch output");
  if (!input_) return Status::NotFound("result stream", "end of stream");
  while (true) {
    if (cancellation_ && cancellation_->IsCancelled()) {
      return terminal_status_ = Status::QueryCancelled(
          "result stream", "query cancelled during DISTINCT");
    }
    if (spilling_) return NextSpilled(batch);
    if (memory_account_ && memory_account_->ShouldSpill()) {
      const Status spilled = BeginSpill();
      if (!spilled.ok()) return terminal_status_ = spilled;
      return NextSpilled(batch);
    }
    if (!pending_batch_.has_value()) {
      ResultBatch source;
      const Status next = input_->Next(&source);
      if (!next.ok()) {
        if (!next.IsNotFound()) terminal_status_ = next;
        return next;
      }
      pending_batch_ = std::move(source);
      pending_row_ = 0;
    }
    const ResultBatch& source = *pending_batch_;
    std::vector<uint32_t> selected;
    uint64_t selected_output_bytes = 0;
    while (pending_row_ < source.batch().row_count()) {
      const uint32_t row = pending_row_++;
      std::vector<ResultValueCell> candidate;
      candidate.reserve(source.batch().column_count());
      for (uint32_t column = 0; column < source.batch().column_count(); ++column) {
        candidate.push_back(ResultCellAt(source.batch(), column, row));
      }
      const std::string key = ResultRowKey(candidate);
      if (std::find(seen_rows_.begin(), seen_rows_.end(), key) != seen_rows_.end()) continue;
      if (memory_account_) {
        const uint64_t key_bytes = sizeof(std::string) + key.size();
        const uint64_t row_bytes = ResultRowBytes(candidate);
        const uint64_t used = memory_account_->used_bytes();
        const uint64_t hard = memory_account_->hard_limit_bytes();
        const bool output_overflow =
            selected_output_bytes > std::numeric_limits<uint64_t>::max() -
                row_bytes;
        const uint64_t prospective_output = output_overflow
            ? std::numeric_limits<uint64_t>::max()
            : selected_output_bytes + row_bytes;
        const bool hard_overflow =
            key_bytes > hard || used > hard - key_bytes ||
            prospective_output > hard - used - key_bytes;
        if (hard_overflow && !selected.empty()) {
          --pending_row_;
          break;
        }
        if (hard_overflow) {
          return terminal_status_ = Status::QueryMemoryLimit(
              "result stream", "DISTINCT output row exceeds hard memory limit");
        }
        const Status reserved = memory_account_->Reserve(key_bytes);
        if (!reserved.ok()) return terminal_status_ = reserved;
        reserved_bytes_ += key_bytes;
        memory_peak_bytes_ = std::max(memory_peak_bytes_, reserved_bytes_);
        selected_output_bytes = prospective_output;
      }
      seen_rows_.push_back(key);
      selected.push_back(row);
      if (memory_account_ && memory_account_->ShouldSpill()) break;
    }
    if (!selected.empty()) {
      const Status selected_rows = SelectResultRows(
          source, selected, batch, memory_account_);
      if (!selected_rows.ok()) terminal_status_ = selected_rows;
      if (pending_row_ == source.batch().row_count()) {
        pending_batch_.reset();
        pending_row_ = 0;
      }
      return selected_rows;
    }
    pending_batch_.reset();
    pending_row_ = 0;
  }
}

DistinctResultStream::~DistinctResultStream() {
  ReleaseSeenRows();
}

SortResultStream::SortResultStream(
    std::unique_ptr<QueryResultStream> input, uint32_t column, bool descending,
    uint32_t batch_capacity, std::shared_ptr<QueryCancellation> cancellation,
    std::shared_ptr<QueryMemoryAccount> memory_account,
    std::string spill_directory,
    std::shared_ptr<ResourceGovernorExtension> spill_resources)
    : input_(std::move(input)), column_(column), descending_(descending),
      batch_capacity_(batch_capacity), cancellation_(std::move(cancellation)),
      memory_account_(std::move(memory_account)),
      spill_directory_(std::move(spill_directory)),
      spill_resources_(std::move(spill_resources)) {}

Status SortResultStream::CheckCancelled() const {
  return cancellation_ && cancellation_->IsCancelled()
      ? Status::QueryCancelled("result stream", "query cancelled during sort")
      : Status::OK();
}

void SortResultStream::ReleaseRows() {
  rows_.clear();
  if (memory_account_ && reserved_bytes_ != 0) {
    memory_account_->Release(reserved_bytes_);
  }
  reserved_bytes_ = 0;
  next_row_ = 0;
}

Status SortResultStream::AppendRowsToSpill(
    QuerySpillFile* spill,
    const std::vector<std::vector<ResultValueCell>>& rows,
    size_t start, size_t end) {
  if (spill == nullptr || start > end || end > rows.size()) {
    return Status::InvalidArgument(
        "result stream", "invalid sort spill row range");
  }
  if (start == end) return Status::OK();
  const Status cancelled = CheckCancelled();
  if (!cancelled.ok()) return cancelled;
  Status attempted = Status::OK();
  {
    auto lease = std::make_shared<QueryMemoryLease>(memory_account_, 0);
    attempted = lease->ReserveAdditional(ResultRowsBytes(rows, start, end));
    if (attempted.ok()) {
      ResultBatch batch;
      attempted = BuildResultBatch(
          column_names_, column_kinds_, rows, start, end,
          temporal_metadata_, &batch, lease);
      if (attempted.ok()) attempted = spill->Append(batch);
    }
  }
  if (!attempted.IsQueryMemoryLimit() || end - start == 1) return attempted;
  const size_t middle = start + (end - start) / 2;
  const Status first = AppendRowsToSpill(spill, rows, start, middle);
  if (!first.ok()) return first;
  return AppendRowsToSpill(spill, rows, middle, end);
}

Status SortResultStream::SpillCurrentRun() {
  if (rows_.empty()) return Status::OK();
  const Status cancelled = CheckCancelled();
  if (!cancelled.ok()) return cancelled;
  std::stable_sort(rows_.begin(), rows_.end(), [this](const auto& left, const auto& right) {
    const int comparison = CompareResultCells(left[column_], right[column_]);
    return descending_ ? comparison > 0 : comparison < 0;
  });

  const std::string directory = spill_directory_.empty() ? "/tmp" : spill_directory_;
  auto spill = std::make_unique<QuerySpillFile>(
      directory, cancellation_, spill_resources_, memory_account_,
      [this](uint64_t bytes) { spill_bytes_ += bytes; });
  Status status = spill->Open();
  if (!status.ok()) return status;
  const size_t output_capacity = std::min<size_t>(batch_capacity_, kTcypherStandardBatchCapacity);
  for (size_t start = 0; start < rows_.size(); start += output_capacity) {
    const size_t end = std::min(rows_.size(), start + output_capacity);
    std::vector<std::vector<ResultValueCell>> output_rows(
        rows_.begin() + static_cast<std::ptrdiff_t>(start),
        rows_.begin() + static_cast<std::ptrdiff_t>(end));
    status = AppendRowsToSpill(
        spill.get(), output_rows, 0, output_rows.size());
    if (!status.ok()) return status;
  }
  status = spill->Seal();
  if (!status.ok()) return status;
  ReleaseRows();
  spilling_ = true;
  spilled_runs_.push_back(std::move(spill));
  return Status::OK();
}

Status SortResultStream::LoadNextMergeBatch(
    std::vector<MergeCursor>* cursors, size_t cursor) {
  if (cursors == nullptr || cursor >= cursors->size()) {
    return Status::InvalidArgument("result stream", "invalid sort merge cursor");
  }
  MergeCursor& state = (*cursors)[cursor];
  while (true) {
    const Status cancelled = CheckCancelled();
    if (!cancelled.ok()) return cancelled;
    ResultBatch batch;
    const Status next = state.spill->Next(&batch);
    if (next.IsNotFound()) {
      state.has_row = false;
      return Status::OK();
    }
    if (!next.ok()) return next;
    if (batch.batch().row_count() == 0) continue;
    state.batch = std::move(batch);
    state.row = 0;
    state.has_row = true;
    return Status::OK();
  }
}

bool SortResultStream::CursorPrecedes(const std::vector<MergeCursor>& cursors,
                                      size_t left, size_t right) const {
  const MergeCursor& lhs = cursors[left];
  const MergeCursor& rhs = cursors[right];
  const int comparison = CompareResultCells(
      ResultCellAt(lhs.batch.batch(), column_, lhs.row),
      ResultCellAt(rhs.batch.batch(), column_, rhs.row));
  if (comparison == 0) return lhs.run_ordinal < rhs.run_ordinal;
  return descending_ ? comparison > 0 : comparison < 0;
}

StatusOr<std::unique_ptr<QuerySpillFile>> SortResultStream::MergeRunGroup(
    std::vector<std::unique_ptr<QuerySpillFile>>* runs, size_t begin, size_t end) {
  if (runs == nullptr || begin >= end || end > runs->size()) {
    return Status::InvalidArgument("result stream", "invalid sort merge run group");
  }
  const Status cancelled = CheckCancelled();
  if (!cancelled.ok()) return cancelled;
  const std::string directory = spill_directory_.empty() ? "/tmp" : spill_directory_;
  auto output = std::make_unique<QuerySpillFile>(
      directory, cancellation_, spill_resources_, memory_account_,
      [this](uint64_t bytes) { spill_bytes_ += bytes; });
  Status status = output->Open();
  if (!status.ok()) return status;

  std::vector<MergeCursor> cursors;
  cursors.reserve(end - begin);
  for (size_t index = begin; index < end; ++index) {
    MergeCursor cursor;
    cursor.spill = std::move((*runs)[index]);
    cursor.run_ordinal = static_cast<uint32_t>(index - begin);
    status = cursor.spill->Rewind();
    if (!status.ok()) return status;
    cursors.push_back(std::move(cursor));
    status = LoadNextMergeBatch(&cursors, cursors.size() - 1);
    if (!status.ok()) return status;
  }

  auto comes_after = [this, &cursors](size_t left, size_t right) {
    return CursorPrecedes(cursors, right, left);
  };
  std::vector<size_t> heap;
  for (size_t index = 0; index < cursors.size(); ++index) {
    if (!cursors[index].has_row) continue;
    heap.push_back(index);
    std::push_heap(heap.begin(), heap.end(), comes_after);
  }

  const size_t output_capacity = std::min<size_t>(batch_capacity_, kTcypherStandardBatchCapacity);
  const uint64_t output_byte_capacity = memory_account_
      ? std::max<uint64_t>(1, memory_account_->hard_limit_bytes() / 4)
      : std::numeric_limits<uint64_t>::max();
  std::vector<std::vector<ResultValueCell>> output_rows;
  output_rows.reserve(output_capacity);
  auto output_rows_lease =
      std::make_shared<QueryMemoryLease>(memory_account_, 0);
  uint64_t output_row_bytes = 0;
  while (!heap.empty()) {
    status = CheckCancelled();
    if (!status.ok()) return status;
    std::pop_heap(heap.begin(), heap.end(), comes_after);
    const size_t index = heap.back();
    heap.pop_back();
    MergeCursor& cursor = cursors[index];
    std::vector<ResultValueCell> row;
    row.reserve(cursor.batch.batch().column_count());
    for (uint32_t column = 0; column < cursor.batch.batch().column_count(); ++column) {
      row.push_back(ResultCellAt(cursor.batch.batch(), column, cursor.row));
    }
    const uint64_t row_bytes = ResultRowBytes(row);
    if (!output_rows.empty() &&
        (output_rows.size() == output_capacity ||
         row_bytes > output_byte_capacity -
             std::min(output_byte_capacity, output_row_bytes))) {
      status = AppendRowsToSpill(
          output.get(), output_rows, 0, output_rows.size());
      if (!status.ok()) return status;
      output_rows.clear();
      output_rows_lease =
          std::make_shared<QueryMemoryLease>(memory_account_, 0);
      output_row_bytes = 0;
    }
    status = output_rows_lease->ReserveAdditional(row_bytes);
    if (!status.ok()) return status;
    output_row_bytes += row_bytes;
    output_rows.push_back(std::move(row));
    if (++cursor.row == cursor.batch.batch().row_count()) {
      status = LoadNextMergeBatch(&cursors, index);
      if (!status.ok()) return status;
    }
    if (cursor.has_row) {
      heap.push_back(index);
      std::push_heap(heap.begin(), heap.end(), comes_after);
    }
    if (output_rows.size() == output_capacity) {
      status = AppendRowsToSpill(
          output.get(), output_rows, 0, output_rows.size());
      if (!status.ok()) return status;
      output_rows.clear();
      output_rows_lease =
          std::make_shared<QueryMemoryLease>(memory_account_, 0);
      output_row_bytes = 0;
    }
  }
  status = AppendRowsToSpill(
      output.get(), output_rows, 0, output_rows.size());
  if (!status.ok()) return status;
  status = output->Seal();
  if (!status.ok()) return status;
  return output;
}

Status SortResultStream::PrepareMerge() {
  constexpr size_t kMergeFanIn = 2;
  while (spilled_runs_.size() > kMergeFanIn) {
    const Status cancelled = CheckCancelled();
    if (!cancelled.ok()) return cancelled;
    std::vector<std::unique_ptr<QuerySpillFile>> next_pass;
    next_pass.reserve((spilled_runs_.size() + kMergeFanIn - 1) / kMergeFanIn);
    for (size_t begin = 0; begin < spilled_runs_.size(); begin += kMergeFanIn) {
      const size_t end = std::min(spilled_runs_.size(), begin + kMergeFanIn);
      auto merged = MergeRunGroup(&spilled_runs_, begin, end);
      if (!merged.ok()) return merged.status();
      next_pass.push_back(std::move(merged).ConsumeValueOrDie());
    }
    spilled_runs_ = std::move(next_pass);
  }

  merge_cursors_.reserve(spilled_runs_.size());
  for (size_t index = 0; index < spilled_runs_.size(); ++index) {
    MergeCursor cursor;
    cursor.spill = std::move(spilled_runs_[index]);
    cursor.run_ordinal = static_cast<uint32_t>(index);
    Status status = cursor.spill->Rewind();
    if (!status.ok()) return status;
    merge_cursors_.push_back(std::move(cursor));
    status = LoadNextMergeBatch(&merge_cursors_, merge_cursors_.size() - 1);
    if (!status.ok()) return status;
  }
  spilled_runs_.clear();
  auto comes_after = [this](size_t left, size_t right) {
    return CursorPrecedes(merge_cursors_, right, left);
  };
  for (size_t index = 0; index < merge_cursors_.size(); ++index) {
    if (!merge_cursors_[index].has_row) continue;
    merge_heap_.push_back(index);
    std::push_heap(merge_heap_.begin(), merge_heap_.end(), comes_after);
  }
  merge_ready_ = true;
  return Status::OK();
}

Status SortResultStream::Initialize() {
  if (initialized_) return terminal_status_;
  initialized_ = true;
  if (!input_ || batch_capacity_ == 0) {
    return terminal_status_ = Status::InvalidArgument(
        "result stream", "invalid sort input or capacity");
  }
  for (;;) {
    Status status = CheckCancelled();
    if (!status.ok()) return terminal_status_ = status;
    if (memory_account_ && memory_account_->ShouldSpill() && !rows_.empty()) {
      status = SpillCurrentRun();
      if (!status.ok()) return terminal_status_ = status;
    }
    ResultBatch source;
    const Status next = input_->Next(&source);
    if (next.IsNotFound()) {
      const Status terminal = ResultStreamTerminalAtEnd(input_.get());
      if (!terminal.ok()) return terminal_status_ = terminal;
      break;
    }
    if (!next.ok()) return terminal_status_ = next;
    if (column_ >= source.batch().column_count()) {
      return terminal_status_ = Status::BindError(
          "result stream", "ORDER BY column is not projected");
    }
    if (column_names_.empty()) {
      column_names_ = source.column_names();
      temporal_metadata_ = source.temporal_metadata();
      for (uint32_t column = 0; column < source.batch().column_count(); ++column) {
        column_kinds_.push_back(ResultColumnKind(source.batch(), column));
      }
    } else if (column_names_ != source.column_names()) {
      return terminal_status_ = Status::Corruption(
          "result stream", "inconsistent result columns");
    } else {
      for (uint32_t column = 0; column < source.batch().column_count(); ++column) {
        if (column_kinds_[column] != ResultColumnKind(source.batch(), column)) {
          return terminal_status_ = Status::Corruption(
              "result stream", "inconsistent result vector kind");
        }
      }
    }
    for (uint32_t row = 0; row < source.batch().row_count(); ++row) {
      status = CheckCancelled();
      if (!status.ok()) return terminal_status_ = status;
      if (memory_account_ && memory_account_->ShouldSpill() && !rows_.empty()) {
        status = SpillCurrentRun();
        if (!status.ok()) return terminal_status_ = status;
      }
      uint64_t bytes = sizeof(std::vector<ResultValueCell>) +
          static_cast<uint64_t>(source.batch().column_count()) * sizeof(ResultValueCell);
      for (uint32_t column = 0; column < source.batch().column_count(); ++column) {
        bytes += ResultCellBytes(ResultCellAt(source.batch(), column, row));
      }
      if (memory_account_) {
        status = memory_account_->Reserve(bytes);
        if (!status.ok() && !rows_.empty()) {
          status = SpillCurrentRun();
          if (!status.ok()) return terminal_status_ = status;
          status = memory_account_->Reserve(bytes);
        }
        if (!status.ok()) return terminal_status_ = status;
        reserved_bytes_ += bytes;
        memory_peak_bytes_ = std::max(memory_peak_bytes_, reserved_bytes_);
      }
      std::vector<ResultValueCell> values;
      values.reserve(source.batch().column_count());
      for (uint32_t column = 0; column < source.batch().column_count(); ++column) {
        values.push_back(ResultCellAt(source.batch(), column, row));
      }
      rows_.push_back(std::move(values));
    }
  }
  if (spilling_) {
    const Status status = SpillCurrentRun();
    if (!status.ok()) return terminal_status_ = status;
    const Status merged = PrepareMerge();
    if (!merged.ok()) return terminal_status_ = merged;
    return Status::OK();
  }
  std::stable_sort(rows_.begin(), rows_.end(), [this](const auto& left, const auto& right) {
    const int comparison = CompareResultCells(left[column_], right[column_]);
    return descending_ ? comparison > 0 : comparison < 0;
  });
  return Status::OK();
}

Status SortResultStream::NextMerged(ResultBatch* batch) {
  if (!merge_ready_) {
    return Status::InvalidArgument("result stream", "sort merge is not initialized");
  }
  if (merge_heap_.empty()) {
    merge_cursors_.clear();
    return Status::NotFound("result stream", "end of stream");
  }
  auto comes_after = [this](size_t left, size_t right) {
    return CursorPrecedes(merge_cursors_, right, left);
  };
  std::vector<std::vector<ResultValueCell>> output_rows;
  output_rows.reserve(batch_capacity_);
  const uint64_t output_byte_capacity = memory_account_
      ? std::max<uint64_t>(1, memory_account_->hard_limit_bytes() / 4)
      : std::numeric_limits<uint64_t>::max();
  auto output_rows_lease =
      std::make_shared<QueryMemoryLease>(memory_account_, 0);
  uint64_t output_row_bytes = 0;
  while (!merge_heap_.empty() && output_rows.size() < batch_capacity_) {
    Status status = CheckCancelled();
    if (!status.ok()) return terminal_status_ = status;
    std::pop_heap(merge_heap_.begin(), merge_heap_.end(), comes_after);
    const size_t index = merge_heap_.back();
    merge_heap_.pop_back();
    MergeCursor& cursor = merge_cursors_[index];
    std::vector<ResultValueCell> row;
    row.reserve(cursor.batch.batch().column_count());
    for (uint32_t column = 0; column < cursor.batch.batch().column_count(); ++column) {
      row.push_back(ResultCellAt(cursor.batch.batch(), column, cursor.row));
    }
    const uint64_t row_bytes = ResultRowBytes(row);
    if (!output_rows.empty() &&
        row_bytes > output_byte_capacity -
            std::min(output_byte_capacity, output_row_bytes)) {
      merge_heap_.push_back(index);
      std::push_heap(merge_heap_.begin(), merge_heap_.end(), comes_after);
      break;
    }
    status = output_rows_lease->ReserveAdditional(row_bytes);
    if (!status.ok()) return terminal_status_ = status;
    output_row_bytes += row_bytes;
    output_rows.push_back(std::move(row));
    if (++cursor.row == cursor.batch.batch().row_count()) {
      status = LoadNextMergeBatch(&merge_cursors_, index);
      if (!status.ok()) return terminal_status_ = status;
    }
    if (cursor.has_row) {
      merge_heap_.push_back(index);
      std::push_heap(merge_heap_.begin(), merge_heap_.end(), comes_after);
    }
  }
  auto batch_lease = std::make_shared<QueryMemoryLease>(memory_account_, 0);
  Status built = batch_lease->ReserveAdditional(
      ResultRowsBytes(output_rows, 0, output_rows.size()));
  if (built.ok()) {
    built = BuildResultBatch(
        column_names_, column_kinds_, output_rows, 0, output_rows.size(),
        temporal_metadata_, batch, batch_lease);
  }
  if (!built.ok()) return terminal_status_ = built;
  if (merge_heap_.empty()) merge_cursors_.clear();
  return Status::OK();
}

SortResultStream::~SortResultStream() { ReleaseRows(); }

Status SortResultStream::Next(ResultBatch* batch) {
  if (batch == nullptr) return Status::InvalidArgument("result stream", "missing batch output");
  const Status initialized = Initialize();
  if (!initialized.ok()) return initialized;
  const Status cancelled = CheckCancelled();
  if (!cancelled.ok()) return terminal_status_ = cancelled;
  if (spilling_) return NextMerged(batch);
  if (next_row_ == rows_.size()) return Status::NotFound("result stream", "end of stream");
  const size_t end = std::min(rows_.size(), next_row_ + batch_capacity_);
  auto batch_lease = std::make_shared<QueryMemoryLease>(memory_account_, 0);
  Status built = batch_lease->ReserveAdditional(
      ResultRowsBytes(rows_, next_row_, end));
  if (built.ok()) {
    built = BuildResultBatch(
        column_names_, column_kinds_, rows_, next_row_, end,
        temporal_metadata_, batch, batch_lease);
  }
  if (!built.ok()) return terminal_status_ = built;
  next_row_ = end;
  return Status::OK();
}

Status CountResultStream::Next(ResultBatch* batch) {
  if (batch == nullptr) return Status::InvalidArgument("result stream", "missing batch output");
  if (emitted_) return Status::NotFound("result stream", "end of stream");
  if (!input_) return Status::InvalidArgument("result stream", "missing aggregate input");
  uint64_t count = 0;
  for (;;) {
    ResultBatch source;
    const Status next = input_->Next(&source);
    if (next.IsNotFound()) {
      const Status terminal = ResultStreamTerminalAtEnd(input_.get());
      if (!terminal.ok()) return terminal_status_ = terminal;
      break;
    }
    if (!next.ok()) {
      terminal_status_ = next;
      return next;
    }
    count += source.batch().row_count();
  }
  if (count > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
    return Status::InvalidArgument("result stream", "COUNT exceeds Int64");
  }
  ColumnBatch values(1);
  const Status added = values.AddVector(std::make_shared<FlatVector>(
      std::vector<Value>{Value::Int64(static_cast<int64_t>(count))}, std::vector<bool>{}));
  if (!added.ok()) return added;
  *batch = ResultBatch({column_name_}, std::move(values));
  emitted_ = true;
  return batch->Validate();
}

Status GroupedCountResultStream::Initialize() {
  if (initialized_) return terminal_status_;
  initialized_ = true;
  if (!input_ || batch_capacity_ == 0) {
    terminal_status_ = Status::InvalidArgument("result stream", "invalid grouped COUNT input or capacity");
    return terminal_status_;
  }
  for (;;) {
    ResultBatch source;
    const Status next = input_->Next(&source);
    if (next.IsNotFound()) {
      const Status terminal = ResultStreamTerminalAtEnd(input_.get());
      if (!terminal.ok()) return terminal_status_ = terminal;
      break;
    }
    if (!next.ok()) {
      terminal_status_ = next;
      return next;
    }
    if (count_column_ >= source.batch().column_count()) {
      terminal_status_ = Status::InvalidArgument("result stream", "COUNT input column is absent");
      return terminal_status_;
    }
    for (uint32_t column : group_columns_) {
      if (column >= source.batch().column_count()) {
        terminal_status_ = Status::InvalidArgument("result stream", "grouping column is absent");
        return terminal_status_;
      }
    }
    if (group_names_.empty()) {
      for (uint32_t column : group_columns_) group_names_.push_back(source.column_names()[column]);
      temporal_metadata_ = source.temporal_metadata();
    }
    for (uint32_t row = 0; row < source.batch().row_count(); ++row) {
      if (!source.batch().ValueAt(count_column_, row).has_value()) continue;
      std::vector<std::optional<Value>> values;
      values.reserve(group_columns_.size());
      for (uint32_t column : group_columns_) values.push_back(source.batch().ValueAt(column, row));
      auto group = std::find_if(groups_.begin(), groups_.end(), [&values](const Group& candidate) {
        return candidate.values == values;
      });
      if (group == groups_.end()) {
        groups_.push_back(Group{std::move(values), 1});
      } else {
        if (group->count == std::numeric_limits<uint64_t>::max()) {
          terminal_status_ = Status::InvalidArgument("result stream", "COUNT exceeds UInt64");
          return terminal_status_;
        }
        ++group->count;
      }
    }
  }
  std::stable_sort(groups_.begin(), groups_.end(), [](const Group& left, const Group& right) {
    return GroupValuesLess(left.values, right.values);
  });
  return Status::OK();
}

Status GroupedCountResultStream::Next(ResultBatch* batch) {
  if (batch == nullptr) return Status::InvalidArgument("result stream", "missing batch output");
  const Status initialized = Initialize();
  if (!initialized.ok()) return initialized;
  if (next_group_ == groups_.size()) return Status::NotFound("result stream", "end of stream");
  const size_t end = std::min(groups_.size(), next_group_ + batch_capacity_);
  ColumnBatch grouped(static_cast<uint32_t>(end - next_group_));
  std::vector<std::vector<std::optional<Value>>> group_values;
  group_values.reserve(groups_.size());
  for (const Group& group : groups_) group_values.push_back(group.values);
  const Status columns = AppendGroupedColumns(group_columns_, group_names_, group_values,
                                              next_group_, end, &grouped);
  if (!columns.ok()) return columns;
  std::vector<Value> counts;
  counts.reserve(end - next_group_);
  for (size_t row = next_group_; row < end; ++row) {
    if (groups_[row].count > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
      return Status::InvalidArgument("result stream", "COUNT exceeds Int64");
    }
    counts.push_back(Value::Int64(static_cast<int64_t>(groups_[row].count)));
  }
  const Status added = grouped.AddVector(
      std::make_shared<FlatVector>(std::move(counts), std::vector<bool>{}));
  if (!added.ok()) return added;
  std::vector<std::string> names = group_names_;
  names.push_back(count_name_);
  next_group_ = end;
  *batch = ResultBatch(std::move(names), std::move(grouped), temporal_metadata_);
  return batch->Validate();
}

Status GroupedSumResultStream::Initialize() {
  if (initialized_) return terminal_status_;
  initialized_ = true;
  if (!input_ || batch_capacity_ == 0) {
    terminal_status_ = Status::InvalidArgument("result stream", "invalid grouped SUM input or capacity");
    return terminal_status_;
  }
  for (;;) {
    ResultBatch source;
    const Status next = input_->Next(&source);
    if (next.IsNotFound()) {
      const Status terminal = ResultStreamTerminalAtEnd(input_.get());
      if (!terminal.ok()) return terminal_status_ = terminal;
      break;
    }
    if (!next.ok()) {
      terminal_status_ = next;
      return next;
    }
    if (sum_column_ >= source.batch().column_count()) {
      terminal_status_ = Status::InvalidArgument("result stream", "SUM input column is absent");
      return terminal_status_;
    }
    for (uint32_t column : group_columns_) {
      if (column >= source.batch().column_count()) {
        terminal_status_ = Status::InvalidArgument("result stream", "grouping column is absent");
        return terminal_status_;
      }
    }
    if (group_names_.empty()) {
      for (uint32_t column : group_columns_) group_names_.push_back(source.column_names()[column]);
      temporal_metadata_ = source.temporal_metadata();
    }
    for (uint32_t row = 0; row < source.batch().row_count(); ++row) {
      std::vector<std::optional<Value>> values;
      values.reserve(group_columns_.size());
      for (uint32_t column : group_columns_) values.push_back(source.batch().ValueAt(column, row));
      auto group = std::find_if(groups_.begin(), groups_.end(), [&values](const Group& candidate) {
        return candidate.values == values;
      });
      if (group == groups_.end()) {
        groups_.push_back(Group{std::move(values), 0, false});
        group = std::prev(groups_.end());
      }
      const auto value = source.batch().ValueAt(sum_column_, row);
      if (!value.has_value()) continue;
      int64_t addend = 0;
      if (value->type() == PhysicalType::kInt32) {
        addend = static_cast<int64_t>(std::get<int32_t>(value->data()));
      } else if (value->type() == PhysicalType::kInt64) {
        addend = std::get<int64_t>(value->data());
      } else {
        terminal_status_ = Status::SchemaMismatch("result stream", "SUM requires Int32 or Int64 values");
        return terminal_status_;
      }
      if ((addend > 0 && group->sum > std::numeric_limits<int64_t>::max() - addend) ||
          (addend < 0 && group->sum < std::numeric_limits<int64_t>::min() - addend)) {
        terminal_status_ = Status::InvalidArgument("result stream", "SUM exceeds Int64");
        return terminal_status_;
      }
      group->sum += addend;
      group->has_value = true;
    }
  }
  std::stable_sort(groups_.begin(), groups_.end(), [](const Group& left, const Group& right) {
    return GroupValuesLess(left.values, right.values);
  });
  return Status::OK();
}

Status GroupedSumResultStream::Next(ResultBatch* batch) {
  if (batch == nullptr) return Status::InvalidArgument("result stream", "missing batch output");
  const Status initialized = Initialize();
  if (!initialized.ok()) return initialized;
  if (next_group_ == groups_.size()) return Status::NotFound("result stream", "end of stream");
  const size_t end = std::min(groups_.size(), next_group_ + batch_capacity_);
  ColumnBatch grouped(static_cast<uint32_t>(end - next_group_));
  std::vector<std::vector<std::optional<Value>>> group_values;
  group_values.reserve(groups_.size());
  for (const Group& group : groups_) group_values.push_back(group.values);
  const Status columns = AppendGroupedColumns(group_columns_, group_names_, group_values,
                                              next_group_, end, &grouped);
  if (!columns.ok()) return columns;
  std::vector<Value> sums;
  std::vector<bool> validity;
  sums.reserve(end - next_group_);
  validity.reserve(end - next_group_);
  for (size_t row = next_group_; row < end; ++row) {
    validity.push_back(groups_[row].has_value);
    sums.push_back(groups_[row].has_value ? Value::Int64(groups_[row].sum) : Value::Binary(""));
  }
  const Status added = grouped.AddVector(
      std::make_shared<FlatVector>(std::move(sums), std::move(validity)));
  if (!added.ok()) return added;
  std::vector<std::string> names = group_names_;
  names.push_back(sum_name_);
  next_group_ = end;
  *batch = ResultBatch(std::move(names), std::move(grouped), temporal_metadata_);
  return batch->Validate();
}

Status GroupedAvgResultStream::Initialize() {
  if (initialized_) return terminal_status_;
  initialized_ = true;
  if (!input_ || batch_capacity_ == 0) {
    terminal_status_ = Status::InvalidArgument("result stream", "invalid grouped AVG input or capacity");
    return terminal_status_;
  }
  for (;;) {
    ResultBatch source;
    const Status next = input_->Next(&source);
    if (next.IsNotFound()) {
      const Status terminal = ResultStreamTerminalAtEnd(input_.get());
      if (!terminal.ok()) return terminal_status_ = terminal;
      break;
    }
    if (!next.ok()) return terminal_status_ = next;
    if (avg_column_ >= source.batch().column_count()) {
      return terminal_status_ = Status::InvalidArgument("result stream", "AVG input column is absent");
    }
    for (uint32_t column : group_columns_) {
      if (column >= source.batch().column_count()) {
        return terminal_status_ = Status::InvalidArgument("result stream", "grouping column is absent");
      }
    }
    if (group_names_.empty()) {
      for (uint32_t column : group_columns_) group_names_.push_back(source.column_names()[column]);
      temporal_metadata_ = source.temporal_metadata();
    }
    for (uint32_t row = 0; row < source.batch().row_count(); ++row) {
      std::vector<std::optional<Value>> values;
      values.reserve(group_columns_.size());
      for (uint32_t column : group_columns_) values.push_back(source.batch().ValueAt(column, row));
      auto group = std::find_if(groups_.begin(), groups_.end(), [&values](const Group& candidate) {
        return candidate.values == values;
      });
      if (group == groups_.end()) {
        groups_.push_back(Group{std::move(values)});
        group = std::prev(groups_.end());
      }
      const auto value = source.batch().ValueAt(avg_column_, row);
      if (!value.has_value()) continue;
      switch (value->type()) {
        case PhysicalType::kInt32: group->sum += std::get<int32_t>(value->data()); break;
        case PhysicalType::kInt64: group->sum += std::get<int64_t>(value->data()); break;
        case PhysicalType::kFloat32: group->sum += std::get<float>(value->data()); break;
        case PhysicalType::kFloat64: group->sum += std::get<double>(value->data()); break;
        default:
          return terminal_status_ = Status::SchemaMismatch("result stream", "AVG requires numeric values");
      }
      if (group->count == std::numeric_limits<uint64_t>::max()) {
        return terminal_status_ = Status::InvalidArgument("result stream", "AVG value count exceeds UInt64");
      }
      ++group->count;
    }
  }
  std::stable_sort(groups_.begin(), groups_.end(), [](const Group& left, const Group& right) {
    return GroupValuesLess(left.values, right.values);
  });
  return Status::OK();
}

Status GroupedAvgResultStream::Next(ResultBatch* batch) {
  if (batch == nullptr) return Status::InvalidArgument("result stream", "missing batch output");
  const Status initialized = Initialize();
  if (!initialized.ok()) return initialized;
  if (next_group_ == groups_.size()) return Status::NotFound("result stream", "end of stream");
  const size_t end = std::min(groups_.size(), next_group_ + batch_capacity_);
  ColumnBatch grouped(static_cast<uint32_t>(end - next_group_));
  std::vector<std::vector<std::optional<Value>>> group_values;
  group_values.reserve(groups_.size());
  for (const Group& group : groups_) group_values.push_back(group.values);
  const Status columns = AppendGroupedColumns(group_columns_, group_names_, group_values,
                                              next_group_, end, &grouped);
  if (!columns.ok()) return columns;
  std::vector<Value> averages;
  std::vector<bool> validity;
  averages.reserve(end - next_group_);
  validity.reserve(end - next_group_);
  for (size_t row = next_group_; row < end; ++row) {
    validity.push_back(groups_[row].count != 0);
    averages.push_back(groups_[row].count == 0 ? Value::Binary("")
        : Value::Float64(static_cast<double>(groups_[row].sum / groups_[row].count)));
  }
  const Status added = grouped.AddVector(
      std::make_shared<FlatVector>(std::move(averages), std::move(validity)));
  if (!added.ok()) return added;
  std::vector<std::string> names = group_names_;
  names.push_back(avg_name_);
  next_group_ = end;
  *batch = ResultBatch(std::move(names), std::move(grouped), temporal_metadata_);
  return batch->Validate();
}

Status GroupedExtremaResultStream::Initialize() {
  if (initialized_) return terminal_status_;
  initialized_ = true;
  if (!input_ || batch_capacity_ == 0) {
    terminal_status_ = Status::InvalidArgument("result stream", "invalid grouped MIN/MAX input or capacity");
    return terminal_status_;
  }
  for (;;) {
    ResultBatch source;
    const Status next = input_->Next(&source);
    if (next.IsNotFound()) {
      const Status terminal = ResultStreamTerminalAtEnd(input_.get());
      if (!terminal.ok()) return terminal_status_ = terminal;
      break;
    }
    if (!next.ok()) return terminal_status_ = next;
    if (value_column_ >= source.batch().column_count()) {
      return terminal_status_ = Status::InvalidArgument("result stream", "MIN/MAX input column is absent");
    }
    for (uint32_t column : group_columns_) {
      if (column >= source.batch().column_count()) {
        return terminal_status_ = Status::InvalidArgument("result stream", "grouping column is absent");
      }
    }
    if (group_names_.empty()) {
      for (uint32_t column : group_columns_) group_names_.push_back(source.column_names()[column]);
      temporal_metadata_ = source.temporal_metadata();
    }
    for (uint32_t row = 0; row < source.batch().row_count(); ++row) {
      std::vector<std::optional<Value>> values;
      values.reserve(group_columns_.size());
      for (uint32_t column : group_columns_) values.push_back(source.batch().ValueAt(column, row));
      auto group = std::find_if(groups_.begin(), groups_.end(), [&values](const Group& candidate) {
        return candidate.values == values;
      });
      if (group == groups_.end()) {
        groups_.push_back(Group{std::move(values), std::nullopt});
        group = std::prev(groups_.end());
      }
      const auto value = source.batch().ValueAt(value_column_, row);
      if (!value.has_value() || (group->selected.has_value() &&
          (minimum_ ? CompareResultValues(*value, *group->selected) >= 0
                    : CompareResultValues(*value, *group->selected) <= 0))) {
        continue;
      }
      group->selected = value;
    }
  }
  std::stable_sort(groups_.begin(), groups_.end(), [](const Group& left, const Group& right) {
    return GroupValuesLess(left.values, right.values);
  });
  return Status::OK();
}

Status GroupedExtremaResultStream::Next(ResultBatch* batch) {
  if (batch == nullptr) return Status::InvalidArgument("result stream", "missing batch output");
  const Status initialized = Initialize();
  if (!initialized.ok()) return initialized;
  if (next_group_ == groups_.size()) return Status::NotFound("result stream", "end of stream");
  const size_t end = std::min(groups_.size(), next_group_ + batch_capacity_);
  ColumnBatch grouped(static_cast<uint32_t>(end - next_group_));
  std::vector<std::vector<std::optional<Value>>> group_values;
  group_values.reserve(groups_.size());
  for (const Group& group : groups_) group_values.push_back(group.values);
  const Status columns = AppendGroupedColumns(group_columns_, group_names_, group_values,
                                              next_group_, end, &grouped);
  if (!columns.ok()) return columns;
  std::vector<Value> extrema;
  std::vector<bool> validity;
  extrema.reserve(end - next_group_);
  validity.reserve(end - next_group_);
  for (size_t row = next_group_; row < end; ++row) {
    validity.push_back(groups_[row].selected.has_value());
    extrema.push_back(groups_[row].selected.value_or(Value::Binary("")));
  }
  const Status added = grouped.AddVector(
      std::make_shared<FlatVector>(std::move(extrema), std::move(validity)));
  if (!added.ok()) return added;
  std::vector<std::string> names = group_names_;
  names.push_back(value_name_);
  next_group_ = end;
  *batch = ResultBatch(std::move(names), std::move(grouped), temporal_metadata_);
  return batch->Validate();
}

Status SumResultStream::Next(ResultBatch* batch) {
  if (batch == nullptr) return Status::InvalidArgument("result stream", "missing batch output");
  if (emitted_) return Status::NotFound("result stream", "end of stream");
  if (!input_) return Status::InvalidArgument("result stream", "missing aggregate input");
  int64_t sum = 0;
  bool has_value = false;
  for (;;) {
    ResultBatch source;
    const Status next = input_->Next(&source);
    if (next.IsNotFound()) {
      const Status terminal = ResultStreamTerminalAtEnd(input_.get());
      if (!terminal.ok()) return terminal_status_ = terminal;
      break;
    }
    if (!next.ok()) {
      terminal_status_ = next;
      return next;
    }
    if (source.batch().column_count() != 1) {
      return Status::InvalidArgument("result stream", "SUM requires one projected input column");
    }
    for (uint32_t row = 0; row < source.batch().row_count(); ++row) {
      const auto value = source.batch().ValueAt(0, row);
      if (!value.has_value()) continue;
      int64_t addend = 0;
      if (value->type() == PhysicalType::kInt32) {
        addend = static_cast<int64_t>(std::get<int32_t>(value->data()));
      } else if (value->type() == PhysicalType::kInt64) {
        addend = std::get<int64_t>(value->data());
      } else {
        return Status::SchemaMismatch("result stream", "SUM requires Int32 or Int64 values");
      }
      if ((addend > 0 && sum > std::numeric_limits<int64_t>::max() - addend) ||
          (addend < 0 && sum < std::numeric_limits<int64_t>::min() - addend)) {
        return Status::InvalidArgument("result stream", "SUM exceeds Int64");
      }
      sum += addend;
      has_value = true;
    }
  }
  ColumnBatch values(1);
  const Status added = values.AddVector(std::make_shared<FlatVector>(
      std::vector<Value>{has_value ? Value::Int64(sum) : Value::Binary("")},
      std::vector<bool>{has_value}));
  if (!added.ok()) return added;
  *batch = ResultBatch({column_name_}, std::move(values));
  emitted_ = true;
  return batch->Validate();
}

Status AvgResultStream::Next(ResultBatch* batch) {
  if (batch == nullptr) return Status::InvalidArgument("result stream", "missing batch output");
  if (emitted_) return Status::NotFound("result stream", "end of stream");
  if (!input_) return Status::InvalidArgument("result stream", "missing aggregate input");
  long double sum = 0;
  uint64_t count = 0;
  for (;;) {
    ResultBatch source;
    const Status next = input_->Next(&source);
    if (next.IsNotFound()) {
      const Status terminal = ResultStreamTerminalAtEnd(input_.get());
      if (!terminal.ok()) return terminal_status_ = terminal;
      break;
    }
    if (!next.ok()) {
      terminal_status_ = next;
      return next;
    }
    if (source.batch().column_count() != 1) {
      return Status::InvalidArgument("result stream", "AVG requires one projected input column");
    }
    for (uint32_t row = 0; row < source.batch().row_count(); ++row) {
      const auto value = source.batch().ValueAt(0, row);
      if (!value.has_value()) continue;
      switch (value->type()) {
        case PhysicalType::kInt32: sum += std::get<int32_t>(value->data()); break;
        case PhysicalType::kInt64: sum += std::get<int64_t>(value->data()); break;
        case PhysicalType::kFloat32: sum += std::get<float>(value->data()); break;
        case PhysicalType::kFloat64: sum += std::get<double>(value->data()); break;
        default:
          return Status::SchemaMismatch("result stream", "AVG requires numeric values");
      }
      if (count == std::numeric_limits<uint64_t>::max()) {
        return Status::InvalidArgument("result stream", "AVG value count exceeds UInt64");
      }
      ++count;
    }
  }
  const bool has_value = count != 0;
  ColumnBatch values(1);
  const Status added = values.AddVector(std::make_shared<FlatVector>(
      std::vector<Value>{has_value ? Value::Float64(static_cast<double>(sum / count))
                                   : Value::Binary("")},
      std::vector<bool>{has_value}));
  if (!added.ok()) return added;
  *batch = ResultBatch({column_name_}, std::move(values));
  emitted_ = true;
  return batch->Validate();
}

Status ExtremaResultStream::Next(ResultBatch* batch) {
  if (batch == nullptr) return Status::InvalidArgument("result stream", "missing batch output");
  if (emitted_) return Status::NotFound("result stream", "end of stream");
  if (!input_) return Status::InvalidArgument("result stream", "missing aggregate input");
  std::optional<Value> selected;
  for (;;) {
    ResultBatch source;
    const Status next = input_->Next(&source);
    if (next.IsNotFound()) {
      const Status terminal = ResultStreamTerminalAtEnd(input_.get());
      if (!terminal.ok()) return terminal_status_ = terminal;
      break;
    }
    if (!next.ok()) {
      terminal_status_ = next;
      return next;
    }
    if (source.batch().column_count() != 1) {
      return Status::InvalidArgument("result stream", "MIN/MAX requires one projected input column");
    }
    for (uint32_t row = 0; row < source.batch().row_count(); ++row) {
      const auto value = source.batch().ValueAt(0, row);
      if (!value.has_value()) continue;
      if (!selected.has_value() ||
          (minimum_ ? CompareResultValues(*value, *selected) < 0
                    : CompareResultValues(*value, *selected) > 0)) {
        selected = value;
      }
    }
  }
  ColumnBatch values(1);
  const Status added = values.AddVector(std::make_shared<FlatVector>(
      std::vector<Value>{selected.value_or(Value::Binary(""))},
      std::vector<bool>{selected.has_value()}));
  if (!added.ok()) return added;
  *batch = ResultBatch({column_name_}, std::move(values));
  emitted_ = true;
  return batch->Validate();
}

Status MultiAggregateResultStream::Initialize() {
  if (initialized_) return terminal_status_;
  initialized_ = true;
  if (!input_ || aggregates_.empty()) {
    return terminal_status_ = Status::InvalidArgument("result stream", "missing multi-aggregate input");
  }
  states_.resize(aggregates_.size());
  for (;;) {
    ResultBatch source;
    const Status next = input_->Next(&source);
    if (next.IsNotFound()) {
      const Status terminal = ResultStreamTerminalAtEnd(input_.get());
      if (!terminal.ok()) return terminal_status_ = terminal;
      break;
    }
    if (!next.ok()) return terminal_status_ = next;
    for (size_t index = 0; index < aggregates_.size(); ++index) {
      if (aggregates_[index].input_column >= source.batch().column_count()) {
        return terminal_status_ = Status::InvalidArgument("result stream", "aggregate input column is absent");
      }
    }
    for (uint32_t row = 0; row < source.batch().row_count(); ++row) {
      for (size_t index = 0; index < aggregates_.size(); ++index) {
        const ResultAggregateSpec& aggregate = aggregates_[index];
        State& state = states_[index];
        const auto value = source.batch().ValueAt(aggregate.input_column, row);
        if (!value.has_value()) continue;
        if (aggregate.kind == ResultAggregateKind::kCount) {
          if (state.count == std::numeric_limits<uint64_t>::max()) {
            return terminal_status_ = Status::InvalidArgument("result stream", "COUNT exceeds UInt64");
          }
          ++state.count;
          continue;
        }
        if (aggregate.kind == ResultAggregateKind::kSum) {
          int64_t addend = 0;
          if (value->type() == PhysicalType::kInt32) {
            addend = std::get<int32_t>(value->data());
          } else if (value->type() == PhysicalType::kInt64) {
            addend = std::get<int64_t>(value->data());
          } else {
            return terminal_status_ = Status::SchemaMismatch("result stream", "SUM requires Int32 or Int64 values");
          }
          if ((addend > 0 && state.sum > std::numeric_limits<int64_t>::max() - addend) ||
              (addend < 0 && state.sum < std::numeric_limits<int64_t>::min() - addend)) {
            return terminal_status_ = Status::InvalidArgument("result stream", "SUM exceeds Int64");
          }
          state.sum += addend;
          state.has_sum = true;
          continue;
        }
        if (aggregate.kind == ResultAggregateKind::kAvg) {
          switch (value->type()) {
            case PhysicalType::kInt32: state.average_sum += std::get<int32_t>(value->data()); break;
            case PhysicalType::kInt64: state.average_sum += std::get<int64_t>(value->data()); break;
            case PhysicalType::kFloat32: state.average_sum += std::get<float>(value->data()); break;
            case PhysicalType::kFloat64: state.average_sum += std::get<double>(value->data()); break;
            default:
              return terminal_status_ = Status::SchemaMismatch("result stream", "AVG requires numeric values");
          }
          if (state.average_count == std::numeric_limits<uint64_t>::max()) {
            return terminal_status_ = Status::InvalidArgument("result stream", "AVG value count exceeds UInt64");
          }
          ++state.average_count;
          continue;
        }
        if (!state.extrema.has_value() ||
            (aggregate.kind == ResultAggregateKind::kMin
                 ? CompareResultValues(*value, *state.extrema) < 0
                 : CompareResultValues(*value, *state.extrema) > 0)) {
          state.extrema = value;
        }
      }
    }
  }
  return Status::OK();
}

Status MultiAggregateResultStream::Next(ResultBatch* batch) {
  if (batch == nullptr) return Status::InvalidArgument("result stream", "missing batch output");
  const Status initialized = Initialize();
  if (!initialized.ok()) return initialized;
  if (emitted_) return Status::NotFound("result stream", "end of stream");
  ColumnBatch values(1);
  std::vector<std::string> names;
  names.reserve(aggregates_.size());
  for (size_t index = 0; index < aggregates_.size(); ++index) {
    const ResultAggregateSpec& aggregate = aggregates_[index];
    const State& state = states_[index];
    std::optional<Value> value;
    switch (aggregate.kind) {
      case ResultAggregateKind::kCount:
        if (state.count > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
          return Status::InvalidArgument("result stream", "COUNT exceeds Int64");
        }
        value = Value::Int64(static_cast<int64_t>(state.count));
        break;
      case ResultAggregateKind::kSum:
        if (state.has_sum) value = Value::Int64(state.sum);
        break;
      case ResultAggregateKind::kAvg:
        if (state.average_count != 0) {
          value = Value::Float64(static_cast<double>(state.average_sum / state.average_count));
        }
        break;
      case ResultAggregateKind::kMin:
      case ResultAggregateKind::kMax:
        value = state.extrema;
        break;
    }
    const Status added = values.AddVector(std::make_shared<FlatVector>(
        std::vector<Value>{value.value_or(Value::Binary(""))}, std::vector<bool>{value.has_value()}));
    if (!added.ok()) return added;
    names.push_back(aggregate.column_name);
  }
  *batch = ResultBatch(std::move(names), std::move(values));
  emitted_ = true;
  return batch->Validate();
}

GroupedMultiAggregateResultStream::GroupedMultiAggregateResultStream(
    std::unique_ptr<QueryResultStream> input,
    std::vector<uint32_t> group_columns,
    std::vector<ResultAggregateSpec> aggregates,
    std::vector<ResultOutputSlot> output_slots, uint32_t batch_capacity,
    std::shared_ptr<QueryCancellation> cancellation,
    std::shared_ptr<QueryMemoryAccount> memory_account,
    std::string spill_directory,
    std::shared_ptr<ResourceGovernorExtension> spill_resources)
    : input_(std::move(input)), group_columns_(std::move(group_columns)),
      aggregates_(std::move(aggregates)),
      output_slots_(std::move(output_slots)), batch_capacity_(batch_capacity),
      cancellation_(std::move(cancellation)),
      memory_account_(std::move(memory_account)),
      spill_directory_(std::move(spill_directory)),
      spill_resources_(std::move(spill_resources)) {}

Status GroupedMultiAggregateResultStream::UpdateAggregate(
    AggregateState* state, const ResultAggregateSpec& aggregate,
    const std::optional<Value>& value) {
  if (state == nullptr) return Status::InvalidArgument("result stream", "missing aggregate state");
  if (!value.has_value()) return Status::OK();
  switch (aggregate.kind) {
    case ResultAggregateKind::kCount:
      if (state->count == std::numeric_limits<uint64_t>::max()) {
        return Status::InvalidArgument("result stream", "COUNT exceeds UInt64");
      }
      ++state->count;
      return Status::OK();
    case ResultAggregateKind::kSum: {
      int64_t addend = 0;
      if (value->type() == PhysicalType::kInt32) {
        addend = std::get<int32_t>(value->data());
      } else if (value->type() == PhysicalType::kInt64) {
        addend = std::get<int64_t>(value->data());
      } else {
        return Status::SchemaMismatch("result stream", "SUM requires Int32 or Int64 values");
      }
      if ((addend > 0 && state->sum > std::numeric_limits<int64_t>::max() - addend) ||
          (addend < 0 && state->sum < std::numeric_limits<int64_t>::min() - addend)) {
        return Status::InvalidArgument("result stream", "SUM exceeds Int64");
      }
      state->sum += addend;
      state->has_sum = true;
      return Status::OK();
    }
    case ResultAggregateKind::kAvg:
      switch (value->type()) {
        case PhysicalType::kInt32: state->average_sum += std::get<int32_t>(value->data()); break;
        case PhysicalType::kInt64: state->average_sum += std::get<int64_t>(value->data()); break;
        case PhysicalType::kFloat32: state->average_sum += std::get<float>(value->data()); break;
        case PhysicalType::kFloat64: state->average_sum += std::get<double>(value->data()); break;
        default:
          return Status::SchemaMismatch("result stream", "AVG requires numeric values");
      }
      if (state->average_count == std::numeric_limits<uint64_t>::max()) {
        return Status::InvalidArgument("result stream", "AVG value count exceeds UInt64");
      }
      ++state->average_count;
      return Status::OK();
    case ResultAggregateKind::kMin:
    case ResultAggregateKind::kMax:
      if (!state->extrema.has_value() ||
          (aggregate.kind == ResultAggregateKind::kMin
              ? CompareResultValues(*value, *state->extrema) < 0
              : CompareResultValues(*value, *state->extrema) > 0)) {
        state->extrema = value;
      }
      return Status::OK();
  }
  return Status::Corruption("result stream", "unknown aggregate kind");
}

StatusOr<std::optional<Value>> GroupedMultiAggregateResultStream::FinalizeAggregate(
    const AggregateState& state, const ResultAggregateSpec& aggregate) const {
  switch (aggregate.kind) {
    case ResultAggregateKind::kCount:
      if (state.count > static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
        return Status::InvalidArgument("result stream", "COUNT exceeds Int64");
      }
      return std::optional<Value>(Value::Int64(static_cast<int64_t>(state.count)));
    case ResultAggregateKind::kSum:
      return state.has_sum ? std::optional<Value>(Value::Int64(state.sum)) : std::nullopt;
    case ResultAggregateKind::kAvg:
      return state.average_count == 0 ? std::nullopt
          : std::optional<Value>(Value::Float64(
              static_cast<double>(state.average_sum / state.average_count)));
    case ResultAggregateKind::kMin:
    case ResultAggregateKind::kMax:
      return state.extrema;
  }
  return Status::Corruption("result stream", "unknown aggregate kind");
}

Status GroupedMultiAggregateResultStream::MergeAggregateState(
    AggregateState* target, const AggregateState& source,
    const ResultAggregateSpec& aggregate) {
  if (target == nullptr) {
    return Status::InvalidArgument("result stream", "missing aggregate merge target");
  }
  switch (aggregate.kind) {
    case ResultAggregateKind::kCount:
      if (source.count > std::numeric_limits<uint64_t>::max() - target->count) {
        return Status::InvalidArgument("result stream", "COUNT exceeds UInt64");
      }
      target->count += source.count;
      return Status::OK();
    case ResultAggregateKind::kSum:
      if (!source.has_sum) return Status::OK();
      if (target->has_sum &&
          ((source.sum > 0 &&
            target->sum > std::numeric_limits<int64_t>::max() - source.sum) ||
           (source.sum < 0 &&
            target->sum < std::numeric_limits<int64_t>::min() - source.sum))) {
        return Status::InvalidArgument("result stream", "SUM exceeds Int64");
      }
      target->sum = target->has_sum ? target->sum + source.sum : source.sum;
      target->has_sum = true;
      return Status::OK();
    case ResultAggregateKind::kAvg:
      if (source.average_count >
          std::numeric_limits<uint64_t>::max() - target->average_count) {
        return Status::InvalidArgument(
            "result stream", "AVG value count exceeds UInt64");
      }
      target->average_sum += source.average_sum;
      target->average_count += source.average_count;
      return Status::OK();
    case ResultAggregateKind::kMin:
    case ResultAggregateKind::kMax:
      if (source.extrema.has_value() &&
          (!target->extrema.has_value() ||
           (aggregate.kind == ResultAggregateKind::kMin
                ? CompareResultValues(*source.extrema, *target->extrema) < 0
                : CompareResultValues(*source.extrema, *target->extrema) > 0))) {
        target->extrema = source.extrema;
      }
      return Status::OK();
  }
  return Status::Corruption("result stream", "unknown aggregate kind");
}

uint64_t GroupedMultiAggregateResultStream::GroupBytes(const Group& group) const {
  uint64_t bytes = sizeof(Group) +
      static_cast<uint64_t>(group.values.size()) * sizeof(ResultValueCell) +
      static_cast<uint64_t>(group.aggregates.size()) * sizeof(AggregateState);
  for (const ResultValueCell& value : group.values) bytes += ResultCellBytes(value);
  for (const AggregateState& aggregate : group.aggregates) {
    if (aggregate.extrema.has_value()) bytes += aggregate.extrema->Encode().size();
  }
  return bytes;
}

std::string GroupedMultiAggregateResultStream::GroupKey(const Group& group) const {
  return ResultRowKey(group.values);
}

StatusOr<std::string> GroupedMultiAggregateResultStream::EncodeGroup(
    const Group& group) const {
  if (group.values.size() > kMaxGroupedAggregateSpillValues ||
      group.aggregates.size() > kMaxGroupedAggregateSpillValues ||
      group.aggregates.size() != aggregates_.size()) {
    return Status::InvalidArgument(
        "grouped aggregate spill", "group state exceeds spill bounds");
  }
  std::string record;
  AppendU32(&record, kGroupedAggregateSpillMagic);
  AppendU32(&record, static_cast<uint32_t>(group.values.size()));
  for (const ResultValueCell& value : group.values) {
    if (ResultCellBytes(value) > std::numeric_limits<uint32_t>::max()) {
      return Status::QueryMemoryLimit(
          "grouped aggregate spill", "group value exceeds spill bound");
    }
    AppendResultCell(&record, value);
  }
  AppendU32(&record, static_cast<uint32_t>(group.aggregates.size()));
  for (const AggregateState& state : group.aggregates) {
    AppendU64(&record, state.count);
    AppendU64(&record, static_cast<uint64_t>(state.sum));
    AppendU8(&record, state.has_sum ? 1 : 0);
    AppendU8(&record, static_cast<uint8_t>(sizeof(long double)));
    record.append(reinterpret_cast<const char*>(&state.average_sum),
                  sizeof(long double));
    AppendU64(&record, state.average_count);
    AppendOptionalScalar(&record, state.extrema);
  }
  return record;
}

StatusOr<GroupedMultiAggregateResultStream::Group>
GroupedMultiAggregateResultStream::DecodeGroup(
    const std::string& record) const {
  size_t offset = 0;
  uint32_t magic = 0;
  uint32_t value_count = 0;
  if (!ReadU32(record, &offset, &magic) || magic != kGroupedAggregateSpillMagic ||
      !ReadU32(record, &offset, &value_count) ||
      value_count != group_columns_.size() ||
      value_count > kMaxGroupedAggregateSpillValues) {
    return Status::Corruption(
        "grouped aggregate spill", "invalid group record header");
  }
  Group group;
  group.values.resize(value_count);
  for (ResultValueCell& value : group.values) {
    if (!ReadResultCell(record, &offset, &value)) {
      return Status::Corruption(
          "grouped aggregate spill", "invalid group value");
    }
  }
  uint32_t aggregate_count = 0;
  if (!ReadU32(record, &offset, &aggregate_count) ||
      aggregate_count != aggregates_.size() ||
      aggregate_count > kMaxGroupedAggregateSpillValues) {
    return Status::Corruption(
        "grouped aggregate spill", "invalid aggregate state count");
  }
  group.aggregates.resize(aggregate_count);
  for (AggregateState& state : group.aggregates) {
    uint64_t sum_bits = 0;
    uint8_t has_sum = 0;
    uint8_t average_bytes = 0;
    if (!ReadU64(record, &offset, &state.count) ||
        !ReadU64(record, &offset, &sum_bits) ||
        !ReadU8(record, &offset, &has_sum) || has_sum > 1 ||
        !ReadU8(record, &offset, &average_bytes) ||
        average_bytes != sizeof(long double) ||
        record.size() - offset < average_bytes) {
      return Status::Corruption(
          "grouped aggregate spill", "invalid aggregate state");
    }
    state.sum = static_cast<int64_t>(sum_bits);
    state.has_sum = has_sum == 1;
    std::memcpy(&state.average_sum, record.data() + offset, average_bytes);
    offset += average_bytes;
    if (!ReadU64(record, &offset, &state.average_count) ||
        !ReadOptionalScalar(record, &offset, &state.extrema)) {
      return Status::Corruption(
          "grouped aggregate spill", "invalid aggregate payload");
    }
  }
  if (offset != record.size()) {
    return Status::Corruption(
        "grouped aggregate spill", "trailing aggregate spill bytes");
  }
  return group;
}

Status GroupedMultiAggregateResultStream::AppendSpillGroup(const Group& group) {
  if (!spill_) {
    return Status::InvalidArgument(
        "grouped aggregate spill", "spill set is not open");
  }
  const auto encoded = EncodeGroup(group);
  if (!encoded.ok()) return encoded.status();
  const uint32_t partition = static_cast<uint32_t>(
      StableResultKeyHash(GroupKey(group)) % spill_->partition_count());
  return spill_->AppendRecord(partition, encoded.ValueOrDie());
}

void GroupedMultiAggregateResultStream::ReleaseGroups() {
  if (memory_account_ && reserved_bytes_ != 0) {
    memory_account_->Release(reserved_bytes_);
  }
  reserved_bytes_ = 0;
  groups_.clear();
  next_group_ = 0;
}

Status GroupedMultiAggregateResultStream::SpillGroups() {
  if (!spill_) {
    const std::string directory =
        spill_directory_.empty() ? "/tmp" : spill_directory_;
    spill_ = std::make_unique<PartitionedSpillSet>(
        directory, kGroupedAggregateSpillPartitions, cancellation_,
        spill_resources_, memory_account_,
        [this](uint64_t bytes) { spill_bytes_ += bytes; });
    const Status opened = spill_->Open();
    if (!opened.ok()) return opened;
  }
  for (const Group& group : groups_) {
    const Status appended = AppendSpillGroup(group);
    if (!appended.ok()) return appended;
  }
  ReleaseGroups();
  spilling_ = true;
  return Status::OK();
}

StatusOr<bool> GroupedMultiAggregateResultStream::LoadNextSpillPartition() {
  ReleaseGroups();
  while (spill_ && next_spill_partition_ < spill_->partition_count()) {
    if (cancellation_ && cancellation_->IsCancelled()) {
      return Status::QueryCancelled(
          "result stream", "query cancelled during aggregate spill replay");
    }
    const uint32_t partition = next_spill_partition_++;
    if (!spill_->HasData(partition)) continue;
    const Status rewound = spill_->Rewind(partition);
    if (!rewound.ok()) return rewound;
    for (;;) {
      std::string record;
      const Status next = spill_->NextRecord(partition, &record);
      if (next.IsNotFound()) break;
      if (!next.ok()) return next;
      auto decoded = DecodeGroup(record);
      if (!decoded.ok()) return decoded.status();
      Group partial = std::move(decoded).ConsumeValueOrDie();
      auto group = std::find_if(
          groups_.begin(), groups_.end(), [&partial](const Group& candidate) {
            return GroupCellsEqual(candidate.values, partial.values);
          });
      if (group == groups_.end()) {
        const uint64_t bytes = GroupBytes(partial);
        if (memory_account_) {
          const Status reserved = memory_account_->Reserve(bytes);
          if (!reserved.ok()) return reserved;
          reserved_bytes_ += bytes;
          memory_peak_bytes_ = std::max(memory_peak_bytes_, reserved_bytes_);
        }
        groups_.push_back(std::move(partial));
        continue;
      }
      for (size_t index = 0; index < aggregates_.size(); ++index) {
        const Status merged = MergeAggregateState(
            &group->aggregates[index], partial.aggregates[index],
            aggregates_[index]);
        if (!merged.ok()) return merged;
      }
    }
    const Status sealed = spill_->Seal(partition);
    if (!sealed.ok()) return sealed;
    std::stable_sort(
        groups_.begin(), groups_.end(), [](const Group& left, const Group& right) {
          return GroupCellsLess(left.values, right.values);
        });
    if (!groups_.empty()) return true;
  }
  return false;
}

Status GroupedMultiAggregateResultStream::Initialize() {
  if (initialized_) return terminal_status_;
  initialized_ = true;
  if (!input_ || aggregates_.empty() || output_slots_.empty() || batch_capacity_ == 0) {
    return terminal_status_ = Status::InvalidArgument("result stream", "invalid grouped aggregate input");
  }
  for (;;) {
    if (cancellation_ && cancellation_->IsCancelled()) {
      return terminal_status_ = Status::QueryCancelled("result stream",
                                                        "query cancelled during aggregation");
    }
    ResultBatch source;
    const Status next = input_->Next(&source);
    if (next.IsNotFound()) {
      const Status terminal = ResultStreamTerminalAtEnd(input_.get());
      if (!terminal.ok()) return terminal_status_ = terminal;
      break;
    }
    if (!next.ok()) return terminal_status_ = next;
    for (uint32_t column : group_columns_) {
      if (column >= source.batch().column_count()) {
        return terminal_status_ = Status::InvalidArgument("result stream", "grouping column is absent");
      }
    }
    for (const ResultAggregateSpec& aggregate : aggregates_) {
      if (aggregate.input_column >= source.batch().column_count()) {
        return terminal_status_ = Status::InvalidArgument("result stream", "aggregate input column is absent");
      }
    }
    if (group_names_.empty()) {
      for (uint32_t column : group_columns_) {
        group_names_.push_back(source.column_names()[column]);
        group_kinds_.push_back(ResultColumnKind(source.batch(), column));
      }
      temporal_metadata_ = source.temporal_metadata();
    }
    for (uint32_t row = 0; row < source.batch().row_count(); ++row) {
      std::vector<ResultValueCell> values;
      values.reserve(group_columns_.size());
      for (uint32_t column : group_columns_) {
        values.push_back(ResultCellAt(source.batch(), column, row));
      }
      if (spilling_) {
        Group partial{
            std::move(values), std::vector<AggregateState>(aggregates_.size())};
        for (size_t index = 0; index < aggregates_.size(); ++index) {
          const Status updated = UpdateAggregate(
              &partial.aggregates[index], aggregates_[index],
              source.batch().ValueAt(aggregates_[index].input_column, row));
          if (!updated.ok()) return terminal_status_ = updated;
        }
        const Status appended = AppendSpillGroup(partial);
        if (!appended.ok()) return terminal_status_ = appended;
        continue;
      }
      auto group = std::find_if(groups_.begin(), groups_.end(), [&values](const Group& candidate) {
        return GroupCellsEqual(candidate.values, values);
      });
      if (group == groups_.end()) {
        Group inserted{
            std::move(values), std::vector<AggregateState>(aggregates_.size())};
        for (size_t index = 0; index < aggregates_.size(); ++index) {
          const Status updated = UpdateAggregate(
              &inserted.aggregates[index], aggregates_[index],
              source.batch().ValueAt(aggregates_[index].input_column, row));
          if (!updated.ok()) return terminal_status_ = updated;
        }
        if (memory_account_) {
          const uint64_t bytes = GroupBytes(inserted);
          const Status reserved = memory_account_->Reserve(bytes);
          if (!reserved.ok()) return terminal_status_ = reserved;
          reserved_bytes_ += bytes;
          memory_peak_bytes_ = std::max(memory_peak_bytes_, reserved_bytes_);
        }
        groups_.push_back(std::move(inserted));
      } else {
        for (size_t index = 0; index < aggregates_.size(); ++index) {
          const Status updated = UpdateAggregate(
              &group->aggregates[index], aggregates_[index],
              source.batch().ValueAt(aggregates_[index].input_column, row));
          if (!updated.ok()) return terminal_status_ = updated;
        }
      }
      if (memory_account_ && memory_account_->ShouldSpill()) {
        const Status spilled = SpillGroups();
        if (!spilled.ok()) return terminal_status_ = spilled;
      }
    }
  }
  if (!spilling_) {
    std::stable_sort(groups_.begin(), groups_.end(), [](const Group& left, const Group& right) {
      return GroupCellsLess(left.values, right.values);
    });
  } else {
    const Status sealed = spill_->Seal();
    if (!sealed.ok()) return terminal_status_ = sealed;
  }
  return Status::OK();
}

GroupedMultiAggregateResultStream::~GroupedMultiAggregateResultStream() {
  ReleaseGroups();
}

Status GroupedMultiAggregateResultStream::Next(ResultBatch* batch) {
  if (batch == nullptr) return Status::InvalidArgument("result stream", "missing batch output");
  const Status initialized = Initialize();
  if (!initialized.ok()) return initialized;
  while (next_group_ == groups_.size()) {
    if (!spilling_) return Status::NotFound("result stream", "end of stream");
    const auto loaded = LoadNextSpillPartition();
    if (!loaded.ok()) return terminal_status_ = loaded.status();
    if (!loaded.ValueOrDie()) {
      return Status::NotFound("result stream", "end of stream");
    }
  }
  const size_t end = std::min(groups_.size(), next_group_ + batch_capacity_);
  std::vector<std::string> names;
  std::vector<ResultValueKind> kinds;
  std::vector<std::vector<ResultValueCell>> rows(end - next_group_);
  uint64_t transferred_bytes = 0;
  for (size_t row = next_group_; row < end; ++row) {
    const uint64_t bytes = GroupBytes(groups_[row]);
    if (bytes > std::numeric_limits<uint64_t>::max() - transferred_bytes) {
      return terminal_status_ = Status::QueryMemoryLimit(
          "result stream", "aggregate output charge overflow");
    }
    transferred_bytes += bytes;
  }
  names.reserve(output_slots_.size());
  kinds.reserve(output_slots_.size());
  for (std::vector<ResultValueCell>& row : rows) {
    row.reserve(output_slots_.size());
  }
  for (const ResultOutputSlot& slot : output_slots_) {
    names.push_back(slot.aggregate ? aggregates_[slot.index].column_name
                                   : group_names_[slot.index]);
    kinds.push_back(slot.aggregate ? ResultValueKind::kScalar
                                   : group_kinds_[slot.index]);
    for (size_t row = next_group_; row < end; ++row) {
      ResultValueCell value;
      if (slot.aggregate) {
        if (slot.index >= aggregates_.size()) {
          return Status::Corruption("result stream", "aggregate output slot is absent");
        }
        const auto finalized = FinalizeAggregate(groups_[row].aggregates[slot.index],
                                                 aggregates_[slot.index]);
        if (!finalized.ok()) return finalized.status();
        value.kind = ResultValueKind::kScalar;
        value.scalar = finalized.ValueOrDie();
      } else {
        if (slot.index >= group_columns_.size()) {
          return Status::Corruption("result stream", "group output slot is absent");
        }
        value = std::move(groups_[row].values[slot.index]);
      }
      rows[row - next_group_].push_back(std::move(value));
    }
  }
  for (size_t row = next_group_; row < end; ++row) {
    std::vector<ResultValueCell>().swap(groups_[row].values);
    std::vector<AggregateState>().swap(groups_[row].aggregates);
  }
  if (memory_account_ && transferred_bytes > reserved_bytes_) {
    return terminal_status_ = Status::Corruption(
        "result stream", "aggregate output exceeds retained charge");
  }
  if (memory_account_) reserved_bytes_ -= transferred_bytes;
  auto lease = std::make_shared<QueryMemoryLease>(
      memory_account_, memory_account_ ? transferred_bytes : 0);
  const Status built = BuildResultBatchMoving(
      names, kinds, &rows, temporal_metadata_, batch, lease);
  if (!built.ok()) return terminal_status_ = built;
  next_group_ = end;
  return Status::OK();
}

}  // namespace cedar
