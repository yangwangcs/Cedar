// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/tcypher/runtime/column_batch.h"

namespace cedar {
namespace {

uint64_t OwnedValuePayloadBytes(const Value& value) {
  switch (value.type()) {
    case PhysicalType::kBool:
    case PhysicalType::kInt32:
    case PhysicalType::kFloat32:
    case PhysicalType::kInt64:
    case PhysicalType::kFloat64:
    case PhysicalType::kTimestamp64: return 0;
    case PhysicalType::kString:
    case PhysicalType::kBinary:
      return std::get<std::string>(value.data()).size();
  }
  return 0;
}

uint64_t OwnedValueRetainedBytes(const Value& value) {
  switch (value.type()) {
    case PhysicalType::kBool:
    case PhysicalType::kInt32:
    case PhysicalType::kFloat32:
    case PhysicalType::kInt64:
    case PhysicalType::kFloat64:
    case PhysicalType::kTimestamp64: return 0;
    case PhysicalType::kString:
    case PhysicalType::kBinary:
      return std::get<std::string>(value.data()).capacity();
  }
  return 0;
}

}  // namespace

uint64_t Vector::ValuePayloadBytesAt(uint32_t row) const {
  const Value* value = ValueRefAt(row);
  if (value == nullptr ||
      (value->type() != PhysicalType::kString &&
       value->type() != PhysicalType::kBinary)) {
    return 0;
  }
  return std::get<std::string>(value->data()).size();
}
uint64_t Vector::RetainedPayloadBytesAt(uint32_t row) const {
  const Value* value = ValueRefAt(row);
  if (value == nullptr ||
      (value->type() != PhysicalType::kString &&
       value->type() != PhysicalType::kBinary)) {
    return 0;
  }
  return OwnedValueRetainedBytes(*value);
}
FlatVector::FlatVector(std::vector<Value> values, std::vector<bool> validity,
                       std::shared_ptr<void> retention)
    : retention_(std::move(retention)), values_(std::move(values)),
      validity_(std::move(validity)) {
  if (validity_.empty()) validity_.assign(values_.size(), true);
}
uint32_t FlatVector::size() const { return static_cast<uint32_t>(values_.size()); }
std::optional<Value> FlatVector::ValueAt(uint32_t row) const {
  const Value* value = ValueRefAt(row);
  return value == nullptr ? std::optional<Value>{} : *value;
}
const Value* FlatVector::ValueRefAt(uint32_t row) const {
  if (row >= values_.size() || row >= validity_.size() || !validity_[row]) {
    return nullptr;
  }
  return &values_[row];
}
DictionaryVector::DictionaryVector(std::shared_ptr<const Vector> parent,
                                   std::vector<uint32_t> indices)
    : parent_(std::move(parent)), indices_(std::move(indices)) {}
uint32_t DictionaryVector::size() const { return static_cast<uint32_t>(indices_.size()); }
std::optional<Value> DictionaryVector::ValueAt(uint32_t row) const {
  const Value* value = ValueRefAt(row);
  return value == nullptr ? std::optional<Value>{} : *value;
}
const Value* DictionaryVector::ValueRefAt(uint32_t row) const {
  return parent_ && row < indices_.size()
      ? parent_->ValueRefAt(indices_[row]) : nullptr;
}
uint64_t DictionaryVector::ValuePayloadBytesAt(uint32_t row) const {
  return parent_ && row < indices_.size()
      ? parent_->ValuePayloadBytesAt(indices_[row]) : 0;
}
uint64_t DictionaryVector::RetainedPayloadBytesAt(uint32_t row) const {
  return parent_ && row < indices_.size()
      ? parent_->RetainedPayloadBytesAt(indices_[row]) : 0;
}
const StructValue* DictionaryVector::StructRefAt(uint32_t row) const {
  return parent_ && row < indices_.size()
      ? parent_->StructRefAt(indices_[row]) : nullptr;
}
const ListValue* DictionaryVector::ListRefAt(uint32_t row) const {
  return parent_ && row < indices_.size()
      ? parent_->ListRefAt(indices_[row]) : nullptr;
}
std::optional<StructValue> DictionaryVector::StructAt(uint32_t row) const {
  const StructValue* value = StructRefAt(row);
  return value == nullptr ? std::optional<StructValue>{} : *value;
}
std::optional<ListValue> DictionaryVector::ListAt(uint32_t row) const {
  const ListValue* value = ListRefAt(row);
  return value == nullptr ? std::optional<ListValue>{} : *value;
}
std::optional<Value> SliceVector::ValueAt(uint32_t row) const {
  const Value* value = ValueRefAt(row);
  return value == nullptr ? std::optional<Value>{} : *value;
}
const Value* SliceVector::ValueRefAt(uint32_t row) const {
  return parent_ && row < size_ ? parent_->ValueRefAt(start_ + row) : nullptr;
}
uint64_t SliceVector::ValuePayloadBytesAt(uint32_t row) const {
  return parent_ && row < size_
      ? parent_->ValuePayloadBytesAt(start_ + row) : 0;
}
uint64_t SliceVector::RetainedPayloadBytesAt(uint32_t row) const {
  return parent_ && row < size_
      ? parent_->RetainedPayloadBytesAt(start_ + row) : 0;
}
const StructValue* SliceVector::StructRefAt(uint32_t row) const {
  return parent_ && row < size_ ? parent_->StructRefAt(start_ + row) : nullptr;
}
const ListValue* SliceVector::ListRefAt(uint32_t row) const {
  return parent_ && row < size_ ? parent_->ListRefAt(start_ + row) : nullptr;
}
std::optional<StructValue> SliceVector::StructAt(uint32_t row) const {
  const StructValue* value = StructRefAt(row);
  return value == nullptr ? std::optional<StructValue>{} : *value;
}
std::optional<ListValue> SliceVector::ListAt(uint32_t row) const {
  const ListValue* value = ListRefAt(row);
  return value == nullptr ? std::optional<ListValue>{} : *value;
}
StructVector::StructVector(std::vector<StructValue> values, std::vector<bool> validity,
                           std::shared_ptr<void> retention)
    : retention_(std::move(retention)), values_(std::move(values)),
      validity_(std::move(validity)) {
  if (validity_.empty()) validity_.assign(values_.size(), true);
}
uint32_t StructVector::size() const { return static_cast<uint32_t>(values_.size()); }
uint64_t StructVector::ValuePayloadBytesAt(uint32_t row) const {
  if (row >= values_.size() || row >= validity_.size() || !validity_[row]) {
    return 0;
  }
  uint64_t bytes = static_cast<uint64_t>(values_[row].fields.size()) *
      sizeof(StructField);
  for (const StructField& field : values_[row].fields) {
    bytes += field.name.size();
    if (field.value.has_value()) {
      bytes += OwnedValuePayloadBytes(*field.value);
    }
  }
  return bytes;
}
uint64_t StructVector::RetainedPayloadBytesAt(uint32_t row) const {
  if (row >= values_.size() || row >= validity_.size() || !validity_[row]) {
    return 0;
  }
  uint64_t bytes = static_cast<uint64_t>(values_[row].fields.capacity()) *
      sizeof(StructField);
  for (const StructField& field : values_[row].fields) {
    bytes += field.name.capacity();
    if (field.value.has_value()) bytes += OwnedValueRetainedBytes(*field.value);
  }
  return bytes;
}
const StructValue* StructVector::StructRefAt(uint32_t row) const {
  return row < values_.size() && row < validity_.size() && validity_[row]
      ? &values_[row] : nullptr;
}
std::optional<StructValue> StructVector::StructAt(uint32_t row) const {
  const StructValue* value = StructRefAt(row);
  return value == nullptr ? std::optional<StructValue>{} : *value;
}
ListVector::ListVector(std::vector<ListValue> values, std::vector<bool> validity,
                       std::shared_ptr<void> retention)
    : retention_(std::move(retention)), values_(std::move(values)),
      validity_(std::move(validity)) {
  if (validity_.empty()) validity_.assign(values_.size(), true);
  valid_ = validity_.size() == values_.size();
  for (size_t row = 0; valid_ && row < values_.size(); ++row) {
    valid_ = !validity_[row] || values_[row].IsConsistent();
  }
}
uint32_t ListVector::size() const { return static_cast<uint32_t>(values_.size()); }
uint64_t ListVector::ValuePayloadBytesAt(uint32_t row) const {
  if (row >= values_.size() || row >= validity_.size() || !validity_[row]) {
    return 0;
  }
  uint64_t bytes = 0;
  if (values_[row].element_kind == ListElementKind::kScalar) {
    bytes = static_cast<uint64_t>(values_[row].elements.size()) *
        sizeof(std::optional<Value>);
    for (const std::optional<Value>& element : values_[row].elements) {
      if (element.has_value()) bytes += OwnedValuePayloadBytes(*element);
    }
  } else {
    bytes = static_cast<uint64_t>(
        values_[row].structured_elements.size()) * sizeof(StructValue);
    for (const StructValue& element : values_[row].structured_elements) {
      bytes += static_cast<uint64_t>(element.fields.size()) *
          sizeof(StructField);
      for (const StructField& field : element.fields) {
        if (field.value.has_value()) {
          bytes += OwnedValuePayloadBytes(*field.value);
        }
      }
    }
  }
  return bytes;
}
uint64_t ListVector::RetainedPayloadBytesAt(uint32_t row) const {
  if (row >= values_.size() || row >= validity_.size() || !validity_[row]) {
    return 0;
  }
  uint64_t bytes = 0;
  if (values_[row].element_kind == ListElementKind::kScalar) {
    bytes = static_cast<uint64_t>(values_[row].elements.capacity()) *
        sizeof(std::optional<Value>);
    for (const auto& element : values_[row].elements) {
      if (element.has_value()) bytes += OwnedValueRetainedBytes(*element);
    }
  } else {
    bytes = static_cast<uint64_t>(values_[row].structured_elements.capacity()) *
        sizeof(StructValue);
    for (const StructValue& element : values_[row].structured_elements) {
      bytes += static_cast<uint64_t>(element.fields.capacity()) * sizeof(StructField);
      for (const StructField& field : element.fields) {
        bytes += field.name.capacity();
        if (field.value.has_value()) bytes += OwnedValueRetainedBytes(*field.value);
      }
    }
  }
  return bytes;
}
const ListValue* ListVector::ListRefAt(uint32_t row) const {
  return row < values_.size() && row < validity_.size() && validity_[row]
      ? &values_[row] : nullptr;
}
std::optional<ListValue> ListVector::ListAt(uint32_t row) const {
  const ListValue* value = ListRefAt(row);
  return value == nullptr ? std::optional<ListValue>{} : *value;
}
ColumnBatch::ColumnBatch(uint32_t capacity) : capacity_(capacity) {}
Status ColumnBatch::AddVector(std::shared_ptr<const Vector> vector) {
  if (!vector || !vector->is_valid() || vector->size() > capacity_) return Status::InvalidArgument("column batch", "invalid vector capacity");
  if (!vectors_.empty() && vector->size() != vectors_.front()->size())
    return Status::InvalidArgument("column batch", "vector row count mismatch");
  vectors_.push_back(std::move(vector)); return Status::OK();
}
Status ColumnBatch::SetSelection(std::vector<uint32_t> selection) {
  const uint32_t source_rows = vectors_.empty() ? 0 : vectors_.front()->size();
  if (selection.size() > capacity_) return Status::InvalidArgument("column batch", "selection exceeds capacity");
  for (uint32_t row : selection) if (row >= source_rows) return Status::InvalidArgument("column batch", "selection out of range");
  selection_ = std::move(selection); return Status::OK();
}
uint32_t ColumnBatch::row_count() const {
  return selection_.has_value()
      ? static_cast<uint32_t>(selection_->size())
      : (vectors_.empty() ? 0 : vectors_.front()->size());
}
std::optional<uint32_t> ColumnBatch::SourceRowAt(uint32_t logical_row) const {
  if (logical_row >= row_count()) return std::nullopt;
  return selection_.has_value() ? (*selection_)[logical_row] : logical_row;
}
std::optional<Value> ColumnBatch::ValueAt(uint32_t column, uint32_t logical_row) const {
  const Value* value = ValueRefAt(column, logical_row);
  return value == nullptr ? std::optional<Value>{} : *value;
}
const Value* ColumnBatch::ValueRefAt(uint32_t column,
                                     uint32_t logical_row) const {
  if (column >= vectors_.size() || logical_row >= row_count()) return nullptr;
  return vectors_[column]->ValueRefAt(*SourceRowAt(logical_row));
}
uint64_t ColumnBatch::ValuePayloadBytesAt(uint32_t column,
                                          uint32_t logical_row) const {
  if (column >= vectors_.size() || logical_row >= row_count()) return 0;
  return vectors_[column]->ValuePayloadBytesAt(*SourceRowAt(logical_row));
}
uint64_t ColumnBatch::RetainedPayloadBytesAt(uint32_t column,
                                             uint32_t logical_row) const {
  if (column >= vectors_.size()) return 0;
  const auto source_row = SourceRowAt(logical_row);
  return source_row.has_value()
      ? vectors_[column]->RetainedPayloadBytesAt(*source_row) : 0;
}
const StructValue* ColumnBatch::StructRefAt(
    uint32_t column, uint32_t logical_row) const {
  if (column >= vectors_.size() || logical_row >= row_count()) return nullptr;
  return vectors_[column]->StructRefAt(*SourceRowAt(logical_row));
}
const ListValue* ColumnBatch::ListRefAt(
    uint32_t column, uint32_t logical_row) const {
  if (column >= vectors_.size() || logical_row >= row_count()) return nullptr;
  return vectors_[column]->ListRefAt(*SourceRowAt(logical_row));
}
std::optional<StructValue> ColumnBatch::StructAt(uint32_t column, uint32_t logical_row) const {
  const StructValue* value = StructRefAt(column, logical_row);
  return value == nullptr ? std::optional<StructValue>{} : *value;
}
std::optional<ListValue> ColumnBatch::ListAt(uint32_t column, uint32_t logical_row) const {
  const ListValue* value = ListRefAt(column, logical_row);
  return value == nullptr ? std::optional<ListValue>{} : *value;
}
}  // namespace cedar
