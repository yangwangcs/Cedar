// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#ifndef CEDAR_TCYPHER_RUNTIME_COLUMN_BATCH_H_
#define CEDAR_TCYPHER_RUNTIME_COLUMN_BATCH_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/types/value.h"

namespace cedar {
constexpr uint32_t kTcypherStandardBatchCapacity = 2048;

struct StructField {
  std::string name;
  std::optional<Value> value;
};
struct StructValue {
  std::vector<StructField> fields;
};
enum class ListElementKind : uint8_t { kScalar, kStruct };
struct ListValue {
  std::vector<std::optional<Value>> elements;
  std::vector<StructValue> structured_elements;
  ListElementKind element_kind = ListElementKind::kScalar;

  bool IsConsistent() const {
    switch (element_kind) {
      case ListElementKind::kScalar: return structured_elements.empty();
      case ListElementKind::kStruct: return elements.empty();
    }
    return false;
  }
};

class Vector {
 public:
  virtual ~Vector() = default;
  virtual uint32_t size() const = 0;
  virtual std::optional<Value> ValueAt(uint32_t row) const = 0;
  virtual const Value* ValueRefAt(uint32_t row) const = 0;
  virtual uint64_t ValuePayloadBytesAt(uint32_t row) const;
  // Bytes in the logical value payload, excluding allocator slack.  Runtime
  // memory charging uses RetainedPayloadBytesAt so capacity growth remains
  // conservatively accounted for without contaminating copy statistics.
  virtual uint64_t RetainedPayloadBytesAt(uint32_t row) const;
  virtual const StructValue* StructRefAt(uint32_t row) const { return nullptr; }
  virtual const ListValue* ListRefAt(uint32_t row) const { return nullptr; }
  virtual std::optional<StructValue> StructAt(uint32_t row) const { return std::nullopt; }
  virtual std::optional<ListValue> ListAt(uint32_t row) const { return std::nullopt; }
  virtual bool is_structured() const { return false; }
  virtual bool is_list() const { return false; }
  virtual bool is_valid() const { return true; }
};

class FlatVector final : public Vector {
 public:
  FlatVector(std::vector<Value> values, std::vector<bool> validity,
             std::shared_ptr<void> retention = nullptr);
 uint32_t size() const override;
  std::optional<Value> ValueAt(uint32_t row) const override;
  const Value* ValueRefAt(uint32_t row) const override;
 private:
  std::shared_ptr<void> retention_;
  std::vector<Value> values_;
  std::vector<bool> validity_;
};

class ConstantVector final : public Vector {
 public:
  ConstantVector(std::optional<Value> value, uint32_t size) : value_(std::move(value)), size_(size) {}
  uint32_t size() const override { return size_; }
  std::optional<Value> ValueAt(uint32_t row) const override { return row < size_ ? value_ : std::nullopt; }
  const Value* ValueRefAt(uint32_t row) const override {
    return row < size_ && value_.has_value() ? &*value_ : nullptr;
  }
 private:
  std::optional<Value> value_;
  uint32_t size_;
};

class DictionaryVector final : public Vector {
 public:
  DictionaryVector(std::shared_ptr<const Vector> parent, std::vector<uint32_t> indices);
  uint32_t size() const override;
  std::optional<Value> ValueAt(uint32_t row) const override;
  const Value* ValueRefAt(uint32_t row) const override;
  uint64_t ValuePayloadBytesAt(uint32_t row) const override;
  uint64_t RetainedPayloadBytesAt(uint32_t row) const override;
  const StructValue* StructRefAt(uint32_t row) const override;
  const ListValue* ListRefAt(uint32_t row) const override;
  std::optional<StructValue> StructAt(uint32_t row) const override;
  std::optional<ListValue> ListAt(uint32_t row) const override;
  bool is_structured() const override {
    return parent_ && parent_->is_structured();
  }
  bool is_list() const override { return parent_ && parent_->is_list(); }
  bool is_valid() const override { return parent_ && parent_->is_valid(); }
 private:
  std::shared_ptr<const Vector> parent_;
  std::vector<uint32_t> indices_;
};

class SliceVector final : public Vector {
 public:
  SliceVector(std::shared_ptr<const Vector> parent, uint32_t start,
              uint32_t size)
      : parent_(std::move(parent)), start_(start), size_(size) {}
  uint32_t size() const override { return size_; }
  std::optional<Value> ValueAt(uint32_t row) const override;
  const Value* ValueRefAt(uint32_t row) const override;
  uint64_t ValuePayloadBytesAt(uint32_t row) const override;
  uint64_t RetainedPayloadBytesAt(uint32_t row) const override;
  const StructValue* StructRefAt(uint32_t row) const override;
  const ListValue* ListRefAt(uint32_t row) const override;
  std::optional<StructValue> StructAt(uint32_t row) const override;
  std::optional<ListValue> ListAt(uint32_t row) const override;
  bool is_structured() const override {
    return parent_ && parent_->is_structured();
  }
  bool is_list() const override { return parent_ && parent_->is_list(); }
  bool is_valid() const override { return parent_ && parent_->is_valid(); }

 private:
  std::shared_ptr<const Vector> parent_;
  uint32_t start_ = 0;
  uint32_t size_ = 0;
};

class StructVector final : public Vector {
 public:
  StructVector(std::vector<StructValue> values, std::vector<bool> validity,
               std::shared_ptr<void> retention = nullptr);
  uint32_t size() const override;
  // Structured values intentionally do not coerce into the scalar Value type.
  std::optional<Value> ValueAt(uint32_t row) const override { return std::nullopt; }
  const Value* ValueRefAt(uint32_t row) const override { return nullptr; }
  uint64_t ValuePayloadBytesAt(uint32_t row) const override;
  uint64_t RetainedPayloadBytesAt(uint32_t row) const override;
  const StructValue* StructRefAt(uint32_t row) const override;
  std::optional<StructValue> StructAt(uint32_t row) const override;
  bool is_structured() const override { return true; }
 private:
  std::shared_ptr<void> retention_;
  std::vector<StructValue> values_;
  std::vector<bool> validity_;
};

class ListVector final : public Vector {
 public:
  ListVector(std::vector<ListValue> values, std::vector<bool> validity,
             std::shared_ptr<void> retention = nullptr);
  uint32_t size() const override;
  std::optional<Value> ValueAt(uint32_t row) const override { return std::nullopt; }
  const Value* ValueRefAt(uint32_t row) const override { return nullptr; }
  uint64_t ValuePayloadBytesAt(uint32_t row) const override;
  uint64_t RetainedPayloadBytesAt(uint32_t row) const override;
  const ListValue* ListRefAt(uint32_t row) const override;
  std::optional<ListValue> ListAt(uint32_t row) const override;
  bool is_list() const override { return true; }
  bool is_valid() const override { return valid_; }
 private:
  std::shared_ptr<void> retention_;
  std::vector<ListValue> values_;
  std::vector<bool> validity_;
  bool valid_ = true;
};

class ColumnBatch {
 public:
  explicit ColumnBatch(uint32_t capacity = kTcypherStandardBatchCapacity);
  Status AddVector(std::shared_ptr<const Vector> vector);
  Status SetSelection(std::vector<uint32_t> selection);
  uint32_t capacity() const { return capacity_; }
  uint32_t column_count() const { return static_cast<uint32_t>(vectors_.size()); }
  uint32_t source_row_count() const {
    return vectors_.empty() ? 0 : vectors_.front()->size();
  }
  uint32_t row_count() const;
  std::optional<uint32_t> SourceRowAt(uint32_t logical_row) const;
  std::optional<Value> ValueAt(uint32_t column, uint32_t logical_row) const;
  const Value* ValueRefAt(uint32_t column, uint32_t logical_row) const;
  uint64_t ValuePayloadBytesAt(uint32_t column, uint32_t logical_row) const;
  uint64_t RetainedPayloadBytesAt(uint32_t column, uint32_t logical_row) const;
  const StructValue* StructRefAt(uint32_t column, uint32_t logical_row) const;
  const ListValue* ListRefAt(uint32_t column, uint32_t logical_row) const;
  std::optional<StructValue> StructAt(uint32_t column, uint32_t logical_row) const;
  std::optional<ListValue> ListAt(uint32_t column, uint32_t logical_row) const;
  bool IsStructured(uint32_t column) const {
    return column < vectors_.size() && vectors_[column]->is_structured();
  }
  bool IsList(uint32_t column) const {
    return column < vectors_.size() && vectors_[column]->is_list();
  }
  std::shared_ptr<const Vector> VectorAt(uint32_t column) const {
    return column < vectors_.size() ? vectors_[column] : nullptr;
  }
 private:
  uint32_t capacity_;
  std::vector<std::shared_ptr<const Vector>> vectors_;
  std::optional<std::vector<uint32_t>> selection_;
};
}  // namespace cedar
#endif  // CEDAR_TCYPHER_RUNTIME_COLUMN_BATCH_H_
