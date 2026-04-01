// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef FERN_API_TYPED_VALUE_H_
#define FERN_API_TYPED_VALUE_H_

#include <cstdint>
#include <string>
#include <optional>
#include <type_traits>
#include <stdexcept>

#include "cedar/types/descriptor.h"
#include "cedar/types/cedar_key.h"
#include "cedar/core/slice.h"

namespace cedar {
namespace api {

// ============================================================================
// 类型特征模板
// ============================================================================

template<typename T>
struct TypeTraits {
  static constexpr bool kIsInline = false;
  static constexpr size_t kMaxInlineSize = 0;
  static constexpr EntryKind kKind = EntryKind::ExternalRef;
};

template<>
struct TypeTraits<int32_t> {
  static constexpr bool kIsInline = true;
  static constexpr size_t kMaxInlineSize = 4;
  static constexpr EntryKind kKind = EntryKind::InlineInt;
};

template<>
struct TypeTraits<float> {
  static constexpr bool kIsInline = true;
  static constexpr size_t kMaxInlineSize = 4;
  static constexpr EntryKind kKind = EntryKind::InlineFloat;
};

template<>
struct TypeTraits<bool> {
  static constexpr bool kIsInline = true;
  static constexpr size_t kMaxInlineSize = 1;
  static constexpr EntryKind kKind = EntryKind::InlineInt;
};

template<>
struct TypeTraits<Timestamp> {
  static constexpr bool kIsInline = true;
  static constexpr size_t kMaxInlineSize = 8;
  static constexpr EntryKind kKind = EntryKind::InlineInt;
};

template<>
struct TypeTraits<uint64_t> {
  static constexpr bool kIsInline = true;
  static constexpr size_t kMaxInlineSize = 8;
  static constexpr EntryKind kKind = EntryKind::InlineInt;
};

template<>
struct TypeTraits<std::string> {
  static constexpr bool kIsInline = true;  // 短字符串可内联
  static constexpr size_t kMaxInlineSize = 4;
  static constexpr EntryKind kKind = EntryKind::InlineShortStr;
};

// ============================================================================
// TypedValue - 类型安全的值包装
// ============================================================================

template<typename T>
class TypedValue {
 public:
  using ValueType = T;
  
  // 默认构造
  TypedValue() = default;
  
  // 从值构造
  explicit TypedValue(T value) : value_(std::move(value)) {}
  
  // 赋值操作符
  TypedValue& operator=(T value) {
    value_ = std::move(value);
    return *this;
  }
  
  // 隐式转换到值类型
  operator T() const { return value_; }
  operator T&() { return value_; }
  
  // 获取值
  const T& value() const { return value_; }
  T& value() { return value_; }
  
  // 获取值（可选，如果未设置返回 nullopt）
  std::optional<T> optional_value() const {
    if (is_set_) {
      return value_;
    }
    return std::nullopt;
  }
  
  // 检查是否已设置
  bool is_set() const { return is_set_; }
  void set_is_set(bool v) { is_set_ = v; }
  
  // ============================================================================
  // 编码为 Descriptor
  // ============================================================================
  
  Descriptor ToDescriptor(uint16_t column_id = 0) const {
    if constexpr (std::is_same_v<T, int32_t>) {
      return Descriptor::InlineInt(column_id, value_);
    } else if constexpr (std::is_same_v<T, float>) {
      return Descriptor::InlineFloat(column_id, value_);
    } else if constexpr (std::is_same_v<T, bool>) {
      return Descriptor::InlineInt(column_id, value_ ? 1 : 0);
    } else if constexpr (std::is_same_v<T, Timestamp>) {
      // Timestamp 存储为 int64，但 Descriptor 只有 32bit payload
      // 这里存储低32位，完整时间戳需要外部处理
      return Descriptor::InlineInt(column_id, static_cast<int32_t>(value_.value() & 0xFFFFFFFF));
    } else if constexpr (std::is_same_v<T, uint64_t>) {
      // 存储低32位
      return Descriptor::InlineInt(column_id, static_cast<int32_t>(value_ & 0xFFFFFFFF));
    } else if constexpr (std::is_same_v<T, std::string>) {
      // 短字符串内联，长字符串需要外部存储
      if (value_.size() <= 4) {
        auto desc_opt = Descriptor::InlineShortStr(column_id, Slice(value_.data(), value_.size()));
        if (desc_opt.has_value()) {
          return *desc_opt;
        } else {
          // 长字符串返回空描述符，需要外部处理
          return Descriptor();
        }
      } else {
        // 长字符串返回空描述符，需要外部处理
        return Descriptor();
      }
    } else {
      static_assert(std::is_same_v<T, void>, "Unsupported type for TypedValue");
    }
  }
  
  // ============================================================================
  // 从 Descriptor 解码
  // ============================================================================
  
  static std::optional<TypedValue<T>> FromDescriptor(const Descriptor& desc) {
    if constexpr (std::is_same_v<T, int32_t>) {
      auto val = desc.AsInlineInt();
      if (val.has_value()) {
        return TypedValue<T>(val.value());
      }
    } else if constexpr (std::is_same_v<T, float>) {
      auto val = desc.AsInlineFloat();
      if (val.has_value()) {
        return TypedValue<T>(val.value());
      }
    } else if constexpr (std::is_same_v<T, bool>) {
      auto val = desc.AsInlineInt();
      if (val.has_value()) {
        return TypedValue<T>(val.value() != 0);
      }
    } else if constexpr (std::is_same_v<T, std::string>) {
      std::string str = desc.AsInlineShortStr();
      if (!str.empty()) {
        return TypedValue<T>(std::move(str));
      }
    }
    return std::nullopt;
  }
  
  // 创建删除标记
  static Descriptor Tombstone(uint16_t column_id) {
    return Descriptor::Tombstone(column_id);
  }
  
 private:
  T value_;
  bool is_set_ = false;
};

// ============================================================================
// 类型别名
// ============================================================================

using Int32Value = TypedValue<int32_t>;
using Int64Value = TypedValue<int64_t>;
using FloatValue = TypedValue<float>;
using DoubleValue = TypedValue<double>;
using BoolValue = TypedValue<bool>;
using StringValue = TypedValue<std::string>;
using TimestampValue = TypedValue<Timestamp>;
using EntityIdValue = TypedValue<uint64_t>;

// ============================================================================
// ValueHolder - 支持多版本的值容器
// ============================================================================

template<typename T>
struct VersionedValue {
  Timestamp timestamp;
  TypedValue<T> value;
  
  VersionedValue() = default;
  VersionedValue(Timestamp ts, T val) 
      : timestamp(ts), value(std::move(val)) {}
};

// ============================================================================
// Property - 属性包装（包含字段元信息）
// ============================================================================

template<typename FieldDef>
class Property {
 public:
  using ValueType = typename FieldDef::ValueType;
  static constexpr uint16_t kColumnId = FieldDef::kId;
  static constexpr bool kIsStatic = FieldDef::kIsStatic;
  
  Property() = default;
  explicit Property(ValueType v) : value_(std::move(v)) {}
  
  // 赋值
  Property& operator=(ValueType v) {
    value_ = std::move(v);
    is_set_ = true;
    return *this;
  }
  
  // 获取值
  const ValueType& get() const { return value_.value(); }
  ValueType& get() { return value_.value(); }
  
  // 获取 TypedValue
  const TypedValue<ValueType>& typed_value() const { return value_; }
  
  // 检查是否已设置
  bool is_set() const { return is_set_; }
  
  // 编码为 Descriptor
  Descriptor ToDescriptor() const {
    return value_.ToDescriptor(kColumnId);
  }
  
  // 解码
  void FromDescriptor(const Descriptor& desc) {
    auto opt = TypedValue<ValueType>::FromDescriptor(desc);
    if (opt.has_value()) {
      value_ = opt.value();
      is_set_ = true;
    }
  }
  
  // 创建 CedarKey
  CedarKey MakeKey(uint64_t entity_id, Timestamp timestamp) const {
    if constexpr (FieldDef::kSchema::kIsEdge) {
      // 边类型需要特殊处理
      return CedarKey();  // 边需要 src/dst，这里返回空
    } else {
      return CedarKey::Vertex(entity_id, kColumnId, timestamp);
    }
  }
  
 private:
  TypedValue<ValueType> value_;
  bool is_set_ = false;
};

} // namespace api
} // namespace cedar

#endif // FERN_API_TYPED_VALUE_H_
