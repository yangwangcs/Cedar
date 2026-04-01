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

#ifndef FERN_API_ENTITY_H_
#define FERN_API_ENTITY_H_

#include <cstdint>
#include <optional>
#include <vector>
#include <memory>
#include <utility>
#include <map>

#include "cedar/api/schema.h"
#include "cedar/api/typed_value.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

namespace cedar {
namespace api {

// 前向声明
class TypedEngine;

// ============================================================================
// Entity 基类 - 类型安全的实体封装
// ============================================================================

template<typename SchemaType>
class Entity {
 public:
  using Schema = SchemaType;
  static constexpr bool kIsEdge = Schema::kIsEdge;
  
  // 节点构造
  explicit Entity(uint64_t id) : id_(id) {
    static_assert(!kIsEdge, "Use Edge constructor for edge entities");
  }
  
  // 边构造
  Entity(uint64_t src_id, uint64_t dst_id) 
      : id_(0), src_id_(src_id), dst_id_(dst_id) {
    static_assert(kIsEdge, "Use single id constructor for vertex entities");
  }
  
  virtual ~Entity() = default;
  
  // 禁止拷贝，允许移动
  Entity(const Entity&) = delete;
  Entity& operator=(const Entity&) = delete;
  Entity(Entity&&) = default;
  Entity& operator=(Entity&&) = default;
  
  // ============================================================================
  // ID 访问
  // ============================================================================
  
  uint64_t id() const { return id_; }
  void set_id(uint64_t id) { id_ = id; }
  
  // 边特有
  uint64_t src_id() const { return src_id_.value_or(0); }
  uint64_t dst_id() const { return dst_id_.value_or(0); }
  
  // ============================================================================
  // 属性设置 (需要在子类中实现)
  // ============================================================================
  
  // 设置属性值 (子类实现)
  template<typename Field>
  Entity& set(typename Field::ValueType value) {
    static_assert(std::is_base_of_v<Schema, SchemaType>, "Invalid field for this entity");
    // 子类需要重写此方法
    return *this;
  }
  
  // 获取属性值 (子类实现)
  template<typename Field>
  std::optional<typename Field::ValueType> get() const {
    return std::nullopt;
  }
  
  // ============================================================================
  // 序列化
  // ============================================================================
  
  // 转换为键值对列表 (需要在子类中实现)
  virtual std::vector<std::pair<CedarKey, Descriptor>> ToKeyValues(Timestamp timestamp) const {
    return {};
  }
  
  // 检查是否有未保存的更改
  bool IsDirty() const { return is_dirty_; }
  void MarkClean() { is_dirty_ = false; }
  void MarkDirty() { is_dirty_ = true; }
  
 protected:
  uint64_t id_;
  std::optional<uint64_t> src_id_;
  std::optional<uint64_t> dst_id_;
  bool is_dirty_ = true;
  
  // 历史值存储 (column_id -> list of versioned values)
  std::map<uint16_t, std::vector<VersionedValue<int32_t>>> historical_values_;
};

// ============================================================================
// Entity 使用示例
// ============================================================================
/*
// 定义具体的 User 实体
template<typename SchemaType>
class UserEntity : public Entity<SchemaType> {
 public:
  explicit UserEntity(uint64_t id) : Entity<SchemaType>(id) {}
  
  // 声明属性
  Property<typename SchemaType::Id> id_prop;
  Property<typename SchemaType::Name> name;
  // ... 其他属性
  
  // 实现设置方法
  template<typename Field>
  UserEntity& set(typename Field::ValueType value) {
    // 实现字段设置...
    return *this;
  }
  
  // 实现获取方法
  template<typename Field>
  std::optional<typename Field::ValueType> get() const {
    // 实现字段获取...
    return std::nullopt;
  }
  
  // 实现序列化
  std::vector<std::pair<CedarKey, Descriptor>> ToKeyValues(Timestamp timestamp) const override {
    // 实现序列化...
    return {};
  }
};
*/

} // namespace api
} // namespace cedar

#endif // FERN_API_ENTITY_H_
