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

#ifndef FERN_API_SCHEMA_H_
#define FERN_API_SCHEMA_H_

#include <cstdint>
#include <string>
#include <type_traits>
#include <vector>
#include <map>
#include <optional>

#include "cedar/types/descriptor.h"
#include "cedar/types/cedar_key.h"

namespace cedar {
namespace api {

// ============================================================================
// Schema 基础设施
// ============================================================================

// 字段类型枚举
enum class FieldType : uint8_t {
  Int32 = 0,
  Int64 = 1,
  Float = 2,
  Double = 3,
  Bool = 4,
  String = 5,
  Timestamp = 6,
  EntityId = 7,
  Binary = 8,
};

// 字段修饰符
enum class FieldModifier : uint8_t {
  Static = 0,      // 静态属性，不变化
  Dynamic = 1,     // 动态属性，支持多版本
  Indexed = 2,     // 需要索引
  Required = 4,    // 必需字段
};

// 字段元信息
template<typename T, FieldType Type, uint16_t Id, FieldModifier Mod>
struct Field {
  using ValueType = T;
  static constexpr FieldType kType = Type;
  static constexpr uint16_t kId = Id;
  static constexpr FieldModifier kModifier = Mod;
  static constexpr bool kIsStatic = (static_cast<uint8_t>(Mod) & static_cast<uint8_t>(FieldModifier::Static)) != 0;
  static constexpr bool kIsDynamic = (static_cast<uint8_t>(Mod) & static_cast<uint8_t>(FieldModifier::Dynamic)) != 0;
  
  ValueType value;
  
  Field() = default;
  explicit Field(ValueType v) : value(std::move(v)) {}
  
  // 隐式转换为值类型
  operator ValueType() const { return value; }
  operator ValueType&() { return value; }
};

// ============================================================================
// 字段声明宏
// ============================================================================

// 基础字段声明
#define FERN_FIELD(name, type, type_enum, id, modifier) \
  using name = cedar::api::Field<type, type_enum, id, modifier>; \
  static constexpr uint16_t k##name##Id = id;

// 静态字段（创建时确定，不变化）
#define STATIC_INT32_FIELD(name, id) \
  FERN_FIELD(name, int32_t, cedar::api::FieldType::Int32, id, cedar::api::FieldModifier::Static)

#define STATIC_INT64_FIELD(name, id) \
  FERN_FIELD(name, int64_t, cedar::api::FieldType::Int64, id, cedar::api::FieldModifier::Static)

#define STATIC_FLOAT_FIELD(name, id) \
  FERN_FIELD(name, float, cedar::api::FieldType::Float, id, cedar::api::FieldModifier::Static)

#define STATIC_STRING_FIELD(name, id, max_len) \
  FERN_FIELD(name, std::string, cedar::api::FieldType::String, id, cedar::api::FieldModifier::Static)

#define STATIC_TIMESTAMP_FIELD(name, id) \
  FERN_FIELD(name, cedar::Timestamp, cedar::api::FieldType::Timestamp, id, cedar::api::FieldModifier::Static)

#define STATIC_ID_FIELD(name, id) \
  FERN_FIELD(name, uint64_t, cedar::api::FieldType::EntityId, id, cedar::api::FieldModifier::Static)

// 动态字段（支持多版本历史）
#define DYNAMIC_INT32_FIELD(name, id) \
  FERN_FIELD(name, int32_t, cedar::api::FieldType::Int32, id, cedar::api::FieldModifier::Dynamic)

#define DYNAMIC_INT64_FIELD(name, id) \
  FERN_FIELD(name, int64_t, cedar::api::FieldType::Int64, id, cedar::api::FieldModifier::Dynamic)

#define DYNAMIC_FLOAT_FIELD(name, id) \
  FERN_FIELD(name, float, cedar::api::FieldType::Float, id, cedar::api::FieldModifier::Dynamic)

#define DYNAMIC_DOUBLE_FIELD(name, id) \
  FERN_FIELD(name, double, cedar::api::FieldType::Double, id, cedar::api::FieldModifier::Dynamic)

#define DYNAMIC_STRING_FIELD(name, id, max_len) \
  FERN_FIELD(name, std::string, cedar::api::FieldType::String, id, cedar::api::FieldModifier::Dynamic)

#define DYNAMIC_BOOL_FIELD(name, id) \
  FERN_FIELD(name, bool, cedar::api::FieldType::Bool, id, cedar::api::FieldModifier::Dynamic)

// 边类型声明
#define EDGE_TYPE(id) \
  static constexpr uint16_t kEdgeTypeId = id; \
  static constexpr bool kIsEdge = true;

// 节点类型声明
#define NODE_TYPE(id) \
  static constexpr uint16_t kNodeTypeId = id; \
  static constexpr bool kIsEdge = false;

// ============================================================================
// Schema 基类
// ============================================================================

template<typename Derived>
class Schema {
 public:
  // 获取实体类型 ID
  static constexpr uint16_t TypeId() {
    if constexpr (Derived::kIsEdge) {
      return Derived::kEdgeTypeId;
    } else {
      return Derived::kNodeTypeId;
    }
  }
  
  // 判断是否是边
  static constexpr bool IsEdge() { return Derived::kIsEdge; }
  
  // 获取所有字段 ID
  static std::vector<uint16_t> GetFieldIds() {
    return Derived::GetFieldIdsImpl();
  }
  
  // 获取字段名称映射
  static const std::map<std::string, uint16_t>& GetFieldNameMap() {
    return Derived::GetFieldNameMapImpl();
  }
  
  // 根据名称获取字段 ID
  static std::optional<uint16_t> GetFieldIdByName(const std::string& name) {
    const auto& map = GetFieldNameMap();
    auto it = map.find(name);
    if (it != map.end()) {
      return it->second;
    }
    return std::nullopt;
  }
};

// ============================================================================
// 系统保留字段
// ============================================================================

namespace system_fields {

// 系统保留列 ID 范围: 0-999
static constexpr uint16_t kExistence = 0;     // 存在标记
static constexpr uint16_t kLabel = 1;         // 标签类型
static constexpr uint16_t kCreatedAt = 2;     // 创建时间
static constexpr uint16_t kUpdatedAt = 3;     // 更新时间
static constexpr uint16_t kDeletedAt = 4;     // 删除时间
static constexpr uint16_t kSchemaVersion = 5; // Schema 版本

// 边系统字段
static constexpr uint16_t kEdgeExistence = 100;
static constexpr uint16_t kEdgeType = 101;
static constexpr uint16_t kSrcId = 102;
static constexpr uint16_t kDstId = 103;
static constexpr uint16_t kEdgeWeight = 104;

// 用户字段起始 ID
static constexpr uint16_t kUserFieldStart = 1000;

} // namespace system_fields

// ============================================================================
// Schema 使用示例 (请根据业务需求定义具体的 Schema)
// ============================================================================
/*
// 用户节点 Schema 示例
struct UserSchema : public Schema<UserSchema> {
  NODE_TYPE(1);
  
  STATIC_ID_FIELD(Id, 1000);
  STATIC_STRING_FIELD(Name, 1001, 64);
  DYNAMIC_INT32_FIELD(Age, 2000);
  
  static std::vector<uint16_t> GetFieldIdsImpl() {
    return {kIdId, kNameId, kAgeId};
  }
  
  static const std::map<std::string, uint16_t>& GetFieldNameMapImpl() {
    static const std::map<std::string, uint16_t> kMap = {
      {"id", kIdId}, {"name", kNameId}, {"age", kAgeId}
    };
    return kMap;
  }
};

// 关注边 Schema 示例
struct FollowsSchema : public Schema<FollowsSchema> {
  EDGE_TYPE(1);
  
  STATIC_FLOAT_FIELD(Weight, 1000);
  STATIC_TIMESTAMP_FIELD(Since, 1001);
  
  static std::vector<uint16_t> GetFieldIdsImpl() {
    return {kWeightId, kSinceId};
  }
  
  static const std::map<std::string, uint16_t>& GetFieldNameMapImpl() {
    static const std::map<std::string, uint16_t> kMap = {
      {"weight", kWeightId}, {"since", kSinceId}
    };
    return kMap;
  }
};
*/

} // namespace api
} // namespace cedar

#endif // FERN_API_SCHEMA_H_
