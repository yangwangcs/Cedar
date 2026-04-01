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

#ifndef FERN_API_SCHEMA_V2_H_
#define FERN_API_SCHEMA_V2_H_

#include <cstdint>
#include <string>
#include <vector>
#include <map>
#include <type_traits>

#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

namespace cedar {
namespace api {

// ============================================================================
// 属性分类标记
// ============================================================================

// 属性存储类型
enum class StorageType : uint8_t {
  Inline = 0,      // 内联存储（≤4B，存在 Descriptor 中）
  External = 1,    // 外部存储（大对象，存 blob）
  Compressed = 2,  // 压缩存储
};

// 属性时间特性
enum class TemporalType : uint8_t {
  Static = 0,      // 静态：创建时确定，终身不变（如用户ID、注册时间）
  Dynamic = 1,     // 动态：会变化，保留历史版本（如年龄、积分）
  Temporal = 2,    // 时态：时间序列数据（如位置轨迹、传感器读数）
  Ephemeral = 3,   // 短暂：只有最新值有意义（如在线状态、心跳）
};

// 属性索引标记
enum class IndexType : uint8_t {
  None = 0,        // 无索引
  Primary = 1,     // 主键索引
  Secondary = 2,   // 二级索引
  FullText = 3,    // 全文索引
  Spatial = 4,     // 空间索引
};

// ============================================================================
// 字段定义（通用版）
// ============================================================================

template<typename T,
         uint16_t Id,
         TemporalType Temporal,
         StorageType Storage = StorageType::Inline,
         IndexType Index = IndexType::None>
struct Field {
  using ValueType = T;
  static constexpr uint16_t kId = Id;
  static constexpr TemporalType kTemporal = Temporal;
  static constexpr StorageType kStorage = Storage;
  static constexpr IndexType kIndex = Index;
  
  // 便捷判断
  static constexpr bool kIsStatic = (Temporal == TemporalType::Static);
  static constexpr bool kIsDynamic = (Temporal == TemporalType::Dynamic);
  static constexpr bool kIsTemporal = (Temporal == TemporalType::Temporal);
  static constexpr bool kIsEphemeral = (Temporal == TemporalType::Ephemeral);
  static constexpr bool kIsIndexed = (Index != IndexType::None);
  
  ValueType value;
  
  Field() = default;
  explicit Field(ValueType v) : value(std::move(v)) {}
  
  operator ValueType() const { return value; }
  operator ValueType&() { return value; }
};

// ============================================================================
// 便捷宏定义（带语义化前缀）
// ============================================================================

// 静态字段宏（生命周期不变）
#define STATIC_FIELD(name, type, id) \
  using name = cedar::api::Field<type, id, cedar::api::TemporalType::Static>

#define STATIC_INLINE_INT32(name, id) \
  using name = cedar::api::Field<int32_t, id, cedar::api::TemporalType::Static, cedar::api::StorageType::Inline>

#define STATIC_INLINE_INT64(name, id) \
  using name = cedar::api::Field<int64_t, id, cedar::api::TemporalType::Static, cedar::api::StorageType::Inline>

#define STATIC_INLINE_FLOAT(name, id) \
  using name = cedar::api::Field<float, id, cedar::api::TemporalType::Static, cedar::api::StorageType::Inline>

#define STATIC_INLINE_STRING(name, id, max_len) \
  using name = cedar::api::Field<std::string, id, cedar::api::TemporalType::Static, cedar::api::StorageType::Inline>

#define STATIC_EXTERNAL_STRING(name, id) \
  using name = cedar::api::Field<std::string, id, cedar::api::TemporalType::Static, cedar::api::StorageType::External>

// 动态字段宏（会变化，保留历史）
#define DYNAMIC_FIELD(name, type, id) \
  using name = cedar::api::Field<type, id, cedar::api::TemporalType::Dynamic>

#define DYNAMIC_INLINE_INT32(name, id) \
  using name = cedar::api::Field<int32_t, id, cedar::api::TemporalType::Dynamic, cedar::api::StorageType::Inline>

#define DYNAMIC_INLINE_INT64(name, id) \
  using name = cedar::api::Field<int64_t, id, cedar::api::TemporalType::Dynamic, cedar::api::StorageType::Inline>

#define DYNAMIC_INLINE_FLOAT(name, id) \
  using name = cedar::api::Field<float, id, cedar::api::TemporalType::Dynamic, cedar::api::StorageType::Inline>

#define DYNAMIC_EXTERNAL_STRING(name, id) \
  using name = cedar::api::Field<std::string, id, cedar::api::TemporalType::Dynamic, cedar::api::StorageType::External>

// 时态字段宏（时间序列）
#define TEMPORAL_FIELD(name, type, id) \
  using name = cedar::api::Field<type, id, cedar::api::TemporalType::Temporal>

// 短暂字段宏（只有最新值）
#define EPHEMERAL_FIELD(name, type, id) \
  using name = cedar::api::Field<type, id, cedar::api::TemporalType::Ephemeral>

// ============================================================================
// 实体基类（通用版）
// ============================================================================

// 实体类型标记
enum class EntityKind : uint8_t {
  Vertex = 0,
  Edge = 1,
};

// 实体基类模板
template<typename Derived, EntityKind Kind>
class EntityDef {
 public:
  static constexpr EntityKind kKind = Kind;
  static constexpr bool kIsVertex = (Kind == EntityKind::Vertex);
  static constexpr bool kIsEdge = (Kind == EntityKind::Edge);
  
  // 获取实体类型名称
  static const char* KindName() {
    return kIsVertex ? "Vertex" : "Edge";
  }
  
  // 获取 Schema 元信息（子类实现）
  static const std::vector<uint16_t>& GetStaticFieldIds() {
    return Derived::GetStaticFieldIdsImpl();
  }
  
  static const std::vector<uint16_t>& GetDynamicFieldIds() {
    return Derived::GetDynamicFieldIdsImpl();
  }
  
  static const std::map<std::string, uint16_t>& GetFieldNameMap() {
    return Derived::GetFieldNameMapImpl();
  }
  
  static const std::map<uint16_t, std::string>& GetFieldTypeMap() {
    return Derived::GetFieldTypeMapImpl();
  }
  
  // 打印 Schema 信息
  static void PrintSchema() {
    std::cout << "========================================" << std::endl;
    std::cout << "Schema: " << Derived::kName << std::endl;
    std::cout << "Type: " << KindName() << std::endl;
    std::cout << "Type ID: " << Derived::kTypeId << std::endl;
    std::cout << "----------------------------------------" << std::endl;
    
    std::cout << "Static Fields (生命周期不变):" << std::endl;
    for (const auto& [name, id] : GetFieldNameMap()) {
      auto type_it = GetFieldTypeMap().find(id);
      if (type_it != GetFieldTypeMap().end()) {
        // 只显示静态字段
        if (type_it->second.find("[static]") != std::string::npos) {
          std::cout << "  [" << id << "] " << name << " (" << type_it->second << ")" << std::endl;
        }
      }
    }
    
    std::cout << "Dynamic Fields (支持历史版本):" << std::endl;
    for (const auto& [name, id] : GetFieldNameMap()) {
      auto type_it = GetFieldTypeMap().find(id);
      if (type_it != GetFieldTypeMap().end()) {
        // 只显示动态字段
        if (type_it->second.find("[dynamic]") != std::string::npos) {
          std::cout << "  [" << id << "] " << name << " (" << type_it->second << ")" << std::endl;
        }
      }
    }
    std::cout << "========================================" << std::endl;
  }
};

// 节点基类
template<typename Derived>
using VertexDef = EntityDef<Derived, EntityKind::Vertex>;

// 边基类
template<typename Derived>
using EdgeDef = EntityDef<Derived, EntityKind::Edge>;

// ============================================================================
// 实体声明宏（简化定义）
// ============================================================================

#define DEFINE_VERTEX(name, type_id) \
  static constexpr const char* kName = #name; \
  static constexpr uint16_t kTypeId = type_id; \
  static constexpr bool kIsEdge = false;

#define DEFINE_EDGE(name, type_id) \
  static constexpr const char* kName = #name; \
  static constexpr uint16_t kTypeId = type_id; \
  static constexpr bool kIsEdge = true;

// ============================================================================
// 示例：通用社交网络 Schema
// ============================================================================

// 用户节点（Vertex）
struct UserDef : public VertexDef<UserDef> {
  DEFINE_VERTEX(User, 1);
  
  // ==================== 静态属性（创建时确定，终身不变）====================
  // 身份信息
  STATIC_INLINE_INT64(UserId, 1000);           // 用户唯一ID
  STATIC_INLINE_STRING(Username, 1001, 32);    // 用户名（不可改）
  STATIC_INLINE_STRING(Email, 1002, 64);       // 邮箱（需验证）
  STATIC_INLINE_INT64(CreatedAt, 1003);        // 注册时间戳
  STATIC_INLINE_INT32(AccountType, 1004);      // 账号类型：1=个人, 2=企业
  
  // 固定属性
  STATIC_INLINE_STRING(CountryCode, 1005, 2);  // 国家代码（ISO 3166-1 alpha-2）
  STATIC_INLINE_INT32(Gender, 1006);           // 性别：0=未知, 1=男, 2=女
  
  // ==================== 动态属性（会变化，保留历史版本）====================
  // 基本信息
  DYNAMIC_INLINE_INT32(Age, 2000);             // 年龄（每年变化）
  DYNAMIC_EXTERNAL_STRING(Bio, 2001);          // 个人简介
  DYNAMIC_EXTERNAL_STRING(Avatar, 2002);       // 头像URL
  
  // 社交属性
  DYNAMIC_INLINE_INT32(FollowersCount, 2100);  // 粉丝数
  DYNAMIC_INLINE_INT32(FollowingCount, 2101);  // 关注数
  DYNAMIC_INLINE_INT32(PostsCount, 2102);      // 发帖数
  
  // 状态属性
  DYNAMIC_INLINE_FLOAT(Reputation, 2200);      // 信誉分 0.0-100.0
  DYNAMIC_INLINE_INT32(Level, 2201);           // 用户等级
  
  // ==================== 短暂属性（只有最新值有意义）====================
  EPHEMERAL_FIELD(OnlineStatus, int32_t, 3000); // 在线状态：0=离线, 1=在线, 2=忙碌
  EPHEMERAL_FIELD(LastActiveAt, int64_t, 3001); // 最后活跃时间
  
  // ==================== Schema 元信息实现 ====================
  static const std::vector<uint16_t>& GetStaticFieldIdsImpl() {
    static const std::vector<uint16_t> kIds = {
      1000, 1001, 1002, 1003, 1004, 1005, 1006
    };
    return kIds;
  }
  
  static const std::vector<uint16_t>& GetDynamicFieldIdsImpl() {
    static const std::vector<uint16_t> kIds = {
      2000, 2001, 2002, 2100, 2101, 2102, 2200, 2201
    };
    return kIds;
  }
  
  static const std::map<std::string, uint16_t>& GetFieldNameMapImpl() {
    static const std::map<std::string, uint16_t> kMap = {
      // 静态字段
      {"user_id", 1000}, {"username", 1001}, {"email", 1002},
      {"created_at", 1003}, {"account_type", 1004},
      {"country_code", 1005}, {"gender", 1006},
      // 动态字段
      {"age", 2000}, {"bio", 2001}, {"avatar", 2002},
      {"followers_count", 2100}, {"following_count", 2101}, {"posts_count", 2102},
      {"reputation", 2200}, {"level", 2201},
      // 短暂字段
      {"online_status", 3000}, {"last_active_at", 3001},
    };
    return kMap;
  }
  
  static const std::map<uint16_t, std::string>& GetFieldTypeMapImpl() {
    static const std::map<uint16_t, std::string> kMap = {
      // 静态字段
      {1000, "int64[static]"}, {1001, "string[static]"}, {1002, "string[static]"},
      {1003, "timestamp[static]"}, {1004, "int32[static]"},
      {1005, "string[static]"}, {1006, "int32[static]"},
      // 动态字段
      {2000, "int32[dynamic]"}, {2001, "string[dynamic,external]"}, {2002, "string[dynamic,external]"},
      {2100, "int32[dynamic]"}, {2101, "int32[dynamic]"}, {2102, "int32[dynamic]"},
      {2200, "float[dynamic]"}, {2201, "int32[dynamic]"},
      // 短暂字段
      {3000, "int32[ephemeral]"}, {3001, "timestamp[ephemeral]"},
    };
    return kMap;
  }
};

// 关注边（Edge）
struct FollowsDef : public EdgeDef<FollowsDef> {
  DEFINE_EDGE(Follows, 1);
  
  // ==================== 静态属性（创建时确定）====================
  STATIC_INLINE_INT64(SourceId, 1000);         // 源用户ID
  STATIC_INLINE_INT64(TargetId, 1001);         // 目标用户ID
  STATIC_INLINE_INT64(CreatedAt, 1002);        // 关注时间
  STATIC_INLINE_INT32(FollowType, 1003);       // 关注类型：1=普通, 2=悄悄关注
  
  // ==================== 动态属性（会变化）====================
  DYNAMIC_INLINE_FLOAT(Weight, 2000);          // 关注权重（算法计算）
  DYNAMIC_INLINE_INT32(InteractCount, 2001);   // 互动次数
  DYNAMIC_INLINE_INT32(Visibility, 2002);      // 可见性：0=公开, 1=仅自己
  
  // ==================== Schema 元信息实现 ====================
  static const std::vector<uint16_t>& GetStaticFieldIdsImpl() {
    static const std::vector<uint16_t> kIds = {1000, 1001, 1002, 1003};
    return kIds;
  }
  
  static const std::vector<uint16_t>& GetDynamicFieldIdsImpl() {
    static const std::vector<uint16_t> kIds = {2000, 2001, 2002};
    return kIds;
  }
  
  static const std::map<std::string, uint16_t>& GetFieldNameMapImpl() {
    static const std::map<std::string, uint16_t> kMap = {
      {"source_id", 1000}, {"target_id", 1001}, {"created_at", 1002}, {"follow_type", 1003},
      {"weight", 2000}, {"interact_count", 2001}, {"visibility", 2002},
    };
    return kMap;
  }
  
  static const std::map<uint16_t, std::string>& GetFieldTypeMapImpl() {
    static const std::map<uint16_t, std::string> kMap = {
      {1000, "int64[static]"}, {1001, "int64[static]"}, {1002, "timestamp[static]"}, {1003, "int32[static]"},
      {2000, "float[dynamic]"}, {2001, "int32[dynamic]"}, {2002, "int32[dynamic]"},
    };
    return kMap;
  }
};

// 帖子节点（Vertex）
struct PostDef : public VertexDef<PostDef> {
  DEFINE_VERTEX(Post, 2);
  
  // ==================== 静态属性 ====================
  STATIC_INLINE_INT64(PostId, 1000);           // 帖子ID
  STATIC_INLINE_INT64(AuthorId, 1001);         // 作者ID
  STATIC_INLINE_INT64(CreatedAt, 1002);        // 发布时间
  STATIC_INLINE_INT32(ContentType, 1003);      // 内容类型：1=文本, 2=图片, 3=视频
  
  // ==================== 动态属性 ====================
  DYNAMIC_EXTERNAL_STRING(Content, 2000);      // 内容（可编辑）
  DYNAMIC_INLINE_INT32(LikesCount, 2001);      // 点赞数
  DYNAMIC_INLINE_INT32(CommentsCount, 2002);   // 评论数
  DYNAMIC_INLINE_INT32(SharesCount, 2003);     // 分享数
  DYNAMIC_INLINE_INT32(ViewsCount, 2004);      // 浏览数
  
  static const std::vector<uint16_t>& GetStaticFieldIdsImpl() {
    static const std::vector<uint16_t> kIds = {1000, 1001, 1002, 1003};
    return kIds;
  }
  
  static const std::vector<uint16_t>& GetDynamicFieldIdsImpl() {
    static const std::vector<uint16_t> kIds = {2000, 2001, 2002, 2003, 2004};
    return kIds;
  }
  
  static const std::map<std::string, uint16_t>& GetFieldNameMapImpl() {
    static const std::map<std::string, uint16_t> kMap = {
      {"post_id", 1000}, {"author_id", 1001}, {"created_at", 1002}, {"content_type", 1003},
      {"content", 2000}, {"likes_count", 2001}, {"comments_count", 2002}, 
      {"shares_count", 2003}, {"views_count", 2004},
    };
    return kMap;
  }
  
  static const std::map<uint16_t, std::string>& GetFieldTypeMapImpl() {
    static const std::map<uint16_t, std::string> kMap = {
      {1000, "int64[static]"}, {1001, "int64[static]"}, {1002, "timestamp[static]"}, {1003, "int32[static]"},
      {2000, "string[dynamic,external]"}, {2001, "int32[dynamic]"}, {2002, "int32[dynamic]"},
      {2003, "int32[dynamic]"}, {2004, "int32[dynamic]"},
    };
    return kMap;
  }
};

// 点赞边（Edge）
struct LikeDef : public EdgeDef<LikeDef> {
  DEFINE_EDGE(Like, 2);
  
  STATIC_INLINE_INT64(UserId, 1000);           // 用户ID
  STATIC_INLINE_INT64(PostId, 1001);           // 帖子ID
  STATIC_INLINE_INT64(CreatedAt, 1002);        // 点赞时间
  STATIC_INLINE_INT32(LikeType, 1003);         // 点赞类型：1=普通, 2=超级赞
  
  static const std::vector<uint16_t>& GetStaticFieldIdsImpl() {
    static const std::vector<uint16_t> kIds = {1000, 1001, 1002, 1003};
    return kIds;
  }
  
  static const std::vector<uint16_t>& GetDynamicFieldIdsImpl() {
    static const std::vector<uint16_t> kIds = {};
    return kIds;
  }
  
  static const std::map<std::string, uint16_t>& GetFieldNameMapImpl() {
    static const std::map<std::string, uint16_t> kMap = {
      {"user_id", 1000}, {"post_id", 1001}, {"created_at", 1002}, {"like_type", 1003},
    };
    return kMap;
  }
  
  static const std::map<uint16_t, std::string>& GetFieldTypeMapImpl() {
    static const std::map<uint16_t, std::string> kMap = {
      {1000, "int64[static]"}, {1001, "int64[static]"}, {1002, "timestamp[static]"}, {1003, "int32[static]"},
    };
    return kMap;
  }
};

} // namespace api
} // namespace cedar

#endif // FERN_API_SCHEMA_V2_H_
