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

// =============================================================================
// CedarKey - CedarGraph 标准 Key 设计
// =============================================================================
// 32 字节固定长度，针对现代 CPU 优化：
// - 缓存行对齐：64B 缓存行可放 2 个 Key
// - SIMD 友好：8B 对齐，memcmp 可用 AVX2
// - 固定长度：简化比较逻辑，无分支预测失败
// - 降序时间戳：最新版本自然排前
// - 双向边支持：EdgeOut + EdgeIn 实现 O(log N) 入边查询
// =============================================================================

#ifndef FERN_FERN_KEY_H_
#define FERN_FERN_KEY_H_

#include <cstdint>
#include <cstring>
#include <limits>
#include <chrono>
#include <optional>
#include <string>
#include <string_view>

// 字节序转换（跨平台）
#ifdef __APPLE__
#include <libkern/OSByteOrder.h>
#define cedar_htobe64(x) OSSwapHostToBigInt64(x)
#define cedar_be64toh(x) OSSwapBigToHostInt64(x)
#elif defined(__linux__)
#include <endian.h>
#define cedar_htobe64(x) htobe64(x)
#define cedar_be64toh(x) be64toh(x)
#else
// 通用实现
inline uint64_t cedar_htobe64(uint64_t x) {
  #if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  return ((x & 0xFF00000000000000ULL) >> 56) |
         ((x & 0x00FF000000000000ULL) >> 40) |
         ((x & 0x0000FF0000000000ULL) >> 24) |
         ((x & 0x000000FF00000000ULL) >> 8)  |
         ((x & 0x00000000FF000000ULL) << 8)  |
         ((x & 0x0000000000FF0000ULL) << 24) |
         ((x & 0x000000000000FF00ULL) << 40) |
         ((x & 0x00000000000000FFULL) << 56);
  #else
  return x;
  #endif
}
inline uint64_t cedar_be64toh(uint64_t x) { return cedar_htobe64(x); }
#endif

namespace cedar {

// =============================================================================
// 时间戳（微秒级）- 降序存储编码
// =============================================================================

class Timestamp {
 public:
  Timestamp(uint64_t micros = 0) : value_(micros) {}
  
  uint64_t value() const { return value_; }
  
  // 转换为用于降序存储的大端序
  // 原理：时间戳越大（越新），存储值越小（排前面）
  uint64_t EncodeForStorage() const {
    const uint64_t max = std::numeric_limits<uint64_t>::max();
    return cedar_htobe64(max - value_);
  }
  
  // 从存储格式解码
  static Timestamp DecodeFromStorage(uint64_t stored_be) {
    const uint64_t max = std::numeric_limits<uint64_t>::max();
    uint64_t decoded = max - cedar_be64toh(stored_be);
    return Timestamp(decoded);
  }
  
  static Timestamp Max() { return Timestamp(std::numeric_limits<uint64_t>::max()); }
  static Timestamp Min() { return Timestamp(0); }
  /// 静态属性时间戳（值为1，区分于Min()=0）
  static Timestamp Static() { return Timestamp(1); }
  /// 检查是否为静态属性时间戳
  bool IsStatic() const { return value_ == 1; }
  static Timestamp Now() {
    // Get current time in microseconds since epoch
    auto now = std::chrono::system_clock::now();
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch()).count();
    return Timestamp(static_cast<uint64_t>(micros));
  }
  
  // 显式转换到 uint64_t
  explicit operator uint64_t() const { return value_; }
  
  bool operator==(const Timestamp& other) const { return value_ == other.value_; }
  bool operator!=(const Timestamp& other) const { return value_ != other.value_; }
  bool operator<(const Timestamp& other) const { return value_ < other.value_; }
  bool operator>(const Timestamp& other) const { return value_ > other.value_; }
  bool operator<=(const Timestamp& other) const { return value_ <= other.value_; }
  bool operator>=(const Timestamp& other) const { return value_ >= other.value_; }

 private:
  uint64_t value_;
};

// =============================================================================
// 强类型 ID（编译期类型安全）
// =============================================================================

/// 点的列ID（属性ID）- 仅用于 Vertex
struct VertexColumnId {
  uint16_t value;
  explicit constexpr VertexColumnId(uint16_t v) : value(v) {}
  operator uint16_t() const { return value; }
};

/// 边的类型ID - 仅用于 Edge
struct EdgeTypeId {
  uint16_t value;
  explicit constexpr EdgeTypeId(uint16_t v) : value(v) {}
  operator uint16_t() const { return value; }
};

// 便捷字面量
inline constexpr VertexColumnId operator""_vcol(unsigned long long v) {
  return VertexColumnId(static_cast<uint16_t>(v));
}
inline constexpr EdgeTypeId operator""_etype(unsigned long long v) {
  return EdgeTypeId(static_cast<uint16_t>(v));
}

// =============================================================================
// 实体类型
// =============================================================================

enum class EntityType : uint8_t {
  Vertex = 0,    // 点
  EdgeOut = 1,   // 出边 (src->dst)
  EdgeIn = 2,    // 入边 (dst<-src)，用于快速反向查询
};

// =============================================================================
// Flags 位定义
// =============================================================================

namespace key_flags {
  constexpr uint8_t kNone = 0;
  constexpr uint8_t kDeleted = 1 << 0;       // 逻辑删除标记
  constexpr uint8_t kCompressed = 1 << 1;    // Value 使用压缩
  constexpr uint8_t kHasExtension = 1 << 2;  // target_id 包含扩展数据
  constexpr uint8_t kTombstone = 1 << 3;     // 墓碑标记（用于 MVCC）
}

// =============================================================================
// 32 字节固定长度 Key - CedarGraph 标准 Key
// =============================================================================

// 内存布局（严格按此顺序，确保 8 字节对齐）：
// Offset 0-7:   entity_id (uint64_t)
// Offset 8-15:  timestamp_be (uint64_t) - 降序存储
// Offset 16-23: target_id (uint64_t) - dst_id 或扩展数据
// Offset 24-25: column_id (uint16_t)
// Offset 26-27: sequence (uint16_t)
// Offset 28:    entity_type (uint8_t)
// Offset 29:    flags (uint8_t)
// Offset 30-31: reserved (uint16_t)
class alignas(8) CedarKey {
 public:
  // 大小常量
  static constexpr size_t kKeySize = 32;
  static constexpr size_t kUserKeySize = 19;  // 不含 timestamp_be(8) + sequence(2) + flags(1) + reserved(2) = 13
  
  // ==================== 默认构造 ====================
  
  /// 默认构造 - 创建空 Key（所字段为0）
  CedarKey() 
      : entity_id_(0),
        timestamp_be_(0),
        target_id_(0),
        column_id_(0),
        sequence_(0),
        entity_type_(0),
        flags_(0),
        reserved_(0) {}
  
  /// 完整构造 - 兼容旧代码用法
  /// \note 仅用于内部实现，外部应使用 Vertex/EdgeOut/EdgeIn 工厂方法
  CedarKey(uint64_t entity_id, 
          EntityType entity_type,
          uint16_t column_id,
          Timestamp timestamp,
          uint16_t sequence = 0,
          uint64_t target_id = 0,
          uint8_t flags = 0)
      : entity_id_(cedar_htobe64(entity_id)),  // 统一字节序转换
        timestamp_be_(timestamp.EncodeForStorage()),
        target_id_(target_id),
        column_id_(column_id),
        sequence_(sequence),
        entity_type_(static_cast<uint8_t>(entity_type)),
        flags_(flags),
        reserved_(0) {}
  
  // ==================== 工厂方法 ====================
  
  /// 构造点 Key
  static CedarKey Vertex(uint64_t vertex_id,
                        VertexColumnId col,
                        Timestamp ts,
                        uint16_t seq = 0,
                        uint64_t extension = 0,
                        uint8_t flags = 0) {
    CedarKey key;
    key.entity_id_ = cedar_htobe64(vertex_id);
    key.timestamp_be_ = ts.EncodeForStorage();
    key.target_id_ = extension;  // 点使用 target_id 存储扩展数据（如轻量权重）
    key.column_id_ = col.value;
    key.sequence_ = seq;
    key.entity_type_ = static_cast<uint8_t>(EntityType::Vertex);
    key.flags_ = flags;
    key.reserved_ = 0;
    return key;
  }
  
  /// 构造点 Key（兼容 uint16_t）
  static CedarKey Vertex(uint64_t vertex_id,
                        uint16_t col_id,
                        Timestamp ts,
                        uint16_t seq = 0,
                        uint64_t extension = 0,
                        uint8_t flags = 0) {
    return Vertex(vertex_id, VertexColumnId(col_id), ts, seq, extension, flags);
  }
  
  /// 构造出边 Key (src -> dst)
  static CedarKey EdgeOut(uint64_t src_id,
                         uint64_t dst_id,
                         EdgeTypeId edge_type,
                         Timestamp ts,
                         uint16_t seq = 0,
                         uint8_t flags = 0) {
    CedarKey key;
    key.entity_id_ = cedar_htobe64(src_id);
    key.timestamp_be_ = ts.EncodeForStorage();
    key.target_id_ = dst_id;
    key.column_id_ = edge_type.value;
    key.sequence_ = seq;
    key.entity_type_ = static_cast<uint8_t>(EntityType::EdgeOut);
    key.flags_ = flags;
    key.reserved_ = 0;
    return key;
  }
  
  /// 构造出边 Key（兼容 uint16_t）
  static CedarKey EdgeOut(uint64_t src_id,
                         uint64_t dst_id,
                         uint16_t edge_type,
                         Timestamp ts,
                         uint16_t seq = 0,
                         uint8_t flags = 0) {
    return EdgeOut(src_id, dst_id, EdgeTypeId(edge_type), ts, seq, flags);
  }
  
  /// 构造入边 Key (dst <- src)，用于反向索引
  static CedarKey EdgeIn(uint64_t dst_id,
                        uint64_t src_id,
                        EdgeTypeId edge_type,
                        Timestamp ts,
                        uint16_t seq = 0,
                        uint8_t flags = 0) {
    CedarKey key;
    key.entity_id_ = cedar_htobe64(dst_id);   // 注意：这里存的是 dst（查询入口）
    key.timestamp_be_ = ts.EncodeForStorage();
    key.target_id_ = src_id;   // 这里存的是 src（邻居）
    key.column_id_ = edge_type.value;
    key.sequence_ = seq;
    key.entity_type_ = static_cast<uint8_t>(EntityType::EdgeIn);
    key.flags_ = flags;
    key.reserved_ = 0;
    return key;
  }
  
  /// 构造入边 Key（兼容 uint16_t）
  static CedarKey EdgeIn(uint64_t dst_id,
                        uint64_t src_id,
                        uint16_t edge_type,
                        Timestamp ts,
                        uint16_t seq = 0,
                        uint8_t flags = 0) {
    return EdgeIn(dst_id, src_id, EdgeTypeId(edge_type), ts, seq, flags);
  }
  
  /// 创建通用边（自动选择正向/反向）
  static std::pair<CedarKey, CedarKey> MakeEdge(uint64_t src_id,
                                              uint64_t dst_id,
                                              EdgeTypeId edge_type,
                                              Timestamp ts,
                                              uint16_t seq = 0) {
    return {
      EdgeOut(src_id, dst_id, edge_type, ts, seq),
      EdgeIn(dst_id, src_id, edge_type, ts, seq)
    };
  }

  // ==================== 编码/解码 ====================
  
  /// 编码为 32 字节字符串（可直接写入 LSM-Tree）
  std::string Encode() const {
    std::string result;
    result.resize(kKeySize);
    std::memcpy(result.data(), this, kKeySize);
    return result;
  }
  
  /// 编码到已有缓冲区
  void EncodeTo(void* buffer) const {
    std::memcpy(buffer, this, kKeySize);
  }
  
  /// 从字节切片解码（要求长度 >= 32）
  static std::optional<CedarKey> Decode(std::string_view slice) {
    if (slice.size() < kKeySize) return std::nullopt;
    CedarKey key;
    std::memcpy(&key, slice.data(), kKeySize);
    return key;
  }
  
  /// 从指针解码（要求有效内存 >= 32）
  static CedarKey Decode(const void* ptr) {
    CedarKey key;
    std::memcpy(&key, ptr, kKeySize);
    return key;
  }
  


  // ==================== 访问器 ====================
  
  uint64_t entity_id() const { return cedar_be64toh(entity_id_); }
  uint64_t target_id() const { return target_id_; }
  uint16_t column_id() const { return column_id_; }
  uint16_t sequence() const { return sequence_; }
  EntityType entity_type() const { return static_cast<EntityType>(entity_type_); }
  uint8_t flags() const { return flags_; }
  uint16_t reserved() const { return reserved_; }
  
  Timestamp timestamp() const {
    return Timestamp::DecodeFromStorage(timestamp_be_);
  }
  
  // 原始时间戳编码值（用于比较）
  uint64_t timestamp_be() const { return timestamp_be_; }
  
  // 便捷判断
  bool IsVertex() const { return entity_type_ == 0; }
  bool IsEdgeOut() const { return entity_type_ == 1; }
  bool IsEdgeIn() const { return entity_type_ == 2; }
  bool IsEdge() const { return entity_type_ == 1 || entity_type_ == 2; }
  
  // 标记检查
  bool IsDeleted() const { return (flags_ & key_flags::kDeleted) != 0; }
  bool IsCompressed() const { return (flags_ & key_flags::kCompressed) != 0; }
  bool IsTombstone() const { return (flags_ & key_flags::kTombstone) != 0; }
  
  // 获取边的对端 ID（自动处理出边/入边）
  uint64_t GetEdgePeerId() const {
    if (IsEdge()) return target_id_;
    return 0;  // Vertex 返回 0
  }
  
  // 获取入边的源 ID（仅入边有效）
  uint64_t GetInEdgeSrcId() const {
    return IsEdgeIn() ? target_id_ : 0;
  }
  
  // 获取出边的目标 ID（仅出边有效）
  uint64_t GetOutEdgeDstId() const {
    return IsEdgeOut() ? target_id_ : 0;
  }

  // ==================== 修改器 ====================
  
  void SetFlags(uint8_t flags) { flags_ = flags; }
  void AddFlags(uint8_t flags) { flags_ |= flags; }
  void ClearFlags(uint8_t flags) { flags_ &= ~flags; }
  void SetSequence(uint16_t seq) { sequence_ = seq; }
  void SetReserved(uint16_t reserved) { reserved_ = reserved; }

  // ==================== LSM-Tree 比较器支持 ====================
  
  // 三向比较（按字段比较，正确处理小端整数）
  int Compare(const CedarKey& other) const {
    // 按字段顺序比较，避免小端字节序问题
    // entity_id 和 timestamp 是大端序存储，但使用数值比较更安全
    if (entity_id() != other.entity_id()) {
      return entity_id() < other.entity_id() ? -1 : 1;
    }
    if (entity_type_ != other.entity_type_) {
      return entity_type_ < other.entity_type_ ? -1 : 1;
    }
    if (column_id_ != other.column_id_) {
      return column_id_ < other.column_id_ ? -1 : 1;
    }
    if (target_id_ != other.target_id_) {
      return target_id_ < other.target_id_ ? -1 : 1;
    }
    // timestamp 是大端序存储，直接比较字节
    if (timestamp_be_ != other.timestamp_be_) {
      return timestamp_be_ < other.timestamp_be_ ? -1 : 1;
    }
    if (sequence_ != other.sequence_) {
      return sequence_ < other.sequence_ ? -1 : 1;
    }
    if (flags_ != other.flags_) {
      return flags_ < other.flags_ ? -1 : 1;
    }
    return 0;
  }
  
  // 比较器运算符
  bool operator<(const CedarKey& other) const { return Compare(other) < 0; }
  bool operator>(const CedarKey& other) const { return Compare(other) > 0; }
  bool operator==(const CedarKey& other) const { return Compare(other) == 0; }
  bool operator!=(const CedarKey& other) const { return Compare(other) != 0; }
  bool operator<=(const CedarKey& other) const { return Compare(other) <= 0; }
  bool operator>=(const CedarKey& other) const { return Compare(other) >= 0; }
  
  // 仅比较 UserKey 部分（不含 timestamp/sequence，用于 MVCC）
  int CompareUserKey(const CedarKey& other) const {
    // 比较：entity_id(8) + entity_type(1) + column_id(2) + target_id(8)
    if (entity_id() != other.entity_id()) {
      return entity_id() < other.entity_id() ? -1 : 1;
    }
    if (entity_type_ != other.entity_type_) {
      return entity_type_ < other.entity_type_ ? -1 : 1;
    }
    if (column_id_ != other.column_id_) {
      return column_id_ < other.column_id_ ? -1 : 1;
    }
    if (target_id_ != other.target_id_) {
      return target_id_ < other.target_id_ ? -1 : 1;
    }
    return 0;
  }
  
  bool SameUserKey(const CedarKey& other) const {
    return CompareUserKey(other) == 0;
  }

  // ==================== 序列化辅助 ====================
  
  // 获取 UserKey 字节（用于 Bloom Filter）
  std::string GetUserKeyBytes() const {
    std::string result;
    result.resize(kUserKeySize);
    char* p = result.data();
    std::memcpy(p, &entity_id_, 8); p += 8;
    std::memcpy(p, &entity_type_, 1); p += 1;
    std::memcpy(p, &column_id_, 2); p += 2;
    std::memcpy(p, &target_id_, 8);
    return result;
  }
  
  // 调试字符串
  std::string DebugString() const;

 private:
  // 严格按此顺序布局，确保 8 字节对齐
  uint64_t entity_id_;      // +0  - 点 ID / 边 Src ID / 反向边 Dst ID
  uint64_t timestamp_be_;   // +8  - 降序存储的大端序时间戳
  uint64_t target_id_;      // +16 - 边 Dst ID / 反向边 Src ID / 点扩展数据
  uint16_t column_id_;      // +24 - 属性 ID（点）/ 边类型 ID（边）
  uint16_t sequence_;       // +26 - 同一微秒内的版本序列号
  uint8_t  entity_type_;    // +28 - EntityType
  uint8_t  flags_;          // +29 - 标记位
  uint16_t reserved_;       // +30 - 未来扩展（TTL、分区 ID 等）
};

static_assert(sizeof(CedarKey) == 32, "CedarKey must be exactly 32 bytes");
static_assert(alignof(CedarKey) == 8, "CedarKey must be 8-byte aligned");

// =============================================================================
// LSM-Tree 比较器
// =============================================================================

class CedarKeyComparator {
 public:
  using KeyType = CedarKey;
  
  // 标准 LSM-Tree 比较接口
  int Compare(std::string_view a, std::string_view b) const {
    // 直接比较 32 字节内存（利用 memcmp 的 SIMD 优化）
    if (a.size() < CedarKey::kKeySize || b.size() < CedarKey::kKeySize) {
      return a.size() < b.size() ? -1 : (a.size() > b.size() ? 1 : 0);
    }
    return std::memcmp(a.data(), b.data(), CedarKey::kKeySize);
  }
  
  // 比较两个 CedarKey 对象
  int Compare(const CedarKey& a, const CedarKey& b) const {
    return a.Compare(b);
  }
  
  // 前缀比较：用于 Bloom Filter 等优化
  bool EqualPrefix(std::string_view a, std::string_view b, size_t prefix_len = 24) const {
    size_t len = std::min({a.size(), b.size(), prefix_len});
    return std::memcmp(a.data(), b.data(), len) == 0;
  }
  
  // 获取 Key 的 UserKey 部分（不含 sequence/timestamp，用于 MVCC）
  std::string_view ExtractUserKey(std::string_view key) const {
    if (key.size() < CedarKey::kUserKeySize) return key;
    return key.substr(0, CedarKey::kUserKeySize);
  }
  
  // 比较 UserKey 部分
  int CompareUserKey(std::string_view a, std::string_view b) const {
    if (a.size() < CedarKey::kKeySize || b.size() < CedarKey::kKeySize) {
      return a.size() < b.size() ? -1 : (a.size() > b.size() ? 1 : 0);
    }
    CedarKey ka = CedarKey::Decode(a.data());
    CedarKey kb = CedarKey::Decode(b.data());
    return ka.CompareUserKey(kb);
  }
  
  // 名称（用于 LSM-Tree 配置）
  static const char* Name() { return "cedar.CedarKeyComparator"; }
};

// =============================================================================
// 范围扫描辅助
// =============================================================================

class CedarKeyRange {
 public:
  // 查询某实体的所有版本（任意时间）
  static std::pair<CedarKey, CedarKey> AllVersions(
      uint64_t entity_id, EntityType type, uint16_t column_id);
  
  // 查询某实体的时间范围
  static std::pair<CedarKey, CedarKey> TimeRange(
      uint64_t entity_id, EntityType type, uint16_t column_id,
      Timestamp start, Timestamp end);
};

// =============================================================================
// InternalKey - MemTable 内部使用的键（不含时间戳）
// =============================================================================

struct InternalKey {
  uint64_t entity_id;
  EntityType entity_type;
  uint16_t column_id;
  uint64_t target_id;  // 边的 dst_id 或点的扩展数据
  
  InternalKey() = default;
  
  // 从组件构造
  InternalKey(uint64_t eid, EntityType type, uint16_t col, uint64_t target = 0)
      : entity_id(eid), entity_type(type), column_id(col), target_id(target) {}
  
  // 从 CedarKey 构造（提取非时间戳部分）
  explicit InternalKey(const CedarKey& key)
      : entity_id(key.entity_id()),
        entity_type(key.entity_type()),
        column_id(key.column_id()),
        target_id(key.target_id()) {}
  
  bool operator==(const InternalKey& other) const {
    return entity_id == other.entity_id &&
           entity_type == other.entity_type &&
           column_id == other.column_id &&
           target_id == other.target_id;
  }
  
  bool operator<(const InternalKey& other) const {
    if (entity_id != other.entity_id) return entity_id < other.entity_id;
    if (entity_type != other.entity_type)
      return static_cast<uint8_t>(entity_type) < static_cast<uint8_t>(other.entity_type);
    if (column_id != other.column_id) return column_id < other.column_id;
    return target_id < other.target_id;
  }
};

// InternalKey 的哈希函数
struct InternalKeyHash {
  size_t operator()(const InternalKey& k) const noexcept {
    return std::hash<uint64_t>()(k.entity_id) ^
           (std::hash<uint8_t>()(static_cast<uint8_t>(k.entity_type)) << 1) ^
           (std::hash<uint16_t>()(k.column_id) << 2) ^
           (std::hash<uint64_t>()(k.target_id) << 3);
  }
};

}  // namespace cedar

// std::hash 特化 for InternalKey
namespace std {
template <>
struct hash<cedar::InternalKey> {
  size_t operator()(const cedar::InternalKey& k) const noexcept {
    return cedar::InternalKeyHash{}(k);
  }
};
}  // namespace std

#endif  // FERN_FERN_KEY_H_
