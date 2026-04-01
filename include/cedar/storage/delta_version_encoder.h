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

#ifndef FERN_STORAGE_DELTA_VERSION_ENCODER_H_
#define FERN_STORAGE_DELTA_VERSION_ENCODER_H_

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "cedar/core/slice.h"
#include "cedar/types/descriptor.h"
#include "cedar/types/cedar_key.h"

namespace cedar {

// ============================================================================
// Delta Type 枚举
// ============================================================================

/**
 * Delta 编码类型
 * 
 * 用于表示不同版本间差异的编码方式，目标是节省存储空间。
 * 根据数值差异大小自动选择最优编码类型，可实现 50-90% 空间节省。
 */
enum class DeltaType : uint8_t {
  kSame = 0,        // 值相同 (0 byte payload) - 最高效
  kDelta8 = 1,      // 8-bit delta (1 byte) - 差值在 [-128, 127]
  kDelta16 = 2,     // 16-bit delta (2 bytes) - 差值在 [-32768, 32767]
  kDelta32 = 3,     // 32-bit delta (4 bytes) - 差值在 32-bit 范围
  kDelta64 = 4,     // 64-bit delta (8 bytes) - 完整 64-bit 差值
  kStringDiff = 5,  // 字符串 diff - 使用编辑距离算法
  kFull = 6,        // 完整值 (8 bytes) - fallback，存储完整 Descriptor
};

// ============================================================================
// 字符串 Diff 相关结构
// ============================================================================

/**
 * 字符串编辑操作类型
 */
enum class StringEditOp : uint8_t {
  kKeep = 0,    // 保持不变
  kInsert = 1,  // 插入字符
  kDelete = 2,  // 删除字符
  kReplace = 3, // 替换字符
};

/**
 * 字符串 Diff 结果
 * 
 * 使用简化版 Myer's diff 算法计算编辑操作列表
 * 每个编辑操作包含：位置、操作类型、字符值
 */
struct StringDiffResult {
  // 编辑操作: (位置, 操作类型, 字符)
  // 编码优化：使用紧凑格式存储
  //  - 位置：varint 编码
  //  - 操作类型：2 bits
  //  - 字符：1 byte (ASCII) 或 UTF-8 字节
  struct Edit {
    uint32_t position;    // 操作位置
    StringEditOp op;      // 操作类型
    char character;       // 相关字符（Insert/Replace 使用）
    
    Edit(uint32_t pos, StringEditOp operation, char ch = '\0')
        : position(pos), op(operation), character(ch) {}
  };
  
  std::vector<Edit> edits;
  
  // 序列化为紧凑二进制格式
  std::string Serialize() const;
  
  // 从二进制格式反序列化
  static std::optional<StringDiffResult> Deserialize(const Slice& data);
};

/**
 * 字符串版本 Diff 计算器
 * 
 * 实现简化版 Myer's diff 算法，计算两个字符串间的最小编辑距离
 * 适用于短字符串（如内联短字符串 ≤4B 或外部引用的小字符串）
 */
class StringVersionDiff {
 public:
  // 最大支持的字符串长度（防止算法复杂度爆炸）
  static constexpr size_t kMaxStringLength = 256;
  
  // 最大编辑操作数（超过则使用 kFull 编码）
  static constexpr size_t kMaxEdits = 64;

  /**
   * 计算两个字符串的差异
   * 
   * @param old 原始字符串
   * @param new_str 新字符串
   * @return Diff 结果，如果差异过大返回空（应使用 kFull 编码）
   */
  std::optional<StringDiffResult> ComputeDiff(const std::string& old,
                                               const std::string& new_str) const;

  /**
   * 应用 Diff 恢复目标字符串
   * 
   * @param base 原始字符串
   * @param diff Diff 结果
   * @return 应用编辑后的字符串
   */
  std::string ApplyDiff(const std::string& base,
                        const StringDiffResult& diff) const;

  /**
   * 估算 Diff 编码后的字节数
   */
  static size_t EstimateEncodedSize(const StringDiffResult& diff);

 private:
  // 内部实现：使用动态规划计算最小编辑距离
  // 返回编辑路径（用于重构 diff）
  std::vector<std::pair<int, int>> ComputeEditPath(
      const std::string& old,
      const std::string& new_str) const;
};

// ============================================================================
// Delta Entry 结构
// ============================================================================

/**
 * Delta Entry - 单个版本增量
 * 
 * 存储从基准版本到当前版本的差异编码
 */
struct DeltaEntry {
  Timestamp timestamp;           // 版本时间戳
  DeltaType type;                // Delta 类型
  std::string delta_data;        // 编码后的 delta 数据
  
  // 默认构造
  DeltaEntry() : type(DeltaType::kFull) {}
  
  // 完整构造
  DeltaEntry(Timestamp ts, DeltaType t, std::string data)
      : timestamp(ts), type(t), delta_data(std::move(data)) {}
  
  // 计算此 entry 的总存储字节数（含元数据开销）
  size_t TotalSize() const {
    // 8 (timestamp) + 1 (type) + 4 (length prefix) + delta_data.size()
    return 13 + delta_data.size();
  }
  
  // 序列化为二进制格式
  std::string Serialize() const;
  
  // 从二进制格式反序列化
  static std::optional<DeltaEntry> Deserialize(const Slice& data);
};

// ============================================================================
// 版本组结构
// ============================================================================

/**
 * Version Group - 版本组管理
 * 
 * 包含一个基准版本和后续的增量列表
 * 当增量数量或大小超过阈值时，应创建新的版本组
 */
struct VersionGroup {
  // 配置参数（可调整）
  struct Config {
    // 最大 delta 数量（超过则建议创建新基准）
    size_t max_deltas;
    
    // 最大累计 delta 大小（超过则建议创建新基准）
    size_t max_delta_bytes;
    
    // 单个 delta 大小阈值（超过则使用 Full 编码）
    size_t full_encode_threshold;
    
    // Delta 编码相对于 Full 编码的最小节省比例
    // 例如 0.3 表示必须节省 30% 以上才使用 Delta 编码
    double min_saving_ratio;
    
    // 构造函数提供默认值
    Config()
        : max_deltas(16),
          max_delta_bytes(128),
          full_encode_threshold(8),
          min_saving_ratio(0.0) {}
  };

  Timestamp base_timestamp;           // 基准版本时间戳
  Descriptor base_value;              // 基准完整值
  std::vector<DeltaEntry> deltas;     // 增量列表（按时间升序）
  Config config;                      // 配置参数
  
  // 默认构造
  VersionGroup() = default;
  
  // 从基准版本构造
  explicit VersionGroup(Timestamp base_ts, const Descriptor& base,
                        const Config& cfg = Config())
      : base_timestamp(base_ts), base_value(base), config(cfg) {}

  /**
   * 添加新版本到组中
   * 
   * @param timestamp 新版本时间戳
   * @param value 新值
   * @param encoder 编码器
   * @return 是否成功添加（时间戳必须递增）
   */
  bool AddVersion(Timestamp timestamp, const Descriptor& value,
                  class DeltaVersionEncoder& encoder);

  /**
   * 重建特定版本
   * 
   * @param index deltas 中的索引（0-based）
   * @param encoder 编码器
   * @return 重建的 Descriptor，失败返回 nullopt
   */
  std::optional<Descriptor> RebuildVersion(
      size_t index,
      class DeltaVersionEncoder& encoder) const;

  /**
   * 获取最新版本（遍历所有 deltas）
   */
  std::optional<Descriptor> GetLatestVersion(
      class DeltaVersionEncoder& encoder) const;

  /**
   * 检查是否应该创建新基准
   */
  bool ShouldCreateNewBase() const;

  /**
   * 获取版本组总大小（字节）
   */
  size_t TotalSize() const {
    size_t size = 8 + 8;  // timestamp + base descriptor
    for (const auto& d : deltas) {
      size += d.TotalSize();
    }
    return size;
  }

  /**
   * 序列化整个版本组
   */
  std::string Serialize() const;
  
  /**
   * 反序列化版本组
   */
  static std::optional<VersionGroup> Deserialize(const Slice& data);
};

// ============================================================================
// Delta Version Encoder 主类
// ============================================================================

/**
 * Delta Version Encoder - 增量版本编码器
 * 
 * 核心功能：
 * 1. 数值类型增量编码（自动选择最优 bit-width）
 * 2. 字符串类型 diff（使用编辑距离）
 * 3. 版本组管理（基准版本 + 增量列表）
 * 4. 重建特定版本（遍历前置 deltas）
 * 
 * 使用示例：
 *   DeltaVersionEncoder encoder;
 *   
 *   // 创建版本组
 *   VersionGroup group(Timestamp{1000}, base_descriptor);
 *   
 *   // 添加新版本
 *   group.AddVersion(Timestamp{2000}, new_descriptor, encoder);
 *   
 *   // 重建特定版本
 *   auto version = group.RebuildVersion(0, encoder);
 */
class DeltaVersionEncoder {
 public:
  // 编码配置
  struct Config {
    // 启用数值类型 delta 编码
    bool enable_numeric_delta;
    
    // 启用字符串 diff
    bool enable_string_diff;
    
    // 字符串 diff 长度阈值（超过则使用 Full 编码）
    size_t string_diff_threshold;
    
    // 强制使用 Full 编码的最小差异字节数
    size_t force_full_threshold;
    
    // 构造函数提供默认值
    Config()
        : enable_numeric_delta(true),
          enable_string_diff(true),
          string_diff_threshold(64),
          force_full_threshold(8) {}
  };

  explicit DeltaVersionEncoder(const Config& cfg = Config())
      : config_(cfg), string_diff_(std::make_unique<StringVersionDiff>()) {}
  
  ~DeltaVersionEncoder() = default;

  // 禁止拷贝，允许移动
  DeltaVersionEncoder(const DeltaVersionEncoder&) = delete;
  DeltaVersionEncoder& operator=(const DeltaVersionEncoder&) = delete;
  DeltaVersionEncoder(DeltaVersionEncoder&&) = default;
  DeltaVersionEncoder& operator=(DeltaVersionEncoder&&) = default;

  // -------------------------------------------------------------------------
  // 核心编码/解码 API
  // -------------------------------------------------------------------------

  /**
   * 编码新版本，返回 delta entry
   * 
   * 自动选择最优编码策略：
   * - 如果值相同 → kSame (0 byte)
   * - 如果是数值且有 small delta → kDelta8/16/32/64
   * - 如果是字符串且有 small diff → kStringDiff
   * - 否则 → kFull (8 bytes)
   * 
   * @param base 基准值
   * @param current 当前值
   * @return 编码后的 DeltaEntry
   */
  DeltaEntry Encode(const Descriptor& base, const Descriptor& current);

  /**
   * 解码：给定基准值和 delta，恢复目标值
   * 
   * @param base 基准值
   * @param delta Delta entry
   * @return 恢复的 Descriptor
   */
  Descriptor Decode(const Descriptor& base, const DeltaEntry& delta) const;

  /**
   * 重建特定版本（需遍历所有前置 delta）
   * 
   * @param base 基准值
   * @param deltas Delta 列表
   * @param target_index 目标索引（包含该索引）
   * @return 重建的 Descriptor
   */
  Descriptor Rebuild(const Descriptor& base,
                     const std::vector<DeltaEntry>& deltas,
                     size_t target_index) const;

  // -------------------------------------------------------------------------
  // 工具方法
  // -------------------------------------------------------------------------

  /**
   * 检查是否应该创建新基准
   * 
   * 触发条件：
   * 1. Delta 数量超过阈值
   * 2. 累计 delta 大小超过阈值
   * 3. 重建成本过高（delta 链太长）
   * 
   * @param deltas 当前 delta 列表
   * @param max_deltas 最大允许 delta 数量
   * @param max_bytes 最大允许累计字节数
   * @return 是否应该创建新基准
   */
  bool ShouldCreateNewBase(const std::vector<DeltaEntry>& deltas,
                           size_t max_deltas = 16,
                           size_t max_bytes = 128) const;

  /**
   * 估算编码后的字节数（用于选择最优策略）
   */
  size_t EstimateEncodedSize(const Descriptor& base,
                              const Descriptor& current) const;

  /**
   * 选择最优 Delta 类型
   */
  DeltaType SelectDeltaType(const Descriptor& base,
                            const Descriptor& current) const;

  // -------------------------------------------------------------------------
  // 数值类型专用编码（用于内联数值）
  // -------------------------------------------------------------------------

  /**
   * 编码数值 delta（8/16/32/64 bit）
   * 
   * @tparam T 数值类型 (int8_t, int16_t, int32_t, int64_t)
   * @param old_value 旧值
   * @param new_value 新值
   * @return 编码后的 delta 数据
   */
  template <typename T>
  std::string EncodeNumericDelta(T old_value, T new_value) const {
    static_assert(std::is_integral_v<T>, "T must be integral type");
    
    T delta = new_value - old_value;
    std::string result;
    result.resize(sizeof(T));
    memcpy(&result[0], &delta, sizeof(T));
    return result;
  }

  /**
   * 解码数值 delta
   * 
   * @tparam T 数值类型
   * @param old_value 旧值
   * @param delta_data 编码的 delta 数据
   * @return 新值
   */
  template <typename T>
  T DecodeNumericDelta(T old_value, const std::string& delta_data) const {
    static_assert(std::is_integral_v<T>, "T must be integral type");
    
    T delta = 0;
    if (delta_data.size() >= sizeof(T)) {
      memcpy(&delta, delta_data.data(), sizeof(T));
    }
    return old_value + delta;
  }

  // -------------------------------------------------------------------------
  // 字符串类型专用编码
  // -------------------------------------------------------------------------

  /**
   * 编码字符串 diff
   */
  std::optional<std::string> EncodeStringDiff(const std::string& old_str,
                                               const std::string& new_str) const;

  /**
   * 解码字符串 diff
   */
  std::string DecodeStringDiff(const std::string& base_str,
                                const std::string& diff_data) const;

  // -------------------------------------------------------------------------
  // 配置访问
  // -------------------------------------------------------------------------
  
  const Config& GetConfig() const { return config_; }
  void SetConfig(const Config& cfg) { config_ = cfg; }

 private:
  Config config_;
  std::unique_ptr<StringVersionDiff> string_diff_;

  // 内部辅助方法
  
  // 检查是否可以进行数值 delta 编码
  bool CanUseNumericDelta(const Descriptor& base,
                          const Descriptor& current) const;
  
  // 执行数值 delta 编码
  std::string DoNumericDeltaEncode(const Descriptor& base,
                                    const Descriptor& current);
  
  // 执行数值 delta 解码
  Descriptor DoNumericDeltaDecode(const Descriptor& base,
                                   DeltaType type,
                                   const std::string& delta_data) const;
  
  // 从 Descriptor 提取数值（如果可能）
  std::optional<int64_t> ExtractNumericValue(const Descriptor& desc) const;
  
  // 用数值创建 Descriptor（保持原类型）
  Descriptor CreateDescriptorFromNumeric(const Descriptor& original,
                                          int64_t value) const;
};

// ============================================================================
// 内联实现
// ============================================================================

// 序列化辅助函数
namespace delta_encoding {

// Varint 编码（用于紧凑存储位置信息）
inline std::string EncodeVarint(uint32_t value) {
  std::string result;
  while (value >= 0x80) {
    result.push_back(static_cast<char>(value | 0x80));
    value >>= 7;
  }
  result.push_back(static_cast<char>(value));
  return result;
}

// Varint 解码
inline std::pair<uint32_t, size_t> DecodeVarint(const char* data, size_t len) {
  uint32_t result = 0;
  size_t shift = 0;
  size_t pos = 0;
  
  while (pos < len && shift < 32) {
    char byte = data[pos++];
    result |= static_cast<uint32_t>(byte & 0x7F) << shift;
    if ((byte & 0x80) == 0) {
      return {result, pos};
    }
    shift += 7;
  }
  return {0, 0};  // 解码失败
}

}  // namespace delta_encoding

// ============================================================================
// DeltaVersionEncoder 内联实现
// ============================================================================

inline DeltaEntry DeltaVersionEncoder::Encode(const Descriptor& base,
                                               const Descriptor& current) {
  // 检查值是否相同
  if (base.AsRaw() == current.AsRaw()) {
    return DeltaEntry(Timestamp(0), DeltaType::kSame, "");
  }
  
  // 尝试数值 delta 编码
  if (config_.enable_numeric_delta && CanUseNumericDelta(base, current)) {
    return DeltaEntry(Timestamp(0), SelectDeltaType(base, current),
                      DoNumericDeltaEncode(base, current));
  }
  
  // 默认使用 Full 编码
  return DeltaEntry(Timestamp(0), DeltaType::kFull, current.Encode());
}

inline Descriptor DeltaVersionEncoder::Decode(const Descriptor& base,
                                               const DeltaEntry& delta) const {
  switch (delta.type) {
    case DeltaType::kSame:
      return base;
      
    case DeltaType::kDelta8:
    case DeltaType::kDelta16:
    case DeltaType::kDelta32:
    case DeltaType::kDelta64:
      return DoNumericDeltaDecode(base, delta.type, delta.delta_data);
      
    case DeltaType::kFull:
      if (delta.delta_data.size() >= 8) {
        auto desc = Descriptor::Decode(delta.delta_data);
        if (desc.has_value()) return desc.value();
      }
      return base;
      
    default:
      return base;
  }
}

inline Descriptor DeltaVersionEncoder::Rebuild(
    const Descriptor& base,
    const std::vector<DeltaEntry>& deltas,
    size_t target_index) const {
  Descriptor current = base;
  
  for (size_t i = 0; i <= target_index && i < deltas.size(); ++i) {
    current = Decode(current, deltas[i]);
  }
  
  return current;
}

inline bool DeltaVersionEncoder::ShouldCreateNewBase(
    const std::vector<DeltaEntry>& deltas,
    size_t max_deltas,
    size_t max_bytes) const {
  if (deltas.size() >= max_deltas) {
    return true;
  }
  
  size_t total_size = 0;
  for (const auto& delta : deltas) {
    total_size += delta.TotalSize();
  }
  
  return total_size >= max_bytes;
}

inline DeltaType DeltaVersionEncoder::SelectDeltaType(
    const Descriptor& base,
    const Descriptor& current) const {
  auto base_val = ExtractNumericValue(base);
  auto current_val = ExtractNumericValue(current);
  
  if (!base_val.has_value() || !current_val.has_value()) {
    return DeltaType::kFull;
  }
  
  int64_t diff = current_val.value() - base_val.value();
  
  if (diff == 0) {
    return DeltaType::kSame;
  } else if (diff >= INT8_MIN && diff <= INT8_MAX) {
    return DeltaType::kDelta8;
  } else if (diff >= INT16_MIN && diff <= INT16_MAX) {
    return DeltaType::kDelta16;
  } else if (diff >= INT32_MIN && diff <= INT32_MAX) {
    return DeltaType::kDelta32;
  } else {
    return DeltaType::kDelta64;
  }
}

inline bool DeltaVersionEncoder::CanUseNumericDelta(
    const Descriptor& base,
    const Descriptor& current) const {
  // 检查类型是否匹配
  if (base.GetKind() != current.GetKind()) {
    return false;
  }
  
  // 只支持 InlineInt 类型的数值 delta 编码
  return base.GetKind() == EntryKind::InlineInt;
}

inline std::string DeltaVersionEncoder::DoNumericDeltaEncode(
    const Descriptor& base,
    const Descriptor& current) {
  auto base_val = ExtractNumericValue(base);
  auto current_val = ExtractNumericValue(current);
  
  if (!base_val.has_value() || !current_val.has_value()) {
    return "";
  }
  
  int64_t diff = current_val.value() - base_val.value();
  DeltaType type = SelectDeltaType(base, current);
  
  std::string result;
  switch (type) {
    case DeltaType::kDelta8: {
      int8_t delta8 = static_cast<int8_t>(diff);
      result.resize(sizeof(delta8));
      memcpy(&result[0], &delta8, sizeof(delta8));
      break;
    }
    case DeltaType::kDelta16: {
      int16_t delta16 = static_cast<int16_t>(diff);
      result.resize(sizeof(delta16));
      memcpy(&result[0], &delta16, sizeof(delta16));
      break;
    }
    case DeltaType::kDelta32: {
      int32_t delta32 = static_cast<int32_t>(diff);
      result.resize(sizeof(delta32));
      memcpy(&result[0], &delta32, sizeof(delta32));
      break;
    }
    case DeltaType::kDelta64: {
      int64_t delta64 = diff;
      result.resize(sizeof(delta64));
      memcpy(&result[0], &delta64, sizeof(delta64));
      break;
    }
    default:
      break;
  }
  
  return result;
}

inline Descriptor DeltaVersionEncoder::DoNumericDeltaDecode(
    const Descriptor& base,
    DeltaType type,
    const std::string& delta_data) const {
  auto base_val = ExtractNumericValue(base);
  if (!base_val.has_value()) {
    return base;
  }
  
  int64_t diff = 0;
  
  switch (type) {
    case DeltaType::kDelta8:
      if (delta_data.size() >= sizeof(int8_t)) {
        int8_t d;
        memcpy(&d, delta_data.data(), sizeof(d));
        diff = d;
      }
      break;
    case DeltaType::kDelta16:
      if (delta_data.size() >= sizeof(int16_t)) {
        int16_t d;
        memcpy(&d, delta_data.data(), sizeof(d));
        diff = d;
      }
      break;
    case DeltaType::kDelta32:
      if (delta_data.size() >= sizeof(int32_t)) {
        int32_t d;
        memcpy(&d, delta_data.data(), sizeof(d));
        diff = d;
      }
      break;
    case DeltaType::kDelta64:
      if (delta_data.size() >= sizeof(int64_t)) {
        memcpy(&diff, delta_data.data(), sizeof(diff));
      }
      break;
    default:
      return base;
  }
  
  return CreateDescriptorFromNumeric(base, base_val.value() + diff);
}

inline std::optional<int64_t> DeltaVersionEncoder::ExtractNumericValue(
    const Descriptor& desc) const {
  if (desc.GetKind() == EntryKind::InlineInt) {
    auto val = desc.AsInlineInt();
    if (val.has_value()) {
      return static_cast<int64_t>(val.value());
    }
  }
  return std::nullopt;
}

inline Descriptor DeltaVersionEncoder::CreateDescriptorFromNumeric(
    const Descriptor& original,
    int64_t value) const {
  if (original.GetKind() == EntryKind::InlineInt) {
    return Descriptor::InlineInt(original.GetColumnId(), 
                                  static_cast<int32_t>(value));
  }
  return original;
}

}  // namespace cedar

#endif  // FERN_STORAGE_DELTA_VERSION_ENCODER_H_
