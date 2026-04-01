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

#ifndef FERN_DESCRIPTOR_H_
#define FERN_DESCRIPTOR_H_

#include <cstdint>
#include <cstring>
#include <optional>

#include "cedar/types/cedar_key.h"  // 包含字节序转换函数
#include "cedar/core/slice.h"

namespace cedar {

// 数据类型
enum class EntryKind : uint8_t {
  InlineInt = 0,      // 内联整数 (4B直接存储)
  InlineFloat = 1,    // 内联浮点数 (4B直接存储)
  InlineShortStr = 2, // 内联短字符串 (≤4B直接存储)
  ExternalRef = 3,    // 外部引用 (>4B数据，存储偏移)
  EdgeRef = 4,        // 边引用 (特殊类型)
  Tombstone = 5,      // 删除标记
  Metadata = 6,       // 元数据
};

// 压缩类型
enum class CompressionType : uint8_t {
  None = 0,
  Lz4 = 1,
  Zstd = 2,
};

// 8B Descriptor，紧凑高效
//
// Bit 分配:
// [0-3]   Kind: 数据类型
// [4-15]  ColumnID: 12bit 冗余（加速过滤）
// [16-47] Payload: 内联数据或外部偏移
// [48-55] Length: 数据长度
// [56-57] Compression: 压缩类型
// [58-63] Reserved: 保留位
class Descriptor {
 public:
  // 默认构造（Tombstone）
  Descriptor() : value_(0) {
    SetKind(EntryKind::Tombstone);
  }

  // 从原始值构造
  explicit Descriptor(uint64_t raw) : value_(raw) {}

  // 完整构造
  Descriptor(EntryKind kind, uint16_t column_id, uint32_t payload,
             uint8_t length, CompressionType compression = CompressionType::None) {
    value_ = 0;
    SetKind(kind);
    SetColumnId(column_id);
    SetPayload(payload);
    SetLength(length);
    SetCompression(compression);
  }

  // 获取原始u64值
  uint64_t AsRaw() const { return value_; }

  // 获取数据类型
  EntryKind GetKind() const {
    switch (value_ & 0x0F) {
      case 0: return EntryKind::InlineInt;
      case 1: return EntryKind::InlineFloat;
      case 2: return EntryKind::InlineShortStr;
      case 3: return EntryKind::ExternalRef;
      case 4: return EntryKind::EdgeRef;
      case 5: return EntryKind::Tombstone;
      case 6: return EntryKind::Metadata;
      default: return EntryKind::Tombstone;
    }
  }

  // 设置数据类型
  void SetKind(EntryKind kind) {
    value_ = (value_ & ~0x0FULL) | (static_cast<uint64_t>(kind) & 0x0FULL);
  }

  // 获取 ColumnID
  uint16_t GetColumnId() const {
    return static_cast<uint16_t>((value_ >> 4) & 0x0FFF);
  }

  // 设置 ColumnID
  void SetColumnId(uint16_t column_id) {
    value_ = (value_ & ~0xFFF0ULL) | ((static_cast<uint64_t>(column_id) & 0x0FFFULL) << 4);
  }

  // 获取 Payload
  uint32_t GetPayload() const {
    return static_cast<uint32_t>((value_ >> 16) & 0xFFFFFFFF);
  }

  // 设置 Payload
  void SetPayload(uint32_t payload) {
    value_ = (value_ & ~0xFFFFFFFF0000ULL) | ((static_cast<uint64_t>(payload) & 0xFFFFFFFFULL) << 16);
  }

  // 获取 Length
  uint8_t GetLength() const {
    return static_cast<uint8_t>((value_ >> 48) & 0xFF);
  }

  // 设置 Length
  void SetLength(uint8_t length) {
    value_ = (value_ & ~0xFF000000000000ULL) | ((static_cast<uint64_t>(length) & 0xFFULL) << 48);
  }

  // 获取压缩类型
  CompressionType GetCompression() const {
    switch ((value_ >> 56) & 0x03) {
      case 0: return CompressionType::None;
      case 1: return CompressionType::Lz4;
      case 2: return CompressionType::Zstd;
      default: return CompressionType::None;
    }
  }

  // 设置压缩类型
  void SetCompression(CompressionType compression) {
    value_ = (value_ & ~0x0300000000000000ULL) | ((static_cast<uint64_t>(compression) & 0x03ULL) << 56);
  }

  // 公共访问方法
  EntryKind kind() const { return GetKind(); }
  uint32_t payload() const { return GetPayload(); }
  
  // 是否是删除标记
  bool IsTombstone() const {
    return GetKind() == EntryKind::Tombstone;
  }

  // 是否是内联数据
  bool IsInline() const {
    EntryKind kind = GetKind();
    return kind == EntryKind::InlineInt ||
           kind == EntryKind::InlineFloat ||
           kind == EntryKind::InlineShortStr;
  }

  // 是否需要外部存储
  bool IsExternal() const {
    return GetKind() == EntryKind::ExternalRef;
  }

  // 静态工厂方法：创建内联整数
  static Descriptor InlineInt(uint16_t column_id, int32_t value) {
    return Descriptor(EntryKind::InlineInt, column_id,
                      static_cast<uint32_t>(value), 4);
  }

  // 获取内联整数值
  std::optional<int32_t> AsInlineInt() const {
    if (GetKind() == EntryKind::InlineInt) {
      return static_cast<int32_t>(GetPayload());
    }
    return std::nullopt;
  }

  // 静态工厂方法：创建内联浮点数
  static Descriptor InlineFloat(uint16_t column_id, float value) {
    union { float f; uint32_t i; } u;
    u.f = value;
    return Descriptor(EntryKind::InlineFloat, column_id, u.i, 4);
  }

  // 获取内联浮点数值
  std::optional<float> AsInlineFloat() const {
    if (GetKind() == EntryKind::InlineFloat) {
      union { float f; uint32_t i; } u;
      u.i = GetPayload();
      return u.f;
    }
    return std::nullopt;
  }

  // 静态工厂方法：创建内联短字符串（最多4字节）
  // 注意：超过4字节的字符串返回 std::nullopt，调用者需要检查
  static std::optional<Descriptor> InlineShortStr(uint16_t column_id, const Slice& value) {
    if (value.size() > 4) {
      // 如果超过4字节，返回空（需要调用者处理，如使用 ExternalRef）
      return std::nullopt;
    }
    uint32_t payload = 0;
    memcpy(&payload, value.data(), value.size());
    return Descriptor(EntryKind::InlineShortStr, column_id,
                      payload, static_cast<uint8_t>(value.size()));
  }

  // 获取内联短字符串
  std::string AsInlineShortStr() const {
    if (GetKind() == EntryKind::InlineShortStr) {
      uint32_t payload = GetPayload();
      uint8_t len = GetLength();
      if (len <= 4) {
        return std::string(reinterpret_cast<const char*>(&payload), len);
      }
    }
    return "";
  }

  // 静态工厂方法：创建外部引用（兼容旧接口）
  static Descriptor ExternalRef(uint16_t column_id, uint32_t offset,
                                uint8_t length,
                                CompressionType compression = CompressionType::None) {
    return Descriptor(EntryKind::ExternalRef, column_id, offset, length, compression);
  }

  // 获取外部引用信息
  struct ExternalInfo {
    uint32_t offset;
    uint8_t length;
    CompressionType compression;
  };
  std::optional<ExternalInfo> AsExternalRef() const {
    if (GetKind() == EntryKind::ExternalRef) {
      return ExternalInfo{GetPayload(), GetLength(), GetCompression()};
    }
    return std::nullopt;
  }

  // ============================================================================
  // MVP Blob 存储扩展（8B Descriptor + 外部 Blob）
  // ============================================================================
  
  // Blob 引用结构（存储在 8B Descriptor 中）
  struct BlobRef {
    uint32_t offset;       // Blob 文件内 4KB 对齐偏移
    uint16_t size_kb;      // 大小（单位 KB，最大 64MB）
    uint8_t  checksum;     // 低 8 位校验
  };
  
  // 创建 Blob 引用（大值外部存储）
  static Descriptor MakeBlobRef(uint32_t offset, uint16_t size_kb, uint8_t checksum = 0) {
    Descriptor d;
    d.SetKind(EntryKind::ExternalRef);
    d.SetBlobOffset(offset);
    d.SetBlobSizeKb(size_kb);
    d.SetBlobChecksum(checksum);
    return d;
  }
  
  // 获取 Blob 引用信息
  struct BlobRef GetBlobRef() const {
    struct BlobRef ref;
    ref.offset = GetBlobOffset();
    ref.size_kb = GetBlobSizeKb();
    ref.checksum = GetBlobChecksum();
    return ref;
  }
  
  // 检查是否是 Blob 引用
  bool IsBlobRef() const {
    return GetKind() == EntryKind::ExternalRef;
  }
  
 private:
  // Blob 引用位操作（复用原有位域）
  // [0-3]   Kind: 数据类型 (ExternalRef = 3)
  // [4-15]  reserved (Blob 不用于 column_id)
  // [16-47] offset: Blob 文件内 4KB 对齐偏移
  // [48-63] size_kb: 大小（单位 KB）
  
 public:
  uint32_t GetBlobOffset() const {
    return static_cast<uint32_t>((value_ >> 16) & 0xFFFFFFFF);
  }
  
  void SetBlobOffset(uint32_t offset) {
    value_ = (value_ & ~0xFFFFFFFF0000ULL) | ((static_cast<uint64_t>(offset) & 0xFFFFFFFFULL) << 16);
  }
  
  uint16_t GetBlobSizeKb() const {
    return static_cast<uint16_t>((value_ >> 48) & 0xFFFF);
  }
  
  void SetBlobSizeKb(uint16_t size_kb) {
    value_ = (value_ & ~0xFFFF000000000000ULL) | ((static_cast<uint64_t>(size_kb) & 0xFFFFULL) << 48);
  }
  
  // checksum 存储在 [4-11] 位（复用 column_id 位置）
  uint8_t GetBlobChecksum() const {
    return static_cast<uint8_t>((value_ >> 4) & 0xFF);
  }
  
  void SetBlobChecksum(uint8_t checksum) {
    value_ = (value_ & ~0xFF0ULL) | ((static_cast<uint64_t>(checksum) & 0xFFULL) << 4);
  }

  // 静态工厂方法：删除标记
  static Descriptor Tombstone(uint16_t column_id) {
    return Descriptor(EntryKind::Tombstone, column_id, 0, 0);
  }

  // 编码为字节
  std::string Encode() const {
    std::string buf;
    buf.resize(8);
    uint64_t be = cedar_htobe64(value_);
    memcpy(&buf[0], &be, 8);
    return buf;
  }

  // 从字节解码
  static std::optional<Descriptor> Decode(const Slice& bytes) {
    if (bytes.size() < 8) {
      return std::nullopt;
    }
    uint64_t be;
    memcpy(&be, bytes.data(), 8);
    return Descriptor(cedar_be64toh(be));
  }

  // 调试字符串
  std::string DebugString() const;

 private:
  uint64_t value_;
};

}  // namespace cedar

#endif  // FERN_DESCRIPTOR_H_
