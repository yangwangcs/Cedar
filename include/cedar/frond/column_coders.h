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
// Column Coders - 6 独立列的编码/解码实现
// =============================================================================
// 基于 32B CedarKey 的列式存储方案：
// - Column 0: Entity IDs (Delta + RLE + Varint)
// - Column 1: Timestamps (Delta-of-Delta)
// - Column 2: Target IDs (Delta + Prefix)
// - Column 3: Sequences (RLE + Varint)
// - Column 4: Flags (Bitmap)
// - Column 5: Descriptors (固定 8B 或 LZ4)
// =============================================================================

#ifndef FERN_COLUMN_CODERS_H_
#define FERN_COLUMN_CODERS_H_

#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "cedar/frond/compression.h"
#include "cedar/types/cedar_key.h"

namespace cedar {

// =============================================================================
// 编码辅助函数
// =============================================================================

// Varint 编码（无符号 64bit）
void EncodeVarUint64(uint64_t value, std::string* out);
std::optional<uint64_t> DecodeVarUint64(const char** p, size_t* remaining);

// Varint 编码（无符号 16bit）
void EncodeVarUint16(uint16_t value, std::string* out);
std::optional<uint16_t> DecodeVarUint16(const char** p, size_t* remaining);

// ZigZag 编码（有符号转无符号）
inline uint64_t ZigZagEncode(int64_t value) {
  return (static_cast<uint64_t>(value) << 1) ^
         static_cast<uint64_t>(value >> 63);
}

inline int64_t ZigZagDecode(uint64_t value) {
  return static_cast<int64_t>((value >> 1) ^ (-(value & 1)));
}

// =============================================================================
// Column 0: Entity IDs (Delta + RLE + Varint)
// =============================================================================
class EntityIdColumn {
 public:
  EntityIdColumn() = default;

  // 添加一个 entity_id
  void Add(uint64_t id);

  // 完成编码，返回压缩后的数据
  // 格式: [base_id:varint64] [count:varint] [delta|rle_marker:varint]...
  std::string Finish();

  // 当前字节数（估算）
  size_t CurrentOffset() const { return encoded_.size(); }

  // 最后一个值
  uint64_t LastValue() const { return last_id_; }

  // 当前行数
  size_t Count() const { return count_; }

  // 重置
  void Reset();

 private:
  void FlushRun();

  uint64_t base_id_ = 0;
  uint64_t last_id_ = 0;
  uint32_t run_length_ = 0;
  size_t count_ = 0;
  std::string encoded_;
  bool first_ = true;
};

// Entity IDs 解码器
class EntityIdColumnDecoder {
 public:
  // 从压缩数据初始化
  bool Init(const char* data, size_t size);

  // 获取指定索引的值（支持随机访问，通过 Restart Points）
  uint64_t Get(size_t idx) const;

  // 获取总行数
  size_t Count() const { return count_; }

 private:
  const char* data_ = nullptr;
  size_t size_ = 0;
  size_t count_ = 0;
  uint64_t base_id_ = 0;
};

// =============================================================================
// Column 1: Timestamps (Delta-of-Delta)
// =============================================================================
class TimestampColumn {
 public:
  TimestampColumn() = default;

  // 添加一个时间戳（原始微秒值）
  void Add(uint64_t ts);

  // 完成编码
  // 格式: [first_ts:8B] [first_delta:varint] [dods:varint[]]
  std::string Finish();

  // 当前字节数
  size_t CurrentOffset() const { return encoded_.size(); }

  // 最后一个值
  uint64_t LastValue() const { return prev_ts_.value_or(0); }

  // 当前行数
  size_t Count() const { return count_; }

  // 重置
  void Reset();

 private:
  std::optional<uint64_t> first_ts_;
  std::optional<uint64_t> prev_ts_;
  std::optional<int64_t> prev_delta_;
  std::string encoded_;
  size_t count_ = 0;
};

// Timestamp 解码器
class TimestampColumnDecoder {
 public:
  bool Init(const char* data, size_t size);
  uint64_t Get(size_t idx) const;
  size_t Count() const { return count_; }

 private:
  const char* data_ = nullptr;
  size_t size_ = 0;
  size_t count_ = 0;
  uint64_t first_ts_ = 0;
};

// =============================================================================
// Column 2: Target IDs (Delta + Prefix Compression)
// =============================================================================
class TargetIdColumn {
 public:
  TargetIdColumn() = default;

  void Add(uint64_t id);
  std::string Finish();
  size_t CurrentOffset() const { return encoded_.size(); }
  uint64_t LastValue() const { return last_id_; }
  size_t Count() const { return count_; }
  void Reset();

 private:
  uint64_t first_id_ = 0;  // 第一个 ID
  uint64_t last_id_ = 0;   // 最后一个 ID（用于计算 delta）
  size_t count_ = 0;
  std::string encoded_;
  bool first_ = true;
};

class TargetIdColumnDecoder {
 public:
  bool Init(const char* data, size_t size);
  uint64_t Get(size_t idx) const;
  size_t Count() const { return count_; }

 private:
  const char* data_ = nullptr;
  size_t size_ = 0;
  size_t count_ = 0;
};

// =============================================================================
// Column 3: Sequences (RLE + Varint)
// =============================================================================
class SequenceColumn {
 public:
  SequenceColumn() = default;

  void Add(uint16_t seq);
  std::string Finish();
  size_t CurrentOffset() const { return encoded_.size(); }
  uint16_t LastValue() const { return last_seq_; }
  size_t Count() const { return count_; }
  bool AllZero() const { return all_zero_; }
  void Reset();

 private:
  void FlushZeros();

  uint16_t last_seq_ = 0;
  uint32_t zero_count_ = 0;
  size_t count_ = 0;
  std::string encoded_;
  bool all_zero_ = true;
  bool first_ = true;
};

class SequenceColumnDecoder {
 public:
  bool Init(const char* data, size_t size, bool all_zero);
  uint16_t Get(size_t idx) const;
  size_t Count() const { return count_; }

 private:
  const char* data_ = nullptr;
  size_t size_ = 0;
  size_t count_ = 0;
  bool all_zero_ = false;
};

// =============================================================================
// Column 4: Flags Bitmap (1 bit/key)
// =============================================================================
class FlagsColumn {
 public:
  FlagsColumn() = default;

  void Add(uint8_t flags);
  std::string Finish();
  size_t CurrentOffset() const { return bitmap_.size(); }
  uint8_t LastValue() const { return last_flags_; }
  size_t Count() const { return count_; }
  void Reset();

 private:
  std::string bitmap_;
  uint8_t last_flags_ = 0;
  size_t count_ = 0;
};

class FlagsColumnDecoder {
 public:
  bool Init(const char* data, size_t size);
  uint8_t Get(size_t idx) const;
  size_t Count() const { return count_; }

 private:
  const char* data_ = nullptr;
  size_t size_ = 0;
  size_t count_ = 0;
};

// =============================================================================
// Column 5: Descriptors (固定 8B/key，可选 LZ4 整体压缩)
// =============================================================================
class DescriptorColumn {
 public:
  DescriptorColumn() = default;

  void Add(const class Descriptor& desc);
  
  // 完成编码，可选择是否压缩
  // actual_type: 输出参数，返回实际使用的压缩类型
  std::string Finish(CedarCompressionType compression, 
                     CedarCompressionType* actual_type = nullptr);
  
  // 原始大小
  size_t RawSize() const { return data_.size(); }
  
  size_t Count() const { return count_; }
  void Reset();

  // 获取原始数据（用于压缩）
  const std::string& RawData() const { return data_; }

 private:
  std::string data_;  // 原始 8B * count
  size_t count_ = 0;
};

class DescriptorColumnDecoder {
 public:
  bool Init(const char* data, size_t size, CedarCompressionType compression);
  class Descriptor Get(size_t idx) const;
  size_t Count() const { return count_; }

 private:
  std::string uncompressed_;  // 解压后的数据
  size_t count_ = 0;
};

}  // namespace cedar

#endif  // FERN_COLUMN_CODERS_H_
