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

#ifndef FERN_COMPRESSION_H_
#define FERN_COMPRESSION_H_

#include <cstdint>
#include <string>
#include <optional>

#include "cedar/core/status.h"
#include "cedar/core/slice.h"

namespace cedar {

// 压缩类型 (使用 CedarCompressionType 避免命名冲突)
enum class CedarCompressionType : uint8_t {
  None = 0,      // 无压缩
  LZ4 = 1,       // LZ4 - 速度优先
  Zstd = 2,      // Zstd - 压缩率优先 (预留)
};

// 压缩工具类
class Compression {
 public:
  // 压缩数据
  // 如果压缩后大小 >= 原始大小，返回 uncompressed 数据并设置 type 为 None
  static Status Compress(CedarCompressionType type,
                         const Slice& input,
                         std::string* output,
                         CedarCompressionType* actual_type);
  
  // 解压数据
  static Status Decompress(CedarCompressionType type,
                           const Slice& input,
                           std::string* output,
                           size_t uncompressed_size);
  
  // 获取压缩类型名称
  static const char* TypeName(CedarCompressionType type);
  
  // 估计压缩后大小上限
  static size_t MaxCompressedSize(CedarCompressionType type, size_t uncompressed_size);
  
  // 是否支持该压缩类型
  static bool IsSupported(CedarCompressionType type);
  
  // 获取默认压缩类型
  static CedarCompressionType DefaultType() { return CedarCompressionType::LZ4; }
};

// Block 统计信息 (用于快速过滤)
struct BlockStats {
  uint64_t min_entity_id = 0;
  uint64_t max_entity_id = 0;
  uint64_t min_timestamp = 0;
  uint64_t max_timestamp = 0;
  uint32_t null_count = 0;        // NULL 值数量
  uint32_t distinct_count = 0;    // 不同值数量 (预留)
  
  // 编码到字符串
  void EncodeTo(std::string* dst) const;
  
  // 从字符串解码
  Status DecodeFrom(Slice* input);
  
  static constexpr size_t kEncodedSize = 40;  // 8*4 + 4*2 = 40
};

}  // namespace cedar

#endif  // FERN_COMPRESSION_H_
