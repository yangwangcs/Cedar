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
// Bloom Filter - 从 frond_format.h 提取
// =============================================================================

#ifndef FERN_BLOOM_FILTER_H_
#define FERN_BLOOM_FILTER_H_

#include <cstdint>
#include <vector>

#include "cedar/core/slice.h"

namespace cedar {

// 简单的 Bloom Filter 实现
class BloomFilter {
 public:
  BloomFilter() = default;
  explicit BloomFilter(size_t bits_per_key, size_t num_keys = 0);
  
  // 从已构建的数据初始化（用于读取）
  void Init(const char* data, size_t size);
  
  // 添加 key（字符串版本）
  void Add(const char* key, size_t len);
  void Add(uint64_t key);  // 整数版本
  
  // 检查 key 可能存在（有一定误判率）或肯定不存在
  bool MayContain(const char* key, size_t len) const;
  bool MayContain(uint64_t key) const;  // 整数版本
  
  // 完成构建，返回数据
  std::vector<char> Finish();
  
  // 编码到 Slice（用于写入）
  void EncodeTo(std::string* buf) const;
  
  // 从 Slice 解码（用于读取）
  bool DecodeFrom(const char* data, size_t size, size_t num_keys_hint = 0);
  bool DecodeFrom(const Slice* slice, size_t num_keys_hint = 0);
  
  // 获取原始数据（读取模式）
  const char* data() const { return data_.data(); }
  size_t size() const { return data_.size(); }
  
  // 获取预估的 key 数量
  size_t NumKeys() const { return num_keys_; }
  
  // 清空
  void Clear();
  
  // 是否为空
  bool empty() const { return data_.empty() && hash_funcs_ == 0; }

 private:
  static uint32_t Hash(const char* data, size_t len, uint32_t seed);
  
  std::vector<char> data_;
  size_t bits_per_key_ = 10;
  size_t hash_funcs_ = 0;
  size_t num_keys_ = 0;
  bool initialized_ = false;
};

}  // namespace cedar

#endif  // FERN_BLOOM_FILTER_H_
