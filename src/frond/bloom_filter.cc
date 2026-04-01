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

#include "cedar/frond/bloom_filter.h"

#include <cstring>

namespace cedar {

BloomFilter::BloomFilter(size_t bits_per_key, size_t num_keys)
    : bits_per_key_(bits_per_key), num_keys_(num_keys) {
  if (num_keys > 0) {
    size_t bits = num_keys * bits_per_key;
    size_t bytes = (bits + 7) / 8;
    // 限制最小大小，避免过小
    if (bytes < 64) bytes = 64;
    data_.resize(bytes, 0);
    // 计算 hash 函数数量
    hash_funcs_ = static_cast<size_t>(bits_per_key * 0.69);  // ln(2)
    if (hash_funcs_ < 1) hash_funcs_ = 1;
    if (hash_funcs_ > 30) hash_funcs_ = 30;
  }
}

void BloomFilter::Init(const char* data, size_t size) {
  data_.assign(data, data + size);
  initialized_ = true;
  // 对于已构建的 filter，无法知道原始参数
  hash_funcs_ = 6;  // 默认值
}

void BloomFilter::Add(const char* key, size_t len) {
  if (data_.empty()) return;
  
  uint32_t h = Hash(key, len, 0xbc9f1d34);
  const uint32_t delta = (h >> 17) | (h << 15);  // 旋转
  
  for (size_t i = 0; i < hash_funcs_; ++i) {
    const uint32_t bitpos = h % (data_.size() * 8);
    data_[bitpos / 8] |= (1 << (bitpos % 8));
    h += delta;
  }
  num_keys_++;
}

void BloomFilter::Add(uint64_t key) {
  Add(reinterpret_cast<const char*>(&key), sizeof(key));
}

bool BloomFilter::MayContain(const char* key, size_t len) const {
  if (data_.empty()) return true;  // 无 filter，假设存在
  
  uint32_t h = Hash(key, len, 0xbc9f1d34);
  const uint32_t delta = (h >> 17) | (h << 15);
  
  for (size_t i = 0; i < hash_funcs_; ++i) {
    const uint32_t bitpos = h % (data_.size() * 8);
    if ((data_[bitpos / 8] & (1 << (bitpos % 8))) == 0) {
      return false;  // 肯定不存在
    }
    h += delta;
  }
  return true;  // 可能存在
}

bool BloomFilter::MayContain(uint64_t key) const {
  return MayContain(reinterpret_cast<const char*>(&key), sizeof(key));
}

std::vector<char> BloomFilter::Finish() {
  std::vector<char> result = std::move(data_);
  Clear();
  return result;
}

void BloomFilter::EncodeTo(std::string* buf) const {
  if (buf && !data_.empty()) {
    buf->append(data_.data(), data_.size());
  }
}

bool BloomFilter::DecodeFrom(const char* data, size_t size, size_t num_keys_hint) {
  if (!data || size == 0) {
    return false;
  }
  data_.assign(data, data + size);
  initialized_ = true;
  num_keys_ = num_keys_hint;
  // 估算 hash 函数数量
  hash_funcs_ = 6;  // 默认值，通常足够
  return true;
}

bool BloomFilter::DecodeFrom(const Slice* slice, size_t num_keys_hint) {
  if (!slice || slice->size() == 0) {
    return false;
  }
  return DecodeFrom(slice->data(), slice->size(), num_keys_hint);
}

void BloomFilter::Clear() {
  data_.clear();
  hash_funcs_ = 0;
  num_keys_ = 0;
  initialized_ = false;
}

uint32_t BloomFilter::Hash(const char* data, size_t len, uint32_t seed) {
  // MurmurHash2 变种
  const uint32_t m = 0x5bd1e995;
  const int r = 24;
  
  uint32_t h = seed ^ static_cast<uint32_t>(len);
  
  // 按 4 字节处理
  while (len >= 4) {
    uint32_t k = *reinterpret_cast<const uint32_t*>(data);
    k *= m;
    k ^= k >> r;
    k *= m;
    
    h *= m;
    h ^= k;
    
    data += 4;
    len -= 4;
  }
  
  // 处理剩余字节
  switch (len) {
    case 3: h ^= static_cast<uint8_t>(data[2]) << 16;
    case 2: h ^= static_cast<uint8_t>(data[1]) << 8;
    case 1: h ^= static_cast<uint8_t>(data[0]);
            h *= m;
  }
  
  h ^= h >> 13;
  h *= m;
  h ^= h >> 15;
  
  return h;
}

}  // namespace cedar
