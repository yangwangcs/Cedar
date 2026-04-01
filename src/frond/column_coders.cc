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

#include "cedar/frond/column_coders.h"

#include "cedar/types/descriptor.h"
#include "cedar/frond/compression.h"

namespace cedar {

// =============================================================================
// Varint 编码/解码
// =============================================================================

void EncodeVarUint64(uint64_t value, std::string* out) {
  while (value >= 0x80) {
    out->push_back(static_cast<char>(0x80 | (value & 0x7F)));
    value >>= 7;
  }
  out->push_back(static_cast<char>(value));
}

std::optional<uint64_t> DecodeVarUint64(const char** p, size_t* remaining) {
  if (*remaining == 0) return std::nullopt;

  uint64_t result = 0;
  uint32_t shift = 0;

  while (*remaining > 0) {
    unsigned char byte = static_cast<unsigned char>(**p);
    (*p)++;
    (*remaining)--;

    result |= static_cast<uint64_t>(byte & 0x7F) << shift;

    if ((byte & 0x80) == 0) {
      return result;
    }

    shift += 7;
    if (shift >= 64) {
      return std::nullopt;
    }
  }

  return std::nullopt;
}

void EncodeVarUint16(uint16_t value, std::string* out) {
  EncodeVarUint64(value, out);
}

std::optional<uint16_t> DecodeVarUint16(const char** p, size_t* remaining) {
  auto result = DecodeVarUint64(p, remaining);
  if (!result.has_value() || result.value() > 0xFFFF) {
    return std::nullopt;
  }
  return static_cast<uint16_t>(result.value());
}

// =============================================================================
// EntityIdColumn (Delta + RLE + Varint)
// =============================================================================

void EntityIdColumn::Add(uint64_t id) {
  if (first_) {
    base_id_ = id;
    last_id_ = id;
    first_ = false;
    count_++;
    return;
  }

  if (id == last_id_) {
    // RLE: 连续相同的 ID
    run_length_++;
  } else {
    FlushRun();
    // Delta 编码
    int64_t delta = static_cast<int64_t>(id) - static_cast<int64_t>(last_id_);
    EncodeVarUint64(ZigZagEncode(delta), &encoded_);
    last_id_ = id;
  }
  count_++;
}

void EntityIdColumn::FlushRun() {
  if (run_length_ > 0) {
    // RLE 标记: 存储 0 表示重复，后面跟重复次数
    encoded_.push_back(0);  // delta = 0 表示 RLE
    EncodeVarUint64(run_length_, &encoded_);
    run_length_ = 0;
  }
}

std::string EntityIdColumn::Finish() {
  FlushRun();
  
  // 格式: [base_id:varint] [count:varint] [encoded_deltas...]
  std::string result;
  EncodeVarUint64(base_id_, &result);
  EncodeVarUint64(count_, &result);
  result += encoded_;
  
  return result;
}

void EntityIdColumn::Reset() {
  base_id_ = 0;
  last_id_ = 0;
  run_length_ = 0;
  count_ = 0;
  encoded_.clear();
  first_ = true;
}

// =============================================================================
// EntityIdColumnDecoder
// =============================================================================

bool EntityIdColumnDecoder::Init(const char* data, size_t size) {
  data_ = data;
  size_ = size;
  
  if (size < 2) return false;
  
  const char* p = data;
  size_t remaining = size;
  
  auto base_opt = DecodeVarUint64(&p, &remaining);
  if (!base_opt.has_value()) return false;
  base_id_ = base_opt.value();
  
  auto count_opt = DecodeVarUint64(&p, &remaining);
  if (!count_opt.has_value()) return false;
  count_ = count_opt.value();
  
  return true;
}

uint64_t EntityIdColumnDecoder::Get(size_t idx) const {
  if (idx >= count_) return 0;
  if (idx == 0) return base_id_;
  
  // 顺序解压到目标位置（需要优化为 Restart Points）
  // 简化实现：线性扫描
  const char* p = data_;
  size_t remaining = size_;
  
  // 跳过 header
  DecodeVarUint64(&p, &remaining);  // base_id
  DecodeVarUint64(&p, &remaining);  // count
  
  uint64_t current = base_id_;
  size_t current_idx = 1;
  
  while (remaining > 0 && current_idx <= idx) {
    auto delta_opt = DecodeVarUint64(&p, &remaining);
    if (!delta_opt.has_value()) break;
    
    if (delta_opt.value() == 0) {
      // RLE 标记
      auto run_len_opt = DecodeVarUint64(&p, &remaining);
      if (!run_len_opt.has_value()) break;
      uint64_t run_len = run_len_opt.value();
      
      // 跳过重复值
      if (current_idx + run_len > idx) {
        return current;
      }
      current_idx += run_len;
    } else {
      int64_t delta = ZigZagDecode(delta_opt.value());
      current = static_cast<uint64_t>(static_cast<int64_t>(current) + delta);
      if (current_idx == idx) {
        return current;
      }
      current_idx++;
    }
  }
  
  return current;
}

// =============================================================================
// TimestampColumn (Delta-of-Delta)
// =============================================================================

void TimestampColumn::Add(uint64_t ts) {
  if (!first_ts_.has_value()) {
    first_ts_ = ts;
    prev_ts_ = ts;
    count_++;
    return;
  }

  if (!prev_delta_.has_value()) {
    int64_t delta = static_cast<int64_t>(ts) - static_cast<int64_t>(*prev_ts_);
    prev_delta_ = delta;
    EncodeVarUint64(ZigZagEncode(delta), &encoded_);
    prev_ts_ = ts;
    count_++;
    return;
  }

  int64_t delta = static_cast<int64_t>(ts) - static_cast<int64_t>(*prev_ts_);
  int64_t dod = delta - *prev_delta_;  // Delta of Delta
  EncodeVarUint64(ZigZagEncode(dod), &encoded_);
  
  prev_delta_ = delta;
  prev_ts_ = ts;
  count_++;
}

std::string TimestampColumn::Finish() {
  // 格式: [first_ts:8B] [first_delta:varint] [dods:varint[]]
  std::string result;
  
  uint64_t first_ts = first_ts_.value_or(0);
  result.append(reinterpret_cast<const char*>(&first_ts), sizeof(first_ts));
  result += encoded_;
  
  return result;
}

void TimestampColumn::Reset() {
  first_ts_.reset();
  prev_ts_.reset();
  prev_delta_.reset();
  encoded_.clear();
  count_ = 0;
}

// =============================================================================
// TimestampColumnDecoder
// =============================================================================

bool TimestampColumnDecoder::Init(const char* data, size_t size) {
  data_ = data;
  size_ = size;
  
  if (size < 8) return false;
  
  memcpy(&first_ts_, data, sizeof(first_ts_));
  
  // 计算 count（需要解析所有数据）
  count_ = 1;  // 至少有一个 first_ts
  const char* p = data + 8;
  size_t remaining = size - 8;
  
  // 简单计算：每遇到一个 varint 就增加 count
  while (remaining > 0) {
    auto opt = DecodeVarUint64(&p, &remaining);
    if (!opt.has_value()) break;
    count_++;
  }
  
  return true;
}

uint64_t TimestampColumnDecoder::Get(size_t idx) const {
  if (idx >= count_) return 0;
  if (idx == 0) return first_ts_;
  
  const char* p = data_ + 8;  // 跳过 first_ts
  size_t remaining = size_ - 8;
  
  uint64_t current = first_ts_;
  int64_t prev_delta = 0;
  size_t current_idx = 1;
  
  while (remaining > 0 && current_idx <= idx) {
    auto opt = DecodeVarUint64(&p, &remaining);
    if (!opt.has_value()) break;
    
    if (current_idx == 1) {
      // 第一个 delta
      prev_delta = ZigZagDecode(opt.value());
      current = static_cast<uint64_t>(static_cast<int64_t>(current) + prev_delta);
    } else {
      // Delta of Delta
      int64_t dod = ZigZagDecode(opt.value());
      prev_delta += dod;
      current = static_cast<uint64_t>(static_cast<int64_t>(current) + prev_delta);
    }
    
    if (current_idx == idx) {
      return current;
    }
    current_idx++;
  }
  
  return current;
}

// =============================================================================
// TargetIdColumn (Delta)
// =============================================================================

void TargetIdColumn::Add(uint64_t id) {
  if (first_) {
    first_id_ = id;
    last_id_ = id;
    first_ = false;
    count_++;
    return;
  }
  
  int64_t delta = static_cast<int64_t>(id) - static_cast<int64_t>(last_id_);
  EncodeVarUint64(ZigZagEncode(delta), &encoded_);
  last_id_ = id;
  count_++;
}

std::string TargetIdColumn::Finish() {
  // 格式: [first_id:8B] [deltas:varint[]]
  std::string result;
  result.append(reinterpret_cast<const char*>(&first_id_), sizeof(first_id_));
  result += encoded_;
  return result;
}

void TargetIdColumn::Reset() {
  first_id_ = 0;
  last_id_ = 0;
  count_ = 0;
  encoded_.clear();
  first_ = true;
}

// =============================================================================
// TargetIdColumnDecoder
// =============================================================================

bool TargetIdColumnDecoder::Init(const char* data, size_t size) {
  data_ = data;
  size_ = size;
  
  if (size < 8) return false;
  
  // 计算 count
  count_ = 1;
  const char* p = data + 8;
  size_t remaining = size - 8;
  
  while (remaining > 0) {
    auto opt = DecodeVarUint64(&p, &remaining);
    if (!opt.has_value()) break;
    count_++;
  }
  
  return true;
}

uint64_t TargetIdColumnDecoder::Get(size_t idx) const {
  if (idx >= count_) return 0;
  
  uint64_t first_id;
  memcpy(&first_id, data_, sizeof(first_id));
  if (idx == 0) return first_id;
  
  const char* p = data_ + 8;
  size_t remaining = size_ - 8;
  
  uint64_t current = first_id;
  size_t current_idx = 1;
  
  while (remaining > 0 && current_idx <= idx) {
    auto opt = DecodeVarUint64(&p, &remaining);
    if (!opt.has_value()) break;
    
    int64_t delta = ZigZagDecode(opt.value());
    current = static_cast<uint64_t>(static_cast<int64_t>(current) + delta);
    
    if (current_idx == idx) {
      return current;
    }
    current_idx++;
  }
  
  return current;
}

// =============================================================================
// SequenceColumn (RLE + Varint)
// =============================================================================

void SequenceColumn::Add(uint16_t seq) {
  if (seq != 0) {
    all_zero_ = false;
  }
  
  if (first_) {
    last_seq_ = seq;
    first_ = false;
    count_++;
    return;
  }
  
  if (seq == 0) {
    zero_count_++;
  } else {
    FlushZeros();
    EncodeVarUint16(seq, &encoded_);
  }
  last_seq_ = seq;
  count_++;
}

void SequenceColumn::FlushZeros() {
  if (zero_count_ > 0) {
    // 标记 0xFFFF 表示后面跟着 N 个零
    encoded_.push_back(0xFF);
    encoded_.push_back(0xFF);
    EncodeVarUint64(zero_count_, &encoded_);
    zero_count_ = 0;
  }
}

std::string SequenceColumn::Finish() {
  FlushZeros();
  
  if (all_zero_) {
    return "";  // 空字符串表示全零
  }
  
  return encoded_;
}

void SequenceColumn::Reset() {
  last_seq_ = 0;
  zero_count_ = 0;
  count_ = 0;
  encoded_.clear();
  all_zero_ = true;
  first_ = true;
}

// =============================================================================
// SequenceColumnDecoder
// =============================================================================

bool SequenceColumnDecoder::Init(const char* data, size_t size, bool all_zero) {
  data_ = data;
  size_ = size;
  all_zero_ = all_zero;
  
  if (all_zero_) {
    // 需要从其他地方获取 count
    return true;
  }
  
  // 计算 count
  count_ = 0;
  const char* p = data;
  size_t remaining = size;
  
  while (remaining > 0) {
    if (remaining >= 2 && 
        static_cast<unsigned char>(p[0]) == 0xFF && 
        static_cast<unsigned char>(p[1]) == 0xFF) {
      // RLE 零标记
      p += 2;
      remaining -= 2;
      auto opt = DecodeVarUint64(&p, &remaining);
      if (!opt.has_value()) break;
      count_ += opt.value();
    } else {
      auto opt = DecodeVarUint16(&p, &remaining);
      if (!opt.has_value()) break;
      count_++;
    }
  }
  
  return true;
}

uint16_t SequenceColumnDecoder::Get(size_t idx) const {
  if (all_zero_) return 0;
  if (idx >= count_) return 0;
  
  const char* p = data_;
  size_t remaining = size_;
  size_t current_idx = 0;
  
  while (remaining > 0) {
    if (remaining >= 2 && 
        static_cast<unsigned char>(p[0]) == 0xFF && 
        static_cast<unsigned char>(p[1]) == 0xFF) {
      // RLE 零标记
      p += 2;
      remaining -= 2;
      auto opt = DecodeVarUint64(&p, &remaining);
      if (!opt.has_value()) break;
      uint64_t run_len = opt.value();
      
      if (current_idx + run_len > idx) {
        return 0;
      }
      current_idx += run_len;
    } else {
      auto opt = DecodeVarUint16(&p, &remaining);
      if (!opt.has_value()) break;
      
      if (current_idx == idx) {
        return opt.value();
      }
      current_idx++;
    }
  }
  
  return 0;
}

// =============================================================================
// FlagsColumn (Bitmap)
// =============================================================================

void FlagsColumn::Add(uint8_t flags) {
  size_t byte_idx = count_ / 8;
  size_t bit_idx = count_ % 8;
  
  if (byte_idx >= bitmap_.size()) {
    bitmap_.push_back(0);
  }
  
  if (flags) {
    bitmap_[byte_idx] |= (1 << bit_idx);
  }
  
  last_flags_ = flags;
  count_++;
}

std::string FlagsColumn::Finish() {
  return bitmap_;
}

void FlagsColumn::Reset() {
  bitmap_.clear();
  last_flags_ = 0;
  count_ = 0;
}

// =============================================================================
// FlagsColumnDecoder
// =============================================================================

bool FlagsColumnDecoder::Init(const char* data, size_t size) {
  data_ = data;
  size_ = size;
  count_ = size * 8;
  return true;
}

uint8_t FlagsColumnDecoder::Get(size_t idx) const {
  if (idx >= count_) return 0;
  
  size_t byte_idx = idx / 8;
  size_t bit_idx = idx % 8;
  
  if (byte_idx >= size_) return 0;
  
  return (data_[byte_idx] >> bit_idx) & 1;
}

// =============================================================================
// DescriptorColumn
// =============================================================================

void DescriptorColumn::Add(const Descriptor& desc) {
  std::string encoded = desc.Encode();
  data_.append(encoded);
  count_++;
}

std::string DescriptorColumn::Finish(CedarCompressionType compression,
                                       CedarCompressionType* actual_type) {
  if (compression == CedarCompressionType::LZ4 || 
      compression == CedarCompressionType::Zstd) {
    std::string compressed;
    CedarCompressionType used_type;
    Compression::Compress(compression, Slice(data_), &compressed, &used_type);
    if (actual_type) *actual_type = used_type;
    return compressed;
  }
  if (actual_type) *actual_type = CedarCompressionType::None;
  return data_;
}

void DescriptorColumn::Reset() {
  data_.clear();
  count_ = 0;
}

// =============================================================================
// DescriptorColumnDecoder
// =============================================================================

bool DescriptorColumnDecoder::Init(const char* data, size_t size, 
                                    CedarCompressionType compression) {
  if (compression == CedarCompressionType::LZ4 || 
      compression == CedarCompressionType::Zstd) {
    size_t uncompressed_size = count_ * 8;  // 预估解压后大小
    if (Compression::Decompress(compression, Slice(data, size), &uncompressed_, 
                                uncompressed_size).ok()) {
      // 成功
    } else {
      return false;
    }
  } else {
    uncompressed_.assign(data, size);
  }
  
  count_ = uncompressed_.size() / 8;  // 每个 Descriptor 8B
  return true;
}

Descriptor DescriptorColumnDecoder::Get(size_t idx) const {
  if (idx >= count_) return Descriptor();
  
  size_t offset = idx * 8;
  if (offset + 8 > uncompressed_.size()) return Descriptor();
  
  Slice slice(uncompressed_.data() + offset, 8);
  auto opt = Descriptor::Decode(slice);
  if (opt.has_value()) {
    return opt.value();
  }
  return Descriptor();
}

}  // namespace cedar
