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

#include "cedar/frond/zone_encoder.h"

#include <algorithm>
#include <cstring>

namespace cedar {

// =============================================================================
// Varint 编码/解码实现
// =============================================================================

size_t VarintCodec::Encode(uint64_t value, char* buf) {
  size_t i = 0;
  while (value >= 0x80) {
    buf[i++] = static_cast<char>(value | 0x80);
    value >>= 7;
  }
  buf[i++] = static_cast<char>(value);
  return i;
}

void VarintCodec::Encode(uint64_t value, std::string& dst) {
  char buf[10];  // Varint 最大 10 字节
  size_t len = Encode(value, buf);
  dst.append(buf, len);
}

size_t VarintCodec::EncodeSigned(int64_t value, char* buf) {
  // ZigZag 编码：将符号位移到最低位
  uint64_t encoded = (value << 1) ^ (value >> 63);
  return Encode(encoded, buf);
}

void VarintCodec::EncodeSigned(int64_t value, std::string& dst) {
  char buf[10];
  size_t len = EncodeSigned(value, buf);
  dst.append(buf, len);
}

std::pair<uint64_t, size_t> VarintCodec::Decode(const char* buf, size_t max_len) {
  uint64_t result = 0;
  size_t shift = 0;
  size_t pos = 0;
  
  while (pos < max_len && pos < 10) {
    uint8_t byte = static_cast<uint8_t>(buf[pos]);
    result |= static_cast<uint64_t>(byte & 0x7F) << shift;
    pos++;
    if ((byte & 0x80) == 0) {
      return {result, pos};
    }
    shift += 7;
    if (shift >= 64) break;
  }
  return {0, 0};  // 解码失败
}

std::pair<int64_t, size_t> VarintCodec::DecodeSigned(const char* buf, size_t max_len) {
  auto [encoded, bytes] = Decode(buf, max_len);
  if (bytes == 0) return {0, 0};
  // ZigZag 解码
  int64_t result = static_cast<int64_t>((encoded >> 1) ^ (-(encoded & 1)));
  return {result, bytes};
}

size_t VarintCodec::Size(uint64_t value) {
  size_t size = 1;
  while (value >= 0x80) {
    size++;
    value >>= 7;
  }
  return size;
}

// =============================================================================
// Zone 0: Entity IDs 编码器 (RLE)
// =============================================================================

std::string EntityIdZoneEncoder::Encode(const std::vector<uint64_t>& ids) {
  ZoneMap dummy;
  return Encode(ids, &dummy);
}

std::string EntityIdZoneEncoder::Encode(const std::vector<uint64_t>& ids, ZoneMap* zone_map) {
  if (ids.empty()) return "";
  
  std::string result;
  result.reserve(ids.size() * 8);  // 预分配
  
  uint64_t prev = ids[0];
  uint32_t run = 1;
  
  zone_map->Update(prev);
  
  for (size_t i = 1; i < ids.size(); ++i) {
    zone_map->Update(ids[i]);
    if (ids[i] == prev) {
      run++;
    } else {
      // 写入 [value:8B][run:varint]
      result.append(reinterpret_cast<char*>(&prev), 8);
      VarintCodec::Encode(run, result);
      prev = ids[i];
      run = 1;
    }
  }
  
  // 处理尾部
  result.append(reinterpret_cast<char*>(&prev), 8);
  VarintCodec::Encode(run, result);
  
  return result;
}

// EntityIdZoneEncoder::Decoder 实现
EntityIdZoneEncoder::Decoder::Decoder(const std::string& data) : data_(data) {
  // 计算总条目数
  size_t pos = 0;
  while (pos < data_.size()) {
    if (pos + 8 > data_.size()) break;
    pos += 8;  // skip value
    auto [run, bytes] = VarintCodec::Decode(data_.data() + pos, data_.size() - pos);
    if (bytes == 0) break;
    total_count_ += run;
    pos += bytes;
  }
}

void EntityIdZoneEncoder::Decoder::BuildRestartPoints() const {
  if (!restart_points_.empty()) return;
  
  size_t pos = 0;
  size_t idx = 0;
  
  while (pos < data_.size()) {
    if (idx % kRestartInterval == 0) {
      restart_points_.push_back({pos, idx});
    }
    
    if (pos + 8 > data_.size()) break;
    uint64_t value;
    memcpy(&value, data_.data() + pos, 8);
    pos += 8;
    
    auto [run, bytes] = VarintCodec::Decode(data_.data() + pos, data_.size() - pos);
    if (bytes == 0) break;
    pos += bytes;
    idx += run;
  }
}

uint64_t EntityIdZoneEncoder::Decoder::Get(size_t idx) const {
  BuildRestartPoints();
  
  // 找到最近的重启点
  size_t restart_idx = idx / kRestartInterval;
  if (restart_idx >= restart_points_.size()) {
    restart_idx = restart_points_.size() - 1;
  }
  
  size_t pos = restart_points_[restart_idx].first;
  size_t current_idx = restart_points_[restart_idx].second;
  
  // 从重启点开始扫描
  while (pos < data_.size()) {
    if (pos + 8 > data_.size()) return 0;
    uint64_t value;
    memcpy(&value, data_.data() + pos, 8);
    pos += 8;
    
    auto [run, bytes] = VarintCodec::Decode(data_.data() + pos, data_.size() - pos);
    if (bytes == 0) return 0;
    pos += bytes;
    
    if (idx < current_idx + run) {
      return value;
    }
    current_idx += run;
  }
  
  return 0;
}

void EntityIdZoneEncoder::Decoder::GetRange(
    size_t start_idx, size_t count, std::vector<uint64_t>* out) const {
  out->reserve(out->size() + count);
  
  for (size_t i = 0; i < count; ++i) {
    out->push_back(Get(start_idx + i));
  }
}

// OPTIMIZATION: 查询结果缓存实现
void EntityIdZoneEncoder::Decoder::AddToCache(
    uint64_t entity_id, const std::vector<size_t>& positions) const {
  if (position_cache_.size() >= kCacheSize) {
    position_cache_.erase(position_cache_.begin());  // LRU: 移除最旧的
  }
  position_cache_.push_back({entity_id, positions});
}

const std::vector<size_t>* EntityIdZoneEncoder::Decoder::GetFromCache(uint64_t entity_id) const {
  for (const auto& [id, positions] : position_cache_) {
    if (id == entity_id) {
      return &positions;
    }
  }
  return nullptr;
}

// OPTIMIZATION: 使用 Bitmap 快速检查 entity 是否存在
bool EntityIdZoneEncoder::Decoder::MayContainEntity(uint64_t entity_id) const {
  // 如果索引已构建，使用索引
  if (index_built_) {
    return entity_index_.find(entity_id) != entity_index_.end();
  }
  
  // 否则线性扫描（只检查是否存在，不记录所有位置）
  size_t pos = 0;
  while (pos < data_.size()) {
    if (pos + 8 > data_.size()) break;
    uint64_t value;
    memcpy(&value, data_.data() + pos, 8);
    pos += 8;
    
    auto [run, bytes] = VarintCodec::Decode(data_.data() + pos, data_.size() - pos);
    if (bytes == 0) break;
    pos += bytes;
    
    if (value == entity_id) {
      return true;
    }
  }
  return false;
}

// OPTIMIZATION: 构建 Entity 倒排索引（延迟构建，第一次查询时触发）
void EntityIdZoneEncoder::Decoder::BuildEntityIndex() const {
  if (index_built_) return;
  
  entity_index_.reserve(total_count_ / 16);  // 估算唯一 entity 数量
  
  size_t pos = 0;
  size_t idx = 0;
  
  while (pos < data_.size()) {
    if (pos + 8 > data_.size()) break;
    uint64_t value;
    memcpy(&value, data_.data() + pos, 8);
    pos += 8;
    
    auto [run, bytes] = VarintCodec::Decode(data_.data() + pos, data_.size() - pos);
    if (bytes == 0) break;
    pos += bytes;
    
    auto& positions = entity_index_[value];
    for (uint32_t i = 0; i < run; ++i) {
      positions.push_back(idx + i);
    }
    idx += run;
  }
  
  index_built_ = true;
}

std::vector<size_t> EntityIdZoneEncoder::Decoder::FindEntityPositions(uint64_t entity_id) const {
  // OPTIMIZATION 1: 检查缓存
  if (auto* cached = GetFromCache(entity_id)) {
    cache_hits_++;
    return *cached;
  }
  cache_misses_++;
  
  // OPTIMIZATION 2: 如果查询次数较多，构建倒排索引
  // 策略：当缓存未命中次数超过阈值且索引未构建时，构建索引
  if (!index_built_ && cache_misses_ > 100) {
    BuildEntityIndex();
  }
  
  // 如果索引已构建，使用索引查询
  if (index_built_) {
    auto it = entity_index_.find(entity_id);
    if (it != entity_index_.end()) {
      AddToCache(entity_id, it->second);
      return it->second;
    }
    return {};
  }
  
  // 回退到线性扫描（原始实现）
  std::vector<size_t> positions;
  positions.reserve(32);  // 预分配合理大小
  
  size_t pos = 0;
  size_t idx = 0;
  
  while (pos < data_.size()) {
    if (pos + 8 > data_.size()) break;
    uint64_t value;
    memcpy(&value, data_.data() + pos, 8);
    pos += 8;
    
    auto [run, bytes] = VarintCodec::Decode(data_.data() + pos, data_.size() - pos);
    if (bytes == 0) break;
    pos += bytes;
    
    if (value == entity_id) {
      for (uint32_t i = 0; i < run; ++i) {
        positions.push_back(idx + i);
      }
    }
    idx += run;
  }
  
  // 添加到缓存
  if (!positions.empty()) {
    AddToCache(entity_id, positions);
  }
  
  return positions;
}

// =============================================================================
// Zone 1: Timestamps 编码器 (Delta-of-Delta)
// =============================================================================

std::string TimestampZoneEncoder::Encode(const std::vector<uint64_t>& timestamps) {
  ZoneMap dummy;
  return Encode(timestamps, &dummy);
}

std::string TimestampZoneEncoder::Encode(const std::vector<uint64_t>& timestamps, 
                                         ZoneMap* zone_map) {
  if (timestamps.empty()) return "";
  
  std::string result;
  result.reserve(timestamps.size() * 4);
  
  // 存储第一个时间戳
  uint64_t first_ts = timestamps[0];
  result.append(reinterpret_cast<char*>(&first_ts), 8);
  
  zone_map->Update(first_ts);
  
  if (timestamps.size() == 1) {
    return result;
  }
  
  // 计算第一个 delta
  int64_t prev_delta = static_cast<int64_t>(timestamps[0]) - static_cast<int64_t>(timestamps[1]);
  VarintCodec::EncodeSigned(prev_delta, result);
  
  // Delta-of-Delta 编码剩余值 (i 从 2 到 timestamps.size()-1，共 timestamps.size()-2 个)
  for (size_t i = 2; i < timestamps.size(); ++i) {
    zone_map->Update(timestamps[i]);
    int64_t delta = static_cast<int64_t>(timestamps[i-1]) - static_cast<int64_t>(timestamps[i]);
    int64_t dod = delta - prev_delta;
    VarintCodec::EncodeSigned(dod, result);
    prev_delta = delta;
  }
  
  // 修复：确保写入正确数量的 DoD 值
  // 对于 n 个时间戳，应该写入：1 个 first_ts + 1 个 delta + (n-2) 个 dod = n 个值
  
  return result;
}

// TimestampZoneEncoder::Decoder 实现
TimestampZoneEncoder::Decoder::Decoder(const std::string& data) : data_(data) {
  if (data_.size() < 8) return;
  
  memcpy(&first_ts_, data_.data(), 8);
  min_ts_ = max_ts_ = first_ts_;
  total_count_ = 1;
  
  if (data_.size() > 8) {
    auto [delta, bytes] = VarintCodec::DecodeSigned(data_.data() + 8, data_.size() - 8);
    if (bytes > 0) {
      first_delta_ = delta;
      
      // 计算总条目数：1 (first) + 1 (second via delta) + DoD count
      total_count_ = 2;  // 第一个和第二个时间戳
      size_t pos = 8 + bytes;
      while (pos < data_.size()) {
        auto [dod, bytes_read] = VarintCodec::DecodeSigned(data_.data() + pos, data_.size() - pos);
        if (bytes_read == 0) break;
        pos += bytes_read;
        total_count_++;  // 剩余的通过 DoD 解码
      }
      
      // 计算 min/max timestamp
      decoded_cache_.reserve(total_count_);
      decoded_cache_.push_back(first_ts_);
      
      if (total_count_ > 1) {
        uint64_t prev_ts = first_ts_ - static_cast<uint64_t>(first_delta_);
        decoded_cache_.push_back(prev_ts);
        min_ts_ = std::min(min_ts_, prev_ts);
        max_ts_ = std::max(max_ts_, prev_ts);
        
        int64_t prev_delta = first_delta_;
        pos = 8 + bytes;
        
        // 修复：处理所有剩余的 DoD 值（total_count_ - 2 个）
        for (size_t i = 2; i < total_count_; ++i) {
          if (pos >= data_.size()) break;
          auto [dod, bytes_read] = VarintCodec::DecodeSigned(data_.data() + pos, data_.size() - pos);
          if (bytes_read == 0) break;
          pos += bytes_read;
          
          int64_t delta = prev_delta + dod;
          uint64_t ts = prev_ts - static_cast<uint64_t>(delta);
          decoded_cache_.push_back(ts);
          min_ts_ = std::min(min_ts_, ts);
          max_ts_ = std::max(max_ts_, ts);
          
          prev_delta = delta;
          prev_ts = ts;
        }
      }
    }
  }
  decoded_up_to_ = total_count_;
  decompressed_ = true;
}

void TimestampZoneEncoder::Decoder::BuildRestartPoints() const {
  // 时间戳解码器预解压所有数据，不需要重启点
}

uint64_t TimestampZoneEncoder::Decoder::Get(size_t idx) const {
  if (idx >= decoded_cache_.size()) return 0;
  return decoded_cache_[idx];
}

void TimestampZoneEncoder::Decoder::GetRange(
    size_t start_idx, size_t count, std::vector<uint64_t>* out) const {
  for (size_t i = 0; i < count && (start_idx + i) < decoded_cache_.size(); ++i) {
    out->push_back(decoded_cache_[start_idx + i]);
  }
}

// OPTIMIZATION: 构建升序索引以支持二分查找
void TimestampZoneEncoder::Decoder::BuildSortedIndex() const {
  if (index_built_) return;
  
  // 创建升序索引（原始数据是降序的）
  sorted_index_.resize(decoded_cache_.size());
  for (size_t i = 0; i < decoded_cache_.size(); ++i) {
    sorted_index_[i] = i;
  }
  
  // 按时间戳升序排序索引
  std::sort(sorted_index_.begin(), sorted_index_.end(),
            [this](size_t a, size_t b) {
              return decoded_cache_[a] < decoded_cache_[b];
            });
  
  index_built_ = true;
}

// OPTIMIZATION: 二分查找 LowerBound（升序排列）
// 返回第一个大于等于 target_ts 的索引
size_t TimestampZoneEncoder::Decoder::LowerBound(uint64_t target_ts) const {
  if (!index_built_) BuildSortedIndex();
  if (sorted_index_.empty()) return 0;
  
  // 在升序索引上进行二分查找
  size_t left = 0;
  size_t right = sorted_index_.size();
  
  while (left < right) {
    size_t mid = left + (right - left) / 2;
    if (decoded_cache_[sorted_index_[mid]] < target_ts) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  
  return left;
}

// OPTIMIZATION: 二分查找 UpperBound（升序排列）
// 返回第一个大于 target_ts 的索引
size_t TimestampZoneEncoder::Decoder::UpperBound(uint64_t target_ts) const {
  if (!index_built_) BuildSortedIndex();
  if (sorted_index_.empty()) return 0;
  
  // 在升序索引上进行二分查找
  size_t left = 0;
  size_t right = sorted_index_.size();
  
  while (left < right) {
    size_t mid = left + (right - left) / 2;
    if (decoded_cache_[sorted_index_[mid]] <= target_ts) {
      left = mid + 1;
    } else {
      right = mid;
    }
  }
  
  return left;
}

// OPTIMIZATION: 使用二分查找优化 FindTimeRange
std::vector<size_t> TimestampZoneEncoder::Decoder::FindTimeRange(
    uint64_t start_ts, uint64_t end_ts) const {
  std::vector<size_t> positions;
  
  if (decoded_cache_.empty()) return positions;
  
  // 使用二分查找快速定位范围
  size_t lb = LowerBound(start_ts);  // 第一个 >= start_ts
  size_t ub = UpperBound(end_ts);     // 第一个 > end_ts
  
  // 收集范围内的索引
  positions.reserve(ub - lb);
  for (size_t i = lb; i < ub && i < sorted_index_.size(); ++i) {
    positions.push_back(sorted_index_[i]);
  }
  
  // 按原始顺序排序（降序）以保持一致性
  std::sort(positions.begin(), positions.end(), std::greater<size_t>());
  
  return positions;
}

// =============================================================================
// Zone 2: Target IDs 编码器 (Delta 或 RLE)
// =============================================================================

std::string TargetIdZoneEncoder::Encode(const std::vector<uint64_t>& target_ids,
                                        const std::vector<uint64_t>& entity_ids) {
  ZoneMap dummy;
  return Encode(target_ids, entity_ids, &dummy);
}

std::string TargetIdZoneEncoder::Encode(const std::vector<uint64_t>& target_ids,
                                        const std::vector<uint64_t>& entity_ids,
                                        ZoneMap* zone_map) {
  if (target_ids.empty()) return "";
  
  // 决定编码策略
  size_t same_as_entity = 0;
  size_t small_delta = 0;
  
  for (size_t i = 0; i < target_ids.size() && i < entity_ids.size(); ++i) {
    if (target_ids[i] == entity_ids[i]) {
      same_as_entity++;
    }
    int64_t delta = static_cast<int64_t>(target_ids[i]) - static_cast<int64_t>(entity_ids[i]);
    if (delta >= -32768 && delta <= 32767) {
      small_delta++;
    }
    zone_map->Update(target_ids[i]);
  }
  
  // 如果大部分与 Entity ID 相同，使用特殊的短编码
  if (same_as_entity > target_ids.size() * 0.8) {
    // 使用位图标记哪些位置不同
    std::string result;
    result.push_back(static_cast<char>(EncodingType::kDelta));
    
    size_t bitmap_size = (target_ids.size() + 7) / 8;
    std::vector<uint8_t> bitmap(bitmap_size, 0);
    std::string deltas;
    
    for (size_t i = 0; i < target_ids.size() && i < entity_ids.size(); ++i) {
      if (target_ids[i] != entity_ids[i]) {
        bitmap[i / 8] |= (1 << (i % 8));
        int64_t delta = static_cast<int64_t>(target_ids[i]) - static_cast<int64_t>(entity_ids[i]);
        VarintCodec::EncodeSigned(delta, deltas);
      }
    }
    
    // 写入位图和 deltas
    result.append(reinterpret_cast<char*>(bitmap.data()), bitmap.size());
    result.append(deltas);
    return result;
  }
  
  // 默认使用原始编码
  std::string result;
  result.push_back(static_cast<char>(EncodingType::kRaw));
  
  // 使用简单 varint 编码
  for (size_t i = 0; i < target_ids.size(); ++i) {
    VarintCodec::Encode(target_ids[i], result);
  }
  
  return result;
}

// TargetIdZoneEncoder::Decoder 实现
TargetIdZoneEncoder::Decoder::Decoder(const std::string& data, EncodingType type) 
    : data_(data), type_(type) {
  if (data_.empty()) return;
  
  // 跳过编码类型字节
  size_t pos = 1;
  
  if (type_ == EncodingType::kRaw) {
    while (pos < data_.size()) {
      auto [value, bytes] = VarintCodec::Decode(data_.data() + pos, data_.size() - pos);
      if (bytes == 0) break;
      pos += bytes;
      total_count_++;
    }
  } else if (type_ == EncodingType::kDelta) {
    // 需要知道总数，这里简化处理
    // 实际应该从外部传入 count
    total_count_ = (data_.size() - 1) * 8;  // 估算
  }
}

uint64_t TargetIdZoneEncoder::Decoder::Get(
    size_t idx, const std::vector<uint64_t>& entity_ids) const {
  if (type_ == EncodingType::kRaw) {
    size_t pos = 1;
    size_t current = 0;
    while (pos < data_.size() && current < idx) {
      auto [value, bytes] = VarintCodec::Decode(data_.data() + pos, data_.size() - pos);
      if (bytes == 0) return 0;
      pos += bytes;
      current++;
    }
    auto [value, bytes] = VarintCodec::Decode(data_.data() + pos, data_.size() - pos);
    return value;
  } else if (type_ == EncodingType::kDelta) {
    // Delta 编码解码
    if (idx < entity_ids.size()) {
      size_t bitmap_size = (entity_ids.size() + 7) / 8;
      if (idx / 8 < bitmap_size) {
        const uint8_t* bitmap = reinterpret_cast<const uint8_t*>(data_.data() + 1);
        if ((bitmap[idx / 8] & (1 << (idx % 8))) == 0) {
          return entity_ids[idx];  // 相同
        }
      }
    }
  }
  return 0;
}

void TargetIdZoneEncoder::Decoder::GetRange(
    size_t start_idx, size_t count,
    const std::vector<uint64_t>& entity_ids,
    std::vector<uint64_t>* out) const {
  for (size_t i = 0; i < count; ++i) {
    out->push_back(Get(start_idx + i, entity_ids));
  }
}

// =============================================================================
// Zone 3: Key Metadata 编码器 (Sequence + Flags)
// =============================================================================

static void EncodeFixed32Str(std::string* dst, uint32_t value) {
  char buf[4];
  buf[0] = value & 0xff;
  buf[1] = (value >> 8) & 0xff;
  buf[2] = (value >> 16) & 0xff;
  buf[3] = (value >> 24) & 0xff;
  dst->append(buf, 4);
}

std::string KeyMetadataZoneEncoder::Encode(const std::vector<MetadataEntry>& entries) {
  auto result = EncodeWithResult(entries);
  // 合并序列化和 flags 到单个字符串（用长度前缀分隔）
  std::string combined;
  EncodeFixed32Str(&combined, result.sequence_rle.size());
  combined.append(result.sequence_rle);
  combined.append(result.flags_bitmap);
  return combined;
}

static void EncodeFixed32(std::string* dst, uint32_t value) {
  char buf[4];
  buf[0] = value & 0xff;
  buf[1] = (value >> 8) & 0xff;
  buf[2] = (value >> 16) & 0xff;
  buf[3] = (value >> 24) & 0xff;
  dst->append(buf, 4);
}

static uint32_t DecodeFixed32(const char* ptr) {
  return ((static_cast<uint32_t>(static_cast<unsigned char>(ptr[0])))
      | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[1])) << 8)
      | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[2])) << 16)
      | (static_cast<uint32_t>(static_cast<unsigned char>(ptr[3])) << 24));
}

std::string KeyMetadataZoneEncoder::Encode(const std::vector<CedarKey>& keys) {
  std::vector<MetadataEntry> entries;
  entries.reserve(keys.size());
  for (const auto& key : keys) {
    entries.emplace_back(key.sequence(), key.flags());
  }
  return Encode(entries);
}

KeyMetadataZoneEncoder::EncodedResult KeyMetadataZoneEncoder::EncodeWithResult(
    const std::vector<MetadataEntry>& entries) {
  EncodedResult result;
  result.count = entries.size();
  
  // 分离 sequences 和 flags
  std::vector<uint16_t> sequences;
  std::vector<uint8_t> flags;
  sequences.reserve(entries.size());
  flags.reserve(entries.size());
  
  for (const auto& e : entries) {
    sequences.push_back(e.sequence);
    flags.push_back(e.flags);
  }
  
  result.sequence_rle = EncodeSequenceRLE(sequences);
  result.flags_bitmap = EncodeFlagsBitmap(flags);
  
  return result;
}

std::string KeyMetadataZoneEncoder::EncodeSequenceRLE(const std::vector<uint16_t>& sequences) {
  std::string result;
  
  if (sequences.empty()) return result;
  
  uint16_t prev = sequences[0];
  uint32_t run = 1;
  
  // 写入第一个值
  VarintCodec::Encode(prev, result);
  
  for (size_t i = 1; i < sequences.size(); ++i) {
    if (sequences[i] == prev) {
      run++;
    } else {
      // 写入 run length
      VarintCodec::Encode(run, result);
      // 写入新值
      VarintCodec::Encode(sequences[i], result);
      prev = sequences[i];
      run = 1;
    }
  }
  // 写入最后的 run length
  VarintCodec::Encode(run, result);
  
  return result;
}

std::string KeyMetadataZoneEncoder::EncodeFlagsBitmap(const std::vector<uint8_t>& flags) {
  size_t bitmap_size = (flags.size() + 7) / 8;
  std::string result(bitmap_size, 0);
  
  for (size_t i = 0; i < flags.size(); ++i) {
    if (flags[i] & key_flags::kTombstone) {
      result[i / 8] |= (1 << (i % 8));
    }
  }
  
  return result;
}

std::vector<uint16_t> KeyMetadataZoneEncoder::DecodeSequenceRLE(
    const std::string& data, uint32_t count) {
  std::vector<uint16_t> result;
  result.reserve(count);
  
  size_t pos = 0;
  bool first = true;
  uint16_t current_value = 0;
  
  while (pos < data.size() && result.size() < count) {
    auto [value, bytes] = VarintCodec::Decode(data.data() + pos, data.size() - pos);
    if (bytes == 0) break;
    pos += bytes;
    
    if (first) {
      current_value = static_cast<uint16_t>(value);
      first = false;
    } else {
      // value 是 run length
      uint32_t run = static_cast<uint32_t>(value);
      for (uint32_t i = 0; i < run && result.size() < count; ++i) {
        result.push_back(current_value);
      }
      first = true;  // 下一个值是新的 sequence
    }
  }
  
  return result;
}

std::vector<uint8_t> KeyMetadataZoneEncoder::DecodeFlagsBitmap(
    const std::string& data, uint32_t count) {
  std::vector<uint8_t> result;
  result.reserve(count);
  
  for (uint32_t i = 0; i < count; ++i) {
    uint8_t flags = 0;
    if (i / 8 < data.size()) {
      if (data[i / 8] & (1 << (i % 8))) {
        flags |= key_flags::kTombstone;
      }
    }
    result.push_back(flags);
  }
  
  return result;
}

// KeyMetadataZoneEncoder::Decoder 实现
KeyMetadataZoneEncoder::Decoder::Decoder(
    const std::string& sequence_rle, 
    const std::string& flags_bitmap, 
    uint32_t count)
    : sequence_rle_(sequence_rle), flags_bitmap_(flags_bitmap), count_(count) {
}

void KeyMetadataZoneEncoder::Decoder::EnsureDecoded() const {
  if (decoded_) return;
  
  auto sequences = DecodeSequenceRLE(sequence_rle_, count_);
  auto flags = DecodeFlagsBitmap(flags_bitmap_, count_);
  
  decoded_cache_.reserve(count_);
  for (size_t i = 0; i < count_; ++i) {
    MetadataEntry entry;
    if (i < sequences.size()) entry.sequence = sequences[i];
    if (i < flags.size()) entry.flags = flags[i];
    decoded_cache_.push_back(entry);
  }
  
  decoded_ = true;
}

KeyMetadataZoneEncoder::MetadataEntry KeyMetadataZoneEncoder::Decoder::Get(size_t idx) const {
  EnsureDecoded();
  if (idx < decoded_cache_.size()) {
    return decoded_cache_[idx];
  }
  return MetadataEntry();
}

void KeyMetadataZoneEncoder::Decoder::GetRange(
    size_t start_idx, size_t count, std::vector<MetadataEntry>* out) const {
  EnsureDecoded();
  for (size_t i = 0; i < count && (start_idx + i) < decoded_cache_.size(); ++i) {
    out->push_back(decoded_cache_[start_idx + i]);
  }
}

bool KeyMetadataZoneEncoder::Decoder::IsTombstone(size_t idx) const {
  if (idx / 8 >= flags_bitmap_.size()) return false;
  return (flags_bitmap_[idx / 8] & (1 << (idx % 8))) != 0;
}

uint16_t KeyMetadataZoneEncoder::Decoder::GetSequence(size_t idx) const {
  EnsureDecoded();
  if (idx < decoded_cache_.size()) {
    return decoded_cache_[idx].sequence;
  }
  return 0;
}

uint8_t KeyMetadataZoneEncoder::Decoder::GetFlags(size_t idx) const {
  EnsureDecoded();
  if (idx < decoded_cache_.size()) {
    return decoded_cache_[idx].flags;
  }
  return 0;
}

uint32_t KeyMetadataZoneEncoder::Decoder::CountTombstones() const {
  uint32_t count = 0;
  for (size_t i = 0; i < count_; ++i) {
    if (IsTombstone(i)) count++;
  }
  return count;
}

// =============================================================================
// Zone 4: Values 编码器 (Dictionary 或 LZ4/Zstd)
// =============================================================================

std::string ValueZoneEncoder::Encode(const std::vector<Descriptor>& values) {
  auto result = EncodeWithType(values);
  return result.data;
}

std::string ValueZoneEncoder::Encode(const std::vector<Descriptor>& values, ZoneMap* zone_map) {
  for (const auto& v : values) {
    zone_map->Update(v.AsRaw());
  }
  return Encode(values);
}

ValueZoneEncoder::EncodedResult ValueZoneEncoder::EncodeWithType(
    const std::vector<Descriptor>& values) {
  EncodedResult result;
  
  if (values.empty()) {
    result.type = EncodingType::kRaw;
    return result;
  }
  
  // 统计唯一值
  std::unordered_map<uint64_t, size_t> value_to_idx;
  for (const auto& v : values) {
    value_to_idx[v.AsRaw()] = 0;
  }
  
  // 如果唯一值数量少，使用字典编码
  if (value_to_idx.size() <= kDictionaryThreshold) {
    result.type = EncodingType::kDictionary;
    
    // 构建字典
    std::vector<uint64_t> unique_values;
    for (const auto& [val, _] : value_to_idx) {
      size_t idx = unique_values.size();
      value_to_idx[val] = idx;
      unique_values.push_back(val);
    }
    
    // 编码字典
    for (uint64_t val : unique_values) {
      char buf[8];
      memcpy(buf, &val, 8);
      result.dictionary.emplace_back(buf, 8);
    }
    
    // 编码索引
    size_t bits_per_idx = 1;
    while ((1ULL << bits_per_idx) < unique_values.size()) {
      bits_per_idx++;
    }
    
    if (bits_per_idx <= 8) {
      // 使用 1 字节索引
      for (const auto& v : values) {
        result.data.push_back(static_cast<char>(value_to_idx[v.AsRaw()]));
      }
    } else if (bits_per_idx <= 16) {
      // 使用 2 字节索引
      for (const auto& v : values) {
        uint16_t idx = static_cast<uint16_t>(value_to_idx[v.AsRaw()]);
        result.data.push_back(idx & 0xFF);
        result.data.push_back((idx >> 8) & 0xFF);
      }
    } else {
      // 使用 varint 索引
      for (const auto& v : values) {
        VarintCodec::Encode(value_to_idx[v.AsRaw()], result.data);
      }
    }
    
    return result;
  }
  
  // 否则使用原始编码（实际应该用 LZ4/Zstd）
  result.type = EncodingType::kRaw;
  for (const auto& v : values) {
    uint64_t raw = v.AsRaw();
    result.data.append(reinterpret_cast<char*>(&raw), 8);
  }
  
  return result;
}

// ValueZoneEncoder::Decoder 实现
ValueZoneEncoder::Decoder::Decoder(const std::string& data, 
                                   EncodingType type,
                                   const std::vector<std::string>& dictionary)
    : data_(data), type_(type), dictionary_(dictionary) {
  if (type_ == EncodingType::kRaw) {
    total_count_ = data_.size() / 8;
  } else if (type_ == EncodingType::kDictionary) {
    if (dictionary_.size() <= 256) {
      total_count_ = data_.size();  // 1 byte per index
    } else if (dictionary_.size() <= 65536) {
      total_count_ = data_.size() / 2;  // 2 bytes per index
    } else {
      // varint 编码，需要扫描
      size_t pos = 0;
      while (pos < data_.size()) {
        auto [idx, bytes] = VarintCodec::Decode(data_.data() + pos, data_.size() - pos);
        if (bytes == 0) break;
        pos += bytes;
        total_count_++;
      }
    }
  }
}

void ValueZoneEncoder::Decoder::EnsureDecompressed() const {
  if (decompressed_) return;
  
  if (type_ == EncodingType::kRaw) {
    for (size_t i = 0; i < data_.size(); i += 8) {
      if (i + 8 > data_.size()) break;
      uint64_t raw;
      memcpy(&raw, data_.data() + i, 8);
      decompressed_cache_.emplace_back(Descriptor(raw));
    }
  } else if (type_ == EncodingType::kDictionary) {
    if (dictionary_.size() <= 256) {
      for (size_t i = 0; i < data_.size(); ++i) {
        uint8_t idx = static_cast<uint8_t>(data_[i]);
        if (idx < dictionary_.size()) {
          uint64_t raw;
          memcpy(&raw, dictionary_[idx].data(), 8);
          decompressed_cache_.emplace_back(Descriptor(raw));
        }
      }
    } else if (dictionary_.size() <= 65536) {
      for (size_t i = 0; i + 1 < data_.size(); i += 2) {
        uint16_t idx = static_cast<uint8_t>(data_[i]) | 
                      (static_cast<uint8_t>(data_[i+1]) << 8);
        if (idx < dictionary_.size()) {
          uint64_t raw;
          memcpy(&raw, dictionary_[idx].data(), 8);
          decompressed_cache_.emplace_back(Descriptor(raw));
        }
      }
    }
  }
  
  decompressed_ = true;
}

Descriptor ValueZoneEncoder::Decoder::Get(size_t idx) const {
  EnsureDecompressed();
  if (idx < decompressed_cache_.size()) {
    return decompressed_cache_[idx];
  }
  return Descriptor();
}

void ValueZoneEncoder::Decoder::GetRange(
    size_t start_idx, size_t count, std::vector<Descriptor>* out) const {
  EnsureDecompressed();
  for (size_t i = 0; i < count && (start_idx + i) < decompressed_cache_.size(); ++i) {
    out->push_back(decompressed_cache_[start_idx + i]);
  }
}

void ValueZoneEncoder::Decoder::GetSelective(
    const std::vector<size_t>& indices, std::vector<Descriptor>* out) const {
  out->reserve(out->size() + indices.size());
  for (size_t idx : indices) {
    out->push_back(Get(idx));
  }
}

// =============================================================================
// ZoneColumnar Builder
// =============================================================================

void ZoneColumnarBuilder::Add(const CedarKey& key, const Descriptor& value) {
  entity_ids_.push_back(key.entity_id());
  timestamps_.push_back(key.timestamp().value());
  target_ids_.push_back(key.target_id());
  sequences_.push_back(key.sequence());
  flags_.push_back(key.flags());
  values_.push_back(value);
}

std::vector<ZoneData> ZoneColumnarBuilder::Finish() {
  std::vector<ZoneData> zones;
  
  if (entity_ids_.empty()) {
    return zones;
  }
  
  // Zone 0: Entity IDs
  ZoneMap entity_map;
  std::string entity_data = EntityIdZoneEncoder::Encode(entity_ids_, &entity_map);
  zones.push_back(ZoneData(ZoneType::kEntityIds, std::move(entity_data), entity_map));
  
  // Zone 1: Timestamps
  ZoneMap ts_map;
  std::string ts_data = TimestampZoneEncoder::Encode(timestamps_, &ts_map);
  ZoneData ts_zone(ZoneType::kTimestamps, std::move(ts_data), ts_map);
  ts_zone.raw_timestamps = timestamps_;  // 保存原始时间戳用于统一编码
  zones.push_back(std::move(ts_zone));
  
  // Zone 2: Target IDs
  ZoneMap target_map;
  std::string target_data = TargetIdZoneEncoder::Encode(target_ids_, entity_ids_, &target_map);
  zones.push_back(ZoneData(ZoneType::kTargetIds, std::move(target_data), target_map));
  
  // Zone 3: Key Metadata (Sequence + Flags)
  ZoneMap metadata_map;
  std::vector<KeyMetadataZoneEncoder::MetadataEntry> metadata_entries;
  metadata_entries.reserve(sequences_.size());
  for (size_t i = 0; i < sequences_.size(); ++i) {
    metadata_entries.emplace_back(sequences_[i], flags_[i]);
    metadata_map.Update(sequences_[i]);
  }
  auto metadata_result = KeyMetadataZoneEncoder::EncodeWithResult(metadata_entries);
  ZoneData metadata_zone(ZoneType::kKeyMetadata, "", metadata_map);
  metadata_zone.sequence_rle = std::move(metadata_result.sequence_rle);
  metadata_zone.flags_bitmap = std::move(metadata_result.flags_bitmap);
  zones.push_back(std::move(metadata_zone));
  
  // Zone 4: Values
  ZoneMap value_map;
  auto value_result = ValueZoneEncoder::EncodeWithType(values_);
  
  // FIX: Dictionary encoding not properly supported in current format
  // Re-encode as kRaw if dictionary encoding was selected
  if (value_result.type == ValueZoneEncoder::EncodingType::kDictionary) {
    value_result.type = ValueZoneEncoder::EncodingType::kRaw;
    value_result.data.clear();
    for (const auto& v : values_) {
      uint64_t raw = v.AsRaw();
      value_result.data.append(reinterpret_cast<char*>(&raw), 8);
    }
    value_result.dictionary.clear();
  }
  
  ZoneData value_zone(ZoneType::kValues, std::move(value_result.data), value_map);
  value_zone.encoding.value_encoding = value_result.type;
  zones.push_back(std::move(value_zone));
  
  return zones;
}

void ZoneColumnarBuilder::Reset() {
  entity_ids_.clear();
  timestamps_.clear();
  target_ids_.clear();
  sequences_.clear();
  flags_.clear();
  values_.clear();
}

void ZoneColumnarBuilder::Reserve(size_t count) {
  entity_ids_.reserve(count);
  timestamps_.reserve(count);
  target_ids_.reserve(count);
  sequences_.reserve(count);
  flags_.reserve(count);
  values_.reserve(count);
}

CedarKey ZoneColumnarBuilder::ReconstructKey(
    size_t idx, uint16_t column_id, uint8_t entity_type) const {
  if (idx >= entity_ids_.size()) return CedarKey();
  
  return CedarKey(
      entity_ids_[idx],
      static_cast<EntityType>(entity_type),
      column_id,
      Timestamp(timestamps_[idx]),
      sequences_[idx],
      target_ids_[idx],
      flags_[idx]
  );
}

// =============================================================================
// ZoneColumnar Reader
// =============================================================================

void ZoneColumnarReader::InitZone(ZoneType type, const std::string& data) {
  switch (type) {
    case ZoneType::kEntityIds:
      entity_decoder_.emplace(data);
      count_ = entity_decoder_->Count();
      break;
    case ZoneType::kTimestamps:
      timestamp_decoder_.emplace(data);
      break;
    case ZoneType::kTargetIds:
      target_decoder_.emplace(data, TargetIdZoneEncoder::EncodingType::kRaw);
      break;
    case ZoneType::kValues:
      value_decoder_.emplace(data, ValueZoneEncoder::EncodingType::kRaw);
      break;
    default:
      break;
  }
}

void ZoneColumnarReader::Init(const std::vector<ZoneData>& zones) {
  for (const auto& zone : zones) {
    if (zone.type == ZoneType::kKeyMetadata) {
      // Key Metadata Zone 特殊处理
      metadata_decoder_.emplace(zone.sequence_rle, zone.flags_bitmap, 
                                zone.zone_map.count);
    } else {
      InitZone(zone.type, zone.encoded_data);
    }
  }
}

bool ZoneColumnarReader::Get(size_t idx, CedarKey* key, Descriptor* value) const {
  if (idx >= count_) return false;
  
  if (!entity_decoder_ || !timestamp_decoder_ || !value_decoder_) {
    return false;
  }
  
  uint64_t entity_id = entity_decoder_->Get(idx);
  uint64_t ts = timestamp_decoder_->Get(idx);
  
  // 获取 metadata
  uint16_t seq = metadata_decoder_ ? metadata_decoder_->GetSequence(idx) : 0;
  uint8_t flags = metadata_decoder_ ? metadata_decoder_->GetFlags(idx) : 0;
  
  // 构建 CedarKey（默认作为 Vertex，实际应从 SST Header 获取类型）
  *key = CedarKey(entity_id, EntityType::Vertex, 0, Timestamp(ts), seq, 0, flags);
  *value = value_decoder_->Get(idx);
  
  return true;
}

void ZoneColumnarReader::ScanRange(
    size_t start_idx, size_t count,
    std::function<void(size_t, const CedarKey&, const Descriptor&)> callback) const {
  for (size_t i = 0; i < count && (start_idx + i) < count_; ++i) {
    size_t idx = start_idx + i;
    CedarKey key;
    Descriptor value;
    if (Get(idx, &key, &value)) {
      callback(idx, key, value);
    }
  }
}

std::vector<size_t> ZoneColumnarReader::FindByEntityId(uint64_t entity_id) const {
  if (!entity_decoder_) return {};
  return entity_decoder_->FindEntityPositions(entity_id);
}

std::vector<size_t> ZoneColumnarReader::FindByTimeRange(
    uint64_t start_ts, uint64_t end_ts) const {
  if (!timestamp_decoder_) return {};
  return timestamp_decoder_->FindTimeRange(start_ts, end_ts);
}

void ZoneColumnarReader::ScanWithLateMaterialization(
    std::function<bool(size_t, uint64_t, uint64_t)> predicate,
    std::function<void(size_t, const CedarKey&, const Descriptor&)> callback) const {
  // Phase 1: 过滤索引（不解压 Values）
  std::vector<size_t> matching_indices;
  for (size_t i = 0; i < count_; ++i) {
    uint64_t entity_id = entity_decoder_->Get(i);
    uint64_t ts = timestamp_decoder_->Get(i);
    if (predicate(i, entity_id, ts)) {
      matching_indices.push_back(i);
    }
  }
  
  // Phase 2: 只读取匹配的 Values（延迟物化）
  for (size_t idx : matching_indices) {
    CedarKey key;
    Descriptor value = value_decoder_->Get(idx);
    
    uint64_t entity_id = entity_decoder_->Get(idx);
    uint64_t ts = timestamp_decoder_->Get(idx);
    uint64_t target_id = target_decoder_ ? target_decoder_->Get(idx, {}) : 0;
    
    key = CedarKey(entity_id, EntityType::Vertex, 0, Timestamp(ts), 0, target_id);
    callback(idx, key, value);
  }
}

bool ZoneColumnarReader::ZoneMapCheck(uint64_t entity_id) const {
  return entity_id >= entity_zone_map_.min_value && 
         entity_id <= entity_zone_map_.max_value;
}

bool ZoneColumnarReader::ZoneMapCheck(uint64_t start_ts, uint64_t end_ts) const {
  // 检查时间范围是否与 SST 时间范围有交集
  return !(end_ts < timestamp_zone_map_.min_value || 
           start_ts > timestamp_zone_map_.max_value);
}

void ZoneColumnarReader::ValidateConsistency() {
  // 确保所有 Zone 的条目数一致
  size_t entity_count = entity_decoder_ ? entity_decoder_->Count() : 0;
  size_t ts_count = timestamp_decoder_ ? timestamp_decoder_->Count() : 0;
  size_t target_count = target_decoder_ ? target_decoder_->Count() : 0;
  size_t value_count = value_decoder_ ? value_decoder_->Count() : 0;
  
  if (entity_count != ts_count || entity_count != target_count || entity_count != value_count) {
    // 不一致，使用最小的
    count_ = std::min({entity_count, ts_count, target_count, value_count});
  }
}

// 重建完整的 32B CedarKey
CedarKey ZoneColumnarReader::ReconstructKey(
    size_t idx, uint16_t column_id, uint8_t entity_type) const {
  if (idx >= count_) return CedarKey();
  
  uint64_t entity_id = entity_decoder_ ? entity_decoder_->Get(idx) : 0;
  uint64_t ts = timestamp_decoder_ ? timestamp_decoder_->Get(idx) : 0;
  uint64_t target_id = target_decoder_ ? target_decoder_->Get(idx, {}) : 0;
  uint16_t seq = metadata_decoder_ ? metadata_decoder_->GetSequence(idx) : 0;
  uint8_t flags = metadata_decoder_ ? metadata_decoder_->GetFlags(idx) : 0;
  
  return CedarKey(
      entity_id,
      static_cast<EntityType>(entity_type),
      column_id,
      Timestamp(ts),
      seq,
      target_id,
      flags
  );
}

bool ZoneColumnarReader::IsTombstone(size_t idx) const {
  return metadata_decoder_ ? metadata_decoder_->IsTombstone(idx) : false;
}

uint16_t ZoneColumnarReader::GetSequence(size_t idx) const {
  return metadata_decoder_ ? metadata_decoder_->GetSequence(idx) : 0;
}

uint8_t ZoneColumnarReader::GetFlags(size_t idx) const {
  return metadata_decoder_ ? metadata_decoder_->GetFlags(idx) : 0;
}

size_t ZoneColumnarReader::CountTombstones() const {
  return metadata_decoder_ ? metadata_decoder_->CountTombstones() : 0;
}

// ZoneData 辅助方法
ZoneData ZoneData::MakeKeyMetadata(const std::string& seq_rle, 
                                    const std::string& flags_bm,
                                    uint32_t count) {
  ZoneData zone;
  zone.type = ZoneType::kKeyMetadata;
  zone.sequence_rle = seq_rle;
  zone.flags_bitmap = flags_bm;
  zone.zone_map.count = count;
  return zone;
}

}  // namespace cedar
