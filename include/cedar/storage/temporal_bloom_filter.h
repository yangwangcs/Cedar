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

#ifndef FERN_STORAGE_TEMPORAL_BLOOM_FILTER_H_
#define FERN_STORAGE_TEMPORAL_BLOOM_FILTER_H_

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "cedar/core/slice.h"
#include "cedar/core/status.h"
#include "cedar/types/cedar_key.h"

namespace cedar {

// ============================================================================
// 基础布隆过滤器实现 (Classic Bloom Filter)
// ============================================================================
//
// 使用经典的布隆过滤器算法，支持：
// - 添加元素 (Add)
// - 查询元素可能存在 (MayContain)
// - 序列化/反序列化
// - 计算最优哈希函数数量和位数组大小
//
// 假阳性率公式: p ≈ (1 - e^(-kn/m))^k
// 其中: k=哈希函数数, n=元素数, m=位数
//
// 最优参数:
// - 位数: m = -n * ln(p) / (ln(2)^2)
// - 哈希数: k = m/n * ln(2) ≈ 0.693 * m/n
//
class ClassicBloomFilter {
 public:
  // 创建空的布隆过滤器（用于反序列化）
  ClassicBloomFilter() = default;

  // 创建指定参数的布隆过滤器
  // @param num_bits: 位数组大小（比特数）
  // @param num_hashes: 哈希函数数量
  explicit ClassicBloomFilter(size_t num_bits, uint32_t num_hashes = 0)
      : num_hashes_(num_hashes > 0 ? num_hashes : OptimalHashCount(num_bits)),
        bits_((num_bits + 7) / 8, 0),
        num_bits_(num_bits) {}

  // 根据预期元素数和假阳性率创建布隆过滤器
  // @param expected_keys: 预期元素数量
  // @param false_positive_rate: 目标假阳性率 (默认 1%)
  static ClassicBloomFilter Create(size_t expected_keys,
                                   double false_positive_rate = 0.01) {
    size_t num_bits = OptimalBitCount(expected_keys, false_positive_rate);
    uint32_t num_hashes = OptimalHashCount(num_bits, expected_keys);
    return ClassicBloomFilter(num_bits, num_hashes);
  }

  // 添加元素到过滤器
  // @param data: 元素数据的指针
  // @param len: 数据长度
  void Add(const void* data, size_t len) {
    uint64_t h1, h2;
    Hash128(data, len, &h1, &h2);
    
    for (uint32_t i = 0; i < num_hashes_; ++i) {
      size_t bit_pos = (h1 + i * h2) % num_bits_;
      SetBit(bit_pos);
    }
  }

  // 添加元素（Slice 版本）
  void Add(const Slice& key) { Add(key.data(), key.size()); }

  // 查询元素是否可能存在
  // @return true: 可能存在; false: 一定不存在
  bool MayContain(const void* data, size_t len) const {
    uint64_t h1, h2;
    Hash128(data, len, &h1, &h2);
    
    for (uint32_t i = 0; i < num_hashes_; ++i) {
      size_t bit_pos = (h1 + i * h2) % num_bits_;
      if (!GetBit(bit_pos)) {
        return false;  // 有任何一个位为0，则元素一定不存在
      }
    }
    return true;  // 所有位都为1，元素可能存在
  }

  // 查询元素（Slice 版本）
  bool MayContain(const Slice& key) const {
    return MayContain(key.data(), key.size());
  }

  // 序列化到字符串
  // 格式: [num_hashes(4B)][num_bits(8B)][bits_data]
  void Serialize(std::string* dst) const {
    // 写入元数据
    dst->append(reinterpret_cast<const char*>(&num_hashes_), sizeof(num_hashes_));
    uint64_t num_bits = num_bits_;
    dst->append(reinterpret_cast<const char*>(&num_bits), sizeof(num_bits));
    
    // 写入位数组数据（使用简单压缩：只写入非零字节位置）
    // 格式: [non_zero_count(4B)][(index, value) pairs...]
    uint32_t non_zero_count = 0;
    size_t count_pos = dst->size();
    dst->append(sizeof(non_zero_count), '\0');  // 占位
    
    for (size_t i = 0; i < bits_.size(); ++i) {
      if (bits_[i] != 0) {
        uint32_t idx = static_cast<uint32_t>(i);
        dst->append(reinterpret_cast<const char*>(&idx), sizeof(idx));
        dst->append(reinterpret_cast<const char*>(&bits_[i]), 1);
        ++non_zero_count;
      }
    }
    
    // 回写非零字节数量
    memcpy(&(*dst)[count_pos], &non_zero_count, sizeof(non_zero_count));
  }

  // 从 Slice 反序列化
  // @return true: 成功; false: 失败
  bool Deserialize(Slice* input) {
    if (input->size() < sizeof(num_hashes_) + sizeof(num_bits_)) {
      return false;
    }
    
    // 读取元数据
    memcpy(&num_hashes_, input->data(), sizeof(num_hashes_));
    input->remove_prefix(sizeof(num_hashes_));
    
    uint64_t num_bits;
    memcpy(&num_bits, input->data(), sizeof(num_bits));
    input->remove_prefix(sizeof(num_bits));
    num_bits_ = static_cast<size_t>(num_bits);
    
    // 初始化位数组
    bits_.assign((num_bits_ + 7) / 8, 0);
    
    // 读取非零字节
    if (input->size() < sizeof(uint32_t)) {
      return false;
    }
    uint32_t non_zero_count;
    memcpy(&non_zero_count, input->data(), sizeof(non_zero_count));
    input->remove_prefix(sizeof(non_zero_count));
    
    for (uint32_t i = 0; i < non_zero_count; ++i) {
      if (input->size() < sizeof(uint32_t) + 1) {
        return false;
      }
      uint32_t idx;
      memcpy(&idx, input->data(), sizeof(idx));
      input->remove_prefix(sizeof(idx));
      bits_[idx] = static_cast<uint8_t>((*input)[0]);
      input->remove_prefix(1);
    }
    
    return true;
  }

  // 从字符串反序列化
  bool Deserialize(const std::string& data) {
    Slice slice(data);
    return Deserialize(&slice);
  }

  // 获取位数组大小（比特数）
  size_t NumBits() const { return num_bits_; }

  // 获取哈希函数数量
  uint32_t NumHashes() const { return num_hashes_; }

  // 获取内存使用量（字节）
  size_t MemoryUsage() const { return bits_.size() + sizeof(*this); }

  // 清空过滤器
  void Clear() { std::fill(bits_.begin(), bits_.end(), 0); }

  // 计算最优位数组大小
  // m = -n * ln(p) / (ln(2)^2)
  static size_t OptimalBitCount(size_t expected_keys,
                                 double false_positive_rate) {
    if (expected_keys == 0) return 8;  // 最小8位
    double m = -static_cast<double>(expected_keys) * std::log(false_positive_rate) /
               (std::log(2) * std::log(2));
    return std::max(static_cast<size_t>(m), size_t{8});
  }

  // 计算最优哈希函数数量
  // k = m/n * ln(2)
  static uint32_t OptimalHashCount(size_t num_bits, size_t expected_keys) {
    if (expected_keys == 0) return 1;
    double k = static_cast<double>(num_bits) / expected_keys * std::log(2);
    return std::max(static_cast<uint32_t>(k), uint32_t{1});
  }

  static uint32_t OptimalHashCount(size_t expected_keys) {
    return std::max(static_cast<uint32_t>(0.693 * expected_keys), uint32_t{1});
  }

 private:
  // MurmurHash3 128-bit 哈希函数（内部使用）
  // 参考: https://github.com/aappleby/smhasher/wiki/MurmurHash3
  static void Hash128(const void* data, size_t len, uint64_t* out_h1,
                      uint64_t* out_h2) {
    const uint64_t c1 = 0x87c37b91114253d5ULL;
    const uint64_t c2 = 0x4cf5ad432745937fULL;
    const uint64_t seed = 0x9747b28c;

    const uint64_t* blocks = reinterpret_cast<const uint64_t*>(data);
    size_t num_blocks = len / 16;

    uint64_t h1 = seed;
    uint64_t h2 = seed;

    // 处理 128-bit 块
    for (size_t i = 0; i < num_blocks; ++i) {
      uint64_t k1 = blocks[i * 2];
      uint64_t k2 = blocks[i * 2 + 1];

      k1 *= c1;
      k1 = RotateLeft64(k1, 31);
      k1 *= c2;
      h1 ^= k1;
      h1 = RotateLeft64(h1, 27);
      h1 += h2;
      h1 = h1 * 5 + 0x52dce729;

      k2 *= c2;
      k2 = RotateLeft64(k2, 33);
      k2 *= c1;
      h2 ^= k2;
      h2 = RotateLeft64(h2, 31);
      h2 += h1;
      h2 = h2 * 5 + 0x38495ab5;
    }

    // 处理尾部
    const uint8_t* tail = reinterpret_cast<const uint8_t*>(data) + num_blocks * 16;
    uint64_t k1 = 0;
    uint64_t k2 = 0;

    switch (len & 15) {
      case 15: k2 ^= static_cast<uint64_t>(tail[14]) << 48; [[fallthrough]];
      case 14: k2 ^= static_cast<uint64_t>(tail[13]) << 40; [[fallthrough]];
      case 13: k2 ^= static_cast<uint64_t>(tail[12]) << 32; [[fallthrough]];
      case 12: k2 ^= static_cast<uint64_t>(tail[11]) << 24; [[fallthrough]];
      case 11: k2 ^= static_cast<uint64_t>(tail[10]) << 16; [[fallthrough]];
      case 10: k2 ^= static_cast<uint64_t>(tail[9]) << 8; [[fallthrough]];
      case 9:
        k2 ^= static_cast<uint64_t>(tail[8]);
        k2 *= c2;
        k2 = RotateLeft64(k2, 33);
        k2 *= c1;
        h2 ^= k2;
        [[fallthrough]];
      case 8: k1 ^= static_cast<uint64_t>(tail[7]) << 56; [[fallthrough]];
      case 7: k1 ^= static_cast<uint64_t>(tail[6]) << 48; [[fallthrough]];
      case 6: k1 ^= static_cast<uint64_t>(tail[5]) << 40; [[fallthrough]];
      case 5: k1 ^= static_cast<uint64_t>(tail[4]) << 32; [[fallthrough]];
      case 4: k1 ^= static_cast<uint64_t>(tail[3]) << 24; [[fallthrough]];
      case 3: k1 ^= static_cast<uint64_t>(tail[2]) << 16; [[fallthrough]];
      case 2: k1 ^= static_cast<uint64_t>(tail[1]) << 8; [[fallthrough]];
      case 1:
        k1 ^= static_cast<uint64_t>(tail[0]);
        k1 *= c1;
        k1 = RotateLeft64(k1, 31);
        k1 *= c2;
        h1 ^= k1;
    }

    // 最终化
    h1 ^= len;
    h2 ^= len;
    h1 += h2;
    h2 += h1;
    h1 = FMix64(h1);
    h2 = FMix64(h2);
    h1 += h2;
    h2 += h1;

    *out_h1 = h1;
    *out_h2 = h2;
  }

  static uint64_t RotateLeft64(uint64_t x, int r) {
    return (x << r) | (x >> (64 - r));
  }

  static uint64_t FMix64(uint64_t k) {
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33;
    return k;
  }

  // 位操作
  void SetBit(size_t pos) {
    size_t byte_pos = pos / 8;
    size_t bit_offset = pos % 8;
    bits_[byte_pos] |= (1 << bit_offset);
  }

  bool GetBit(size_t pos) const {
    size_t byte_pos = pos / 8;
    size_t bit_offset = pos % 8;
    return (bits_[byte_pos] & (1 << bit_offset)) != 0;
  }

  uint32_t num_hashes_ = 0;
  std::vector<uint8_t> bits_;
  size_t num_bits_ = 0;
};

// ============================================================================
// 时间范围布隆过滤器 (Temporal Bloom Filter)
// ============================================================================
//
// 用于高效判断"某时间段内某实体是否有数据"的布隆过滤器。
//
// 设计思想:
// - Level 0: 整体过滤器（所有键的实体ID）- 用于快速排除不存在任何时间段的实体
// - Level 1+: 时间桶过滤器 - 将时间范围划分为多个桶，每个桶维护自己的布隆过滤器
//
// 查询优化:
// - MayContain(): 只查 Level 0（整体过滤器）
// - MayExistInRange(): 先查 Level 0，如果命中，再查相关时间桶的过滤器
//
// 时间桶划分策略:
// - 根据 SST 文件的时间跨度自动选择粒度（小时/天）
// - 支持不均匀时间分布（某些时间段数据多，某些少）
//
class TemporalBloomFilter {
 public:
  // 配置参数
  struct Config {
    double false_positive_rate;  // 1% 假阳性率
    size_t expected_keys;        // 预期键数量（用于 Level 0）
    uint32_t hours_per_bucket;   // 每个时间桶的小时数（默认1小时）
    
    // 默认构造函数
    Config()
        : false_positive_rate(0.01),
          expected_keys(100000),
          hours_per_bucket(1) {}
    
    // 计算每个桶的预期键数
    size_t ExpectedKeysPerBucket(size_t num_buckets) const {
      if (num_buckets == 0) return expected_keys;
      return std::max(expected_keys / num_buckets, size_t{100});
    }
  };

  // 时间范围信息
  struct TimeRange {
    Timestamp start;
    Timestamp end;
    
    TimeRange() : start(0), end(0) {}
    TimeRange(Timestamp s, Timestamp e) : start(s), end(e) {}
    
    uint64_t DurationMicros() const { return end.value() - start.value(); }
    uint64_t DurationHours() const { return DurationMicros() / kMicrosPerHour; }
    
    bool Contains(Timestamp ts) const { return ts >= start && ts <= end; }
    bool IsValid() const { return end.value() > start.value(); }
  };

  // 默认构造（用于反序列化）
  TemporalBloomFilter() = default;

  // 构造时间范围布隆过滤器
  // @param start_time: SST 文件起始时间
  // @param end_time: SST 文件结束时间
  // @param config: 配置参数
  TemporalBloomFilter(Timestamp start_time, Timestamp end_time,
                      const Config& config = Config())
      : time_range_(start_time, end_time),
        config_(config),
        time_bucket_size_micros_(config.hours_per_bucket * kMicrosPerHour) {
    InitializeBuckets();
  }

  // 添加键（完整 CedarKey）
  void Add(const CedarKey& key) {
    Add(key.entity_id(), key.timestamp());
  }

  // 添加键（entity_id + timestamp）
  void Add(uint64_t entity_id, Timestamp timestamp) {
    // 添加到整体过滤器
    if (level0_filter_) {
      level0_filter_->Add(&entity_id, sizeof(entity_id));
    }
    
    // 添加到对应的时间桶过滤器
    auto* bucket = GetBucketForTimestamp(timestamp);
    if (bucket) {
      bucket->Add(&entity_id, sizeof(entity_id));
    }
    
    ++total_keys_added_;
  }

  // 批量添加键
  template<typename Iterator>
  void AddRange(Iterator begin, Iterator end) {
    for (auto it = begin; it != end; ++it) {
      Add(*it);
    }
  }

  // 查询实体是否存在（整体过滤器，不考虑时间）
  // @return true: 可能存在; false: 一定不存在
  bool MayContain(uint64_t entity_id) const {
    if (!level0_filter_) {
      return true;  // 保守策略：没有过滤器则假设可能存在
    }
    return level0_filter_->MayContain(&entity_id, sizeof(entity_id));
  }

  // 查询某时间段内某实体是否有数据
  // 这是核心接口，用于 SST 文件跳过优化
  //
  // @param entity_id: 实体 ID
  // @param start: 查询起始时间（包含）
  // @param end: 查询结束时间（包含）
  // @return true: 可能有数据（需要查 SST）
  // @return false: 一定没有数据（可以跳过 SST）
  bool MayExistInRange(uint64_t entity_id, Timestamp start, Timestamp end) const {
    // 快速路径：先检查整体过滤器
    if (!MayContain(entity_id)) {
      return false;  // 实体整体不存在
    }
    
    // 如果时间范围覆盖了整个 SST 文件时间范围，直接返回 true
    if (start <= time_range_.start && end >= time_range_.end) {
      return true;
    }
    
    // 计算需要检查的时间桶索引范围
    size_t start_bucket = TimestampToBucketIndex(start);
    size_t end_bucket = TimestampToBucketIndex(end);
    
    // 限制在有效范围内
    start_bucket = std::min(start_bucket, buckets_.size());
    end_bucket = std::min(end_bucket, buckets_.size() - 1);
    
    // 检查相关时间桶
    for (size_t i = start_bucket; i <= end_bucket && i < buckets_.size(); ++i) {
      if (buckets_[i].MayContain(&entity_id, sizeof(entity_id))) {
        return true;  // 任意一个桶命中，则可能存在
      }
    }
    
    return false;  // 所有相关桶都未命中，一定不存在
  }

  // 简化的范围查询接口（使用 TimeRange）
  bool MayExistInRange(uint64_t entity_id, const TimeRange& range) const {
    return MayExistInRange(entity_id, range.start, range.end);
  }

  // 序列化到字符串
  // 格式:
  //   [header]
  //   - magic(4B): "TBF\0"
  //   - version(4B): 版本号
  //   - start_time(8B): 起始时间戳
  //   - end_time(8B): 结束时间戳
  //   - hours_per_bucket(4B)
  //   - num_buckets(4B)
  //   [level0_filter]
  //   [bucket_filters...]
  std::string Serialize() const {
    std::string result;
    result.reserve(EstimateSerializedSize());
    
    // Header
    const char magic[4] = {'T', 'B', 'F', '\0'};
    result.append(magic, 4);
    
    uint32_t version = kCurrentVersion;
    result.append(reinterpret_cast<const char*>(&version), sizeof(version));
    
    uint64_t start = time_range_.start.value();
    uint64_t end = time_range_.end.value();
    result.append(reinterpret_cast<const char*>(&start), sizeof(start));
    result.append(reinterpret_cast<const char*>(&end), sizeof(end));
    
    result.append(reinterpret_cast<const char*>(&config_.hours_per_bucket),
                  sizeof(config_.hours_per_bucket));
    
    uint32_t num_buckets = static_cast<uint32_t>(buckets_.size());
    result.append(reinterpret_cast<const char*>(&num_buckets), sizeof(num_buckets));
    
    // Level 0 filter
    level0_filter_->Serialize(&result);
    
    // Bucket filters
    for (const auto& bucket : buckets_) {
      bucket.Serialize(&result);
    }
    
    return result;
  }

  // 反序列化
  static std::optional<TemporalBloomFilter> Deserialize(const std::string& data) {
    Slice slice(data);
    return Deserialize(&slice);
  }

  static std::optional<TemporalBloomFilter> Deserialize(Slice* input) {
    TemporalBloomFilter tbf;
    
    // 检查 magic
    if (input->size() < 4) return std::nullopt;
    if (input->data()[0] != 'T' || input->data()[1] != 'B' ||
        input->data()[2] != 'F' || input->data()[3] != '\0') {
      return std::nullopt;
    }
    input->remove_prefix(4);
    
    // 检查版本
    if (input->size() < sizeof(uint32_t)) return std::nullopt;
    uint32_t version;
    memcpy(&version, input->data(), sizeof(version));
    if (version != kCurrentVersion) {
      return std::nullopt;  // 版本不兼容
    }
    input->remove_prefix(sizeof(version));
    
    // 读取时间范围
    if (input->size() < sizeof(uint64_t) * 2) return std::nullopt;
    uint64_t start, end;
    memcpy(&start, input->data(), sizeof(start));
    input->remove_prefix(sizeof(start));
    memcpy(&end, input->data(), sizeof(end));
    input->remove_prefix(sizeof(end));
    tbf.time_range_ = TimeRange(Timestamp(start), Timestamp(end));
    
    // 读取配置
    if (input->size() < sizeof(uint32_t)) return std::nullopt;
    memcpy(&tbf.config_.hours_per_bucket, input->data(),
           sizeof(tbf.config_.hours_per_bucket));
    input->remove_prefix(sizeof(tbf.config_.hours_per_bucket));
    
    tbf.time_bucket_size_micros_ =
        tbf.config_.hours_per_bucket * kMicrosPerHour;
    
    // 读取桶数量
    if (input->size() < sizeof(uint32_t)) return std::nullopt;
    uint32_t num_buckets;
    memcpy(&num_buckets, input->data(), sizeof(num_buckets));
    input->remove_prefix(sizeof(num_buckets));
    
    // 反序列化 Level 0 filter
    tbf.level0_filter_ = std::make_unique<ClassicBloomFilter>();
    if (!tbf.level0_filter_->Deserialize(input)) {
      return std::nullopt;
    }
    
    // 反序列化桶过滤器
    tbf.buckets_.reserve(num_buckets);
    for (uint32_t i = 0; i < num_buckets; ++i) {
      ClassicBloomFilter bucket;
      if (!bucket.Deserialize(input)) {
        return std::nullopt;
      }
      tbf.buckets_.push_back(std::move(bucket));
    }
    
    return tbf;
  }

  // 获取内存使用量
  size_t MemoryUsage() const {
    size_t usage = sizeof(*this) + level0_filter_->MemoryUsage();
    for (const auto& bucket : buckets_) {
      usage += bucket.MemoryUsage();
    }
    return usage;
  }

  // 获取时间范围
  const TimeRange& GetTimeRange() const { return time_range_; }

  // 获取配置
  const Config& GetConfig() const { return config_; }

  // 获取时间桶数量
  size_t NumBuckets() const { return buckets_.size(); }

  // 获取已添加的键数量
  size_t TotalKeysAdded() const { return total_keys_added_; }

  // 估算假阳性率
  double EstimatedFalsePositiveRate() const {
    if (!level0_filter_ || total_keys_added_ == 0) {
      return 0.0;
    }
    // 简化的 FPR 估计: (1 - e^(-kn/m))^k
    double m = static_cast<double>(level0_filter_->NumBits());
    double n = static_cast<double>(total_keys_added_);
    double k = static_cast<double>(level0_filter_->NumHashes());
    double fpr = std::pow(1.0 - std::exp(-k * n / m), k);
    return fpr;
  }

  // 调试信息
  std::string DebugString() const {
    std::string result = "TemporalBloomFilter{";
    result += "range=[" + std::to_string(time_range_.start.value()) + ", " +
              std::to_string(time_range_.end.value()) + "], ";
    result += "buckets=" + std::to_string(buckets_.size()) + ", ";
    result += "keys_added=" + std::to_string(total_keys_added_) + ", ";
    result += "memory=" + std::to_string(MemoryUsage()) + "bytes, ";
    result += "est_fpr=" + std::to_string(EstimatedFalsePositiveRate());
    result += "}";
    return result;
  }

 private:
  static constexpr uint64_t kMicrosPerHour = 3600ULL * 1000000ULL;
  static constexpr uint32_t kCurrentVersion = 1;

  // 初始化时间桶
  void InitializeBuckets() {
    if (!time_range_.IsValid()) {
      return;
    }
    
    // 创建 Level 0 过滤器（整体过滤器）
    level0_filter_ = std::make_unique<ClassicBloomFilter>(
        ClassicBloomFilter::Create(config_.expected_keys,
                                   config_.false_positive_rate));
    
    // 计算时间桶数量
    uint64_t duration_micros = time_range_.DurationMicros();
    size_t num_buckets =
        (duration_micros + time_bucket_size_micros_ - 1) / time_bucket_size_micros_;
    num_buckets = std::max(num_buckets, size_t{1});
    
    // 创建时间桶过滤器
    size_t keys_per_bucket = config_.ExpectedKeysPerBucket(num_buckets);
    buckets_.reserve(num_buckets);
    for (size_t i = 0; i < num_buckets; ++i) {
      buckets_.emplace_back(
          ClassicBloomFilter::Create(keys_per_bucket, config_.false_positive_rate));
    }
  }

  // 将时间戳转换为桶索引
  size_t TimestampToBucketIndex(Timestamp ts) const {
    if (ts < time_range_.start) return 0;
    uint64_t offset = ts.value() - time_range_.start.value();
    size_t idx = static_cast<size_t>(offset / time_bucket_size_micros_);
    return std::min(idx, buckets_.size() - 1);
  }

  // 获取时间戳对应的桶
  ClassicBloomFilter* GetBucketForTimestamp(Timestamp ts) {
    if (buckets_.empty() || ts < time_range_.start || ts > time_range_.end) {
      return nullptr;
    }
    size_t idx = TimestampToBucketIndex(ts);
    return &buckets_[idx];
  }

  // 估算序列化后大小
  size_t EstimateSerializedSize() const {
    size_t header_size = 32;  // 固定头部大小
    size_t level0_size = level0_filter_->MemoryUsage();
    size_t buckets_size = 0;
    for (const auto& bucket : buckets_) {
      buckets_size += bucket.MemoryUsage();
    }
    return header_size + level0_size + buckets_size;
  }

  TimeRange time_range_;
  Config config_;
  uint64_t time_bucket_size_micros_ = kMicrosPerHour;  // 默认1小时
  
  std::unique_ptr<ClassicBloomFilter> level0_filter_;  // 整体过滤器
  std::vector<ClassicBloomFilter> buckets_;            // 时间桶过滤器
  
  size_t total_keys_added_ = 0;  // 统计信息
};

// ============================================================================
// SST 文件时间范围布隆过滤器管理器
// ============================================================================
//
// 用于 SST 文件创建、加载和管理 TemporalBloomFilter 的工具类。
//
// 使用示例:
//   // 创建 SST 文件时生成过滤器
//   auto keys = CollectKeysForSST(...);
//   auto tbf = SSTTemporalBloomFilter::CreateForSST(start, end, keys);
//   std::string metadata = tbf->Serialize();
//   // 将 metadata 写入 SST 文件 footer
//
//   // 查询时加载过滤器
//   auto tbf = SSTTemporalBloomFilter::LoadFromMetadata(metadata);
//   if (tbf && !tbf->MayExistInRange(entity_id, query_start, query_end)) {
//     // 跳过此 SST 文件
//   }
//
class SSTTemporalBloomFilter {
 public:
  // 为 SST 文件创建新的时间范围布隆过滤器
  //
  // @param file_start_time: SST 文件中数据的最小时间戳
  // @param file_end_time: SST 文件中数据的最大时间戳
  // @param keys: SST 文件中的所有键
  // @param config: 过滤器配置（可选，使用默认配置）
  // @return 创建的 TemporalBloomFilter 智能指针
  static std::unique_ptr<TemporalBloomFilter> CreateForSST(
      Timestamp file_start_time,
      Timestamp file_end_time,
      const std::vector<CedarKey>& keys,
      const TemporalBloomFilter::Config& config = TemporalBloomFilter::Config()) {
    auto tbf = std::make_unique<TemporalBloomFilter>(file_start_time, file_end_time, config);
    
    for (const auto& key : keys) {
      tbf->Add(key);
    }
    
    return tbf;
  }

  // 从 SST 文件元数据加载过滤器
  //
  // @param metadata: 从 SST 文件 footer 中读取的过滤器元数据
  // @return 加载的 TemporalBloomFilter，失败返回 nullopt
  static std::optional<TemporalBloomFilter> LoadFromMetadata(
      const std::string& metadata) {
    return TemporalBloomFilter::Deserialize(metadata);
  }

  // 从 SST 文件元数据加载过滤器（智能指针版本）
  static std::unique_ptr<TemporalBloomFilter> LoadFromMetadataPtr(
      const std::string& metadata) {
    auto result = LoadFromMetadata(metadata);
    if (!result) {
      return nullptr;
    }
    return std::make_unique<TemporalBloomFilter>(std::move(*result));
  }

  // 创建空的过滤器（用于测试或特殊场景）
  static std::unique_ptr<TemporalBloomFilter> CreateEmpty() {
    return std::make_unique<TemporalBloomFilter>();
  }

  // 估算给定 SST 文件的过滤器大小
  // 用于 SST 文件布局规划
  static size_t EstimateFilterSize(
      uint64_t duration_hours,
      size_t num_keys,
      double false_positive_rate = 0.01,
      uint32_t hours_per_bucket = 1) {
    // Level 0 大小
    size_t level0_bits = ClassicBloomFilter::OptimalBitCount(num_keys, false_positive_rate);
    size_t level0_size = (level0_bits + 7) / 8;
    
    // 桶数量和大小
    size_t num_buckets = std::max(duration_hours / hours_per_bucket, uint64_t{1});
    size_t keys_per_bucket = std::max(num_keys / num_buckets, size_t{1});
    size_t bucket_bits = ClassicBloomFilter::OptimalBitCount(keys_per_bucket, false_positive_rate);
    size_t bucket_size = (bucket_bits + 7) / 8;
    
    // 总大小（包含元数据开销）
    return 32 + level0_size + num_buckets * bucket_size;
  }
};

// ============================================================================
// 布隆过滤器工具函数
// ============================================================================

// 计算查询优化效果（理论值）
// 返回可以跳过的 SST 文件比例估计
inline double EstimateFilterSkipRate(double actual_selectivity,
                                      double false_positive_rate) {
  // 如果查询实际只覆盖 10% 的文件，FPR=1% 时:
  // 可以跳过的比例 = 1 - 0.1 - (0.9 * 0.01) ≈ 89%
  return 1.0 - actual_selectivity -
         (1.0 - actual_selectivity) * false_positive_rate;
}

// 根据数据分布推荐配置
inline TemporalBloomFilter::Config RecommendConfig(
    uint64_t duration_hours,
    size_t total_keys,
    double target_fpr = 0.01) {
  TemporalBloomFilter::Config config;
  config.false_positive_rate = target_fpr;
  config.expected_keys = total_keys;
  
  // 根据时间跨度选择桶大小
  // - 短时间跨度 (< 24h): 1小时桶
  // - 中等时间跨度 (24h-7d): 4小时桶
  // - 长时间跨度 (> 7d): 12小时桶
  if (duration_hours <= 24) {
    config.hours_per_bucket = 1;
  } else if (duration_hours <= 168) {  // 7 days
    config.hours_per_bucket = 4;
  } else {
    config.hours_per_bucket = 12;
  }
  
  return config;
}

}  // namespace cedar

#endif  // FERN_STORAGE_TEMPORAL_BLOOM_FILTER_H_
