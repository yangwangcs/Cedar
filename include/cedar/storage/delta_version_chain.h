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

#ifndef FERN_STORAGE_DELTA_VERSION_CHAIN_H_
#define FERN_STORAGE_DELTA_VERSION_CHAIN_H_

// ============================================================================
// Delta 版本链 (DeltaVersionChain)
// ============================================================================
//
// 基于增量编码的高效版本存储系统。
//
// 核心设计：
// - 使用 VersionGroup 管理版本，每个组包含一个基准版本和多个增量
// - 通过 DeltaVersionEncoder 压缩版本间的差异
// - 支持组链结构，当增量超过阈值时自动创建新组
// - 热点缓存优化频繁访问的版本
//
// 存储结构：
//   Group N (最新)    Group N-1         Group 0 (最旧)
//   +-----------+     +-----------+     +-----------+
//   | Base V_k  |---->| Base V_j  |---->| Base V_0  |
//   | Delta...  |     | Delta...  |     | Delta...  |
//   +-----------+     +-----------+     +-----------+
//
// 空间效率：相比完整存储，可实现 50-90% 空间节省（取决于数据变化模式）
//
// 使用示例：
//   DeltaVersionChain chain;
//   chain.InsertVersion(Timestamp{1000}, desc1);
//   chain.InsertVersion(Timestamp{2000}, desc2);  // 存储为 delta
//   
//   auto result = chain.GetVersionAt(Timestamp{2000});
//   auto stats = chain.GetCompressionStats();
//
// ============================================================================

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <vector>

#include "cedar/storage/delta_version_encoder.h"
#include "cedar/types/descriptor.h"
#include "cedar/types/cedar_key.h"

namespace cedar {

// ============================================================================
// 前向声明
// ============================================================================

class DeltaVersionChain;

// ============================================================================
// 压缩版本节点（VersionGroup 的链表节点）
// ============================================================================

/**
 * CompressedVersionNode - 压缩版本链节点
 *
 * 每个节点代表一个版本组，包含：
 * - 基准版本（完整存储）
 * - 增量列表（压缩存储）
 * - 指向下一个组的指针
 * - 压缩统计信息
 */
struct CompressedVersionNode {
  // 基准版本信息
  Timestamp base_timestamp;           // 基准版本时间戳
  Descriptor base_value;              // 基准版本完整值
  
  // 增量列表（按时间升序排列）
  std::vector<DeltaEntry> deltas;
  
  // 组链指针（指向前一个/更旧的组）
  CompressedVersionNode* prev_group;
  
  // 指向下一个/更新的组
  CompressedVersionNode* next_group;
  
  // 压缩统计
  size_t original_size;               // 如果存储完整值需要的字节数
  size_t compressed_size;             // 实际存储的字节数
  size_t version_count;               // 本组包含的版本数量（含基准）
  
  // 配置参数
  VersionGroup::Config config;
  
  // 默认构造函数
  CompressedVersionNode()
      : base_timestamp(0),
        prev_group(nullptr),
        next_group(nullptr),
        original_size(0),
        compressed_size(0),
        version_count(0) {}
  
  // 构造函数
  explicit CompressedVersionNode(const VersionGroup::Config& cfg)
      : base_timestamp(0),
        prev_group(nullptr),
        next_group(nullptr),
        original_size(0),
        compressed_size(0),
        version_count(0),
        config(cfg) {}
  
  CompressedVersionNode(Timestamp ts, const Descriptor& value,
                        const VersionGroup::Config& cfg)
      : base_timestamp(ts),
        base_value(value),
        prev_group(nullptr),
        next_group(nullptr),
        original_size(8),  // Descriptor 大小
        compressed_size(8),
        version_count(1),
        config(cfg) {}
  
  // 禁用拷贝，允许移动
  CompressedVersionNode(const CompressedVersionNode&) = delete;
  CompressedVersionNode& operator=(const CompressedVersionNode&) = delete;
  CompressedVersionNode(CompressedVersionNode&&) = default;
  CompressedVersionNode& operator=(CompressedVersionNode&&) = default;
  
  /**
   * 计算组内包含的时间戳范围
   * @return [最早时间戳, 最晚时间戳]
   */
  std::pair<Timestamp, Timestamp> GetTimestampRange() const {
    if (deltas.empty()) {
      return {base_timestamp, base_timestamp};
    }
    // deltas 按时间升序，最后一个是最新的
    return {base_timestamp, deltas.back().timestamp};
  }
  
  /**
   * 检查时间戳是否在本组范围内
   */
  bool Contains(Timestamp ts) const {
    auto [min_ts, max_ts] = GetTimestampRange();
    return ts >= min_ts && ts <= max_ts;
  }
  
  /**
   * 获取本组最新时间戳
   */
  Timestamp GetLatestTimestamp() const {
    return deltas.empty() ? base_timestamp : deltas.back().timestamp;
  }
  
  /**
   * 更新压缩统计
   */
  void UpdateStats(size_t original, size_t compressed) {
    original_size += original;
    compressed_size += compressed;
  }
  
  /**
   * 获取压缩率 (compressed / original)
   */
  double GetCompressionRatio() const {
    if (original_size == 0) return 1.0;
    return static_cast<double>(compressed_size) / original_size;
  }
  
  /**
   * 获取空间节省率
   */
  double GetSpaceSaving() const {
    if (original_size == 0) return 0.0;
    return 1.0 - GetCompressionRatio();
  }
};

// ============================================================================
// 热点缓存（用于优化频繁访问的版本）
// ============================================================================

/**
 * VersionCache - 热点版本缓存
 *
 * 缓存最近重建的版本，避免重复计算 delta 链。
 * 使用 LRU 淘汰策略，线程安全。
 */
class VersionCache {
 public:
  struct CacheEntry {
    Timestamp timestamp;
    Descriptor value;
    uint64_t access_count;
    uint64_t last_access;
    
    CacheEntry(Timestamp ts, const Descriptor& val)
        : timestamp(ts), value(val), access_count(1), last_access(0) {}
  };
  
  VersionCache() : capacity_(16) {}
  explicit VersionCache(size_t capacity) : capacity_(capacity) {}
  
  // 自定义移动构造函数
  VersionCache(VersionCache&& other) noexcept
      : capacity_(other.capacity_),
        entries_(std::move(other.entries_)),
        access_counter_(other.access_counter_.load(std::memory_order_relaxed)),
        cache_hits_(other.cache_hits_.load(std::memory_order_relaxed)),
        cache_misses_(other.cache_misses_.load(std::memory_order_relaxed)) {}
  
  // 自定义移动赋值
  VersionCache& operator=(VersionCache&& other) noexcept {
    if (this != &other) {
      capacity_ = other.capacity_;
      entries_ = std::move(other.entries_);
      access_counter_.store(other.access_counter_.load(std::memory_order_relaxed), std::memory_order_relaxed);
      cache_hits_.store(other.cache_hits_.load(std::memory_order_relaxed), std::memory_order_relaxed);
      cache_misses_.store(other.cache_misses_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    }
    return *this;
  }
  
  // 获取缓存中的版本
  std::optional<Descriptor> Get(Timestamp ts) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    for (auto& entry : entries_) {
      if (entry.timestamp == ts) {
        entry.access_count++;
        entry.last_access = ++access_counter_;
        return entry.value;
      }
    }
    return std::nullopt;
  }
  
  // 添加版本到缓存
  void Put(Timestamp ts, const Descriptor& value) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    // 检查是否已存在
    for (auto& entry : entries_) {
      if (entry.timestamp == ts) {
        entry.value = value;
        entry.access_count++;
        entry.last_access = ++access_counter_;
        return;
      }
    }
    
    // 如果缓存已满，淘汰最久未访问的
    if (entries_.size() >= capacity_) {
      EvictLRU();
    }
    
    entries_.emplace_back(ts, value);
    entries_.back().last_access = ++access_counter_;
  }
  
  // 清空缓存
  void Clear() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    entries_.clear();
    access_counter_ = 0;
  }
  
  // 获取缓存命中率统计
  struct Stats {
    size_t hits;
    size_t misses;
    double hit_rate;
  };
  
  Stats GetStats() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    size_t total = cache_hits_ + cache_misses_;
    return {
        cache_hits_,
        cache_misses_,
        total > 0 ? static_cast<double>(cache_hits_) / total : 0.0
    };
  }
  
  void RecordHit() { cache_hits_++; }
  void RecordMiss() { cache_misses_++; }
  
  size_t Size() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return entries_.size();
  }
  
 private:
  void EvictLRU() {
    if (entries_.empty()) return;
    
    size_t min_idx = 0;
    uint64_t min_access = entries_[0].last_access;
    
    for (size_t i = 1; i < entries_.size(); ++i) {
      if (entries_[i].last_access < min_access) {
        min_access = entries_[i].last_access;
        min_idx = i;
      }
    }
    
    entries_.erase(entries_.begin() + min_idx);
  }
  
  size_t capacity_;
  std::vector<CacheEntry> entries_;
  mutable std::shared_mutex mutex_;
  mutable std::atomic<uint64_t> access_counter_{0};
  mutable std::atomic<size_t> cache_hits_{0};
  mutable std::atomic<size_t> cache_misses_{0};
};

// ============================================================================
// 压缩统计
// ============================================================================

/**
 * CompressionStats - 压缩效果统计
 */
struct CompressionStats {
  size_t total_original_bytes;      // 原始大小（如果存储完整值）
  size_t total_compressed_bytes;    // 实际压缩后大小
  size_t total_versions;            // 总版本数量
  size_t total_groups;              // 组数量
  size_t cache_hits;                // 缓存命中次数
  size_t cache_misses;              // 缓存未命中次数
  
  // 计算压缩率
  double CompressionRatio() const {
    if (total_original_bytes == 0) return 1.0;
    return static_cast<double>(total_compressed_bytes) / total_original_bytes;
  }
  
  // 计算节省率
  double SpaceSaving() const {
    return 1.0 - CompressionRatio();
  }
  
  // 计算平均每版本大小
  double AvgBytesPerVersion() const {
    if (total_versions == 0) return 0.0;
    return static_cast<double>(total_compressed_bytes) / total_versions;
  }
  
  // 计算缓存命中率
  double CacheHitRate() const {
    size_t total = cache_hits + cache_misses;
    if (total == 0) return 0.0;
    return static_cast<double>(cache_hits) / total;
  }
};

// ============================================================================
// Delta 版本链主类
// ============================================================================

/**
 * DeltaVersionChain - 基于增量编码的版本链
 *
 * 特性：
 * 1. 空间高效：使用 delta 编码存储版本差异
 * 2. 自动分组：超过阈值时自动创建新版本组
 * 3. 热点缓存：缓存频繁访问的版本
 * 4. 线程安全：支持并发读，互斥写
 * 5. API 兼容：与 TemporalVersionNode 链兼容的接口
 *
 * 时间复杂度：
 * - InsertVersion: O(1) 平均，O(k) 最坏（需要重建基准时）
 * - GetVersionAt: O(log G + D) 其中 G 是组数，D 是组内 delta 数
 * - GetLatest: O(1)
 */
class DeltaVersionChain {
 public:
  // 配置参数
  struct Config {
    // 每组最大 delta 数量（超过则创建新组）
    size_t max_deltas_per_group;
    
    // 每组最大字节数（超过则创建新组）
    size_t max_bytes_per_group;
    
    // 热点缓存大小
    size_t cache_capacity;
    
    // 是否启用缓存
    bool enable_cache;
    
    // Delta 编码配置
    DeltaVersionEncoder::Config encoder_config;
    
    // VersionGroup 配置
    VersionGroup::Config group_config;
    
    // 构造函数提供默认值
    Config()
        : max_deltas_per_group(16),
          max_bytes_per_group(128),
          cache_capacity(16),
          enable_cache(true) {}
  };
  
  /**
   * 构造函数
   * @param config 配置参数
   */
  explicit DeltaVersionChain(const Config& config = Config());
  
  // 禁止拷贝，允许移动
  DeltaVersionChain(const DeltaVersionChain&) = delete;
  DeltaVersionChain& operator=(const DeltaVersionChain&) = delete;
  DeltaVersionChain(DeltaVersionChain&& other) noexcept
      : config_(std::move(other.config_)),
        encoder_(std::move(other.encoder_)),
        cache_(std::move(other.cache_)),
        head_group_(other.head_group_),
        tail_group_(other.tail_group_),
        version_count_(other.version_count_.load()),
        total_original_bytes_(other.total_original_bytes_.load()),
        total_compressed_bytes_(other.total_compressed_bytes_.load()) {
    other.head_group_ = nullptr;
    other.tail_group_ = nullptr;
    other.version_count_ = 0;
  }
  DeltaVersionChain& operator=(DeltaVersionChain&& other) noexcept {
    if (this != &other) {
      FreeAllGroups();
      config_ = std::move(other.config_);
      encoder_ = std::move(other.encoder_);
      cache_ = std::move(other.cache_);
      head_group_ = other.head_group_;
      tail_group_ = other.tail_group_;
      version_count_ = other.version_count_.load();
      total_original_bytes_ = other.total_original_bytes_.load();
      total_compressed_bytes_ = other.total_compressed_bytes_.load();
      other.head_group_ = nullptr;
      other.tail_group_ = nullptr;
      other.version_count_ = 0;
    }
    return *this;
  }
  
  ~DeltaVersionChain();
  
  // ==========================================================================
  // 核心 API（与 TemporalVersionNode 链兼容）
  // ==========================================================================
  
  /**
   * 插入新版本
   *
   * 自动选择编码方式：
   * - 如果是第一个版本，作为基准存储
   * - 否则，计算与上一个版本的 delta 并存储
   * - 如果超过阈值，创建新的 VersionGroup
   *
   * @param timestamp 版本时间戳（必须递增）
   * @param value 版本值
   * @return true 如果成功插入
   */
  bool InsertVersion(Timestamp timestamp, const Descriptor& value);
  
  /**
   * 查询指定时间戳的版本
   *
   * 查找时间戳小于等于给定时间戳的最新版本 (<= ts 的最大时间戳)。
   * 这是 MVCC 快照读的核心操作。
   *
   * 算法：
   * 1. 定位包含目标时间戳的 VersionGroup
   * 2. 如果是基准版本，直接返回
   * 3. 否则，从基准版本开始重建目标版本
   *
   * @param timestamp 目标时间戳
   * @return 找到的版本值，未找到返回 nullopt
   */
  std::optional<Descriptor> GetVersionAt(Timestamp timestamp) const;
  
  /**
   * 获取最新版本
   * @return 最新版本值，如果没有版本返回 nullopt
   */
  std::optional<Descriptor> GetLatest() const;
  
  /**
   * 获取最新版本的时间戳
   */
  std::optional<Timestamp> GetLatestTimestamp() const;
  
  /**
   * 遍历版本链
   *
   * 从最新版本到最旧版本遍历，callback 返回 false 时停止。
   * 
   * @param callback 回调函数，参数为 (timestamp, descriptor)
   *                 返回 false 停止遍历
   */
  void Traverse(std::function<bool(Timestamp, const Descriptor&)> callback) const;
  
  /**
   * 获取所有版本（按时间降序）
   * @return 版本列表
   */
  std::vector<std::pair<Timestamp, Descriptor>> GetAllVersions() const;
  
  // ==========================================================================
  // 批量操作
  // ==========================================================================
  
  /**
   * 批量插入版本（更高效）
   *
   * 批量插入时：
   * - 使用第一个值作为基准
   * - 后续值计算 delta
   * - 可能一次性创建多个组
   *
   * @param versions (timestamp, descriptor) 列表（必须按时间升序排列）
   * @return 成功插入的数量
   */
  size_t BatchInsert(const std::vector<std::pair<Timestamp, Descriptor>>& versions);
  
  /**
   * 批量重建多个版本（一次性重建，共享计算）
   *
   * 适用于范围查询场景，避免重复重建中间版本。
   *
   * @param start_ts 起始时间戳
   * @param end_ts 结束时间戳
   * @return 时间戳范围内的所有版本
   */
  std::vector<std::pair<Timestamp, Descriptor>> RebuildRange(
      Timestamp start_ts, Timestamp end_ts) const;
  
  // ==========================================================================
  // 统计与信息
  // ==========================================================================
  
  /**
   * 获取版本总数
   */
  size_t GetVersionCount() const {
    return version_count_.load(std::memory_order_relaxed);
  }
  
  /**
   * 获取组数量
   */
  size_t GetGroupCount() const;
  
  /**
   * 获取压缩统计信息
   */
  CompressionStats GetCompressionStats() const;
  
  /**
   * 获取缓存统计
   */
  VersionCache::Stats GetCacheStats() const {
    return cache_.GetStats();
  }
  
  // ==========================================================================
  // 配置管理
  // ==========================================================================
  
  const Config& GetConfig() const { return config_; }
  void SetConfig(const Config& config) { config_ = config; }
  
  // ==========================================================================
  // 高级接口
  // ==========================================================================
  
  /**
   * 获取包含指定时间戳的 VersionGroup
   * @param ts 目标时间戳
   * @return 指向组的指针，未找到返回 nullptr
   */
  CompressedVersionNode* FindGroupContaining(Timestamp ts) const;
  
  /**
   * 从指定组重建特定版本
   *
   * @param group 目标组
   * @param timestamp 目标时间戳
   * @return 重建的版本值
   */
  std::optional<Descriptor> RebuildFromGroup(
      CompressedVersionNode* group, Timestamp timestamp) const;
  
  /**
   * 清空所有版本
   */
  void Clear();
  
  /**
   * 预分配组（优化批量插入）
   * @param estimated_versions 预估版本数
   */
  void Reserve(size_t estimated_versions);

 private:
  // ==========================================================================
  // 内部辅助方法
  // ==========================================================================
  
  /**
   * 找到最新（时间戳最大）的组
   */
  CompressedVersionNode* FindLatestGroup() const;
  
  /**
   * 创建新的版本组
   * @param timestamp 基准时间戳
   * @param value 基准值
   * @return 新创建的组
   */
  CompressedVersionNode* CreateNewGroup(Timestamp timestamp, const Descriptor& value);
  
  /**
   * 将新版本添加到现有组
   * @param group 目标组
   * @param timestamp 版本时间戳
   * @param value 版本值
   * @return 是否成功（时间戳必须递增）
   */
  bool AddToGroup(CompressedVersionNode* group, Timestamp timestamp,
                  const Descriptor& value);
  
  /**
   * 检查是否应该创建新组
   * @param group 当前组
   * @return true 如果应该创建新组
   */
  bool ShouldCreateNewGroup(const CompressedVersionNode* group) const;
  
  /**
   * 释放所有组节点
   */
  void FreeAllGroups();
  
  /**
   * 更新全局统计
   */
  void UpdateStats(CompressedVersionNode* group, size_t original, size_t compressed);
  
  // ==========================================================================
  // 成员变量
  // ==========================================================================
  
  Config config_;
  
  // Delta 编码器
  mutable DeltaVersionEncoder encoder_;
  
  // 热点缓存
  mutable VersionCache cache_;
  
  // 组链头节点（最新组）
  CompressedVersionNode* head_group_;
  
  // 组链尾节点（最旧组）
  CompressedVersionNode* tail_group_;
  
  // 版本计数
  std::atomic<size_t> version_count_{0};
  
  // 读写锁（支持并发读）
  mutable std::shared_mutex mutex_;
  
  // 全局统计
  mutable std::atomic<size_t> total_original_bytes_{0};
  mutable std::atomic<size_t> total_compressed_bytes_{0};
};

// ============================================================================
// 内联实现
// ============================================================================

// 构造函数
inline DeltaVersionChain::DeltaVersionChain(const Config& config)
    : config_(config),
      encoder_(config.encoder_config),
      cache_(config.cache_capacity),
      head_group_(nullptr),
      tail_group_(nullptr) {}

// 析构函数
inline DeltaVersionChain::~DeltaVersionChain() {
  FreeAllGroups();
}

// 释放所有组
inline void DeltaVersionChain::FreeAllGroups() {
  CompressedVersionNode* current = head_group_;
  while (current) {
    CompressedVersionNode* next = current->next_group;
    delete current;
    current = next;
  }
  head_group_ = nullptr;
  tail_group_ = nullptr;
  version_count_ = 0;
}

// 清空
inline void DeltaVersionChain::Clear() {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  FreeAllGroups();
  cache_.Clear();
  total_original_bytes_ = 0;
  total_compressed_bytes_ = 0;
}

// 找到最新组
inline CompressedVersionNode* DeltaVersionChain::FindLatestGroup() const {
  return head_group_;
}

// 检查是否应该创建新组
inline bool DeltaVersionChain::ShouldCreateNewGroup(
    const CompressedVersionNode* group) const {
  if (!group) return true;
  
  // 检查 delta 数量
  if (group->deltas.size() >= config_.max_deltas_per_group) {
    return true;
  }
  
  // 检查累计大小
  size_t total_delta_size = 0;
  for (const auto& delta : group->deltas) {
    total_delta_size += delta.TotalSize();
  }
  if (total_delta_size >= config_.max_bytes_per_group) {
    return true;
  }
  
  return false;
}

// 创建新组
inline CompressedVersionNode* DeltaVersionChain::CreateNewGroup(
    Timestamp timestamp, const Descriptor& value) {
  auto* group = new CompressedVersionNode(timestamp, value, config_.group_config);
  
  if (!head_group_) {
    // 第一个组
    head_group_ = group;
    tail_group_ = group;
  } else {
    // 添加到链表头部（新组是更新的）
    group->prev_group = head_group_;
    head_group_->next_group = group;
    head_group_ = group;
  }
  
  version_count_.fetch_add(1, std::memory_order_relaxed);
  total_original_bytes_.fetch_add(8, std::memory_order_relaxed);
  total_compressed_bytes_.fetch_add(8, std::memory_order_relaxed);
  
  return group;
}

// 更新统计
inline void DeltaVersionChain::UpdateStats(CompressedVersionNode* group,
                                           size_t original, size_t compressed) {
  group->UpdateStats(original, compressed);
  total_original_bytes_.fetch_add(original, std::memory_order_relaxed);
  total_compressed_bytes_.fetch_add(compressed, std::memory_order_relaxed);
}

// 插入新版本
inline bool DeltaVersionChain::InsertVersion(Timestamp timestamp,
                                              const Descriptor& value) {
  std::unique_lock<std::shared_mutex> lock(mutex_);
  
  // 检查时间戳有效性
  if (head_group_) {
    auto latest_ts = head_group_->GetLatestTimestamp();
    if (timestamp <= latest_ts) {
      // 时间戳必须递增
      return false;
    }
  }
  
  // 如果没有组或需要创建新组，创建新组
  if (!head_group_ || ShouldCreateNewGroup(head_group_)) {
    CreateNewGroup(timestamp, value);
    return true;
  }
  
  // 添加到当前组
  return AddToGroup(head_group_, timestamp, value);
}

// 添加到组
inline bool DeltaVersionChain::AddToGroup(CompressedVersionNode* group,
                                          Timestamp timestamp,
                                          const Descriptor& value) {
  // 编码 delta
  DeltaEntry delta = encoder_.Encode(group->base_value, value);
  
  // 使用增量编码时，需要基于上一个版本（而不是基准）
  if (!group->deltas.empty()) {
    // 获取上一个版本（最后一个 delta 对应的值）
    auto prev = encoder_.Rebuild(group->base_value, group->deltas,
                                  group->deltas.size() - 1);
    delta = encoder_.Encode(prev, value);
  }
  
  delta.timestamp = timestamp;
  group->deltas.push_back(std::move(delta));
  group->version_count++;
  
  // 更新统计（假设完整存储需要 8 字节）
  UpdateStats(group, 8, delta.TotalSize());
  
  version_count_.fetch_add(1, std::memory_order_relaxed);
  
  return true;
}

// 查找包含时间戳的组
inline CompressedVersionNode* DeltaVersionChain::FindGroupContaining(
    Timestamp ts) const {
  CompressedVersionNode* current = head_group_;
  
  while (current) {
    auto [min_ts, max_ts] = current->GetTimestampRange();
    if (ts >= min_ts && ts <= max_ts) {
      return current;
    }
    // 如果当前组的最小时间戳已经大于目标，往前找
    if (min_ts > ts) {
      current = current->prev_group;
    } else {
      // 如果当前组的最大时间戳已经小于目标，说明目标不存在
      // （因为组之间是连续的，没有空隙）
      break;
    }
  }
  
  return nullptr;
}

// 从组重建版本
inline std::optional<Descriptor> DeltaVersionChain::RebuildFromGroup(
    CompressedVersionNode* group, Timestamp timestamp) const {
  if (!group) return std::nullopt;
  
  // 检查是否是基准版本
  if (timestamp == group->base_timestamp) {
    return group->base_value;
  }
  
  // 在 deltas 中查找
  for (size_t i = 0; i < group->deltas.size(); ++i) {
    if (group->deltas[i].timestamp == timestamp) {
      // 重建到该位置
      return encoder_.Rebuild(group->base_value, group->deltas, i);
    }
  }
  
  return std::nullopt;
}

// 获取指定时间戳的版本
inline std::optional<Descriptor> DeltaVersionChain::GetVersionAt(
    Timestamp timestamp) const {
  // 首先检查缓存
  if (config_.enable_cache) {
    auto cached = cache_.Get(timestamp);
    if (cached.has_value()) {
      cache_.RecordHit();
      return cached;
    }
    cache_.RecordMiss();
  }
  
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  // 找到包含目标时间戳的组
  CompressedVersionNode* group = FindGroupContaining(timestamp);
  if (!group) {
    // 如果没找到精确匹配的组，尝试找到不超过目标时间戳的最新版本
    // 这是 MVCC 的标准语义：<= ts 的最新版本
    CompressedVersionNode* current = head_group_;
    while (current) {
      auto [min_ts, max_ts] = current->GetTimestampRange();
      if (min_ts <= timestamp) {
        // 该组包含 <= timestamp 的版本
        group = current;
        break;
      }
      current = current->prev_group;
    }
  }
  
  if (!group) {
    return std::nullopt;
  }
  
  // 重建版本
  auto result = RebuildFromGroup(group, timestamp);
  
  // 如果精确匹配失败，返回该组最新版本（如果 <= timestamp）
  if (!result.has_value()) {
    auto latest_ts = group->GetLatestTimestamp();
    if (latest_ts <= timestamp) {
      result = RebuildFromGroup(group, latest_ts);
    }
  }
  
  // 缓存结果
  if (result.has_value() && config_.enable_cache) {
    cache_.Put(timestamp, result.value());
  }
  
  return result;
}

// 获取最新版本
inline std::optional<Descriptor> DeltaVersionChain::GetLatest() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  if (!head_group_) {
    return std::nullopt;
  }
  
  // 获取最新组的最新版本
  auto latest_ts = head_group_->GetLatestTimestamp();
  return RebuildFromGroup(head_group_, latest_ts);
}

// 获取最新时间戳
inline std::optional<Timestamp> DeltaVersionChain::GetLatestTimestamp() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  if (!head_group_) {
    return std::nullopt;
  }
  
  return head_group_->GetLatestTimestamp();
}

// 遍历版本链
inline void DeltaVersionChain::Traverse(
    std::function<bool(Timestamp, const Descriptor&)> callback) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  CompressedVersionNode* group = head_group_;
  
  while (group) {
    // 从最新到最旧遍历当前组的版本
    // 先处理 deltas（从后往前）
    for (int i = static_cast<int>(group->deltas.size()) - 1; i >= 0; --i) {
      auto value = encoder_.Rebuild(group->base_value, group->deltas, i);
      if (!callback(group->deltas[i].timestamp, value)) {
        return;
      }
    }
    
    // 处理基准版本
    if (!callback(group->base_timestamp, group->base_value)) {
      return;
    }
    
    group = group->prev_group;
  }
}

// 获取所有版本
inline std::vector<std::pair<Timestamp, Descriptor>>
DeltaVersionChain::GetAllVersions() const {
  std::vector<std::pair<Timestamp, Descriptor>> result;
  result.reserve(GetVersionCount());
  
  Traverse([&](Timestamp ts, const Descriptor& desc) {
    result.emplace_back(ts, desc);
    return true;
  });
  
  return result;
}

// 批量插入
inline size_t DeltaVersionChain::BatchInsert(
    const std::vector<std::pair<Timestamp, Descriptor>>& versions) {
  if (versions.empty()) return 0;
  
  std::unique_lock<std::shared_mutex> lock(mutex_);
  
  size_t inserted = 0;
  CompressedVersionNode* current_group = head_group_;
  
  for (const auto& [timestamp, value] : versions) {
    // 检查时间戳递增
    if (current_group) {
      auto latest_ts = current_group->GetLatestTimestamp();
      if (timestamp <= latest_ts) {
        continue;  // 跳过无效时间戳
      }
    }
    
    // 检查是否需要创建新组
    if (!current_group || ShouldCreateNewGroup(current_group)) {
      current_group = CreateNewGroup(timestamp, value);
    } else {
      AddToGroup(current_group, timestamp, value);
    }
    
    inserted++;
  }
  
  return inserted;
}

// 批量重建范围
inline std::vector<std::pair<Timestamp, Descriptor>>
DeltaVersionChain::RebuildRange(Timestamp start_ts, Timestamp end_ts) const {
  std::vector<std::pair<Timestamp, Descriptor>> result;
  
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  // 找到起始组
  CompressedVersionNode* group = FindGroupContaining(start_ts);
  if (!group) {
    group = head_group_;
    while (group && group->GetTimestampRange().first > start_ts) {
      group = group->prev_group;
    }
  }
  
  // 遍历所有相关组
  while (group) {
    auto [min_ts, max_ts] = group->GetTimestampRange();
    
    // 如果当前组完全在范围之后，跳过
    if (min_ts > end_ts) {
      group = group->prev_group;
      continue;
    }
    
    // 如果当前组完全在范围之前，结束
    if (max_ts < start_ts) {
      break;
    }
    
    // 收集该组在范围内的版本
    // 从基准版本开始，逐个重建并检查
    Descriptor current = group->base_value;
    if (group->base_timestamp >= start_ts && group->base_timestamp <= end_ts) {
      result.emplace_back(group->base_timestamp, current);
    }
    
    for (const auto& delta : group->deltas) {
      current = encoder_.Decode(current, delta);
      if (delta.timestamp >= start_ts && delta.timestamp <= end_ts) {
        result.emplace_back(delta.timestamp, current);
      }
    }
    
    group = group->prev_group;
  }
  
  // 按时间戳降序排序
  std::sort(result.begin(), result.end(),
            [](const auto& a, const auto& b) { return a.first > b.first; });
  
  return result;
}

// 获取组数量
inline size_t DeltaVersionChain::GetGroupCount() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  size_t count = 0;
  CompressedVersionNode* current = head_group_;
  while (current) {
    count++;
    current = current->prev_group;
  }
  return count;
}

// 获取压缩统计
inline CompressionStats DeltaVersionChain::GetCompressionStats() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  CompressionStats stats;
  stats.total_original_bytes = total_original_bytes_.load(std::memory_order_relaxed);
  stats.total_compressed_bytes = total_compressed_bytes_.load(std::memory_order_relaxed);
  stats.total_versions = version_count_.load(std::memory_order_relaxed);
  stats.total_groups = GetGroupCount();
  
  auto cache_stats = cache_.GetStats();
  stats.cache_hits = cache_stats.hits;
  stats.cache_misses = cache_stats.misses;
  
  return stats;
}

// 预分配
inline void DeltaVersionChain::Reserve(size_t estimated_versions) {
  // 估算需要的组数
  size_t estimated_groups = (estimated_versions + config_.max_deltas_per_group - 1) 
                          / config_.max_deltas_per_group;
  
  // 实际预分配可以在这里实现内存池
  // 目前仅作为接口保留
  (void)estimated_groups;
}

}  // namespace cedar

#endif  // FERN_STORAGE_DELTA_VERSION_CHAIN_H_
