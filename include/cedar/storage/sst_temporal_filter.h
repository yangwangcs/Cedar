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

#ifndef FERN_STORAGE_SST_TEMPORAL_FILTER_H_
#define FERN_STORAGE_SST_TEMPORAL_FILTER_H_

#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "cedar/core/slice.h"
#include "cedar/core/status.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/temporal_bloom_filter.h"
#include "cedar/types/cedar_key.h"

namespace cedar {

// ============================================================================
// 前置声明
// ============================================================================
class TemporalBloomFilter;
class LsmEngine;

// ============================================================================
// Frond 文件元数据扩展 - 集成时间范围布隆过滤器
// ============================================================================
//
// 在 SstFileMeta 基础上增加时间过滤器支持，实现 Frond 文件快速跳过。
//
// 设计要点:
// - temporal_filter_metadata: 序列化的过滤器数据，存储在 Frond 文件中
// - cached_filter: 懒加载的过滤器对象，避免重复反序列化
// - 线程安全：cached_filter 的访问需要外部同步或使用 GetFilter() 的互斥保护
//
struct SstFileMetaOptimized : public SstFileMeta {
  // 序列化的过滤器元数据（存储在 Frond 文件 footer 中）
  std::string temporal_filter_metadata;

  // 缓存的反序列化过滤器对象（mutable 支持懒加载）
  mutable std::unique_ptr<TemporalBloomFilter> cached_filter;

  // 过滤器加载状态标志
  mutable std::atomic<bool> filter_loaded_{false};

  // 保护缓存加载的互斥锁
  mutable std::mutex filter_mutex_;

  // 构造函数
  SstFileMetaOptimized() = default;

  // 从基础 SstFileMeta 构造
  explicit SstFileMetaOptimized(const SstFileMeta& base)
      : SstFileMeta(base),
        temporal_filter_metadata(),
        cached_filter(nullptr),
        filter_loaded_(false) {}

  // 禁止拷贝（避免过滤器重复）
  SstFileMetaOptimized(const SstFileMetaOptimized&) = delete;
  SstFileMetaOptimized& operator=(const SstFileMetaOptimized&) = delete;

  // 允许移动
  SstFileMetaOptimized(SstFileMetaOptimized&& other) noexcept
      : SstFileMeta(other),
        temporal_filter_metadata(std::move(other.temporal_filter_metadata)),
        cached_filter(std::move(other.cached_filter)),
        filter_loaded_(other.filter_loaded_.load()) {}

  SstFileMetaOptimized& operator=(SstFileMetaOptimized&& other) noexcept {
    if (this != &other) {
      SstFileMeta::operator=(other);
      temporal_filter_metadata = std::move(other.temporal_filter_metadata);
      cached_filter = std::move(other.cached_filter);
      filter_loaded_.store(other.filter_loaded_.load());
    }
    return *this;
  }

  // -------------------------------------------------------------------------
  // 懒加载获取过滤器
  //
  // 线程安全的懒加载：如果缓存未加载，从 metadata 反序列化
  // 返回 nullptr 如果 metadata 为空或反序列化失败
  //
  TemporalBloomFilter* GetFilter() const {
    // 快速路径：已加载
    if (filter_loaded_.load(std::memory_order_acquire)) {
      return cached_filter.get();
    }

    // 慢速路径：需要加载
    std::lock_guard<std::mutex> lock(filter_mutex_);
    // 双重检查
    if (filter_loaded_.load(std::memory_order_relaxed)) {
      return cached_filter.get();
    }

    // 反序列化过滤器
    if (!temporal_filter_metadata.empty()) {
      auto filter = TemporalBloomFilter::Deserialize(temporal_filter_metadata);
      if (filter) {
        cached_filter = std::make_unique<TemporalBloomFilter>(std::move(*filter));
      }
    }

    filter_loaded_.store(true, std::memory_order_release);
    return cached_filter.get();
  }

  // -------------------------------------------------------------------------
  // 检查时间范围是否可能包含指定实体
  //
  // 这是 Frond 文件跳过的核心判断逻辑：
  // - 如果时间范围不重叠，直接返回 false
  // - 如果无过滤器，保守返回 true（需要查 Frond）
  // - 使用过滤器判断 entity_id 在 [start, end] 时间范围内是否可能存在
  //
  // @return true: 可能包含（需要查 Frond）
  // @return false: 一定不包含（可以跳过 Frond）
  //
  bool MayContainRange(uint64_t entity_id, Timestamp start, Timestamp end) const {
    // 快速路径：时间范围完全不重叠
    uint64_t frond_start = min_tx_time;
    uint64_t frond_end = max_tx_time;
    uint64_t query_start = start.value();
    uint64_t query_end = end.value();

    // 查询时间完全在 Frond 时间范围之前或之后
    if (query_end < frond_start || query_start > frond_end) {
      return false;
    }

    // 查询实体 ID 完全在 Frond 实体范围之外
    if (entity_id < min_entity_id || entity_id > max_entity_id) {
      return false;
    }

    // 获取过滤器进行精确判断
    auto* filter = GetFilter();
    if (!filter) {
      // 无过滤器，保守返回 true
      return true;
    }

    return filter->MayExistInRange(entity_id, start, end);
  }

  // -------------------------------------------------------------------------
  // 重置缓存（用于 Compact 后刷新）
  void ResetCache() const {
    std::lock_guard<std::mutex> lock(filter_mutex_);
    cached_filter.reset();
    filter_loaded_.store(false, std::memory_order_release);
  }

  // -------------------------------------------------------------------------
  // 检查是否有过滤器元数据
  bool HasFilterMetadata() const { return !temporal_filter_metadata.empty(); }

  // -------------------------------------------------------------------------
  // 获取过滤器内存使用量（包含缓存）
  size_t FilterMemoryUsage() const {
    size_t metadata_size = temporal_filter_metadata.size();
    size_t cache_size = 0;
    if (cached_filter) {
      cache_size = cached_filter->MemoryUsage();
    }
    return metadata_size + cache_size;
  }
};

// ============================================================================
// Frond 文件时间范围过滤器管理工具类
// ============================================================================
//
// 提供创建、保存、加载 TemporalBloomFilter 的静态工具方法。
// 在 Flush、Compact、Query 流程中集成使用。
//
class SstTemporalFilter {
 public:
  // 默认过滤器配置
  static TemporalBloomFilter::Config DefaultConfig() {
    TemporalBloomFilter::Config config;
    config.false_positive_rate = 0.01;  // 1% 假阳性率
    config.expected_keys = 100000;       // 预期 10 万条记录
    config.hours_per_bucket = 1;         // 1 小时一个桶
    return config;
  }

  // -------------------------------------------------------------------------
  // 创建 Frond 文件的过滤器
  //
  // 在 Flush MemTable 到 SST 时调用，根据所有键生成时间范围过滤器。
  //
  // @param keys: Frond 文件中的所有键（已排序或无序）
  // @param file_start_time: Frond 文件中数据的最小时间戳
  // @param file_end_time: Frond 文件中数据的最大时间戳
  // @param config: 过滤器配置（可选，使用默认配置）
  // @return 创建的过滤器智能指针，失败返回 nullptr
  //
  static std::unique_ptr<TemporalBloomFilter> CreateFilter(
      const std::vector<CedarKey>& keys,
      Timestamp file_start_time,
      Timestamp file_end_time,
      const TemporalBloomFilter::Config& config = DefaultConfig()) {
    if (keys.empty()) {
      return nullptr;
    }

    auto filter = std::make_unique<TemporalBloomFilter>(
        file_start_time, file_end_time, config);

    for (const auto& key : keys) {
      filter->Add(key);
    }

    return filter;
  }

  // 从 MemTableEntry 创建过滤器（兼容 LsmEngine 内部格式）
  template <typename Iterator>
  static std::unique_ptr<TemporalBloomFilter> CreateFilterFromEntries(
      Iterator begin,
      Iterator end,
      Timestamp file_start_time,
      Timestamp file_end_time,
      const TemporalBloomFilter::Config& config = DefaultConfig()) {
    auto filter = std::make_unique<TemporalBloomFilter>(
        file_start_time, file_end_time, config);

    for (auto it = begin; it != end; ++it) {
      // 假设迭代器指向 MemTableEntry 或类似结构
      filter->Add(it->entity_id, Timestamp(it->tx_time));
    }

    return filter;
  }

  // -------------------------------------------------------------------------
  // 保存过滤器到 SST 元数据
  //
  // 在 Frond 文件写入时调用，将序列化后的过滤器存入元数据。
  // 建议存储在 Frond 文件的 footer 中。
  //
  // @param filter: 要保存的过滤器
  // @return 序列化的元数据字符串
  //
  static std::string SaveToMetadata(const TemporalBloomFilter& filter) {
    return filter.Serialize();
  }

  // -------------------------------------------------------------------------
  // 从 Frond 元数据加载过滤器
  //
  // 在 Frond 文件加载时调用，从元数据反序列化过滤器。
  //
  // @param metadata: 从 Frond 文件读取的序列化元数据
  // @return 加载的过滤器智能指针，失败返回 nullptr
  //
  static std::unique_ptr<TemporalBloomFilter> LoadFromMetadata(
      const std::string& metadata) {
    auto result = TemporalBloomFilter::Deserialize(metadata);
    if (!result) {
      return nullptr;
    }
    return std::make_unique<TemporalBloomFilter>(std::move(*result));
  }

  // -------------------------------------------------------------------------
  // 判断 Frond 文件是否可能包含指定实体的时间范围
  //
  // 静态工具方法，用于查询时快速判断是否需要访问 Frond 文件。
  //
  // @param filter: 过滤器指针（可能为 nullptr）
  // @param entity_id: 实体 ID
  // @param query_start: 查询起始时间（包含）
  // @param query_end: 查询结束时间（包含）
  // @return true: 可能有数据（需要查 Frond）
  // @return false: 一定无数据（可以跳过 Frond）
  //
  static bool MayContainRange(
      const TemporalBloomFilter* filter,
      uint64_t entity_id,
      Timestamp query_start,
      Timestamp query_end) {
    if (!filter) {
      // 无过滤器，保守返回 true
      return true;
    }
    return filter->MayExistInRange(entity_id, query_start, query_end);
  }

  // -------------------------------------------------------------------------
  // 合并多个过滤器（用于 Compact）
  //
  // 在 Compact 多个 Frond 文件时，合并它们的过滤器生成新的过滤器。
  // 注意：由于布隆过滤器的特性，合并后假阳性率会上升，建议重新生成。
  //
  // @param filters: 要合并的过滤器列表
  // @param new_start_time: 合并后的起始时间
  // @param new_end_time: 合并后的结束时间
  // @param total_keys: 合并后的总键数（用于重新计算过滤器大小）
  // @return 合并后的新过滤器
  //
  static std::unique_ptr<TemporalBloomFilter> MergeFilters(
      const std::vector<const TemporalBloomFilter*>& filters,
      Timestamp new_start_time,
      Timestamp new_end_time,
      size_t total_keys,
      const TemporalBloomFilter::Config& config = DefaultConfig()) {
    // 布隆过滤器不支持直接合并，这里重新创建一个新的
    // 实际 Compact 过程中应该从输入文件读取所有键重新生成
    (void)filters;  // 暂时未使用，保留接口
    (void)total_keys;

    auto new_config = config;
    new_config.expected_keys = total_keys;

    return std::make_unique<TemporalBloomFilter>(
        new_start_time, new_end_time, new_config);
  }

  // -------------------------------------------------------------------------
  // 推荐过滤器配置
  //
  // 根据 Frond 文件的特性（大小、时间跨度、键数量）推荐最佳配置。
  //
  // @param duration_hours: Frond 文件时间跨度（小时）
  // @param num_keys: 键数量
  // @param file_size_mb: 文件大小（MB）
  // @return 推荐的配置
  //
  static TemporalBloomFilter::Config RecommendConfig(
      uint64_t duration_hours,
      size_t num_keys,
      double file_size_mb = 0.0) {
    TemporalBloomFilter::Config config;

    // 根据时间跨度选择桶大小
    if (duration_hours <= 24) {
      // 短时间跨度 (< 1天): 1小时桶
      config.hours_per_bucket = 1;
    } else if (duration_hours <= 168) {  // 7 天
      // 中等时间跨度 (1-7天): 4小时桶
      config.hours_per_bucket = 4;
    } else {
      // 长时间跨度 (> 7天): 12小时桶
      config.hours_per_bucket = 12;
    }

    // 根据键数量调整假阳性率
    // 大文件使用更低的 FPR，因为跳过的收益更大
    if (file_size_mb > 100) {
      config.false_positive_rate = 0.005;  // 0.5%
    } else if (file_size_mb > 10) {
      config.false_positive_rate = 0.01;   // 1%
    } else {
      config.false_positive_rate = 0.02;   // 2%（小文件，FPR 影响小）
    }

    config.expected_keys = std::max(num_keys, size_t{1000});

    return config;
  }

  // -------------------------------------------------------------------------
  // 估算过滤器大小
  //
  // 用于 Frond 文件布局规划，预估 footer 大小。
  //
  static size_t EstimateFilterSize(
      uint64_t duration_hours,
      size_t num_keys,
      double false_positive_rate = 0.01,
      uint32_t hours_per_bucket = 1) {
    return SSTTemporalBloomFilter::EstimateFilterSize(
        duration_hours, num_keys, false_positive_rate, hours_per_bucket);
  }
};

// ============================================================================
// Frond 文件选择器 - 基于时间过滤器的查询优化
// ============================================================================
//
// 在查询时根据时间和实体范围选择需要访问的 Frond 文件，
// 跳过确定不包含目标数据的文件，大幅减少磁盘 I/O。
//
// 预期优化效果:
// - 随机查询：跳过 90%+ 的 Frond 文件
// - 减少磁盘 I/O：90%+
//
class SstFileSelector {
 public:
  // 查询统计信息
  struct Stats {
    size_t total_files = 0;      // 总 Frond 文件数
    size_t skipped_files = 0;    // 跳过的文件数
    size_t checked_files = 0;    // 需要检查的文件数
    double skip_rate = 0.0;      // 跳过率

    void Update() {
      if (total_files > 0) {
        skip_rate = static_cast<double>(skipped_files) / total_files;
      }
    }
  };

  // -------------------------------------------------------------------------
  // 基于实体 ID 和时间范围选择 Frond 文件
  //
  // 核心查询优化函数，返回可能需要访问的文件列表。
  //
  // @param files: 所有候选 Frond 文件（某一层级或全部）
  // @param entity_id: 查询的实体 ID
  // @param start: 查询起始时间
  // @param end: 查询结束时间
  // @param stats: 输出统计信息（可选）
  // @return 需要访问的文件索引列表
  //
  static std::vector<size_t> SelectFilesForQuery(
      const std::vector<SstFileMetaOptimized>& files,
      uint64_t entity_id,
      Timestamp start,
      Timestamp end,
      Stats* stats = nullptr) {
    std::vector<size_t> result;
    result.reserve(files.size() / 4);  // 预分配，假设 75% 被跳过

    Stats local_stats;
    local_stats.total_files = files.size();

    for (size_t i = 0; i < files.size(); ++i) {
      const auto& file = files[i];

      // 快速检查：实体范围
      if (entity_id < file.min_entity_id || entity_id > file.max_entity_id) {
        ++local_stats.skipped_files;
        continue;
      }

      // 快速检查：时间范围
      if (static_cast<uint64_t>(end.value()) < file.min_tx_time ||
          static_cast<uint64_t>(start.value()) > file.max_tx_time) {
        ++local_stats.skipped_files;
        continue;
      }

      // 过滤器检查：时间范围过滤器
      if (!file.MayContainRange(entity_id, start, end)) {
        ++local_stats.skipped_files;
        continue;
      }

      // 需要通过所有检查
      result.push_back(i);
      ++local_stats.checked_files;
    }

    local_stats.Update();

    if (stats) {
      *stats = local_stats;
    }

    return result;
  }

  // -------------------------------------------------------------------------
  // 批量选择：多个实体的时间范围查询
  //
  // 用于批量查询场景，返回覆盖所有实体的文件集合。
  //
  static std::vector<size_t> SelectFilesForBatchQuery(
      const std::vector<SstFileMetaOptimized>& files,
      const std::vector<uint64_t>& entity_ids,
      Timestamp start,
      Timestamp end) {
    std::vector<bool> file_needed(files.size(), false);

    for (uint64_t entity_id : entity_ids) {
      for (size_t i = 0; i < files.size(); ++i) {
        if (file_needed[i]) {
          continue;  // 已标记需要访问
        }

        const auto& file = files[i];

        // 实体范围检查
        if (entity_id < file.min_entity_id || entity_id > file.max_entity_id) {
          continue;
        }

        // 时间范围检查
        if (static_cast<uint64_t>(end.value()) < file.min_tx_time ||
            static_cast<uint64_t>(start.value()) > file.max_tx_time) {
          continue;
        }

        // 过滤器检查
        if (file.MayContainRange(entity_id, start, end)) {
          file_needed[i] = true;
        }
      }
    }

    std::vector<size_t> result;
    for (size_t i = 0; i < files.size(); ++i) {
      if (file_needed[i]) {
        result.push_back(i);
      }
    }

    return result;
  }

  // -------------------------------------------------------------------------
  // 全表扫描优化选择
  //
  // 对于全表扫描或大范围查询，只基于时间范围选择文件。
  //
  static std::vector<size_t> SelectFilesForTimeRange(
      const std::vector<SstFileMetaOptimized>& files,
      Timestamp start,
      Timestamp end,
      Stats* stats = nullptr) {
    std::vector<size_t> result;
    result.reserve(files.size());

    Stats local_stats;
    local_stats.total_files = files.size();

    for (size_t i = 0; i < files.size(); ++i) {
      const auto& file = files[i];

      // 只检查时间范围
      if (static_cast<uint64_t>(end.value()) < file.min_tx_time ||
          static_cast<uint64_t>(start.value()) > file.max_tx_time) {
        ++local_stats.skipped_files;
        continue;
      }

      result.push_back(i);
      ++local_stats.checked_files;
    }

    local_stats.Update();

    if (stats) {
      *stats = local_stats;
    }

    return result;
  }
};

// ============================================================================
// 时间过滤器缓存管理器
// ============================================================================
//
// 管理 Frond 文件过滤器缓存的 LRU 缓存，控制内存使用。
// 在内存受限环境下，优先缓存热数据的过滤器。
//
class TemporalFilterCache {
 public:
  struct CacheEntry {
    uint64_t file_number;
    std::unique_ptr<TemporalBloomFilter> filter;
    size_t last_access;
    size_t access_count;
  };

  explicit TemporalFilterCache(size_t max_memory_bytes = 64 * 1024 * 1024)
      : max_memory_bytes_(max_memory_bytes),
        current_memory_bytes_(0),
        access_counter_(0) {}

  // 获取或加载过滤器
  TemporalBloomFilter* GetOrLoad(
      uint64_t file_number,
      const std::string& metadata,
      std::function<std::string(uint64_t)> metadata_loader = nullptr) {
    std::shared_lock<std::shared_mutex> read_lock(mutex_);

    auto it = cache_.find(file_number);
    if (it != cache_.end()) {
      // 缓存命中
      it->second.last_access = ++access_counter_;
      it->second.access_count++;
      return it->second.filter.get();
    }

    read_lock.unlock();
    std::unique_lock<std::shared_mutex> write_lock(mutex_);

    // 双重检查
    it = cache_.find(file_number);
    if (it != cache_.end()) {
      it->second.last_access = ++access_counter_;
      it->second.access_count++;
      return it->second.filter.get();
    }

    // 加载过滤器
    std::string filter_metadata = metadata;
    if (filter_metadata.empty() && metadata_loader) {
      filter_metadata = metadata_loader(file_number);
    }

    if (filter_metadata.empty()) {
      return nullptr;
    }

    auto filter = SstTemporalFilter::LoadFromMetadata(filter_metadata);
    if (!filter) {
      return nullptr;
    }

    // 检查内存限制并驱逐
    size_t filter_size = filter->MemoryUsage();
    while (current_memory_bytes_ + filter_size > max_memory_bytes_ &&
           !cache_.empty()) {
      EvictLRU();
    }

    // 插入缓存
    auto* filter_ptr = filter.get();
    CacheEntry entry;
    entry.file_number = file_number;
    entry.filter = std::move(filter);
    entry.last_access = ++access_counter_;
    entry.access_count = 1;

    cache_[file_number] = std::move(entry);
    current_memory_bytes_ += filter_size;

    return filter_ptr;
  }

  // 使缓存失效
  void Invalidate(uint64_t file_number) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    auto it = cache_.find(file_number);
    if (it != cache_.end() && it->second.filter) {
      current_memory_bytes_ -= it->second.filter->MemoryUsage();
      cache_.erase(it);
    }
  }

  // 清空缓存
  void Clear() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    cache_.clear();
    current_memory_bytes_ = 0;
  }

  // 获取统计信息
  struct CacheStats {
    size_t num_entries;
    size_t memory_usage;
    size_t max_memory;
    double utilization;
  };

  CacheStats GetStats() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    CacheStats stats;
    stats.num_entries = cache_.size();
    stats.memory_usage = current_memory_bytes_;
    stats.max_memory = max_memory_bytes_;
    stats.utilization = static_cast<double>(current_memory_bytes_) /
                        max_memory_bytes_;
    return stats;
  }

 private:
  void EvictLRU() {
    if (cache_.empty()) return;

    uint64_t lru_file = 0;
    size_t min_access = std::numeric_limits<size_t>::max();

    for (const auto& [file_num, entry] : cache_) {
      if (entry.last_access < min_access) {
        min_access = entry.last_access;
        lru_file = file_num;
      }
    }

    auto it = cache_.find(lru_file);
    if (it != cache_.end() && it->second.filter) {
      current_memory_bytes_ -= it->second.filter->MemoryUsage();
      cache_.erase(it);
    }
  }

  mutable std::shared_mutex mutex_;
  std::unordered_map<uint64_t, CacheEntry> cache_;
  size_t max_memory_bytes_;
  size_t current_memory_bytes_;
  std::atomic<size_t> access_counter_;
};

// ============================================================================
// LsmEngine 集成扩展接口
// ============================================================================
//
// 提供与 LsmEngine 集成的辅助类，建议在 LsmEngine 内部使用这些接口。
//
class LsmEngineTemporalFilterExt {
 public:
  // 在 Flush 时创建过滤器并附加到 SST 元数据
  static void AttachFilterToSstMeta(
      SstFileMetaOptimized* meta,
      const std::vector<CedarKey>& keys) {
    if (!meta || keys.empty()) {
      return;
    }

    Timestamp start(meta->min_tx_time);
    Timestamp end(meta->max_tx_time);

    // 推荐配置
    uint64_t duration_hours = (meta->max_tx_time - meta->min_tx_time) /
                               (3600ULL * 1000000ULL);
    auto config = SstTemporalFilter::RecommendConfig(
        duration_hours, keys.size(), meta->file_size / (1024.0 * 1024.0));

    // 创建过滤器
    auto filter = SstTemporalFilter::CreateFilter(keys, start, end, config);
    if (filter) {
      meta->temporal_filter_metadata = SstTemporalFilter::SaveToMetadata(*filter);
    }
  }

  // 在 Compact 时合并过滤器
  static std::unique_ptr<TemporalBloomFilter> CreateMergedFilter(
      const std::vector<SstFileMetaOptimized>& input_files,
      Timestamp new_start,
      Timestamp new_end,
      size_t total_keys) {
    // 收集所有输入文件的过滤器
    std::vector<const TemporalBloomFilter*> filters;
    for (const auto& file : input_files) {
      auto* filter = file.GetFilter();
      if (filter) {
        filters.push_back(filter);
      }
    }

    return SstTemporalFilter::MergeFilters(
        filters, new_start, new_end, total_keys);
  }

  // 优化 QueryFrondFiles：先使用过滤器选择文件
  template <typename QueryFunc>
  static void OptimizedQueryFrondFiles(
      const std::vector<std::vector<SstFileMetaOptimized>>& levels,
      uint64_t entity_id,
      EntityType entity_type,
      uint16_t column_id,
      Timestamp start,
      Timestamp end,
      QueryFunc query_func,
      SstFileSelector::Stats* stats = nullptr) {
    SstFileSelector::Stats total_stats;

    for (const auto& level_files : levels) {
      auto selected = SstFileSelector::SelectFilesForQuery(
          level_files, entity_id, start, end, &total_stats);

      for (size_t idx : selected) {
        // 调用实际的 SST 查询函数
        query_func(level_files[idx], entity_id, entity_type, column_id);
      }
    }

    if (stats) {
      *stats = total_stats;
    }
  }
};

// ============================================================================
// 使用示例和最佳实践
// ============================================================================
//
// 1. Flush MemTable 时创建过滤器:
//
//    std::vector<CedarKey> keys = CollectKeys(memtable);
//    SstFileMetaOptimized meta;
//    meta.min_tx_time = ...; meta.max_tx_time = ...;
//    meta.min_entity_id = ...; meta.max_entity_id = ...;
//    LsmEngineTemporalFilterExt::AttachFilterToSstMeta(&meta, keys);
//    // 将 meta 和过滤器数据写入 SST
//
// 2. Query 时使用过滤器跳过 Frond:
//
//    auto selected = SstFileSelector::SelectFilesForQuery(
//        level0_files, entity_id, start, end);
//    for (size_t idx : selected) {
//      ReadAndQuerySST(level0_files[idx], ...);
//    }
//
// 3. Compact 时合并过滤器:
//
//    auto merged_filter = LsmEngineTemporalFilterExt::CreateMergedFilter(
//        input_files, new_start, new_end, total_keys);
//    // 将所有键添加到新过滤器
//    for (auto& key : all_keys) {
//      merged_filter->Add(key);
//    }
//    new_meta.temporal_filter_metadata =
//        SstTemporalFilter::SaveToMetadata(*merged_filter);
//
// 4. 预估优化效果:
//
//    // 假设查询覆盖 10% 的时间范围
//    double skip_rate = EstimateFilterSkipRate(0.1, 0.01);
//    // skip_rate ≈ 0.89 (跳过 89% 的文件)
//
// ============================================================================

}  // namespace cedar

#endif  // FERN_STORAGE_SST_TEMPORAL_FILTER_H_
