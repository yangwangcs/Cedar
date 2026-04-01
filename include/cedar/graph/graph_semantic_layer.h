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

#ifndef FERN_GRAPH_SEMANTIC_LAYER_H_
#define FERN_GRAPH_SEMANTIC_LAYER_H_

#include <cstdint>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <mutex>
#include <atomic>

#include "cedar/graph/pushdown_predicate.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "cedar/core/status.h"
#include "cedar/core/threading.h"

namespace cedar {

// 前向声明（完整定义在 cedar_graph.h 中）
struct Neighbor;
struct BatchNeighborResult;

// 注意：GraphSemanticLayer 的实现文件中包含 cedar_graph.h
// 以获取 Neighbor 和 BatchNeighborResult 的完整定义

// 前向声明
class CedarGraphStorage;
class FrondSstReader;
class Frond;

// Block 缓存条目
struct CachedBlock {
  std::vector<uint8_t> data;      // 解压后的 Block 数据
  Timestamp min_timestamp;        // Block 内最小时间戳
  Timestamp max_timestamp;        // Block 内最大时间戳
  uint64_t min_entity_id;         // Block 内最小 entity_id
  uint64_t max_entity_id;         // Block 内最大 entity_id
  std::atomic<size_t> ref_count{0};  // 引用计数
  std::chrono::steady_clock::time_point last_access;  // 最后访问时间
};

// 共享 IO 上下文 - 用于批量查询时共享 Block 缓存
class SharedIOContext {
 public:
  SharedIOContext();
  ~SharedIOContext();

  // 禁用拷贝
  SharedIOContext(const SharedIOContext&) = delete;
  SharedIOContext& operator=(const SharedIOContext&) = delete;

  // ========== Block 缓存接口 ==========
  
  // 获取缓存的 Block
  // 如果不存在或已过期，返回 nullptr
  std::shared_ptr<CachedBlock> GetBlock(
      const std::string& file_path, size_t block_idx);
  
  // 添加 Block 到缓存
  void CacheBlock(const std::string& file_path, size_t block_idx,
                  std::shared_ptr<CachedBlock> block);
  
  // 检查 Block 是否在缓存中
  bool HasBlock(const std::string& file_path, size_t block_idx) const;
  
  // 根据时间范围预筛选：检查 Block 是否可能包含目标数据
  bool BlockMayContainTimeRange(const std::string& file_path, size_t block_idx,
                                Timestamp start, Timestamp end) const;
  
  // 根据 entity_id 预筛选
  bool BlockMayContainEntity(const std::string& file_path, size_t block_idx,
                             uint64_t entity_id) const;
  
  // ========== 预读接口 ==========
  
  // 标记 Block 为已预读
  void MarkBlockPrefetched(const std::string& file_path, size_t block_idx);
  
  // 检查 Block 是否已被预读
  bool IsBlockPrefetched(const std::string& file_path, size_t block_idx) const;
  
  // 获取所有需要预读的 Blocks（基于已访问的 blocks）
  std::vector<std::pair<std::string, size_t>> GetPrefetchCandidates(
      size_t max_candidates = 4) const;
  
  // ========== 统计接口 ==========
  
  size_t CacheSize() const { return block_cache_.size(); }
  void ClearCache();
  
  // 获取缓存命中率（用于调优）
  double GetHitRate() const;
  void ResetStats();

 private:
  // Block 缓存：key = "file_path:block_idx"
  using CacheKey = std::pair<std::string, size_t>;
  struct CacheKeyHash {
    size_t operator()(const CacheKey& key) const {
      return std::hash<std::string>{}(key.first) ^ (key.second * 31);
    }
  };
  
  mutable std::mutex cache_mutex_;
  std::unordered_map<CacheKey, std::weak_ptr<CachedBlock>, CacheKeyHash> block_cache_;
  
  // 预读标记
  mutable std::mutex prefetch_mutex_;
  std::unordered_set<CacheKey, CacheKeyHash> prefetched_blocks_;
  
  // 访问历史（用于智能预读）
  mutable std::mutex history_mutex_;
  std::vector<CacheKey> access_history_;
  static constexpr size_t kMaxHistorySize = 1000;
  
  // 统计
  mutable std::mutex stats_mutex_;
  size_t cache_hits_ = 0;
  size_t cache_misses_ = 0;
};

// 图语义层 - 核心类
// 实现计算下推、批量查询、Block 缓存、预读优化
class GraphSemanticLayer {
 public:
  explicit GraphSemanticLayer(CedarGraphStorage* storage);
  ~GraphSemanticLayer();

  // 禁止拷贝
  GraphSemanticLayer(const GraphSemanticLayer&) = delete;
  GraphSemanticLayer& operator=(const GraphSemanticLayer&) = delete;

  // ========== 批量查询接口 ==========
  
  // 批量获取邻居（带下推优化和 Block 缓存）
  std::vector<BatchNeighborResult> BatchGetNeighbors(
      const std::vector<uint64_t>& vertex_ids,
      uint16_t edge_type,
      const PushdownPredicate& predicate,
      size_t num_threads = 0);  // 0 = 使用全局线程池

  // 批量点查询（带下推）
  std::vector<std::pair<CedarKey, Descriptor>> BatchGet(
      const std::vector<CedarKey>& keys,
      const PushdownPredicate& predicate,
      size_t num_threads = 0);

  // ========== 时态查询优化 ==========
  
  // 时态范围查询（利用 Frond Footer 时间范围索引）
  std::vector<std::pair<Timestamp, Descriptor>> TemporalScan(
      uint64_t entity_id,
      EntityType entity_type,
      uint16_t column_id,
      const PushdownPredicate& predicate);

  // ========== 图分析算法接口 ==========
  
  // 带下推谓词的 BFS
  std::vector<std::vector<uint64_t>> BfsWithPushdown(
      uint64_t start, uint16_t edge_type, size_t max_depth,
      const PushdownPredicate& predicate,
      size_t num_threads = 0);

  // 度数中心性计算（批量优化版）
  std::unordered_map<uint64_t, size_t> DegreeCentrality(
      const std::vector<uint64_t>& vertex_ids,
      uint16_t edge_type,
      const PushdownPredicate& predicate);

  // 连通分量（批量 BFS）
  std::vector<std::vector<uint64_t>> ConnectedComponents(
      const std::vector<uint64_t>& seed_vertices,
      uint16_t edge_type);

  // ========== 性能调优接口 ==========
  
  // 设置是否启用预读
  void EnablePrefetch(bool enable) { enable_prefetch_ = enable; }
  
  // 设置是否启用 Block 缓存
  void EnableBlockCache(bool enable) { enable_block_cache_ = enable; }
  
  // 获取缓存统计
  void GetCacheStats(size_t* cache_size, double* hit_rate) const;

 private:
  // 内部实现：带下推的单个顶点邻居查询
  std::vector<Neighbor> GetOutNeighborsWithPushdown(
      uint64_t vertex_id,
      uint16_t edge_type,
      const PushdownPredicate& predicate,
      SharedIOContext* shared_io = nullptr);

  // 内部实现：从 Frond 文件扫描（带下推、缓存、预读）
  Status ScanFrondFileWithPushdown(
      const std::string& file_path,
      uint64_t entity_id,
      uint16_t edge_type,
      const PushdownPredicate& predicate,
      std::vector<Neighbor>& results,
      SharedIOContext* shared_io = nullptr);

  // DEPRECATED: Old format - now using ZoneColumnar
  // // 内部实现：读取 Frond（带缓存）
  // Status ReadFrondWithCache(
  //     FrondSstReader* reader,
  //     const std::string& file_path,
  //     size_t block_idx,
  //     Frond* frond,
  //     SharedIOContext* shared_io = nullptr);

  // 内部实现：预读 Blocks
  void PrefetchBlocks(
      const std::vector<std::pair<std::string, size_t>>& blocks,
      SharedIOContext* shared_io);

  // 内部实现：Block 级过滤
  bool ShouldSkipBlock(
      const std::string& file_path,
      size_t block_idx,
      const PushdownPredicate& predicate,
      SharedIOContext* shared_io);

  CedarGraphStorage* storage_;
  
  // 配置
  std::atomic<bool> enable_prefetch_{true};
  std::atomic<bool> enable_block_cache_{true};
  
  // 查询缓存（简单 LRU）- 用于热点顶点
  struct CacheEntry {
    std::vector<Neighbor> neighbors;
    Timestamp timestamp;  // 缓存时间
  };
  std::unordered_map<uint64_t, CacheEntry> neighbor_cache_;
  mutable std::mutex cache_mutex_;
  static constexpr size_t kMaxCacheSize = 10000;
};

}  // namespace cedar

#endif  // FERN_GRAPH_SEMANTIC_LAYER_H_
