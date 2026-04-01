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

#ifndef FERN_FERN_GRAPH_H_
#define FERN_FERN_GRAPH_H_

#include <cstdint>
#include <optional>
#include <unordered_set>
#include <vector>
#include <memory>
#include <unordered_map>
#include <string>

#include "cedar/types/cedar_key.h"
#include "cedar/graph/pushdown_predicate.h"

namespace cedar {

// Forward declarations
class CedarGraphStorage;
class OCCTransaction;
struct TransactionOptions;
class TemporalQueryEngine;

// Forward declarations for temporal types
enum class AllenRelation;

namespace cypher {
  class CypherEngine;
  struct ResultSet;
}

// Forward declaration
class GraphSemanticLayer;

// Neighbor query result
struct Neighbor {
  uint64_t id;                  // Neighbor ID
  uint16_t edge_type;           // Edge type
  Timestamp timestamp;          // Timestamp
  std::optional<int32_t> value; // Property value

  Neighbor() = default;
  Neighbor(uint64_t id_, uint16_t edge_type_, Timestamp ts,
           std::optional<int32_t> val)
      : id(id_), edge_type(edge_type_), timestamp(ts), value(val) {}
};

// Batch neighbor query result
struct BatchNeighborResult {
  uint64_t vertex_id;
  std::vector<Neighbor> neighbors;
  
  explicit BatchNeighborResult(uint64_t id = 0) : vertex_id(id) {}
};

/**
 * @brief CedarGraph - Unified graph query interface
 * Based on CedarGraphStorage providing graph query capabilities
 */
class CedarGraph {
 public:
  // Constructor - from CedarGraphStorage
  explicit CedarGraph(CedarGraphStorage* storage);
  
  // Destructor
  ~CedarGraph();

  // Disable copy
  CedarGraph(const CedarGraph&) = delete;
  CedarGraph& operator=(const CedarGraph&) = delete;

  // ========== Transaction Interface ==========
  
  /// 开启事务 (beginTX)
  /// 
  /// 图层的事务主要用于：
  /// 1. 在一致性快照内执行图查询
  /// 2. 批量图修改操作的事务封装
  ///
  /// 使用示例:
  ///   OCCTransaction* txn = graph->beginTX();
  ///   auto neighbors = graph->GetOutNeighborsInTxn(txn, vertex_id, ...);
  ///   txn->Commit();
  ///   delete txn;
  ///
  /// @param options 事务选项，nullptr 表示使用默认选项
  /// @return 事务对象指针，失败返回 nullptr。调用者负责 delete。
  OCCTransaction* BeginTransaction(const TransactionOptions* options = nullptr);
  
  /// 简化的 beginTX 别名
  /// @see BeginTransaction
  OCCTransaction* beginTX() { return BeginTransaction(nullptr); }

  // ========== Basic Query Interface ==========
  
  // Get neighbors (outgoing edges)
  std::vector<Neighbor> GetOutNeighbors(uint64_t vertex_id,
                                         uint16_t edge_type,
                                         Timestamp start_time,
                                         Timestamp end_time);

  // Get neighbors (incoming edges) - requires reverse index
  std::vector<Neighbor> GetInNeighbors(uint64_t vertex_id,
                                        uint16_t edge_type,
                                        Timestamp start_time,
                                        Timestamp end_time);

  // Get vertex property history
  std::vector<std::pair<Timestamp, int32_t>> GetVertexHistory(
      uint64_t vertex_id, uint16_t column_id,
      Timestamp start_time, Timestamp end_time);

  // ========== Transaction-aware Query Interface ==========
  // 事务感知的查询接口 - 在事务快照内执行查询
  
  /// 在事务快照内查询出边邻居
  /// @param txn 事务上下文，使用其读时间戳作为快照点
  /// @param vertex_id 源节点ID
  /// @param edge_type 边类型
  /// @return 邻居列表（在事务快照内可见的版本）
  std::vector<Neighbor> GetOutNeighborsInTxn(OCCTransaction* txn,
                                              uint64_t vertex_id,
                                              uint16_t edge_type);
  
  /// 在事务快照内执行BFS遍历
  /// @param txn 事务上下文
  /// @param start 起始节点
  /// @param edge_type 边类型
  /// @param max_depth 最大深度
  /// @return 每层访问的节点列表
  std::vector<std::vector<uint64_t>> BfsInTxn(OCCTransaction* txn,
                                               uint64_t start, 
                                               uint16_t edge_type, 
                                               size_t max_depth);

  // ========== Batch Query Interface (with pushdown optimization) ==========
  
  // Batch get neighbors
  std::vector<BatchNeighborResult> BatchGetNeighbors(
      const std::vector<uint64_t>& vertex_ids,
      uint16_t edge_type,
      const PushdownPredicate& predicate,
      size_t num_threads = 0);
  
  // Batch get neighbors (simplified version)
  std::vector<BatchNeighborResult> BatchGetNeighbors(
      const std::vector<uint64_t>& vertex_ids,
      uint16_t edge_type,
      size_t num_threads = 0);

  // ========== BFS Traversal Interface ==========
  
  // BFS traversal (with time range)
  std::vector<std::vector<uint64_t>> Bfs(
      uint64_t start, uint16_t edge_type, size_t max_depth,
      Timestamp start_time, Timestamp end_time);

  // BFS traversal (with pushdown predicate)
  std::vector<std::vector<uint64_t>> Bfs(
      uint64_t start, uint16_t edge_type, size_t max_depth,
      const PushdownPredicate& predicate,
      size_t num_threads = 0);

  // ========== Graph Analysis Algorithm Interface ==========
  
  // Degree centrality calculation
  std::unordered_map<uint64_t, size_t> DegreeCentrality(
      const std::vector<uint64_t>& vertex_ids,
      uint16_t edge_type,
      const PushdownPredicate& predicate);

  // Connected components
  std::vector<std::vector<uint64_t>> ConnectedComponents(
      const std::vector<uint64_t>& seed_vertices,
      uint16_t edge_type);

  // K-hop neighbors query
  std::vector<uint64_t> GetKHopNeighbors(
      uint64_t vertex_id, uint16_t edge_type, size_t k,
      Timestamp start_time, Timestamp end_time);

  // Get all versions of an edge
  std::vector<Neighbor> GetEdgeHistory(
      uint64_t src_id, uint64_t dst_id, uint16_t edge_type,
      Timestamp start_time, Timestamp end_time);

  // ========== Cypher Query Interface ==========
  
  // Execute Cypher query
  cypher::ResultSet ExecuteCypher(const std::string& query);
  
  // View Cypher query execution plan
  std::string ExplainCypher(const std::string& query);
  
  // Check if Cypher query is valid
  bool IsValidCypher(const std::string& query);

  // ========== Temporal Query Interface (Temporal Query API) ==========
  
  // AS OF query - get entity state at specific time point
  std::vector<Neighbor> GetOutNeighborsAsOf(uint64_t vertex_id,
                                            uint16_t edge_type,
                                            Timestamp as_of_time);
  
  // BETWEEN query - get all versions within time range
  std::vector<Neighbor> GetOutNeighborsBetween(uint64_t vertex_id,
                                               uint16_t edge_type,
                                               Timestamp start_time,
                                               Timestamp end_time);
  
  // ALL VERSIONS query - get all historical versions of entity
  std::vector<std::pair<Timestamp, Neighbor>> GetOutNeighborsAllVersions(
      uint64_t vertex_id, uint16_t edge_type);
  
  // VERSION k query - get specific version of entity
  std::optional<Neighbor> GetOutNeighborsAtVersion(uint64_t vertex_id,
                                                   uint16_t edge_type,
                                                   uint64_t version);
  
  // Temporal path query - using Allen relations
  std::vector<Neighbor> GetOutNeighborsWithRelation(
      uint64_t vertex_id,
      uint16_t edge_type,
      AllenRelation relation,
      Timestamp other_start,
      Timestamp other_end);
  
  // ========== Temporal Aggregation Query Interface ==========
  
  // Time-weighted average
  double GetTemporalAverage(uint64_t vertex_id, uint16_t property_id,
                            Timestamp start_time, Timestamp end_time);
  
  // Time range sum
  int64_t GetTemporalSum(uint64_t vertex_id, uint16_t property_id,
                         Timestamp start_time, Timestamp end_time);
  
  // Duration statistics
  int64_t GetTotalDuration(uint64_t vertex_id, uint16_t edge_type,
                           Timestamp start_time, Timestamp end_time);
  
  // Version count
  uint64_t GetVersionCount(uint64_t vertex_id, uint16_t edge_type);
  
  // ========== Temporal Index Management ==========
  
  // Build temporal index
  void BuildTemporalIndex();
  
  // Get temporal index statistics
  struct TemporalIndexStats {
    size_t index_entries = 0;
    size_t blocks_pruned = 0;
    size_t blocks_checked = 0;
    double avg_query_time_ms = 0.0;
  };
  TemporalIndexStats GetTemporalIndexStats() const;
  
  // ========== Configuration Interface ==========
  
  // Enable/disable Block cache
  void EnableBlockCache(bool enable);
  
  // Enable/disable prefetch
  void EnablePrefetch(bool enable);
  
  // Get cache statistics
  void GetCacheStats(size_t* cache_size, double* hit_rate) const;
  
  // Clear cache
  void ClearCache();

 private:
  CedarGraphStorage* storage_;
  std::unique_ptr<GraphSemanticLayer> semantic_layer_;
  std::unique_ptr<cypher::CypherEngine> cypher_engine_;
  std::unique_ptr<TemporalQueryEngine> temporal_engine_;
};

}  // namespace cedar

#endif  // FERN_FERN_GRAPH_H_
