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

#include "cedar/graph/cedar_graph.h"

#include <queue>
#include <thread>
#include <vector>
#include <unordered_set>
#include <future>

#include "cedar/graph/graph_semantic_layer.h"
#include "cedar/cypher/cypher_engine.h"
#include "cedar/cypher/value.h"
#include "cedar/storage/temporal_storage_layer.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/transaction/occ_transaction.h"
#include "cedar/types/descriptor.h"

namespace cedar {

// Constructor
CedarGraph::CedarGraph(CedarGraphStorage* storage) 
    : storage_(storage) {
  if (storage_) {
    semantic_layer_ = std::make_unique<GraphSemanticLayer>(storage_);
    temporal_engine_ = std::make_unique<TemporalQueryEngine>(storage_);
  }
}

// Destructor
CedarGraph::~CedarGraph() = default;

// Get neighbors (outgoing edges)
std::vector<Neighbor> CedarGraph::GetOutNeighbors(uint64_t vertex_id,
                                                  uint16_t edge_type,
                                                  Timestamp start_time,
                                                  Timestamp end_time) {
  std::vector<Neighbor> result;
  
  if (!storage_) {
    return result;
  }
  
  // Use ScanMemTableOnly for fast queries
  const size_t MAX_VERSIONS = 10;
  auto versions = storage_->ScanMemTableOnly(vertex_id, start_time, end_time, MAX_VERSIONS);
  
  for (const auto& [ts, desc] : versions) {
    Neighbor neighbor;
    neighbor.id = vertex_id;
    neighbor.edge_type = edge_type;
    neighbor.timestamp = ts;
    
    // Extract value from descriptor
    if (auto int_val = desc.AsInlineInt()) {
      neighbor.value = *int_val;
    }
    
    result.push_back(neighbor);
  }
  
  return result;
}

// Get neighbors (incoming edges)
std::vector<Neighbor> CedarGraph::GetInNeighbors(uint64_t vertex_id,
                                                 uint16_t edge_type,
                                                 Timestamp start_time,
                                                 Timestamp end_time) {
  // Requires reverse index support
  (void)vertex_id;
  (void)edge_type;
  (void)start_time;
  (void)end_time;
  return {};
}

// ========== Transaction-aware Query Implementation ==========

std::vector<Neighbor> CedarGraph::GetOutNeighborsInTxn(OCCTransaction* txn,
                                                       uint64_t vertex_id,
                                                       uint16_t edge_type) {
  std::vector<Neighbor> result;
  
  if (!storage_ || !txn) {
    return result;
  }
  
  // 获取事务的读时间戳作为快照点
  Timestamp read_ts = txn->GetReadTimestamp();
  
  // 查询边数据 - 使用事务快照时间戳
  // 对于出边，我们需要查询 EdgeOut 类型的数据
  // 简化实现：查询指定时间戳的版本
  auto edge_result = storage_->Get(vertex_id, EntityType::EdgeOut, edge_type, read_ts);
  
  if (edge_result.has_value()) {
    Neighbor neighbor;
    neighbor.id = vertex_id;
    neighbor.edge_type = edge_type;
    neighbor.timestamp = read_ts;
    
    if (auto int_val = edge_result->AsInlineInt()) {
      neighbor.value = *int_val;
    }
    
    result.push_back(neighbor);
  }
  
  return result;
}

std::vector<std::vector<uint64_t>> CedarGraph::BfsInTxn(OCCTransaction* txn,
                                                        uint64_t start,
                                                        uint16_t edge_type,
                                                        size_t max_depth) {
  std::vector<std::vector<uint64_t>> result;
  
  if (!storage_ || !txn || max_depth == 0) {
    return result;
  }
  
  // 获取事务的读时间戳
  Timestamp read_ts = txn->GetReadTimestamp();
  
  std::unordered_set<uint64_t> visited;
  std::vector<uint64_t> current_level = {start};
  visited.insert(start);
  
  for (size_t depth = 0; depth < max_depth && !current_level.empty(); ++depth) {
    std::vector<uint64_t> next_level;
    std::vector<uint64_t> current_result;
    
    for (uint64_t vertex_id : current_level) {
      // 在事务快照内查询邻居
      auto neighbors = GetOutNeighborsInTxn(txn, vertex_id, edge_type);
      
      for (const auto& neighbor : neighbors) {
        if (visited.insert(neighbor.id).second) {
          next_level.push_back(neighbor.id);
          current_result.push_back(neighbor.id);
        }
      }
    }
    
    if (!current_result.empty()) {
      result.push_back(std::move(current_result));
    }
    
    current_level = std::move(next_level);
  }
  
  return result;
}

// Get vertex property history
std::vector<std::pair<Timestamp, int32_t>> CedarGraph::GetVertexHistory(
    uint64_t vertex_id, uint16_t column_id,
    Timestamp start_time, Timestamp end_time) {
  std::vector<std::pair<Timestamp, int32_t>> history;

  if (!storage_) {
    return history;
  }

  (void)column_id;
  
  auto versions = storage_->Scan(vertex_id, start_time, end_time);

  for (const auto& [ts, desc] : versions) {
    if (auto int_val = desc.AsInlineInt()) {
      history.emplace_back(ts, *int_val);
    }
  }

  return history;
}

// Batch get neighbors
std::vector<BatchNeighborResult> CedarGraph::BatchGetNeighbors(
    const std::vector<uint64_t>& vertex_ids,
    uint16_t edge_type,
    const PushdownPredicate& predicate,
    size_t num_threads) {
  if (semantic_layer_) {
    return semantic_layer_->BatchGetNeighbors(vertex_ids, edge_type, predicate, num_threads);
  }
  
  // Fallback: use storage directly
  std::vector<BatchNeighborResult> results;
  for (uint64_t vertex_id : vertex_ids) {
    BatchNeighborResult result(vertex_id);
    result.neighbors = GetOutNeighbors(vertex_id, edge_type, 
                                       predicate.time_start.value_or(0),
                                       predicate.time_end.value_or(Timestamp::Max()));
    results.push_back(std::move(result));
  }
  return results;
}

// Batch get neighbors (simplified version)
std::vector<BatchNeighborResult> CedarGraph::BatchGetNeighbors(
    const std::vector<uint64_t>& vertex_ids,
    uint16_t edge_type,
    size_t num_threads) {
  return BatchGetNeighbors(vertex_ids, edge_type, PushdownPredicate(), num_threads);
}

// BFS traversal (with time range)
std::vector<std::vector<uint64_t>> CedarGraph::Bfs(
    uint64_t start, uint16_t edge_type, size_t max_depth,
    Timestamp start_time, Timestamp end_time) {
  PushdownPredicate predicate;
  predicate.time_start = start_time;
  predicate.time_end = end_time;
  return Bfs(start, edge_type, max_depth, predicate, 0);
}

// BFS traversal (with pushdown predicate)
std::vector<std::vector<uint64_t>> CedarGraph::Bfs(
    uint64_t start, uint16_t edge_type, size_t max_depth,
    const PushdownPredicate& predicate,
    size_t num_threads) {
  if (semantic_layer_) {
    return semantic_layer_->BfsWithPushdown(start, edge_type, max_depth, predicate, num_threads);
  }
  return {};
}

// Degree centrality calculation
std::unordered_map<uint64_t, size_t> CedarGraph::DegreeCentrality(
    const std::vector<uint64_t>& vertex_ids,
    uint16_t edge_type,
    const PushdownPredicate& predicate) {
  if (semantic_layer_) {
    return semantic_layer_->DegreeCentrality(vertex_ids, edge_type, predicate);
  }
  return {};
}

// Connected components
std::vector<std::vector<uint64_t>> CedarGraph::ConnectedComponents(
    const std::vector<uint64_t>& seed_vertices,
    uint16_t edge_type) {
  if (semantic_layer_) {
    return semantic_layer_->ConnectedComponents(seed_vertices, edge_type);
  }
  return {};
}

// K-hop neighbors query
std::vector<uint64_t> CedarGraph::GetKHopNeighbors(
    uint64_t vertex_id, uint16_t edge_type, size_t k,
    Timestamp start_time, Timestamp end_time) {
  std::vector<uint64_t> result;
  
  if (!storage_ || k == 0) {
    return result;
  }

  std::unordered_set<uint64_t> visited;
  std::vector<uint64_t> current_level = {vertex_id};
  visited.insert(vertex_id);

  for (size_t hop = 0; hop < k; ++hop) {
    auto batch_results = BatchGetNeighbors(current_level, edge_type, 
                                           PushdownPredicate(), 4);
    
    std::vector<uint64_t> next_level;
    for (const auto& batch_result : batch_results) {
      for (const auto& neighbor : batch_result.neighbors) {
        if (visited.insert(neighbor.id).second) {
          next_level.push_back(neighbor.id);
          if (hop == k - 1) {
            result.push_back(neighbor.id);
          }
        }
      }
    }
    
    current_level = std::move(next_level);
    if (current_level.empty()) {
      break;
    }
  }

  return result;
}

// Get all versions of an edge
std::vector<Neighbor> CedarGraph::GetEdgeHistory(
    uint64_t src_id, uint64_t dst_id, uint16_t edge_type,
    Timestamp start_time, Timestamp end_time) {
  std::vector<Neighbor> history;

  if (!storage_) {
    return history;
  }

  auto results = storage_->Scan(src_id, start_time, end_time);

  for (const auto& [ts, desc] : results) {
    if (auto int_val = desc.AsInlineInt()) {
      history.emplace_back(dst_id, edge_type, ts, *int_val);
    }
  }

  return history;
}

// ========== Configuration Interface ==========

void CedarGraph::EnableBlockCache(bool enable) {
  if (semantic_layer_) {
    semantic_layer_->EnableBlockCache(enable);
  }
}

void CedarGraph::EnablePrefetch(bool enable) {
  if (semantic_layer_) {
    semantic_layer_->EnablePrefetch(enable);
  }
}

void CedarGraph::GetCacheStats(size_t* cache_size, double* hit_rate) const {
  if (semantic_layer_) {
    semantic_layer_->GetCacheStats(cache_size, hit_rate);
  } else {
    *cache_size = 0;
    *hit_rate = 0.0;
  }
}

void CedarGraph::ClearCache() {
  if (semantic_layer_) {
    // TODO: Implement ClearCache
  }
}

// ========== Cypher Query Interface ==========

cypher::ResultSet CedarGraph::ExecuteCypher(const std::string& query) {
  (void)query;
  return cypher::ResultSet();
}

std::string CedarGraph::ExplainCypher(const std::string& query) {
  (void)query;
  return "CedarGraph Cypher not yet implemented";
}

bool CedarGraph::IsValidCypher(const std::string& query) {
  (void)query;
  return false;
}

// ========== Temporal Query Interface ==========

std::vector<Neighbor> CedarGraph::GetOutNeighborsAsOf(uint64_t vertex_id,
                                                      uint16_t edge_type,
                                                      Timestamp as_of_time) {
  return GetOutNeighbors(vertex_id, edge_type, 0, as_of_time);
}

std::vector<Neighbor> CedarGraph::GetOutNeighborsBetween(uint64_t vertex_id,
                                                         uint16_t edge_type,
                                                         Timestamp start_time,
                                                         Timestamp end_time) {
  return GetOutNeighbors(vertex_id, edge_type, start_time, end_time);
}

std::vector<std::pair<Timestamp, Neighbor>> CedarGraph::GetOutNeighborsAllVersions(
    uint64_t vertex_id, uint16_t edge_type) {
  std::vector<std::pair<Timestamp, Neighbor>> result;
  
  auto neighbors = GetOutNeighbors(vertex_id, edge_type, Timestamp(0), Timestamp::Max());
  for (const auto& neighbor : neighbors) {
    result.emplace_back(neighbor.timestamp, neighbor);
  }
  
  return result;
}

std::optional<Neighbor> CedarGraph::GetOutNeighborsAtVersion(uint64_t vertex_id,
                                                             uint16_t edge_type,
                                                             uint64_t version) {
  (void)version;
  auto neighbors = GetOutNeighbors(vertex_id, edge_type, Timestamp(0), Timestamp::Max());
  if (!neighbors.empty()) {
    return neighbors[0];
  }
  return std::nullopt;
}

std::vector<Neighbor> CedarGraph::GetOutNeighborsWithRelation(
    uint64_t vertex_id,
    uint16_t edge_type,
    AllenRelation relation,
    Timestamp other_start,
    Timestamp other_end) {
  (void)relation;
  (void)other_start;
  (void)other_end;
  return GetOutNeighbors(vertex_id, edge_type, 0, Timestamp::Max());
}

// ========== Temporal Aggregation Interface ==========

double CedarGraph::GetTemporalAverage(uint64_t vertex_id, uint16_t property_id,
                                     Timestamp start_time, Timestamp end_time) {
  (void)property_id;
  auto history = GetVertexHistory(vertex_id, 0, start_time, end_time);
  if (history.empty()) {
    return 0.0;
  }
  
  double sum = 0.0;
  for (const auto& [ts, val] : history) {
    (void)ts;
    sum += val;
  }
  return sum / history.size();
}

int64_t CedarGraph::GetTemporalSum(uint64_t vertex_id, uint16_t property_id,
                                  Timestamp start_time, Timestamp end_time) {
  (void)property_id;
  auto history = GetVertexHistory(vertex_id, 0, start_time, end_time);
  
  int64_t sum = 0;
  for (const auto& [ts, val] : history) {
    (void)ts;
    sum += val;
  }
  return sum;
}

int64_t CedarGraph::GetTotalDuration(uint64_t vertex_id, uint16_t edge_type,
                                    Timestamp start_time, Timestamp end_time) {
  (void)vertex_id;
  (void)edge_type;
  return static_cast<int64_t>(end_time.value() - start_time.value());
}

uint64_t CedarGraph::GetVersionCount(uint64_t vertex_id, uint16_t edge_type) {
  auto history = GetOutNeighbors(vertex_id, edge_type, 0, Timestamp::Max());
  return history.size();
}

// ========== Temporal Index Management ==========

void CedarGraph::BuildTemporalIndex() {
  // TODO: Implement temporal index building
}

CedarGraph::TemporalIndexStats CedarGraph::GetTemporalIndexStats() const {
  TemporalIndexStats stats;
  return stats;
}

// ========== Transaction Interface ==========

OCCTransaction* CedarGraph::BeginTransaction(const TransactionOptions* options) {
  if (!storage_) {
    return nullptr;
  }
  
  // 委托给存储层的事务管理
  return storage_->BeginTransaction(options);
}

}  // namespace cedar
