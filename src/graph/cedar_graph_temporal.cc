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

// CedarGraph 时态查询接口实现

#include "cedar/graph/cedar_graph.h"
#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/graph/graph_semantic_layer.h"
#include "cedar/storage/temporal_storage_layer.h"
#include "cedar/cypher/value.h"

namespace cedar {

// ============================================================================
// 时态查询接口实现
// ============================================================================

std::vector<Neighbor> CedarGraph::GetOutNeighborsAsOf(uint64_t vertex_id,
                                                      uint16_t edge_type,
                                                      Timestamp as_of_time) {
  // 使用传统的GetOutNeighbors，但设置精确的时间范围
  return GetOutNeighbors(vertex_id, edge_type, as_of_time, Timestamp(as_of_time.value() + 1));
}

std::vector<Neighbor> CedarGraph::GetOutNeighborsBetween(uint64_t vertex_id,
                                                        uint16_t edge_type,
                                                        Timestamp start_time,
                                                        Timestamp end_time) {
  // 使用传统的GetOutNeighbors，设置时间范围
  return GetOutNeighbors(vertex_id, edge_type, start_time, end_time);
}

std::vector<std::pair<Timestamp, Neighbor>> CedarGraph::GetOutNeighborsAllVersions(
    uint64_t vertex_id, uint16_t edge_type) {
  std::vector<std::pair<Timestamp, Neighbor>> results;
  
  // 获取所有版本 - 使用最大时间范围
  auto neighbors = GetOutNeighbors(vertex_id, edge_type, Timestamp(0), Timestamp::Max());
  
  for (const auto& neighbor : neighbors) {
    results.emplace_back(neighbor.timestamp, neighbor);
  }
  
  return results;
}

std::optional<Neighbor> CedarGraph::GetOutNeighborsAtVersion(uint64_t vertex_id,
                                                            uint16_t edge_type,
                                                            uint64_t version) {
  // 首先获取所有版本
  auto all_versions = GetOutNeighborsAllVersions(vertex_id, edge_type);
  
  // 按版本查找（假设版本按时间戳递增）
  // TODO: 需要实际的版本号存储和索引
  if (version < all_versions.size()) {
    return all_versions[version].second;
  }
  
  return std::nullopt;
}

std::vector<Neighbor> CedarGraph::GetOutNeighborsWithRelation(
    uint64_t vertex_id,
    uint16_t edge_type,
    AllenRelation relation,
    Timestamp other_start,
    Timestamp other_end) {
  std::vector<Neighbor> results;
  
  // 获取候选邻居
  auto candidates = GetOutNeighbors(vertex_id, edge_type, 0, Timestamp::Max());
  
  // 使用Allen谓词过滤
  for (const auto& neighbor : candidates) {
    // 假设邻居有一个有效时间范围 [neighbor.timestamp, neighbor.timestamp + 1)
    // 实际上应该从存储中获取完整的时态元数据
    Timestamp neighbor_start = neighbor.timestamp;
    Timestamp neighbor_end = Timestamp(neighbor.timestamp.value() + 1);  // 简化处理
    
    if (AllenPredicateEvaluator::Evaluate(
            relation, neighbor_start, neighbor_end, other_start, other_end)) {
      results.push_back(neighbor);
    }
  }
  
  return results;
}

// ============================================================================
// 时态聚合查询接口实现
// ============================================================================

double CedarGraph::GetTemporalAverage(uint64_t vertex_id, uint16_t property_id,
                                     Timestamp start_time, Timestamp end_time) {
  // 获取属性历史
  auto history = GetVertexHistory(vertex_id, property_id, start_time, end_time);
  
  if (history.empty()) {
    return 0.0;
  }
  
  // 提取值和时间
  std::vector<double> values;
  std::vector<Timestamp> valid_froms;
  std::vector<Timestamp> valid_tos;
  
  for (size_t i = 0; i < history.size(); ++i) {
    values.push_back(static_cast<double>(history[i].second));
    valid_froms.push_back(history[i].first);
    // 使用下一个条目的时间作为当前条目的结束时间
    if (i + 1 < history.size()) {
      valid_tos.push_back(history[i + 1].first);
    } else {
      valid_tos.push_back(end_time);
    }
  }
  
  return TemporalAggregator::TemporalAverage(values, valid_froms, valid_tos, 
                                             start_time, end_time);
}

int64_t CedarGraph::GetTemporalSum(uint64_t vertex_id, uint16_t property_id,
                                  Timestamp start_time, Timestamp end_time) {
  // 获取属性历史
  auto history = GetVertexHistory(vertex_id, property_id, start_time, end_time);
  
  int64_t sum = 0;
  for (const auto& [ts, value] : history) {
    sum += value;
  }
  
  return sum;
}

int64_t CedarGraph::GetTotalDuration(uint64_t vertex_id, uint16_t edge_type,
                                    Timestamp start_time, Timestamp end_time) {
  // 获取边的历史
  auto history = GetOutNeighborsBetween(vertex_id, edge_type, start_time, end_time);
  
  // 计算总持续时间
  int64_t total_duration = 0;
  for (const auto& neighbor : history) {
    // 简化：假设每条边的持续时间是1个时间单位
    // 实际应该从时态元数据中计算
    total_duration += 1;
  }
  
  return total_duration;
}

uint64_t CedarGraph::GetVersionCount(uint64_t vertex_id, uint16_t edge_type) {
  auto all_versions = GetOutNeighborsAllVersions(vertex_id, edge_type);
  return all_versions.size();
}

// ============================================================================
// 时态索引管理
// ============================================================================

void CedarGraph::BuildTemporalIndex() {
  if (temporal_engine_) {
    temporal_engine_->BuildIndex();
  }
}

CedarGraph::TemporalIndexStats CedarGraph::GetTemporalIndexStats() const {
  TemporalIndexStats stats{};
  
  if (temporal_engine_) {
    auto engine_stats = temporal_engine_->GetStats();
    stats.index_entries = engine_stats.index_entries;
    stats.blocks_pruned = engine_stats.blocks_pruned;
    stats.blocks_checked = engine_stats.blocks_checked;
    stats.avg_query_time_ms = engine_stats.avg_query_time_ms;
  }
  
  return stats;
}

}  // namespace cedar
