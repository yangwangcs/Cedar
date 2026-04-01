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

#ifndef FERN_FERN_GRAPH_DB_H_
#define FERN_FERN_GRAPH_DB_H_

#include <cstdint>
#include <optional>
#include <unordered_set>
#include <vector>
#include <memory>
#include <unordered_map>
#include <string>
#include <utility>

#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "cedar/graph/pushdown_predicate.h"
#include "cedar/graph/cedar_graph.h"  // For Neighbor and BatchNeighborResult

namespace cedar {

// Forward declarations
class CedarGraphStorage;
class TemporalQueryEngine;

namespace cypher {
  class CypherEngine;
  struct ResultSet;
}

/**
 * @brief CedarGraphDB - Graph interface backed by CedarGraphStorage
 * 
 * Provides graph query capabilities on top of CedarGraphStorage engine.
 * Supports temporal queries, batch operations, and Cypher query language.
 */
class CedarGraphDB {
 public:
  // Constructor
  explicit CedarGraphDB(CedarGraphStorage* storage);
  
  // Destructor
  ~CedarGraphDB();

  // Disable copy
  CedarGraphDB(const CedarGraphDB&) = delete;
  CedarGraphDB& operator=(const CedarGraphDB&) = delete;

  // ========== Basic Query Interface ==========
  
  // Get neighbors (outgoing edges)
  std::vector<Neighbor> GetOutNeighbors(uint64_t vertex_id,
                                         uint16_t edge_type,
                                         Timestamp start_time,
                                         Timestamp end_time);

  // Get neighbors (incoming edges)
  std::vector<Neighbor> GetInNeighbors(uint64_t vertex_id,
                                        uint16_t edge_type,
                                        Timestamp start_time,
                                        Timestamp end_time);

  // Get vertex property history
  std::vector<std::pair<Timestamp, int32_t>> GetVertexHistory(
      uint64_t vertex_id, uint16_t column_id,
      Timestamp start_time, Timestamp end_time);

  // ========== Batch Query Interface ==========
  
  // Batch get neighbors
  std::vector<BatchNeighborResult> BatchGetNeighbors(
      const std::vector<uint64_t>& vertex_ids,
      uint16_t edge_type,
      size_t num_threads = 0);

  // ========== Cypher Query Interface ==========
  
  // Execute Cypher query
  cypher::ResultSet ExecuteCypher(const std::string& query);
  
  // Get EXPLAIN output
  std::string ExplainCypher(const std::string& query);
  
  // Check if query is valid
  bool IsValidCypher(const std::string& query);

  // ========== Entity ID Encoding Utilities ==========
  
  // Decode composite entity_id to (high_bits, low_bits)
  // Useful for applications that encode multiple fields into entity_id
  static std::pair<uint32_t, uint16_t> DecodeEntityId(uint64_t entity_id) {
    uint32_t high_bits = static_cast<uint32_t>(entity_id >> 32);
    uint16_t low_bits = static_cast<uint16_t>(entity_id & 0xFFFF);
    return {high_bits, low_bits};
  }
  
  // Encode (high_bits, low_bits) to entity_id
  static uint64_t EncodeEntityId(uint32_t high_bits, uint16_t low_bits) {
    return (static_cast<uint64_t>(high_bits) << 32) | low_bits;
  }
  
  // ========== Entity Enumeration ==========
  
  // Get all entities within a range
  // @param min_entity_id Minimum entity ID (inclusive)
  // @param max_entity_id Maximum entity ID (inclusive)  
  // @param step Step size for sampling (1 = all entities)
  // @return Vector of entity IDs
  std::vector<uint64_t> GetAllEntities(
      uint64_t min_entity_id = 1,
      uint64_t max_entity_id = 1000,
      uint64_t step = 1);
  
  // Get time series data for a specific entity
  std::vector<std::pair<Timestamp, Descriptor>> GetTimeSeries(
      uint64_t entity_id,
      Timestamp start_time,
      Timestamp end_time);

 private:
  CedarGraphStorage* storage_;
  std::unique_ptr<cypher::CypherEngine> cypher_engine_;
};

}  // namespace cedar

#endif  // FERN_FERN_GRAPH_DB_H_
