// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Query Result Cache - LRU cache for query results

#ifndef FERN_QUERY_CACHE_H_
#define FERN_QUERY_CACHE_H_

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <list>
#include <optional>

#include "cedar/types/descriptor.h"
#include "cedar/types/cedar_key.h"

namespace cedar {

// Cache key for query results
struct QueryCacheKey {
  uint64_t entity_id;
  uint16_t column_id;
  uint64_t timestamp;
  
  bool operator==(const QueryCacheKey& other) const {
    return entity_id == other.entity_id && 
           column_id == other.column_id && 
           timestamp == other.timestamp;
  }
};

// Hash function for cache key
struct QueryCacheKeyHash {
  size_t operator()(const QueryCacheKey& key) const {
    // FNV-1a hash
    size_t hash = 14695981039346656037ull;
    hash ^= key.entity_id;
    hash *= 1099511628211ull;
    hash ^= key.column_id;
    hash *= 1099511628211ull;
    hash ^= key.timestamp;
    hash *= 1099511628211ull;
    return hash;
  }
};

// Cache entry
struct QueryCacheEntry {
  std::optional<Descriptor> result;
  size_t size_bytes;
  std::chrono::steady_clock::time_point access_time;
};

// LRU Query Result Cache
class QueryCache {
 public:
  explicit QueryCache(size_t max_size_bytes = 64 * 1024 * 1024);  // 64MB default
  
  ~QueryCache() = default;
  
  // Get cached result
  std::optional<Descriptor> Get(uint64_t entity_id, uint16_t column_id, uint64_t timestamp);
  
  // Put result into cache
  void Put(uint64_t entity_id, uint16_t column_id, uint64_t timestamp, 
           const std::optional<Descriptor>& result);
  
  // Invalidate cache for an entity (call on write)
  void Invalidate(uint64_t entity_id);
  
  // Invalidate entire cache
  void Clear();
  
  // Get cache statistics
  struct Stats {
    size_t size_bytes = 0;
    size_t max_size_bytes = 0;
    size_t entries = 0;
    uint64_t hits = 0;
    uint64_t misses = 0;
    double hit_rate = 0.0;
  };
  Stats GetStats() const;
  
 private:
  size_t max_size_bytes_;
  size_t current_size_bytes_ = 0;
  
  mutable std::mutex mutex_;
  
  // LRU list: front = most recent
  std::list<QueryCacheKey> lru_list_;
  
  // Map from key to iterator and entry
  std::unordered_map<QueryCacheKey, 
                     std::pair<std::list<QueryCacheKey>::iterator, QueryCacheEntry>,
                     QueryCacheKeyHash> cache_;
  
  // Statistics
  mutable std::atomic<uint64_t> hits_{0};
  mutable std::atomic<uint64_t> misses_{0};
  
  void MoveToFront(const QueryCacheKey& key);
  void EvictLRU();
  size_t CalculateEntrySize(const std::optional<Descriptor>& result);
};

}  // namespace cedar

#endif  // FERN_QUERY_CACHE_H_
