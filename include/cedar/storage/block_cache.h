// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Block Cache - Caches decoded zone data to reduce decompression/decoding overhead

#ifndef FERN_STORAGE_BLOCK_CACHE_H_
#define FERN_STORAGE_BLOCK_CACHE_H_

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <list>
#include <vector>

namespace cedar {

// Cache entry for decoded zone data
struct CachedBlock {
  std::string key;           // cache key: file_path + zone_type
  std::string data;          // decoded/decompressed data
  size_t size = 0;           // size in bytes
  uint64_t access_count = 0; // for statistics
  
  CachedBlock(const std::string& k, std::string d) 
      : key(k), data(std::move(d)), size(key.size() + data.size()) {}
};

// Simple LRU Block Cache
class BlockCache {
 public:
  explicit BlockCache(size_t capacity_bytes = 8 * 1024 * 1024)  // default 8MB
      : capacity_bytes_(capacity_bytes), used_bytes_(0) {}
  
  ~BlockCache() { Clear(); }
  
  // Get cached block, returns nullptr if not found
  std::shared_ptr<CachedBlock> Get(const std::string& key);
  
  // Insert block into cache
  void Insert(const std::string& key, std::string data);
  
  // Clear all cached blocks
  void Clear();
  
  // Get cache statistics
  struct Stats {
    size_t capacity_bytes = 0;
    size_t used_bytes = 0;
    size_t num_entries = 0;
    uint64_t hits = 0;
    uint64_t misses = 0;
    uint64_t evictions = 0;
    uint64_t insertions = 0;
  };
  
  Stats GetStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return {capacity_bytes_, used_bytes_, cache_map_.size(), 
            hits_, misses_, evictions_, insertions_};
  }
  
  void ResetStats() {
    std::lock_guard<std::mutex> lock(mutex_);
    hits_ = misses_ = evictions_ = insertions_ = 0;
  }
  
  // Set capacity and evict if necessary
  void SetCapacity(size_t capacity_bytes);

 private:
  void EvictLRU(size_t required_bytes);
  
  mutable std::mutex mutex_;
  size_t capacity_bytes_;
  size_t used_bytes_;
  
  // LRU list - most recently used at front
  std::list<std::string> lru_list_;
  
  // Cache map: key -> {iterator, cached_block}
  std::unordered_map<std::string, std::pair<
      std::list<std::string>::iterator,
      std::shared_ptr<CachedBlock>>> cache_map_;
  
  // Statistics
  uint64_t hits_ = 0;
  uint64_t misses_ = 0;
  uint64_t evictions_ = 0;
  uint64_t insertions_ = 0;
};

// Global block cache instance (singleton pattern)
class BlockCacheManager {
 public:
  static BlockCacheManager& Instance() {
    static BlockCacheManager instance;
    return instance;
  }
  
  BlockCache* GetCache(const std::string& db_path) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = caches_.find(db_path);
    if (it == caches_.end()) {
      auto cache = std::make_unique<BlockCache>();
      auto* ptr = cache.get();
      caches_[db_path] = std::move(cache);
      return ptr;
    }
    return it->second.get();
  }
  
  void ClearAll() {
    std::lock_guard<std::mutex> lock(mutex_);
    caches_.clear();
  }

 private:
  BlockCacheManager() = default;
  std::mutex mutex_;
  std::unordered_map<std::string, std::unique_ptr<BlockCache>> caches_;
};

}  // namespace cedar

#endif  // FERN_STORAGE_BLOCK_CACHE_H_
