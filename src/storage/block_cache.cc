// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Block Cache implementation

#include "cedar/storage/block_cache.h"

namespace cedar {

std::shared_ptr<CachedBlock> BlockCache::Get(const std::string& key) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = cache_map_.find(key);
  if (it != cache_map_.end()) {
    // Move to front (MRU)
    lru_list_.erase(it->second.first);
    lru_list_.push_front(key);
    it->second.first = lru_list_.begin();
    it->second.second->access_count++;
    hits_++;
    return it->second.second;
  }
  
  misses_++;
  return nullptr;
}

void BlockCache::Insert(const std::string& key, std::string data) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  // Check if already exists
  auto it = cache_map_.find(key);
  if (it != cache_map_.end()) {
    // Update existing entry
    used_bytes_ -= it->second.second->size;
    lru_list_.erase(it->second.first);
    cache_map_.erase(it);
  }
  
  size_t entry_size = key.size() + data.size();
  
  // Evict if necessary
  if (entry_size > capacity_bytes_) {
    // Entry too large to cache
    return;
  }
  
  while (used_bytes_ + entry_size > capacity_bytes_ && !lru_list_.empty()) {
    EvictLRU(entry_size);
  }
  
  // Insert new entry
  lru_list_.push_front(key);
  auto cached = std::make_shared<CachedBlock>(key, std::move(data));
  cache_map_[key] = {lru_list_.begin(), cached};
  used_bytes_ += cached->size;
  insertions_++;
}

void BlockCache::Clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  cache_map_.clear();
  lru_list_.clear();
  used_bytes_ = 0;
}

void BlockCache::SetCapacity(size_t capacity_bytes) {
  std::lock_guard<std::mutex> lock(mutex_);
  capacity_bytes_ = capacity_bytes;
  while (used_bytes_ > capacity_bytes_ && !lru_list_.empty()) {
    EvictLRU(0);
  }
}

void BlockCache::EvictLRU(size_t required_bytes) {
  if (lru_list_.empty()) return;
  
  // Evict from tail (LRU)
  const std::string& evict_key = lru_list_.back();
  auto it = cache_map_.find(evict_key);
  
  if (it != cache_map_.end()) {
    used_bytes_ -= it->second.second->size;
    cache_map_.erase(it);
    lru_list_.pop_back();
    evictions_++;
  }
}

}  // namespace cedar
