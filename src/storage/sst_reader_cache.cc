// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// SST Reader Cache implementation

#include "cedar/storage/sst_reader_cache.h"

namespace cedar {

std::shared_ptr<ZoneColumnarSstReader> SstReaderCache::Get(const std::string& file_path) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = cache_map_.find(file_path);
  if (it != cache_map_.end()) {
    // 缓存命中 - 移动到 LRU 列表前面
    lru_list_.erase(it->second.first);
    lru_list_.push_front(file_path);
    it->second.first = lru_list_.begin();
    it->second.second->ref_count++;
    stats_.hits++;
    return it->second.second->reader;
  }
  
  // 缓存未命中 - 创建新的 Reader
  stats_.misses++;
  
  auto reader = std::make_shared<ZoneColumnarSstReader>(file_path);
  Status s = reader->Open();
  if (!s.ok()) {
    return nullptr;
  }
  
  // 如果需要，淘汰旧项
  while (cache_map_.size() >= capacity_) {
    EvictLRU();
  }
  
  // 添加到缓存
  lru_list_.push_front(file_path);
  auto cached = std::make_shared<CachedSstReader>(file_path, reader);
  cached->ref_count = 1;
  cache_map_[file_path] = {lru_list_.begin(), cached};
  
  return reader;
}

void SstReaderCache::Release(const std::string& file_path) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = cache_map_.find(file_path);
  if (it != cache_map_.end()) {
    if (it->second.second->ref_count > 0) {
      it->second.second->ref_count--;
    }
  }
}

void SstReaderCache::Clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  cache_map_.clear();
  lru_list_.clear();
}

void SstReaderCache::SetCapacity(size_t capacity) {
  std::lock_guard<std::mutex> lock(mutex_);
  capacity_ = capacity;
  while (cache_map_.size() > capacity_) {
    EvictLRU();
  }
}

void SstReaderCache::EvictLRU() {
  if (lru_list_.empty()) return;
  
  // 从 LRU 列表尾部淘汰（最久未使用）
  const std::string& evict_path = lru_list_.back();
  auto it = cache_map_.find(evict_path);
  
  if (it != cache_map_.end()) {
    // 只有引用计数为 0 时才真正淘汰
    if (it->second.second->ref_count == 0) {
      cache_map_.erase(it);
      lru_list_.pop_back();
      stats_.evictions++;
    } else {
      // 如果引用计数不为 0，移到前面，稍后淘汰
      lru_list_.pop_back();
      lru_list_.push_front(evict_path);
      it->second.first = lru_list_.begin();
    }
  }
}

// OPTIMIZATION: P1 - 标记热数据文件
void SstReaderCache::MarkAsHot(const std::string& file_path) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto it = cache_map_.find(file_path);
  if (it != cache_map_.end()) {
    it->second.second->is_hot = true;
  }
}

// OPTIMIZATION: P1 - 预加载指定文件
bool SstReaderCache::Preload(const std::string& file_path) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  // 检查是否已缓存
  auto it = cache_map_.find(file_path);
  if (it != cache_map_.end()) {
    it->second.second->hit_count++;
    it->second.second->last_access = std::chrono::steady_clock::now();
    return true;  // 已在缓存中
  }
  
  // 创建新的 Reader
  auto reader = std::make_shared<ZoneColumnarSstReader>(file_path);
  Status s = reader->Open();
  if (!s.ok()) {
    return false;
  }
  
  // 如果需要，淘汰旧项
  while (cache_map_.size() >= capacity_) {
    EvictLRU();
  }
  
  // 添加到缓存并标记为预加载
  lru_list_.push_front(file_path);
  auto cached = std::make_shared<CachedSstReader>(file_path, reader);
  cached->data_preloaded = true;
  cache_map_[file_path] = {lru_list_.begin(), cached};
  
  stats_.preloads++;
  return true;
}

// OPTIMIZATION: P1 - 批量预加载多个文件
void SstReaderCache::PreloadFiles(const std::vector<std::string>& file_paths) {
  for (const auto& path : file_paths) {
    Preload(path);
  }
}

// OPTIMIZATION: P1 - 获取热数据文件列表
std::vector<std::string> SstReaderCache::GetHotFiles() const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::vector<std::string> hot_files;
  for (const auto& [path, cached] : cache_map_) {
    if (cached.second->is_hot) {
      hot_files.push_back(path);
    }
  }
  return hot_files;
}

// OPTIMIZATION: P1 - 清理冷数据
size_t SstReaderCache::EvictColdData(std::chrono::seconds idle_threshold) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto now = std::chrono::steady_clock::now();
  size_t evicted = 0;
  
  auto it = lru_list_.begin();
  while (it != lru_list_.end() && cache_map_.size() > capacity_ / 2) {
    auto map_it = cache_map_.find(*it);
    if (map_it != cache_map_.end()) {
      auto& cached = map_it->second.second;
      // 只清理非热数据且长时间未访问的文件
      if (!cached->is_hot && cached->ref_count == 0) {
        auto idle_time = std::chrono::duration_cast<std::chrono::seconds>(
            now - cached->last_access);
        if (idle_time > idle_threshold) {
          it = lru_list_.erase(it);
          cache_map_.erase(map_it);
          stats_.evictions++;
          evicted++;
          continue;
        }
      }
    }
    ++it;
  }
  
  return evicted;
}

}  // namespace cedar
