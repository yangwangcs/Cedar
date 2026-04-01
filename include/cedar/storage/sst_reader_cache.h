// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// SST Reader Cache - LRU cache for opened SST file handles

#ifndef FERN_STORAGE_SST_READER_CACHE_H_
#define FERN_STORAGE_SST_READER_CACHE_H_

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <list>

#include "cedar/frond/zone_columnar_format.h"

namespace cedar {

// SST Reader 缓存项
struct CachedSstReader {
  std::string file_path;
  std::shared_ptr<ZoneColumnarSstReader> reader;
  size_t ref_count = 0;
  
  // OPTIMIZATION: P1 - 热数据标记
  bool is_hot = false;           // 是否是热数据文件
  uint64_t hit_count = 0;        // 命中次数
  std::chrono::steady_clock::time_point last_access;
  
  // OPTIMIZATION: P1 - 预加载标记
  bool data_preloaded = false;   // 是否已预加载数据
  
  CachedSstReader(const std::string& path, std::shared_ptr<ZoneColumnarSstReader> r)
      : file_path(path), reader(std::move(r)), last_access(std::chrono::steady_clock::now()) {}
};

// LRU SST Reader 缓存
class SstReaderCache {
 public:
  explicit SstReaderCache(size_t capacity = 16) : capacity_(capacity) {}
  
  ~SstReaderCache() {
    Clear();
  }
  
  // 获取或创建 Reader
  std::shared_ptr<ZoneColumnarSstReader> Get(const std::string& file_path);
  
  // 释放 Reader（减少引用计数）
  void Release(const std::string& file_path);
  
  // 清除所有缓存
  void Clear();
  
  // 获取当前缓存大小
  size_t Size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cache_map_.size();
  }
  
  // 获取缓存容量
  size_t Capacity() const { return capacity_; }
  
  // 设置缓存容量
  void SetCapacity(size_t capacity);
  
  // 获取统计信息
  struct Stats {
    uint64_t hits = 0;
    uint64_t misses = 0;
    uint64_t evictions = 0;
    uint64_t preloads = 0;  // 预加载次数
  };
  
  Stats GetStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return stats_;
  }
  
  void ResetStats() {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_ = Stats{};
  }
  
  // OPTIMIZATION: P1 - 标记热数据文件
  void MarkAsHot(const std::string& file_path);
  
  // OPTIMIZATION: P1 - 预加载指定文件
  bool Preload(const std::string& file_path);
  
  // OPTIMIZATION: P1 - 批量预加载多个文件
  void PreloadFiles(const std::vector<std::string>& file_paths);
  
  // OPTIMIZATION: P1 - 获取热数据文件列表
  std::vector<std::string> GetHotFiles() const;
  
  // OPTIMIZATION: P1 - 清理冷数据（长时间未访问的非热数据）
  size_t EvictColdData(std::chrono::seconds idle_threshold);

 private:
  void EvictLRU();
  
  mutable std::mutex mutex_;
  size_t capacity_;
  
  // LRU 列表 - 最近使用的在前面
  std::list<std::string> lru_list_;
  
  // 缓存映射
  std::unordered_map<std::string, std::pair<
      std::list<std::string>::iterator, 
      std::shared_ptr<CachedSstReader>>> cache_map_;
  
  Stats stats_;
};

}  // namespace cedar

#endif  // FERN_STORAGE_SST_READER_CACHE_H_
