// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// SST Reader Cache - LRU cache for opened SST file handles
//
// Task 3.4：缓存 key 改为 file_number（file_number 是 SST 的稳定身份，路径可迁移）；
// 修复"容量满且所有 reader 都被 pin 住"时的忙等：Get 返回 shared_ptr，淘汰 map
// 条目不会影响仍在使用的 reader，因此 EvictLRU 总是能真正淘汰，不存在死循环。
// 允许短时间内超过容量（有上限），不做忙等。

#ifndef FERN_STORAGE_SST_READER_CACHE_H_
#define FERN_STORAGE_SST_READER_CACHE_H_

#include <chrono>
#include <cstdint>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "cedar/frond/zone_columnar_format.h"

namespace cedar {

// SST Reader 缓存项
struct CachedSstReader {
  uint64_t file_number = 0;
  std::string file_path;
  std::shared_ptr<ZoneColumnarSstReader> reader;

  bool is_hot = false;
  uint64_t hit_count = 0;
  std::chrono::steady_clock::time_point last_access;

  CachedSstReader(uint64_t number, const std::string& path,
                  std::shared_ptr<ZoneColumnarSstReader> r)
      : file_number(number),
        file_path(path),
        reader(std::move(r)),
        last_access(std::chrono::steady_clock::now()) {}
};

// LRU SST Reader 缓存（按 file_number 索引）
class SstReaderCache {
 public:
  explicit SstReaderCache(size_t capacity = 16) : capacity_(capacity) {}

  ~SstReaderCache() { Clear(); }

  // Task 3.4：主入口——按 file_number + path 获取（缓存未命中时打开并缓存）。
  std::shared_ptr<ZoneColumnarSstReader> Get(uint64_t file_number,
                                             const std::string& file_path);

  // 兼容旧 API：从路径解析 file_number 后走上面的入口；解析失败时直接打开
  // （不缓存），保证旧调用方功能不变。
  std::shared_ptr<ZoneColumnarSstReader> Get(const std::string& file_path);

  // 兼容旧 API：现在 reader 由 shared_ptr 保活，Release 只更新统计。
  void Release(const std::string& file_path);
  void Release(uint64_t file_number);

  void Clear();

  size_t Size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return cache_map_.size();
  }

  size_t Capacity() const { return capacity_; }
  void SetCapacity(size_t capacity);

  struct Stats {
    uint64_t hits = 0;
    uint64_t misses = 0;
    uint64_t evictions = 0;
    uint64_t preloads = 0;
  };

  Stats GetStats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return stats_;
  }

  void ResetStats() {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_ = Stats{};
  }

  void MarkAsHot(const std::string& file_path);
  bool Preload(const std::string& file_path);
  void PreloadFiles(const std::vector<std::string>& file_paths);
  std::vector<std::string> GetHotFiles() const;
  size_t EvictColdData(std::chrono::seconds idle_threshold);

  // 从 "123.frond" 解析文件号；失败返回 false。
  static bool ParseFileNumber(const std::string& file_path, uint64_t* number);

 private:
  // 只做真正的淘汰：条目总是可从 map 移除（reader 由 shared_ptr 保活），返回是否淘汰。
  bool EvictLRU();

  mutable std::mutex mutex_;
  size_t capacity_;
  size_t max_overflow_ = 1024;  // 打开失败的并发场景下允许的临时超容量上限

  std::list<uint64_t> lru_list_;  // 最近使用在前
  std::unordered_map<uint64_t, std::pair<std::list<uint64_t>::iterator,
                                         std::shared_ptr<CachedSstReader>>>
      cache_map_;
  Stats stats_;
};

}  // namespace cedar

#endif  // FERN_STORAGE_SST_READER_CACHE_H_
