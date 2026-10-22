// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// SST Reader Cache implementation (Task 3.4)

#include "cedar/storage/sst_reader_cache.h"

#include <algorithm>

namespace cedar {

bool SstReaderCache::ParseFileNumber(const std::string& file_path, uint64_t* number) {
  if (number == nullptr) return false;
  const size_t slash = file_path.find_last_of('/');
  const std::string base =
      slash == std::string::npos ? file_path : file_path.substr(slash + 1);
  const size_t dot = base.find('.');
  const std::string digits = base.substr(0, dot);
  if (digits.empty()) return false;
  uint64_t value = 0;
  for (char c : digits) {
    if (c < '0' || c > '9') return false;
    value = value * 10 + static_cast<uint64_t>(c - '0');
  }
  *number = value;
  return true;
}

std::shared_ptr<ZoneColumnarSstReader> SstReaderCache::Get(
    uint64_t file_number, const std::string& file_path) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = cache_map_.find(file_number);
    if (it != cache_map_.end()) {
      lru_list_.erase(it->second.first);
      lru_list_.push_front(file_number);
      it->second.first = lru_list_.begin();
      it->second.second->hit_count++;
      it->second.second->last_access = std::chrono::steady_clock::now();
      stats_.hits++;
      return it->second.second->reader;
    }
    stats_.misses++;
  }

  // 在锁外打开文件（Open 可能做 mmap/fstat；不阻塞其它 reader 命中）
  auto reader = std::make_shared<ZoneColumnarSstReader>(file_path);
  Status s = reader->Open();
  if (!s.ok()) {
    return nullptr;
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    // 双重检查：并发 miss 时另一线程可能已插入
    auto it = cache_map_.find(file_number);
    if (it != cache_map_.end()) {
      return it->second.second->reader;
    }

    // 不忙等：条目总是可从 map 淘汰（reader 由 shared_ptr 保活）。
    // 仅当超过硬上限（异常情况）时不再插入，避免无界增长。
    if (cache_map_.size() >= max_overflow_) {
      return reader;
    }
    while (cache_map_.size() >= capacity_ && !cache_map_.empty()) {
      if (!EvictLRU()) break;  // 防御：理论上不会发生
    }

    lru_list_.push_front(file_number);
    auto cached = std::make_shared<CachedSstReader>(file_number, file_path, reader);
    cache_map_[file_number] = {lru_list_.begin(), cached};
  }
  return reader;
}

std::shared_ptr<ZoneColumnarSstReader> SstReaderCache::Get(const std::string& file_path) {
  uint64_t file_number = 0;
  if (ParseFileNumber(file_path, &file_number)) {
    return Get(file_number, file_path);
  }
  auto reader = std::make_shared<ZoneColumnarSstReader>(file_path);
  if (!reader->Open().ok()) return nullptr;
  return reader;
}

void SstReaderCache::Release(const std::string& file_path) {
  uint64_t file_number = 0;
  if (ParseFileNumber(file_path, &file_number)) Release(file_number);
}

void SstReaderCache::Release(uint64_t file_number) {
  // reader 由 shared_ptr 保活；这里只更新最近访问时间（保留 API 兼容）。
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = cache_map_.find(file_number);
  if (it != cache_map_.end()) {
    it->second.second->last_access = std::chrono::steady_clock::now();
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
    if (!EvictLRU()) break;
  }
}

bool SstReaderCache::EvictLRU() {
  if (lru_list_.empty() || cache_map_.empty()) return false;
  const uint64_t victim = lru_list_.back();
  lru_list_.pop_back();
  auto it = cache_map_.find(victim);
  if (it == cache_map_.end()) return true;  // 列表与 map 暂不一致：已移除，继续
  cache_map_.erase(it);
  stats_.evictions++;
  return true;
}

void SstReaderCache::MarkAsHot(const std::string& file_path) {
  uint64_t file_number = 0;
  if (!ParseFileNumber(file_path, &file_number)) return;
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = cache_map_.find(file_number);
  if (it != cache_map_.end()) {
    it->second.second->is_hot = true;
  }
}

bool SstReaderCache::Preload(const std::string& file_path) {
  auto reader = Get(file_path);
  if (!reader) return false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stats_.preloads++;
  }
  return true;
}

void SstReaderCache::PreloadFiles(const std::vector<std::string>& file_paths) {
  for (const auto& path : file_paths) {
    Preload(path);
  }
}

std::vector<std::string> SstReaderCache::GetHotFiles() const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<std::string> hot_files;
  for (const auto& [number, cached] : cache_map_) {
    if (cached.second->is_hot) {
      hot_files.push_back(cached.second->file_path);
    }
  }
  return hot_files;
}

size_t SstReaderCache::EvictColdData(std::chrono::seconds idle_threshold) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto now = std::chrono::steady_clock::now();
  size_t evicted = 0;

  for (auto it = lru_list_.begin(); it != lru_list_.end() &&
                                     cache_map_.size() > capacity_ / 2;) {
    const uint64_t number = *it;
    auto map_it = cache_map_.find(number);
    if (map_it == cache_map_.end()) {
      it = lru_list_.erase(it);
      continue;
    }
    auto& cached = map_it->second.second;
    if (!cached->is_hot) {
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
    ++it;
  }
  return evicted;
}

}  // namespace cedar
