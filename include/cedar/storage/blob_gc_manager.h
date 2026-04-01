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

// =============================================================================
// Blob GC Manager - Blob 文件垃圾回收管理器
// =============================================================================
// SST 被删除后，延迟删除对应的 Blob 文件（防止正在进行的读取）
// =============================================================================

#ifndef FERN_BLOB_GC_MANAGER_H_
#define FERN_BLOB_GC_MANAGER_H_

#include <cstdint>
#include <string>
#include <vector>
#include <queue>
#include <chrono>
#include <mutex>
#include <thread>
#include <atomic>

#include "cedar/core/status.h"

namespace cedar {

// 待删除的 Blob 文件条目
struct BlobGCItem {
  std::string blob_path;                                           // Blob 文件路径
  std::chrono::steady_clock::time_point delete_time;               // 计划删除时间
  uint32_t sst_id;                                                 // SST 文件 ID
  
  BlobGCItem(const std::string& path, 
             std::chrono::steady_clock::time_point time,
             uint32_t sid)
      : blob_path(path), delete_time(time), sst_id(sid) {}
};

// Blob GC 管理器
class BlobGCManager {
 public:
  // delay_seconds: SST 删除后延迟多少秒再删除 Blob 文件
  explicit BlobGCManager(uint32_t delay_seconds = 3600);  // 默认 1 小时
  ~BlobGCManager();

  BlobGCManager(const BlobGCManager&) = delete;
  BlobGCManager& operator=(const BlobGCManager&) = delete;

  // 启动后台 GC 线程
  Status Start();
  
  // 停止 GC 线程
  Status Stop();
  
  // SST 被删除时调用，将 Blob 文件加入延迟删除队列
  void OnSSTDeleted(const std::string& blob_path, uint32_t sst_id);
  
  // 立即执行 GC（删除所有到期的 Blob 文件）
  Status RunGC();
  
  // 获取待删除队列大小
  size_t PendingCount() const;
  
  // 获取已删除的 Blob 文件数量
  uint64_t DeletedCount() const { return deleted_count_.load(); }

 private:
  // 后台 GC 线程函数
  void BackgroundGC();
  
  // 删除单个 Blob 文件
  Status DeleteBlobFile(const std::string& path);

  uint32_t delay_seconds_;
  std::atomic<bool> running_{false};
  std::atomic<bool> shutdown_{false};
  std::atomic<uint64_t> deleted_count_{0};
  
  mutable std::mutex mutex_;
  std::vector<BlobGCItem> gc_queue_;
  
  std::unique_ptr<std::thread> bg_thread_;
};

}  // namespace cedar

#endif  // FERN_BLOB_GC_MANAGER_H_
