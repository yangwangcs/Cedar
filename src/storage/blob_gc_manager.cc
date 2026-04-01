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

#include "cedar/storage/blob_gc_manager.h"

#include <filesystem>
#include <algorithm>

namespace cedar {

BlobGCManager::BlobGCManager(uint32_t delay_seconds) 
    : delay_seconds_(delay_seconds) {}

BlobGCManager::~BlobGCManager() {
  if (running_) {
    Stop();
  }
}

Status BlobGCManager::Start() {
  if (running_.exchange(true)) {
    return Status::OK();  // 已经在运行
  }
  
  shutdown_ = false;
  bg_thread_ = std::make_unique<std::thread>(&BlobGCManager::BackgroundGC, this);
  
  return Status::OK();
}

Status BlobGCManager::Stop() {
  if (!running_.exchange(false)) {
    return Status::OK();  // 已经停止
  }
  
  shutdown_ = true;
  
  if (bg_thread_ && bg_thread_->joinable()) {
    bg_thread_->join();
  }
  
  bg_thread_.reset();
  
  return Status::OK();
}

void BlobGCManager::OnSSTDeleted(const std::string& blob_path, uint32_t sst_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto delete_time = std::chrono::steady_clock::now() + 
                     std::chrono::seconds(delay_seconds_);
  
  gc_queue_.emplace_back(blob_path, delete_time, sst_id);
}

Status BlobGCManager::RunGC() {
  std::vector<BlobGCItem> to_delete;
  
  {
    std::lock_guard<std::mutex> lock(mutex_);
    
    auto now = std::chrono::steady_clock::now();
    
    // 找出所有到期的条目
    for (auto it = gc_queue_.begin(); it != gc_queue_.end(); ) {
      if (now >= it->delete_time) {
        to_delete.push_back(std::move(*it));
        it = gc_queue_.erase(it);
      } else {
        ++it;
      }
    }
  }
  
  // 删除文件（在锁外执行）
  Status final_status = Status::OK();
  for (const auto& item : to_delete) {
    Status s = DeleteBlobFile(item.blob_path);
    if (!s.ok()) {
      final_status = s;  // 记录最后一个错误
    }
  }
  
  return final_status;
}

Status BlobGCManager::DeleteBlobFile(const std::string& path) {
  try {
    if (std::filesystem::exists(path)) {
      std::filesystem::remove(path);
      deleted_count_++;
      return Status::OK();
    }
    return Status::OK();  // 文件不存在也算成功
  } catch (const std::exception& e) {
    return Status::IOError("BlobGCManager::DeleteBlobFile", e.what());
  }
}

void BlobGCManager::BackgroundGC() {
  while (!shutdown_) {
    // 每 60 秒执行一次 GC
    for (int i = 0; i < 60 && !shutdown_; ++i) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    
    if (!shutdown_) {
      RunGC();
    }
  }
}

size_t BlobGCManager::PendingCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return gc_queue_.size();
}

}  // namespace cedar
