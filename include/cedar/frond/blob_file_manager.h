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

#ifndef FERN_BLOB_FILE_MANAGER_H_
#define FERN_BLOB_FILE_MANAGER_H_

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "cedar/core/slice.h"
#include "cedar/core/status.h"

#include "cedar/core/env.h"

namespace cedar {

// Blob 文件中的条目头
struct BlobEntryHeader {
  uint32_t size;        // 数据大小
  uint32_t checksum;    // CRC32 校验
  uint8_t compression;  // 压缩类型
  uint8_t reserved[3];  // 保留
  
  static constexpr size_t kHeaderSize = 12;
};

// Blob 文件管理器 - 处理大对象存储
class BlobFileManager {
 public:
  struct Config {
    std::string blob_dir;           // Blob 文件目录
    size_t max_blob_file_size;      // 单个 blob 文件最大大小 (默认 256MB)
    size_t min_blob_size;           // 最小 blob 大小 (默认 256B)
    
    Config()
        : max_blob_file_size(256 * 1024 * 1024),
          min_blob_size(256) {}
  };

  // 创建 BlobFileManager
  static Status Open(const Config& config, cedar::Env* env,
                     std::unique_ptr<BlobFileManager>* manager);

  ~BlobFileManager();

  // 禁止拷贝
  BlobFileManager(const BlobFileManager&) = delete;
  BlobFileManager& operator=(const BlobFileManager&) = delete;

  // 写入大对象，返回 (file_id, offset)
  // 如果数据太小 (< min_blob_size)，返回 Status::NotSupported
  Status WriteBlob(const Slice& data, 
                   uint32_t* file_id, 
                   uint32_t* offset,
                   uint32_t* size);

  // 读取大对象
  Status ReadBlob(uint32_t file_id, 
                  uint32_t offset, 
                  uint32_t size,
                  std::string* data);

  // 获取当前 blob 文件大小（用于估算）
  size_t GetCurrentBlobSize() const;

  // 强制同步当前 blob 文件
  Status Sync();

 private:
  explicit BlobFileManager(const Config& config, cedar::Env* env);

  // 打开新的 blob 文件
  Status OpenNewBlobFile();

  // 获取 blob 文件路径
  std::string GetBlobFilePath(uint32_t file_id) const;

  Config config_;
  cedar::Env* env_;
  
  // 当前写入的 blob 文件
  uint32_t current_file_id_;
  WritableFile* current_file_;
  size_t current_file_size_;
  
  // 已打开的读取文件缓存
  mutable std::mutex mutex_;
  std::unordered_map<uint32_t, RandomAccessFile*> read_files_;
};

}  // namespace cedar

#endif  // FERN_BLOB_FILE_MANAGER_H_
