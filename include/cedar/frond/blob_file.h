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
// Blob File - 大值外部存储文件
// =============================================================================
// 与 SST 文件 1:1 映射：sst_000001.frond <-> sst_000001.blob
// 所有 Entry 4KB 对齐，支持 O_DIRECT 和预读
// =============================================================================

#ifndef FERN_BLOB_FILE_H_
#define FERN_BLOB_FILE_H_

#include <cstdint>
#include <string>
#include <memory>
#include <vector>
#include <mutex>

#include "cedar/core/status.h"
#include "cedar/core/slice.h"

namespace cedar {

// Blob 文件魔数
static constexpr uint32_t kBlobMagic = 0x424C4200;  // "BLB\0"
static constexpr uint32_t kBlobVersion = 1;
static constexpr uint32_t kBlobBlockSize = 4096;    // 4KB 对齐
static constexpr uint32_t kBlobHeaderSize = 4096;   // Header 占 1 个 Block

// Blob 文件 Header
#pragma pack(push, 1)
struct BlobHeader {
  uint32_t magic = kBlobMagic;
  uint32_t version = kBlobVersion;
  uint32_t sst_id = 0;           // 对应 SST 文件 ID
  uint32_t block_size = kBlobBlockSize;
  uint32_t entry_count = 0;      // Entry 数量
  uint32_t reserved1 = 0;
  uint64_t data_size = 0;        // 实际数据总大小（不含 padding）
  uint64_t checksum = 0;         // Header 校验
  uint8_t  reserved2[4056] = {}; // 填充到 4KB (4096 - 40 = 4056)
  
  static constexpr size_t kEncodedSize = 4096;
  
  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& input);
};
static_assert(sizeof(BlobHeader) == 4096, "BlobHeader must be 4096 bytes");
#pragma pack(pop)

// Blob Entry 信息（内存中管理）
struct BlobEntryInfo {
  uint32_t offset;       // 在 Blob 文件中的偏移
  uint32_t actual_size;  // 实际数据大小
  uint32_t aligned_size; // 对齐后大小（4KB 倍数）
};

// =============================================================================
// BlobFileWriter - 追加写入 Blob 文件
// =============================================================================
class BlobFileWriter {
 public:
  BlobFileWriter(const std::string& path, uint32_t sst_id);
  ~BlobFileWriter();

  // 禁止拷贝
  BlobFileWriter(const BlobFileWriter&) = delete;
  BlobFileWriter& operator=(const BlobFileWriter&) = delete;

  // 打开文件（创建或追加）
  Status Open();
  
  // 关闭文件
  Status Close();
  
  // 追加写入数据（自动 4KB 对齐）
  // 返回：写入后的偏移（用于构建 Descriptor）
  Status Append(const Slice& data, uint32_t* out_offset);
  
  // 同步到磁盘
  Status Sync();
  
  // 获取当前文件大小
  uint64_t FileSize() const { return file_size_; }
  
  // 获取写入的 Entry 数量
  uint32_t EntryCount() const { return entry_count_; }

 private:
  // 写入 Header
  Status WriteHeader();
  
  // 更新 Header（entry_count 等）
  Status UpdateHeader();
  
  // 对齐写入
  Status WriteAligned(const char* data, size_t size, uint32_t alignment);

  std::string path_;
  uint32_t sst_id_;
  int fd_ = -1;
  uint64_t file_size_ = 0;
  uint64_t data_size_ = 0;  // 实际数据大小（不含 padding）
  uint32_t entry_count_ = 0;
  bool opened_ = false;
};

// =============================================================================
// BlobFileReader - 读取 Blob 文件
// =============================================================================
class BlobFileReader {
 public:
  explicit BlobFileReader(const std::string& path);
  ~BlobFileReader();

  // 禁止拷贝
  BlobFileReader(const BlobFileReader&) = delete;
  BlobFileReader& operator=(const BlobFileReader&) = delete;

  // 打开文件
  Status Open();
  
  // 关闭文件
  void Close();
  
  // 读取指定偏移的数据
  // offset: 4KB 对齐的偏移
  // size: 要读取的大小（字节）
  // buffer: 输出缓冲区，需要至少 size 字节
  Status Read(uint32_t offset, uint32_t size, char* buffer);
  
  // 批量预读（优化顺序扫描）
  // offsets: 要预读的偏移列表（会被排序和合并）
  Status Prefetch(const std::vector<uint32_t>& offsets);
  
  // 获取文件大小
  uint64_t FileSize() const { return file_size_; }
  
  // 获取 Entry 数量
  uint32_t EntryCount() const { return header_.entry_count; }

 private:
  // 读取 Header
  Status ReadHeader();
  
  // 验证 Header
  Status ValidateHeader();

  std::string path_;
  int fd_ = -1;
  uint64_t file_size_ = 0;
  BlobHeader header_;
  bool opened_ = false;
};

}  // namespace cedar

#endif  // FERN_BLOB_FILE_H_
