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

#include "cedar/frond/blob_file_manager.h"

#include <cstring>
#include <string>
#include <machine/endian.h>

#include "cedar/core/env.h"
#include "cedar/core/crc32c.h"

namespace cedar {

// 魔数和版本
static constexpr uint32_t kBlobMagic = 0x424C4F42;  // "BLOB"
static constexpr uint32_t kBlobVersion = 1;

// Blob 文件头
struct BlobFileHeader {
  uint32_t magic;
  uint32_t version;
  uint32_t reserved;
  
  void EncodeTo(std::string* dst) const {
    char buf[12];
    memcpy(buf, &magic, 4);
    memcpy(buf + 4, &version, 4);
    memcpy(buf + 8, &reserved, 4);
    dst->append(buf, 12);
  }
  
  bool DecodeFrom(const char* data) {
    memcpy(&magic, data, 4);
    memcpy(&version, data + 4, 4);
    memcpy(&reserved, data + 8, 4);
    return magic == kBlobMagic && version == kBlobVersion;
  }
};

Status BlobFileManager::Open(const Config& config, cedar::Env* env,
                             std::unique_ptr<BlobFileManager>* manager) {
  if (!env) {
    return Status::InvalidArgument("BlobFileManager", "env is null");
  }
  
  // 创建 blob 目录
  if (!env->FileExists(config.blob_dir)) {
    auto s = env->CreateDir(config.blob_dir);
    if (!s.ok()) {
      return Status::IOError("BlobFileManager", s.ToString());
    }
  }
  
  manager->reset(new BlobFileManager(config, env));
  
  // 查找已有的 blob 文件，确定下一个 file_id
  std::vector<std::string> files;
  env->GetChildren(config.blob_dir, &files);
  
  uint32_t max_id = 0;
  for (const auto& file : files) {
    if (file.size() > 5 && file.substr(file.size() - 5) == ".blob") {
      // 解析文件名中的数字ID (不使用异常)
      uint32_t id = 0;
      bool valid = true;
      for (size_t i = 0; i < file.size() - 5; ++i) {
        char c = file[i];
        if (c < '0' || c > '9') {
          valid = false;
          break;
        }
        id = id * 10 + (c - '0');
      }
      if (valid) {
        max_id = std::max(max_id, id);
      }
    }
  }
  
  (*manager)->current_file_id_ = max_id;
  
  // 打开新的 blob 文件
  auto s = (*manager)->OpenNewBlobFile();
  if (!s.ok()) {
    manager->reset();
    return s;
  }
  
  return Status::OK();
}

BlobFileManager::BlobFileManager(const Config& config, cedar::Env* env)
    : config_(config),
      env_(env),
      current_file_id_(0),
      current_file_(nullptr),
      current_file_size_(0) {}

BlobFileManager::~BlobFileManager() {
  if (current_file_) {
    current_file_->Sync();
    delete current_file_;
  }
  
  for (auto& [id, file] : read_files_) {
    delete file;
  }
}

Status BlobFileManager::OpenNewBlobFile() {
  if (current_file_) {
    current_file_->Sync();
    delete current_file_;
  }
  
  current_file_id_++;
  std::string path = GetBlobFilePath(current_file_id_);
  
  cedar::Status s = env_->NewWritableFile(path, &current_file_);
  if (!s.ok()) {
    return Status::IOError("BlobFileManager", s.ToString());
  }
  
  // 写入文件头
  BlobFileHeader header;
  header.magic = kBlobMagic;
  header.version = kBlobVersion;
  header.reserved = 0;
  
  std::string header_data;
  header.EncodeTo(&header_data);
  
  s = current_file_->Append(header_data);
  if (!s.ok()) {
    delete current_file_;
    current_file_ = nullptr;
    return Status::IOError("BlobFileManager", s.ToString());
  }
  
  current_file_size_ = 12;  // 文件头大小
  return Status::OK();
}

Status BlobFileManager::WriteBlob(const Slice& data, 
                                  uint32_t* file_id, 
                                  uint32_t* offset,
                                  uint32_t* size) {
  if (data.size() < config_.min_blob_size) {
    return Status::NotSupported("BlobFileManager", "data too small for blob");
  }
  
  if (data.size() > UINT32_MAX - BlobEntryHeader::kHeaderSize) {
    return Status::InvalidArgument("BlobFileManager", "data too large");
  }
  
  std::lock_guard<std::mutex> lock(mutex_);
  
  // 检查是否需要创建新文件
  uint32_t entry_total_size = static_cast<uint32_t>(BlobEntryHeader::kHeaderSize + data.size());
  if (current_file_size_ + entry_total_size > config_.max_blob_file_size) {
    auto s = OpenNewBlobFile();
    if (!s.ok()) {
      return s;
    }
  }
  
  // 准备条目头
  BlobEntryHeader entry_header;
  entry_header.size = static_cast<uint32_t>(data.size());
  entry_header.checksum = cedar::crc32c::Value(data.data(), data.size());
  entry_header.compression = 0;  // 无压缩
  memset(entry_header.reserved, 0, 3);
  
  // 写入条目头
  char header_buf[BlobEntryHeader::kHeaderSize];
  memcpy(header_buf, &entry_header.size, 4);
  memcpy(header_buf + 4, &entry_header.checksum, 4);
  memcpy(header_buf + 8, &entry_header.compression, 1);
  memcpy(header_buf + 9, entry_header.reserved, 3);
  
  cedar::Status s = current_file_->Append(
      cedar::Slice(header_buf, BlobEntryHeader::kHeaderSize));
  if (!s.ok()) {
    return Status::IOError("BlobFileManager", s.ToString());
  }
  
  // 写入数据
  s = current_file_->Append(cedar::Slice(data.data(), data.size()));
  if (!s.ok()) {
    return Status::IOError("BlobFileManager", s.ToString());
  }
  
  // 返回位置信息
  // offset是相对于blob数据区域的偏移（不包括12字节文件头）
  *file_id = current_file_id_;
  *offset = static_cast<uint32_t>(current_file_size_ - 12);  // 减去文件头
  *size = entry_header.size;  // 返回实际数据大小，不是总大小
  
  current_file_size_ += entry_total_size;
  
  // Flush to ensure data is visible to concurrent readers
  cedar::Status flush_status = current_file_->Flush();
  if (!flush_status.ok()) {
    return Status::IOError("BlobFileManager", flush_status.ToString());
  }
  
  return Status::OK();
}

Status BlobFileManager::ReadBlob(uint32_t file_id, 
                                 uint32_t offset, 
                                 uint32_t size,
                                 std::string* data) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  // 获取或打开 blob 文件
  RandomAccessFile* file = nullptr;
  auto it = read_files_.find(file_id);
  if (it != read_files_.end()) {
    file = it->second;
  } else {
    std::string path = GetBlobFilePath(file_id);
    cedar::Status s = env_->NewRandomAccessFile(path, &file);
    if (!s.ok()) {
      return Status::IOError("BlobFileManager", s.ToString());
    }
    read_files_[file_id] = file;
  }
  
  // 跳过文件头 (12 bytes) + offset
  uint64_t read_offset = 12 + offset;
  
  // 读取条目头
  // 注意：使用堆分配的缓冲区，因为某些 RandomAccessFile 实现（如 PosixMmapReadableFile）
  // 会返回指向内部缓冲区的 Slice，而不会复制到 scratch 缓冲区
  std::string header_buf;
  header_buf.resize(BlobEntryHeader::kHeaderSize);
  cedar::Slice result;
  cedar::Status s = file->Read(read_offset, BlobEntryHeader::kHeaderSize, 
                                  &result, &header_buf[0]);
  if (!s.ok()) {
    return Status::IOError("BlobFileManager", s.ToString());
  }
  
  // 使用 result 中的数据（可能是 mmap 内存或 scratch 缓冲区）
  const char* header_data = result.data();
  
  BlobEntryHeader entry_header;
  memcpy(&entry_header.size, header_data, 4);
  memcpy(&entry_header.checksum, header_data + 4, 4);
  memcpy(&entry_header.compression, header_data + 8, 1);
  
  // 读取数据
  // 注意：对于 PosixMmapReadableFile，Read 返回的 Slice 指向 mmap 内存
  // 需要检查 result 的大小，如果匹配则直接使用，否则需要复制
  std::string temp_buf;
  temp_buf.resize(entry_header.size);
  s = file->Read(read_offset + BlobEntryHeader::kHeaderSize, 
                 entry_header.size, &result, &temp_buf[0]);
  if (!s.ok()) {
    return Status::IOError("BlobFileManager", s.ToString());
  }
  
  // 使用 result 中的数据（可能是 mmap 内存或 scratch 缓冲区）
  if (result.size() == entry_header.size) {
    data->assign(result.data(), result.size());
  } else {
    // 读取失败或返回了不同的数据大小
    return Status::IOError("BlobFileManager", "short read");
  }
  
  // 校验 CRC
  uint32_t actual_checksum = cedar::crc32c::Value(data->data(), data->size());
  if (actual_checksum != entry_header.checksum) {
    return Status::Corruption("BlobFileManager", "blob checksum mismatch");
  }
  
  return Status::OK();
}

size_t BlobFileManager::GetCurrentBlobSize() const {
  return current_file_size_;
}

Status BlobFileManager::Sync() {
  if (current_file_) {
    cedar::Status s = current_file_->Sync();
    if (!s.ok()) {
      return Status::IOError("BlobFileManager", s.ToString());
    }
  }
  return Status::OK();
}

std::string BlobFileManager::GetBlobFilePath(uint32_t file_id) const {
  return config_.blob_dir + "/" + std::to_string(file_id) + ".blob";
}

}  // namespace cedar
