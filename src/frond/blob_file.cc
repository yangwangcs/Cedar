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

#include "cedar/frond/blob_file.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>

namespace cedar {

// =============================================================================
// Helper functions
// =============================================================================

static uint32_t AlignUp(uint32_t value, uint32_t alignment) {
  return (value + alignment - 1) & ~(alignment - 1);
}

static uint64_t ComputeCRC64(const void* data, size_t size) {
  // 简单校验和（生产环境应使用 CRC64 或 xxHash）
  const uint8_t* ptr = static_cast<const uint8_t*>(data);
  uint64_t checksum = 0;
  for (size_t i = 0; i < size; ++i) {
    checksum = checksum * 31 + ptr[i];
  }
  return checksum;
}

// =============================================================================
// BlobHeader 实现
// =============================================================================

void BlobHeader::EncodeTo(std::string* dst) const {
  dst->resize(kEncodedSize);
  char* buf = &(*dst)[0];
  
  memcpy(buf, &magic, 4);
  memcpy(buf + 4, &version, 4);
  memcpy(buf + 8, &sst_id, 4);
  memcpy(buf + 12, &block_size, 4);
  memcpy(buf + 16, &entry_count, 4);
  memcpy(buf + 20, &reserved1, 4);
  memcpy(buf + 24, &data_size, 8);
  memcpy(buf + 32, &checksum, 8);
  memset(buf + 40, 0, 4056);
}

Status BlobHeader::DecodeFrom(const Slice& input) {
  if (input.size() < kEncodedSize) {
    return Status::Corruption("BlobHeader", "truncated");
  }
  
  const char* buf = input.data();
  memcpy(&magic, buf, 4);
  memcpy(&version, buf + 4, 4);
  memcpy(&sst_id, buf + 8, 4);
  memcpy(&block_size, buf + 12, 4);
  memcpy(&entry_count, buf + 16, 4);
  memcpy(&reserved1, buf + 20, 4);
  memcpy(&data_size, buf + 24, 8);
  memcpy(&checksum, buf + 32, 8);
  
  if (magic != kBlobMagic) {
    return Status::Corruption("BlobHeader", "invalid magic");
  }
  if (version != kBlobVersion) {
    return Status::Corruption("BlobHeader", "invalid version");
  }
  
  return Status::OK();
}

// =============================================================================
// BlobFileWriter 实现
// =============================================================================

BlobFileWriter::BlobFileWriter(const std::string& path, uint32_t sst_id)
    : path_(path), sst_id_(sst_id) {}

BlobFileWriter::~BlobFileWriter() {
  if (opened_) {
    Close();
  }
}

Status BlobFileWriter::Open() {
  if (opened_) return Status::OK();
  
  fd_ = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd_ < 0) {
    return Status::IOError("BlobFileWriter::Open", strerror(errno));
  }
  
  // 写入初始 Header
  Status s = WriteHeader();
  if (!s.ok()) {
    ::close(fd_);
    fd_ = -1;
    return s;
  }
  
  file_size_ = kBlobHeaderSize;
  opened_ = true;
  return Status::OK();
}

Status BlobFileWriter::Close() {
  if (!opened_) return Status::OK();
  
  // 更新最终 Header
  UpdateHeader();
  
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
  
  opened_ = false;
  return Status::OK();
}

Status BlobFileWriter::WriteHeader() {
  BlobHeader header;
  header.sst_id = sst_id_;
  header.block_size = kBlobBlockSize;
  header.entry_count = 0;
  header.data_size = 0;
  header.checksum = 0;
  
  std::string buf;
  header.EncodeTo(&buf);
  
  ssize_t written = ::write(fd_, buf.data(), buf.size());
  if (written != static_cast<ssize_t>(buf.size())) {
    return Status::IOError("BlobFileWriter::WriteHeader", strerror(errno));
  }
  
  return Status::OK();
}

Status BlobFileWriter::UpdateHeader() {
  BlobHeader header;
  header.sst_id = sst_id_;
  header.block_size = kBlobBlockSize;
  header.entry_count = entry_count_;
  header.data_size = data_size_;
  // 重新计算校验和（Header 前 40 字节）
  std::string header_buf;
  header.EncodeTo(&header_buf);
  header.checksum = ComputeCRC64(header_buf.data(), 40);
  
  // 回到文件开头
  off_t pos = ::lseek(fd_, 0, SEEK_SET);
  if (pos != 0) {
    return Status::IOError("BlobFileWriter::UpdateHeader", strerror(errno));
  }
  
  std::string buf;
  header.EncodeTo(&buf);
  ssize_t written = ::write(fd_, buf.data(), buf.size());
  if (written != static_cast<ssize_t>(buf.size())) {
    return Status::IOError("BlobFileWriter::UpdateHeader", strerror(errno));
  }
  
  // 回到文件末尾
  ::lseek(fd_, file_size_, SEEK_SET);
  
  return Status::OK();
}

Status BlobFileWriter::Append(const Slice& data, uint32_t* out_offset) {
  if (!opened_) {
    return Status::IOError("BlobFileWriter::Append", "not opened");
  }
  
  // 计算对齐后的偏移（从 Header 后开始）
  uint32_t aligned_offset = AlignUp(file_size_, kBlobBlockSize);
  
  // 回到对齐位置（如果当前位置不对）
  if (static_cast<uint64_t>(::lseek(fd_, 0, SEEK_CUR)) != aligned_offset) {
    off_t pos = ::lseek(fd_, aligned_offset, SEEK_SET);
    if (pos != static_cast<off_t>(aligned_offset)) {
      return Status::IOError("BlobFileWriter::Append", strerror(errno));
    }
  }
  
  // 写入数据大小（4B）+ 实际数据
  uint32_t actual_size = data.size();
  ssize_t written = ::write(fd_, &actual_size, 4);
  if (written != 4) {
    return Status::IOError("BlobFileWriter::Append", strerror(errno));
  }
  
  written = ::write(fd_, data.data(), data.size());
  if (written != static_cast<ssize_t>(data.size())) {
    return Status::IOError("BlobFileWriter::Append", strerror(errno));
  }
  
  // 计算对齐后的大小
  uint32_t total_written = 4 + actual_size;
  uint32_t aligned_size = AlignUp(total_written, kBlobBlockSize);
  
  // 填充到对齐边界
  if (aligned_size > total_written) {
    std::string padding(aligned_size - total_written, 0);
    written = ::write(fd_, padding.data(), padding.size());
    if (written != static_cast<ssize_t>(padding.size())) {
      return Status::IOError("BlobFileWriter::Append", strerror(errno));
    }
  }
  
  *out_offset = aligned_offset;
  file_size_ = aligned_offset + aligned_size;
  data_size_ += actual_size;
  entry_count_++;
  
  return Status::OK();
}

Status BlobFileWriter::Sync() {
  if (!opened_) return Status::OK();
  
  // 更新 Header
  Status s = UpdateHeader();
  if (!s.ok()) return s;
  
  // 同步到磁盘
  int ret = ::fsync(fd_);
  if (ret != 0) {
    return Status::IOError("BlobFileWriter::Sync", strerror(errno));
  }
  
  return Status::OK();
}

// =============================================================================
// BlobFileReader 实现
// =============================================================================

BlobFileReader::BlobFileReader(const std::string& path) : path_(path) {}

BlobFileReader::~BlobFileReader() {
  if (opened_) {
    Close();
  }
}

Status BlobFileReader::Open() {
  if (opened_) return Status::OK();
  
  fd_ = ::open(path_.c_str(), O_RDONLY);
  if (fd_ < 0) {
    return Status::IOError("BlobFileReader::Open", strerror(errno));
  }
  
  // 获取文件大小
  struct stat st;
  if (::fstat(fd_, &st) != 0) {
    ::close(fd_);
    fd_ = -1;
    return Status::IOError("BlobFileReader::Open", strerror(errno));
  }
  file_size_ = st.st_size;
  
  // 读取 Header
  Status s = ReadHeader();
  if (!s.ok()) {
    ::close(fd_);
    fd_ = -1;
    return s;
  }
  
  opened_ = true;
  return Status::OK();
}

void BlobFileReader::Close() {
  if (!opened_) return;
  
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
  
  opened_ = false;
}

Status BlobFileReader::ReadHeader() {
  std::string buf(kBlobHeaderSize, 0);
  ssize_t n = ::pread(fd_, &buf[0], kBlobHeaderSize, 0);
  if (n != static_cast<ssize_t>(kBlobHeaderSize)) {
    return Status::IOError("BlobFileReader::ReadHeader", strerror(errno));
  }
  
  return header_.DecodeFrom(Slice(buf));
}

Status BlobFileReader::Read(uint32_t offset, uint32_t size, char* buffer) {
  if (!opened_) {
    return Status::IOError("BlobFileReader::Read", "not opened");
  }
  
  // 读取 4B size + 实际数据
  uint32_t actual_size = 0;
  ssize_t n = ::pread(fd_, &actual_size, 4, offset);
  if (n != 4) {
    return Status::IOError("BlobFileReader::Read", strerror(errno));
  }
  
  // 读取实际数据
  uint32_t to_read = std::min(actual_size, size);
  n = ::pread(fd_, buffer, to_read, offset + 4);
  if (n != static_cast<ssize_t>(to_read)) {
    return Status::IOError("BlobFileReader::Read", strerror(errno));
  }
  
  return Status::OK();
}

Status BlobFileReader::Prefetch(const std::vector<uint32_t>& offsets) {
  if (!opened_) return Status::OK();
  
  // Linux 预读建议（POSIX_FADV_WILLNEED）
  #ifdef POSIX_FADV_WILLNEED
  for (uint32_t offset : offsets) {
    ::posix_fadvise(fd_, offset, kBlobBlockSize, POSIX_FADV_WILLNEED);
  }
  #endif
  
  return Status::OK();
}

}  // namespace cedar
