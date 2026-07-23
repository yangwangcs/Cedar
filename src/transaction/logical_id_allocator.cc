// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "cedar/transaction/logical_id_allocator.h"

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <unistd.h>

#include "cedar/core/crc32c.h"

namespace cedar {
namespace {

constexpr char kMagic[] = "LID1";

Status FsyncDirectory(const std::filesystem::path& directory) {
  const std::string path = directory.string();
  const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY);
  if (fd < 0) return Status::IOError(path, std::strerror(errno));
  Status status = Status::OK();
  if (::fsync(fd) != 0) status = Status::IOError(path, std::strerror(errno));
  if (::close(fd) != 0 && status.ok()) status = Status::IOError(path, std::strerror(errno));
  return status;
}

void Put64(std::string* output, uint64_t value) {
  for (int shift = 0; shift < 64; shift += 8) output->push_back(static_cast<char>(value >> shift));
}

bool Get64(const std::string& input, size_t* offset, uint64_t* value) {
  if (input.size() - *offset < sizeof(uint64_t)) return false;
  *value = 0;
  for (int shift = 0; shift < 64; shift += 8) {
    *value |= static_cast<uint64_t>(static_cast<uint8_t>(input[(*offset)++])) << shift;
  }
  return true;
}

void Put32(std::string* output, uint32_t value) {
  for (int shift = 0; shift < 32; shift += 8) output->push_back(static_cast<char>(value >> shift));
}

bool Get32(const std::string& input, size_t* offset, uint32_t* value) {
  if (input.size() - *offset < sizeof(uint32_t)) return false;
  *value = 0;
  for (int shift = 0; shift < 32; shift += 8) {
    *value |= static_cast<uint32_t>(static_cast<uint8_t>(input[(*offset)++])) << shift;
  }
  return true;
}

}  // namespace

Status LogicalIdAllocator::Open() {
  std::lock_guard<std::mutex> lock(mutex_);
  next_id_ = 1;
  if (!std::filesystem::exists(checkpoint_path_)) {
    opened_ = true;
    return Status::OK();
  }
  const int fd = ::open(checkpoint_path_.c_str(), O_RDONLY);
  if (fd < 0) return Status::IOError(checkpoint_path_, std::strerror(errno));
  std::string data;
  char buffer[64];
  for (;;) {
    const ssize_t read_count = ::read(fd, buffer, sizeof(buffer));
    if (read_count == 0) break;
    if (read_count < 0) {
      const int error = errno;
      ::close(fd);
      return Status::IOError(checkpoint_path_, std::strerror(error));
    }
    data.append(buffer, static_cast<size_t>(read_count));
  }
  ::close(fd);
  size_t offset = 0;
  uint64_t stored_next = 0;
  uint32_t stored_crc = 0;
  if (data.size() != 16 || data.compare(0, 4, kMagic) != 0 ||
      !Get64(data, &(offset = 4), &stored_next) || !Get32(data, &offset, &stored_crc) ||
      stored_next == 0 || stored_crc != crc32c::Value(data.data(), 12)) {
    return Status::Corruption("logical id allocator", "invalid checkpoint");
  }
  next_id_ = stored_next;
  opened_ = true;
  return Status::OK();
}

StatusOr<uint64_t> LogicalIdAllocator::Allocate() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!opened_ || next_id_ == UINT64_MAX) {
    return Status::InvalidArgument("logical id allocator", "allocator is closed or exhausted");
  }
  const uint64_t allocated = next_id_;
  const Status persisted = PersistLocked(next_id_ + 1);
  if (!persisted.ok()) return persisted;
  ++next_id_;
  return allocated;
}

Status LogicalIdAllocator::PersistLocked(uint64_t next_id) const {
  std::error_code error;
  std::filesystem::create_directories(std::filesystem::path(checkpoint_path_).parent_path(), error);
  if (error) return Status::IOError(checkpoint_path_, error.message());
  std::string data(kMagic, 4);
  Put64(&data, next_id);
  Put32(&data, crc32c::Value(data.data(), data.size()));
  const std::string temporary = checkpoint_path_ + ".tmp";
  const int fd = ::open(temporary.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
  if (fd < 0) return Status::IOError(temporary, std::strerror(errno));
  size_t written = 0;
  while (written < data.size()) {
    const ssize_t count = ::write(fd, data.data() + written, data.size() - written);
    if (count < 0) {
      if (errno == EINTR) continue;
      const int write_error = errno;
      ::close(fd);
      return Status::IOError(temporary, std::strerror(write_error));
    }
    written += static_cast<size_t>(count);
  }
  if (::fsync(fd) != 0) {
    const int sync_error = errno;
    ::close(fd);
    return Status::IOError(temporary, std::strerror(sync_error));
  }
  ::close(fd);
  if (::rename(temporary.c_str(), checkpoint_path_.c_str()) != 0) {
    return Status::IOError(checkpoint_path_, std::strerror(errno));
  }
  return FsyncDirectory(std::filesystem::path(checkpoint_path_).parent_path());
}

}  // namespace cedar
