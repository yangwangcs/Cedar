// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Async I/O support for high-performance operations

#ifndef FERN_ASYNC_IO_H_
#define FERN_ASYNC_IO_H_

#include <cstdint>
#include <functional>
#include <memory>
#include <vector>
#include <future>

namespace cedar {

// Platform detection
#if defined(__linux__)
  #define FERN_HAS_IO_URING 1
#elif defined(__APPLE__)
  #define FERN_HAS_KQUEUE 1
#endif

using AsyncIoCallback = std::function<void(bool success, size_t bytes_transferred)>;

class AsyncIoContext {
 public:
  virtual ~AsyncIoContext() = default;
  virtual bool Initialize() = 0;
  virtual void Shutdown() = 0;
  virtual bool SubmitRead(int fd, void* buf, size_t len, off_t offset, 
                          AsyncIoCallback callback) = 0;
  virtual int Poll(int timeout_ms = 0) = 0;
};

std::unique_ptr<AsyncIoContext> CreateAsyncIoContext();

// Thread pool based async reader (cross-platform)
class ThreadPoolAsyncReader {
 public:
  explicit ThreadPoolAsyncReader(size_t num_threads = 4);
  ~ThreadPoolAsyncReader();
  std::future<std::vector<uint8_t>> ReadAsync(const std::string& filepath, 
                                               size_t offset, size_t len);
 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace cedar

#endif  // FERN_ASYNC_IO_H_
