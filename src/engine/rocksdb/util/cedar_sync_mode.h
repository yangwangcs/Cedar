// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#pragma once

namespace ROCKSDB_NAMESPACE {
namespace cedar {

// Cedar scopes a relaxed WAL durability policy to one synchronous epoch write
// on the calling thread. The scope is thread-local, so background flushes and
// every other thread keep the platform default flush primitive.
inline thread_local bool g_use_filesystem_flush = false;

class ScopedFilesystemFlush {
 public:
  explicit ScopedFilesystemFlush(bool enabled)
      : previous_(g_use_filesystem_flush) {
    g_use_filesystem_flush = enabled;
  }
  ~ScopedFilesystemFlush() { g_use_filesystem_flush = previous_; }
  ScopedFilesystemFlush(const ScopedFilesystemFlush&) = delete;
  ScopedFilesystemFlush& operator=(const ScopedFilesystemFlush&) = delete;

 private:
  bool previous_;
};

inline bool UseFilesystemFlush() { return g_use_filesystem_flush; }

}  // namespace cedar
}  // namespace ROCKSDB_NAMESPACE
