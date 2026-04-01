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

#ifndef FERN_CORE_ENV_H_
#define FERN_CORE_ENV_H_

#include <cstdarg>
#include <cstdint>
#include <string>
#include <vector>

#include "cedar/core/slice.h"
#include "cedar/core/status.h"

namespace cedar {

class FileLock;
class Logger;
class RandomAccessFile;
class SequentialFile;
class WritableFile;

// An Env is an interface used by the storage engine to access operating
// system functionality like the filesystem etc.
// All Env implementations are safe for concurrent access from multiple
// threads without any external synchronization.
class Env {
 public:
  Env();
  Env(const Env&) = delete;
  Env& operator=(const Env&) = delete;
  virtual ~Env();

  // Return a default environment suitable for the current operating system.
  // The result of Default() belongs to cedar and must never be deleted.
  static Env* Default();

  // Create an object that sequentially reads the file with the specified name.
  // On success, stores a pointer to the new file in *result and returns OK.
  // On failure stores nullptr in *result and returns non-OK.
  virtual Status NewSequentialFile(const std::string& fname,
                                   SequentialFile** result) = 0;

  // Create an object supporting random-access reads from the file with the
  // specified name. The returned file may be concurrently accessed by
  // multiple threads.
  virtual Status NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile** result) = 0;

  // Create an object that writes to a new file with the specified name.
  // Deletes any existing file with the same name and creates a new file.
  virtual Status NewWritableFile(const std::string& fname,
                                 WritableFile** result) = 0;

  // Create an object that either appends to an existing file, or writes to
  // a new file (if the file does not exist to begin with).
  virtual Status NewAppendableFile(const std::string& fname,
                                   WritableFile** result);

  // Returns true iff the named file exists.
  virtual bool FileExists(const std::string& fname) = 0;

  // Store in *result the names of the children of the specified directory.
  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) = 0;

  // Delete the named file.
  virtual Status RemoveFile(const std::string& fname);

  // Deprecated: Use RemoveFile instead.
  virtual Status DeleteFile(const std::string& fname);

  // Create the specified directory.
  virtual Status CreateDir(const std::string& dirname) = 0;

  // Delete the specified directory.
  virtual Status RemoveDir(const std::string& dirname);

  // Deprecated: Use RemoveDir instead.
  virtual Status DeleteDir(const std::string& dirname);

  // Store the size of fname in *file_size.
  virtual Status GetFileSize(const std::string& fname, uint64_t* file_size) = 0;

  // Rename file src to target.
  virtual Status RenameFile(const std::string& src,
                            const std::string& target) = 0;

  // Lock the specified file. Used to prevent concurrent access to
  // the same db by multiple processes.
  virtual Status LockFile(const std::string& fname, FileLock** lock) = 0;

  // Release the lock acquired by a previous successful call to LockFile.
  virtual Status UnlockFile(FileLock* lock) = 0;

  // Arrange to run "(*function)(arg)" once in a background thread.
  virtual void Schedule(void (*function)(void* arg), void* arg) = 0;

  // Start a new thread, invoking "function(arg)" within the new thread.
  virtual void StartThread(void (*function)(void* arg), void* arg) = 0;

  // *path is set to a temporary directory that can be used for testing.
  virtual Status GetTestDirectory(std::string* path) = 0;

  // Create and return a log file for storing informational messages.
  virtual Status NewLogger(const std::string& fname, Logger** result) = 0;

  // Returns the number of micro-seconds since some fixed point in time.
  virtual uint64_t NowMicros() = 0;

  // Sleep/delay the thread for the prescribed number of micro-seconds.
  virtual void SleepForMicroseconds(int micros) = 0;
};

// A file abstraction for reading sequentially through a file
class SequentialFile {
 public:
  SequentialFile() = default;
  SequentialFile(const SequentialFile&) = delete;
  SequentialFile& operator=(const SequentialFile&) = delete;
  virtual ~SequentialFile();

  // Read up to "n" bytes from the file.
  // Sets "*result" to the data that was read (including if fewer than "n"
  // bytes were successfully read).
  virtual Status Read(size_t n, Slice* result, char* scratch) = 0;

  // Skip "n" bytes from the file.
  virtual Status Skip(uint64_t n) = 0;
};

// A file abstraction for randomly reading the contents of a file.
class RandomAccessFile {
 public:
  RandomAccessFile() = default;
  RandomAccessFile(const RandomAccessFile&) = delete;
  RandomAccessFile& operator=(const RandomAccessFile&) = delete;
  virtual ~RandomAccessFile();

  // Read up to "n" bytes from the file starting at "offset".
  // Safe for concurrent use by multiple threads.
  virtual Status Read(uint64_t offset, size_t n, Slice* result,
                      char* scratch) const = 0;
};

// A file abstraction for sequential writing.
class WritableFile {
 public:
  WritableFile() = default;
  WritableFile(const WritableFile&) = delete;
  WritableFile& operator=(const WritableFile&) = delete;
  virtual ~WritableFile();

  virtual Status Append(const Slice& data) = 0;
  virtual Status Close() = 0;
  virtual Status Flush() = 0;
  virtual Status Sync() = 0;
};

// An interface for writing log messages.
class Logger {
 public:
  Logger() = default;
  Logger(const Logger&) = delete;
  Logger& operator=(const Logger&) = delete;
  virtual ~Logger();

  // Write an entry to the log file with the specified format.
  virtual void Logv(const char* format, std::va_list ap) = 0;
};

// Identifies a locked file.
class FileLock {
 public:
  FileLock() = default;
  FileLock(const FileLock&) = delete;
  FileLock& operator=(const FileLock&) = delete;
  virtual ~FileLock();
};

// Log the specified data to *info_log if info_log is non-null.
void Log(Logger* info_log, const char* format, ...)
#if defined(__GNUC__) || defined(__clang__)
    __attribute__((__format__(__printf__, 2, 3)))
#endif
    ;

// A utility routine: write "data" to the named file.
Status WriteStringToFile(Env* env, const Slice& data,
                         const std::string& fname);

// A utility routine: read contents of named file into *data
Status ReadFileToString(Env* env, const std::string& fname,
                        std::string* data);

// An implementation of Env that forwards all calls to another Env.
class EnvWrapper : public Env {
 public:
  explicit EnvWrapper(Env* t) : target_(t) {}
  virtual ~EnvWrapper();

  Env* target() const { return target_; }

  Status NewSequentialFile(const std::string& f, SequentialFile** r) override {
    return target_->NewSequentialFile(f, r);
  }
  Status NewRandomAccessFile(const std::string& f,
                             RandomAccessFile** r) override {
    return target_->NewRandomAccessFile(f, r);
  }
  Status NewWritableFile(const std::string& f, WritableFile** r) override {
    return target_->NewWritableFile(f, r);
  }
  Status NewAppendableFile(const std::string& f, WritableFile** r) override {
    return target_->NewAppendableFile(f, r);
  }
  bool FileExists(const std::string& f) override {
    return target_->FileExists(f);
  }
  Status GetChildren(const std::string& dir,
                     std::vector<std::string>* r) override {
    return target_->GetChildren(dir, r);
  }
  Status RemoveFile(const std::string& f) override {
    return target_->RemoveFile(f);
  }
  Status CreateDir(const std::string& d) override {
    return target_->CreateDir(d);
  }
  Status RemoveDir(const std::string& d) override {
    return target_->RemoveDir(d);
  }
  Status GetFileSize(const std::string& f, uint64_t* s) override {
    return target_->GetFileSize(f, s);
  }
  Status RenameFile(const std::string& s, const std::string& t) override {
    return target_->RenameFile(s, t);
  }
  Status LockFile(const std::string& f, FileLock** l) override {
    return target_->LockFile(f, l);
  }
  Status UnlockFile(FileLock* l) override { return target_->UnlockFile(l); }
  void Schedule(void (*f)(void*), void* a) override {
    return target_->Schedule(f, a);
  }
  void StartThread(void (*f)(void*), void* a) override {
    return target_->StartThread(f, a);
  }
  Status GetTestDirectory(std::string* path) override {
    return target_->GetTestDirectory(path);
  }
  Status NewLogger(const std::string& fname, Logger** result) override {
    return target_->NewLogger(fname, result);
  }
  uint64_t NowMicros() override { return target_->NowMicros(); }
  void SleepForMicroseconds(int micros) override {
    target_->SleepForMicroseconds(micros);
  }

 private:
  Env* target_;
};

}  // namespace cedar

#endif  // FERN_CORE_ENV_H_
