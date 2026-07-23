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

#ifndef FERN_CORE_STATUS_H_
#define FERN_CORE_STATUS_H_

#include <cstring>
#include <string>

#include "cedar/core/slice.h"

namespace cedar {

class Status {
 public:
  // Create a success status.
  Status() noexcept : state_(nullptr) {}
  ~Status() { delete[] state_; }

  Status(const Status& rhs);
  Status& operator=(const Status& rhs);

  Status(Status&& rhs) noexcept : state_(rhs.state_) { rhs.state_ = nullptr; }
  Status& operator=(Status&& rhs) noexcept;

  // Return a success status.
  static Status OK() { return Status(); }

  // Return error status of an appropriate type.
  static Status NotFound(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kNotFound, msg, msg2);
  }
  static Status Corruption(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kCorruption, msg, msg2);
  }
  static Status NotSupported(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kNotSupported, msg, msg2);
  }
  static Status InvalidArgument(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kInvalidArgument, msg, msg2);
  }
  static Status IOError(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kIOError, msg, msg2);
  }
  static Status Conflict(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kConflict, msg, msg2);
  }
  static Status SchemaMismatch(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kSchemaMismatch, msg, msg2);
  }
  static Status ParseError(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kParseError, msg, msg2);
  }
  static Status BindError(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kBindError, msg, msg2);
  }
  static Status BlobCorruption(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kBlobCorruption, msg, msg2);
  }
  static Status QueryCancelled(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kQueryCancelled, msg, msg2);
  }
  static Status QueryMemoryLimit(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kQueryMemoryLimit, msg, msg2);
  }
  static Status WriteStalled(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kWriteStalled, msg, msg2);
  }
  static Status ResourceExhausted(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kResourceExhausted, msg, msg2);
  }
  static Status Indeterminate(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kIndeterminate, msg, msg2);
  }
  static Status RecoveryRequired(const Slice& msg, const Slice& msg2 = Slice()) {
    return Status(kRecoveryRequired, msg, msg2);
  }
  static Status ShutdownInProgress(const Slice& msg,
                                   const Slice& msg2 = Slice()) {
    return Status(kShutdownInProgress, msg, msg2);
  }
  static Status MaintenanceBackoff(const Slice& msg,
                                   const Slice& msg2 = Slice()) {
    return Status(kMaintenanceBackoff, msg, msg2);
  }

  // Returns true iff the status indicates success.
  bool ok() const { return (state_ == nullptr); }

  // Returns true iff the status indicates a NotFound error.
  bool IsNotFound() const { return code() == kNotFound; }

  // Returns true iff the status indicates a Corruption error.
  bool IsCorruption() const { return code() == kCorruption; }

  // Returns true iff the status indicates an IOError.
  bool IsIOError() const { return code() == kIOError; }

  // Returns true iff the status indicates a NotSupportedError.
  bool IsNotSupportedError() const { return code() == kNotSupported; }

  // Returns true iff the status indicates an InvalidArgument.
  bool IsInvalidArgument() const { return code() == kInvalidArgument; }

  // Returns true iff the status indicates a Conflict (OCC).
  bool IsConflict() const { return code() == kConflict; }
  bool IsSchemaMismatch() const { return code() == kSchemaMismatch; }
  bool IsParseError() const { return code() == kParseError; }
  bool IsBindError() const { return code() == kBindError; }
  bool IsBlobCorruption() const { return code() == kBlobCorruption; }
  bool IsQueryCancelled() const { return code() == kQueryCancelled; }
  bool IsQueryMemoryLimit() const { return code() == kQueryMemoryLimit; }
  bool IsWriteStalled() const { return code() == kWriteStalled; }
  bool IsResourceExhausted() const { return code() == kResourceExhausted; }
  bool IsIndeterminate() const { return code() == kIndeterminate; }
  bool IsRecoveryRequired() const { return code() == kRecoveryRequired; }
  bool IsShutdownInProgress() const { return code() == kShutdownInProgress; }
  bool IsMaintenanceBackoff() const { return code() == kMaintenanceBackoff; }

  // Return a string representation of this status suitable for printing.
  // Returns the string "OK" for success.
  std::string ToString() const;

  // Ignore the error (for use in destructors where we can't throw)
  void IgnoreError() const {}

 private:
  enum Code {
    kOk = 0,
    kNotFound = 1,
    kCorruption = 2,
    kNotSupported = 3,
    kInvalidArgument = 4,
    kIOError = 5,
    kConflict = 6,
    kSchemaMismatch = 7,
    kParseError = 8,
    kBindError = 9,
    kBlobCorruption = 10,
    kQueryCancelled = 11,
    kQueryMemoryLimit = 12,
    kWriteStalled = 13,
    kResourceExhausted = 14,
    kIndeterminate = 15,
    kRecoveryRequired = 16,
    kShutdownInProgress = 17,
    kMaintenanceBackoff = 18
  };

  Code code() const {
    return (state_ == nullptr) ? kOk : static_cast<Code>(state_[4]);
  }

  Status(Code code, const Slice& msg, const Slice& msg2);
  static const char* CopyState(const char* s);

  // OK status has a null state_.  Otherwise, state_ is a new[] array
  // of the following form:
  //    state_[0..3] == length of message
  //    state_[4]    == code
  //    state_[5..]  == message
  const char* state_;
};

inline Status::Status(const Status& rhs) {
  state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
}

inline Status& Status::operator=(const Status& rhs) {
  // The following condition catches both aliasing (when this == &rhs),
  // and the common case where both rhs and *this are ok.
  if (state_ != rhs.state_) {
    delete[] state_;
    state_ = (rhs.state_ == nullptr) ? nullptr : CopyState(rhs.state_);
  }
  return *this;
}

inline Status& Status::operator=(Status&& rhs) noexcept {
  std::swap(state_, rhs.state_);
  return *this;
}

// StatusOr<T> is a template class that holds either a Status or a value of type T.
// It is used for functions that may return either an error or a value.
template <typename T>
class StatusOr {
 public:
  StatusOr() = default;

  StatusOr(const Status& status) : status_(status), value_() {}
  StatusOr(const T& value) : status_(), value_(value) {}
  StatusOr(T&& value) : status_(), value_(std::move(value)) {}

  StatusOr(const StatusOr&) = default;
  StatusOr& operator=(const StatusOr&) = default;
  StatusOr(StatusOr&&) = default;
  StatusOr& operator=(StatusOr&&) = default;

  bool ok() const { return status_.ok(); }
  const Status& status() const { return status_; }

  const T& ValueOrDie() const {
    assert(ok());
    return value_;
  }

  T& ValueOrDie() {
    assert(ok());
    return value_;
  }

  T ConsumeValueOrDie() {
    assert(ok());
    return std::move(value_);
  }

  const T& value() const {
    assert(ok());
    return value_;
  }

  T& value() {
    assert(ok());
    return value_;
  }

 private:
  Status status_;
  T value_;
};

}  // namespace cedar

#endif  // FERN_CORE_STATUS_H_
