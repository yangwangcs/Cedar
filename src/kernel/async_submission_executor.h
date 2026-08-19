// Copyright 2026 The Cedar Authors

#ifndef CEDAR_KERNEL_ASYNC_SUBMISSION_EXECUTOR_H_
#define CEDAR_KERNEL_ASYNC_SUBMISSION_EXECUTOR_H_

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"

namespace cedar {

class AsyncSubmissionExecutor {
 public:
  struct Options {
    uint32_t worker_count = 1;
    uint32_t max_requests = 32;
    uint64_t max_bytes = 4ULL * 1024ULL * 1024ULL;
  };

  struct Ticket {
    uint64_t id = 0;
    uint64_t estimated_bytes = 0;
    std::function<Status()> handoff;
    std::function<void(const Status&)> fail;
    std::function<void()> release;
    std::atomic<bool> released{false};
    std::atomic<bool> cancelled{false};
  };

  explicit AsyncSubmissionExecutor(Options options);
  ~AsyncSubmissionExecutor();

  AsyncSubmissionExecutor(const AsyncSubmissionExecutor&) = delete;
  AsyncSubmissionExecutor& operator=(const AsyncSubmissionExecutor&) = delete;

  Status Start();
  Status TrySubmit(std::shared_ptr<Ticket> ticket);
  void Cancel(uint64_t ticket_id);
  void Release(uint64_t ticket_id);
  void Stop(const Status& status);

  uint64_t ReservedRequests() const;
  uint64_t ReservedBytes() const;

 private:
  void WorkerMain();

  const Options options_;
  mutable std::mutex mutex_;
  std::condition_variable condition_;
  std::deque<std::shared_ptr<Ticket>> queue_;
  std::unordered_map<uint64_t, std::shared_ptr<Ticket>> tickets_;
  std::vector<std::thread> workers_;
  uint64_t next_ticket_id_ = 1;
  uint64_t reserved_requests_ = 0;
  uint64_t reserved_bytes_ = 0;
  bool started_ = false;
  bool stopping_ = false;
};

}  // namespace cedar

#endif  // CEDAR_KERNEL_ASYNC_SUBMISSION_EXECUTOR_H_
