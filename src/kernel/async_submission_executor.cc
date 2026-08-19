// Copyright 2026 The Cedar Authors

#include "async_submission_executor.h"

#include <limits>

namespace cedar {

AsyncSubmissionExecutor::AsyncSubmissionExecutor(Options options)
    : options_(options) {}

AsyncSubmissionExecutor::~AsyncSubmissionExecutor() {
  Stop(Status::ShutdownInProgress("async executor", "executor destroyed"));
}

Status AsyncSubmissionExecutor::Start() {
  std::lock_guard<std::mutex> lock(mutex_);
  if (started_) return Status::InvalidArgument("async executor", "already started");
  if (options_.worker_count == 0) {
    return Status::InvalidArgument("async executor", "worker count must be positive");
  }
  if (options_.max_requests == 0 || options_.max_bytes == 0) {
    return Status::InvalidArgument("async executor", "mailbox bounds must be positive");
  }
  started_ = true;
  workers_.reserve(options_.worker_count);
  for (uint32_t index = 0; index < options_.worker_count; ++index) {
    workers_.emplace_back(&AsyncSubmissionExecutor::WorkerMain, this);
  }
  return Status::OK();
}

Status AsyncSubmissionExecutor::TrySubmit(std::shared_ptr<Ticket> ticket) {
  if (ticket == nullptr || !ticket->handoff || !ticket->fail ||
      !ticket->release || ticket->estimated_bytes == 0) {
    return Status::InvalidArgument("async executor", "incomplete ticket");
  }
  std::lock_guard<std::mutex> lock(mutex_);
  if (!started_ || stopping_) {
    return Status::ShutdownInProgress("async executor", "executor is stopping");
  }
  if (reserved_requests_ >= options_.max_requests) {
    return Status::ResourceExhausted("async executor", "mailbox request bound is full");
  }
  if (ticket->estimated_bytes > options_.max_bytes -
                                   std::min(options_.max_bytes, reserved_bytes_)) {
    return Status::ResourceExhausted("async executor", "mailbox byte bound is full");
  }
  ticket->id = next_ticket_id_++;
  ticket->released.store(false, std::memory_order_release);
  ticket->cancelled.store(false, std::memory_order_release);
  tickets_.emplace(ticket->id, ticket);
  ++reserved_requests_;
  reserved_bytes_ += ticket->estimated_bytes;
  queue_.push_back(std::move(ticket));
  condition_.notify_one();
  return Status::OK();
}

void AsyncSubmissionExecutor::Cancel(uint64_t ticket_id) {
  std::lock_guard<std::mutex> lock(mutex_);
  const auto found = tickets_.find(ticket_id);
  if (found == tickets_.end()) return;
  found->second->cancelled.store(true, std::memory_order_release);
  condition_.notify_all();
}

void AsyncSubmissionExecutor::Release(uint64_t ticket_id) {
  std::shared_ptr<Ticket> ticket;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = tickets_.find(ticket_id);
    if (found == tickets_.end()) return;
    ticket = found->second;
    if (ticket->released.exchange(true, std::memory_order_acq_rel)) return;
    --reserved_requests_;
    reserved_bytes_ -= ticket->estimated_bytes;
    tickets_.erase(found);
  }
  ticket->release();
}

void AsyncSubmissionExecutor::Stop(const Status& status) {
  std::vector<std::shared_ptr<Ticket>> pending;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!started_ || stopping_) return;
    stopping_ = true;
    while (!queue_.empty()) {
      pending.push_back(std::move(queue_.front()));
      queue_.pop_front();
    }
    condition_.notify_all();
  }
  for (const auto& ticket : pending) {
    ticket->fail(status);
    Release(ticket->id);
  }
  for (auto& worker : workers_) {
    if (worker.joinable()) worker.join();
  }
  workers_.clear();

  std::vector<std::shared_ptr<Ticket>> remaining;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    remaining.reserve(tickets_.size());
    for (const auto& entry : tickets_) remaining.push_back(entry.second);
  }
  for (const auto& ticket : remaining) {
    ticket->fail(status);
    Release(ticket->id);
  }
}

uint64_t AsyncSubmissionExecutor::ReservedRequests() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return reserved_requests_;
}

uint64_t AsyncSubmissionExecutor::ReservedBytes() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return reserved_bytes_;
}

void AsyncSubmissionExecutor::WorkerMain() {
  for (;;) {
    std::shared_ptr<Ticket> ticket;
    {
      std::unique_lock<std::mutex> lock(mutex_);
      condition_.wait(lock, [this] { return stopping_ || !queue_.empty(); });
      if (queue_.empty()) {
        if (stopping_) return;
        continue;
      }
      ticket = std::move(queue_.front());
      queue_.pop_front();
    }
    if (ticket->cancelled.load(std::memory_order_acquire)) {
      ticket->fail(Status::ResourceExhausted(
          "async executor", "submission was cancelled before handoff"));
      Release(ticket->id);
      continue;
    }
    const Status handed_off = ticket->handoff();
    if (!handed_off.ok()) {
      ticket->fail(handed_off);
      Release(ticket->id);
    }
  }
}

}  // namespace cedar
