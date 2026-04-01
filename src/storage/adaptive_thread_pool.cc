// Copyright (c) 2025 The Cedar Authors. All rights reserved.

#include "cedar/storage/adaptive_thread_pool.h"

namespace cedar {

// Worker 实现
Worker::Worker(int id, std::function<void()> work_loop)
    : id_(id), work_loop_(std::move(work_loop)) {
  last_active_time_ = std::chrono::steady_clock::now();
}

Worker::~Worker() {
  if (thread_.joinable()) {
    RequestShutdown();
    Join();
  }
}

void Worker::Start() {
  thread_ = std::thread(work_loop_);
}

void Worker::RequestShutdown() {
  shutdown_requested_.store(true);
  SetState(State::SHUTDOWN);
}

void Worker::Join() {
  if (thread_.joinable()) {
    thread_.join();
  }
}

}  // namespace cedar
