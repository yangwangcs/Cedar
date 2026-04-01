// Copyright (c) 2024 Cedar Storage Engine Authors.
// Threading implementation using C++11 STL threads.

#include "cedar/core/threading.h"

#include <condition_variable>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace cedar {

// ============================================================================
// Mutex
// ============================================================================

class Mutex::Impl {
 public:
  std::mutex mutex_;
};

Mutex::Mutex() : impl_(std::make_unique<Impl>()) {}
Mutex::~Mutex() = default;
void Mutex::Lock() { impl_->mutex_.lock(); }
void Mutex::Unlock() { impl_->mutex_.unlock(); }

// ============================================================================
// CondVar
// ============================================================================

class CondVar::Impl {
 public:
  explicit Impl(Mutex* mu) : mu_(mu) {}

  void Wait() {
    std::unique_lock<std::mutex> lock(mu_->impl_->mutex_, std::adopt_lock);
    cond_.wait(lock);
    lock.release();
  }
  void Signal() { cond_.notify_one(); }
  void SignalAll() { cond_.notify_all(); }

 private:
  Mutex* mu_;
  std::condition_variable cond_;
};

CondVar::CondVar(Mutex* mu) : impl_(std::make_unique<Impl>(mu)) {}
CondVar::~CondVar() = default;
void CondVar::Wait() { impl_->Wait(); }
void CondVar::Signal() { impl_->Signal(); }
void CondVar::SignalAll() { impl_->SignalAll(); }

// ============================================================================
// ThreadPool
// ============================================================================

class ThreadPool::Impl {
 public:
  explicit Impl(size_t num_threads) : shutdown_(false), active_tasks_(0) {
    for (size_t i = 0; i < num_threads; ++i) {
      threads_.emplace_back([this] { WorkerLoop(); });
    }
  }

  ~Impl() {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      shutdown_ = true;
    }
    cond_.notify_all();
    for (auto& t : threads_) {
      t.join();
    }
  }

  void Schedule(std::function<void()> task) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      tasks_.push(std::move(task));
    }
    cond_.notify_one();
  }

  void WaitForAll() {
    while (true) {
      std::unique_lock<std::mutex> lock(mutex_);
      if (tasks_.empty() && active_tasks_ == 0) break;
      lock.unlock();
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

 private:
  void WorkerLoop() {
    while (true) {
      std::function<void()> task;
      {
        std::unique_lock<std::mutex> lock(mutex_);
        cond_.wait(lock, [this] { return shutdown_ || !tasks_.empty(); });
        if (shutdown_ && tasks_.empty()) break;
        task = std::move(tasks_.front());
        tasks_.pop();
        ++active_tasks_;
      }
      task();
      {
        std::unique_lock<std::mutex> lock(mutex_);
        --active_tasks_;
      }
    }
  }

  std::vector<std::thread> threads_;
  std::queue<std::function<void()>> tasks_;
  std::mutex mutex_;
  std::condition_variable cond_;
  bool shutdown_;
  size_t active_tasks_;
};

ThreadPool::ThreadPool(size_t num_threads)
    : impl_(std::make_unique<Impl>(num_threads)) {}
ThreadPool::~ThreadPool() = default;
void ThreadPool::Schedule(std::function<void()> task) {
  impl_->Schedule(std::move(task));
}
void ThreadPool::WaitForAll() { impl_->WaitForAll(); }

// ============================================================================
// BackgroundWorker
// ============================================================================

class BackgroundWorker::Impl {
 public:
  Impl() : shutdown_(false) {
    thread_ = std::thread([this] { Run(); });
  }

  ~Impl() {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      shutdown_ = true;
    }
    cond_.notify_one();
    thread_.join();
  }

  void Schedule(std::function<void()> task) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      tasks_.push(std::move(task));
    }
    cond_.notify_one();
  }

 private:
  void Run() {
    while (true) {
      std::function<void()> task;
      {
        std::unique_lock<std::mutex> lock(mutex_);
        cond_.wait(lock, [this] { return shutdown_ || !tasks_.empty(); });
        if (shutdown_ && tasks_.empty()) break;
        task = std::move(tasks_.front());
        tasks_.pop();
      }
      task();
    }
  }

  std::thread thread_;
  std::queue<std::function<void()>> tasks_;
  std::mutex mutex_;
  std::condition_variable cond_;
  bool shutdown_;
};

BackgroundWorker::BackgroundWorker() : impl_(std::make_unique<Impl>()) {}
BackgroundWorker::~BackgroundWorker() = default;
void BackgroundWorker::Schedule(std::function<void()> task) {
  impl_->Schedule(std::move(task));
}

}  // namespace cedar
