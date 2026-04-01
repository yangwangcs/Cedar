// Copyright (c) 2024 Cedar Storage Engine Authors.
// Threading abstraction layer using C++11 STL threads.

#ifndef FERN_CORE_THREADING_H_
#define FERN_CORE_THREADING_H_

#include <functional>
#include <memory>

namespace cedar {

// ============================================================================
// Mutex
// ============================================================================

class Mutex {
 public:
  Mutex();
  ~Mutex();

  void Lock();
  void Unlock();

  // Non-copyable
  Mutex(const Mutex&) = delete;
  Mutex& operator=(const Mutex&) = delete;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
  
  // Allow CondVar to access Impl
  friend class CondVar;
};

// RAII lock guard
class LockGuard {
 public:
  explicit LockGuard(Mutex* mu) : mu_(mu) { mu_->Lock(); }
  ~LockGuard() { mu_->Unlock(); }

  LockGuard(const LockGuard&) = delete;
  LockGuard& operator=(const LockGuard&) = delete;

 private:
  Mutex* mu_;
};

// ============================================================================
// Condition Variable
// ============================================================================

class CondVar {
 public:
  explicit CondVar(Mutex* mu);
  ~CondVar();

  void Wait();
  void Signal();
  void SignalAll();

  // Non-copyable
  CondVar(const CondVar&) = delete;
  CondVar& operator=(const CondVar&) = delete;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

// ============================================================================
// Thread Pool
// ============================================================================

class ThreadPool {
 public:
  explicit ThreadPool(size_t num_threads);
  ~ThreadPool();

  // Schedule a task to run in the thread pool
  void Schedule(std::function<void()> task);

  // Wait for all tasks to complete
  void WaitForAll();

  // Non-copyable
  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

// ============================================================================
// Background Worker
// ============================================================================

// Simple background thread for Env::Schedule()
class BackgroundWorker {
 public:
  BackgroundWorker();
  ~BackgroundWorker();

  void Schedule(std::function<void()> task);

  // Non-copyable
  BackgroundWorker(const BackgroundWorker&) = delete;
  BackgroundWorker& operator=(const BackgroundWorker&) = delete;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace cedar

#endif  // FERN_CORE_THREADING_H_
