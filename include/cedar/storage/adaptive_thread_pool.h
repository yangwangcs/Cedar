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

// =============================================================================
// 自适应线程池 - 根据负载动态调整线程数
// =============================================================================
// 特性：
// 1. 根据任务队列长度自动增减线程
// 2. 根据 CPU 利用率调整（避免过度抢占）
// 3. 支持 Compaction 和查询的独立线程池
// =============================================================================

#ifndef FERN_ADAPTIVE_THREAD_POOL_H_
#define FERN_ADAPTIVE_THREAD_POOL_H_

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>
#include <condition_variable>

namespace cedar {

// 线程池统计
struct ThreadPoolMetrics {
  int current_threads = 0;
  int active_tasks = 0;
  size_t pending_tasks = 0;
  double avg_task_duration_ms = 0;
  double cpu_usage_percent = 0;
  uint64_t tasks_completed = 0;
  uint64_t tasks_rejected = 0;
};

// 自适应配置
struct AdaptiveConfig {
  // 线程数范围
  int min_threads = 1;
  int max_threads = 16;
  
  // 初始线程数
  int initial_threads = 4;
  
  // 扩容阈值：当 pending_tasks / current_threads > 此值时扩容
  double scale_up_threshold = 2.0;
  
  // 缩容阈值：当 pending_tasks == 0 且空闲时间 > 此值（秒）时缩容
  int scale_down_idle_seconds = 60;
  
  // 调整间隔
  int adjust_interval_ms = 5000;
  
  // CPU 上限（超过此值不扩容）
  double max_cpu_percent = 80.0;
  
  // 是否启用自适应
  bool enable_adaptive = true;
};

// 工作线程状态
class Worker {
 public:
  enum class State {
    IDLE,       // 空闲
    BUSY,       // 工作中
    SHUTDOWN    // 即将关闭
  };
  
  Worker(int id, std::function<void()> work_loop);
  ~Worker();
  
  void Start();
  void RequestShutdown();
  void Join();
  
  State GetState() const { return state_.load(); }
  void SetState(State state) { state_.store(state); }
  
  int GetId() const { return id_; }
  std::chrono::steady_clock::time_point GetLastActiveTime() const {
    return last_active_time_;
  }
  void UpdateActiveTime() { last_active_time_ = std::chrono::steady_clock::now(); }

 private:
  int id_;
  std::function<void()> work_loop_;
  std::thread thread_;
  std::atomic<State> state_{State::IDLE};
  std::atomic<bool> shutdown_requested_{false};
  std::chrono::steady_clock::time_point last_active_time_;
};

// 自适应线程池
template <typename Task>
class AdaptiveThreadPool {
 public:
  using TaskResult = typename std::result_of<Task()>::type;
  
  explicit AdaptiveThreadPool(const AdaptiveConfig& config) : config_(config) {
    metrics_.current_threads = 0;
    shutdown_ = false;
  }
  
  ~AdaptiveThreadPool() { Shutdown(); }
  
  // 启动线程池
  void Start() {
    // 启动初始线程
    for (int i = 0; i < config_.initial_threads; ++i) {
      AddWorker();
    }
    
    // 启动自适应调整线程
    if (config_.enable_adaptive) {
      adjuster_thread_ = std::thread(&AdaptiveThreadPool::AdjustThreadCount, this);
    }
  }
  
  // 停止线程池
  void Shutdown() {
    shutdown_ = true;
    task_cv_.notify_all();
    
    if (adjuster_thread_.joinable()) {
      adjuster_thread_.join();
    }
    
    // 停止所有工作线程
    {
      std::lock_guard<std::mutex> lock(workers_mutex_);
      for (auto& worker : workers_) {
        worker->RequestShutdown();
      }
    }
    
    // 等待线程结束
    {
      std::lock_guard<std::mutex> lock(workers_mutex_);
      for (auto& worker : workers_) {
        worker->Join();
      }
      workers_.clear();
    }
  }
  
  // 提交任务
  template <typename Func>
  auto Submit(Func&& f) -> std::future<decltype(f())> {
    using ReturnType = decltype(f());
    
    auto task = std::make_shared<std::packaged_task<ReturnType()>>(
        std::forward<Func>(f));
    std::future<ReturnType> result = task->get_future();
    
    {
      std::lock_guard<std::mutex> lock(task_mutex_);
      if (shutdown_) {
        throw std::runtime_error("ThreadPool is shutdown");
      }
      task_queue_.emplace([task]() { (*task)(); });
      metrics_.pending_tasks = task_queue_.size();
    }
    
    task_cv_.notify_one();
    return result;
  }
  
  // 获取统计
  ThreadPoolMetrics GetMetrics() const {
    std::lock_guard<std::mutex> lock(task_mutex_);
    return metrics_;
  }

 private:
  void AddWorker() {
    int id = next_worker_id_.fetch_add(1);
    auto worker = std::make_unique<Worker>(id, [this, id]() { WorkLoop(id); });
    worker->Start();
    
    std::lock_guard<std::mutex> lock(workers_mutex_);
    workers_.push_back(std::move(worker));
    metrics_.current_threads = workers_.size();
  }
  
  void RemoveWorker() {
    std::lock_guard<std::mutex> lock(workers_mutex_);
    // 找最空闲的线程
    auto it = std::find_if(workers_.begin(), workers_.end(),
        [](const auto& w) { return w->GetState() == Worker::State::IDLE; });
    
    if (it != workers_.end()) {
      (*it)->RequestShutdown();
      (*it)->Join();
      workers_.erase(it);
      metrics_.current_threads = workers_.size();
    }
  }
  
  void WorkLoop(int worker_id) {
    while (!shutdown_) {
      std::function<void()> task;
      
      {
        std::unique_lock<std::mutex> lock(task_mutex_);
        
        // 等待任务或关闭
        auto pred = [this] { return shutdown_ || !task_queue_.empty(); };
        task_cv_.wait(lock, pred);
        
        if (shutdown_ && task_queue_.empty()) {
          return;
        }
        
        if (!task_queue_.empty()) {
          task = std::move(task_queue_.front());
          task_queue_.pop();
          metrics_.pending_tasks = task_queue_.size();
        }
      }
      
      if (task) {
        // 更新状态
        auto worker = FindWorker(worker_id);
        if (worker) {
          worker->SetState(Worker::State::BUSY);
          worker->UpdateActiveTime();
        }
        metrics_.active_tasks++;
        
        // 执行任务
        auto start = std::chrono::steady_clock::now();
        task();
        auto end = std::chrono::steady_clock::now();
        
        // 更新统计
        double duration = std::chrono::duration<double, std::milli>(end - start).count();
        UpdateAvgDuration(duration);
        metrics_.tasks_completed++;
        metrics_.active_tasks--;
        
        if (worker) {
          worker->SetState(Worker::State::IDLE);
          worker->UpdateActiveTime();
        }
      }
    }
  }
  
  Worker* FindWorker(int id) {
    std::lock_guard<std::mutex> lock(workers_mutex_);
    for (auto& w : workers_) {
      if (w->GetId() == id) return w.get();
    }
    return nullptr;
  }
  
  void UpdateAvgDuration(double duration) {
    // 指数移动平均
    const double alpha = 0.1;
    metrics_.avg_task_duration_ms = 
        (1 - alpha) * metrics_.avg_task_duration_ms + alpha * duration;
  }
  
  void AdjustThreadCount() {
    while (!shutdown_) {
      std::this_thread::sleep_for(std::chrono::milliseconds(config_.adjust_interval_ms));
      
      if (!config_.enable_adaptive || shutdown_) break;
      
      auto metrics = GetMetrics();
      
      // 检查是否需要扩容
      double load_ratio = metrics.pending_tasks / 
                          static_cast<double>(std::max(1, metrics.current_threads));
      
      if (load_ratio > config_.scale_up_threshold && 
          metrics.current_threads < config_.max_threads &&
          metrics.cpu_usage_percent < config_.max_cpu_percent) {
        // 扩容一个线程
        AddWorker();
      }
      
      // 检查是否需要缩容
      if (metrics.current_threads > config_.min_threads && metrics.pending_tasks == 0) {
        // 找空闲时间最长的线程
        std::lock_guard<std::mutex> lock(workers_mutex_);
        auto now = std::chrono::steady_clock::now();
        
        for (auto& worker : workers_) {
          if (worker->GetState() == Worker::State::IDLE) {
            auto idle_time = std::chrono::duration_cast<std::chrono::seconds>(
                now - worker->GetLastActiveTime()).count();
            
            if (idle_time > config_.scale_down_idle_seconds) {
              RemoveWorker();
              break;
            }
          }
        }
      }
    }
  }
  
  AdaptiveConfig config_;
  std::atomic<bool> shutdown_{false};
  std::atomic<int> next_worker_id_{0};
  
  mutable std::mutex task_mutex_;
  std::queue<std::function<void()>> task_queue_;
  std::condition_variable task_cv_;
  mutable ThreadPoolMetrics metrics_;
  
  mutable std::mutex workers_mutex_;
  std::vector<std::unique_ptr<Worker>> workers_;
  std::thread adjuster_thread_;
};

// 专门的 Compaction 线程池类型（使用时定义）
// using CompactionThreadPool = AdaptiveThreadPool<std::function<Status()>>;

// 专门的查询线程池类型（使用时定义）
// using QueryThreadPool = AdaptiveThreadPool<std::function<std::optional<Descriptor>()>>;

}  // namespace cedar

#endif  // FERN_ADAPTIVE_THREAD_POOL_H_
