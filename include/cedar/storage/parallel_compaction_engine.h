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
// 并行 Compaction 引擎 - 多线程列级合并
// =============================================================================
// 特性:
// 1. 按 (entity_type, column_id) 分组并行合并
// 2. 优先级队列（L0 优先，读放大优先）
// 3. 线程池动态调度
// 4. 流式处理降低内存
// =============================================================================

#ifndef FERN_PARALLEL_COMPACTION_ENGINE_H_
#define FERN_PARALLEL_COMPACTION_ENGINE_H_

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/core/env.h"
#include "cedar/frond/zone_columnar_format.h"
#include "cedar/storage/size_tiered_compaction.h"

namespace cedar {

// =============================================================================
// 并行 Compaction 配置
// =============================================================================
struct ParallelCompactionConfig {
  // 线程池大小 (0 = 硬件线程数)
  int num_threads = 0;
  
  // 每列最大并发 Compaction 数
  int max_concurrent_per_column = 2;
  
  // 任务队列容量
  size_t task_queue_capacity = 100;
  
  // 流式处理缓冲区大小 (KB)
  size_t stream_buffer_kb = 64;
  
  // 批量输出条目数
  size_t output_batch_size = 1024;
  
  // L0 紧急阈值 (超过立即触发)
  double l0_emergency_ratio = 0.8;
  
  // 优先级权重
  double level_weight = 100.0;      // 层级权重
  double urgency_weight = 50.0;     // 超阈值权重
  double read_amp_weight = 0.001;   // 读放大权重
};

// =============================================================================
// Compaction 任务优先级
// =============================================================================
struct CompactionTaskPriority {
  int level = 0;
  double urgency = 0.0;      // current_size / threshold
  uint64_t estimated_read_amp = 0;
  uint64_t timestamp = 0;    // 创建时间（FIFO）
  
  // 优先级分数（越小越优先）
  double Score(const ParallelCompactionConfig& config) const {
    return level * config.level_weight 
         + urgency * config.urgency_weight
         + estimated_read_amp * config.read_amp_weight
         + timestamp * 0.0001;  // 时间衰减
  }
  
  bool operator<(const CompactionTaskPriority& other) const {
    // 注意: priority_queue 默认最大堆，所以这里反转比较
    return level > other.level || 
           (level == other.level && urgency < other.urgency);
  }
};

// =============================================================================
// 带优先级的 Compaction 任务
// =============================================================================
struct PrioritizedCompactionTask {
  CompactionTask task;
  CompactionTaskPriority priority;
  std::string column_key;  // "entity_type:column_id"
  
  bool operator<(const PrioritizedCompactionTask& other) const {
    return priority < other.priority;
  }
};

// =============================================================================
// 列级任务队列
// =============================================================================
class ColumnGroupTaskQueue {
 public:
  explicit ColumnGroupTaskQueue(const ParallelCompactionConfig& config);
  ~ColumnGroupTaskQueue();
  
  // 添加任务
  void Push(PrioritizedCompactionTask task);
  
  // 获取任务（阻塞）
  bool Pop(PrioritizedCompactionTask* out_task, std::chrono::milliseconds timeout);
  
  // 获取指定列的任务（保证同列不并发）
  bool PopForColumn(const std::string& column_key, PrioritizedCompactionTask* out_task);
  
  // 标记列忙/闲
  void MarkColumnBusy(const std::string& column_key, bool busy);
  bool IsColumnBusy(const std::string& column_key) const;
  
  // 获取队列大小
  size_t Size() const;
  bool Empty() const;
  
  // 停止队列
  void Shutdown();

 private:
  ParallelCompactionConfig config_;
  mutable std::mutex mutex_;
  std::condition_variable cv_;
  std::priority_queue<PrioritizedCompactionTask> queue_;
  std::unordered_map<std::string, bool> busy_columns_;
  bool shutdown_ = false;
};

// =============================================================================
// 线程池 Compaction 执行器
// =============================================================================
class ThreadPoolCompactionExecutor {
 public:
  ThreadPoolCompactionExecutor(SizeTieredCompactionEngine* engine,
                                const ParallelCompactionConfig& config);
  ~ThreadPoolCompactionExecutor();
  
  // 启动线程池
  void Start();
  
  // 停止线程池
  void Stop();
  
  // 提交任务
  std::future<Status> Submit(CompactionTask task);
  
  // 等待所有任务完成
  void WaitForAll();
  
  // 获取活跃任务数
  int ActiveTasks() const { return active_tasks_.load(); }

 private:
  void WorkerThread();
  void ExecuteCompaction(const PrioritizedCompactionTask& ptask);
  
  SizeTieredCompactionEngine* engine_;
  ParallelCompactionConfig config_;
  std::unique_ptr<ColumnGroupTaskQueue> task_queue_;
  std::vector<std::thread> workers_;
  std::atomic<bool> shutdown_{false};
  std::atomic<int> active_tasks_{0};
  std::atomic<uint64_t> task_counter_{0};
};

// =============================================================================
// 流式 Compaction Merger - 低内存实现
// =============================================================================
class StreamingCompactionMerger {
 public:
  struct Config {
    size_t prefetch_buffer_size = 64 * 1024;  // 64KB 预读
    size_t output_batch_size = 1024;           // 1K 条批量输出
    int output_level = 0;
  };
  
  StreamingCompactionMerger(const std::vector<ZoneSstMeta>& inputs,
                            const std::string& output_path,
                            const std::string& db_path,
                            const Config& config);
  ~StreamingCompactionMerger();
  
  // 执行流式合并
  std::unique_ptr<ZoneSstMeta> Run();
  
  // 获取统计
  struct Stats {
    uint64_t input_entries = 0;
    uint64_t output_entries = 0;
    uint64_t dropped_duplicates = 0;
    uint64_t dropped_tombstones = 0;
    uint64_t peak_memory_bytes = 0;
  };
  const Stats& GetStats() const { return stats_; }

 private:
  struct InputStream {
    ZoneSstMeta meta;
    std::unique_ptr<ZoneColumnarSstReader> reader;
    std::unique_ptr<ZoneColumnarIterator> iterator;
    std::vector<uint8_t> prefetch_buffer;
    bool exhausted = false;
    
    void Prefetch();
    bool Next();
  };
  
  struct HeapItem {
    CedarKey key;
    Descriptor value;
    size_t stream_idx;
    
    bool operator>(const HeapItem& other) const {
      return key > other.key;
    }
  };
  
  std::vector<std::unique_ptr<InputStream>> streams_;
  std::string output_path_;
  std::string db_path_;
  Config config_;
  Stats stats_;
};

// =============================================================================
// 并行 Compaction 引擎（包装器）
// =============================================================================
class ParallelCompactionEngine {
 public:
  ParallelCompactionEngine(SizeTieredCompactionEngine* engine,
                           const ParallelCompactionConfig& config);
  ~ParallelCompactionEngine();
  
  // 启动
  void Start();
  void Stop();
  
  // 调度 Compaction（自动优先级）
  bool ScheduleIfNeeded();
  
  // 强制调度指定任务
  std::future<Status> Schedule(const CompactionTask& task);
  
  // 等待完成
  void WaitForAll();
  
  // 获取统计
  int ActiveTasks() const;
  size_t PendingTasks() const;

 private:
  CompactionTaskPriority CalculatePriority(const CompactionTask& task);
  
  SizeTieredCompactionEngine* engine_;
  ParallelCompactionConfig config_;
  std::unique_ptr<ThreadPoolCompactionExecutor> executor_;
};

}  // namespace cedar

#endif  // FERN_PARALLEL_COMPACTION_ENGINE_H_
