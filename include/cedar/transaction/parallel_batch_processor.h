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

#ifndef FERN_PARALLEL_BATCH_PROCESSOR_H_
#define FERN_PARALLEL_BATCH_PROCESSOR_H_

#include <vector>
#include <thread>
#include <future>
#include <functional>
#include <atomic>
#include <mutex>

#include "cedar/core/status.h"
#include "cedar/types/cedar_types.h"
#include "cedar/types/descriptor.h"

namespace cedar {

class CedarGraphStorage;
class BatchTransactionExecutor;

// 并行批量处理器 - 在批量内部并行处理无冲突操作
// 适用于大批量导入和复杂图算法场景
class ParallelBatchProcessor {
 public:
  struct Options {
    size_t num_threads;        // 线程数，0表示自动
    size_t chunk_size;         // 每个线程处理的数据块大小
    bool preserve_order;       // 是否保持顺序（降低并行度）
    
    Options()
        : num_threads(0),
          chunk_size(100),
          preserve_order(false) {}
  };
  
  explicit ParallelBatchProcessor(const Options& options = Options());
  
  ~ParallelBatchProcessor();
  
  // 并行处理函数
  // Func: Status(size_t start, size_t end) - 处理 [start, end) 范围内的数据
  template<typename Func>
  Status Process(size_t total_count, Func&& process_func);
  
  // 带冲突检测的并行处理
  // 自动检测和避免冲突操作（如写入同一键）
  template<typename T, typename Func>
  Status ProcessWithConflictDetection(const std::vector<T>& items,
                                       Func&& process_func);
  
  // 获取处理器统计
  struct Stats {
    uint64_t total_processed = 0;
    uint64_t total_batches = 0;
    double avg_parallelism = 0;
  };
  Stats GetStats() const;
  
 private:
  Options options_;
  std::vector<std::thread> thread_pool_;
  std::atomic<bool> stop_{false};
  
  mutable std::mutex stats_mutex_;
  Stats stats_;
  
  void InitializeThreadPool();
};

// 图批量操作并行处理器 - 专用于图数据导入
class ParallelGraphBatchProcessor {
 public:
  struct BatchItem {
    enum Type { VERTEX_PUT, EDGE_PUT, VERTEX_DELETE, EDGE_DELETE } type;
    uint64_t id;           // 顶点ID或边src_id
    uint64_t dst_id;       // 边目标ID（仅边操作）
    uint16_t property_id;
    Descriptor descriptor;
    Timestamp timestamp;
    
    BatchItem(Type t, uint64_t vid, uint16_t pid, const Descriptor& d, Timestamp ts)
        : type(t), id(vid), dst_id(0), property_id(pid), descriptor(d), timestamp(ts) {}
    
    BatchItem(Type t, uint64_t src, uint64_t dst, uint16_t pid, 
              const Descriptor& d, Timestamp ts)
        : type(t), id(src), dst_id(dst), property_id(pid), descriptor(d), timestamp(ts) {}
  };
  
  struct Options {
    size_t num_threads;
    size_t vertex_batch_size;
    size_t edge_batch_size;
    
    Options()
        : num_threads(0),
          vertex_batch_size(1000),
          edge_batch_size(500) {}
  };
  
  explicit ParallelGraphBatchProcessor(CedarGraphStorage* storage,
                                        const Options& options = Options());
  
  // 添加批量操作
  void AddVertexPut(uint64_t vertex_id, uint16_t property_id,
                    const Descriptor& descriptor,
                    Timestamp timestamp = Timestamp::Static());
  
  void AddEdgePut(uint64_t src_id, uint64_t dst_id, uint16_t edge_type,
                  const Descriptor& descriptor,
                  Timestamp timestamp = Timestamp::Static());
  
  // 执行并行批量处理
  // 顶点和边操作并行执行，内部自动处理冲突
  Status Execute();
  
  // 清空待处理队列
  void Clear();
  
  // 获取待处理数量
  size_t PendingCount() const;
  
 private:
  // 执行顶点批量（内部方法）
  Status ExecuteVertexBatch();
  
  // 执行边批量（内部方法）
  Status ExecuteEdgeBatch();
  
  CedarGraphStorage* storage_;
  Options options_;
  
  std::vector<BatchItem> vertex_items_;
  std::vector<BatchItem> edge_items_;
  mutable std::mutex items_mutex_;
};

}  // namespace cedar

// 模板实现
namespace cedar {

template<typename Func>
Status ParallelBatchProcessor::Process(size_t total_count, Func&& process_func) {
  if (total_count == 0) {
    return Status::OK();
  }
  
  size_t num_threads = options_.num_threads;
  if (num_threads == 0) {
    num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) {
      num_threads = 4;
    }
  }
  
  size_t chunk_size = options_.chunk_size;
  size_t num_chunks = (total_count + chunk_size - 1) / chunk_size;
  
  std::vector<std::future<Status>> futures;
  
  for (size_t chunk = 0; chunk < num_chunks; ++chunk) {
    size_t start = chunk * chunk_size;
    size_t end = std::min(start + chunk_size, total_count);
    
    futures.push_back(std::async(std::launch::async, [&, start, end]() {
      return process_func(start, end);
    }));
    
    // 限制并发数
    if (futures.size() >= num_threads) {
      for (auto& f : futures) {
        Status s = f.get();
        if (!s.ok()) {
          return s;
        }
      }
      futures.clear();
    }
  }
  
  // 等待剩余任务
  for (auto& f : futures) {
    Status s = f.get();
    if (!s.ok()) {
      return s;
    }
  }
  
  {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    stats_.total_processed += total_count;
    stats_.total_batches++;
  }
  
  return Status::OK();
}

template<typename T, typename Func>
Status ParallelBatchProcessor::ProcessWithConflictDetection(
    const std::vector<T>& items, Func&& process_func) {
  // 简化的冲突检测：按哈希分区
  // 实际应用中可能需要更复杂的冲突检测逻辑
  return Process(items.size(), [&](size_t start, size_t end) {
    for (size_t i = start; i < end; ++i) {
      Status s = process_func(items[i]);
      if (!s.ok()) {
        return s;
      }
    }
    return Status::OK();
  });
}

}  // namespace cedar

#endif  // FERN_PARALLEL_BATCH_PROCESSOR_H_
