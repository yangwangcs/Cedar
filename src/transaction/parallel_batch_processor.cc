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

#include "cedar/transaction/parallel_batch_processor.h"

#include <algorithm>

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/transaction/batch_transaction.h"

namespace cedar {

// ==================== ParallelBatchProcessor ====================

ParallelBatchProcessor::ParallelBatchProcessor(const Options& options)
    : options_(options) {
  InitializeThreadPool();
}

ParallelBatchProcessor::~ParallelBatchProcessor() {
  stop_.store(true);
  for (auto& t : thread_pool_) {
    if (t.joinable()) {
      t.join();
    }
  }
}

void ParallelBatchProcessor::InitializeThreadPool() {
  size_t num_threads = options_.num_threads;
  if (num_threads == 0) {
    num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) {
      num_threads = 4;
    }
  }
  
  // 线程池在实际使用时动态创建
  // 这里只是预分配空间
  thread_pool_.reserve(num_threads);
}

ParallelBatchProcessor::Stats ParallelBatchProcessor::GetStats() const {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  return stats_;
}

// ==================== ParallelGraphBatchProcessor ====================

ParallelGraphBatchProcessor::ParallelGraphBatchProcessor(
    CedarGraphStorage* storage,
    const Options& options)
    : storage_(storage), options_(options) {}

void ParallelGraphBatchProcessor::AddVertexPut(
    uint64_t vertex_id, 
    uint16_t property_id,
    const Descriptor& descriptor,
    Timestamp timestamp) {
  std::lock_guard<std::mutex> lock(items_mutex_);
  vertex_items_.emplace_back(BatchItem::VERTEX_PUT, vertex_id, property_id,
                             descriptor, timestamp);
}

void ParallelGraphBatchProcessor::AddEdgePut(
    uint64_t src_id, 
    uint64_t dst_id, 
    uint16_t edge_type,
    const Descriptor& descriptor,
    Timestamp timestamp) {
  std::lock_guard<std::mutex> lock(items_mutex_);
  edge_items_.emplace_back(BatchItem::EDGE_PUT, src_id, dst_id, edge_type,
                           descriptor, timestamp);
}

Status ParallelGraphBatchProcessor::Execute() {
  std::lock_guard<std::mutex> lock(items_mutex_);
  
  // 先处理顶点（通常顶点操作之间无冲突）
  Status s = ExecuteVertexBatch();
  if (!s.ok()) {
    return s;
  }
  
  // 再处理边
  s = ExecuteEdgeBatch();
  if (!s.ok()) {
    return s;
  }
  
  // 清空已处理的项目
  vertex_items_.clear();
  edge_items_.clear();
  
  return Status::OK();
}

Status ParallelGraphBatchProcessor::ExecuteVertexBatch() {
  if (vertex_items_.empty()) {
    return Status::OK();
  }
  
  // 按顶点ID分区，避免冲突
  std::sort(vertex_items_.begin(), vertex_items_.end(),
            [](const BatchItem& a, const BatchItem& b) {
              return a.id < b.id;
            });
  
  size_t num_threads = options_.num_threads;
  if (num_threads == 0) {
    num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) {
      num_threads = 4;
    }
  }
  
  size_t batch_size = options_.vertex_batch_size;
  size_t total = vertex_items_.size();
  size_t num_batches = (total + batch_size - 1) / batch_size;
  
  std::vector<std::future<Status>> futures;
  
  for (size_t b = 0; b < num_batches; ++b) {
    size_t start = b * batch_size;
    size_t end = std::min(start + batch_size, total);
    
    futures.push_back(std::async(std::launch::async, [this, start, end]() {
      // 创建批量事务执行器
      BatchTransactionExecutor batch(storage_);
      
      for (size_t i = start; i < end; ++i) {
        const auto& item = vertex_items_[i];
        if (item.type == BatchItem::VERTEX_PUT) {
          batch.AddPut(item.id, EntityType::Vertex, item.property_id,
                       item.descriptor, item.timestamp);
        }
      }
      
      return batch.Execute();
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
  
  return Status::OK();
}

Status ParallelGraphBatchProcessor::ExecuteEdgeBatch() {
  if (edge_items_.empty()) {
    return Status::OK();
  }
  
  // 按源顶点ID分区
  std::sort(edge_items_.begin(), edge_items_.end(),
            [](const BatchItem& a, const BatchItem& b) {
              return a.id < b.id;
            });
  
  // 边操作使用单线程批量处理（避免复杂冲突）
  BatchTransactionExecutor batch(storage_);
  
  for (const auto& item : edge_items_) {
    if (item.type == BatchItem::EDGE_PUT) {
      // 边使用 EdgeOut 类型
      batch.AddPut(item.id, EntityType::EdgeOut, item.property_id,
                   item.descriptor, item.timestamp);
    }
  }
  
  return batch.Execute();
}

void ParallelGraphBatchProcessor::Clear() {
  std::lock_guard<std::mutex> lock(items_mutex_);
  vertex_items_.clear();
  edge_items_.clear();
}

size_t ParallelGraphBatchProcessor::PendingCount() const {
  std::lock_guard<std::mutex> lock(items_mutex_);
  return vertex_items_.size() + edge_items_.size();
}

}  // namespace cedar
