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

#include "cedar/transaction/sharded_timestamp_allocator.h"

#include <thread>

namespace cedar {

// ==================== ShardedTimestampAllocator ====================

ShardedTimestampAllocator::ShardedTimestampAllocator(const Options& options)
    : batch_size_(options.batch_size) {
  size_t num_shards = options.num_shards;
  if (num_shards == 0) {
    num_shards = std::thread::hardware_concurrency();
    if (num_shards == 0) {
      num_shards = 4;  // 默认值
    }
  }
  
  // 创建分片
  for (size_t i = 0; i < num_shards; ++i) {
    shards_.push_back(std::make_unique<Shard>());
    RefillShard(*shards_.back());
  }
}

Timestamp ShardedTimestampAllocator::Allocate() {
  size_t idx = GetShardIndex();
  Shard& shard = *shards_[idx];
  
  // 尝试从本地分配
  uint64_t local = shard.local_next.load(std::memory_order_relaxed);
  uint64_t end = shard.local_end.load(std::memory_order_relaxed);
  
  if (local < end) {
    // 本地分配成功
    shard.local_next.store(local + 1, std::memory_order_relaxed);
    shard.allocated_count.fetch_add(1, std::memory_order_relaxed);
    return Timestamp(local);
  }
  
  // 本地耗尽，需要重新填充
  if (RefillShard(shard)) {
    local = shard.local_next.load(std::memory_order_relaxed);
    shard.local_next.store(local + 1, std::memory_order_relaxed);
    shard.allocated_count.fetch_add(1, std::memory_order_relaxed);
    return Timestamp(local);
  }
  
  // 回退：直接分配（不应该发生）
  return Timestamp(global_next_.fetch_add(1, std::memory_order_acq_rel));
}

Timestamp ShardedTimestampAllocator::AllocateBatch(uint32_t count) {
  if (count == 0) {
    return Timestamp(0);
  }
  
  if (count == 1) {
    return Allocate();
  }
  
  // 批量分配：直接操作全局计数器
  uint64_t start = global_next_.fetch_add(count, std::memory_order_acq_rel);
  
  size_t idx = GetShardIndex();
  shards_[idx]->allocated_count.fetch_add(count, std::memory_order_relaxed);
  
  return Timestamp(start);
}

Timestamp ShardedTimestampAllocator::CurrentTimestamp() const {
  return Timestamp(global_next_.load(std::memory_order_acquire) - 1);
}

ShardedTimestampAllocator::Stats ShardedTimestampAllocator::GetStats() const {
  Stats stats;
  stats.num_shards = static_cast<uint32_t>(shards_.size());
  
  for (const auto& shard : shards_) {
    stats.total_allocated += shard->allocated_count.load(std::memory_order_relaxed);
  }
  
  std::lock_guard<std::mutex> lock(stats_mutex_);
  stats.global_refills = global_refills_;
  
  return stats;
}

size_t ShardedTimestampAllocator::GetShardIndex() const {
  // 使用线程ID的哈希值选择分片
  std::hash<std::thread::id> hasher;
  size_t tid = hasher(std::this_thread::get_id());
  return tid % shards_.size();
}

bool ShardedTimestampAllocator::RefillShard(Shard& shard) {
  // 从全局获取一批时间戳
  uint64_t batch_start = global_next_.fetch_add(batch_size_, std::memory_order_acq_rel);
  
  shard.local_next.store(batch_start, std::memory_order_relaxed);
  shard.local_end.store(batch_start + batch_size_, std::memory_order_relaxed);
  
  {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    global_refills_++;
  }
  
  return true;
}

// ==================== ShardedTxnIdAllocator ====================

ShardedTxnIdAllocator::ShardedTxnIdAllocator(const Options& options)
    : batch_size_(options.batch_size) {
  size_t num_shards = options.num_shards;
  if (num_shards == 0) {
    num_shards = std::thread::hardware_concurrency();
    if (num_shards == 0) {
      num_shards = 4;
    }
  }
  
  for (size_t i = 0; i < num_shards; ++i) {
    shards_.push_back(std::make_unique<Shard>());
    RefillShard(*shards_.back());
  }
}

uint64_t ShardedTxnIdAllocator::Allocate() {
  size_t idx = GetShardIndex();
  Shard& shard = *shards_[idx];
  
  uint64_t local = shard.local_next.load(std::memory_order_relaxed);
  uint64_t end = shard.local_end.load(std::memory_order_relaxed);
  
  if (local < end) {
    shard.local_next.store(local + 1, std::memory_order_relaxed);
    return local;
  }
  
  if (RefillShard(shard)) {
    local = shard.local_next.load(std::memory_order_relaxed);
    shard.local_next.store(local + 1, std::memory_order_relaxed);
    return local;
  }
  
  return global_next_.fetch_add(1, std::memory_order_acq_rel);
}

size_t ShardedTxnIdAllocator::GetShardIndex() const {
  std::hash<std::thread::id> hasher;
  size_t tid = hasher(std::this_thread::get_id());
  return tid % shards_.size();
}

bool ShardedTxnIdAllocator::RefillShard(Shard& shard) {
  uint64_t batch_start = global_next_.fetch_add(batch_size_, std::memory_order_acq_rel);
  
  shard.local_next.store(batch_start, std::memory_order_relaxed);
  shard.local_end.store(batch_start + batch_size_, std::memory_order_relaxed);
  
  return true;
}

}  // namespace cedar
