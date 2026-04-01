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

#ifndef FERN_SHARDED_TIMESTAMP_ALLOCATOR_H_
#define FERN_SHARDED_TIMESTAMP_ALLOCATOR_H_

#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>
#include <mutex>

#include "cedar/types/cedar_key.h"

namespace cedar {

// 分片时间戳分配器 - 消除多线程竞争
// 每个线程独立分配时间戳，仅在需要时从全局获取新批次
class ShardedTimestampAllocator {
 public:
  // 配置选项
  struct Options {
    size_t num_shards;        // 分片数，0表示自动(使用CPU核心数)
    uint32_t batch_size;      // 每批次预分配的时间戳数量
    
    Options() : num_shards(0), batch_size(1000) {}
    explicit Options(size_t shards, uint32_t batch = 1000)
        : num_shards(shards), batch_size(batch) {}
  };
  
  explicit ShardedTimestampAllocator(const Options& options = Options());
  
  ~ShardedTimestampAllocator() = default;
  
  // 分配时间戳 - 线程安全，无锁（大部分情况）
  Timestamp Allocate();
  
  // 批量分配时间戳
  Timestamp AllocateBatch(uint32_t count);
  
  // 获取当前全局时间戳（近似值）
  Timestamp CurrentTimestamp() const;
  
  // 获取全局下一个时间戳（用于GetMinActiveTimestamp）
  uint64_t GetGlobalNext() const {
    return global_next_.load(std::memory_order_acquire);
  }
  
  // 获取分配器统计
  struct Stats {
    uint64_t total_allocated = 0;
    uint64_t global_refills = 0;  // 全局补充次数
    uint32_t num_shards = 0;
  };
  Stats GetStats() const;
  
 private:
  // 线程本地分片
  struct alignas(64) Shard {  // 缓存行对齐避免伪共享
    std::atomic<uint64_t> local_next{0};      // 本地下一个时间戳
    std::atomic<uint64_t> local_end{0};       // 本地范围结束
    std::atomic<uint64_t> allocated_count{0}; // 分配计数
    
    // 填充到64字节避免伪共享
    char padding[64 - 3 * sizeof(std::atomic<uint64_t>)];
  };
  
  // 全局状态
  std::atomic<uint64_t> global_next_{1};  // 全局下一个时间戳
  
  std::vector<std::unique_ptr<Shard>> shards_;
  uint32_t batch_size_;
  
  mutable std::mutex stats_mutex_;
  uint64_t global_refills_ = 0;
  
  // 获取当前线程的分片索引
  size_t GetShardIndex() const;
  
  // 从全局获取新批次
  bool RefillShard(Shard& shard);
};

// 分片事务ID分配器
class ShardedTxnIdAllocator {
 public:
  struct Options {
    size_t num_shards;
    uint32_t batch_size;
    
    Options() : num_shards(0), batch_size(1000) {}
    Options(size_t shards, uint32_t batch) : num_shards(shards), batch_size(batch) {}
  };
  
  explicit ShardedTxnIdAllocator(const Options& options = Options());
  
  uint64_t Allocate();
  
 private:
  struct alignas(64) Shard {
    std::atomic<uint64_t> local_next{0};
    std::atomic<uint64_t> local_end{0};
    char padding[64 - 2 * sizeof(std::atomic<uint64_t>)];
  };
  
  std::atomic<uint64_t> global_next_{1};
  std::vector<std::unique_ptr<Shard>> shards_;
  uint32_t batch_size_;
  
  size_t GetShardIndex() const;
  bool RefillShard(Shard& shard);
};

}  // namespace cedar

#endif  // FERN_SHARDED_TIMESTAMP_ALLOCATOR_H_
