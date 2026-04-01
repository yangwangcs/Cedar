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

#ifndef FERN_ASYNC_INDEX_BUILDER_H_
#define FERN_ASYNC_INDEX_BUILDER_H_

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cedar/storage/version_chain_index.h"
#include "cedar/core/status.h"

namespace cedar {

// 前向声明
class LockFreeMemTableFull;
struct LockFreeHashNode;
struct TemporalVersionNode;

// 索引构建任务
struct IndexBuildTask {
  uint64_t entity_id;
  EntityType entity_type;
  uint16_t column_id;
  TemporalVersionNode* version_head;
  size_t version_count;
  uint64_t priority;  // 优先级 (越小越优先)
  std::chrono::steady_clock::time_point submit_time;
  
  // 回调函数 (构建完成后调用)
  std::function<void(VersionChainIndex*)> on_complete;
  
  // 计算优先级 (基于版本数量和等待时间)
  static uint64_t CalculatePriority(size_t version_count, 
                                     std::chrono::steady_clock::time_point submit_time);
  
  // 比较操作符 (用于优先队列)
  bool operator<(const IndexBuildTask& other) const {
    return priority > other.priority;  // 越小优先级越高
  }
};

// 索引构建结果
struct IndexBuildResult {
  uint64_t entity_id;
  EntityType entity_type;
  uint16_t column_id;
  bool success;
  VersionChainIndex* index;  // 如果成功，指向新构建的索引
  std::chrono::milliseconds build_duration;
  size_t versions_indexed;
  std::string error_message;
};

// 异步索引构建器配置
struct AsyncIndexBuilderOptions {
  // 工作线程数
  uint32_t num_worker_threads = 4;
  
  // 任务队列容量
  size_t task_queue_capacity = 10000;
  
  // 构建索引的阈值 (版本数量)
  size_t build_threshold = 100;
  
  // 最大同时构建的索引数
  uint32_t max_concurrent_builds = 16;
  
  // 构建超时 (毫秒)
  uint32_t build_timeout_ms = 5000;
  
  // 是否启用优先级调度
  bool enable_priority_scheduling = true;
  
  // 是否启用批量构建
  bool enable_batch_building = true;
  
  // 批量构建的最大批次大小
  size_t batch_max_size = 10;
  
  // 批量构建的超时 (毫秒)
  uint32_t batch_timeout_ms = 10;
  
  // 是否启用构建缓存 (避免重复构建)
  bool enable_build_cache = true;
  
  // 构建缓存大小
  size_t build_cache_size = 1000;
};

// 异步索引构建器统计 (内部使用原子，外部使用普通类型)
struct AsyncIndexBuilderStats {
  uint64_t tasks_submitted = 0;
  uint64_t tasks_completed = 0;
  uint64_t tasks_failed = 0;
  uint64_t tasks_timeout = 0;
  uint64_t indices_built = 0;
  uint64_t versions_indexed = 0;
  uint64_t batch_builds = 0;
  uint64_t avg_build_time_ms = 0;
  uint64_t queue_depth = 0;
};

// 索引构建缓存项
struct BuildCacheEntry {
  uint64_t entity_hash;
  std::chrono::steady_clock::time_point build_time;
  size_t version_count_at_build;
  bool valid;
};

// 异步索引构建器
// 后台线程池异步构建版本链索引，不阻塞前台读写
class AsyncIndexBuilder {
 public:
  explicit AsyncIndexBuilder(const AsyncIndexBuilderOptions& options = {});
  ~AsyncIndexBuilder();
  
  AsyncIndexBuilder(const AsyncIndexBuilder&) = delete;
  AsyncIndexBuilder& operator=(const AsyncIndexBuilder&) = delete;
  
  // 初始化
  Status Initialize();
  
  // 关闭
  Status Shutdown();
  
  // ========== 任务提交接口 ==========
  
  // 提交索引构建任务
  // 非阻塞，任务加入队列后立即返回
  Status SubmitTask(const IndexBuildTask& task);
  
  // 提交并等待完成 (阻塞)
  Status SubmitAndWait(const IndexBuildTask& task, 
                       IndexBuildResult* result,
                       uint32_t timeout_ms = 5000);
  
  // 批量提交任务
  Status SubmitBatch(const std::vector<IndexBuildTask>& tasks);
  
  // ========== 自动触发接口 ==========
  
  // 检查是否应该构建索引
  bool ShouldBuildIndex(size_t version_count) const {
    return version_count >= options_.build_threshold;
  }
  
  // 自动提交构建任务 (如果版本数超过阈值)
  Status MaybeSubmitBuild(uint64_t entity_id,
                          EntityType entity_type,
                          uint16_t column_id,
                          TemporalVersionNode* version_head,
                          size_t version_count);
  
  // ========== 查询接口 ==========
  
  // 获取统计
  AsyncIndexBuilderStats GetStats() const;
  
  // 获取队列深度
  size_t GetQueueDepth() const;
  
  // 检查是否正在构建指定实体的索引
  bool IsBuilding(uint64_t entity_id, EntityType entity_type, uint16_t column_id) const;
  
  // 等待所有任务完成
  Status WaitForAll(uint32_t timeout_ms = 30000);
  
 private:
  // 工作线程主循环
  void WorkerThreadLoop(uint32_t worker_id);
  
  // 执行单个构建任务
  IndexBuildResult ExecuteBuild(const IndexBuildTask& task);
  
  // 执行批量构建
  std::vector<IndexBuildResult> ExecuteBatchBuild(const std::vector<IndexBuildTask>& tasks);
  
  // 批量收集任务
  std::vector<IndexBuildTask> CollectBatch();
  
  // 检查缓存
  bool CheckCache(uint64_t entity_id, EntityType entity_type, uint16_t column_id);
  
  // 更新缓存
  void UpdateCache(uint64_t entity_id, EntityType entity_type, 
                   uint16_t column_id, size_t version_count);
  
  // 生成实体哈希
  static uint64_t HashEntity(uint64_t entity_id, EntityType entity_type, uint16_t column_id);
  
  AsyncIndexBuilderOptions options_;
  
  // 任务队列
  std::priority_queue<IndexBuildTask> task_queue_;
  mutable std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  
  // 工作线程
  std::vector<std::unique_ptr<std::thread>> workers_;
  std::atomic<bool> stop_flag_{false};
  
  // 正在构建的集合 (防止重复提交)
  std::unordered_map<uint64_t, std::chrono::steady_clock::time_point> building_set_;
  mutable std::mutex building_mutex_;
  
  // 结果回调映射
  std::unordered_map<uint64_t, std::promise<IndexBuildResult>> result_promises_;
  mutable std::mutex promise_mutex_;
  
  // 构建缓存 (LRU)
  std::unordered_map<uint64_t, BuildCacheEntry> build_cache_;
  mutable std::mutex cache_mutex_;
  std::queue<uint64_t> cache_lru_queue_;
  
  // 统计 (原子成员)
  mutable std::atomic<uint64_t> tasks_submitted_;
  mutable std::atomic<uint64_t> tasks_completed_;
  mutable std::atomic<uint64_t> tasks_failed_;
  mutable std::atomic<uint64_t> tasks_timeout_;
  mutable std::atomic<uint64_t> indices_built_;
  mutable std::atomic<uint64_t> versions_indexed_;
  mutable std::atomic<uint64_t> batch_builds_;
  mutable std::atomic<uint64_t> total_build_time_ms_;
};

// 与 LockFreeMemTableFull 的集成帮助类 (全局静态方法)
namespace AsyncIndexBuilderHelper {
  // 为 MemTable 启用异步索引构建
  Status EnableForMemTable(LockFreeMemTableFull* memtable,
                           const AsyncIndexBuilderOptions& options);
  
  // 禁用异步索引构建
  Status DisableForMemTable(LockFreeMemTableFull* memtable);
  
  // 获取与 MemTable 关联的构建器
  AsyncIndexBuilder* GetBuilderForMemTable(const LockFreeMemTableFull* memtable);
};

}  // namespace cedar

#endif  // FERN_ASYNC_INDEX_BUILDER_H_
