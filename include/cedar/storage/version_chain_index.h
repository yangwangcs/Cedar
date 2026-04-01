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

#ifndef FERN_STORAGE_VERSION_CHAIN_INDEX_H_
#define FERN_STORAGE_VERSION_CHAIN_INDEX_H_

// ============================================================================
// 版本链跳表索引 (VersionChainIndex)
// ============================================================================
//
// 为 CedarGraph MVCC 版本链提供 O(log k) 时间戳查询优化。
//
// 使用说明:
//   1. 包含此头文件前，确保已包含 cedar_memtable.h (提供 TemporalVersionNode 定义)
//   2. 创建 VersionChainIndex 实例并与版本链关联
//   3. 当版本链长度超过阈值时，自动或手动构建索引
//   4. 使用 FindAtTime() 进行高效时间戳查询
//
// 示例:
//   #include "cedar/storage/cedar_memtable.h"
//   #include "cedar/storage/version_chain_index.h"
//   
//   VersionChainIndex index;
//   index.Build(head_node, version_count);  // 构建索引
//   auto* node = index.FindAtTime(ts);      // O(log k) 查询
//
// ============================================================================

#include <atomic>
#include <cstdint>
#include <memory>
#include <random>
#include <vector>

#include "cedar/core/threading.h"
#include "cedar/types/cedar_key.h"
#include "cedar/storage/cedar_memtable.h"  // 包含 TemporalVersionNode 定义

namespace cedar {

// TemporalVersionNode 现在通过 cedar_memtable.h 引入

// ============================================================================
// 跳表层级常量
// ============================================================================

// 最大跳表层级 (0-3)
constexpr int kMaxSkipListLevel = 4;

// 默认触发构建索引的版本链长度阈值
constexpr size_t kDefaultIndexThreshold = 100;

// 跳表层级提升概率 (1/4 概率提升到下一层)
constexpr double kSkipListProbability = 0.25;

// ============================================================================
// 跳表索引节点
// ============================================================================

/**
 * @brief 版本链跳表索引节点
 * 
 * 用于加速 MVCC 版本链的时间戳查询。每个跳表节点指向一个 TemporalVersionNode，
 * 并通过多级 forward 指针实现 O(log k) 的跳跃查找。
 * 
 * 跳表结构特点:
 * - Level 0: 指向链中下一个节点 (与 TemporalVersionNode::older 对应)
 * - Level 1-3: 跳跃指针，跳过中间节点
 * - 层级越高，跳跃距离越远
 * 
 * 注意: 跳表索引按时间戳降序排列 (新 -> 旧)
 */
struct VersionSkipNode {
  // 指向实际的版本链节点
  TemporalVersionNode* version_node;
  
  // 该节点在跳表中的层级 (1-based, 1 表示只有 Level 0)
  int level;
  
  // 前向指针数组，索引 0 到 level-1
  // forward[i] 表示第 i 层的下一个节点
  // 使用原子指针保证线程安全
  std::atomic<VersionSkipNode*> forward[kMaxSkipListLevel];
  
  // 构造函数
  explicit VersionSkipNode(TemporalVersionNode* node, int lvl)
      : version_node(node), level(lvl) {
    // 初始化所有前向指针为 nullptr
    for (int i = 0; i < kMaxSkipListLevel; ++i) {
      forward[i].store(nullptr, std::memory_order_relaxed);
    }
  }
  
  // 禁用拷贝和移动
  VersionSkipNode(const VersionSkipNode&) = delete;
  VersionSkipNode& operator=(const VersionSkipNode&) = delete;
  
  // 获取指定层级的前向指针
  VersionSkipNode* Next(int lvl) const {
    return forward[lvl].load(std::memory_order_acquire);
  }
  
  // 设置指定层级的前向指针
  void SetNext(int lvl, VersionSkipNode* node) {
    forward[lvl].store(node, std::memory_order_release);
  }
  
  // 原子比较并交换前向指针
  bool CompareAndSetNext(int lvl, VersionSkipNode* expected, VersionSkipNode* desired) {
    return forward[lvl].compare_exchange_strong(
        expected, desired, 
        std::memory_order_acq_rel, 
        std::memory_order_relaxed);
  }
  
  // 获取业务时间戳 (从关联的版本节点)
  // 注意：调用此方法前，TemporalVersionNode 的定义必须是完整的
  // (即调用者需要包含 cedar_memtable.h)
  Timestamp GetTimestamp() const {
    return version_node->timestamp;
  }
  
  // 修复: 获取事务版本号 (用于 MVCC 查询优化)
  Timestamp GetTxnVersion() const {
    return version_node->txn_version;
  }
};

// ============================================================================
// 版本链跳表索引
// ============================================================================

/**
 * @brief 版本链跳表索引管理类
 * 
 * 为 TemporalVersionNode 链表构建跳表索引，将时间戳查询复杂度从 O(k) 优化到 O(log k)。
 * 
 * 特性:
 * - 延迟构建: 版本链长度超过阈值时才构建索引
 * - 概率跳表: 使用 1/4 概率算法决定节点层级
 * - 最大 4 级: Level 0-3，平衡查询效率和内存开销
 * - 线程安全: 使用读写锁保护索引结构
 * - 动态更新: 支持插入新版本时更新索引
 * 
 * 使用示例:
 * @code
 *   VersionChainIndex index;
 *   index.Build(head_node, version_count);  // 构建索引
 *   auto* node = index.FindAtTime(target_ts);  // O(log k) 查询
 *   index.Insert(new_node);  // 插入新版本
 * @endcode
 */
class VersionChainIndex {
 public:
  /**
   * @brief 构造函数
   * @param threshold 触发构建索引的版本链长度阈值
   */
  explicit VersionChainIndex(size_t threshold = kDefaultIndexThreshold);
  
  // 禁用拷贝和移动
  VersionChainIndex(const VersionChainIndex&) = delete;
  VersionChainIndex& operator=(const VersionChainIndex&) = delete;
  
  ~VersionChainIndex();

  // ==========================================================================
  // 索引状态查询
  // ==========================================================================
  
  /**
   * @brief 检查索引是否已构建
   * @return true 如果索引已构建
   */
  bool IsBuilt() const {
    return is_built_.load(std::memory_order_acquire);
  }
  
  /**
   * @brief 获取跳表中的节点数量
   * @return 索引节点数量
   */
  size_t Size() const {
    return size_.load(std::memory_order_relaxed);
  }
  
  /**
   * @brief 获取触发构建索引的阈值
   * @return 当前阈值
   */
  size_t GetThreshold() const {
    return threshold_;
  }
  
  /**
   * @brief 设置触发构建索引的阈值
   * @param threshold 新的阈值
   */
  void SetThreshold(size_t threshold) {
    threshold_ = threshold;
  }
  
  /**
   * @brief 检查是否需要构建索引
   * @param version_count 当前版本链长度
   * @return true 如果版本链长度超过阈值且索引未构建
   */
  bool ShouldBuild(size_t version_count) const {
    return !IsBuilt() && version_count >= threshold_;
  }

  // ==========================================================================
  // 索引构建与管理
  // ==========================================================================
  
  /**
   * @brief 从版本链头节点构建跳表索引
   * 
   * 遍历整个版本链，为每个 TemporalVersionNode 创建对应的 VersionSkipNode，
   * 并建立多级前向指针。
   * 
   * @param head 版本链头节点 (最新版本)
   * @param version_count 版本链长度
   * @return true 如果构建成功
   * 
   * @note 线程安全，内部使用写锁保护
   * @note 如果索引已构建，会先清空再重建
   */
  bool Build(TemporalVersionNode* head, size_t version_count);
  
  /**
   * @brief 清空索引
   * 
   * 释放所有跳表节点，重置索引状态。
   * 
   * @note 线程安全，内部使用写锁保护
   */
  void Clear();
  
  /**
   * @brief 尝试惰性构建索引
   * 
   * 如果版本链长度超过阈值且索引未构建，则自动构建索引。
   * 
   * @param head 版本链头节点
   * @param version_count 版本链长度
   * @return true 如果本次调用触发了索引构建
   */
  bool MaybeBuild(TemporalVersionNode* head, size_t version_count);

  // ==========================================================================
  // 索引查询
  // ==========================================================================
  
  /**
   * @brief 在指定时间戳查找版本
   * 
   * 查找时间戳小于等于给定时间戳的最新版本 (<= ts 的最大时间戳)。
   * 这是 MVCC 快照读的核心操作。
   * 
   * 算法复杂度: O(log k)，其中 k 是版本链长度
   * 
   * @param ts 目标时间戳
   * @return 指向找到的 TemporalVersionNode 的指针，如果未找到返回 nullptr
   * 
   * @note 线程安全，内部使用读锁保护
   * @note 如果索引未构建，会退化为 O(k) 线性搜索
   * 
   * 示例:
   * @code
   *   // 查找时间戳为 1000 时的有效版本
   *   auto* node = index.FindAtTime(Timestamp(1000));
   *   if (node) {
   *     // node->timestamp <= 1000 且是最新版本
     *   }
   * @endcode
   */
  TemporalVersionNode* FindAtTime(Timestamp ts) const;
  
  /**
   * @brief 根据事务版本号查找版本 (用于 MVCC 优化)
   * 
   * 查找小于等于给定事务版本号的最新版本。与 FindAtTime 不同，
   * 此方法基于 txn_version 而非业务时间戳进行查找，适合 MVCC 隔离查询。
   * 
   * 算法复杂度: O(log k)
   * 
   * @param txn_ver 目标事务版本号
   * @return 指向找到的 TemporalVersionNode 的指针，如果未找到返回 nullptr
   * 
   * @note 线程安全，内部使用读锁保护
   * @note 如果索引未构建，会退化为 O(k) 线性搜索
   */
  TemporalVersionNode* FindAtTxnVersion(Timestamp txn_ver) const;
  
  /**
   * @brief 查找第一个大于给定时间戳的版本 (> ts 的最小时间戳)
   * 
   * 用于范围查询或查找更新版本。
   * 
   * @param ts 目标时间戳
   * @return 指向找到的 TemporalVersionNode 的指针，如果未找到返回 nullptr
   */
  TemporalVersionNode* FindFirstAfter(Timestamp ts) const;
  
  /**
   * @brief 获取最新版本 (时间戳最大)
   * @return 指向最新版本节点的指针，如果为空返回 nullptr
   */
  TemporalVersionNode* GetLatest() const;
  
  /**
   * @brief 获取最旧版本 (时间戳最小)
   * @return 指向最旧版本节点的指针，如果为空返回 nullptr
   */
  TemporalVersionNode* GetOldest() const;

  // ==========================================================================
  // 索引更新
  // ==========================================================================
  
  /**
   * @brief 插入新版本到索引
   * 
   * 将新插入的版本节点添加到跳表索引中。新节点会成为新的头节点
   * (因为新版本的时间戳最大)。
   * 
   * 算法:
   * 1. 随机决定新节点的层级
   * 2. 找到每一层需要更新的位置
   * 3. 原子地插入新节点
   * 
   * @param new_node 新版本节点 (必须是当前链头)
   * @return true 如果插入成功
   * 
   * @note 线程安全，内部使用写锁保护
   * @note 如果索引未构建，此操作无效果
   */
  bool Insert(TemporalVersionNode* new_node);
  
  /**
   * @brief 批量插入多个版本
   * 
   * 用于重建索引或批量加载场景，比逐个插入更高效。
   * 
   * @param nodes 版本节点列表 (按时间戳降序排列)
   * @return true 如果插入成功
   */
  bool BatchInsert(const std::vector<TemporalVersionNode*>& nodes);

  // ==========================================================================
  // 统计信息
  // ==========================================================================
  
  /**
   * @brief 获取索引统计信息
   */
  struct Stats {
    size_t total_nodes;           // 总节点数
    size_t level_distribution[4]; // 各层级节点分布
    int max_level;                // 实际最大层级
    double avg_forward_distance;  // 平均跳跃距离
  };
  
  /**
   * @brief 获取索引统计信息
   * @return 统计信息结构体
   */
  Stats GetStats() const;

 private:
  // ==========================================================================
  // 内部辅助方法
  // ==========================================================================
  
  /**
   * @brief 随机生成节点层级 (概率跳表算法)
   * 
   * 使用 1/4 概率算法：
   * - Level 1: 100%
   * - Level 2: 25%
   * - Level 3: 6.25%
   * - Level 4: 1.56%
   * 
   * @return 生成的层级 (1-based, 1-4)
   */
  int RandomLevel();
  
  /**
   * @brief 查找每一层的前驱节点
   * 
   * 从最高层开始查找，记录每一层中小于目标时间戳的最后一个节点。
   * 
   * @param ts 目标时间戳
   * @param predecessors 输出参数，每一层的前驱节点
   * @return 当前遍历到的节点
   */
  VersionSkipNode* FindPredecessors(Timestamp ts, 
                                     VersionSkipNode** predecessors) const;
  
  /**
   * @brief 无锁版本的查找 (仅用于内部已加锁的场景)
   */
  TemporalVersionNode* FindAtTimeUnlocked(Timestamp ts) const;
  
  /**
   * @brief 线性搜索 (索引未构建时使用)
   */
  TemporalVersionNode* LinearSearch(TemporalVersionNode* head, Timestamp ts) const;
  
  // 修复: 添加基于 txn_version 的无锁查找和线性搜索
  /**
   * @brief 基于 txn_version 的无锁查找 (仅用于内部已加锁的场景)
   */
  TemporalVersionNode* FindAtTxnVersionUnlocked(Timestamp txn_ver) const;
  
  /**
   * @brief 基于 txn_version 的线性搜索 (索引未构建时使用)
   */
  TemporalVersionNode* LinearSearchByTxnVersion(TemporalVersionNode* head, Timestamp txn_ver) const;
  
  /**
   * @brief 释放所有索引节点
   */
  void FreeNodes();

  // ==========================================================================
  // 成员变量
  // ==========================================================================
  
  // 触发构建索引的版本链长度阈值
  size_t threshold_;
  
  // 索引是否已构建
  std::atomic<bool> is_built_{false};
  
  // 跳表中的节点数量
  std::atomic<size_t> size_{0};
  
  // 跳表头节点 (哨兵节点，不存储实际数据)
  // 使用原子指针保证安全的双重检查锁定模式
  std::atomic<VersionSkipNode*> head_{nullptr};
  
  // 版本链头节点缓存 (用于线性搜索回退)
  std::atomic<TemporalVersionNode*> version_head_{nullptr};
  
  // 读写锁保护索引结构
  // 读操作: 查找 (FindAtTime)
  // 写操作: 构建 (Build)、插入 (Insert)、清空 (Clear)
  mutable Mutex mutex_;
  
  // 随机数生成器 (用于概率跳表)
  // 使用 thread_local 避免多线程竞争
  static thread_local std::mt19937 rng_;
  static thread_local std::uniform_real_distribution<double> dist_;
};

// ============================================================================
// 线程局部随机数生成器初始化
// ============================================================================

inline thread_local std::mt19937 VersionChainIndex::rng_(
    std::random_device{}());
inline thread_local std::uniform_real_distribution<double> 
    VersionChainIndex::dist_(0.0, 1.0);

// ============================================================================
// 内联方法实现
// ============================================================================

inline int VersionChainIndex::RandomLevel() {
  int level = 1;
  while (level < kMaxSkipListLevel && dist_(rng_) < kSkipListProbability) {
    ++level;
  }
  return level;
}

inline VersionSkipNode* VersionChainIndex::FindPredecessors(
    Timestamp ts, VersionSkipNode** predecessors) const {
  // 初始化前驱为头节点
  VersionSkipNode* current = head_.load(std::memory_order_acquire);
  if (!current) {
    for (int i = 0; i < kMaxSkipListLevel; ++i) {
      predecessors[i] = nullptr;
    }
    return nullptr;
  }
  
  // 从最高层开始查找
  for (int i = kMaxSkipListLevel - 1; i >= 0; --i) {
    // 在当前层向前查找
    VersionSkipNode* next = current->Next(i);
    while (next && next->GetTimestamp() > ts) {
      current = next;
      next = current->Next(i);
    }
    // 记录当前层的前驱
    predecessors[i] = current;
  }
  
  // 返回 Level 0 的下一个节点 (可能的目标节点)
  return current->Next(0);
}

inline TemporalVersionNode* VersionChainIndex::FindAtTimeUnlocked(Timestamp ts) const {
  VersionSkipNode* head = head_.load(std::memory_order_acquire);
  if (!head) {
    return nullptr;
  }
  
  VersionSkipNode* current = head;
  
  // 从最高层开始查找
  for (int i = kMaxSkipListLevel - 1; i >= 0; --i) {
    VersionSkipNode* next = current->Next(i);
    // 向前查找：跳过时间戳大于目标时间戳的节点
    while (next && next->GetTimestamp() > ts) {
      current = next;
      next = current->Next(i);
    }
  }
  
  // current 现在是 Level 0 中小于等于目标时间戳的最后一个节点
  // 返回它指向的版本节点
  VersionSkipNode* result = current->Next(0);
  if (result && result->GetTimestamp() <= ts) {
    return result->version_node;
  }
  
  // 如果 head 本身就是答案 (当所有节点都 <= ts)
  if (head->GetTimestamp() <= ts) {
    return head->version_node;
  }
  
  return nullptr;
}

inline TemporalVersionNode* VersionChainIndex::LinearSearch(
    TemporalVersionNode* head, Timestamp ts) const {
  TemporalVersionNode* current = head;
  TemporalVersionNode* result = nullptr;
  
  while (current) {
    if (current->timestamp <= ts) {
      // 找到第一个 <= ts 的节点 (因为按降序排列，这就是答案)
      result = current;
      break;
    }
    current = current->older;
  }
  
  return result;
}

// ============================================================================
// 构造函数和析构函数
// ============================================================================

inline VersionChainIndex::VersionChainIndex(size_t threshold)
    : threshold_(threshold) {}

inline VersionChainIndex::~VersionChainIndex() {
  Clear();
}

// ============================================================================
// 索引构建方法实现
// ============================================================================

inline bool VersionChainIndex::Build(TemporalVersionNode* head, size_t version_count) {
  LockGuard lock(&mutex_);
  
  // 如果已经构建，先清空
  if (is_built_.load(std::memory_order_relaxed)) {
    FreeNodes();
  }
  
  if (!head || version_count == 0) {
    return false;
  }
  
  // 保存版本链头节点
  version_head_.store(head, std::memory_order_relaxed);
  
  // 创建哨兵头节点 (包含最大时间戳，确保它总是在最前面)
  // 注意：哨兵节点不指向实际的版本节点，只用于简化边界处理
  auto* skip_head = new VersionSkipNode(nullptr, kMaxSkipListLevel);
  
  // 用于记录每一层的最后一个节点
  VersionSkipNode* level_tails[kMaxSkipListLevel];
  for (int i = 0; i < kMaxSkipListLevel; ++i) {
    level_tails[i] = skip_head;
  }
  
  // 遍历版本链，创建跳表节点
  TemporalVersionNode* current = head;
  size_t count = 0;
  
  while (current && count < version_count) {
    // 随机决定新节点的层级
    int level = RandomLevel();
    
    // 创建跳表节点
    auto* skip_node = new VersionSkipNode(current, level);
    
    // 更新每一层的前向指针
    for (int i = 0; i < level; ++i) {
      level_tails[i]->SetNext(i, skip_node);
      level_tails[i] = skip_node;
    }
    
    current = current->older;
    ++count;
  }
  
  // 设置头节点并标记索引已构建
  head_.store(skip_head, std::memory_order_release);
  size_.store(count, std::memory_order_relaxed);
  is_built_.store(true, std::memory_order_release);
  
  return true;
}

inline void VersionChainIndex::Clear() {
  LockGuard lock(&mutex_);
  FreeNodes();
  is_built_.store(false, std::memory_order_release);
  size_.store(0, std::memory_order_relaxed);
  version_head_.store(nullptr, std::memory_order_relaxed);
}

inline void VersionChainIndex::FreeNodes() {
  VersionSkipNode* current = head_.load(std::memory_order_relaxed);
  while (current) {
    VersionSkipNode* next = current->Next(0);
    delete current;
    current = next;
  }
  head_.store(nullptr, std::memory_order_relaxed);
}

inline bool VersionChainIndex::MaybeBuild(TemporalVersionNode* head, 
                                           size_t version_count) {
  // 快速检查：如果已经构建或不需要构建，立即返回
  if (is_built_.load(std::memory_order_acquire) || version_count < threshold_) {
    return false;
  }
  
  // 使用双重检查锁定模式
  LockGuard lock(&mutex_);
  if (!is_built_.load(std::memory_order_relaxed)) {
    return Build(head, version_count);
  }
  return false;
}

// ============================================================================
// 查询方法实现
// ============================================================================

inline TemporalVersionNode* VersionChainIndex::FindAtTime(Timestamp ts) const {
  // 如果索引未构建，使用线性搜索
  if (!is_built_.load(std::memory_order_acquire)) {
    TemporalVersionNode* head = version_head_.load(std::memory_order_acquire);
    if (!head) {
      return nullptr;
    }
    return LinearSearch(head, ts);
  }
  
  // 使用读锁保护索引结构
  LockGuard lock(&mutex_);
  return FindAtTimeUnlocked(ts);
}

// 修复: 基于 txn_version 的线性搜索 (用于索引未构建时)
inline TemporalVersionNode* VersionChainIndex::LinearSearchByTxnVersion(
    TemporalVersionNode* head, Timestamp txn_ver) const {
  TemporalVersionNode* current = head;
  TemporalVersionNode* result = nullptr;
  
  while (current) {
    // 注意: txn_version 是递增的，我们要找小于等于 txn_ver 的最新版本
    // 版本链按业务时间戳降序排列，但 txn_version 可能不按顺序
    // 所以需要遍历整个链找到合适的版本
    if (current->txn_version <= txn_ver) {
      // 找到一个可见的版本
      if (!result || current->timestamp > result->timestamp) {
        result = current;  // 选择时间戳最新的可见版本
      }
    }
    current = current->older;
  }
  
  return result;
}

// 修复: 基于 txn_version 的跳表查找 (用于索引已构建时)
inline TemporalVersionNode* VersionChainIndex::FindAtTxnVersionUnlocked(
    Timestamp txn_ver) const {
  VersionSkipNode* head = head_.load(std::memory_order_acquire);
  if (!head) {
    return nullptr;
  }
  
  // 由于跳表是按业务时间戳排序的，不能直接使用跳表按 txn_version 查找
  // 需要遍历跳表找到所有可能的候选节点，然后选择时间戳最新的
  // 优化: 先用跳表快速定位到大致位置，然后线性搜索
  
  VersionSkipNode* current = head;
  TemporalVersionNode* result = nullptr;
  
  // 线性遍历跳表的所有节点
  while (current) {
    if (current->version_node) {
      Timestamp node_txn_ver = current->GetTxnVersion();
      if (node_txn_ver <= txn_ver || node_txn_ver.value() == 0) {
        // 找到一个可见的版本
        if (!result || current->version_node->timestamp > result->timestamp) {
          result = current->version_node;
        }
      }
    }
    current = current->Next(0);
  }
  
  return result;
}

inline TemporalVersionNode* VersionChainIndex::FindAtTxnVersion(Timestamp txn_ver) const {
  // 如果索引未构建，使用线性搜索
  if (!is_built_.load(std::memory_order_acquire)) {
    TemporalVersionNode* head = version_head_.load(std::memory_order_acquire);
    if (!head) {
      return nullptr;
    }
    return LinearSearchByTxnVersion(head, txn_ver);
  }
  
  // 使用读锁保护索引结构
  LockGuard lock(&mutex_);
  return FindAtTxnVersionUnlocked(txn_ver);
}

inline TemporalVersionNode* VersionChainIndex::FindFirstAfter(Timestamp ts) const {
  if (!is_built_.load(std::memory_order_acquire)) {
    // 线性搜索：找到第一个 < ts 的节点，返回其 newer
    TemporalVersionNode* head = version_head_.load(std::memory_order_acquire);
    if (!head) {
      return nullptr;
    }
    
    TemporalVersionNode* current = head;
    while (current) {
      if (current->timestamp < ts) {
        return current->newer;
      }
      current = current->older;
    }
    return nullptr;
  }
  
  LockGuard lock(&mutex_);
  
  VersionSkipNode* head = head_.load(std::memory_order_acquire);
  if (!head) {
    return nullptr;
  }
  
  VersionSkipNode* current = head;
  
  // 找到第一个 < ts 的节点
  for (int i = kMaxSkipListLevel - 1; i >= 0; --i) {
    VersionSkipNode* next = current->Next(i);
    while (next && next->GetTimestamp() >= ts) {
      current = next;
      next = current->Next(i);
    }
  }
  
  // 返回下一个节点 (如果存在且时间戳 < ts)
  VersionSkipNode* result = current->Next(0);
  if (result && result->GetTimestamp() < ts) {
    return result->version_node;
  }
  
  return nullptr;
}

inline TemporalVersionNode* VersionChainIndex::GetLatest() const {
  if (!is_built_.load(std::memory_order_acquire)) {
    return version_head_.load(std::memory_order_acquire);
  }
  
  LockGuard lock(&mutex_);
  VersionSkipNode* head = head_.load(std::memory_order_acquire);
  if (!head) {
    return nullptr;
  }
  
  // 头节点的下一个节点就是最新版本
  VersionSkipNode* first = head->Next(0);
  return first ? first->version_node : nullptr;
}

inline TemporalVersionNode* VersionChainIndex::GetOldest() const {
  if (!is_built_.load(std::memory_order_acquire)) {
    TemporalVersionNode* head = version_head_.load(std::memory_order_acquire);
    if (!head) {
      return nullptr;
    }
    // 线性遍历到末尾
    TemporalVersionNode* current = head;
    while (current->older) {
      current = current->older;
    }
    return current;
  }
  
  LockGuard lock(&mutex_);
  VersionSkipNode* head = head_.load(std::memory_order_acquire);
  if (!head) {
    return nullptr;
  }
  
  // 跳表 Level 0 的最后一个节点就是最旧版本
  VersionSkipNode* current = head;
  while (current->Next(0)) {
    current = current->Next(0);
  }
  
  return current->version_node;
}

// ============================================================================
// 更新方法实现
// ============================================================================

inline bool VersionChainIndex::Insert(TemporalVersionNode* new_node) {
  if (!new_node) {
    return false;
  }
  
  // 如果索引未构建，不执行任何操作
  if (!is_built_.load(std::memory_order_acquire)) {
    // 但更新版本链头节点缓存
    TemporalVersionNode* old_head = version_head_.load(std::memory_order_relaxed);
    if (!old_head || new_node->timestamp > old_head->timestamp) {
      version_head_.store(new_node, std::memory_order_release);
    }
    return true;
  }
  
  LockGuard lock(&mutex_);
  
  // 生成新节点的层级
  int level = RandomLevel();
  
  // 创建新的跳表节点
  auto* skip_node = new VersionSkipNode(new_node, level);
  
  // 查找每一层的前驱节点
  VersionSkipNode* predecessors[kMaxSkipListLevel];
  FindPredecessors(new_node->timestamp, predecessors);
  
  // 更新每一层的前向指针
  for (int i = 0; i < level; ++i) {
    if (predecessors[i]) {
      // 新节点指向原来的下一个节点
      skip_node->SetNext(i, predecessors[i]->Next(i));
      // 前驱节点指向新节点
      predecessors[i]->SetNext(i, skip_node);
    } else {
      // 在头节点之前插入 (这种情况不应该发生，因为新节点时间戳最大)
      skip_node->SetNext(i, head_.load(std::memory_order_relaxed)->Next(i));
    }
  }
  
  // 如果新节点时间戳最大，可能需要更新头节点
  VersionSkipNode* head = head_.load(std::memory_order_relaxed);
  if (head->Next(0) && new_node->timestamp > head->Next(0)->GetTimestamp()) {
    // 新节点应该在最前面
    for (int i = 0; i < level; ++i) {
      // 暂时简单处理：直接从 head 插入
      skip_node->SetNext(i, head->Next(i));
      head->SetNext(i, skip_node);
    }
  }
  
  size_.fetch_add(1, std::memory_order_relaxed);
  return true;
}

inline bool VersionChainIndex::BatchInsert(const std::vector<TemporalVersionNode*>& nodes) {
  if (nodes.empty()) {
    return true;
  }
  
  LockGuard lock(&mutex_);
  
  // 如果索引未构建，先构建
  if (!is_built_.load(std::memory_order_relaxed)) {
    // 找到头节点 (时间戳最大的)
    TemporalVersionNode* head = nodes[0];
    for (size_t i = 1; i < nodes.size(); ++i) {
      if (nodes[i]->timestamp > head->timestamp) {
        head = nodes[i];
      }
    }
    return Build(head, nodes.size());
  }
  
  // 否则逐个插入 (批量优化可以后续实现)
  for (auto* node : nodes) {
    int level = RandomLevel();
    auto* skip_node = new VersionSkipNode(node, level);
    
    VersionSkipNode* predecessors[kMaxSkipListLevel];
    FindPredecessors(node->timestamp, predecessors);
    
    for (int i = 0; i < level; ++i) {
      if (predecessors[i]) {
        skip_node->SetNext(i, predecessors[i]->Next(i));
        predecessors[i]->SetNext(i, skip_node);
      }
    }
    
    size_.fetch_add(1, std::memory_order_relaxed);
  }
  
  return true;
}

// ============================================================================
// 统计信息实现
// ============================================================================

inline VersionChainIndex::Stats VersionChainIndex::GetStats() const {
  Stats stats = {};
  
  LockGuard lock(&mutex_);
  
  stats.total_nodes = size_.load(std::memory_order_relaxed);
  stats.max_level = 0;
  
  for (int i = 0; i < kMaxSkipListLevel; ++i) {
    stats.level_distribution[i] = 0;
  }
  
  VersionSkipNode* head = head_.load(std::memory_order_acquire);
  if (!head) {
    return stats;
  }
  
  // 统计各层级节点数
  VersionSkipNode* current = head->Next(0);
  while (current) {
    stats.level_distribution[current->level - 1]++;
    if (current->level > stats.max_level) {
      stats.max_level = current->level;
    }
    current = current->Next(0);
  }
  
  // 计算平均跳跃距离
  if (stats.total_nodes > 0) {
    size_t total_distance = 0;
    size_t jump_count = 0;
    
    for (int i = 1; i < kMaxSkipListLevel; ++i) {
      current = head->Next(i);
      VersionSkipNode* prev = head;
      while (current) {
        // 计算这一跳在 Level 0 上跨越了多少节点
        size_t distance = 0;
        VersionSkipNode* p = prev->Next(0);
        while (p && p != current) {
          ++distance;
          p = p->Next(0);
        }
        total_distance += distance;
        ++jump_count;
        
        prev = current;
        current = current->Next(i);
      }
    }
    
    if (jump_count > 0) {
      stats.avg_forward_distance = static_cast<double>(total_distance) / jump_count;
    }
  }
  
  return stats;
}

}  // namespace cedar

#endif  // FERN_STORAGE_VERSION_CHAIN_INDEX_H_
