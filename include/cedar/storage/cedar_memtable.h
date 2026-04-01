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

#ifndef FERN_FERN_MEMTABLE_H_
#define FERN_FERN_MEMTABLE_H_

#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "cedar/types/descriptor.h"
#include "cedar/types/cedar_key.h"

namespace cedar {

// 前向声明
struct TemporalVersionNode;

// MemTable 中的条目 (保留所有历史版本)
struct MemTableEntry {
  Timestamp timestamp;      // 业务时间戳
  Timestamp txn_version;    // 事务版本号（用于 MVCC）
  Descriptor descriptor;
  std::optional<uint64_t> dst_id;  // 目标节点ID (用于边)

  MemTableEntry() = default;
  MemTableEntry(Timestamp ts, const Descriptor& desc, Timestamp txn_ver)
      : timestamp(ts), txn_version(txn_ver), descriptor(desc) {}
  MemTableEntry(Timestamp ts, const Descriptor& desc, std::optional<uint64_t> dst, Timestamp txn_ver)
      : timestamp(ts), txn_version(txn_ver), descriptor(desc), dst_id(dst) {}
  
  // 显式构造函数，避免 emplace_back 歧义
  static MemTableEntry Make(Timestamp ts, const Descriptor& desc, 
                            std::optional<uint64_t> dst, Timestamp txn_ver) {
    return MemTableEntry(ts, desc, dst, txn_ver);
  }
};

// MVCC 版本链节点 - 用于高效遍历实体的所有时间版本
struct TemporalVersionNode {
  Timestamp timestamp;      // 业务时间戳（用于时序查询和 Key 排序）
  Timestamp txn_version;    // 事务版本号（用于 MVCC 隔离）
  Descriptor descriptor;
  
  // 版本链指针 (按时间降序: newer -> older)
  TemporalVersionNode* newer;  // 更新的版本 (时间戳更大)
  TemporalVersionNode* older;  // 更旧的版本 (时闶戳更小)
  
  TemporalVersionNode(Timestamp ts, const Descriptor& desc, Timestamp txn_ver)
      : timestamp(ts), txn_version(txn_ver), descriptor(desc), newer(nullptr), older(nullptr) {}
};

// 时态 MemTable - 支持多版本存储
class CedarMemTable {
 public:
  // 构造函数
  explicit CedarMemTable(size_t size_threshold = 4 * 1024 * 1024)
      : size_threshold_(size_threshold),
        approximate_size_(0),
        sequence_counter_(0) {}

  // 禁止拷贝
  CedarMemTable(const CedarMemTable&) = delete;
  CedarMemTable& operator=(const CedarMemTable&) = delete;

  // 插入数据 (保留所有历史版本)
  // 会自动分配序列号用于同时间戳去重
  void Put(CedarKey key, const Descriptor& descriptor, Timestamp txn_version);

  // 查询所有版本 (按 timestamp 降序返回)
  std::vector<MemTableEntry> GetAll(uint64_t entity_id,
                                     EntityType entity_type,
                                     uint16_t column_id) const;

  // 查询特定时间点的值 (返回 <= timestamp 的最新版本)
  std::optional<Descriptor> GetAtTime(uint64_t entity_id,
                                       EntityType entity_type,
                                       uint16_t column_id,
                                       Timestamp timestamp) const;

  // 查询时间范围 [start, end]
  std::vector<MemTableEntry> GetRange(uint64_t entity_id,
                                       EntityType entity_type,
                                       uint16_t column_id,
                                       Timestamp start,
                                       Timestamp end) const;

  // 简化版 Get (兼容旧接口) - 返回最新版本
  bool Get(uint64_t entity_id, uint64_t timestamp,
           std::string* value, bool* is_deleted) const;

  // 检查是否达到阈值
  bool IsFull() const {
    return approximate_size_.load(std::memory_order_relaxed) >= size_threshold_;
  }

  // 获取当前大小 (估算值)
  size_t ApproximateSize() const {
    return approximate_size_.load(std::memory_order_relaxed);
  }

  // 获取条目数 (不同的 InternalKey 数量)
  size_t NumEntries() const;

  // 是否为空
  bool IsEmpty() const;

  // 获取所有条目按时间排序 (用于 Flush 到 SST)
  // 返回: (CedarKey, Descriptor) 列表，按 (entity_id, entity_type, column_id, timestamp desc) 排序
  std::vector<std::pair<CedarKey, Descriptor>> GetSortedEntries() const;

  // ========== MVCC 版本链接口 ==========
  
  // 获取实体的版本链头节点 (最新版本)
  // 返回 nullptr 如果实体不存在
  TemporalVersionNode* GetVersionChainHead(uint64_t entity_id,
                                           EntityType entity_type,
                                           uint16_t column_id) const;
  
  // 获取实体的完整版本链 (按时间降序: 最新 -> 最旧)
  // 通过版本链遍历，O(k) 复杂度，k 为版本数量
  std::vector<MemTableEntry> GetVersionChain(uint64_t entity_id,
                                              EntityType entity_type,
                                              uint16_t column_id) const;
  
  // 遍历版本链的辅助函数
  // callback: 返回 false 停止遍历
  void TraverseVersionChain(uint64_t entity_id,
                            EntityType entity_type,
                            uint16_t column_id,
                            std::function<bool(const MemTableEntry&)> callback) const;
  
  // 获取版本链长度 (版本数量)
  size_t GetVersionChainLength(uint64_t entity_id,
                               EntityType entity_type,
                               uint16_t column_id) const;

  // ========== 多列支持 (Multi-Column Support) ==========
  
  // 获取实体的所有列 ID
  // 返回该实体在指定类型下的所有列标识符
  std::vector<uint16_t> GetColumnIds(uint64_t entity_id, 
                                      EntityType entity_type) const;
  
  // 获取实体所有列的最新版本（完整快照）
  // 返回每个列的最新值（各列时间戳可能不同）
  struct ColumnSnapshot {
    uint16_t column_id;
    Timestamp timestamp;
    Descriptor descriptor;
  };
  std::vector<ColumnSnapshot> GetLatestSnapshot(uint64_t entity_id, 
                                                 EntityType entity_type) const;
  
  // 获取实体在特定时间点的完整快照（所有列）
  // 返回 <= timestamp 的最新版本
  std::vector<ColumnSnapshot> GetSnapshotAtTime(uint64_t entity_id,
                                                 EntityType entity_type,
                                                 Timestamp timestamp) const;
  
  // 获取实体的所有版本链（跨所有列）
  struct MultiColumnVersion {
    uint16_t column_id;
    std::vector<MemTableEntry> versions;
  };
  std::vector<MultiColumnVersion> GetAllColumnChains(uint64_t entity_id,
                                                       EntityType entity_type) const;

  // 迭代器
  class Iterator;
  Iterator* NewIterator() const;

  // 版本链迭代器 - 专门用于遍历单个实体的所有版本
  class VersionChainIterator;
  VersionChainIterator* NewVersionChainIterator(uint64_t entity_id,
                                                 EntityType entity_type,
                                                 uint16_t column_id) const;

 private:
  // 获取下一个序列号
  uint16_t NextSequence();
  
  // 维护版本链: 在 Put 时更新
  void UpdateVersionChain(const InternalKey& internal_key, 
                          Timestamp timestamp, 
                          const Descriptor& descriptor,
                          Timestamp txn_version);

  // 内部存储: InternalKey -> 按 timestamp 降序排列的 entries
  // 使用 std::map 代替 Rust 的 SkipMap
  mutable std::mutex mutex_;
  std::map<InternalKey, std::vector<MemTableEntry>> map_;
  
  // MVCC 版本链存储: InternalKey -> 版本链头节点 (最新版本)
  // 使用 unordered_map 提供 O(1) 查找
  std::unordered_map<InternalKey, TemporalVersionNode*> version_chains_;
  
  // 版本链节点内存池 (避免频繁分配)
  std::vector<std::unique_ptr<TemporalVersionNode>> node_pool_;

  size_t size_threshold_;
  std::atomic<size_t> approximate_size_;
  std::atomic<uint16_t> sequence_counter_;
  std::atomic<size_t> version_chain_count_{0};  // 版本链数量统计
};

// MemTable 迭代器
class CedarMemTable::Iterator {
 public:
  explicit Iterator(const CedarMemTable* memtable);

  // 定位到第一行
  void SeekToFirst();

  // 定位到指定键
  void Seek(const CedarKey& key);

  // 移动到下一行
  void Next();

  // 是否有效
  bool Valid() const;

  // 获取当前 Key
  CedarKey Key() const;

  // 获取当前 Descriptor
  Descriptor Descriptor() const;

  // 获取当前 Entry (包含 timestamp)
  const MemTableEntry* Entry() const;

 private:
  const CedarMemTable* memtable_;
  std::map<InternalKey, std::vector<MemTableEntry>>::const_iterator outer_iter_;
  size_t inner_idx_;
  bool valid_;
};

// MVCC 版本链迭代器 - 高效遍历单个实体的所有版本
class CedarMemTable::VersionChainIterator {
 public:
  VersionChainIterator(TemporalVersionNode* head);

  // 定位到最新版本 (链头)
  void SeekToFirst();
  
  // 定位到最旧版本 (链尾)
  void SeekToLast();

  // 移动到更旧的版本
  void NextOlder();
  
  // 移动到更新的版本
  void NextNewer();

  // 是否有效
  bool Valid() const;
  
  // 获取当前节点
  const TemporalVersionNode* Node() const;

  // 获取当前 Entry
  MemTableEntry Entry() const;
  
  // 获取当前 Timestamp
  Timestamp GetTimestamp() const;
  
  // 获取当前 Descriptor
  Descriptor GetDescriptor() const;

 private:
  TemporalVersionNode* head_;      // 链头（最新版本），用于重新定位
  TemporalVersionNode* current_;   // 当前位置
};

}  // namespace cedar

#endif  // FERN_FERN_MEMTABLE_H_
