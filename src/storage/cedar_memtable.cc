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

#include "cedar/storage/cedar_memtable.h"

#include <iostream>
#include <algorithm>

namespace cedar {

uint16_t CedarMemTable::NextSequence() {
  return sequence_counter_.fetch_add(1, std::memory_order_seq_cst) % 65536;
}

void CedarMemTable::Put(CedarKey key, const Descriptor& descriptor, Timestamp txn_version) {
  std::lock_guard<std::mutex> lock(mutex_);

  // 分配序列号
  key.SetSequence(NextSequence());

  InternalKey internal_key(key);
  // 修复: 使用传入的 txn_version 创建 MemTableEntry
  uint64_t tgt_id = key.target_id();
  if (key.entity_id() == 344 && key.entity_type() == EntityType::EdgeOut) {
    std::cerr << "DEBUG: CedarMemTable::Put entity_id=344, target_id=" << tgt_id << std::endl;
  }
  MemTableEntry entry(key.timestamp(), descriptor, tgt_id, txn_version);

  // 计算大小增量 (估算)
  size_t size_increment = sizeof(CedarKey) + sizeof(Descriptor) + sizeof(MemTableEntry);

  // 查找或插入
  auto it = map_.find(internal_key);
  if (it != map_.end()) {
    // 已存在，插入到合适位置 (保持 timestamp 降序)
    std::vector<MemTableEntry>& entries = it->second;

    // 找到插入位置
    auto insert_pos = entries.begin();
    while (insert_pos != entries.end() && insert_pos->timestamp > entry.timestamp) {
      ++insert_pos;
    }

    entries.insert(insert_pos, entry);
  } else {
    // 新建
    map_[internal_key] = {entry};
    size_increment += sizeof(internal_key);  // key 本身的开销
  }

  approximate_size_.fetch_add(size_increment, std::memory_order_relaxed);
  
  // 更新 MVCC 版本链
  UpdateVersionChain(internal_key, key.timestamp(), descriptor, txn_version);
}

void CedarMemTable::UpdateVersionChain(const InternalKey& internal_key, 
                                       Timestamp timestamp, 
                                       const Descriptor& descriptor,
                                       Timestamp txn_version) {
  // 创建新节点，传入 txn_version 用于 MVCC
  auto new_node = std::make_unique<TemporalVersionNode>(timestamp, descriptor, txn_version);
  TemporalVersionNode* node_ptr = new_node.get();
  node_pool_.push_back(std::move(new_node));
  
  // 查找或创建版本链
  auto it = version_chains_.find(internal_key);
  if (it == version_chains_.end()) {
    // 新建版本链
    version_chains_[internal_key] = node_ptr;
    version_chain_count_.fetch_add(1, std::memory_order_relaxed);
  } else {
    // 插入到链头 (最新版本)
    TemporalVersionNode* head = it->second;
    
    // 按时间戳排序插入
    if (timestamp > head->timestamp) {
      // 新节点成为新的链头 (最新版本)
      node_ptr->older = head;
      head->newer = node_ptr;
      it->second = node_ptr;
    } else {
      // 遍历找到合适位置插入 (保持降序)
      TemporalVersionNode* current = head;
      while (current->older != nullptr && current->older->timestamp > timestamp) {
        current = current->older;
      }
      // 插入到 current 之后
      node_ptr->older = current->older;
      node_ptr->newer = current;
      if (current->older != nullptr) {
        current->older->newer = node_ptr;
      }
      current->older = node_ptr;
    }
  }
}

std::vector<MemTableEntry> CedarMemTable::GetAll(uint64_t entity_id,
                                                 EntityType entity_type,
                                                 uint16_t column_id) const {
  std::lock_guard<std::mutex> lock(mutex_);

  InternalKey internal_key(entity_id, entity_type, column_id);

  auto it = map_.find(internal_key);
  if (it != map_.end()) {
    return it->second;  // 已经是降序排列
  }

  return {};
}

std::optional<Descriptor> CedarMemTable::GetAtTime(uint64_t entity_id,
                                                   EntityType entity_type,
                                                   uint16_t column_id,
                                                   Timestamp timestamp) const {
  auto all = GetAll(entity_id, entity_type, column_id);

  // 查找 <= timestamp 的最新版本
  for (const auto& entry : all) {
    if (entry.timestamp <= timestamp) {
      return entry.descriptor;
    }
  }

  return std::nullopt;
}

std::vector<MemTableEntry> CedarMemTable::GetRange(uint64_t entity_id,
                                                   EntityType entity_type,
                                                   uint16_t column_id,
                                                   Timestamp start,
                                                   Timestamp end) const {
  auto all = GetAll(entity_id, entity_type, column_id);

  std::vector<MemTableEntry> result;
  for (const auto& entry : all) {
    if (entry.timestamp >= start && entry.timestamp <= end) {
      result.push_back(entry);
    }
  }

  return result;
}

bool CedarMemTable::Get(uint64_t entity_id, uint64_t timestamp,
                       std::string* value, bool* is_deleted) const {
  auto desc_opt = GetAtTime(entity_id, EntityType::Vertex, 0, Timestamp(timestamp));

  if (!desc_opt.has_value()) {
    return false;
  }

  const Descriptor& desc = desc_opt.value();

  if (desc.IsTombstone()) {
    *is_deleted = true;
    return true;
  }

  *is_deleted = false;

  // 从 descriptor 提取值
  if (auto int_val = desc.AsInlineInt()) {
    *value = std::to_string(*int_val);
    return true;
  }
  if (auto float_val = desc.AsInlineFloat()) {
    *value = std::to_string(*float_val);
    return true;
  }

  std::string str_val = desc.AsInlineShortStr();
  if (!str_val.empty()) {
    *value = str_val;
    return true;
  }

  // External ref 需要额外处理
  return false;
}

size_t CedarMemTable::NumEntries() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return map_.size();
}

// ========== MVCC 版本链实现 ==========

TemporalVersionNode* CedarMemTable::GetVersionChainHead(
    uint64_t entity_id,
    EntityType entity_type,
    uint16_t column_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  InternalKey internal_key(entity_id, entity_type, column_id);
  auto it = version_chains_.find(internal_key);
  if (it != version_chains_.end()) {
    return it->second;  // 返回链头 (最新版本)
  }
  return nullptr;
}

std::vector<MemTableEntry> CedarMemTable::GetVersionChain(
    uint64_t entity_id,
    EntityType entity_type,
    uint16_t column_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::vector<MemTableEntry> result;
  
  InternalKey internal_key(entity_id, entity_type, column_id);
  auto it = version_chains_.find(internal_key);
  if (it == version_chains_.end()) {
    return result;  // 空结果
  }
  
  // 遍历版本链 (从最新到最旧)
  TemporalVersionNode* current = it->second;
  while (current != nullptr) {
    result.emplace_back(current->timestamp, current->descriptor, current->txn_version);
    current = current->older;
  }
  
  return result;
}

void CedarMemTable::TraverseVersionChain(
    uint64_t entity_id,
    EntityType entity_type,
    uint16_t column_id,
    std::function<bool(const MemTableEntry&)> callback) const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  InternalKey internal_key(entity_id, entity_type, column_id);
  auto it = version_chains_.find(internal_key);
  if (it == version_chains_.end()) {
    return;
  }
  
  // 遍历版本链
  TemporalVersionNode* current = it->second;
  while (current != nullptr) {
    MemTableEntry entry(current->timestamp, current->descriptor, current->txn_version);
    if (!callback(entry)) {
      break;  // 回调返回 false，停止遍历
    }
    current = current->older;
  }
}

size_t CedarMemTable::GetVersionChainLength(
    uint64_t entity_id,
    EntityType entity_type,
    uint16_t column_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  InternalKey internal_key(entity_id, entity_type, column_id);
  auto it = version_chains_.find(internal_key);
  if (it == version_chains_.end()) {
    return 0;
  }
  
  // 遍历计数
  size_t count = 0;
  TemporalVersionNode* current = it->second;
  while (current != nullptr) {
    ++count;
    current = current->older;
  }
  return count;
}

CedarMemTable::VersionChainIterator* CedarMemTable::NewVersionChainIterator(
    uint64_t entity_id,
    EntityType entity_type,
    uint16_t column_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  InternalKey internal_key(entity_id, entity_type, column_id);
  auto it = version_chains_.find(internal_key);
  if (it != version_chains_.end()) {
    return new VersionChainIterator(it->second);
  }
  return new VersionChainIterator(nullptr);  // 空迭代器
}

// ========== 多列支持实现 ==========

std::vector<uint16_t> CedarMemTable::GetColumnIds(
    uint64_t entity_id, 
    EntityType entity_type) const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::vector<uint16_t> column_ids;
  
  // 遍历所有 InternalKey，找出匹配的 entity_id 和 entity_type
  for (const auto& [internal_key, entries] : map_) {
    if (internal_key.entity_id == entity_id && 
        internal_key.entity_type == entity_type) {
      column_ids.push_back(internal_key.column_id);
    }
  }
  
  // 去重并排序
  std::sort(column_ids.begin(), column_ids.end());
  column_ids.erase(std::unique(column_ids.begin(), column_ids.end()), 
                   column_ids.end());
  
  return column_ids;
}

std::vector<CedarMemTable::ColumnSnapshot> CedarMemTable::GetLatestSnapshot(
    uint64_t entity_id, 
    EntityType entity_type) const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::vector<ColumnSnapshot> snapshot;
  
  // 直接在锁内查找所有列（避免调用 GetColumnIds 导致死锁）
  std::vector<uint16_t> column_ids;
  for (const auto& [internal_key, entries] : map_) {
    if (internal_key.entity_id == entity_id && 
        internal_key.entity_type == entity_type) {
      column_ids.push_back(internal_key.column_id);
    }
  }
  std::sort(column_ids.begin(), column_ids.end());
  column_ids.erase(std::unique(column_ids.begin(), column_ids.end()), 
                   column_ids.end());
  
  for (uint16_t col_id : column_ids) {
    InternalKey internal_key(entity_id, entity_type, col_id);
    auto it = version_chains_.find(internal_key);
    
    if (it != version_chains_.end() && it->second != nullptr) {
      // 链头是最新版本
      TemporalVersionNode* head = it->second;
      snapshot.push_back({col_id, head->timestamp, head->descriptor});
    }
  }
  
  // 按 column_id 排序
  std::sort(snapshot.begin(), snapshot.end(),
    [](const auto& a, const auto& b) {
      return a.column_id < b.column_id;
    });
  
  return snapshot;
}

std::vector<CedarMemTable::ColumnSnapshot> CedarMemTable::GetSnapshotAtTime(
    uint64_t entity_id,
    EntityType entity_type,
    Timestamp timestamp) const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::vector<ColumnSnapshot> snapshot;
  
  // 直接在锁内查找所有列
  std::vector<uint16_t> column_ids;
  for (const auto& [internal_key, entries] : map_) {
    if (internal_key.entity_id == entity_id && 
        internal_key.entity_type == entity_type) {
      column_ids.push_back(internal_key.column_id);
    }
  }
  std::sort(column_ids.begin(), column_ids.end());
  column_ids.erase(std::unique(column_ids.begin(), column_ids.end()), 
                   column_ids.end());
  
  for (uint16_t col_id : column_ids) {
    InternalKey internal_key(entity_id, entity_type, col_id);
    
    // 查找 <= timestamp 的最新版本
    auto it = map_.find(internal_key);
    if (it != map_.end()) {
      const auto& entries = it->second;  // 已按降序排列
      for (const auto& entry : entries) {
        if (entry.timestamp <= timestamp) {
          snapshot.push_back({col_id, entry.timestamp, entry.descriptor});
          break;
        }
      }
    }
  }
  
  // 按 column_id 排序
  std::sort(snapshot.begin(), snapshot.end(),
    [](const auto& a, const auto& b) {
      return a.column_id < b.column_id;
    });
  
  return snapshot;
}

std::vector<CedarMemTable::MultiColumnVersion> CedarMemTable::GetAllColumnChains(
    uint64_t entity_id,
    EntityType entity_type) const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  std::vector<MultiColumnVersion> result;
  
  // 直接在锁内查找所有列
  std::vector<uint16_t> column_ids;
  for (const auto& [internal_key, entries] : map_) {
    if (internal_key.entity_id == entity_id && 
        internal_key.entity_type == entity_type) {
      column_ids.push_back(internal_key.column_id);
    }
  }
  std::sort(column_ids.begin(), column_ids.end());
  column_ids.erase(std::unique(column_ids.begin(), column_ids.end()), 
                   column_ids.end());
  
  for (uint16_t col_id : column_ids) {
    InternalKey internal_key(entity_id, entity_type, col_id);
    
    MultiColumnVersion mcv;
    mcv.column_id = col_id;
    
    // 获取该列的版本链
    auto it = map_.find(internal_key);
    if (it != map_.end()) {
      mcv.versions = it->second;  // 复制版本列表
    }
    
    result.push_back(std::move(mcv));
  }
  
  // 按 column_id 排序
  std::sort(result.begin(), result.end(),
    [](const auto& a, const auto& b) {
      return a.column_id < b.column_id;
    });
  
  return result;
}

bool CedarMemTable::IsEmpty() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return map_.empty();
}

std::vector<std::pair<CedarKey, Descriptor>> CedarMemTable::GetSortedEntries() const {
  std::lock_guard<std::mutex> lock(mutex_);

  std::vector<std::pair<CedarKey, Descriptor>> result;

  for (const auto& [internal_key, entries] : map_) {
    for (const auto& entry : entries) {
      CedarKey key(
          internal_key.entity_id,
          internal_key.entity_type,
          internal_key.column_id,
          entry.timestamp,
          0,  // sequence
          internal_key.target_id);

      result.emplace_back(key, entry.descriptor);
    }
  }

  // 排序: entity_id → entity_type → column_id → timestamp desc
  std::sort(result.begin(), result.end(),
    [](const auto& a, const auto& b) {
      const CedarKey& k1 = a.first;
      const CedarKey& k2 = b.first;

      if (k1.entity_id() != k2.entity_id()) {
        return k1.entity_id() < k2.entity_id();
      }
      if (k1.entity_type() != k2.entity_type()) {
        return static_cast<uint8_t>(k1.entity_type()) <
               static_cast<uint8_t>(k2.entity_type());
      }
      if (k1.column_id() != k2.column_id()) {
        return k1.column_id() < k2.column_id();
      }
      // timestamp 降序
      return k1.timestamp() > k2.timestamp();
    });

  return result;
}

// Iterator 实现

CedarMemTable::Iterator::Iterator(const CedarMemTable* memtable)
    : memtable_(memtable),
      outer_iter_(memtable->map_.begin()),
      inner_idx_(0),
      valid_(false) {
  if (outer_iter_ != memtable->map_.end()) {
    valid_ = true;
  }
}

void CedarMemTable::Iterator::SeekToFirst() {
  outer_iter_ = memtable_->map_.begin();
  inner_idx_ = 0;
  valid_ = (outer_iter_ != memtable_->map_.end());
}

void CedarMemTable::Iterator::Seek(const CedarKey& key) {
  // 简化实现: 先定位到对应的 internal_key
  InternalKey internal_key(key.entity_id(), key.entity_type(),
                           key.column_id(), key.target_id());

  outer_iter_ = memtable_->map_.find(internal_key);
  if (outer_iter_ == memtable_->map_.end()) {
    valid_ = false;
    return;
  }

  // 在 entries 中查找对应 timestamp 的位置
  const auto& entries = outer_iter_->second;
  for (size_t i = 0; i < entries.size(); ++i) {
    if (entries[i].timestamp == key.timestamp()) {
      inner_idx_ = i;
      valid_ = true;
      return;
    }
  }

  valid_ = false;
}

void CedarMemTable::Iterator::Next() {
  if (!valid_) return;

  const auto& entries = outer_iter_->second;
  inner_idx_++;

  if (inner_idx_ >= entries.size()) {
    // 移动到下一个 internal_key
    ++outer_iter_;
    inner_idx_ = 0;
    valid_ = (outer_iter_ != memtable_->map_.end());
  }
}

bool CedarMemTable::Iterator::Valid() const {
  return valid_;
}

CedarKey CedarMemTable::Iterator::Key() const {
  if (!valid_) return CedarKey();

  const InternalKey& internal_key = outer_iter_->first;
  const auto& entry = outer_iter_->second[inner_idx_];

  return CedarKey(
      internal_key.entity_id,
      internal_key.entity_type,
      internal_key.column_id,
      entry.timestamp,
      0,
      internal_key.target_id);
}

Descriptor CedarMemTable::Iterator::Descriptor() const {
  if (!valid_) return cedar::Descriptor();

  return outer_iter_->second[inner_idx_].descriptor;
}

const MemTableEntry* CedarMemTable::Iterator::Entry() const {
  if (!valid_) return nullptr;

  return &outer_iter_->second[inner_idx_];
}

// ========== VersionChainIterator 实现 ==========

CedarMemTable::VersionChainIterator::VersionChainIterator(TemporalVersionNode* head)
    : head_(head), current_(head) {}

void CedarMemTable::VersionChainIterator::SeekToFirst() {
  // 定位到链头 (最新版本 - timestamp 最大)
  current_ = head_;
}

void CedarMemTable::VersionChainIterator::SeekToLast() {
  // 定位到链尾 (最旧版本 - timestamp 最小)
  if (head_ == nullptr) {
    current_ = nullptr;
    return;
  }
  current_ = head_;
  while (current_->older != nullptr) {
    current_ = current_->older;
  }
}

void CedarMemTable::VersionChainIterator::NextOlder() {
  // 移动到更旧的版本
  if (current_ != nullptr) {
    current_ = current_->older;
  }
}

void CedarMemTable::VersionChainIterator::NextNewer() {
  // 移动到更新的版本 (时间戳更大)
  if (current_ != nullptr) {
    current_ = current_->newer;
  }
}

bool CedarMemTable::VersionChainIterator::Valid() const {
  return current_ != nullptr;
}

const TemporalVersionNode* CedarMemTable::VersionChainIterator::Node() const {
  return current_;
}

MemTableEntry CedarMemTable::VersionChainIterator::Entry() const {
  if (current_ == nullptr) {
    return MemTableEntry();
  }
  return MemTableEntry(current_->timestamp, current_->descriptor, current_->txn_version);
}

Timestamp CedarMemTable::VersionChainIterator::GetTimestamp() const {
  if (current_ == nullptr) return Timestamp(0);
  return current_->timestamp;
}

Descriptor CedarMemTable::VersionChainIterator::GetDescriptor() const {
  if (current_ == nullptr) return cedar::Descriptor();
  return current_->descriptor;
}

}  // namespace cedar
