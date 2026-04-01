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

#include "cedar/transaction/occ_transaction.h"

#include <iostream>  // DEBUG

#include "cedar/storage/cedar_memtable.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/transaction/batch_api.h"

namespace cedar {

// ========== TransactionManager ==========

TransactionManager::TransactionManager() = default;

uint64_t TransactionManager::AllocateTransactionId() {
  return next_txn_id_.fetch_add(1, std::memory_order_acq_rel);
}

Timestamp TransactionManager::AllocateTimestamp() {
  // 使用分片时间戳分配器 - 消除高并发下的原子操作竞争
  // ShardedTimestampAllocator 为每个线程提供本地缓存
  return timestamp_allocator_.Allocate();
}

Timestamp TransactionManager::AllocateTimestampBatch(uint32_t count) {
  if (count == 0) count = 1;
  // 使用分片分配器的批量分配接口
  // AllocateBatch 返回批次中的第一个时间戳
  return timestamp_allocator_.AllocateBatch(count);
}

void TransactionManager::RegisterActiveTransaction(uint64_t txn_id, 
                                                    Timestamp start_ts) {
  std::lock_guard<std::mutex> lock(active_txns_mutex_);
  active_txns_[txn_id] = start_ts;
  stats_.txn_started.fetch_add(1, std::memory_order_relaxed);
}

void TransactionManager::UnregisterActiveTransaction(uint64_t txn_id) {
  std::lock_guard<std::mutex> lock(active_txns_mutex_);
  active_txns_.erase(txn_id);
}

std::vector<std::pair<uint64_t, Timestamp>> 
TransactionManager::GetActiveTransactions() const {
  std::lock_guard<std::mutex> lock(active_txns_mutex_);
  std::vector<std::pair<uint64_t, Timestamp>> result;
  result.reserve(active_txns_.size());
  for (const auto& [txn_id, ts] : active_txns_) {
    result.emplace_back(txn_id, ts);
  }
  return result;
}

bool TransactionManager::HasActiveTransactionBefore(Timestamp ts) const {
  std::lock_guard<std::mutex> lock(active_txns_mutex_);
  for (const auto& [txn_id, start_ts] : active_txns_) {
    if (start_ts < ts) {
      return true;
    }
  }
  return false;
}

Timestamp TransactionManager::GetMinActiveTimestamp() const {
  std::lock_guard<std::mutex> lock(active_txns_mutex_);
  if (active_txns_.empty()) {
    // 使用分片分配器的当前全局计数
    return Timestamp(timestamp_allocator_.GetGlobalNext());
  }
  
  Timestamp min_ts = Timestamp::Max();
  for (const auto& [txn_id, start_ts] : active_txns_) {
    if (start_ts < min_ts) {
      min_ts = start_ts;
    }
  }
  return min_ts;
}

// ========== OCCTransaction ==========

OCCTransaction::OCCTransaction(TransactionManager* txn_manager,
                                VSLMemTable* memtable,
                                LsmEngine* lsm_engine,
                                WalWriter* wal_writer,
                                const TransactionOptions& options)
    : txn_manager_(txn_manager),
      memtable_(memtable),
      lsm_engine_(lsm_engine),
      wal_writer_(wal_writer),
      options_(options) {}

OCCTransaction::~OCCTransaction() {
  // 如果事务还在活跃状态，自动回滚
  if (state_.load() == TransactionState::kActive) {
    Abort();
  }
}

Status OCCTransaction::Begin() {
  if (state_.load() != TransactionState::kActive) {
    return Status::InvalidArgument("OCCTransaction", 
        "Transaction already started or finished");
  }
  
  // 分配事务 ID 和读取时间戳
  txn_id_ = txn_manager_->AllocateTransactionId();
  read_timestamp_ = txn_manager_->AllocateTimestamp();
  
  // 注册到活跃事务表
  txn_manager_->RegisterActiveTransaction(txn_id_, read_timestamp_);
  
  return Status::OK();
}

Status OCCTransaction::Commit() {
  // 状态检查
  TransactionState expected = TransactionState::kActive;
  if (!state_.compare_exchange_strong(expected, TransactionState::kValidating)) {
    return Status::InvalidArgument("OCCTransaction", 
        "Transaction not in active state");
  }
  
  // 空事务直接成功
  if (write_set_.empty() && read_set_.empty()) {
    state_.store(TransactionState::kCommitted);
    txn_manager_->UnregisterActiveTransaction(txn_id_);
    txn_manager_->mutable_stats().txn_committed.fetch_add(1, std::memory_order_relaxed);
    return Status::OK();
  }
  
  // 只读事务也直接成功
  if (write_set_.empty()) {
    state_.store(TransactionState::kCommitted);
    txn_manager_->UnregisterActiveTransaction(txn_id_);
    txn_manager_->mutable_stats().txn_committed.fetch_add(1, std::memory_order_relaxed);
    return Status::OK();
  }
  
  // 分配提交时间戳
  commit_timestamp_ = txn_manager_->AllocateTimestamp();
  
  // 验证阶段
  Status validation_status = Validate();
  if (!validation_status.ok()) {
    state_.store(TransactionState::kAborted);
    txn_manager_->UnregisterActiveTransaction(txn_id_);
    txn_manager_->mutable_stats().txn_aborted.fetch_add(1, std::memory_order_relaxed);
    txn_manager_->mutable_stats().validation_failures.fetch_add(1, std::memory_order_relaxed);
    return validation_status;
  }
  
  state_.store(TransactionState::kCommitting);
  
  // 写入 MemTable
  Status write_status = WriteToMemTable();
  if (!write_status.ok()) {
    state_.store(TransactionState::kAborted);
    txn_manager_->UnregisterActiveTransaction(txn_id_);
    txn_manager_->mutable_stats().txn_aborted.fetch_add(1, std::memory_order_relaxed);
    return write_status;
  }
  
  // 写入 WAL
  if (wal_writer_) {
    Status wal_status = WriteToWAL();
    if (!wal_status.ok()) {
      state_.store(TransactionState::kAborted);
      txn_manager_->UnregisterActiveTransaction(txn_id_);
      txn_manager_->mutable_stats().txn_aborted.fetch_add(1, std::memory_order_relaxed);
      return wal_status;
    }
  }
  
  // 标记提交完成
  state_.store(TransactionState::kCommitted);
  txn_manager_->UnregisterActiveTransaction(txn_id_);
  txn_manager_->mutable_stats().txn_committed.fetch_add(1, std::memory_order_relaxed);
  
  return Status::OK();
}

Status OCCTransaction::Abort() {
  TransactionState current = state_.load();
  if (current == TransactionState::kCommitted) {
    return Status::InvalidArgument("OCCTransaction", 
        "Cannot abort committed transaction");
  }
  
  if (current == TransactionState::kAborted) {
    return Status::OK();  // 已经是中止状态
  }
  
  state_.store(TransactionState::kAborted);
  txn_manager_->UnregisterActiveTransaction(txn_id_);
  txn_manager_->mutable_stats().txn_aborted.fetch_add(1, std::memory_order_relaxed);
  
  // 写入 WAL 中止记录 (用于恢复)
  if (wal_writer_) {
    wal_writer_->WriteAbort(txn_id_, commit_timestamp_);
  }
  
  Cleanup();
  return Status::OK();
}

Status OCCTransaction::Get(uint64_t entity_id,
                            EntityType entity_type,
                            uint16_t column_id,
                            Descriptor* descriptor,
                            Timestamp* version_ts) {
  if (!descriptor || !version_ts) {
    return Status::InvalidArgument("OCCTransaction", "null pointer");
  }
  
  if (state_.load() != TransactionState::kActive) {
    return Status::InvalidArgument("OCCTransaction", "transaction not active");
  }
  
  std::lock_guard<std::mutex> lock(mutex_);
  
  // 1. 检查写集 (读己之写)
  std::string write_key = std::to_string(entity_id) + 
                          std::to_string(static_cast<int>(entity_type)) +
                          std::to_string(column_id);
  
  for (const auto& write_entry : write_set_) {
    if (write_entry.entity_id == entity_id &&
        write_entry.entity_type == entity_type &&
        write_entry.column_id == column_id) {
      *descriptor = write_entry.descriptor;
      *version_ts = read_timestamp_;
      return Status::OK();
    }
  }
  
  // 2. 从 MemTable 读取
  auto chain_opt = memtable_->GetVersionChain(entity_id, entity_type, column_id);
  
  // 3. 根据隔离级别和事务版本号选择版本
  // 快照隔离: 读取 read_timestamp_ (事务版本号) 之前的最新版本
  // 修复: 使用 txn_version 而非业务时间戳进行 MVCC 版本比较
  for (const auto& entry : chain_opt) {
    if (entry.txn_version <= read_timestamp_) {
      *descriptor = entry.descriptor;
      *version_ts = entry.timestamp;  // 返回业务时间戳给用户
      
      // 记录到读集（业务时间戳 + 事务版本号）
      read_set_.push_back({entity_id, entity_type, column_id, 
                           entry.timestamp, entry.txn_version});
      
      return Status::OK();
    }
  }
  
  // 4. MemTable 中没有找到，查询 LsmEngine (SST 文件)
  // SST 文件中的数据已经过 compaction，视为已提交数据，对所有事务可见
  if (lsm_engine_) {
    // 修复: 使用 GetAll 获取所有版本，然后找到最新的
    // 因为 GetAtTime 依赖的 GetVersionChain 在 SST 中未实现
    auto all_versions = lsm_engine_->GetAll(entity_id, entity_type, column_id);
    if (!all_versions.empty()) {
      // 返回最新版本（GetAll 返回按时间戳降序排列）
      *descriptor = all_versions.front().descriptor;
      *version_ts = all_versions.front().timestamp;
      
      // 记录到读集（使用 Max() 标记为 SST 数据，事务版本号也为 Max()）
      read_set_.push_back({entity_id, entity_type, column_id, 
                           Timestamp::Max(), Timestamp::Max()});
      
      return Status::OK();
    }
  }
  
  return Status::NotFound("OCCTransaction", "no valid version found");
}

Status OCCTransaction::GetAllColumns(uint64_t entity_id,
                                      EntityType entity_type,
                                      std::vector<std::pair<uint16_t, Descriptor>>* columns) {
  if (!columns) {
    return Status::InvalidArgument("OCCTransaction", "null pointer");
  }
  
  if (state_.load() != TransactionState::kActive) {
    return Status::InvalidArgument("OCCTransaction", "transaction not active");
  }
  
  std::lock_guard<std::mutex> lock(mutex_);
  columns->clear();
  
  // 获取所有列 ID
  // GetColumnIds not implemented in VSLMemTable
  std::vector<uint16_t> column_ids;
  
  // 读取每一列
  for (uint16_t col_id : column_ids) {
    // 1. 检查写集
    bool found_in_write_set = false;
    for (const auto& write_entry : write_set_) {
      if (write_entry.entity_id == entity_id &&
          write_entry.entity_type == entity_type &&
          write_entry.column_id == col_id) {
        columns->emplace_back(col_id, write_entry.descriptor);
        found_in_write_set = true;
        break;
      }
    }
    
    if (found_in_write_set) {
      continue;
    }
    
    // 2. 从 MemTable 读取
    auto chain_opt = memtable_->GetVersionChain(entity_id, entity_type, col_id);
    if (!chain_opt.empty()) {
      for (const auto& entry : chain_opt) {
        // 修复: 使用 txn_version 而非业务时间戳进行 MVCC 版本比较
        if (entry.txn_version <= read_timestamp_) {
          columns->emplace_back(col_id, entry.descriptor);
          
          // 记录到读集（业务时间戳 + 事务版本号）
          read_set_.push_back({entity_id, entity_type, col_id, 
                               entry.timestamp, entry.txn_version});
          break;
        }
      }
    } else if (lsm_engine_) {
      // 3. MemTable 中没有找到，查询 LsmEngine (SST 文件)
      auto desc_opt = lsm_engine_->GetAtTime(entity_id, entity_type, col_id, Timestamp::Max());
      if (desc_opt.has_value()) {
        columns->emplace_back(col_id, desc_opt.value());
        read_set_.push_back({entity_id, entity_type, col_id, 
                             Timestamp::Max(), Timestamp::Max()});
      }
    }
  }
  
  return Status::OK();
}

Status OCCTransaction::Put(uint64_t entity_id,
                            EntityType entity_type,
                            uint16_t column_id,
                            const Descriptor& descriptor,
                            Timestamp user_timestamp,
                            uint64_t target_id) {
  if (state_.load() != TransactionState::kActive) {
    return Status::InvalidArgument("OCCTransaction", "transaction not active");
  }
  
  std::lock_guard<std::mutex> lock(mutex_);
  
  // 修复 1: 使用系统时间作为默认业务时间戳
  // 如果用户提供了业务时间戳，使用它；否则使用当前系统时间
  // 绝对不使用 read_timestamp_ 或 commit_timestamp_（事务版本号）作为 Key 的时间戳
  Timestamp ts = user_timestamp.value() > 0 ? user_timestamp : Timestamp::Now();
  
  // 使用事务版本号作为 MVCC 版本控制（后续会在 Commit 时分配 commit_timestamp_）
  Timestamp txn_version = read_timestamp_;
  
  CedarKey temp_key = MakeKey(entity_id, entity_type, column_id, ts, target_id);
  write_set_.push_back({entity_id, entity_type, column_id, descriptor, temp_key, ts, txn_version, target_id});
  
  return Status::OK();
}

Status OCCTransaction::Delete(uint64_t entity_id,
                               EntityType entity_type,
                               uint16_t column_id) {
  // 删除就是写入 Tombstone
  return Put(entity_id, entity_type, column_id, 
             Descriptor::Tombstone(column_id));
}

Status OCCTransaction::PutBatch(const std::vector<WriteSetEntry>& entries) {
  if (state_.load() != TransactionState::kActive) {
    return Status::InvalidArgument("OCCTransaction", "transaction not active");
  }
  
  std::lock_guard<std::mutex> lock(mutex_);
  
  for (const auto& entry : entries) {
    // 检查是否已在写集中
    bool updated = false;
    for (auto& existing : write_set_) {
      if (existing.entity_id == entry.entity_id &&
          existing.entity_type == entry.entity_type &&
          existing.column_id == entry.column_id) {
        existing.descriptor = entry.descriptor;
        existing.key = entry.key;
        updated = true;
        break;
      }
    }
    
    if (!updated) {
      write_set_.push_back(entry);
    }
    
    // 添加到 WAL 批次
    // 修复: 传递 txn_version 给 WAL
    if (wal_writer_) {
      wal_batch_.Put(entry.key, entry.descriptor, entry.txn_version);
    }
  }
  
  return Status::OK();
}

Status OCCTransaction::GetVersionHistory(uint64_t entity_id,
                                          EntityType entity_type,
                                          uint16_t column_id,
                                          std::vector<std::pair<Timestamp, Descriptor>>* history) {
  if (!history) {
    return Status::InvalidArgument("OCCTransaction", "null pointer");
  }
  
  std::lock_guard<std::mutex> lock(mutex_);
  history->clear();
  
  // 从 MemTable 读取完整历史
  auto chain_opt = memtable_->GetVersionChain(entity_id, entity_type, column_id);
  
  for (const auto& entry : chain_opt) {
    // 只包含在读取时间戳之前的版本
    // 修复: 使用 txn_version 而非业务时间戳进行 MVCC 版本比较
    if (entry.txn_version <= read_timestamp_) {
      history->emplace_back(entry.timestamp, entry.descriptor);
    }
  }
  
  // 如果 MemTable 中没有历史，查询 SST 文件
  if (history->empty() && lsm_engine_) {
    auto all_versions = lsm_engine_->GetAll(entity_id, entity_type, column_id);
    for (const auto& entry : all_versions) {
      // SST 文件中的数据视为已提交，全部可见
      history->emplace_back(entry.timestamp, entry.descriptor);
    }
  }
  
  return Status::OK();
}

Status OCCTransaction::Validate() {
  conflicts_.clear();
  
  // 获取当前活跃事务
  std::vector<std::pair<uint64_t, Timestamp>> active_txns = 
      txn_manager_->GetActiveTransactions();
  
  // 对于快照隔离，我们需要验证:
  // 1. 读集中的记录在读取后没有被其他已提交事务修改
  // 2. 写集中没有与其他并发事务产生冲突
  
  // 验证读集 (支持并行验证)
  if (options_.parallel_validation && read_set_.size() > 100) {
    ValidateReadSetParallel();
  } else {
    // 串行验证
    for (const auto& read_entry : read_set_) {
      if (!ValidateReadEntry(read_entry)) {
        ConflictInfo conflict;
        conflict.type = ConflictType::kReadWrite;
        conflict.entity_id = read_entry.entity_id;
        conflict.entity_type = read_entry.entity_type;
        conflict.column_id = read_entry.column_id;
        conflict.conflicting_txn_id = 0;
        conflicts_.push_back(conflict);
      }
    }
  }
  
  // 验证写集 (写写冲突检测) - 通常较小，使用串行验证
  for (const auto& write_entry : write_set_) {
    // 检查是否有其他事务在当前事务开始后修改了相同键
    auto chain_opt = memtable_->GetVersionChain(
        write_entry.entity_id, 
        write_entry.entity_type, 
        write_entry.column_id);
    
    if (!chain_opt.empty()) {
      // 获取最新版本
      const auto& latest = chain_opt.front();
      
      // 如果最新版本在当前事务开始之后，说明有冲突
      // 修复: 使用 txn_version 而非业务时间戳进行版本比较
      if (latest.txn_version > read_timestamp_) {
        ConflictInfo conflict;
        conflict.type = ConflictType::kWriteWrite;
        conflict.entity_id = write_entry.entity_id;
        conflict.entity_type = write_entry.entity_type;
        conflict.column_id = write_entry.column_id;
        conflict.conflicting_txn_id = 0;
        conflicts_.push_back(conflict);
      }
    }
  }
  
  if (!conflicts_.empty()) {
    txn_manager_->mutable_stats().conflicts_detected.fetch_add(
        conflicts_.size(), std::memory_order_relaxed);
    return Status::Conflict("OCCTransaction", "validation failed");
  }
  
  return Status::OK();
}

void OCCTransaction::ValidateReadSetParallel() {
  const size_t num_threads = options_.validation_threads;
  const size_t chunk_size = (read_set_.size() + num_threads - 1) / num_threads;
  
  std::vector<std::thread> threads;
  std::vector<std::vector<ConflictInfo>> thread_conflicts(num_threads);
  
  for (size_t t = 0; t < num_threads; t++) {
    threads.emplace_back([this, t, chunk_size, &thread_conflicts]() {
      size_t start = t * chunk_size;
      size_t end = std::min(start + chunk_size, read_set_.size());
      
      for (size_t i = start; i < end; i++) {
        if (!ValidateReadEntry(read_set_[i])) {
          ConflictInfo conflict;
          conflict.type = ConflictType::kReadWrite;
          conflict.entity_id = read_set_[i].entity_id;
          conflict.entity_type = read_set_[i].entity_type;
          conflict.column_id = read_set_[i].column_id;
          conflict.conflicting_txn_id = 0;
          thread_conflicts[t].push_back(conflict);
        }
      }
    });
  }
  
  // 等待所有线程完成
  for (auto& t : threads) {
    t.join();
  }
  
  // 合并冲突结果
  for (const auto& tc : thread_conflicts) {
    conflicts_.insert(conflicts_.end(), tc.begin(), tc.end());
  }
}

bool OCCTransaction::ValidateReadEntry(const ReadSetEntry& entry) {
  // 如果读取的是 SST 文件中的数据（标记为 Max()），不需要验证
  // 因为 SST 数据是不可变的，不会发生冲突
  if (entry.read_txn_version == Timestamp::Max()) {
    return true;
  }
  
  // 获取当前最新版本
  auto chain_opt = memtable_->GetVersionChain(
      entry.entity_id, entry.entity_type, entry.column_id);
  
  if (chain_opt.empty()) {
    // 记录被删除，但如果读时存在则冲突
    // 但如果读取的是 SST 数据，MemTable 为空是正常的
    // 需要检查 SST 中是否仍有该数据
    if (lsm_engine_) {
      auto desc_opt = lsm_engine_->GetAtTime(
          entry.entity_id, entry.entity_type, entry.column_id, Timestamp::Max());
      // 如果 SST 中仍有数据，则验证通过（SST 数据不可变）
      return desc_opt.has_value();
    }
    return false;
  }
  
  // 检查最新版本是否仍是读取时的版本
  const auto& latest = chain_opt.front();
  
  // ✅ 修复: 使用 entry.read_txn_version（读取时的事务版本号）进行比较
  // 如果最新版本的事务版本号 > 读取时记录的事务版本号，说明被修改过
  if (latest.txn_version > entry.read_txn_version) {
    return false;
  }
  
  return true;
}

Status OCCTransaction::WriteToMemTable() {
  if (!memtable_) {
    return Status::InvalidArgument("OCCTransaction", "memtable is null");
  }
  
  for (const auto& entry : write_set_) {
    // 修复 2: 直接使用写入集中存储的业务时间戳
    // user_timestamp 在 Put() 阶段已经确定是业务时间戳（系统时间或用户指定）
    // 不再回退到 commit_timestamp_（事务版本号）
    Timestamp ts = entry.user_timestamp;
    
    // 构建键 - 使用业务时间戳和 target_id
    CedarKey key = MakeKey(entry.entity_id, entry.entity_type, 
                          entry.column_id, ts, entry.target_id);
    
    // 写入 MemTable - 同时传递事务版本号用于 MVCC
    // 注意：这里需要支持传递 txn_version 给 MemTable
    Status s = memtable_->Put(key, entry.descriptor, entry.txn_version);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

Status OCCTransaction::WriteToWAL() {
  if (!wal_writer_ || wal_batch_.empty()) {
    return Status::OK();
  }
  
  return wal_writer_->WriteBatch(wal_batch_);
}

void OCCTransaction::Cleanup() {
  read_set_.clear();
  write_set_.clear();
  write_set_keys_.clear();
  conflicts_.clear();
  wal_batch_.Clear();
}

CedarKey OCCTransaction::MakeKey(uint64_t entity_id, EntityType type, 
                                 uint16_t column_id, Timestamp ts,
                                 uint64_t target_id) {
  // Use factory methods to ensure proper byte order encoding
  // Note: sequence=0 for query compatibility
  uint16_t seq = 0;
  if (type == EntityType::EdgeOut) {
    // EdgeOut: src=entity_id, dst=target_id, edge_type=column_id
    return CedarKey::EdgeOut(entity_id, target_id, column_id, ts, seq);
  } else if (type == EntityType::EdgeIn) {
    // EdgeIn: dst=entity_id, src=target_id, edge_type=column_id
    return CedarKey::EdgeIn(entity_id, target_id, column_id, ts, seq);
  } else {
    // Vertex: extension data stored in target_id
    return CedarKey::Vertex(entity_id, VertexColumnId(column_id), ts, seq, target_id);
  }
}

// ========== TransactionRetryWrapper ==========

TransactionRetryWrapper::TransactionRetryWrapper(LsmEngine* lsm_engine,
                                                  const TransactionOptions& options)
    : lsm_engine_(lsm_engine), options_(options) {}

}  // namespace cedar
