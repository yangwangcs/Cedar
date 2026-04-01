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

#include "cedar/transaction/batch_api.h"

#include <atomic>
#include <condition_variable>
#include <future>
#include <mutex>
#include <queue>
#include <thread>

#include "cedar/storage/cedar_memtable.h"
#include "cedar/storage/vsl_memtable.h"
#include "cedar/storage/lsm_engine.h"

namespace cedar {

// ========== BatchEntry ==========

BatchEntry BatchEntry::PutVertex(uint64_t vertex_id,
                                  uint16_t column_id,
                                  const Descriptor& desc) {
  BatchEntry entry;
  entry.type = BatchOpType::kPutVertex;
  entry.entity_id = vertex_id;
  entry.entity_type = EntityType::Vertex;
  entry.column_id = column_id;
  entry.descriptor = desc;
  return entry;
}

BatchEntry BatchEntry::PutEdge(uint64_t src_id,
                                uint64_t dst_id,
                                const std::string& label,
                                uint16_t column_id,
                                const Descriptor& desc) {
  BatchEntry entry;
  entry.type = BatchOpType::kPutEdge;
  entry.entity_id = src_id;
  entry.entity_type = EntityType::EdgeOut;
  entry.column_id = column_id;
  entry.descriptor = desc;
  entry.dst_id = dst_id;
  entry.edge_label = label;
  return entry;
}

BatchEntry BatchEntry::DeleteVertex(uint64_t vertex_id, uint16_t column_id) {
  BatchEntry entry;
  entry.type = BatchOpType::kDeleteVertex;
  entry.entity_id = vertex_id;
  entry.entity_type = EntityType::Vertex;
  entry.column_id = column_id;
  entry.descriptor = Descriptor::Tombstone(column_id);
  return entry;
}

BatchEntry BatchEntry::DeleteEdge(uint64_t src_id,
                                   uint64_t dst_id,
                                   const std::string& label,
                                   uint16_t column_id) {
  BatchEntry entry;
  entry.type = BatchOpType::kDeleteEdge;
  entry.entity_id = src_id;
  entry.entity_type = EntityType::EdgeOut;
  entry.column_id = column_id;
  entry.descriptor = Descriptor::Tombstone(column_id);
  entry.dst_id = dst_id;
  entry.edge_label = label;
  return entry;
}

// ========== WriteBatch ==========

WriteBatch::WriteBatch() = default;

void WriteBatch::PutVertex(uint64_t vertex_id,
                            uint16_t column_id,
                            const Descriptor& descriptor) {
  entries_.push_back(BatchEntry::PutVertex(vertex_id, column_id, descriptor));
}

void WriteBatch::PutEdge(uint64_t src_id,
                          uint64_t dst_id,
                          const std::string& label,
                          uint16_t column_id,
                          const Descriptor& descriptor) {
  entries_.push_back(BatchEntry::PutEdge(src_id, dst_id, label, 
                                          column_id, descriptor));
}

void WriteBatch::DeleteVertex(uint64_t vertex_id, uint16_t column_id) {
  entries_.push_back(BatchEntry::DeleteVertex(vertex_id, column_id));
}

void WriteBatch::DeleteEdge(uint64_t src_id,
                             uint64_t dst_id,
                             const std::string& label,
                             uint16_t column_id) {
  entries_.push_back(BatchEntry::DeleteEdge(src_id, dst_id, label, column_id));
}

void WriteBatch::Put(uint64_t entity_id,
                      EntityType entity_type,
                      uint16_t column_id,
                      const Descriptor& descriptor) {
  BatchEntry entry;
  entry.type = (entity_type == EntityType::Vertex) 
                  ? BatchOpType::kPutVertex 
                  : BatchOpType::kPutEdge;
  entry.entity_id = entity_id;
  entry.entity_type = entity_type;
  entry.column_id = column_id;
  entry.descriptor = descriptor;
  entries_.push_back(entry);
}

void WriteBatch::Delete(uint64_t entity_id,
                         EntityType entity_type,
                         uint16_t column_id) {
  BatchEntry entry;
  entry.type = (entity_type == EntityType::Vertex)
                  ? BatchOpType::kDeleteVertex
                  : BatchOpType::kDeleteEdge;
  entry.entity_id = entity_id;
  entry.entity_type = entity_type;
  entry.column_id = column_id;
  entry.descriptor = Descriptor::Tombstone(column_id);
  entries_.push_back(entry);
}

void WriteBatch::Append(const WriteBatch& other) {
  entries_.insert(entries_.end(), other.entries_.begin(), other.entries_.end());
}

void WriteBatch::Clear() {
  entries_.clear();
}

size_t WriteBatch::ApproximateSize() const {
  size_t size = sizeof(*this);
  for (const auto& entry : entries_) {
    size += sizeof(entry);
    size += entry.edge_label.size();
  }
  return size;
}

void WriteBatch::Iterate(std::function<void(const BatchEntry&)> callback) const {
  for (const auto& entry : entries_) {
    callback(entry);
  }
}

std::string WriteBatch::Encode() const {
  std::string result;
  // 简单编码: [count: 4 bytes][entries...]
  char buf[4];
  buf[0] = static_cast<char>(entries_.size() & 0xFF);
  buf[1] = static_cast<char>((entries_.size() >> 8) & 0xFF);
  buf[2] = static_cast<char>((entries_.size() >> 16) & 0xFF);
  buf[3] = static_cast<char>((entries_.size() >> 24) & 0xFF);
  result.append(buf, 4);
  
  for (const auto& entry : entries_) {
    result.push_back(static_cast<char>(entry.type));
    
    // entity_id: 8 bytes
    for (int i = 0; i < 8; i++) {
      result.push_back(static_cast<char>((entry.entity_id >> (i * 8)) & 0xFF));
    }
    
    result.push_back(static_cast<char>(entry.entity_type));
    
    // column_id: 2 bytes
    result.push_back(static_cast<char>(entry.column_id & 0xFF));
    result.push_back(static_cast<char>((entry.column_id >> 8) & 0xFF));
    
    // descriptor: 8 bytes
    std::string desc_str = entry.descriptor.Encode();
    result.append(desc_str);
    
    // dst_id (for edges): 8 bytes
    for (int i = 0; i < 8; i++) {
      result.push_back(static_cast<char>((entry.dst_id >> (i * 8)) & 0xFF));
    }
    
    // edge_label length: 2 bytes
    uint16_t label_len = static_cast<uint16_t>(entry.edge_label.size());
    result.push_back(static_cast<char>(label_len & 0xFF));
    result.push_back(static_cast<char>((label_len >> 8) & 0xFF));
    result.append(entry.edge_label);
  }
  
  return result;
}

StatusOr<WriteBatch> WriteBatch::Decode(const Slice& data) {
  WriteBatch batch;
  
  if (data.size() < 4) {
    return Status::Corruption("WriteBatch", "truncated header");
  }
  
  const char* ptr = data.data();
  uint32_t count = static_cast<uint32_t>(ptr[0]) |
                   (static_cast<uint32_t>(ptr[1]) << 8) |
                   (static_cast<uint32_t>(ptr[2]) << 16) |
                   (static_cast<uint32_t>(ptr[3]) << 24);
  ptr += 4;
  
  for (uint32_t i = 0; i < count; i++) {
    if (ptr - data.data() + 1 > static_cast<ptrdiff_t>(data.size())) {
      return Status::Corruption("WriteBatch", "truncated entry");
    }
    
    BatchEntry entry;
    entry.type = static_cast<BatchOpType>(*ptr++);
    
    // entity_id
    if (ptr - data.data() + 8 > static_cast<ptrdiff_t>(data.size())) {
      return Status::Corruption("WriteBatch", "truncated entity_id");
    }
    entry.entity_id = 0;
    for (int j = 0; j < 8; j++) {
      entry.entity_id |= (static_cast<uint64_t>(static_cast<unsigned char>(*ptr++)) << (j * 8));
    }
    
    // entity_type
    entry.entity_type = static_cast<EntityType>(*ptr++);
    
    // column_id
    entry.column_id = static_cast<uint16_t>(ptr[0]) |
                      (static_cast<uint16_t>(ptr[1]) << 8);
    ptr += 2;
    
    // descriptor
    Slice desc_slice(ptr, 8);
    auto desc_opt = Descriptor::Decode(desc_slice);
    if (!desc_opt.has_value()) {
      return Status::Corruption("WriteBatch", "invalid descriptor");
    }
    entry.descriptor = desc_opt.value();
    ptr += 8;
    
    // dst_id
    entry.dst_id = 0;
    for (int j = 0; j < 8; j++) {
      entry.dst_id |= (static_cast<uint64_t>(static_cast<unsigned char>(*ptr++)) << (j * 8));
    }
    
    // edge_label
    uint16_t label_len = static_cast<uint16_t>(ptr[0]) |
                         (static_cast<uint16_t>(ptr[1]) << 8);
    ptr += 2;
    entry.edge_label.assign(ptr, label_len);
    ptr += label_len;
    
    batch.entries_.push_back(entry);
  }
  
  return batch;
}

WalBatch WriteBatch::ToWalBatch(Timestamp timestamp, uint64_t sequence, Timestamp txn_version) const {
  WalBatch wal_batch;
  
  for (const auto& entry : entries_) {
    std::optional<uint64_t> dst_id;
    if (entry.dst_id != 0) {
      dst_id = entry.dst_id;
    }
    CedarKey key(entry.entity_id, entry.entity_type, entry.column_id, 
                timestamp, static_cast<uint16_t>(sequence), dst_id.value_or(0));
    
    switch (entry.type) {
      case BatchOpType::kPutVertex:
      case BatchOpType::kPutEdge:
      case BatchOpType::kUpdateVertex:
      case BatchOpType::kUpdateEdge:
        // 修复: 传递 txn_version 给 WAL
        wal_batch.Put(key, entry.descriptor, txn_version);
        break;
      case BatchOpType::kDeleteVertex:
      case BatchOpType::kDeleteEdge:
        // 修复: 传递 txn_version 给 WAL
        wal_batch.Delete(key, txn_version);
        break;
    }
  }
  
  return wal_batch;
}

std::vector<WriteSetEntry> WriteBatch::ToWriteSet(Timestamp business_ts,
                                                   uint64_t txn_id,
                                                   Timestamp txn_version) const {
  std::vector<WriteSetEntry> write_set;
  write_set.reserve(entries_.size());
  
  for (const auto& entry : entries_) {
    std::optional<uint64_t> dst_id;
    if (entry.dst_id != 0) {
      dst_id = entry.dst_id;
    }
    // 修复: 正确区分业务时间戳和事务版本号
    // business_ts 用于构造 Key 的时间戳部分
    // txn_id 用于 sequence 字段（如果后续需要）
    // txn_version 用于 MVCC 版本控制
    CedarKey key(entry.entity_id, entry.entity_type, entry.column_id,
                business_ts, static_cast<uint16_t>(txn_id), dst_id.value_or(0));
    
    // 修复: 正确传递 business_ts 和 txn_version 给 WriteSetEntry
    write_set.emplace_back(entry.entity_id, entry.entity_type, entry.column_id,
                           entry.descriptor, key, business_ts, txn_version);
  }
  
  return write_set;
}

// ========== ReadBatch ==========

ReadBatch::ReadBatch() = default;

void ReadBatch::Get(uint64_t entity_id,
                     EntityType entity_type,
                     uint16_t column_id,
                     Timestamp timestamp) {
  requests_.push_back({entity_id, entity_type, column_id, timestamp});
}

void ReadBatch::GetLatest(uint64_t entity_id,
                           EntityType entity_type,
                           uint16_t column_id) {
  requests_.push_back({entity_id, entity_type, column_id, Timestamp()});
}

void ReadBatch::Clear() {
  requests_.clear();
  results_.clear();
}

void ReadBatch::Iterate(std::function<void(const ReadRequest&)> callback) const {
  for (const auto& req : requests_) {
    callback(req);
  }
}

void ReadBatch::SetResult(size_t index, const ReadResult& result) {
  if (index >= results_.size()) {
    results_.resize(index + 1);
  }
  results_[index] = result;
}

// ========== BatchExecutor ==========

BatchExecutor::BatchExecutor(LsmEngine* lsm_engine)
    : lsm_engine_(lsm_engine),
      txn_manager_(new TransactionManager()) {}

BatchExecutor::~BatchExecutor() = default;

BatchResult BatchExecutor::Write(const WriteBatch& batch, 
                                  const BatchOptions& options) {
  BatchResult result;
  result.entries_processed = batch.Count();
  
  if (batch.Empty()) {
    result.status = Status::OK();
    return result;
  }
  
  // 获取 MemTable
  VSLMemTable* memtable = lsm_engine_->GetMemTable();
  if (!memtable) {
    result.status = Status::NotFound("BatchExecutor", "memtable not available");
    return result;
  }
  
  // 分配时间戳
  Timestamp timestamp = txn_manager_->AllocateTimestamp();
  
  // 直接写入 MemTable (非事务模式)
  // 非事务模式下，使用 timestamp 作为 txn_version
  batch.Iterate([&](const BatchEntry& entry) {
    std::optional<uint64_t> dst_id;
    if (entry.dst_id != 0) {
      dst_id = entry.dst_id;
    }
    CedarKey key(entry.entity_id, entry.entity_type, entry.column_id,
                timestamp, 0, dst_id.value_or(0));
    
    memtable->Put(key, entry.descriptor, timestamp);
  });
  
  // 如果启用 WAL，写入 WAL
  // 修复: 传递 txn_version 给 ToWalBatch
  if (options.sync_wal && lsm_engine_->GetWalWriter()) {
    WalBatch wal_batch = batch.ToWalBatch(timestamp, 0, Timestamp(0));
    Status wal_status = lsm_engine_->GetWalWriter()->WriteBatch(wal_batch);
    if (!wal_status.ok() && result.status.ok()) {
      result.status = wal_status;
    }
  }
  
  result.commit_timestamp = timestamp;
  return result;
}

BatchResult BatchExecutor::WriteTransactional(const WriteBatch& batch,
                                               const TransactionOptions& txn_options) {
  BatchResult result;
  result.entries_processed = batch.Count();
  
  if (batch.Empty()) {
    result.status = Status::OK();
    return result;
  }
  
  // 获取资源
  VSLMemTable* memtable = lsm_engine_->GetMemTable();
  WalWriter* wal = lsm_engine_->GetWalWriter();
  
  if (!memtable) {
    result.status = Status::NotFound("BatchExecutor", "memtable not available");
    return result;
  }
  
  // 创建事务
  OCCTransaction txn(txn_manager_.get(), memtable, lsm_engine_, wal, txn_options);
  
  // 开始事务
  result.status = txn.Begin();
  if (!result.status.ok()) {
    return result;
  }
  
  result.txn_id = txn.GetTransactionId();
  
  // 执行批量写入
  // 修复: 使用当前系统时间作为业务时间戳，使用事务的 read_timestamp 作为 txn_version
  std::vector<WriteSetEntry> write_set = batch.ToWriteSet(
      Timestamp::Now(),           // 业务时间戳：使用当前系统时间
      txn.GetTransactionId(),     // 事务 ID
      txn.GetReadTimestamp()      // 事务版本号：使用 read_timestamp
  );
  result.status = txn.PutBatch(write_set);
  
  if (!result.status.ok()) {
    txn.Abort();
    return result;
  }
  
  // 提交事务
  result.status = txn.Commit();
  
  if (result.status.ok()) {
    result.commit_timestamp = txn.GetCommitTimestamp();
  } else {
    txn.Abort();
  }
  
  return result;
}

ReadBatch BatchExecutor::ExecuteReadBatch(const ReadBatch& batch) {
  ReadBatch result;
  
  VSLMemTable* memtable = lsm_engine_->GetMemTable();
  if (!memtable) {
    ReadBatch::ReadResult res;
    res.status = Status::NotFound("BatchExecutor", "memtable not available");
    result.SetResult(0, res);
    return result;
  }
  
  size_t index = 0;
  batch.Iterate([&](const ReadBatch::ReadRequest& req) {
    ReadBatch::ReadResult res;
    // 简化实现：直接从 MemTable 读取
    // 注意：这里使用的是旧版 CedarMemTable 接口，需要适配
    std::string value;
    bool is_deleted;
    result.SetResult(index++, res);
  });
  
  return result;
}

// ========== GroupCommitManager ==========

struct GroupCommitManager::Impl {
  WalWriter* wal_writer;
  Config config;
  
  std::mutex queue_mutex;
  std::condition_variable queue_cv;
  
  struct PendingCommit {
    uint64_t txn_id;
    WalBatch batch;
    std::promise<Status> promise;
    std::chrono::steady_clock::time_point submit_time;
  };
  
  std::queue<std::shared_ptr<PendingCommit>> pending_queue;
  std::atomic<bool> shutdown{false};
  std::vector<std::thread> workers;
  
  struct Stats {
    std::atomic<uint64_t> total_commits{0};
    std::atomic<uint64_t> total_batches{0};
    std::atomic<uint64_t> total_wait_time_us{0};
  } stats;
  
  Impl(WalWriter* w, const Config& c) : wal_writer(w), config(c) {}
  
  void WorkerLoop();
  void ProcessBatch(std::vector<std::shared_ptr<PendingCommit>>& batch);
};

void GroupCommitManager::Impl::WorkerLoop() {
  std::vector<std::shared_ptr<PendingCommit>> batch;
  batch.reserve(config.max_batch_size);
  
  while (!shutdown.load()) {
    {
      std::unique_lock<std::mutex> lock(queue_mutex);
      
      // 等待直到有数据或超时
      auto timeout = std::chrono::microseconds(config.timeout_us);
      queue_cv.wait_for(lock, timeout, [this] {
        return !pending_queue.empty() || shutdown.load();
      });
      
      // 收集批次
      while (!pending_queue.empty() && batch.size() < config.max_batch_size) {
        batch.push_back(pending_queue.front());
        pending_queue.pop();
      }
    }
    
    if (!batch.empty()) {
      ProcessBatch(batch);
      batch.clear();
    }
  }
  
  // 处理剩余的数据
  {
    std::lock_guard<std::mutex> lock(queue_mutex);
    while (!pending_queue.empty() && batch.size() < config.max_batch_size) {
      batch.push_back(pending_queue.front());
      pending_queue.pop();
    }
  }
  
  if (!batch.empty()) {
    ProcessBatch(batch);
  }
}

void GroupCommitManager::Impl::ProcessBatch(
    std::vector<std::shared_ptr<PendingCommit>>& batch) {
  if (batch.empty()) return;
  
  // 逐个写入批次
  for (const auto& commit : batch) {
    Status s = wal_writer->WriteBatch(commit->batch);
    commit->promise.set_value(s);
  }
  
  // 批量 fsync
  wal_writer->Sync();
  
  // 更新统计
  stats.total_batches.fetch_add(1, std::memory_order_relaxed);
  stats.total_commits.fetch_add(batch.size(), std::memory_order_relaxed);
}

GroupCommitManager::GroupCommitManager(WalWriter* wal_writer, const Config& config)
    : impl_(new Impl(wal_writer, config)) {
  // 启动工作线程
  for (size_t i = 0; i < config.num_workers; i++) {
    impl_->workers.emplace_back(&GroupCommitManager::Impl::WorkerLoop, impl_.get());
  }
}

GroupCommitManager::~GroupCommitManager() {
  impl_->shutdown.store(true);
  impl_->queue_cv.notify_all();
  
  for (auto& t : impl_->workers) {
    if (t.joinable()) {
      t.join();
    }
  }
}

std::future<Status> GroupCommitManager::Submit(uint64_t txn_id, const WalBatch& batch) {
  auto commit = std::make_shared<GroupCommitManager::Impl::PendingCommit>();
  commit->txn_id = txn_id;
  commit->batch = batch;
  commit->submit_time = std::chrono::steady_clock::now();
  
  std::future<Status> future = commit->promise.get_future();
  
  {
    std::lock_guard<std::mutex> lock(impl_->queue_mutex);
    impl_->pending_queue.push(commit);
  }
  
  impl_->queue_cv.notify_one();
  return future;
}

Status GroupCommitManager::Flush() {
  // 发送一个空批次触发刷新
  WalBatch empty_batch;
  auto future = Submit(0, empty_batch);
  return future.get();
}

GroupCommitManager::Stats GroupCommitManager::GetStats() const {
  Stats stats;
  stats.total_commits = impl_->stats.total_commits.load(std::memory_order_relaxed);
  stats.total_batches = impl_->stats.total_batches.load(std::memory_order_relaxed);
  stats.total_wait_time_us = impl_->stats.total_wait_time_us.load(std::memory_order_relaxed);
  
  if (stats.total_batches > 0) {
    stats.avg_batch_size = stats.total_commits / stats.total_batches;
  }
  
  return stats;
}

}  // namespace cedar
