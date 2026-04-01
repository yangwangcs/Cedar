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

#include "cedar/transaction/wal_batch_writer.h"

#include <chrono>

namespace cedar {

// ==================== WalBatchWriter ====================

WalBatchWriter::WalBatchWriter(const std::string& wal_dir,
                               cedar::Env* env,
                               const WalOptions& wal_options,
                               const Options& options)
    : options_(options) {
  wal_writer_ = std::make_unique<WalWriter>(wal_dir, env, wal_options);
  
  if (options_.enable_background_flush) {
    background_thread_ = std::thread(&WalBatchWriter::BackgroundFlushLoop, this);
  }
}

WalBatchWriter::~WalBatchWriter() {
  stop_background_.store(true);
  flush_cv_.notify_all();
  
  if (background_thread_.joinable()) {
    background_thread_.join();
  }
  
  // 确保所有数据刷盘
  Flush();
}

Status WalBatchWriter::Write(uint64_t txn_id,
                             const CedarKey& key,
                             const Descriptor& descriptor) {
  std::lock_guard<std::mutex> lock(buffer_mutex_);
  
  if (buffer_.size() >= options_.batch_size) {
    // 缓冲区满，立即刷盘
    Status s = DoFlush(buffer_);
    if (!s.ok()) {
      return s;
    }
    buffer_.clear();
  }
  
  buffer_.emplace_back(WalBatchEntry::PUT, txn_id, key, descriptor, Timestamp(0));
  
  // 触发后台刷盘
  if (options_.enable_background_flush && 
      buffer_.size() >= options_.batch_size / 2) {
    flush_cv_.notify_one();
  }
  
  return Status::OK();
}

Status WalBatchWriter::WriteCommit(uint64_t txn_id, Timestamp commit_ts) {
  std::lock_guard<std::mutex> lock(buffer_mutex_);
  
  buffer_.emplace_back(WalBatchEntry::COMMIT, txn_id);
  buffer_.back().timestamp = commit_ts;
  
  // 提交操作触发刷盘
  Status s = DoFlush(buffer_);
  if (s.ok()) {
    buffer_.clear();
  }
  
  return s;
}

Status WalBatchWriter::WriteAbort(uint64_t txn_id, Timestamp abort_ts) {
  std::lock_guard<std::mutex> lock(buffer_mutex_);
  
  buffer_.emplace_back(WalBatchEntry::ABORT, txn_id);
  buffer_.back().timestamp = abort_ts;
  
  return Status::OK();
}

Status WalBatchWriter::Flush() {
  std::lock_guard<std::mutex> lock(buffer_mutex_);
  
  if (buffer_.empty()) {
    return Status::OK();
  }
  
  Status s = DoFlush(buffer_);
  if (s.ok()) {
    buffer_.clear();
  }
  
  return s;
}

Status WalBatchWriter::Sync() {
  Status s = Flush();
  if (!s.ok()) {
    return s;
  }
  
  // 调用底层 WAL 的 sync
  // WalWriter 没有直接暴露 sync，通过 Flush 间接实现
  
  return Status::OK();
}

Status WalBatchWriter::DoFlush(std::vector<WalBatchEntry>& entries) {
  if (entries.empty()) {
    return Status::OK();
  }
  
  for (const auto& entry : entries) {
    Status s;
    switch (entry.type) {
      case WalBatchEntry::PUT:
        s = wal_writer_->WritePut(entry.key, entry.descriptor, Timestamp(entry.txn_id));
        break;
      case WalBatchEntry::COMMIT:
        s = wal_writer_->WriteCommit(entry.txn_id, entry.timestamp);
        break;
      case WalBatchEntry::ABORT:
        s = wal_writer_->WriteAbort(entry.txn_id, entry.timestamp);
        break;
    }
    
    if (!s.ok()) {
      return s;
    }
  }
  
  // 更新统计
  {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    stats_.total_written += entries.size();
    stats_.total_batches++;
  }
  
  return Status::OK();
}

void WalBatchWriter::BackgroundFlushLoop() {
  while (!stop_background_.load()) {
    std::unique_lock<std::mutex> lock(buffer_mutex_);
    
    // 等待刷盘条件或超时
    flush_cv_.wait_for(lock, 
                       std::chrono::milliseconds(options_.flush_interval_ms),
                       [this] {
                         return stop_background_.load() || 
                                buffer_.size() >= options_.batch_size / 2;
                       });
    
    if (!buffer_.empty()) {
      std::vector<WalBatchEntry> to_flush = std::move(buffer_);
      buffer_.clear();
      lock.unlock();
      
      DoFlush(to_flush);
    }
  }
}

WalBatchWriter::Stats WalBatchWriter::GetStats() const {
  std::lock_guard<std::mutex> lock(stats_mutex_);
  Stats stats = stats_;
  
  {
    std::lock_guard<std::mutex> buf_lock(buffer_mutex_);
    stats.pending_count = buffer_.size();
  }
  
  if (stats.total_batches > 0) {
    stats.avg_batch_size = static_cast<double>(stats.total_written) / stats.total_batches;
  }
  
  return stats;
}

Status WalBatchWriter::WriteEntry(const WalBatchEntry& entry) {
  std::lock_guard<std::mutex> lock(buffer_mutex_);
  
  if (buffer_.size() >= options_.batch_size) {
    Status s = DoFlush(buffer_);
    if (!s.ok()) {
      return s;
    }
    buffer_.clear();
  }
  
  buffer_.push_back(entry);
  
  return Status::OK();
}

// ==================== TransactionWalBatch ====================

TransactionWalBatch::TransactionWalBatch(WalBatchWriter* writer)
    : writer_(writer) {}

TransactionWalBatch::~TransactionWalBatch() {
  if (!entries_.empty()) {
    Commit();
  }
}

void TransactionWalBatch::AddPut(uint64_t txn_id, 
                                  const CedarKey& key, 
                                  const Descriptor& descriptor) {
  entries_.emplace_back(WalBatchEntry::PUT, txn_id, key, descriptor, Timestamp(0));
}

void TransactionWalBatch::AddPutWithTimestamp(uint64_t txn_id, 
                                               const CedarKey& key,
                                               const Descriptor& descriptor, 
                                               Timestamp ts) {
  entries_.emplace_back(WalBatchEntry::PUT, txn_id, key, descriptor, ts);
}

Status TransactionWalBatch::Commit() {
  if (!writer_ || entries_.empty()) {
    return Status::OK();
  }
  
  for (const auto& entry : entries_) {
    Status s = writer_->WriteEntry(entry);
    if (!s.ok()) {
      return s;
    }
  }
  
  entries_.clear();
  return Status::OK();
}

Status TransactionWalBatch::CommitAndSync() {
  Status s = Commit();
  if (!s.ok()) {
    return s;
  }
  
  return writer_->Sync();
}

void TransactionWalBatch::Clear() {
  entries_.clear();
}

}  // namespace cedar
