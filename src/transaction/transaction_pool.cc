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

#include "cedar/transaction/transaction_pool.h"

namespace cedar {

TransactionPool::TransactionPool(TransactionManager* txn_manager,
                                  VSLMemTable* memtable,
                                  LsmEngine* lsm_engine,
                                  WalWriter* wal_writer,
                                  size_t initial_size)
    : txn_manager_(txn_manager),
      memtable_(memtable),
      lsm_engine_(lsm_engine),
      wal_writer_(wal_writer) {
  // 预创建事务对象
  for (size_t i = 0; i < initial_size; ++i) {
    auto* txn = new OCCTransaction(txn_manager_, memtable_, lsm_engine_, 
                                   wal_writer_, TransactionOptions());
    pool_.push(txn);
    size_.fetch_add(1, std::memory_order_relaxed);
  }
}

TransactionPool::~TransactionPool() {
  Clear();
}

OCCTransaction* TransactionPool::Acquire(const TransactionOptions& options) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (!pool_.empty()) {
    auto* txn = pool_.front();
    pool_.pop();
    size_.fetch_sub(1, std::memory_order_relaxed);
    
    // 重置事务状态（简化处理：直接使用新选项）
    // 注意：这里需要确保事务已结束状态
    return txn;
  }
  
  // 池为空，创建新事务
  return new OCCTransaction(txn_manager_, memtable_, lsm_engine_, 
                            wal_writer_, options);
}

void TransactionPool::Release(OCCTransaction* txn) {
  if (!txn) return;
  
  // 重置事务状态
  // 简化处理：中止事务并重置
  if (txn->GetState() != TransactionState::kCommitted && 
      txn->GetState() != TransactionState::kAborted) {
    txn->Abort();
  }
  
  std::lock_guard<std::mutex> lock(mutex_);
  pool_.push(txn);
  size_.fetch_add(1, std::memory_order_relaxed);
}

void TransactionPool::Clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  while (!pool_.empty()) {
    auto* txn = pool_.front();
    pool_.pop();
    delete txn;
  }
  
  size_.store(0, std::memory_order_relaxed);
}

}  // namespace cedar
