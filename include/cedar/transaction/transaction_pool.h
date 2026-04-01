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

#ifndef FERN_TRANSACTION_POOL_H_
#define FERN_TRANSACTION_POOL_H_

#include <memory>
#include <queue>
#include <mutex>
#include <atomic>

#include "cedar/transaction/occ_transaction.h"

namespace cedar {

// 事务对象池 - 减少频繁创建/销毁事务的开销
class TransactionPool {
 public:
  TransactionPool(TransactionManager* txn_manager,
                  VSLMemTable* memtable,
                  LsmEngine* lsm_engine,
                  WalWriter* wal_writer,
                  size_t initial_size = 8);
  
  ~TransactionPool();
  
  // 获取一个事务对象
  OCCTransaction* Acquire(const TransactionOptions& options);
  
  // 归还事务对象
  void Release(OCCTransaction* txn);
  
  // 获取池大小
  size_t Size() const { return size_.load(); }
  
  // 清空池
  void Clear();
  
 private:
  TransactionManager* txn_manager_;
  VSLMemTable* memtable_;
  LsmEngine* lsm_engine_;
  WalWriter* wal_writer_;
  
  std::queue<OCCTransaction*> pool_;
  mutable std::mutex mutex_;
  std::atomic<size_t> size_{0};
};

// 事务自动归还 RAII 包装器
class PooledTransaction {
 public:
  PooledTransaction(TransactionPool* pool, OCCTransaction* txn)
      : pool_(pool), txn_(txn) {}
  
  ~PooledTransaction() {
    if (pool_ && txn_) {
      pool_->Release(txn_);
    }
  }
  
  // 禁止拷贝
  PooledTransaction(const PooledTransaction&) = delete;
  PooledTransaction& operator=(const PooledTransaction&) = delete;
  
  // 允许移动
  PooledTransaction(PooledTransaction&& other) noexcept
      : pool_(other.pool_), txn_(other.txn_) {
    other.pool_ = nullptr;
    other.txn_ = nullptr;
  }
  
  OCCTransaction* operator->() { return txn_; }
  OCCTransaction* get() { return txn_; }
  
 private:
  TransactionPool* pool_;
  OCCTransaction* txn_;
};

}  // namespace cedar

#endif  // FERN_TRANSACTION_POOL_H_
