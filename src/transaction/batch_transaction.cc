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

#include "cedar/transaction/batch_transaction.h"

namespace cedar {

// ==================== BatchTransactionExecutor ====================

BatchTransactionExecutor::BatchTransactionExecutor(CedarGraphStorage* storage)
    : storage_(storage) {}

BatchTransactionExecutor::~BatchTransactionExecutor() = default;

void BatchTransactionExecutor::AddPut(uint64_t entity_id,
                                       EntityType entity_type,
                                       uint16_t column_id,
                                       const Descriptor& descriptor,
                                       Timestamp user_timestamp) {
  operations_.emplace_back(OpType::PUT, entity_id, entity_type, 
                          column_id, descriptor, user_timestamp);
}

void BatchTransactionExecutor::AddGet(uint64_t entity_id,
                                       EntityType entity_type,
                                       uint16_t column_id) {
  operations_.emplace_back(OpType::GET, entity_id, entity_type, 
                          column_id);
}

void BatchTransactionExecutor::AddDelete(uint64_t entity_id,
                                          EntityType entity_type,
                                          uint16_t column_id) {
  operations_.emplace_back(OpType::DELETE, entity_id, entity_type, 
                          column_id);
}

Status BatchTransactionExecutor::Execute() {
  if (operations_.empty()) {
    return Status::OK();
  }
  
  auto* txn = storage_->beginTX();
  if (!txn) {
    return Status::InvalidArgument("BatchTransaction", "failed to begin transaction");
  }
  
  get_results_.clear();
  get_results_.reserve(operations_.size());
  
  for (const auto& op : operations_) {
    switch (op.type) {
      case OpType::PUT: {
        Status s = txn->Put(op.entity_id, op.entity_type, op.column_id,
                            op.descriptor, op.user_timestamp);
        if (!s.ok()) {
          txn->Abort();
          delete txn;
          return s;
        }
        break;
      }
      
      case OpType::GET: {
        Descriptor desc;
        Timestamp ver;
        Status s = txn->Get(op.entity_id, op.entity_type, op.column_id,
                            &desc, &ver);
        if (s.ok()) {
          get_results_.push_back(desc);
        } else {
          get_results_.push_back(std::nullopt);
        }
        break;
      }
      
      case OpType::DELETE: {
        Status s = txn->Delete(op.entity_id, op.entity_type, op.column_id);
        if (!s.ok()) {
          txn->Abort();
          delete txn;
          return s;
        }
        break;
      }
    }
  }
  
  Status commit_status = txn->Commit();
  delete txn;
  
  operations_.clear();
  
  return commit_status;
}

std::optional<Descriptor> BatchTransactionExecutor::GetResult(size_t index) {
  if (index < get_results_.size()) {
    return get_results_[index];
  }
  return std::nullopt;
}

void BatchTransactionExecutor::Clear() {
  operations_.clear();
  get_results_.clear();
}

// ==================== AutoBatchExecutor ====================

AutoBatchExecutor::AutoBatchExecutor(CedarGraphStorage* storage, 
                                      const Options& options)
    : storage_(storage), options_(options) {}

AutoBatchExecutor::~AutoBatchExecutor() {
  Flush();
}

Status AutoBatchExecutor::SubmitPut(uint64_t entity_id,
                                     EntityType entity_type,
                                     uint16_t column_id,
                                     const Descriptor& descriptor,
                                     Timestamp user_timestamp) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  buffer_.emplace_back(OpType::PUT,
                       entity_id, entity_type, column_id,
                       descriptor, user_timestamp);
  
  if (buffer_.size() >= options_.batch_size) {
    return Flush();
  }
  
  return Status::OK();
}

Status AutoBatchExecutor::Flush() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (buffer_.empty()) {
    return Status::OK();
  }
  
  auto* txn = storage_->beginTX();
  if (!txn) {
    return Status::InvalidArgument("AutoBatchExecutor", "failed to begin transaction");
  }
  
  for (const auto& op : buffer_) {
    Status s = txn->Put(op.entity_id, op.entity_type, op.column_id,
                        op.descriptor, op.user_timestamp);
    if (!s.ok()) {
      txn->Abort();
      delete txn;
      return s;
    }
  }
  
  Status commit_status = txn->Commit();
  delete txn;
  
  if (commit_status.ok()) {
    executed_count_.fetch_add(buffer_.size(), std::memory_order_relaxed);
  }
  
  buffer_.clear();
  
  return commit_status;
}

}  // namespace cedar
