// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.
#include "table/cedar_parquet/cedar_parquet_read_budget.h"

#include <algorithm>
#include <atomic>
#include <new>
#include <utility>

namespace ROCKSDB_NAMESPACE::cedar_parquet {

struct CedarParquetReadBudgetState {
  CedarParquetReadBudgetState(uint64_t bytes,
                              CedarParquetReadBudgetKind budget_kind)
      : limit(bytes), kind(budget_kind) {}

  // A rejection must name the pool that ran out: reader metadata and page-decode
  // working have separate limits, and the previous shared message made a
  // metadata rejection look like a working-pool failure.
  Status BudgetExhausted() const {
    return Status::MemoryLimit(kind == CedarParquetReadBudgetKind::kMetadata
                                   ? "Cedar Parquet reader metadata budget exhausted"
                                   : "Cedar Parquet read working budget exhausted");
  }

  Status Reserve(uint64_t bytes) {
    uint64_t current = used.load(std::memory_order_relaxed);
    for (;;) {
      if (bytes > limit - current) {
        rejected.fetch_add(1, std::memory_order_relaxed);
        return BudgetExhausted();
      }
      if (used.compare_exchange_weak(current, current + bytes,
                                     std::memory_order_acq_rel)) break;
    }
    const uint64_t next = current + bytes;
    uint64_t observed = peak.load(std::memory_order_relaxed);
    while (observed < next && !peak.compare_exchange_weak(
               observed, next, std::memory_order_relaxed)) {}
    return Status::OK();
  }

  const uint64_t limit;
  const CedarParquetReadBudgetKind kind;
  std::atomic<uint64_t> used{0};
  std::atomic<uint64_t> peak{0};
  std::atomic<uint64_t> rejected{0};
};

CedarParquetMemoryLease::CedarParquetMemoryLease(
    std::shared_ptr<CedarParquetReadBudgetState> state, uint64_t bytes)
    : state_(std::move(state)), bytes_(bytes) {}

CedarParquetMemoryLease::~CedarParquetMemoryLease() {
  if (query_) query_->Release(bytes_);
  state_->used.fetch_sub(bytes_, std::memory_order_acq_rel);
}

Status CedarParquetMemoryLease::Grow(uint64_t additional) {
  Status status = state_->Reserve(additional);
  if (status.ok() && query_) {
    status = query_->Reserve(additional);
    if (!status.ok()) {
      state_->used.fetch_sub(additional, std::memory_order_acq_rel);
      state_->rejected.fetch_add(1, std::memory_order_relaxed);
    }
  }
  if (status.ok()) bytes_ += additional;
  return status;
}

void CedarParquetMemoryLease::Shrink(uint64_t bytes) {
  const uint64_t released = std::min(bytes, bytes_);
  bytes_ -= released;
  if (query_) query_->Release(released);
  state_->used.fetch_sub(released, std::memory_order_acq_rel);
}

CedarParquetReadBudget::CedarParquetReadBudget(
    uint64_t limit_bytes, CedarParquetReadBudgetKind kind)
    : state_(std::make_shared<CedarParquetReadBudgetState>(limit_bytes, kind)) {}

Status CedarParquetReadBudget::Acquire(
    uint64_t bytes, std::unique_ptr<CedarParquetMemoryLease>* out,
    std::shared_ptr<CedarParquetQueryMemory> query) {
  if (out == nullptr || *out != nullptr) {
    return Status::InvalidArgument("Cedar Parquet read lease destination is not empty");
  }
  Status status = state_->Reserve(bytes);
  if (!status.ok()) return status;
  if (query) {
    status = query->Reserve(bytes);
    if (!status.ok()) {
      state_->used.fetch_sub(bytes, std::memory_order_acq_rel);
      state_->rejected.fetch_add(1, std::memory_order_relaxed);
      return status;
    }
  }
  auto* lease = new (std::nothrow) CedarParquetMemoryLease(state_, bytes);
  if (lease == nullptr) {
    if (query) query->Release(bytes);
    state_->used.fetch_sub(bytes, std::memory_order_acq_rel);
    state_->rejected.fetch_add(1, std::memory_order_relaxed);
    return Status::MemoryLimit("Cedar Parquet cannot allocate read lease");
  }
  lease->query_ = std::move(query);
  out->reset(lease);
  return Status::OK();
}

uint64_t CedarParquetReadBudget::limit_bytes() const { return state_->limit; }
uint64_t CedarParquetReadBudget::current_bytes() const {
  return state_->used.load(std::memory_order_relaxed);
}
uint64_t CedarParquetReadBudget::peak_bytes() const {
  return state_->peak.load(std::memory_order_relaxed);
}
uint64_t CedarParquetReadBudget::rejections() const {
  return state_->rejected.load(std::memory_order_relaxed);
}

}  // namespace ROCKSDB_NAMESPACE::cedar_parquet
