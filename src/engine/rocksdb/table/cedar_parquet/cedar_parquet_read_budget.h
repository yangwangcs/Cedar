// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <cstdint>
#include <memory>

#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE::cedar_parquet {

struct CedarParquetReadBudgetState;
class CedarParquetQueryMemory {
 public:
  virtual ~CedarParquetQueryMemory() = default;
  virtual Status Reserve(uint64_t bytes) = 0;
  virtual void Release(uint64_t bytes) = 0;
  virtual Status Pin(const void* owner, uint64_t bytes,
                     std::shared_ptr<const void>* lease) = 0;
};

// Distinguishes the two DB read pools that share this accounting so a
// rejection names the pool that actually ran out. CedarParquetReadBudget packs
// page-decode scratch (working) and long-lived reader metadata into one class.
enum class CedarParquetReadBudgetKind : uint8_t { kWorking = 0, kMetadata };

// A single-owner reservation; its shared accounting state outlives the facade.
class CedarParquetMemoryLease {
 public:
  ~CedarParquetMemoryLease();
  CedarParquetMemoryLease(const CedarParquetMemoryLease&) = delete;
  CedarParquetMemoryLease& operator=(const CedarParquetMemoryLease&) = delete;
  Status Grow(uint64_t additional);
  void Shrink(uint64_t bytes);
  uint64_t bytes() const { return bytes_; }

 private:
  friend class CedarParquetReadBudget;
  CedarParquetMemoryLease(std::shared_ptr<CedarParquetReadBudgetState> state,
                          uint64_t bytes);
  std::shared_ptr<CedarParquetReadBudgetState> state_;
  std::shared_ptr<CedarParquetQueryMemory> query_;
  uint64_t bytes_;
};

class CedarParquetReadBudget {
 public:
  explicit CedarParquetReadBudget(
      uint64_t limit_bytes,
      CedarParquetReadBudgetKind kind = CedarParquetReadBudgetKind::kWorking);
  Status Acquire(uint64_t bytes, std::unique_ptr<CedarParquetMemoryLease>* out,
                 std::shared_ptr<CedarParquetQueryMemory> query = {});
  uint64_t limit_bytes() const;
  uint64_t current_bytes() const;
  uint64_t peak_bytes() const;
  uint64_t rejections() const;

 private:
  std::shared_ptr<CedarParquetReadBudgetState> state_;
};

}  // namespace ROCKSDB_NAMESPACE::cedar_parquet
