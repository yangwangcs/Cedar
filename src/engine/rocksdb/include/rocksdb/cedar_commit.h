// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <atomic>
#include <cstdint>
#include <string>

#include "rocksdb/rocksdb_namespace.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE {

class DB;
struct WriteOptions;
class WriteBatch;
class Slice;

using WalDurableCallback = void (*)(void* context) noexcept;

// Explicit options for Cedar's already-decided epoch path. The path always
// uses one synchronous WAL record with WAL enabled; these fields are kept
// narrow so Cedar cannot accidentally inherit unrelated DB write policy.
struct CedarEpochOptions {
  bool sync = true;
  bool disable_wal = false;
  uint8_t protection_bytes_per_key = 0;
  // Owned by Cedar's maintenance scheduler. RocksDB sets this only while the
  // epoch's WAL durability boundary is in progress, then clears it before the
  // MemTable stage so maintenance can resume immediately afterwards.
  std::atomic<bool>* wal_sync_critical = nullptr;
};

// Populated only by WriteCedarEpoch. Each duration is measured at the
// corresponding RocksDB execution point, not inferred from outer wall time.
struct CedarEpochMetrics {
  uint64_t wal_append_us = 0;
  uint64_t wal_sync_us = 0;
  uint64_t manifest_us = 0;
  uint64_t memtable_insert_us = 0;
  uint64_t wal_rotations = 0;
};

// Writes one Cedar-decided epoch through RocksDB's WAL, recovery, sequence,
// and MemTable primitives. The callback is
// invoked exactly once after WAL durability and any required MANIFEST update,
// before MemTable insertion.
Status WriteCedarEpoch(DB* db, const CedarEpochOptions& options,
                       WriteBatch* batch,
                       WalDurableCallback on_wal_durable,
                       void* callback_context,
                       CedarEpochMetrics* metrics = nullptr);

Status MakeCedarParquetSortLowerBound(const Slice& user_key,
                                      std::string* sort_key);

}  // namespace ROCKSDB_NAMESPACE
