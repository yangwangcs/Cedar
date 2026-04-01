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

#ifndef FERN_GRAPH_DB_H_
#define FERN_GRAPH_DB_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <map>

#include "cedar/core/status.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"
#include "cedar/transaction/occ_transaction.h"
#include "cedar/storage/cedar_options.h"

namespace cedar {

// 前向声明
class CedarGraphDBImpl;
class LsmEngine;
class ColumnFamilyHandle;
struct CedarGraphOptions;
class Snapshot;
struct Range;

// 使用 storage/cedar_options.h 中的选项
// Flush 选项（扩展）
// 使用 storage/cedar_options.h 中的 FlushOptions

// 压缩范围选项
struct CompactRangeOptions {
  uint64_t start_entity_id = 0;
  uint64_t end_entity_id = UINT64_MAX;
  int target_level = -1;
  bool bottommost_level_compaction = true;
};

// 快照接口
class Snapshot {
 public:
  virtual ~Snapshot() = default;
  virtual uint64_t GetSequenceNumber() const = 0;
  virtual uint64_t GetTimestamp() const = 0;
};

// 列族句柄
class ColumnFamilyHandle {
 public:
  virtual ~ColumnFamilyHandle() = default;
  virtual uint32_t GetID() const = 0;
  virtual const std::string& GetName() const = 0;
  virtual std::string GetStats() const = 0;
};

// CedarGraphDB - 统一的数据库接口
class CedarGraphDB {
 public:
  // 静态工厂方法
  static Status Open(const std::string& db_path,
                     const CedarGraphOptions& options,
                     CedarGraphDB** db_ptr);
  
  static Status Open(const std::string& db_path,
                     const CedarGraphOptions& options,
                     const std::vector<std::string>& column_families,
                     std::vector<CedarGraphDB*>* db_ptrs,
                     std::vector<ColumnFamilyHandle*>* handles = nullptr);
  
  static Status DestroyDB(const std::string& db_path,
                          const CedarGraphOptions& options);
  
  static Status RepairDB(const std::string& db_path,
                         const CedarGraphOptions& options);
  
  ~CedarGraphDB();
  
  CedarGraphDB(const CedarGraphDB&) = delete;
  CedarGraphDB& operator=(const CedarGraphDB&) = delete;
  
  // 基本 CRUD 操作
  Status Put(const CedarKey& key, 
             const Descriptor& descriptor,
             const WriteOptions& options = WriteOptions());
  
  Status Delete(const CedarKey& key,
                const WriteOptions& options = WriteOptions());
  
  std::optional<Descriptor> Get(const CedarKey& key,
                                const ReadOptions& options = ReadOptions());
  
  // 事务支持
  std::unique_ptr<OCCTransaction> BeginTransaction(
      const TransactionOptions& options = TransactionOptions());
  
  // 管理操作
  Status Flush(const FlushOptions& options = FlushOptions{});
  Status CompactRange(const CompactRangeOptions& options = CompactRangeOptions());
  
  // 快照支持
  const Snapshot* GetSnapshot();
  void ReleaseSnapshot(const Snapshot* snapshot);
  
  // 列族操作
  Status CreateColumnFamily(const std::string& name,
                            CedarGraphDB** cf_handle);
  Status DropColumnFamily(CedarGraphDB* cf_handle);
  ColumnFamilyHandle* DefaultColumnFamily();
  
  // 属性与统计
  Status GetProperty(const std::string& property, std::string* value);
  std::string GetStatsString();
  
  // 元数据查询
  uint64_t GetLatestSequenceNumber() const;
  std::string GetName() const;
  std::string GetColumnFamilyName() const;
  
  // 备份
  Status CreateCheckpoint(const std::string& checkpoint_dir);
  static Status RestoreFromCheckpoint(const std::string& checkpoint_dir,
                                       const std::string& db_dir);
  
  // 内部接口
  CedarGraphDBImpl* GetInternalImpl() { return impl_.get(); }
  LsmEngine* GetLsmEngine();
  
 private:
  explicit CedarGraphDB(std::unique_ptr<CedarGraphDBImpl> impl);
  
  std::unique_ptr<CedarGraphDBImpl> impl_;
};

// 范围结构
struct Range {
  CedarKey start;
  CedarKey limit;
};

// 选项结构
struct CedarGraphOptions {
  // 创建选项
  bool create_if_missing = true;
  bool error_if_exists = false;
  bool paranoid_checks = false;
  
  // 存储选项
  size_t write_buffer_size = 64 * 1024 * 1024;
  int max_write_buffer_number = 2;
  int min_write_buffer_number_to_merge = 1;
  
  // LSM 树选项
  size_t target_file_size_base = 256 * 1024 * 1024;
  int max_bytes_for_level_multiplier = 10;
  int max_background_compactions = 1;
  int max_background_flushes = 1;
  int max_levels = 7;
  
  // Manifest 选项
  size_t manifest_preallocate_size = 4 * 1024 * 1024;
  bool enable_manifest_compression = true;
  size_t keep_manifest_file_count = 10;
  
  // WAL 选项
  bool enable_wal = true;
  bool manual_wal_flush = false;
  uint64_t wal_ttl_seconds = 0;
  uint64_t wal_size_limit_mb = 0;
  
  // 事务选项
  bool enable_transaction = true;
  IsolationLevel default_isolation = IsolationLevel::kSnapshot;
  uint64_t transaction_timeout_ms = 30000;
  int max_transaction_retries = 3;
  
  // 缓存选项
  size_t block_cache_size = 512 * 1024 * 1024;
  size_t file_cache_size = 256;
  size_t table_cache_size = 128 * 1024 * 1024;
  
  // 压缩选项
  CompressionType compression = CompressionType::Lz4;
  int compression_level = -1;
  
  // 布隆过滤器选项
  bool enable_bloom_filter = true;
  double bloom_filter_bits_per_key = 10.0;
  
  // 统计选项
  bool enable_statistics = true;
  bool enable_slow_query_log = true;
  uint64_t slow_query_threshold_ms = 100;
  
  // 高级选项
  Env* env = nullptr;
  int log_level = 1;
  std::string log_file_path;
  bool use_direct_io = false;
  bool use_mmap_reads = true;
};

}  // namespace cedar

#endif  // FERN_GRAPH_DB_H_
