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

#ifndef FERN_LSM_ENGINE_H_
#define FERN_LSM_ENGINE_H_

#include <atomic>
#include <chrono>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

#include "cedar/storage/cedar_memtable.h"
#include "cedar/storage/cedar_options.h"
#include "cedar/core/status.h"
// #include "cedar/frond/frond_format.h"  // 已迁移到 zone_columnar_format.h
#include "cedar/transaction/wal.h"
#include "cedar/transaction/occ_transaction.h"
#include "cedar/storage/vsl_memtable.h"
// DEPRECATED: Old format file cache - now using ZoneColumnar
// #include "cedar/storage/frond_file_cache.h"
#include "cedar/storage/query_cache.h"
#include "cedar/storage/size_tiered_compaction.h"
#include "cedar/storage/sst_reader_cache.h"
#include "cedar/common/roaring_bitmap.h"

#include <set>
#include <unordered_set>

#include "cedar/core/env.h"

namespace cedar {

// 前向声明
class BatchExecutor;

// Frond file metadata
struct FrondFileMeta {
  uint64_t file_number;
  uint64_t file_size;
  uint64_t num_entries;
  uint64_t min_entity_id;
  uint64_t max_entity_id;
  uint64_t min_tx_time;
  uint64_t max_tx_time;
  int level;
  
  // Column and type info for filtering
  uint16_t column_id = 0;
  uint8_t entity_type = 0;
  
  // Version chain for historical queries
  uint64_t prev_file_number = 0;
  
  // OPTIMIZATION: Bloom Filter for fast negative check
  // This allows excluding files without opening them
  uint64_t bloom_filter_hash = 0;
  bool has_bloom_filter = false;
  
  std::string file_name() const;
};

// LSM Storage Engine
class LsmEngine {
 public:
  LsmEngine(const std::string& db_path,
            const CedarOptions& options,
            cedar::Env* env);
  virtual ~LsmEngine();

  LsmEngine(const LsmEngine&) = delete;
  LsmEngine& operator=(const LsmEngine&) = delete;

  // Open the database
  virtual Status Open();

  // Close the database
  virtual Status Close();

  // Put a key-value pair (legacy interface)
  Status Put(uint64_t entity_id, uint64_t tx_time, const Slice& value, Timestamp txn_version);

  // Put with new types
  Status Put(const CedarKey& key, const Descriptor& descriptor, Timestamp txn_version);

  // Delete a key (legacy interface)
  Status Delete(uint64_t entity_id, uint64_t tx_time, Timestamp txn_version);

  // Delete with new types
  Status Delete(const CedarKey& key, Timestamp txn_version);

  // Get a value (legacy interface)
  Status Get(uint64_t entity_id, uint64_t tx_time, std::string* value);

  // Get with new types
  std::optional<Descriptor> Get(const CedarKey& key);

  // Get all versions
  std::vector<MemTableEntry> GetAll(uint64_t entity_id,
                                     EntityType entity_type,
                                     uint16_t column_id);

  // Get at specific time
  std::optional<Descriptor> GetAtTime(uint64_t entity_id,
                                       EntityType entity_type,
                                       uint16_t column_id,
                                       Timestamp timestamp);

  // Get time range
  std::vector<MemTableEntry> GetRange(uint64_t entity_id,
                                       EntityType entity_type,
                                       uint16_t column_id,
                                       Timestamp start,
                                       Timestamp end);

  // Get time range with limit - stops after max_results entries
  std::vector<MemTableEntry> GetRangeLimit(uint64_t entity_id,
                                            EntityType entity_type,
                                            uint16_t column_id,
                                            Timestamp start,
                                            Timestamp end,
                                            size_t max_results);
  
  // ========== 批量查询接口 (优化) ==========
  // Batch query multiple entity IDs at once - reduces lock overhead
  struct BatchQueryItem {
    uint64_t entity_id;
    EntityType entity_type;
    uint16_t column_id;
    Timestamp timestamp;
    std::optional<Descriptor> result;
  };
  
  // Batch GetAtTime - query multiple entities with single lock acquisition
  void BatchGetAtTime(std::vector<BatchQueryItem>& items);
  
  // Batch GetRange - query multiple entities with single lock acquisition
  struct BatchRangeItem {
    uint64_t entity_id;
    EntityType entity_type;
    uint16_t column_id;
    Timestamp start;
    Timestamp end;
    size_t max_results;
    std::vector<MemTableEntry> results;
  };
  void BatchGetRange(std::vector<BatchRangeItem>& items);
  
  // OPTIMIZATION: 并行 BatchGetRange - 使用线程池加速多 entity 查询
  void ParallelBatchGetRange(std::vector<BatchRangeItem>& items, size_t num_threads = 4);
  
  // OPTIMIZATION: P0 - 优化的批量时间范围查询
  // 使用 SST 层的 BatchGetRange 减少文件扫描次数
  // 返回：entity_id -> MemTableEntry 列表
  std::unordered_map<uint64_t, std::vector<MemTableEntry>> 
  BatchGetRangeOptimized(const std::vector<uint64_t>& entity_ids,
                         EntityType entity_type,
                         uint16_t column_id,
                         Timestamp start,
                         Timestamp end,
                         size_t max_results_per_entity = 200);
  
  // OPTIMIZATION: P2 - 并行查询多个 SST 文件
  // 使用线程池并行查询多个文件，加速大数据量查询
  std::unordered_map<uint64_t, std::vector<MemTableEntry>>
  ParallelGetRangeFromSST(const std::vector<uint64_t>& entity_ids,
                           EntityType entity_type,
                           uint16_t column_id,
                           Timestamp start,
                           Timestamp end,
                           size_t max_results_per_entity = 200,
                           size_t num_threads = 4);

  // ========== MVCC 时态版本链查询 (融合方案) ==========
  // 获取实体的完整时间历史 (跨 MemTable + SST)
  // 融合 MemTable 版本链 + SST Full-History，支持 O(k) 遍历版本链
  struct TemporalVersion {
    Timestamp timestamp;
    Descriptor descriptor;
    int source_level;  // -1=MemTable, 0+=SST Level
  };
  
  // 获取完整版本链 (MemTable + 所有 SST 层级)
  // 按时间戳降序返回 (最新 -> 最旧)
  std::vector<TemporalVersion> GetTemporalChain(uint64_t entity_id,
                                                 EntityType entity_type,
                                                 uint16_t column_id);
  
  // 遍历版本链 (回调方式，支持提前终止)
  // 先遍历 MemTable (热数据)，再遍历 SST (冷数据)
  void TraverseTemporalChain(uint64_t entity_id,
                              EntityType entity_type,
                              uint16_t column_id,
                              std::function<bool(const TemporalVersion&)> callback);

  // Force flush memtable to disk
  Status ForceFlush();

  // Trigger compaction manually
  Status Compact();

  // Get SST file list for a level
  std::vector<FrondFileMeta> GetFrondFiles(int level) const;

  // Get statistics
  struct Stats {
    size_t memtable_size = 0;
    size_t imm_memtable_size = 0;
    size_t frond_count = 0;
    size_t frond_size = 0;
    int num_levels = 0;
  };
  Stats GetStats() const;

  // ========== WAL 和事务支持 ==========
  
  // 获取 WAL 写入器
  WalWriter* GetWalWriter() const { return wal_writer_.get(); }
  
  // 获取事务管理器
  TransactionManager* GetTransactionManager() const { 
    return txn_manager_.get(); 
  }
  
  // 获取 MemTable
  VSLMemTable* GetMemTable() const { return mem_.get(); }
  
  // 获取 Batch 执行器
  BatchExecutor* GetBatchExecutor() const { return batch_executor_.get(); }
  
  // 创建新的事务
  std::unique_ptr<OCCTransaction> BeginTransaction(
      const TransactionOptions& options = TransactionOptions());
  
  // 同步 WAL
  Status SyncWAL();
  
  // Get available column IDs for an entity (optimization for Scan)
  std::vector<uint16_t> GetEntityColumnIds(uint64_t entity_id);
  
  // DEPRECATED: Old format reader cache - now using ZoneColumnarSstReader
  // FrondSstReader* GetCachedReader(const std::string& filepath, uint64_t file_size);
  
  // Query cache operations
  QueryCache* GetQueryCache() const { return query_cache_.get(); }
  void InvalidateQueryCache(uint64_t entity_id);
  
  // Get database path
  std::string GetDbPath() const { return db_path_; }
  
  // Get SST files for checkpoint
  const std::vector<std::vector<FrondFileMeta>>& GetSstFiles() const { return levels_; }
  
  // ========== Size-Tiered Compaction 引擎 ==========
  
  // 获取 Compaction 引擎
  SizeTieredCompactionEngine* GetCompactionEngine() const { return compaction_engine_.get(); }
  
  // 手动触发全量合并
  Status CompactAll();
  
  // 等待所有后台合并完成
  void WaitForCompactions();

 private:
  // 初始化 WAL
  Status InitWAL();
  // Background compaction worker
  void BackgroundCompaction();

  // Check if compaction is needed and trigger it
  void MaybeScheduleCompaction();

  // Flush memtable to SST file
  Status FlushMemTable(VSLMemTable* mem);
  
  // Flush a group of entries (same entity_type and column_id) to SST file
  // Used by FlushMemTable to support mixed entity types
  Status FlushEntityGroup(uint8_t entity_type, uint16_t column_id,
                          const std::vector<std::pair<CedarKey, Descriptor>>& entries);

  // Do compaction on specified level
  Status DoCompaction(int level, const std::vector<FrondFileMeta>& inputs);

  // Select files for compaction
  std::vector<FrondFileMeta> SelectCompactionFiles(int level);
  
  // Query SST files for entity
  void QueryFrondFiles(uint64_t entity_id, EntityType entity_type,
                     uint16_t column_id, std::vector<MemTableEntry>* results);
  
  // Helper: Get entries from SST files for specific entity (replaces GetVersionChain)
  void GetEntriesFromSst(uint64_t entity_id, EntityType entity_type,
                         uint16_t column_id, std::vector<std::pair<CedarKey, Descriptor>>* results);
  
  // Check if compaction is needed
  bool NeedsCompaction(int level) const;

  // Load existing SST files
  Status LoadSstFiles();

  // Generate next file number
  uint64_t NewFileNumber();

  // SST file path
  std::string SstFilePath(uint64_t file_number);

  std::string db_path_;
  CedarOptions options_;
  cedar::Env* env_;

  // MemTables (使用 Lock-Free VSL MemTable)
  std::unique_ptr<VSLMemTable> mem_;
  std::unique_ptr<VSLMemTable> imm_;  // Immutable memtable being flushing
  mutable std::shared_mutex mutex_;  // 使用 shared_mutex 支持并发读
  std::atomic<bool> has_work_{false};

  // SST files by level
  std::vector<std::vector<FrondFileMeta>> levels_;

  // Background thread
  std::atomic<bool> shutdown_;
  std::thread* bg_thread_;
  std::atomic<bool> compaction_scheduled_;

  // File number generator
  std::atomic<uint64_t> next_file_number_;

  // State
  bool opened_ = false;
  
  // WAL 和事务支持
  std::unique_ptr<WalWriter> wal_writer_;
  std::unique_ptr<TransactionManager> txn_manager_;
  std::unique_ptr<BatchExecutor> batch_executor_;
  
  // DEPRECATED: Old format file cache - now using ZoneColumnar
  // File handle cache for better read performance
  // std::unique_ptr<FrondFileCache> file_cache_;
  // OPTIMIZATION: Hot data preload flag
  bool hot_data_preloaded_ = false;
  
  // OPTIMIZATION: Preload hot entities on startup
  void PreloadHotEntities();
  
  // OPTIMIZATION: 热数据预读取 - 根据查询模式预读取热数据
  // 在查询 SST 文件之前，先检查是否需要预读取热数据
  void PrefetchHotData(uint64_t entity_id, EntityType entity_type, uint16_t column_id);
  
  // OPTIMIZATION: 查询模式追踪 - 用于识别热数据
  void TrackQueryPattern(uint64_t entity_id, EntityType entity_type, uint16_t column_id);

  
  // Query result cache
  std::unique_ptr<QueryCache> query_cache_;
  
  // OPTIMIZATION: 跨查询缓存 - 缓存热数据的查询结果
  // 格式: (entity_id, column_id) -> vector<MemTableEntry>
  struct CacheKey {
    uint64_t entity_id;
    uint16_t column_id;
    
    bool operator==(const CacheKey& other) const {
      return entity_id == other.entity_id && column_id == other.column_id;
    }
  };
  
  struct CacheKeyHash {
    size_t operator()(const CacheKey& key) const {
      return std::hash<uint64_t>()(key.entity_id) ^ 
             (std::hash<uint16_t>()(key.column_id) << 1);
    }
  };
  
  // 缓存条目
  struct CacheEntry {
    std::vector<MemTableEntry> data;
    std::chrono::steady_clock::time_point timestamp;
    uint32_t hit_count = 0;
  };
  
  mutable std::unordered_map<CacheKey, CacheEntry, CacheKeyHash> cross_query_cache_;
  mutable std::shared_mutex cross_query_cache_mutex_;
  static constexpr size_t kMaxCrossQueryCacheSize = 1000;  // 最大缓存条目数
  static constexpr auto kCrossQueryCacheTTL = std::chrono::seconds(60);  // 缓存过期时间
  
  // 查询模式追踪
  struct QueryPattern {
    uint64_t count = 0;
    std::chrono::steady_clock::time_point last_query;
  };
  mutable std::unordered_map<CacheKey, QueryPattern, CacheKeyHash> query_patterns_;
  mutable std::shared_mutex query_pattern_mutex_;
  static constexpr uint64_t kHotDataThreshold = 10;  // 热数据阈值：查询次数超过此值
  
  // 跨查询缓存操作
  std::optional<std::vector<MemTableEntry>> GetFromCrossQueryCache(
      uint64_t entity_id, uint16_t column_id) const;
  void AddToCrossQueryCache(uint64_t entity_id, uint16_t column_id, 
                            const std::vector<MemTableEntry>& data);
  void InvalidateCrossQueryCache(uint64_t entity_id, uint16_t column_id);
  void CleanupCrossQueryCache();
  
  // OPTIMIZATION: P1 - 时间范围预计算缓存
  // 缓存常见时间范围的查询结果
  struct TimeRangeCacheKey {
    uint64_t entity_id;
    uint16_t column_id;
    uint64_t start_ts;
    uint64_t end_ts;
    
    bool operator==(const TimeRangeCacheKey& other) const {
      return entity_id == other.entity_id && 
             column_id == other.column_id &&
             start_ts == other.start_ts && 
             end_ts == other.end_ts;
    }
  };
  
  struct TimeRangeCacheKeyHash {
    size_t operator()(const TimeRangeCacheKey& key) const {
      return std::hash<uint64_t>()(key.entity_id) ^ 
             (std::hash<uint16_t>()(key.column_id) << 1) ^
             (std::hash<uint64_t>()(key.start_ts) << 2) ^
             (std::hash<uint64_t>()(key.end_ts) << 3);
    }
  };
  
  mutable std::unordered_map<TimeRangeCacheKey, 
                             std::pair<std::vector<MemTableEntry>, 
                                       std::chrono::steady_clock::time_point>,
                             TimeRangeCacheKeyHash> time_range_cache_;
  mutable std::shared_mutex time_range_cache_mutex_;
  static constexpr size_t kMaxTimeRangeCacheSize = 10000;
  static constexpr auto kTimeRangeCacheTTL = std::chrono::seconds(30);
  
  std::optional<std::vector<MemTableEntry>> GetFromTimeRangeCache(
      uint64_t entity_id, uint16_t column_id, uint64_t start_ts, uint64_t end_ts) const;
  void AddToTimeRangeCache(uint64_t entity_id, uint16_t column_id, 
                           uint64_t start_ts, uint64_t end_ts,
                           const std::vector<MemTableEntry>& data);
  void CleanupTimeRangeCache();
  
  // SST Reader cache for better read performance
  std::unique_ptr<SstReaderCache> sst_reader_cache_;
  
  // OPTIMIZATION: P1 - SST 文件预加载管理
  // 预加载热数据文件到缓存
  void PreloadHotSSTFiles();
  
  // 获取查询涉及的热点文件列表
  std::vector<std::string> GetHotSSTFilesForQuery(
      const std::vector<uint64_t>& entity_ids,
      uint16_t column_id,
      uint8_t entity_type) const;
  
  // Column ID tracking for efficient Scan queries
  // Maps entity_id -> roaring bitmap of column_ids that have data
  std::unordered_map<uint64_t, RoaringBitmap> entity_column_map_;
  mutable std::shared_mutex column_map_mutex_;
  
  // ============================================================================
  // QUERY OPTIMIZATION: Fast File Lookup by Entity Range
  // ============================================================================
  // Organize files by column_id for faster lookup
  // column_id -> list of file metadata (sorted by min_entity_id)
  std::unordered_map<uint16_t, std::vector<FrondFileMeta*>> column_file_index_;
  mutable std::shared_mutex column_index_mutex_;
  
  // Build column-based file index
  void BuildColumnFileIndex();
  
  // Get candidate files for an entity (using range filtering + Bloom Filter)
  std::vector<FrondFileMeta*> GetCandidateFiles(uint64_t entity_id, 
                                                 EntityType entity_type, 
                                                 uint16_t column_id) const;
  
  // Track column IDs during write for quick lookup
  void TrackColumnId(uint64_t entity_id, uint16_t column_id);
  
  // ============================================================================
  // BULK IMPORT OPTIMIZATIONS
  // ============================================================================
 public:
  // Enable/disable optimizations for bulk import
  void SetBulkImportMode(bool enabled) {
    disable_query_cache_invalidate_ = enabled;
    disable_column_tracking_ = enabled;
    batch_tracking_enabled_ = enabled;
  }
  
  // Flush batched column ID tracking
  void FlushColumnIdBatch();
  
 private:
  // Optimization flags
  bool disable_query_cache_invalidate_ = false;  // Skip cache invalidate during import
  bool disable_column_tracking_ = false;         // Skip column tracking during import
  bool batch_tracking_enabled_ = false;          // Use batch tracking
  
  // Batch tracking buffer for column IDs
  static constexpr size_t kTrackBatchSize = 100000;
  std::vector<std::pair<uint64_t, uint16_t>> batch_buffer_{kTrackBatchSize};
  std::atomic<size_t> batch_buffer_index_{0};
  
  // ========== Size-Tiered Compaction 引擎 ==========
  std::unique_ptr<SizeTieredCompactionEngine> compaction_engine_;
  SizeTieredConfig compaction_config_;
  
  // 后台自动触发 Compaction 线程
  std::atomic<bool> auto_compaction_enabled_{true};
  std::thread* auto_compaction_thread_ = nullptr;
  void AutoCompactionThread();
  
  // 迁移现有的 SST 文件到新的 Compaction 引擎
  void MigrateExistingSstFiles();
};

}  // namespace cedar

#endif  // FERN_LSM_ENGINE_H_
