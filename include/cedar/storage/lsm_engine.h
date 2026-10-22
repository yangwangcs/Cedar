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
#include "cedar/storage/wal_replay.h"
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

  // Get at specific time（旧签名：Vertex 行为，不区分 target）
  std::optional<Descriptor> GetAtTime(uint64_t entity_id,
                                       EntityType entity_type,
                                       uint16_t column_id,
                                       Timestamp timestamp);

  // Get at specific time（Task 1.5 / 审计 2.4/8.2：按完整 user key 含 target 定位）
  std::optional<Descriptor> GetAtTime(uint64_t entity_id,
                                       EntityType entity_type,
                                       uint16_t column_id,
                                       uint64_t target_id,
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
  
  // 获取 MemTable（裸指针，保留旧签名；生命周期由 LsmEngine 负责）
  VSLMemTable* GetMemTable() const { return GetMemTableShared().get(); }
  
  // 获取 Batch 执行器
  BatchExecutor* GetBatchExecutor() const { return batch_executor_.get(); }
  
  // 创建新的事务
  std::unique_ptr<OCCTransaction> BeginTransaction(
      const TransactionOptions& options = TransactionOptions());
  
  // 同步 WAL
  Status SyncWAL();

  // ========== 持久化 / 事务（Task 1.2，审计 3.3） ==========
  //
  // WAL-first 的统一入口（Task 1.2 / t9）：append 后由 WalWriter 组提交一次
  // sync；返回 OK 表示这条 record 已持久化（失败返回真实错误）。
  // 事务（写集 + 提交围栏）与直接写（单条 op）共用这个入口，因此共享同一
  // WAL 顺序与"已提交"语义。
  Status WriteWALBatch(const WalBatch& batch);

  // 只写当前 memtable（不写 WAL）：内部持有 memtable 的 shared_ptr 并在共享锁
  // 内完成写入，保证并发 flush 换表时既不丢写也不会写到已销毁的表（审计 3.5）。
  // 事务提交路径在 WAL 成功之后调用它（避免重复写 WAL）。
  Status PutPinned(const CedarKey& key, const Descriptor& descriptor,
                   Timestamp txn_version);

  // 组提交窗口（微秒）：0 = 同步模式；默认 1000。必须在 Open() 之前设置。
  // 直接写与事务提交都走同一窗口（同一次 append+持久化动作可被共享）。
  void SetWALGroupCommitWindow(uint32_t micros) {
    wal_group_commit_window_us_ = micros;
  }
  uint32_t GetWALGroupCommitWindow() const { return wal_group_commit_window_us_; }

  // 直接写（LsmEngine::Put/Delete）是否要求 fsync。
  // 默认 false：append + flush 到 OS，进程崩溃/被 kill 后可恢复（崩溃恢复测试的语义），
  // 不付 macOS F_FULLFSYNC（本机约 2.7ms/次）的代价。
  // 设为 true 则直接写与事务提交一样每次批次 fsync（掉电可恢复），吞吐下降。
  // 事务提交（OCCTransaction::Commit）始终 fsync，不受此开关影响。
  void SetDirectWriteSync(bool enabled) { direct_write_sync_ = enabled; }
  bool GetDirectWriteSync() const { return direct_write_sync_; }

  // ========== WAL checkpoint / 截断（t10） ==========
  //
  // 水位定义：wal_checkpoint_watermark_ = W 表示段号 < W 的 WAL 记录全部已包含在
  // 已 fsync 的 SST 中，重放只读段号 >= W 的段。flush 成功后严格按以下顺序推进：
  //   1) SST 数据 fsync（FlushEntityGroup，恢复 t7 注释要求的"截断前落盘"）
  //   2) 持久化 checkpoint（MANIFEST-wal-checkpoint；Sync 顺带 fsync 父目录）
  //   3) 删除段号 < W 的 WAL 段（失败只影响空间）
  // 2 轮滞后：WAL-first 下"append 完成、memtable 写入尚未执行"的跨界窗口存在，
  // 因此段 S_i 的记录要到第 i+1 轮 flush 才全部落在 SST，水位才允许覆盖 S_i。
  // 关闭截断（false）会恢复 t7 的 flush 快路径（SST 只 Flush 不 fsync），
  // 代价是 WAL 不再回收（旧行为）。
  void SetWALTruncationEnabled(bool enabled) { wal_truncation_enabled_ = enabled; }
  bool GetWALTruncationEnabled() const { return wal_truncation_enabled_; }
  // 当前水位（段号 < 它视为已落 SST）。诊断/测试用。
  uint64_t GetWALCheckpointWatermark() const { return wal_checkpoint_watermark_; }

  // GetAtTime 的公共实现；target_id 为空表示旧签名（不区分 target）。
  std::optional<Descriptor> GetAtTimeImpl(uint64_t entity_id,
                                          EntityType entity_type,
                                          uint16_t column_id,
                                          const std::optional<uint64_t>& target_id,
                                          Timestamp timestamp);

  // 当前 memtable / immutable memtable 的强引用（事务可在操作期间临时持有）
  std::shared_ptr<VSLMemTable> GetMemTableShared() const;
  std::shared_ptr<VSLMemTable> GetImmutableMemTableShared() const;

  // 版本链查询：优先当前 memtable，为空时回退 immutable memtable。
  // 事务不再直接解引用 Begin 时捕获的裸指针（审计 3.5）。
  std::vector<MemTableEntry> GetVersionChain(uint64_t entity_id,
                                             EntityType entity_type,
                                             uint16_t column_id);

  // 指定 target 的版本链（Task 1.5；边必须用它，否则会读到别的 dst 的版本）
  std::vector<MemTableEntry> GetVersionChain(uint64_t entity_id,
                                             EntityType entity_type,
                                             uint16_t column_id,
                                             uint64_t target_id);

  // Open 期间 WAL 重放的统计（Task 1.2）
  WalReplayStats GetWalReplayStats() const;
  
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
  // entries 是调用方的临时向量：t7 的"重复排序消除"要求原地排序（先 is_sorted）。
  Status FlushEntityGroup(uint8_t entity_type, uint16_t column_id,
                          std::vector<std::pair<CedarKey, Descriptor>>& entries);

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
  
  // ========== Flush 生命周期（Task 1.3，审计 3.4/3.5） ==========
  //
  // 同一时刻只允许一个 flush：BeginFlushExclusive() 抢占 flush 槽位，
  // 失败方返回 "flush already in progress"（保持旧行为）。
  bool BeginFlushExclusive();
  void EndFlushExclusive(const Status& status);
  // 调用方已抢占 flush 槽位：flush table，成功后才释放它对应的 imm_。
  Status FlushClaimedTable(const std::shared_ptr<VSLMemTable>& table);
  // Put 之后按 ApproximateMemoryUsage 检查是否需要换表并 flush。
  void MaybeAutoFlush();
  // 在一把共享锁内同时取出 mem_/imm_ 的强引用（读路径使用）。
  void PinMemTables(std::shared_ptr<VSLMemTable>* mem,
                    std::shared_ptr<VSLMemTable>* imm) const;

  // ========== WAL checkpoint / 截断（t10） ==========
  // flush 成功后：轮转 WAL → 按 2 轮滞后计算可覆盖段 → 持久化水位 → 删段。
  Status AdvanceWALCheckpointAfterFlush();
  // Close：写者已停止、所有数据已 flush + fsync，可把水位推进到当前活动段。
  Status FinalizeWALCheckpointOnClose();
  Status PersistWALCheckpoint(uint64_t watermark_file_number,
                              uint64_t file_number_floor);
  void DeleteWALSegmentsBefore(uint64_t watermark_file_number);

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
  // shared_ptr：读/写在操作期间持有强引用，flush 换表后旧表不会被立即销毁
  //（审计 3.5）。换表在 mutex_ 独占锁下完成，写路径持共享锁，写到一半不会被换走。
  std::shared_ptr<VSLMemTable> mem_;
  std::shared_ptr<VSLMemTable> imm_;  // Immutable memtable being flushed
  mutable std::shared_mutex mutex_;  // 使用 shared_mutex 支持并发读
  std::atomic<bool> has_work_{false};

  // flush 槽位（Task 1.3）：同一时刻只允许一个 flush 修改 imm_/levels_
  std::mutex flush_mutex_;
  bool flush_in_progress_ = false;
  uint64_t flush_count_ = 0;
  Status last_flush_status_;

  // WAL 组提交窗口（微秒，0 = 同步）；InitWAL 用它构造 WalOptions
  uint32_t wal_group_commit_window_us_ = 1000;
  // 直接写是否要求 fsync（默认 false：flush 到 OS，进程崩溃可恢复）
  bool direct_write_sync_ = false;
  WalReplayStats wal_replay_stats_;

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

  // ============================================================================
  // Task 3.4：每 (entity_type, column_id) 的文件区间索引
  // ============================================================================
  // 文件按 min_entity_id 升序 + 前缀最大 max_entity_id：
  //   - 点查：upper_bound(min_entity <= e) 后向前线性回退，prefix_max < e 即停
  //   - 批量：区间 [min,max] 与文件 [min_entity,max_entity] 求交集
  // 索引是 compaction engine 元数据的副本，刷新点：Open/flush/compaction 完成。
  struct IndexedFile {
    uint64_t file_number = 0;
    uint64_t file_size = 0;
    uint64_t num_entries = 0;
    uint64_t min_entity_id = 0;
    uint64_t max_entity_id = 0;
    uint64_t min_timestamp = 0;
    uint64_t max_timestamp = 0;
    uint16_t column_id = 0;
    uint8_t entity_type = 0;
    int level = 0;
    std::string path;
  };

  struct FileGroupIndex {
    std::vector<IndexedFile> files;           // 按 min_entity_id 升序
    std::vector<uint64_t> prefix_max_entity;  // 前缀最大 max_entity_id
  };

  void RebuildFileIndex();
  void AddFileToIndex(const IndexedFile& file);
  void EnsureFileIndexFresh() const;
  // 覆盖 entity 的文件（O(log F + k)）；按值返回避免索引并发更新时指针悬空
  std::vector<IndexedFile> LookupFiles(uint64_t entity_id, uint8_t entity_type,
                                       uint16_t column_id) const;
  // 与 entity 区间 [min_entity,max_entity] 有交集的文件（批量查询用）
  std::vector<IndexedFile> LookupFilesInRange(uint64_t min_entity,
                                              uint64_t max_entity,
                                              uint8_t entity_type,
                                              uint16_t column_id) const;

  // Task 3.4：所有读路径统一的 reader 入口（SstReaderCache，key=file_number）
  std::shared_ptr<ZoneColumnarSstReader> GetCachedReader(uint64_t file_number,
                                                         const std::string& path) const;

  mutable std::shared_mutex file_index_mutex_;
  std::unordered_map<uint64_t, FileGroupIndex> file_interval_index_;  // key=(type<<16)|column
  mutable uint64_t file_index_compaction_generation_ = 0;
  mutable bool file_index_valid_ = false;
  
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
  
  // WAL checkpoint / 截断（t10）
  bool wal_truncation_enabled_ = true;
  uint64_t wal_checkpoint_watermark_ = 0;   // 段号 < 它视为已落 SST
  uint64_t wal_file_number_floor_ = 0;      // WAL 段号下限（单调，防回绕）
  std::deque<uint64_t> wal_seal_history_;   // 最近轮转封印的段号（2 轮滞后）
  std::mutex wal_checkpoint_mutex_;
  std::string wal_checkpoint_path_;

  // 迁移现有的 SST 文件到新的 Compaction 引擎
  void MigrateExistingSstFiles();
};

}  // namespace cedar

#endif  // FERN_LSM_ENGINE_H_
