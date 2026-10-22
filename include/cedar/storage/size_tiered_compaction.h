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

// =============================================================================
// Size-Tiered Compaction Engine for Zone-Columnar SST
// =============================================================================
// 基于纯容量驱动的 LSM-Tree 合并策略：
// - Level 0: 64MB (4个 16MB MemTable Flush)
// - Level N: Size(Level N-1) × 4
// - 整层合并（写放大最低）
// - 与 Zone-Columnar 格式深度集成
// =============================================================================

#ifndef FERN_SIZE_TIERED_COMPACTION_H_
#define FERN_SIZE_TIERED_COMPACTION_H_

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <queue>
#include <condition_variable>
#include <unordered_map>
#include <unordered_set>

#include "cedar/core/status.h"
#include "cedar/core/slice.h"
#include "cedar/core/env.h"
#include "cedar/types/cedar_types.h"
#include "cedar/types/descriptor.h"
#include "cedar/frond/zone_columnar_format.h"

namespace cedar {

// =============================================================================
// 配置常量
// =============================================================================
struct SizeTieredConfig {
  // L0 配置
  uint64_t l0_max_size = 64 * 1024 * 1024;      // 64MB
  uint64_t l0_file_size = 16 * 1024 * 1024;     // 16MB 单个文件
  size_t l0_max_files = 4;                       // L0 最大文件数
  
  // 层级增长倍数
  double size_ratio = 4.0;                       // 每层容量是上一层的 4 倍
  
  // 最大层级
  int max_levels = 7;                            // L0-L6
  
  // 触发条件
  double level_size_trigger_ratio = 1.2;         // 当前大小/阈值 > 1.2 触发
  double disk_usage_threshold = 0.9;             // 磁盘使用 > 90% 紧急合并
  
  // 合并行为
  size_t max_merge_width = 32;                   // 单次合并最多文件数
  uint64_t blob_rewrite_threshold = 1024 * 1024; // Blob > 1MB 重写，否则仅拷贝指针
  
  // Tombstone 清理
  int tombstone_cleanup_level = 3;               // L3+ 才允许清理 tombstone
  double tombstone_cleanup_disk_ratio = 0.7;     // 磁盘 > 70% 时清理
  
  // 后台线程
  int compaction_threads = 2;                    // 合并线程数
  bool enable_background_compaction = true;      // 是否启用后台合并

  // 调度（Task 4.3）：有界 pending + 失败指数退避
  size_t max_pending_tasks = 8;                  // pending_tasks_ 上限
  uint64_t failure_backoff_base_ms = 50;         // 首次失败退避
  uint64_t failure_backoff_max_ms = 5000;        // 退避上限
};

// =============================================================================
// Zone-Columnar SST 文件元数据
// =============================================================================
struct ZoneSstMeta {
  uint64_t file_number = 0;
  uint64_t file_size = 0;
  uint64_t num_entries = 0;
  int level = 0;
  
  // Key 范围（用于重叠检测）
  uint64_t min_entity_id = 0;
  uint64_t max_entity_id = 0;
  uint64_t min_timestamp = 0;
  uint64_t max_timestamp = 0;
  
  // Column/Type 信息
  uint16_t column_id = 0;
  uint8_t entity_type = 0;  // 0=Vertex, 1=EdgeOut, 2=EdgeIn
  
  // Blob 文件关联
  uint64_t blob_file_number = 0;
  uint64_t blob_file_size = 0;
  
  // 统计信息
  uint64_t creation_time = 0;
  uint64_t total_uncompressed_size = 0;
  double compression_ratio = 0.0;
  
  // 文件路径
  std::string path;
  std::string blob_path;
  
  // Zone 统计（用于选择编码策略）
  struct ZoneStats {
    uint64_t uncompressed_size = 0;
    uint64_t compressed_size = 0;
    uint8_t encoding_type = 0;
  };
  std::array<ZoneStats, 5> zone_stats;
  
  std::string DebugString() const;
};

// =============================================================================
// 层级状态
// =============================================================================
struct LevelState {
  int level = 0;
  std::vector<ZoneSstMeta> files;
  
  // 计算总大小
  uint64_t total_size() const {
    uint64_t size = 0;
    for (const auto& f : files) {
      size += f.file_size;
    }
    return size;
  }
  
  // 计算文件数
  size_t file_count() const { return files.size(); }
  
  // 计算该层容量阈值
  uint64_t capacity_threshold(const SizeTieredConfig& config) const {
    if (level == 0) return config.l0_max_size;
    double multiplier = std::pow(config.size_ratio, level);
    return static_cast<uint64_t>(config.l0_max_size * multiplier);
  }
  
  // 是否需要合并
  bool needs_compaction(const SizeTieredConfig& config) const {
    uint64_t threshold = capacity_threshold(config);
    return total_size() > threshold * config.level_size_trigger_ratio;
  }
  
  // 找出与给定 Key 范围重叠的文件
  std::vector<ZoneSstMeta> FindOverlapping(
      uint64_t min_entity, uint64_t max_entity,
      uint64_t min_ts, uint64_t max_ts) const;

  // 找出与给定 Key 范围重叠、且属于同一 (entity_type, column_id) 的文件。
  // 审计 9.1：一个压缩任务只能合并同组文件（输出文件头只有一组列/类型），
  // 因此下层重叠文件的选取必须同时按列/类型过滤。
  std::vector<ZoneSstMeta> FindOverlapping(
      uint64_t min_entity, uint64_t max_entity,
      uint64_t min_ts, uint64_t max_ts,
      uint16_t column_id, uint8_t entity_type) const;
};

// =============================================================================
// 多路归并结果（输出文件元数据）
// =============================================================================
// 一次归并（可能切成多个输出分段）的整体统计；分段自己的统计由调用方维护。
struct MergeZoneResult {
  uint64_t num_rows = 0;
  uint64_t min_entity_id = UINT64_MAX;
  uint64_t max_entity_id = 0;
  uint64_t min_timestamp = UINT64_MAX;
  uint64_t max_timestamp = 0;
};

// 归并输出汇：每写一行调用一次；返回非 OK 立即中止归并（调用方清理输出）。
// Task 4.1：MergeZones 改为 K 路堆，逐行 push 给 sink；Task 4.2 的分段输出
// 由 DoZoneCompaction 的 sink 负责切文件。
using MergeRowSink = std::function<Status(const CedarKey&, const Descriptor&)>;

// =============================================================================
// 单一文件号分配器（Task 1.7 / 审计 9.2 / 设计 D2）
// =============================================================================
// 全库只有一个计数器（LsmEngine::next_file_number_）：flush 直接用它取号，
// 压缩引擎通过 BindFileNumberAllocator 绑定同一个原子计数器，所以任何写文件
// 路径都不会再拿到重复文件号。取号水位（Peek）写入 MANIFEST，重开时只前进
// 不后退。未绑定外部计数器时使用自有计数器（独立使用/测试场景）。
class FileNumberAllocator {
 public:
  explicit FileNumberAllocator(uint64_t next_file_number = 1)
      : owned_(next_file_number) {}

  // 共享外部计数器（LsmEngine 持有的那个）。指针生命周期由调用方保证；
  // 传 nullptr 恢复为内部计数器。
  void Bind(std::atomic<uint64_t>* counter) { counter_ = counter; }
  bool bound() const { return counter_ != nullptr; }

  // 唯一取号点（并发唯一）。
  uint64_t Allocate() {
    return counter_->fetch_add(1, std::memory_order_relaxed);
  }

  // 下一个将分配的值（MANIFEST 持久化的水位）。
  uint64_t Peek() const { return counter_->load(std::memory_order_relaxed); }

  // 从磁盘状态恢复水位：只前进不后退（CAS 循环，可与并发取号交错）。
  void EnsureAtLeast(uint64_t next_file_number) {
    uint64_t current = counter_->load(std::memory_order_relaxed);
    while (current < next_file_number &&
           !counter_->compare_exchange_weak(current, next_file_number,
                                            std::memory_order_relaxed,
                                            std::memory_order_relaxed)) {
    }
  }

 private:
  mutable std::atomic<uint64_t> owned_;
  std::atomic<uint64_t>* counter_ = &owned_;
};

// =============================================================================
// Compaction 任务
// =============================================================================
struct CompactionTask {
  int input_level = 0;
  int output_level = 0;
  std::vector<ZoneSstMeta> input_files;
  std::vector<ZoneSstMeta> overlapping_files;  // 下层重叠文件
  
  // 任务优先级（数值越小优先级越高）
  int priority = 0;
  
  // 预估输出大小
  uint64_t estimated_output_size = 0;
  
  // 是否是紧急合并
  bool is_emergency = false;
  
  std::string DebugString() const;
};

// =============================================================================
// Blob 引用计数管理器（用于延迟拷贝策略）
// =============================================================================
class BlobReferenceManager {
 public:
  BlobReferenceManager();
  ~BlobReferenceManager();
  
  // 禁止拷贝
  BlobReferenceManager(const BlobReferenceManager&) = delete;
  BlobReferenceManager& operator=(const BlobReferenceManager&) = delete;
  
  // 注册 Blob 文件
  void RegisterBlobFile(uint64_t blob_file_number, uint64_t file_size);
  
  // 增加引用计数（当新 SST 引用此 Blob 文件时）
  void AddReference(uint64_t blob_file_number, uint64_t sst_file_number);
  
  // 减少引用计数（当 SST 文件被删除时）
  void RemoveReference(uint64_t blob_file_number, uint64_t sst_file_number);
  
  // 获取引用计数
  int32_t GetReferenceCount(uint64_t blob_file_number) const;
  
  // 检查是否可以删除 Blob 文件
  bool CanDeleteBlob(uint64_t blob_file_number) const;
  
  // 获取可回收的空间
  uint64_t GetWastedSpace() const;
  
  // 获取需要 GC 的 Blob 文件列表
  std::vector<uint64_t> GetBlobFilesForGC(uint64_t min_wasted_bytes) const;
  
  // 序列化/反序列化（用于持久化）
  Status SaveToFile(const std::string& path);
  Status LoadFromFile(const std::string& path);
  
  // 清理不存在的引用（启动时调用）
  void CleanupReferences(const std::unordered_set<uint64_t>& existing_sst_files);

 private:
  struct BlobInfo {
    uint64_t file_size = 0;
    std::unordered_set<uint64_t> referencing_ssts;
  };
  
  mutable std::mutex mutex_;
  std::unordered_map<uint64_t, BlobInfo> blob_infos_;
};

// =============================================================================
// Compaction 统计
// =============================================================================
struct CompactionStats {
  std::atomic<uint64_t> total_compactions{0};
  std::atomic<uint64_t> total_input_files{0};
  std::atomic<uint64_t> total_output_files{0};
  std::atomic<uint64_t> total_input_bytes{0};
  std::atomic<uint64_t> total_output_bytes{0};
  std::atomic<uint64_t> total_duration_ms{0};
  
  // 各层级统计
  std::array<std::atomic<uint64_t>, 7> level_compactions{};
  std::array<std::atomic<uint64_t>, 7> level_input_bytes{};
  std::array<std::atomic<uint64_t>, 7> level_output_bytes{};
  
  // 当前正在运行的合并
  std::atomic<int> active_compactions{0};
  std::atomic<uint64_t> pending_compaction_bytes{0};
  
  void RecordCompaction(const CompactionTask& task, uint64_t duration_ms,
                        uint64_t output_files = 1);
  std::string Report() const;
};

// =============================================================================
// Size-Tiered Compaction 引擎
// =============================================================================
class SizeTieredCompactionEngine {
 public:
  SizeTieredCompactionEngine(const std::string& db_path,
                              const SizeTieredConfig& config,
                              Env* env);
  ~SizeTieredCompactionEngine();
  
  // 禁止拷贝
  SizeTieredCompactionEngine(const SizeTieredCompactionEngine&) = delete;
  SizeTieredCompactionEngine& operator=(const SizeTieredCompactionEngine&) = delete;
  
  // 初始化
  Status Open();
  Status Close();
  
  // ============= 文件管理 =============
  
  // 添加新 SST 文件（MemTable Flush 后调用）
  Status AddSSTFile(const ZoneSstMeta& meta);
  
  // 删除 SST 文件
  Status RemoveSSTFile(uint64_t file_number);
  
  // 获取某层的文件列表
  std::vector<ZoneSstMeta> GetLevelFiles(int level) const;
  
  // 获取所有层级状态
  std::vector<LevelState> GetAllLevels() const;
  
  // ============= Compaction 调度 =============
  
  // 检查是否需要合并（后台线程调用）
  bool NeedsCompaction() const;
  
  // 选择下一个合并任务
  std::optional<CompactionTask> PickNextCompaction();
  
  // 执行合并（同步）
  Status ExecuteCompaction(const CompactionTask& task);
  
  // 触发后台合并（异步）
  void ScheduleCompaction();
  
  // 等待所有合并完成
  void WaitForCompactions();
  
  // 手动触发全量合并
  Status CompactAll();
  
  // ============= 查询支持 =============
  
  // 获取覆盖特定 Key 范围的所有 SST 文件（跨所有层级）
  std::vector<ZoneSstMeta> GetOverlappingFiles(
      uint64_t min_entity, uint64_t max_entity,
      uint64_t min_ts, uint64_t max_ts,
      uint16_t column_id, uint8_t entity_type) const;
  
  // 获取某 Entity 的所有相关文件（用于点查优化）
  std::vector<ZoneSstMeta> GetFilesForEntity(
      uint64_t entity_id, uint16_t column_id, uint8_t entity_type) const;
  
  // ============= Blob 管理 =============
  
  // 获取 Blob 引用管理器
  BlobReferenceManager* GetBlobManager() { return &blob_manager_; }
  
  // 触发 Blob GC
  Status TriggerBlobGC();
  
  // ============= 统计与监控 =============
  
  void GetStats(CompactionStats* out_stats) const {
    out_stats->total_compactions = stats_.total_compactions.load();
    out_stats->total_input_files = stats_.total_input_files.load();
    out_stats->total_output_files = stats_.total_output_files.load();
    out_stats->total_input_bytes = stats_.total_input_bytes.load();
    out_stats->total_output_bytes = stats_.total_output_bytes.load();
    out_stats->total_duration_ms = stats_.total_duration_ms.load();
    out_stats->active_compactions = stats_.active_compactions.load();
    out_stats->pending_compaction_bytes = stats_.pending_compaction_bytes.load();
    for (int i = 0; i < 7; ++i) {
      out_stats->level_compactions[i] = stats_.level_compactions[i].load();
      out_stats->level_input_bytes[i] = stats_.level_input_bytes[i].load();
      out_stats->level_output_bytes[i] = stats_.level_output_bytes[i].load();
    }
  }
  
  // 获取数据库总大小
  uint64_t GetTotalSize() const;
  
  // 获取各层大小
  std::vector<uint64_t> GetLevelSizes() const;
  
  // 打印层级状态
  std::string LevelSummary() const;
  
  // ============= 并行 Compaction 支持 =============
  
  // 友元类访问私有成员
  friend class ThreadPoolCompactionExecutor;
  friend class ParallelCompactionEngine;
  
  // 获取下一个文件编号（唯一取号点）
  uint64_t NewFileNumber() { return file_number_allocator_.Allocate(); }

  // ============= 文件号分配（Task 1.7 / 审计 9.2） =============
  //
  // 共享 LsmEngine 持有的唯一分配器：绑定后 flush 与 compaction 从同一处取号，
  // 压缩输出号自动回写到 LsmEngine 的计数器。必须在 Open() 之前调用
  //（LoadManifest 会用 MANIFEST 水位推进分配器）。
  void BindFileNumberAllocator(std::atomic<uint64_t>* counter) {
    file_number_allocator_.Bind(counter);
  }

  // 下一个将分配的文件号（MANIFEST 持久化的水位）。
  uint64_t PeekNextFileNumber() const { return file_number_allocator_.Peek(); }

  // 该文件号是否已登记在任意层级（重开迁移去重，审计 9.3）。
  bool HasFile(uint64_t file_number) const;

  // ============= 调度诊断（Task 4.3，只增 API） =============
  //
  // 单一调度器：PickNextCompaction 返回任务时按文件集原子登记 reserved_files_，
  // ExecuteCompaction 结束/失败/Close 时释放；同一文件不会被两个任务同时持有。
  size_t PendingTaskCount() const;
  std::vector<CompactionTask> PendingTasksSnapshot() const;
  size_t ReservedFileCount() const;

  // 连续失败次数 / 剩余退避时间（微秒，0 表示可以立即调度）。
  uint32_t ConsecutiveCompactionFailures() const;
  uint64_t BackoffRemainingMicros() const;
  // 运维/测试钩子：清空退避（下一次 ScheduleCompaction 立即生效）。
  void ResetCompactionBackoff();

 private:
  // ============= 内部方法 =============
  
  // 后台合并线程
  void BackgroundCompactionThread();
  
  // 执行具体的 Zone-Columnar 合并（Task 4.2：输出按 l0_file_size 切成多段）
  // output_files 非空时回填本次实际写出的 SST 文件数（统计用）。
  Status DoZoneCompaction(const CompactionTask& task,
                          uint64_t* output_files = nullptr);
  
  // 多路堆归并（核心算法，Task 4.1）
  //
  // inputs 必须同属 (entity_type, column_id)；调用方传入 inputs ∪ overlaps
  //（审计 3.1：重叠文件的数据必须参与合并，不能直接删除）。
  // 采用 std::priority_queue（O(log K)/行）而不是逐行扫描 K 个游标（O(K)/行）。
  // 同 key 取舍规则（代码内固定，见 MergeHeapGreater）：
  //   1) 同 user key 且同 timestamp：只保留 sequence 更大的版本；
  //   2) 完整 key 完全相同：只保留输入文件号更大的那份；
  // 输出行按 CedarKey::Compare 递增（每个 (user key, ts) 只留一行，顺序与唯一
  // 化后的 key 序一致）。
  // 逐行交给 sink 写盘；整体真实行数/范围回填到 result。
  Status MergeZones(const std::vector<ZoneSstMeta>& inputs,
                    const MergeRowSink& sink,
                    MergeZoneResult* result);
  
  // 处理 Tombstone 清理
  bool ShouldDropTombstone(const CedarKey& key, int output_level);
  
  // 处理 Blob 引用（选择重写或仅拷贝指针）
  Descriptor HandleBlobReference(const Descriptor& old_desc,
                                  ZoneColumnarSstBuilder* builder,
                                  const ZoneSstMeta& source_file);
  
  // 原子操作：添加文件到某层
  void AddFileToLevel(const ZoneSstMeta& meta, int level);
  
  // 持久化层级状态（MANIFEST）
  Status SaveManifest();
  // 调用方已持有 levels_mutex_（压缩提交: 先写 MANIFEST 再删输入文件）
  Status SaveManifestLocked();
  Status LoadManifest();
  // fsync 目录项：rename 之后调用，让 MANIFEST 的替换在掉电后也可见
  //（复用 PosixWritableFile 对 "MANIFEST*" 文件 Sync 时顺带 fsync 目录的行为）
  Status SyncParentDirectory();

  // Task 4.3：失败指数退避（失败不紧循环）
  void RecordCompactionFailure();
  void RecordCompactionSuccess();

  // 只从层级元数据里移除（不删物理文件）；物理删除见 DeleteSstFile。
  // 调用方必须持有 levels_mutex_。
  void RemoveFileMetaFromLevelLocked(uint64_t file_number, int level);
  // 删除物理 SST/Blob 文件（必须在 MANIFEST 提交之后调用）
  void DeleteSstFile(const ZoneSstMeta& meta);
  
  // 查找文件
  std::optional<ZoneSstMeta> FindFile(uint64_t file_number) const;
  
  // 生成 SST 文件路径
  std::string GetSstPath(uint64_t file_number) const;
  std::string GetBlobPath(uint64_t file_number) const;
  std::string GetManifestPath() const;
  
  // ============= 成员变量 =============
  
  std::string db_path_;
  SizeTieredConfig config_;
  Env* env_;
  
  // 层级状态（L0-L6）
  mutable std::mutex levels_mutex_;
  std::vector<LevelState> levels_;  // index = level
  
  // Blob 引用管理
  BlobReferenceManager blob_manager_;
  
  // 文件号分配器（默认自有计数器）。BindFileNumberAllocator 后它与
  // LsmEngine::next_file_number_ 共享同一个原子计数器（单一分配器，审计 9.2）。
  FileNumberAllocator file_number_allocator_;
  
  // 后台线程控制
  std::atomic<bool> shutdown_{false};
  std::atomic<bool> opened_{false};
  std::vector<std::thread> compaction_threads_;
  
  // 任务队列（mutable：pending 数量/快照是 const 诊断接口）
  mutable std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::queue<CompactionTask> pending_tasks_;
  
  // 统计
  mutable CompactionStats stats_;
  
  // 当前被任务保留的文件（reserved_files_，防止并发合并同一文件）。
  // PickNextCompaction 返回任务时登记，ExecuteCompaction 结束/失败释放。
  mutable std::mutex compacting_mutex_;
  std::unordered_set<uint64_t> compacting_files_;

  // 调度退避（Task 4.3）：连续失败次数与下次允许调度的时刻（Env::NowMicros）
  std::atomic<uint32_t> consecutive_failures_{0};
  std::atomic<uint64_t> backoff_until_us_{0};
};

// =============================================================================
// 工具函数
// =============================================================================

// 检查两个文件 Key 范围是否重叠
bool FileRangeOverlaps(const ZoneSstMeta& a, const ZoneSstMeta& b);

// 检查 Key 是否在文件范围内
bool KeyInFileRange(const ZoneSstMeta& file, uint64_t entity_id, uint64_t timestamp);

// 计算合并优先级（数值越小优先级越高）
int CalculateCompactionPriority(const LevelState& level, const SizeTieredConfig& config);

}  // namespace cedar

#endif  // FERN_SIZE_TIERED_COMPACTION_H_
