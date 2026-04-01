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
  
  void RecordCompaction(const CompactionTask& task, uint64_t duration_ms);
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
  
  // 获取下一个文件编号
  uint64_t NewFileNumber() { return next_file_number_.fetch_add(1); }

 private:
  // ============= 内部方法 =============
  
  // 后台合并线程
  void BackgroundCompactionThread();
  
  // 执行具体的 Zone-Columnar 合并
  Status DoZoneCompaction(const CompactionTask& task);
  
  // 多路归并（核心算法）
  Status MergeZones(const std::vector<ZoneSstMeta>& inputs,
                    ZoneColumnarSstBuilder* output);
  
  // 处理 Tombstone 清理
  bool ShouldDropTombstone(const CedarKey& key, int output_level);
  
  // 处理 Blob 引用（选择重写或仅拷贝指针）
  Descriptor HandleBlobReference(const Descriptor& old_desc,
                                  ZoneColumnarSstBuilder* builder,
                                  const ZoneSstMeta& source_file);
  
  // 原子操作：添加文件到某层
  void AddFileToLevel(const ZoneSstMeta& meta, int level);
  
  // 原子操作：从某层移除文件
  void RemoveFileFromLevel(uint64_t file_number, int level);
  
  // 持久化层级状态（MANIFEST）
  Status SaveManifest();
  Status LoadManifest();
  
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
  
  // 文件编号生成器
  std::atomic<uint64_t> next_file_number_{1};
  
  // 后台线程控制
  std::atomic<bool> shutdown_{false};
  std::atomic<bool> opened_{false};
  std::vector<std::thread> compaction_threads_;
  
  // 任务队列
  std::mutex queue_mutex_;
  std::condition_variable queue_cv_;
  std::queue<CompactionTask> pending_tasks_;
  
  // 统计
  mutable CompactionStats stats_;
  
  // 当前正在合并的文件（防止并发合并同一文件）
  mutable std::mutex compacting_mutex_;
  std::unordered_set<uint64_t> compacting_files_;
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
