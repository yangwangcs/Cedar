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

#include "cedar/storage/size_tiered_compaction.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <map>
#include <sstream>
#include <utility>

#include "cedar/core/crc32c.h"

namespace cedar {

// =============================================================================
// ZoneSstMeta 实现
// =============================================================================

std::string ZoneSstMeta::DebugString() const {
  std::ostringstream oss;
  oss << "ZoneSstMeta{file=" << file_number 
      << ", level=" << level
      << ", size=" << file_size
      << ", entries=" << num_entries
      << ", entity_range=[" << min_entity_id << ", " << max_entity_id << "]"
      << ", ts_range=[" << min_timestamp << ", " << max_timestamp << "]"
      << ", column=" << column_id << ", type=" << (int)entity_type << "}";
  return oss.str();
}

// =============================================================================
// LevelState 实现
// =============================================================================

std::vector<ZoneSstMeta> LevelState::FindOverlapping(
    uint64_t min_entity, uint64_t max_entity,
    uint64_t min_ts, uint64_t max_ts) const {
  std::vector<ZoneSstMeta> result;
  for (const auto& f : files) {
    // 检查 Entity ID 范围重叠
    bool entity_overlap = !(f.max_entity_id < min_entity || f.min_entity_id > max_entity);
    // 检查 Timestamp 范围重叠
    bool ts_overlap = !(f.max_timestamp < min_ts || f.min_timestamp > max_ts);
    
    if (entity_overlap && ts_overlap) {
      result.push_back(f);
    }
  }
  return result;
}

// 审计 9.1：压缩任务只能合并同一 (entity_type, column_id) 的文件，
// 因此下层重叠文件的选取也必须按列/类型过滤。
std::vector<ZoneSstMeta> LevelState::FindOverlapping(
    uint64_t min_entity, uint64_t max_entity,
    uint64_t min_ts, uint64_t max_ts,
    uint16_t column_id, uint8_t entity_type) const {
  std::vector<ZoneSstMeta> result;
  for (const auto& f : files) {
    if (f.column_id != column_id || f.entity_type != entity_type) {
      continue;
    }
    const bool entity_overlap = !(f.max_entity_id < min_entity ||
                                  f.min_entity_id > max_entity);
    const bool ts_overlap = !(f.max_timestamp < min_ts ||
                              f.min_timestamp > max_ts);
    if (entity_overlap && ts_overlap) {
      result.push_back(f);
    }
  }
  return result;
}

// =============================================================================
// CompactionTask 实现
// =============================================================================

std::string CompactionTask::DebugString() const {
  std::ostringstream oss;
  oss << "CompactionTask{L" << input_level << "->L" << output_level
      << ", inputs=" << input_files.size()
      << ", overlaps=" << overlapping_files.size()
      << ", priority=" << priority
      << ", est_output=" << estimated_output_size
      << ", emergency=" << (is_emergency ? "yes" : "no") << "}";
  return oss.str();
}

// =============================================================================
// BlobReferenceManager 实现
// =============================================================================

BlobReferenceManager::BlobReferenceManager() = default;
BlobReferenceManager::~BlobReferenceManager() = default;

void BlobReferenceManager::RegisterBlobFile(uint64_t blob_file_number, uint64_t file_size) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto& info = blob_infos_[blob_file_number];
  info.file_size = file_size;
}

void BlobReferenceManager::AddReference(uint64_t blob_file_number, uint64_t sst_file_number) {
  std::lock_guard<std::mutex> lock(mutex_);
  blob_infos_[blob_file_number].referencing_ssts.insert(sst_file_number);
}

void BlobReferenceManager::RemoveReference(uint64_t blob_file_number, uint64_t sst_file_number) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = blob_infos_.find(blob_file_number);
  if (it != blob_infos_.end()) {
    it->second.referencing_ssts.erase(sst_file_number);
  }
}

int32_t BlobReferenceManager::GetReferenceCount(uint64_t blob_file_number) const {
  std::lock_guard<std::mutex> lock(mutex_);
  auto it = blob_infos_.find(blob_file_number);
  if (it != blob_infos_.end()) {
    return static_cast<int32_t>(it->second.referencing_ssts.size());
  }
  return 0;
}

bool BlobReferenceManager::CanDeleteBlob(uint64_t blob_file_number) const {
  return GetReferenceCount(blob_file_number) == 0;
}

uint64_t BlobReferenceManager::GetWastedSpace() const {
  std::lock_guard<std::mutex> lock(mutex_);
  uint64_t wasted = 0;
  for (const auto& [blob_id, info] : blob_infos_) {
    if (info.referencing_ssts.empty()) {
      wasted += info.file_size;
    }
  }
  return wasted;
}

std::vector<uint64_t> BlobReferenceManager::GetBlobFilesForGC(uint64_t min_wasted_bytes) const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<uint64_t> result;
  uint64_t total_wasted = 0;
  
  for (const auto& [blob_id, info] : blob_infos_) {
    if (info.referencing_ssts.empty()) {
      result.push_back(blob_id);
      total_wasted += info.file_size;
      if (total_wasted >= min_wasted_bytes) {
        break;
      }
    }
  }
  return result;
}

Status BlobReferenceManager::SaveToFile(const std::string& path) {
  std::lock_guard<std::mutex> lock(mutex_);
  std::ofstream file(path, std::ios::binary);
  if (!file) {
    return Status::IOError("BlobReferenceManager", "Cannot open file for writing");
  }
  
  // 写入文件数量
  uint64_t count = blob_infos_.size();
  file.write(reinterpret_cast<const char*>(&count), sizeof(count));
  
  // 写入每个 Blob 文件的信息
  for (const auto& [blob_id, info] : blob_infos_) {
    file.write(reinterpret_cast<const char*>(&blob_id), sizeof(blob_id));
    file.write(reinterpret_cast<const char*>(&info.file_size), sizeof(info.file_size));
    
    uint64_t ref_count = info.referencing_ssts.size();
    file.write(reinterpret_cast<const char*>(&ref_count), sizeof(ref_count));
    
    for (uint64_t sst_id : info.referencing_ssts) {
      file.write(reinterpret_cast<const char*>(&sst_id), sizeof(sst_id));
    }
  }
  
  return file.good() ? Status::OK() : Status::IOError("BlobReferenceManager", "Write failed");
}

Status BlobReferenceManager::LoadFromFile(const std::string& path) {
  std::lock_guard<std::mutex> lock(mutex_);
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    return Status::NotFound("BlobReferenceManager", "File not found");
  }
  
  blob_infos_.clear();
  
  uint64_t count;
  file.read(reinterpret_cast<char*>(&count), sizeof(count));
  
  for (uint64_t i = 0; i < count; ++i) {
    uint64_t blob_id;
    file.read(reinterpret_cast<char*>(&blob_id), sizeof(blob_id));
    
    BlobInfo info;
    file.read(reinterpret_cast<char*>(&info.file_size), sizeof(info.file_size));
    
    uint64_t ref_count;
    file.read(reinterpret_cast<char*>(&ref_count), sizeof(ref_count));
    
    for (uint64_t j = 0; j < ref_count; ++j) {
      uint64_t sst_id;
      file.read(reinterpret_cast<char*>(&sst_id), sizeof(sst_id));
      info.referencing_ssts.insert(sst_id);
    }
    
    blob_infos_[blob_id] = std::move(info);
  }
  
  return file.good() ? Status::OK() : Status::IOError("BlobReferenceManager", "Read failed");
}

void BlobReferenceManager::CleanupReferences(const std::unordered_set<uint64_t>& existing_sst_files) {
  std::lock_guard<std::mutex> lock(mutex_);
  for (auto& [blob_id, info] : blob_infos_) {
    std::unordered_set<uint64_t> valid_refs;
    for (uint64_t sst_id : info.referencing_ssts) {
      if (existing_sst_files.count(sst_id) > 0) {
        valid_refs.insert(sst_id);
      }
    }
    info.referencing_ssts = std::move(valid_refs);
  }
}

// =============================================================================
// CompactionStats 实现
// =============================================================================

void CompactionStats::RecordCompaction(const CompactionTask& task,
                                      uint64_t duration_ms,
                                      uint64_t output_files) {
  total_compactions.fetch_add(1);
  total_input_files.fetch_add(task.input_files.size() + task.overlapping_files.size());
  total_output_files.fetch_add(output_files);
  total_duration_ms.fetch_add(duration_ms);
  
  if (task.input_level < 7) {
    level_compactions[task.input_level].fetch_add(1);
  }
}

std::string CompactionStats::Report() const {
  std::ostringstream oss;
  oss << "=== Compaction Statistics ===" << std::endl;
  oss << "Total compactions: " << total_compactions.load() << std::endl;
  oss << "Total input files: " << total_input_files.load() << std::endl;
  oss << "Total output files: " << total_output_files.load() << std::endl;
  oss << "Total input bytes: " << total_input_bytes.load() << std::endl;
  oss << "Total output bytes: " << total_output_bytes.load() << std::endl;
  oss << "Total duration: " << total_duration_ms.load() << "ms" << std::endl;
  
  if (total_compactions.load() > 0) {
    double avg_duration = static_cast<double>(total_duration_ms.load()) / total_compactions.load();
    oss << "Avg duration: " << std::fixed << std::setprecision(2) << avg_duration << "ms" << std::endl;
  }
  
  oss << "Per-level stats:" << std::endl;
  for (int i = 0; i < 7; ++i) {
    uint64_t count = level_compactions[i].load();
    if (count > 0) {
      oss << "  L" << i << ": " << count << " compactions";
      oss << " (input: " << level_input_bytes[i].load();
      oss << ", output: " << level_output_bytes[i].load() << ")" << std::endl;
    }
  }
  
  return oss.str();
}


// =============================================================================
// SizeTieredCompactionEngine 实现
// =============================================================================

SizeTieredCompactionEngine::SizeTieredCompactionEngine(
    const std::string& db_path,
    const SizeTieredConfig& config,
    Env* env)
    : db_path_(db_path),
      config_(config),
      env_(env) {
  // 初始化层级
  levels_.resize(config_.max_levels);
  for (int i = 0; i < config_.max_levels; ++i) {
    levels_[i].level = i;
  }
}

SizeTieredCompactionEngine::~SizeTieredCompactionEngine() {
  if (opened_.load()) {
    Close().IgnoreError();
  }
}

Status SizeTieredCompactionEngine::Open() {
  if (opened_.load()) {
    return Status::OK();
  }
  
  // 创建数据库目录
  if (!std::filesystem::exists(db_path_)) {
    std::filesystem::create_directories(db_path_);
  }
  
  // 加载 MANIFEST
  Status s = LoadManifest();
  if (!s.ok() && !s.IsNotFound()) {
    return s;
  }
  
  // 启动后台合并线程
  if (config_.enable_background_compaction) {
    for (int i = 0; i < config_.compaction_threads; ++i) {
      compaction_threads_.emplace_back(
          &SizeTieredCompactionEngine::BackgroundCompactionThread, this);
    }
  }
  
  opened_.store(true);
  return Status::OK();
}

Status SizeTieredCompactionEngine::Close() {
  if (!opened_.load()) {
    return Status::OK();
  }
  
  shutdown_.store(true);
  queue_cv_.notify_all();
  
  // 等待后台线程结束
  for (auto& t : compaction_threads_) {
    if (t.joinable()) {
      t.join();
    }
  }
  compaction_threads_.clear();

  // 丢弃未执行任务留下的文件保留与待执行任务（单一调度器：Close 后不再运行）
  {
    std::lock_guard<std::mutex> lock(compacting_mutex_);
    compacting_files_.clear();
  }
  {
    std::lock_guard<std::mutex> lock(queue_mutex_);
    std::queue<CompactionTask> empty;
    pending_tasks_.swap(empty);
  }

  // 保存 MANIFEST
  Status s = SaveManifest();
  
  opened_.store(false);
  return s;
}

// =============================================================================
// 文件管理
// =============================================================================

Status SizeTieredCompactionEngine::AddSSTFile(const ZoneSstMeta& meta) {
  bool needs_scheduling = false;
  {
    std::lock_guard<std::mutex> lock(levels_mutex_);
    
    if (meta.level < 0 || meta.level >= config_.max_levels) {
      return Status::InvalidArgument("SizeTieredCompactionEngine", "Invalid level");
    }
    
    // 检查文件是否已存在
    for (const auto& f : levels_[meta.level].files) {
      if (f.file_number == meta.file_number) {
        return Status::OK();  // 已存在，幂等
      }
    }
    
    levels_[meta.level].files.push_back(meta);
    
    // 更新文件号水位（只前进不后退）；绑定 LsmEngine 的分配器后同样生效
    file_number_allocator_.EnsureAtLeast(meta.file_number + 1);
    
    // 注册 Blob 引用
    if (meta.blob_file_number != 0) {
      blob_manager_.RegisterBlobFile(meta.blob_file_number, meta.blob_file_size);
      blob_manager_.AddReference(meta.blob_file_number, meta.file_number);
    }

    // 检查是否需要立即触发合并（同一把锁内读取，避免与压缩/刷新并发读写 levels_）
    needs_scheduling = meta.level == 0 && config_.enable_background_compaction &&
                       levels_[0].needs_compaction(config_);
  }

  // 在锁外调度：ScheduleCompaction 会拿 queue_mutex_ 再取 levels_mutex_
  if (needs_scheduling) {
    ScheduleCompaction();
  }
  
  return Status::OK();
}

Status SizeTieredCompactionEngine::RemoveSSTFile(uint64_t file_number) {
  std::string sst_path;
  std::string blob_path;
  
  {
    std::lock_guard<std::mutex> lock(levels_mutex_);
    
    for (int level = 0; level < config_.max_levels; ++level) {
      auto& files = levels_[level].files;
      for (auto it = files.begin(); it != files.end(); ++it) {
        if (it->file_number == file_number) {
          // 移除 Blob 引用
          if (it->blob_file_number != 0) {
            blob_manager_.RemoveReference(it->blob_file_number, file_number);
          }
          
          // 保存路径用于后续删除
          sst_path = it->path;
          blob_path = it->blob_path;
          
          // 从列表移除
          files.erase(it);
          break;
        }
      }
    }
  }
  
  // 在锁外删除物理文件
  if (!sst_path.empty()) {
    std::error_code ec;
    std::filesystem::remove(sst_path, ec);
    if (!blob_path.empty()) {
      std::filesystem::remove(blob_path, ec);
    }
    return Status::OK();
  }
  
  return Status::NotFound("SizeTieredCompactionEngine", "File not found");
}

std::vector<ZoneSstMeta> SizeTieredCompactionEngine::GetLevelFiles(int level) const {
  std::lock_guard<std::mutex> lock(levels_mutex_);
  if (level < 0 || level >= config_.max_levels) {
    return {};
  }
  return levels_[level].files;
}

std::vector<LevelState> SizeTieredCompactionEngine::GetAllLevels() const {
  std::lock_guard<std::mutex> lock(levels_mutex_);
  return levels_;
}

// =============================================================================
// Compaction 调度
// =============================================================================

bool SizeTieredCompactionEngine::NeedsCompaction() const {
  std::lock_guard<std::mutex> lock(levels_mutex_);
  
  for (int i = 0; i < config_.max_levels - 1; ++i) {
    if (levels_[i].needs_compaction(config_)) {
      return true;
    }
  }
  
  return false;
}

namespace {

// Size-Tiered 的 tier：按文件大小每翻一倍分一档（类似 LSM 的 size-tier）。
// Task 4.2：任务选择按 tier 进行，而不是"整层复制后按大小截断"。
uint64_t TierForFileSize(uint64_t file_size) {
  if (file_size == 0) {
    return 0;
  }
  uint64_t tier = 0;
  while (file_size > 1) {
    file_size >>= 1;
    ++tier;
  }
  return tier;
}

}  // namespace

std::optional<CompactionTask> SizeTieredCompactionEngine::PickNextCompaction() {
  std::lock_guard<std::mutex> lock(levels_mutex_);

  // 优先级：L0 > L1 > L2 > ... （从下往上）
  for (int level = 0; level < config_.max_levels - 1; ++level) {
    if (!levels_[level].needs_compaction(config_)) {
      continue;
    }

    // 审计 9.1：按 (entity_type, column_id) 分组。输出 SST 的 header 只保存
    // 一组列/类型，把不同组的文件混进同一个任务会改写/隐藏其它列的数据。
    // 分组只保存指针，不在 Pick 路径上复制整层元数据。
    std::map<std::pair<uint8_t, uint16_t>, std::vector<const ZoneSstMeta*>> groups;
    for (const auto& meta : levels_[level].files) {
      groups[{meta.entity_type, meta.column_id}].push_back(&meta);
    }

    for (auto& [group_key, group_files] : groups) {
      const uint8_t entity_type = group_key.first;
      const uint16_t column_id = group_key.second;

      // Task 4.2：按文件大小分 tier。选最小的、至少 2 个文件的 tier（相似大小
      // 一起合并，写放大最低）；每个 tier 只有 1 个文件时退化为选最小文件
      //（层容量溢出时必须把文件往下搬）。
      std::map<uint64_t, std::vector<const ZoneSstMeta*>> tiers;
      for (const ZoneSstMeta* f : group_files) {
        tiers[TierForFileSize(f->file_size)].push_back(f);
      }
      std::vector<const ZoneSstMeta*> selected;
      for (auto& [tier, files] : tiers) {  // tier 升序：优先合并小文件
        if (files.size() >= 2) {
          selected = files;
          break;
        }
      }
      if (selected.empty()) {
        selected = tiers.begin()->second;  // 单文件 tier：取最小的那个
      }

      // 同 tier 内按文件号升序（确定性），并按 max_merge_width 限制宽度
      std::sort(selected.begin(), selected.end(),
                [](const ZoneSstMeta* a, const ZoneSstMeta* b) {
                  return a->file_number < b->file_number;
                });
      if (selected.size() > config_.max_merge_width) {
        selected.resize(config_.max_merge_width);
      }

      CompactionTask task;
      task.input_level = level;
      task.output_level = level + 1;
      task.input_files.reserve(selected.size());
      uint64_t min_entity = UINT64_MAX, max_entity = 0;
      uint64_t min_ts = UINT64_MAX, max_ts = 0;
      for (const ZoneSstMeta* f : selected) {
        task.input_files.push_back(*f);
        min_entity = std::min(min_entity, f->min_entity_id);
        max_entity = std::max(max_entity, f->max_entity_id);
        min_ts = std::min(min_ts, f->min_timestamp);
        max_ts = std::max(max_ts, f->max_timestamp);
      }

      // 下层重叠文件：同列/同类型 + 范围重叠（审计 9.1）。
      // 它们的数据必须参与合并（审计 3.1），任务里一并带上。
      if (level + 1 < config_.max_levels) {
        task.overlapping_files = levels_[level + 1].FindOverlapping(
            min_entity, max_entity, min_ts, max_ts, column_id, entity_type);
      }

      // 原子登记保留（reserved_files_）：同一文件集不会被并发重复选中。
      // 输入或重叠文件已被其它任务保留时跳过本组，继续尝试下一组。
      {
        std::lock_guard<std::mutex> compact_lock(compacting_mutex_);
        bool busy = false;
        for (const auto& f : task.input_files) {
          if (compacting_files_.count(f.file_number) > 0) {
            busy = true;
            break;
          }
        }
        if (!busy) {
          for (const auto& f : task.overlapping_files) {
            if (compacting_files_.count(f.file_number) > 0) {
              busy = true;
              break;
            }
          }
        }
        if (busy) {
          continue;
        }
        for (const auto& f : task.input_files) {
          compacting_files_.insert(f.file_number);
        }
        for (const auto& f : task.overlapping_files) {
          compacting_files_.insert(f.file_number);
        }
      }

      // 预估输出大小（含重叠文件；假设压缩率 0.7）。
      // 注：priority 仍按层溢出比例计算，供并行引擎/外部调度排序使用。
      task.estimated_output_size = 0;
      for (const auto& f : task.input_files) {
        task.estimated_output_size += f.file_size;
      }
      for (const auto& f : task.overlapping_files) {
        task.estimated_output_size += f.file_size;
      }
      task.estimated_output_size =
          static_cast<uint64_t>(task.estimated_output_size * 0.7);

      task.priority = CalculateCompactionPriority(levels_[level], config_);
      return task;
    }
  }

  return std::nullopt;
}

Status SizeTieredCompactionEngine::ExecuteCompaction(const CompactionTask& task) {
  auto start_time = std::chrono::steady_clock::now();

  // 保留（reserved_files_）：PickNextCompaction 返回任务时已登记；这里再做一次
  // 幂等登记，允许直接调用 ExecuteCompaction（测试/并行引擎）。
  {
    std::lock_guard<std::mutex> lock(compacting_mutex_);
    for (const auto& f : task.input_files) {
      compacting_files_.insert(f.file_number);
    }
    for (const auto& f : task.overlapping_files) {
      compacting_files_.insert(f.file_number);
    }
  }

  // 执行合并（Task 4.2：一次任务可能写出多个分段）
  uint64_t output_files = 0;
  Status s = DoZoneCompaction(task, &output_files);

  // 解除保留：成功或失败都必须释放，否则该文件集永久不可再被选中
  {
    std::lock_guard<std::mutex> lock(compacting_mutex_);
    for (const auto& f : task.input_files) {
      compacting_files_.erase(f.file_number);
    }
    for (const auto& f : task.overlapping_files) {
      compacting_files_.erase(f.file_number);
    }
  }

  auto end_time = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

  if (s.ok()) {
    stats_.RecordCompaction(task, static_cast<uint64_t>(duration), output_files);
    RecordCompactionSuccess();
  } else {
    // Task 4.3：失败指数退避，调度器不会紧循环重试同一失败
    RecordCompactionFailure();
  }

  return s;
}

void SizeTieredCompactionEngine::ScheduleCompaction() {
  // 只有 engine 后台 worker 是调度器（Task 4.3：删除 LsmEngine 里的第二个
  // 直接执行线程）。没有消费者时不能入队：
  //   - 任务会永久留在 pending_tasks_，WaitForCompactions 会一直等下去；
  //   - PickNextCompaction 已原子 reserved_files_ 登记，该组文件会永久不可选。
  if (!config_.enable_background_compaction) {
    return;
  }

  // 失败指数退避：连续失败期间不调度（失败不紧循环）
  if (env_->NowMicros() <
      backoff_until_us_.load(std::memory_order_relaxed)) {
    return;
  }

  std::lock_guard<std::mutex> lock(queue_mutex_);

  // pending 有界：队列满时跳过本次调度，worker 完成当前任务后会再次调度
  if (pending_tasks_.size() >= config_.max_pending_tasks) {
    return;
  }

  // PickNextCompaction 按文件集原子保留（reserved_files_）：同一文件绝不会
  // 出现在两个 pending/执行中的任务里（任务去重）。
  auto task = PickNextCompaction();
  if (task.has_value()) {
    pending_tasks_.push(std::move(*task));
    queue_cv_.notify_one();
  }
}

void SizeTieredCompactionEngine::WaitForCompactions() {
  // 以"实际启动的后台线程"为准：enable_background_compaction=true 但
  // compaction_threads=0 时没有消费者，调用线程必须自己排空队列（否则死等）。
  const bool has_worker = !compaction_threads_.empty();
  while (true) {
    CompactionTask task;
    bool has_task = false;
    {
      std::lock_guard<std::mutex> lock(queue_mutex_);
      if (!has_worker && !pending_tasks_.empty()) {
        // 没有后台线程时在调用线程把队列排空，避免永久等待
        task = pending_tasks_.front();
        pending_tasks_.pop();
        has_task = true;
      } else if (pending_tasks_.empty() &&
                 stats_.active_compactions.load() == 0) {
        break;
      }
    }

    if (has_task) {
      ExecuteCompaction(task).IgnoreError();
      continue;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

Status SizeTieredCompactionEngine::CompactAll() {
  // 反复执行所有需要合并的层级任务，直到没有任务为止。
  // 注意：PickNextCompaction 返回的任务已原子登记 reserved_files_，因此任何
  // 被选中的任务都必须执行（不能丢弃，否则该组文件会永久处于保留状态）。
  // 文件每次都会上升一层，层级有上限，循环必然终止。
  while (true) {
    auto task = PickNextCompaction();
    if (!task.has_value()) {
      break;
    }

    Status s = ExecuteCompaction(*task);
    if (!s.ok()) {
      return s;
    }
  }
  return Status::OK();
}

// =============================================================================
// 后台线程（唯一的调度执行器）
// =============================================================================

void SizeTieredCompactionEngine::BackgroundCompactionThread() {
  while (!shutdown_.load()) {
    std::unique_lock<std::mutex> lock(queue_mutex_);

    queue_cv_.wait(lock, [this] {
      return shutdown_.load() || !pending_tasks_.empty();
    });

    if (shutdown_.load()) {
      break;
    }

    if (pending_tasks_.empty()) {
      continue;
    }

    CompactionTask task = pending_tasks_.front();
    pending_tasks_.pop();

    lock.unlock();

    stats_.active_compactions.fetch_add(1);
    stats_.pending_compaction_bytes.fetch_add(task.estimated_output_size);

    Status s = ExecuteCompaction(task);

    stats_.active_compactions.fetch_sub(1);
    stats_.pending_compaction_bytes.fetch_sub(task.estimated_output_size);

    if (!s.ok()) {
      // Task 4.3：失败后按指数退避等待，避免紧循环重试同一个失败
      const uint64_t remaining = BackoffRemainingMicros();
      if (remaining > 0 && !shutdown_.load()) {
        std::unique_lock<std::mutex> backoff_lock(queue_mutex_);
        queue_cv_.wait_for(backoff_lock, std::chrono::microseconds(remaining),
                           [this] { return shutdown_.load(); });
      }
    }

    // 继续调度可能的新任务（若仍在退避窗口内，ScheduleCompaction 会直接返回）
    ScheduleCompaction();
  }
}

// =============================================================================
// 压缩输出分段（Task 4.2：按 l0_file_size 切分输出）
// =============================================================================
namespace {

// 一个输出 SST 分段：归并过程中按目标大小轮转，每段一个文件号、一份真实统计。
struct OutputSegment {
  uint64_t file_number = 0;
  std::string path;
  std::string blob_path;
  std::unique_ptr<WritableFile> file;
  std::unique_ptr<ZoneColumnarSstBuilder> builder;
  uint64_t rows = 0;
  uint64_t file_size = 0;
  uint64_t min_entity_id = UINT64_MAX;
  uint64_t max_entity_id = 0;
  uint64_t min_timestamp = UINT64_MAX;
  uint64_t max_timestamp = 0;
  bool finished = false;

  // 失败清理：放弃 builder 并关闭句柄（物理文件由调用方删除）。
  void Abandon() {
    if (!finished && builder) {
      builder->Abandon();
    }
    builder.reset();
    file.reset();
  }
};

}  // namespace

// =============================================================================
// Zone-Columnar 合并实现（核心算法）
// =============================================================================

Status SizeTieredCompactionEngine::DoZoneCompaction(const CompactionTask& task,
                                                    uint64_t* output_files) {
  if (output_files != nullptr) {
    *output_files = 0;
  }
  if (task.input_files.empty()) {
    return Status::InvalidArgument("SizeTieredCompactionEngine",
                                   "compaction task has no input files");
  }
  if (task.input_level < 0 || task.input_level >= config_.max_levels ||
      task.output_level < 0 || task.output_level >= config_.max_levels ||
      task.input_level == task.output_level) {
    return Status::InvalidArgument("SizeTieredCompactionEngine",
                                   "compaction task has invalid levels");
  }

  const uint8_t entity_type = task.input_files[0].entity_type;
  const uint16_t column_id = task.input_files[0].column_id;

  // 审计 9.1：一个任务（inputs ∪ overlaps）必须同属一个 (entity_type, column_id)。
  // 输出 SST 的 header 只保存一组列/类型，混组会改写/隐藏其它列的数据。
  for (const auto& f : task.input_files) {
    if (f.entity_type != entity_type || f.column_id != column_id) {
      return Status::InvalidArgument(
          "SizeTieredCompactionEngine",
          "input files have different entity_type or column_id");
    }
  }
  for (const auto& f : task.overlapping_files) {
    if (f.entity_type != entity_type || f.column_id != column_id) {
      return Status::InvalidArgument(
          "SizeTieredCompactionEngine",
          "overlapping files have different entity_type or column_id");
    }
  }

  // 审计 3.1：合并输入 = inputs ∪ overlaps。重叠文件的数据必须写进输出，
  // 旧实现只合并 inputs 却把 overlaps 直接删除（下层数据丢失）。
  std::vector<ZoneSstMeta> merge_inputs = task.input_files;
  merge_inputs.insert(merge_inputs.end(), task.overlapping_files.begin(),
                      task.overlapping_files.end());

  // Task 4.2：按 l0_file_size 切分输出。builder 的 FileSize() 在 Finish 前始终
  // 为 0（Zone 数据在 Finish 统一落盘），因此用"输入平均编码字节/行"估算目标
  // 行数，并在每段结束后用该段真实 file_size/rows 重新校准。
  const uint64_t target_file_size = config_.l0_file_size;
  uint64_t input_rows = 0;
  uint64_t input_bytes = 0;
  for (const auto& f : merge_inputs) {
    input_rows += f.num_entries;
    input_bytes += f.file_size;
  }
  double avg_row_bytes =
      input_rows > 0 ? static_cast<double>(input_bytes) / input_rows : 24.0;
  if (avg_row_bytes <= 0.0) {
    avg_row_bytes = 24.0;
  }
  uint64_t target_rows =
      target_file_size > 0
          ? std::max<uint64_t>(1, static_cast<uint64_t>(target_file_size / avg_row_bytes))
          : 0;

  CedarOptions options;
  options.env = env_;
  options.column_id = column_id;

  std::vector<OutputSegment> segments;
  auto remove_segments = [&]() {
    for (auto& seg : segments) {
      seg.Abandon();
      if (!seg.path.empty()) {
        env_->RemoveFile(seg.path);
      }
      if (!seg.blob_path.empty()) {
        env_->RemoveFile(seg.blob_path);
      }
    }
    segments.clear();
  };

  auto start_segment = [&]() -> Status {
    OutputSegment seg;
    seg.file_number = NewFileNumber();
    seg.path = GetSstPath(seg.file_number);
    seg.blob_path = GetBlobPath(seg.file_number);

    WritableFile* raw = nullptr;
    Status s = env_->NewWritableFile(seg.path, &raw);
    if (!s.ok()) {
      return s;
    }
    seg.file.reset(raw);
    seg.builder = std::make_unique<ZoneColumnarSstBuilder>(options, raw, column_id,
                                                           db_path_);
    seg.builder->SetLevel(task.output_level);
    seg.builder->SetFileNumber(seg.file_number);
    segments.push_back(std::move(seg));
    return Status::OK();
  };

  // 结束当前段：builder.Finish() 内含 Sync（压缩输出是 MANIFEST 提交后唯一
  // 副本、且输入随后被删除，必须 fsync 后才能提交元数据），再关闭句柄并统计。
  auto finish_segment = [&]() -> Status {
    if (segments.empty() || segments.back().finished) {
      return Status::OK();
    }
    OutputSegment& seg = segments.back();
    Status s = seg.builder->Finish();
    seg.finished = true;
    if (s.ok()) {
      s = seg.file->Close();
    }
    seg.builder.reset();
    seg.file.reset();
    if (!s.ok()) {
      return s;
    }
    s = env_->GetFileSize(seg.path, &seg.file_size);
    if (!s.ok()) {
      return s;
    }
    // 用真实编码结果校准下一段的目标行数（每段 ≤ 目标 + 一次 block 刷写）
    if (seg.rows > 0 && target_file_size > 0) {
      avg_row_bytes = static_cast<double>(seg.file_size) / static_cast<double>(seg.rows);
      if (avg_row_bytes > 0.0) {
        target_rows = std::max<uint64_t>(
            1, static_cast<uint64_t>(target_file_size / avg_row_bytes));
      }
    }
    return Status::OK();
  };

  // 归并 sink：把每一行写进当前分段；达到目标行数就先切段再写新行。
  auto add_row = [&](const CedarKey& key, const Descriptor& value) -> Status {
    if (segments.empty()) {
      Status s = start_segment();
      if (!s.ok()) {
        return s;
      }
    }
    if (target_rows > 0 && segments.back().rows >= target_rows) {
      Status s = finish_segment();
      if (!s.ok()) {
        return s;
      }
      s = start_segment();
      if (!s.ok()) {
        return s;
      }
    }

    OutputSegment& seg = segments.back();
    seg.builder->Add(key, value);
    ++seg.rows;
    seg.min_entity_id = std::min(seg.min_entity_id, key.entity_id());
    seg.max_entity_id = std::max(seg.max_entity_id, key.entity_id());
    const uint64_t ts = key.timestamp().value();
    seg.min_timestamp = std::min(seg.min_timestamp, ts);
    seg.max_timestamp = std::max(seg.max_timestamp, ts);
    return Status::OK();
  };

  // 多路堆归并 inputs ∪ overlaps（Task 4.1），逐行写分段输出
  MergeZoneResult merge_result;
  Status s = MergeZones(merge_inputs, add_row, &merge_result);
  if (!s.ok()) {
    remove_segments();
    return s;
  }
  s = finish_segment();
  if (!s.ok()) {
    remove_segments();
    return s;
  }

  // 输出元数据：列/类型来自任务分组，行数/范围来自每段真实写出内容
  std::vector<ZoneSstMeta> output_metas;
  output_metas.reserve(segments.size());
  for (const auto& seg : segments) {
    ZoneSstMeta meta;
    meta.file_number = seg.file_number;
    meta.level = task.output_level;
    meta.file_size = seg.file_size;
    meta.num_entries = seg.rows;
    meta.path = seg.path;
    meta.blob_path = seg.blob_path;
    meta.column_id = column_id;
    meta.entity_type = entity_type;
    meta.min_entity_id = seg.min_entity_id;
    meta.max_entity_id = seg.max_entity_id;
    meta.min_timestamp = seg.min_timestamp;
    meta.max_timestamp = seg.max_timestamp;
    output_metas.push_back(std::move(meta));
  }

  // 提交顺序（审计 3.1 / Task 4.4）：一次 MANIFEST 写覆盖所有分段，成功之后
  // 才删除输入的物理文件。失败则回滚内存状态并清理全部分段输出。
  Status manifest_status = Status::OK();
  {
    std::lock_guard<std::mutex> lock(levels_mutex_);

    const LevelState saved_input_level = levels_[task.input_level];
    const LevelState saved_output_level = levels_[task.output_level];

    for (const auto& f : task.input_files) {
      RemoveFileMetaFromLevelLocked(f.file_number, task.input_level);
    }
    for (const auto& f : task.overlapping_files) {
      RemoveFileMetaFromLevelLocked(f.file_number, task.output_level);
    }
    for (auto& meta : output_metas) {
      levels_[task.output_level].files.push_back(std::move(meta));
    }

    manifest_status = SaveManifestLocked();
    if (!manifest_status.ok()) {
      levels_[task.input_level] = saved_input_level;
      levels_[task.output_level] = saved_output_level;
    }
  }

  if (!manifest_status.ok()) {
    remove_segments();
    return manifest_status;
  }

  // MANIFEST 已提交，现在才删除被合并掉的物理输入文件。
  for (const auto& f : merge_inputs) {
    DeleteSstFile(f);
  }

  if (output_files != nullptr) {
    *output_files = segments.size();
  }
  return Status::OK();
}

// =============================================================================
// K 路堆归并（Task 4.1：O(N×K) → O(N log K)）
// =============================================================================

namespace {

// 归并堆项：每个输入文件在堆里最多有一个"当前行"。
struct MergeHeapItem {
  CedarKey key;
  Descriptor value;
  uint32_t file_index = 0;   // inputs 下标（取 file_number / Blob 归属）
  uint64_t file_number = 0;  // 输入文件号（完整同 key 时用它取舍）
};

// 小顶堆比较器（std::priority_queue 是最大堆：返回 true 表示 a 优先级更低）。
// 取舍规则（Task 4.1，代码即规范）：
//   主序：CedarKey::Compare（entity → type → column → target → ts 降序 → seq → flags）；
//   同 user key 且同 timestamp：sequence 更大的先出（只保留 sequence 大的版本）；
//   完整 key 完全相同：输入文件号更大的先出（保留更新的文件）。
// 这是严格弱序：组间按 Compare，组内按 (seq, flags) 反序，完全相同再按文件号反序。
struct MergeHeapGreater {
  bool operator()(const MergeHeapItem& a, const MergeHeapItem& b) const {
    const int c = a.key.Compare(b.key);
    if (c != 0) {
      if (a.key.SameUserKey(b.key) && a.key.timestamp() == b.key.timestamp()) {
        // Compare 在 sequence 上递增；反转 → 大 sequence 先出
        return c < 0;
      }
      return c > 0;
    }
    return a.file_number < b.file_number;
  }
};

// next 是否已被 last 覆盖（被丢弃）：
//   - 完整相同：保留文件号更大的（堆序已把大的排在前）；
//   - 同 user key + 同 timestamp：只保留 sequence 更大的（堆序已把大的排在前）。
bool IsDominatedBy(const CedarKey& next, const CedarKey& last) {
  if (next.Compare(last) == 0) {
    return true;
  }
  return next.SameUserKey(last) && next.timestamp() == last.timestamp();
}

}  // namespace

Status SizeTieredCompactionEngine::MergeZones(
    const std::vector<ZoneSstMeta>& inputs,
    const MergeRowSink& sink,
    MergeZoneResult* result) {
  if (result != nullptr) {
    *result = MergeZoneResult();
  }

  // 打开所有输入文件（= inputs ∪ overlaps，由 DoZoneCompaction 组装）
  std::vector<std::unique_ptr<ZoneColumnarSstReader>> readers;
  std::vector<std::unique_ptr<ZoneColumnarIterator>> iterators;
  readers.reserve(inputs.size());
  iterators.reserve(inputs.size());

  // 堆底层容器预分配 K 个元素（priority_queue 本身没有 reserve）
  std::vector<MergeHeapItem> heap_storage;
  heap_storage.reserve(inputs.size());
  std::priority_queue<MergeHeapItem, std::vector<MergeHeapItem>, MergeHeapGreater>
      heap(MergeHeapGreater(), std::move(heap_storage));

  for (size_t i = 0; i < inputs.size(); ++i) {
    auto reader = std::make_unique<ZoneColumnarSstReader>(inputs[i].path);
    const Status s = reader->Open();
    if (!s.ok()) {
      // 打开失败不得静默跳过：那会把"读不到"变成"数据被合并掉"
      return Status::IOError("SizeTieredCompactionEngine",
                             "cannot open compaction input " + inputs[i].path +
                                 ": " + s.ToString());
    }
    auto iterator = std::make_unique<ZoneColumnarIterator>(reader.get());
    iterator->SeekToFirst();
    if (iterator->Valid()) {
      MergeHeapItem item;
      item.key = iterator->Key();
      item.value = iterator->Value();
      item.file_index = static_cast<uint32_t>(i);
      item.file_number = inputs[i].file_number;
      heap.push(item);
    }
    readers.push_back(std::move(reader));
    iterators.push_back(std::move(iterator));
  }

  // 归并循环：每行 O(log K)
  bool has_last_key = false;
  CedarKey last_key;
  while (!heap.empty()) {
    const MergeHeapItem item = heap.top();
    heap.pop();

    if (!(has_last_key && IsDominatedBy(item.key, last_key))) {
      Descriptor value = item.value;
      // 处理 Blob 引用（当前策略：仅拷贝指针，不重写 Blob）
      if (value.AsExternalRef().has_value()) {
        value = HandleBlobReference(value, nullptr, inputs[item.file_index]);
      }

      const Status s = sink(item.key, value);
      if (!s.ok()) {
        return s;
      }

      if (result != nullptr) {
        const uint64_t ts = item.key.timestamp().value();
        ++result->num_rows;
        result->min_entity_id = std::min(result->min_entity_id, item.key.entity_id());
        result->max_entity_id = std::max(result->max_entity_id, item.key.entity_id());
        result->min_timestamp = std::min(result->min_timestamp, ts);
        result->max_timestamp = std::max(result->max_timestamp, ts);
      }

      last_key = item.key;
      has_last_key = true;
    }

    // 同一路前进一行
    auto& iterator = iterators[item.file_index];
    iterator->Next();
    if (iterator->Valid()) {
      MergeHeapItem next;
      next.key = iterator->Key();
      next.value = iterator->Value();
      next.file_index = item.file_index;
      next.file_number = item.file_number;
      heap.push(next);
    }
  }

  return Status::OK();
}

bool SizeTieredCompactionEngine::ShouldDropTombstone(const CedarKey& key, int output_level) {
  // L0-L2: 永不物理删除（保证快照读）
  if (output_level < config_.tombstone_cleanup_level) {
    return false;
  }
  
  // L3+: 当磁盘使用 > 70% 时，允许清理 Tombstone
  // TODO: 计算实际磁盘使用率
  // if (disk_usage_ratio > config_.tombstone_cleanup_disk_ratio) {
  //   return true;
  // }
  
  // 最底层总是清理 Tombstone
  if (output_level == config_.max_levels - 1) {
    return true;
  }
  
  return false;
}

Descriptor SizeTieredCompactionEngine::HandleBlobReference(
    const Descriptor& old_desc,
    ZoneColumnarSstBuilder* builder,
    const ZoneSstMeta& source_file) {
  
  if (!old_desc.AsExternalRef().has_value()) {
    return old_desc;
  }
  
  // TODO: 实现 Blob 重写策略
  // 当前简化处理：仅拷贝指针（延迟拷贝策略）
  // 引用计数管理由上层调用者处理
  (void)builder; (void)source_file;
  
  return old_desc;
}

// =============================================================================
// 查询支持
// =============================================================================

std::vector<ZoneSstMeta> SizeTieredCompactionEngine::GetOverlappingFiles(
    uint64_t min_entity, uint64_t max_entity,
    uint64_t min_ts, uint64_t max_ts,
    uint16_t column_id, uint8_t entity_type) const {
  std::lock_guard<std::mutex> lock(levels_mutex_);
  
  std::vector<ZoneSstMeta> result;
  
  for (const auto& level : levels_) {
    for (const auto& f : level.files) {
      // 列和类型过滤
      if (f.column_id != column_id || f.entity_type != entity_type) {
        continue;
      }
      
      // 范围重叠检查
      bool entity_overlap = !(f.max_entity_id < min_entity || f.min_entity_id > max_entity);
      bool ts_overlap = !(f.max_timestamp < min_ts || f.min_timestamp > max_ts);
      
      if (entity_overlap && ts_overlap) {
        result.push_back(f);
      }
    }
  }
  
  return result;
}

std::vector<ZoneSstMeta> SizeTieredCompactionEngine::GetFilesForEntity(
    uint64_t entity_id, uint16_t column_id, uint8_t entity_type) const {
  std::lock_guard<std::mutex> lock(levels_mutex_);
  
  std::vector<ZoneSstMeta> result;
  
  for (const auto& level : levels_) {
    for (const auto& f : level.files) {
      if (f.column_id != column_id || f.entity_type != entity_type) {
        continue;
      }
      
      if (entity_id >= f.min_entity_id && entity_id <= f.max_entity_id) {
        result.push_back(f);
      }
    }
  }
  
  return result;
}

// =============================================================================
// Blob GC
// =============================================================================

Status SizeTieredCompactionEngine::TriggerBlobGC() {
  // 获取需要 GC 的 Blob 文件
  auto blob_files = blob_manager_.GetBlobFilesForGC(1024 * 1024);  // 1MB 阈值
  
  for (uint64_t blob_id : blob_files) {
    if (blob_manager_.CanDeleteBlob(blob_id)) {
      std::string blob_path = GetBlobPath(blob_id);
      std::error_code ec;
      std::filesystem::remove(blob_path, ec);
      if (ec) {
        // 记录错误但继续
      }
    }
  }
  
  return Status::OK();
}

// =============================================================================
// 辅助方法
// =============================================================================

void SizeTieredCompactionEngine::AddFileToLevel(const ZoneSstMeta& meta, int level) {
  std::lock_guard<std::mutex> lock(levels_mutex_);
  if (level >= 0 && level < config_.max_levels) {
    levels_[level].files.push_back(meta);
  }
}

// 只移除元数据（调用方持有 levels_mutex_，且必须在 MANIFEST 提交前调用）。
void SizeTieredCompactionEngine::RemoveFileMetaFromLevelLocked(uint64_t file_number,
                                                               int level) {
  if (level < 0 || level >= config_.max_levels) {
    return;
  }
  
  auto& files = levels_[level].files;
  for (auto it = files.begin(); it != files.end(); ++it) {
    if (it->file_number == file_number) {
      // 移除 Blob 引用
      if (it->blob_file_number != 0) {
        blob_manager_.RemoveReference(it->blob_file_number, file_number);
      }
      files.erase(it);
      break;
    }
  }
}

// 删除物理文件：只在 MANIFEST 提交成功之后调用（审计 3.1 的提交顺序）。
void SizeTieredCompactionEngine::DeleteSstFile(const ZoneSstMeta& meta) {
  if (!meta.path.empty()) {
    std::error_code ec;
    std::filesystem::remove(meta.path, ec);
  }
  if (!meta.blob_path.empty()) {
    std::error_code ec;
    std::filesystem::remove(meta.blob_path, ec);
  }
}

Status SizeTieredCompactionEngine::SaveManifest() {
  std::lock_guard<std::mutex> lock(levels_mutex_);
  return SaveManifestLocked();
}

// 调用方已持有 levels_mutex_。
// 持久化内容：文件号水位 + 每层文件元数据（磁盘格式与旧版逐字节相同）。
// 写入顺序：tmp 文件 → fsync（MANIFEST 前缀会顺带 fsync 目录）→ 原子 rename，
// 因此 SaveManifestLocked 返回 OK 时元数据已经提交，之后才能删除输入 SST。
Status SizeTieredCompactionEngine::SaveManifestLocked() {
  std::string manifest_path = GetManifestPath();
  // tmp 文件名故意不以 MANIFEST 开头：PosixWritableFile 只对 MANIFEST* 文件
  // 在 Sync() 时 fsync 目录。tmp 的目录项由 rename 之后的 SyncParentDirectory
  // 统一 fsync 一次（否则每提交会付两次目录 fsync，本机 ~4-8ms/次）。
  std::string tmp_path = db_path_ + "/zc-manifest.tmp";

  std::string buf;
  auto append = [&buf](const void* data, size_t size) {
    buf.append(static_cast<const char*>(data), size);
  };

  // 写入下一个文件编号（绑定 LsmEngine 后即它的共享计数器水位）
  uint64_t next_file = file_number_allocator_.Peek();
  append(&next_file, sizeof(next_file));

  // 写入层级数量
  int32_t num_levels = static_cast<int32_t>(levels_.size());
  append(&num_levels, sizeof(num_levels));

  // 写入每个层级的文件
  for (const auto& level : levels_) {
    int32_t file_count = static_cast<int32_t>(level.files.size());
    append(&file_count, sizeof(file_count));

    for (const auto& f : level.files) {
      append(&f.file_number, sizeof(f.file_number));
      append(&f.file_size, sizeof(f.file_size));
      append(&f.num_entries, sizeof(f.num_entries));
      append(&f.level, sizeof(f.level));
      append(&f.min_entity_id, sizeof(f.min_entity_id));
      append(&f.max_entity_id, sizeof(f.max_entity_id));
      append(&f.min_timestamp, sizeof(f.min_timestamp));
      append(&f.max_timestamp, sizeof(f.max_timestamp));
      append(&f.column_id, sizeof(f.column_id));
      append(&f.entity_type, sizeof(f.entity_type));
      append(&f.blob_file_number, sizeof(f.blob_file_number));
      append(&f.blob_file_size, sizeof(f.blob_file_size));

      // 写入路径
      uint32_t path_len = static_cast<uint32_t>(f.path.size());
      append(&path_len, sizeof(path_len));
      append(f.path.data(), path_len);
    }
  }

  WritableFile* file_raw = nullptr;
  Status s = env_->NewWritableFile(tmp_path, &file_raw);
  if (!s.ok()) {
    return Status::IOError("SizeTieredCompactionEngine",
                           "Cannot open manifest for writing: " + s.ToString());
  }
  std::unique_ptr<WritableFile> file(file_raw);

  s = file->Append(Slice(buf));
  if (s.ok()) {
    s = file->Sync();
  }
  const Status close_status = file->Close();
  file.reset();

  if (!s.ok() || !close_status.ok()) {
    env_->RemoveFile(tmp_path);
    return s.ok() ? close_status : s;
  }

  // 原子重命名：rename 返回后 MANIFEST 内容即已生效
  s = env_->RenameFile(tmp_path, manifest_path);
  if (!s.ok()) {
    env_->RemoveFile(tmp_path);
    return Status::IOError("SizeTieredCompactionEngine",
                           "Failed to rename manifest: " + s.ToString());
  }

  // Task 4.4：rename 之后再 fsync 一次目录项（best-effort：rename 已生效，
  // 这里失败只影响掉电后的目录持久性，不让上层回滚已提交的元数据）。
  SyncParentDirectory().IgnoreError();

  return Status::OK();
}

// MANIFEST-zc 加载校验上限（Task 4.4：长度/层级/路径上限，损坏文件不越界）
namespace {

constexpr uint32_t kMaxManifestPathLength = 4096;
constexpr int32_t kMaxManifestFilesPerLevel = 1 << 20;

// 带边界检查的顺序读取器（解析失败返回 false，绝不越界读）。
class ManifestReader {
 public:
  explicit ManifestReader(std::string data) : data_(std::move(data)) {}

  bool ReadU8(uint8_t* v) { return Read(v, sizeof(*v)); }
  bool ReadU16(uint16_t* v) { return Read(v, sizeof(*v)); }
  bool ReadU32(uint32_t* v) { return Read(v, sizeof(*v)); }
  bool ReadI32(int32_t* v) { return Read(v, sizeof(*v)); }
  bool ReadU64(uint64_t* v) { return Read(v, sizeof(*v)); }
  bool ReadDouble(double* v) { return Read(v, sizeof(*v)); }

  bool ReadBytes(std::string* out, size_t n) {
    if (n > remaining()) {
      return false;
    }
    out->assign(data_.data() + pos_, n);
    pos_ += n;
    return true;
  }

  bool Read(void* out, size_t n) {
    if (n > remaining()) {
      return false;
    }
    std::memcpy(out, data_.data() + pos_, n);
    pos_ += n;
    return true;
  }

  size_t remaining() const { return data_.size() - pos_; }

 private:
  std::string data_;
  size_t pos_ = 0;
};

}  // namespace

Status SizeTieredCompactionEngine::SyncParentDirectory() {
  // Task 4.4：rename 之后 fsync 目录项。Env 没有暴露"fsync 目录"的 API，而
  // PosixWritableFile::Sync() 在 macOS 上用 F_FULLFSYNC（会把目录里所有脏
  // 元数据重刷，实测 ~5ms/次）；这里直接用标准 POSIX open(dir)+fsync(fd)，
  // 语义就是需求里的 fsync(dir)，代价 ~0.1ms。
  const std::string dir = db_path_;
#if defined(__APPLE__) || defined(__linux__)
  const int fd = ::open(dir.c_str(), O_RDONLY);
  if (fd < 0) {
    return Status::IOError("SizeTieredCompactionEngine",
                           "cannot open directory: " + dir);
  }
  const int rc = ::fsync(fd);
  const int saved_errno = errno;
  ::close(fd);
  if (rc != 0) {
    return Status::IOError("SizeTieredCompactionEngine",
                           "fsync directory failed (errno " +
                               std::to_string(saved_errno) + "): " + dir);
  }
  return Status::OK();
#else
  (void)dir;
  return Status::OK();
#endif
}

Status SizeTieredCompactionEngine::LoadManifest() {
  const std::string manifest_path = GetManifestPath();

  if (!std::filesystem::exists(manifest_path)) {
    return Status::NotFound("SizeTieredCompactionEngine", "Manifest not found");
  }

  // 整文件读入 + 带边界校验解析：截断/损坏的 MANIFEST 返回 Corruption，
  // 半截字段不会进入层级状态（Task 4.4）。残留的 MANIFEST-zc.tmp 一律忽略。
  std::string data;
  {
    std::ifstream file(manifest_path, std::ios::binary | std::ios::ate);
    if (!file) {
      return Status::IOError("SizeTieredCompactionEngine", "Cannot open manifest");
    }
    const std::streamoff size = file.tellg();
    const std::streamoff min_size =
        static_cast<std::streamoff>(sizeof(uint64_t) + sizeof(int32_t));
    if (size < min_size) {
      return Status::Corruption("SizeTieredCompactionEngine",
                                "manifest too small");
    }
    data.resize(static_cast<size_t>(size));
    file.seekg(0, std::ios::beg);
    if (!file.read(data.data(), size)) {
      return Status::Corruption("SizeTieredCompactionEngine",
                                "manifest read failed");
    }
  }

  ManifestReader reader(std::move(data));

  // 文件号水位（只前进不后退）
  uint64_t next_file = 0;
  int32_t num_levels = 0;
  if (!reader.ReadU64(&next_file) || !reader.ReadI32(&num_levels)) {
    return Status::Corruption("SizeTieredCompactionEngine",
                              "truncated manifest header");
  }
  if (num_levels < 1 || num_levels > config_.max_levels) {
    return Status::Corruption("SizeTieredCompactionEngine",
                              "manifest level count out of range: " +
                                  std::to_string(num_levels));
  }

  std::vector<LevelState> loaded(static_cast<size_t>(config_.max_levels));
  for (int level = 0; level < config_.max_levels; ++level) {
    loaded[level].level = level;
  }

  for (int level = 0; level < num_levels; ++level) {
    int32_t file_count = 0;
    if (!reader.ReadI32(&file_count) || file_count < 0 ||
        file_count > kMaxManifestFilesPerLevel) {
      return Status::Corruption("SizeTieredCompactionEngine",
                                "invalid file count in manifest");
    }

    for (int32_t i = 0; i < file_count; ++i) {
      ZoneSstMeta meta;
      uint32_t path_len = 0;
      const bool ok =
          reader.ReadU64(&meta.file_number) &&
          reader.ReadU64(&meta.file_size) &&
          reader.ReadU64(&meta.num_entries) &&
          reader.ReadI32(&meta.level) &&
          reader.ReadU64(&meta.min_entity_id) &&
          reader.ReadU64(&meta.max_entity_id) &&
          reader.ReadU64(&meta.min_timestamp) &&
          reader.ReadU64(&meta.max_timestamp) &&
          reader.ReadU16(&meta.column_id) &&
          reader.ReadU8(&meta.entity_type) &&
          reader.ReadU64(&meta.blob_file_number) &&
          reader.ReadU64(&meta.blob_file_size) &&
          reader.ReadU32(&path_len);
      if (!ok) {
        return Status::Corruption("SizeTieredCompactionEngine",
                                  "truncated file entry in manifest");
      }
      if (path_len == 0 || path_len > kMaxManifestPathLength ||
          !reader.ReadBytes(&meta.path, path_len)) {
        return Status::Corruption("SizeTieredCompactionEngine",
                                  "invalid file path in manifest");
      }
      // 层级以 MANIFEST 的分层结构为准（不信任记录里的 level 字段）
      meta.level = level;
      loaded[level].files.push_back(std::move(meta));
    }
  }

  // 全部校验通过后才替换内存状态，并恢复文件号水位 / Blob 引用
  {
    std::lock_guard<std::mutex> lock(levels_mutex_);
    levels_ = std::move(loaded);
    for (auto& level : levels_) {
      for (const auto& meta : level.files) {
        if (meta.blob_file_number != 0) {
          blob_manager_.RegisterBlobFile(meta.blob_file_number,
                                         meta.blob_file_size);
          blob_manager_.AddReference(meta.blob_file_number, meta.file_number);
        }
      }
    }
  }
  file_number_allocator_.EnsureAtLeast(next_file);

  return Status::OK();
}

size_t SizeTieredCompactionEngine::PendingTaskCount() const {
  std::lock_guard<std::mutex> lock(queue_mutex_);
  return pending_tasks_.size();
}

std::vector<CompactionTask> SizeTieredCompactionEngine::PendingTasksSnapshot() const {
  std::lock_guard<std::mutex> lock(queue_mutex_);
  std::vector<CompactionTask> out;
  out.reserve(pending_tasks_.size());
  std::queue<CompactionTask> copy = pending_tasks_;
  while (!copy.empty()) {
    out.push_back(std::move(copy.front()));
    copy.pop();
  }
  return out;
}

size_t SizeTieredCompactionEngine::ReservedFileCount() const {
  std::lock_guard<std::mutex> lock(compacting_mutex_);
  return compacting_files_.size();
}

uint32_t SizeTieredCompactionEngine::ConsecutiveCompactionFailures() const {
  return consecutive_failures_.load(std::memory_order_relaxed);
}

uint64_t SizeTieredCompactionEngine::BackoffRemainingMicros() const {
  const uint64_t until = backoff_until_us_.load(std::memory_order_relaxed);
  const uint64_t now = env_->NowMicros();
  return until > now ? until - now : 0;
}

void SizeTieredCompactionEngine::ResetCompactionBackoff() {
  consecutive_failures_.store(0, std::memory_order_relaxed);
  backoff_until_us_.store(0, std::memory_order_relaxed);
}

void SizeTieredCompactionEngine::RecordCompactionFailure() {
  const uint32_t failures =
      consecutive_failures_.fetch_add(1, std::memory_order_relaxed) + 1;
  uint64_t base = config_.failure_backoff_base_ms;
  uint64_t max_delay = config_.failure_backoff_max_ms;
  if (base == 0) {
    base = 1;
  }
  if (max_delay < base) {
    max_delay = base;
  }
  // base * 2^(failures-1)，封顶 max_delay（指数退避）
  uint64_t delay_ms = base;
  const uint32_t shift = std::min<uint32_t>(failures - 1, 20);
  for (uint32_t i = 0; i < shift && delay_ms < max_delay; ++i) {
    delay_ms *= 2;
  }
  if (delay_ms > max_delay) {
    delay_ms = max_delay;
  }
  backoff_until_us_.store(env_->NowMicros() + delay_ms * 1000,
                          std::memory_order_relaxed);
}

void SizeTieredCompactionEngine::RecordCompactionSuccess() {
  consecutive_failures_.store(0, std::memory_order_relaxed);
  backoff_until_us_.store(0, std::memory_order_relaxed);
}

bool SizeTieredCompactionEngine::HasFile(uint64_t file_number) const {
  std::lock_guard<std::mutex> lock(levels_mutex_);

  for (const auto& level : levels_) {
    for (const auto& f : level.files) {
      if (f.file_number == file_number) {
        return true;
      }
    }
  }
  return false;
}

std::optional<ZoneSstMeta> SizeTieredCompactionEngine::FindFile(uint64_t file_number) const {
  std::lock_guard<std::mutex> lock(levels_mutex_);
  
  for (const auto& level : levels_) {
    for (const auto& f : level.files) {
      if (f.file_number == file_number) {
        return f;
      }
    }
  }
  return std::nullopt;
}

std::string SizeTieredCompactionEngine::GetSstPath(uint64_t file_number) const {
  // 与 LsmEngine::SstFilePath 保持一致
  return db_path_ + "/" + std::to_string(file_number) + ".frond";
}

std::string SizeTieredCompactionEngine::GetBlobPath(uint64_t file_number) const {
  // 与 LsmEngine 保持一致
  return db_path_ + "/sst_" + std::to_string(file_number) + ".blob";
}

std::string SizeTieredCompactionEngine::GetManifestPath() const {
  return db_path_ + "/MANIFEST-zc";
}

uint64_t SizeTieredCompactionEngine::GetTotalSize() const {
  std::lock_guard<std::mutex> lock(levels_mutex_);
  uint64_t total = 0;
  for (const auto& level : levels_) {
    total += level.total_size();
  }
  return total;
}

std::vector<uint64_t> SizeTieredCompactionEngine::GetLevelSizes() const {
  std::lock_guard<std::mutex> lock(levels_mutex_);
  std::vector<uint64_t> sizes;
  for (const auto& level : levels_) {
    sizes.push_back(level.total_size());
  }
  return sizes;
}

std::string SizeTieredCompactionEngine::LevelSummary() const {
  std::lock_guard<std::mutex> lock(levels_mutex_);
  
  std::ostringstream oss;
  oss << "=== Level Summary ===" << std::endl;
  
  for (int i = 0; i < config_.max_levels; ++i) {
    const auto& level = levels_[i];
    uint64_t size = level.total_size();
    uint64_t threshold = level.capacity_threshold(config_);
    double ratio = static_cast<double>(size) / threshold;
    
    oss << "L" << i << ": ";
    oss << level.file_count() << " files, ";
    oss << size << " bytes / " << threshold << " bytes (";
    oss << std::fixed << std::setprecision(1) << (ratio * 100) << "%)";
    
    if (level.needs_compaction(config_)) {
      oss << " [NEEDS COMPACTION]";
    }
    oss << std::endl;
  }
  
  return oss.str();
}

// =============================================================================
// 工具函数
// =============================================================================

bool FileRangeOverlaps(const ZoneSstMeta& a, const ZoneSstMeta& b) {
  bool entity_overlap = !(a.max_entity_id < b.min_entity_id || a.min_entity_id > b.max_entity_id);
  bool ts_overlap = !(a.max_timestamp < b.min_timestamp || a.min_timestamp > b.max_timestamp);
  return entity_overlap && ts_overlap;
}

bool KeyInFileRange(const ZoneSstMeta& file, uint64_t entity_id, uint64_t timestamp) {
  return entity_id >= file.min_entity_id && entity_id <= file.max_entity_id &&
         timestamp >= file.min_timestamp && timestamp <= file.max_timestamp;
}

int CalculateCompactionPriority(const LevelState& level, const SizeTieredConfig& config) {
  uint64_t size = level.total_size();
  uint64_t threshold = level.capacity_threshold(config);
  
  if (size <= threshold) {
    return 100;  // 不需要合并
  }
  
  double overflow_ratio = static_cast<double>(size) / threshold;
  
  // 优先级 = 层级 * 10 - 溢出比例 * 5（数值越小优先级越高）
  int priority = level.level * 10 - static_cast<int>(overflow_ratio * 5);
  
  // L0 特殊处理：文件数过多时提高优先级
  if (level.level == 0 && level.file_count() > config.l0_max_files) {
    priority -= 20;
  }
  
  return priority;
}

}  // namespace cedar
