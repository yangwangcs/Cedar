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
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>

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

void CompactionStats::RecordCompaction(const CompactionTask& task, uint64_t duration_ms) {
  total_compactions.fetch_add(1);
  total_input_files.fetch_add(task.input_files.size() + task.overlapping_files.size());
  total_output_files.fetch_add(1);  // 预估
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
  
  // 保存 MANIFEST
  Status s = SaveManifest();
  
  opened_.store(false);
  return s;
}

// =============================================================================
// 文件管理
// =============================================================================

Status SizeTieredCompactionEngine::AddSSTFile(const ZoneSstMeta& meta) {
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
    
    // 更新文件编号计数器
    if (meta.file_number >= next_file_number_.load()) {
      next_file_number_.store(meta.file_number + 1);
    }
    
    // 注册 Blob 引用
    if (meta.blob_file_number != 0) {
      blob_manager_.RegisterBlobFile(meta.blob_file_number, meta.blob_file_size);
      blob_manager_.AddReference(meta.blob_file_number, meta.file_number);
    }
  }
  
  // 检查是否需要立即触发合并
  if (meta.level == 0 && config_.enable_background_compaction) {
    if (levels_[0].needs_compaction(config_)) {
      ScheduleCompaction();
    }
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

std::optional<CompactionTask> SizeTieredCompactionEngine::PickNextCompaction() {
  std::lock_guard<std::mutex> lock(levels_mutex_);
  
  // 优先级：L0 > L1 > L2 > ... （从下往上）
  for (int level = 0; level < config_.max_levels - 1; ++level) {
    if (!levels_[level].needs_compaction(config_)) {
      continue;
    }
    
    // 检查是否有文件正在被合并
    bool files_available = true;
    for (const auto& f : levels_[level].files) {
      std::lock_guard<std::mutex> compact_lock(compacting_mutex_);
      if (compacting_files_.count(f.file_number) > 0) {
        files_available = false;
        break;
      }
    }
    
    if (!files_available) {
      continue;
    }
    
    CompactionTask task;
    task.input_level = level;
    task.output_level = level + 1;
    task.input_files = levels_[level].files;
    
    // 限制合并宽度
    if (task.input_files.size() > config_.max_merge_width) {
      // 按文件大小排序，优先合并小文件
      std::sort(task.input_files.begin(), task.input_files.end(),
                [](const ZoneSstMeta& a, const ZoneSstMeta& b) {
                  return a.file_size < b.file_size;
                });
      task.input_files.resize(config_.max_merge_width);
    }
    
    // 计算输入范围
    uint64_t min_entity = UINT64_MAX, max_entity = 0;
    uint64_t min_ts = UINT64_MAX, max_ts = 0;
    for (const auto& f : task.input_files) {
      min_entity = std::min(min_entity, f.min_entity_id);
      max_entity = std::max(max_entity, f.max_entity_id);
      min_ts = std::min(min_ts, f.min_timestamp);
      max_ts = std::max(max_ts, f.max_timestamp);
    }
    
    // 查找下层重叠文件
    if (level + 1 < config_.max_levels) {
      task.overlapping_files = levels_[level + 1].FindOverlapping(
          min_entity, max_entity, min_ts, max_ts);
    }
    
    // 计算预估输出大小
    task.estimated_output_size = 0;
    for (const auto& f : task.input_files) {
      task.estimated_output_size += f.file_size;
    }
    for (const auto& f : task.overlapping_files) {
      task.estimated_output_size += f.file_size;
    }
    // 假设压缩率 0.7
    task.estimated_output_size = static_cast<uint64_t>(task.estimated_output_size * 0.7);
    
    // 计算优先级
    task.priority = CalculateCompactionPriority(levels_[level], config_);
    
    return task;
  }
  
  return std::nullopt;
}

Status SizeTieredCompactionEngine::ExecuteCompaction(const CompactionTask& task) {
  auto start_time = std::chrono::steady_clock::now();
  
  // 标记文件正在合并
  {
    std::lock_guard<std::mutex> lock(compacting_mutex_);
    for (const auto& f : task.input_files) {
      compacting_files_.insert(f.file_number);
    }
    for (const auto& f : task.overlapping_files) {
      compacting_files_.insert(f.file_number);
    }
  }
  
  // 执行合并
  Status s = DoZoneCompaction(task);
  
  // 解除标记
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
    stats_.RecordCompaction(task, static_cast<uint64_t>(duration));
  }
  
  return s;
}

void SizeTieredCompactionEngine::ScheduleCompaction() {
  std::lock_guard<std::mutex> lock(queue_mutex_);
  
  auto task = PickNextCompaction();
  if (task.has_value()) {
    pending_tasks_.push(task.value());
    queue_cv_.notify_one();
  }
}

void SizeTieredCompactionEngine::WaitForCompactions() {
  while (true) {
    {
      std::lock_guard<std::mutex> lock(queue_mutex_);
      if (pending_tasks_.empty() && stats_.active_compactions.load() == 0) {
        break;
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

Status SizeTieredCompactionEngine::CompactAll() {
  // 逐层强制合并
  for (int level = 0; level < config_.max_levels - 1; ++level) {
    while (true) {
      auto task = PickNextCompaction();
      if (!task.has_value() || task->input_level != level) {
        break;
      }
      
      Status s = ExecuteCompaction(task.value());
      if (!s.ok()) {
        return s;
      }
    }
  }
  return Status::OK();
}

// =============================================================================
// 后台线程
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
      // 记录错误，继续处理其他任务
      // TODO: 添加错误日志
    }
    
    // 继续调度可能的新任务
    ScheduleCompaction();
  }
}



// =============================================================================
// Zone-Columnar 合并实现（核心算法）
// =============================================================================

Status SizeTieredCompactionEngine::DoZoneCompaction(const CompactionTask& task) {
  // 生成输出文件编号
  uint64_t output_file_number = NewFileNumber();
  std::string output_path = GetSstPath(output_file_number);
  std::string output_blob_path = GetBlobPath(output_file_number);
  
  // 创建 WritableFile
  WritableFile* writable_file_raw;
  Status s = env_->NewWritableFile(output_path, &writable_file_raw);
  if (!s.ok()) {
    return s;
  }
  std::unique_ptr<WritableFile> writable_file(writable_file_raw);
  if (!s.ok()) {
    return s;
  }
  
  // 创建临时 CedarOptions
  CedarOptions options;
  options.env = env_;
  options.column_id = 0;  // 默认列，后续从输入文件推断
  if (!task.input_files.empty()) {
    options.column_id = task.input_files[0].column_id;
  }
  
  // 创建输出 Builder
  ZoneColumnarSstBuilder builder(options, writable_file.get(), options.column_id, db_path_);
  builder.SetLevel(task.output_level);
  builder.SetFileNumber(output_file_number);
  
  // 执行多路归并
  s = MergeZones(task.input_files, &builder);
  if (!s.ok()) {
    builder.Abandon();
    return s;
  }
  
  // 完成构建
  s = builder.Finish();
  if (!s.ok()) {
    return s;
  }
  
  // 获取文件大小
  uint64_t file_size = builder.FileSize();
  uint64_t num_entries = builder.NumEntries();
  
  // 构建输出元数据
  ZoneSstMeta output_meta;
  output_meta.file_number = output_file_number;
  output_meta.level = task.output_level;
  output_meta.file_size = file_size;
  output_meta.num_entries = num_entries;
  output_meta.path = output_path;
  output_meta.blob_path = output_blob_path;
  
  // 从输入文件推断范围（实际应该从 builder 获取）
  if (!task.input_files.empty()) {
    output_meta.column_id = task.input_files[0].column_id;
    output_meta.entity_type = task.input_files[0].entity_type;
    
    // 计算合并后的范围
    uint64_t min_entity = UINT64_MAX, max_entity = 0;
    uint64_t min_ts = UINT64_MAX, max_ts = 0;
    for (const auto& f : task.input_files) {
      min_entity = std::min(min_entity, f.min_entity_id);
      max_entity = std::max(max_entity, f.max_entity_id);
      min_ts = std::min(min_ts, f.min_timestamp);
      max_ts = std::max(max_ts, f.max_timestamp);
    }
    for (const auto& f : task.overlapping_files) {
      min_entity = std::min(min_entity, f.min_entity_id);
      max_entity = std::max(max_entity, f.max_entity_id);
      min_ts = std::min(min_ts, f.min_timestamp);
      max_ts = std::max(max_ts, f.max_timestamp);
    }
    output_meta.min_entity_id = min_entity;
    output_meta.max_entity_id = max_entity;
    output_meta.min_timestamp = min_ts;
    output_meta.max_timestamp = max_ts;
  }
  
  // 添加到新层级
  {
    std::lock_guard<std::mutex> lock(levels_mutex_);
    levels_[task.output_level].files.push_back(output_meta);
  }
  
  // 从旧层级移除输入文件
  for (const auto& f : task.input_files) {
    RemoveFileFromLevel(f.file_number, task.input_level);
  }
  
  // 从下层移除重叠文件
  for (const auto& f : task.overlapping_files) {
    RemoveFileFromLevel(f.file_number, task.output_level);
  }
  
  // 保存 MANIFEST
  s = SaveManifest();
  
  return s;
}

// 归并迭代器辅助结构
struct MergeIteratorItem {
  std::unique_ptr<ZoneColumnarSstReader> reader;
  std::unique_ptr<ZoneColumnarIterator> iterator;
  CedarKey current_key;
  Descriptor current_value;
  size_t file_index;
  
  bool valid = false;
  
  void Next() {
    if (iterator && iterator->Valid()) {
      iterator->Next();
      UpdateCurrent();
    }
  }
  
  void UpdateCurrent() {
    if (iterator && iterator->Valid()) {
      current_key = iterator->Key();
      current_value = iterator->Value();
      valid = true;
    } else {
      valid = false;
    }
  }
};

Status SizeTieredCompactionEngine::MergeZones(
    const std::vector<ZoneSstMeta>& inputs,
    ZoneColumnarSstBuilder* output) {
  
  // 打开所有输入文件
  std::vector<std::unique_ptr<MergeIteratorItem>> items;
  
  for (size_t i = 0; i < inputs.size(); ++i) {
    auto item = std::make_unique<MergeIteratorItem>();
    item->file_index = i;
    
    item->reader = std::make_unique<ZoneColumnarSstReader>(inputs[i].path);
    Status s = item->reader->Open();
    if (!s.ok()) {
      return s;
    }
    
    item->iterator = std::make_unique<ZoneColumnarIterator>(item->reader.get());
    item->iterator->SeekToFirst();
    item->UpdateCurrent();
    
    if (item->valid) {
      items.push_back(std::move(item));
    }
  }
  
  // 批量写入缓冲区
  const size_t kBatchSize = 65536;
  std::vector<CedarKey> key_buffer;
  std::vector<Descriptor> value_buffer;
  key_buffer.reserve(kBatchSize);
  value_buffer.reserve(kBatchSize);
  
  // 归并循环
  while (!items.empty()) {
    // 找到最小的 Key
    size_t min_idx = 0;
    for (size_t i = 1; i < items.size(); ++i) {
      if (items[i]->current_key < items[min_idx]->current_key) {
        min_idx = i;
      }
    }
    
    auto& min_item = items[min_idx];
    CedarKey key = min_item->current_key;
    Descriptor value = min_item->current_value;
    
    // 处理相同 Key 的去重（保留最新版本）
    // 注：CedarKey 包含 timestamp 和 sequence，相同逻辑 Key 会有不同 timestamp
    // 这里我们假设输入已经按 timestamp 降序排列，所以遇到相同 Key 跳过即可
    
    // 处理 Tombstone（在较低层级可以清理）
    // TODO: 传递 output level 给 ShouldDropTombstone
    
    // 处理 Blob 引用
    if (value.AsExternalRef().has_value()) {
      value = HandleBlobReference(value, output, inputs[min_item->file_index]);
    }
    
    // 添加到缓冲区
    key_buffer.push_back(key);
    value_buffer.push_back(value);
    
    // 批量写入
    if (key_buffer.size() >= kBatchSize) {
      for (size_t i = 0; i < key_buffer.size(); ++i) {
        output->Add(key_buffer[i], value_buffer[i]);
      }
      key_buffer.clear();
      value_buffer.clear();
    }
    
    // 移动到下一个
    min_item->Next();
    if (!min_item->valid) {
      items.erase(items.begin() + min_idx);
    }
  }
  
  // 刷盘剩余数据
  if (!key_buffer.empty()) {
    for (size_t i = 0; i < key_buffer.size(); ++i) {
      output->Add(key_buffer[i], value_buffer[i]);
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

void SizeTieredCompactionEngine::RemoveFileFromLevel(uint64_t file_number, int level) {
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
      
      // 删除物理文件
      std::error_code ec;
      std::filesystem::remove(it->path, ec);
      
      files.erase(it);
      break;
    }
  }
}

Status SizeTieredCompactionEngine::SaveManifest() {
  std::string manifest_path = GetManifestPath();
  std::string tmp_path = manifest_path + ".tmp";
  
  std::ofstream file(tmp_path, std::ios::binary);
  if (!file) {
    return Status::IOError("SizeTieredCompactionEngine", "Cannot open manifest for writing");
  }
  
  std::lock_guard<std::mutex> lock(levels_mutex_);
  
  // 写入下一个文件编号
  uint64_t next_file = next_file_number_.load();
  file.write(reinterpret_cast<const char*>(&next_file), sizeof(next_file));
  
  // 写入层级数量
  int32_t num_levels = static_cast<int32_t>(levels_.size());
  file.write(reinterpret_cast<const char*>(&num_levels), sizeof(num_levels));
  
  // 写入每个层级的文件
  for (const auto& level : levels_) {
    int32_t file_count = static_cast<int32_t>(level.files.size());
    file.write(reinterpret_cast<const char*>(&file_count), sizeof(file_count));
    
    for (const auto& f : level.files) {
      file.write(reinterpret_cast<const char*>(&f.file_number), sizeof(f.file_number));
      file.write(reinterpret_cast<const char*>(&f.file_size), sizeof(f.file_size));
      file.write(reinterpret_cast<const char*>(&f.num_entries), sizeof(f.num_entries));
      file.write(reinterpret_cast<const char*>(&f.level), sizeof(f.level));
      file.write(reinterpret_cast<const char*>(&f.min_entity_id), sizeof(f.min_entity_id));
      file.write(reinterpret_cast<const char*>(&f.max_entity_id), sizeof(f.max_entity_id));
      file.write(reinterpret_cast<const char*>(&f.min_timestamp), sizeof(f.min_timestamp));
      file.write(reinterpret_cast<const char*>(&f.max_timestamp), sizeof(f.max_timestamp));
      file.write(reinterpret_cast<const char*>(&f.column_id), sizeof(f.column_id));
      file.write(reinterpret_cast<const char*>(&f.entity_type), sizeof(f.entity_type));
      file.write(reinterpret_cast<const char*>(&f.blob_file_number), sizeof(f.blob_file_number));
      file.write(reinterpret_cast<const char*>(&f.blob_file_size), sizeof(f.blob_file_size));
      
      // 写入路径
      uint32_t path_len = static_cast<uint32_t>(f.path.size());
      file.write(reinterpret_cast<const char*>(&path_len), sizeof(path_len));
      file.write(f.path.data(), path_len);
    }
  }
  
  file.close();
  
  // 原子重命名
  std::error_code ec;
  std::filesystem::rename(tmp_path, manifest_path, ec);
  if (ec) {
    return Status::IOError("SizeTieredCompactionEngine", "Failed to rename manifest");
  }
  
  return Status::OK();
}

Status SizeTieredCompactionEngine::LoadManifest() {
  std::string manifest_path = GetManifestPath();
  
  if (!std::filesystem::exists(manifest_path)) {
    return Status::NotFound("SizeTieredCompactionEngine", "Manifest not found");
  }
  
  std::ifstream file(manifest_path, std::ios::binary);
  if (!file) {
    return Status::IOError("SizeTieredCompactionEngine", "Cannot open manifest");
  }
  
  // 读取下一个文件编号
  uint64_t next_file;
  file.read(reinterpret_cast<char*>(&next_file), sizeof(next_file));
  next_file_number_.store(next_file);
  
  // 读取层级数量
  int32_t num_levels;
  file.read(reinterpret_cast<char*>(&num_levels), sizeof(num_levels));
  
  levels_.clear();
  levels_.resize(num_levels);
  
  // 读取每个层级的文件
  for (int level = 0; level < num_levels; ++level) {
    levels_[level].level = level;
    
    int32_t file_count;
    file.read(reinterpret_cast<char*>(&file_count), sizeof(file_count));
    
    for (int i = 0; i < file_count; ++i) {
      ZoneSstMeta meta;
      
      file.read(reinterpret_cast<char*>(&meta.file_number), sizeof(meta.file_number));
      file.read(reinterpret_cast<char*>(&meta.file_size), sizeof(meta.file_size));
      file.read(reinterpret_cast<char*>(&meta.num_entries), sizeof(meta.num_entries));
      file.read(reinterpret_cast<char*>(&meta.level), sizeof(meta.level));
      file.read(reinterpret_cast<char*>(&meta.min_entity_id), sizeof(meta.min_entity_id));
      file.read(reinterpret_cast<char*>(&meta.max_entity_id), sizeof(meta.max_entity_id));
      file.read(reinterpret_cast<char*>(&meta.min_timestamp), sizeof(meta.min_timestamp));
      file.read(reinterpret_cast<char*>(&meta.max_timestamp), sizeof(meta.max_timestamp));
      file.read(reinterpret_cast<char*>(&meta.column_id), sizeof(meta.column_id));
      file.read(reinterpret_cast<char*>(&meta.entity_type), sizeof(meta.entity_type));
      file.read(reinterpret_cast<char*>(&meta.blob_file_number), sizeof(meta.blob_file_number));
      file.read(reinterpret_cast<char*>(&meta.blob_file_size), sizeof(meta.blob_file_size));
      
      // 读取路径
      uint32_t path_len;
      file.read(reinterpret_cast<char*>(&path_len), sizeof(path_len));
      meta.path.resize(path_len);
      file.read(meta.path.data(), path_len);
      
      levels_[level].files.push_back(meta);
      
      // 注册 Blob 引用
      if (meta.blob_file_number != 0) {
        blob_manager_.RegisterBlobFile(meta.blob_file_number, meta.blob_file_size);
        blob_manager_.AddReference(meta.blob_file_number, meta.file_number);
      }
    }
  }
  
  return file.good() ? Status::OK() : Status::IOError("SizeTieredCompactionEngine", "Manifest read failed");
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
