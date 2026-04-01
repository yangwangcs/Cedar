// Copyright (c) 2025 The Cedar Authors. All rights reserved.

#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/compaction_merger.h"
// DEPRECATED: Old format - now using ZoneColumnar
// #include "cedar/frond/frond_builder.h"
// #include "cedar/frond/frond_reader.h"
#include "cedar/frond/zone_columnar_format.h"
#include "cedar/transaction/batch_api.h"
#include "cedar/transaction/wal.h"

#include <filesystem>
#include <thread>
#include <chrono>
#include <iostream>
#include <unordered_set>

namespace cedar {

LsmEngine::LsmEngine(const std::string& db_path,
                     const CedarOptions& options,
                     cedar::Env* env)
    : db_path_(db_path),
      options_(options),
      env_(env ? env : cedar::Env::Default()),
      mem_(std::make_unique<VSLMemTable>()),
      opened_(false),
      next_file_number_(1),
      shutdown_(false),
      bg_thread_(nullptr),
      // DEPRECATED: Old format file cache
      // file_cache_(nullptr),
      query_cache_(std::make_unique<QueryCache>(10000)),
      compaction_scheduled_(false),
      has_work_(false),
      disable_column_tracking_(false),
      disable_query_cache_invalidate_(false) {
  // Initialize Size-Tiered Compaction config from CedarOptions
  if (options_.use_zone_columnar_format) {
    compaction_config_.l0_max_size = options_.size_tiered_config.l0_max_size;
    compaction_config_.l0_file_size = options_.size_tiered_config.l0_file_size;
    compaction_config_.l0_max_files = options_.size_tiered_config.l0_max_files;
    compaction_config_.size_ratio = options_.size_tiered_config.size_ratio;
    compaction_config_.max_levels = options_.size_tiered_config.max_levels;
    compaction_config_.level_size_trigger_ratio = options_.size_tiered_config.level_size_trigger_ratio;
    compaction_config_.max_merge_width = options_.size_tiered_config.max_merge_width;
    compaction_config_.compaction_threads = options_.size_tiered_config.compaction_threads;
    compaction_config_.enable_background_compaction = options_.size_tiered_config.enable_background_compaction;
    compaction_config_.tombstone_cleanup_level = options_.size_tiered_config.tombstone_cleanup_level;
    compaction_config_.blob_rewrite_threshold = options_.size_tiered_config.blob_rewrite_threshold;
  }
}

LsmEngine::~LsmEngine() {
  Close();
}

Status LsmEngine::Open() {
  if (options_.create_if_missing) {
    if (!std::filesystem::exists(db_path_)) {
      std::filesystem::create_directories(db_path_);
    }
  }

  if (!std::filesystem::exists(db_path_)) {
    return Status::InvalidArgument("LsmEngine", "database path does not exist");
  }

  // Initialize levels vector
  levels_.resize(options_.compaction_config.max_levels);

  Status s = LoadSstFiles();
  if (!s.ok()) return s;
  
  // DEPRECATED: Old format file cache - now using ZoneColumnar
  // Initialize file cache
  // file_cache_ = std::make_unique<FrondFileCache>(options_.file_cache_size, env_, &options_);
  
  // Initialize SST reader cache
  sst_reader_cache_ = std::make_unique<SstReaderCache>(options_.file_cache_size > 0 ? options_.file_cache_size : 16);
  
  // Build column-based file index for fast query
  BuildColumnFileIndex();
  
  // Initialize WAL and TransactionManager
  s = InitWAL();
  if (!s.ok()) return s;
  
  // ========== Initialize Size-Tiered Compaction Engine ==========
  // 从 options 中读取配置或使用默认配置
  compaction_engine_ = std::make_unique<SizeTieredCompactionEngine>(
      db_path_, compaction_config_, env_);
  
  s = compaction_engine_->Open();
  if (!s.ok()) {
    return Status::IOError("LsmEngine", "Failed to open compaction engine: " + s.ToString());
  }
  
  // 启动自动 Compaction 后台线程
  auto_compaction_enabled_.store(true);
  auto_compaction_thread_ = new std::thread(&LsmEngine::AutoCompactionThread, this);
  
  // 如果存在现有的 SST 文件，迁移到新的 Compaction 引擎
  MigrateExistingSstFiles();
  
  opened_ = true;
  
  return Status::OK();
}

Status LsmEngine::Close() {
  if (!opened_) return Status::OK();

  shutdown_ = true;

  // 关闭自动 Compaction 线程
  auto_compaction_enabled_.store(false);
  if (auto_compaction_thread_ && auto_compaction_thread_->joinable()) {
    auto_compaction_thread_->join();
    delete auto_compaction_thread_;
    auto_compaction_thread_ = nullptr;
  }

  if (bg_thread_ && bg_thread_->joinable()) {
    bg_thread_->join();
    delete bg_thread_;
    bg_thread_ = nullptr;
  }

  if (imm_) {
    FlushMemTable(imm_.get());
    imm_.reset();
  }

  if (mem_ && !mem_->IsEmpty()) {
    FlushMemTable(mem_.get());
  }
  
  // 关闭 Compaction 引擎
  if (compaction_engine_) {
    compaction_engine_->WaitForCompactions();
    compaction_engine_->Close();
    compaction_engine_.reset();
  }

  opened_ = false;
  return Status::OK();
}

Status LsmEngine::Put(const CedarKey& key, const Descriptor& descriptor, Timestamp txn_version) {
  if (!opened_) {
    return Status::InvalidArgument("LsmEngine", "not opened");
  }

  mem_->Put(key, descriptor, txn_version);
  
  if (!disable_column_tracking_) {
    TrackColumnId(key.entity_id(), descriptor.GetColumnId());
  }
  
  return Status::OK();
}

Status LsmEngine::Put(uint64_t entity_id, uint64_t tx_time, const Slice& value, Timestamp txn_version) {
  CedarKey key(entity_id, EntityType::Vertex, 0, Timestamp(tx_time));
  auto desc_opt = Descriptor::InlineShortStr(0, value);
  if (!desc_opt.has_value()) {
    return Status::InvalidArgument("LsmEngine", "value too long for InlineShortStr, use ExternalRef");
  }
  return Put(key, *desc_opt, txn_version);
}

Status LsmEngine::Delete(const CedarKey& key, Timestamp txn_version) {
  if (!opened_) {
    return Status::InvalidArgument("LsmEngine", "not opened");
  }
  // VSLMemTable doesn't have Delete, use Tombstone descriptor
  Put(key, Descriptor(), txn_version);
  return Status::OK();
}

Status LsmEngine::Delete(uint64_t entity_id, uint64_t tx_time, Timestamp txn_version) {
  CedarKey key(entity_id, EntityType::Vertex, 0, Timestamp(tx_time));
  return Delete(key, txn_version);
}

Status LsmEngine::Get(uint64_t entity_id, uint64_t tx_time, std::string* value) {
  return Status::NotSupported("LsmEngine", "legacy Get not supported");
}

std::optional<Descriptor> LsmEngine::Get(const CedarKey& key) {
  if (!opened_) {
    return std::nullopt;
  }

  // 1. Query MemTable (hot data)
  Descriptor desc;
  Status s = mem_->Get(key.entity_id(), key.entity_type(), key.column_id(), 
                       key.timestamp(), &desc);
  if (s.ok()) {
    return desc;
  }

  // 2. Query Immutable MemTable
  if (imm_) {
    s = imm_->Get(key.entity_id(), key.entity_type(), key.column_id(), 
                  key.timestamp(), &desc);
    if (s.ok()) {
      return desc;
    }
  }
  
  // 3. Query SST files via Size-Tiered Compaction Engine
  if (compaction_engine_) {
    // 获取覆盖该 Entity 的所有文件
    auto files = compaction_engine_->GetFilesForEntity(
        key.entity_id(), key.column_id(), static_cast<uint8_t>(key.entity_type()));
    
    // 按时间从新到旧排序（文件号大的通常更新）
    std::sort(files.begin(), files.end(), 
              [](const ZoneSstMeta& a, const ZoneSstMeta& b) {
                return a.file_number > b.file_number;
              });
    
    // 查询每个文件
    for (const auto& file_meta : files) {
      // 快速范围检查
      if (key.entity_id() < file_meta.min_entity_id || 
          key.entity_id() > file_meta.max_entity_id) {
        continue;
      }
      
      // 使用 ZoneColumnarSstReader 查询
      ZoneColumnarSstReader reader(file_meta.path);
      Status open_status = reader.Open();
      if (!open_status.ok()) {
        continue;
      }
      
      auto result = reader.Get(key);
      if (result.has_value()) {
        return result.value();
      }
    }
  }
  
  return std::nullopt;
}

std::vector<MemTableEntry> LsmEngine::GetAll(uint64_t entity_id,
                                              EntityType entity_type,
                                              uint16_t column_id) {
  std::vector<MemTableEntry> results;
  if (!opened_) {
    return results;
  }

  // 1. Query MemTable (hot data)
  auto mem_results = mem_->GetAll(entity_id, entity_type, column_id);
  results.insert(results.end(), mem_results.begin(), mem_results.end());

  // 2. Query Immutable MemTable
  if (imm_) {
    auto imm_results = imm_->GetAll(entity_id, entity_type, column_id);
    results.insert(results.end(), imm_results.begin(), imm_results.end());
  }
  
  // 3. Query SST files via Size-Tiered Compaction Engine
  if (compaction_engine_) {
    auto files = compaction_engine_->GetFilesForEntity(
        entity_id, column_id, static_cast<uint8_t>(entity_type));
    
    for (const auto& file_meta : files) {
      // 快速范围检查
      if (entity_id < file_meta.min_entity_id || 
          entity_id > file_meta.max_entity_id) {
        continue;
      }
      
      ZoneColumnarSstReader reader(file_meta.path);
      if (!reader.Open().ok()) {
        continue;
      }
      
      // 获取该 Entity 的所有版本
      auto range_results = reader.GetRange(entity_id, entity_type, column_id,
                                           Timestamp(0), Timestamp(UINT64_MAX));
      
      for (const auto& [key, descriptor] : range_results) {
        std::optional<uint64_t> dst_id = (key.target_id() != 0) 
            ? std::optional<uint64_t>(key.target_id()) 
            : std::nullopt;
        results.emplace_back(key.timestamp(), descriptor, dst_id, Timestamp(0));
      }
    }
  }
  
  // Sort by timestamp descending, then by dst_id for stable ordering
  std::sort(results.begin(), results.end(), [](const auto& a, const auto& b) {
    if (a.timestamp.value() != b.timestamp.value()) {
      return a.timestamp.value() > b.timestamp.value();
    }
    // For edges, sort by dst_id to ensure stable ordering
    if (a.dst_id.has_value() && b.dst_id.has_value()) {
      return a.dst_id.value() < b.dst_id.value();
    }
    return a.dst_id.has_value() > b.dst_id.has_value();
  });
  
  // Remove duplicates - only if both timestamp AND dst_id are the same
  auto last = std::unique(results.begin(), results.end(), [](const auto& a, const auto& b) {
    if (a.timestamp.value() != b.timestamp.value()) return false;
    // For edges, also check dst_id
    if (a.dst_id.has_value() && b.dst_id.has_value()) {
      return a.dst_id.value() == b.dst_id.value();
    }
    return a.dst_id.has_value() == b.dst_id.has_value();
  });
  results.erase(last, results.end());
  
  return results;
}

std::optional<Descriptor> LsmEngine::GetAtTime(uint64_t entity_id,
                                               EntityType entity_type,
                                               uint16_t column_id,
                                               Timestamp timestamp) {
  if (!opened_) {
    return std::nullopt;
  }

  // 1. Query MemTable (hot data)
  auto desc = mem_->GetAtTime(entity_id, entity_type, column_id, timestamp);
  if (desc.has_value()) {
    return desc;
  }

  // 2. Query Immutable MemTable
  if (imm_) {
    desc = imm_->GetAtTime(entity_id, entity_type, column_id, timestamp);
    if (desc.has_value()) {
      return desc;
    }
  }
  
  // 3. Query SST files via Size-Tiered Compaction Engine
  if (compaction_engine_) {
    auto files = compaction_engine_->GetFilesForEntity(
        entity_id, column_id, static_cast<uint8_t>(entity_type));
    
    // 收集所有匹配的条目
    std::vector<std::pair<CedarKey, Descriptor>> all_entries;
    
    for (const auto& file_meta : files) {
      // 快速范围检查
      if (entity_id < file_meta.min_entity_id || 
          entity_id > file_meta.max_entity_id) {
        continue;
      }
      
      // 使用缓存的 Reader
      std::shared_ptr<ZoneColumnarSstReader> reader;
      if (sst_reader_cache_) {
        reader = sst_reader_cache_->Get(file_meta.path);
      }
      if (!reader) {
        // 缓存未命中，创建新的
        reader = std::make_shared<ZoneColumnarSstReader>(file_meta.path);
        if (!reader->Open().ok()) {
          continue;
        }
      }
      
      // 使用 GetRange 获取该 Entity 的所有版本
      auto range_results = reader->GetRange(entity_id, entity_type, column_id,
                                           Timestamp(0), Timestamp(UINT64_MAX));
      all_entries.insert(all_entries.end(), range_results.begin(), range_results.end());
      
      // 释放 Reader（减少引用计数）
      if (sst_reader_cache_) {
        sst_reader_cache_->Release(file_meta.path);
      }
    }
    
    // 按时间戳降序排序
    std::sort(all_entries.begin(), all_entries.end(),
              [](const auto& a, const auto& b) {
                return a.first.timestamp().value() > b.first.timestamp().value();
              });
    
    // 找到指定时间的版本（<= timestamp 的最新版本）
    for (const auto& [key, descriptor] : all_entries) {
      if (key.timestamp().value() <= timestamp.value()) {
        return descriptor;
      }
    }
  }
  
  return std::nullopt;
}

std::vector<MemTableEntry> LsmEngine::GetRange(uint64_t entity_id,
                                                EntityType entity_type,
                                                uint16_t column_id,
                                                Timestamp start,
                                                Timestamp end) {
  std::vector<MemTableEntry> results;
  if (!opened_) {
    return results;
  }

  // 1. Query MemTable (hot data)
  auto mem_results = mem_->GetRange(entity_id, entity_type, column_id, start, end);
  results.insert(results.end(), mem_results.begin(), mem_results.end());

  // 2. Query Immutable MemTable
  if (imm_) {
    auto imm_results = imm_->GetRange(entity_id, entity_type, column_id, start, end);
    results.insert(results.end(), imm_results.begin(), imm_results.end());
  }
  
  // 3. Query SST files via Size-Tiered Compaction Engine
  if (compaction_engine_) {
    auto files = compaction_engine_->GetFilesForEntity(
        entity_id, column_id, static_cast<uint8_t>(entity_type));
    
    for (const auto& file_meta : files) {
      // 快速范围检查
      if (entity_id < file_meta.min_entity_id || 
          entity_id > file_meta.max_entity_id) {
        continue;
      }
      
      // 时间范围快速检查
      if (file_meta.max_timestamp < start.value() || 
          file_meta.min_timestamp > end.value()) {
        continue;
      }
      
      // 使用缓存的 Reader
      std::shared_ptr<ZoneColumnarSstReader> reader;
      if (sst_reader_cache_) {
        reader = sst_reader_cache_->Get(file_meta.path);
      }
      if (!reader) {
        reader = std::make_shared<ZoneColumnarSstReader>(file_meta.path);
        if (!reader->Open().ok()) {
          continue;
        }
      }
      
      // 使用 GetRange 获取时间范围内的版本
      auto range_results = reader->GetRange(entity_id, entity_type, column_id, start, end);
      
      // 释放 Reader
      if (sst_reader_cache_) {
        sst_reader_cache_->Release(file_meta.path);
      }
      
      for (const auto& [key, descriptor] : range_results) {
        std::optional<uint64_t> dst_id = (key.target_id() != 0) 
            ? std::optional<uint64_t>(key.target_id()) 
            : std::nullopt;
        results.emplace_back(key.timestamp(), descriptor, dst_id, Timestamp(0));
      }
    }
  }
  
  return results;
}

std::vector<MemTableEntry> LsmEngine::GetRangeLimit(uint64_t entity_id,
                                                     EntityType entity_type,
                                                     uint16_t column_id,
                                                     Timestamp start,
                                                     Timestamp end,
                                                     size_t max_results) {
  if (!opened_) {
    return {};
  }

  // OPTIMIZATION: 追踪查询模式（用于识别热数据）
  TrackQueryPattern(entity_id, entity_type, column_id);
  
  // OPTIMIZATION: 尝试从跨查询缓存获取（如果是全范围查询）
  if (start.value() == 0 && end.value() == UINT64_MAX) {
    auto cached = GetFromCrossQueryCache(entity_id, column_id);
    if (cached.has_value()) {
      return cached.value();
    }
  }
  
  // OPTIMIZATION: 预读取热数据到缓存
  PrefetchHotData(entity_id, entity_type, column_id);

  std::vector<MemTableEntry> result;
  // 修复: 只有当 max_results 是合理值时才预分配，避免 SIZE_MAX 导致内存分配失败
  if (max_results > 0 && max_results < 1000000) {
    result.reserve(max_results);
  } else if (max_results > 0) {
    result.reserve(1024);  // 使用合理的默认值
  }
  
  // 1. Query MemTable (hot data)
  result = mem_->GetRange(entity_id, entity_type, column_id, start, end);
  if (result.size() >= max_results) {
    result.resize(max_results);
    return result;
  }

  // 2. Query Immutable MemTable
  if (imm_) {
    auto imm_result = imm_->GetRange(entity_id, entity_type, column_id, start, end);
    for (const auto& entry : imm_result) {
      if (result.size() >= max_results) {
        return result;
      }
      result.push_back(entry);
    }
  }
  
  // 3. Query SST files via Size-Tiered Compaction Engine
  if (result.size() < max_results && compaction_engine_) {
    auto files = compaction_engine_->GetFilesForEntity(
        entity_id, column_id, static_cast<uint8_t>(entity_type));
    
    // OPTIMIZATION: 按时间戳范围排序文件，优先查询时间范围匹配度高的文件
    // 同时限制最大查询文件数，避免扫描过多文件
    std::vector<std::pair<ZoneSstMeta, uint64_t>> files_with_overlap;
    files_with_overlap.reserve(files.size());
    
    for (const auto& file_meta : files) {
      // 快速范围检查: entity_id
      if (entity_id < file_meta.min_entity_id || 
          entity_id > file_meta.max_entity_id) {
        continue;
      }
      
      // 快速范围检查: 时间戳范围
      if (file_meta.max_timestamp < start.value() || 
          file_meta.min_timestamp > end.value()) {
        continue;
      }
      
      // 计算时间范围重叠度（用于排序）
      uint64_t overlap_start = std::max(file_meta.min_timestamp, start.value());
      uint64_t overlap_end = std::min(file_meta.max_timestamp, end.value());
      uint64_t overlap = (overlap_end > overlap_start) ? (overlap_end - overlap_start) : 0;
      
      files_with_overlap.push_back({file_meta, overlap});
    }
    
    // 按时间范围重叠度降序排序（优先查询重叠度高的文件）
    std::sort(files_with_overlap.begin(), files_with_overlap.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });
    
    // OPTIMIZATION: 限制最大查询文件数
    // 对于时间范围查询，通常只需要检查最近的几个文件
    const size_t kMaxFilesToQuery = 10;
    size_t files_queried = 0;
    
    for (const auto& [file_meta, overlap] : files_with_overlap) {
      if (result.size() >= max_results) {
        break;
      }
      
      if (++files_queried > kMaxFilesToQuery) {
        break;  // 已达到最大文件查询限制
      }
      
      // OPTIMIZATION: 使用缓存的 Reader（如果可用）
      std::shared_ptr<ZoneColumnarSstReader> reader;
      if (sst_reader_cache_) {
        reader = sst_reader_cache_->Get(file_meta.path);
      }
      if (!reader) {
        reader = std::make_shared<ZoneColumnarSstReader>(file_meta.path);
        if (!reader->Open().ok()) {
          continue;
        }
      }
      
      auto range_results = reader->GetRange(entity_id, entity_type, column_id, start, end);
      
      // 释放 Reader
      if (sst_reader_cache_) {
        sst_reader_cache_->Release(file_meta.path);
      }
      
      for (const auto& [key, descriptor] : range_results) {
        if (result.size() >= max_results) {
          break;
        }
        std::optional<uint64_t> dst_id = (key.target_id() != 0) 
            ? std::optional<uint64_t>(key.target_id()) 
            : std::nullopt;
        result.emplace_back(key.timestamp(), descriptor, dst_id, Timestamp(0));
      }
    }
  }
  
  // OPTIMIZATION: 将全范围查询结果缓存
  if (start.value() == 0 && end.value() == UINT64_MAX && !result.empty()) {
    AddToCrossQueryCache(entity_id, column_id, result);
  }
  
  return result;
}

void LsmEngine::BatchGetAtTime(std::vector<BatchQueryItem>& items) {
  for (auto& item : items) {
    item.result = GetAtTime(item.entity_id, item.entity_type, item.column_id, item.timestamp);
  }
}

void LsmEngine::BatchGetRange(std::vector<BatchRangeItem>& items) {
  for (auto& item : items) {
    item.results = GetRangeLimit(item.entity_id, item.entity_type, item.column_id, 
                                  item.start, item.end, item.max_results);
  }
}

// OPTIMIZATION: 并行 BatchGetRange - 使用线程池加速多 entity 查询
void LsmEngine::ParallelBatchGetRange(std::vector<BatchRangeItem>& items, 
                                       size_t num_threads) {
  if (items.empty()) return;
  
  // 如果只有少量查询，使用串行版本
  if (items.size() < num_threads * 2) {
    BatchGetRange(items);
    return;
  }
  
  // 限制线程数
  num_threads = std::min(num_threads, items.size());
  
  // 计算每个线程处理的查询数量
  size_t items_per_thread = items.size() / num_threads;
  size_t remainder = items.size() % num_threads;
  
  std::vector<std::thread> threads;
  size_t start_idx = 0;
  
  for (size_t t = 0; t < num_threads; ++t) {
    size_t count = items_per_thread + (t < remainder ? 1 : 0);
    if (count == 0) break;
    
    threads.emplace_back([this, &items, start_idx, count]() {
      for (size_t i = start_idx; i < start_idx + count; ++i) {
        auto& item = items[i];
        item.results = GetRangeLimit(item.entity_id, item.entity_type, item.column_id,
                                      item.start, item.end, item.max_results);
      }
    });
    
    start_idx += count;
  }
  
  // 等待所有线程完成
  for (auto& t : threads) {
    t.join();
  }
}

// OPTIMIZATION: P0 - 优化的批量时间范围查询
// 使用 SST 层的 BatchGetRange 减少文件扫描次数
std::unordered_map<uint64_t, std::vector<MemTableEntry>> 
LsmEngine::BatchGetRangeOptimized(const std::vector<uint64_t>& entity_ids,
                                   EntityType entity_type,
                                   uint16_t column_id,
                                   Timestamp start,
                                   Timestamp end,
                                   size_t max_results_per_entity) {
  std::unordered_map<uint64_t, std::vector<MemTableEntry>> results;
  
  if (!opened_ || entity_ids.empty()) return results;
  
  // 预分配结果空间
  for (uint64_t eid : entity_ids) {
    results[eid].reserve(max_results_per_entity);
  }
  
  // 1. 从 MemTable 查询（热数据）
  for (uint64_t eid : entity_ids) {
    auto mem_results = mem_->GetRange(eid, entity_type, column_id, start, end);
    for (const auto& entry : mem_results) {
      if (results[eid].size() >= max_results_per_entity) break;
      results[eid].push_back(entry);
    }
  }
  
  // 2. 从 Immutable MemTable 查询
  if (imm_) {
    for (uint64_t eid : entity_ids) {
      if (results[eid].size() >= max_results_per_entity) continue;
      
      auto imm_results = imm_->GetRange(eid, entity_type, column_id, start, end);
      for (const auto& entry : imm_results) {
        if (results[eid].size() >= max_results_per_entity) break;
        results[eid].push_back(entry);
      }
    }
  }
  
  // 3. 从 SST 文件批量查询
  if (compaction_engine_) {
    // 获取所有相关的 SST 文件（使用第一个 entity 获取文件列表）
    // 注意：这里假设所有 entity 的数据分布在相同的文件中
    auto files = compaction_engine_->GetFilesForEntity(
        entity_ids[0], column_id, static_cast<uint8_t>(entity_type));
    
    // 过滤时间范围相关的文件
    std::vector<ZoneSstMeta> relevant_files;
    for (const auto& file_meta : files) {
      if (file_meta.max_timestamp < start.value() || 
          file_meta.min_timestamp > end.value()) {
        continue;  // 时间范围不重叠
      }
      relevant_files.push_back(file_meta);
    }
    
    // 限制文件数量
    const size_t kMaxFilesToQuery = 10;
    if (relevant_files.size() > kMaxFilesToQuery) {
      relevant_files.resize(kMaxFilesToQuery);
    }
    
    // 对每个 SST 文件使用批量查询
    for (const auto& file_meta : relevant_files) {
      // 检查是否所有 entity 都已经收集够数据
      bool all_full = true;
      for (uint64_t eid : entity_ids) {
        if (results[eid].size() < max_results_per_entity) {
          all_full = false;
          break;
        }
      }
      if (all_full) break;
      
      // 使用缓存的 Reader
      std::shared_ptr<ZoneColumnarSstReader> reader;
      if (sst_reader_cache_) {
        reader = sst_reader_cache_->Get(file_meta.path);
      }
      if (!reader) {
        reader = std::make_shared<ZoneColumnarSstReader>(file_meta.path);
        if (!reader->Open().ok()) continue;
      }
      
      // 批量查询该文件中的所有 entity
      auto file_results = reader->BatchGetRange(entity_ids, entity_type, column_id, start, end);
      
      // 合并结果
      for (const auto& [eid, entries] : file_results) {
        if (results[eid].size() >= max_results_per_entity) continue;
        
        for (const auto& [key, desc] : entries) {
          if (results[eid].size() >= max_results_per_entity) break;
          
          std::optional<uint64_t> dst_id = (key.target_id() != 0) 
              ? std::optional<uint64_t>(key.target_id()) 
              : std::nullopt;
          results[eid].emplace_back(key.timestamp(), desc, dst_id, Timestamp(0));
        }
      }
      
      // 释放 Reader
      if (sst_reader_cache_) {
        sst_reader_cache_->Release(file_meta.path);
      }
    }
  }
  
  return results;
}

// OPTIMIZATION: P2 - 并行查询多个 SST 文件
std::unordered_map<uint64_t, std::vector<MemTableEntry>>
LsmEngine::ParallelGetRangeFromSST(const std::vector<uint64_t>& entity_ids,
                                     EntityType entity_type,
                                     uint16_t column_id,
                                     Timestamp start,
                                     Timestamp end,
                                     size_t max_results_per_entity,
                                     size_t num_threads) {
  std::unordered_map<uint64_t, std::vector<MemTableEntry>> results;
  
  if (!opened_ || !compaction_engine_ || entity_ids.empty()) return results;
  
  // 预分配结果空间
  for (uint64_t eid : entity_ids) {
    results[eid].reserve(max_results_per_entity);
  }
  
  // 获取相关 SST 文件
  auto files = compaction_engine_->GetFilesForEntity(
      entity_ids[0], column_id, static_cast<uint8_t>(entity_type));
  
  // 过滤时间范围相关的文件
  std::vector<ZoneSstMeta> relevant_files;
  for (const auto& file_meta : files) {
    if (file_meta.max_timestamp < start.value() || 
        file_meta.min_timestamp > end.value()) {
      continue;
    }
    relevant_files.push_back(file_meta);
  }
  
  if (relevant_files.empty()) return results;
  
  // 限制文件数量
  const size_t kMaxFilesToQuery = 20;
  if (relevant_files.size() > kMaxFilesToQuery) {
    relevant_files.resize(kMaxFilesToQuery);
  }
  
  // 如果文件数量少，使用串行查询
  if (relevant_files.size() <= 2) {
    return BatchGetRangeOptimized(entity_ids, entity_type, column_id, 
                                   start, end, max_results_per_entity);
  }
  
  // 并行查询多个文件
  num_threads = std::min(num_threads, relevant_files.size());
  size_t files_per_thread = relevant_files.size() / num_threads;
  size_t remainder = relevant_files.size() % num_threads;
  
  // 每个线程的结果
  std::vector<std::unordered_map<uint64_t, std::vector<std::pair<CedarKey, Descriptor>>>>
      thread_results(num_threads);
  
  std::vector<std::thread> threads;
  size_t start_idx = 0;
  
  // 构建 entity_set 用于快速查找
  std::unordered_set<uint64_t> entity_set(entity_ids.begin(), entity_ids.end());
  
  for (size_t t = 0; t < num_threads; ++t) {
    size_t count = files_per_thread + (t < remainder ? 1 : 0);
    if (count == 0) continue;
    
    threads.emplace_back([this, &relevant_files, &entity_set, &thread_results, 
                          t, start_idx, count, entity_type, column_id, start, end]() {
      for (size_t i = start_idx; i < start_idx + count; ++i) {
        const auto& file_meta = relevant_files[i];
        
        // 使用缓存的 Reader
        std::shared_ptr<ZoneColumnarSstReader> reader;
        if (sst_reader_cache_) {
          reader = sst_reader_cache_->Get(file_meta.path);
        }
        if (!reader) {
          reader = std::make_shared<ZoneColumnarSstReader>(file_meta.path);
          if (!reader->Open().ok()) continue;
        }
        
        // 获取时间范围内的所有数据
        auto positions = reader->GetRange(0, entity_type, column_id, start, end);
        
        // 过滤 entity_ids 中的数据
        for (const auto& [key, desc] : positions) {
          if (entity_set.find(key.entity_id()) != entity_set.end()) {
            thread_results[t][key.entity_id()].emplace_back(key, desc);
          }
        }
        
        // 释放 Reader
        if (sst_reader_cache_) {
          sst_reader_cache_->Release(file_meta.path);
        }
      }
    });
    
    start_idx += count;
  }
  
  // 等待所有线程完成
  for (auto& t : threads) {
    t.join();
  }
  
  // 合并结果
  for (size_t t = 0; t < num_threads; ++t) {
    for (const auto& [eid, entries] : thread_results[t]) {
      if (results[eid].size() >= max_results_per_entity) continue;
      
      for (const auto& [key, desc] : entries) {
        if (results[eid].size() >= max_results_per_entity) break;
        
        std::optional<uint64_t> dst_id = (key.target_id() != 0) 
            ? std::optional<uint64_t>(key.target_id()) 
            : std::nullopt;
        results[eid].emplace_back(key.timestamp(), desc, dst_id, Timestamp(0));
      }
    }
  }
  
  return results;
}

std::vector<LsmEngine::TemporalVersion> LsmEngine::GetTemporalChain(uint64_t entity_id,
                                                                     EntityType entity_type,
                                                                     uint16_t column_id) {
  std::vector<TemporalVersion> result;
  
  // First, get from MemTable (hot data)
  auto mem_entries = mem_->GetAll(entity_id, entity_type, column_id);
  for (const auto& entry : mem_entries) {
    result.push_back({entry.timestamp, entry.descriptor, -1});
  }
  
  // Sort by timestamp descending
  std::sort(result.begin(), result.end(), [](const auto& a, const auto& b) {
    return a.timestamp.value() > b.timestamp.value();
  });
  
  return result;
}

void LsmEngine::TraverseTemporalChain(uint64_t entity_id,
                                       EntityType entity_type,
                                       uint16_t column_id,
                                       std::function<bool(const TemporalVersion&)> callback) {
  auto chain = GetTemporalChain(entity_id, entity_type, column_id);
  for (const auto& version : chain) {
    if (!callback(version)) {
      break;
    }
  }
}

Status LsmEngine::ForceFlush() {
  std::unique_lock<std::shared_mutex> lock(mutex_);

  if (imm_) {
    return Status::IOError("LsmEngine", "flush already in progress");
  }

  if (mem_->IsEmpty()) {
    return Status::OK();
  }

  imm_ = std::move(mem_);
  mem_ = std::make_unique<VSLMemTable>();

  VSLMemTable* imm = imm_.get();
  lock.unlock();

  Status s = FlushMemTable(imm);

  lock.lock();
  imm_.reset();

  return s;
}

Status LsmEngine::Compact() {
  // Stub implementation
  return Status::OK();
}

LsmEngine::Stats LsmEngine::GetStats() const {
  Stats stats;
  stats.memtable_size = mem_->size();
  if (imm_) {
    stats.imm_memtable_size = imm_->size();
  }
  stats.num_levels = levels_.size();
  for (const auto& level : levels_) {
    stats.frond_count += level.size();
    for (const auto& meta : level) {
      stats.frond_size += meta.file_size;
    }
  }
  return stats;
}

std::vector<uint16_t> LsmEngine::GetEntityColumnIds(uint64_t entity_id) {
  std::shared_lock<std::shared_mutex> lock(column_map_mutex_);
  auto it = entity_column_map_.find(entity_id);
  if (it == entity_column_map_.end()) {
    return {};
  }
  return it->second.ToVector();
}

void LsmEngine::TrackColumnId(uint64_t entity_id, uint16_t column_id) {
  if (batch_tracking_enabled_) {
    size_t idx = batch_buffer_index_.fetch_add(1);
    if (idx < kTrackBatchSize) {
      batch_buffer_[idx] = {entity_id, column_id};
    }
    return;
  }
  
  std::unique_lock<std::shared_mutex> lock(column_map_mutex_);
  entity_column_map_[entity_id].Add(column_id);
}

void LsmEngine::FlushColumnIdBatch() {
  if (!batch_tracking_enabled_) return;
  
  size_t count = batch_buffer_index_.load();
  if (count == 0) return;
  
  std::unique_lock<std::shared_mutex> lock(column_map_mutex_);
  for (size_t i = 0; i < count && i < kTrackBatchSize; i++) {
    entity_column_map_[batch_buffer_[i].first].Add(batch_buffer_[i].second);
  }
  batch_buffer_index_.store(0);
}

Status LsmEngine::InitWAL() {
  // 初始化 TransactionManager
  txn_manager_ = std::make_unique<TransactionManager>();
  
  // 初始化 WAL Writer（如果启用）
  if (options_.enable_wal) {
    std::string wal_path = db_path_ + "/wal";
    WalOptions wal_options;
    wal_writer_ = std::make_unique<WalWriter>(wal_path, env_, wal_options);
    Status s = wal_writer_->Open();
    if (!s.ok()) {
      return s;
    }
  }
  
  return Status::OK();
}

std::unique_ptr<OCCTransaction> LsmEngine::BeginTransaction(const TransactionOptions& options) {
  if (!txn_manager_ || !mem_) {
    return nullptr;
  }
  
  // 创建 OCC 事务
  auto txn = std::make_unique<OCCTransaction>(
      txn_manager_.get(),      // TransactionManager
      mem_.get(),              // VSLMemTable
      this,                    // LsmEngine (用于查询 SST)
      wal_writer_.get(),       // WalWriter
      options                  // TransactionOptions
  );
  
  // 开始事务
  Status s = txn->Begin();
  if (!s.ok()) {
    return nullptr;
  }
  
  return txn;
}

Status LsmEngine::SyncWAL() {
  if (wal_writer_) {
    return wal_writer_->Sync();
  }
  return Status::OK();
}

// DEPRECATED: Old format reader cache - now using ZoneColumnarSstReader
// FrondSstReader* LsmEngine::GetCachedReader(const std::string& filepath, uint64_t file_size) {
//   if (!file_cache_) {
//     return nullptr;
//   }
//   return file_cache_->Get(filepath, file_size);
// }

void LsmEngine::InvalidateQueryCache(uint64_t entity_id) {
  if (query_cache_ && !disable_query_cache_invalidate_) {
    query_cache_->Invalidate(entity_id);
  }
}

// 新的 FlushMemTable 实现 - 支持按 entity_type 分组

Status LsmEngine::FlushMemTable(VSLMemTable* mem) {
  if (mem->IsEmpty()) {
    return Status::OK();
  }

  // 按 (entity_type, column_id) 分组收集条目
  std::map<std::pair<uint8_t, uint16_t>, std::vector<std::pair<CedarKey, Descriptor>>> groups;
  
  mem->Traverse([&](const CedarKey& key, const Descriptor& descriptor) -> bool {
    if (!descriptor.IsTombstone()) {
      uint8_t et = static_cast<uint8_t>(key.entity_type());
      uint16_t col = key.column_id();
      groups[{et, col}].emplace_back(key, descriptor);
    }
    return true;
  });
  
  // 为每个组创建单独的 SST 文件
  for (auto& [type_col, entries] : groups) {
    auto [entity_type, column_id] = type_col;
    Status s = FlushEntityGroup(entity_type, column_id, entries);
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

Status LsmEngine::FlushEntityGroup(uint8_t entity_type, uint16_t column_id,
                                   const std::vector<std::pair<CedarKey, Descriptor>>& entries) {
  if (entries.empty()) {
    return Status::OK();
  }

  uint64_t file_number = next_file_number_.fetch_add(1);
  std::string filepath = SstFilePath(file_number);

  uint64_t min_entity_id = UINT64_MAX;
  uint64_t max_entity_id = 0;
  uint64_t min_tx_time = UINT64_MAX;
  uint64_t max_tx_time = 0;
  
  // 复制条目并排序
  auto sorted_entries = entries;
  std::sort(sorted_entries.begin(), sorted_entries.end(),
    [](const auto& a, const auto& b) { return a.first < b.first; });
  
  // 使用 ZoneColumnarSstBuilder
  cedar::WritableFile* file = nullptr;
  cedar::Status ls = env_->NewWritableFile(filepath, &file);
  if (!ls.ok()) {
    return Status::IOError("LsmEngine", ls.ToString());
  }
  
  ZoneColumnarSstBuilder builder(options_, file, column_id, db_path_);
  builder.SetFileNumber(file_number);
  builder.SetLevel(0);
  
  for (const auto& [key, descriptor] : sorted_entries) {
    builder.Add(key, descriptor);
    
    min_entity_id = std::min(min_entity_id, key.entity_id());
    max_entity_id = std::max(max_entity_id, key.entity_id());
    min_tx_time = std::min(min_tx_time, key.timestamp().value());
    max_tx_time = std::max(max_tx_time, key.timestamp().value());
  }

  Status fs = builder.Finish();
  delete file;

  if (!fs.ok()) {
    env_->RemoveFile(filepath);
    return fs;
  }

  uint64_t file_size = 0;
  ls = env_->GetFileSize(filepath, &file_size);
  if (!ls.ok()) {
    return Status::IOError("LsmEngine", ls.ToString());
  }

  FrondFileMeta meta;
  meta.file_number = file_number;
  meta.file_size = file_size;
  meta.num_entries = builder.NumEntries();
  meta.min_entity_id = min_entity_id;
  meta.max_entity_id = max_entity_id;
  meta.min_tx_time = min_tx_time;
  meta.max_tx_time = max_tx_time;
  meta.level = 0;
  meta.column_id = column_id;
  meta.entity_type = entity_type;

  {
    std::lock_guard<std::shared_mutex> lock(mutex_);
    levels_[0].push_back(meta);
    
    std::unique_lock<std::shared_mutex> index_lock(column_index_mutex_);
    column_file_index_[column_id].push_back(&levels_[0].back());
  }
  
  if (compaction_engine_) {
    ZoneSstMeta zone_meta;
    zone_meta.file_number = file_number;
    zone_meta.file_size = file_size;
    zone_meta.num_entries = builder.NumEntries();
    zone_meta.level = 0;
    zone_meta.min_entity_id = min_entity_id;
    zone_meta.max_entity_id = max_entity_id;
    zone_meta.min_timestamp = min_tx_time;
    zone_meta.max_timestamp = max_tx_time;
    zone_meta.column_id = column_id;
    zone_meta.entity_type = entity_type;
    zone_meta.path = filepath;
    zone_meta.blob_path = db_path_ + "/sst_" + std::to_string(file_number) + ".blob";
    
    Status cs = compaction_engine_->AddSSTFile(zone_meta);
    if (!cs.ok()) {
      // TODO: 添加日志记录
    }
    
    compaction_engine_->ScheduleCompaction();
  }

  return Status::OK();
}

Status LsmEngine::DoCompaction(int level, const std::vector<FrondFileMeta>& inputs) {
  if (inputs.empty()) {
    return Status::OK();
  }

  // 提取 entity_type 和 column_id（应该都相同）
  uint8_t entity_type = inputs[0].entity_type;
  uint16_t column_id = inputs[0].column_id;
  
  // 验证所有输入文件类型一致
  for (const auto& input : inputs) {
    if (input.entity_type != entity_type || input.column_id != column_id) {
      return Status::InvalidArgument("DoCompaction", 
          "input files have different entity_type or column_id");
    }
  }
  
  // 打开所有输入 SST
  std::vector<std::shared_ptr<ZoneColumnarSstReader>> readers;
  for (const auto& input : inputs) {
    std::string input_path = SstFilePath(input.file_number);
    auto reader = std::make_shared<ZoneColumnarSstReader>(input_path);
    Status s = reader->Open();
    if (!s.ok()) {
      return s;
    }
    readers.push_back(reader);
  }
  
  // 准备 reader 指针向量
  std::vector<ZoneColumnarSstReader*> reader_ptrs;
  for (auto& r : readers) {
    reader_ptrs.push_back(r.get());
  }
  
  // 创建输出文件
  uint64_t file_number = next_file_number_.fetch_add(1);
  std::string output_path = SstFilePath(file_number);
  int output_level = level + 1;
  
  // 执行归并
  CompactionMerger merger(reader_ptrs, entity_type, column_id);
  auto output_meta = merger.Run(output_path, db_path_);
  
  if (!output_meta) {
    return Status::IOError("DoCompaction", "merger failed");
  }
  
  // 设置文件号
  output_meta->file_number = file_number;
  output_meta->level = output_level;
  output_meta->min_entity_id = inputs[0].min_entity_id;
  output_meta->max_entity_id = inputs[0].max_entity_id;
  for (const auto& input : inputs) {
    output_meta->min_entity_id = std::min(output_meta->min_entity_id, input.min_entity_id);
    output_meta->max_entity_id = std::max(output_meta->max_entity_id, input.max_entity_id);
    output_meta->min_timestamp = std::min(output_meta->min_timestamp, input.min_tx_time);
    output_meta->max_timestamp = std::max(output_meta->max_timestamp, input.max_tx_time);
  }
  
  // 获取文件大小
  uint64_t file_size = 0;
  Status ls = env_->GetFileSize(output_path, &file_size);
  if (!ls.ok()) {
    return Status::IOError("DoCompaction", ls.ToString());
  }
  
  // 创建 FrondFileMeta
  FrondFileMeta new_meta;
  new_meta.file_number = file_number;
  new_meta.file_size = file_size;
  new_meta.num_entries = output_meta->num_entries;
  new_meta.min_entity_id = output_meta->min_entity_id;
  new_meta.max_entity_id = output_meta->max_entity_id;
  new_meta.min_tx_time = output_meta->min_timestamp;
  new_meta.max_tx_time = output_meta->max_timestamp;
  new_meta.level = output_level;
  new_meta.column_id = column_id;
  new_meta.entity_type = entity_type;
  
  // 原子更新 levels_
  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    // 从当前层级删除输入文件
    auto& current_level = levels_[level];
    for (const auto& input : inputs) {
      auto it = std::remove_if(current_level.begin(), current_level.end(),
                               [&input](const FrondFileMeta& m) {
                                 return m.file_number == input.file_number;
                               });
      current_level.erase(it, current_level.end());
    }
    
    // 添加新文件到下一层级
    if (output_level >= static_cast<int>(levels_.size())) {
      levels_.resize(output_level + 1);
    }
    levels_[output_level].push_back(new_meta);
  }
  
  // 删除旧文件
  for (const auto& input : inputs) {
    std::string old_path = SstFilePath(input.file_number);
    env_->RemoveFile(old_path);
  }
  
  // 更新 Compaction Engine
  if (compaction_engine_) {
    compaction_engine_->RemoveSSTFile(output_meta->file_number);
    compaction_engine_->AddSSTFile(*output_meta);
  }
  
  return Status::OK();
}

std::vector<FrondFileMeta> LsmEngine::SelectCompactionFiles(int level) {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  if (level < 0 || level >= static_cast<int>(levels_.size())) {
    return {};
  }
  
  const auto& level_files = levels_[level];
  if (level_files.size() < options_.compaction_config.min_files) {
    return {};
  }
  
  // Simple strategy: select oldest files (smallest file numbers)
  std::vector<FrondFileMeta> candidates = level_files;
  std::sort(candidates.begin(), candidates.end(),
            [](const auto& a, const auto& b) {
              return a.file_number < b.file_number;
            });
  
  // Select up to 4 files for compaction
  size_t num_to_select = std::min(candidates.size(), size_t(4));
  return std::vector<FrondFileMeta>(candidates.begin(), candidates.begin() + num_to_select);
}

bool LsmEngine::NeedsCompaction(int level) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  
  if (level < 0 || level >= static_cast<int>(levels_.size())) {
    return false;
  }
  
  const auto& level_files = levels_[level];
  
  // Check file count
  if (level_files.size() >= options_.compaction_config.min_files) {
    return true;
  }
  
  // Check total size
  uint64_t total_size = 0;
  for (const auto& meta : level_files) {
    total_size += meta.file_size;
  }
  
  if (total_size >= options_.compaction_config.min_size) {
    return true;
  }
  
  return false;
}

void LsmEngine::QueryFrondFiles(uint64_t entity_id, EntityType entity_type,
                                uint16_t column_id, std::vector<MemTableEntry>* results) {
  // Stub
}

void LsmEngine::GetEntriesFromSst(uint64_t entity_id, EntityType entity_type,
                                  uint16_t column_id, 
                                  std::vector<std::pair<CedarKey, Descriptor>>* results) {
  if (!results || levels_.empty()) {
    return;
  }
  
  // OPTIMIZATION: Use column-based file index to quickly find candidate files
  std::vector<FrondFileMeta*> candidates = GetCandidateFiles(entity_id, entity_type, column_id);
  
  // If index returns empty, fall back to building from levels
  if (candidates.empty()) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    for (int level = 0; level < static_cast<int>(levels_.size()); ++level) {
      for (const auto& meta : levels_[level]) {
        if (entity_id >= meta.min_entity_id && entity_id <= meta.max_entity_id &&
            column_id == meta.column_id &&
            static_cast<uint8_t>(entity_type) == meta.entity_type) {
          candidates.push_back(const_cast<FrondFileMeta*>(&meta));
        }
      }
    }
  }
  
  // OPTIMIZATION: If too many candidates, limit to first 20 files
  // For temporal data, recent files (lower file numbers) are more likely to contain the data
  // But for point query, we need all files that might contain the entity
  // So we use Bloom Filter to quickly skip files
  size_t max_files_to_check = 50;
  if (candidates.size() > max_files_to_check) {
    // Sort by file_number (newest first)
    std::sort(candidates.begin(), candidates.end(), 
      [](FrondFileMeta* a, FrondFileMeta* b) {
        return a->file_number > b->file_number;
      });
    candidates.resize(max_files_to_check);
  }
  
  // Query candidate files
  for (FrondFileMeta* meta : candidates) {
    std::string filepath = SstFilePath(meta->file_number);
    
    // ========== 使用 ZoneColumnarSstReader（新格式）==========
    ZoneColumnarSstReader reader(filepath);
    if (!reader.Open().ok()) {
      continue;
    }
    
    // Bloom Filter optimization: skip files that definitely don't contain the entity
    if (!reader.MayContainEntity(entity_id)) {
      continue;
    }
    
    // Get all versions for this entity using GetRange
    // Use full time range to get all versions
    auto versions = reader.GetRange(entity_id, entity_type, column_id, 
                                     Timestamp(0), 
                                     Timestamp(UINT64_MAX));
    
    // Add results
    for (auto& [key, desc] : versions) {
      results->emplace_back(key, desc);
    }
  }
  
  // Sort results by timestamp descending (newest first)
  std::sort(results->begin(), results->end(), 
            [](const auto& a, const auto& b) {
              return a.first.timestamp().value() > b.first.timestamp().value();
            });
}

void LsmEngine::PreloadHotEntities() {
  // Stub
}

Status LsmEngine::LoadSstFiles() {
  if (!std::filesystem::exists(db_path_)) {
    return Status::OK();
  }

  for (const auto& entry : std::filesystem::directory_iterator(db_path_)) {
    if (entry.is_regular_file() && entry.path().extension() == ".frond") {
      std::string filename = entry.path().filename().string();
      size_t dot_pos = filename.find('.');
      if (dot_pos == std::string::npos) continue;
      std::string number_str = filename.substr(0, dot_pos);
      if (number_str.empty() || !std::all_of(number_str.begin(), number_str.end(), ::isdigit)) {
        continue;
      }
      uint64_t file_number = std::stoull(number_str);
      next_file_number_ = std::max(next_file_number_.load(), file_number + 1);

      uint64_t file_size = entry.file_size();

      std::string filepath = entry.path().string();
      // ========== 使用 ZoneColumnarSstReader（新格式）==========
      ZoneColumnarSstReader reader(filepath);
      if (!reader.Open().ok()) {
        continue;
      }

      FrondFileMeta meta;
      meta.file_number = file_number;
      meta.file_size = file_size;
      meta.num_entries = reader.NumEntries();
      meta.min_entity_id = reader.MinEntityId();
      meta.max_entity_id = reader.MaxEntityId();
      meta.min_tx_time = reader.MinTimestamp();
      meta.max_tx_time = reader.MaxTimestamp();
      meta.level = 0;
      // Get column_id from header
      meta.column_id = reader.ColumnId();
      // entity_type from header
      meta.entity_type = reader.GetEntityType();

      levels_[0].push_back(meta);
    }
  }

  return Status::OK();
}

uint64_t LsmEngine::NewFileNumber() {
  return next_file_number_.fetch_add(1);
}

std::string LsmEngine::SstFilePath(uint64_t file_number) {
  return db_path_ + "/" + std::to_string(file_number) + ".frond";
}

std::string FrondFileMeta::file_name() const {
  return std::to_string(file_number) + ".frond";
}

std::vector<FrondFileMeta> LsmEngine::GetFrondFiles(int level) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  if (level >= 0 && level < static_cast<int>(levels_.size())) {
    return levels_[level];
  }
  return {};
}

// ============================================================================
// QUERY OPTIMIZATION: Fast File Lookup by Entity Range
// ============================================================================

void LsmEngine::BuildColumnFileIndex() {
  std::unique_lock<std::shared_mutex> lock(column_index_mutex_);
  column_file_index_.clear();
  
  // Iterate through all levels and files
  for (int level = 0; level < static_cast<int>(levels_.size()); ++level) {
    for (auto& meta : levels_[level]) {
      // Group by column_id
      column_file_index_[meta.column_id].push_back(&meta);
    }
  }
  
  // Sort each column's files by min_entity_id for binary search
  for (auto& [col_id, files] : column_file_index_) {
    std::sort(files.begin(), files.end(), [](FrondFileMeta* a, FrondFileMeta* b) {
      return a->min_entity_id < b->min_entity_id;
    });
  }
}

std::vector<FrondFileMeta*> LsmEngine::GetCandidateFiles(uint64_t entity_id, 
                                                           EntityType entity_type, 
                                                           uint16_t column_id) const {
  std::shared_lock<std::shared_mutex> lock(column_index_mutex_);
  
  auto it = column_file_index_.find(column_id);
  if (it == column_file_index_.end()) {
    return {};
  }
  
  const auto& files = it->second;
  std::vector<FrondFileMeta*> candidates;
  
  // Use binary search to find files that might contain entity_id
  // Files are sorted by min_entity_id
  for (FrondFileMeta* meta : files) {
    // Quick range check: entity_id must be within [min_entity_id, max_entity_id]
    if (entity_id >= meta->min_entity_id && entity_id <= meta->max_entity_id) {
      // Additional check: entity_type must match
      if (static_cast<uint8_t>(entity_type) == meta->entity_type) {
        candidates.push_back(meta);
      }
    }
    // Early termination: if file's min_entity_id > entity_id, no need to check further
    // But we can't break here because files may have overlapping ranges
  }
  
  return candidates;
}

// ========== Size-Tiered Compaction 集成 ==========

void LsmEngine::AutoCompactionThread() {
  while (auto_compaction_enabled_.load()) {
    if (compaction_engine_ && compaction_engine_->NeedsCompaction()) {
      // 尝试获取下一个 Compaction 任务
      auto task = compaction_engine_->PickNextCompaction();
      if (task.has_value()) {
        // 执行 Compaction
        Status s = compaction_engine_->ExecuteCompaction(task.value());
        if (!s.ok()) {
          // 记录错误但继续运行
          // TODO: 添加日志
        }
      }
    }
    
    // 休眠一段时间再检查
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

void LsmEngine::MigrateExistingSstFiles() {
  // 将现有的 SST 文件从旧格式迁移到新的 Compaction 引擎
  // 注意：这是一个一次性迁移，在首次启用新引擎时调用
  
  if (!compaction_engine_) return;
  
  for (int level = 0; level < static_cast<int>(levels_.size()); ++level) {
    for (const auto& old_meta : levels_[level]) {
      ZoneSstMeta zone_meta;
      zone_meta.file_number = old_meta.file_number;
      zone_meta.file_size = old_meta.file_size;
      zone_meta.num_entries = old_meta.num_entries;
      zone_meta.level = level;
      zone_meta.min_entity_id = old_meta.min_entity_id;
      zone_meta.max_entity_id = old_meta.max_entity_id;
      zone_meta.min_timestamp = old_meta.min_tx_time;
      zone_meta.max_timestamp = old_meta.max_tx_time;
      zone_meta.column_id = old_meta.column_id;
      zone_meta.entity_type = old_meta.entity_type;
      zone_meta.path = SstFilePath(old_meta.file_number);
      zone_meta.blob_path = db_path_ + "/sst_" + std::to_string(old_meta.file_number) + ".blob";
      
      // 静默添加，不触发 Compaction
      compaction_engine_->AddSSTFile(zone_meta);
    }
  }
}

Status LsmEngine::CompactAll() {
  if (!compaction_engine_) {
    return Status::InvalidArgument("LsmEngine", "Compaction engine not initialized");
  }
  
  // 等待所有自动 Compaction 完成
  auto_compaction_enabled_.store(false);
  if (auto_compaction_thread_ && auto_compaction_thread_->joinable()) {
    auto_compaction_thread_->join();
  }
  
  // 执行全量合并
  Status s = compaction_engine_->CompactAll();
  
  // 重新启动自动 Compaction 线程
  auto_compaction_enabled_.store(true);
  auto_compaction_thread_ = new std::thread(&LsmEngine::AutoCompactionThread, this);
  
  return s;
}

void LsmEngine::WaitForCompactions() {
  if (compaction_engine_) {
    compaction_engine_->WaitForCompactions();
  }
}

// =============================================================================
// OPTIMIZATION: 跨查询缓存实现
// =============================================================================

std::optional<std::vector<MemTableEntry>> LsmEngine::GetFromCrossQueryCache(
    uint64_t entity_id, uint16_t column_id) const {
  CacheKey key{entity_id, column_id};
  
  std::shared_lock<std::shared_mutex> lock(cross_query_cache_mutex_);
  auto it = cross_query_cache_.find(key);
  if (it == cross_query_cache_.end()) {
    return std::nullopt;
  }
  
  // 检查是否过期
  auto now = std::chrono::steady_clock::now();
  if (now - it->second.timestamp > kCrossQueryCacheTTL) {
    lock.unlock();
    // 过期，移除缓存
    std::unique_lock<std::shared_mutex> write_lock(cross_query_cache_mutex_);
    cross_query_cache_.erase(it);
    return std::nullopt;
  }
  
  // 缓存命中
  it->second.hit_count++;
  return it->second.data;
}

void LsmEngine::AddToCrossQueryCache(uint64_t entity_id, uint16_t column_id,
                                     const std::vector<MemTableEntry>& data) {
  // 清理过期缓存
  CleanupCrossQueryCache();
  
  // 如果缓存已满，不添加
  {
    std::shared_lock<std::shared_mutex> lock(cross_query_cache_mutex_);
    if (cross_query_cache_.size() >= kMaxCrossQueryCacheSize) {
      return;
    }
  }
  
  CacheKey key{entity_id, column_id};
  CacheEntry entry;
  entry.data = data;
  entry.timestamp = std::chrono::steady_clock::now();
  entry.hit_count = 1;
  
  std::unique_lock<std::shared_mutex> lock(cross_query_cache_mutex_);
  cross_query_cache_[key] = std::move(entry);
}

void LsmEngine::InvalidateCrossQueryCache(uint64_t entity_id, uint16_t column_id) {
  CacheKey key{entity_id, column_id};
  std::unique_lock<std::shared_mutex> lock(cross_query_cache_mutex_);
  cross_query_cache_.erase(key);
}

void LsmEngine::CleanupCrossQueryCache() {
  auto now = std::chrono::steady_clock::now();
  std::unique_lock<std::shared_mutex> lock(cross_query_cache_mutex_);
  
  for (auto it = cross_query_cache_.begin(); it != cross_query_cache_.end();) {
    if (now - it->second.timestamp > kCrossQueryCacheTTL) {
      it = cross_query_cache_.erase(it);
    } else {
      ++it;
    }
  }
}

// =============================================================================
// OPTIMIZATION: 热数据预读取和查询模式追踪
// =============================================================================

void LsmEngine::TrackQueryPattern(uint64_t entity_id, EntityType entity_type, 
                                   uint16_t column_id) {
  CacheKey key{entity_id, column_id};
  
  std::unique_lock<std::shared_mutex> lock(query_pattern_mutex_);
  auto& pattern = query_patterns_[key];
  pattern.count++;
  pattern.last_query = std::chrono::steady_clock::now();
}

void LsmEngine::PrefetchHotData(uint64_t entity_id, EntityType entity_type, 
                                 uint16_t column_id) {
  // 检查是否是热数据
  CacheKey key{entity_id, column_id};
  
  {
    std::shared_lock<std::shared_mutex> lock(query_pattern_mutex_);
    auto it = query_patterns_.find(key);
    if (it == query_patterns_.end() || it->second.count < kHotDataThreshold) {
      return;  // 不是热数据，不预读取
    }
  }
  
  // 检查是否已在缓存中
  if (GetFromCrossQueryCache(entity_id, column_id).has_value()) {
    return;  // 已在缓存中
  }
  
  // 预读取数据到缓存（使用全时间范围）
  auto data = GetRange(entity_id, entity_type, column_id, 
                       Timestamp(0), Timestamp(UINT64_MAX));
  
  if (!data.empty()) {
    AddToCrossQueryCache(entity_id, column_id, data);
  }
}

// =============================================================================
// OPTIMIZATION: P1 - 时间范围预计算缓存实现
// =============================================================================

std::optional<std::vector<MemTableEntry>> LsmEngine::GetFromTimeRangeCache(
    uint64_t entity_id, uint16_t column_id, uint64_t start_ts, uint64_t end_ts) const {
  TimeRangeCacheKey key{entity_id, column_id, start_ts, end_ts};
  
  std::shared_lock<std::shared_mutex> lock(time_range_cache_mutex_);
  auto it = time_range_cache_.find(key);
  if (it == time_range_cache_.end()) {
    return std::nullopt;
  }
  
  // 检查是否过期
  auto now = std::chrono::steady_clock::now();
  if (now - it->second.second > kTimeRangeCacheTTL) {
    lock.unlock();
    std::unique_lock<std::shared_mutex> write_lock(time_range_cache_mutex_);
    time_range_cache_.erase(it);
    return std::nullopt;
  }
  
  return it->second.first;
}

void LsmEngine::AddToTimeRangeCache(uint64_t entity_id, uint16_t column_id, 
                                     uint64_t start_ts, uint64_t end_ts,
                                     const std::vector<MemTableEntry>& data) {
  // 清理过期缓存
  CleanupTimeRangeCache();
  
  // 如果缓存已满，不添加
  {
    std::shared_lock<std::shared_mutex> lock(time_range_cache_mutex_);
    if (time_range_cache_.size() >= kMaxTimeRangeCacheSize) {
      return;
    }
  }
  
  TimeRangeCacheKey key{entity_id, column_id, start_ts, end_ts};
  std::unique_lock<std::shared_mutex> lock(time_range_cache_mutex_);
  time_range_cache_[key] = {data, std::chrono::steady_clock::now()};
}

void LsmEngine::CleanupTimeRangeCache() {
  auto now = std::chrono::steady_clock::now();
  std::unique_lock<std::shared_mutex> lock(time_range_cache_mutex_);
  
  for (auto it = time_range_cache_.begin(); it != time_range_cache_.end();) {
    if (now - it->second.second > kTimeRangeCacheTTL) {
      it = time_range_cache_.erase(it);
    } else {
      ++it;
    }
  }
}

// =============================================================================
// OPTIMIZATION: P1 - SST 文件预加载管理
// =============================================================================

void LsmEngine::PreloadHotSSTFiles() {
  if (!sst_reader_cache_ || !compaction_engine_) return;
  
  // 获取最近频繁访问的文件
  auto stats = sst_reader_cache_->GetStats();
  if (stats.hits < 100) return;  // 访问次数不够，不预加载
  
  // 获取热数据文件列表
  auto hot_files = sst_reader_cache_->GetHotFiles();
  
  // 预加载热数据文件（最多预加载 5 个）
  size_t preload_count = 0;
  for (const auto& file_path : hot_files) {
    if (preload_count >= 5) break;
    
    if (sst_reader_cache_->Preload(file_path)) {
      preload_count++;
    }
  }
}

std::vector<std::string> LsmEngine::GetHotSSTFilesForQuery(
    const std::vector<uint64_t>& entity_ids,
    uint16_t column_id,
    uint8_t entity_type) const {
  std::vector<std::string> hot_files;
  
  if (!compaction_engine_) return hot_files;
  
  // 找出包含最多查询 entity 的文件
  std::unordered_map<std::string, size_t> file_entity_count;
  
  for (uint64_t entity_id : entity_ids) {
    auto files = compaction_engine_->GetFilesForEntity(entity_id, column_id, entity_type);
    for (const auto& file : files) {
      file_entity_count[file.path]++;
    }
  }
  
  // 按包含 entity 数量排序，返回前 10 个
  std::vector<std::pair<std::string, size_t>> sorted_files(
      file_entity_count.begin(), file_entity_count.end());
  std::sort(sorted_files.begin(), sorted_files.end(),
            [](const auto& a, const auto& b) { return a.second > b.second; });
  
  for (size_t i = 0; i < std::min(size_t(10), sorted_files.size()); ++i) {
    hot_files.push_back(sorted_files[i].first);
  }
  
  return hot_files;
}

}  // namespace cedar
