// Copyright (c) 2025 The Cedar Authors. All rights reserved.

#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/compaction_merger.h"
// DEPRECATED: Old format - now using ZoneColumnar
// #include "cedar/frond/frond_builder.h"
// #include "cedar/frond/frond_reader.h"
#include "cedar/frond/zone_columnar_format.h"
#include "cedar/transaction/batch_api.h"
#include "cedar/transaction/wal.h"

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <thread>
#include <unordered_set>

namespace cedar {

namespace {

// 墓碑过滤辅助（审计 3.2）：读路径统一把墓碑视为删除。
// 同一 (timestamp, dst_id) 版本槽位上只要存在墓碑，就整槽视为已删除，
// 避免"墓碑在文件 A、活值在文件 B"时把旧值读出来。
std::string VersionSlotKey(const MemTableEntry& entry) {
  std::string key;
  key.resize(17);
  const uint64_t ts = entry.timestamp.value();
  for (int i = 0; i < 8; ++i) key[i] = static_cast<char>((ts >> (8 * i)) & 0xFF);
  key[8] = entry.dst_id.has_value() ? 1 : 0;
  const uint64_t dst = entry.dst_id.value_or(0);
  for (int i = 0; i < 8; ++i) key[9 + i] = static_cast<char>((dst >> (8 * i)) & 0xFF);
  return key;
}

// 去重（同一版本槽位只保留一份）+ 墓碑优先（墓碑覆盖同槽位的所有值）。
std::vector<MemTableEntry> ApplyTombstoneFilter(std::vector<MemTableEntry> entries) {
  std::unordered_set<std::string> tombstones;
  tombstones.reserve(entries.size());
  for (const auto& entry : entries) {
    if (entry.descriptor.IsTombstone()) {
      tombstones.insert(VersionSlotKey(entry));
    }
  }

  std::unordered_set<std::string> seen;
  seen.reserve(entries.size());
  std::vector<MemTableEntry> out;
  out.reserve(entries.size());
  for (auto& entry : entries) {
    if (entry.descriptor.IsTombstone()) {
      continue;  // 墓碑本身不返回给调用方
    }
    std::string slot = VersionSlotKey(entry);
    if (!tombstones.empty() && tombstones.count(slot) != 0) {
      continue;  // 该版本已被墓碑删除
    }
    if (!seen.insert(slot).second) {
      continue;  // 同一版本来自多处（mem/imm/SST），只保留先出现的一份
    }
    out.push_back(std::move(entry));
  }
  return out;
}

// flush 写 SST 的 WritableFile 包装（t7）：WAL 开启且不做 WAL 截断时，
// 数据在 WAL 里已有持久副本，不需要每个文件付 F_FULLFSYNC —— Sync 退化为 Flush。
//
// 注意（t10）：一旦启用 WAL 截断，必须在删除 WAL 段之前把 SST 真正 fsync；
// 此时不再使用这个包装（见 FlushEntityGroup 的 durable_sst 分支）。
class FlushSstWritableFile final : public WritableFile {
 public:
  explicit FlushSstWritableFile(WritableFile* inner) : inner_(inner) {}
  ~FlushSstWritableFile() override { delete inner_; }

  Status Append(const Slice& data) override { return inner_->Append(data); }
  Status Close() override { return inner_->Close(); }
  Status Flush() override { return inner_->Flush(); }
  // 只把用户态缓冲写进 OS（写错误仍会上报），不做 fsync
  Status Sync() override { return inner_->Flush(); }

 private:
  WritableFile* inner_;
};

// 从 SST 读出的 (key, descriptor) 结果集中剔除墓碑（审计 3.2）
std::vector<std::pair<CedarKey, Descriptor>> DropTombstones(
    std::vector<std::pair<CedarKey, Descriptor>> entries) {
  entries.erase(std::remove_if(entries.begin(), entries.end(),
                               [](const auto& entry) {
                                 return entry.second.IsTombstone();
                               }),
                entries.end());
  return entries;
}


}  // namespace

LsmEngine::LsmEngine(const std::string& db_path,
                     const CedarOptions& options,
                     cedar::Env* env)
    : db_path_(db_path),
      options_(options),
      env_(env ? env : cedar::Env::Default()),
      mem_(std::make_shared<VSLMemTable>()),
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
  
  // ========== WAL checkpoint 水位（t10） ==========
  // 必须在 InitWAL 之前读：checkpoint 里的 file_number_floor 决定新段号下限，
  // 水位决定重放跳过哪些段。checkpoint 缺失/损坏时按"水位 0"处理（重放全部，
  // 安全方向）。
  wal_checkpoint_path_ = WalCheckpointFile::DefaultPath(db_path_);
  if (wal_truncation_enabled_) {
    WalCheckpoint checkpoint;
    Status cps = WalCheckpointFile::Load(wal_checkpoint_path_, env_, &checkpoint);
    if (cps.ok()) {
      wal_checkpoint_watermark_ = checkpoint.watermark_file_number;
      wal_file_number_floor_ = checkpoint.file_number_floor;
    } else if (!cps.IsNotFound()) {
      // 损坏的 checkpoint 不能让 Open 失败：删除它并按无水位重放（安全）
      env_->RemoveFile(wal_checkpoint_path_).IgnoreError();
      wal_checkpoint_watermark_ = 0;
      wal_file_number_floor_ = 0;
    }
  }

  // Initialize WAL and TransactionManager
  s = InitWAL();
  if (!s.ok()) return s;

  // ========== WAL 重放（Task 1.2 / 审计 3.3） ==========
  // 位置：LoadSstFiles 之后、接受写之前。只重放"带提交围栏"的 record，
  // 未提交事务的写集不落表；重放按 (key, txn_version, descriptor) 去重。
  // t10：段号 < checkpoint 水位的段已在 fsync 过的 SST 里，直接跳过。
  if (wal_writer_) {
    s = WalReplay::Replay(db_path_ + "/wal", env_, mem_.get(), &wal_replay_stats_,
                          wal_checkpoint_watermark_);
    if (!s.ok()) {
      return Status::IOError("LsmEngine", "WAL replay failed: " + s.ToString());
    }
  }

  // ========== Initialize Size-Tiered Compaction Engine ==========
  // 从 options 中读取配置或使用默认配置
  compaction_engine_ = std::make_unique<SizeTieredCompactionEngine>(
      db_path_, compaction_config_, env_);

  // 单一文件号分配器（Task 1.7 / 审计 9.2 / 设计 D2）：flush 与 compaction 输出
  // 共用 LsmEngine 持有的这个计数器；压缩输出号自动回写，二者不会再撞号。
  // 必须在 Open() 之前绑定：LoadManifest 会用 MANIFEST 水位推进它（只前进不后退）。
  compaction_engine_->BindFileNumberAllocator(&next_file_number_);
  
  s = compaction_engine_->Open();
  if (!s.ok()) {
    return Status::IOError("LsmEngine", "Failed to open compaction engine: " + s.ToString());
  }
  
  // 如果存在现有的 SST 文件，迁移到新的 Compaction 引擎
  MigrateExistingSstFiles();

  // Task 3.4：构建 (entity_type,column) 文件区间索引（之后 O(log F) 选文件）
  RebuildFileIndex();
  
  opened_ = true;
  
  return Status::OK();
}

Status LsmEngine::Close() {
  if (!opened_) return Status::OK();
  opened_ = false;  // 先置位：即使下面失败，析构里的再次 Close 也不会重入

  shutdown_ = true;

  if (bg_thread_ && bg_thread_->joinable()) {
    bg_thread_->join();
    delete bg_thread_;
    bg_thread_ = nullptr;
  }

  // 落盘残留数据：先重试失败遗留的 imm_，再 flush 当前 mem_（Task 1.3）
  Status flush_status = ForceFlush();

  // t10：写者已停止且数据已 flush + fsync → 把水位推进到当前活动段并删掉旧段。
  // flush 失败时不做截断（WAL 是唯一副本）。
  Status checkpoint_status = Status::OK();
  if (flush_status.ok() && wal_truncation_enabled_) {
    checkpoint_status = FinalizeWALCheckpointOnClose();
  }

  // 关闭 WAL：把 WritableFile 缓冲区刷到磁盘，否则同进程重开时尾部记录会缺失
  Status wal_status = Status::OK();
  if (wal_writer_) {
    wal_status = wal_writer_->Close();
  }

  // 关闭 Compaction 引擎
  if (compaction_engine_) {
    compaction_engine_->WaitForCompactions();
    compaction_engine_->Close();
    compaction_engine_.reset();
  }

  // Close 传播错误（审计 3.4）：调用方必须知道数据没有完全落盘
  if (!flush_status.ok()) {
    return Status::IOError("LsmEngine",
                           "Close flush failed: " + flush_status.ToString());
  }
  if (!wal_status.ok()) {
    return Status::IOError("LsmEngine", "Close WAL failed: " + wal_status.ToString());
  }
  if (!checkpoint_status.ok()) {
    // 截断是空间优化：失败不影响已提交数据（WAL 仍在），只上报
    return Status::IOError("LsmEngine",
                           "Close WAL checkpoint failed: " +
                               checkpoint_status.ToString());
  }
  return Status::OK();
}

// 直接写路径（LsmEngine::Put/Delete、CedarGraphStorage 的所有非事务写）：
// WAL-first —— 先落一条单 op 的 WAL record（与事务提交共享组提交批次），
// 成功后才写 memtable。单 op record 在重放端被视为"已提交的直接写"，
// 因此不需要事务的提交围栏（见 WalReplay::Replay 的兼容策略）。
//
// 持久化级别：默认 WalDurability::kWrite（append + flush 到 OS），保证进程
// 崩溃/kill 后可恢复；SetDirectWriteSync(true) 可提升为 fsync（掉电可恢复，
// 但要付 F_FULLFSYNC 的代价）。事务提交始终走 kSync。
Status LsmEngine::Put(const CedarKey& key, const Descriptor& descriptor, Timestamp txn_version) {
  if (!opened_) {
    return Status::InvalidArgument("LsmEngine", "not opened");
  }

  if (wal_writer_) {
    WalBatch batch;
    batch.Put(key, descriptor, txn_version);
    const WalDurability durability =
        direct_write_sync_ ? WalDurability::kSync : WalDurability::kWrite;
    Status wal_status = wal_writer_->WriteBatch(batch, durability);
    if (!wal_status.ok()) {
      // WAL 失败 → 不写 memtable，调用方看到错误且数据不可见（与事务路径一致）
      return wal_status;
    }
  }

  return PutPinned(key, descriptor, txn_version);
}

Status LsmEngine::PutPinned(const CedarKey& key, const Descriptor& descriptor,
                            Timestamp txn_version) {
  if (!opened_) {
    return Status::InvalidArgument("LsmEngine", "not opened");
  }

  Status s;
  {
    // 持共享锁写入：ForceFlush/自动切换在独占锁下换表，必须等本次写入完成，
    // 因此这次写入一定落在随后被 flush 的那张表里（审计 3.5：不丢写）
    std::shared_lock<std::shared_mutex> lock(mutex_);
    s = mem_->Put(key, descriptor, txn_version);
  }
  if (!s.ok()) {
    return s;
  }

  if (!disable_column_tracking_) {
    // 用 key 的 column（墓碑 descriptor 的 column 字段不可靠）
    TrackColumnId(key.entity_id(), key.column_id());
  }

  // Task 3.5（读一致性）：写入成功后必须让同一 (entity, column) 的查询缓存失效，
  // 否则 GetRangeLimit 的全范围跨查询缓存（TTL 60s）会返回写前的旧结果。
  // 所有写路径（直接 Put/Delete、事务 WriteToMemTable）最终都经过 PutPinned。
  if (!disable_query_cache_invalidate_) {
    InvalidateQueryCache(key.entity_id());
    InvalidateCrossQueryCache(key.entity_id(), key.column_id());
  }

  // 按字节水位自动换表 + flush（审计 3.4）
  MaybeAutoFlush();
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
  // 删除 = 落一条墓碑 Descriptor（审计 3.2 的统一持久化表示）。
  // 用 key.column_id() 而不是默认 0，保证墓碑归属正确的列分组。
  return Put(key, Descriptor::Tombstone(key.column_id()), txn_version);
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

  // 读路径持 memtable 强引用：flush 换表后旧表不会在读取期间被销毁（审计 3.5）
  std::shared_ptr<VSLMemTable> mem;
  std::shared_ptr<VSLMemTable> imm;
  PinMemTables(&mem, &imm);

  // 1. Query MemTable (hot data)
  // Task 1.5（审计 2.4/8.2）：点查必须把 target_id 带进 memtable，
  // 否则同 src/type、不同 dst 的边会读到彼此的 value。
  Descriptor desc;
  if (mem) {
    Status s = mem->Get(key.entity_id(), key.entity_type(), key.column_id(),
                        key.target_id(), key.timestamp(), &desc);
    if (s.ok()) {
      // 墓碑即删除（审计 3.2）：不得回退到 imm/SST 的旧值
      if (desc.IsTombstone()) return std::nullopt;
      return desc;
    }
  }

  // 2. Query Immutable MemTable
  if (imm) {
    Status s = imm->Get(key.entity_id(), key.entity_type(), key.column_id(),
                        key.target_id(), key.timestamp(), &desc);
    if (s.ok()) {
      if (desc.IsTombstone()) return std::nullopt;
      return desc;
    }
  }

  // 3. Query SST files via Size-Tiered Compaction Engine
  if (compaction_engine_) {
    // Task 3.4：区间索引 O(log F) 选文件；reader 统一走 SstReaderCache
    auto files = LookupFiles(key.entity_id(),
                             static_cast<uint8_t>(key.entity_type()),
                             key.column_id());
    
    // 按时间从新到旧排序（文件号大的通常更新）
    std::sort(files.begin(), files.end(), 
              [](const IndexedFile& a, const IndexedFile& b) {
                return a.file_number > b.file_number;
              });
    
    // 查询每个文件
    for (const auto& file_meta : files) {
      auto reader = GetCachedReader(file_meta.file_number, file_meta.path);
      if (!reader) continue;
      
      auto result = reader->Get(key);
      if (result.has_value()) {
        // 最新文件里的同 key 版本是墓碑 → 已删除，不得回退到更老的文件
        if (result->IsTombstone()) return std::nullopt;
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

  // 读路径持 memtable 强引用（审计 3.5）
  std::shared_ptr<VSLMemTable> mem;
  std::shared_ptr<VSLMemTable> imm;
  PinMemTables(&mem, &imm);

  // 1. Query MemTable (hot data)
  // Task 1.5：GetAll 是“所有版本/所有边”语义，memtable 路径必须遍历所有 target，
  // 否则同 src/type 的多条边只会返回首个 dst（审计 2.5）。
  if (mem) {
    auto mem_results = mem->GetRangeAllTargets(entity_id, entity_type, column_id,
                                               Timestamp(0), Timestamp::Max());
    results.insert(results.end(), mem_results.begin(), mem_results.end());
  }

  // 2. Query Immutable MemTable
  if (imm) {
    auto imm_results = imm->GetRangeAllTargets(entity_id, entity_type, column_id,
                                               Timestamp(0), Timestamp::Max());
    results.insert(results.end(), imm_results.begin(), imm_results.end());
  }

  // 3. Query SST files via Size-Tiered Compaction Engine
  if (compaction_engine_) {
    auto files = LookupFiles(entity_id, static_cast<uint8_t>(entity_type), column_id);
    
    for (const auto& file_meta : files) {
      auto reader = GetCachedReader(file_meta.file_number, file_meta.path);
      if (!reader) continue;
      
      // 获取该 Entity 的所有版本
      auto range_results = reader->GetRange(entity_id, entity_type, column_id,
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
  
  // 去重（timestamp + dst_id 相同视为同版本）+ 墓碑过滤（审计 3.2）：
  // 同一版本槽位存在墓碑时整槽视为已删除
  return ApplyTombstoneFilter(std::move(results));
}

std::optional<Descriptor> LsmEngine::GetAtTime(uint64_t entity_id,
                                               EntityType entity_type,
                                               uint16_t column_id,
                                               Timestamp timestamp) {
  // 旧签名：Vertex 行为，不区分 target（保持既有语义）
  return GetAtTimeImpl(entity_id, entity_type, column_id, std::nullopt, timestamp);
}

std::optional<Descriptor> LsmEngine::GetAtTime(uint64_t entity_id,
                                               EntityType entity_type,
                                               uint16_t column_id,
                                               uint64_t target_id,
                                               Timestamp timestamp) {
  // Task 1.5 / 审计 2.4/8.2：按完整 user key（含 target）定位
  return GetAtTimeImpl(entity_id, entity_type, column_id, target_id, timestamp);
}

std::optional<Descriptor> LsmEngine::GetAtTimeImpl(
    uint64_t entity_id,
    EntityType entity_type,
    uint16_t column_id,
    const std::optional<uint64_t>& target_id,
    Timestamp timestamp) {
  if (!opened_) {
    return std::nullopt;
  }

  // 读路径持 memtable 强引用（审计 3.5）
  std::shared_ptr<VSLMemTable> mem;
  std::shared_ptr<VSLMemTable> imm;
  PinMemTables(&mem, &imm);

  // 1. Query MemTable (hot data)
  std::optional<Descriptor> desc;
  if (mem) {
    desc = target_id.has_value()
               ? mem->GetAtTime(entity_id, entity_type, column_id, *target_id,
                                timestamp)
               : mem->GetAtTime(entity_id, entity_type, column_id, timestamp);
    if (desc.has_value()) {
      // 最新可见版本是墓碑 → 该时间点已删除（审计 3.2）
      if (desc->IsTombstone()) return std::nullopt;
      return desc;
    }
  }

  // 2. Query Immutable MemTable
  if (imm) {
    desc = target_id.has_value()
               ? imm->GetAtTime(entity_id, entity_type, column_id, *target_id,
                                timestamp)
               : imm->GetAtTime(entity_id, entity_type, column_id, timestamp);
    if (desc.has_value()) {
      if (desc->IsTombstone()) return std::nullopt;
      return desc;
    }
  }

  // 3. Query SST files via Size-Tiered Compaction Engine
  if (compaction_engine_) {
    // Task 3.4：区间索引 + 统一 reader cache
    auto files = LookupFiles(entity_id, static_cast<uint8_t>(entity_type), column_id);
    
    // 收集所有匹配的条目
    std::vector<std::pair<CedarKey, Descriptor>> all_entries;
    
    for (const auto& file_meta : files) {
      auto reader = GetCachedReader(file_meta.file_number, file_meta.path);
      if (!reader) continue;
      
      // 使用 GetRange 获取该 Entity 的所有版本
      auto range_results = reader->GetRange(entity_id, entity_type, column_id,
                                           Timestamp(0), Timestamp(UINT64_MAX));
      all_entries.insert(all_entries.end(), range_results.begin(), range_results.end());
    }
    
    // 按时间戳降序排序
    std::sort(all_entries.begin(), all_entries.end(),
              [](const auto& a, const auto& b) {
                return a.first.timestamp().value() > b.first.timestamp().value();
              });
    
    // 找到指定时间的版本（<= timestamp 的最新版本）
    for (const auto& [entry_key, descriptor] : all_entries) {
      if (entry_key.timestamp().value() > timestamp.value()) continue;
      // Task 1.5：target 重载必须按完整 user key 过滤（审计 2.4/8.2）
      if (target_id.has_value() && entry_key.target_id() != *target_id) continue;
      // 命中的最新版本是墓碑 → 删除，不得回退到更老版本（审计 3.2）
      if (descriptor.IsTombstone()) return std::nullopt;
      return descriptor;
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

  // 读路径持 memtable 强引用（审计 3.5）
  std::shared_ptr<VSLMemTable> mem;
  std::shared_ptr<VSLMemTable> imm;
  PinMemTables(&mem, &imm);

  // 1. Query MemTable (hot data)
  // Task 1.5：范围查询同样必须覆盖所有 target（多条边的 dst 不能丢）。
  if (mem) {
    auto mem_results = mem->GetRangeAllTargets(entity_id, entity_type, column_id,
                                               start, end);
    results.insert(results.end(), mem_results.begin(), mem_results.end());
  }

  // 2. Query Immutable MemTable
  if (imm) {
    auto imm_results = imm->GetRangeAllTargets(entity_id, entity_type, column_id,
                                               start, end);
    results.insert(results.end(), imm_results.begin(), imm_results.end());
  }

  // 3. Query SST files via Size-Tiered Compaction Engine
  if (compaction_engine_) {
    auto files = LookupFiles(entity_id, static_cast<uint8_t>(entity_type), column_id);
    
    for (const auto& file_meta : files) {
      // 时间范围快速检查
      if (file_meta.max_timestamp < start.value() || 
          file_meta.min_timestamp > end.value()) {
        continue;
      }
      
      auto reader = GetCachedReader(file_meta.file_number, file_meta.path);
      if (!reader) continue;
      
      // 使用 GetRange 获取时间范围内的版本
      auto range_results = reader->GetRange(entity_id, entity_type, column_id, start, end);
      
      for (const auto& [entry_key, descriptor] : range_results) {
        std::optional<uint64_t> dst_id = (entry_key.target_id() != 0)
            ? std::optional<uint64_t>(entry_key.target_id())
            : std::nullopt;
        results.emplace_back(entry_key.timestamp(), descriptor, dst_id, Timestamp(0));
      }
    }
  }

  // 去重 + 墓碑过滤（审计 3.2）
  return ApplyTombstoneFilter(std::move(results));
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
  if (max_results == 0) {
    return {};
  }

  // OPTIMIZATION: 追踪查询模式（用于识别热数据）
  TrackQueryPattern(entity_id, entity_type, column_id);
  
  // OPTIMIZATION: 尝试从跨查询缓存获取（如果是全范围查询）
  // Task 3.5：缓存存的是完整结果，limit 必须在返回前应用（不能直接返回全量）。
  if (start.value() == 0 && end.value() == UINT64_MAX) {
    auto cached = GetFromCrossQueryCache(entity_id, column_id);
    if (cached.has_value()) {
      std::vector<MemTableEntry> hit = std::move(cached.value());
      if (hit.size() > max_results) hit.resize(max_results);
      return hit;
    }
  }
  
  // OPTIMIZATION: 预读取热数据到缓存
  PrefetchHotData(entity_id, entity_type, column_id);
  
  // 读路径持 memtable 强引用（审计 3.5）
  std::shared_ptr<VSLMemTable> mem;
  std::shared_ptr<VSLMemTable> imm;
  PinMemTables(&mem, &imm);

  // 每个来源内部按 (timestamp desc, dst asc) 有序；Task 3.5：用 K 路归并
  // 取代“拼接后全排序”，limit 在归并中截断。
  std::vector<std::vector<MemTableEntry>> sources;
  auto normalize = [](std::vector<MemTableEntry> entries) {
    std::stable_sort(entries.begin(), entries.end(),
                     [](const MemTableEntry& a, const MemTableEntry& b) {
                       if (a.timestamp.value() != b.timestamp.value()) {
                         return a.timestamp.value() > b.timestamp.value();
                       }
                       return a.dst_id.value_or(0) < b.dst_id.value_or(0);
                     });
    return entries;
  };

  if (mem) {
    sources.push_back(normalize(mem->GetRangeAllTargets(entity_id, entity_type,
                                                        column_id, start, end)));
  }
  if (imm) {
    sources.push_back(normalize(imm->GetRangeAllTargets(entity_id, entity_type,
                                                        column_id, start, end)));
  }

  // Task 3.5：去掉 kMaxFilesToQuery=10 的静默截断；所有覆盖该 entity 且时间范围
  // 有交集的文件都参与归并（文件选择走 Task 3.4 区间索引，O(log F)）。
  if (compaction_engine_) {
    auto files = LookupFiles(entity_id, static_cast<uint8_t>(entity_type), column_id);
    std::sort(files.begin(), files.end(),
              [](const IndexedFile& a, const IndexedFile& b) {
                return a.file_number > b.file_number;  // 新文件优先（同 key 去重）
              });
    for (const auto& file_meta : files) {
      if (file_meta.max_timestamp < start.value() ||
          file_meta.min_timestamp > end.value()) {
        continue;
      }
      auto reader = GetCachedReader(file_meta.file_number, file_meta.path);
      if (!reader) continue;
      auto range_results =
          reader->GetRange(entity_id, entity_type, column_id, start, end);
      if (range_results.empty()) continue;
      std::vector<MemTableEntry> entries;
      entries.reserve(range_results.size());
      for (auto& [key, descriptor] : range_results) {
        std::optional<uint64_t> dst_id =
            key.target_id() != 0 ? std::optional<uint64_t>(key.target_id())
                                 : std::nullopt;
        entries.emplace_back(key.timestamp(), descriptor, dst_id, Timestamp(0));
      }
      sources.push_back(normalize(std::move(entries)));
    }
  }

  // 墓碑优先：同一 (timestamp, dst) 槽位存在墓碑时整槽删除（审计 3.2）。
  // 先做一遍 O(N) 收集，再在归并中跳过（limit 语义仍是“删除后最新 N 条”）。
  std::unordered_set<std::string> tombstones;
  for (const auto& source : sources) {
    for (const auto& entry : source) {
      if (entry.descriptor.IsTombstone()) {
        tombstones.insert(VersionSlotKey(entry));
      }
    }
  }

  struct Cursor {
    const std::vector<MemTableEntry>* entries;
    size_t index;
  };
  struct CursorGreater {
    bool operator()(const Cursor& a, const Cursor& b) const {
      const MemTableEntry& ea = (*a.entries)[a.index];
      const MemTableEntry& eb = (*b.entries)[b.index];
      if (ea.timestamp.value() != eb.timestamp.value()) {
        return ea.timestamp.value() < eb.timestamp.value();
      }
      return ea.dst_id.value_or(0) > eb.dst_id.value_or(0);
    }
  };

  std::priority_queue<Cursor, std::vector<Cursor>, CursorGreater> heap;
  for (const auto& source : sources) {
    if (!source.empty()) heap.push(Cursor{&source, 0});
  }

  std::unordered_set<std::string> seen;
  std::vector<MemTableEntry> result;
  if (max_results < 1000000) result.reserve(max_results);
  while (!heap.empty()) {
    Cursor cursor = heap.top();
    heap.pop();
    const MemTableEntry& entry = (*cursor.entries)[cursor.index];
    if (++cursor.index < cursor.entries->size()) {
      heap.push(cursor);
    }

    if (entry.descriptor.IsTombstone()) continue;
    std::string slot = VersionSlotKey(entry);
    if (!tombstones.empty() && tombstones.count(slot) != 0) continue;
    if (!seen.insert(slot).second) continue;

    result.push_back(entry);
    if (result.size() >= max_results) break;
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
  
  // 1. 从 MemTable 查询（热数据；Task 1.5：覆盖所有 target）
  for (uint64_t eid : entity_ids) {
    auto mem_results = mem_->GetRangeAllTargets(eid, entity_type, column_id,
                                                start, end);
    for (const auto& entry : mem_results) {
      if (results[eid].size() >= max_results_per_entity) break;
      results[eid].push_back(entry);
    }
  }
  
  // 2. 从 Immutable MemTable 查询
  if (imm_) {
    for (uint64_t eid : entity_ids) {
      if (results[eid].size() >= max_results_per_entity) continue;
      
      auto imm_results = imm_->GetRangeAllTargets(eid, entity_type, column_id,
                                                  start, end);
      for (const auto& entry : imm_results) {
        if (results[eid].size() >= max_results_per_entity) break;
        results[eid].push_back(entry);
      }
    }
  }
  
  // 3. 从 SST 文件批量查询
  // Task 3.5：文件集合取所有 entity_ids 的并集（不再只用 entity_ids[0]），
  // 且去掉 10 文件的静默截断；文件选择走区间索引。
  if (compaction_engine_) {
    uint64_t min_entity = entity_ids.front();
    uint64_t max_entity = entity_ids.front();
    for (uint64_t eid : entity_ids) {
      min_entity = std::min(min_entity, eid);
      max_entity = std::max(max_entity, eid);
    }
    auto relevant_files = LookupFilesInRange(min_entity, max_entity,
                                             static_cast<uint8_t>(entity_type),
                                             column_id);
    std::sort(relevant_files.begin(), relevant_files.end(),
              [](const IndexedFile& a, const IndexedFile& b) {
                return a.file_number > b.file_number;
              });
    
    // 对每个 SST 文件使用批量查询
    for (const auto& file_meta : relevant_files) {
      if (file_meta.max_timestamp < start.value() || 
          file_meta.min_timestamp > end.value()) {
        continue;  // 时间范围不重叠
      }
      // 检查是否所有 entity 都已经收集够数据
      bool all_full = true;
      for (uint64_t eid : entity_ids) {
        if (results[eid].size() < max_results_per_entity) {
          all_full = false;
          break;
        }
      }
      if (all_full) break;
      
      auto reader = GetCachedReader(file_meta.file_number, file_meta.path);
      if (!reader) continue;
      
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
  
  // Task 3.5：所有 entity_ids 的文件并集（区间索引），无静默截断
  uint64_t min_entity = entity_ids.front();
  uint64_t max_entity = entity_ids.front();
  for (uint64_t eid : entity_ids) {
    min_entity = std::min(min_entity, eid);
    max_entity = std::max(max_entity, eid);
  }
  std::vector<IndexedFile> relevant_files =
      LookupFilesInRange(min_entity, max_entity, static_cast<uint8_t>(entity_type),
                         column_id);
  std::sort(relevant_files.begin(), relevant_files.end(),
            [](const IndexedFile& a, const IndexedFile& b) {
              return a.file_number > b.file_number;
            });
  
  if (relevant_files.empty()) return results;
  
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
        
        auto reader = GetCachedReader(file_meta.file_number, file_meta.path);
        if (!reader) continue;
        
        // 获取时间范围内的所有数据
        auto positions = reader->GetRange(0, entity_type, column_id, start, end);
        
        // 过滤 entity_ids 中的数据
        for (const auto& [key, desc] : positions) {
          if (entity_set.find(key.entity_id()) != entity_set.end()) {
            thread_results[t][key.entity_id()].emplace_back(key, desc);
          }
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
  // 读路径持 memtable 强引用（审计 3.5）
  std::shared_ptr<VSLMemTable> mem;
  std::shared_ptr<VSLMemTable> imm;
  PinMemTables(&mem, &imm);
  if (mem) {
    auto mem_entries = mem->GetAll(entity_id, entity_type, column_id);
    for (const auto& entry : mem_entries) {
      // 墓碑不进入版本链结果（审计 3.2）
      if (entry.descriptor.IsTombstone()) continue;
      result.push_back({entry.timestamp, entry.descriptor, -1});
    }
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

bool LsmEngine::BeginFlushExclusive() {
  std::lock_guard<std::mutex> lock(flush_mutex_);
  if (flush_in_progress_) {
    return false;
  }
  flush_in_progress_ = true;
  return true;
}

void LsmEngine::EndFlushExclusive(const Status& status) {
  std::lock_guard<std::mutex> lock(flush_mutex_);
  flush_in_progress_ = false;
  last_flush_status_ = status;
  ++flush_count_;
}

Status LsmEngine::FlushClaimedTable(const std::shared_ptr<VSLMemTable>& table) {
  if (!table) {
    EndFlushExclusive(Status::OK());
    return Status::OK();
  }
  Status s = FlushMemTable(table.get());
  if (s.ok()) {
    {
      std::unique_lock<std::shared_mutex> lock(mutex_);
      if (imm_ == table) {
        imm_.reset();
      }
    }
    // t10：flush 成功（SST 已 fsync）后推进 WAL checkpoint 并回收旧段。
    // 截断失败不影响已提交数据（WAL 还在），只记录错误。
    Status cps = AdvanceWALCheckpointAfterFlush();
    if (!cps.ok()) {
      fprintf(stderr, "[cedar] WAL checkpoint advance failed: %s\n",
              cps.ToString().c_str());
    }
  }
  // 失败时保留 imm_：数据仍可读，下一次 ForceFlush 可以重试（审计 3.4）
  EndFlushExclusive(s);
  return s;
}

void LsmEngine::MaybeAutoFlush() {
  const size_t buffer_size =
      options_.write_buffer_size > 0 ? options_.write_buffer_size : (4u << 20);

  // 快路径：未到水位直接返回
  size_t usage = 0;
  {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (!mem_) return;
    usage = mem_->ApproximateMemoryUsage();
  }
  if (usage < buffer_size) {
    return;
  }

  // 同一时刻只允许一个 flush；已有 flush 在跑就下次 Put 再试
  if (!BeginFlushExclusive()) {
    return;
  }

  std::shared_ptr<VSLMemTable> to_flush;
  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    // 双检：抢到槽位后重新确认水位与 imm_ 空闲（并发 ForceFlush 可能已换表）
    if (!imm_ && mem_ && !mem_->IsEmpty() &&
        mem_->ApproximateMemoryUsage() >= buffer_size) {
      imm_ = mem_;
      mem_ = std::make_shared<VSLMemTable>();
      to_flush = imm_;
    }
  }
  if (!to_flush) {
    EndFlushExclusive(Status::OK());
    return;
  }

  // 自动 flush 同步执行（后台化留给 Phase 4 的 flush 优化）；
  // 失败时 FlushClaimedTable 保留 imm_，下次 ForceFlush 可重试
  FlushClaimedTable(to_flush);
}

Status LsmEngine::ForceFlush() {
  // 抢占唯一 flush 槽位（并发调用保持旧行为：返回 flush already in progress）
  if (!BeginFlushExclusive()) {
    return Status::IOError("LsmEngine", "flush already in progress");
  }

  // 1) 先重试上一次失败遗留的 imm_（失败不 reset，数据必须保持可读）
  std::shared_ptr<VSLMemTable> retained;
  {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    retained = imm_;
  }
  if (retained) {
    Status s = FlushMemTable(retained.get());
    if (!s.ok()) {
      EndFlushExclusive(s);
      return s;
    }
    {
      std::unique_lock<std::shared_mutex> lock(mutex_);
      if (imm_ == retained) {
        imm_.reset();
      }
    }
  }

  // 2) 再 flush 当前 memtable：在独占锁下换表，写集边界清晰（不丢写）
  std::shared_ptr<VSLMemTable> to_flush;
  {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    if (!imm_ && mem_ && !mem_->IsEmpty()) {
      imm_ = mem_;
      mem_ = std::make_shared<VSLMemTable>();
      to_flush = imm_;
    }
  }

  Status result = Status::OK();
  if (to_flush) {
    result = FlushMemTable(to_flush.get());
    if (result.ok()) {
      {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (imm_ == to_flush) {
          imm_.reset();
        }
      }
      // t10：flush 成功（SST 已 fsync）后推进 WAL checkpoint
      Status cps = AdvanceWALCheckpointAfterFlush();
      if (!cps.ok()) {
        fprintf(stderr, "[cedar] WAL checkpoint advance failed: %s\n",
                cps.ToString().c_str());
      }
    }
  }

  EndFlushExclusive(result);
  return result;
}

// =============================================================================
// WAL checkpoint / 截断（t10）
// =============================================================================

Status LsmEngine::PersistWALCheckpoint(uint64_t watermark_file_number,
                                       uint64_t file_number_floor) {
  WalCheckpoint checkpoint;
  checkpoint.watermark_file_number = watermark_file_number;
  checkpoint.file_number_floor = file_number_floor;
  if (wal_writer_ != nullptr) {
    checkpoint.flushed_records = wal_writer_->GetStats().records_written.load();
  }
  return WalCheckpointFile::Save(wal_checkpoint_path_, env_, checkpoint);
}

void LsmEngine::DeleteWALSegmentsBefore(uint64_t watermark_file_number) {
  if (watermark_file_number == 0) {
    return;
  }
  const std::string wal_dir = db_path_ + "/wal";
  std::vector<std::string> children;
  Status s = env_->GetChildren(wal_dir, &children);
  if (!s.ok()) {
    return;  // 段回收失败只影响空间，不影响正确性
  }
  for (const auto& child : children) {
    if (child.size() <= 4 || child.compare(child.size() - 4, 4, ".wal") != 0) {
      continue;
    }
    const std::string digits = child.substr(0, child.size() - 4);
    if (digits.empty()) continue;
    bool numeric = true;
    for (char c : digits) {
      if (c < '0' || c > '9') {
        numeric = false;
        break;
      }
    }
    if (!numeric) continue;
    const uint64_t number = std::strtoull(digits.c_str(), nullptr, 10);
    if (number >= watermark_file_number) {
      continue;
    }
    // 删除失败（权限/占用）只影响 WAL 目录大小：重放时这些段本来就低于水位，
    // 会被跳过；下次 flush 还会再尝试。
    env_->RemoveFile(wal_dir + "/" + child).IgnoreError();
  }
}

Status LsmEngine::AdvanceWALCheckpointAfterFlush() {
  if (!wal_truncation_enabled_ || wal_writer_ == nullptr) {
    return Status::OK();
  }

  std::lock_guard<std::mutex> lock(wal_checkpoint_mutex_);

  // 1) 轮转：把当前活动段封印，后续写入进入新段。
  uint64_t sealed = 0;
  Status s = wal_writer_->Rotate(&sealed);
  if (!s.ok()) {
    return s;
  }
  wal_seal_history_.push_back(sealed);
  while (wal_seal_history_.size() > 8) {
    wal_seal_history_.pop_front();
  }

  // 2) 覆盖判定：WAL-first 下"append 已完成、memtable 写入尚未执行"的写会落到
  //    下一张 memtable（跨界窗口）。因此段 S_i 的记录要到第 i+1 轮 flush 之后
  //    才全部在 SST 里 —— 水位只覆盖 S_{i-1}（2 轮滞后）。
  if (wal_seal_history_.size() < 2) {
    return Status::OK();  // 第一轮 flush：sealed 段可能含跨界写，不能删
  }
  const uint64_t covered_seal =
      wal_seal_history_[wal_seal_history_.size() - 2];
  uint64_t candidate = covered_seal + 1;
  const uint64_t active = wal_writer_->CurrentFileNumber();
  if (candidate > active) {
    candidate = active;  // 永不删活动段
  }
  if (candidate <= wal_checkpoint_watermark_) {
    return Status::OK();
  }

  // 3) 先持久化水位（此时本轮 SST 已在 FlushEntityGroup 里 fsync；
  //    checkpoint 文件名以 MANIFEST 开头，Sync 会顺带 fsync 目录），再删段。
  Status cps = PersistWALCheckpoint(candidate, active + 1);
  if (!cps.ok()) {
    fprintf(stderr,
            "[cedar] WAL checkpoint persist failed (keep segments): %s\n",
            cps.ToString().c_str());
    return cps;
  }
  wal_checkpoint_watermark_ = candidate;
  wal_file_number_floor_ = active + 1;

  // 4) 最后才删除已确认落盘的段
  DeleteWALSegmentsBefore(candidate);
  return Status::OK();
}

Status LsmEngine::FinalizeWALCheckpointOnClose() {
  if (!wal_truncation_enabled_ || wal_writer_ == nullptr) {
    return Status::OK();
  }

  std::lock_guard<std::mutex> lock(wal_checkpoint_mutex_);

  // 写者已停止、当前 memtable 已 flush + fsync：再封印一次，把水位推进到
  // "除这个空活动段之外的全部段"。
  uint64_t sealed = 0;
  Status s = wal_writer_->Rotate(&sealed);
  if (!s.ok()) {
    return s;
  }
  wal_seal_history_.push_back(sealed);
  const uint64_t active = wal_writer_->CurrentFileNumber();
  uint64_t candidate = active;
  if (candidate <= wal_checkpoint_watermark_) {
    return Status::OK();
  }
  Status cps = PersistWALCheckpoint(candidate, active + 1);
  if (!cps.ok()) {
    return cps;
  }
  wal_checkpoint_watermark_ = candidate;
  wal_file_number_floor_ = active + 1;
  DeleteWALSegmentsBefore(candidate);
  return Status::OK();
}

Status LsmEngine::Compact() {
  // Stub implementation
  return Status::OK();
}

LsmEngine::Stats LsmEngine::GetStats() const {
  Stats stats;
  std::shared_ptr<VSLMemTable> mem;
  std::shared_ptr<VSLMemTable> imm;
  PinMemTables(&mem, &imm);
  if (mem) {
    stats.memtable_size = mem->size();
  }
  if (imm) {
    stats.imm_memtable_size = imm->size();
  }
  // levels_ 会被 flush 在独占锁下 push_back（vector 扩容会使迭代失效），
  // 统计必须持共享锁读取，否则与并发 flush 构成数据竞争（TSan 可见）。
  {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    stats.num_levels = levels_.size();
    for (const auto& level : levels_) {
      stats.frond_count += level.size();
      for (const auto& meta : level) {
        stats.frond_size += meta.file_size;
      }
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
    // t9：恢复组提交。窗口可配置（默认沿用 WalOptions 的 1000µs）。
    // WalWriter::WriteBatch 返回 OK 蕴含"这条 record 已 append + sync"，
    // 因此 WAL-first 仍能拿到真实结果；多个并发提交共享一次 fsync。
    // 注意：必须在 Open 之前调用 SetWALGroupCommitWindow 才能生效。
    wal_options.group_commit_timeout_us = wal_group_commit_window_us_;
    // t10：段号下限 = max(checkpoint 里的 floor, 水位)，保证截断后不复用小号
    wal_options.min_file_number =
        std::max(wal_file_number_floor_, wal_checkpoint_watermark_);
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

  // 取当前 memtable 的裸指针（事务只在 Commit 时通过 LsmEngine::PutPinned 写、
  // 通过 LsmEngine::GetVersionChain 读，不会解引用这个裸指针，见审计 3.5）
  std::shared_ptr<VSLMemTable> mem;
  {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    mem = mem_;
  }
  if (!mem) {
    return nullptr;
  }

  // 创建 OCC 事务
  auto txn = std::make_unique<OCCTransaction>(
      txn_manager_.get(),      // TransactionManager
      mem.get(),               // VSLMemTable
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

Status LsmEngine::WriteWALBatch(const WalBatch& batch) {
  if (!wal_writer_ || batch.empty()) {
    return Status::OK();
  }
  // t9：恢复 WalWriter 组提交。WriteBatch 在"包含这条 record 的批次"完成
  // append+sync 之后才返回，所以这里不需要再 Sync；并发的事务提交与直接写
  // 会共享同一次 fsync（窗口由 SetWALGroupCommitWindow 配置）。
  // append/sync 的实际串行化在 WalWriter 内部（io_mutex_），这里不再加锁，
  // 避免把提交者串在 Engine 层。
  return wal_writer_->WriteBatch(batch);
}

std::shared_ptr<VSLMemTable> LsmEngine::GetMemTableShared() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return mem_;
}

std::shared_ptr<VSLMemTable> LsmEngine::GetImmutableMemTableShared() const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  return imm_;
}

std::vector<MemTableEntry> LsmEngine::GetVersionChain(uint64_t entity_id,
                                                     EntityType entity_type,
                                                     uint16_t column_id) {
  std::shared_ptr<VSLMemTable> mem;
  std::shared_ptr<VSLMemTable> imm;
  PinMemTables(&mem, &imm);
  if (mem) {
    auto chain = mem->GetVersionChain(entity_id, entity_type, column_id);
    if (!chain.empty()) {
      return chain;
    }
  }
  if (imm) {
    return imm->GetVersionChain(entity_id, entity_type, column_id);
  }
  return {};
}

std::vector<MemTableEntry> LsmEngine::GetVersionChain(uint64_t entity_id,
                                                     EntityType entity_type,
                                                     uint16_t column_id,
                                                     uint64_t target_id) {
  // Task 1.5：指定 target 的版本链（边查询/写写验证不再串到别的 dst）
  std::shared_ptr<VSLMemTable> mem;
  std::shared_ptr<VSLMemTable> imm;
  PinMemTables(&mem, &imm);
  if (mem) {
    auto chain = mem->GetVersionChain(entity_id, entity_type, column_id, target_id);
    if (!chain.empty()) {
      return chain;
    }
  }
  if (imm) {
    return imm->GetVersionChain(entity_id, entity_type, column_id, target_id);
  }
  return {};
}


WalReplayStats LsmEngine::GetWalReplayStats() const { return wal_replay_stats_; }

void LsmEngine::PinMemTables(std::shared_ptr<VSLMemTable>* mem,
                            std::shared_ptr<VSLMemTable>* imm) const {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  if (mem != nullptr) *mem = mem_;
  if (imm != nullptr) *imm = imm_;
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

  // 按 (entity_type, column_id) 分组收集条目（t7：向量分组替代 std::map，
  // 组数通常很小，线性查找即可；避免 map 节点分配与额外的整组复制）。
  struct Group {
    uint8_t entity_type = 0;
    uint16_t column_id = 0;
    std::vector<std::pair<CedarKey, Descriptor>> entries;
  };
  std::vector<Group> groups;
  groups.reserve(8);

  mem->Traverse([&](const CedarKey& key, const Descriptor& descriptor) -> bool {
    // 墓碑必须落盘（审计 3.2）：删除是持久化语义的一部分。compaction 在
    // Phase 4 之前不清理墓碑（ShouldDropTombstone/CanDropTombstone 保持 false）。
    const uint8_t et = static_cast<uint8_t>(key.entity_type());
    const uint16_t col = key.column_id();
    Group* target = nullptr;
    for (auto& group : groups) {
      if (group.entity_type == et && group.column_id == col) {
        target = &group;
        break;
      }
    }
    if (target == nullptr) {
      groups.push_back(Group{et, col, {}});
      target = &groups.back();
    }
    target->entries.emplace_back(key, descriptor);
    return true;
  });

  // 为每个组创建单独的 SST 文件
  for (auto& group : groups) {
    Status s = FlushEntityGroup(group.entity_type, group.column_id, group.entries);
    if (!s.ok()) {
      return s;
    }
  }

  return Status::OK();
}

Status LsmEngine::FlushEntityGroup(uint8_t entity_type, uint16_t column_id,
                                   std::vector<std::pair<CedarKey, Descriptor>>& entries) {
  if (entries.empty()) {
    return Status::OK();
  }

  uint64_t file_number = next_file_number_.fetch_add(1);
  std::string filepath = SstFilePath(file_number);

  uint64_t min_entity_id = UINT64_MAX;
  uint64_t max_entity_id = 0;
  uint64_t min_tx_time = UINT64_MAX;
  uint64_t max_tx_time = 0;

  // Task 4.5：调用方已按 CedarKey::Compare 排序时直接使用（is_sorted O(n)），
  // 只有乱序输入才原地排序（entries 是调用方传入的临时向量）。
  if (!std::is_sorted(entries.begin(), entries.end(),
                      [](const auto& a, const auto& b) {
                        return a.first < b.first;
                      })) {
    std::sort(entries.begin(), entries.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
  }

  // SST 的持久化策略（t10）：
  //  - WAL 截断开启（默认）：SST 必须在本轮 flush 结束前真正 fsync，因为随后会
  //    删除被它覆盖的 WAL 段；否则掉电时 WAL 与 SST 可能同时丢失（t7 在
  //    FlushSstWritableFile 的注释里也提示了这一点）。
  //  - WAL 截断关闭：沿用 t7 的快路径（包装类把 Sync 退化为 Flush），
  //    数据在 WAL 里有副本，WAL 不回收。
  const bool durable_sst =
      options_.enable_wal && wal_writer_ != nullptr && wal_truncation_enabled_;

  cedar::WritableFile* raw_file = nullptr;
  cedar::Status ls = env_->NewWritableFile(filepath, &raw_file);
  if (!ls.ok()) {
    return Status::IOError("LsmEngine", ls.ToString());
  }
  std::unique_ptr<cedar::WritableFile> owned_file;
  cedar::WritableFile* builder_file = raw_file;
  if (options_.enable_wal && wal_writer_ != nullptr && !durable_sst) {
    owned_file = std::make_unique<FlushSstWritableFile>(raw_file);
    builder_file = owned_file.get();
  } else {
    owned_file.reset(raw_file);
  }

  ZoneColumnarSstBuilder builder(options_, builder_file, column_id, db_path_);
  builder.SetFileNumber(file_number);
  builder.SetLevel(0);

  for (const auto& [key, descriptor] : entries) {
    builder.Add(key, descriptor);

    min_entity_id = std::min(min_entity_id, key.entity_id());
    max_entity_id = std::max(max_entity_id, key.entity_id());
    min_tx_time = std::min(min_tx_time, key.timestamp().value());
    max_tx_time = std::max(max_tx_time, key.timestamp().value());
  }

  Status fs = builder.Finish();
  if (fs.ok() && durable_sst) {
    // WAL 截断的前置条件：SST 数据先落盘（F_FULLFSYNC），再允许删 WAL。
    // 这里直接对底层文件 Sync（绕过 FlushSstWritableFile 的退化包装）。
    fs = raw_file->Sync();
  }
  // 关闭文件（wrapper 析构时释放 inner；裸指针直接 delete）
  owned_file.reset();

  if (!fs.ok()) {
    // 半截 SST 不能留在目录里被重启当成有效文件引用
    env_->RemoveFile(filepath).IgnoreError();
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

    // Task 3.4：新 flush 文件增量进区间索引（索引未就绪时下次查询全量重建）
    IndexedFile indexed;
    indexed.file_number = file_number;
    indexed.file_size = file_size;
    indexed.num_entries = builder.NumEntries();
    indexed.min_entity_id = min_entity_id;
    indexed.max_entity_id = max_entity_id;
    indexed.min_timestamp = min_tx_time;
    indexed.max_timestamp = max_tx_time;
    indexed.column_id = column_id;
    indexed.entity_type = entity_type;
    indexed.level = 0;
    indexed.path = filepath;
    AddFileToIndex(indexed);
    
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
  
  // Task 3.4/3.5：区间索引选文件 + reader cache；去掉 50 文件的静默截断。
  auto candidates = LookupFiles(entity_id, static_cast<uint8_t>(entity_type), column_id);
  std::sort(candidates.begin(), candidates.end(),
            [](const IndexedFile& a, const IndexedFile& b) {
              return a.file_number > b.file_number;
            });
  
  // Query candidate files
  for (const auto& meta : candidates) {
    auto reader = GetCachedReader(meta.file_number, meta.path);
    if (!reader) continue;
    
    // Bloom Filter optimization: skip files that definitely don't contain the entity
    if (!reader->MayContainEntity(entity_id)) {
      continue;
    }
    
    // Get all versions for this entity using GetRange
    // Use full time range to get all versions
    auto versions = reader->GetRange(entity_id, entity_type, column_id, 
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

// ============================================================================
// Task 3.4：每 (entity_type, column_id) 的文件区间索引
// ============================================================================

namespace {
inline uint64_t FileGroupKey(uint8_t entity_type, uint16_t column_id) {
  return (static_cast<uint64_t>(entity_type) << 32) | column_id;
}
}  // namespace

void LsmEngine::RebuildFileIndex() {
  std::unordered_map<uint64_t, FileGroupIndex> index;
  uint64_t generation = 0;

  if (compaction_engine_) {
    for (int level = 0; level < compaction_config_.max_levels; ++level) {
      auto metas = compaction_engine_->GetLevelFiles(level);
      for (auto& meta : metas) {
        IndexedFile file;
        file.file_number = meta.file_number;
        file.file_size = meta.file_size;
        file.num_entries = meta.num_entries;
        file.min_entity_id = meta.min_entity_id;
        file.max_entity_id = meta.max_entity_id;
        file.min_timestamp = meta.min_timestamp;
        file.max_timestamp = meta.max_timestamp;
        file.column_id = meta.column_id;
        file.entity_type = meta.entity_type;
        file.level = level;
        file.path = std::move(meta.path);
        index[FileGroupKey(file.entity_type, file.column_id)].files.push_back(
            std::move(file));
      }
    }
    CompactionStats stats;
    compaction_engine_->GetStats(&stats);
    generation = stats.total_compactions.load();
  }

  for (auto& [key, group] : index) {
    auto& files = group.files;
    std::sort(files.begin(), files.end(), [](const IndexedFile& a, const IndexedFile& b) {
      return a.min_entity_id < b.min_entity_id;
    });
    group.prefix_max_entity.resize(files.size());
    uint64_t prefix_max = 0;
    for (size_t i = 0; i < files.size(); ++i) {
      prefix_max = std::max(prefix_max, files[i].max_entity_id);
      group.prefix_max_entity[i] = prefix_max;
    }
  }

  {
    std::unique_lock<std::shared_mutex> lock(file_index_mutex_);
    file_interval_index_ = std::move(index);
    file_index_compaction_generation_ = generation;
    file_index_valid_ = true;
  }
}

void LsmEngine::AddFileToIndex(const IndexedFile& file) {
  std::unique_lock<std::shared_mutex> lock(file_index_mutex_);
  if (!file_index_valid_) return;  // 下次查询会全量重建

  auto& group = file_interval_index_[FileGroupKey(file.entity_type, file.column_id)];
  auto& files = group.files;
  auto it = std::lower_bound(files.begin(), files.end(), file.min_entity_id,
                             [](const IndexedFile& a, uint64_t min_entity) {
                               return a.min_entity_id < min_entity;
                             });
  files.insert(it, file);

  // 前缀最大 max_entity 需要重建（单组 O(k)）；pick 查询的正确性依赖它。
  group.prefix_max_entity.resize(files.size());
  uint64_t prefix_max = 0;
  for (size_t i = 0; i < files.size(); ++i) {
    prefix_max = std::max(prefix_max, files[i].max_entity_id);
    group.prefix_max_entity[i] = prefix_max;
  }
}

void LsmEngine::EnsureFileIndexFresh() const {
  if (!compaction_engine_) return;
  CompactionStats stats;
  compaction_engine_->GetStats(&stats);
  const uint64_t generation = stats.total_compactions.load();
  bool stale = false;
  {
    std::shared_lock<std::shared_mutex> lock(file_index_mutex_);
    stale = !file_index_valid_ || generation != file_index_compaction_generation_;
  }
  if (stale) {
    // 后台 compaction 完成后统计计数会变化；索引只在此时全量重建。
    const_cast<LsmEngine*>(this)->RebuildFileIndex();
  }
}

std::vector<LsmEngine::IndexedFile> LsmEngine::LookupFiles(
    uint64_t entity_id, uint8_t entity_type, uint16_t column_id) const {
  EnsureFileIndexFresh();

  std::vector<IndexedFile> out;
  std::shared_lock<std::shared_mutex> lock(file_index_mutex_);
  auto it = file_interval_index_.find(FileGroupKey(entity_type, column_id));
  if (it == file_interval_index_.end()) return out;

  const auto& group = it->second;
  const auto& files = group.files;
  // 第一个 min_entity_id > entity_id 的位置
  const size_t hi = static_cast<size_t>(
      std::upper_bound(files.begin(), files.end(), entity_id,
                       [](uint64_t value, const IndexedFile& file) {
                         return value < file.min_entity_id;
                       }) -
      files.begin());
  for (size_t i = hi; i > 0; --i) {
    if (group.prefix_max_entity[i - 1] < entity_id) break;
    const IndexedFile& file = files[i - 1];
    if (entity_id >= file.min_entity_id && entity_id <= file.max_entity_id) {
      out.push_back(file);
    }
  }
  return out;
}

std::vector<LsmEngine::IndexedFile> LsmEngine::LookupFilesInRange(
    uint64_t min_entity, uint64_t max_entity, uint8_t entity_type,
    uint16_t column_id) const {
  EnsureFileIndexFresh();

  std::vector<IndexedFile> out;
  if (min_entity > max_entity) return out;
  std::shared_lock<std::shared_mutex> lock(file_index_mutex_);
  auto it = file_interval_index_.find(FileGroupKey(entity_type, column_id));
  if (it == file_interval_index_.end()) return out;

  const auto& group = it->second;
  const auto& files = group.files;
  const size_t hi = static_cast<size_t>(
      std::upper_bound(files.begin(), files.end(), max_entity,
                       [](uint64_t value, const IndexedFile& file) {
                         return value < file.min_entity_id;
                       }) -
      files.begin());
  for (size_t i = hi; i > 0; --i) {
    if (group.prefix_max_entity[i - 1] < min_entity) break;
    const IndexedFile& file = files[i - 1];
    if (file.max_entity_id >= min_entity) {
      out.push_back(file);
    }
  }
  return out;
}

std::shared_ptr<ZoneColumnarSstReader> LsmEngine::GetCachedReader(
    uint64_t file_number, const std::string& path) const {
  if (!sst_reader_cache_) return nullptr;
  return sst_reader_cache_->Get(file_number, path);
}

// ============================================================================
// Task 3.4：每 (entity_type, column_id) 的文件区间索引
// ============================================================================

namespace {
}  // namespace







// ========== Size-Tiered Compaction 集成 ==========


void LsmEngine::MigrateExistingSstFiles() {
  // 将现有的 SST 文件从旧格式迁移到新的 Compaction 引擎
  // 注意：这是一个一次性迁移，在首次启用新引擎时调用
  
  if (!compaction_engine_) return;
  
  for (int level = 0; level < static_cast<int>(levels_.size()); ++level) {
    for (const auto& old_meta : levels_[level]) {
      // 审计 9.3：LoadManifest 已把 MANIFEST 里的高层文件恢复好（同一目录下的
      // .frond 也会被 LoadSstFiles 当作 L0 扫描到）。同一文件号绝不能同时登记到
      // L0 和 L1 —— 否则 L0 压缩会删掉 L1 仍在引用的物理文件。
      if (compaction_engine_->HasFile(old_meta.file_number)) {
        continue;
      }
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
  
  // 执行全量合并
  Status s = compaction_engine_->CompactAll();
  
  
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
    // 过期，移除缓存（用 key 重新查找：解锁后 it 可能已失效）
    std::unique_lock<std::shared_mutex> write_lock(cross_query_cache_mutex_);
    cross_query_cache_.erase(key);
    return std::nullopt;
  }

  // 缓存命中。hit_count 是可变的命中统计：多个读者同时自增会在共享锁下
  // 产生数据竞争（TSan 可见），因此这里升级为独占锁并按 key 重新查找。
  lock.unlock();
  {
    std::unique_lock<std::shared_mutex> write_lock(cross_query_cache_mutex_);
    auto write_it = cross_query_cache_.find(key);
    if (write_it == cross_query_cache_.end()) {
      return std::nullopt;  // 读取期间被清理或失效
    }
    write_it->second.hit_count++;
    return write_it->second.data;
  }
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
    auto files = LookupFiles(entity_id, entity_type, column_id);
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
