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

#ifndef FERN_GRAPH_DB_IMPL_H_
#define FERN_GRAPH_DB_IMPL_H_

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>
#include <thread>
#include <condition_variable>

#include "cedar/db/graph_db.h"
#include "cedar/db/manifest.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/transaction/wal.h"

namespace cedar {

// 列族数据
struct ColumnFamilyData {
  uint32_t id;
  std::string name;
  std::unique_ptr<LsmEngine> engine;
  std::atomic<bool> dropped{false};
  
  ColumnFamilyData(uint32_t id_, const std::string& name_)
      : id(id_), name(name_) {}
};

// CedarGraphDB 实现
class CedarGraphDBImpl {
 public:
  CedarGraphDBImpl(const std::string& db_path, 
                  const CedarGraphOptions& options);
  ~CedarGraphDBImpl();
  
  // 禁止拷贝
  CedarGraphDBImpl(const CedarGraphDBImpl&) = delete;
  CedarGraphDBImpl& operator=(const CedarGraphDBImpl&) = delete;
  
  // 打开/关闭
  Status Open();
  Status Close();
  
  // 基本操作
  Status Put(const CedarKey& key, const Descriptor& descriptor,
             const WriteOptions& options);
  Status Delete(const CedarKey& key, const WriteOptions& options);
  std::optional<Descriptor> Get(const CedarKey& key,
                                const ReadOptions& options);
  
  // 事务
  std::unique_ptr<OCCTransaction> BeginTransaction(
      const TransactionOptions& options);
  
  // 管理操作
  Status Flush(const FlushOptions& options = FlushOptions{});
  Status CompactRange(const CompactRangeOptions& options);
  
  // 快照
  const Snapshot* GetSnapshot();
  void ReleaseSnapshot(const Snapshot* snapshot);
  
  // 列族操作
  Status CreateColumnFamily(const std::string& name,
                            ColumnFamilyHandle** handle);
  Status DropColumnFamily(ColumnFamilyHandle* handle);
  ColumnFamilyHandle* DefaultColumnFamily();
  
  // 属性
  Status GetProperty(const std::string& property, std::string* value);
  std::string GetStatsString();
  uint64_t GetLatestSequenceNumber() const;
  
  // 备份
  Status CreateCheckpoint(const std::string& checkpoint_dir);
  
  // 内部访问
  LsmEngine* GetLsmEngine() { return default_cf_->engine.get(); }
  VersionSet* GetVersionSet() { return &version_set_; }
  ManifestManager* GetManifestManager() { return &manifest_manager_; }
  
 private:
  // 数据库路径和选项
  std::string db_path_;
  CedarGraphOptions options_;
  Env* env_;
  
  // 状态
  std::atomic<bool> opened_{false};
  std::atomic<bool> shutting_down_{false};
  
  // Manifest 和版本管理
  ManifestManager manifest_manager_;
  VersionSet version_set_;
  
  // 列族
  std::mutex cf_mutex_;
  std::vector<std::unique_ptr<ColumnFamilyData>> column_families_;
  ColumnFamilyData* default_cf_ = nullptr;
  std::atomic<uint32_t> next_cf_id_{1};
  
  // WAL
  std::unique_ptr<WalWriter> wal_writer_;
  std::mutex wal_mutex_;
  
  // 后台线程
  std::thread bg_flush_thread_;
  std::thread bg_compact_thread_;
  std::condition_variable bg_cv_;
  std::mutex bg_mutex_;
  std::atomic<bool> bg_work_paused_{false};
  
  // 快照
  std::mutex snapshot_mutex_;
  std::vector<const Snapshot*> snapshots_;
  std::atomic<uint64_t> next_snapshot_id_{1};
  
  // 统计
  struct Stats {
    std::atomic<uint64_t> puts{0};
    std::atomic<uint64_t> gets{0};
    std::atomic<uint64_t> deletes{0};
    std::atomic<uint64_t> flushes{0};
    std::atomic<uint64_t> compactions{0};
    std::atomic<uint64_t> transactions{0};
  } stats_;
  
  // 内部方法
  Status Recover();
  Status CreateColumnFamilyInternal(const std::string& name, 
                                     uint32_t cf_id,
                                     ColumnFamilyData** cf_data);
  Status WriteToWAL(const CedarKey& key, const Descriptor& desc,
                    uint64_t sequence);
  void BackgroundFlushThread();
  void BackgroundCompactThread();
  Status MaybeScheduleFlush();
  Status MaybeScheduleCompaction();
  Status DoCompaction(int level);
  ColumnFamilyData* FindColumnFamily(uint32_t id);
  ColumnFamilyData* FindColumnFamily(const std::string& name);
};

// 快照实现
class SnapshotImpl : public Snapshot {
 public:
  SnapshotImpl(uint64_t id, uint64_t seq, uint64_t ts)
      : id_(id), sequence_number_(seq), timestamp_(ts) {}
  
  uint64_t GetSequenceNumber() const override { return sequence_number_; }
  uint64_t GetTimestamp() const override { return timestamp_; }
  uint64_t GetId() const { return id_; }
  
 private:
  uint64_t id_;
  uint64_t sequence_number_;
  uint64_t timestamp_;
};

// 列族句柄实现
class ColumnFamilyHandleImpl : public ColumnFamilyHandle {
 public:
  ColumnFamilyHandleImpl(ColumnFamilyData* cfd) : cfd_(cfd) {}
  
  uint32_t GetID() const override { return cfd_->id; }
  const std::string& GetName() const override { return cfd_->name; }
  std::string GetStats() const override;
  
  ColumnFamilyData* GetCFD() const { return cfd_; }
  
 private:
  ColumnFamilyData* cfd_;
};

}  // namespace cedar

#endif  // FERN_GRAPH_DB_IMPL_H_
