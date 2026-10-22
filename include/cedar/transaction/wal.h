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

#ifndef FERN_WAL_H_
#define FERN_WAL_H_

#include <atomic>
#include <condition_variable>
#include <deque>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "cedar/types/descriptor.h"
#include "cedar/types/cedar_key.h"
#include "cedar/core/slice.h"
#include "cedar/core/status.h"

#include "cedar/core/env.h"

namespace cedar {

// WAL 日志记录类型
enum class WalRecordType : uint8_t {
  kInvalid = 0,
  kPut = 1,           // 插入/更新
  kDelete = 2,        // 删除
  kBatch = 3,         // 批量操作
  kCommit = 4,        // 事务提交
  kAbort = 5,         // 事务回滚
  kMax = 6
};

// WAL 日志记录头部 (固定 16 字节)
struct WalRecordHeader {
  uint32_t crc32;           // 校验和
  uint16_t type;            // 记录类型 (WalRecordType)
  uint16_t flags;           // 标志位
  uint32_t data_length;     // 数据长度
  uint32_t sequence;        // 序列号
  
  static constexpr size_t kEncodedSize = 16;
  
  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);
};

// 单个写操作
// 修复: 添加 txn_version 字段以支持崩溃恢复后的完整 MVCC 隔离
struct WalWriteOp {
  WalRecordType type;
  CedarKey key;
  Descriptor descriptor;    // 对于 Delete，descriptor 是 Tombstone
  Timestamp txn_version;    // 事务版本号（用于 MVCC 恢复）
  
  WalWriteOp() = default;
  WalWriteOp(WalRecordType t, const CedarKey& k, const Descriptor& d, Timestamp txn_ver)
      : type(t), key(k), descriptor(d), txn_version(txn_ver) {}
};

// 批量写操作 (Group Commit)
class WalBatch {
 public:
  WalBatch() = default;
  
  // 添加操作
  void Put(const CedarKey& key, const Descriptor& descriptor, Timestamp txn_version);
  void Delete(const CedarKey& key, Timestamp txn_version);
  
  // 清空
  void Clear();
  
  // 编码/解码
  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);
  
  // 获取操作列表
  const std::vector<WalWriteOp>& ops() const { return ops_; }
  size_t size() const { return ops_.size(); }
  bool empty() const { return ops_.empty(); }
  
 private:
  std::vector<WalWriteOp> ops_;
};

// WAL 单次提交的持久化级别（t9）。
//  - kSync  : append + fsync（macOS 用 F_FULLFSYNC），掉电/OS 崩溃可恢复；
//             事务提交用这个级别（与 Task 1.2 的语义一致）。
//  - kWrite : append + flush 到 OS（write(2)），进程崩溃/被 kill 后可恢复，
//             但不保证掉电持久；直接写（LsmEngine::Put/Delete）默认用这个级别，
//             避免每次写都付 F_FULLFSYNC（本机约 2.7ms）。可用
//             LsmEngine::SetDirectWriteSync(true) 提升为 kSync。
enum class WalDurability : uint8_t {
  kWrite = 0,
  kSync = 1,
};

// WAL 写入器配置
struct WalOptions {
  // WAL 文件大小限制 (默认 64MB)
  size_t max_file_size = 64 * 1024 * 1024;
  
  // 组提交窗口 (微秒)。语义（Task t9）：
  //  - 0  : 同步模式——每个 WriteBatch 自己 append + sync 后才返回；
  //  - >0 : 组提交模式——多个并发 WriteBatch 共享一次 append + sync，
  //         但每个调用必须等到「包含自己那条 record 的批次」sync 完成后才返回
  //         （GroupCommitRequest::promise 在 Sync 之后才 set_value）。
  //         该值同时是组提交线程空闲时的等待上限。
  // 组提交汇聚窗口上限：即使配置更大，单批也只等这么多（控制单写者延迟）
  static constexpr uint32_t kMaxAccumulateUs = 50;
  uint32_t group_commit_timeout_us = 1000;  // 1ms

  // 单次组提交最多合并的请求数
  size_t group_commit_max_batch = 1000;
  
  // 是否启用 fsync (false 使用 fdatasync)
  bool use_fsync = false;

  // 预分配文件大小
  size_t preallocate_size = 64 * 1024 * 1024;

  // 文件号下限（t10）：Open 时创建的 WAL 段号 >= 该值。
  // 用于 WAL 截断后保持段号单调：段被删除后目录里可能只剩一个空段，
  // 若仅按目录扫描取号会回绕到小号，重放端会按 checkpoint 水位跳过新记录。
  uint64_t min_file_number = 0;
};

// WAL 统计
struct WalStats {
  std::atomic<uint64_t> bytes_written{0};
  std::atomic<uint64_t> records_written{0};
  std::atomic<uint64_t> batches_committed{0};
  std::atomic<uint64_t> syncs{0};
  std::atomic<uint64_t> group_commit_waits{0};
  
  WalStats() = default;
  WalStats(const WalStats& other) {
    bytes_written.store(other.bytes_written.load(), std::memory_order_relaxed);
    records_written.store(other.records_written.load(), std::memory_order_relaxed);
    batches_committed.store(other.batches_committed.load(), std::memory_order_relaxed);
    syncs.store(other.syncs.load(), std::memory_order_relaxed);
    group_commit_waits.store(other.group_commit_waits.load(), std::memory_order_relaxed);
  }
  WalStats& operator=(const WalStats& other) {
    if (this != &other) {
      bytes_written.store(other.bytes_written.load(), std::memory_order_relaxed);
      records_written.store(other.records_written.load(), std::memory_order_relaxed);
      batches_committed.store(other.batches_committed.load(), std::memory_order_relaxed);
      syncs.store(other.syncs.load(), std::memory_order_relaxed);
      group_commit_waits.store(other.group_commit_waits.load(), std::memory_order_relaxed);
    }
    return *this;
  }
};

// WAL 写入器 - 支持组提交
class WalWriter {
 public:
  WalWriter(const std::string& wal_dir,
            cedar::Env* env,
            const WalOptions& options);
  ~WalWriter();
  
  // 禁止拷贝
  WalWriter(const WalWriter&) = delete;
  WalWriter& operator=(const WalWriter&) = delete;
  
  // 打开 WAL
  Status Open();
  
  // 关闭 WAL
  Status Close();
  
  // 同步 WAL 到磁盘
  Status Sync();

  // 轮转到新的 WAL 段（t10）：同步并关闭当前段，创建一个新的空段。
  // 返回刚关闭（封印）的段号。截断流程必须在"memtable 已换表"之后调用它，
  // 这样被封印段里的记录要么已在本轮 flush 的 SST 里，要么在下一轮 flush 的
  // memtable 里（WAL-first 的跨界窗口，见 LsmEngine 的 2 轮滞后规则）。
  Status Rotate(uint64_t* sealed_file_number);

  // 当前活动段号（未开始写时是最近创建的段）。
  uint64_t CurrentFileNumber() const;
  
  // ========== 单条写入接口 ==========
  
  // 写入单条 Put 记录
  Status WritePut(const CedarKey& key, const Descriptor& descriptor, Timestamp txn_version);
  
  // 写入单条 Delete 记录
  Status WriteDelete(const CedarKey& key, Timestamp txn_version);
  
  // ========== 批量/组提交接口 ==========

  // 提交批量操作（默认 kSync 持久化级别）。
  // 返回 OK 当且仅当这条 record 已 append 并完成了对应持久化级别要求的动作
  // （kSync: fsync；kWrite: flush 到 OS）。组提交模式下多个并发调用共享一次
  // 持久化动作，但每个调用都等到包含自己的批次完成后才返回；失败返回真实错误。
  Status WriteBatch(const WalBatch& batch);

  // 指定持久化级别的提交。同一批次里只要有一条 kSync，整批就按 kSync 处理
  //（顺带满足 kWrite 请求，语义只强不弱）。
  Status WriteBatch(const WalBatch& batch, WalDurability durability);
  
  // 异步提交批量操作，返回一个 future 用于等待完成
  // 调用者可以通过返回的 sequence 查询写入状态
  Status WriteBatchAsync(const WalBatch& batch, uint64_t* sequence);
  
  // 等待指定序列号到达持久化水位。
  // 注意：该接口只回答"是否已持久化"，不携带失败原因；
  // 需要错误码的调用方请使用 WriteBatch。
  Status WaitForSequence(uint64_t sequence);

  // 组提交窗口 (微秒)，0 = 同步模式；必须在 Open() 之前设置。
  void SetGroupCommitWindow(uint32_t micros) {
    options_.group_commit_timeout_us = micros;
  }
  uint32_t GetGroupCommitWindow() const {
    return options_.group_commit_timeout_us;
  }
  
  // ========== 事务接口 ==========
  
  // 写入事务提交记录
  Status WriteCommit(uint64_t txn_id, Timestamp txn_version);
  
  // 写入事务回滚记录
  Status WriteAbort(uint64_t txn_id, Timestamp txn_version);
  
  // 获取当前序列号
  uint64_t CurrentSequence() const {
    return next_sequence_.load(std::memory_order_acquire) - 1;
  }
  
  // 获取统计信息
  WalStats GetStats() const { return stats_; }
  
 private:
  // 内部写入实现（调用方必须已持有 io_mutex_）
  Status WriteInternalLocked(const WalBatch& batch, uint64_t sequence);

  // 切换 WAL 文件（公开路径自己加 io_mutex_，内部路径用 Locked 版本）
  Status SwitchWALFile();
  Status SwitchWALFileLocked();
  Status RotateLocked(uint64_t* sealed_file_number);

  Status SyncLocked();
  // 只把 WritableFile 的用户态缓冲写进 OS（不 fsync）
  Status FlushLocked();

  // 组提交工作线程
  void GroupCommitThread();

  // 处理组提交队列：取一批请求 → 逐条 append → 一次 sync → 同步后唤醒调用方
  void ProcessGroupCommit();

  // 记录持久化水位（成功）或失败序列号，并唤醒 WaitForSequence 的等待者
  void MarkDurable(uint64_t sequence, const Status& status);

  // append 失败后换掉当前 WAL 文件：半条 record 不能污染后续写入（best effort）
  void RotateAfterFailureLocked();
  
  std::string wal_dir_;
  cedar::Env* env_;
  WalOptions options_;
  
  // 当前 WAL 文件
  WritableFile* current_file_;
  std::string current_file_path_;
  uint64_t current_file_size_;
  uint32_t file_number_;
  
  // 序列号生成
  std::atomic<uint64_t> next_sequence_{1};
  
  // 组提交相关
  struct GroupCommitRequest {
    WalBatch batch;
    uint64_t sequence;
    WalDurability durability = WalDurability::kSync;
    std::promise<Status> promise;
  };
  
  std::mutex commit_queue_mutex_;
  std::condition_variable commit_cv_;
  std::deque<std::shared_ptr<GroupCommitRequest>> commit_queue_;
  
  std::atomic<bool> shutdown_{false};
  std::unique_ptr<std::thread> group_commit_thread_;

  // WritableFile 不是线程安全的：append/sync/switch 全部串行化
  mutable std::mutex io_mutex_;
  // 持久化水位：durable_sequence_ 及其之前的请求已 append + sync
  mutable std::mutex durable_mutex_;
  std::condition_variable durable_cv_;
  uint64_t durable_sequence_ = 0;
  uint64_t failed_sequence_ = 0;  // 第一个失败请求的 sequence（0 = 无失败）
  Status last_failure_;
  // 串行化 ProcessGroupCommit（调用方内联驱动 + 组提交线程共用）
  mutable std::mutex process_mutex_;

  // 统计
  mutable WalStats stats_;
};

// WAL 读取器 - 用于恢复
class WalReader {
 public:
  WalReader(const std::string& wal_file, cedar::Env* env);
  ~WalReader();
  
  // 打开 WAL 文件
  Status Open();
  
  // 读取下一条记录
  // 返回 Status::NotFound() 表示到达文件末尾
  Status ReadNextRecord(WalBatch* batch, uint64_t* sequence);
  
  // 获取 WAL 文件列表 (按文件名排序)
  static Status ListWALFiles(const std::string& wal_dir, 
                              cedar::Env* env,
                              std::vector<std::string>* files);
  
 private:
  std::string wal_file_;
  cedar::Env* env_;
  RandomAccessFile* file_;
  uint64_t file_size_;
  uint64_t current_offset_;
};

}  // namespace cedar

#endif  // FERN_WAL_H_
