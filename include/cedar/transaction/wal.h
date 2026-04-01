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

// WAL 写入器配置
struct WalOptions {
  // WAL 文件大小限制 (默认 64MB)
  size_t max_file_size = 64 * 1024 * 1024;
  
  // 组提交超时 (微秒)，0 表示不启用组提交
  uint32_t group_commit_timeout_us = 1000;  // 1ms
  
  // 组提交最大批次大小
  size_t group_commit_max_batch = 1000;
  
  // 是否启用 fsync (false 使用 fdatasync)
  bool use_fsync = false;
  
  // 预分配文件大小
  size_t preallocate_size = 64 * 1024 * 1024;
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
  
  // ========== 单条写入接口 ==========
  
  // 写入单条 Put 记录
  Status WritePut(const CedarKey& key, const Descriptor& descriptor, Timestamp txn_version);
  
  // 写入单条 Delete 记录
  Status WriteDelete(const CedarKey& key, Timestamp txn_version);
  
  // ========== 批量/组提交接口 ==========
  
  // 提交批量操作 (可能加入组提交队列等待)
  // 如果启用了组提交，调用会阻塞直到批次被写入
  Status WriteBatch(const WalBatch& batch);
  
  // 异步提交批量操作，返回一个 future 用于等待完成
  // 调用者可以通过返回的 sequence 查询写入状态
  Status WriteBatchAsync(const WalBatch& batch, uint64_t* sequence);
  
  // 等待指定序列号的写入完成
  Status WaitForSequence(uint64_t sequence);
  
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
  // 内部写入实现
  Status WriteInternal(const WalBatch& batch);
  
  // 切换 WAL 文件
  Status SwitchWALFile();
  
  // 组提交工作线程
  void GroupCommitThread();
  
  // 处理组提交队列
  void ProcessGroupCommit();
  
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
    std::promise<Status> promise;
  };
  
  std::mutex commit_queue_mutex_;
  std::condition_variable commit_cv_;
  std::deque<std::shared_ptr<GroupCommitRequest>> commit_queue_;
  
  std::atomic<bool> shutdown_{false};
  std::unique_ptr<std::thread> group_commit_thread_;
  
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
