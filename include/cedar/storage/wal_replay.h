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
// WAL 重放（Task 1.2 / 审计 3.3）
// =============================================================================
// 设计约束：不修改 CedarKey 32B 布局与 WAL 磁盘格式，也不改 WalWriter/WalBatch
// 的既有语义。现有 WAL 只能编码 kPut/kDelete 两种 op，没有 kCommit 的写入口，
// 因此"提交围栏"（commit fence）以一条特殊 op 的形式追加到写集**同一个 WAL
// record** 里：
//
//   WalRecord = [num_ops][op...][fence_op]
//   fence_op  = kPut + 保留 key(entity=0, ts=Max, type=0xFF, reserved=0xF00D)
//               + Metadata descriptor(payload=kFenceMagic)
//
// 一条 record 由 CRC 保护，整条读成功才解码，因此"写集 + 围栏"天然原子：
//  - 带围栏的 record  = 已提交事务 → 按顺序重放；
//  - 不带围栏的 record = 未提交（进程在写集落盘后、提交前崩溃）→ 整条跳过；
//  - record 截断/CRC 错  = 崩溃尾巴 → 停止读该文件，保留已重放部分。
//
// 重放按 (key 字节, txn_version, descriptor 原始 64bit) 去重，重复执行幂等。
//
// 无围栏 record 的兼容策略（t9 固定，升级不丢直接写数据）：
//  - 单条 kPut/kDelete：直接写路径（LsmEngine::Put/Delete，无事务）的 record，
//    天然原子 → 视为已提交并应用；
//  - 多条 op：未提交事务的写集（或 t9 之前旧版的多写 record）→ 整条跳过，
//    保证"未提交事务不落表"。
//  事务记录的写集与围栏是同一条 record（见上），因此单条事务写也一定是 2 条 op
//  （1 条写 + 围栏），不会误判成直接写。
//
// =============================================================================
// WAL checkpoint / 截断（t10）
// =============================================================================
// 水位语义：WalCheckpoint::watermark_file_number = W 表示
//   "所有段号 < W 的 .wal 记录都已包含在已 fsync 的 SST 里"，
// 因此重放只读段号 >= W 的段；更老的段可以被安全删除。
//
// 顺序（崩溃安全，任何情况下不得先删 WAL 再 fsync SST）：
//   1) flush 写出 SST 并逐个 fsync（文件数据）；
//   2) 持久化 checkpoint 文件（写 + fsync；文件名以 MANIFEST 开头，
//      PosixEnv 的 Sync 会顺带 fsync 父目录，使 SST 目录项也落盘）；
//   3) 才删除段号 < W 的 WAL 段（删除失败只影响空间，不影响正确性）。
// 反过来：checkpoint 缺失/损坏 → 水位视为 0 → 重放全部段（安全，只是更慢）。
// 段号单调：checkpoint 里同时记录 file_number_floor，WalWriter 用它避免
// 目录被清空后文件号回绕（否则新记录会被旧水位当成已覆盖而跳过）。
// =============================================================================

#ifndef CEDAR_STORAGE_WAL_REPLAY_H_
#define CEDAR_STORAGE_WAL_REPLAY_H_

#include <cstdint>
#include <string>
#include <vector>

#include "cedar/core/env.h"
#include "cedar/core/status.h"
#include "cedar/transaction/wal.h"

namespace cedar {

class VSLMemTable;

// 重放统计（供诊断与测试断言）
struct WalReplayStats {
  uint64_t files = 0;                  // 打开的 .wal 文件数
  uint64_t records = 0;                // 读到的完整 record 数
  uint64_t committed_records = 0;      // 带提交围栏的事务 record 数
  uint64_t direct_records = 0;         // 无围栏但按"已提交直接写"应用的 record 数
  uint64_t uncommitted_records = 0;    // 无围栏且被跳过的 record 数
  uint64_t ops_applied = 0;            // 写入 memtable 的 op 数
  uint64_t duplicate_ops_skipped = 0;  // 去重跳过的 op 数
  uint64_t corrupted_records = 0;      // 截断/CRC 错导致的截断点数量
  uint64_t checkpoint_watermark = 0;   // 本次重放使用的水位（段号 < 它被跳过）
  uint64_t skipped_files = 0;          // 因低于水位而跳过的段数
};

// WAL checkpoint（t10）：水位 + 段号下限。持久化在 db_path/MANIFEST-wal-checkpoint。
struct WalCheckpoint {
  uint64_t watermark_file_number = 0;  // 段号 < 它的记录都已在 fsync 过的 SST 中
  uint64_t file_number_floor = 0;      // 下一个 WAL 段号必须 >= 该值（单调）
  uint64_t flushed_records = 0;        // 诊断：截至水位大约覆盖的记录数
};

class WalCheckpointFile {
 public:
  // db_path/MANIFEST-wal-checkpoint（以 MANIFEST 开头 → Env::Sync 会 fsync 目录）
  static std::string DefaultPath(const std::string& db_path);

  // 读取 checkpoint：文件不存在（NotFound）或内容损坏/版本不符时返回错误，
  // 调用方应把 checkpoint 视为"不存在"（水位 0）——重放全部段是安全方向。
  static Status Load(const std::string& path, Env* env, WalCheckpoint* out);

  // 原子持久化：写 + Sync（文件名以 MANIFEST 开头，顺带 fsync 父目录）。
  // 返回 OK 才能删除 WAL 段。
  static Status Save(const std::string& path, Env* env, const WalCheckpoint& checkpoint);
};

class WalReplay {
 public:
  // 提交围栏的保留字段（key 侧）
  static constexpr uint8_t kFenceEntityType = 0xFF;  // 非法 EntityType，工厂方法不会产生
  static constexpr uint16_t kFenceReserved = 0xF00D;
  // descriptor 侧：kind=Metadata + column=哨兵 + payload=magic（payload 必须原样比较，
  // 不能与事务号异或，否则低位校验会失效）
  static constexpr uint16_t kFenceColumn = 0x0CE;
  static constexpr uint32_t kFenceMagic = 0x0CED2025;

  // 在写集末尾追加提交围栏，使"写集 + commit"落在同一条 WAL record 中。
  // txn_id 的低 8 位写入 descriptor.length，仅用于诊断，不参与围栏识别。
  static void AppendCommitFence(WalBatch* batch, uint64_t txn_id, Timestamp commit_ts);

  // 判断一条 op 是否是提交围栏。
  static bool IsCommitFence(const WalWriteOp& op);

  // 按 WAL 顺序重放 wal_dir/*.wal 到 memtable。
  // - wal_dir 不存在（未开启 WAL / 全新库）：返回 OK
  // - watermark_file_number > 0：跳过段号 < watermark 的段（checkpoint 已覆盖）
  // - record 截断/CRC 错：停止该文件，返回 OK（崩溃尾巴是正常现象）
  // - memtable 写入失败：返回错误
  static Status Replay(const std::string& wal_dir, Env* env, VSLMemTable* memtable,
                       WalReplayStats* stats = nullptr,
                       uint64_t watermark_file_number = 0);

  // 列出 wal_dir 下的 .wal 文件（按数字文件号升序）。
  static Status ListWalFiles(const std::string& wal_dir, Env* env,
                             std::vector<std::string>* files);
};

}  // namespace cedar

#endif  // CEDAR_STORAGE_WAL_REPLAY_H_
