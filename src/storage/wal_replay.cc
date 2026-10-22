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

#include "cedar/storage/wal_replay.h"

#include <algorithm>
#include <cstdlib>
#include <unordered_set>

#include "cedar/core/crc32c.h"
#include "cedar/storage/vsl_memtable.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

namespace cedar {

namespace {

// 围栏 key：默认构造的 CedarKey（entity=0, target=0, column=0, seq=0, flags=0,
// 时间戳编码为 0 即 Timestamp::Max()），再把 entity_type 设为非法值、reserved 设为
// 哨兵值。CedarKey 的 Vertex/EdgeOut/EdgeIn 工厂只会产生 0/1/2，因此正常写入不可能
// 与之碰撞；再叠加 Metadata descriptor + magic 双重校验。
CedarKey MakeFenceKey() {
  CedarKey key(0, static_cast<EntityType>(WalReplay::kFenceEntityType), 0,
               Timestamp::Max(), 0, 0, 0);
  key.SetReserved(WalReplay::kFenceReserved);
  return key;
}

Descriptor MakeFenceDescriptor(uint64_t txn_id) {
  // payload 原样写 magic（识别时精确比较）；txn_id 低 8 位放 length 便于诊断，
  // 不参与围栏识别。
  const uint8_t txn_low8 = static_cast<uint8_t>(txn_id & 0xFF);
  Descriptor d(EntryKind::Metadata, WalReplay::kFenceColumn, WalReplay::kFenceMagic,
               txn_low8);
  return d;
}

// std::string 作为去重键：32B key + 8B txn_version + 8B descriptor
void AppendDedupeKey(std::string* out, const WalWriteOp& op) {
  out->clear();
  out->reserve(CedarKey::kKeySize + 16);
  out->append(op.key.Encode());
  char buf[16];
  const uint64_t txn = op.txn_version.value();
  for (int i = 0; i < 8; ++i) buf[i] = static_cast<char>((txn >> (8 * i)) & 0xFF);
  const uint64_t raw = op.descriptor.AsRaw();
  for (int i = 0; i < 8; ++i) buf[8 + i] = static_cast<char>((raw >> (8 * i)) & 0xFF);
  out->append(buf, 16);
}

// 从 "123.wal" 解析文件号。注意 0 是合法文件号（WalWriter 第一次 Open 会创建
// 0.wal），因此用返回值表示是否解析成功，不能用 0 当哨兵。
bool ParseWalFileNumber(const std::string& filename, uint64_t* number) {
  if (filename.size() <= 4 || filename.compare(filename.size() - 4, 4, ".wal") != 0) {
    return false;
  }
  const std::string digits = filename.substr(0, filename.size() - 4);
  if (digits.empty()) return false;
  for (char c : digits) {
    if (c < '0' || c > '9') return false;
  }
  *number = std::strtoull(digits.c_str(), nullptr, 10);
  return true;
}

}  // namespace

void WalReplay::AppendCommitFence(WalBatch* batch, uint64_t txn_id,
                                  Timestamp commit_ts) {
  if (batch == nullptr) return;
  // WalBatch::Put 只接受 kPut；围栏是"带特殊 key/descriptor 的 kPut"。
  batch->Put(MakeFenceKey(), MakeFenceDescriptor(txn_id), commit_ts);
}

bool WalReplay::IsCommitFence(const WalWriteOp& op) {
  if (op.type != WalRecordType::kPut) return false;
  const CedarKey& key = op.key;
  if (key.entity_id() != 0) return false;
  if (key.column_id() != 0) return false;
  if (key.target_id() != 0) return false;
  if (key.sequence() != 0) return false;
  if (key.flags() != 0) return false;
  if (static_cast<uint8_t>(key.entity_type()) != kFenceEntityType) return false;
  if (key.reserved() != kFenceReserved) return false;
  if (key.timestamp().value() != Timestamp::Max().value()) return false;

  const Descriptor& desc = op.descriptor;
  if (desc.GetKind() != EntryKind::Metadata) return false;
  if (desc.GetColumnId() != kFenceColumn) return false;
  if (desc.GetPayload() != kFenceMagic) return false;
  return true;
}

namespace {

// 列出 wal_dir 下的 .wal 段（文件号 + 路径），按数字段号升序。
Status ListWalEntries(const std::string& wal_dir, Env* env,
                      std::vector<std::pair<uint64_t, std::string>>* out) {
  out->clear();
  if (env == nullptr) env = Env::Default();

  if (!env->FileExists(wal_dir)) {
    return Status::NotFound("WalReplay", "wal directory does not exist");
  }

  std::vector<std::string> children;
  Status s = env->GetChildren(wal_dir, &children);
  if (!s.ok()) {
    return Status::IOError("WalReplay", s.ToString());
  }

  out->reserve(children.size());
  for (const auto& child : children) {
    uint64_t number = 0;
    if (!ParseWalFileNumber(child, &number)) continue;  // 非 N.wal
    out->emplace_back(number, wal_dir + "/" + child);
  }
  // 按数字文件号排序，避免 "10.wal" 排在 "2.wal" 前面
  std::sort(out->begin(), out->end(),
            [](const auto& a, const auto& b) { return a.first < b.first; });
  return Status::OK();
}

}  // namespace

Status WalReplay::ListWalFiles(const std::string& wal_dir, Env* env,
                               std::vector<std::string>* files) {
  if (files == nullptr) {
    return Status::InvalidArgument("WalReplay", "files is null");
  }
  std::vector<std::pair<uint64_t, std::string>> entries;
  Status s = ListWalEntries(wal_dir, env, &entries);
  if (!s.ok()) {
    return s;
  }
  files->clear();
  files->reserve(entries.size());
  for (const auto& entry : entries) {
    files->push_back(entry.second);
  }
  return Status::OK();
}

Status WalReplay::Replay(const std::string& wal_dir, Env* env, VSLMemTable* memtable,
                         WalReplayStats* stats, uint64_t watermark_file_number) {
  if (memtable == nullptr) {
    return Status::InvalidArgument("WalReplay", "memtable is null");
  }
  if (env == nullptr) env = Env::Default();

  WalReplayStats local_stats;
  WalReplayStats* out = stats != nullptr ? stats : &local_stats;
  *out = WalReplayStats();
  out->checkpoint_watermark = watermark_file_number;

  std::vector<std::pair<uint64_t, std::string>> entries;
  Status s = ListWalEntries(wal_dir, env, &entries);
  if (!s.ok()) {
    if (s.IsNotFound()) return Status::OK();  // 未开启 WAL / 全新库
    return s;
  }

  std::unordered_set<std::string> seen_ops;
  std::string dedupe_key;
  dedupe_key.reserve(CedarKey::kKeySize + 16);

  for (const auto& entry : entries) {
    // WAL checkpoint 水位：段号 < 水位 的记录一定已在 fsync 过的 SST 里，
    // 不必也不应重放（t10）。
    if (entry.first < watermark_file_number) {
      ++out->skipped_files;
      continue;
    }
    const std::string& file = entry.second;
    // 空 WAL 文件是正常状态（WalWriter 每次 Open 都会先建一个空文件）：
    // 直接跳过，否则 PosixEnv mmap(size=0) 会让 WalReader::Open 失败。
    uint64_t file_size = 0;
    if (env->GetFileSize(file, &file_size).ok() && file_size == 0) {
      continue;
    }

    WalReader reader(file, env);
    Status open_status = reader.Open();
    if (!open_status.ok()) {
      // 文件无法打开（权限/损坏）：跳过并记录，不影响其余 WAL
      ++out->corrupted_records;
      continue;
    }
    ++out->files;

    WalBatch batch;
    uint64_t sequence = 0;
    while (true) {
      Status read_status = reader.ReadNextRecord(&batch, &sequence);
      if (read_status.IsNotFound()) {
        break;  // 正常读到文件末尾
      }
      if (!read_status.ok()) {
        // 截断的 record / CRC 错：崩溃尾巴，保留已重放部分后停止本文件
        ++out->corrupted_records;
        break;
      }
      ++out->records;

      bool has_fence = false;
      for (const auto& op : batch.ops()) {
        if (IsCommitFence(op)) {
          has_fence = true;
          break;
        }
      }
      if (!has_fence) {
        // 无提交围栏：区分"直接写"与"未提交事务"（t9 兼容策略，见文件头）
        const bool direct_write =
            batch.size() == 1 &&
            (batch.ops()[0].type == WalRecordType::kPut ||
             batch.ops()[0].type == WalRecordType::kDelete);
        if (!direct_write) {
          // 未提交事务的写集（或旧版多写 record）：整条丢弃
          ++out->uncommitted_records;
          continue;
        }
        ++out->direct_records;
      } else {
        ++out->committed_records;
      }

      for (const auto& op : batch.ops()) {
        if (IsCommitFence(op)) continue;
        AppendDedupeKey(&dedupe_key, op);
        if (!seen_ops.insert(dedupe_key).second) {
          ++out->duplicate_ops_skipped;
          continue;
        }
        Status put_status = memtable->Put(op.key, op.descriptor, op.txn_version);
        if (!put_status.ok()) {
          return put_status;
        }
        ++out->ops_applied;
      }
    }
  }

  return Status::OK();
}


// =============================================================================
// WalCheckpointFile（t10）
// =============================================================================
namespace {

constexpr uint32_t kCheckpointMagic = 0x57414C43;  // 'WALC'
constexpr uint32_t kCheckpointVersion = 1;
constexpr size_t kCheckpointEncodedSize = 36;  // magic+ver+w+floor+records+crc

void PutFixed32(std::string* dst, uint32_t v) {
  char b[4];
  b[0] = static_cast<char>(v & 0xFF);
  b[1] = static_cast<char>((v >> 8) & 0xFF);
  b[2] = static_cast<char>((v >> 16) & 0xFF);
  b[3] = static_cast<char>((v >> 24) & 0xFF);
  dst->append(b, 4);
}

void PutFixed64(std::string* dst, uint64_t v) {
  char b[8];
  for (int i = 0; i < 8; ++i) {
    b[i] = static_cast<char>((v >> (8 * i)) & 0xFF);
  }
  dst->append(b, 8);
}

uint32_t GetFixed32(const char* p) {
  return static_cast<uint32_t>(static_cast<unsigned char>(p[0])) |
         (static_cast<uint32_t>(static_cast<unsigned char>(p[1])) << 8) |
         (static_cast<uint32_t>(static_cast<unsigned char>(p[2])) << 16) |
         (static_cast<uint32_t>(static_cast<unsigned char>(p[3])) << 24);
}

uint64_t GetFixed64(const char* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) {
    v |= (static_cast<uint64_t>(static_cast<unsigned char>(p[i])) << (8 * i));
  }
  return v;
}

}  // namespace

std::string WalCheckpointFile::DefaultPath(const std::string& db_path) {
  // 文件名以 MANIFEST 开头是有意为之：PosixEnv 的 WritableFile::Sync 对
  // MANIFEST* 会额外 fsync 父目录，这样 checkpoint 与同批 SST 的目录项一起落盘。
  return db_path + "/MANIFEST-wal-checkpoint";
}

Status WalCheckpointFile::Load(const std::string& path, Env* env,
                               WalCheckpoint* out) {
  if (out == nullptr) {
    return Status::InvalidArgument("WalCheckpointFile", "out is null");
  }
  if (env == nullptr) env = Env::Default();

  if (!env->FileExists(path)) {
    return Status::NotFound("WalCheckpointFile", "checkpoint does not exist");
  }
  uint64_t file_size = 0;
  Status s = env->GetFileSize(path, &file_size);
  if (!s.ok()) {
    return Status::IOError("WalCheckpointFile", s.ToString());
  }
  if (file_size != kCheckpointEncodedSize) {
    return Status::Corruption("WalCheckpointFile", "unexpected size");
  }

  RandomAccessFile* file = nullptr;
  s = env->NewRandomAccessFile(path, &file);
  if (!s.ok()) {
    return Status::IOError("WalCheckpointFile", s.ToString());
  }
  std::unique_ptr<char[]> buf(new char[kCheckpointEncodedSize]);
  Slice data;
  s = file->Read(0, kCheckpointEncodedSize, &data, buf.get());
  if (!s.ok()) {
    delete file;
    return Status::IOError("WalCheckpointFile", s.ToString());
  }
  if (data.size() != kCheckpointEncodedSize) {
    delete file;
    return Status::Corruption("WalCheckpointFile", "truncated");
  }

  // 注意：RandomAccessFile 的 mmap 实现返回的 Slice 指向映射区，
  // 必须先解析完再 delete file（否则是 use-after-munmap）。
  const char* p = data.data();
  const uint32_t magic = GetFixed32(p);
  const uint32_t version = GetFixed32(p + 4);
  if (magic != kCheckpointMagic) {
    return Status::Corruption("WalCheckpointFile", "bad magic");
  }
  if (version != kCheckpointVersion) {
    return Status::Corruption("WalCheckpointFile", "unsupported version");
  }
  const uint32_t stored_crc = GetFixed32(p + 32);
  const uint32_t actual_crc = cedar::crc32c::Value(p, 32);
  if (stored_crc != actual_crc) {
    return Status::Corruption("WalCheckpointFile", "crc mismatch");
  }

  out->watermark_file_number = GetFixed64(p + 8);
  out->file_number_floor = GetFixed64(p + 16);
  out->flushed_records = GetFixed64(p + 24);
  delete file;
  return Status::OK();
}

Status WalCheckpointFile::Save(const std::string& path, Env* env,
                               const WalCheckpoint& checkpoint) {
  if (env == nullptr) env = Env::Default();

  std::string data;
  data.reserve(kCheckpointEncodedSize);
  PutFixed32(&data, kCheckpointMagic);
  PutFixed32(&data, kCheckpointVersion);
  PutFixed64(&data, checkpoint.watermark_file_number);
  PutFixed64(&data, checkpoint.file_number_floor);
  PutFixed64(&data, checkpoint.flushed_records);
  PutFixed32(&data, cedar::crc32c::Value(data.data(), data.size()));

  WritableFile* file = nullptr;
  Status s = env->NewWritableFile(path, &file);
  if (!s.ok()) {
    return Status::IOError("WalCheckpointFile", s.ToString());
  }
  Status result = file->Append(Slice(data));
  if (result.ok()) {
    // Sync 必须成功：返回 OK 之后调用方才会删 WAL 段
    result = file->Sync();
  }
  Status close_status = file->Close();
  delete file;
  if (!result.ok()) {
    return Status::IOError("WalCheckpointFile", result.ToString());
  }
  if (!close_status.ok()) {
    return Status::IOError("WalCheckpointFile", close_status.ToString());
  }
  return Status::OK();
}

}  // namespace cedar
