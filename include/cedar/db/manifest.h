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

#ifndef FERN_MANIFEST_H_
#define FERN_MANIFEST_H_

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <atomic>
#include <mutex>
#include <chrono>

#include "cedar/core/status.h"
#include "cedar/core/slice.h"
#include "cedar/core/env.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

namespace cedar {

// Manifest 编辑类型
enum class ManifestEditType : uint8_t {
  kAddFile = 1,           // 添加 SST 文件
  kDeleteFile = 2,        // 删除 SST 文件
  kColumnFamilyAdd = 3,   // 添加列族
  kColumnFamilyDrop = 4,  // 删除列族
  kNextFileNumber = 5,    // 更新下一个文件号
  kLastSequence = 6,      // 更新最后序列号
  kCompactionPointer = 7, // 记录压缩位置
  kLogNumber = 8,         // 更新日志号
};

// SST 文件元数据
struct FileMetaData {
  uint64_t file_number = 0;           // 文件号
  int level = 0;                      // LSM 层级
  uint64_t file_size = 0;             // 文件大小
  
  // Key 范围
  uint64_t smallest_entity_id = 0;
  uint64_t largest_entity_id = 0;
  uint64_t smallest_timestamp = 0;
  uint64_t largest_timestamp = 0;
  
  // 统计信息
  uint64_t num_entries = 0;
  uint64_t num_deletions = 0;
  
  // 校验和
  uint32_t crc32_checksum = 0;
  
  // 序列号范围
  uint64_t smallest_sequence = 0;
  uint64_t largest_sequence = 0;
  
  // 检查是否与范围重叠
  bool Overlaps(uint64_t entity_id_start, uint64_t entity_id_end) const;
  
  // 编码/解码
  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);
};

// Manifest 编辑记录
struct ManifestEdit {
  ManifestEditType type;
  
  // 用于 kAddFile / kDeleteFile
  int level = 0;
  FileMetaData file_meta;
  uint32_t column_family_id = 0;
  
  // 用于 kColumnFamilyAdd / kColumnFamilyDrop
  std::string column_family_name;
  
  // 用于 kNextFileNumber / kLastSequence / kLogNumber
  uint64_t number = 0;
  
  // 用于 kCompactionPointer
  uint64_t entity_id = 0;
  
  ManifestEdit() = default;
  
  // 工厂方法
  static ManifestEdit AddFile(int level, const FileMetaData& meta, uint32_t cf_id = 0);
  static ManifestEdit DeleteFile(int level, uint64_t file_number, uint32_t cf_id = 0);
  static ManifestEdit AddColumnFamily(const std::string& name);
  static ManifestEdit DropColumnFamily(const std::string& name);
  static ManifestEdit NextFileNumber(uint64_t num);
  static ManifestEdit LastSequence(uint64_t seq);
  static ManifestEdit LogNumber(uint64_t num);
  
  // 编码/解码
  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(Slice* input);
};

// 版本（数据库某一时刻的快照）
class Version {
 public:
  Version(uint64_t version_id, uint64_t sequence_number);
  ~Version();
  
  uint64_t GetVersionId() const { return version_id_; }
  uint64_t GetSequenceNumber() const { return sequence_number_; }
  uint64_t GetTimestamp() const { return timestamp_; }
  
  // 获取某层的所有文件
  const std::vector<FileMetaData>& GetFiles(int level) const;
  
  // 获取文件总数
  size_t GetFileCount() const;
  
  // 查找包含指定 key 的文件
  std::vector<FileMetaData> FindFiles(uint64_t entity_id, 
                                       EntityType type,
                                       uint16_t column_id) const;
  
  // 检查是否有重叠文件
  bool HasOverlappingFiles(int level, 
                           uint64_t entity_id_start,
                           uint64_t entity_id_end) const;
  
  // 应用编辑
  void ApplyEdit(const ManifestEdit& edit);
  
  // 复制版本
  std::shared_ptr<Version> Copy(uint64_t new_version_id) const;
  
 private:
  uint64_t version_id_;
  uint64_t sequence_number_;
  uint64_t timestamp_;
  
  // files_[level][file_index]
  std::vector<std::vector<FileMetaData>> files_;
  
  // 列族信息
  std::map<uint32_t, std::string> column_families_;
  
  mutable std::mutex mutex_;
};

// Manifest 管理器
class ManifestManager {
 public:
  ManifestManager(const std::string& db_path, Env* env);
  ~ManifestManager();
  
  // 初始化（打开或创建 Manifest）
  Status Initialize(bool create_if_missing);
  
  // 关闭
  Status Close();
  
  // 加载当前版本
  Status LoadCurrentVersion(std::shared_ptr<Version>* version,
                            uint64_t* next_file_number,
                            uint64_t* last_sequence);
  
  // 原子应用编辑（写入 Manifest 并切换版本）
  Status ApplyEdit(const ManifestEdit& edit,
                   std::shared_ptr<Version>* new_version);
  
  // 批量应用编辑
  Status ApplyEdits(const std::vector<ManifestEdit>& edits,
                    std::shared_ptr<Version>* new_version);
  
  // 获取当前 Manifest 文件号
  uint64_t GetManifestFileNumber() const;
  
  // 获取 Manifest 大小
  uint64_t GetManifestSize() const;
  
  // 清理旧的 Manifest 文件
  Status CleanupOldManifests(size_t keep_count);
  
  // 强制同步 Manifest 到磁盘
  Status Sync();
  
  // 归档旧的 Manifest（压缩并移动到归档目录）
  Status ArchiveOldManifests(size_t keep_count);
  
  // 压缩 Manifest 文件
  Status CompactManifest();
  
 private:
  std::string db_path_;
  Env* env_;
  
  // 当前 Manifest 文件
  std::string manifest_filename_;
  WritableFile* manifest_file_ = nullptr;
  uint64_t manifest_file_number_ = 0;
  
  // 版本号
  std::atomic<uint64_t> next_version_id_{1};
  
  // 保护 Manifest 写入的锁
  std::mutex manifest_mutex_;
  
  // 内部方法
  Status CreateNewManifestFile();
  Status ReadCurrentFile(std::string* manifest_filename);
  Status WriteCurrentFile(const std::string& manifest_filename);
  Status WriteEditToManifest(const ManifestEdit& edit);
  Status RecoverManifest(const std::string& manifest_file,
                         std::shared_ptr<Version>* version);
};

// VersionSet - 管理所有版本
class VersionSet {
 public:
  VersionSet();
  ~VersionSet();
  
  // 获取当前版本
  std::shared_ptr<Version> GetCurrentVersion() const;
  
  // 获取指定版本
  std::shared_ptr<Version> GetVersion(uint64_t version_id) const;
  
  // 应用编辑并创建新版本
  Status ApplyEdit(const ManifestEdit& edit,
                   std::shared_ptr<Version>* new_version);
  
  // 批量应用编辑
  Status ApplyEdits(const std::vector<ManifestEdit>& edits,
                    std::shared_ptr<Version>* new_version);
  
  // 获取下一个文件号
  uint64_t GetNextFileNumber();
  
  // 获取最后序列号
  uint64_t GetLastSequence() const;
  void SetLastSequence(uint64_t seq);
  
  // 获取当前日志号
  uint64_t GetLogNumber() const;
  void SetLogNumber(uint64_t num);
  
  // 获取统计信息
  std::string GetStats() const;
  
 private:
  mutable std::mutex mutex_;
  
  std::shared_ptr<Version> current_version_;
  std::map<uint64_t, std::weak_ptr<Version>> versions_;
  
  std::atomic<uint64_t> next_file_number_{1};
  std::atomic<uint64_t> last_sequence_{0};
  std::atomic<uint64_t> log_number_{0};
  std::atomic<uint64_t> next_version_id_{1};
};

}  // namespace cedar

#endif  // FERN_MANIFEST_H_
