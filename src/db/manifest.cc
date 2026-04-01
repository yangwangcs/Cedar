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

#include "cedar/db/manifest.h"

#include <filesystem>
#include <fstream>

namespace cedar {

// Manifest 文件魔数和版本
static const uint32_t kManifestMagic = 0x464D4E00;  // "FMN\0"
static const uint32_t kManifestVersion = 1;

// 编码/解码辅助函数
static inline void EncodeFixed32(char* buf, uint32_t value) {
  buf[0] = static_cast<char>(value & 0xFF);
  buf[1] = static_cast<char>((value >> 8) & 0xFF);
  buf[2] = static_cast<char>((value >> 16) & 0xFF);
  buf[3] = static_cast<char>((value >> 24) & 0xFF);
}

static inline void EncodeFixed64(char* buf, uint64_t value) {
  buf[0] = static_cast<char>(value & 0xFF);
  buf[1] = static_cast<char>((value >> 8) & 0xFF);
  buf[2] = static_cast<char>((value >> 16) & 0xFF);
  buf[3] = static_cast<char>((value >> 24) & 0xFF);
  buf[4] = static_cast<char>((value >> 32) & 0xFF);
  buf[5] = static_cast<char>((value >> 40) & 0xFF);
  buf[6] = static_cast<char>((value >> 48) & 0xFF);
  buf[7] = static_cast<char>((value >> 56) & 0xFF);
}

static inline uint32_t DecodeFixed32(const char* buf) {
  return static_cast<uint32_t>(static_cast<unsigned char>(buf[0])) |
         (static_cast<uint32_t>(static_cast<unsigned char>(buf[1])) << 8) |
         (static_cast<uint32_t>(static_cast<unsigned char>(buf[2])) << 16) |
         (static_cast<uint32_t>(static_cast<unsigned char>(buf[3])) << 24);
}

static inline uint64_t DecodeFixed64(const char* buf) {
  return static_cast<uint64_t>(static_cast<unsigned char>(buf[0])) |
         (static_cast<uint64_t>(static_cast<unsigned char>(buf[1])) << 8) |
         (static_cast<uint64_t>(static_cast<unsigned char>(buf[2])) << 16) |
         (static_cast<uint64_t>(static_cast<unsigned char>(buf[3])) << 24) |
         (static_cast<uint64_t>(static_cast<unsigned char>(buf[4])) << 32) |
         (static_cast<uint64_t>(static_cast<unsigned char>(buf[5])) << 40) |
         (static_cast<uint64_t>(static_cast<unsigned char>(buf[6])) << 48) |
         (static_cast<uint64_t>(static_cast<unsigned char>(buf[7])) << 56);
}

// ==================== FileMetaData ====================

bool FileMetaData::Overlaps(uint64_t entity_id_start, 
                            uint64_t entity_id_end) const {
  return !(largest_entity_id < entity_id_start || 
           smallest_entity_id > entity_id_end);
}

void FileMetaData::EncodeTo(std::string* dst) const {
  char buf[8];
  
  EncodeFixed64(buf, file_number);
  dst->append(buf, 8);
  
  EncodeFixed32(buf, static_cast<uint32_t>(level));
  dst->append(buf, 4);
  
  EncodeFixed64(buf, file_size);
  dst->append(buf, 8);
  
  EncodeFixed64(buf, smallest_entity_id);
  dst->append(buf, 8);
  
  EncodeFixed64(buf, largest_entity_id);
  dst->append(buf, 8);
  
  EncodeFixed64(buf, smallest_timestamp);
  dst->append(buf, 8);
  
  EncodeFixed64(buf, largest_timestamp);
  dst->append(buf, 8);
  
  EncodeFixed64(buf, num_entries);
  dst->append(buf, 8);
  
  EncodeFixed64(buf, num_deletions);
  dst->append(buf, 8);
  
  EncodeFixed32(buf, crc32_checksum);
  dst->append(buf, 4);
}

Status FileMetaData::DecodeFrom(Slice* input) {
  if (input->size() < 68) {
    return Status::Corruption("FileMetaData", "truncated");
  }
  
  const char* p = input->data();
  file_number = DecodeFixed64(p);
  p += 8;
  
  level = static_cast<int>(DecodeFixed32(p));
  p += 4;
  
  file_size = DecodeFixed64(p);
  p += 8;
  
  smallest_entity_id = DecodeFixed64(p);
  p += 8;
  
  largest_entity_id = DecodeFixed64(p);
  p += 8;
  
  smallest_timestamp = DecodeFixed64(p);
  p += 8;
  
  largest_timestamp = DecodeFixed64(p);
  p += 8;
  
  num_entries = DecodeFixed64(p);
  p += 8;
  
  num_deletions = DecodeFixed64(p);
  p += 8;
  
  crc32_checksum = DecodeFixed32(p);
  p += 4;
  
  input->remove_prefix(68);
  return Status::OK();
}

// ==================== ManifestEdit ====================

ManifestEdit ManifestEdit::AddFile(int level, const FileMetaData& meta, 
                                   uint32_t cf_id) {
  ManifestEdit edit;
  edit.type = ManifestEditType::kAddFile;
  edit.level = level;
  edit.file_meta = meta;
  edit.column_family_id = cf_id;
  return edit;
}

ManifestEdit ManifestEdit::DeleteFile(int level, uint64_t file_number,
                                      uint32_t cf_id) {
  ManifestEdit edit;
  edit.type = ManifestEditType::kDeleteFile;
  edit.level = level;
  edit.file_meta.file_number = file_number;
  edit.column_family_id = cf_id;
  return edit;
}

ManifestEdit ManifestEdit::AddColumnFamily(const std::string& name) {
  ManifestEdit edit;
  edit.type = ManifestEditType::kColumnFamilyAdd;
  edit.column_family_name = name;
  return edit;
}

ManifestEdit ManifestEdit::DropColumnFamily(const std::string& name) {
  ManifestEdit edit;
  edit.type = ManifestEditType::kColumnFamilyDrop;
  edit.column_family_name = name;
  return edit;
}

ManifestEdit ManifestEdit::NextFileNumber(uint64_t num) {
  ManifestEdit edit;
  edit.type = ManifestEditType::kNextFileNumber;
  edit.number = num;
  return edit;
}

ManifestEdit ManifestEdit::LastSequence(uint64_t seq) {
  ManifestEdit edit;
  edit.type = ManifestEditType::kLastSequence;
  edit.number = seq;
  return edit;
}

ManifestEdit ManifestEdit::LogNumber(uint64_t num) {
  ManifestEdit edit;
  edit.type = ManifestEditType::kLogNumber;
  edit.number = num;
  return edit;
}

void ManifestEdit::EncodeTo(std::string* dst) const {
  // 类型
  dst->push_back(static_cast<char>(type));
  
  char buf[8];
  
  switch (type) {
    case ManifestEditType::kAddFile:
      EncodeFixed32(buf, column_family_id);
      dst->append(buf, 4);
      file_meta.EncodeTo(dst);
      break;
      
    case ManifestEditType::kDeleteFile:
      EncodeFixed32(buf, column_family_id);
      dst->append(buf, 4);
      EncodeFixed64(buf, file_meta.file_number);
      dst->append(buf, 8);
      break;
      
    case ManifestEditType::kColumnFamilyAdd:
    case ManifestEditType::kColumnFamilyDrop:
      EncodeFixed32(buf, static_cast<uint32_t>(column_family_name.size()));
      dst->append(buf, 4);
      dst->append(column_family_name);
      break;
      
    case ManifestEditType::kNextFileNumber:
    case ManifestEditType::kLastSequence:
    case ManifestEditType::kLogNumber:
      EncodeFixed64(buf, number);
      dst->append(buf, 8);
      break;
      
    default:
      break;
  }
}

Status ManifestEdit::DecodeFrom(Slice* input) {
  if (input->size() < 1) {
    return Status::Corruption("ManifestEdit", "truncated type");
  }
  
  type = static_cast<ManifestEditType>(input->data()[0]);
  input->remove_prefix(1);
  
  char buf[8];
  
  switch (type) {
    case ManifestEditType::kAddFile:
      if (input->size() < 4) return Status::Corruption("ManifestEdit", "truncated cf_id");
      column_family_id = DecodeFixed32(input->data());
      input->remove_prefix(4);
      return file_meta.DecodeFrom(input);
      
    case ManifestEditType::kDeleteFile:
      if (input->size() < 4) return Status::Corruption("ManifestEdit", "truncated cf_id");
      column_family_id = DecodeFixed32(input->data());
      input->remove_prefix(4);
      if (input->size() < 8) return Status::Corruption("ManifestEdit", "truncated file_number");
      file_meta.file_number = DecodeFixed64(input->data());
      input->remove_prefix(8);
      return Status::OK();
      
    case ManifestEditType::kColumnFamilyAdd:
    case ManifestEditType::kColumnFamilyDrop:
      if (input->size() < 4) return Status::Corruption("ManifestEdit", "truncated name length");
      {
        uint32_t len = DecodeFixed32(input->data());
        input->remove_prefix(4);
        if (input->size() < len) return Status::Corruption("ManifestEdit", "truncated name");
        column_family_name.assign(input->data(), len);
        input->remove_prefix(len);
      }
      return Status::OK();
      
    case ManifestEditType::kNextFileNumber:
    case ManifestEditType::kLastSequence:
    case ManifestEditType::kLogNumber:
      if (input->size() < 8) return Status::Corruption("ManifestEdit", "truncated number");
      number = DecodeFixed64(input->data());
      input->remove_prefix(8);
      return Status::OK();
      
    default:
      return Status::Corruption("ManifestEdit", "unknown type");
  }
}

// ==================== Version ====================

Version::Version(uint64_t version_id, uint64_t sequence_number)
    : version_id_(version_id),
      sequence_number_(sequence_number),
      timestamp_(std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::system_clock::now().time_since_epoch()).count()) {}

Version::~Version() = default;

const std::vector<FileMetaData>& Version::GetFiles(int level) const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (level < 0 || level >= static_cast<int>(files_.size())) {
    static const std::vector<FileMetaData> empty;
    return empty;
  }
  return files_[level];
}

size_t Version::GetFileCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  size_t count = 0;
  for (const auto& level_files : files_) {
    count += level_files.size();
  }
  return count;
}

std::vector<FileMetaData> Version::FindFiles(uint64_t entity_id, 
                                              EntityType type,
                                              uint16_t column_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  std::vector<FileMetaData> result;
  
  for (const auto& level_files : files_) {
    for (const auto& file : level_files) {
      if (entity_id >= file.smallest_entity_id && 
          entity_id <= file.largest_entity_id) {
        result.push_back(file);
      }
    }
  }
  
  return result;
}

bool Version::HasOverlappingFiles(int level, 
                                   uint64_t entity_id_start,
                                   uint64_t entity_id_end) const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  if (level < 0 || level >= static_cast<int>(files_.size())) {
    return false;
  }
  
  for (const auto& file : files_[level]) {
    if (file.Overlaps(entity_id_start, entity_id_end)) {
      return true;
    }
  }
  
  return false;
}

void Version::ApplyEdit(const ManifestEdit& edit) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  switch (edit.type) {
    case ManifestEditType::kAddFile:
      if (edit.level >= static_cast<int>(files_.size())) {
        files_.resize(edit.level + 1);
      }
      files_[edit.level].push_back(edit.file_meta);
      break;
      
    case ManifestEditType::kDeleteFile:
      if (edit.level < static_cast<int>(files_.size())) {
        auto& level_files = files_[edit.level];
        level_files.erase(
          std::remove_if(level_files.begin(), level_files.end(),
            [&edit](const FileMetaData& f) {
              return f.file_number == edit.file_meta.file_number;
            }),
          level_files.end());
      }
      break;
      
    case ManifestEditType::kColumnFamilyAdd:
      column_families_[edit.column_family_id] = edit.column_family_name;
      break;
      
    case ManifestEditType::kColumnFamilyDrop:
      column_families_.erase(edit.column_family_id);
      break;
      
    default:
      break;
  }
}

std::shared_ptr<Version> Version::Copy(uint64_t new_version_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  
  auto new_version = std::make_shared<Version>(new_version_id, sequence_number_);
  new_version->files_ = files_;
  new_version->column_families_ = column_families_;
  
  return new_version;
}

// ==================== ManifestManager ====================

ManifestManager::ManifestManager(const std::string& db_path, Env* env)
    : db_path_(db_path), env_(env) {}

ManifestManager::~ManifestManager() {
  if (manifest_file_) {
    Close();
  }
}

Status ManifestManager::Initialize(bool create_if_missing) {
  std::string current_file = db_path_ + "/CURRENT";
  
  if (std::filesystem::exists(current_file)) {
    // 读取现有 Manifest
    std::string manifest_name;
    Status s = ReadCurrentFile(&manifest_name);
    if (!s.ok()) {
      return s;
    }
    
    manifest_filename_ = db_path_ + "/" + manifest_name;
    // TODO: 打开并读取 Manifest 文件
    
  } else if (create_if_missing) {
    // 创建新的 Manifest
    Status s = CreateNewManifestFile();
    if (!s.ok()) {
      return s;
    }
  } else {
    return Status::NotFound("Manifest", "No CURRENT file");
  }
  
  return Status::OK();
}

Status ManifestManager::Close() {
  if (manifest_file_) {
    manifest_file_->Close();
    delete manifest_file_;
    manifest_file_ = nullptr;
  }
  return Status::OK();
}

Status ManifestManager::CreateNewManifestFile() {
  manifest_file_number_ = 1;
  manifest_filename_ = db_path_ + "/MANIFEST-" + 
                       std::to_string(manifest_file_number_);
  
  // 创建文件
  Status s = env_->NewWritableFile(manifest_filename_, &manifest_file_);
  if (!s.ok()) {
    return s;
  }
  
  // 写入头部
  std::string header;
  char buf[12];
  EncodeFixed32(buf, kManifestMagic);
  EncodeFixed32(buf + 4, kManifestVersion);
  EncodeFixed32(buf + 8, 0);  // 预留
  header.append(buf, 12);
  
  s = manifest_file_->Append(header);
  if (!s.ok()) {
    return s;
  }
  
  // 写入 CURRENT 文件
  return WriteCurrentFile("MANIFEST-" + std::to_string(manifest_file_number_));
}

Status ManifestManager::ReadCurrentFile(std::string* manifest_filename) {
  std::ifstream file(db_path_ + "/CURRENT");
  if (!file.is_open()) {
    return Status::IOError("Manifest", "Cannot open CURRENT file");
  }
  
  std::getline(file, *manifest_filename);
  file.close();
  
  if (manifest_filename->empty()) {
    return Status::Corruption("Manifest", "CURRENT file is empty");
  }
  
  return Status::OK();
}

Status ManifestManager::WriteCurrentFile(const std::string& manifest_filename) {
  std::string tmp_file = db_path_ + "/CURRENT.tmp";
  
  std::ofstream file(tmp_file);
  if (!file.is_open()) {
    return Status::IOError("Manifest", "Cannot create CURRENT.tmp");
  }
  
  file << manifest_filename << std::endl;
  file.close();
  
  // 原子重命名
  std::filesystem::rename(tmp_file, db_path_ + "/CURRENT");
  
  return Status::OK();
}

Status ManifestManager::LoadCurrentVersion(std::shared_ptr<Version>* version,
                                           uint64_t* next_file_number,
                                           uint64_t* last_sequence) {
  // TODO: 从 Manifest 文件读取版本信息
  // 暂时返回空版本
  *version = std::make_shared<Version>(1, 0);
  *next_file_number = 1;
  *last_sequence = 0;
  return Status::OK();
}

Status ManifestManager::ApplyEdit(const ManifestEdit& edit,
                                  std::shared_ptr<Version>* new_version) {
  std::lock_guard<std::mutex> lock(manifest_mutex_);
  
  // 写入 Manifest
  Status s = WriteEditToManifest(edit);
  if (!s.ok()) {
    return s;
  }
  
  // 应用编辑到版本
  // TODO: 创建新版本
  
  return Status::OK();
}

Status ManifestManager::WriteEditToManifest(const ManifestEdit& edit) {
  if (!manifest_file_) {
    return Status::IOError("Manifest", "not opened");
  }
  
  std::string record;
  edit.EncodeTo(&record);
  
  // 写入长度前缀
  char len_buf[4];
  EncodeFixed32(len_buf, static_cast<uint32_t>(record.size()));
  
  Status s = manifest_file_->Append(Slice(len_buf, 4));
  if (!s.ok()) {
    return s;
  }
  
  s = manifest_file_->Append(record);
  if (!s.ok()) {
    return s;
  }
  
  return manifest_file_->Sync();
}

uint64_t ManifestManager::GetManifestFileNumber() const {
  return manifest_file_number_;
}

uint64_t ManifestManager::GetManifestSize() const {
  if (!manifest_file_) {
    return 0;
  }
  // 尝试从文件系统获取文件大小
  try {
    return std::filesystem::file_size(manifest_filename_);
  } catch (...) {
    return 0;
  }
}

Status ManifestManager::Sync() {
  if (manifest_file_) {
    return manifest_file_->Sync();
  }
  return Status::OK();
}

// ==================== VersionSet ====================

VersionSet::VersionSet() {
  current_version_ = std::make_shared<Version>(1, 0);
}

VersionSet::~VersionSet() = default;

std::shared_ptr<Version> VersionSet::GetCurrentVersion() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return current_version_;
}

Status VersionSet::ApplyEdits(const std::vector<ManifestEdit>& edits,
                              std::shared_ptr<Version>* new_version) {
  std::lock_guard<std::mutex> lock(mutex_);
  
  // 复制当前版本
  uint64_t new_version_id = next_version_id_.fetch_add(1);
  auto version = current_version_->Copy(new_version_id);
  
  // 应用所有编辑
  for (const auto& edit : edits) {
    version->ApplyEdit(edit);
    
    // 更新元数据
    switch (edit.type) {
      case ManifestEditType::kNextFileNumber:
        next_file_number_.store(edit.number, std::memory_order_relaxed);
        break;
      case ManifestEditType::kLastSequence:
        last_sequence_.store(edit.number, std::memory_order_relaxed);
        break;
      case ManifestEditType::kLogNumber:
        log_number_.store(edit.number, std::memory_order_relaxed);
        break;
      default:
        break;
    }
  }
  
  // 更新当前版本
  current_version_ = version;
  versions_[new_version_id] = version;
  
  if (new_version) {
    *new_version = version;
  }
  
  return Status::OK();
}

uint64_t VersionSet::GetNextFileNumber() {
  return next_file_number_.fetch_add(1, std::memory_order_relaxed);
}

uint64_t VersionSet::GetLastSequence() const {
  return last_sequence_.load(std::memory_order_relaxed);
}

void VersionSet::SetLastSequence(uint64_t seq) {
  last_sequence_.store(seq, std::memory_order_relaxed);
}

uint64_t VersionSet::GetLogNumber() const {
  return log_number_.load(std::memory_order_relaxed);
}

void VersionSet::SetLogNumber(uint64_t num) {
  log_number_.store(num, std::memory_order_relaxed);
}

// ==================== Manifest 归档和压缩 ====================

Status ManifestManager::CleanupOldManifests(size_t keep_count) {
  // 获取所有 Manifest 文件
  std::vector<std::pair<uint64_t, std::string>> manifests;
  
  for (const auto& entry : std::filesystem::directory_iterator(db_path_)) {
    if (entry.is_regular_file()) {
      std::string filename = entry.path().filename().string();
      if (filename.find("MANIFEST-") == 0) {
        // 提取文件号
        size_t dash_pos = filename.find('-');
        if (dash_pos != std::string::npos) {
          std::string num_str = filename.substr(dash_pos + 1);
          try {
            uint64_t file_num = std::stoull(num_str);
            manifests.push_back({file_num, entry.path().string()});
          } catch (...) {
            // 忽略无法解析的文件名
          }
        }
      }
    }
  }
  
  // 按文件号排序
  std::sort(manifests.begin(), manifests.end());
  
  // 删除旧的文件
  if (manifests.size() > keep_count) {
    for (size_t i = 0; i < manifests.size() - keep_count; i++) {
      try {
        std::filesystem::remove(manifests[i].second);
      } catch (...) {
        // 忽略删除错误
      }
    }
  }
  
  return Status::OK();
}

Status ManifestManager::ArchiveOldManifests(size_t keep_count) {
  // 创建归档目录
  std::string archive_dir = db_path_ + "/archive";
  if (!std::filesystem::exists(archive_dir)) {
    try {
      std::filesystem::create_directories(archive_dir);
    } catch (...) {
      return Status::IOError("Manifest", "Failed to create archive directory");
    }
  }
  
  // 获取所有 Manifest 文件
  std::vector<std::pair<uint64_t, std::string>> manifests;
  
  for (const auto& entry : std::filesystem::directory_iterator(db_path_)) {
    if (entry.is_regular_file()) {
      std::string filename = entry.path().filename().string();
      if (filename.find("MANIFEST-") == 0) {
        size_t dash_pos = filename.find('-');
        if (dash_pos != std::string::npos) {
          std::string num_str = filename.substr(dash_pos + 1);
          try {
            uint64_t file_num = std::stoull(num_str);
            // 跳过当前使用的 Manifest
            if (file_num != manifest_file_number_) {
              manifests.push_back({file_num, entry.path().string()});
            }
          } catch (...) {
            // 忽略无法解析的文件名
          }
        }
      }
    }
  }
  
  // 按文件号排序
  std::sort(manifests.begin(), manifests.end());
  
  // 归档旧的文件（保留 keep_count 个）
  if (manifests.size() > keep_count) {
    for (size_t i = 0; i < manifests.size() - keep_count; i++) {
      try {
        std::string src = manifests[i].second;
        std::string dst = archive_dir + "/" + 
                         std::filesystem::path(src).filename().string() + ".gz";
        
        // TODO: 实现 gzip 压缩
        // 暂时直接移动文件
        std::filesystem::rename(src, dst);
      } catch (...) {
        // 忽略归档错误
      }
    }
  }
  
  return Status::OK();
}

Status ManifestManager::CompactManifest() {
  // Manifest 压缩策略：
  // 1. 读取当前所有版本记录
  // 2. 只保留最新的完整版本（删除中间版本）
  // 3. 写入新的 Manifest 文件
  // 4. 原子切换
  
  // 创建新的 Manifest 文件
  uint64_t new_file_number = manifest_file_number_ + 1;
  std::string new_manifest_file = db_path_ + "/MANIFEST-" + 
                                   std::to_string(new_file_number);
  
  WritableFile* new_file = nullptr;
  Status s = env_->NewWritableFile(new_manifest_file, &new_file);
  if (!s.ok()) {
    return s;
  }
  
  // 写入头部
  std::string header;
  char buf[12];
  EncodeFixed32(buf, kManifestMagic);
  EncodeFixed32(buf + 4, kManifestVersion);
  EncodeFixed32(buf + 8, 0);
  header.append(buf, 12);
  
  s = new_file->Append(header);
  if (!s.ok()) {
    delete new_file;
    return s;
  }
  
  // TODO: 写入当前的完整版本状态（而不是所有编辑记录）
  
  // 同步并关闭
  s = new_file->Sync();
  if (!s.ok()) {
    delete new_file;
    return s;
  }
  
  // 更新 CURRENT 文件
  s = WriteCurrentFile("MANIFEST-" + std::to_string(new_file_number));
  if (!s.ok()) {
    delete new_file;
    return s;
  }
  
  // 关闭旧文件
  if (manifest_file_) {
    manifest_file_->Close();
    delete manifest_file_;
  }
  
  // 切换到新文件
  manifest_file_ = new_file;
  manifest_file_number_ = new_file_number;
  manifest_filename_ = new_manifest_file;
  
  // 归档旧的 Manifest
  ArchiveOldManifests(5);
  
  return Status::OK();
}

}  // namespace cedar
