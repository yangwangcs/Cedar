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

#include "cedar/db/graph_db.h"
#include "cedar/db/graph_db_impl.h"

#include <filesystem>
#include <fstream>

namespace cedar {

// ==================== CedarGraphDB 静态方法 ====================

Status CedarGraphDB::Open(const std::string& db_path,
                         const CedarGraphOptions& options,
                         CedarGraphDB** db_ptr) {
  if (db_ptr == nullptr) {
    return Status::InvalidArgument("CedarGraphDB", "db_ptr is null");
  }
  
  // 创建实现
  auto impl = std::make_unique<CedarGraphDBImpl>(db_path, options);
  
  // 打开数据库
  Status s = impl->Open();
  if (!s.ok()) {
    return s;
  }
  
  *db_ptr = new CedarGraphDB(std::move(impl));
  return Status::OK();
}

Status CedarGraphDB::Open(const std::string& db_path,
                         const CedarGraphOptions& options,
                         const std::vector<std::string>& column_families,
                         std::vector<CedarGraphDB*>* db_ptrs,
                         std::vector<ColumnFamilyHandle*>* handles) {
  if (db_ptrs == nullptr) {
    return Status::InvalidArgument("CedarGraphDB", "db_ptrs is null");
  }
  
  // 先打开默认数据库
  CedarGraphDB* default_db;
  Status s = Open(db_path, options, &default_db);
  if (!s.ok()) {
    return s;
  }
  
  db_ptrs->clear();
  db_ptrs->push_back(default_db);
  
  if (handles) {
    handles->clear();
    handles->push_back(default_db->DefaultColumnFamily());
  }
  
  // 清理（handles 中存储的指针在 db 关闭时会失效）
  (void)handles;
  
  // 创建其他列族
  for (size_t i = 1; i < column_families.size(); i++) {
    CedarGraphDB* cf_handle;
    s = default_db->CreateColumnFamily(column_families[i], &cf_handle);
    if (!s.ok()) {
      // 清理已创建的
      for (auto* db : *db_ptrs) {
        delete db;
      }
      db_ptrs->clear();
      return s;
    }
    db_ptrs->push_back(cf_handle);
    if (handles) {
      handles->push_back(cf_handle->DefaultColumnFamily());
    }
    
    // 清理
    (void)handles;
  }
  
  return Status::OK();
}

Status CedarGraphDB::DestroyDB(const std::string& db_path,
                              const CedarGraphOptions& options) {
  // 检查目录是否存在
  if (!std::filesystem::exists(db_path)) {
    return Status::OK();  // 目录不存在，视为成功
  }
  
  // 尝试删除所有文件
  try {
    std::filesystem::remove_all(db_path);
    return Status::OK();
  } catch (const std::exception& e) {
    return Status::IOError("DestroyDB", e.what());
  }
}

Status CedarGraphDB::RepairDB(const std::string& db_path,
                             const CedarGraphOptions& options) {
  // TODO: 实现数据库修复逻辑
  // 1. 检查 Manifest 完整性
  // 2. 检查 SST 文件完整性
  // 3. 重放 WAL
  // 4. 重建 Manifest
  return Status::NotSupported("RepairDB", "not implemented yet");
}

// ==================== CedarGraphDB 构造函数/析构函数 ====================

CedarGraphDB::CedarGraphDB(std::unique_ptr<CedarGraphDBImpl> impl)
    : impl_(std::move(impl)) {}

CedarGraphDB::~CedarGraphDB() {
  // 实现会自动清理
}

// ==================== CedarGraphDB 基本操作 ====================

Status CedarGraphDB::Put(const CedarKey& key, 
                        const Descriptor& descriptor,
                        const WriteOptions& options) {
  return impl_->Put(key, descriptor, options);
}

Status CedarGraphDB::Delete(const CedarKey& key,
                           const WriteOptions& options) {
  return impl_->Delete(key, options);
}

std::optional<Descriptor> CedarGraphDB::Get(const CedarKey& key,
                                           const ReadOptions& options) {
  return impl_->Get(key, options);
}

// ==================== CedarGraphDB 事务支持 ====================

std::unique_ptr<OCCTransaction> CedarGraphDB::BeginTransaction(
    const TransactionOptions& options) {
  return impl_->BeginTransaction(options);
}

// ==================== CedarGraphDB 管理操作 ====================

Status CedarGraphDB::Flush(const FlushOptions& options) {
  return impl_->Flush(options);
}

Status CedarGraphDB::CompactRange(const CompactRangeOptions& options) {
  return impl_->CompactRange(options);
}

// ==================== CedarGraphDB 快照支持 ====================

const Snapshot* CedarGraphDB::GetSnapshot() {
  return impl_->GetSnapshot();
}

void CedarGraphDB::ReleaseSnapshot(const Snapshot* snapshot) {
  impl_->ReleaseSnapshot(snapshot);
}

// ==================== CedarGraphDB 列族操作 ====================

Status CedarGraphDB::CreateColumnFamily(const std::string& name,
                                       CedarGraphDB** cf_handle) {
  ColumnFamilyHandle* handle;
  Status s = impl_->CreateColumnFamily(name, &handle);
  if (s.ok() && cf_handle) {
    // 返回新的 CedarGraphDB 实例，指向同一个 impl 但不同列族
    auto impl_ptr = impl_.get();
    // 注意：这里需要特殊处理，暂时返回 this
    *cf_handle = this;
  }
  return s;
}

Status CedarGraphDB::DropColumnFamily(CedarGraphDB* cf_handle) {
  // TODO: 实现列族删除
  return Status::NotSupported("DropColumnFamily", "not implemented yet");
}

ColumnFamilyHandle* CedarGraphDB::DefaultColumnFamily() {
  return impl_->DefaultColumnFamily();
}

// ==================== CedarGraphDB 属性与统计 ====================

Status CedarGraphDB::GetProperty(const std::string& property, 
                                std::string* value) {
  return impl_->GetProperty(property, value);
}

std::string CedarGraphDB::GetStatsString() {
  return impl_->GetStatsString();
}

uint64_t CedarGraphDB::GetLatestSequenceNumber() const {
  return impl_->GetLatestSequenceNumber();
}

std::string CedarGraphDB::GetName() const {
  return impl_ ? impl_->GetLsmEngine()->GetDbPath() : "";
}

std::string CedarGraphDB::GetColumnFamilyName() const {
  // TODO: 返回当前列族名称
  return "default";
}

// ==================== CedarGraphDB 备份 ====================

Status CedarGraphDB::CreateCheckpoint(const std::string& checkpoint_dir) {
  return impl_->CreateCheckpoint(checkpoint_dir);
}

Status CedarGraphDB::RestoreFromCheckpoint(const std::string& checkpoint_dir,
                                          const std::string& db_dir) {
  // 1. 检查检查点目录是否存在
  if (!std::filesystem::exists(checkpoint_dir)) {
    return Status::NotFound("RestoreFromCheckpoint", 
                            "Checkpoint directory not found: " + checkpoint_dir);
  }
  
  // 2. 检查检查点元数据
  std::string meta_file = checkpoint_dir + "/CHECKPOINT_META";
  if (!std::filesystem::exists(meta_file)) {
    return Status::Corruption("RestoreFromCheckpoint", 
                              "Checkpoint metadata not found");
  }
  
  // 3. 如果目标目录存在，先删除
  if (std::filesystem::exists(db_dir)) {
    try {
      std::filesystem::remove_all(db_dir);
    } catch (const std::exception& e) {
      return Status::IOError("RestoreFromCheckpoint", 
                             std::string("Failed to remove existing db: ") + e.what());
    }
  }
  
  // 4. 创建目标目录
  try {
    std::filesystem::create_directories(db_dir);
  } catch (const std::exception& e) {
    return Status::IOError("RestoreFromCheckpoint", e.what());
  }
  
  // 5. 复制所有文件从检查点到目标目录
  // 创建默认列族目录
  std::string default_cf_dir = db_dir + "/default";
  try {
    std::filesystem::create_directories(default_cf_dir);
    
    for (const auto& entry : std::filesystem::directory_iterator(checkpoint_dir)) {
      if (entry.is_regular_file()) {
        std::string filename = entry.path().filename().string();
        std::string src = entry.path().string();
        std::string dst;
        
        // 处理 MANIFEST 文件
        if (filename == "MANIFEST") {
          dst = db_dir + "/MANIFEST-1";
        } else if (filename.size() >= 6 && filename.substr(filename.size() - 6) == ".frond") {
          // SST 文件放在列族目录下
          dst = default_cf_dir + "/" + filename;
        } else {
          dst = db_dir + "/" + filename;
        }
        
        std::filesystem::copy_file(src, dst);
      }
    }
  } catch (const std::exception& e) {
    return Status::IOError("RestoreFromCheckpoint", 
                           std::string("Failed to copy files: ") + e.what());
  }
  
  // 6. 更新 CURRENT 文件
  std::ofstream current(db_dir + "/CURRENT");
  if (current.is_open()) {
    current << "MANIFEST-1" << std::endl;
    current.close();
  }
  
  return Status::OK();
}

// ==================== CedarGraphDB 内部接口 ====================

LsmEngine* CedarGraphDB::GetLsmEngine() {
  return impl_->GetLsmEngine();
}

}  // namespace cedar
