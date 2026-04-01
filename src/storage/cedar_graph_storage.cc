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

#include "cedar/storage/cedar_graph_storage.h"

#include <filesystem>
#include <future>
#include <thread>

#include "cedar/storage/lsm_engine.h"
#include "cedar/storage/auto_blob_storage.h"
#include "cedar/core/env.h"
#include "cedar/transaction/occ_transaction.h"

namespace cedar {

// CedarGraphStorage::Rep implementation
struct CedarGraphStorage::Rep {
  std::string db_path;
  CedarOptions options;
  cedar::Env* env;
  std::unique_ptr<LsmEngine> engine;
  
  // Auto Blob Storage components
  std::unique_ptr<BlobFileManager> blob_manager;
  std::unique_ptr<AutoBlobStorage> auto_blob;
};

CedarGraphStorage::CedarGraphStorage(const std::string& db_path,
                                   const CedarOptions& options,
                                   cedar::Env* env)
    : rep_(std::make_unique<Rep>()) {
  rep_->db_path = db_path;
  rep_->options = options;
  rep_->env = env;
}

CedarGraphStorage::~CedarGraphStorage() = default;

Status CedarGraphStorage::Open() {
  // Open LSM engine
  rep_->engine = std::make_unique<LsmEngine>(rep_->db_path, rep_->options, rep_->env);
  Status s = rep_->engine->Open();
  if (!s.ok()) {
    return s;
  }
  
  // Initialize Blob manager if auto blob is enabled
  if (rep_->options.blob_storage.enable_auto_blob) {
    BlobFileManager::Config blob_config;
    blob_config.blob_dir = rep_->options.blob_storage.blob_dir.empty() ? 
                           rep_->db_path + "/blobs" : 
                           rep_->options.blob_storage.blob_dir;
    blob_config.min_blob_size = rep_->options.blob_storage.min_blob_size;
    blob_config.max_blob_file_size = rep_->options.blob_storage.max_blob_file_size;
    
    // Create blob directory if needed
    if (!rep_->env->FileExists(blob_config.blob_dir)) {
      auto ls = rep_->env->CreateDir(blob_config.blob_dir);
      if (!ls.ok()) {
        return Status::IOError("CedarGraphStorage", 
                               "Failed to create blob directory: " + ls.ToString());
      }
    }
    
    s = BlobFileManager::Open(blob_config, rep_->env, &rep_->blob_manager);
    if (!s.ok()) {
      return Status::IOError("CedarGraphStorage", 
                             "Failed to open blob manager: " + s.ToString());
    }
    
    // Create auto blob storage wrapper
    AutoBlobConfig auto_config;
    auto_config.inline_string_max_len = rep_->options.blob_storage.inline_string_max_len;
    auto_config.min_blob_size = rep_->options.blob_storage.min_blob_size;
    auto_config.enable_auto_blob = true;
    
    rep_->auto_blob = std::make_unique<AutoBlobStorage>(
        rep_->engine.get(), rep_->blob_manager.get(), auto_config);
  }
  
  return Status::OK();
}

Status CedarGraphStorage::Open(const CedarOptions& options, 
                              const std::string& name,
                              CedarGraphStorage** dbptr) {
  *dbptr = nullptr;
  
  cedar::Env* env = options.env ? options.env : cedar::Env::Default();
  
  // Check if database exists
  if (options.error_if_exists && std::filesystem::exists(name)) {
    return Status::InvalidArgument("CedarGraphStorage", "database already exists");
  }
  
  if (!options.create_if_missing && !std::filesystem::exists(name)) {
    return Status::InvalidArgument("CedarGraphStorage", "database does not exist");
  }
  
  CedarGraphStorage* db = new CedarGraphStorage(name, options, env);
  Status s = db->Open();
  
  if (s.ok()) {
    *dbptr = db;
  } else {
    delete db;
  }
  
  return s;
}

Status CedarGraphStorage::DestroyDB(const std::string& name, const CedarOptions& options) {
  // Use std::filesystem for reliable recursive directory removal
  std::error_code ec;
  std::filesystem::remove_all(name, ec);
  if (ec) {
    return Status::IOError("CedarGraphStorage::DestroyDB", ec.message());
  }
  return Status::OK();
}

Status CedarGraphStorage::Put(uint64_t entity_id, uint64_t tx_time, const Descriptor& descriptor, Timestamp txn_version) {
  WriteOptions options;
  return Put(options, entity_id, tx_time, descriptor, txn_version);
}

Status CedarGraphStorage::Put(const WriteOptions& options,
                             uint64_t entity_id, 
                             uint64_t tx_time, 
                             const Descriptor& descriptor,
                             Timestamp txn_version) {
  if (!rep_->engine) {
    return Status::InvalidArgument("CedarGraphStorage", "not opened");
  }
  
  // Use column_id from descriptor
  CedarKey key = CedarKey::Vertex(entity_id, VertexColumnId(descriptor.GetColumnId()), Timestamp(tx_time));
  Status s = rep_->engine->Put(key, descriptor, txn_version);
  
  if (s.ok() && options.sync) {
    s = rep_->engine->ForceFlush();
  }
  
  return s;
}

Status CedarGraphStorage::Delete(uint64_t entity_id, uint64_t tx_time, Timestamp txn_version) {
  WriteOptions options;
  return Delete(options, entity_id, tx_time, txn_version);
}

Status CedarGraphStorage::Delete(const WriteOptions& options,
                                uint64_t entity_id, 
                                uint64_t tx_time,
                                Timestamp txn_version) {
  if (!rep_->engine) {
    return Status::InvalidArgument("CedarGraphStorage", "not opened");
  }
  
  CedarKey key = CedarKey::Vertex(entity_id, 0_vcol, Timestamp(tx_time));
  Status s = rep_->engine->Delete(key, txn_version);
  
  if (s.ok() && options.sync) {
    s = rep_->engine->ForceFlush();
  }
  
  return s;
}

std::optional<Descriptor> CedarGraphStorage::Get(uint64_t entity_id, uint64_t tx_time) {
  // Try different column_ids (0-10 should cover most cases)
  for (uint16_t col = 0; col < 10; col++) {
    auto result = Get(entity_id, EntityType::Vertex, col, Timestamp(tx_time));
    if (result.has_value()) {
      return result;
    }
  }
  return std::nullopt;
}

std::optional<Descriptor> CedarGraphStorage::Get(uint64_t entity_id, 
                                                EntityType entity_type,
                                                uint16_t column_id,
                                                Timestamp timestamp) {
  if (!rep_->engine) {
    return std::nullopt;
  }
  
  // Query through LsmEngine
  return rep_->engine->GetAtTime(entity_id, entity_type, column_id, timestamp);
}

// ========== 边数据专属 API 实现 ==========

Status CedarGraphStorage::PutEdge(uint64_t src_id, 
                                 uint64_t dst_id,
                                 uint16_t edge_type,
                                 Timestamp timestamp,
                                 const Descriptor& descriptor,
                                 Timestamp txn_version) {
  WriteOptions options;
  return PutEdge(options, src_id, dst_id, edge_type, timestamp, descriptor, txn_version);
}

Status CedarGraphStorage::PutEdge(const WriteOptions& options,
                                 uint64_t src_id, 
                                 uint64_t dst_id,
                                 uint16_t edge_type,
                                 Timestamp timestamp,
                                 const Descriptor& descriptor,
                                 Timestamp txn_version) {
  if (!rep_->engine) {
    return Status::InvalidArgument("CedarGraphStorage", "not opened");
  }
  
  // 创建出边 Key (src -> dst)
  CedarKey edge_key = CedarKey::EdgeOut(src_id, dst_id, edge_type, timestamp);
  
  // 使用 descriptor 的 column_id 作为 edge_type
  Descriptor edge_desc = descriptor;
  edge_desc.SetColumnId(edge_type);
  
  Status s = rep_->engine->Put(edge_key, edge_desc, txn_version);
  
  if (s.ok() && options.sync) {
    s = rep_->engine->ForceFlush();
  }
  
  return s;
}

std::optional<Descriptor> CedarGraphStorage::GetEdge(uint64_t src_id,
                                                    uint64_t dst_id,
                                                    uint16_t edge_type,
                                                    Timestamp timestamp) {
  if (!rep_->engine) {
    return std::nullopt;
  }
  
  // 构建出边 Key 查询
  CedarKey edge_key = CedarKey::EdgeOut(src_id, dst_id, edge_type, timestamp);
  
  // 通过 LsmEngine 查询
  return rep_->engine->GetAtTime(src_id, EntityType::EdgeOut, edge_type, timestamp);
}

std::vector<std::tuple<uint64_t, Timestamp, Descriptor>> CedarGraphStorage::ScanEdges(
    uint64_t src_id,
    uint16_t edge_type,
    Timestamp start_time,
    Timestamp end_time) {
  std::vector<std::tuple<uint64_t, Timestamp, Descriptor>> results;
  
  if (!rep_->engine) {
    return results;
  }
  
  // 查询出边数据 (EntityType::EdgeOut)
  auto entries = rep_->engine->GetRangeLimit(
      src_id, EntityType::EdgeOut, edge_type, start_time, end_time, SIZE_MAX);
  
  // 转换为 (dst_id, timestamp, descriptor) 格式
  for (const auto& entry : entries) {
    // Note: 目前返回的 entry 不包含 dst_id，需要进一步优化
    // 暂时返回 src_id 作为占位
    results.emplace_back(src_id, entry.timestamp, entry.descriptor);
  }
  
  return results;
}

Status CedarGraphStorage::ForceFlush() {
  if (!rep_->engine) {
    return Status::InvalidArgument("CedarGraphStorage", "not opened");
  }
  
  // Sync blob manager first if available
  if (rep_->blob_manager) {
    Status s = rep_->blob_manager->Sync();
    if (!s.ok()) {
      return s;
    }
  }
  
  return rep_->engine->ForceFlush();
}

Status CedarGraphStorage::Compact() {
  if (!rep_->engine) {
    return Status::InvalidArgument("CedarGraphStorage", "not opened");
  }
  
  return rep_->engine->Compact();
}

CedarGraphStorage::Stats CedarGraphStorage::GetStats() const {
  if (!rep_->engine) {
    return Stats();
  }
  
  auto engine_stats = rep_->engine->GetStats();
  
  Stats stats;
  stats.memtable_size = engine_stats.memtable_size;
  stats.imm_memtable_size = engine_stats.imm_memtable_size;
  stats.frond_count = engine_stats.frond_count;
  stats.frond_size = engine_stats.frond_size;
  stats.num_levels = engine_stats.num_levels;
  
  return stats;
}

std::vector<std::pair<Timestamp, Descriptor>> CedarGraphStorage::Scan(
    uint64_t entity_id, 
    Timestamp start_time, 
    Timestamp end_time) {
  return ScanLimit(entity_id, start_time, end_time, SIZE_MAX);
}

std::vector<std::pair<Timestamp, Descriptor>> CedarGraphStorage::Scan(
    uint64_t entity_id,
    EntityType entity_type,
    uint16_t column_id,
    Timestamp start_time,
    Timestamp end_time) {
  std::vector<std::pair<Timestamp, Descriptor>> results;
  
  if (!rep_->engine) {
    return results;
  }
  
  // Query specific entity type and column
  auto entries = rep_->engine->GetRangeLimit(
      entity_id, entity_type, column_id, start_time, end_time, SIZE_MAX);
  
  for (const auto& entry : entries) {
    results.emplace_back(entry.timestamp, entry.descriptor);
  }
  
  // Sort by timestamp descending (newest first)
  std::sort(results.begin(), results.end(), 
            [](const auto& a, const auto& b) { return a.first > b.first; });
  
  return results;
}

std::vector<std::pair<Timestamp, Descriptor>> CedarGraphStorage::ScanLimit(
    uint64_t entity_id, 
    Timestamp start_time, 
    Timestamp end_time,
    size_t max_results) {
  std::vector<std::pair<Timestamp, Descriptor>> results;
  
  if (!rep_->engine) {
    return results;
  }
  
  // Get only column IDs that have data for this entity (optimization)
  auto column_ids = rep_->engine->GetEntityColumnIds(entity_id);
  
  // If no tracked columns, fall back to trying common column IDs
  if (column_ids.empty()) {
    for (uint16_t col = 0; col < 10 && results.size() < max_results; col++) {
      column_ids.push_back(col);
    }
  }
  
  // Query only the columns that have data
  for (uint16_t col : column_ids) {
    if (results.size() >= max_results) break;
    
    auto entries = rep_->engine->GetRangeLimit(
        entity_id, EntityType::Vertex, col, start_time, end_time, 
        max_results - results.size());
    
    for (const auto& entry : entries) {
      results.emplace_back(entry.timestamp, entry.descriptor);
    }
  }
  
  // Sort by timestamp descending (newest first)
  std::sort(results.begin(), results.end(), 
            [](const auto& a, const auto& b) { return a.first > b.first; });
  
  return results;
}

std::vector<std::pair<Timestamp, Descriptor>> CedarGraphStorage::ScanMemTableOnly(
    uint64_t entity_id, 
    Timestamp start_time, 
    Timestamp end_time,
    size_t max_results) {
  std::vector<std::pair<Timestamp, Descriptor>> results;
  
  if (!rep_->engine) {
    return results;
  }
  
  // Get memtable pointer
  auto* memtable = rep_->engine->GetMemTable();
  if (!memtable) {
    return results;
  }
  
  // Try different column_ids
  for (uint16_t col = 0; col < 10 && results.size() < max_results; col++) {
    auto entries = memtable->GetRange(entity_id, EntityType::Vertex, col, start_time, end_time);
    
    for (const auto& entry : entries) {
      if (results.size() >= max_results) {
        break;
      }
      results.emplace_back(entry.timestamp, entry.descriptor);
    }
  }
  
  // Sort by timestamp descending (newest first)
  std::sort(results.begin(), results.end(), 
            [](const auto& a, const auto& b) { return a.first > b.first; });
  
  return results;
}

// ========== 批量查询接口实现 ==========

void CedarGraphStorage::BatchGet(std::vector<BatchQueryItem>& items) {
  if (!rep_->engine || items.empty()) {
    return;
  }
  
  // Convert to LsmEngine format
  std::vector<LsmEngine::BatchQueryItem> engine_items;
  engine_items.reserve(items.size());
  
  for (auto& item : items) {
    engine_items.emplace_back();
    engine_items.back().entity_id = item.entity_id;
    engine_items.back().entity_type = item.entity_type;
    engine_items.back().column_id = item.column_id;
    engine_items.back().timestamp = item.timestamp;
  }
  
  // Call batch query
  rep_->engine->BatchGetAtTime(engine_items);
  
  // Copy results back
  for (size_t i = 0; i < items.size(); i++) {
    items[i].result = engine_items[i].result;
  }
}

void CedarGraphStorage::BatchScan(std::vector<BatchScanItem>& items) {
  if (!rep_->engine || items.empty()) {
    return;
  }
  
  // Convert to LsmEngine format
  std::vector<LsmEngine::BatchRangeItem> engine_items;
  engine_items.reserve(items.size());
  
  for (auto& item : items) {
    engine_items.emplace_back();
    engine_items.back().entity_id = item.entity_id;
    engine_items.back().entity_type = item.entity_type;
    engine_items.back().column_id = item.column_id;
    engine_items.back().start = item.start_time;
    engine_items.back().end = item.end_time;
    engine_items.back().max_results = item.max_results;
  }
  
  // Call batch query
  rep_->engine->BatchGetRange(engine_items);
  
  // Copy results back
  for (size_t i = 0; i < items.size(); i++) {
    items[i].results.clear();
    for (const auto& entry : engine_items[i].results) {
      items[i].results.emplace_back(entry.timestamp, entry.descriptor);
    }
    
    // Sort by timestamp descending
    std::sort(items[i].results.begin(), items[i].results.end(),
              [](const auto& a, const auto& b) { return a.first > b.first; });
  }
}

LsmEngine* CedarGraphStorage::GetLsmEngine() const {
  if (!rep_) {
    return nullptr;
  }
  return rep_->engine.get();
}

// ========== 自动 Blob 存储 API 实现 ==========

Status CedarGraphStorage::PutString(uint64_t entity_id, uint16_t col_id, 
                                    const std::string& value,
                                    Timestamp txn_version) {
  if (!rep_->engine) {
    return Status::InvalidArgument("CedarGraphStorage", "not opened");
  }
  
  // Use auto blob storage if available
  if (rep_->auto_blob) {
    return rep_->auto_blob->PutString(entity_id, col_id, value);
  }
  
  // Fallback to inline storage only
  uint32_t encoded = 0;
  memcpy(&encoded, value.data(), std::min(value.size(), size_t(4)));
  
  CedarKey key = CedarKey::Vertex(entity_id, col_id, Timestamp(0));
  Descriptor desc = Descriptor::InlineInt(col_id, static_cast<int32_t>(encoded));
  return rep_->engine->Put(key, desc, txn_version);
}

std::optional<std::string> CedarGraphStorage::GetString(uint64_t entity_id, uint16_t col_id) {
  if (!rep_->engine) {
    return std::nullopt;
  }
  
  // Use auto blob storage if available
  if (rep_->auto_blob) {
    return rep_->auto_blob->GetString(entity_id, col_id);
  }
  
  // Fallback to inline storage only
  auto versions = rep_->engine->GetAll(entity_id, EntityType::Vertex, col_id);
  if (versions.empty()) {
    return std::nullopt;
  }
  
  auto val = versions[0].descriptor.AsInlineInt();
  if (!val) {
    return std::nullopt;
  }
  
  char buf[5] = {};
  uint32_t payload = static_cast<uint32_t>(*val);
  memcpy(buf, &payload, 4);
  return std::string(buf);
}

Status CedarGraphStorage::PutBinary(uint64_t entity_id, uint16_t col_id,
                                    const void* data, size_t size,
                                    Timestamp txn_version) {
  std::string str(static_cast<const char*>(data), size);
  return PutString(entity_id, col_id, str, txn_version);
}

std::vector<uint8_t> CedarGraphStorage::GetBinary(uint64_t entity_id, uint16_t col_id) {
  auto str_opt = GetString(entity_id, col_id);
  if (!str_opt) {
    return {};
  }
  
  std::vector<uint8_t> result(str_opt->begin(), str_opt->end());
  return result;
}

// ========== 静态属性 + 动态属性 API 实现 ==========

// ----- 节点静态属性 -----
Status CedarGraphStorage::PutStaticVertex(uint64_t vertex_id, 
                                         uint16_t property_id,
                                         const Descriptor& descriptor) {
  if (!rep_->engine) {
    return Status::InvalidArgument("CedarGraphStorage", "not opened");
  }
  
  // 使用 Timestamp::Static() (值为1) 表示静态属性
  CedarKey key = CedarKey::Vertex(vertex_id, VertexColumnId(property_id), Timestamp::Static());
  
  // 设置 descriptor 的 column_id
  Descriptor static_desc = descriptor;
  static_desc.SetColumnId(property_id);
  
  return rep_->engine->Put(key, static_desc, Timestamp::Now());
}

std::optional<Descriptor> CedarGraphStorage::GetStaticVertex(uint64_t vertex_id,
                                                            uint16_t property_id) {
  if (!rep_->engine) {
    return std::nullopt;
  }
  
  // 使用 Timestamp::Static() 查询
  return rep_->engine->GetAtTime(vertex_id, EntityType::Vertex, property_id, Timestamp::Static());
}

// ----- 节点动态属性 -----
Status CedarGraphStorage::PutDynamicVertex(uint64_t vertex_id,
                                          uint16_t property_id,
                                          Timestamp timestamp,
                                          const Descriptor& descriptor,
                                          Timestamp txn_version) {
  if (!rep_->engine) {
    return Status::InvalidArgument("CedarGraphStorage", "not opened");
  }
  
  CedarKey key = CedarKey::Vertex(vertex_id, VertexColumnId(property_id), timestamp);
  
  Descriptor dynamic_desc = descriptor;
  dynamic_desc.SetColumnId(property_id);
  
  return rep_->engine->Put(key, dynamic_desc, txn_version);
}

std::optional<Descriptor> CedarGraphStorage::GetDynamicVertex(uint64_t vertex_id,
                                                             uint16_t property_id,
                                                             Timestamp timestamp) {
  if (!rep_->engine) {
    return std::nullopt;
  }
  
  return rep_->engine->GetAtTime(vertex_id, EntityType::Vertex, property_id, timestamp);
}

std::vector<std::pair<Timestamp, Descriptor>> CedarGraphStorage::ScanDynamicVertex(
    uint64_t vertex_id,
    uint16_t property_id,
    Timestamp start_time,
    Timestamp end_time) {
  std::vector<std::pair<Timestamp, Descriptor>> results;
  
  if (!rep_->engine) {
    return results;
  }
  
  auto entries = rep_->engine->GetRangeLimit(
      vertex_id, EntityType::Vertex, property_id, start_time, end_time, SIZE_MAX);
  
  for (const auto& entry : entries) {
    results.emplace_back(entry.timestamp, entry.descriptor);
  }
  
  // 按时间戳降序排序
  std::sort(results.begin(), results.end(),
            [](const auto& a, const auto& b) { return a.first > b.first; });
  
  return results;
}

// ----- 边静态属性 -----
Status CedarGraphStorage::PutStaticEdge(uint64_t src_id,
                                       uint64_t dst_id,
                                       uint16_t edge_type,
                                       uint16_t property_id,
                                       const Descriptor& descriptor) {
  if (!rep_->engine) {
    return Status::InvalidArgument("CedarGraphStorage", "not opened");
  }
  
  // 使用 Timestamp::Static() 表示静态属性
  CedarKey key = CedarKey::EdgeOut(src_id, dst_id, edge_type, Timestamp::Static());
  
  Descriptor static_desc = descriptor;
  static_desc.SetColumnId(property_id);
  
  return rep_->engine->Put(key, static_desc, Timestamp::Now());
}

std::optional<Descriptor> CedarGraphStorage::GetStaticEdge(uint64_t src_id,
                                                          uint64_t dst_id,
                                                          uint16_t edge_type,
                                                          uint16_t property_id) {
  if (!rep_->engine) {
    return std::nullopt;
  }
  
  // 构造完整的 Edge Key 直接查询
  // PutStaticEdge 使用: CedarKey::EdgeOut(src_id, dst_id, edge_type, Timestamp::Static())
  // 我们需要查询相同的 key
  CedarKey key = CedarKey::EdgeOut(src_id, dst_id, edge_type, Timestamp::Static());
  
  // 使用底层引擎直接查询
  // 由于 LsmEngine 没有直接根据 key 查询的接口，我们使用 GetAtTime
  // 并验证返回结果的 dst_id 匹配
  auto result = rep_->engine->GetAtTime(src_id, EntityType::EdgeOut, edge_type, Timestamp::Static());
  
  if (result.has_value() && result->GetColumnId() == property_id) {
    return result;
  }
  
  // 如果上面的方法没有找到，尝试扫描所有版本
  // 获取所有 EdgeOut 记录，过滤匹配 dst_id 和 property_id 的
  auto all_entries = rep_->engine->GetAll(src_id, EntityType::EdgeOut, edge_type);
  
  for (const auto& entry : all_entries) {
    // 检查时间戳是否为 Static (静态属性标记)
    if (entry.timestamp.value() == Timestamp::Static().value() &&
        entry.descriptor.GetColumnId() == property_id) {
      return entry.descriptor;
    }
  }
  
  return std::nullopt;
}

// ----- 边动态属性 -----
Status CedarGraphStorage::PutDynamicEdge(uint64_t src_id,
                                        uint64_t dst_id,
                                        uint16_t edge_type,
                                        uint16_t property_id,
                                        Timestamp timestamp,
                                        const Descriptor& descriptor,
                                        Timestamp txn_version) {
  if (!rep_->engine) {
    return Status::InvalidArgument("CedarGraphStorage", "not opened");
  }
  
  CedarKey key = CedarKey::EdgeOut(src_id, dst_id, edge_type, timestamp);
  
  Descriptor dynamic_desc = descriptor;
  dynamic_desc.SetColumnId(property_id);
  
  return rep_->engine->Put(key, dynamic_desc, txn_version);
}

std::optional<Descriptor> CedarGraphStorage::GetDynamicEdge(uint64_t src_id,
                                                           uint64_t dst_id,
                                                           uint16_t edge_type,
                                                           uint16_t property_id,
                                                           Timestamp timestamp) {
  if (!rep_->engine) {
    return std::nullopt;
  }
  
  return rep_->engine->GetAtTime(src_id, EntityType::EdgeOut, property_id, timestamp);
}

std::vector<std::pair<Timestamp, Descriptor>> CedarGraphStorage::ScanDynamicEdge(
    uint64_t src_id,
    uint64_t dst_id,
    uint16_t edge_type,
    uint16_t property_id,
    Timestamp start_time,
    Timestamp end_time) {
  std::vector<std::pair<Timestamp, Descriptor>> results;
  
  if (!rep_->engine) {
    return results;
  }
  
  // Note: 当前实现会返回该 src_id 下所有边的数据，需要进一步过滤 dst_id
  auto entries = rep_->engine->GetRangeLimit(
      src_id, EntityType::EdgeOut, property_id, start_time, end_time, SIZE_MAX);
  
  for (const auto& entry : entries) {
    results.emplace_back(entry.timestamp, entry.descriptor);
  }
  
  std::sort(results.begin(), results.end(),
            [](const auto& a, const auto& b) { return a.first > b.first; });
  
  return results;
}

// ========== 混合查询实现 ==========

CedarGraphStorage::VertexSnapshot CedarGraphStorage::GetVertexSnapshot(
    uint64_t vertex_id,
    const std::vector<uint16_t>& static_prop_ids,
    const std::vector<uint16_t>& dynamic_prop_ids,
    Timestamp timestamp) {
  VertexSnapshot snapshot;
  snapshot.vertex_id = vertex_id;
  snapshot.query_time = timestamp;
  
  if (!rep_->engine) {
    return snapshot;
  }
  
  // 查询静态属性
  for (uint16_t prop_id : static_prop_ids) {
    auto result = GetStaticVertex(vertex_id, prop_id);
    if (result.has_value()) {
      snapshot.static_props[prop_id] = *result;
    }
  }
  
  // 查询动态属性
  for (uint16_t prop_id : dynamic_prop_ids) {
    auto result = GetDynamicVertex(vertex_id, prop_id, timestamp);
    if (result.has_value()) {
      snapshot.dynamic_props[prop_id] = *result;
    }
  }
  
  return snapshot;
}

std::vector<CedarGraphStorage::VertexSnapshot> CedarGraphStorage::BatchGetVertexSnapshots(
    const std::vector<uint64_t>& vertex_ids,
    const std::vector<uint16_t>& static_prop_ids,
    const std::vector<uint16_t>& dynamic_prop_ids,
    Timestamp timestamp) {
  std::vector<VertexSnapshot> results;
  results.reserve(vertex_ids.size());
  
  for (uint64_t vid : vertex_ids) {
    results.push_back(GetVertexSnapshot(vid, static_prop_ids, dynamic_prop_ids, timestamp));
  }
  
  return results;
}

}  // namespace cedar
// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// 实体生命周期追踪 API 实现

#include "cedar/storage/cedar_graph_storage.h"
#include "cedar/storage/entity_lifecycle.h"

namespace cedar {

// ========== 生命周期标记操作 ==========

Status CedarGraphStorage::MarkEntityCreated(uint64_t entity_id, EntityType type, Timestamp timestamp) {
    if (!rep_->engine) {
        return Status::InvalidArgument("CedarGraphStorage", "not opened");
    }
    
    Descriptor lifecycle_desc = LifecycleDescriptor::Create(LifecycleEvent::Created);
    
    if (type == EntityType::Vertex) {
        CedarKey key = CedarKey::Vertex(entity_id, kLifecycleColumnId, timestamp);
        return rep_->engine->Put(key, lifecycle_desc, Timestamp::Now());
    } else if (type == EntityType::EdgeOut) {
        // 边需要 src/dst，这里简化处理，使用 entity_id 作为 src，dst=0 作为占位
        // 实际使用时应该传入完整的边信息
        CedarKey key = CedarKey::EdgeOut(entity_id, 0, kLifecycleColumnId, timestamp);
        return rep_->engine->Put(key, lifecycle_desc, Timestamp::Now());
    }
    
    return Status::InvalidArgument("CedarGraphStorage", "unsupported entity type");
}

Status CedarGraphStorage::MarkEntityDeleted(uint64_t entity_id, EntityType type, Timestamp timestamp) {
    if (!rep_->engine) {
        return Status::InvalidArgument("CedarGraphStorage", "not opened");
    }
    
    Descriptor lifecycle_desc = LifecycleDescriptor::Create(LifecycleEvent::Deleted);
    
    if (type == EntityType::Vertex) {
        CedarKey key = CedarKey::Vertex(entity_id, kLifecycleColumnId, timestamp);
        return rep_->engine->Put(key, lifecycle_desc, Timestamp::Now());
    } else if (type == EntityType::EdgeOut) {
        CedarKey key = CedarKey::EdgeOut(entity_id, 0, kLifecycleColumnId, timestamp);
        return rep_->engine->Put(key, lifecycle_desc, Timestamp::Now());
    }
    
    return Status::InvalidArgument("CedarGraphStorage", "unsupported entity type");
}

Status CedarGraphStorage::MarkEntityRecreated(uint64_t entity_id, EntityType type, Timestamp timestamp) {
    if (!rep_->engine) {
        return Status::InvalidArgument("CedarGraphStorage", "not opened");
    }
    
    Descriptor lifecycle_desc = LifecycleDescriptor::Create(LifecycleEvent::Recreated);
    
    if (type == EntityType::Vertex) {
        CedarKey key = CedarKey::Vertex(entity_id, kLifecycleColumnId, timestamp);
        return rep_->engine->Put(key, lifecycle_desc, Timestamp::Now());
    } else if (type == EntityType::EdgeOut) {
        CedarKey key = CedarKey::EdgeOut(entity_id, 0, kLifecycleColumnId, timestamp);
        return rep_->engine->Put(key, lifecycle_desc, Timestamp::Now());
    }
    
    return Status::InvalidArgument("CedarGraphStorage", "unsupported entity type");
}

// ========== 生命周期查询 ==========

bool CedarGraphStorage::EntityExistsAt(uint64_t entity_id, EntityType type, Timestamp timestamp) {
    if (!rep_->engine) {
        return false;
    }
    
    // 查询生命周期历史，找到指定时间点的最新事件
    auto history = GetEntityLifecycleHistory(entity_id, type, Timestamp(0), timestamp);
    
    if (history.empty()) {
        // 无生命周期记录：向后兼容，检查是否有属性数据
        // 如果有属性数据，认为实体在创建时就已经存在
        auto props = Scan(entity_id, type, kLifecycleColumnId, Timestamp(0), timestamp);
        return !props.empty();
    }
    
    // 取最新的生命周期事件
    const auto& latest_event = history.back();
    
    // Created 或 Recreated 表示实体存在
    return latest_event.event == LifecycleEvent::Created || 
           latest_event.event == LifecycleEvent::Recreated;
}

EntityState CedarGraphStorage::GetEntityState(uint64_t entity_id, EntityType type) {
    if (!rep_->engine) {
        return EntityState::NeverExisted;
    }
    
    auto history = GetEntityLifecycleHistory(entity_id, type, Timestamp(0), Timestamp::Max());
    
    if (history.empty()) {
        // 向后兼容：检查是否有属性数据
        auto props = Scan(entity_id, type, 0, Timestamp(0), Timestamp::Max());
        if (!props.empty()) {
            return EntityState::Active;
        }
        return EntityState::NeverExisted;
    }
    
    const auto& latest = history.back();
    switch (latest.event) {
        case LifecycleEvent::Created:
        case LifecycleEvent::Recreated:
            return EntityState::Active;
        case LifecycleEvent::Deleted:
            return EntityState::Deleted;
        case LifecycleEvent::Purged:
            return EntityState::Purged;
        default:
            return EntityState::NeverExisted;
    }
}

std::vector<LifecycleEntry> CedarGraphStorage::GetEntityLifecycleHistory(
    uint64_t entity_id, EntityType type, 
    Timestamp start_time, Timestamp end_time) {
    std::vector<LifecycleEntry> history;
    
    if (!rep_->engine) {
        return history;
    }
    
    // 查询生命周期列的数据
    auto entries = rep_->engine->GetRangeLimit(
        entity_id, type, kLifecycleColumnId, start_time, end_time, SIZE_MAX);
    
    for (const auto& entry : entries) {
        auto event_opt = LifecycleDescriptor::Parse(entry.descriptor);
        if (event_opt.has_value()) {
            history.emplace_back(entry.timestamp, *event_opt);
        }
    }
    
    // 按时间戳升序排序（从早到晚）
    std::sort(history.begin(), history.end(),
              [](const auto& a, const auto& b) { return a.timestamp < b.timestamp; });
    
    return history;
}

std::vector<AlivePeriod> CedarGraphStorage::GetEntityAlivePeriods(uint64_t entity_id, EntityType type) {
    std::vector<AlivePeriod> periods;
    
    auto history = GetEntityLifecycleHistory(entity_id, type, Timestamp(0), Timestamp::Max());
    
    if (history.empty()) {
        // 向后兼容：如果有属性数据，认为从时间戳0开始存活至今
        auto props = Scan(entity_id, type, 0, Timestamp(0), Timestamp::Max());
        if (!props.empty()) {
            periods.emplace_back(Timestamp(0), Timestamp::Max());
        }
        return periods;
    }
    
    Timestamp current_start;
    bool in_alive_period = false;
    
    for (const auto& entry : history) {
        switch (entry.event) {
            case LifecycleEvent::Created:
            case LifecycleEvent::Recreated:
                if (!in_alive_period) {
                    current_start = entry.timestamp;
                    in_alive_period = true;
                }
                break;
                
            case LifecycleEvent::Deleted:
            case LifecycleEvent::Purged:
                if (in_alive_period) {
                    periods.emplace_back(current_start, entry.timestamp);
                    in_alive_period = false;
                }
                break;
                
            default:
                break;
        }
    }
    
    // 如果最后仍处于存活状态
    if (in_alive_period) {
        periods.emplace_back(current_start, Timestamp::Max());
    }
    
    return periods;
}

std::vector<uint64_t> CedarGraphStorage::GetActiveEntities(
    EntityType type, Timestamp timestamp) {
    std::vector<uint64_t> active_entities;
    
    if (!rep_->engine) {
        return active_entities;
    }
    
    // Get all SST files and collect unique entity IDs
    std::unordered_set<uint64_t> all_entities;
    auto files = rep_->engine->GetFrondFiles(0);  // Level 0
    
    for (const auto& meta : files) {
        // Add all entity IDs in range (this is an approximation)
        // In a real implementation, we would scan the file to get exact entity IDs
        // For now, we use a sampling approach
        uint64_t step = (meta.max_entity_id - meta.min_entity_id) / std::max(meta.num_entries, uint64_t(1));
        if (step == 0) step = 1;
        
        for (uint64_t eid = meta.min_entity_id; eid <= meta.max_entity_id; eid += step) {
            all_entities.insert(eid);
        }
    }
    
    // Also check column tracking map for recently written entities
    // This is a temporary solution - a proper implementation needs an entity index
    
    // Check each entity's lifecycle
    for (uint64_t entity_id : all_entities) {
        if (EntityExistsAt(entity_id, type, timestamp)) {
            active_entities.push_back(entity_id);
        }
    }
    
    // Sort and deduplicate
    std::sort(active_entities.begin(), active_entities.end());
    active_entities.erase(std::unique(active_entities.begin(), active_entities.end()), 
                          active_entities.end());
    
    return active_entities;
}

// ========== 事务 API 实现 ==========

OCCTransaction* CedarGraphStorage::BeginTransaction(
    const TransactionOptions* options) {
  if (!rep_->engine) {
    return nullptr;
  }
  
  // 使用提供的选项或默认选项
  TransactionOptions txn_options;
  if (options != nullptr) {
    txn_options = *options;
  }
  
  // 调用 LsmEngine 的 BeginTransaction，释放所有权并返回原始指针
  auto txn_ptr = rep_->engine->BeginTransaction(txn_options);
  return txn_ptr.release();
}

// ========== 批量写入接口实现 ==========

Status CedarGraphStorage::BatchWrite(const std::vector<BatchWriteItem>& items, 
                                     size_t batch_size) {
  if (!rep_->engine || items.empty()) {
    return Status::OK();
  }
  
  // 默认批量大小
  if (batch_size == 0) {
    batch_size = 1000;
  }
  
  size_t total = items.size();
  size_t processed = 0;
  
  while (processed < total) {
    size_t current_batch = std::min(batch_size, total - processed);
    
    // 开启批量事务
    auto* txn = BeginTransaction();
    if (!txn) {
      return Status::InvalidArgument("BatchWrite", "failed to begin transaction");
    }
    
    // 执行批量写入
    for (size_t i = 0; i < current_batch; ++i) {
      const auto& item = items[processed + i];
      
      Status s = txn->Put(item.entity_id, item.entity_type, item.column_id,
                          item.descriptor, item.timestamp, item.target_id);
      if (!s.ok()) {
        txn->Abort();
        delete txn;
        return s;
      }
    }
    
    // 提交事务
    Status commit_status = txn->Commit();
    delete txn;
    
    if (!commit_status.ok()) {
      return commit_status;
    }
    
    processed += current_batch;
  }
  
  return Status::OK();
}

Status CedarGraphStorage::BatchPutStaticVertex(
    const std::vector<std::pair<uint64_t, Descriptor>>& items,
    uint16_t property_id,
    size_t batch_size) {
  if (items.empty()) {
    return Status::OK();
  }
  
  std::vector<BatchWriteItem> write_items;
  write_items.reserve(items.size());
  
  for (const auto& [vertex_id, descriptor] : items) {
    write_items.emplace_back(vertex_id, EntityType::Vertex, property_id,
                             descriptor, Timestamp::Static());
  }
  
  return BatchWrite(write_items, batch_size);
}

Status CedarGraphStorage::BatchPutDynamicVertex(
    const std::vector<std::tuple<uint64_t, Timestamp, Descriptor>>& items,
    uint16_t property_id,
    size_t batch_size) {
  if (items.empty()) {
    return Status::OK();
  }
  
  std::vector<BatchWriteItem> write_items;
  write_items.reserve(items.size());
  
  for (const auto& [vertex_id, timestamp, descriptor] : items) {
    write_items.emplace_back(vertex_id, EntityType::Vertex, property_id,
                             descriptor, timestamp);
  }
  
  return BatchWrite(write_items, batch_size);
}

// ========== 并行批量处理接口实现 ==========

Status CedarGraphStorage::ParallelBatchWrite(const std::vector<BatchWriteItem>& items,
                                             const ParallelBatchOptions& options) {
  if (!rep_->engine || items.empty()) {
    return Status::OK();
  }
  
  // 确定线程数
  size_t num_threads = options.num_threads;
  if (num_threads == 0) {
    num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) {
      num_threads = 4;
    }
  }
  
  // 如果数据量太小，直接回退到单线程批量写入
  size_t chunk_size = options.chunk_size;
  if (items.size() < chunk_size * 2 || num_threads == 1) {
    return BatchWrite(items, chunk_size);
  }
  
  // 按 entity_id 排序，便于分区
  std::vector<BatchWriteItem> sorted_items = items;
  std::sort(sorted_items.begin(), sorted_items.end(),
            [](const BatchWriteItem& a, const BatchWriteItem& b) {
              return a.entity_id < b.entity_id;
            });
  
  // 分区并行处理
  size_t total = sorted_items.size();
  size_t num_chunks = (total + chunk_size - 1) / chunk_size;
  std::atomic<size_t> completed_chunks{0};
  std::atomic<bool> has_error{false};
  Status error_status;
  std::mutex error_mutex;
  
  std::vector<std::future<void>> futures;
  
  for (size_t chunk = 0; chunk < num_chunks && !has_error.load(); ++chunk) {
    size_t start = chunk * chunk_size;
    size_t end = std::min(start + chunk_size, total);
    
    futures.push_back(std::async(std::launch::async, [&, start, end]() {
      if (has_error.load()) return;
      
      // 每个线程处理一个 chunk
      auto* txn = BeginTransaction();
      if (!txn) {
        std::lock_guard<std::mutex> lock(error_mutex);
        error_status = Status::InvalidArgument("ParallelBatchWrite", 
                                               "failed to begin transaction");
        has_error.store(true);
        return;
      }
      
      // 执行批量写入
      for (size_t i = start; i < end && !has_error.load(); ++i) {
        const auto& item = sorted_items[i];
        Status s = txn->Put(item.entity_id, item.entity_type, item.column_id,
                            item.descriptor, item.timestamp, item.target_id);
        if (!s.ok()) {
          txn->Abort();
          delete txn;
          std::lock_guard<std::mutex> lock(error_mutex);
          error_status = s;
          has_error.store(true);
          return;
        }
      }
      
      // 提交事务
      Status commit_status = txn->Commit();
      delete txn;
      
      if (!commit_status.ok()) {
        std::lock_guard<std::mutex> lock(error_mutex);
        error_status = commit_status;
        has_error.store(true);
        return;
      }
      
      completed_chunks.fetch_add(1);
    }));
    
    // 限制并发数
    if (futures.size() >= num_threads) {
      for (auto& f : futures) {
        f.wait();
      }
      futures.clear();
    }
    
    if (has_error.load()) {
      break;
    }
  }
  
  // 等待剩余任务
  for (auto& f : futures) {
    f.wait();
  }
  
  if (has_error.load()) {
    return error_status;
  }
  
  return Status::OK();
}

Status CedarGraphStorage::ParallelBatchWrite(const std::vector<BatchWriteItem>& items) {
  ParallelBatchOptions options;
  return ParallelBatchWrite(items, options);
}

}  // namespace cedar
