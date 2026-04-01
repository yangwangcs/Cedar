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

#ifndef FERN_API_BATCH_INSERTER_H_
#define FERN_API_BATCH_INSERTER_H_

#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <functional>
#include <fstream>
#include <sstream>
#include <iostream>
#include <filesystem>

#include "cedar/api/entity.h"
#include "cedar/storage/lsm_engine.h"
#include "cedar/core/env.h"

namespace cedar {
namespace api {

// ============================================================================
// 批量导入配置
// ============================================================================

struct BatchInserterConfig {
  size_t batch_size = 10000;           // 每批写入数量
  size_t buffer_size = 100 * 1024 * 1024;  // 缓冲区大小 (100MB)
  bool auto_flush = true;              // 自动刷盘
  bool show_progress = true;           // 显示进度
  Timestamp default_timestamp;         // 默认时间戳
  
  BatchInserterConfig() 
      : default_timestamp(
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::system_clock::now().time_since_epoch()).count()) {}
};

// ============================================================================
// CSV 解析器
// ============================================================================

class CsvParser {
 public:
  explicit CsvParser(char delimiter = ',') : delimiter_(delimiter) {}
  
  // 设置列映射
  void MapColumn(const std::string& col_name, uint16_t field_id) {
    column_map_[col_name] = field_id;
  }
  
  void MapColumn(size_t col_index, uint16_t field_id) {
    index_map_[col_index] = field_id;
  }
  
  // 解析单行
  std::vector<std::pair<uint16_t, std::string>> ParseLine(const std::string& line) {
    std::vector<std::pair<uint16_t, std::string>> result;
    std::stringstream ss(line);
    std::string cell;
    size_t col_idx = 0;
    
    while (std::getline(ss, cell, delimiter_)) {
      // 优先使用名称映射，其次使用索引映射
      auto it = column_map_.find(cell);
      if (it != column_map_.end()) {
        // 这是标题行，建立索引映射
        continue;
      }
      
      auto idx_it = index_map_.find(col_idx);
      if (idx_it != index_map_.end()) {
        result.push_back({idx_it->second, cell});
      }
      col_idx++;
    }
    
    return result;
  }
  
  // 解析标题行
  void ParseHeader(const std::string& line) {
    std::stringstream ss(line);
    std::string cell;
    size_t idx = 0;
    
    while (std::getline(ss, cell, delimiter_)) {
      auto it = column_map_.find(cell);
      if (it != column_map_.end()) {
        index_map_[idx] = it->second;
      }
      idx++;
    }
  }
  
 private:
  char delimiter_;
  std::map<std::string, uint16_t> column_map_;
  std::map<size_t, uint16_t> index_map_;
};

// ============================================================================
// 批量导入器
// ============================================================================

class BatchInserter {
 public:
  explicit BatchInserter(LsmEngine* engine, 
                         const BatchInserterConfig& config = BatchInserterConfig())
      : engine_(engine), config_(config) {}
  
  // ============================================================================
  // 流式插入接口
  // ============================================================================
  
  // 插入节点
  template<typename EntityType>
  BatchInserter& operator<<(EntityType&& entity) {
    auto kvs = entity.ToKeyValues(config_.default_timestamp);
    buffer_.insert(buffer_.end(), kvs.begin(), kvs.end());
    
    if (buffer_.size() >= config_.batch_size) {
      Flush();
    }
    
    if (config_.show_progress && ++count_ % 10000 == 0) {
      std::cout << "Inserted: " << count_ << " entities\r" << std::flush;
    }
    
    return *this;
  }
  
  // 带时间戳的插入
  template<typename EntityType>
  BatchInserter& Insert(EntityType&& entity, Timestamp timestamp) {
    auto kvs = entity.ToKeyValues(timestamp);
    buffer_.insert(buffer_.end(), kvs.begin(), kvs.end());
    
    if (buffer_.size() >= config_.batch_size) {
      Flush();
    }
    
    return *this;
  }
  
  // ============================================================================
  // CSV 导入
  // ============================================================================
  
  // 从 CSV 文件导入
  template<typename EntityType>
  BatchInserter& FromCsv(const std::string& filepath, 
                         const std::map<std::string, uint16_t>& column_mapping) {
    std::ifstream file(filepath);
    if (!file.is_open()) {
      throw std::runtime_error("Cannot open file: " + filepath);
    }
    
    CsvParser parser;
    for (const auto& [col_name, field_id] : column_mapping) {
      parser.MapColumn(col_name, field_id);
    }
    
    std::string line;
    bool is_header = true;
    size_t line_num = 0;
    
    while (std::getline(file, line)) {
      line_num++;
      
      if (is_header) {
        parser.ParseHeader(line);
        is_header = false;
        continue;
      }
      
      auto parsed = parser.ParseLine(line);
      if (parsed.empty()) continue;
      
      // 创建实体并填充字段
      EntityType entity = CreateEntityFromParsed<EntityType>(parsed);
      *this << std::move(entity);
    }
    
    if (config_.show_progress) {
      std::cout << "CSV import completed: " << line_num << " lines processed" << std::endl;
    }
    
    return *this;
  }
  
  // ============================================================================
  // JSON 导入 (简化实现)
  // ============================================================================
  
  BatchInserter& FromJson(const std::string& json_str) {
    // 简化实现：解析基本的 JSON 数组格式
    // 实际生产环境应使用 nlohmann/json 等库
    
    // 示例格式:
    // {
    //   "users": [...],
    //   "edges": [...]
    // }
    
    // 这里仅作为接口示例
    std::cout << "JSON import not fully implemented, use CSV instead" << std::endl;
    return *this;
  }
  
  // ============================================================================
  // 刷盘和清理
  // ============================================================================
  
  void Flush() {
    if (buffer_.empty()) return;
    
    // 批量写入引擎
    // engine_->IngestBatch(buffer_);  // 需要 LsmEngine 支持批量接口
    
    // 临时：逐条写入
    for (const auto& [key, desc] : buffer_) {
      engine_->Put(key, desc);
    }
    
    buffer_.clear();
    
    if (config_.auto_flush) {
      engine_->ForceFlush();
    }
  }
  
  void Close() {
    Flush();
    if (config_.show_progress) {
      std::cout << "\nTotal inserted: " << count_ << " entities" << std::endl;
    }
  }
  
  size_t Count() const { return count_; }
  
 private:
  LsmEngine* engine_;
  BatchInserterConfig config_;
  std::vector<std::pair<CedarKey, Descriptor>> buffer_;
  size_t count_ = 0;
  
  template<typename EntityType>
  EntityType CreateEntityFromParsed(const std::vector<std::pair<uint16_t, std::string>>& parsed) {
    EntityType entity(0);  // 临时 ID
    
    // 根据解析的字段设置属性
    // 这里需要根据 EntityType 的具体字段进行设置
    // 简化实现，实际应使用反射或模板特化
    
    return entity;
  }
};

// ============================================================================
// 便捷函数
// ============================================================================

// 从 CSV 导入节点
template<typename EntityType>
void ImportNodesFromCsv(LsmEngine* engine, 
                        const std::string& filepath,
                        const std::map<std::string, uint16_t>& column_mapping,
                        const BatchInserterConfig& config = BatchInserterConfig()) {
  BatchInserter inserter(engine, config);
  inserter.FromCsv<EntityType>(filepath, column_mapping);
  inserter.Close();
}

// 从 CSV 导入边
void ImportEdgesFromCsv(LsmEngine* engine,
                        const std::string& filepath,
                        const std::string& src_col,
                        const std::string& dst_col,
                        uint16_t edge_type,
                        const BatchInserterConfig& config = BatchInserterConfig()) {
  // 实现边导入逻辑
  std::ifstream file(filepath);
  std::string line;
  
  BatchInserter inserter(engine, config);
  
  while (std::getline(file, line)) {
    // 解析 src_id, dst_id
    // 创建边并插入
  }
  
  inserter.Close();
}

} // namespace api
} // namespace cedar

#endif // FERN_API_BATCH_INSERTER_H_
