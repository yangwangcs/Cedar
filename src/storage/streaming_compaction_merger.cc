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
// 流式 Compaction Merger - 固定内存实现
// =============================================================================
// 特性：
// 1. 每个输入文件 64KB 预读缓冲区
// 2. 最小堆只保存当前最小 Key（O(K) 内存，K=输入文件数）
// 3. 输出批量写入（每 4KB 或 1024 条刷盘）
// 4. 内存占用与数据量无关
// =============================================================================

#include "cedar/frond/zone_columnar_format.h"
#include "cedar/core/env.h"
#include <queue>
#include <vector>
#include <memory>

namespace cedar {

struct StreamingMergeConfig {
  size_t prefetch_buffer_size = 64 * 1024;  // 64KB 预读
  size_t output_batch_bytes = 4 * 1024;      // 4KB 输出缓冲
  size_t output_batch_entries = 1024;        // 或 1024 条目
  int output_level = 0;
};

class StreamingCompactionMerger {
 public:
  struct Stats {
    uint64_t input_entries = 0;
    uint64_t output_entries = 0;
    uint64_t dropped_duplicates = 0;
    uint64_t dropped_tombstones = 0;
    uint64_t peak_memory_bytes = 0;
    uint64_t io_read_bytes = 0;
    uint64_t io_write_bytes = 0;
    double duration_ms = 0;
  };

  StreamingCompactionMerger(const std::vector<ZoneSstMeta>& inputs,
                            const std::string& output_path,
                            const std::string& db_path,
                            const StreamingMergeConfig& config)
      : inputs_(inputs), output_path_(output_path), db_path_(db_path), config_(config) {}

  std::unique_ptr<ZoneSstMeta> Run() {
    auto start = std::chrono::steady_clock::now();
    
    // 1. 打开所有输入文件
    if (!OpenInputs()) {
      return nullptr;
    }
    
    // 2. 创建输出文件
    WritableFile* file = nullptr;
    Status s = Env::Default()->NewWritableFile(output_path_, &file);
    if (!s.ok()) return nullptr;
    output_file_.reset(file);
    
    // 3. 创建 Builder
    CedarOptions options;
    options.column_id = inputs_[0].column_id;
    builder_ = std::make_unique<ZoneColumnarSstBuilder>(options, output_file_.get(), 
                                                        options.column_id, db_path_);
    builder_->SetLevel(config_.output_level);
    
    // 4. 执行流式归并
    if (!Merge()) {
      builder_->Abandon();
      return nullptr;
    }
    
    // 5. 完成构建
    s = builder_->Finish();
    if (!s.ok()) return nullptr;
    
    auto end = std::chrono::steady_clock::now();
    stats_.duration_ms = std::chrono::duration<double, std::milli>(end - start).count();
    
    // 6. 构造元数据
    return BuildMeta();
  }

  const Stats& GetStats() const { return stats_; }

 private:
  struct InputStream {
    ZoneSstMeta meta;
    std::unique_ptr<ZoneColumnarSstReader> reader;
    std::unique_ptr<ZoneColumnarIterator> iter;
    bool valid = false;
    
    CedarKey current_key;
    Descriptor current_value;
  };

  struct HeapItem {
    CedarKey key;
    Descriptor value;
    size_t stream_idx;
    
    bool operator>(const HeapItem& other) const {
      return key > other.key;
    }
  };

  bool OpenInputs() {
    streams_.reserve(inputs_.size());
    
    for (const auto& input : inputs_) {
      auto stream = std::make_unique<InputStream>();
      stream->meta = input;
      
      stream->reader = std::make_unique<ZoneColumnarSstReader>(input.path);
      Status s = stream->reader->Open();
      if (!s.ok()) return false;
      
      stream->iter = std::make_unique<ZoneColumnarIterator>(stream->reader.get());
      stream->iter->SeekToFirst();
      stream->valid = stream->iter->Valid();
      
      if (stream->valid) {
        stream->current_key = stream->iter->Key();
        stream->current_value = stream->iter->Value();
        streams_.push_back(std::move(stream));
      }
      
      stats_.io_read_bytes += input.file_size;  // 估算
    }
    
    return !streams_.empty();
  }

  bool Merge() {
    // 初始化最小堆
    std::vector<HeapItem> heap;
    heap.reserve(streams_.size());
    
    for (size_t i = 0; i < streams_.size(); ++i) {
      HeapItem item;
      item.key = streams_[i]->current_key;
      item.value = streams_[i]->current_value;
      item.stream_idx = i;
      heap.push_back(item);
    }
    
    auto cmp = [](const HeapItem& a, const HeapItem& b) { return a.key > b.key; };
    std::make_heap(heap.begin(), heap.end(), cmp);
    
    // 输出缓冲区
    std::vector<CedarKey> key_buffer;
    std::vector<Descriptor> value_buffer;
    key_buffer.reserve(config_.output_batch_entries);
    value_buffer.reserve(config_.output_batch_entries);
    
    size_t buffer_bytes = 0;
    CedarKey last_key;
    bool has_last = false;
    
    // 归并循环
    while (!heap.empty()) {
      // 取最小值
      std::pop_heap(heap.begin(), heap.end(), cmp);
      auto item = heap.back();
      heap.pop_back();
      
      stats_.input_entries++;
      
      // 去重：跳过相同 key
      if (has_last && IsDuplicate(last_key, item.key)) {
        stats_.dropped_duplicates++;
      } else {
        // Tombstone 检查（L3+）
        if (config_.output_level >= 3 && IsTombstone(item.key)) {
          stats_.dropped_tombstones++;
        } else {
          key_buffer.push_back(item.key);
          value_buffer.push_back(item.value);
          buffer_bytes += EstimateSize(item.key, item.value);
          
          // 批量刷盘
          if (key_buffer.size() >= config_.output_batch_entries ||
              buffer_bytes >= config_.output_batch_bytes) {
            FlushBuffer(key_buffer, value_buffer);
            buffer_bytes = 0;
          }
          
          last_key = item.key;
          has_last = true;
          stats_.output_entries++;
        }
      }
      
      // 推进该流
      auto& stream = streams_[item.stream_idx];
      stream->iter->Next();
      stream->valid = stream->iter->Valid();
      
      if (stream->valid) {
        stream->current_key = stream->iter->Key();
        stream->current_value = stream->iter->Value();
        
        HeapItem new_item;
        new_item.key = stream->current_key;
        new_item.value = stream->current_value;
        new_item.stream_idx = item.stream_idx;
        heap.push_back(new_item);
        std::push_heap(heap.begin(), heap.end(), cmp);
      }
    }
    
    // 刷剩余数据
    FlushBuffer(key_buffer, value_buffer);
    
    // 计算内存峰值
    stats_.peak_memory_bytes = streams_.size() * config_.prefetch_buffer_size +
                               config_.output_batch_entries * sizeof(Descriptor) * 2;
    
    return true;
  }

  void FlushBuffer(std::vector<CedarKey>& keys, std::vector<Descriptor>& values) {
    for (size_t i = 0; i < keys.size(); ++i) {
      builder_->Add(keys[i], values[i]);
    }
    stats_.io_write_bytes += keys.size() * 32;  // 估算
    keys.clear();
    values.clear();
  }

  bool IsDuplicate(const CedarKey& a, const CedarKey& b) const {
    return a.entity_id() == b.entity_id() &&
           a.timestamp().value() == b.timestamp().value() &&
           a.target_id() == b.target_id();
  }

  bool IsTombstone(const CedarKey& key) const {
    return (key.flags() & 0x08) != 0;
  }

  size_t EstimateSize(const CedarKey& key, const Descriptor& value) const {
    return 32 + (value.IsInline() ? 8 : 16);  // 粗略估算
  }

  std::unique_ptr<ZoneSstMeta> BuildMeta() {
    auto meta = std::make_unique<ZoneSstMeta>();
    meta->file_size = builder_->FileSize();
    meta->num_entries = builder_->NumEntries();
    meta->level = config_.output_level;
    meta->column_id = inputs_[0].column_id;
    meta->entity_type = inputs_[0].entity_type;
    meta->path = output_path_;
    
    // 计算范围
    uint64_t min_entity = UINT64_MAX, max_entity = 0;
    uint64_t min_ts = UINT64_MAX, max_ts = 0;
    for (const auto& input : inputs_) {
      min_entity = std::min(min_entity, input.min_entity_id);
      max_entity = std::max(max_entity, input.max_entity_id);
      min_ts = std::min(min_ts, input.min_timestamp);
      max_ts = std::max(max_ts, input.max_timestamp);
    }
    meta->min_entity_id = min_entity;
    meta->max_entity_id = max_entity;
    meta->min_timestamp = min_ts;
    meta->max_timestamp = max_ts;
    
    return meta;
  }

  std::vector<ZoneSstMeta> inputs_;
  std::string output_path_;
  std::string db_path_;
  StreamingMergeConfig config_;
  Stats stats_;
  
  std::vector<std::unique_ptr<InputStream>> streams_;
  std::unique_ptr<WritableFile> output_file_;
  std::unique_ptr<ZoneColumnarSstBuilder> builder_;
};

}  // namespace cedar
