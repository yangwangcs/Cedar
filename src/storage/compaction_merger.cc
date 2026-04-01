// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Compaction Merger implementation

#include "cedar/storage/compaction_merger.h"

#include <algorithm>
#include <numeric>

#include "cedar/frond/zone_columnar_format.h"
#include "cedar/core/env.h"

namespace cedar {

CompactionMerger::CompactionMerger(const std::vector<ZoneColumnarSstReader*>& readers,
                                   uint8_t entity_type,
                                   uint16_t column_id)
    : entity_type_(entity_type), column_id_(column_id) {
  
  // 为每个 reader 创建迭代器
  for (auto* reader : readers) {
    auto iter = std::make_unique<ZoneColumnarIterator>(reader);
    iter->SeekToFirst();
    iterators_.push_back(std::move(iter));
  }
  
  InitHeap();
}

CompactionMerger::~CompactionMerger() = default;

void CompactionMerger::InitHeap() {
  for (size_t i = 0; i < iterators_.size(); ++i) {
    if (iterators_[i]->Valid()) {
      MergeHeapItem item;
      item.key = iterators_[i]->Key();
      item.value = iterators_[i]->Value();
      item.source_idx = i;
      heap_.push(item);
      stats_.input_entries++;
    }
  }
}

bool CompactionMerger::IsDuplicate(const CedarKey& a, const CedarKey& b) const {
  // 判断是否是同一实体的同一版本
  // 考虑 entity_id, timestamp, target_id
  if (a.entity_id() != b.entity_id()) return false;
  if (a.timestamp().value() != b.timestamp().value()) return false;
  if (a.target_id() != b.target_id()) return false;
  return true;
}

bool CompactionMerger::CanDropTombstone(const CedarKey& key) const {
  // L0-L2: 保留 tombstone（可能有旧快照读）
  if (output_level_ < 3) {
    return false;
  }
  
  // L3+: 可以安全删除 tombstone（假设没有读快照依赖）
  // TODO: 检查是否有读快照依赖该版本
  return (key.flags() & 0x08) != 0;  // kTombstone flag
}

std::unique_ptr<ZoneSstMeta> CompactionMerger::Run(const std::string& output_path,
                                                   const std::string& db_path) {
  if (heap_.empty()) {
    return nullptr;
  }
  
  // 创建输出文件
  Env* env = Env::Default();
  WritableFile* file = nullptr;
  Status s = env->NewWritableFile(output_path, &file);
  if (!s.ok()) {
    return nullptr;
  }
  
  // 创建 builder
  CedarOptions options;
  ZoneColumnarSstBuilder builder(options, file, column_id_, db_path);
  
  // 归并循环
  CedarKey last_key;
  bool has_last_key = false;
  
  while (!heap_.empty()) {
    auto item = heap_.top();
    heap_.pop();
    
    // 去重：如果和上一个 key 相同，跳过（保留最新的）
    if (has_last_key && IsDuplicate(item.key, last_key)) {
      stats_.dropped_duplicates++;
      
      // 推进该迭代器
      size_t src_idx = item.source_idx;
      iterators_[src_idx]->Next();
      if (iterators_[src_idx]->Valid()) {
        MergeHeapItem new_item;
        new_item.key = iterators_[src_idx]->Key();
        new_item.value = iterators_[src_idx]->Value();
        new_item.source_idx = src_idx;
        heap_.push(new_item);
      }
      continue;
    }
    
    // 检查 tombstone
    if (CanDropTombstone(item.key)) {
      stats_.dropped_tombstones++;
      
      // 推进该迭代器
      size_t src_idx = item.source_idx;
      iterators_[src_idx]->Next();
      if (iterators_[src_idx]->Valid()) {
        MergeHeapItem new_item;
        new_item.key = iterators_[src_idx]->Key();
        new_item.value = iterators_[src_idx]->Value();
        new_item.source_idx = src_idx;
        heap_.push(new_item);
      }
      continue;
    }
    
    // 写入 builder
    builder.Add(item.key, item.value);
    stats_.output_entries++;
    
    last_key = item.key;
    has_last_key = true;
    
    // 推进该迭代器
    size_t src_idx = item.source_idx;
    iterators_[src_idx]->Next();
    if (iterators_[src_idx]->Valid()) {
      MergeHeapItem new_item;
      new_item.key = iterators_[src_idx]->Key();
      new_item.value = iterators_[src_idx]->Value();
      new_item.source_idx = src_idx;
      heap_.push(new_item);
    }
  }
  
  // 完成 builder
  s = builder.Finish();
  delete file;
  
  if (!s.ok()) {
    env->RemoveFile(output_path);
    return nullptr;
  }
  
  // 获取文件大小
  uint64_t file_size = 0;
  s = env->GetFileSize(output_path, &file_size);
  if (!s.ok()) {
    return nullptr;
  }
  
  // 创建元数据
  auto meta = std::make_unique<ZoneSstMeta>();
  meta->file_number = 0;  // 由调用者设置
  meta->file_size = file_size;
  meta->num_entries = stats_.output_entries;
  meta->level = output_level_;
  meta->column_id = column_id_;
  meta->entity_type = entity_type_;
  meta->path = output_path;
  
  // 计算压缩率
  if (stats_.input_entries > 0) {
    // 简化计算，实际应该比较字节数
    stats_.compression_ratio = static_cast<double>(stats_.output_entries) / 
                               static_cast<double>(stats_.input_entries);
  }
  
  return meta;
}

}  // namespace cedar
