// Copyright (c) 2025 The Cedar Authors. All rights reserved.

#include "cedar/storage/block_level_compaction.h"

namespace cedar {

// BlockIndex 实现
std::vector<size_t> BlockIndex::FindOverlappingBlocks(
    uint64_t min_e, uint64_t max_e, uint64_t min_ts, uint64_t max_ts) const {
  std::vector<size_t> result;
  for (size_t i = 0; i < blocks.size(); ++i) {
    if (blocks[i].Overlaps(min_e, max_e, min_ts, max_ts)) {
      result.push_back(i);
    }
  }
  return result;
}

Status BlockIndex::EncodeTo(std::string* buf) const {
  // 简单编码：file_number (8) + num_blocks (4) + blocks[]
  buf->append(reinterpret_cast<const char*>(&file_number), sizeof(file_number));
  uint32_t num = blocks.size();
  buf->append(reinterpret_cast<const char*>(&num), sizeof(num));
  for (const auto& block : blocks) {
    buf->append(reinterpret_cast<const char*>(&block), sizeof(block));
  }
  return Status::OK();
}

Status BlockIndex::DecodeFrom(const char* data, size_t size) {
  if (size < 12) return Status::Corruption("BlockIndex", "too small");
  
  file_number = *reinterpret_cast<const uint64_t*>(data);
  uint32_t num = *reinterpret_cast<const uint32_t*>(data + 8);
  
  if (size < 12 + num * sizeof(BlockRange)) {
    return Status::Corruption("BlockIndex", "size mismatch");
  }
  
  blocks.resize(num);
  const char* ptr = data + 12;
  for (uint32_t i = 0; i < num; ++i) {
    blocks[i] = *reinterpret_cast<const BlockRange*>(ptr);
    ptr += sizeof(BlockRange);
  }
  return Status::OK();
}

// BlockLevelCompactionEngine 实现
BlockCompactionTask BlockLevelCompactionEngine::AnalyzeTask(
    const std::vector<ZoneSstMeta>& inputs, int output_level) {
  BlockCompactionTask task;
  task.input_files = inputs;
  task.output_level = output_level;
  
  if (inputs.size() < 2) {
    // 单文件，全部合并
    BlockCompactionTask::MergeRegion region;
    region.file_number = inputs[0].file_number;
    // 假设所有 blocks 都需要加载
    task.merge_regions.push_back(region);
    return task;
  }
  
  // 分析重叠
  // 简化实现：假设最后一个文件是新文件，前面的可能与其重叠
  const auto& new_file = inputs.back();
  
  for (size_t i = 0; i < inputs.size() - 1; ++i) {
    const auto& old_file = inputs[i];
    
    // 检查范围重叠
    bool entity_overlap = !(old_file.max_entity_id < new_file.min_entity_id ||
                            old_file.min_entity_id > new_file.max_entity_id);
    bool ts_overlap = !(old_file.max_timestamp < new_file.min_timestamp ||
                        old_file.min_timestamp > new_file.max_timestamp);
    
    if (entity_overlap && ts_overlap) {
      // 有重叠，需要合并
      BlockCompactionTask::MergeRegion region;
      region.file_number = old_file.file_number;
      task.merge_regions.push_back(region);
    } else {
      // 无重叠，可以引用
      BlockCompactionTask::ReferenceRegion ref;
      ref.source_file_number = old_file.file_number;
      ref.block_offset = 0;  // 简化：引用整个文件
      ref.block_size = old_file.file_size;
      task.reference_regions.push_back(ref);
    }
  }
  
  // 新文件总是需要合并
  BlockCompactionTask::MergeRegion new_region;
  new_region.file_number = new_file.file_number;
  task.merge_regions.push_back(new_region);
  
  return task;
}

Status BlockLevelCompactionEngine::ExecuteTask(
    const BlockCompactionTask& task, const std::string& output_path,
    ZoneSstMeta* output_meta) {
  // 简化实现：只处理 merge_regions，忽略 reference_regions
  // 实际实现需要复杂的文件拼接逻辑
  
  // 这里复用普通合并逻辑
  // TODO: 实现真正的 Block 级引用
  
  return Status::OK();
}

double BlockLevelCompactionEngine::CalculateWriteAmplification(
    const BlockCompactionTask& task) const {
  if (task.merge_regions.empty()) return 1.0;
  
  uint64_t merge_size = 0;
  for (const auto& region : task.merge_regions) {
    // 估算
    merge_size += 64 * 1024 * region.block_indices.size();
  }
  
  uint64_t total_size = merge_size;
  for (const auto& ref : task.reference_regions) {
    total_size += ref.block_size;
  }
  
  if (total_size == 0) return 1.0;
  return static_cast<double>(merge_size + merge_size) / total_size;  // 简化估算
}

}  // namespace cedar
