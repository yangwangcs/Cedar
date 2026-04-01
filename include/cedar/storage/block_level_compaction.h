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
// Block 级增量 Compaction
// =============================================================================
// 特性：
// 1. 只合并重叠的 Block，非重叠 Block 直接引用
// 2. 大幅降低写放大（从 2-3x 降到 1.2-1.5x）
// 3. 需要维护 Block 级索引
// =============================================================================

#ifndef FERN_BLOCK_LEVEL_COMPACTION_H_
#define FERN_BLOCK_LEVEL_COMPACTION_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "cedar/core/status.h"
#include "cedar/storage/size_tiered_compaction.h"

namespace cedar {

// Block 范围信息
struct BlockRange {
  uint64_t offset;
  uint64_t size;
  uint64_t min_entity_id;
  uint64_t max_entity_id;
  uint64_t min_timestamp;
  uint64_t max_timestamp;
  uint64_t num_entries;
  
  // 检查是否与 key 范围重叠
  bool Overlaps(uint64_t min_e, uint64_t max_e, uint64_t min_ts, uint64_t max_ts) const {
    bool entity_overlap = !(max_entity_id < min_e || min_entity_id > max_e);
    bool ts_overlap = !(max_timestamp < min_ts || min_timestamp > max_ts);
    return entity_overlap && ts_overlap;
  }
};

// Block 级索引（存储在 SST footer 或单独文件）
struct BlockIndex {
  uint64_t file_number;
  std::vector<BlockRange> blocks;
  
  // 查找覆盖特定范围的 Blocks
  std::vector<size_t> FindOverlappingBlocks(
      uint64_t min_e, uint64_t max_e, uint64_t min_ts, uint64_t max_ts) const;
  
  // 序列化/反序列化
  Status EncodeTo(std::string* buf) const;
  Status DecodeFrom(const char* data, size_t size);
};

// Block 级 Compaction 任务
struct BlockCompactionTask {
  // 输入文件
  std::vector<ZoneSstMeta> input_files;
  
  // 需要合并的 Blocks（重叠部分）
  struct MergeRegion {
    uint64_t file_number;
    std::vector<size_t> block_indices;  // 该文件中需要合并的 block 索引
  };
  std::vector<MergeRegion> merge_regions;
  
  // 可以直接引用的 Blocks（非重叠）
  struct ReferenceRegion {
    uint64_t source_file_number;
    uint64_t block_offset;
    uint64_t block_size;
  };
  std::vector<ReferenceRegion> reference_regions;
  
  int output_level = 0;
};

// Block 级 Compaction 引擎
class BlockLevelCompactionEngine {
 public:
  struct Config {
    // 触发 Block 级合并的阈值：重叠比例超过此值才合并
    double overlap_threshold = 0.3;  // 30%
    
    // 最大 Block 大小（用于新产生的 Block）
    size_t max_block_size = 64 * 1024;  // 64KB
    
    // 是否启用引用计数（用于共享 Block）
    bool enable_block_refcount = true;
  };

  BlockLevelCompactionEngine(const Config& config) : config_(config) {}
  
  // 分析输入文件，生成 Block 级合并任务
  BlockCompactionTask AnalyzeTask(const std::vector<ZoneSstMeta>& inputs, int output_level);
  
  // 执行 Block 级合并
  Status ExecuteTask(const BlockCompactionTask& task, const std::string& output_path,
                     ZoneSstMeta* output_meta);
  
  // 计算写放大比率
  double CalculateWriteAmplification(const BlockCompactionTask& task) const;

 private:
  Config config_;
  
  // 加载文件的 Block 索引
  Status LoadBlockIndex(uint64_t file_number, BlockIndex* index);
  
  // 合并指定的 Blocks
  Status MergeBlocks(const std::vector<BlockCompactionTask::MergeRegion>& regions,
                     WritableFile* output, std::vector<BlockRange>* output_blocks);
  
  // 直接引用 Block（零拷贝）
  Status ReferenceBlocks(const std::vector<BlockCompactionTask::ReferenceRegion>& regions,
                         WritableFile* output, std::vector<BlockRange>* output_blocks);
};

// Block 引用计数管理器（用于共享 Blocks）
class BlockReferenceManager {
 public:
  void AddReference(uint64_t file_number, uint64_t block_offset);
  void RemoveReference(uint64_t file_number, uint64_t block_offset);
  int GetReferenceCount(uint64_t file_number, uint64_t block_offset) const;
  
  // 获取可以被清理的 Blocks
  std::vector<std::pair<uint64_t, uint64_t>> GetDereferencedBlocks() const;

 private:
  struct BlockRef {
    uint64_t file_number;
    uint64_t block_offset;
    
    bool operator==(const BlockRef& other) const {
      return file_number == other.file_number && block_offset == other.block_offset;
    }
  };
  
  struct BlockRefHash {
    size_t operator()(const BlockRef& ref) const {
      return std::hash<uint64_t>()(ref.file_number) ^ 
             (std::hash<uint64_t>()(ref.block_offset) << 1);
    }
  };
  
  mutable std::mutex mutex_;
  std::unordered_map<BlockRef, int, BlockRefHash> ref_counts_;
};

}  // namespace cedar

#endif  // FERN_BLOCK_LEVEL_COMPACTION_H_
