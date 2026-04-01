// Copyright (c) 2025 The Cedar Authors. All rights reserved.
// Compaction Merger - K-Way Merge for Zone-Columnar SST files

#ifndef FERN_STORAGE_COMPACTION_MERGER_H_
#define FERN_STORAGE_COMPACTION_MERGER_H_

#include <memory>
#include <queue>
#include <vector>
#include <string>

#include "cedar/frond/zone_columnar_format.h"
#include "cedar/storage/size_tiered_compaction.h"
#include "cedar/types/cedar_key.h"
#include "cedar/types/descriptor.h"

namespace cedar {

// 归并堆项
struct MergeHeapItem {
  CedarKey key;
  Descriptor value;
  size_t source_idx;  // 来自哪个输入迭代器
  
  bool operator>(const MergeHeapItem& other) const {
    return key > other.key;  // 最小堆
  }
};

// Zone-Columnar Compaction 归并器
class CompactionMerger {
 public:
  // 输入：多个 SST 文件的 reader
  CompactionMerger(const std::vector<ZoneColumnarSstReader*>& readers,
                   uint8_t entity_type,
                   uint16_t column_id);
  
  ~CompactionMerger();
  
  // 执行归并，输出到新的 SST 文件
  // output_path: 输出文件路径
  // db_path: 用于 Blob 文件定位
  // 返回：输出文件的元数据，失败返回 nullptr
  std::unique_ptr<ZoneSstMeta> Run(const std::string& output_path,
                                   const std::string& db_path);
  
  // 获取统计信息
  struct Stats {
    size_t input_entries = 0;
    size_t output_entries = 0;
    size_t dropped_duplicates = 0;
    size_t dropped_tombstones = 0;
    double compression_ratio = 0.0;
  };
  
  Stats GetStats() const { return stats_; }

 private:
  void InitHeap();
  bool IsDuplicate(const CedarKey& a, const CedarKey& b) const;
  bool CanDropTombstone(const CedarKey& key) const;
  
  std::vector<std::unique_ptr<ZoneColumnarIterator>> iterators_;
  std::priority_queue<MergeHeapItem, std::vector<MergeHeapItem>, std::greater<>> heap_;
  
  uint8_t entity_type_;
  uint16_t column_id_;
  int output_level_ = 0;  // 输出层级（用于决定是否清理 tombstone）
  
  Stats stats_;
};

}  // namespace cedar

#endif  // FERN_STORAGE_COMPACTION_MERGER_H_
