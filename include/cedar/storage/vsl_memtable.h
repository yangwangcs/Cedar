// VSLMemTable - 使用 LockFreeVSL 的 MemTable 实现
// 完整替换 LockFreeMemTable + VersionChain
// 使用 Lock-Free CAS 操作实现真正的无锁并发

#ifndef FERN_VSL_MEMTABLE_H_
#define FERN_VSL_MEMTABLE_H_

#include "cedar/storage/versioned_skiplist_lockfree.h"
#include "cedar/storage/cedar_memtable.h"
#include "cedar/types/descriptor.h"
#include "cedar/types/cedar_key.h"
#include "cedar/core/status.h"
#include <atomic>

namespace cedar {

// VSLMemTable - 基于 LockFreeVSL 的 MemTable
class VSLMemTable {
 public:
  VSLMemTable();
  ~VSLMemTable();
  
  // 禁止拷贝
  VSLMemTable(const VSLMemTable&) = delete;
  VSLMemTable& operator=(const VSLMemTable&) = delete;
  
  // 基本操作
  Status Put(const CedarKey& key, const Descriptor& descriptor, Timestamp txn_version);
  
  Status Get(uint64_t entity_id,
             EntityType entity_type,
             uint16_t column_id,
             Timestamp timestamp,
             Descriptor* descriptor) const;
             
  std::optional<Descriptor> GetAtTime(uint64_t entity_id,
                                       EntityType entity_type,
                                       uint16_t column_id,
                                       Timestamp timestamp) const;
  
  // 批量查询
  std::vector<MemTableEntry> GetRange(uint64_t entity_id,
                                       EntityType entity_type,
                                       uint16_t column_id,
                                       Timestamp start,
                                       Timestamp end) const;
  
  // 获取最新版本
  std::optional<Descriptor> GetLatest(uint64_t entity_id,
                                       EntityType entity_type,
                                       uint16_t column_id) const;
  
  // 获取所有版本 (用于 flush)
  std::vector<MemTableEntry> GetAll(uint64_t entity_id,
                                     EntityType entity_type,
                                     uint16_t column_id) const;
  
  // 获取版本链
  std::vector<MemTableEntry> GetVersionChain(
      uint64_t entity_id,
      EntityType entity_type,
      uint16_t column_id) const;
  
  // 统计
  size_t ApproximateMemoryUsage() const;
  size_t ApproximateSize() const { return size_.load(); }
  bool IsEmpty() const { return size_.load() == 0; }
  bool Empty() const { return IsEmpty(); }
  bool IsFull() const;
  size_t size() const;
  
  // 遍历 (用于 flush 到 SST)
  void Traverse(std::function<bool(const CedarKey&, const Descriptor&)> callback) const;
  
  // 批量导入优化
  void SetBulkImportMode(bool enabled);
  
 private:
  std::unique_ptr<LockFreeVSL> skiplist_;
  std::atomic<size_t> size_{0};
  bool bulk_import_mode_ = false;
};

}  // namespace cedar

#endif  // FERN_VSL_MEMTABLE_H_
