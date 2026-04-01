// VSLAdapter - 让 LsmEngine 可以使用 VersionedSkipList
#ifndef FERN_VSL_ADAPTER_H_
#define FERN_VSL_ADAPTER_H_

#include "cedar/storage/versioned_skiplist.h"
#include "cedar/storage/lockfree_memtable.h"

namespace cedar {

// 适配器模式: 将 VSL 包装成 MemTable 接口
class VSLMemTableAdapter {
 public:
  VSLMemTableAdapter() = default;
  
  Status Put(const CedarKey& key, const Descriptor& desc, Timestamp txn_version) {
    skiplist_.Insert(key, desc, txn_version);
    return Status::OK();
  }
  
  std::optional<Descriptor> GetAtTime(uint64_t entity_id,
                                       EntityType entity_type,
                                       uint16_t column_id,
                                       Timestamp timestamp) const {
    return skiplist_.GetAtTime(entity_id, entity_type, column_id, timestamp);
  }
  
  std::vector<MemTableEntry> GetRange(uint64_t entity_id,
                                       EntityType entity_type,
                                       uint16_t column_id,
                                       Timestamp start,
                                       Timestamp end) const {
    std::vector<MemTableEntry> results;
    auto versions = skiplist_.ScanRange(entity_id, entity_type, column_id, start, end);
    
    // 修复: 正确构造 MemTableEntry，包含 target_id 和 txn_version
    for (const auto& version : versions) {
      std::optional<uint64_t> dst_id = (version.target_id != 0) 
          ? std::optional<uint64_t>(version.target_id) 
          : std::nullopt;
      results.emplace_back(version.timestamp, version.descriptor, dst_id, Timestamp(0));
    }
    return results;
  }
  
  void Traverse(std::function<bool(const CedarKey&, const Descriptor&)> callback) const {
    skiplist_.Traverse(callback);
  }
  
  size_t ApproximateMemoryUsage() const {
    return skiplist_.ApproximateMemoryUsage();
  }
  
  bool IsEmpty() const { return skiplist_.size() == 0; }
  size_t size() const { return skiplist_.size(); }
  
  // 1GB threshold
  bool IsFull() const { return ApproximateMemoryUsage() >= 1024 * 1024 * 1024; }
  
 private:
  VersionedSkipList skiplist_;
};

}  // namespace cedar

#endif  // FERN_VSL_ADAPTER_H_
