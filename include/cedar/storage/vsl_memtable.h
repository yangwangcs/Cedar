// VSLMemTable - 使用 LockFreeVSL 的 MemTable 实现
// 完整替换 LockFreeMemTable + VersionChain
// 使用 Lock-Free CAS 操作实现真正的无锁并发（Task 2.1-2.4）

#ifndef FERN_VSL_MEMTABLE_H_
#define FERN_VSL_MEMTABLE_H_

#include <atomic>
#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "cedar/storage/versioned_skiplist_lockfree.h"
#include "cedar/storage/cedar_memtable.h"
#include "cedar/types/descriptor.h"
#include "cedar/types/cedar_key.h"
#include "cedar/core/status.h"

namespace cedar {

// VSLMemTable - 基于 LockFreeVSL 的 MemTable
class VSLMemTable {
 public:
  // 批量写条目（Task 2.4），与 LockFreeVSL::Insert 的三元组一致。
  using BatchEntry = VSLInsertEntry;

  VSLMemTable();
  ~VSLMemTable();

  // 禁止拷贝
  VSLMemTable(const VSLMemTable&) = delete;
  VSLMemTable& operator=(const VSLMemTable&) = delete;

  // 基本操作
  Status Put(const CedarKey& key, const Descriptor& descriptor, Timestamp txn_version);

  // 批量写（Task 2.4）：内部按跳表 key 序排序后共享一次层级定位，
  // 每个节点一次 level-0 CAS；语义与逐条 Put 完全一致。
  Status PutBatch(const std::vector<BatchEntry>& entries);
  Status PutBatch(const BatchEntry* entries, size_t count);

  Status Get(uint64_t entity_id,
             EntityType entity_type,
             uint16_t column_id,
             Timestamp timestamp,
             Descriptor* descriptor) const;

  std::optional<Descriptor> GetAtTime(uint64_t entity_id,
                                       EntityType entity_type,
                                       uint16_t column_id,
                                       Timestamp timestamp) const;

  // ===== Task 1.5：target 维度的重载（审计 1.5/2.4/8.2）=====
  // 旧签名保持既有语义（Vertex 行为，不区分 target）；边必须走这些重载，
  // 否则同 (entity,type,column) 不同 dst 的边会互相串读。
  Status Get(uint64_t entity_id,
             EntityType entity_type,
             uint16_t column_id,
             uint64_t target_id,
             Timestamp timestamp,
             Descriptor* descriptor) const;

  std::optional<Descriptor> GetAtTime(uint64_t entity_id,
                                       EntityType entity_type,
                                       uint16_t column_id,
                                       uint64_t target_id,
                                       Timestamp timestamp) const;

  std::optional<Descriptor> GetLatest(uint64_t entity_id,
                                       EntityType entity_type,
                                       uint16_t column_id,
                                       uint64_t target_id) const;

  // 指定 target 的版本范围/版本链（最新 → 最旧）
  std::vector<MemTableEntry> GetRange(uint64_t entity_id,
                                       EntityType entity_type,
                                       uint16_t column_id,
                                       uint64_t target_id,
                                       Timestamp start,
                                       Timestamp end) const;
  std::vector<MemTableEntry> GetVersionChain(
      uint64_t entity_id,
      EntityType entity_type,
      uint16_t column_id,
      uint64_t target_id) const;

  // 遍历同一 (entity,type,column) 下**所有 target** 的版本（Task 1.5）。
  // 供 LsmEngine::GetAll/GetRange/GetRangeLimit 这类“所有边/所有版本”语义的
  // API 使用；旧的无 target 签名保持既有语义（Vertex / 首个 target 的链）。
  std::vector<MemTableEntry> GetRangeAllTargets(uint64_t entity_id,
                                                EntityType entity_type,
                                                uint16_t column_id,
                                                Timestamp start,
                                                Timestamp end) const;

  // 批量查询（timestamp 降序，Task 2.3：单层向量，无中间拷贝）
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

  // 获取版本链（最新 → 最旧）
  std::vector<MemTableEntry> GetVersionChain(
      uint64_t entity_id,
      EntityType entity_type,
      uint16_t column_id) const;

  // 统计
  size_t ApproximateMemoryUsage() const;
  size_t ApproximateSize() const { return size(); }
  bool IsEmpty() const { return size() == 0; }
  bool Empty() const { return IsEmpty(); }
  bool IsFull() const;
  size_t size() const;

  // 遍历 (用于 flush 到 SST)
  void Traverse(const std::function<bool(const CedarKey&, const Descriptor&)>& callback) const;

  // Task 2.3：模板快路径，去掉 std::function 的间接调用与堆分配。
  template <typename Fn>
  void Traverse(Fn&& callback) const {
    skiplist_->Traverse(std::forward<Fn>(callback));
  }

  // 批量导入优化
  void SetBulkImportMode(bool enabled);

  // 底层跳表（测试/统计用）
  const LockFreeVSL& skiplist() const { return *skiplist_; }

 private:
  std::unique_ptr<LockFreeVSL> skiplist_;
  bool bulk_import_mode_ = false;
};

}  // namespace cedar

#endif  // FERN_VSL_MEMTABLE_H_