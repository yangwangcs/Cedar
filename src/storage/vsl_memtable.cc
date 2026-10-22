#include "cedar/storage/vsl_memtable.h"

#include <algorithm>

namespace cedar {

VSLMemTable::VSLMemTable()
    : skiplist_(std::make_unique<LockFreeVSL>()) {}

VSLMemTable::~VSLMemTable() = default;

Status VSLMemTable::Put(const CedarKey& key, const Descriptor& descriptor, Timestamp txn_version) {
  // 修复: 传递 txn_version 给 LockFreeVSL
  skiplist_->Insert(key, descriptor, txn_version);
  return Status::OK();
}

Status VSLMemTable::PutBatch(const std::vector<BatchEntry>& entries) {
  return PutBatch(entries.data(), entries.size());
}

Status VSLMemTable::PutBatch(const BatchEntry* entries, size_t count) {
  if (entries == nullptr || count == 0) {
    return Status::OK();
  }

  // 按跳表 key 序排序（Task 2.4 的“一次定位”前提）。
  std::vector<const VSLInsertEntry*> order;
  order.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    order.push_back(&entries[i]);
  }
  std::sort(order.begin(), order.end(), [](const VSLInsertEntry* a, const VSLInsertEntry* b) {
    return LockFreeVSL::CompareKeys(a->key, b->key) < 0;
  });

  skiplist_->InsertSorted(order.data(), order.size());
  return Status::OK();
}

Status VSLMemTable::Get(uint64_t entity_id,
                        EntityType entity_type,
                        uint16_t column_id,
                        Timestamp timestamp,
                        Descriptor* descriptor) const {
  auto result = skiplist_->GetAtTime(entity_id, entity_type, column_id, timestamp);
  if (result) {
    *descriptor = *result;
    return Status::OK();
  }
  return Status::NotFound("VSLMemTable", "key not found");
}

std::optional<Descriptor> VSLMemTable::GetAtTime(uint64_t entity_id,
                                                  EntityType entity_type,
                                                  uint16_t column_id,
                                                  Timestamp timestamp) const {
  return skiplist_->GetAtTime(entity_id, entity_type, column_id, timestamp);
}

// ===== Task 1.5：target 维度重载 =====

Status VSLMemTable::Get(uint64_t entity_id,
                        EntityType entity_type,
                        uint16_t column_id,
                        uint64_t target_id,
                        Timestamp timestamp,
                        Descriptor* descriptor) const {
  auto result =
      skiplist_->GetAtTime(entity_id, entity_type, column_id, target_id, timestamp);
  if (result) {
    *descriptor = *result;
    return Status::OK();
  }
  return Status::NotFound("VSLMemTable", "key not found (target)");
}

std::optional<Descriptor> VSLMemTable::GetAtTime(uint64_t entity_id,
                                                  EntityType entity_type,
                                                  uint16_t column_id,
                                                  uint64_t target_id,
                                                  Timestamp timestamp) const {
  return skiplist_->GetAtTime(entity_id, entity_type, column_id, target_id,
                              timestamp);
}

std::optional<Descriptor> VSLMemTable::GetLatest(uint64_t entity_id,
                                                  EntityType entity_type,
                                                  uint16_t column_id,
                                                  uint64_t target_id) const {
  return skiplist_->GetLatest(entity_id, entity_type, column_id, target_id);
}

std::vector<MemTableEntry> VSLMemTable::GetRange(uint64_t entity_id,
                                                  EntityType entity_type,
                                                  uint16_t column_id,
                                                  Timestamp start,
                                                  Timestamp end) const {
  // Task 2.3：直接消费版本链，只构造一层 MemTableEntry 向量
  //（原先 ScanRange→GetRange→GetAll→GetVersionChain 逐层复制）。
  std::vector<MemTableEntry> results;
  results.reserve(8);
  skiplist_->ForEachVersion(
      entity_id, entity_type, column_id, start, end,
      [&results](Timestamp ts, Timestamp txn_version, const Descriptor& descriptor,
                 uint64_t target_id) {
        std::optional<uint64_t> dst_id;
        if (target_id != 0) {
          dst_id = target_id;
        }
        results.push_back(MemTableEntry(ts, descriptor, dst_id, txn_version));
      });
  return results;
}

std::optional<Descriptor> VSLMemTable::GetLatest(uint64_t entity_id,
                                                  EntityType entity_type,
                                                  uint16_t column_id) const {
  return skiplist_->GetLatest(entity_id, entity_type, column_id);
}

std::vector<MemTableEntry> VSLMemTable::GetAll(uint64_t entity_id,
                                                EntityType entity_type,
                                                uint16_t column_id) const {
  // 获取所有版本 (从最新到最旧)
  return GetRange(entity_id, entity_type, column_id, Timestamp(0), Timestamp::Max());
}

std::vector<MemTableEntry> VSLMemTable::GetVersionChain(
    uint64_t entity_id,
    EntityType entity_type,
    uint16_t column_id) const {
  // Task 2.3：一层拷贝（不再经 GetAll→GetRange 复制）。
  return GetRange(entity_id, entity_type, column_id, Timestamp(0), Timestamp::Max());
}

std::vector<MemTableEntry> VSLMemTable::GetRange(uint64_t entity_id,
                                                  EntityType entity_type,
                                                  uint16_t column_id,
                                                  uint64_t target_id,
                                                  Timestamp start,
                                                  Timestamp end) const {
  std::vector<MemTableEntry> results;
  results.reserve(8);
  skiplist_->ForEachVersionForTarget(
      entity_id, entity_type, column_id, target_id, start, end,
      [&results](Timestamp ts, Timestamp txn_version, const Descriptor& descriptor,
                 uint64_t node_target) {
        results.push_back(MemTableEntry(
            ts, descriptor,
            node_target != 0 ? std::optional<uint64_t>(node_target) : std::nullopt,
            txn_version));
      });
  return results;
}

std::vector<MemTableEntry> VSLMemTable::GetVersionChain(
    uint64_t entity_id,
    EntityType entity_type,
    uint16_t column_id,
    uint64_t target_id) const {
  return GetRange(entity_id, entity_type, column_id, target_id, Timestamp(0),
                  Timestamp::Max());
}

std::vector<MemTableEntry> VSLMemTable::GetRangeAllTargets(
    uint64_t entity_id,
    EntityType entity_type,
    uint16_t column_id,
    Timestamp start,
    Timestamp end) const {
  // ForEachVersionAllTargets 的输出顺序是“按 target 分组、组内 timestamp 降序”；
  // 这里统一重排成 timestamp 降序 + dst 升序，与 LsmEngine 的排序/去重契约一致。
  std::vector<MemTableEntry> results;
  results.reserve(8);
  skiplist_->ForEachVersionAllTargets(
      entity_id, entity_type, column_id, start, end,
      [&results](Timestamp ts, Timestamp txn_version, const Descriptor& descriptor,
                 uint64_t target_id) {
        results.push_back(MemTableEntry(
            ts, descriptor,
            target_id != 0 ? std::optional<uint64_t>(target_id) : std::nullopt,
            txn_version));
      });
  std::stable_sort(results.begin(), results.end(),
                   [](const MemTableEntry& a, const MemTableEntry& b) {
                     if (a.timestamp.value() != b.timestamp.value()) {
                       return a.timestamp.value() > b.timestamp.value();
                     }
                     return a.dst_id.value_or(0) < b.dst_id.value_or(0);
                   });
  return results;
}

size_t VSLMemTable::ApproximateMemoryUsage() const {
  // Task 2.2：arena 已申请 slab 容量，O(1) 且单调（供 1.3 的自动切换使用）。
  return skiplist_->ApproximateMemoryUsage();
}

bool VSLMemTable::IsFull() const {
  // 64MB threshold for faster testing
  return ApproximateMemoryUsage() >= 64 * 1024 * 1024;
}

size_t VSLMemTable::size() const {
  // 单一计数权威：LockFreeVSL::size()（避免每次 Put 两次原子 RMW，审计 1.9）。
  return skiplist_->size();
}

void VSLMemTable::Traverse(
    const std::function<bool(const CedarKey&, const Descriptor&)>& callback) const {
  skiplist_->Traverse(callback);
}

void VSLMemTable::SetBulkImportMode(bool enabled) {
  bulk_import_mode_ = enabled;
}

}  // namespace cedar