#include "cedar/storage/vsl_memtable.h"

namespace cedar {

VSLMemTable::VSLMemTable() 
    : skiplist_(std::make_unique<LockFreeVSL>()) {}

VSLMemTable::~VSLMemTable() = default;

Status VSLMemTable::Put(const CedarKey& key, const Descriptor& descriptor, Timestamp txn_version) {
  // 修复: 传递 txn_version 给 LockFreeVSL
  bool inserted = skiplist_->Insert(key, descriptor, txn_version);
  if (inserted) {
    size_.fetch_add(1, std::memory_order_relaxed);
  }
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

std::vector<MemTableEntry> VSLMemTable::GetRange(uint64_t entity_id,
                                                  EntityType entity_type,
                                                  uint16_t column_id,
                                                  Timestamp start,
                                                  Timestamp end) const {
  std::vector<MemTableEntry> results;
  
  auto versions = skiplist_->ScanRange(entity_id, entity_type, column_id, start, end);
  
  // 修复: 正确处理包含 txn_version 和 target_id 的返回值
  for (const auto& version : versions) {
    std::optional<uint64_t> dst_id = (version.target_id != 0) 
        ? std::optional<uint64_t>(version.target_id) 
        : std::nullopt;
    // 使用显式构造避免 emplace_back 解析歧义
    results.push_back(MemTableEntry(version.timestamp, version.descriptor, dst_id, version.txn_version));
  }
  
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
  return GetRange(entity_id, entity_type, column_id, Timestamp(0), Timestamp(UINT64_MAX));
}

std::vector<MemTableEntry> VSLMemTable::GetVersionChain(
    uint64_t entity_id,
    EntityType entity_type,
    uint16_t column_id) const {
  std::vector<MemTableEntry> result;
  
  // 获取所有版本
  auto all = GetAll(entity_id, entity_type, column_id);
  for (const auto& entry : all) {
    result.push_back(entry);  // 保留完整的 MemTableEntry，包含 txn_version
  }
  
  return result;
}

size_t VSLMemTable::ApproximateMemoryUsage() const {
  return skiplist_->ApproximateMemoryUsage();
}

bool VSLMemTable::IsFull() const {
  // 64MB threshold for faster testing
  return ApproximateMemoryUsage() >= 64 * 1024 * 1024;
}

size_t VSLMemTable::size() const {
  return size_.load(std::memory_order_acquire);
}

void VSLMemTable::Traverse(
    std::function<bool(const CedarKey&, const Descriptor&)> callback) const {
  skiplist_->Traverse(callback);
}

void VSLMemTable::SetBulkImportMode(bool enabled) {
  bulk_import_mode_ = enabled;
}

}  // namespace cedar
