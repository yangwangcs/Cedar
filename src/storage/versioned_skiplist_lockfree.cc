// Lock-Free Versioned SkipList 实现
#include "cedar/storage/versioned_skiplist_lockfree.h"

namespace cedar {

// ═══════════════════════════════════════════════════════════════════
// LFNode 实现
// ═══════════════════════════════════════════════════════════════════

LFNode::LFNode(const CedarKey& key, const Descriptor& desc, int height, Timestamp txn_version)
    : entity_id_(key.entity_id()), 
      timestamp_(key.timestamp().value()), 
      txn_version_(txn_version.value()),
      target_id_(key.target_id()),
      descriptor_(desc), 
      column_id_(key.column_id()), 
      sequence_(key.sequence()),
      entity_type_(static_cast<uint8_t>(key.entity_type())),
      flags_(key.flags()),
      height_(height),
      older_version_(nullptr), 
      newer_version_(nullptr) {
  for (int i = 0; i < height; i++) {
    next_[i].store(nullptr, std::memory_order_relaxed);
  }
}

CedarKey LFNode::GetKey() const {
  EntityType type = static_cast<EntityType>(entity_type_);
  if (type == EntityType::EdgeOut) {
    return CedarKey::EdgeOut(entity_id_, target_id_, EdgeTypeId(column_id_), 
                            Timestamp(timestamp_), sequence_, flags_);
  } else if (type == EntityType::EdgeIn) {
    return CedarKey::EdgeIn(entity_id_, target_id_, EdgeTypeId(column_id_), 
                           Timestamp(timestamp_), sequence_, flags_);
  } else {
    // Vertex
    return CedarKey::Vertex(entity_id_, VertexColumnId(column_id_), 
                           Timestamp(timestamp_), sequence_, target_id_, flags_);
  }
}

LFNode::~LFNode() = default;

LFNode* LFNode::Next(int level) const {
  return next_[level].load(std::memory_order_acquire);
}

void LFNode::SetNext(int level, LFNode* next) {
  next_[level].store(next, std::memory_order_release);
}

bool LFNode::CASNext(int level, LFNode* expected, LFNode* desired) {
  return next_[level].compare_exchange_strong(expected, desired, 
                                               std::memory_order_acq_rel);
}

LFNode* LFNode::OlderVersion() const {
  return older_version_.load(std::memory_order_acquire);
}

void LFNode::SetOlderVersion(LFNode* node) {
  older_version_.store(node, std::memory_order_release);
}

bool LFNode::MarkDeleted() { return true; }
bool LFNode::IsMarked() const { return false; }

// ═══════════════════════════════════════════════════════════════════
// LockFreeVSL 实现
// ═══════════════════════════════════════════════════════════════════

LockFreeVSL::LockFreeVSL() : max_height_(1), size_(0), rnd_(0) {
  // Use dummy keys for head and tail
  CedarKey head_key = CedarKey::Vertex(0, 0_vcol, Timestamp(UINT64_MAX));
  CedarKey tail_key = CedarKey::Vertex(UINT64_MAX, 0_vcol, Timestamp(0));
  
  head_ = new LFNode(head_key, Descriptor(), kMaxHeight, Timestamp(0));
  tail_ = new LFNode(tail_key, Descriptor(), kMaxHeight, Timestamp(0));
  
  for (int i = 0; i < kMaxHeight; i++) {
    head_->SetNext(i, tail_);
  }
}

LockFreeVSL::~LockFreeVSL() {
  LFNode* node = head_->Next(0);
  while (node != tail_) {
    LFNode* next = node->Next(0);
    delete node;
    node = next;
  }
  delete head_;
  delete tail_;
}

// 单线程 Insert
bool LockFreeVSL::Insert(const CedarKey& key, const Descriptor& value, Timestamp txn_version) {
  int height = RandomHeight();
  LFNode* new_node = new LFNode(key, value, height, txn_version);
  
  LFNode* preds[kMaxHeight];
  LFNode* succs[kMaxHeight];
  
  // 查找插入位置
  LFNode* pred = head_;
  int curr_max = max_height_.load(std::memory_order_acquire);
  
  for (int level = curr_max - 1; level >= 0; level--) {
    LFNode* curr = pred->Next(level);
    while (curr != tail_) {
      // Compare: entity_id asc, column_id asc, target_id asc, timestamp DESC
      // 注意：timestamp 降序，这样最新版本排在前面，与 FindLatestVersion 一致
      if (curr->entity_id() < key.entity_id() ||
          (curr->entity_id() == key.entity_id() && curr->column_id() < key.column_id()) ||
          (curr->entity_id() == key.entity_id() && curr->column_id() == key.column_id() 
           && curr->target_id() < key.target_id()) ||
          (curr->entity_id() == key.entity_id() && curr->column_id() == key.column_id() 
           && curr->target_id() == key.target_id()
           && curr->timestamp() > key.timestamp().value())) {
        pred = curr;
        curr = pred->Next(level);
      } else {
        break;
      }
    }
    preds[level] = pred;
    succs[level] = curr;
  }
  
  // 填充高层的前驱和后继
  for (int i = curr_max; i < height; i++) {
    preds[i] = head_;
    succs[i] = tail_;
  }
  
  // 设置新节点的 next 指针
  for (int i = 0; i < height; i++) {
    new_node->SetNext(i, succs[i]);
  }
  
  // 链接到 SkipList
  for (int i = 0; i < height; i++) {
    preds[i]->SetNext(i, new_node);
  }
  
  // 链接版本链 - 使用 succs[0] 作为旧版本（需要 entity_id, column_id, target_id, entity_type 都匹配）
  if (succs[0] != tail_ && 
      succs[0]->entity_id() == key.entity_id() &&
      succs[0]->column_id() == key.column_id() &&
      succs[0]->target_id() == key.target_id() &&
      succs[0]->entity_type() == key.entity_type()) {
    new_node->SetOlderVersion(succs[0]);
  }
  
  size_.fetch_add(1, std::memory_order_acq_rel);
  
  // 更新 max_height
  int old_max = max_height_.load(std::memory_order_relaxed);
  if (height > old_max) {
    max_height_.compare_exchange_strong(old_max, height, std::memory_order_relaxed);
  }
  
  return true;
}

// 优化的 SkipList 查找
LFNode* LockFreeVSL::FindNode(const CedarKey& key, LFNode* preds[], LFNode* succs[]) {
  LFNode* pred = head_;
  int curr_max = max_height_.load(std::memory_order_acquire);
  
  for (int level = curr_max - 1; level >= 0; level--) {
    LFNode* curr = pred->Next(level);
    while (curr != tail_) {
      // Compare: entity_id asc, column_id asc, target_id asc, timestamp desc
      if (curr->entity_id() < key.entity_id() ||
          (curr->entity_id() == key.entity_id() && curr->column_id() < key.column_id()) ||
          (curr->entity_id() == key.entity_id() && curr->column_id() == key.column_id() 
           && curr->target_id() < key.target_id()) ||
          (curr->entity_id() == key.entity_id() && curr->column_id() == key.column_id() 
           && curr->target_id() == key.target_id()
           && curr->timestamp() > key.timestamp().value())) {
        pred = curr;
        curr = pred->Next(level);
      } else {
        break;
      }
    }
    preds[level] = pred;
    succs[level] = curr;
  }
  
  if (succs[0] != tail_ && succs[0]->entity_id() == key.entity_id() && 
      succs[0]->column_id() == key.column_id() &&
      succs[0]->target_id() == key.target_id() &&
      succs[0]->timestamp() == key.timestamp().value()) {
    return succs[0];
  }
  
  return nullptr;
}

// 优化的 FindLatestVersion - 使用 SkipList 层级搜索
LFNode* LockFreeVSL::FindLatestVersion(uint64_t entity_id, 
                                        EntityType entity_type, 
                                        uint16_t column_id) const {
  // 使用 SkipList 层级快速定位
  LFNode* pred = head_;
  int curr_max = max_height_.load(std::memory_order_acquire);
  
  for (int level = curr_max - 1; level >= 0; level--) {
    LFNode* curr = pred->Next(level);
    while (curr != tail_ && curr->entity_id() < entity_id) {
      pred = curr;
      curr = pred->Next(level);
    }
  }
  
  // 现在在 level 0，pred->next[0] 是第一个 entity_id >= target 的节点
  LFNode* result = pred->Next(0);
  
  // 检查是否匹配 - 包括 entity_type
  // 注意：此版本不检查 target_id，会返回第一个匹配的 (entity_id, column_id, entity_type)
  // 用于向后兼容，但 EdgeOut/EdgeIn 查询应该使用带 target_id 的版本
  while (result != tail_ && result->entity_id() == entity_id) {
    if (result->column_id() == column_id && 
        result->entity_type() == entity_type) {
      return result;
    }
    result = result->Next(0);
  }
  
  return nullptr;
}

// 带 target_id 的 FindLatestVersion - 用于 EdgeOut/EdgeIn
LFNode* LockFreeVSL::FindLatestVersion(uint64_t entity_id, 
                                        EntityType entity_type, 
                                        uint16_t column_id,
                                        uint64_t target_id) const {
  // 使用 SkipList 层级快速定位
  LFNode* pred = head_;
  int curr_max = max_height_.load(std::memory_order_acquire);
  
  for (int level = curr_max - 1; level >= 0; level--) {
    LFNode* curr = pred->Next(level);
    while (curr != tail_ && curr->entity_id() < entity_id) {
      pred = curr;
      curr = pred->Next(level);
    }
  }
  
  // 现在在 level 0，pred->next[0] 是第一个 entity_id >= target 的节点
  LFNode* result = pred->Next(0);
  
  // 检查是否匹配 - 包括 entity_type 和 target_id
  while (result != tail_ && result->entity_id() == entity_id) {
    if (result->column_id() == column_id && 
        result->target_id() == target_id &&
        result->entity_type() == entity_type) {
      return result;
    }
    result = result->Next(0);
  }
  
  return nullptr;
}

std::optional<Descriptor> LockFreeVSL::GetAtTime(
    uint64_t entity_id, EntityType entity_type, uint16_t column_id, 
    Timestamp timestamp) const {
  LFNode* node = FindLatestVersion(entity_id, entity_type, column_id);
  
  if (node == nullptr) {
    return std::nullopt;
  }
  
  // 沿版本链回溯
  while (node != nullptr && node->timestamp() > timestamp.value()) {
    node = node->OlderVersion();
  }
  
  if (node != nullptr) {
    return node->descriptor();
  }
  
  return std::nullopt;
}

// 带 target_id 的 GetAtTime - 用于 EdgeOut/EdgeIn
std::optional<Descriptor> LockFreeVSL::GetAtTime(
    uint64_t entity_id, EntityType entity_type, uint16_t column_id,
    uint64_t target_id, Timestamp timestamp) const {
  LFNode* node = FindLatestVersion(entity_id, entity_type, column_id, target_id);
  
  if (node == nullptr) {
    return std::nullopt;
  }
  
  // 沿版本链回溯
  while (node != nullptr && node->timestamp() > timestamp.value()) {
    node = node->OlderVersion();
  }
  
  if (node != nullptr) {
    return node->descriptor();
  }
  
  return std::nullopt;
}

std::optional<Descriptor> LockFreeVSL::GetLatest(
    uint64_t entity_id, EntityType entity_type, uint16_t column_id) const {
  LFNode* node = FindLatestVersion(entity_id, entity_type, column_id);
  if (node != nullptr) {
    return node->descriptor();
  }
  return std::nullopt;
}

// 带 target_id 的 GetLatest - 用于 EdgeOut/EdgeIn
std::optional<Descriptor> LockFreeVSL::GetLatest(
    uint64_t entity_id, EntityType entity_type, uint16_t column_id,
    uint64_t target_id) const {
  LFNode* node = FindLatestVersion(entity_id, entity_type, column_id, target_id);
  if (node != nullptr) {
    return node->descriptor();
  }
  return std::nullopt;
}

std::vector<LockFreeVSL::VersionInfo> LockFreeVSL::ScanRange(
    uint64_t entity_id, EntityType entity_type, uint16_t column_id,
    Timestamp start, Timestamp end) const {
  std::vector<VersionInfo> results;
  
  LFNode* node = FindLatestVersion(entity_id, entity_type, column_id);
  
  if (node == nullptr) {
    return results;
  }
  
  while (node != nullptr && node->timestamp() > end.value()) {
    node = node->OlderVersion();
  }
  
  while (node != nullptr && node->timestamp() >= start.value()) {
    // 修复: 同时返回业务时间戳、事务版本号和target_id
    results.emplace_back(Timestamp(node->timestamp()), 
                          Timestamp(node->txn_version()), 
                          node->descriptor(),
                          node->target_id());
    node = node->OlderVersion();
  }
  
  return results;
}

void LockFreeVSL::Traverse(
    std::function<bool(const CedarKey&, const Descriptor&)> callback) const {
  LFNode* node = head_->Next(0);
  
  while (node != tail_) {
    CedarKey key = node->GetKey();  // Use the full key reconstruction
    if (!callback(key, node->descriptor())) {
      break;
    }
    node = node->Next(0);
  }
}

size_t LockFreeVSL::ApproximateMemoryUsage() const {
  size_t total = sizeof(*this);
  total += size_.load(std::memory_order_acquire) * sizeof(LFNode);
  return total;
}

int LockFreeVSL::RandomHeight() {
  int height = 1;
  uint32_t rnd = rnd_.fetch_add(1, std::memory_order_relaxed);
  
  while (height < kMaxHeight && (rnd & 3) == 0) {
    height++;
    rnd >>= 2;
  }
  
  return height;
}

}  // namespace cedar
