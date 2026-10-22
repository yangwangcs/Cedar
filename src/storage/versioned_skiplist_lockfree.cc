// Lock-Free Versioned SkipList 实现（Task 2.1-2.3）
#include "cedar/storage/versioned_skiplist_lockfree.h"

#include <algorithm>
#include <limits>
#include <type_traits>

namespace cedar {

namespace {

// 排序用的 timestamp 值：max - timestamp。
//   - CedarKey 里 timestamp 以“降序存储”的大端编码保存，
//     cedar_be64toh(timestamp_be) 正好等于 max - timestamp；
//   - LFNode 里保存的是解码后的 timestamp，取反即 max - timestamp。
// 两者都按该值升序比较 = 业务 timestamp 降序（Task 2.3：复用 timestamp_be）。
inline uint64_t TimestampOrderValue(const CedarKey& key) {
  return cedar_be64toh(key.timestamp_be());
}

inline uint64_t TimestampOrderValue(const LFNode* node) {
  return ~node->timestamp();
}

int CompareNodeToKey(const LFNode* node, const CedarKey& key) {
  const uint64_t node_entity = node->entity_id();
  const uint64_t key_entity = key.entity_id();
  if (node_entity != key_entity) {
    return node_entity < key_entity ? -1 : 1;
  }
  const uint8_t node_type = node->entity_type_value();
  const uint8_t key_type = static_cast<uint8_t>(key.entity_type());
  if (node_type != key_type) {
    return node_type < key_type ? -1 : 1;
  }
  if (node->column_id() != key.column_id()) {
    return node->column_id() < key.column_id() ? -1 : 1;
  }
  if (node->target_id() != key.target_id()) {
    return node->target_id() < key.target_id() ? -1 : 1;
  }
  const uint64_t node_ts = TimestampOrderValue(node);
  const uint64_t key_ts = TimestampOrderValue(key);
  if (node_ts != key_ts) {
    return node_ts < key_ts ? -1 : 1;
  }
  if (node->sequence() != key.sequence()) {
    return node->sequence() < key.sequence() ? -1 : 1;
  }
  if (node->flags() != key.flags()) {
    return node->flags() < key.flags() ? -1 : 1;
  }
  return 0;
}

int CompareNodeToNode(const LFNode* a, const LFNode* b) {
  if (a->entity_id() != b->entity_id()) {
    return a->entity_id() < b->entity_id() ? -1 : 1;
  }
  if (a->entity_type_value() != b->entity_type_value()) {
    return a->entity_type_value() < b->entity_type_value() ? -1 : 1;
  }
  if (a->column_id() != b->column_id()) {
    return a->column_id() < b->column_id() ? -1 : 1;
  }
  if (a->target_id() != b->target_id()) {
    return a->target_id() < b->target_id() ? -1 : 1;
  }
  const uint64_t a_ts = TimestampOrderValue(a);
  const uint64_t b_ts = TimestampOrderValue(b);
  if (a_ts != b_ts) {
    return a_ts < b_ts ? -1 : 1;
  }
  if (a->sequence() != b->sequence()) {
    return a->sequence() < b->sequence() ? -1 : 1;
  }
  if (a->flags() != b->flags()) {
    return a->flags() < b->flags() ? -1 : 1;
  }
  return 0;
}

}  // namespace

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
  const EntityType type = static_cast<EntityType>(entity_type_);
  if (type == EntityType::EdgeOut) {
    return CedarKey::EdgeOut(entity_id_, target_id_, EdgeTypeId(column_id_),
                             Timestamp(timestamp_), sequence_, flags_);
  }
  if (type == EntityType::EdgeIn) {
    return CedarKey::EdgeIn(entity_id_, target_id_, EdgeTypeId(column_id_),
                            Timestamp(timestamp_), sequence_, flags_);
  }
  return CedarKey::Vertex(entity_id_, VertexColumnId(column_id_), Timestamp(timestamp_),
                          sequence_, target_id_, flags_);
}

int LFNode::CompareTo(const CedarKey& key) const { return CompareNodeToKey(this, key); }

bool LFNode::SameUserKey(const CedarKey& key) const {
  return entity_id_ == key.entity_id() &&
         entity_type_ == static_cast<uint8_t>(key.entity_type()) &&
         column_id_ == key.column_id() && target_id_ == key.target_id();
}

bool LFNode::SameUserKey(const LFNode& other) const {
  return entity_id_ == other.entity_id_ && entity_type_ == other.entity_type_ &&
         column_id_ == other.column_id_ && target_id_ == other.target_id_;
}

LFNode* LFNode::Next(int level) const {
  return next_[level].load(std::memory_order_acquire);
}

void LFNode::SetNext(int level, LFNode* next) {
  // 发布语义由紧随其后的 CASNext(release) 提供：读者先 acquire 到本节点，
  // 再读 next_[level] 时一定能看到这里写入的值。
  next_[level].store(next, std::memory_order_relaxed);
}

bool LFNode::CASNext(int level, LFNode* expected, LFNode* desired) {
  // release 发布本节点及其 next/older 字段；失败时不需要 acquire（调用方会
  // 重新定位并重读所有指针）。
  return next_[level].compare_exchange_strong(expected, desired,
                                              std::memory_order_release,
                                              std::memory_order_relaxed);
}

LFNode* LFNode::OlderVersion() const {
  return older_version_.load(std::memory_order_acquire);
}

LFNode* LFNode::NewerVersion() const {
  return newer_version_.load(std::memory_order_acquire);
}

void LFNode::SetOlderVersion(LFNode* node) {
  // older 在节点发布前写入，由 level-0 CAS 的 release 语义发布。
  older_version_.store(node, std::memory_order_relaxed);
}

void LFNode::SetNewerVersion(LFNode* node) {
  newer_version_.store(node, std::memory_order_relaxed);
}

bool LFNode::CASOlderVersion(LFNode* expected, LFNode* desired) {
  return older_version_.compare_exchange_strong(expected, desired,
                                                std::memory_order_release,
                                                std::memory_order_relaxed);
}

bool LFNode::CASNewerVersion(LFNode* expected, LFNode* desired) {
  // newer 为辅助指针，没有读者依赖其同步语义。
  return newer_version_.compare_exchange_strong(expected, desired,
                                                std::memory_order_relaxed,
                                                std::memory_order_relaxed);
}

bool LFNode::MarkDeleted() { return true; }
bool LFNode::IsMarked() const { return false; }

// ═══════════════════════════════════════════════════════════════════
// LockFreeVSL 实现
// ═══════════════════════════════════════════════════════════════════

static_assert(std::is_trivially_destructible<LFNode>::value,
              "LFNode must be trivially destructible: MemTableArena releases slabs "
              "without running per-node destructors");

LockFreeVSL::LockFreeVSL()
    : arena_(), head_(nullptr), tail_(nullptr), max_height_(1), size_(0) {
  // head/tail 为哨兵节点：head 排在所有 key 之前，tail 排在所有 key 之后。
  const CedarKey head_key = CedarKey::Vertex(0, VertexColumnId(0), Timestamp::Max());
  const CedarKey tail_key = CedarKey::Vertex(std::numeric_limits<uint64_t>::max(),
                                              VertexColumnId(0), Timestamp(0));
  head_ = arena_.New<LFNode>(head_key, Descriptor(), kMaxHeight, Timestamp(0));
  tail_ = arena_.New<LFNode>(tail_key, Descriptor(), kMaxHeight, Timestamp(0));
  for (int i = 0; i < kMaxHeight; ++i) {
    head_->SetNext(i, tail_);
  }
}

LockFreeVSL::~LockFreeVSL() = default;  // arena 整块释放全部节点（D6）

int LockFreeVSL::CompareKeys(const CedarKey& a, const CedarKey& b) {
  if (a.entity_id() != b.entity_id()) {
    return a.entity_id() < b.entity_id() ? -1 : 1;
  }
  const uint8_t a_type = static_cast<uint8_t>(a.entity_type());
  const uint8_t b_type = static_cast<uint8_t>(b.entity_type());
  if (a_type != b_type) {
    return a_type < b_type ? -1 : 1;
  }
  if (a.column_id() != b.column_id()) {
    return a.column_id() < b.column_id() ? -1 : 1;
  }
  if (a.target_id() != b.target_id()) {
    return a.target_id() < b.target_id() ? -1 : 1;
  }
  const uint64_t a_ts = TimestampOrderValue(a);
  const uint64_t b_ts = TimestampOrderValue(b);
  if (a_ts != b_ts) {
    return a_ts < b_ts ? -1 : 1;
  }
  if (a.sequence() != b.sequence()) {
    return a.sequence() < b.sequence() ? -1 : 1;
  }
  if (a.flags() != b.flags()) {
    return a.flags() < b.flags() ? -1 : 1;
  }
  return 0;
}

int LockFreeVSL::CompareNodes(const LFNode* a, const LFNode* b) {
  return CompareNodeToNode(a, b);
}

// ───────────────────────────────────────────────────────────────────
// 定位
// ───────────────────────────────────────────────────────────────────

void LockFreeVSL::FindSplice(const CedarKey& key, LFNode* preds[],
                             LFNode* succs[]) const {
  // 只下降 max_height_ 以内的层：不变式是“任何已链接节点的 height <=
  // max_height_”（发布 level-0 后、链接高层前先更新 max_height_）。
  int top = max_height_.load(std::memory_order_acquire);
  if (top > kMaxHeight) {
    top = kMaxHeight;
  }
  if (top < 1) {
    top = 1;
  }
  for (int level = top; level < kMaxHeight; ++level) {
    preds[level] = head_;
    succs[level] = tail_;
  }
  LFNode* pred = head_;
  for (int level = top - 1; level >= 0; --level) {
    LFNode* curr = pred->Next(level);
    while (curr != tail_ && CompareNodeToKey(curr, key) < 0) {
      pred = curr;
      curr = pred->Next(level);
    }
    preds[level] = pred;
    succs[level] = curr;
  }
}

void LockFreeVSL::AdvanceSplice(const CedarKey& key, LFNode* preds[],
                                LFNode* succs[]) const {
  // 批量写快路径：上一次定位的 preds[level] 一定 < 当前 key（输入非递减），
  // 因此从该点继续前进即可，无需从 head 重新下降。高层 preds 可能是 head
  // （上次定位时 max_height_ 还较低），从 head 前进仍然是正确的。
  for (int level = kMaxHeight - 1; level >= 0; --level) {
    LFNode* pred = preds[level];
    LFNode* curr = pred->Next(level);
    while (curr != tail_ && CompareNodeToKey(curr, key) < 0) {
      pred = curr;
      curr = pred->Next(level);
    }
    preds[level] = pred;
    succs[level] = curr;
  }
}

LFNode* LockFreeVSL::PublishNode(const CedarKey& key, LFNode* node, int height,
                                 LFNode* preds[], LFNode* succs[],
                                 bool* have_hint) {
  // 1) level-0 CAS 发布：next/older 必须先填好（审计 1.1/1.2）。
  LFNode* linked_pred = nullptr;
  for (;;) {
    if (*have_hint) {
      // 冲突重试：preds 里的节点不会被删除，且 key(pred) < key，因此从旧
      // 前驱沿 next 前进即可得到新的 splice（比每次从 head 重新下降便宜）。
      AdvanceSplice(key, preds, succs);
    } else {
      FindSplice(key, preds, succs);
      *have_hint = true;
    }
    if (succs[0] != tail_ && succs[0]->SameUserKey(key)) {
      node->SetOlderVersion(succs[0]);
    }
    node->SetNext(0, succs[0]);
    if (preds[0]->CASNext(0, succs[0], node)) {
      linked_pred = preds[0];
      break;
    }
    // 前驱/后继已被并发修改：保留 splice 作为下一轮的前进起点。
  }

  // 2) 先更新 max_height_（使“已链接节点 height <= max_height_”对并发读者
  //    成立），再逐层补齐 level>=1。level-0 已保证节点可达，高层链接失败
  //    只需重定位后重试（不会重复发布）。
  UpdateMaxHeight(height);
  int linked_level = 0;
  for (;;) {
    bool all_linked = true;
    for (int level = linked_level + 1; level < height; ++level) {
      node->SetNext(level, succs[level]);
      if (preds[level]->CASNext(level, succs[level], node)) {
        linked_level = level;
      } else {
        all_linked = false;
        break;
      }
    }
    if (all_linked) {
      break;
    }
    // 高层冲突：同样从当前 splice 前进重试，不回到 head。
    AdvanceSplice(key, preds, succs);
  }
  *have_hint = true;
  return linked_pred;
}

// ───────────────────────────────────────────────────────────────────
// 版本链
// ───────────────────────────────────────────────────────────────────

void LockFreeVSL::LinkOlder(LFNode* pred, LFNode* node) {
  // 并发插入时，pred 的 older 可能已被更靠近 pred 的版本占用；只有 node
  // 比当前 older 更靠近 pred（即 node 更新）时才 CAS 回链（审计 1.2）。
  for (;;) {
    LFNode* cur = pred->OlderVersion();
    if (cur == node) {
      return;
    }
    if (cur != nullptr) {
      if (!cur->SameUserKey(*node)) {
        return;  // 链上出现其他 user key：保守放弃，读路径仍可从 level-0 找到
      }
      if (CompareNodes(node, cur) >= 0) {
        return;  // node 更旧：已有更新的版本占据该位置
      }
    }
    if (pred->CASOlderVersion(cur, node)) {
      // newer 为辅助指针（当前无读者依赖），CAS 设置避免覆盖并发修复结果。
      node->CASNewerVersion(nullptr, pred);
      if (cur != nullptr) {
        cur->CASNewerVersion(pred, node);
      }
      return;
    }
  }
}

// ───────────────────────────────────────────────────────────────────
// 插入
// ───────────────────────────────────────────────────────────────────

bool LockFreeVSL::Insert(const CedarKey& key, const Descriptor& value,
                         Timestamp txn_version) {
  const int height = ThreadLevelHeight();
  LFNode* node = arena_.New<LFNode>(key, value, height, txn_version);

  LFNode* preds[kMaxHeight];
  LFNode* succs[kMaxHeight];
  bool have_hint = false;
  LFNode* linked_pred = PublishNode(key, node, height, preds, succs, &have_hint);

  if (linked_pred != nullptr && linked_pred != head_ && linked_pred->SameUserKey(key)) {
    LinkOlder(linked_pred, node);
  }

  size_.fetch_add(1, std::memory_order_relaxed);
  return true;
}

void LockFreeVSL::InsertSorted(const VSLInsertEntry* const* entries, size_t count) {
  if (entries == nullptr || count == 0) {
    return;
  }
  LFNode* preds[kMaxHeight];
  LFNode* succs[kMaxHeight];
  bool have_hint = false;

  for (size_t i = 0; i < count; ++i) {
    const VSLInsertEntry& entry = *entries[i];
    const int height = ThreadLevelHeight();
    LFNode* node = arena_.New<LFNode>(entry.key, entry.descriptor, height,
                                     entry.txn_version);

    LFNode* linked_pred = PublishNode(entry.key, node, height, preds, succs, &have_hint);
    if (linked_pred != nullptr && linked_pred != head_ && linked_pred->SameUserKey(entry.key)) {
      LinkOlder(linked_pred, node);
    }

    size_.fetch_add(1, std::memory_order_relaxed);
  }
}

// ───────────────────────────────────────────────────────────────────
// 查询
// ───────────────────────────────────────────────────────────────────

LFNode* LockFreeVSL::FindVersionHead(uint64_t entity_id, EntityType entity_type,
                                     uint16_t column_id, uint64_t target_id,
                                     bool match_target) const {
  const uint8_t type = static_cast<uint8_t>(entity_type);
  int top = max_height_.load(std::memory_order_acquire);
  if (top > kMaxHeight) {
    top = kMaxHeight;
  }
  LFNode* pred = head_;
  for (int level = top - 1; level >= 0; --level) {
    LFNode* curr = pred->Next(level);
    while (curr != tail_) {
      bool less = false;
      if (curr->entity_id() != entity_id) {
        less = curr->entity_id() < entity_id;
      } else if (curr->entity_type_value() != type) {
        less = curr->entity_type_value() < type;
      } else if (curr->column_id() != column_id) {
        less = curr->column_id() < column_id;
      } else if (match_target && curr->target_id() != target_id) {
        less = curr->target_id() < target_id;
      }
      if (!less) {
        break;
      }
      pred = curr;
      curr = pred->Next(level);
    }
  }

  LFNode* node = pred->Next(0);
  if (node == tail_) {
    return nullptr;
  }
  if (node->entity_id() != entity_id || node->entity_type_value() != type ||
      node->column_id() != column_id) {
    return nullptr;
  }
  if (match_target && node->target_id() != target_id) {
    return nullptr;
  }
  return node;
}

std::optional<Descriptor> LockFreeVSL::GetAtTime(uint64_t entity_id,
                                                 EntityType entity_type,
                                                 uint16_t column_id,
                                                 Timestamp timestamp) const {
  LFNode* node = FindVersionHead(entity_id, entity_type, column_id, 0, false);
  while (node != nullptr && node->timestamp() > timestamp.value()) {
    node = node->OlderVersion();
  }
  if (node != nullptr) {
    return node->descriptor();
  }
  return std::nullopt;
}

std::optional<Descriptor> LockFreeVSL::GetAtTime(uint64_t entity_id,
                                                 EntityType entity_type,
                                                 uint16_t column_id,
                                                 uint64_t target_id,
                                                 Timestamp timestamp) const {
  LFNode* node = FindVersionHead(entity_id, entity_type, column_id, target_id, true);
  while (node != nullptr && node->timestamp() > timestamp.value()) {
    node = node->OlderVersion();
  }
  if (node != nullptr) {
    return node->descriptor();
  }
  return std::nullopt;
}

std::optional<Descriptor> LockFreeVSL::GetLatest(uint64_t entity_id,
                                                 EntityType entity_type,
                                                 uint16_t column_id) const {
  LFNode* node = FindVersionHead(entity_id, entity_type, column_id, 0, false);
  if (node != nullptr) {
    return node->descriptor();
  }
  return std::nullopt;
}

std::optional<Descriptor> LockFreeVSL::GetLatest(uint64_t entity_id,
                                                 EntityType entity_type,
                                                 uint16_t column_id,
                                                 uint64_t target_id) const {
  LFNode* node = FindVersionHead(entity_id, entity_type, column_id, target_id, true);
  if (node != nullptr) {
    return node->descriptor();
  }
  return std::nullopt;
}

std::vector<LockFreeVSL::VersionInfo> LockFreeVSL::ScanRange(
    uint64_t entity_id, EntityType entity_type, uint16_t column_id,
    Timestamp start, Timestamp end) const {
  std::vector<VersionInfo> results;
  results.reserve(8);
  ForEachVersion(entity_id, entity_type, column_id, start, end,
                 [&results](Timestamp ts, Timestamp txn_version,
                            const Descriptor& descriptor, uint64_t target_id) {
                   results.emplace_back(ts, txn_version, descriptor, target_id);
                 });
  return results;
}

void LockFreeVSL::Traverse(
    const std::function<bool(const CedarKey&, const Descriptor&)>& callback) const {
  Traverse([&callback](const CedarKey& key, const Descriptor& descriptor) {
    return callback(key, descriptor);
  });
}

size_t LockFreeVSL::ApproximateMemoryUsage() const {
  return sizeof(*this) + arena_.capacity_bytes();
}

int LockFreeVSL::ThreadLevelHeight() {
  // Task 2.1：高度分配改为每线程状态，去掉共享 rnd_ 计数器的缓存行争用，
  // 并修复“第一个插入节点必为 16 层”的退化。
  //
  // 高度序列使用 thread_local PRNG 生成随机相位 + 低差异（确定性 4 进制）
  // 几何分布：每个线程的第 i 个插入高度 = 1 + v_4(i + phase)。这样既保留
  // 1/4 的均匀层间隔（点查 level-0 步数约为纯随机采样的 1/2），又避免多个
  // 写线程相位对齐。纯 per-insert 随机采样会把 GetAtTime 的 level-0 步数从
  // ~13 提到 ~21+（实测点查 -50%，见 benchmarks/RESULTS.md）。
  static thread_local uint64_t position = 0;
  static thread_local uint64_t phase = 0;
  if (phase == 0) {
    static std::atomic<uint64_t> seed_counter{0x9E3779B97F4A7C15ULL};
    uint64_t seed = seed_counter.fetch_add(0x9E3779B97F4A7C15ULL,
                                           std::memory_order_relaxed) ^
                    static_cast<uint64_t>(reinterpret_cast<uintptr_t>(&phase));
    // splitmix64
    seed += 0x9E3779B97F4A7C15ULL;
    seed = (seed ^ (seed >> 30)) * 0xBF58476D1CE4E5B9ULL;
    seed = (seed ^ (seed >> 27)) * 0x94D049BB133111EBULL;
    seed ^= seed >> 31;
    phase = seed | 1;  // 随机相位（奇数与非奇数均可，仅用于错开线程）
  }

  uint64_t value = position++ + phase;
  int height = 1;
  while (height < kMaxHeight && (value & (kBranching - 1)) == 0) {
    ++height;
    value >>= 2;
  }
  return height;
}

void LockFreeVSL::UpdateMaxHeight(int height) {
  // 只在超过当前最大高度时做 CAS；绝大多数插入高度 <= max_height，走 load 快路径。
  int old = max_height_.load(std::memory_order_acquire);
  while (height > old) {
    if (max_height_.compare_exchange_weak(old, height, std::memory_order_release,
                                          std::memory_order_acquire)) {
      return;
    }
  }
}

}  // namespace cedar