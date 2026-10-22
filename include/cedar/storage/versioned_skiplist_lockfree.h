// Lock-Free Versioned SkipList
// 使用原子操作实现真正的无锁并发（Task 2.1-2.3）
//
// 排序契约（与版本链语义一致，审计 1.3）：
//   entity_id 升序 → entity_type 升序 → column_id 升序 → target_id 升序 →
//   timestamp 降序 → sequence 升序 → flags 升序
// 同一 user key（entity/type/column/target）的所有版本在 level-0 上连续，
// older 指针按 timestamp 降序把整条版本链串起来。

#ifndef FERN_VERSIONED_SKIPLIST_LOCKFREE_H_
#define FERN_VERSIONED_SKIPLIST_LOCKFREE_H_

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <vector>

#include "cedar/storage/memtable_arena.h"
#include "cedar/types/descriptor.h"
#include "cedar/types/cedar_key.h"

namespace cedar {

// Lock-Free VSL 节点
//
// 所有节点由 LockFreeVSL 的 MemTableArena 分配（Task 2.2），节点不可单独
// 回收：memtable 析构时 arena 整块释放（设计决策 D6，避免 ABA）。
class LFNode {
 public:
  LFNode(const CedarKey& key, const Descriptor& desc, int height, Timestamp txn_version);
  ~LFNode() = default;

  // 获取信息
  uint64_t entity_id() const { return entity_id_; }
  uint64_t timestamp() const { return timestamp_; }
  uint64_t txn_version() const { return txn_version_; }
  uint16_t column_id() const { return column_id_; }
  EntityType entity_type() const { return static_cast<EntityType>(entity_type_); }
  uint8_t entity_type_value() const { return entity_type_; }
  uint64_t target_id() const { return target_id_; }
  uint32_t sequence() const { return sequence_; }
  uint8_t flags() const { return flags_; }
  int height() const { return height_; }

  // 重建完整的 CedarKey（只在真正需要 key 的路径调用：Traverse/批量写）
  CedarKey GetKey() const;

  const Descriptor& descriptor() const { return descriptor_; }
  Descriptor& descriptor() { return descriptor_; }

  // 排序契约（Task 2.1）：与 LockFreeVSL::CompareKeys 完全一致。
  int CompareTo(const CedarKey& key) const;
  bool SameUserKey(const CedarKey& key) const;
  bool SameUserKey(const LFNode& other) const;

  // SkipList 指针操作 (原子)
  LFNode* Next(int level) const;
  void SetNext(int level, LFNode* next);
  bool CASNext(int level, LFNode* expected, LFNode* desired);

  // 版本链指针 (原子)
  LFNode* OlderVersion() const;
  LFNode* NewerVersion() const;
  void SetOlderVersion(LFNode* node);
  void SetNewerVersion(LFNode* node);
  bool CASOlderVersion(LFNode* expected, LFNode* desired);
  bool CASNewerVersion(LFNode* expected, LFNode* desired);

  // 标记删除（VSL 暂不支持原地删除；逐节点回收由 memtable 生命周期负责）
  bool MarkDeleted();
  bool IsMarked() const;

 private:
  uint64_t entity_id_;
  uint64_t timestamp_;        // 业务时间戳 (用于 Key 排序和时序查询)
  uint64_t txn_version_;      // 事务版本号 (用于 MVCC 隔离)
  uint64_t target_id_;        // target_id for EdgeOut/EdgeIn / Vertex extension
  Descriptor descriptor_;
  uint16_t column_id_;
  uint32_t sequence_;
  uint8_t entity_type_;       // EntityType enum value
  uint8_t flags_;
  int height_;

  // 固定大小数组避免内存对齐问题；只有 level < height_ 的槽位会被使用。
  std::atomic<LFNode*> next_[16];
  std::atomic<LFNode*> older_version_;
  std::atomic<LFNode*> newer_version_;
};

// 批量写条目（Task 2.4）：WAL replay / 批量导入使用。
struct VSLInsertEntry {
  CedarKey key;
  Descriptor descriptor;
  Timestamp txn_version;

  VSLInsertEntry() = default;
  VSLInsertEntry(const CedarKey& k, const Descriptor& d, Timestamp txn)
      : key(k), descriptor(d), txn_version(txn) {}
};

// Lock-Free Versioned SkipList
class LockFreeVSL {
 public:
  static constexpr int kMaxHeight = 16;
  static constexpr int kBranching = 4;

  LockFreeVSL();
  ~LockFreeVSL();

  LockFreeVSL(const LockFreeVSL&) = delete;
  LockFreeVSL& operator=(const LockFreeVSL&) = delete;

  // 核心操作 (Lock-Free CAS 插入，Task 2.1)
  bool Insert(const CedarKey& key, const Descriptor& value, Timestamp txn_version);

  // 批量写（Task 2.4）：entries 必须已按 CompareKeys 升序排列（允许相等 key）。
  // 整批共享一次层级定位（hint 前进），每个节点一次 level-0 CAS。
  void InsertSorted(const VSLInsertEntry* const* entries, size_t count);

  // 跳表排序契约（Task 2.1）。PutBatch 排序、测试和批量写共用。
  static int CompareKeys(const CedarKey& a, const CedarKey& b);

  // 基本查询（不带 target_id，用于 Vertex 类型）
  std::optional<Descriptor> GetAtTime(uint64_t entity_id,
                                       EntityType entity_type,
                                       uint16_t column_id,
                                       Timestamp timestamp) const;
  std::optional<Descriptor> GetLatest(uint64_t entity_id,
                                       EntityType entity_type,
                                       uint16_t column_id) const;

  // 带 target_id 的查询（用于 EdgeOut/EdgeIn 类型）
  std::optional<Descriptor> GetAtTime(uint64_t entity_id,
                                       EntityType entity_type,
                                       uint16_t column_id,
                                       uint64_t target_id,
                                       Timestamp timestamp) const;
  std::optional<Descriptor> GetLatest(uint64_t entity_id,
                                       EntityType entity_type,
                                       uint16_t column_id,
                                       uint64_t target_id) const;

  // 批量查询 - 返回完整的版本信息（包含 txn_version）
  struct VersionInfo {
    Timestamp timestamp;      // 业务时间戳
    Timestamp txn_version;    // 事务版本号
    Descriptor descriptor;
    uint64_t target_id;       // 目标ID（用于边）

    VersionInfo(Timestamp ts, Timestamp txn_ver, const Descriptor& desc, uint64_t tgt = 0)
        : timestamp(ts), txn_version(txn_ver), descriptor(desc), target_id(tgt) {}
  };

  std::vector<VersionInfo> ScanRange(
      uint64_t entity_id,
      EntityType entity_type,
      uint16_t column_id,
      Timestamp start,
      Timestamp end) const;

  // 版本链遍历（最新 → 最旧），回调直接消费，避免中间 vector（Task 2.3）。
  // fn 签名：fn(Timestamp timestamp, Timestamp txn_version,
  //             const Descriptor& descriptor, uint64_t target_id)
  template <typename Fn>
  void ForEachVersion(uint64_t entity_id,
                      EntityType entity_type,
                      uint16_t column_id,
                      Timestamp start,
                      Timestamp end,
                      Fn&& fn) const {
    LFNode* node = FindVersionHead(entity_id, entity_type, column_id,
                                   /*target_id=*/0, /*match_target=*/false);
    while (node != nullptr && node->timestamp() > end.value()) {
      node = node->OlderVersion();
    }
    while (node != nullptr && node->timestamp() >= start.value()) {
      fn(Timestamp(node->timestamp()), Timestamp(node->txn_version()),
         node->descriptor(), node->target_id());
      node = node->OlderVersion();
    }
  }

  // 指定 target 的版本链遍历（Task 1.5 新增，供 VSLMemTable 的 target 重载使用）。
  // 与 ForEachVersion 相同的回调签名；match_target=true，只走同一
  // (entity, type, column, target) 的版本链。
  template <typename Fn>
  void ForEachVersionForTarget(uint64_t entity_id,
                               EntityType entity_type,
                               uint16_t column_id,
                               uint64_t target_id,
                               Timestamp start,
                               Timestamp end,
                               Fn&& fn) const {
    LFNode* node = FindVersionHead(entity_id, entity_type, column_id, target_id,
                                   /*match_target=*/true);
    while (node != nullptr && node->timestamp() > end.value()) {
      node = node->OlderVersion();
    }
    while (node != nullptr && node->timestamp() >= start.value()) {
      fn(Timestamp(node->timestamp()), Timestamp(node->txn_version()),
         node->descriptor(), node->target_id());
      node = node->OlderVersion();
    }
  }

  // 遍历同一 (entity, type, column) 下所有 target 的版本（Task 1.5 新增）。
  // 这些节点在 level-0 上按完整 key 序连续（target 升序 → timestamp 降序），
  // 供 "所有版本 / 所有边" 语义的 GetRange/GetAll 使用。回调签名同 ForEachVersion。
  template <typename Fn>
  void ForEachVersionAllTargets(uint64_t entity_id,
                                EntityType entity_type,
                                uint16_t column_id,
                                Timestamp start,
                                Timestamp end,
                                Fn&& fn) const {
    const uint8_t type = static_cast<uint8_t>(entity_type);
    LFNode* node = FindVersionHead(entity_id, entity_type, column_id, 0,
                                   /*match_target=*/false);
    while (node != nullptr && node->entity_id() == entity_id &&
           node->entity_type_value() == type && node->column_id() == column_id) {
      const uint64_t ts = node->timestamp();
      if (ts <= end.value() && ts >= start.value()) {
        fn(Timestamp(ts), Timestamp(node->txn_version()), node->descriptor(),
           node->target_id());
      }
      node = node->Next(0);
    }
  }

  // 统计
  size_t size() const { return size_.load(std::memory_order_acquire); }
  size_t ApproximateMemoryUsage() const;

  // 遍历（Task 2.3：模板快路径去掉 std::function 间接调用；
  // std::function 重载保留以兼容既有 API/调用方）。
  void Traverse(const std::function<bool(const CedarKey&, const Descriptor&)>& callback) const;

  template <typename Fn>
  void Traverse(Fn&& callback) const {
    LFNode* node = head_->Next(0);
    CedarKey key;
    while (node != tail_) {
      key = node->GetKey();
      if (!callback(key, node->descriptor())) {
        break;
      }
      node = node->Next(0);
    }
  }

  // arena 统计（Task 2.2 验收：slab 数 / 容量，O(1)）
  const MemTableArena& arena() const { return arena_; }

 private:
  // 把已构造节点发布到跳表：level-0 CAS 后逐层补齐 level>=1。
  // preds/succs 为输入输出（批量写复用上一次定位结果做 hint 前进）。
  LFNode* PublishNode(const CedarKey& key, LFNode* node, int height,
                      LFNode* preds[], LFNode* succs[], bool* have_hint);

  // 从 head 自顶向下完整定位所有层的 (pred, succ)。
  void FindSplice(const CedarKey& key, LFNode* preds[], LFNode* succs[]) const;

  // 批量写快路径：从上一次定位结果继续前进（key 必须非递减）。
  void AdvanceSplice(const CedarKey& key, LFNode* preds[], LFNode* succs[]) const;

  // 找到 user key 的第一个节点（即最新版本）；match_target=false 时忽略 target。
  LFNode* FindVersionHead(uint64_t entity_id,
                          EntityType entity_type,
                          uint16_t column_id,
                          uint64_t target_id,
                          bool match_target) const;

  // 版本链回链修复：pred->older 应指向 node（node 更靠近 pred 时）。
  void LinkOlder(LFNode* pred, LFNode* node);

  void UpdateMaxHeight(int height);

  // Task 2.1：每线程高度调度（thread_local PRNG 相位 + 低差异几何序列）。
  int ThreadLevelHeight();

  static int CompareNodes(const LFNode* a, const LFNode* b);

  MemTableArena arena_;
  LFNode* head_;
  LFNode* tail_;
  alignas(64) std::atomic<int> max_height_;
  alignas(64) std::atomic<size_t> size_;
};

}  // namespace cedar

#endif  // FERN_VERSIONED_SKIPLIST_LOCKFREE_H_