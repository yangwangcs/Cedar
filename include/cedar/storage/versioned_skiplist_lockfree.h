// Lock-Free Versioned SkipList
// 使用原子操作实现真正的无锁并发

#ifndef FERN_VERSIONED_SKIPLIST_LOCKFREE_H_
#define FERN_VERSIONED_SKIPLIST_LOCKFREE_H_

#include <atomic>
#include <cstdint>
#include <vector>
#include <memory>
#include <functional>
#include <optional>

#include "cedar/types/descriptor.h"
#include "cedar/types/cedar_key.h"

namespace cedar {

// Lock-Free VSL 节点
class LFNode {
 public:
  LFNode(const CedarKey& key, const Descriptor& desc, int height, Timestamp txn_version);
  ~LFNode();
  
  // 获取信息
  uint64_t entity_id() const { return entity_id_; }
  uint64_t timestamp() const { return timestamp_; }
  uint64_t txn_version() const { return txn_version_; }
  uint16_t column_id() const { return column_id_; }
  EntityType entity_type() const { return static_cast<EntityType>(entity_type_); }
  uint64_t target_id() const { return target_id_; }
  uint32_t sequence() const { return sequence_; }
  uint8_t flags() const { return flags_; }
  
  // 重建完整的 CedarKey
  CedarKey GetKey() const;
  
  const Descriptor& descriptor() const { return descriptor_; }
  Descriptor& descriptor() { return descriptor_; }
  int height() const { return height_; }
  
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
  
  // 标记删除
  bool MarkDeleted();
  bool IsMarked() const;
  
 private:
  uint64_t entity_id_;
  uint64_t timestamp_;        // 业务时间戳 (用于 Key 排序和时序查询)
  uint64_t txn_version_;      // 事务版本号 (用于 MVCC 隔离)
  uint64_t target_id_;        // target_id for EdgeOut/EdgeIn
  Descriptor descriptor_;
  uint16_t column_id_;
  uint32_t sequence_;
  uint8_t entity_type_;       // EntityType enum value
  uint8_t flags_;
  int height_;
  
  // 固定大小数组避免内存对齐问题
  std::atomic<LFNode*> next_[16];
  std::atomic<LFNode*> older_version_;
  std::atomic<LFNode*> newer_version_;
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
  
  // 核心操作 (Lock-Free)
  bool Insert(const CedarKey& key, const Descriptor& value, Timestamp txn_version);
  
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
  
  // 统计
  size_t size() const { return size_.load(std::memory_order_acquire); }
  size_t ApproximateMemoryUsage() const;
  
  // 遍历
  void Traverse(std::function<bool(const CedarKey&, const Descriptor&)> callback) const;
  
 private:
  int RandomHeight();
  LFNode* FindNode(const CedarKey& key, LFNode* preds[], LFNode* succs[]);
  LFNode* FindLatestVersion(uint64_t entity_id, EntityType entity_type, 
                           uint16_t column_id) const;
  LFNode* FindLatestVersion(uint64_t entity_id, EntityType entity_type, 
                           uint16_t column_id, uint64_t target_id) const;
  void HelpDelete(LFNode* node, int level);
  
  LFNode* head_;
  LFNode* tail_;
  std::atomic<int> max_height_;
  std::atomic<size_t> size_;
  std::atomic<uint32_t> rnd_;
};

}  // namespace cedar

#endif  // FERN_VERSIONED_SKIPLIST_LOCKFREE_H_
