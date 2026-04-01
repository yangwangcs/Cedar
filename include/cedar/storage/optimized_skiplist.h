// Optimized SkipList - 针对批量导入优化的简化版本
// 特点: 单线程写入，多线程读取，无版本链开销

#ifndef FERN_OPTIMIZED_SKIPLIST_H_
#define FERN_OPTIMIZED_SKIPLIST_H_

#include <vector>
#include <memory>
#include <algorithm>

namespace cedar {

// 简化的 SkipList 节点 - 无锁结构
struct SimpleNode {
  uint64_t entity_id;
  uint64_t timestamp;
  int32_t value;
  std::vector<SimpleNode*> next;
  
  SimpleNode(uint64_t eid, uint64_t ts, int32_t val, int height)
    : entity_id(eid), timestamp(ts), value(val), next(height, nullptr) {}
};

// 优化的 SkipList - 批量构建，一次性排序
class OptimizedSkipList {
 public:
  static constexpr int kMaxHeight = 12;
  static constexpr int kBranching = 4;
  
  OptimizedSkipList();
  ~OptimizedSkipList();
  
  // 批量插入 - 收集后统一构建
  void BatchInsert(const std::vector<std::tuple<uint64_t, uint64_t, int32_t>>& items);
  
  // 查询
  std::optional<int32_t> Get(uint64_t entity_id, uint64_t timestamp) const;
  
  // 获取所有数据 (用于 flush)
  std::vector<std::tuple<uint64_t, uint64_t, int32_t>> GetAll() const;
  
  size_t size() const { return size_; }
  
 private:
  int RandomHeight();
  void RebuildIndex();
  
  SimpleNode* head_;
  std::vector<std::unique_ptr<SimpleNode>> nodes_;  // 所有权管理
  size_t size_ = 0;
};

}  // namespace cedar

#endif  // FERN_OPTIMIZED_SKIPLIST_H_
