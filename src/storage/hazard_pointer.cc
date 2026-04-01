#include "cedar/storage/hazard_pointer.h"

namespace cedar {

template<typename T>
HazardPointerDomain<T>::~HazardPointerDomain() {
  Clear();
}

template<typename T>
HazardPointer<T>* HazardPointerDomain<T>::AllocateSlot() {
  std::lock_guard<std::mutex> lock(mutex_);
  auto* hp = new HazardPointer<T>(this);
  slots_.push_back(hp);
  return hp;
}

template<typename T>
void HazardPointerDomain<T>::Retire(T* node) {
  std::lock_guard<std::mutex> lock(mutex_);
  retired_list_.push_back(node);
  
  // 当退休列表达到一定大小时，尝试回收
  if (retired_list_.size() >= 100) {
    TryReclaim();
  }
}

template<typename T>
void HazardPointerDomain<T>::TryReclaim() {
  // 收集所有被保护的指针
  std::vector<T*> protected_ptrs;
  for (auto* hp : slots_) {
    T* ptr = hp->Get();
    if (ptr != nullptr) {
      protected_ptrs.push_back(ptr);
    }
  }
  
  // 排序以便二分查找
  std::sort(protected_ptrs.begin(), protected_ptrs.end());
  
  // 尝试回收
  std::vector<T*> remaining;
  for (T* node : retired_list_) {
    // 检查是否被保护
    bool is_protected = std::binary_search(
        protected_ptrs.begin(), protected_ptrs.end(), node);
    
    if (!is_protected) {
      // 安全删除
      delete node;
    } else {
      remaining.push_back(node);
    }
  }
  
  retired_list_.swap(remaining);
}

template<typename T>
void HazardPointerDomain<T>::Clear() {
  std::lock_guard<std::mutex> lock(mutex_);
  
  // 清理所有 hazard pointer slots
  for (auto* hp : slots_) {
    delete hp;
  }
  slots_.clear();
  
  // 清理所有退休的节点（不安全，仅用于测试）
  for (T* node : retired_list_) {
    delete node;
  }
  retired_list_.clear();
}

// 显式实例化 - 需要在 VersionedSkipListNode 定义后实例化
// 暂时注释掉，避免前向声明问题
// template class HazardPointerDomain<VersionedSkipListNode>;
// template class HazardPointer<VersionedSkipListNode>;

}  // namespace cedar
