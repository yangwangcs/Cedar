// Hazard Pointer - 无锁数据结构的内存回收机制
// 参考: https://www.cs.toronto.edu/~tompa/papers/hazard.pdf

#ifndef FERN_HAZARD_POINTER_H_
#define FERN_HAZARD_POINTER_H_

#include <atomic>
#include <vector>
#include <thread>
#include <mutex>

namespace cedar {

// 前向声明
template<typename T>
class HazardPointer;

template<typename T>
class HazardPointerDomain {
 public:
  static HazardPointerDomain& GetInstance() {
    static HazardPointerDomain instance;
    return instance;
  }
  
  // 分配一个 hazard pointer slot
  HazardPointer<T>* AllocateSlot();
  
  // 回收节点
  void Retire(T* node);
  
  // 尝试回收已退休的节点
  void TryReclaim();
  
  // 清理所有资源（仅用于测试）
  void Clear();
  
 private:
  HazardPointerDomain() = default;
  ~HazardPointerDomain();
  
  std::vector<HazardPointer<T>*> slots_;
  std::vector<T*> retired_list_;
  std::mutex mutex_;
};

// Hazard Pointer - 保护节点不被提前释放
template<typename T>
class HazardPointer {
 public:
  explicit HazardPointer(HazardPointerDomain<T>* domain)
    : domain_(domain), pointer_(nullptr) {}
  
  // 设置保护的指针
  void Protect(T* ptr) {
    pointer_.store(ptr, std::memory_order_release);
  }
  
  // 清除保护
  void Clear() {
    pointer_.store(nullptr, std::memory_order_release);
  }
  
  // 获取保护的指针
  T* Get() const {
    return pointer_.load(std::memory_order_acquire);
  }
  
  // 检查指针是否被保护
  bool IsProtecting(T* ptr) const {
    return pointer_.load(std::memory_order_acquire) == ptr;
  }
  
 private:
  HazardPointerDomain<T>* domain_;
  std::atomic<T*> pointer_;
  
  friend class HazardPointerDomain<T>;
};

// RAII 包装的 Hazard Pointer
template<typename T>
class HazardPointerGuard {
 public:
  explicit HazardPointerGuard(HazardPointerDomain<T>& domain)
    : hp_(domain.AllocateSlot()) {}
  
  ~HazardPointerGuard() {
    if (hp_) {
      hp_->Clear();
    }
  }
  
  void Protect(T* ptr) {
    if (hp_) {
      hp_->Protect(ptr);
    }
  }
  
  T* Get() const {
    return hp_ ? hp_->Get() : nullptr;
  }
  
 private:
  HazardPointer<T>* hp_;
};

}  // namespace cedar

#endif  // FERN_HAZARD_POINTER_H_
