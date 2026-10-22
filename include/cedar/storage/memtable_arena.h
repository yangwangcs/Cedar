// Copyright 2025 The Cedar Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// =============================================================================
// MemTableArena - memtable 节点用的 slab 分配器（Task 2.2）
//
// 设计：
//   - 固定 1MB slab，slab 只在 memtable 析构时整块释放（不做节点级回收，D6）；
//   - 每线程缓存一个 64KB region：region 内 bump 分配完全无原子操作；region
//     用尽才向 slab 领取新 region（一次 CAS），避免 8 线程共用一个热 cache line；
//   - 节点分配不经过 operator new（slab 本身用 malloc），插入热路径 0 per-node
//     malloc/free；
//   - ApproximateMemoryUsage 用已申请 slab 容量，O(1) 且单调。
//
// 线程安全：Allocate/New 可被多线程并发调用；析构要求外部保证无并发访问
// （与 LockFreeVSL 的生命周期契约一致）。
// =============================================================================

#ifndef CEDAR_STORAGE_MEMTABLE_ARENA_H_
#define CEDAR_STORAGE_MEMTABLE_ARENA_H_

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <new>
#include <utility>

namespace cedar {

class MemTableArena {
 public:
  static constexpr size_t kDefaultSlabSize = 1u << 20;  // 1MB
  static constexpr size_t kThreadRegionSize = 64u << 10;  // 每线程 64KB region

  explicit MemTableArena(size_t slab_size = kDefaultSlabSize)
      : slab_size_(std::max<size_t>(slab_size, kMinSlabSize)),
        id_(NextArenaId()),
        current_(nullptr),
        slabs_(nullptr),
        slab_count_(0),
        capacity_bytes_(0) {}

  ~MemTableArena() {
    Slab* slab = slabs_;
    while (slab != nullptr) {
      Slab* next = slab->next;
      std::free(slab);
      slab = next;
    }
  }

  MemTableArena(const MemTableArena&) = delete;
  MemTableArena& operator=(const MemTableArena&) = delete;

  // 分配 bytes 字节，按 alignment 对齐（alignment <= alignof(std::max_align_t)）。
  // 返回的内存从 arena 析构前一直有效；线程安全。
  void* Allocate(size_t bytes, size_t alignment = alignof(std::max_align_t)) {
    const size_t align = NormalizeAlignment(alignment);
    Region* region = FindRegion();
    if (region != nullptr) {
      void* p = TryAllocateInRegion(*region, bytes, align);
      if (p != nullptr) {
        return p;
      }
      region->arena_id = 0;  // region 用尽，释放缓存槽位
    }
    region = ClaimRegion(bytes, align);
    void* p = TryAllocateInRegion(*region, bytes, align);
    return p;  // ClaimRegion 保证容量足够
  }

  // 在 arena 上构造 T（placement new，不经过 operator new）。
  template <typename T, typename... Args>
  T* New(Args&&... args) {
    static_assert(alignof(T) <= alignof(std::max_align_t),
                  "MemTableArena only supports max_align_t alignment");
    void* p = Allocate(sizeof(T), alignof(T));
    return ::new (p) T(std::forward<Args>(args)...);
  }

  size_t slab_count() const { return slab_count_.load(std::memory_order_relaxed); }

  // 已申请 slab 的总容量（字节）：ApproximateMemoryUsage 的 O(1) 来源。
  size_t capacity_bytes() const {
    return capacity_bytes_.load(std::memory_order_relaxed);
  }

  size_t slab_size() const { return slab_size_; }

 private:
  static constexpr size_t kMinSlabSize = 4096;
  static constexpr int kRegionSlots = 4;  // 每线程缓存 4 个 arena region

  // slab 头与数据区之间的 padding，保证数据区按 max_align 对齐。
  static constexpr size_t kDataOffset =
      (sizeof(void*) + sizeof(size_t) + sizeof(std::atomic<size_t>) +
       alignof(std::max_align_t) - 1) /
          alignof(std::max_align_t) *
      alignof(std::max_align_t);

  struct Slab {
    Slab* next;
    size_t capacity;
    std::atomic<size_t> used;
  };

  // 线程本地 region 缓存。arena_id 全局唯一且不复用：arena 析构后，残留的
  // region 记录因 id 不匹配而不会被解引用（只比较 id，不碰指针）。
  struct Region {
    uint64_t arena_id;
    char* cursor;
    char* end;
  };

  static uint64_t NextArenaId() {
    static std::atomic<uint64_t> next{1};
    return next.fetch_add(1, std::memory_order_relaxed);
  }

  static Region* ThreadRegions() {
    static thread_local Region regions[kRegionSlots] = {};
    return regions;
  }

  Region* FindRegion() {
    Region* regions = ThreadRegions();
    for (int i = 0; i < kRegionSlots; ++i) {
      if (regions[i].arena_id == id_) {
        return &regions[i];
      }
    }
    return nullptr;
  }

  static constexpr size_t NormalizeAlignment(size_t alignment) {
    if (alignment == 0) {
      return 1;
    }
    return alignment > alignof(std::max_align_t) ? alignof(std::max_align_t)
                                                 : alignment;
  }

  static char* DataOf(Slab* slab) {
    return reinterpret_cast<char*>(slab) + kDataOffset;
  }

  static void* TryAllocateInRegion(Region& region, size_t bytes,
                                   size_t alignment) {
    const uintptr_t base = reinterpret_cast<uintptr_t>(region.cursor);
    const uintptr_t aligned = (base + alignment - 1) & ~(uintptr_t)(alignment - 1);
    if (aligned + bytes > reinterpret_cast<uintptr_t>(region.end)) {
      return nullptr;
    }
    region.cursor = reinterpret_cast<char*>(aligned + bytes);
    return reinterpret_cast<void*>(aligned);
  }

  // 从 slab 领取一块 region 并登记到当前线程的缓存槽。
  Region* ClaimRegion(size_t bytes, size_t alignment) {
    Region* regions = ThreadRegions();
    Region* slot = nullptr;
    for (int i = 0; i < kRegionSlots; ++i) {
      if (regions[i].arena_id == 0) {
        slot = &regions[i];
        break;
      }
    }
    if (slot == nullptr) {
      static thread_local unsigned round = 0;
      slot = &regions[round++ % kRegionSlots];
    }

    const size_t chunk = std::max(kThreadRegionSize, bytes + alignment);
    slot->arena_id = id_;
    slot->cursor = ClaimChunk(chunk, alignment);
    slot->end = slot->cursor + chunk;
    return slot;
  }

  // 从共享 slab 领取 chunk（每线程每 64KB 才走一次 CAS/mutex）。
  char* ClaimChunk(size_t chunk, size_t alignment) {
    for (;;) {
      Slab* slab = current_.load(std::memory_order_acquire);
      if (slab != nullptr) {
        size_t used = slab->used.load(std::memory_order_relaxed);
        for (;;) {
          const size_t start = (used + alignment - 1) / alignment * alignment;
          if (start > slab->capacity || chunk > slab->capacity - start) {
            break;
          }
          if (slab->used.compare_exchange_weak(used, start + chunk,
                                               std::memory_order_acq_rel,
                                               std::memory_order_relaxed)) {
            return DataOf(slab) + start;
          }
        }
      }

      // slab 用尽：mutex 下换新 slab（约每 1MB 一次）。
      bool rotated = false;
      {
        std::lock_guard<std::mutex> lock(rotate_mutex_);
        Slab* cur = current_.load(std::memory_order_relaxed);
        if (cur == slab) {
          Slab* fresh = NewSlabLocked(std::max(slab_size_, chunk + alignment));
          current_.store(fresh, std::memory_order_release);
          rotated = true;
        }
      }
      // 竞争失败或已经换块：重新尝试从 current_ 领取。
      (void)rotated;
    }
  }

  Slab* NewSlabLocked(size_t capacity) {
    const size_t total = kDataOffset + capacity;
    void* raw = std::malloc(total);
    if (raw == nullptr) {
      throw std::bad_alloc();
    }
    Slab* slab = static_cast<Slab*>(raw);
    slab->next = slabs_;
    slab->capacity = capacity;
    slab->used.store(0, std::memory_order_relaxed);
    slabs_ = slab;
    slab_count_.fetch_add(1, std::memory_order_relaxed);
    capacity_bytes_.fetch_add(capacity, std::memory_order_relaxed);
    return slab;
  }

  const size_t slab_size_;
  const uint64_t id_;
  std::atomic<Slab*> current_;
  std::mutex rotate_mutex_;
  Slab* slabs_;  // 仅在 rotate_mutex_ 下写；析构时无并发访问
  std::atomic<size_t> slab_count_;
  std::atomic<size_t> capacity_bytes_;
};

}  // namespace cedar

#endif  // CEDAR_STORAGE_MEMTABLE_ARENA_H_
