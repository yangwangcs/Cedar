// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "rocksdb/memtablerep.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include "db/dbformat.h"
#include "db/memtable.h"
#include "memory/allocator.h"
#include "port/port.h"
#include "util/coding.h"

namespace ROCKSDB_NAMESPACE {
namespace {

constexpr size_t kV2UserKeyBytes = 32;
constexpr size_t kV2InternalKeyBytes = 40;
constexpr uint8_t kNoChild = 0xff;

void StoreBigEndian64(char* destination, uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    *destination++ = static_cast<char>(value >> shift);
  }
}

std::array<unsigned char, kV2InternalKeyBytes> NormalizeInternalKey(
    const Slice& internal_key) {
  std::array<unsigned char, kV2InternalKeyBytes> result{};
  const bool is_short_v2_seek =
      internal_key.size() > 8 && internal_key.size() < kV2UserKeyBytes &&
      static_cast<unsigned char>(internal_key[0]) == 2;
  const size_t user_key_bytes = is_short_v2_seek
                                    ? internal_key.size() - 8
                                    : std::min(internal_key.size(),
                                               kV2UserKeyBytes);
  std::memcpy(result.data(), internal_key.data(), user_key_bytes);
  if (internal_key.size() >= kV2InternalKeyBytes) {
    const uint64_t tag = DecodeFixed64(internal_key.data() + kV2UserKeyBytes);
    StoreBigEndian64(reinterpret_cast<char*>(result.data() + kV2UserKeyBytes),
                     ~tag);
  }
  return result;
}

bool IsCanonicalV2InternalKey(const Slice& internal_key) {
  if (internal_key.size() != kV2InternalKeyBytes ||
      static_cast<unsigned char>(internal_key[0]) != 2) {
    return false;
  }
  const unsigned char family = static_cast<unsigned char>(internal_key[5]);
  const uint16_t property_id =
      (static_cast<uint16_t>(static_cast<unsigned char>(internal_key[6])) << 8) |
      static_cast<unsigned char>(internal_key[7]);
  uint64_t entity_id = 0;
  for (size_t index = 8; index < 16; ++index) {
    entity_id = (entity_id << 8) | static_cast<unsigned char>(internal_key[index]);
  }
  const ValueType value_type = static_cast<ValueType>(
      DecodeFixed64(internal_key.data() + kV2UserKeyBytes) & 0xffU);
  const bool is_state_or_identity = family == 1 || family == 3 || family == 4;
  const bool is_property = family == 2 || family == 5;
  return entity_id != 0 &&
         ((is_state_or_identity && property_id == 0) ||
          (is_property && property_id != 0)) &&
         (value_type == kTypeValue || value_type == kTypeDeletion);
}

enum class NodeKind : uint8_t { kNode4, kNode16, kNode48, kNode256 };

struct Node {
  NodeKind kind;
  uint8_t prefix_length;
  std::array<unsigned char, kV2InternalKeyBytes> prefix{};
  const char* entry = nullptr;
};

struct Node4 : Node {
  std::array<unsigned char, 4> keys{};
  std::array<Node*, 4> children{};
};

struct Node16 : Node {
  std::array<unsigned char, 16> keys{};
  std::array<Node*, 16> children{};
};

struct Node48 : Node {
  std::array<unsigned char, 256> indexes{};
  std::array<Node*, 48> children{};
};

struct Node256 : Node {
  std::array<Node*, 256> children{};
};

class PartitionedVersionRadixMemTable final : public MemTableRep {
 private:
  class Iterator final : public MemTableRep::Iterator {
   public:
    Iterator(const PartitionedVersionRadixMemTable& table, Arena* arena)
        : table_(table) {
      (void)arena;
    }

    bool Valid() const override { return entry_ != nullptr; }
    const char* key() const override {
      assert(Valid());
      return entry_;
    }
    void Next() override {
      assert(Valid());
      auto target = key_;
      if (!Increment(&target)) {
        entry_ = nullptr;
        return;
      }
      PositionAtOrAfter(target);
    }
    void Prev() override {
      assert(Valid());
      auto target = key_;
      if (!Decrement(&target)) {
        entry_ = nullptr;
        return;
      }
      PositionAtOrBefore(target);
    }
    void Seek(const Slice& internal_key, const char* memtable_key) override {
      PositionAtOrAfter(NormalizeInternalKey(
          memtable_key == nullptr ? internal_key
                                  : GetLengthPrefixedSlice(memtable_key)));
    }
    void SeekForPrev(const Slice& internal_key,
                     const char* memtable_key) override {
      PositionAtOrBefore(NormalizeInternalKey(
          memtable_key == nullptr ? internal_key
                                  : GetLengthPrefixedSlice(memtable_key)));
    }
    void SeekToFirst() override { Position(table_.FirstEntry()); }
    void SeekToLast() override { Position(table_.LastEntry()); }

   private:
    static bool Increment(
        std::array<unsigned char, kV2InternalKeyBytes>* key) {
      for (size_t index = key->size(); index > 0; --index) {
        unsigned char& byte = (*key)[index - 1];
        if (byte == 0xff) continue;
        ++byte;
        std::fill(key->begin() + index, key->end(), 0);
        return true;
      }
      return false;
    }

    static bool Decrement(
        std::array<unsigned char, kV2InternalKeyBytes>* key) {
      for (size_t index = key->size(); index > 0; --index) {
        unsigned char& byte = (*key)[index - 1];
        if (byte == 0) continue;
        --byte;
        std::fill(key->begin() + index, key->end(), 0xff);
        return true;
      }
      return false;
    }

    void Position(const char* entry) {
      entry_ = entry;
      if (entry_ != nullptr) {
        key_ = NormalizeInternalKey(GetLengthPrefixedSlice(entry_));
      }
    }

    void PositionAtOrAfter(
        const std::array<unsigned char, kV2InternalKeyBytes>& target) {
      Position(table_.FirstAtOrAfter(target));
    }

    void PositionAtOrBefore(
        const std::array<unsigned char, kV2InternalKeyBytes>& target) {
      Position(table_.LastAtOrBefore(target));
    }

    const PartitionedVersionRadixMemTable& table_;
    const char* entry_ = nullptr;
    std::array<unsigned char, kV2InternalKeyBytes> key_{};
  };

 public:
  PartitionedVersionRadixMemTable(const KeyComparator& comparator,
                                  Allocator* allocator,
                                  const PartitionedVersionRadixFactory::Options& options)
      : MemTableRep(allocator), comparator_(comparator), options_(options),
        root_(NewNode4()) {}

  KeyHandle Allocate(size_t length, char** buffer) override {
    *buffer = allocator_->Allocate(length);
    return *buffer;
  }

  void Insert(KeyHandle handle) override {
    const bool inserted = InsertEntry(static_cast<const char*>(handle));
    assert(inserted);
    (void)inserted;
  }

  bool InsertKey(KeyHandle handle) override {
    return InsertEntry(static_cast<const char*>(handle));
  }

  void InsertConcurrently(KeyHandle handle) override { Insert(handle); }

  bool InsertKeyConcurrently(KeyHandle handle) override {
    return InsertKey(handle);
  }

  bool Contains(const char* key) const override {
    const auto normalized = NormalizeInternalKey(GetLengthPrefixedSlice(key));
    ReadLock lock(&mutex_);
    const Node* node = root_;
    size_t depth = 0;
    while (node != nullptr) {
      if (std::memcmp(node->prefix.data(), normalized.data() + depth,
                      node->prefix_length) != 0) {
        return false;
      }
      depth += node->prefix_length;
      if (depth == normalized.size()) {
        return node->entry != nullptr && comparator_(node->entry, key) == 0;
      }
      node = FindChild(node, normalized[depth++]);
    }
    return false;
  }

  void Get(const LookupKey& lookup_key, void* callback_args,
           bool (*callback_func)(void*, const char*)) override {
    std::unique_ptr<MemTableRep::Iterator> iterator(GetIterator());
    iterator->Seek(lookup_key.internal_key(), lookup_key.memtable_key().data());
    while (iterator->Valid() && callback_func(callback_args, iterator->key())) {
      iterator->Next();
    }
  }

  Status GetAndValidate(
      const LookupKey& lookup_key, void* callback_args,
      bool (*callback_func)(void*, const char*), bool allow_data_in_errors,
      bool detect_key_out_of_order,
      const std::function<Status(const char*, bool)>& key_validation_callback)
      override {
    std::unique_ptr<MemTableRep::Iterator> iterator(GetIterator());
    iterator->Seek(lookup_key.internal_key(), lookup_key.memtable_key().data());
    return ValidateAndVisit(iterator.get(), callback_args, callback_func,
                            allow_data_in_errors, detect_key_out_of_order,
                            key_validation_callback);
  }

  Status MultiGet(
      size_t num_keys, const char* const* keys, void** callback_args,
      bool (*callback_func)(void*, const char*), bool allow_data_in_errors,
      bool detect_key_out_of_order,
      const std::function<Status(const char*, bool)>& key_validation_callback)
      override {
    std::unique_ptr<MemTableRep::Iterator> iterator(GetIterator());
    Slice ignored_internal_key;
    for (size_t index = 0; index < num_keys; ++index) {
      iterator->Seek(ignored_internal_key, keys[index]);
      Status status = ValidateAndVisit(
          iterator.get(), callback_args[index], callback_func,
          allow_data_in_errors, detect_key_out_of_order,
          key_validation_callback);
      if (!status.ok()) return status;
    }
    return Status::OK();
  }

  size_t ApproximateMemoryUsage() override {
    return memory_usage_.load(std::memory_order_relaxed);
  }

  Iterator* GetIterator(Arena* arena = nullptr) override {
    void* storage = arena == nullptr ? operator new(sizeof(Iterator))
                                     : arena->AllocateAligned(sizeof(Iterator));
    return new (storage) Iterator(*this, arena);
  }

  bool IsMergeOperatorSupported() const override { return false; }

 private:
  Status ValidateAndVisit(
      MemTableRep::Iterator* iterator, void* callback_args,
      bool (*callback_func)(void*, const char*), bool allow_data_in_errors,
      bool detect_key_out_of_order,
      const std::function<Status(const char*, bool)>& key_validation_callback) {
    const char* previous = nullptr;
    while (iterator->Valid()) {
      const char* entry = iterator->key();
      if (key_validation_callback != nullptr) {
        Status status = key_validation_callback(entry, allow_data_in_errors);
        if (!status.ok()) return status;
      }
      if (detect_key_out_of_order && previous != nullptr &&
          comparator_(previous, entry) >= 0) {
        return Status::Corruption("Cedar version-radix MemTable order");
      }
      if (!callback_func(callback_args, entry)) return Status::OK();
      previous = entry;
      iterator->Next();
    }
    return Status::OK();
  }

  template <typename T>
  T* NewNode(NodeKind kind, const unsigned char* prefix = nullptr,
             size_t prefix_length = 0) {
    assert(prefix_length <= kV2InternalKeyBytes);
    char* storage = allocator_->AllocateAligned(sizeof(T));
    memory_usage_.fetch_add(sizeof(T), std::memory_order_relaxed);
    auto* node = new (storage) T();
    node->kind = kind;
    node->prefix_length = static_cast<uint8_t>(prefix_length);
    if (prefix_length != 0) {
      std::memcpy(node->prefix.data(), prefix, prefix_length);
    }
    if constexpr (std::is_same<T, Node48>::value) {
      node->indexes.fill(kNoChild);
    }
    return node;
  }

  Node* NewNode4(const unsigned char* prefix = nullptr, size_t prefix_length = 0) {
    return NewNode<Node4>(NodeKind::kNode4, prefix, prefix_length);
  }

  static size_t ChildCount(const Node* node) {
    switch (node->kind) {
      case NodeKind::kNode4:
        return std::count_if(static_cast<const Node4*>(node)->children.begin(),
                             static_cast<const Node4*>(node)->children.end(),
                             [](Node* child) { return child != nullptr; });
      case NodeKind::kNode16:
        return std::count_if(static_cast<const Node16*>(node)->children.begin(),
                             static_cast<const Node16*>(node)->children.end(),
                             [](Node* child) { return child != nullptr; });
      case NodeKind::kNode48:
        return std::count_if(static_cast<const Node48*>(node)->children.begin(),
                             static_cast<const Node48*>(node)->children.end(),
                             [](Node* child) { return child != nullptr; });
      case NodeKind::kNode256:
        return std::count_if(static_cast<const Node256*>(node)->children.begin(),
                             static_cast<const Node256*>(node)->children.end(),
                             [](Node* child) { return child != nullptr; });
    }
    return 0;
  }

  static Node* FindChild(const Node* node, unsigned char key) {
    switch (node->kind) {
      case NodeKind::kNode4: {
        const auto* typed = static_cast<const Node4*>(node);
        const size_t count = ChildCount(node);
        const auto position = std::lower_bound(typed->keys.begin(),
                                               typed->keys.begin() + count, key);
        return position != typed->keys.begin() + count && *position == key
                   ? typed->children[position - typed->keys.begin()]
                   : nullptr;
      }
      case NodeKind::kNode16: {
        const auto* typed = static_cast<const Node16*>(node);
        const size_t count = ChildCount(node);
        const auto position = std::lower_bound(typed->keys.begin(),
                                               typed->keys.begin() + count, key);
        return position != typed->keys.begin() + count && *position == key
                   ? typed->children[position - typed->keys.begin()]
                   : nullptr;
      }
      case NodeKind::kNode48: {
        const auto* typed = static_cast<const Node48*>(node);
        const unsigned char index = typed->indexes[key];
        return index == kNoChild ? nullptr : typed->children[index];
      }
      case NodeKind::kNode256:
        return static_cast<const Node256*>(node)->children[key];
    }
    return nullptr;
  }

  static const Node* RightmostChild(const Node* node) {
    switch (node->kind) {
      case NodeKind::kNode4: {
        const auto* typed = static_cast<const Node4*>(node);
        const size_t count = ChildCount(node);
        return count == 0 ? nullptr : typed->children[count - 1];
      }
      case NodeKind::kNode16: {
        const auto* typed = static_cast<const Node16*>(node);
        const size_t count = ChildCount(node);
        return count == 0 ? nullptr : typed->children[count - 1];
      }
      case NodeKind::kNode48: {
        const auto* typed = static_cast<const Node48*>(node);
        for (size_t key = typed->indexes.size(); key > 0; --key) {
          const unsigned char index = typed->indexes[key - 1];
          if (index != kNoChild) return typed->children[index];
        }
        return nullptr;
      }
      case NodeKind::kNode256: {
        const auto* typed = static_cast<const Node256*>(node);
        for (size_t key = typed->children.size(); key > 0; --key) {
          if (typed->children[key - 1] != nullptr) {
            return typed->children[key - 1];
          }
        }
        return nullptr;
      }
    }
    return nullptr;
  }

  static Node** FindChildLocation(Node* node, unsigned char key) {
    switch (node->kind) {
      case NodeKind::kNode4: {
        auto* typed = static_cast<Node4*>(node);
        const size_t count = ChildCount(node);
        const auto position = std::lower_bound(typed->keys.begin(),
                                               typed->keys.begin() + count, key);
        return position != typed->keys.begin() + count && *position == key
                   ? &typed->children[position - typed->keys.begin()]
                   : nullptr;
      }
      case NodeKind::kNode16: {
        auto* typed = static_cast<Node16*>(node);
        const size_t count = ChildCount(node);
        const auto position = std::lower_bound(typed->keys.begin(),
                                               typed->keys.begin() + count, key);
        return position != typed->keys.begin() + count && *position == key
                   ? &typed->children[position - typed->keys.begin()]
                   : nullptr;
      }
      case NodeKind::kNode48: {
        auto* typed = static_cast<Node48*>(node);
        const unsigned char index = typed->indexes[key];
        return index == kNoChild ? nullptr : &typed->children[index];
      }
      case NodeKind::kNode256:
        return &static_cast<Node256*>(node)->children[key];
    }
    return nullptr;
  }

  void CopyBase(Node* destination, const Node* source) {
    destination->prefix_length = source->prefix_length;
    destination->prefix = source->prefix;
    destination->entry = source->entry;
  }

  Node* Grow(Node* node) {
    Node* result = nullptr;
    switch (node->kind) {
      case NodeKind::kNode4:
        result = NewNode<Node16>(NodeKind::kNode16);
        break;
      case NodeKind::kNode16:
        result = NewNode<Node48>(NodeKind::kNode48);
        break;
      case NodeKind::kNode48:
        result = NewNode<Node256>(NodeKind::kNode256);
        break;
      case NodeKind::kNode256:
        return node;
    }
    CopyBase(result, node);
    ForEachChild(node, [&result](unsigned char key, Node* child) {
      AddChildWithoutGrowth(result, key, child);
    });
    return result;
  }

  static void AddChildWithoutGrowth(Node* node, unsigned char key, Node* child) {
    switch (node->kind) {
      case NodeKind::kNode4: {
        auto* typed = static_cast<Node4*>(node);
        const size_t count = ChildCount(node);
        const auto position = std::lower_bound(typed->keys.begin(),
                                               typed->keys.begin() + count, key);
        const size_t index = position - typed->keys.begin();
        std::move_backward(typed->keys.begin() + index, typed->keys.begin() + count,
                           typed->keys.begin() + count + 1);
        std::move_backward(typed->children.begin() + index,
                           typed->children.begin() + count,
                           typed->children.begin() + count + 1);
        typed->keys[index] = key;
        typed->children[index] = child;
        return;
      }
      case NodeKind::kNode16: {
        auto* typed = static_cast<Node16*>(node);
        const size_t count = ChildCount(node);
        const auto position = std::lower_bound(typed->keys.begin(),
                                               typed->keys.begin() + count, key);
        const size_t index = position - typed->keys.begin();
        std::move_backward(typed->keys.begin() + index, typed->keys.begin() + count,
                           typed->keys.begin() + count + 1);
        std::move_backward(typed->children.begin() + index,
                           typed->children.begin() + count,
                           typed->children.begin() + count + 1);
        typed->keys[index] = key;
        typed->children[index] = child;
        return;
      }
      case NodeKind::kNode48: {
        auto* typed = static_cast<Node48*>(node);
        const size_t count = ChildCount(node);
        typed->indexes[key] = static_cast<unsigned char>(count);
        typed->children[count] = child;
        return;
      }
      case NodeKind::kNode256:
        static_cast<Node256*>(node)->children[key] = child;
        return;
    }
  }

  void AddChild(Node** location, unsigned char key, Node* child) {
    Node* node = *location;
    if ((node->kind == NodeKind::kNode4 && ChildCount(node) == 4) ||
        (node->kind == NodeKind::kNode16 && ChildCount(node) == 16) ||
        (node->kind == NodeKind::kNode48 && ChildCount(node) == 48)) {
      node = Grow(node);
      *location = node;
    }
    AddChildWithoutGrowth(node, key, child);
  }

  template <typename Callback>
  static void ForEachChild(Node* node, Callback callback) {
    switch (node->kind) {
      case NodeKind::kNode4: {
        auto* typed = static_cast<Node4*>(node);
        const size_t count = ChildCount(node);
        for (size_t index = 0; index < count; ++index) callback(typed->keys[index], typed->children[index]);
        return;
      }
      case NodeKind::kNode16: {
        auto* typed = static_cast<Node16*>(node);
        const size_t count = ChildCount(node);
        for (size_t index = 0; index < count; ++index) callback(typed->keys[index], typed->children[index]);
        return;
      }
      case NodeKind::kNode48: {
        auto* typed = static_cast<Node48*>(node);
        for (size_t key = 0; key < 256; ++key) {
          if (typed->indexes[key] != kNoChild) callback(static_cast<unsigned char>(key), typed->children[typed->indexes[key]]);
        }
        return;
      }
      case NodeKind::kNode256: {
        auto* typed = static_cast<Node256*>(node);
        for (size_t key = 0; key < 256; ++key) {
          if (typed->children[key] != nullptr) callback(static_cast<unsigned char>(key), typed->children[key]);
        }
        return;
      }
    }
  }

  bool InsertEntry(const char* entry) {
    const Slice internal_key = GetLengthPrefixedSlice(entry);
    if (!IsCanonicalV2InternalKey(internal_key)) return false;
    const auto normalized = NormalizeInternalKey(internal_key);
    LockForWriteWithRetry();
    struct UnlockWrite {
      explicit UnlockWrite(port::RWMutex* write_mutex) : mutex(write_mutex) {}
      ~UnlockWrite() { mutex->WriteUnlock(); }
      port::RWMutex* mutex;
    } lock(&mutex_);
    Node** location = &root_;
    size_t depth = 0;
    while (true) {
      Node* node = *location;
      size_t common = 0;
      while (common < node->prefix_length &&
             node->prefix[common] == normalized[depth + common]) {
        ++common;
      }
      if (common != node->prefix_length) {
        Node* parent = NewNode4(node->prefix.data(), common);
        const unsigned char old_edge = node->prefix[common];
        const size_t old_suffix = node->prefix_length - common - 1;
        std::memmove(node->prefix.data(), node->prefix.data() + common + 1,
                     old_suffix);
        node->prefix_length = static_cast<uint8_t>(old_suffix);
        AddChild(&parent, old_edge, node);
        const unsigned char new_edge = normalized[depth + common];
        Node* leaf = NewNode4(normalized.data() + depth + common + 1,
                              normalized.size() - depth - common - 1);
        leaf->entry = entry;
        AddChild(&parent, new_edge, leaf);
        *location = parent;
        return true;
      }
      depth += node->prefix_length;
      if (depth == normalized.size()) {
        if (node->entry != nullptr) return false;
        node->entry = entry;
        return true;
      }
      const unsigned char edge = normalized[depth++];
      Node* child = FindChild(node, edge);
      if (child == nullptr) {
        Node* leaf = NewNode4(normalized.data() + depth,
                              normalized.size() - depth);
        leaf->entry = entry;
        AddChild(location, edge, leaf);
        return true;
      }
      location = FindChildLocation(*location, edge);
      assert(location != nullptr);
    }
  }

  void LockForWriteWithRetry() {
    uint32_t spins = 0;
    while (!mutex_.TryWriteLock()) {
      if (options_.write_lock_retry_observer_for_testing) {
        options_.write_lock_retry_observer_for_testing();
      }
      if (spins < 64) {
        ++spins;
        port::AsmVolatilePause();
      } else {
        spins = 0;
        std::this_thread::yield();
      }
    }
    if (options_.write_lock_acquired_observer_for_testing) {
      options_.write_lock_acquired_observer_for_testing();
    }
  }

  const char* FirstEntry() const {
    ReadLock lock(&mutex_);
    return FirstEntryLocked(root_);
  }

  const char* FirstEntryLocked(const Node* node) const {
    if (node == nullptr) return nullptr;
    if (node->entry != nullptr) return node->entry;
    const char* result = nullptr;
    ForEachChild(const_cast<Node*>(node), [this, &result](unsigned char,
                                                           Node* child) {
      if (result == nullptr) result = FirstEntryLocked(child);
    });
    return result;
  }

  const char* LastEntry() const {
    ReadLock lock(&mutex_);
    return LastEntryLocked(root_);
  }

  const char* LastEntryLocked(const Node* node) const {
    while (node != nullptr) {
      if (options_.last_entry_visit_observer_for_testing) {
        options_.last_entry_visit_observer_for_testing();
      }
      const Node* child = RightmostChild(node);
      if (child == nullptr) return node->entry;
      node = child;
    }
    return nullptr;
  }

  const char* FirstAtOrAfter(
      const std::array<unsigned char, kV2InternalKeyBytes>& target) const {
    ReadLock lock(&mutex_);
    return FirstAtOrAfter(root_, 0, target);
  }

  const char* FirstAtOrAfter(
      const Node* node, size_t depth,
      const std::array<unsigned char, kV2InternalKeyBytes>& target) const {
    if (node == nullptr) return nullptr;
    for (size_t index = 0; index < node->prefix_length; ++index) {
      const unsigned char actual = node->prefix[index];
      const unsigned char expected = target[depth + index];
      if (actual < expected) return nullptr;
      if (actual > expected) return FirstEntryLocked(node);
    }
    depth += node->prefix_length;
    if (depth == target.size()) return node->entry;

    const unsigned char target_edge = target[depth];
    const char* result = nullptr;
    ForEachChild(const_cast<Node*>(node),
                 [this, depth, target_edge, &target, &result](unsigned char edge,
                                                               Node* child) {
                   if (result != nullptr || edge < target_edge) return;
                   result = edge == target_edge
                                ? FirstAtOrAfter(child, depth + 1, target)
                                : FirstEntryLocked(child);
                 });
    return result;
  }

  const char* LastAtOrBefore(
      const std::array<unsigned char, kV2InternalKeyBytes>& target) const {
    ReadLock lock(&mutex_);
    return LastAtOrBefore(root_, 0, target);
  }

  const char* LastAtOrBefore(
      const Node* node, size_t depth,
      const std::array<unsigned char, kV2InternalKeyBytes>& target) const {
    if (node == nullptr) return nullptr;
    for (size_t index = 0; index < node->prefix_length; ++index) {
      const unsigned char actual = node->prefix[index];
      const unsigned char expected = target[depth + index];
      if (actual > expected) return nullptr;
      if (actual < expected) return LastEntryLocked(node);
    }
    depth += node->prefix_length;
    if (depth == target.size()) return node->entry;

    const unsigned char target_edge = target[depth];
    const char* result = nullptr;
    ForEachChild(const_cast<Node*>(node),
                 [this, depth, target_edge, &target, &result](unsigned char edge,
                                                               Node* child) {
                   if (edge > target_edge) return;
                   const char* candidate = edge == target_edge
                                               ? LastAtOrBefore(child, depth + 1,
                                                                target)
                                               : LastEntryLocked(child);
                   if (candidate != nullptr) result = candidate;
                 });
    return result;
  }

  const KeyComparator& comparator_;
  const PartitionedVersionRadixFactory::Options options_;
  mutable port::RWMutex mutex_;
  std::atomic<size_t> memory_usage_{0};
  Node* root_;
};

}  // namespace

MemTableRep* PartitionedVersionRadixFactory::CreateMemTableRep(
    const MemTableRep::KeyComparator& comparator, Allocator* allocator,
    const SliceTransform*, Logger*) {
  return new PartitionedVersionRadixMemTable(comparator, allocator, options_);
}

}  // namespace ROCKSDB_NAMESPACE
