// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#include "memtable/cedar_pure_radix_index.h"

#include <algorithm>
#include <bit>
#include <cassert>
#include <cstring>
#include <limits>
#include <new>
#include <utility>

#include "memory/allocator.h"

namespace ROCKSDB_NAMESPACE {
namespace {
uint16_t Mask(uint8_t n) { return static_cast<uint16_t>(uint16_t{1} << n); }
uint16_t Below(uint8_t n) {
  return static_cast<uint16_t>((uint32_t{1} << n) - 1);
}
}  // namespace

CedarPureRadixIndex::CedarPureRadixIndex(Allocator* allocator, TestHooks hooks)
    : allocator_(allocator), test_hooks_(std::move(hooks)) {
  assert(std::atomic<Node*>::is_always_lock_free);
  assert(std::atomic<ChildTable*>::is_always_lock_free);
}

void* CedarPureRadixIndex::Allocate(size_t length, char** buffer) {
  assert(buffer != nullptr);
  if (length > std::numeric_limits<size_t>::max() - sizeof(Handle)) {
    *buffer = nullptr;
    return nullptr;
  }
  char* storage = allocator_->AllocateAligned(sizeof(Handle) + length);
  auto* handle = new (storage) Handle();
  *buffer = storage + sizeof(Handle);
  handle->leaf.entry = *buffer;
  return handle;
}

const char* CedarPureRadixIndex::EntryForHandle(void* opaque) const {
  const auto* handle = static_cast<const Handle*>(opaque);
  return handle == nullptr ? nullptr : handle->leaf.entry;
}

uint8_t CedarPureRadixIndex::NibbleAt(const Key& key, uint8_t index) {
  assert(index < kNibbles);
  const auto byte = key[index / 2];
  return static_cast<uint8_t>(index % 2 == 0 ? byte >> 4 : byte & 0x0f);
}

uint8_t CedarPureRadixIndex::NibbleAt(const Leaf* leaf, uint8_t index) {
  assert(leaf != nullptr && leaf->key_prefix != nullptr && index < kNibbles);
  const size_t byte_index = index / 2;
  const auto byte = byte_index < 32 ? leaf->key_prefix[byte_index]
                                    : leaf->normalized_suffix[byte_index - 32];
  return static_cast<uint8_t>(index % 2 == 0 ? byte >> 4 : byte & 0x0f);
}

int CedarPureRadixIndex::Compare(const Key& left, const Key& right) {
  return std::memcmp(left.data(), right.data(), left.size());
}
int CedarPureRadixIndex::Compare(const Leaf* left, const Key& right) {
  const int prefix = std::memcmp(left->key_prefix, right.data(), 32);
  return prefix == 0 ? std::memcmp(left->normalized_suffix.data(), right.data() + 32, 8)
                     : prefix;
}
int CedarPureRadixIndex::Compare(const Leaf* left, const Leaf* right) {
  const int prefix = std::memcmp(left->key_prefix, right->key_prefix, 32);
  return prefix == 0 ? std::memcmp(left->normalized_suffix.data(),
                                   right->normalized_suffix.data(), 8)
                     : prefix;
}

uint8_t CedarPureRadixIndex::FirstDifferingNibble(const Key& left,
                                                   const Key& right) {
  for (size_t byte = 0; byte < kKeyBytes; ++byte) {
    const auto delta = static_cast<unsigned char>(left[byte] ^ right[byte]);
    if (delta != 0) return static_cast<uint8_t>(byte * 2 + ((delta & 0xf0) == 0));
  }
  return static_cast<uint8_t>(kNibbles);
}
uint8_t CedarPureRadixIndex::FirstDifferingNibble(const Leaf* left,
                                                   const Key& right) {
  for (size_t byte = 0; byte < kKeyBytes; ++byte) {
    const auto value = byte < 32 ? left->key_prefix[byte]
                                 : left->normalized_suffix[byte - 32];
    const auto delta = static_cast<unsigned char>(value ^ right[byte]);
    if (delta != 0) return static_cast<uint8_t>(byte * 2 + ((delta & 0xf0) == 0));
  }
  return static_cast<uint8_t>(kNibbles);
}

CedarPureRadixIndex::Leaf* CedarPureRadixIndex::AsLeaf(Node* node) {
  assert(node != nullptr && node->kind == NodeKind::kLeaf);
  return static_cast<Leaf*>(node);
}
const CedarPureRadixIndex::Leaf* CedarPureRadixIndex::AsLeaf(const Node* node) {
  assert(node != nullptr && node->kind == NodeKind::kLeaf);
  return static_cast<const Leaf*>(node);
}
CedarPureRadixIndex::Branch* CedarPureRadixIndex::AsBranch(Node* node) {
  assert(node != nullptr && node->kind == NodeKind::kBranch);
  return static_cast<Branch*>(node);
}
const CedarPureRadixIndex::Branch* CedarPureRadixIndex::AsBranch(const Node* node) {
  assert(node != nullptr && node->kind == NodeKind::kBranch);
  return static_cast<const Branch*>(node);
}

CedarPureRadixIndex::Node* CedarPureRadixIndex::ChildAt(const ChildTable* table,
                                                         uint8_t nibble) {
  assert(table != nullptr && nibble < 16);
  if ((table->occupied & Mask(nibble)) == 0) return nullptr;
  const auto rank = std::popcount(static_cast<uint16_t>(table->occupied & Below(nibble)));
  assert(rank < table->child_count);
  return table->children[rank];
}
uint8_t CedarPureRadixIndex::FirstChildAtOrAfter(const ChildTable* table,
                                                 uint8_t nibble) {
  assert(table != nullptr && nibble < 16);
  const auto set = static_cast<uint16_t>(table->occupied & ~Below(nibble));
  return set == 0 ? 16 : static_cast<uint8_t>(std::countr_zero(set));
}
uint8_t CedarPureRadixIndex::LastChildAtOrBefore(const ChildTable* table,
                                                 uint8_t nibble) {
  assert(table != nullptr && nibble < 16);
  const auto set = static_cast<uint16_t>(
      table->occupied & ((uint32_t{1} << (nibble + 1)) - 1));
  return set == 0 ? 16 : static_cast<uint8_t>(15 - std::countl_zero(set));
}

CedarPureRadixIndex::ChildTable* CedarPureRadixIndex::AllocateChildTable(
    uint16_t occupied, Node* const* children) {
  const auto count = static_cast<uint8_t>(std::popcount(occupied));
  assert(count > 0);
  const auto bytes = offsetof(ChildTable, children) + size_t{count} * sizeof(Node*);
  auto* table = reinterpret_cast<ChildTable*>(allocator_->AllocateAligned(bytes));
  table->occupied = occupied;
  table->child_count = count;
  for (size_t i = 0; i < count; ++i) {
    assert(children[i] != nullptr);
    table->children[i] = children[i];
  }
  assert(table->child_count == std::popcount(table->occupied));
  return table;
}
CedarPureRadixIndex::ChildTable* CedarPureRadixIndex::CopyTableWithInsertedChild(
    const ChildTable* old, uint8_t nibble, Node* child) {
  assert(ChildAt(old, nibble) == nullptr && child != nullptr);
  std::array<Node*, 16> packed{};
  const size_t rank = std::popcount(static_cast<uint16_t>(old->occupied & Below(nibble)));
  for (size_t i = 0; i < rank; ++i) packed[i] = old->children[i];
  packed[rank] = child;
  for (size_t i = rank; i < old->child_count; ++i) packed[i + 1] = old->children[i];
  return AllocateChildTable(static_cast<uint16_t>(old->occupied | Mask(nibble)),
                            packed.data());
}
CedarPureRadixIndex::ChildTable* CedarPureRadixIndex::CopyTableWithReplacedChild(
    const ChildTable* old, uint8_t nibble, Node* child) {
  assert(ChildAt(old, nibble) != nullptr && child != nullptr);
  std::array<Node*, 16> packed{};
  for (size_t i = 0; i < old->child_count; ++i) packed[i] = old->children[i];
  packed[std::popcount(static_cast<uint16_t>(old->occupied & Below(nibble)))] = child;
  return AllocateChildTable(old->occupied, packed.data());
}

bool CedarPureRadixIndex::ReplaceTable(Branch* branch, ChildTable* observed,
                                        ChildTable* replacement) const {
  assert(branch != nullptr && observed != nullptr && replacement != nullptr);
  if (test_hooks_.table_snapshot_cas_for_testing) {
    test_hooks_.table_snapshot_cas_for_testing();
  }
  return branch->children.compare_exchange_strong(
      observed, replacement, std::memory_order_release,
      std::memory_order_acquire);
}

CedarPureRadixIndex::Leaf* CedarPureRadixIndex::MinimumLeaf(Node* node) {
  while (node != nullptr && node->kind == NodeKind::kBranch) {
    auto* table = AsBranch(node)->children.load(std::memory_order_acquire);
    node = ChildAt(table, FirstChildAtOrAfter(table, 0));
  }
  return node == nullptr ? nullptr : AsLeaf(node);
}
CedarPureRadixIndex::Leaf* CedarPureRadixIndex::MaximumLeaf(Node* node) {
  while (node != nullptr && node->kind == NodeKind::kBranch) {
    auto* table = AsBranch(node)->children.load(std::memory_order_acquire);
    node = ChildAt(table, LastChildAtOrBefore(table, 15));
  }
  return node == nullptr ? nullptr : AsLeaf(node);
}
CedarPureRadixIndex::Branch* CedarPureRadixIndex::AllocateBranch(
    uint8_t index, Node* old, Leaf* added) {
  auto* old_min = MinimumLeaf(old);
  auto* old_max = MaximumLeaf(old);
  const auto old_nibble = NibbleAt(old_min, index);
  const auto new_nibble = NibbleAt(added, index);
  assert(index < kNibbles && old_nibble != new_nibble);
  auto* branch = new (allocator_->AllocateAligned(sizeof(Branch))) Branch();
  branch->nibble_index = index;
  std::array<Node*, 2> children{};
  if (old_nibble < new_nibble) children = {old, added};
  else children = {added, old};
  branch->children.store(AllocateChildTable(static_cast<uint16_t>(
      Mask(old_nibble) | Mask(new_nibble)), children.data()), std::memory_order_relaxed);
  branch->min_leaf.store(Compare(added, old_min) < 0 ? added : old_min,
                         std::memory_order_relaxed);
  branch->max_leaf.store(Compare(added, old_max) > 0 ? added : old_max,
                         std::memory_order_relaxed);
  return branch;
}

CedarPureRadixIndex::Leaf* CedarPureRadixIndex::FindLeaf(const Key& key) const {
  Node* node = root_.load(std::memory_order_acquire);
  while (node != nullptr && node->kind == NodeKind::kBranch) {
    const auto* branch = AsBranch(node);
    auto* table = branch->children.load(std::memory_order_acquire);
    node = ChildAt(table, NibbleAt(key, branch->nibble_index));
  }
  return node == nullptr ? nullptr : AsLeaf(node);
}

void CedarPureRadixIndex::PublishBoundaryUpdates(const Key&, Leaf* leaf,
                                                  const TraversalFrame* path,
                                                  size_t depth) const {
  for (size_t i = 0; i < depth; ++i) {
    auto* branch = path[i].branch;
    if (test_hooks_.boundary_candidate_for_testing) test_hooks_.boundary_candidate_for_testing();
    auto* min = branch->min_leaf.load(std::memory_order_acquire);
    while (min != nullptr && Compare(leaf, min) < 0 &&
           !branch->min_leaf.compare_exchange_weak(min, leaf, std::memory_order_release,
                                                    std::memory_order_acquire)) {}
    if (test_hooks_.boundary_candidate_for_testing) test_hooks_.boundary_candidate_for_testing();
    auto* max = branch->max_leaf.load(std::memory_order_acquire);
    while (max != nullptr && Compare(leaf, max) > 0 &&
           !branch->max_leaf.compare_exchange_weak(max, leaf, std::memory_order_release,
                                                    std::memory_order_acquire)) {}
  }
}

bool CedarPureRadixIndex::Insert(void* opaque, const Key& key) {
  auto* copied = allocator_->AllocateAligned(kKeyBytes);
  std::memcpy(copied, key.data(), kKeyBytes);
  return InsertWithBorrowedKey(opaque, key,
                               reinterpret_cast<const unsigned char*>(copied));
}
bool CedarPureRadixIndex::InsertWithBorrowedKey(void* opaque, const Key& key,
                                                const unsigned char* prefix) {
  auto* handle = static_cast<Handle*>(opaque);
  if (handle == nullptr || prefix == nullptr) return false;
  bool unsubmitted = false;
  if (!handle->leaf.submitted.compare_exchange_strong(unsubmitted, true,
                                                       std::memory_order_acq_rel,
                                                       std::memory_order_acquire)) return false;
  handle->leaf.key_prefix = prefix;
  std::memcpy(handle->leaf.normalized_suffix.data(), key.data() + 32, 8);
  handle->leaf.frozen_next = nullptr;

  for (;;) {
    std::array<TraversalFrame, kNibbles> path{};
    size_t depth = 0;
    Node* node = root_.load(std::memory_order_acquire);
    while (node != nullptr && node->kind == NodeKind::kBranch) {
      auto* branch = AsBranch(node);
      // A branch below the first differing nibble cannot receive this leaf
      // directly. Stop before it so the branch is wrapped as one old subtree.
      if (FirstDifferingNibble(
              branch->min_leaf.load(std::memory_order_acquire), key) <
          branch->nibble_index) {
        break;
      }
      auto* table = branch->children.load(std::memory_order_acquire);
      const auto nibble = NibbleAt(key, branch->nibble_index);
      auto* child = ChildAt(table, nibble);
      assert(depth < path.size());
      path[depth++] = {branch, table, nibble, child};
      if (test_hooks_.branch_load_for_testing) test_hooks_.branch_load_for_testing();
      node = child;
    }
    if (node == nullptr) {
      if (depth == 0) {
        if (test_hooks_.before_cas) test_hooks_.before_cas();
        Node* expected = nullptr;
        if (root_.compare_exchange_strong(expected, &handle->leaf, std::memory_order_release,
                                          std::memory_order_acquire)) {
          if (test_hooks_.after_cas) test_hooks_.after_cas();
          return true;
        }
      } else {
        auto& parent = path[depth - 1];
        auto* replacement = CopyTableWithInsertedChild(parent.table, parent.nibble,
                                                        &handle->leaf);
        if (test_hooks_.before_cas) test_hooks_.before_cas();
        if (ReplaceTable(parent.branch, parent.table, replacement)) {
          PublishBoundaryUpdates(key, &handle->leaf, path.data(), depth);
          if (test_hooks_.after_cas) test_hooks_.after_cas();
          return true;
        }
      }
      continue;
    }
    auto* matched = node->kind == NodeKind::kLeaf ? AsLeaf(node)
                                                   : MinimumLeaf(node);
    const auto differing = FirstDifferingNibble(matched, key);
    if (differing == kNibbles) return false;
    size_t wrapper = 0;
    while (wrapper < depth && path[wrapper].branch->nibble_index < differing) ++wrapper;
    if (wrapper < depth && path[wrapper].branch->nibble_index == differing) {
      auto& direct = path[wrapper];
      const auto nibble = NibbleAt(key, differing);
      if (ChildAt(direct.table, nibble) == nullptr) {
        auto* replacement = CopyTableWithInsertedChild(direct.table, nibble, &handle->leaf);
        if (test_hooks_.before_cas) test_hooks_.before_cas();
        if (ReplaceTable(direct.branch, direct.table, replacement)) {
          PublishBoundaryUpdates(key, &handle->leaf, path.data(), wrapper + 1);
          if (test_hooks_.after_cas) test_hooks_.after_cas();
          return true;
        }
      }
      continue;
    }
    Node* old = wrapper == depth ? node
                                 : static_cast<Node*>(path[wrapper].branch);
    auto* replacement = AllocateBranch(differing, old, &handle->leaf);
    if (wrapper == 0) {
      if (test_hooks_.before_cas) test_hooks_.before_cas();
      Node* expected = old;
      if (root_.compare_exchange_strong(expected, replacement, std::memory_order_release,
                                        std::memory_order_acquire)) {
        if (test_hooks_.after_cas) test_hooks_.after_cas();
        return true;
      }
    } else {
      auto& parent = path[wrapper - 1];
      auto* table = CopyTableWithReplacedChild(parent.table, parent.nibble, replacement);
      if (test_hooks_.before_cas) test_hooks_.before_cas();
      if (ReplaceTable(parent.branch, parent.table, table)) {
        PublishBoundaryUpdates(key, &handle->leaf, path.data(), wrapper);
        if (test_hooks_.after_cas) test_hooks_.after_cas();
        return true;
      }
    }
  }
}

bool CedarPureRadixIndex::Contains(const Key& key) const {
  const auto* leaf = FindLeaf(key);
  return leaf != nullptr && Compare(leaf, key) == 0;
}
CedarPureRadixIndex::StructureStatsForTesting
CedarPureRadixIndex::GetStructureStatsForTesting() const {
  StructureStatsForTesting stats;
  auto visit = [&](auto&& self, const Node* node, size_t depth) -> void {
    if (node == nullptr || node->kind == NodeKind::kLeaf) return;
    const auto* branch = AsBranch(node);
    auto* table = branch->children.load(std::memory_order_acquire);
    ++stats.branches;
    ++stats.child_tables;
    stats.max_depth = std::max(stats.max_depth, depth);
    for (size_t rank = 0; rank < table->child_count; ++rank)
      self(self, table->children[rank], depth + 1);
  };
  visit(visit, root_.load(std::memory_order_acquire), 1);
  return stats;
}

void CedarPureRadixIndex::MarkReadOnly() { readonly_.store(true, std::memory_order_release); }
void CedarPureRadixIndex::PrepareForFlush() { TryBuildFrozenChain(); }
bool CedarPureRadixIndex::TryBuildFrozenChain() const {
  if (!readonly_.load(std::memory_order_acquire)) return false;
  ChainState state = chain_state_.load(std::memory_order_acquire);
  if (state == ChainState::kReady) return true;
  if (state != ChainState::kUnbuilt || !chain_state_.compare_exchange_strong(
          state, ChainState::kBuilding, std::memory_order_acq_rel,
          std::memory_order_acquire)) return state == ChainState::kReady;
  if (test_hooks_.frozen_chain_builder) test_hooks_.frozen_chain_builder();
  struct Frame { const Branch* branch; uint8_t nibble; };
  std::array<Frame, kNibbles> path{};
  size_t depth = 0;
  Node* node = root_.load(std::memory_order_acquire);
  Leaf* first = nullptr;
  Leaf* previous = nullptr;
  while (node != nullptr || depth != 0) {
    while (node != nullptr && node->kind == NodeKind::kBranch) {
      auto* branch = AsBranch(node);
      auto* table = branch->children.load(std::memory_order_acquire);
      const auto nibble = FirstChildAtOrAfter(table, 0);
      assert(nibble < 16 && depth < path.size());
      path[depth++] = {branch, nibble};
      node = ChildAt(table, nibble);
    }
    if (node != nullptr) {
      auto* leaf = AsLeaf(node);
      if (first == nullptr) first = leaf;
      if (previous != nullptr) previous->frozen_next = leaf;
      previous = leaf;
      node = nullptr;
    }
    while (node == nullptr && depth != 0) {
      auto& frame = path[depth - 1];
      auto* table = const_cast<Branch*>(frame.branch)->children.load(std::memory_order_acquire);
      const auto next = frame.nibble == 15 ? 16 : FirstChildAtOrAfter(
          table, static_cast<uint8_t>(frame.nibble + 1));
      if (next < 16) { frame.nibble = next; node = ChildAt(table, next); break; }
      --depth;
    }
  }
  if (previous != nullptr) previous->frozen_next = nullptr;
  frozen_first_.store(first, std::memory_order_relaxed);
  chain_state_.store(ChainState::kReady, std::memory_order_release);
  return true;
}
CedarPureRadixIndex::Leaf* CedarPureRadixIndex::FirstFrozenLeaf() const {
  return frozen_first_.load(std::memory_order_relaxed);
}

CedarPureRadixIndex::Cursor::Cursor(const CedarPureRadixIndex* index) : index_(index) {}
const char* CedarPureRadixIndex::Cursor::entry() const { assert(Valid()); return leaf_->entry; }
void CedarPureRadixIndex::Cursor::Clear() { depth_ = 0; leaf_ = nullptr; frozen_chain_mode_ = false; }
void CedarPureRadixIndex::Cursor::DescendLeft(const Node* node) {
  while (node != nullptr && node->kind == NodeKind::kBranch) {
    const auto* branch = AsBranch(node);
    auto* table = branch->children.load(std::memory_order_acquire);
    const auto nibble = FirstChildAtOrAfter(table, 0);
    assert(nibble < 16 && depth_ < branches_.size());
    branches_[depth_] = branch; directions_[depth_++] = nibble;
    node = ChildAt(table, nibble);
  }
  leaf_ = node == nullptr ? nullptr : AsLeaf(node);
}
void CedarPureRadixIndex::Cursor::DescendRight(const Node* node) {
  while (node != nullptr && node->kind == NodeKind::kBranch) {
    const auto* branch = AsBranch(node);
    auto* table = branch->children.load(std::memory_order_acquire);
    const auto nibble = LastChildAtOrBefore(table, 15);
    assert(nibble < 16 && depth_ < branches_.size());
    branches_[depth_] = branch; directions_[depth_++] = nibble;
    node = ChildAt(table, nibble);
  }
  leaf_ = node == nullptr ? nullptr : AsLeaf(node);
}
void CedarPureRadixIndex::Cursor::SeekToFirst() {
  Clear();
  if (index_->TryBuildFrozenChain()) { frozen_chain_mode_ = true; leaf_ = index_->FirstFrozenLeaf(); }
  else DescendLeft(index_->root_.load(std::memory_order_acquire));
}
void CedarPureRadixIndex::Cursor::SeekToLast() { Clear(); DescendRight(index_->root_.load(std::memory_order_acquire)); }
bool CedarPureRadixIndex::Cursor::SeekLowerBound(const Node* node, const Key& key) {
  const Node* current = node;
  while (current != nullptr && current->kind == NodeKind::kBranch) {
    const auto* branch = AsBranch(current);
    auto* table = branch->children.load(std::memory_order_acquire);
    const auto* minimum = branch->min_leaf.load(std::memory_order_acquire);
    const auto* maximum = branch->max_leaf.load(std::memory_order_acquire);
    assert(minimum != nullptr && maximum != nullptr);
    if (Compare(minimum, key) >= 0) {
      const auto first = FirstChildAtOrAfter(table, 0);
      assert(first < 16 && depth_ < branches_.size());
      branches_[depth_] = branch;
      directions_[depth_++] = first;
      DescendLeft(ChildAt(table, first));
      return Valid();
    }
    if (Compare(maximum, key) < 0) {
      NextPath();
      return Valid();
    }
    const auto target = NibbleAt(key, branch->nibble_index);
    const auto selected = FirstChildAtOrAfter(table, target);
    if (selected == 16) { NextPath(); return Valid(); }
    branches_[depth_] = branch; directions_[depth_++] = selected;
    if (selected > target) { DescendLeft(ChildAt(table, selected)); return Valid(); }
    current = ChildAt(table, selected);
  }
  if (current == nullptr) return false;
  leaf_ = AsLeaf(current);
  if (Compare(leaf_, key) >= 0) return true;
  NextPath(); return Valid();
}
bool CedarPureRadixIndex::Cursor::SeekUpperBound(const Node* node, const Key& key) {
  const Node* current = node;
  while (current != nullptr && current->kind == NodeKind::kBranch) {
    const auto* branch = AsBranch(current);
    auto* table = branch->children.load(std::memory_order_acquire);
    const auto* minimum = branch->min_leaf.load(std::memory_order_acquire);
    const auto* maximum = branch->max_leaf.load(std::memory_order_acquire);
    assert(minimum != nullptr && maximum != nullptr);
    if (Compare(maximum, key) <= 0) {
      const auto last = LastChildAtOrBefore(table, 15);
      assert(last < 16 && depth_ < branches_.size());
      branches_[depth_] = branch;
      directions_[depth_++] = last;
      DescendRight(ChildAt(table, last));
      return Valid();
    }
    if (Compare(minimum, key) > 0) {
      PrevPath();
      return Valid();
    }
    const auto target = NibbleAt(key, branch->nibble_index);
    const auto selected = LastChildAtOrBefore(table, target);
    if (selected == 16) { PrevPath(); return Valid(); }
    branches_[depth_] = branch; directions_[depth_++] = selected;
    if (selected < target) { DescendRight(ChildAt(table, selected)); return Valid(); }
    current = ChildAt(table, selected);
  }
  if (current == nullptr) return false;
  leaf_ = AsLeaf(current);
  if (Compare(leaf_, key) <= 0) return true;
  PrevPath(); return Valid();
}
void CedarPureRadixIndex::Cursor::SeekPath(const Key& key, bool previous) {
  Clear();
  const bool found = previous ? SeekUpperBound(index_->root_.load(std::memory_order_acquire), key)
                              : SeekLowerBound(index_->root_.load(std::memory_order_acquire), key);
  if (!found) Clear();
}
void CedarPureRadixIndex::Cursor::Seek(const Key& key) {
  SeekPath(key, false);
  frozen_chain_mode_ = Valid() && index_->chain_state_.load(std::memory_order_acquire) == ChainState::kReady;
}
void CedarPureRadixIndex::Cursor::SeekForPrev(const Key& key) {
  SeekPath(key, true);
  frozen_chain_mode_ = Valid() && index_->chain_state_.load(std::memory_order_acquire) == ChainState::kReady;
}
void CedarPureRadixIndex::Cursor::NextPath() {
  for (size_t i = depth_; i > 0; --i) {
    const auto* branch = branches_[i - 1];
    auto* table = branch->children.load(std::memory_order_acquire);
    const auto current = directions_[i - 1];
    const auto next = current == 15 ? 16 : FirstChildAtOrAfter(table, static_cast<uint8_t>(current + 1));
    if (next == 16) continue;
    depth_ = i; directions_[i - 1] = next; DescendLeft(ChildAt(table, next)); return;
  }
  leaf_ = nullptr; depth_ = 0;
}
void CedarPureRadixIndex::Cursor::PrevPath() {
  for (size_t i = depth_; i > 0; --i) {
    const auto* branch = branches_[i - 1];
    auto* table = branch->children.load(std::memory_order_acquire);
    const auto current = directions_[i - 1];
    const auto previous = current == 0 ? 16 : LastChildAtOrBefore(table, static_cast<uint8_t>(current - 1));
    if (previous == 16) continue;
    depth_ = i; directions_[i - 1] = previous; DescendRight(ChildAt(table, previous)); return;
  }
  leaf_ = nullptr; depth_ = 0;
}
void CedarPureRadixIndex::Cursor::Next() {
  assert(Valid());
  if (frozen_chain_mode_) leaf_ = leaf_->frozen_next; else NextPath();
}
void CedarPureRadixIndex::Cursor::Prev() {
  assert(Valid());
  if (frozen_chain_mode_) {
    Key key;
    std::memcpy(key.data(), leaf_->key_prefix, 32);
    std::memcpy(key.data() + 32, leaf_->normalized_suffix.data(), 8);
    frozen_chain_mode_ = false;
    SeekPath(key, true);
  }
  PrevPath();
}
}  // namespace ROCKSDB_NAMESPACE
