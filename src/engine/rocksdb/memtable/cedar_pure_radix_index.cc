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
uint8_t Mask(uint8_t local) {
  assert(local < 8);
  return static_cast<uint8_t>(uint8_t{1} << local);
}
uint8_t Below(uint8_t local) {
  assert(local < 8);
  return static_cast<uint8_t>((uint8_t{1} << local) - 1);
}
uint8_t Through(uint8_t local) {
  assert(local < 8);
  return local == 7
             ? uint8_t{0xff}
             : static_cast<uint8_t>((uint8_t{1} << (local + 1)) - 1);
}
}  // namespace

CedarPureRadixIndex::CedarPureRadixIndex(Allocator* allocator, TestHooks hooks)
    : allocator_(allocator), test_hooks_(std::move(hooks)) {
  assert(std::atomic<Node*>::is_always_lock_free);
  assert(std::atomic<ChildBlock*>::is_always_lock_free);
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

uint8_t CedarPureRadixIndex::ByteAt(const Key& key, uint8_t index) {
  assert(index < kKeyBytes);
  return key[index];
}

uint8_t CedarPureRadixIndex::ByteAt(const Leaf* leaf, uint8_t index) {
  assert(leaf != nullptr && leaf->key_prefix != nullptr && index < kKeyBytes);
  return index < 32 ? leaf->key_prefix[index]
                    : leaf->normalized_suffix[index - 32];
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

uint8_t CedarPureRadixIndex::FirstDifferingByte(const Key& left,
                                                 const Key& right) {
  for (size_t byte = 0; byte < kKeyBytes; ++byte) {
    if (left[byte] != right[byte]) return static_cast<uint8_t>(byte);
  }
  return static_cast<uint8_t>(kKeyBytes);
}
uint8_t CedarPureRadixIndex::FirstDifferingByte(const Leaf* left,
                                                 const Key& right) {
  for (size_t byte = 0; byte < kKeyBytes; ++byte) {
    if (ByteAt(left, static_cast<uint8_t>(byte)) != right[byte]) {
      return static_cast<uint8_t>(byte);
    }
  }
  return static_cast<uint8_t>(kKeyBytes);
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

// A branch owns 32 independent publication edges. A block's local bitmap
// packs only the children for its eight-byte-value interval.
CedarPureRadixIndex::ChildBlock* CedarPureRadixIndex::BlockAt(
    const Branch* branch, uint8_t value) {
  assert(branch != nullptr);
  return branch->segments[SegmentForByte(value)].load(std::memory_order_acquire);
}
CedarPureRadixIndex::Node* CedarPureRadixIndex::ChildAt(const ChildBlock* block,
                                                         uint8_t local) {
  assert(local < 8);
  if (block == nullptr || (block->occupied & Mask(local)) == 0)
    return nullptr;
  const auto rank = std::popcount(
      static_cast<uint8_t>(block->occupied & Below(local)));
  assert(rank < block->child_count);
  return block->children[rank];
}
CedarPureRadixIndex::Node* CedarPureRadixIndex::ChildAt(const Branch* branch,
                                                         uint8_t value) {
  return ChildAt(BlockAt(branch, value), LocalByte(value));
}
uint16_t CedarPureRadixIndex::FirstChildAtOrAfter(const Branch* branch,
                                                   uint8_t value) {
  assert(branch != nullptr);
  for (uint8_t segment = SegmentForByte(value);
       segment < kByteSegmentCount; ++segment) {
    auto* block = branch->segments[segment].load(std::memory_order_acquire);
    const uint8_t local = segment == SegmentForByte(value) ? LocalByte(value) : 0;
    const auto occupied = block == nullptr ? uint8_t{0} : block->occupied;
    const auto set = static_cast<uint8_t>(occupied & ~Below(local));
    if (set != 0) {
      return static_cast<uint16_t>(segment * 8 + std::countr_zero(set));
    }
  }
  return 256;
}
uint16_t CedarPureRadixIndex::LastChildAtOrBefore(const Branch* branch,
                                                   uint8_t value) {
  assert(branch != nullptr);
  for (int segment = SegmentForByte(value); segment >= 0; --segment) {
    auto* block = branch->segments[segment].load(std::memory_order_acquire);
    const uint8_t local = segment == SegmentForByte(value) ? LocalByte(value) : 7;
    const auto occupied = block == nullptr ? uint8_t{0} : block->occupied;
    const auto set = static_cast<uint8_t>(occupied & Through(local));
    if (set != 0) {
      return static_cast<uint16_t>(segment * 8 + 7 - std::countl_zero(set));
    }
  }
  return 256;
}

CedarPureRadixIndex::ChildBlock* CedarPureRadixIndex::AllocateChildBlock(
    uint8_t occupied, Node* const* children) {
  const auto count = static_cast<uint8_t>(std::popcount(occupied));
  assert(count > 0 && count <= 8);
  const auto bytes = offsetof(ChildBlock, children) + size_t{count} * sizeof(Node*);
  auto* block = reinterpret_cast<ChildBlock*>(allocator_->AllocateAligned(bytes));
  if (test_hooks_.block_allocation_bytes_for_testing) {
    test_hooks_.block_allocation_bytes_for_testing(bytes);
  }
  block->occupied = occupied;
  block->child_count = count;
  for (size_t i = 0; i < count; ++i) {
    assert(children[i] != nullptr);
    block->children[i] = children[i];
  }
  return block;
}
CedarPureRadixIndex::ChildBlock* CedarPureRadixIndex::CopyBlockWithInsertedChild(
    const ChildBlock* old, uint8_t value, Node* child) {
  assert(child != nullptr);
  const uint8_t local = LocalByte(value);
  assert(ChildAt(old, local) == nullptr);
  std::array<Node*, 8> packed{};
  const auto old_occupied = old == nullptr ? uint8_t{0} : old->occupied;
  const size_t rank =
      std::popcount(static_cast<uint8_t>(old_occupied & Below(local)));
  const size_t count = old == nullptr ? 0 : old->child_count;
  if (test_hooks_.copied_child_pointers_for_testing) {
    test_hooks_.copied_child_pointers_for_testing(count);
  }
  for (size_t i = 0; i < rank; ++i) packed[i] = old->children[i];
  packed[rank] = child;
  for (size_t i = rank; i < count; ++i) packed[i + 1] = old->children[i];
  return AllocateChildBlock(old_occupied | Mask(local), packed.data());
}
CedarPureRadixIndex::ChildBlock* CedarPureRadixIndex::CopyBlockWithReplacedChild(
    const ChildBlock* old, uint8_t value, Node* child) {
  assert(old != nullptr && child != nullptr);
  const uint8_t local = LocalByte(value);
  assert(ChildAt(old, local) != nullptr);
  std::array<Node*, 8> packed{};
  if (test_hooks_.copied_child_pointers_for_testing) {
    test_hooks_.copied_child_pointers_for_testing(old->child_count);
  }
  for (size_t i = 0; i < old->child_count; ++i) packed[i] = old->children[i];
  packed[std::popcount(
      static_cast<uint8_t>(old->occupied & Below(local)))] = child;
  return AllocateChildBlock(old->occupied, packed.data());
}

bool CedarPureRadixIndex::ReplaceBlock(Branch* branch, uint8_t segment,
                                        ChildBlock* observed,
                                        ChildBlock* replacement) const {
  assert(branch != nullptr && segment < kByteSegmentCount &&
         replacement != nullptr);
  if (test_hooks_.table_snapshot_cas_for_testing) {
    test_hooks_.table_snapshot_cas_for_testing();
  }
  const bool published = branch->segments[segment].compare_exchange_strong(
      observed, replacement, std::memory_order_release,
      std::memory_order_acquire);
  if (test_hooks_.segment_cas_result_for_testing) {
    test_hooks_.segment_cas_result_for_testing(published);
  }
  return published;
}

CedarPureRadixIndex::Leaf* CedarPureRadixIndex::MinimumLeaf(Node* node) {
  while (node != nullptr && node->kind == NodeKind::kBranch) {
    auto* branch = AsBranch(node);
    node = ChildAt(branch,
                   static_cast<uint8_t>(FirstChildAtOrAfter(branch, 0)));
  }
  return node == nullptr ? nullptr : AsLeaf(node);
}
CedarPureRadixIndex::Leaf* CedarPureRadixIndex::MaximumLeaf(Node* node) {
  while (node != nullptr && node->kind == NodeKind::kBranch) {
    auto* branch = AsBranch(node);
    node = ChildAt(branch, static_cast<uint8_t>(LastChildAtOrBefore(branch, 255)));
  }
  return node == nullptr ? nullptr : AsLeaf(node);
}
CedarPureRadixIndex::Branch* CedarPureRadixIndex::AllocateBranch(
    uint8_t index, Node* old, Leaf* added) {
  auto* old_min = MinimumLeaf(old);
  auto* old_max = MaximumLeaf(old);
  const auto old_byte = ByteAt(old_min, index);
  const auto new_byte = ByteAt(added, index);
  assert(index < kKeyBytes && old_byte != new_byte);
  auto* branch = new (allocator_->AllocateAligned(sizeof(Branch))) Branch();
  if (test_hooks_.branch_allocation_bytes_for_testing) {
    test_hooks_.branch_allocation_bytes_for_testing(sizeof(Branch));
  }
  branch->byte_index = index;
  std::array<std::array<Node*, 2>, kByteSegmentCount> children{};
  std::array<uint8_t, kByteSegmentCount> occupied{};
  const auto add_child = [&](uint8_t value, Node* child) {
    const auto segment = SegmentForByte(value);
    const auto local = LocalByte(value);
    const size_t rank = std::popcount(
        static_cast<uint8_t>(occupied[segment] & Below(local)));
    const size_t count = std::popcount(occupied[segment]);
    for (size_t i = count; i > rank; --i) children[segment][i] = children[segment][i - 1];
    children[segment][rank] = child;
    occupied[segment] |= Mask(local);
  };
  add_child(old_byte, old);
  add_child(new_byte, added);
  for (uint8_t segment = 0; segment < kByteSegmentCount; ++segment) {
    if (occupied[segment] != 0)
      branch->segments[segment].store(AllocateChildBlock(occupied[segment],
                                                          children[segment].data()),
                                       std::memory_order_relaxed);
  }
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
    node = ChildAt(branch, ByteAt(key, branch->byte_index));
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
    // Frames are written before depth advances; all consumers read only
    // [0, depth), so avoid clearing the unused tail on every insertion.
    std::array<TraversalFrame, kKeyBytes> path;
    size_t depth = 0;
    Node* node = root_.load(std::memory_order_acquire);
    while (node != nullptr && node->kind == NodeKind::kBranch) {
      auto* branch = AsBranch(node);
      // A branch below the first differing byte cannot receive this leaf
      // directly. Stop before it so the branch is wrapped as one old subtree.
      if (FirstDifferingByte(
              branch->min_leaf.load(std::memory_order_acquire), key) <
          branch->byte_index) {
        break;
      }
      const auto value = ByteAt(key, branch->byte_index);
      const auto segment = SegmentForByte(value);
      auto* block = BlockAt(branch, value);
      auto* child = ChildAt(block, LocalByte(value));
      assert(depth < path.size());
      path[depth++] = {branch, segment, block, value, child};
      if (test_hooks_.branch_load_for_testing) test_hooks_.branch_load_for_testing();
      node = child;
    }
    if (node == nullptr) {
      if (depth == 0) {
        if (test_hooks_.before_cas) test_hooks_.before_cas();
        Node* expected = nullptr;
        const bool published = root_.compare_exchange_strong(
            expected, &handle->leaf, std::memory_order_release,
            std::memory_order_acquire);
        if (test_hooks_.root_cas_result_for_testing) {
          test_hooks_.root_cas_result_for_testing(published);
        }
        if (published) {
          if (test_hooks_.after_cas) test_hooks_.after_cas();
          return true;
        }
      } else {
        auto& parent = path[depth - 1];
        auto* replacement = CopyBlockWithInsertedChild(parent.block, parent.byte,
                                                        &handle->leaf);
        if (test_hooks_.before_cas) test_hooks_.before_cas();
        if (ReplaceBlock(parent.branch, parent.segment, parent.block, replacement)) {
          PublishBoundaryUpdates(key, &handle->leaf, path.data(), depth);
          if (test_hooks_.after_cas) test_hooks_.after_cas();
          return true;
        }
      }
      continue;
    }
    auto* matched = node->kind == NodeKind::kLeaf ? AsLeaf(node)
                                                   : MinimumLeaf(node);
    const auto differing = FirstDifferingByte(matched, key);
    if (differing == kKeyBytes) return false;
    size_t wrapper = 0;
    while (wrapper < depth && path[wrapper].branch->byte_index < differing) ++wrapper;
    if (wrapper < depth && path[wrapper].branch->byte_index == differing) {
      auto& direct = path[wrapper];
      const auto value = ByteAt(key, differing);
      if (ChildAt(direct.block, LocalByte(value)) == nullptr) {
        auto* replacement = CopyBlockWithInsertedChild(direct.block, value, &handle->leaf);
        if (test_hooks_.before_cas) test_hooks_.before_cas();
        if (ReplaceBlock(direct.branch, direct.segment, direct.block, replacement)) {
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
      const bool published = root_.compare_exchange_strong(
          expected, replacement, std::memory_order_release,
          std::memory_order_acquire);
      if (test_hooks_.root_cas_result_for_testing) {
        test_hooks_.root_cas_result_for_testing(published);
      }
      if (published) {
        if (test_hooks_.after_cas) test_hooks_.after_cas();
        return true;
      }
    } else {
      auto& parent = path[wrapper - 1];
      auto* block = CopyBlockWithReplacedChild(parent.block, parent.byte,
                                                replacement);
      if (test_hooks_.before_cas) test_hooks_.before_cas();
      if (ReplaceBlock(parent.branch, parent.segment, parent.block, block)) {
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
    ++stats.branches;
    ++stats.byte_branches;
    stats.max_depth = std::max(stats.max_depth, depth);
    for (uint8_t segment = 0; segment < kByteSegmentCount; ++segment) {
      auto* block = branch->segments[segment].load(std::memory_order_acquire);
      if (block == nullptr) continue;
      ++stats.child_tables;
      ++stats.child_blocks;
      stats.byte_segment_occupied[segment] |= block->occupied;
      stats.byte_segment_child_counts[segment] = static_cast<uint8_t>(
          stats.byte_segment_child_counts[segment] + block->child_count);
      for (size_t rank = 0; rank < block->child_count; ++rank)
        self(self, block->children[rank], depth + 1);
    }
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
  struct Frame { const Branch* branch; uint8_t value; };
  std::array<Frame, kKeyBytes> path{};
  size_t depth = 0;
  Node* node = root_.load(std::memory_order_acquire);
  Leaf* first = nullptr;
  Leaf* previous = nullptr;
  while (node != nullptr || depth != 0) {
    while (node != nullptr && node->kind == NodeKind::kBranch) {
      auto* branch = AsBranch(node);
      const auto value = FirstChildAtOrAfter(branch, 0);
      assert(value < 256 && depth < path.size());
      path[depth++] = {branch, static_cast<uint8_t>(value)};
      node = ChildAt(branch, static_cast<uint8_t>(value));
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
      const auto next = frame.value == 255 ? 256 : FirstChildAtOrAfter(
          frame.branch, static_cast<uint8_t>(frame.value + 1));
      if (next < 256) {
        frame.value = static_cast<uint8_t>(next);
        node = ChildAt(frame.branch, static_cast<uint8_t>(next));
        break;
      }
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
    if (index_->test_hooks_.branch_load_for_testing) {
      index_->test_hooks_.branch_load_for_testing();
    }
    const auto value = FirstChildAtOrAfter(branch, 0);
    assert(value < 256 && depth_ < branches_.size());
    branches_[depth_] = branch;
    directions_[depth_++] = static_cast<uint8_t>(value);
    node = ChildAt(branch, static_cast<uint8_t>(value));
  }
  leaf_ = node == nullptr ? nullptr : AsLeaf(node);
}
void CedarPureRadixIndex::Cursor::DescendRight(const Node* node) {
  while (node != nullptr && node->kind == NodeKind::kBranch) {
    const auto* branch = AsBranch(node);
    if (index_->test_hooks_.branch_load_for_testing) {
      index_->test_hooks_.branch_load_for_testing();
    }
    const auto value = LastChildAtOrBefore(branch, 255);
    assert(value < 256 && depth_ < branches_.size());
    branches_[depth_] = branch;
    directions_[depth_++] = static_cast<uint8_t>(value);
    node = ChildAt(branch, static_cast<uint8_t>(value));
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
    if (index_->test_hooks_.branch_load_for_testing) {
      index_->test_hooks_.branch_load_for_testing();
    }
    const auto* minimum = branch->min_leaf.load(std::memory_order_acquire);
    const auto* maximum = branch->max_leaf.load(std::memory_order_acquire);
    assert(minimum != nullptr && maximum != nullptr);
    if (Compare(minimum, key) >= 0) {
      const auto first = FirstChildAtOrAfter(branch, 0);
      assert(first < 256 && depth_ < branches_.size());
      branches_[depth_] = branch;
      directions_[depth_++] = static_cast<uint8_t>(first);
      DescendLeft(ChildAt(branch, static_cast<uint8_t>(first)));
      return Valid();
    }
    if (Compare(maximum, key) < 0) {
      NextPath();
      return Valid();
    }
    const auto target = ByteAt(key, branch->byte_index);
    const auto selected = FirstChildAtOrAfter(branch, target);
    if (selected == 256) { NextPath(); return Valid(); }
    branches_[depth_] = branch;
    directions_[depth_++] = static_cast<uint8_t>(selected);
    if (selected > target) {
      DescendLeft(ChildAt(branch, static_cast<uint8_t>(selected)));
      return Valid();
    }
    current = ChildAt(branch, static_cast<uint8_t>(selected));
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
    if (index_->test_hooks_.branch_load_for_testing) {
      index_->test_hooks_.branch_load_for_testing();
    }
    const auto* minimum = branch->min_leaf.load(std::memory_order_acquire);
    const auto* maximum = branch->max_leaf.load(std::memory_order_acquire);
    assert(minimum != nullptr && maximum != nullptr);
    if (Compare(maximum, key) <= 0) {
      const auto last = LastChildAtOrBefore(branch, 255);
      assert(last < 256 && depth_ < branches_.size());
      branches_[depth_] = branch;
      directions_[depth_++] = static_cast<uint8_t>(last);
      DescendRight(ChildAt(branch, static_cast<uint8_t>(last)));
      return Valid();
    }
    if (Compare(minimum, key) > 0) {
      PrevPath();
      return Valid();
    }
    const auto target = ByteAt(key, branch->byte_index);
    const auto selected = LastChildAtOrBefore(branch, target);
    if (selected == 256) { PrevPath(); return Valid(); }
    branches_[depth_] = branch;
    directions_[depth_++] = static_cast<uint8_t>(selected);
    if (selected < target) {
      DescendRight(ChildAt(branch, static_cast<uint8_t>(selected)));
      return Valid();
    }
    current = ChildAt(branch, static_cast<uint8_t>(selected));
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
    const auto current = directions_[i - 1];
    const auto next = current == 255 ? 256 : FirstChildAtOrAfter(
        branch, static_cast<uint8_t>(current + 1));
    if (next == 256) continue;
    depth_ = i;
    directions_[i - 1] = static_cast<uint8_t>(next);
    DescendLeft(ChildAt(branch, static_cast<uint8_t>(next)));
    return;
  }
  leaf_ = nullptr; depth_ = 0;
}
void CedarPureRadixIndex::Cursor::PrevPath() {
  for (size_t i = depth_; i > 0; --i) {
    const auto* branch = branches_[i - 1];
    const auto current = directions_[i - 1];
    const auto previous = current == 0 ? 256 : LastChildAtOrBefore(
        branch, static_cast<uint8_t>(current - 1));
    if (previous == 256) continue;
    depth_ = i;
    directions_[i - 1] = static_cast<uint8_t>(previous);
    DescendRight(ChildAt(branch, static_cast<uint8_t>(previous)));
    return;
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
