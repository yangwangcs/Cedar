// Copyright 2026 The Cedar Authors
// Licensed under the Apache License, Version 2.0.

#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>

#include "rocksdb/rocksdb_namespace.h"

namespace ROCKSDB_NAMESPACE {

class Allocator;

// An append-only Patricia radix tree for Cedar's normalized 40-byte internal
// keys. Nodes are never reclaimed until the owning MemTable is destroyed.
class CedarPureRadixIndex {
 public:
  static constexpr size_t kKeyBytes = 40;
  static constexpr size_t kMaxBits = kKeyBytes * 8;
  using Key = std::array<unsigned char, kKeyBytes>;

  struct TestHooks {
    std::function<void()> before_cas;
    std::function<void()> after_cas;
    std::function<void()> frozen_chain_builder;
    std::function<void()> branch_visit_for_testing;
    std::function<void()> branch_load_for_testing;
    std::function<void()> boundary_candidate_for_testing;
    std::function<void()> table_snapshot_cas_for_testing;
  };

  class Cursor;

  struct StructureStatsForTesting {
    size_t branches = 0;
    // Byte-branch representation contract, aggregated across all branches.
    size_t byte_branches = 0;
    std::array<uint64_t, 4> byte_segment_occupied{};
    std::array<uint8_t, 4> byte_segment_child_counts{};
    size_t child_tables = 0;  // Compatibility total for all child blocks.
    size_t child_blocks = 0;
    std::array<uint8_t, 4> segment_occupied{};
    std::array<uint8_t, 4> segment_child_counts{};
    size_t max_depth = 0;
  };

  explicit CedarPureRadixIndex(Allocator* allocator,
                               TestHooks test_hooks = {});

  static constexpr uint8_t SegmentForByte(uint8_t value) {
    return value >> 6;
  }

  // The opaque handle owns one leaf and one private branch candidate. The
  // returned buffer is the MemTable entry that the caller fills before Insert.
  void* Allocate(size_t length, char** buffer);
  const char* EntryForHandle(void* handle) const;
  bool Insert(void* handle, const Key& key);
  // The MemTable entry owns this canonical key prefix for the index lifetime.
  // Callers without such storage use Insert(), which copies the key into the
  // arena and preserves the standalone index ownership contract.
  bool InsertWithBorrowedKey(void* handle, const Key& key,
                             const unsigned char* key_prefix);
  bool Contains(const Key& key) const;
  StructureStatsForTesting GetStructureStatsForTesting() const;
  void MarkReadOnly();
  void PrepareForFlush();
  size_t ExternalMemoryUsage() const { return 0; }

 private:
  enum class NodeKind : uint8_t { kLeaf, kBranch };

  struct Node {
    explicit Node(NodeKind node_kind) : kind(node_kind) {}
    // Node kind is immutable after construction. The node is published only
    // through an acquire/release pointer edge, so readers do not need an
    // atomic load for this discriminator.
    NodeKind kind;
    // Fits in Node's existing tail padding. Only leaves consume it, but
    // keeping the submission state in the common prefix avoids an extra word
    // in every allocation handle.
    std::atomic<bool> submitted{false};
  };

  struct Leaf final : Node {
    Leaf() : Node(NodeKind::kLeaf) {}
    const unsigned char* key_prefix = nullptr;
    std::array<unsigned char, kKeyBytes - 32> normalized_suffix{};
    const char* entry = nullptr;
    Leaf* frozen_next = nullptr;
  };

  // An immutable sparse snapshot for one 64-byte-value segment. `children`
  // is a flexible tail laid out in low-six-bit order according to `occupied`.
  struct ChildBlock {
    uint64_t occupied = 0;
    uint8_t child_count = 0;
    Node* children[1];
  };

  struct Branch final : Node {
    Branch() : Node(NodeKind::kBranch) {}
    uint8_t byte_index = 0;
    std::array<std::atomic<ChildBlock*>, 4> segments{};
    // Branches are immutable after publication. These cached boundaries let
    // Seek compare against a child interval without descending to its edge
    // leaf at every ancestor.
    std::atomic<Leaf*> min_leaf{nullptr};
    std::atomic<Leaf*> max_leaf{nullptr};
  };

  struct Handle {
    Handle() = default;
    Leaf leaf;
  };

  enum class ChainState : uint8_t { kUnbuilt, kBuilding, kReady };

  static uint8_t ByteAt(const Key& key, uint8_t byte_index);
  static uint8_t ByteAt(const Leaf* leaf, uint8_t byte_index);
  static int Compare(const Key& left, const Key& right);
  static int Compare(const Leaf* left, const Key& right);
  static int Compare(const Leaf* left, const Leaf* right);
  static uint8_t FirstDifferingByte(const Key& left, const Key& right);
  static uint8_t FirstDifferingByte(const Leaf* left, const Key& right);

  struct TraversalFrame {
    Branch* branch;
    uint8_t segment;
    ChildBlock* block;
    uint8_t byte;
    Node* child;
  };

  Leaf* FindLeaf(const Key& key) const;
  static Leaf* AsLeaf(Node* node);
  static const Leaf* AsLeaf(const Node* node);
  static Branch* AsBranch(Node* node);
  static const Branch* AsBranch(const Node* node);
  static uint8_t LocalByte(uint8_t value) { return value & 0x3f; }
  static ChildBlock* BlockAt(const Branch* branch, uint8_t value);
  static Node* ChildAt(const ChildBlock* block, uint8_t local_byte);
  static Node* ChildAt(const Branch* branch, uint8_t value);
  static uint16_t FirstChildAtOrAfter(const Branch* branch, uint8_t value);
  static uint16_t LastChildAtOrBefore(const Branch* branch, uint8_t value);
  ChildBlock* AllocateChildBlock(uint64_t occupied, Node* const* children);
  ChildBlock* CopyBlockWithInsertedChild(const ChildBlock* old_block,
                                         uint8_t value, Node* child);
  ChildBlock* CopyBlockWithReplacedChild(const ChildBlock* old_block,
                                         uint8_t value, Node* child);
  bool ReplaceBlock(Branch* branch, uint8_t segment, ChildBlock* observed,
                    ChildBlock* replacement) const;
  Branch* AllocateBranch(uint8_t byte_index, Node* old_node,
                        Leaf* new_leaf);
  static Leaf* MinimumLeaf(Node* node);
  static Leaf* MaximumLeaf(Node* node);
  void PublishBoundaryUpdates(const Key& key, Leaf* leaf,
                              const TraversalFrame* path,
                              size_t depth) const;
  bool TryBuildFrozenChain() const;
  Leaf* FirstFrozenLeaf() const;

  Allocator* const allocator_;
  const TestHooks test_hooks_;
  std::atomic<Node*> root_{nullptr};
  std::atomic<bool> readonly_{false};
  mutable std::atomic<ChainState> chain_state_{ChainState::kUnbuilt};
  mutable std::atomic<Leaf*> frozen_first_{nullptr};

  friend class Cursor;
};

class CedarPureRadixIndex::Cursor {
 public:
  explicit Cursor(const CedarPureRadixIndex* index);

  bool Valid() const { return leaf_ != nullptr; }
  const char* entry() const;
  void Seek(const Key& key);
  void SeekForPrev(const Key& key);
  void SeekToFirst();
  void SeekToLast();
  void Next();
  void Prev();

 private:
  friend class CedarPureRadixIndex;
  void Clear();
  void DescendLeft(const Node* node);
  void DescendRight(const Node* node);
  const Leaf* FirstLeaf(const Node* node) const;
  const Leaf* LastLeaf(const Node* node) const;
  bool SeekLowerBound(const Node* node, const Key& key);
  bool SeekUpperBound(const Node* node, const Key& key);
  void NextPath();
  void PrevPath();
  void SeekPath(const Key& key, bool seek_for_prev);

  const CedarPureRadixIndex* const index_;
  // A direction byte is stored separately so a 40-level cursor consumes one
  // pointer per branch rather than a pointer-sized padded frame per level.
  std::array<const Branch*, kKeyBytes> branches_{};
  std::array<uint8_t, kKeyBytes> directions_{};
  size_t depth_ = 0;
  const Leaf* leaf_ = nullptr;
  bool frozen_chain_mode_ = false;
};

}  // namespace ROCKSDB_NAMESPACE
