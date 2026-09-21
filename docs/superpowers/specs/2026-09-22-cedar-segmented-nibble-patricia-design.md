# Cedar Segmented Nibble Patricia Design

## Intent

Reduce the cost and contention of immutable sparse child-table replacement in
Cedar's pure Patricia Radix MemTable. The retained 16-way nibble tree has
correct ordering and useful four/eight-writer time, but every non-root direct
insert currently copies and publishes one entire `ChildTable`. At 131072
random keys the one-writer diagnostic recorded 131069 snapshot CASes, while
four writers increased retained failed candidates enough to exceed the arena
budget. The representation below changes the publication unit, not the key,
ordering, index class, or RocksDB lifecycle.

Success remains the existing B0 gate: for 131072 random records and one
writer, Radix `phase=memtable` must be at most `1.10 * Vector`. B1 remains
Radix no more than 5% slower than SkipList at four and eight writers. A
candidate that does not meet B0 is not completion of this goal.

## Constraints

- Keys remain the fixed normalized 40-byte Cedar v2 internal keys.
- The index remains a pure path-compressed Patricia Radix tree. It gains no
  Vector, SkipList, ART, staging buffer, lock, shard, or writer thread.
- Published `Leaf`, `Branch`, and child block payloads are immutable. Nothing
  is reclaimed before the owning MemTable is destroyed.
- Publication uses only release CAS on root or a branch segment edge; readers
  acquire-load each observed edge before dereferencing it.
- `min_leaf` and `max_leaf` retain their existing monotonic compare/CAS
  semantics. WAL, recovery, durable format, commits, and kernel append logic
  remain byte-for-byte outside this MemTable representation.
- `ConcurrentArena` allocation remains outside benchmark `phase=index`; all
  benchmarks continue to report both `index` and `memtable` phases.

## Segmented Child Representation

Each 16-way branch has four independent immutable child-block publication
slots. Segment `s` represents nibbles `[4*s, 4*s+3]`.

```c++
struct ChildBlock {
  uint8_t occupied;       // Four low bits only, immutable.
  uint8_t child_count;    // popcount(occupied), immutable.
  Node* children[1];      // flexible packed tail in local-nibble order.
};

struct Branch final : Node {
  uint8_t nibble_index;
  std::array<std::atomic<ChildBlock*>, 4> segments;
  std::atomic<Leaf*> min_leaf;
  std::atomic<Leaf*> max_leaf;
};
```

`ChildBlock` is exact-size arena allocation: a block with `n` children uses
`offsetof(ChildBlock, children) + n * sizeof(Node*)`. `occupied` is indexed
by `nibble & 3`; its child rank is `popcount(occupied & ((1 << local) - 1))`.
The high two nibble bits select the segment. A null segment has no occupied
children and is not allocated merely to represent emptiness.

The greater `Branch` footprint is intentional: it trades three extra atomic
edge pointers per branch for a publication candidate bounded by four child
pointers, and allows writes to different nibble segments to proceed without
sharing a compare-exchange target. This preserves the sparse representation:
there is no eagerly materialized 16-pointer child array.

## Write Protocol

1. A writer initializes its unsubmitted leaf before publication, then
   acquire-loads root and each traversed segment edge. A traversal frame keeps
   `{Branch*, segment, ChildBlock*, nibble, child}`.
2. A missing child builds a replacement only for the observed segment. It
   inserts the leaf at local rank and release-CASes that one segment from the
   observed block to the replacement. On failure it restarts from root; the
   losing block is arena-retained.
3. A leaf collision computes the first differing nibble. It builds a new
   branch and one or two initial blocks holding the old subtree and new leaf.
   If the wrapper is below a parent branch, it replaces only the parent
   segment's selected child; root wrapping still CASes `root_`.
4. Direct insertion at an existing branch uses that branch's segment selected
   by the differing nibble. A concurrent replacement in another segment does
   not make this CAS stale. A concurrent replacement in the same segment does
   make it stale and forces a root restart, so no published child disappears.
5. After successful leaf publication, ancestor boundary updates use the
   retained full path and their existing compare/release-CAS loop. They never
   overwrite a smaller published minimum or greater published maximum.

The initial `Branch` may contain two children in one block or one child in
each of two blocks. It is fully initialized before the root/parent release
CAS. An observed block is therefore always a complete old snapshot or a
complete replacement snapshot.

## Reads And Freeze

`Contains`, insertion descent, Seek, SeekForPrev, and cursor backtracking
derive a segment from the target nibble, acquire-load that segment, and use
the block's local bitmap/rank. To find a successor or predecessor across a
missing nibble, they scan at most four segment pointers and use local bitmap
lower/upper-bound selection. They retain the existing `min_leaf`/`max_leaf`
fast bounds and frozen successor-chain behavior.

The tree's lexicographic order is unchanged: segment order is high nibble
bits, block order is local nibble bits, and every branch still discriminates
one fixed key nibble. `MarkReadOnly`, `PrepareForFlush`, and frozen forward
scans retain their prior external behavior.

## Safety Contracts

- A reader that observes a non-null segment with acquire order can safely
  dereference its immutable block and every child stored in it.
- A same-segment stale CAS cannot replace a newer block. A different-segment
  CAS cannot invalidate another segment because each atomic edge owns a
  disjoint nibble interval.
- A wrapper's parent replacement uses the observed parent segment, rather
  than an old all-branch snapshot. Thus direct insert in one segment and
  wrapper publication in a different segment can both become visible.
- A segment's `occupied` bits never lose an already visible child except by
  replacing that child pointer with a deeper branch that retains the old
  subtree.
- Every branch path remains strictly increasing by `nibble_index`; duplicate
  detection remains equality across all 80 nibbles.

## Test And Evidence Contract

Before production changes, tests must establish compact local-rank order,
cross-segment ordered seek, same-segment stale-CAS rejection, different-
segment simultaneous publication, wrapper/direct races, duplicate races, and
deep 79-nibble paths. The interleaving model must include a negative control
which unconditionally writes a stale same-segment block and loses a key.

Debug, ASan, and TSan focused Radix suites, plus lifecycle, crash recovery,
and format recovery tests, must run after the representation change. Release
evidence uses five independent processes, alternating implementation order
and at least three random seeds. It records hashes, operation counts, errors,
arena bytes, peak RSS, `branch_loads`, and `segment_snapshot_cas` in a
separate diagnostics stream.

The candidate is retained only if all semantic/sanitizer checks pass, the
one-writer Radix median improves repeatably over the retained nibble-table
baseline, B1 does not regress by more than 5%, and Arena is at or below the
retained binary budget. The goal completes only when B0 also passes; otherwise
the report records `PURE-RADIX-CONTINUE` and the goal remains active.
