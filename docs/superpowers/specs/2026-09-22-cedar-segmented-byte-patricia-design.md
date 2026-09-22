# Cedar Segmented Byte Patricia Design

## Intent

Reduce the remaining single-writer insertion cost of Cedar's pure immutable
Patricia Radix MemTable without altering its external RocksDB contract. The
retained 4-bit segmented representation satisfies the concurrent-write gate
but has a 34.976 ms one-writer `phase=memtable` median at 131072 random keys,
2.691 times the Vector reference. Narrow, independently verified constant
factor candidates have not improved this number enough to retain.

This design changes a Patricia discriminator from one nibble to one byte. It
keeps the branch's four independent CAS publication edges, using a compact
64-value immutable bitmap block behind each edge. The intended benefit is
fewer descent comparisons, fewer wrapping levels, and fewer internal branches
for the fixed 40-byte normalized key, with no staging structure or mutable
published node.

## Non-Negotiable Constraints

- The index remains a pure path-compressed Patricia Radix tree.
- Keys remain exactly Cedar's normalized 40-byte V2 internal keys and retain
  bytewise internal-key order.
- Leaf, Branch, and published child-block payloads are immutable after their
  root or segment release-CAS publication. Arena allocations are not reclaimed
  before the owning MemTable is destroyed.
- A branch has exactly four independent atomic child-block publication edges;
  root and segment edges use release success and acquire failure order. A
  reader acquire-loads every edge before dereferencing its target.
- No lock, shard, worker, staging buffer, Vector/SkipList/ART fallback,
  durable-format, WAL, recovery, commit, or kernel-append change is allowed.
- `ConcurrentArena` allocation stays outside benchmark `phase=index`.
  Benchmark stdout remains the existing 28-field CSV schema.
- The B0 pass criterion remains Radix `phase=memtable <= 1.10 * Vector` for
  131072 random keys and one writer. B1 requires no more than 5% Radix
  regression versus the retained source at four and eight writers.

## Representation

`Branch::byte_index` is in `[0, 39]` and denotes the one key byte that a
branch distinguishes. A byte's high two bits select one of four independent
segments. Its lower six bits select a child in the segment's immutable packed
block.

```c++
struct ChildBlock {
  uint64_t occupied;      // one bit per local byte value [0, 63]
  uint8_t child_count;    // popcount(occupied)
  Node* children[1];      // flexible tail, packed in increasing local order
};

struct Branch final : Node {
  uint8_t byte_index;
  std::array<std::atomic<ChildBlock*>, 4> segments;
  std::atomic<Leaf*> min_leaf;
  std::atomic<Leaf*> max_leaf;
};
```

`ChildBlock` allocation is exact-size:
`offsetof(ChildBlock, children) + child_count * sizeof(Node*)`. A child rank
is `popcount(occupied & ((uint64_t{1} << local) - 1))`; no branch scans its
packed child pointers. A null segment represents no child for all 64 values
in its interval. The branch retains four atomic edge pointers, matching the
retained segmented-nibble branch footprint rather than materializing 256
children.

`FirstDifferingByte` returns a byte index in `[0, 39]`, or `kKeyBytes` only
for equality. A collision creates one byte branch and places its old subtree
and new leaf in one or two initialized blocks. All keys below a Branch agree
before `byte_index`; therefore branch byte indices strictly increase down a
path.

## Insertion And Publication

1. A writer initializes an unsubmitted leaf, acquire-loads root and selected
   segment edges, and records `{Branch*, segment, block, byte, child}` frames.
2. A missing byte child copies only the observed 64-value block, inserts the
   packed pointer at its local rank, and release-CASes the same segment. On a
   failed CAS the writer restarts from an acquire root load.
3. A leaf or incompatible subtree collision computes the first differing byte,
   allocates a fully initialized replacement Branch and initial blocks, then
   replaces the observed parent segment or root. A failed parent/root CAS also
   restarts from root.
4. Independent segments may publish concurrently. Same-segment snapshots are
   never overwritten: a stale CAS fails and its retry copies the new block.
5. After a successful leaf publication, every retained ancestor updates
   `min_leaf` and `max_leaf` with the existing compare-then-release-CAS loop.
   Cached boundaries are monotonic and never replace a smaller minimum or
   greater maximum.

## Reads And Freeze

`Contains`, insertion descent, seek, seek-for-prev, cursor backtracking, and
frozen-chain construction select `SegmentForByte(value) == value >> 6` and
`LocalByte(value) == value & 0x3f`. Successor/predecessor lookup scans at most
four segment pointers and uses 64-bit bitmap lower/upper-bound operations to
return the adjacent byte in global order. `min_leaf` and `max_leaf` retain
their acquire-load seek bounds. A frozen-ready forward cursor still walks the
immutable `frozen_next` chain without branch/block loads.

## Validation And Retention

The implementation must begin with RED tests covering all 256 final-byte
values, cross-segment ordered seek boundaries (63/64, 127/128, 191/192),
same-segment stale-block retry, different-segment direct/wrapper publication,
final byte 39 differences, duplicate-key races, deep paths, and frozen scans.
The sequential and interleaving models must include byte-level negative
controls. Debug, ASan, and TSan focused suites plus lifecycle/crash/format
recovery remain mandatory before retaining source.

Release evidence uses five independent processes with alternating
implementation order, 131072 random entries, seed `20260921`, and separate
`index`/`memtable` B0 phases plus 4/8 writer B1 `memtable` phases. Every data
row must have 28 columns, 131072 operations, zero errors, and result hash
`454339457082336225`. Retain the representation only when the one-writer
Radix median improves at least 15% over 34.976 ms, does not regress B1 by more
than 5%, and passes all correctness/sanitizer checks. B0 is passed only at
the 1.10x Vector threshold; otherwise report `PURE-RADIX-CONTINUE` and keep
the goal active.
