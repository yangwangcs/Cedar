# Cedar Adaptive Nibble Patricia Design

## Intent

Reduce the remaining single-writer insertion cost of Cedar's facts MemTable
without changing its externally visible ordering, the normalized fixed 40-byte
key, WAL/recovery behavior, or the lock-free publication model. This design
replaces the binary Patricia node representation with a path-compressed,
16-way Patricia representation whose child-table snapshots are immutable.

The performance target remains the calibrated B0 comparison: at 131072 random
records and one writer, `phase=memtable` for Radix must be no more than 10%
slower than Vector. The four- and eight-writer B1 guard remains Radix no more
than 5% slower than SkipList. A candidate which cannot satisfy these gates is
not silently accepted as a completion of the optimization goal.

## Constraints

- Keys remain exactly 40 normalized bytes and retain bytewise internal-key
  order, including the inverted RocksDB sequence/type tag.
- The index remains pure Patricia Radix: no Vector/SkipList/ART fallback,
  lock, shard, staging buffer, or persistent writer thread is introduced.
- Node payload is initialized before publication and never rewritten. The only
  post-publication state changes are release-CAS replacements of published
  root/child-table edges and monotonic `min_leaf`/`max_leaf` cache updates.
- A node or a child-table snapshot is not reclaimed before the owning
  MemTable is destroyed. Failed CAS candidates are therefore also retained in
  that MemTable's arena.
- Root and child-table publication are CAS-only. Readers use acquire loads
  before dereferencing an observed node/table. Successful publication uses
  release ordering; CAS failure retains acquire ordering when it feeds retry.
- `MarkReadOnly`, flush-chain construction, `MemTableRep`, WAL, commit,
  recovery, durable-format, and kernel append interfaces remain unchanged.

## Why This Representation

The current binary Patricia tree requires about 15.7 child-edge loads per
131072-record random single-writer insertion after boundary publication was
narrowed. A binary Patricia tree cannot reduce that height without abandoning
binary discrimination. The first 64 normalized-key bits are largely common
for the benchmark's v2 facts, so a fixed early-prefix directory would not
remove the effective random entity-id path.

A 16-way branch selects a four-bit nibble. A path-compressed nibble Patricia
tree consequently needs approximately one quarter as many discrimination
levels for random keys. A fixed 16-atomic-pointer branch would erase that CPU
gain through allocation and cache footprint. This design instead gives each
branch one atomic pointer to an immutable, compact sparse child table.

## Data Model

`Leaf` continues to carry the canonical MemTable entry pointer, its borrowed
32-byte key prefix, and the normalized eight-byte suffix. It remains the
opaque allocation handle returned to RocksDB.

`Branch` contains:

```c++
struct Branch final : Node {
  uint8_t nibble_index;                  // [0, 79], immutable
  std::atomic<ChildTable*> children;     // immutable snapshot publication
  std::atomic<Leaf*> min_leaf;
  std::atomic<Leaf*> max_leaf;
};

struct ChildTable {
  uint16_t occupied;                     // immutable nibble bitmap
  uint8_t child_count;                   // popcount(occupied), immutable
  Node* children[1];                     // packed, nibble-order flexible tail
};
```

`ChildTable` is allocated to its exact flexible-array size. Its packed child
pointer for nibble `d` is at rank `popcount(occupied & ((1u << d) - 1))`.
There are no atomic child pointers within a table: a branch atomically
publishes an entire replacement snapshot. The table is therefore immutable
after its release publication.

`Handle` no longer reserves one private branch candidate for every allocated
leaf. `Allocate()` allocates a `Leaf` plus the caller's canonical entry. A
branch and its initial two-child snapshot are allocated only when a distinct
key collides with an existing leaf. This is essential: a multi-way branch can
accept additional direct leaf children, so allocating one branch per leaf
would waste most of the representation's memory benefit.

## Tree Invariants

- On every root-to-leaf path, `nibble_index` strictly increases.
- A branch table has at least two occupied nibbles when created by a leaf
  collision. Later snapshot replacement can only add a nibble or replace one
  existing child by a deeper branch; it never removes a child.
- Every child in nibble `d` shares the branch key's nibble `d`; all keys below
  an occupied nibble have the exact lexicographic interval implied by that
  nibble.
- `min_leaf` and `max_leaf` identify the least/greatest leaf below that
  branch. They may lag only in the direction that cannot invalidate lower or
  upper bound searching; the existing compare-then-release-CAS loop preserves
  this monotonicity under concurrent writers.
- A duplicate key is rejected only if its first differing nibble is 80. This
  requires bytewise equality after the 20-nibble comparison, preserving the
  existing full 320-bit duplicate rule.

## Write Algorithm

1. Claim the leaf's one-shot `submitted` state. Populate its borrowed prefix,
   suffix, entry pointer, and frozen-chain pointer before any publication.
2. Acquire-load `root_`, then acquire-load each `Branch::children` snapshot.
   Record the traversed branches and the exact snapshot/link used for every
   descent. Nibble extraction is `key[nibble_index / 2]` high or low nibble.
3. If the root is null, release-CAS it from null to the leaf and return after
   boundary publication.
4. If a branch lacks the selected nibble, allocate a copied `ChildTable` with
   that leaf inserted at its packed nibble rank. Release-CAS the branch's
   `children` pointer from the observed snapshot to the replacement. A failed
   CAS restarts the descent; the unpublished table remains arena-owned.
5. If descent reaches a distinct leaf, find the first differing nibble. Find
   the first traversed branch whose index is not below that nibble. Create a
   new branch containing the old subtree and new leaf in their two distinct
   nibbles. Publish it either by root CAS or by copying the parent table with
   its observed child pointer replaced by the new branch, then CASing the
   parent table pointer.
6. A CAS failure restarts from `root_`. It never writes an old snapshot or
   mutates an already-published child table.
7. After successful leaf or branch publication, visit the recorded ancestor
   branches and retain the existing compare-then-release-CAS min/max update.
   Unlike binary directional pruning, a sparse nibble table can receive a new
   minimum or maximum through any occupied-rank transition, so correctness
   first uses the complete retained ancestor path. Its expected height is now
   approximately four times smaller.

## Reads And Freeze

`Contains`, `Cursor::Seek`, `SeekForPrev`, forward scanning, and frozen-chain
construction interpret a branch by its nibble bitmap and packed-child rank.
For a missing target nibble, lower-bound seek selects the first occupied nibble
greater than the target and descends to its first leaf; upper-bound seek uses
the preceding occupied nibble and its last leaf. When the target nibble is
occupied, they descend its child exactly as before. This preserves internal
key order even for sparse tables.

Ready frozen forward cursors still use `Leaf::frozen_next` after positioning.
`MarkReadOnly()` remains an O(1) publication; `PrepareForFlush()` alone builds
the successor chain. No recovery or MemTable lifecycle call site changes.

## Failure Handling

All allocation failures retain the existing null/false behavior. A malformed
or noncanonical V2 key is rejected before index publication. CAS contention
is handled only by retry from the acquire-loaded root. A reader can observe an
old child-table snapshot or a newly published snapshot, never a partially
initialized table.

## Test And Measurement Contract

The implementation plan must first add RED tests that prove:

1. packed child-table rank and nibble ordering for all 16 nibbles;
2. sparse-table lower/upper bound seeks at missing, first, and last nibbles;
3. a direct third-child table replacement racing with a concurrent subtree
   wrapper does not lose either unique key;
4. duplicate keys, a 79-nibble path, concurrent readers, and frozen scans
   preserve the existing ordering/hash/visibility contract;
5. a publication observer sees fewer branch descents than the binary baseline
   for a deterministic random entity-id corpus.

Debug, ASan, and TSan focused Radix suites must pass. The existing lifecycle,
crash-recovery, and format-recovery suites must be rerun. Release evidence
must use five independent processes, alternating implementation order, three
random seeds, and the current `index`/`memtable` phase split. It must retain
the complete B0/B1 matrix and report arena bytes and RSS.

The candidate is retained only if all result hashes/counts/errors match, B1
does not regress more than 5% at four and eight writers, arena bytes per
record do not exceed 1.25 times the retained binary Radix baseline, and B0
shows a repeatable improvement. Full completion still requires the existing
`<= 1.10 * Vector` B0 gate; a failed candidate remains visible in evidence.

## Out Of Scope

This work does not add adaptive radix-tree node classes, a lock-protected
directory, a staging write buffer, key-format changes, node reclamation,
on-disk format changes, or throughput claims for the full Cedar transaction
path.
