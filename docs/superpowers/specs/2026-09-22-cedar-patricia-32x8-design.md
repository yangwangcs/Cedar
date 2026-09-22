# Cedar 32-by-8 Byte Patricia Retention Design

## Decision Context

The approved Nibble versus four-by-64 Byte Release matrix rejected Nibble:
one-writer time was 1.193-1.214x Byte and one seed missed the eight-writer
SkipList gate. A 16-by-16 Byte candidate passed one and four writers, but
missed eight writers (0.882/0.972/0.991x SkipList) and Arena
(1.340-1.348x Nibble); it was reverted with evidence preserved. The user
explicitly authorized a new design and its implementation after this result.

The benchmark varies only the low 17 entity-ID bits: a complete 131072-key
tree has 515 byte branches, 512 of which discriminate the final entity-ID
byte. The fixed 16-by-16 branch-head increase cannot account for the measured
Arena gap. A full 256-way branch accumulates an idealized 9.5 MiB of direct
snapshot payload/header allocations across those 512 branches at width 16;
width 8 lowers this to about 5.5 MiB. Initial branch blocks, replacements,
CAS retries, allocator rounding, and scheduling are excluded. This is a
testable allocation hypothesis, not a performance result.

## Intended Outcome And Constraints

Select one in-memory pure Patricia representation that passes every existing
per-seed Release gate: one-writer MemTable <=1.05x four-by-64 Byte, four-writer
MemTable <=0.80x SkipList, eight-writer MemTable <=0.85x SkipList, and Arena
at each writer count <=1.25x Nibble. The historical one-writer Vector <=1.10x
criterion is reported separately and never silently treated as passed.

The key is exactly 40 normalized bytes; byte 39 can discriminate entries.
Published nodes and child-block payloads remain immutable. Root and segment
edges publish by release CAS; failed CAS observes with acquire and retries
from an acquire root load. Reads acquire-load edges. No lock, shard, staging
index, fallback implementation, additional writer, or format change is
allowed. Failed candidates remain owned by the MemTable Arena. Preserve the
three pre-existing dirty benchmark paths untouched and keep benchmark source
hashes identical in every timed variant.

## Considered Representations

1. 32-by-8 Byte (selected candidate): one byte-Patricia branch has 32
   independent atomic segment edges; each exact-size immutable block has an
   eight-bit occupancy mask and up to eight packed children. It halves the
   idealized direct snapshot bytes relative to 16-by-16 while keeping 40
   branch levels. This may still miss eight-writer timing.
2. Further Nibble tuning: already meets Arena and most concurrency gates,
   but needs about 12-14% one-writer improvement and a further eight-writer
   improvement on one seed. It is a different design investigation, not an
   automatic fallback for a failed 32-by-8 candidate.
3. 64-by-4 Byte: smaller immutable blocks and more publication edges, but a
   larger branch head and a longer ordered segment scan. It is not authorized
   as an automatic next candidate if 32-by-8 fails.

## Representation And Publication

`kByteSegmentCount=32`, `SegmentForByte(value)=value>>3`, and
`LocalByte(value)=value&7`. `ChildBlock::occupied` is `uint8_t`;
`child_count` is 1..8; its allocation is exactly
`offsetof(ChildBlock, children) + child_count * sizeof(Node*)` before Arena
alignment. For rank, count only occupancy bits below the local value. A
global byte is `segment*8+local`; use `uint16_t` 256 as the no-child sentinel.
Never shift by eight or narrow the sentinel to `uint8_t`.

Direct insertion and wrapper replacement copy only the observed segment's
block. New branches fully initialize their one or two initial blocks and
min/max caches before publishing the root/parent edge. A same-edge stale CAS
fails and restarts; different segment CAS operations can both succeed. The
existing monotonic boundary-cache updates and duplicate semantics remain.
Contains, Seek, SeekForPrev, reverse/forward backtracking, and frozen-chain
construction share the same byte mapping. The cursor retains 40 branch
frames; a ready frozen forward cursor follows `frozen_next` without a branch
load. The external MemTableRep and on-disk WAL/SST formats do not change.

## Diagnostics And Falsification

Before timing, add test-only, disabled-by-default observers for branch and
block allocation bytes, copied child-pointer counts, and root/segment CAS
attempt/success/failure. In an untimed 131072-key run, distinguish successful
publication from Arena-retained failed candidates; note that every replaced
snapshot remains Arena-owned even on successful publication. Counters are
diagnostic evidence, not timed performance samples, and the existing stdout
CSV schema must remain 28 columns. An instrumented index test can exercise
these hooks without changing the user-owned benchmark file. If the measured
copy volume does not dominate the Arena movement, document that before
claiming an allocation root cause; do not adjust the gates.

## Validation And Selection

Write RED representation, all 31 boundaries (7/8 through 247/248), byte-39,
ordered seek/prev, duplicate, same-segment stale retry, and independent
direct/wrapper publication tests before production changes. Extend both
sequential and interleaving negative-control models. GREEN requires focused
Debug and model runs with no failures. Compare a clean committed candidate
worktree against the exact Nibble and four-by-64 control binaries, rebuilding
the candidate with the same compiler and Release `-O3 -DNDEBUG` settings and
identical benchmark source hash. Run three seeds, five separate processes
per seed/configuration, 131072 random keys, interleaved implementation order,
and no diagnostic hooks. Every row must have 131072 operations, zero errors,
and a matching result hash. Apply all four original gates to each seed's
median; do not average away a failure. Record Arena, peak RSS, raw rows,
source hashes, build flags, and the historical Vector result.

On any gate failure, preserve raw evidence and revert the candidate with new
commits. Return to design review; no 64-by-4 follow-on is implicit. Only a
retained candidate proceeds to Debug/ASan/TSan and the full chain:
MarkReadOnly, PrepareForFlush, repeated frozen scans, a byte-39 Put/Delete
regression, direct lifecycle, Flush/SST, WAL/crash recovery, and bidirectional
old/new format compatibility through separately built fixtures. Every
semantic, sanitizer, persistence, or format failure vetoes retention.
