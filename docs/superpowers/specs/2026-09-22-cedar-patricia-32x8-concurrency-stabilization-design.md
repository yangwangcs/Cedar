# Cedar Patricia 32-by-8 Concurrency Stabilization Design

## Decision

Cedar will retain the pure Patricia 32-by-8 byte-segment representation as
the final in-memory index design. This work does not evaluate Nibble,
16-by-16, 64-by-4, sharding, locks, staging indexes, or fallback indexes.
It restores the already measured 32-by-8 implementation, removes the known
eight-writer sources of avoidable coherence and retry work, and then freezes
the representation after performance and lifecycle validation.

The existing Release evidence is the baseline. With 131072 random keys,
32-by-8 used about 17.68-19.02 MB of Arena and beat SkipList at four writers,
but its eight-writer ratio varied from 0.7686 to 0.9233 across three seeds.
The failing seed was not a single timing outlier: candidate coefficient of
variation was 3.35%. The remaining problem is seed-dependent concurrent work,
not correctness, single-writer performance, or Arena write amplification.

## Fixed Invariants

- A normalized key is exactly 40 bytes. Byte index 39 remains a valid Patricia
  discriminator; the 32 in 32-by-8 describes segmenting each 256-way byte
  branch and does not shorten the key path.
- A branch has 32 independently published segment edges. Each immutable child
  block covers eight byte values with an eight-bit occupancy mask and up to
  eight packed child pointers.
- Published nodes and child blocks are immutable. Nodes live until the owning
  MemTable Arena is destroyed.
- Root and segment publication remains CAS-only with release publication and
  acquire failure observation. Readers use acquire loads.
- The MemTableRep API, normalized-key encoding, WAL records, SST contents,
  comparator behavior, and database format do not change.
- The three pre-existing dirty benchmark paths remain untouched:
  `benchmarks/cedar_radix_memtable_bench.cc`,
  `tests/performance/test_radix_benchmark_csv.cmake`, and
  `benchmarks/run_cedar_radix_read_matrix.sh`.

## Stabilization Changes

### Segment-edge cache-line isolation

The target host reports a 128-byte cache line. The old 32-by-8 `Branch` is
280 bytes and stores its 32 atomic segment pointers adjacently, so unrelated
segment CAS operations invalidate the same cache line. Replace the atomic
pointer array element with a 128-byte `SegmentEdge`: one naturally aligned
atomic `ChildBlock*` followed by inert padding. A compile-time assertion fixes
the edge size at 128 bytes. Consecutive atomics then have identical offsets in
distinct hardware cache lines even though RocksDB's `Allocator` only promises
pointer alignment; the type itself must not require over-aligned allocation.
The immutable branch header precedes the edges and the mutable min/max caches
follow them, so no two segment atomics share a line with each other.

This increases a branch from roughly 280 bytes to roughly 4.1 KB. The measured
workload has about 515 byte branches, so the expected increase is about 2 MB,
keeping total Arena use below the binding 1.25x Nibble limit. Actual allocator
accounting, rather than this estimate, decides retention. If cache-line
isolation does not improve eight-writer stability or violates the Arena gate,
remove this change and retain the compact 32-by-8 edge layout.

### Bounded compatible same-edge retry

Today every failed segment CAS discards the traversal and restarts at the
root. Add one bounded local retry at the same `Branch` and segment edge. After
a failed CAS, use the acquire-observed current block returned by the failed
compare-exchange and inspect the exact local byte:

- For insertion into an empty child, retry locally only if that child is still
  empty in the current block.
- For wrapping/replacing an existing child, retry locally only if that child
  is still the exact pointer observed during traversal.
- Rebuild a fresh immutable block from the current block, attempt one release
  CAS, and stop after that attempt.
- If the child changed, the edge is no longer compatible, or the second CAS
  fails, restart from an acquire root load.

Append-only Patricia publication makes a compatible local retry reachable:
concurrent wrappers preserve the old subtree and no published branch is
removed or reclaimed during the MemTable lifetime. Compatibility is checked
against the exact target child, not merely occupancy, so the retry cannot
overwrite another writer's structural change. The retry remains bounded to
avoid starvation and unbounded Arena retention. Boundary-cache updates run
only after successful publication and use the same path prefix as the
original operation.

Tests must falsify both unsafe variants: retrying after the target child has
changed and retrying indefinitely after repeated same-edge failures. If the
bounded retry does not reduce root restarts and improve the Release result,
remove it; no broader publication protocol is authorized.

## Diagnostics

Disabled-by-default test hooks will separately count root CAS attempts,
segment CAS attempts, segment failures, compatible local retries, successful
local retries, incompatible fallbacks, and retry-exhausted root restarts.
Existing branch bytes, child-block bytes, and copied-pointer accounting stays.
Diagnostics run only in untimed tests and diagnostic processes; the timed CSV
schema and benchmark source hash remain unchanged.

Before performance measurement, an untimed eight-writer run must show whether
cache-line isolation changes segment failure behavior and whether local retry
converts failures into successful local publications. This is explanatory
evidence, not a substitute for the Release gates.

## Correctness Validation

Development follows RED/GREEN tests. Restore the prior 32-by-8 representation
contracts and both deterministic Python publication models first. Add forced
interleavings for compatible empty-child retry, compatible wrapper retry,
changed-child fallback, second-failure fallback, duplicate insertion, and a
concurrent ancestor wrapper. Existing tests continue to cover all 31 segment
boundaries, byte 39, forward/reverse ordering, Seek, SeekForPrev, duplicate
keys, direct and wrapper publication, and repeated frozen scans.

The focused Debug suite and both model negative controls must pass before any
timing. The final candidate must also pass the relevant repository Debug
tests, ASan, and TSan. A sanitizer, ordering, duplicate, or linearization
failure vetoes the optimization that caused it.

## Performance And Memory Gates

All comparisons use clean committed sources, Release `-O3 -DNDEBUG`, identical
benchmark source hashes, 131072 random keys, zero reported errors, and the
expected result hash. Runs are separate processes and implementation order is
interleaved. Per-seed medians are binding; results are never averaged across
seeds to hide a failure.

- One-writer MemTable: 32-by-8 <=1.05x four-by-64 Byte.
- Four-writer MemTable: 32-by-8 <=0.80x SkipList.
- Eight-writer MemTable: 32-by-8 <=0.85x SkipList for every seed.
- Eight-writer stability: candidate coefficient of variation <=5% for every
  seed across ten independent candidate processes.
- Arena at one, four, and eight writers: <=1.25x Nibble for every seed.

The final stability matrix uses seeds 20260920 through 20260924. It runs ten
candidate and ten SkipList processes for the eight-writer workload per seed.
The one- and four-writer regression checks use five processes per seed against
their existing Byte and SkipList controls. Raw rows, medians, coefficients of
variation, Arena, peak RSS, source hashes, compiler flags, and diagnostic
counters are preserved as evidence.

Once these gates pass, performance optimization stops. A sub-change that
fails its isolated A/B check is reverted without reopening representation
design; 32-by-8 remains fixed.

## Freeze And Persistence Validation

After the Release gates pass, validate the complete lifecycle in this order:

1. After RocksDB has quiesced writers, `MarkReadOnly` establishes the freeze
   transition while concurrent readers retain a coherent snapshot. The index
   does not add a new direct-`Insert` rejection contract.
2. `PrepareForFlush` builds one ordered frozen chain; repeated calls and scans
   are idempotent, and byte-39 Put/Delete ordering is preserved.
3. A real RocksDB flush emits SST data whose point lookups and forward/reverse
   scans match the pre-flush MemTable view.
4. Closing and reopening after flush recovers the same data from SST.
5. Closing before flush replays WAL records into a fresh 32-by-8 MemTable and
   produces the same visible state, including deletes and multiple versions.
6. A database fixture written by the pre-change binary opens and reads with
   the final binary; a fixture written by the final binary opens and reads
   with the pre-change binary. Both directions compare logical rows and hashes.

Because Patricia nodes are memory-only and neither WAL nor SST encoding
contains the branch representation, no migration is expected. The two-binary
fixture test is still mandatory evidence. Any lifecycle or compatibility
failure blocks design freeze even when performance passes.

## Finalization

The design is frozen only after correctness, sanitizers, every per-seed
performance and Arena gate, and all six lifecycle stages pass. The final
report records the retained source commit and exact evidence paths. No further
representation experiment or performance tuning is part of this work.
