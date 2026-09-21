# Pure Radix MemTable Concurrency Evidence

## Scope

This evidence covers only the facts `PartitionedVersionRadixFactory` on the
pure Patricia-radix branch. It does not measure Cedar's WAL, commit path,
kernel append path, recovery, query engine, T-Cypher layer, SST flush, or
end-to-end TPS.

The implementation uses an immutable binary Patricia tree over fixed 40-byte
normalized internal keys. A writer publishes either the root leaf or a
wrapping branch through one atomic child-slot CAS. A mutable cursor retains
its branch path; `MarkReadOnly()` only publishes the read-only state, and the
unlocked flush preparation path builds a forward `frozen_next` chain. Later
forward scans use one pointer per `Next()`. A reader concurrent with the
builder observes the `kBuilding` state and uses its independent path cursor
without waiting.

The insertion path retains the first descent's incoming slots and branch
pointers, selecting the insertion slot locally after discovering the first
differing key bit. It therefore avoids the prior second root descent. Its
common path retains 32 entries rather than reserving all 320 key bits on the
stack; an unusually deep Patricia path uses a second descent only to locate
the wrapping slot. A failed CAS restarts from the root because another writer
may have changed that slot.

## Environment

* Baseline: `a53475284915b997e8c9f696afdae1044a51b1f0` (`main`).
* Candidate: uncommitted `codex/pure-radix-cas-cursor` worktree.
* macOS 27.0 (26A428), arm64; Apple Clang 21.0.0 (clang-2100.3.34.2).
* All builds used `CEDAR_ROCKSDB_BUILD_PARALLEL_LEVEL=1`,
  `CMAKE_BUILD_PARALLEL_LEVEL=1`, and `-j1`.

## Correctness Evidence

The following counts are historical snapshots from intermediate binaries;
the current-source result is recorded in the addendum below.

* An intermediate Debug binary passed 19/19 after the inline-path-cache
  change. It covered canonical key validation, iteration and seek ordering,
  snapshot reads, frozen-chain fallback, duplicate CAS races,
  independent-writer progress, and concurrent readers.
* `python3 tests/models/cedar_pure_radix_model.py --key-bits 4 --writers 3
  --check-negative-control`: passed; its deliberately broken publication rule
  produces a counterexample.
* Production implementation files contain no `InlineSkipList`,
  `SkipListFactory`, `RWMutex`, `LockForWriteWithRetry`, ART fallback, or
  shard lock.
* Intermediate ASan and TSan binaries also passed 19/19. These counts are not
  the current-source result; macOS ASan leak detection is not claimed.
* Debug integration tests also passed on the final source: RocksDB kernel
  14/14, Cedar Parquet kernel 11/11, RocksDB production profile 14/14, and
  RocksDB lifecycle 11/11. The lifecycle suite covers flush/reopen, WAL
  replay, checkpoint, backup/restore, repair, compaction, shutdown, and
  flush/manifest I/O recovery.

## Release Microbenchmark

`cedar_radix_memtable_bench` executes concurrent
`Allocate`/`InsertKeyConcurrently`, active scan, first frozen scan, and
frozen-ready scan, verifying count, key order, and hash.

The final matrix contains 270 rows, five process repetitions for every
combination of 1024/16384/131072 entries, ascending/random/versions workload,
and 1/4/8 writers. All 135 baseline and 135 candidate rows report zero
validation errors. For 131072 random unique keys, seed `20260920`:

| Writers | Baseline insert median | Candidate median | Change |
| --- | ---: | ---: | ---: |
| 1 | 29.86 ms | 44.94 ms | 50.5% slower |
| 4 | 40.51 ms | 33.21 ms | 18.0% faster |
| 8 | 99.92 ms | 35.38 ms | 64.6% faster |

For the one-writer run, active scan improved from 71.29 ms to 10.99 ms and
frozen-ready scan from 58.06 ms to 9.46 ms. Candidate arena usage was 146
bytes per handle versus 151 for the baseline at one writer and 158 at four or
eight writers.

The final raw matrix is
`docs/superpowers/evidence/2026-09-20-cedar-pure-radix-matrix-final/matrix.csv`.
It was produced by:

```sh
export CEDAR_RADIX_BASELINE_REVISION=a53475284915b997e8c9f696afdae1044a51b1f0
export CEDAR_RADIX_CANDIDATE_REVISION=candidate-path-once
bash benchmarks/run_cedar_radix_memtable_matrix.sh \
  /tmp/cedar-radix-baseline-bench \
  ./build-radix-release/cedar_radix_memtable_bench \
  "$PWD/docs/superpowers/evidence/2026-09-20-cedar-pure-radix-matrix-final"
```

### Current Hot-Path Recheck

After replacing the 320-entry insertion cache with the 32-entry common-path
cache, a fresh Release build on the same host ran five independent processes
per side with 131072 random unique keys and seed `20260920`. Every one of the
30 samples reported `errors=0`, the expected entry count, canonical order,
and identical result hashes. The benchmark uses `InsertKeyConcurrently` at
all writer counts, so the one-writer row intentionally measures the CAS path.

| Writers | Baseline median | Current pure Radix median | Change |
| --- | ---: | ---: | ---: |
| 1 | 26.29 ms | 40.73 ms | 54.9% slower |
| 4 | 28.88 ms | 20.28 ms | 29.8% faster |
| 8 | 105.84 ms | 34.03 ms | 67.8% faster |

For the same one-writer recheck, median active scan was 9.39 ms versus 55.49
ms for the baseline, and median frozen-ready scan was 6.48 ms versus 49.26
ms. Candidate arena usage remained 146 bytes per handle versus 151 bytes in
the baseline. The current binary's `Insert()` frame is approximately 736 bytes
in a Debug disassembly, rather than the earlier approximately 5 KiB frame;
the hot-path cache removes stack-probe overhead but does not change Patricia's
binary fanout.

### Final Freeze-Boundary Smoke Run

After moving successor-chain construction from the first frozen reader into
unlocked `PrepareForFlush()`, a final Release run on the same host inserted
131072 random unique keys (seed `20260920`) and verified zero errors, the
expected count, canonical order, and the same result hash
(`454339457082336225`) at every writer count.

| Writers | Insert | Active scan | Freeze + first scan | Frozen-ready scan | Arena bytes/handle |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 42.07 ms | 13.06 ms | 15.89 ms | 8.33 ms | 146 |
| 4 | 30.02 ms | 13.75 ms | 14.05 ms | 7.16 ms | 146 |
| 8 | 35.47 ms | 11.64 ms | 13.80 ms | 11.14 ms | 146 |

This is one final smoke sample, not a replacement for the five-process
baseline/candidate matrix above. The first frozen measurement includes the
one-time `O(N)` work intentionally performed by `PrepareForFlush()`; the
frozen-ready measurement is the steady-state forward scan.

## Interpretation And Remaining Work

CAS removes global writer serialization and improves contended inserts. It
does not make binary Patricia insertion constant-time: a random fact key needs
`O(H)` descent where `H` is the number of discriminating bits, typically
`O(log N)` for random keys. ART's byte-wide fanout has fewer levels, explaining
the single-writer regression. Mutable `Next()` is amortized `O(1)` over a full
scan through its retained path; frozen-ready forward `Next()` is strict `O(1)`.

The current candidate still fails the plan's no-more-than-10-percent
single-writer regression gate. It must not be described as an unqualified
write-throughput improvement or as end-to-end Cedar TPS. Achieving that gate
would require relaxing the approved pure-binary-Radix constraint, or defining
a distinct single-writer representation and publication transition; neither
change is silently introduced here.

The current source also reuses only the cached ancestor prefix above a newly
published wrapping branch during boundary publication. Descendant entries
from the stale descent are excluded, and deep paths fall back to the original
root walk. This optimization passed the current Debug, ASan, TSan, randomized
visibility, and lifecycle validation; it does not change the failed write
performance gate.

Earlier serial ASan/TSan counts in this document refer to intermediate
binaries and are superseded by the current-source result below. macOS ASan
leak detection is not claimed.
## Sanitizer follow-up (2026-09-21)

The current-source serial ASan Radix suite passes 23/23 with leak detection
disabled. A fresh current-source TSan build also passes 23/23 with
`TSAN_OPTIONS=halt_on_error=1:report_bugs=1`. The final publication protocol
reuses atomic inherited boundaries, publishes child pointers with release
ordering, and passes the concurrent insertion/read tests without a reported
race. Earlier sanitizer counts in this document refer to older test binaries
and are superseded by this section.

## Current-source addendum (2026-09-21)

The current worktree now has 25 Radix tests. Debug and fresh TSan previously
passed 23/23; the TSan invocation used `TSAN_OPTIONS=halt_on_error=1:report_bugs=1`.
The added `StaleWrappingCasRetriesAfterAnotherWriterPublishesAncestor` test
pauses a stale writer, publishes an ancestor branch from another writer, then
verifies that the stale CAS cannot discard the ancestor subtree. The
interleaving model's negative control independently demonstrates the same
lost-subtree failure.

The current Debug run passed 25/25 at that checkpoint. It adds
`PointGetUsesStackCursorWithoutIteratorHeapAllocation`, which first observed
the old `Get()` implementation allocate one temporary `MemTableRep::Iterator`
per lookup, then verifies the direct stack cursor has no such allocation. The
same direct cursor now serves `Get`, `GetAndValidate`, and `MultiGet`; existing
validated candidate tests preserve callback ordering and validation-error
semantics.

The authoritative current B0/B1/B2 matrices contain 1,215 write rows and
2,430 read rows, all with zero errors and valid operation counts. The current
write gate remains failed (Radix is slower than both controls at one, four,
and eight writers for the 131072 random workload); the point and range16 read
gates pass against SkipList. Lifecycle/profile/Parquet integration is 63/63.

### Direct Point-Get Addendum (2026-09-21)

The benchmark's new `get` phase invokes `MemTableRep::Get` with `LookupKey`,
unlike the existing iterator-based `point` phase. Five independent Release
processes for each implementation read 131072 random unique keys at one
writer and seed `20260920`. Every run produced 131072 hits, zero errors, and
result hash `11476115612886267393`. The B1 SkipList median was 86.61 ms
(81.55--92.62 ms); pure Radix was 68.12 ms (62.98--70.04 ms), 21.4% faster.
Raw rows are in
`docs/superpowers/evidence/2026-09-20-cedar-pure-radix-performance-final/point-get-b1.csv`.
This confirms a repeatable benefit for the direct stack cursor; it does not
alter the failed insertion acceptance gates.

The latest focused Debug run passes 27/27; it includes the deep adversarial
seek-boundary and concurrent SkipList-candidate tests added after the earlier
25-test checkpoint. Both sequential and interleaving models also pass with
their negative controls on the current source.

### Allocation-Boundary Addendum (2026-09-21)

The production leaf now borrows the canonical MemTable key prefix, reducing
Radix arena use from 164 to 136 bytes per record without changing the CAS
publication protocol. A five-process eight-writer comparison over 131072
random keys (seed `20260920`) separates the phases: preallocated
`phase=index` Radix insertion was about 9.97 ms versus 20.13 ms for B1, while
allocation plus insertion (`phase=memtable`) had medians of 29.04 ms Radix
and 20.34 ms B1. All samples had zero errors and the same result hash.

This identifies allocation size and `ConcurrentArena` throughput, rather
than CAS retries or publication visibility, as the remaining contended-write
limit. The raw five-run rows, including 136 versus 62 bytes per record, are
stored in `cedar-pure-radix-performance-final/allocation-boundary-b1.csv`.
The general index retains its `Cursor::entry()` contract because standalone
callers may have distinct entry and key storage; removing that pointer would
not plausibly close the measured gap and is not introduced as an unsafe
special case.

### Current Single-Writer Phase Split (2026-09-21)

Moving `submitted` into existing `Node` tail padding reduces the current
Radix `Handle` from 88 to 80 bytes and the benchmark arena result from 136 to
128 bytes per record. Five independent one-writer Release processes at 131072
random unique keys and seed `20260920` then split preallocated index work from
allocation-plus-insert. All 20 rows returned 131072 operations, zero errors,
and hash `454339457082336225`.

| Phase | Vector median | Pure Radix median |
| --- | ---: | ---: |
| `index` | 1.264 ms | 30.094 ms |
| `memtable` | 13.080 ms | 44.218 ms |

The raw rows are in
`cedar-pure-radix-performance-final/single-writer-phase-split-20260921.csv`.
Because the gap remains when allocation is excluded, the single-writer
shortfall is the immutable Patricia descent, branch publication, and boundary
maintenance itself, not arena allocation or CAS contention. Removing that
work would violate the pure-Radix and CAS-only constraints; the performance
goal remains active pending an explicitly approved representation change.

### Current Contended Recheck (2026-09-21)

Five independent current Release `phase=memtable` processes at 131072 random
unique keys and seed `20260920` now pass both contention gates. Each row had
the expected count, zero errors, and hash `454339457082336225`.

| Writers | SkipList median | Pure Radix median | Change |
| --- | ---: | ---: | ---: |
| 4 | 19.306 ms | 14.917 ms | 22.7% faster |
| 8 | 19.794 ms | 16.217 ms | 18.1% faster |

These numbers do not resolve the structurally separate single-writer B0
failure described above. A fresh Debug focused CTest also passed 29/29 after
the layout change, and both CAS models passed with their negative controls.
