# Cedar Segmented Nibble Patricia Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace whole sparse-child-table snapshots with four independently CAS-published immutable 4-way child blocks, then measure whether it closes Cedar's pure-Radix single-writer and contended-arena gaps.

**Architecture:** Keep path-compressed 16-way nibble Patricia discrimination. Each branch owns four atomic immutable `ChildBlock*` slots selected by the high two nibble bits; an exact-size block packs only its occupied local-nibble children. Insertion and wrapping copy/CAS only the affected block, while readers resolve global nibble order across the four segments.

**Tech Stack:** C++20, embedded RocksDB 11.1.2, GoogleTest, Python publication models, CMake, macOS Clang Debug/Release/ASan/TSan.

**Spec:** `docs/superpowers/specs/2026-09-22-cedar-segmented-nibble-patricia-design.md`

## Global Constraints

- Work only in `/Users/wangyang/Desktop/Cedar/.worktrees/pure-radix-cas-cursor` on `codex/pure-radix-cas-cursor`.
- Preserve pure Patricia Radix, immutable published payloads, CAS-only publication, normalized 40-byte keys, and present WAL/recovery semantics.
- Do not add a lock, shard, writer thread, staging buffer, Vector/SkipList/ART fallback, or on-disk format change.
- Compile exactly one job: `cmake --build <build-dir> -j1`.
- A production change begins with a failing focused test and is retained only after focused correctness, sanitizer coverage, and five independent Release process samples.
- Preserve the benchmark's stable CSV stdout schema; publish representation diagnostics only on stderr/evidence files.

## Review Focus

- A direct insert in segment 0 racing a wrapper replacement in segment 3 leaves both keys visible; Task 2 owns the race test.
- A stale same-segment replacement must fail, reload, and retain all newer children; Task 2 owns its negative-control model and C++ hook interleaving.
- Segment-boundary seeks at nibble 3/4, 7/8, and 11/12 return the adjacent ordered leaf; Task 3 owns these checks.
- A fresh reader never dereferences a partially initialized child block; Task 4 owns TSan publication stress.
- A 79-nibble Patricia path, frozen scan, recovery, and duplicate-key race retain existing behavior; Tasks 3 and 4 own the coverage.

### Task 1: Establish the segmented representation contract

**Files:**
- Modify: `src/engine/rocksdb/memtable/cedar_pure_radix_index.h`
- Modify: `src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc`
- Modify: `tests/models/cedar_pure_radix_model.py`

**Interfaces:**
- Produce `ChildBlock`, `BlockAt(const Branch*, uint8_t)`, local-rank helpers, and `SegmentFor(uint8_t)` where `SegmentFor(nibble) == nibble >> 2`.
- Expose only test hooks/counts needed for `segment_snapshot_cas`; production objects leave hooks empty.

- [ ] **Step 1: Write RED contracts.** Add a C++ test that inserts one deterministic leaf in each global nibble 0..15 and asserts the new testing structure view reports four segments and local bitmap/rank order. Add a Python model assertion that flattening four packed blocks is exactly nibble sorted.
- [ ] **Step 2: Run the RED contracts.** Build the focused target and run `ctest --test-dir /Volumes/E/CedarBuild/pure-radix-cas-cursor-debug-20260921 -R 'SegmentedNibble' --output-on-failure -j1`; run `python3 tests/models/cedar_pure_radix_model.py --check-negative-control`. Expected: compilation/test failure because the segmented test interface and model behavior do not exist.
- [ ] **Step 3: Replace only representation primitives.** Introduce exact-size `ChildBlock`, segment/global-nibble rank helpers, and four atomic slots on `Branch`; update allocation and diagnostics naming without changing public MemTable interfaces.
- [ ] **Step 4: Verify GREEN.** Rebuild `-j1`; run the new focused CTest and sequential model with the negative control. Expected: 16 nibbles flatten in order and every block has at most four children.
- [ ] **Step 5: Commit.** `git add src/engine/rocksdb/memtable/cedar_pure_radix_index.h src/engine/rocksdb/memtable/cedar_pure_radix_index.cc src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc tests/models/cedar_pure_radix_model.py && git commit -m "perf: segment immutable nibble child blocks"`

### Task 2: Publish a single immutable child block per update

**Files:**
- Modify: `src/engine/rocksdb/memtable/cedar_pure_radix_index.cc`
- Modify: `src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc`
- Modify: `tests/models/cedar_pure_radix_interleaving_model.py`

**Interfaces:**
- Consume Task 1's `Branch::segments` and `ChildBlock` helpers.
- Produce `CopyBlockWithInsertedChild`, `CopyBlockWithReplacedChild`, and `ReplaceBlock(Branch*, uint8_t, ChildBlock*, ChildBlock*)` with release success/acquire failure ordering.

- [ ] **Step 1: Write RED concurrent publication tests.** Add a test-hook interleaving where writer A snapshots segment 0 for a direct insert and writer B wraps a segment-3 child; assert both leaves survive. Add a same-segment stale-block test which forces writer A's first CAS to fail, then expects a retry. Extend the model with a broken unconditional stale block store as the negative control.
- [ ] **Step 2: Run RED.** Run the two focused CTests and `python3 tests/models/cedar_pure_radix_interleaving_model.py --key-bits 4 --writers 3 --check-negative-control`. Expected: existing all-table protocol cannot expose the independently published segment behavior or fails the compile contract.
- [ ] **Step 3: Implement block-local CAS.** Change insertion, direct child insertion, and wrapper-parent replacement to copy and release-CAS only the affected segment. On every failed block CAS, restart acquire descent from root. Initialize both initial collision blocks before parent/root publication.
- [ ] **Step 4: Verify GREEN.** Run all `PartitionedVersionRadix` Debug tests and the interleaving model/negative control. Expected: all unique keys, duplicate results, and hashes match; the broken model loses a key.
- [ ] **Step 5: Commit.** `git add src/engine/rocksdb/memtable/cedar_pure_radix_index.cc src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc tests/models/cedar_pure_radix_interleaving_model.py && git commit -m "perf: publish radix child blocks independently"`

### Task 3: Port ordered reads and frozen traversal across segments

**Files:**
- Modify: `src/engine/rocksdb/memtable/cedar_pure_radix_index.cc`
- Modify: `src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc`

**Interfaces:**
- Consume Task 2's block-local child lookup.
- Produce `FirstChildAtOrAfter(Branch*, uint8_t)` and `LastChildAtOrBefore(Branch*, uint8_t)` returning a global nibble or 16.

- [ ] **Step 1: Write RED read contracts.** Add lower/upper-bound tests around global nibble boundaries 3/4, 7/8, 11/12 and a sparse first/last segment test. Add randomized ordered iteration/Seek parity versus SkipList and a frozen forward-scan test after `PrepareForFlush()`.
- [ ] **Step 2: Run RED.** Build and run only the new tests. Expected: compile failure or incorrect result because old helpers expect one `ChildTable*`.
- [ ] **Step 3: Implement segment-order helpers and port readers.** Resolve selected/global adjacent child by loading no more than four segment pointers, then port `MinimumLeaf`, `MaximumLeaf`, cursor descend/seek/backtracking, and frozen-chain construction without relaxing acquire loads.
- [ ] **Step 4: Verify GREEN.** Run all Debug `PartitionedVersionRadix` tests plus `python3 tests/models/cedar_pure_radix_model.py --check-negative-control`. Expected: ordering, snapshots, reverse seeks, point/get adapters, and frozen scans remain identical to SkipList.
- [ ] **Step 5: Commit.** `git add src/engine/rocksdb/memtable/cedar_pure_radix_index.cc src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc && git commit -m "perf: traverse segmented nibble radix blocks"`

### Task 4: Validate publication safety and benchmark the candidate

**Files:**
- Modify: `benchmarks/cedar_radix_memtable_bench.cc`
- Modify: `src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc`
- Create: `docs/superpowers/evidence/2026-09-22-cedar-segmented-nibble-patricia/matrix.csv`
- Create: `docs/superpowers/evidence/2026-09-22-cedar-segmented-nibble-patricia/report.md`

**Interfaces:**
- Rename only the stderr diagnostic label to `segment_snapshot_cas`; stdout retains 28 fields.

- [ ] **Step 1: Write RED publication stress.** Add readers that repeatedly seek/iterate while writers fill all four segments and race same-key insertion; assert expected count, zero errors, and a stable hash once writers finish. Add a benchmark CSV test expecting the segmented diagnostic label only with `--stats`.
- [ ] **Step 2: Run RED.** Build/run focused Debug test and `RadixBenchmarkCsvContract`. Expected: counter name or stress interface is absent.
- [ ] **Step 3: Implement diagnostics only.** Wire the block-CAS observer at `ReplaceBlock`; leave default factory options empty and stdout unchanged.
- [ ] **Step 4: Run full safety verification.** Run `git diff --check`, both models with negative controls, Debug/ASan/TSan `PartitionedVersionRadix` suites with `halt_on_error=1`, plus direct lifecycle (11), crash recovery (6), and format recovery (4) executables.
- [ ] **Step 5: Run the release gate.** With five independent processes and alternating implementation order, collect Vector/SkipList/Radix rows for 131072 random keys in `index` and `memtable` phases at one writer, and direct `memtable` rows at four/eight writers. Require matching hashes, expected ops, zero errors, repeatable one-writer improvement, no more than 5% B1 regression, and arena at/below binary Radix budget.
- [ ] **Step 6: Decide and commit evidence.** Mark B0 passed only at `Radix memtable <= 1.10 * Vector`. If it fails, record `PURE-RADIX-CONTINUE`, preserve raw data and leave the goal active. Commit retained implementation/evidence with `perf: measure segmented nibble radix`.
