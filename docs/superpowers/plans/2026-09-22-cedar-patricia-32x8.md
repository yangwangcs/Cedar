# Cedar 32-by-8 Patricia Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Select and verify a 32-by-8 pure byte-Patricia candidate under the existing Release, Arena, concurrency, and persistence gates.

**Architecture:** Keep one 40-byte Patricia tree. Replace four 64-value segment edges with 32 eight-value independent CAS edges, eight-bit occupancy, and exact-size immutable snapshots. Diagnostics are test-only; timed binaries share one clean benchmark source. Reject and revert on any performance failure; run the full persistence chain only on a retained candidate.

**Tech Stack:** C++20, RocksDB 11.1.2 Cedar kernel, GoogleTest, CMake, Python models and matrix summarizer.

**Spec:** `docs/superpowers/specs/2026-09-22-cedar-patricia-32x8-design.md`

## Global Constraints

- Preserve exactly 40 normalized key bytes including discriminator byte 39.
- Immutable published node/block payloads; release CAS, acquire failure/read, restart from root.
- No lock, shard, staging index, fallback, background writer, or WAL/SST format change.
- Preserve dirty `benchmarks/cedar_radix_memtable_bench.cc`, `tests/performance/test_radix_benchmark_csv.cmake`, and untracked `benchmarks/run_cedar_radix_read_matrix.sh`.
- Retention gates per seed: one writer <=1.05x four-by-64 Byte; four writers <=0.80x SkipList; eight writers <=0.85x SkipList; each Arena <=1.25x Nibble. Three seeds, five processes, 131072 keys.
- Historical Vector <=1.10x is reported separately and cannot be claimed without passing.

## Review Focus

- Distinct values in byte 39 must survive wrapping, Seek/SeekForPrev and frozen traversal.
- Every 7/8 segment boundary must preserve predecessor/successor order, including 247/248.
- A stale same-edge CAS must retry without losing a concurrent leaf, while different edges publish independently.
- Readers during publication must never see partially initialized branches or missing boundaries.
- Post-flush/reopen and unflushed-WAL replay must preserve visibility across old/new binaries.

---

### Task 1: Instrument Snapshot Cost Without Timing Pollution

**Files:** `src/engine/rocksdb/memtable/cedar_pure_radix_index.h`, `.cc`, `src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc`.

**Interfaces:** Add optional test hooks for allocated branch/block bytes, copied child pointers, and segment/root CAS outcome; invocation must be guarded by nonempty hooks. Keep benchmark stdout unchanged. A failed branch or block remains Arena-owned; observers count attempts separately from successful publication.

- [ ] Add a failing test that inserts a deterministic root, wrapper and direct child with hooks and checks positive allocated bytes, copy count and CAS outcomes; the existing API should fail to compile because the hooks are absent.
- [ ] Build `test_partitioned_version_radix_memtable` Debug and confirm that failure references the new hook names.
- [ ] Add minimal guarded hook calls at allocation/copy and CAS sites; no observer executes in a normal production instance.
- [ ] Rebuild and run `ctest --test-dir /Volumes/E/CedarBuild/pure-radix-cas-cursor-debug-20260921 -R PartitionedVersionRadix --output-on-failure`; require zero failures.
- [ ] Commit only these three files as `test: count patricia snapshot allocations`.

### Task 2: Specify 32-by-8 RED Contracts

**Files:** `src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc`, `tests/models/cedar_pure_radix_model.py`, `tests/models/cedar_pure_radix_interleaving_model.py`.

**Interfaces:** `kByteSegmentCount=32`, segment=`value>>3`, local=`value&7`, 8-bit occupancy, 32 exact-size blocks at a complete 256-value branch, and no child list larger than eight.

- [ ] Replace stale 4x64 representation assertions with a full byte-39 fixture; use literal occupancy `0xff`, eight children per segment, all 31 boundaries, Seek, SeekForPrev, and frozen traversal. Add independent-edge direct/wrapper and same-edge stale-CAS cases to the existing concurrency fixture family.
- [ ] Update both models to 32 segments and local width eight, preserving and running their broken-publication negative controls.
- [ ] Run both models with `--check-negative-control`, require pass and a detected negative control.
- [ ] Build the focused C++ target; require RED due to missing `kByteSegmentCount`, rather than an unrelated compiler failure.
- [ ] Commit RED tests/models only as `test: specify 32-by-8 patricia publication`.

### Task 3: Implement 32-by-8 GREEN

**Files:** `src/engine/rocksdb/memtable/cedar_pure_radix_index.h`, `.cc`.

**Interfaces:** `ChildBlock::occupied` is `uint8_t`, `Branch::segments` has 32 atomic edges; `FirstChildAtOrAfter`/`LastChildAtOrBefore` return 256 sentinel in `uint16_t`. `CopyBlockWithInsertedChild` and `CopyBlockWithReplacedChild` copy the one observed eight-value block. Branch initialization and release/acquire ordering are unchanged.

- [ ] Change representation constants, local masks, rank and exact-size allocation; `Through(7)` must use an eight-bit full mask without shifting by eight.
- [ ] Port all forward/reverse segment scans to 0..31, global value=`segment*8+local`, preserving 40-frame cursor/frozen chain.
- [ ] Run Debug build and 43+ focused tests; on failure diagnose before editing tests. Run both Python model negative controls.
- [ ] Run `git diff --check`, then commit only the two production files as `perf: publish 32-by-8 byte patricia`.

### Task 4: Same-Source Release A/B And Decision

**Files:** `docs/superpowers/evidence/2026-09-22-cedar-patricia-32x8/{matrix-raw,summary.json,report.md}`.

**Interfaces:** Candidate is a detached clean worktree at Task 3's commit; controls are existing `/Volumes/E/CedarBuild/patricia-retention-nibble-o3-20260922/cedar_radix_memtable_bench` and `/Volumes/E/CedarBuild/patricia-retention-byte-o3-20260922/cedar_radix_memtable_bench`. `run_cedar_patricia_retention_matrix.sh` writes `<output>/matrix-raw`; use an empty temporary root and move that child directory to evidence.

- [ ] Verify candidate and both controls' benchmark SHA-256 equal `4c4ea921641adcd529a995cfb1fa0e3e9bf398e77b4134a6c65023dc6594fa53` and source worktrees clean. Configure with Release, `BUILD_BENCHMARKS=ON`, `BUILD_TESTS=ON`, `/Volumes/E/CedarRocksDBCache`, parallel level 1; verify `/usr/bin/c++`, `-O3 -DNDEBUG`.
- [ ] Build only `cedar_radix_memtable_bench` in the candidate Release build. Run untimed 1024-key 1/4/8-writer `phase=all` smoke and 131072-key eight-writer diagnostics, checking zero errors and identical hashes. Report branch/block allocation, copied pointer and CAS outcome counters from Task 1's non-timed direct-index test separately.
- [ ] Run the production runner with `--nibble`, `--byte`, `--candidate`, and empty absolute `--output`; do not set test mode or pass `--stats`. Summarize `--candidate candidate`. Require 240 valid samples, expected hashes and no errors.
- [ ] On exit 0 and all four per-seed gates, retain. Otherwise write failed ratios, commit evidence, revert Task 3 and its representation-specific Task 2 assertions with new commits, then stop without starting Task 5. Never average seeds to reverse a failure.
- [ ] Commit raw matrix, summary and report as `perf: measure 32-by-8 patricia`.

### Task 5: Freeze, Persistence, Compatibility And Review (Only If Retained)

**Files:** `src/engine/rocksdb/memtable/partitioned_version_radix_memtable_test.cc`, `tests/storage/test_rocksdb_lifecycle.cc`, `tests/storage/cedar_radix_format_fixture.cc`, `tests/CMakeLists.txt`, `docs/superpowers/evidence/2026-09-22-cedar-patricia-32x8-full-chain/{commands.txt,report.md}`.

**Interfaces:** Fixture modes `write-sst ABS_DB`, `write-wal ABS_DB`, `verify ABS_DB` write fixed facts, flush/wait or exit before destructor, then verify through the normal path. Compile identical fixture source against clean control revision `76adf44` and selected revision.

- [ ] Add RED freeze test: active count/order/hash equals repeated `MarkReadOnly`/`PrepareForFlush` frozen scans across all 32 byte-39 segments; ready forward scan adds zero branch loads after Seek.
- [ ] Add RED raw RocksDB lifecycle test: Put/Delete same fact key and Put a survivor, flush `wait=true`, close/reopen, assert deleted NotFound and survivor value. These internal keys differ in normalized byte 39.
- [ ] Implement fixture and test it within one build for SST and unflushed WAL; verify old->new and new->old for both SST and WAL, count/order/value and commit sequence, with file listing recorded before verification.
- [ ] Run focused Debug, ASan (`ASAN_OPTIONS=halt_on_error=1`) and TSan (`TSAN_OPTIONS=halt_on_error=1:report_bugs=1`) suites; run direct Debug binaries `test_rocksdb_lifecycle`, `test_recovery_crash_matrix`, `test_recovery_format`. Any sanitizer or semantic report fails.
- [ ] Record every command, output, fixture revision, hashes, format result, all per-seed gates and remaining risks. Commit tests, fixture and evidence without staging dirty paths.
- [ ] Perform a fresh whole-branch code review, fix Critical/Important findings with RED/GREEN and rerun green suites; only then audit the complete active goal and consider `complete`.
